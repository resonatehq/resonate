//! Resonate sandbox backend: Tensorlake.
//!
//! Each task gets its own Tensorlake sandbox, booted from the task's image:
//!
//! | trait     | Tensorlake                                                        |
//! |-----------|-------------------------------------------------------------------|
//! | `create`  | `POST {api}/sandboxes`, then `GET {api}/sandboxes/{id}` until running |
//! | `exec`    | `POST {sandbox}/api/v1/processes` with `stdin_mode: "pipe"`       |
//! | stdin     | `POST …/processes/{pid}/stdin`, `POST …/stdin/close` at EOF       |
//! | stdout    | `GET …/processes/{pid}/stdout/follow`, server-sent events         |
//! | stderr    | `GET …/processes/{pid}/stderr/follow`, server-sent events         |
//! | `wait`    | `GET …/processes/{pid}` until it has exited                       |
//! | `destroy` | `DELETE {api}/sandboxes/{id}`                                      |
//!
//! Tensorlake's process API is HTTP, not a pipe, so the backend makes one:
//! the plugin writes and reads ordinary byte streams, and pumps in between
//! turn them into requests and events. Output arrives line by line, which is
//! exactly the shape of frames — one JSON value per line.
//!
//! The sandbox's own `timeout_secs` is the backstop for orphans: a sandbox
//! whose plugin died before `destroy` is reaped by Tensorlake when it expires.

mod sse;
#[cfg(feature = "fake")]
pub mod testing;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use resonate_sandbox::{Backend, Command, Egress, Limits, Process, Stderr, Stdin, Stdout};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;

/// Where Tensorlake is, and how long to wait for it.
#[derive(Debug, Clone)]
pub struct Options {
    /// The control plane [default: https://api.tensorlake.ai].
    pub api_url: String,
    /// The sandbox proxy. A sandbox is reached at this URL with its id
    /// prepended to the host, unless the create answer names its own URL
    /// [default: https://sandbox.tensorlake.ai].
    pub proxy_url: String,
    pub api_key: Option<String>,
    /// Tensorlake terminates the sandbox after this long, whatever happens to
    /// the plugin.
    pub timeout_secs: u64,
    /// How long a new sandbox may take to start running.
    pub ready_timeout: Duration,
    /// How often sandbox and process status are polled.
    pub poll_interval: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            api_url: "https://api.tensorlake.ai".into(),
            proxy_url: "https://sandbox.tensorlake.ai".into(),
            api_key: None,
            timeout_secs: 900,
            ready_timeout: Duration::from_secs(120),
            poll_interval: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tensorlake {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    options: Options,
    limits: Limits,
    /// Built on first use rather than in the constructor: building a client
    /// reads root certificates and the resolver configuration, which the
    /// plugin's `configure` — sync and side-effect-free — must not do.
    client: OnceLock<Result<reqwest::Client, String>>,
}

/// A sandbox this backend created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handle {
    pub id: String,
    /// Its proxy, where processes run.
    pub url: String,
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tensorlake: {}", self.0)
    }
}

impl std::error::Error for Error {}

fn err(e: impl std::fmt::Display) -> Error {
    Error(e.to_string())
}

#[derive(Debug, Deserialize)]
struct Created {
    sandbox_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    sandbox_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Info {
    #[serde(default)]
    status: String,
    #[serde(default)]
    sandbox_url: Option<String>,
    #[serde(default)]
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    termination_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProcessInfo {
    pid: i64,
    #[serde(default)]
    status: String,
    #[serde(default)]
    exit_code: Option<i64>,
    #[serde(default)]
    signal: Option<i64>,
}

impl Tensorlake {
    pub fn new(options: Options, limits: Limits) -> Self {
        Self {
            inner: Arc::new(Inner {
                options,
                limits,
                client: OnceLock::new(),
            }),
        }
    }

    fn client(&self) -> Result<&reqwest::Client, Error> {
        self.inner
            .client
            .get_or_init(|| {
                reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(10))
                    .build()
                    .map_err(|e| format!("cannot build an HTTP client: {e}"))
            })
            .as_ref()
            .map_err(|e| Error(e.clone()))
    }

    fn key(&self) -> Result<&str, Error> {
        self.inner
            .options
            .api_key
            .as_deref()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| err("no API key: set api_key, or TENSORLAKE_API_KEY"))
    }

    fn api(&self, path: &str) -> String {
        format!("{}{path}", self.inner.options.api_url.trim_end_matches('/'))
    }

    /// The create request for one sandbox.
    fn create_body(&self, image: &str) -> Value {
        let limits = &self.inner.limits;
        let mut body = json!({
            "image": image,
            "timeout_secs": self.inner.options.timeout_secs,
            "network": { "allow_internet_access": limits.egress == Egress::All },
        });
        let mut resources = serde_json::Map::new();
        if let Some(cpus) = limits.cpus {
            resources.insert("cpus".into(), json!(cpus as f64));
        }
        if let Some(mib) = limits.memory_mib {
            resources.insert("memory_mb".into(), json!(mib));
        }
        if !resources.is_empty() {
            body["resources"] = Value::Object(resources);
        }
        body
    }

    /// `https://sandbox.tensorlake.ai` → `https://<id>.sandbox.tensorlake.ai`.
    fn derived_url(&self, id: &str) -> String {
        let proxy = self.inner.options.proxy_url.trim_end_matches('/');
        match proxy.split_once("://") {
            Some((scheme, host)) => format!("{scheme}://{id}.{host}"),
            None => format!("https://{id}.{proxy}"),
        }
    }

    async fn info(&self, id: &str) -> Result<Info, Error> {
        let resp = self
            .client()?
            .get(self.api(&format!("/sandboxes/{id}")))
            .bearer_auth(self.key()?)
            .send()
            .await
            .map_err(err)?;
        json_or_error(resp, "get sandbox").await
    }

    /// Poll until the sandbox runs, then answer its proxy URL.
    async fn until_running(&self, created: Created) -> Result<String, Error> {
        let id = &created.sandbox_id;
        let mut url = created.sandbox_url.filter(|u| !u.is_empty());
        let mut status = created.status;
        let deadline = tokio::time::Instant::now() + self.inner.options.ready_timeout;
        loop {
            match status.to_ascii_lowercase().as_str() {
                "running" => return Ok(url.unwrap_or_else(|| self.derived_url(id))),
                "terminated" | "suspended" | "failed" | "timeout" => {
                    return Err(err(format!("sandbox {id} is {status} before it ran")))
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(err(format!("sandbox {id} did not start running in time")));
            }
            tokio::time::sleep(self.inner.options.poll_interval).await;
            let info = self.info(id).await?;
            if let Some(reason) = info.termination_reason.filter(|r| !r.is_empty()) {
                return Err(err(format!("sandbox {id} terminated: {reason}")));
            }
            status = info.status;
            url = url.or(info.sandbox_url.filter(|u| !u.is_empty()));
        }
    }

    async fn delete(&self, id: &str) -> Result<(), Error> {
        let resp = self
            .client()?
            .delete(self.api(&format!("/sandboxes/{id}")))
            .bearer_auth(self.key()?)
            .send()
            .await
            .map_err(err)?;
        // Idempotent: a sandbox that is already gone is destroyed.
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        let status = resp.status();
        Err(err(format!(
            "delete sandbox {id}: {status}: {}",
            resp.text().await.unwrap_or_default()
        )))
    }
}

impl Backend for Tensorlake {
    type Handle = Handle;
    type Process = TensorlakeProcess;
    type Error = Error;

    async fn create(&self, image: &str) -> Result<Handle, Error> {
        let resp = self
            .client()?
            .post(self.api("/sandboxes"))
            .bearer_auth(self.key()?)
            .json(&self.create_body(image))
            .send()
            .await
            .map_err(err)?;
        let created: Created = json_or_error(resp, "create sandbox").await?;
        let id = created.sandbox_id.clone();
        match self.until_running(created).await {
            Ok(url) => Ok(Handle { id, url }),
            Err(e) => {
                // Made but never handed out: nobody else will delete it.
                let _ = self.delete(&id).await;
                Err(e)
            }
        }
    }

    async fn exec(&self, handle: &Handle, command: Command) -> Result<TensorlakeProcess, Error> {
        let argv = if command.argv.is_empty() {
            // No command named: the image's own entrypoint, as Tensorlake
            // reports it for the sandbox.
            self.info(&handle.id)
                .await?
                .entrypoint
                .filter(|e| !e.is_empty())
                .ok_or_else(|| err("the sandbox reports no entrypoint; name the command to run"))?
        } else {
            command.argv
        };
        let env: BTreeMap<String, String> = command.env.into_iter().collect();
        let proxy = Proxy {
            client: self.client()?.clone(),
            key: self.key()?.to_string(),
            url: handle.url.trim_end_matches('/').to_string(),
            sandbox: handle.id.clone(),
            poll: self.inner.options.poll_interval,
        };
        let resp = proxy
            .post("/api/v1/processes")
            .json(&json!({
                "command": argv[0],
                "args": &argv[1..],
                "env": env,
                "stdin_mode": "pipe",
            }))
            .send()
            .await
            .map_err(err)?;
        let started: ProcessInfo = json_or_error(resp, "start process").await?;
        Ok(TensorlakeProcess::start(proxy, started.pid))
    }

    async fn destroy(&self, handle: Handle) -> Result<(), Error> {
        self.delete(&handle.id).await
    }
}

/// One sandbox's process API.
#[derive(Clone)]
struct Proxy {
    client: reqwest::Client,
    key: String,
    url: String,
    sandbox: String,
    poll: Duration,
}

impl Proxy {
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.url))
            .bearer_auth(&self.key)
            .header("X-Tensorlake-Sandbox-Id", &self.sandbox)
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, path)
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, path)
    }
}

/// A process in a Tensorlake sandbox, as the byte streams the plugin expects.
pub struct TensorlakeProcess {
    stdin: Option<Stdin>,
    stdout: Option<Stdout>,
    stderr: Option<Stderr>,
    proxy: Proxy,
    pid: i64,
    pumps: Vec<JoinHandle<()>>,
}

/// How much of a stream may sit between a pump and the plugin.
const PIPE: usize = 1 << 20;

impl TensorlakeProcess {
    fn start(proxy: Proxy, pid: i64) -> Self {
        let (stdin, stdin_pump) = tokio::io::duplex(PIPE);
        let (stdout_pump, stdout) = tokio::io::duplex(PIPE);
        let (stderr_pump, stderr) = tokio::io::duplex(PIPE);
        let pumps = vec![
            tokio::spawn(pump_stdin(proxy.clone(), pid, stdin_pump)),
            tokio::spawn(pump_output(proxy.clone(), pid, "stdout", stdout_pump)),
            tokio::spawn(pump_output(proxy.clone(), pid, "stderr", stderr_pump)),
        ];
        Self {
            stdin: Some(Box::new(stdin)),
            stdout: Some(Box::new(stdout)),
            stderr: Some(Box::new(stderr)),
            proxy,
            pid,
            pumps,
        }
    }
}

impl Drop for TensorlakeProcess {
    fn drop(&mut self) {
        for p in &self.pumps {
            p.abort();
        }
    }
}

impl Process for TensorlakeProcess {
    fn stdin(&mut self) -> Option<Stdin> {
        self.stdin.take()
    }

    fn stdout(&mut self) -> Option<Stdout> {
        self.stdout.take()
    }

    fn stderr(&mut self) -> Option<Stderr> {
        self.stderr.take()
    }

    async fn wait(&mut self) -> std::io::Result<i32> {
        let path = format!("/api/v1/processes/{}", self.pid);
        loop {
            let info: Result<ProcessInfo, Error> = match self.proxy.get(&path).send().await {
                Ok(resp) => json_or_error(resp, "get process").await,
                Err(e) => Err(err(e)),
            };
            match info {
                Ok(p) => {
                    if let Some(sig) = p.signal {
                        return Ok(128 + sig as i32);
                    }
                    if let Some(code) = p.exit_code {
                        return Ok(code as i32);
                    }
                    if !matches!(p.status.as_str(), "running" | "starting" | "") {
                        return Ok(-1);
                    }
                }
                Err(e) => return Err(std::io::Error::other(e.to_string())),
            }
            tokio::time::sleep(self.proxy.poll).await;
        }
    }
}

/// Whatever the plugin writes, as stdin requests, in order; EOF closes stdin.
async fn pump_stdin(proxy: Proxy, pid: i64, mut from: DuplexStream) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        // A read error is the plugin's end gone: treated as EOF.
        let n = from.read(&mut buf).await.unwrap_or_default();
        if n == 0 {
            let _ = proxy
                .post(&format!("/api/v1/processes/{pid}/stdin/close"))
                .send()
                .await;
            return;
        }
        let sent = proxy
            .post(&format!("/api/v1/processes/{pid}/stdin"))
            .header("content-type", "application/octet-stream")
            .body(buf[..n].to_vec())
            .send()
            .await;
        if !matches!(sent, Ok(ref r) if r.status().is_success()) {
            // The process is gone or unreachable. Dropping `from` makes the
            // plugin's next write fail, which is how it finds out.
            return;
        }
    }
}

/// A followed output stream, as lines on `to`. EOF when the stream ends.
async fn pump_output(proxy: Proxy, pid: i64, stream: &'static str, mut to: DuplexStream) {
    let resp = proxy
        .get(&format!("/api/v1/processes/{pid}/{stream}/follow"))
        .header("accept", "text/event-stream")
        .send()
        .await;
    let Ok(mut resp) = resp else { return };
    if !resp.status().is_success() {
        return;
    }
    let mut events = sse::Parser::default();
    while let Ok(Some(chunk)) = resp.chunk().await {
        for data in events.feed(&chunk) {
            // Heartbeats and anything else that is not an output line are
            // skipped, as Tensorlake's own SDK does.
            let Ok(line) = serde_json::from_str::<Value>(&data) else {
                continue;
            };
            let Some(line) = line.get("line").and_then(Value::as_str) else {
                continue;
            };
            if to.write_all(line.as_bytes()).await.is_err() || to.write_all(b"\n").await.is_err() {
                return;
            }
        }
    }
}

async fn json_or_error<T: for<'de> Deserialize<'de>>(
    resp: reqwest::Response,
    what: &str,
) -> Result<T, Error> {
    let status = resp.status();
    let body = resp.text().await.map_err(err)?;
    if !status.is_success() {
        return Err(err(format!("{what}: {status}: {}", body.trim())));
    }
    serde_json::from_str(&body).map_err(|e| err(format!("{what}: unexpected answer {body:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_asks_for_the_limits_and_no_internet() {
        let t = Tensorlake::new(
            Options::default(),
            Limits {
                cpus: Some(2),
                memory_mib: Some(1024),
                egress: Egress::None,
            },
        );
        let body = t.create_body("ghcr.io/a/b@sha256:00");
        assert_eq!(body["image"], "ghcr.io/a/b@sha256:00");
        assert_eq!(body["network"]["allow_internet_access"], false);
        assert_eq!(body["resources"]["cpus"], 2.0);
        assert_eq!(body["resources"]["memory_mb"], 1024);
        assert_eq!(body["timeout_secs"], 900);
    }

    #[test]
    fn egress_all_allows_the_internet_and_no_limits_sends_no_resources() {
        let t = Tensorlake::new(
            Options::default(),
            Limits {
                egress: Egress::All,
                ..Limits::default()
            },
        );
        let body = t.create_body("img");
        assert_eq!(body["network"]["allow_internet_access"], true);
        assert!(body.get("resources").is_none());
    }

    #[test]
    fn the_proxy_url_carries_the_sandbox_id() {
        let t = Tensorlake::new(Options::default(), Limits::default());
        assert_eq!(t.derived_url("sb-1"), "https://sb-1.sandbox.tensorlake.ai");
    }
}
