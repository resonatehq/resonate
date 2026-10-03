//! A Unikraft Cloud instance per task.
//!
//! Unikraft fixes an instance's arguments and environment when it is created,
//! and gives it no stdin, no exec and no console to write to: an instance is a
//! service, reached over HTTP through Unikraft's edge. So:
//!
//! - **create** makes nothing. The instance cannot exist before the command
//!   it runs is known.
//! - **exec** creates the instance, started, with one service: 443 at the edge
//!   to rn8's `--listen` port inside. The environment carries `RN8_LISTEN`
//!   and a fresh `RN8_TOKEN`, so the image's own entrypoint — rn8 — takes its
//!   frames over HTTP: `GET /frames` streams them out, `POST /frames` sends
//!   them in. The token is what makes that public port the plugin's alone.
//! - **stderr** is the instance's console log, which is where rn8's own
//!   diagnostics go; **wait** is the instance stopping, with rn8's exit status.
//! - **destroy** deletes the instance.
//!
//! Unikraft has no outbound-network policy to set, so this backend enforces
//! no egress; the plugin requires `egress = "all"` for it, so that is never a
//! surprise.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use resonate_sandbox::{Backend, Command, Limits, Process, Stderr, Stdin, Stdout};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// How to reach Unikraft Cloud.
#[derive(Debug, Clone)]
pub struct Options {
    /// The API token. Required.
    pub token: Option<String>,
    /// The control plane, e.g. `https://api.fra.unikraft.cloud`.
    pub api_url: String,
    /// The scheme instances are reached on through the edge. `https`, except
    /// against a stand-in.
    pub edge_scheme: String,
    /// The port rn8 listens on inside the instance.
    pub port: u16,
    /// How long a new instance has to boot and answer on its port.
    pub ready_timeout: Duration,
    /// How often to ask whether an instance has stopped, and for new log.
    pub poll: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            token: None,
            api_url: "https://api.fra.unikraft.cloud".into(),
            edge_scheme: "https".into(),
            // Not 8080: that is the worker's, where rn8 pushes the task.
            port: 9000,
            ready_timeout: Duration::from_secs(60),
            poll: Duration::from_millis(250),
        }
    }
}

#[derive(Clone)]
pub struct Unikraft {
    inner: Arc<Inner>,
}

struct Inner {
    options: Options,
    limits: Limits,
    client: Result<reqwest::Client, String>,
}

/// A sandbox: before exec, only its image; after, its instance.
#[derive(Debug)]
pub struct Handle {
    image: String,
    instance: Mutex<Option<String>>,
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unikraft: {}", self.0)
    }
}

impl std::error::Error for Error {}

fn err(e: impl std::fmt::Display) -> Error {
    Error(e.to_string())
}

/// One instance in an API answer: `{"status", "data": {"instances": [...]}}`.
#[derive(Debug, Deserialize)]
struct Instance {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    uuid: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    exit_code: Option<i64>,
    #[serde(default)]
    service_group: Option<ServiceGroup>,
}

#[derive(Debug, Deserialize)]
struct ServiceGroup {
    #[serde(default)]
    domains: Vec<Domain>,
}

#[derive(Debug, Deserialize)]
struct Domain {
    fqdn: String,
}

/// One instance's log in an API answer.
#[derive(Debug, Deserialize)]
struct Log {
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    range: Option<Range>,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Range {
    #[serde(default)]
    start: u64,
    end: u64,
}

impl Unikraft {
    pub fn new(options: Options, limits: Limits) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string());
        Self {
            inner: Arc::new(Inner {
                options,
                limits,
                client,
            }),
        }
    }

    fn api(&self) -> Result<Api, Error> {
        let client = self.inner.client.as_ref().map_err(err)?.clone();
        let token = self
            .inner
            .options
            .token
            .clone()
            .ok_or_else(|| err("no API token"))?;
        Ok(Api {
            client,
            token,
            url: self.inner.options.api_url.trim_end_matches('/').to_string(),
        })
    }

    /// The `POST /v1/instances` body for one task.
    fn create_body(&self, image: &str, command: &Command, token: &str) -> Value {
        let o = &self.inner.options;
        let mut env: BTreeMap<String, String> = command.env.iter().cloned().collect();
        env.insert("RN8_LISTEN".into(), o.port.to_string());
        env.insert("RN8_TOKEN".into(), token.into());
        let mut body = json!({
            "image": image,
            "autostart": true,
            // A step runs once; a crash is the plugin's to see, not to retry.
            "restart_policy": "never",
            "env": env,
            "service_group": {
                "services": [
                    { "port": 443, "destination_port": o.port, "handlers": ["tls", "http"] }
                ]
            },
        });
        if !command.argv.is_empty() {
            body["args"] = json!(command.argv);
        }
        if let Some(cpus) = self.inner.limits.cpus {
            body["vcpus"] = json!(cpus);
        }
        if let Some(mib) = self.inner.limits.memory_mib {
            body["memory_mb"] = json!(mib);
        }
        body
    }
}

#[derive(Clone)]
struct Api {
    client: reqwest::Client,
    token: String,
    url: String,
}

impl Api {
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.url))
            .bearer_auth(&self.token)
    }

    /// The one instance an answer is about, or why not.
    async fn instance(&self, resp: reqwest::Response, what: &str) -> Result<Value, Error> {
        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .map_err(|e| err(format!("{what}: {status}: {e}")))?;
        let message = body.get("message").and_then(Value::as_str);
        let first = body.pointer("/data/instances/0").cloned().ok_or_else(|| {
            err(format!(
                "{what}: {status}: {}",
                message.unwrap_or("no instance")
            ))
        })?;
        if first.get("status").and_then(Value::as_str) == Some("error") {
            let m = first.get("message").and_then(Value::as_str).or(message);
            return Err(err(format!("{what}: {}", m.unwrap_or("error"))));
        }
        Ok(first)
    }

    async fn get(&self, uuid: &str) -> Result<Instance, Error> {
        let resp = self
            .request(reqwest::Method::GET, &format!("/v1/instances/{uuid}"))
            .send()
            .await
            .map_err(err)?;
        serde_json::from_value(self.instance(resp, "get instance").await?).map_err(err)
    }

    async fn delete(&self, uuid: &str) -> Result<(), Error> {
        let resp = self
            .request(reqwest::Method::DELETE, &format!("/v1/instances/{uuid}"))
            .send()
            .await
            .map_err(err)?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        match self.instance(resp, "delete instance").await {
            Ok(_) => Ok(()),
            // Idempotent: an instance that is already gone is deleted.
            Err(e) if e.0.contains("not found") || e.0.contains("No instance") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// The console log from `offset`.
    async fn log(&self, uuid: &str, offset: u64) -> Result<Log, Error> {
        let resp = self
            .request(reqwest::Method::GET, &format!("/v1/instances/{uuid}/log"))
            .json(&json!({ "offset": offset }))
            .send()
            .await
            .map_err(err)?;
        serde_json::from_value(self.instance(resp, "instance log").await?).map_err(err)
    }
}

impl Backend for Unikraft {
    type Handle = Handle;
    type Process = UnikraftProcess;
    type Error = Error;

    async fn create(&self, image: &str) -> Result<Handle, Error> {
        // Nothing here could enforce less; refuse rather than pretend.
        if self.inner.limits.egress != resonate_sandbox::Egress::All {
            return Err(err(format!(
                "{image}: egress {:?} cannot be enforced; Unikraft instances reach the network",
                self.inner.limits.egress
            )));
        }
        Ok(Handle {
            image: image.to_string(),
            instance: Mutex::new(None),
        })
    }

    async fn exec(&self, handle: &Handle, command: Command) -> Result<UnikraftProcess, Error> {
        let api = self.api()?;
        let token = format!("{:016x}{:016x}", fastrand::u64(..), fastrand::u64(..));
        let resp = api
            .request(reqwest::Method::POST, "/v1/instances")
            .json(&self.create_body(&handle.image, &command, &token))
            .send()
            .await
            .map_err(err)?;
        let created: Instance =
            serde_json::from_value(api.instance(resp, "create instance").await?).map_err(err)?;
        let uuid = created
            .uuid
            .ok_or_else(|| err(format!("create instance: no uuid ({:?})", created.message)))?;
        // Recorded before anything else can fail, so destroy finds it.
        *handle.instance.lock().expect("instance") = Some(uuid.clone());
        let fqdn = created
            .service_group
            .and_then(|g| g.domains.into_iter().next())
            .map(|d| d.fqdn)
            .ok_or_else(|| err("create instance: no domain"))?;
        let o = &self.inner.options;
        Ok(UnikraftProcess::start(
            api,
            Edge {
                client: self.inner.client.as_ref().map_err(err)?.clone(),
                url: format!("{}://{fqdn}", o.edge_scheme),
                token,
                ready: Instant::now() + o.ready_timeout,
                poll: o.poll,
            },
            uuid,
            o.poll,
        ))
    }

    async fn destroy(&self, handle: Handle) -> Result<(), Error> {
        let Some(uuid) = handle.instance.lock().expect("instance").take() else {
            return Ok(());
        };
        self.api()?.delete(&uuid).await
    }
}

/// rn8, through Unikraft's edge.
#[derive(Clone)]
struct Edge {
    client: reqwest::Client,
    url: String,
    token: String,
    /// Until then, an edge that does not answer yet is an instance booting.
    ready: Instant,
    poll: Duration,
}

impl Edge {
    /// Send, retrying while the instance may still be booting.
    async fn send(
        &self,
        make: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, String> {
        loop {
            let last = match make().bearer_auth(&self.token).send().await {
                Ok(r) if r.status().is_success() => return Ok(r),
                // Not up yet: the edge has nowhere to send it.
                Ok(r) if r.status().is_server_error() || r.status() == 404 => {
                    format!("{}", r.status())
                }
                Ok(r) => return Err(format!("{}", r.status())),
                Err(e) => e.to_string(),
            };
            if Instant::now() >= self.ready {
                return Err(format!("not ready in time: {last}"));
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}

/// An instance's rn8 as the byte streams the plugin expects.
pub struct UnikraftProcess {
    stdin: Option<Stdin>,
    stdout: Option<Stdout>,
    stderr: Option<Stderr>,
    api: Api,
    uuid: String,
    poll: Duration,
    pumps: Vec<JoinHandle<()>>,
}

/// How much of a stream may sit between a pump and the plugin.
const PIPE: usize = 1 << 20;

impl UnikraftProcess {
    fn start(api: Api, edge: Edge, uuid: String, poll: Duration) -> Self {
        let (stdin, stdin_pump) = tokio::io::duplex(PIPE);
        let (stdout_pump, stdout) = tokio::io::duplex(PIPE);
        let (stderr_pump, stderr) = tokio::io::duplex(PIPE);
        let pumps = vec![
            tokio::spawn(pump_in(edge.clone(), stdin_pump)),
            tokio::spawn(pump_out(edge, stdout_pump)),
        ];
        // Not aborted with the rest: the console's last lines — rn8's
        // diagnostics, the exit — come after the frames end, and are the ones
        // worth having. It ends on its own once the instance has stopped and
        // the log is read, or is gone.
        tokio::spawn(pump_log(api.clone(), uuid.clone(), poll, stderr_pump));
        Self {
            stdin: Some(Box::new(stdin)),
            stdout: Some(Box::new(stdout)),
            stderr: Some(Box::new(stderr)),
            api,
            uuid,
            poll,
            pumps,
        }
    }
}

impl Drop for UnikraftProcess {
    fn drop(&mut self) {
        for p in &self.pumps {
            p.abort();
        }
    }
}

impl Process for UnikraftProcess {
    fn stdin(&mut self) -> Option<Stdin> {
        self.stdin.take()
    }

    fn stdout(&mut self) -> Option<Stdout> {
        self.stdout.take()
    }

    fn stderr(&mut self) -> Option<Stderr> {
        self.stderr.take()
    }

    /// The instance stopping: rn8 exited, and with it the unikernel.
    async fn wait(&mut self) -> std::io::Result<i32> {
        loop {
            let i = self
                .api
                .get(&self.uuid)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            if i.state.as_deref() == Some("stopped") {
                return Ok(i.exit_code.map(|c| c as i32).unwrap_or(-1));
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}

/// Whatever the plugin writes, as `POST /frames`, in order; EOF is
/// `POST /frames/close`.
async fn pump_in(edge: Edge, mut from: DuplexStream) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        // A read error is the plugin's end gone: treated as EOF.
        let n = from.read(&mut buf).await.unwrap_or_default();
        let (path, body) = if n == 0 {
            ("/frames/close", Vec::new())
        } else {
            ("/frames", buf[..n].to_vec())
        };
        let url = format!("{}{path}", edge.url);
        let sent = edge
            .send(|| {
                edge.client
                    .post(&url)
                    .header("content-type", "application/x-ndjson")
                    .body(body.clone())
            })
            .await;
        if sent.is_err() || n == 0 {
            // Gone or unreachable: dropping `from` makes the plugin's next
            // write fail, which is how it finds out.
            return;
        }
    }
}

/// `GET /frames`, as bytes on `to`. EOF when the response ends.
async fn pump_out(edge: Edge, mut to: DuplexStream) {
    let url = format!("{}/frames", edge.url);
    let Ok(mut resp) = edge.send(|| edge.client.get(&url)).await else {
        return;
    };
    while let Ok(Some(chunk)) = resp.chunk().await {
        if to.write_all(&chunk).await.is_err() {
            return;
        }
    }
}

/// The console log, as it grows, until the instance has stopped and it is
/// all read.
async fn pump_log(api: Api, uuid: String, poll: Duration, mut to: DuplexStream) {
    let mut offset = 0;
    loop {
        let Ok(log) = api.log(&uuid, offset).await else {
            return;
        };
        let text = log
            .output
            .as_deref()
            .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
            .unwrap_or_default();
        // The answer can overlap what was already read: `output` is the
        // bytes from `range.start`, whatever offset was asked for.
        let (start, end) = log
            .range
            .map(|r| (r.start, r.end))
            .unwrap_or((offset, offset + text.len() as u64));
        let fresh = &text[(offset.saturating_sub(start) as usize).min(text.len())..];
        if !fresh.is_empty() && to.write_all(fresh).await.is_err() {
            return;
        }
        let moved = end > offset;
        offset = offset.max(end);
        if !moved && log.state.as_deref() == Some("stopped") {
            return;
        }
        if !moved {
            tokio::time::sleep(poll).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(limits: Limits) -> Unikraft {
        Unikraft::new(
            Options {
                token: Some("t".into()),
                ..Options::default()
            },
            limits,
        )
    }

    #[test]
    fn the_instance_runs_rn8_on_its_port_behind_a_token() {
        let b = backend(Limits {
            cpus: Some(1),
            memory_mib: Some(512),
            ..Limits::default()
        });
        let cmd = Command {
            argv: vec![],
            env: vec![("A".into(), "b".into())],
        };
        let body = b.create_body("acme/worker@sha256:00", &cmd, "secret");
        assert_eq!(body["image"], "acme/worker@sha256:00");
        assert_eq!(body["autostart"], true);
        assert_eq!(body["restart_policy"], "never");
        assert_eq!(body["vcpus"], 1);
        assert_eq!(body["memory_mb"], 512);
        assert_eq!(body["env"]["A"], "b");
        assert_eq!(body["env"]["RN8_LISTEN"], "9000");
        assert_eq!(body["env"]["RN8_TOKEN"], "secret");
        assert_eq!(
            body["service_group"]["services"][0],
            json!({ "port": 443, "destination_port": 9000, "handlers": ["tls", "http"] })
        );
        // No command: the image's own, which is rn8.
        assert!(body.get("args").is_none());
    }

    #[test]
    fn a_command_is_the_instances_args_and_no_limits_send_none() {
        let b = backend(Limits::default());
        let cmd = Command {
            argv: vec!["rn8".into(), "--".into(), "node".into()],
            env: vec![],
        };
        let body = b.create_body("img", &cmd, "secret");
        assert_eq!(body["args"], json!(["rn8", "--", "node"]));
        assert!(body.get("vcpus").is_none());
        assert!(body.get("memory_mb").is_none());
    }
}
