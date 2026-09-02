//! Resonate worker: bash execution.
//!
//! A worker rather than a transport: the other end of a `bash://` address is
//! this process, so it does not deliver anywhere — it acquires the task, runs
//! the script, and settles the promise. Every state change goes through the
//! [`ResonateServer`](resonate_plugin::ResonateServer) port, the same path a
//! remote worker's HTTP calls take.
//!
//! Three backends behind one scheme: `bash://` runs locally, `bash://docker/<image>`
//! in a container, `bash://tensorlake/<image>` in a Tensorlake sandbox.

/// The address scheme this worker serves.
pub const SCHEME: &str = "bash";

/// This worker, as a plugin. The one thing a binary names to get `bash://`
/// addresses executed in this process.
pub static PLUGIN: resonate_plugin::WorkerPlugin =
    resonate_plugin::WorkerPlugin::new(env!("CARGO_PKG_NAME"), &[SCHEME], configure);

/// Read `[workers.worker_bash]`, and build the worker unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::WorkerDependencies,
) -> Result<Option<std::sync::Arc<dyn ResonateWorker>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    // `task.acquire` validates ttl >= 1, so a non-positive lease would 400 on
    // every acquire and this worker would silently never run anything.
    if config.lease_timeout < 1 {
        return Err(settings.reject("lease_timeout", "must be at least 1"));
    }
    Ok(Some(std::sync::Arc::new(BashExecTransport::new(
        deps.server,
        config,
    ))))
}

/// Everything under `[transports.bash_exec]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Enable the bash:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// Lease TTL (ms) this worker requests when acquiring a task, and the
    /// basis for its heartbeat interval (a third of it).
    ///
    /// The lease has to outlast the script: if it expires the task is
    /// redispatched to another worker while this one is still running.
    ///
    /// Its own, with its own default. It used to be unset-means-follow
    /// `tasks.lease_timeout`, which was a cross-section read no other plugin
    /// makes: config stops at a plugin's own edge.
    #[serde(default = "default_lease_timeout")]
    pub lease_timeout: i64,
}

fn default_lease_timeout() -> i64 {
    15_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            lease_timeout: default_lease_timeout(),
        }
    }
}

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::process::Command;

use resonate_plugin::types::{
    Message, RequestEnvelope, RequestHead, ResponseEnvelope, TaskAcquireResponseData,
    PROTOCOL_VERSION,
};
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};

// ─── Backend trait ────────────────────────────────────────────────────────────
//
// Every backend just runs a script and returns an outcome. Lifecycle plumbing
// (acquire / heartbeat / fulfill / reject) lives in `run_task` below.

#[async_trait]
pub trait ExecBackend: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run(&self, req: ExecRequest) -> ExecOutcome;
}

pub struct ExecRequest {
    pub task_id: String,
    pub script: String,
    /// Backend-specific selector. Local ignores it; Docker/Tensorlake require
    /// an image name.
    pub target: Option<String>,
    /// Promise creation time (ms since epoch). Stable across retries.
    pub created_at: i64,
    /// Promise timeout / deadline (ms since epoch). Stable across retries —
    /// lets scripts loop until the deadline rather than for a fixed duration,
    /// which makes them idempotent across restart-from-top retries.
    pub timeout_at: i64,
}

/// Env vars exposed to every script, regardless of backend.
fn exec_env(req: &ExecRequest) -> [(&'static str, String); 3] {
    [
        ("RESONATE_PROMISE_ID", req.task_id.clone()),
        ("RESONATE_PROMISE_CREATED_AT", req.created_at.to_string()),
        ("RESONATE_PROMISE_TIMEOUT_AT", req.timeout_at.to_string()),
    ]
}

pub struct ExecOutcome {
    pub result: Result<ExitStatus, String>,
}

pub struct ExitStatus {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
    /// True if the process was killed by a signal (locally) or by the
    /// container/sandbox runtime (docker exit 137/143, tensorlake "signaled").
    /// Killed runs are infrastructure failures, not workflow failures: the
    /// orchestrator drops the task so the lease expires and the message is
    /// re-dispatched to a fresh worker.
    pub killed: bool,
}

// ─── Address parsing ──────────────────────────────────────────────────────────
//
// Umbrella scheme: bash:// is always local execution; bash://<backend>/<image>
// routes to a remote backend.

enum BackendChoice {
    Local,
    Docker { image: String },
    Tensorlake { image: String },
}

fn parse_backend(address: &str) -> Result<BackendChoice, String> {
    let parsed = url::Url::parse(address).map_err(|e| format!("invalid address: {e}"))?;
    if parsed.scheme() != "bash" {
        return Err(format!(
            "expected bash:// scheme, got {}://",
            parsed.scheme()
        ));
    }
    let host = parsed.host_str().unwrap_or("");
    let path = parsed.path().trim_start_matches('/');
    match host {
        "" | "bash" => {
            if !path.is_empty() {
                return Err("local bash:// does not accept a path".into());
            }
            Ok(BackendChoice::Local)
        }
        "docker" => {
            if path.is_empty() {
                return Err("bash://docker/<image> requires an image".into());
            }
            Ok(BackendChoice::Docker {
                image: path.to_string(),
            })
        }
        "tensorlake" => {
            // Empty image → use Tensorlake's default sandbox environment.
            Ok(BackendChoice::Tensorlake {
                image: path.to_string(),
            })
        }
        other => Err(format!("unknown bash backend: {other}")),
    }
}

// ─── Transport ────────────────────────────────────────────────────────────────

pub struct BashExecTransport {
    /// This worker runs in the server's own process, so it reaches the port
    /// directly instead of over the wire. Every state change it makes goes
    /// through `process` — the same path a remote worker's HTTP calls take.
    ///
    /// Weak: the server holds the router, the router holds this worker. A
    /// strong handle back would close that ring and nothing in it would ever
    /// be dropped.
    server: Weak<dyn ResonateServer>,
    /// The lease TTL already resolved against the server-wide default.
    lease_timeout: i64,
    local: Arc<LocalBackend>,
    docker: Arc<DockerBackend>,
    /// Built by `init`, not by `configure`.
    ///
    /// `reqwest::Client::new` initializes a TLS backend and reads the system
    /// resolver configuration — filesystem I/O, and documented to panic when
    /// either fails. `configure` is sync and side-effect-free by contract, so
    /// it cannot be the place that happens; `init` can report it.
    tensorlake: OnceLock<Arc<TensorlakeBackend>>,
    /// The process-wide debug flag, taken in `init`.
    ///
    /// This is the one worker with work on a clock — the lease heartbeat — and
    /// under debug the clock is the caller's. Beating against wall time while a
    /// test drives `debug.tick` would renew a lease the test is trying to let
    /// expire, so the beat does not start.
    debug: AtomicBool,
}

impl BashExecTransport {
    pub fn new(server: Weak<dyn ResonateServer>, config: Config) -> Self {
        let lease_timeout = config.lease_timeout;
        Self {
            server,
            lease_timeout,
            local: Arc::new(LocalBackend),
            docker: Arc::new(DockerBackend),
            tensorlake: OnceLock::new(),
            debug: AtomicBool::new(false),
        }
    }

    pub async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Only `execute` gives this worker something to run; an `unblock` is a
        // notification for a waiting worker and has no local counterpart.
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };
        let task_id = task.id.clone();
        let task_version = task.version;

        let (backend, target): (Arc<dyn ExecBackend>, Option<String>) = match parse_backend(address)
        {
            Ok(BackendChoice::Local) => (self.local.clone(), None),
            Ok(BackendChoice::Docker { image }) => (self.docker.clone(), Some(image)),
            Ok(BackendChoice::Tensorlake { image }) => {
                // Set by `init`; absent only if something called this without
                // starting the worker.
                let Some(tensorlake) = self.tensorlake.get() else {
                    return Err(Unavailable::new(
                        "bash-exec: the tensorlake backend is not started",
                    ));
                };
                (tensorlake.clone() as Arc<dyn ExecBackend>, Some(image))
            }
            Err(e) => {
                return Err(Unavailable::new(format!(
                    "bash-exec: cannot parse address {address}: {e}"
                )))
            }
        };

        // Upgrade or abandon: no server means no work worth doing.
        let Some(server) = self.server.upgrade() else {
            return Err(Unavailable::new("bash-exec: server is gone"));
        };
        let lease_timeout = self.lease_timeout;
        let debug = self.debug.load(Ordering::SeqCst);
        tokio::spawn(async move {
            run_task(
                server,
                lease_timeout,
                debug,
                task_id,
                task_version,
                backend,
                target,
            )
            .await;
        });
        Ok(())
    }
}

#[async_trait]
impl ResonateWorker for BashExecTransport {
    /// Remember the debug flag, for the lease heartbeat in `run_task`.
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        self.debug.store(debug, Ordering::SeqCst);
        let tensorlake = TensorlakeBackend::from_env()
            .map_err(|e| Unavailable::new(format!("bash worker: {e}")))?;
        let _ = self.tensorlake.set(Arc::new(tensorlake));
        Ok(())
    }

    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        BashExecTransport::send(self, address, msg).await
    }
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

/// Issue one protocol request at the server this worker is attached to.
///
/// In-process, so no HTTP hop and no auth — but the same `process` entry point,
/// and therefore the same validation, ghost-timeout handling and response
/// shaping that a remote worker's request would go through.
async fn request(
    server: &dyn ResonateServer,
    kind: &str,
    data: serde_json::Value,
) -> Result<ResponseEnvelope, Unavailable> {
    server
        .process(&RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: format!("bash-exec-{}", fastrand::u64(..)),
                version: PROTOCOL_VERSION.to_string(),
                auth: None,
                debug_time: None,
            },
            data,
        })
        .await
}

/// Settle the task's promise via `task.fulfill`.
///
/// The promise id is the task id in this flow — `op_task_fulfill` settles
/// `data.id`, and `action.data.id` is what the ghost timeout runs against.
async fn settle_task(
    server: &dyn ResonateServer,
    task_id: &str,
    version: i64,
    state: &str,
    value: &str,
) {
    let resp = request(
        server,
        "task.fulfill",
        json!({
            "id": task_id,
            "version": version,
            "action": {
                "kind": "promise.settle",
                "head": {},
                "data": { "id": task_id, "state": state, "value": { "data": value } }
            }
        }),
    )
    .await;

    match resp {
        Ok(r) if (200..300).contains(&r.head.status) => {}
        Ok(r) => tracing::warn!(
            task_id,
            status = r.head.status,
            state,
            "bash-exec: task fulfill rejected"
        ),
        Err(e) => tracing::error!(task_id, error = %e, "bash-exec: task fulfill failed"),
    }
}

async fn run_task(
    server: Arc<dyn ResonateServer>,
    lease_timeout: i64,
    debug: bool,
    task_id: String,
    task_version: i64,
    backend: Arc<dyn ExecBackend>,
    target: Option<String>,
) {
    let pid = format!("bash-exec-{}", fastrand::u64(..));

    // 1. Acquire — racing is normal (409); anything else is transient, so drop
    //    the task and let the lease expire rather than settling the promise.
    let resp = match request(
        server.as_ref(),
        "task.acquire",
        json!({
            "id": task_id,
            "version": task_version,
            "pid": pid,
            "ttl": lease_timeout
        }),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "bash-exec: task acquire failed");
            return;
        }
    };

    if resp.head.status == 409 {
        tracing::debug!(task_id, "bash-exec: task not acquired");
        return;
    }
    if resp.head.status != 200 {
        tracing::warn!(
            task_id,
            status = resp.head.status,
            "bash-exec: task acquire rejected"
        );
        return;
    }

    let acquired: TaskAcquireResponseData = match serde_json::from_value(resp.data) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(task_id, error = %e, "bash-exec: malformed acquire response");
            return;
        }
    };
    let acquired_version = acquired.task.version;
    let promise = acquired.promise;

    // 2. Decode script — param.data is base64-encoded; the decoded value IS the script.
    let script = match decode_param(promise.param.data.as_deref()) {
        Some(s) => s,
        None => {
            settle_task(
                server.as_ref(),
                &task_id,
                acquired_version,
                "rejected",
                "param.data is missing or not valid base64/utf-8",
            )
            .await;
            return;
        }
    };

    // 3. Heartbeat — refreshes the lease while the backend runs. Not under the
    // debug flag: it runs on wall time, and a test that is trying to let this
    // lease expire would find it renewed underneath.
    let heartbeat = if debug {
        None
    } else {
        Some({
            let server = Arc::clone(&server);
            let task_id = task_id.clone();
            let pid = pid.clone();
            let version = acquired_version;
            tokio::spawn(async move {
                let beat_ms = (lease_timeout / 3).max(1000) as u64;
                let mut interval = tokio::time::interval(Duration::from_millis(beat_ms));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    let _ = request(
                        server.as_ref(),
                        "task.heartbeat",
                        json!({
                            "pid": pid,
                            "tasks": [{ "id": task_id, "version": version }]
                        }),
                    )
                    .await;
                }
            })
        })
    };

    // 4. Run.
    tracing::debug!(task_id, backend = backend.name(), "bash-exec: running");
    let outcome = backend
        .run(ExecRequest {
            task_id: task_id.clone(),
            script,
            target,
            created_at: promise.created_at,
            timeout_at: promise.timeout_at,
        })
        .await;
    if let Some(h) = heartbeat {
        h.abort();
    }

    // 5. Fulfill, reject, or drop-for-reschedule.
    match outcome.result {
        Err(msg) => {
            settle_task(
                server.as_ref(),
                &task_id,
                acquired_version,
                "rejected",
                &msg,
            )
            .await
        }
        Ok(status) if status.killed => {
            // Process was killed (signal locally; SIGKILL/SIGTERM in container;
            // "signaled" in sandbox). Treat as infrastructure failure: drop the
            // task without settling so the lease expires and the message is
            // re-dispatched to a fresh worker.
            tracing::warn!(
                task_id,
                code = status.code,
                "bash-exec: process killed, dropping task for reschedule"
            );
        }
        Ok(status) if status.code == 0 => {
            let value = status.stdout.trim().to_string();
            settle_task(
                server.as_ref(),
                &task_id,
                acquired_version,
                "resolved",
                &value,
            )
            .await;
        }
        Ok(status) => {
            let stderr = status.stderr.trim();
            let reason = if stderr.is_empty() {
                format!("exit code {}", status.code)
            } else {
                stderr.to_string()
            };
            settle_task(
                server.as_ref(),
                &task_id,
                acquired_version,
                "rejected",
                &reason,
            )
            .await;
        }
    }
}

// ─── Backend: Local ───────────────────────────────────────────────────────────

pub struct LocalBackend;

#[async_trait]
impl ExecBackend for LocalBackend {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn run(&self, req: ExecRequest) -> ExecOutcome {
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg(&req.script);
        for (k, v) in exec_env(&req) {
            cmd.env(k, v);
        }
        let child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();
        let child = match child {
            Ok(c) => c,
            Err(e) => return err(format!("failed to spawn bash: {e}")),
        };
        match child.wait_with_output().await {
            Err(e) => err(format!("bash wait failed: {e}")),
            Ok(out) => {
                use std::os::unix::process::ExitStatusExt;
                let killed = out.status.signal().is_some();
                let code = out
                    .status
                    .code()
                    .or_else(|| out.status.signal().map(|s| 128 + s))
                    .unwrap_or(-1);
                ok(
                    code,
                    String::from_utf8_lossy(&out.stdout).to_string(),
                    String::from_utf8_lossy(&out.stderr).to_string(),
                    killed,
                )
            }
        }
    }
}

// ─── Backend: Docker ──────────────────────────────────────────────────────────

pub struct DockerBackend;

#[async_trait]
impl ExecBackend for DockerBackend {
    fn name(&self) -> &'static str {
        "docker"
    }

    async fn run(&self, req: ExecRequest) -> ExecOutcome {
        let image = match req.target.as_deref() {
            Some(i) if !i.is_empty() => i,
            _ => return err("docker backend requires an image (bash://docker/<image>)".into()),
        };
        let mut cmd = Command::new("docker");
        cmd.args(["run", "--rm"]);
        for (k, v) in exec_env(&req) {
            cmd.arg("-e").arg(format!("{k}={v}"));
        }
        cmd.args(["--entrypoint", "bash", image, "-c", &req.script]);
        let child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();
        let child = match child {
            Ok(c) => c,
            Err(e) => return err(format!("failed to spawn docker: {e}")),
        };
        match child.wait_with_output().await {
            Err(e) => err(format!("docker wait failed: {e}")),
            Ok(out) => {
                use std::os::unix::process::ExitStatusExt;
                let code = out.status.code().unwrap_or(-1);
                // `docker run` exits normally even when the container was killed —
                // it propagates the container's exit code as 128+signal. Treat the
                // common signal codes (137=SIGKILL, 143=SIGTERM) as kills, plus the
                // case where the docker CLI itself was signal-killed.
                let killed = out.status.signal().is_some() || matches!(code, 137 | 143);
                ok(
                    code,
                    String::from_utf8_lossy(&out.stdout).to_string(),
                    String::from_utf8_lossy(&out.stderr).to_string(),
                    killed,
                )
            }
        }
    }
}

// ─── Backend: Tensorlake ──────────────────────────────────────────────────────
//
// Per-call lifecycle: create sandbox → wait for running → start process →
// poll until exited → fetch stdout/stderr → delete sandbox.
//
// API key from TENSORLAKE_API_KEY env var. If absent at call time, the backend
// returns a clear error instead of attempting any HTTP.

pub struct TensorlakeBackend {
    api_key: Option<String>,
    client: reqwest::Client,
}

impl TensorlakeBackend {
    /// Fallible, and called from `init` rather than `configure`: building a
    /// client loads root certificates and the system resolver config, both of
    /// which can fail on a host that is missing them.
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            api_key: std::env::var("TENSORLAKE_API_KEY").ok(),
            client: reqwest::Client::builder()
                .build()
                .map_err(|e| format!("could not build an HTTP client: {e}"))?,
        })
    }
}

const TENSORLAKE_API: &str = "https://api.tensorlake.ai";
const SANDBOX_READY_TIMEOUT_MS: u64 = 120_000;
const SANDBOX_POLL_INTERVAL_MS: u64 = 1_000;
const PROCESS_POLL_INTERVAL_MS: u64 = 500;

#[async_trait]
impl ExecBackend for TensorlakeBackend {
    fn name(&self) -> &'static str {
        "tensorlake"
    }

    async fn run(&self, req: ExecRequest) -> ExecOutcome {
        let api_key = match &self.api_key {
            Some(k) => k.clone(),
            None => return err("TENSORLAKE_API_KEY env var not set".into()),
        };
        // Empty image → omit from request, server picks the default environment.
        let image = req.target.as_deref().filter(|s| !s.is_empty());

        // 1. Create sandbox.
        let mut body = json!({ "timeout_secs": 600 });
        if let Some(img) = image {
            body["image"] = json!(img);
        }
        let create: serde_json::Value = match self
            .client
            .post(format!("{TENSORLAKE_API}/sandboxes"))
            .bearer_auth(&api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => return err(format!("tensorlake create: bad json: {e}")),
            },
            Err(e) => return err(format!("tensorlake create: {e}")),
        };
        let sandbox_id = match create.get("sandbox_id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return err(format!("tensorlake create: no sandbox_id in {create}")),
        };

        // From here on, always best-effort delete the sandbox before returning.
        let outcome = self.run_in_sandbox(&api_key, &sandbox_id, &req).await;
        let _ = self
            .client
            .delete(format!("{TENSORLAKE_API}/sandboxes/{sandbox_id}"))
            .bearer_auth(&api_key)
            .send()
            .await;
        outcome
    }
}

impl TensorlakeBackend {
    async fn run_in_sandbox(
        &self,
        api_key: &str,
        sandbox_id: &str,
        req: &ExecRequest,
    ) -> ExecOutcome {
        // 2. Wait for sandbox to become running.
        let mut waited = 0u64;
        loop {
            let st = match self
                .client
                .get(format!("{TENSORLAKE_API}/sandboxes/{sandbox_id}"))
                .bearer_auth(api_key)
                .send()
                .await
            {
                Ok(r) => r
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|v| {
                        v.get("status")
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_default(),
                Err(_) => String::new(),
            };
            match st.as_str() {
                "running" => break,
                "terminated" | "suspended" => {
                    return err(format!("tensorlake: sandbox entered {st} before running"));
                }
                _ => {}
            }
            if waited >= SANDBOX_READY_TIMEOUT_MS {
                return err("tensorlake: sandbox did not become running in time".into());
            }
            tokio::time::sleep(Duration::from_millis(SANDBOX_POLL_INTERVAL_MS)).await;
            waited += SANDBOX_POLL_INTERVAL_MS;
        }

        let host = format!("https://{sandbox_id}.sandbox.tensorlake.ai");

        // 3. Start the process.
        let env_obj: serde_json::Map<String, serde_json::Value> = exec_env(req)
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v)))
            .collect();
        let start: serde_json::Value = match self
            .client
            .post(format!("{host}/api/v1/processes"))
            .bearer_auth(api_key)
            .json(&json!({
                "command": "bash",
                "args": ["-c", &req.script],
                "env": env_obj,
                "stdout_mode": "capture",
                "stderr_mode": "capture",
            }))
            .send()
            .await
        {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => return err(format!("tensorlake start: bad json: {e}")),
            },
            Err(e) => return err(format!("tensorlake start: {e}")),
        };
        let pid = match start.get("pid").and_then(|v| v.as_i64()) {
            Some(p) => p,
            None => return err(format!("tensorlake start: no pid in {start}")),
        };

        // 4. Poll until exited / signaled.
        let final_status = loop {
            let s: serde_json::Value = match self
                .client
                .get(format!("{host}/api/v1/processes/{pid}"))
                .bearer_auth(api_key)
                .send()
                .await
            {
                Ok(r) => r.json().await.unwrap_or_else(|_| json!({})),
                Err(_) => json!({}),
            };
            match s
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("running")
            {
                "exited" | "signaled" => break s,
                _ => tokio::time::sleep(Duration::from_millis(PROCESS_POLL_INTERVAL_MS)).await,
            }
        };

        let killed = final_status
            .get("status")
            .and_then(|v| v.as_str())
            .map(|s| s == "signaled")
            .unwrap_or(false);
        let code = final_status
            .get("exit_code")
            .and_then(|v| v.as_i64())
            .map(|c| c as i32)
            .unwrap_or(-1);

        // 5. Fetch combined output. The `?stream=stdout/stderr` filter is silently
        // ignored by the API as of 2026-05; one fetch returns both streams
        // interleaved as a `lines` array. We use it for both fields so the
        // success path gets the script output and the failure path gets a
        // useful rejection reason.
        let combined = fetch_lines(
            &self.client,
            api_key,
            &format!("{host}/api/v1/processes/{pid}/output"),
        )
        .await;

        ok(code, combined.clone(), combined, killed)
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn ok(code: i32, stdout: String, stderr: String, killed: bool) -> ExecOutcome {
    ExecOutcome {
        result: Ok(ExitStatus {
            code,
            stdout,
            stderr,
            killed,
        }),
    }
}

fn err(msg: String) -> ExecOutcome {
    ExecOutcome { result: Err(msg) }
}

/// Tensorlake's `/output` endpoint returns `{"lines":[...], "line_count":N}`.
/// Fetch and join with `\n`. Returns empty string on any error.
async fn fetch_lines(client: &reqwest::Client, api_key: &str, url: &str) -> String {
    let resp = match client.get(url).bearer_auth(api_key).send().await {
        Ok(r) => r,
        Err(_) => return String::new(),
    };
    let body: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return String::new(),
    };
    body.get("lines")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn decode_param(data: Option<&str>) -> Option<String> {
    use base64::Engine;
    let d = data.filter(|s| !s.is_empty())?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(d).ok()?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_local_empty() {
        assert!(matches!(parse_backend("bash://"), Ok(BackendChoice::Local)));
    }

    #[test]
    fn parse_local_explicit() {
        assert!(matches!(
            parse_backend("bash://bash"),
            Ok(BackendChoice::Local)
        ));
    }

    #[test]
    fn parse_local_rejects_path() {
        assert!(parse_backend("bash:///some/path").is_err());
    }

    #[test]
    fn parse_docker() {
        match parse_backend("bash://docker/alpine").unwrap() {
            BackendChoice::Docker { image } => assert_eq!(image, "alpine"),
            _ => panic!("expected Docker"),
        }
    }

    #[test]
    fn parse_docker_with_tag() {
        match parse_backend("bash://docker/library/ubuntu:latest").unwrap() {
            BackendChoice::Docker { image } => assert_eq!(image, "library/ubuntu:latest"),
            _ => panic!("expected Docker"),
        }
    }

    #[test]
    fn parse_docker_requires_image() {
        assert!(parse_backend("bash://docker").is_err());
        assert!(parse_backend("bash://docker/").is_err());
    }

    #[test]
    fn parse_tensorlake() {
        match parse_backend("bash://tensorlake/python-3.11").unwrap() {
            BackendChoice::Tensorlake { image } => assert_eq!(image, "python-3.11"),
            _ => panic!("expected Tensorlake"),
        }
    }

    #[test]
    fn parse_tensorlake_default_image() {
        match parse_backend("bash://tensorlake/").unwrap() {
            BackendChoice::Tensorlake { image } => assert_eq!(image, ""),
            _ => panic!("expected Tensorlake"),
        }
    }

    #[test]
    fn parse_unknown_backend() {
        assert!(parse_backend("bash://nope/foo").is_err());
    }

    #[test]
    fn parse_wrong_scheme() {
        assert!(parse_backend("http://x").is_err());
    }
}
