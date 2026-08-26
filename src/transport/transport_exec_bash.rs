use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::process::Command;

use crate::core::types::{
    Message, RequestEnvelope, RequestHead, ResponseEnvelope, TaskAcquireResponseData,
    PROTOCOL_VERSION,
};
use crate::core::{ResonateServer, ResonateWorker, Unavailable};

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

#[derive(Debug)]
pub struct ExecOutcome {
    pub result: Result<ExitStatus, String>,
}

#[derive(Debug)]
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

/// The three backends a [`BashExecTransport`] can route to.
///
/// A struct rather than three hardcoded fields so a test can substitute a
/// scripted [`ExecBackend`] for any of them. The `ExecBackend` seam existed
/// before this, but nothing could reach it: `new` built `LocalBackend` and
/// friends internally, so every test had to shell out to real `bash`.
pub struct ExecBackends {
    pub local: Arc<dyn ExecBackend>,
    pub docker: Arc<dyn ExecBackend>,
    pub tensorlake: Arc<dyn ExecBackend>,
}

impl ExecBackends {
    /// The real backends: local bash, docker, and Tensorlake from the
    /// environment.
    pub fn production() -> Self {
        Self {
            local: Arc::new(LocalBackend),
            docker: Arc::new(DockerBackend),
            tensorlake: Arc::new(TensorlakeBackend::from_env()),
        }
    }

    /// Every choice routed to the same backend. For tests that do not care
    /// which one the address selected.
    pub fn all(backend: Arc<dyn ExecBackend>) -> Self {
        Self {
            local: Arc::clone(&backend),
            docker: Arc::clone(&backend),
            tensorlake: backend,
        }
    }
}

impl Default for ExecBackends {
    fn default() -> Self {
        Self::production()
    }
}

pub struct BashExecTransport {
    /// This worker runs in the server's own process, so it holds the port
    /// directly instead of reaching it over the wire. Every state change it
    /// makes goes through `process` — the same path a remote worker's HTTP
    /// calls take.
    server: Arc<dyn ResonateServer>,
    /// Lease TTL for tasks this worker acquires. Configuration of the worker,
    /// not of the server it talks to.
    lease_timeout: i64,
    backends: ExecBackends,
    /// Handles for tasks spawned by `send`, so a caller can wait for a run to
    /// finish instead of polling the server until the promise settles.
    running: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl BashExecTransport {
    /// A transport over the real backends.
    pub fn new(server: Arc<dyn ResonateServer>, lease_timeout: i64) -> Self {
        Self::with_backends(server, lease_timeout, ExecBackends::production())
    }

    /// A transport over the supplied backends.
    pub fn with_backends(
        server: Arc<dyn ResonateServer>,
        lease_timeout: i64,
        backends: ExecBackends,
    ) -> Self {
        Self {
            server,
            lease_timeout,
            backends,
            running: Mutex::new(Vec::new()),
        }
    }

    /// Wait for every run this transport has started.
    ///
    /// `send` is fire-and-forget by contract — it means "accepted for
    /// delivery", not "executed" — so the handles are retained rather than
    /// dropped. Without this a test can only sleep-and-poll until the promise
    /// settles, which is both slow and flaky.
    pub async fn join_all(&self) {
        let handles: Vec<_> = self
            .running
            .lock()
            .expect("not poisoned")
            .drain(..)
            .collect();
        for handle in handles {
            let _ = handle.await;
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
            Ok(BackendChoice::Local) => (Arc::clone(&self.backends.local), None),
            Ok(BackendChoice::Docker { image }) => {
                (Arc::clone(&self.backends.docker), Some(image))
            }
            Ok(BackendChoice::Tensorlake { image }) => {
                (Arc::clone(&self.backends.tensorlake), Some(image))
            }
            Err(e) => {
                return Err(Unavailable::new(format!(
                    "bash-exec: cannot parse address {address}: {e}"
                )))
            }
        };

        let server = Arc::clone(&self.server);
        let lease_timeout = self.lease_timeout;
        let handle = tokio::spawn(async move {
            run_task(
                server,
                lease_timeout,
                task_id,
                task_version,
                backend,
                target,
            )
            .await;
        });
        self.running.lock().expect("not poisoned").push(handle);
        Ok(())
    }
}

#[async_trait]
impl ResonateWorker for BashExecTransport {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
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

    // 3. Heartbeat — refreshes the lease while the backend runs.
    let heartbeat = {
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
    heartbeat.abort();

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
    /// API root. A field rather than a `const` so the backend can be pointed at
    /// a local stub; defaults to [`DEFAULT_TENSORLAKE_API`].
    base_url: String,
    client: reqwest::Client,
}

impl TensorlakeBackend {
    /// The production backend: key from `TENSORLAKE_API_KEY`, real API root.
    ///
    /// Reading the environment is confined to this constructor — everything
    /// below takes the resolved values as data.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("TENSORLAKE_API_KEY").ok(),
            DEFAULT_TENSORLAKE_API.to_string(),
        )
    }

    /// A backend with an explicit key and API root.
    pub fn new(api_key: Option<String>, base_url: String) -> Self {
        Self {
            api_key,
            base_url,
            client: reqwest::Client::new(),
        }
    }
}

/// Default Tensorlake API root.
pub const DEFAULT_TENSORLAKE_API: &str = "https://api.tensorlake.ai";
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
            .post(format!("{}/sandboxes", self.base_url))
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
            .delete(format!("{}/sandboxes/{sandbox_id}", self.base_url))
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
                .get(format!("{}/sandboxes/{sandbox_id}", self.base_url))
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

// ─── Test double ──────────────────────────────────────────────────────────────

/// An [`ExecBackend`] that returns a scripted outcome and records what it was
/// asked to run.
///
/// The point of the `ExecBackend` seam. With this, the acquire → run → settle
/// orchestration is testable against every outcome the contract allows —
/// including the killed-by-signal path, which is not reachable by running real
/// `bash` — and without spawning a process or sleeping.
pub struct ScriptedBackend {
    name: &'static str,
    outcome: Mutex<Option<ExecOutcome>>,
    /// Every `(script, target, created_at, timeout_at)` this backend was asked to run.
    runs: Mutex<Vec<(String, Option<String>, i64, i64)>>,
    /// Env vars the last run would have been given.
    env: Mutex<Vec<(String, String)>>,
}

impl ScriptedBackend {
    /// A backend that exits `code` with the given output.
    pub fn exiting(code: i32, stdout: &str, stderr: &str) -> Self {
        Self::returning(ExecOutcome {
            result: Ok(ExitStatus {
                code,
                stdout: stdout.to_string(),
                stderr: stderr.to_string(),
                killed: false,
            }),
        })
    }

    /// A backend whose run was killed by the runtime.
    pub fn killed() -> Self {
        Self::returning(ExecOutcome {
            result: Ok(ExitStatus {
                code: 137,
                stdout: String::new(),
                stderr: String::new(),
                killed: true,
            }),
        })
    }

    /// A backend that could not run the script at all.
    pub fn erroring(message: &str) -> Self {
        Self::returning(ExecOutcome {
            result: Err(message.to_string()),
        })
    }

    pub fn returning(outcome: ExecOutcome) -> Self {
        Self {
            name: "scripted",
            outcome: Mutex::new(Some(outcome)),
            runs: Mutex::new(Vec::new()),
            env: Mutex::new(Vec::new()),
        }
    }

    /// `(script, target, created_at, timeout_at)` per run.
    pub fn runs(&self) -> Vec<(String, Option<String>, i64, i64)> {
        self.runs.lock().expect("not poisoned").clone()
    }

    /// Env vars the backend was handed on its most recent run.
    pub fn env(&self) -> Vec<(String, String)> {
        self.env.lock().expect("not poisoned").clone()
    }
}

#[async_trait]
impl ExecBackend for ScriptedBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn run(&self, req: ExecRequest) -> ExecOutcome {
        self.runs.lock().expect("not poisoned").push((
            req.script.clone(),
            req.target.clone(),
            req.created_at,
            req.timeout_at,
        ));
        *self.env.lock().expect("not poisoned") = exec_env(&req)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        self.outcome
            .lock()
            .expect("not poisoned")
            .take()
            .unwrap_or_else(|| ExecOutcome {
                result: Err("ScriptedBackend was run more than once".into()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use base64::Engine;
    use std::collections::HashMap;

    use crate::core::types::PromiseState;
    use crate::core::ResonateRouter;
    use crate::metrics::Metrics;
    use crate::processing::processing_messages::process_batch;
    use crate::server::Server;
    use crate::testing::{self, ok, T0};
    use crate::transport::TransportDispatcher;

    const TIMEOUT_AT: i64 = T0 + 1_000_000;

    // ---- end-to-end: the worker drives the task through the protocol ----
    //
    // These are the only coverage of the acquire/run/settle path: the
    // differential test never exercises a worker, so nothing else checks that
    // `run_task`'s envelopes are accepted by the server.

    /// Create a promise whose `resonate:target` is a bash address and whose
    /// param carries `script`, then release the task so a message is queued.
    async fn queue_bash_task(server: &Arc<Server>, id: &str, script: &str, address: &str) {
        let encoded = base64::engine::general_purpose::STANDARD.encode(script);
        ok(
            server,
            "task.create",
            json!({
                "pid": "test-worker",
                "ttl": 60_000,
                "action": {
                    "kind": "promise.create",
                    "head": {},
                    "data": {
                        "id": id,
                        "timeoutAt": TIMEOUT_AT,
                        "param": { "data": encoded },
                        "tags": { "resonate:target": address }
                    }
                }
            }),
            T0,
        )
        .await;

        ok(server, "task.release", json!({ "id": id, "version": 1 }), T0).await;
    }

    async fn promise_state(server: &Arc<Server>, id: &str) -> (PromiseState, Option<String>) {
        let data = ok(server, "promise.get", json!({ "id": id }), T0).await;
        let promise = &data["promise"];
        let state = serde_json::from_value(promise["state"].clone()).expect("known state");
        let value = promise["value"]["data"].as_str().map(|s| s.to_string());
        (state, value)
    }

    async fn task_state(server: &Arc<Server>, id: &str) -> String {
        let data = ok(server, "task.get", json!({ "id": id }), T0).await;
        data["task"]["state"].as_str().expect("state").to_string()
    }

    /// Queue a task at `address`, deliver it through a transport backed by
    /// `backend`, and wait for the run to finish.
    ///
    /// Deterministic: `join_all` waits on the spawned run rather than polling
    /// the server until the promise settles.
    async fn run_one_task(
        server: &Arc<Server>,
        id: &str,
        script: &str,
        address: &str,
        backend: Arc<ScriptedBackend>,
    ) -> Arc<BashExecTransport> {
        queue_bash_task(server, id, script, address).await;

        let transport = Arc::new(BashExecTransport::with_backends(
            server.clone() as Arc<dyn ResonateServer>,
            60_000,
            ExecBackends::all(backend as Arc<dyn ExecBackend>),
        ));

        let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
        workers.insert("bash".to_string(), transport.clone());
        let router = TransportDispatcher::new(workers);

        process_batch(
            &server.storage,
            &router as &dyn ResonateRouter,
            100,
            "http://localhost:8001",
            &Metrics::isolated(),
        )
        .await;

        transport.join_all().await;
        transport
    }

    // ---- outcome mapping ----

    #[tokio::test(flavor = "multi_thread")]
    async fn a_successful_run_resolves_the_promise_with_stdout() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(0, "hello\n", ""));
        run_one_task(&server, "bash-ok", "echo hello", "bash://", backend).await;

        let (state, value) = promise_state(&server, "bash-ok").await;
        assert_eq!(state, PromiseState::Resolved);
        assert_eq!(value.as_deref(), Some("hello"), "stdout is trimmed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_run_rejects_the_promise_with_stderr() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(3, "", "boom\n"));
        run_one_task(&server, "bash-fail", "exit 3", "bash://", backend).await;

        let (state, value) = promise_state(&server, "bash-fail").await;
        assert_eq!(state, PromiseState::Rejected);
        assert_eq!(value.as_deref(), Some("boom"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_run_without_stderr_rejects_with_the_exit_code() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(7, "", "   \n  "));
        run_one_task(&server, "bash-code", "exit 7", "bash://", backend).await;

        let (state, value) = promise_state(&server, "bash-code").await;
        assert_eq!(state, PromiseState::Rejected);
        assert_eq!(
            value.as_deref(),
            Some("exit code 7"),
            "whitespace-only stderr is treated as empty"
        );
    }

    /// A killed run is an infrastructure failure, not a workflow failure.
    ///
    /// The documented contract on `ExitStatus::killed`: drop the task so the
    /// lease expires and the message is re-dispatched to a fresh worker, rather
    /// than settling the promise. Unreachable when the test runs real `bash`,
    /// which is why it had no coverage before.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_killed_run_leaves_the_promise_pending_for_another_worker() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::killed());
        run_one_task(&server, "bash-killed", "sleep 999", "bash://", backend).await;

        let (state, _) = promise_state(&server, "bash-killed").await;
        assert_eq!(
            state,
            PromiseState::Pending,
            "a kill must not be reported as a workflow rejection"
        );
    }

    /// A backend that could not run the script rejects the promise.
    ///
    /// Note the asymmetry with the killed case above: a run that was killed is
    /// retried, but a run that never started (`failed to spawn bash`, a
    /// missing Tensorlake key, a docker daemon that is down) is reported to the
    /// caller as a workflow rejection. Both are arguably infrastructure
    /// failures. This pins today's behaviour so that a deliberate change to it
    /// shows up here rather than silently.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_backend_that_cannot_run_at_all_rejects_the_promise() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::erroring("failed to spawn bash: ENOENT"));
        run_one_task(&server, "bash-spawn-fail", "true", "bash://", backend).await;

        let (state, value) = promise_state(&server, "bash-spawn-fail").await;
        assert_eq!(state, PromiseState::Rejected);
        assert_eq!(value.as_deref(), Some("failed to spawn bash: ENOENT"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_promise_whose_param_is_not_base64_is_rejected_immediately() {
        let server = testing::server();
        // Bypass `queue_bash_task` so the param is raw, not base64.
        ok(
            &server,
            "task.create",
            json!({
                "pid": "test-worker",
                "ttl": 60_000,
                "action": {
                    "kind": "promise.create",
                    "head": {},
                    "data": {
                        "id": "bash-badparam",
                        "timeoutAt": TIMEOUT_AT,
                        "param": { "data": "!!!not base64!!!" },
                        "tags": { "resonate:target": "bash://" }
                    }
                }
            }),
            T0,
        )
        .await;
        ok(
            &server,
            "task.release",
            json!({ "id": "bash-badparam", "version": 1 }),
            T0,
        )
        .await;

        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        let transport = Arc::new(BashExecTransport::with_backends(
            server.clone() as Arc<dyn ResonateServer>,
            60_000,
            ExecBackends::all(backend.clone() as Arc<dyn ExecBackend>),
        ));
        let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
        workers.insert("bash".to_string(), transport.clone());
        process_batch(
            &server.storage,
            &TransportDispatcher::new(workers) as &dyn ResonateRouter,
            100,
            "http://localhost:8001",
            &Metrics::isolated(),
        )
        .await;
        transport.join_all().await;

        let (state, value) = promise_state(&server, "bash-badparam").await;
        assert_eq!(state, PromiseState::Rejected);
        assert!(
            value.as_deref().unwrap_or_default().contains("base64"),
            "expected a decode error, got {value:?}"
        );
        assert!(
            backend.runs().is_empty(),
            "the backend must never be reached with an undecodable script"
        );
    }

    // ---- what the backend is handed ----

    #[tokio::test(flavor = "multi_thread")]
    async fn the_decoded_script_reaches_the_backend() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        run_one_task(
            &server,
            "bash-script",
            "echo 'quoted $VAR'\nsecond line",
            "bash://",
            backend.clone(),
        )
        .await;

        let runs = backend.runs();
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].0, "echo 'quoted $VAR'\nsecond line",
            "the script arrives decoded and byte-identical"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_promise_deadline_reaches_the_backend_as_env() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        run_one_task(&server, "bash-env", "true", "bash://", backend.clone()).await;

        let env: HashMap<String, String> = backend.env().into_iter().collect();
        assert_eq!(env.get("RESONATE_PROMISE_ID").map(String::as_str), Some("bash-env"));
        assert_eq!(
            env.get("RESONATE_PROMISE_TIMEOUT_AT").map(String::as_str),
            Some(TIMEOUT_AT.to_string().as_str()),
            "scripts loop until the deadline, so it must be the promise's own"
        );
        assert_eq!(
            env.get("RESONATE_PROMISE_CREATED_AT").map(String::as_str),
            Some(T0.to_string().as_str()),
            "created_at is stable across retries"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_image_from_the_address_reaches_the_backend_as_the_target() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        run_one_task(
            &server,
            "bash-image",
            "true",
            "bash://docker/library/ubuntu:latest",
            backend.clone(),
        )
        .await;

        let runs = backend.runs();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].1.as_deref(), Some("library/ubuntu:latest"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_local_backend_is_given_no_target() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        run_one_task(&server, "bash-local", "true", "bash://", backend.clone()).await;

        assert_eq!(backend.runs()[0].1, None);
    }

    // ---- routing ----

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unparseable_address_is_reported_not_run() {
        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        let transport = BashExecTransport::with_backends(
            Arc::new(testing::NoopServer) as Arc<dyn ResonateServer>,
            60_000,
            ExecBackends::all(backend.clone() as Arc<dyn ExecBackend>),
        );

        let msg = Message::Execute(crate::core::types::ExecuteMsg {
            kind: "execute".to_string(),
            head: crate::core::types::MessageHead {
                server_url: "http://localhost:8001".to_string(),
            },
            data: crate::core::types::ExecuteMsgData {
                task: crate::core::types::ExecuteMsgTask {
                    id: "t1".to_string(),
                    version: 1,
                },
            },
        });

        let err = transport
            .send("bash://nope/foo", &msg)
            .await
            .expect_err("unknown backend");
        assert!(err.to_string().contains("cannot parse address"), "{err}");
        assert!(backend.runs().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unblock_message_is_accepted_and_runs_nothing() {
        let backend = Arc::new(ScriptedBackend::exiting(0, "", ""));
        let transport = BashExecTransport::with_backends(
            Arc::new(testing::NoopServer) as Arc<dyn ResonateServer>,
            60_000,
            ExecBackends::all(backend.clone() as Arc<dyn ExecBackend>),
        );

        let msg = Message::Unblock(crate::core::types::UnblockMsg {
            kind: "unblock".to_string(),
            head: crate::core::types::UnblockMsgHead {},
            data: crate::core::types::UnblockMsgData {
                promise: crate::core::types::PromiseRecord {
                    id: "p1".to_string(),
                    state: PromiseState::Resolved,
                    param: Default::default(),
                    value: Default::default(),
                    tags: HashMap::new(),
                    timeout_at: 0,
                    created_at: 0,
                    settled_at: None,
                },
            },
        });

        transport
            .send("bash://", &msg)
            .await
            .expect("unblock is accepted");
        transport.join_all().await;
        assert!(
            backend.runs().is_empty(),
            "an unblock has no local counterpart to run"
        );
    }

    // ---- lifecycle ----

    /// The heartbeat loop hand-builds a `task.heartbeat` envelope and ignores
    /// the result, so a malformed one would fail silently: the lease would
    /// expire mid-script and the task be redispatched while still running.
    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_envelope_is_accepted_by_the_server() {
        let server = testing::server();
        queue_bash_task(&server, "bash-beat", "echo hi", "bash://").await;

        // Acquire exactly as run_task does, so the pid/version match.
        let pid = "bash-exec-test";
        let data = ok(
            &server,
            "task.acquire",
            json!({ "id": "bash-beat", "version": 1, "pid": pid, "ttl": 60_000 }),
            T0,
        )
        .await;
        let acquired: TaskAcquireResponseData = serde_json::from_value(data).expect("acquire data");

        ok(
            &server,
            "task.heartbeat",
            json!({
                "pid": pid,
                "tasks": [{ "id": "bash-beat", "version": acquired.task.version }]
            }),
            T0 + 1_000,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_settled_run_leaves_the_task_fulfilled() {
        let server = testing::server();
        let backend = Arc::new(ScriptedBackend::exiting(0, "done", ""));
        run_one_task(&server, "bash-fulfil", "true", "bash://", backend).await;

        assert_eq!(task_state(&server, "bash-fulfil").await, "fulfilled");
    }

    // ---- address parsing ----

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
        match parse_backend("bash://docker/alpine").expect("valid") {
            BackendChoice::Docker { image } => assert_eq!(image, "alpine"),
            _ => panic!("expected Docker"),
        }
    }

    #[test]
    fn parse_docker_with_tag() {
        match parse_backend("bash://docker/library/ubuntu:latest").expect("valid") {
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
        match parse_backend("bash://tensorlake/python-3.11").expect("valid") {
            BackendChoice::Tensorlake { image } => assert_eq!(image, "python-3.11"),
            _ => panic!("expected Tensorlake"),
        }
    }

    #[test]
    fn parse_tensorlake_default_image() {
        match parse_backend("bash://tensorlake/").expect("valid") {
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

    // ---- tensorlake configuration ----

    #[tokio::test]
    async fn the_tensorlake_backend_without_a_key_reports_rather_than_calling_out() {
        let backend = TensorlakeBackend::new(None, "http://127.0.0.1:1".to_string());
        let outcome = backend
            .run(ExecRequest {
                task_id: "t".to_string(),
                script: "true".to_string(),
                target: None,
                created_at: T0,
                timeout_at: TIMEOUT_AT,
            })
            .await;
        let err = outcome.result.expect_err("no API key");
        assert!(err.contains("TENSORLAKE_API_KEY"), "{err}");
    }
}
