use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::process::Command;

use crate::persistence::{Storage, TaskAcquireParams, TaskFulfillParams};
use crate::server::Server;
use crate::util::system_time_ms;

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
        return Err(format!("expected bash:// scheme, got {}://", parsed.scheme()));
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
    server: Arc<Server>,
    local: Arc<LocalBackend>,
    docker: Arc<DockerBackend>,
    tensorlake: Arc<TensorlakeBackend>,
}

impl BashExecTransport {
    pub fn new(server: Arc<Server>) -> Self {
        Self {
            server,
            local: Arc::new(LocalBackend),
            docker: Arc::new(DockerBackend),
            tensorlake: Arc::new(TensorlakeBackend::from_env()),
        }
    }

    pub async fn send(&self, address: &str, payload: &serde_json::Value) {
        let task_id = match payload.pointer("/data/task/id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => {
                tracing::warn!("bash-exec: missing task.id in execute payload");
                return;
            }
        };
        let task_version = payload
            .pointer("/data/task/version")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let (backend, target): (Arc<dyn ExecBackend>, Option<String>) = match parse_backend(address)
        {
            Ok(BackendChoice::Local) => (self.local.clone(), None),
            Ok(BackendChoice::Docker { image }) => (self.docker.clone(), Some(image)),
            Ok(BackendChoice::Tensorlake { image }) => (self.tensorlake.clone(), Some(image)),
            Err(e) => {
                tracing::warn!(address, error = %e, "bash-exec: cannot parse address");
                return;
            }
        };

        let server = Arc::clone(&self.server);
        tokio::spawn(async move {
            run_task(server, task_id, task_version, backend, target).await;
        });
    }
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

async fn run_task(
    server: Arc<Server>,
    task_id: String,
    task_version: i64,
    backend: Arc<dyn ExecBackend>,
    target: Option<String>,
) {
    let pid = format!("bash-exec-{}", fastrand::u64(..));
    let lease_timeout = server.config.tasks.lease_timeout;

    // 1. Acquire — racing is normal; storage errors are transient (drop, let lease expire).
    let acquire_result = match server
        .storage
        .transact({
            let task_id = task_id.clone();
            let pid = pid.clone();
            move |db| {
                db.task_acquire(&TaskAcquireParams {
                    task_id: &task_id,
                    version: task_version,
                    time: system_time_ms(),
                    ttl: lease_timeout,
                    pid: &pid,
                })
            }
        })
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "bash-exec: task acquire failed");
            return;
        }
    };

    if !acquire_result.was_acquired {
        tracing::debug!(task_id, "bash-exec: task not acquired");
        return;
    }

    let acquired_version = acquire_result.task_version.unwrap_or(task_version + 1);

    let promise = match acquire_result.promise {
        Some(p) => p,
        None => {
            reject_promise(
                &server.storage,
                &task_id,
                acquired_version,
                "no promise returned after acquire",
            )
            .await;
            return;
        }
    };

    // 2. Decode script — param.data is base64-encoded; the decoded value IS the script.
    let script = match decode_param(promise.param.data.as_deref()) {
        Some(s) => s,
        None => {
            reject_promise(
                &server.storage,
                &task_id,
                acquired_version,
                "param.data is missing or not valid base64/utf-8",
            )
            .await;
            return;
        }
    };

    // 3. Heartbeat — refreshes the lease while the backend runs.
    let heartbeat = {
        let storage = Arc::clone(&server.storage);
        let task_id = task_id.clone();
        let pid = pid.clone();
        let version = acquired_version;
        tokio::spawn(async move {
            let beat_ms = (lease_timeout / 3).max(1000) as u64;
            let mut interval = tokio::time::interval(Duration::from_millis(beat_ms));
            interval.tick().await;
            loop {
                interval.tick().await;
                let task_id = task_id.clone();
                let pid = pid.clone();
                let now = system_time_ms();
                let _ = storage
                    .transact(move |db| db.task_heartbeat(&pid, &[(&task_id, version)], now))
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
        Err(msg) => reject_promise(&server.storage, &task_id, acquired_version, &msg).await,
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
            fulfill_promise(&server.storage, &task_id, acquired_version, &value).await;
        }
        Ok(status) => {
            let stderr = status.stderr.trim();
            let reason = if stderr.is_empty() {
                format!("exit code {}", status.code)
            } else {
                stderr.to_string()
            };
            reject_promise(&server.storage, &task_id, acquired_version, &reason).await;
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
    pub fn from_env() -> Self {
        Self {
            api_key: std::env::var("TENSORLAKE_API_KEY").ok(),
            client: reqwest::Client::new(),
        }
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
        let outcome = self
            .run_in_sandbox(&api_key, &sandbox_id, &req)
            .await;
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
            match s.get("status").and_then(|v| v.as_str()).unwrap_or("running") {
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

async fn fulfill_promise(storage: &Storage, task_id: &str, version: i64, value: &str) {
    let now = system_time_ms();
    let task_id = task_id.to_string();
    let value = value.to_string();
    let _ = storage
        .transact(move |db| {
            db.task_fulfill(&TaskFulfillParams {
                task_id: &task_id,
                version,
                promise_id: &task_id,
                state: "resolved",
                value_headers: None,
                value_data: Some(&value),
                settled_at: now,
            })
        })
        .await;
}

async fn reject_promise(storage: &Storage, task_id: &str, version: i64, reason: &str) {
    let now = system_time_ms();
    let task_id = task_id.to_string();
    let reason = reason.to_string();
    let _ = storage
        .transact(move |db| {
            db.task_fulfill(&TaskFulfillParams {
                task_id: &task_id,
                version,
                promise_id: &task_id,
                state: "rejected",
                value_headers: None,
                value_data: Some(&reason),
                settled_at: now,
            })
        })
        .await;
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
