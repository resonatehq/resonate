use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::process::Command;

use crate::persistence::{Storage, TaskAcquireParams, TaskFulfillParams};
use crate::server::Server;
use crate::util::system_time_ms;

// ─── Transport ────────────────────────────────────────────────────────────────

pub enum WorkingDir {
    Root,
    Script,
    Explicit(PathBuf),
}

impl WorkingDir {
    pub fn from_config(s: &str) -> Self {
        match s {
            "<root>" => Self::Root,
            "<script>" => Self::Script,
            path => Self::Explicit(PathBuf::from(path)),
        }
    }
}

pub struct BashExecTransport {
    server: Arc<Server>,
    root_dir: Option<PathBuf>,
    working_dir: WorkingDir,
}

impl BashExecTransport {
    pub fn new(
        server: Arc<Server>,
        root_dir: Option<impl Into<PathBuf>>,
        working_dir: WorkingDir,
    ) -> Self {
        Self {
            server,
            root_dir: root_dir.map(|d| d.into()),
            working_dir,
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

        let mode = match resolve_mode(address, self.root_dir.as_deref()) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(address, error = %e, "bash-exec: cannot resolve script mode");
                return;
            }
        };

        let working_dir = match &self.working_dir {
            WorkingDir::Root => self.root_dir.clone().map(WorkingDirResolved::Fixed),
            WorkingDir::Script => Some(WorkingDirResolved::ScriptDir),
            WorkingDir::Explicit(path) => Some(WorkingDirResolved::Fixed(path.clone())),
        };

        let server = Arc::clone(&self.server);

        tokio::spawn(async move {
            run_task(server, task_id, task_version, mode, working_dir).await;
        });
    }
}

// ─── Script and working directory modes ──────────────────────────────────────

enum WorkingDirResolved {
    Fixed(PathBuf), // root_dir or an explicit path
    ScriptDir,      // resolved at run time from the script path
}

enum ScriptMode {
    Inline,
    File(PathBuf),
}

fn resolve_mode(address: &str, root_dir: Option<&Path>) -> Result<ScriptMode, String> {
    let parsed = url::Url::parse(address).map_err(|e| format!("invalid address: {e}"))?;
    let path = parsed.path().trim_start_matches('/');
    if path.is_empty() {
        return Ok(ScriptMode::Inline);
    }
    match root_dir {
        None => Err("bash:///path address requires bash_exec.root_dir in config".to_string()),
        Some(dir) => Ok(ScriptMode::File(dir.join(path))),
    }
}

// ─── Task execution ───────────────────────────────────────────────────────────

async fn run_task(
    server: Arc<Server>,
    task_id: String,
    task_version: i64,
    mode: ScriptMode,
    working_dir: Option<WorkingDirResolved>,
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
            tracing::error!(error = %e, "bash-exec: task execution failed");
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

    // 2. Decode param.data — base64-encoded (CLI convention).
    //    Inline mode: the decoded value IS the script.
    //    File mode:   the decoded value is passed as RESONATE_PARAM to the script.
    let param = decode_param(promise.param.data.as_deref());

    // 3. Build the command.
    let child = match mode {
        ScriptMode::Inline => {
            let script = match param {
                Some(s) => s,
                None => {
                    reject_promise(
                        &server.storage,
                        &task_id,
                        acquired_version,
                        "param.data is missing",
                    )
                    .await;
                    return;
                }
            };
            Command::new("bash")
                .arg("-c")
                .arg(script)
                .env("RESONATE_PROMISE_ID", &task_id)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
        }
        ScriptMode::File(path) => {
            if !path.exists() {
                reject_promise(
                    &server.storage,
                    &task_id,
                    acquired_version,
                    &format!("script not found: {}", path.display()),
                )
                .await;
                return;
            }
            let args: Vec<String> = match param.as_deref() {
                None | Some("") => vec![],
                Some(s) => match serde_json::from_str::<Vec<serde_json::Value>>(s) {
                    Ok(arr) => arr
                        .iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .collect(),
                    Err(_) => {
                        reject_promise(
                            &server.storage,
                            &task_id,
                            acquired_version,
                            "param.data must be a JSON array for named scripts",
                        )
                        .await;
                        return;
                    }
                },
            };
            let mut cmd = Command::new("bash");
            cmd.arg(&path)
                .args(args)
                .env("RESONATE_PROMISE_ID", &task_id)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            if let Some(wd) = working_dir {
                let dir = match wd {
                    WorkingDirResolved::Fixed(d) => d,
                    WorkingDirResolved::ScriptDir => path
                        .parent()
                        .map(|p| p.to_path_buf())
                        .unwrap_or_else(|| path.clone()),
                };
                cmd.current_dir(dir);
            }
            cmd.spawn()
        }
    };

    let child = match child {
        Ok(c) => c,
        Err(e) => {
            reject_promise(
                &server.storage,
                &task_id,
                acquired_version,
                &format!("failed to spawn bash: {e}"),
            )
            .await;
            return;
        }
    };

    // 4. Heartbeat — refreshes the lease while bash runs.
    let heartbeat = {
        let storage = Arc::clone(&server.storage);
        let task_id = task_id.clone();
        let pid = pid.clone();
        let version = acquired_version;
        tokio::spawn(async move {
            let beat_ms = (lease_timeout / 3).max(1000) as u64;
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(beat_ms));
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

    // 5. Wait for bash to finish.
    let output = child.wait_with_output().await;
    heartbeat.abort();

    // 6. Handle outcome.
    match output {
        Err(e) => {
            reject_promise(
                &server.storage,
                &task_id,
                acquired_version,
                &format!("bash wait failed: {e}"),
            )
            .await;
        }
        Ok(out) => {
            let now = system_time_ms();
            if out.status.success() {
                let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
                let _ = server
                    .storage
                    .transact({
                        let task_id = task_id.clone();
                        move |db| {
                            db.task_fulfill(&TaskFulfillParams {
                                task_id: &task_id,
                                version: acquired_version,
                                promise_id: &task_id,
                                state: "resolved",
                                value_headers: None,
                                value_data: Some(&value),
                                settled_at: now,
                            })
                        }
                    })
                    .await;
            } else {
                let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                let code = out.status.code().unwrap_or(-1);
                let reason = if stderr.is_empty() {
                    format!("exit code {code}")
                } else {
                    stderr
                };
                reject_promise(&server.storage, &task_id, acquired_version, &reason).await;
            }
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn decode_param(data: Option<&str>) -> Option<String> {
    use base64::Engine;
    let d = data.filter(|s| !s.is_empty())?;
    let bytes = base64::engine::general_purpose::STANDARD.decode(d).ok()?;
    String::from_utf8(bytes).ok()
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
