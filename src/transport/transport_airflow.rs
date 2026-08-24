//! Airflow worker — one durable promise ⇄ one Apache Airflow DAG run.
//!
//! The worked example of an *integration*: a [`ResonateWorker`] that does not
//! run the work itself but makes a promise stand for a run in an external
//! system. Where the bash worker executes a script in process, this one
//! creates a DAG run over HTTP, watches it, and settles the promise from its
//! outcome.
//!
//! Three phases, and the first one is the whole design:
//!
//! 1. **Create.** Runs on *every* delivery. `execute` is at-least-once and is
//!    re-sent until the promise settles, and a worker cannot tell a first
//!    delivery from a redelivery — nothing in the message says. So the create
//!    is issued again every time under a key derived from the promise id, and
//!    Airflow's 409 on a duplicate `dag_run_id` is the recovery path: it means
//!    an earlier attempt already triggered this run, so re-attach and monitor.
//!    This only works because Airflow lets the caller name the run. A
//!    downstream system without that property cannot be integrated this way.
//! 2. **Monitor.** Two clocks, running independently:
//!    - the **lease clock** — heartbeat `task.heartbeat` at a third of the
//!      lease TTL, so the server does not redispatch the task;
//!    - the **downstream clock** — ask Airflow for the run's state on a
//!      backing-off interval sized for Airflow's cost and latency.
//!
//!    They answer to different authorities and must not be collapsed into one
//!    loop. The heartbeat runs in its own task, which is what lets the poll
//!    interval back off past the lease TTL without the lease lapsing.
//! 3. **Settle.** `task.fulfill` with the run's outcome, which is what finally
//!    stops the redelivery loop.
//!
//! ## Address schema
//!
//! ```text
//! airflow://<deployment>/dags/<dag_id>
//! ```
//!
//! The authority is a *deployment name*, not a host: it is the key into
//! `transports.airflow.deployments`, which is where base URLs and credentials
//! live. An address never carries a secret — it lands in logs, tags and error
//! messages.
//!
//! ## Param schema (`promise.param.data`, base64 UTF-8 JSON)
//!
//! ```json
//! { "conf": { "date": "2026-08-24" }, "note": "…", "logicalDate": null }
//! ```
//!
//! The SDK/CLI invocation envelope `{"func","args","version"}` is also
//! accepted, with `args[0]` taken as the object above — so an application can
//! reach this worker with an ordinary remote invocation. An empty param is a
//! trigger with no conf.
//!
//! ## Value schema (`promise.value.data`, base64 UTF-8 JSON)
//!
//! ```json
//! { "run": { "id": "…", "state": "success", "startedAt": 0, "endedAt": 0, "url": "…" },
//!   "output": { "runType": "manual", "conf": {} } }
//! ```
//!
//! and on rejection `error: { "kind": …, "message": … }` in place of `output`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::config::{AirflowConfig, AirflowDeployment};
use crate::core::types::{
    Message, PromiseRecord, RequestEnvelope, RequestHead, ResponseEnvelope,
    TaskAcquireResponseData, PROTOCOL_VERSION,
};
use crate::core::{ResonateServer, ResonateWorker, Unavailable};

// ─── Address ──────────────────────────────────────────────────────────────────

/// A parsed `airflow://<deployment>/dags/<dag_id>` address.
///
/// This worker owns the syntax past the scheme; the router has only checked
/// that the address is a URI whose scheme is `airflow`.
#[derive(Debug, Clone, PartialEq)]
pub struct AirflowAddress {
    pub deployment: String,
    pub dag_id: String,
}

impl AirflowAddress {
    pub fn parse(address: &str) -> Result<Self, String> {
        let parsed = url::Url::parse(address).map_err(|e| format!("not a URI: {e}"))?;
        if parsed.scheme() != "airflow" {
            return Err(format!(
                "expected airflow:// scheme, got {}://",
                parsed.scheme()
            ));
        }
        // `host_str` lowercases and strips the port, so take the authority
        // verbatim — a deployment name is a config key, not a hostname.
        let deployment = parsed.authority().to_string();
        if deployment.is_empty() {
            return Err("missing deployment: expected airflow://<deployment>/dags/<dag_id>".into());
        }
        let path = parsed.path().trim_matches('/');
        let dag_id = match path.split_once('/') {
            Some(("dags", dag)) if !dag.is_empty() && !dag.contains('/') => dag.to_string(),
            _ => {
                return Err(format!(
                    "expected airflow://<deployment>/dags/<dag_id>, got path '/{path}'"
                ))
            }
        };
        Ok(AirflowAddress { deployment, dag_id })
    }
}

// ─── Worker ───────────────────────────────────────────────────────────────────

pub struct AirflowWorker {
    /// This worker runs in the server's own process, so it holds the port
    /// directly. Every state change goes through `process` — the same path a
    /// remote worker's HTTP calls take.
    server: Arc<dyn ResonateServer>,
    client: reqwest::Client,
    deployments: HashMap<String, AirflowDeployment>,
    lease_timeout: i64,
    poll_interval: i64,
    max_poll_interval: i64,
}

impl AirflowWorker {
    pub fn new(
        server: Arc<dyn ResonateServer>,
        config: &AirflowConfig,
        lease_timeout: i64,
    ) -> Self {
        Self {
            server,
            client: reqwest::Client::new(),
            deployments: config.deployments.clone(),
            lease_timeout,
            poll_interval: config.poll_interval,
            max_poll_interval: config.max_poll_interval,
        }
    }
}

#[async_trait]
impl ResonateWorker for AirflowWorker {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Only `execute` asks for work. An `unblock` is a notification for a
        // worker that is waiting on a promise; this worker never waits.
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };

        let addr = AirflowAddress::parse(address)
            .map_err(|e| Unavailable::new(format!("airflow: bad address {address}: {e}")))?;
        let deployment = self
            .deployments
            .get(&addr.deployment)
            .cloned()
            .ok_or_else(|| {
                Unavailable::new(format!(
                    "airflow: no deployment '{}' configured (known: {:?})",
                    addr.deployment,
                    self.deployments.keys().collect::<Vec<_>>()
                ))
            })?;

        // `send` means accepted for delivery, not executed: a DAG run can take
        // hours, and the dispatcher must not block on it.
        let ctx = RunContext {
            server: Arc::clone(&self.server),
            client: self.client.clone(),
            deployment,
            addr,
            lease_timeout: self.lease_timeout,
            poll_interval: self.poll_interval,
            max_poll_interval: self.max_poll_interval,
            task_id: task.id.clone(),
            task_version: task.version,
        };
        tokio::spawn(async move { ctx.run().await });
        Ok(())
    }
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

struct RunContext {
    server: Arc<dyn ResonateServer>,
    client: reqwest::Client,
    deployment: AirflowDeployment,
    addr: AirflowAddress,
    lease_timeout: i64,
    poll_interval: i64,
    max_poll_interval: i64,
    task_id: String,
    task_version: i64,
}

/// What one status check found. `Pending` means keep waiting.
enum RunState {
    Pending,
    Succeeded(Value),
    Failed(Value),
}

/// How monitoring ended. Distinct from [`RunState`] because reaching the
/// promise deadline is an outcome of the loop, not a state Airflow reports.
enum Monitored {
    Succeeded(Value),
    Failed(Value),
    DeadlineReached,
}

/// A downstream failure, classified. The classification decides whether the
/// promise is settled or the task is dropped for redelivery.
#[derive(Debug)]
enum AirflowError {
    /// Can never succeed: reject the promise now.
    Permanent { kind: &'static str, message: String },
    /// Might succeed later, or the outcome is unknown. Drop the task without
    /// settling; the lease expires and the message is redelivered. Safe
    /// because create is idempotent.
    Transient(String),
}

impl RunContext {
    async fn run(self) {
        let pid = format!("airflow-{}", fastrand::u64(..));

        // 1. Acquire. A 409 means another attempt owns this task — drop
        //    silently and never touch Airflow. Anything else non-200 is
        //    transient: drop and let the retry loop redeliver.
        let acquired = match self.acquire(&pid).await {
            Some(a) => a,
            None => return,
        };
        let version = acquired.task.version;
        let promise = acquired.promise;

        // Monitoring runs two independent clocks. The lease clock lives in its
        // own task so that however long the downstream clock sleeps between
        // status checks, the lease is still refreshed underneath it.
        let heartbeat = self.spawn_heartbeat(pid.clone(), version);
        let outcome = self.create_and_monitor(&promise).await;
        heartbeat.abort();

        match outcome {
            Ok(Monitored::Succeeded(run)) => {
                let output = json!({
                    "runType": run.get("run_type").cloned().unwrap_or(Value::Null),
                    "conf": run.get("conf").cloned().unwrap_or_else(|| json!({})),
                    "note": run.get("note").cloned().unwrap_or(Value::Null),
                    "logicalDate": run.get("logical_date").cloned().unwrap_or(Value::Null),
                });
                self.settle(
                    version,
                    "resolved",
                    json!({ "run": self.run_summary(&run), "output": output }),
                )
                .await;
            }
            Ok(Monitored::Failed(run)) => {
                self.settle(
                    version,
                    "rejected",
                    json!({
                        "run": self.run_summary(&run),
                        "error": {
                            "kind": "downstream_failed",
                            "message": format!(
                                "DAG run finished in state {}",
                                run.get("state").and_then(Value::as_str).unwrap_or("unknown")
                            ),
                        }
                    }),
                )
                .await;
            }
            // The server settles the promise `rejected_timedout` at `timeoutAt`
            // on its own; stop watching and leave the DAG run alone rather than
            // racing it.
            Ok(Monitored::DeadlineReached) => {
                tracing::warn!(
                    task_id = %self.task_id,
                    dag_id = %self.addr.dag_id,
                    "airflow: promise deadline reached, stopped monitoring"
                );
            }
            Err(AirflowError::Permanent { kind, message }) => {
                tracing::warn!(task_id = %self.task_id, kind, %message, "airflow: permanent failure");
                self.settle(
                    version,
                    "rejected",
                    json!({ "run": {}, "error": { "kind": kind, "message": message } }),
                )
                .await;
            }
            // Do not settle: the lease will expire and the idempotent create
            // will run again against the same run id.
            Err(AirflowError::Transient(message)) => {
                tracing::warn!(
                    task_id = %self.task_id,
                    %message,
                    "airflow: transient failure, dropping task for redelivery"
                );
            }
        }
    }

    /// Phases 1 and 2. Returns the terminal run, or `Pending` if the promise
    /// deadline arrived first.
    async fn create_and_monitor(&self, promise: &PromiseRecord) -> Result<Monitored, AirflowError> {
        let input = decode_param(promise.param.data.as_deref())?;
        let run_id = derive_run_id(&promise.id);

        // Phase 1 — create. Idempotent by construction.
        self.create_dag_run(&run_id, &input).await?;

        // Phase 2 — monitor, on the downstream clock. Sized for Airflow's cost
        // and latency, and deliberately unrelated to the lease TTL: the
        // heartbeat task holds the lease open independently, so this interval
        // may back off well past it.
        let mut interval = self.poll_interval;
        loop {
            let now = crate::util::system_time_ms();
            if now >= promise.timeout_at {
                return Ok(Monitored::DeadlineReached);
            }
            match self.get_dag_run(&run_id).await? {
                RunState::Pending => {}
                RunState::Succeeded(run) => return Ok(Monitored::Succeeded(run)),
                RunState::Failed(run) => return Ok(Monitored::Failed(run)),
            }
            // Never sleep past the promise deadline: the next iteration has to
            // observe it and stop rather than wake up after the server has
            // already settled the promise.
            let sleep_ms = interval.min(promise.timeout_at - now).max(0) as u64;
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            interval = interval.saturating_mul(2).min(self.max_poll_interval);
        }
    }

    /// Trigger the DAG run, treating "already exists" as success.
    async fn create_dag_run(&self, run_id: &str, input: &Value) -> Result<(), AirflowError> {
        let mut body = json!({
            "dag_run_id": run_id,
            "conf": input.get("conf").cloned().unwrap_or_else(|| json!({})),
        });
        if let Some(note) = input.get("note") {
            body["note"] = note.clone();
        }
        // Airflow 3 requires the key to be present (null means "run now");
        // Airflow 2 defaults it when omitted.
        if self.deployment.api_version != "v1" || input.get("logicalDate").is_some() {
            body["logical_date"] = input.get("logicalDate").cloned().unwrap_or(Value::Null);
        }

        let url = format!(
            "{}/dags/{}/dagRuns",
            self.deployment.api_base(),
            urlencode(&self.addr.dag_id)
        );
        let (status, payload) = self
            .request(reqwest::Method::POST, &url, Some(body))
            .await?;

        match status {
            200 | 201 => {
                tracing::info!(
                    task_id = %self.task_id,
                    dag_id = %self.addr.dag_id,
                    run_id,
                    "airflow: DAG run triggered"
                );
                Ok(())
            }
            // The recovery path, not an error: an earlier delivery already
            // triggered this run. Re-attach and monitor it.
            409 => {
                tracing::info!(
                    task_id = %self.task_id,
                    dag_id = %self.addr.dag_id,
                    run_id,
                    "airflow: DAG run already exists, re-attaching"
                );
                Ok(())
            }
            404 => Err(AirflowError::Permanent {
                kind: "not_found",
                message: format!(
                    "DAG '{}' not found in deployment '{}'",
                    self.addr.dag_id, self.addr.deployment
                ),
            }),
            400 | 422 => Err(AirflowError::Permanent {
                kind: "invalid_request",
                message: format!("Airflow rejected the trigger request: {payload}"),
            }),
            401 | 403 => Err(AirflowError::Permanent {
                kind: "unauthorized",
                message: format!(
                    "Airflow rejected the worker credentials for '{}'",
                    self.addr.deployment
                ),
            }),
            other => Err(AirflowError::Transient(format!(
                "trigger dag '{}': HTTP {other}: {payload}",
                self.addr.dag_id
            ))),
        }
    }

    async fn get_dag_run(&self, run_id: &str) -> Result<RunState, AirflowError> {
        let url = format!(
            "{}/dags/{}/dagRuns/{}",
            self.deployment.api_base(),
            urlencode(&self.addr.dag_id),
            urlencode(run_id)
        );
        let (status, payload) = self.request(reqwest::Method::GET, &url, None).await?;

        match status {
            200 => {}
            // The run is gone — cleared or deleted. Re-triggering here would
            // break the one-promise-one-run contract, and waiting will not
            // bring it back.
            404 => {
                return Err(AirflowError::Permanent {
                    kind: "not_found",
                    message: format!("DAG run '{run_id}' no longer exists"),
                })
            }
            401 | 403 => {
                return Err(AirflowError::Permanent {
                    kind: "unauthorized",
                    message: format!(
                        "Airflow rejected the worker credentials for '{}'",
                        self.addr.deployment
                    ),
                })
            }
            other => {
                return Err(AirflowError::Transient(format!(
                    "get dag run '{run_id}': HTTP {other}: {payload}"
                )))
            }
        }

        Ok(classify_run(payload, run_id))
    }

    /// One authenticated request, with a token refresh on a mid-flight 401.
    async fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), AirflowError> {
        let mut req = self.client.request(method.clone(), url);
        req = self.deployment.authenticate(req);
        if let Some(ref b) = body {
            req = req.json(b);
        }
        let resp = req.send().await.map_err(|e| {
            // Connection refused before the first byte and a timeout after the
            // last are indistinguishable here, so the request may or may not
            // have been applied. Transient is the only safe classification —
            // and it is safe because create is idempotent.
            AirflowError::Transient(format!("{method} {url}: {e}"))
        })?;
        let status = resp.status().as_u16();
        let payload = resp.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, payload))
    }

    fn run_summary(&self, run: &Value) -> Value {
        let run_id = run
            .get("dag_run_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        json!({
            "id": run_id,
            "state": run.get("state").cloned().unwrap_or(Value::Null),
            "startedAt": run.get("start_date").cloned().unwrap_or(Value::Null),
            "endedAt": run.get("end_date").cloned().unwrap_or(Value::Null),
            "url": self.deployment.run_url(&self.addr.dag_id, run_id),
        })
    }

    // -- protocol calls -----------------------------------------------------

    async fn request_server(
        &self,
        kind: &str,
        data: Value,
    ) -> Result<ResponseEnvelope, Unavailable> {
        self.server
            .process(&RequestEnvelope {
                kind: kind.to_string(),
                head: RequestHead {
                    corr_id: format!("airflow-{}", fastrand::u64(..)),
                    version: PROTOCOL_VERSION.to_string(),
                    auth: None,
                    debug_time: None,
                },
                data,
            })
            .await
    }

    async fn acquire(&self, pid: &str) -> Option<TaskAcquireResponseData> {
        let resp = match self
            .request_server(
                "task.acquire",
                json!({
                    "id": self.task_id,
                    "version": self.task_version,
                    "pid": pid,
                    "ttl": self.lease_timeout
                }),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(task_id = %self.task_id, error = %e, "airflow: task acquire failed");
                return None;
            }
        };
        if resp.head.status == 409 {
            tracing::debug!(task_id = %self.task_id, "airflow: task not acquired");
            return None;
        }
        if resp.head.status != 200 {
            tracing::warn!(
                task_id = %self.task_id,
                status = resp.head.status,
                "airflow: task acquire rejected"
            );
            return None;
        }
        match serde_json::from_value(resp.data) {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::error!(task_id = %self.task_id, error = %e, "airflow: malformed acquire response");
                None
            }
        }
    }

    /// The lease clock. Runs until aborted, on a cadence derived from the
    /// lease TTL and from nothing else.
    ///
    /// Its own task on purpose: folding the heartbeat into the poll loop would
    /// tie the lease to the downstream system's cadence, and a poll interval
    /// that backed off past the lease TTL would silently drop the task.
    fn spawn_heartbeat(&self, pid: String, version: i64) -> tokio::task::JoinHandle<()> {
        let server = Arc::clone(&self.server);
        let task_id = self.task_id.clone();
        let beat_ms = heartbeat_interval_ms(self.lease_timeout);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(beat_ms));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                // Deliberately ignored. `task.heartbeat` answers 200 whether or
                // not it refreshed anything — a heartbeat for a lease this
                // worker no longer holds is a silent no-op — so the response
                // carries no signal to act on. Losing the lease surfaces at
                // `task.fulfill`, as a 409.
                let _ = server
                    .process(&RequestEnvelope {
                        kind: "task.heartbeat".to_string(),
                        head: RequestHead {
                            corr_id: format!("airflow-hb-{}", fastrand::u64(..)),
                            version: PROTOCOL_VERSION.to_string(),
                            auth: None,
                            debug_time: None,
                        },
                        data: json!({
                            "pid": pid,
                            "tasks": [{ "id": task_id, "version": version }]
                        }),
                    })
                    .await;
            }
        })
    }

    async fn settle(&self, version: i64, state: &str, value: Value) {
        let encoded = b64_encode(&value.to_string());
        let resp = self
            .request_server(
                "task.fulfill",
                json!({
                    "id": self.task_id,
                    "version": version,
                    "action": {
                        "kind": "promise.settle",
                        "head": {},
                        "data": {
                            "id": self.task_id,
                            "state": state,
                            "value": {
                                "headers": { "content-type": "application/json" },
                                "data": encoded
                            }
                        }
                    }
                }),
            )
            .await;
        match resp {
            Ok(r) if (200..300).contains(&r.head.status) => {
                tracing::info!(task_id = %self.task_id, state, "airflow: promise settled");
            }
            // 409: the lease was lost, or the promise already settled — almost
            // always a timeout. Retrying cannot help.
            Ok(r) => tracing::warn!(
                task_id = %self.task_id,
                status = r.head.status,
                state,
                "airflow: task fulfill rejected"
            ),
            Err(e) => {
                tracing::error!(task_id = %self.task_id, error = %e, "airflow: task fulfill failed")
            }
        }
    }
}

// ─── Pure helpers ─────────────────────────────────────────────────────────────

/// Heartbeat cadence in ms, derived from the lease TTL — never from the poll
/// interval, which answers to the downstream system instead.
///
/// A third of the lease leaves room to miss two beats before the task is
/// redispatched. The 1s floor keeps a short lease from hammering the server,
/// and the half-lease ceiling stops that floor from pushing the first beat past
/// the very lease it exists to refresh.
fn heartbeat_interval_ms(lease_timeout: i64) -> u64 {
    let third = (lease_timeout / 3).max(1_000);
    third.min((lease_timeout / 2).max(1)).max(1) as u64
}

/// Airflow DAG run states (`airflow.utils.state.DagRunState`).
const RUNNING_STATES: [&str; 2] = ["queued", "running"];

/// Map an Airflow DAG run record onto the three outcomes this worker acts on.
///
/// An unrecognised state is **pending**, never failed: a new Airflow release
/// adding a state must not turn healthy promises into rejected ones.
fn classify_run(mut run: Value, run_id: &str) -> RunState {
    // The GET response carries `dag_run_id`, but pin it so the value schema is
    // populated even if a deployment omits it.
    if run.get("dag_run_id").and_then(Value::as_str).is_none() {
        if let Some(obj) = run.as_object_mut() {
            obj.insert("dag_run_id".into(), json!(run_id));
        }
    }
    let state = run
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    match state.as_str() {
        "success" => RunState::Succeeded(run),
        "failed" => RunState::Failed(run),
        s if RUNNING_STATES.contains(&s) => RunState::Pending,
        other => {
            tracing::warn!(
                run_id,
                state = other,
                "airflow: unrecognised DAG run state, treating as still running"
            );
            RunState::Pending
        }
    }
}

/// The idempotency key: a pure function of the promise id.
///
/// Stable across every redelivery, restart and failover — that is the entire
/// requirement, and it rules out UUIDs, clock reads and `task.version`. The
/// readable prefix makes the run findable in the Airflow UI; the digest keeps
/// it unique after sanitising and truncating.
fn derive_run_id(promise_id: &str) -> String {
    let digest = fnv1a64_hex(promise_id.as_bytes());
    let safe: String = promise_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(100)
        .collect();
    format!("resonate-{safe}-{digest}")
}

/// FNV-1a (64-bit) as lowercase hex.
///
/// Deliberately hand-rolled rather than `DefaultHasher`: this value is written
/// into a downstream system and has to be reproducible by every future build,
/// and `DefaultHasher`'s output is explicitly not stable across Rust releases.
/// It is a disambiguator, not a security primitive — the promise id it is
/// derived from is already public.
fn fnv1a64_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Decode `param.data` into the trigger input.
///
/// Accepts the SDK/CLI invocation envelope `{"func","args","version"}` as well
/// as a bare object, so the same worker serves `ctx.rpc` callers and direct
/// `promise.create` callers. An absent param is a trigger with no conf.
fn decode_param(data: Option<&str>) -> Result<Value, AirflowError> {
    let Some(encoded) = data.filter(|s| !s.is_empty()) else {
        return Ok(json!({}));
    };
    let bytes = b64_decode(encoded).ok_or_else(|| AirflowError::Permanent {
        kind: "invalid_request",
        message: "param.data is not valid base64".into(),
    })?;
    let text = String::from_utf8(bytes).map_err(|_| AirflowError::Permanent {
        kind: "invalid_request",
        message: "param.data is not valid UTF-8".into(),
    })?;
    let value: Value = serde_json::from_str(&text).map_err(|e| AirflowError::Permanent {
        kind: "invalid_request",
        message: format!("param.data is not valid JSON: {e}"),
    })?;

    let body = if value.get("func").is_some() && value.get("args").is_some() {
        value
            .get("args")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .cloned()
            .unwrap_or_else(|| json!({}))
    } else {
        value
    };
    if !body.is_object() {
        return Err(AirflowError::Permanent {
            kind: "invalid_request",
            message: "trigger input must be a JSON object".into(),
        });
    }
    Ok(body)
}

fn b64_encode(s: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

/// Percent-encode one path segment. DAG ids and run ids are caller-supplied
/// and routinely contain `:`, `+` and `/`.
fn urlencode(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- address ----

    #[test]
    fn address_parses() {
        let a = AirflowAddress::parse("airflow://prod/dags/etl_daily").unwrap();
        assert_eq!(a.deployment, "prod");
        assert_eq!(a.dag_id, "etl_daily");
    }

    #[test]
    fn address_keeps_the_authority_verbatim() {
        // A deployment name is a config key, not a hostname: no lowercasing,
        // and a port-looking suffix is part of the name.
        let a = AirflowAddress::parse("airflow://Airflow.Internal:8080/dags/x").unwrap();
        assert_eq!(a.deployment, "Airflow.Internal:8080");
    }

    #[test]
    fn address_rejects_wrong_shape() {
        for addr in [
            "airflow://prod",          // no path
            "airflow://prod/dags",     // no dag id
            "airflow://prod/dags/",    // empty dag id
            "airflow://prod/jobs/x",   // wrong collection
            "airflow://prod/dags/a/b", // over-deep
            "airflow:///dags/x",       // no deployment
            "bash://prod/dags/x",      // wrong scheme
            "not a url",
        ] {
            assert!(
                AirflowAddress::parse(addr).is_err(),
                "expected rejection: {addr}"
            );
        }
    }

    // ---- idempotency key ----

    #[test]
    fn run_id_is_a_pure_function_of_the_promise_id() {
        assert_eq!(
            derive_run_id("airflow.etl.1"),
            derive_run_id("airflow.etl.1")
        );
        assert_ne!(
            derive_run_id("airflow.etl.1"),
            derive_run_id("airflow.etl.2")
        );
    }

    #[test]
    fn run_id_is_readable_and_url_safe() {
        let id = derive_run_id("airflow.etl:2026-08-24");
        assert!(id.starts_with("resonate-airflow.etl_2026-08-24-"), "{id}");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_'),
            "{id}"
        );
    }

    #[test]
    fn run_id_stays_unique_after_truncation() {
        // Two ids that share their first 100 characters must not collide: the
        // digest is taken over the whole id, not the truncated prefix.
        let a = derive_run_id(&format!("{}-a", "x".repeat(120)));
        let b = derive_run_id(&format!("{}-b", "x".repeat(120)));
        assert_ne!(a, b);
    }

    // ---- param ----

    #[test]
    fn param_accepts_the_sdk_invocation_envelope() {
        let inner = json!({"func": "trigger", "args": [{"conf": {"a": 1}}], "version": 1});
        let got = decode_param(Some(&b64_encode(&inner.to_string()))).unwrap();
        assert_eq!(got, json!({"conf": {"a": 1}}));
    }

    #[test]
    fn param_accepts_a_bare_object() {
        let inner = json!({"conf": {"a": 1}});
        let got = decode_param(Some(&b64_encode(&inner.to_string()))).unwrap();
        assert_eq!(got, json!({"conf": {"a": 1}}));
    }

    #[test]
    fn absent_param_is_a_trigger_with_no_conf() {
        assert_eq!(decode_param(None).unwrap(), json!({}));
        assert_eq!(decode_param(Some("")).unwrap(), json!({}));
    }

    #[test]
    fn malformed_param_is_permanent_not_transient() {
        for data in [
            "!!!not base64!!!",
            &b64_encode("not json"),
            &b64_encode("[1,2,3]"), // valid JSON, not an object
        ] {
            match decode_param(Some(data)) {
                Err(AirflowError::Permanent { kind, .. }) => assert_eq!(kind, "invalid_request"),
                _ => panic!("expected a permanent error for {data}"),
            }
        }
    }

    // ---- run state ----

    #[test]
    fn terminal_states_are_classified() {
        assert!(matches!(
            classify_run(json!({"state": "success"}), "r"),
            RunState::Succeeded(_)
        ));
        assert!(matches!(
            classify_run(json!({"state": "failed"}), "r"),
            RunState::Failed(_)
        ));
    }

    #[test]
    fn running_states_are_pending() {
        for state in ["queued", "running"] {
            assert!(matches!(
                classify_run(json!({ "state": state }), "r"),
                RunState::Pending
            ));
        }
    }

    #[test]
    fn unknown_state_is_pending_not_failed() {
        // A future Airflow release adding a state must not reject healthy
        // promises.
        for state in ["deferred", "up_for_retry", "", "SOMETHING_NEW"] {
            assert!(
                matches!(
                    classify_run(json!({ "state": state }), "r"),
                    RunState::Pending
                ),
                "state {state:?} should be pending"
            );
        }
    }

    #[test]
    fn run_id_is_pinned_into_the_record() {
        let RunState::Succeeded(run) = classify_run(json!({"state": "success"}), "run-42") else {
            panic!("expected success");
        };
        assert_eq!(run["dag_run_id"], "run-42");
    }

    // ---- the two clocks ----

    #[test]
    fn heartbeat_cadence_is_a_third_of_the_lease() {
        assert_eq!(heartbeat_interval_ms(15_000), 5_000);
        assert_eq!(heartbeat_interval_ms(30_000), 10_000);
        assert_eq!(heartbeat_interval_ms(600_000), 200_000);
    }

    #[test]
    fn heartbeat_always_fits_inside_the_lease() {
        // The 1s floor must never push the first beat past the lease it is
        // meant to refresh — config allows a lease as short as 1ms.
        for lease in [1_i64, 2, 500, 999, 1_000, 2_999, 3_000, 15_000] {
            let beat = heartbeat_interval_ms(lease) as i64;
            assert!(beat >= 1, "lease {lease}: cadence must be positive");
            assert!(
                beat <= lease,
                "lease {lease}: first beat at {beat}ms lands after the lease expires"
            );
        }
    }

    #[test]
    fn poll_backoff_does_not_overflow() {
        // The downstream clock is independent of the lease, so nothing bounds
        // max_poll_interval from above — including a pathological config.
        let max_poll_interval = i64::MAX / 2;
        let mut interval = 5_000_i64;
        for _ in 0..80 {
            interval = interval.saturating_mul(2).min(max_poll_interval);
            assert!(interval > 0, "backoff overflowed to {interval}");
        }
        assert_eq!(
            interval, max_poll_interval,
            "backoff should settle at the cap"
        );
    }

    // ---- url encoding ----

    #[test]
    fn path_segments_are_encoded() {
        assert_eq!(urlencode("etl_daily"), "etl_daily");
        assert_eq!(urlencode("a/b"), "a%2Fb");
        assert_eq!(
            urlencode("2026-08-24T10:00:00+00:00"),
            "2026-08-24T10%3A00%3A00%2B00%3A00"
        );
    }
}
