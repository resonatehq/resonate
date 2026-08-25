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
//! airflow://<deployment>
//! ```
//!
//! That is all of it. The scheme routes the message to this worker; the
//! authority is a *deployment name*, not a host — the key into
//! `transports.airflow.deployments`, which is where base URLs and credentials
//! live. An address never carries a secret: it lands in logs, tags and error
//! messages.
//!
//! Which DAG to trigger is *not* here. That is the request, and the request is
//! the param — one parser, one set of malformed cases, one place to look.
//!
//! ## Param schema (`promise.param.data`, base64 UTF-8 JSON)
//!
//! ```json
//! { "dag": "etl_daily", "conf": { "date": "2026-08-24" },
//!   "note": "…", "logicalDate": null }
//! ```
//!
//! `dag` is required; the rest default. Unknown fields are rejected, so a typo
//! is an error naming the field rather than a silently ignored setting. This is
//! the only accepted shape — the SDK envelope `{"func","args","version"}` is
//! the SDK's convention for SDK functions, and an integration is not one.
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
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::{AirflowConfig, AirflowDeployment};
use crate::core::types::{
    ExecuteMsgTask, Message, PromiseRecord, RequestEnvelope, RequestHead, TaskAcquireResponseData,
    PROTOCOL_VERSION, TARGET_TAG,
};
use crate::core::{ResonateServer, ResonateWorker, Unavailable};

// ─── Address ──────────────────────────────────────────────────────────────────

/// Parse `airflow://<deployment>`, returning the deployment name.
///
/// That is the whole address: the scheme routes the message to this worker, and
/// the authority selects which Airflow to talk to. Nothing else belongs here —
/// *which* DAG to trigger is the request, and the request is the param. Putting
/// it in the path would mean two parsers, two sets of malformed cases, and two
/// places a caller has to look.
///
/// The authority is taken verbatim: `host_str` lowercases and strips the port,
/// and a deployment name is a config key, not a hostname.
fn parse_address(address: &str) -> Result<String, String> {
    let parsed = url::Url::parse(address).map_err(|e| format!("not a URI: {e}"))?;
    if parsed.scheme() != "airflow" {
        return Err(format!(
            "expected airflow:// scheme, got {}://",
            parsed.scheme()
        ));
    }
    let deployment = parsed.authority().to_string();
    if deployment.is_empty() {
        return Err("missing deployment: expected airflow://<deployment>".into());
    }
    let path = parsed.path().trim_matches('/');
    if !path.is_empty() {
        return Err(format!(
            "airflow:// takes no path — the DAG is a param, not a route (got '/{path}')"
        ));
    }
    Ok(deployment)
}

/// The `pid` this worker claims tasks under.
///
/// A constant, and it can be: `pid` only has to match between the `task.acquire`
/// that claims a task and the `task.heartbeat` that refreshes it — the storage
/// guard is `id = ? AND process_id = ?` plus an exists-check on the task's
/// *version*. The version is the real fence, so two runs sharing a pid cannot
/// touch each other's leases, and a stale attempt heartbeating an old version is
/// a no-op. Generating a unique pid per run would only make `TaskRecord.pid`
/// marginally more informative to an operator.
const PID: &str = "self";

// ─── Worker ───────────────────────────────────────────────────────────────────

/// Everything the worker holds. Shared by every run behind one `Arc`, so a
/// delivery costs a pointer clone rather than a copy of the config.
struct Airflow {
    /// The inbound port: how a run claims its task, heartbeats it, and settles
    /// its promise. This worker is in the server's own process, so it holds the
    /// port directly, but every state change still goes through `process` — the
    /// same path a remote worker's HTTP calls take.
    server: Arc<dyn ResonateServer>,
    client: reqwest::Client,
    deployments: HashMap<String, AirflowDeployment>,
    lease_timeout: i64,
    poll_interval: i64,
    max_poll_interval: i64,
}

pub struct AirflowWorker {
    inner: Arc<Airflow>,
}

impl AirflowWorker {
    pub fn new(
        server: Arc<dyn ResonateServer>,
        config: &AirflowConfig,
        lease_timeout: i64,
    ) -> Self {
        Self {
            inner: Arc::new(Airflow {
                server,
                client: reqwest::Client::new(),
                deployments: config.deployments.clone(),
                lease_timeout,
                poll_interval: config.poll_interval,
                max_poll_interval: config.max_poll_interval,
            }),
        }
    }
}

#[async_trait]
impl ResonateWorker for AirflowWorker {
    async fn send(&self, _address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Only `execute` asks for work. An `unblock` is a notification for a
        // worker that is waiting on a promise; this worker never waits.
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };

        // Hand off, and do nothing else here.
        //
        // The first real step is claiming the task, and that is a server round
        // trip. `process_batch` awaits `send` sequentially over the whole
        // batch, so a round trip here would stall delivery of every other
        // message in it — including messages for other schemes. Validation
        // waits too: until the task is claimed the only way to report anything
        // is `Err(Unavailable)`, which the dispatch loop logs and drops.
        //
        // `send` means accepted for delivery, not executed.
        //
        // The `address` parameter goes unused here. It is what a *proxy* worker
        // needs — HTTP push has a URL to POST to, poll a group to fan out to —
        // and they have no promise in hand. This worker claims the task, so it
        // reads its address off the promise instead: one durable source of
        // truth, and the same one every other input comes from.
        let ctx = RunContext {
            worker: Arc::clone(&self.inner),
            task: task.clone(),
        };
        tokio::spawn(async move { ctx.run().await });
        Ok(())
    }
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

/// One delivery in flight. Two things and nothing else: the worker it belongs
/// to, and the task it was told about. Everything else comes from the promise,
/// once the task is claimed.
struct RunContext {
    worker: Arc<Airflow>,
    task: ExecuteMsgTask,
}

/// The deployment this promise targets, resolved once the task is owned.
struct Target {
    /// The address authority, kept for error messages.
    name: String,
    deployment: AirflowDeployment,
}

/// The request, decoded and validated from `promise.param.data`.
///
/// This is the integration's contract with its callers, and the only shape it
/// accepts. `deny_unknown_fields` makes a typo a rejection naming the field
/// rather than a silently ignored setting.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerRequest {
    /// Which DAG to trigger.
    dag: String,
    /// Passed to Airflow as the DAG run's `conf`.
    #[serde(default = "empty_object")]
    conf: Value,
    #[serde(default)]
    note: Option<String>,
    /// Airflow 3 requires the key to be present; `None` means "run now".
    #[serde(default, rename = "logicalDate")]
    logical_date: Option<Value>,
}

fn empty_object() -> Value {
    json!({})
}

/// What one status check found. `Pending` means keep waiting.
enum RunState {
    Pending,
    Succeeded(Value),
    Failed(Value),
}

/// How monitoring ended. Distinct from [`RunState`] because reaching the
/// promise deadline is an outcome of the loop, not a state Airflow reports.
///
/// Carries the finished run summary rather than the raw Airflow record, so
/// settling needs nothing but this.
///
/// There is no `Failed`: a run that finished in a failure state is a permanent
/// error like any other, and folding it into [`AirflowError`] is what lets the
/// settle match have one arm per *outcome* rather than one per variant.
#[derive(Debug)]
enum Monitored {
    Succeeded { run: Value, output: Value },
    DeadlineReached,
}

/// What the work decided the promise should become.
///
/// `None` leaves it alone — the server settles a timed-out promise itself, and
/// a transient failure must be left for redelivery to retry. This is the only
/// vocabulary `run` needs: it settles, it does not interpret.
type Settlement = Option<(&'static str, Value)>;

/// A downstream failure, classified. The classification decides whether the
/// promise is settled or the task is dropped for redelivery.
#[derive(Debug)]
enum AirflowError {
    /// Can never succeed: reject the promise now. `run` is the downstream run
    /// summary when there is one, and empty when the failure happened before a
    /// run existed.
    Permanent {
        kind: &'static str,
        message: String,
        run: Value,
    },
    /// Might succeed later, or the outcome is unknown. Drop the task without
    /// settling; the lease expires and the message is redelivered. Safe
    /// because create is idempotent.
    // Read through `Debug` when an unsettled outcome is logged, which
    // `dead_code` cannot see.
    #[allow(dead_code)]
    Transient(String),
}

impl AirflowError {
    fn permanent(kind: &'static str, message: impl Into<String>) -> Self {
        AirflowError::Permanent {
            kind,
            message: message.into(),
            run: json!({}),
        }
    }

    /// A run that reached a failure state — a permanent error that happens to
    /// know which run it was.
    fn failed(run: Value, message: impl Into<String>) -> Self {
        AirflowError::Permanent {
            kind: "downstream_failed",
            message: message.into(),
            run,
        }
    }
    fn transient(message: impl Into<String>) -> Self {
        AirflowError::Transient(message.into())
    }
}

impl RunContext {
    /// The protocol frame: claim the task, do the work, settle the promise.
    ///
    /// Nothing here is Airflow-specific. It never sees a DAG run, an error kind
    /// or a value schema — `execute` decides all of that and hands back a
    /// [`Settlement`], and this body's whole job is to apply it.
    async fn run(self) {
        // ── 1. Claim the task ────────────────────────────────────────────────
        //
        // Nothing may happen on behalf of a task this worker does not own — and
        // nothing can be *reported* until it does. Before the claim the only
        // outcome available is `Err(Unavailable)`, which the dispatch loop logs
        // and drops; after it, every failure can settle the promise. So the
        // claim comes first and validation comes after.
        //
        // Anything that is not "here is the task" — a 409 race, a transient
        // error, an unreachable server — means this attempt does not run.
        // Redelivery brings us back; there is nothing to decide between them.
        let Ok(claimed) = self
            .worker
            .server
            .process(&RequestEnvelope {
                kind: "task.acquire".to_string(),
                head: RequestHead {
                    corr_id: format!("airflow-{}", fastrand::u64(..)),
                    version: PROTOCOL_VERSION.to_string(),
                    // In process: there is no caller to authenticate.
                    auth: None,
                    debug_time: None,
                },
                data: json!({
                    "id": self.task.id,
                    "version": self.task.version,   // the fencing token from `execute`
                    "pid": PID,
                    "ttl": self.worker.lease_timeout,
                }),
            })
            .await
        else {
            return;
        };
        if claimed.head.status != 200 {
            tracing::debug!(task_id = %self.task.id, status = claimed.head.status, "airflow: task not acquired");
            return;
        }
        // The one case that should never happen: the server answered 200 and
        // the payload is not a task.
        let acquired: TaskAcquireResponseData = match serde_json::from_value(claimed.data) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(task_id = %self.task.id, error = %e, "airflow: malformed acquire response");
                return;
            }
        };
        let version = acquired.task.version; // the RESPONSE version (n+1), from here on
        let promise = acquired.promise; // param, timeoutAt, createdAt, tags

        // ── 2. Do the work, and settle with whatever it decided ──────────────
        let Some((state, value)) = self.execute(&promise, version).await else {
            return;
        };

        let settled = self
            .worker
            .server
            .process(&RequestEnvelope {
                kind: "task.fulfill".to_string(),
                head: RequestHead {
                    corr_id: format!("airflow-{}", fastrand::u64(..)),
                    version: PROTOCOL_VERSION.to_string(),
                    auth: None,
                    debug_time: None,
                },
                data: json!({
                    "id": self.task.id,
                    "version": version,
                    "action": {
                        "kind": "promise.settle",
                        "head": {},
                        "data": {
                            "id": self.task.id,   // must equal the task id
                            "state": state,
                            "value": {
                                "headers": { "content-type": "application/json" },
                                "data": b64_encode(&value.to_string())
                            }
                        }
                    }
                }),
            })
            .await;

        match settled {
            Ok(r) if (200..300).contains(&r.head.status) => {
                tracing::info!(task_id = %self.task.id, state, "airflow: promise settled");
            }
            // A 409 means the lease was lost or the promise already settled —
            // almost always a timeout. Nothing here is retryable either way.
            other => {
                tracing::warn!(task_id = %self.task.id, state, ?other, "airflow: promise not settled")
            }
        }
    }

    /// Do the work, and decide what the promise becomes.
    ///
    /// This is where the integration's error policy lives: three outcomes —
    /// resolve, reject, leave alone — and the mapping from what happened to
    /// which one. `run` above only applies the answer.
    async fn execute(&self, promise: &PromiseRecord, version: i64) -> Settlement {
        match self.work(promise, version).await {
            Ok(Monitored::Succeeded { run, output }) => {
                Some(("resolved", json!({ "run": run, "output": output })))
            }
            Err(AirflowError::Permanent { kind, message, run }) => {
                tracing::warn!(task_id = %self.task.id, kind, %message, "airflow: rejecting");
                Some((
                    "rejected",
                    json!({ "run": run, "error": { "kind": kind, "message": message } }),
                ))
            }
            // Nothing to settle: the server settles a timed-out promise itself,
            // and a transient failure must be left for redelivery to retry.
            other => {
                tracing::warn!(task_id = %self.task.id, ?other, "airflow: promise left unsettled");
                None
            }
        }
    }

    /// Resolve, create, monitor. One error channel, so `?` does the work.
    ///
    /// The two ways resolution fails are not the same failure: a malformed
    /// address is the caller's error and can never become valid, because
    /// promise tags are immutable, so it rejects the promise; an unconfigured
    /// deployment is the operator's error that a rollout fixes, so it retries.
    async fn work(&self, promise: &PromiseRecord, version: i64) -> Result<Monitored, AirflowError> {
        // The address comes off the promise, not off the message: the promise
        // is the durable record, and it is where every other input already
        // comes from. A promise that has a task always carries this tag — that
        // tag is what caused the task to exist.
        let address = promise.tags.get(TARGET_TAG).ok_or_else(|| {
            AirflowError::permanent("invalid_request", format!("no {TARGET_TAG} tag"))
        })?;
        let name = parse_address(address).map_err(|e| {
            AirflowError::permanent("invalid_request", format!("bad address '{address}': {e}"))
        })?;
        let deployment =
            self.worker.deployments.get(&name).cloned().ok_or_else(|| {
                AirflowError::transient(format!("no deployment '{name}' configured"))
            })?;
        let target = Target { name, deployment };

        let request = decode_param(promise.param.data.as_deref())?;
        let run_id = derive_run_id(&promise.id);

        // No `?` past this point: the heartbeat below has to be aborted, and an
        // early return would leave it beating for a lease nobody holds.
        let heartbeat = {
            // The lease clock. Its own task, on a cadence derived from the lease
            // TTL and nothing else — which is what lets the downstream clock
            // back off past the lease without the lease lapsing.
            let server = Arc::clone(&self.worker.server);
            let task_id = self.task.id.clone();
            let beat_ms = heartbeat_interval_ms(self.worker.lease_timeout);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(beat_ms));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    // Deliberately ignored. `task.heartbeat` answers 200 whether
                    // or not it refreshed anything — a heartbeat for a lease this
                    // worker no longer holds is a silent no-op — so the response
                    // carries no signal. Losing the lease surfaces at
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
                                "pid": PID,
                                "tasks": [{ "id": task_id, "version": version }]
                            }),
                        })
                        .await;
                }
            })
        };

        // Start the run. Idempotent by construction: this line runs on every
        // delivery, and the duplicate is the recovery path.
        let outcome = match self.start(&target, &request, &run_id).await {
            Err(e) => Err(e),
            Ok(()) => {
                self.poll_until_done(&target, promise, &request, &run_id)
                    .await
            }
        };
        heartbeat.abort();
        outcome
    }

    /// Phases 1 and 2. Returns the terminal run, or `Pending` if the promise
    /// deadline arrived first.
    /// Watch the run on the downstream clock until it reaches a terminal state
    /// or the promise deadline arrives.
    async fn poll_until_done(
        &self,
        target: &Target,
        promise: &PromiseRecord,
        request: &TriggerRequest,
        run_id: &str,
    ) -> Result<Monitored, AirflowError> {
        // The downstream clock. Sized for Airflow's cost
        // and latency, and deliberately unrelated to the lease TTL: the
        // heartbeat task holds the lease open independently, so this interval
        // may back off well past it.
        let mut interval = self.worker.poll_interval;
        loop {
            let now = crate::util::system_time_ms();
            if now >= promise.timeout_at {
                return Ok(Monitored::DeadlineReached);
            }
            match self.check(target, &request.dag, run_id).await? {
                RunState::Pending => {}
                RunState::Succeeded(run) => {
                    return Ok(Monitored::Succeeded {
                        output: json!({
                            "runType": run.get("run_type").cloned().unwrap_or(Value::Null),
                            "conf": run.get("conf").cloned().unwrap_or_else(|| json!({})),
                            "note": run.get("note").cloned().unwrap_or(Value::Null),
                            "logicalDate": run.get("logical_date").cloned().unwrap_or(Value::Null),
                        }),
                        run: self.run_summary(target, &request.dag, &run),
                    })
                }
                RunState::Failed(run) => {
                    let message = format!(
                        "DAG run finished in state {}",
                        run.get("state")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                    );
                    return Err(AirflowError::failed(
                        self.run_summary(target, &request.dag, &run),
                        message,
                    ));
                }
            }
            // Never sleep past the promise deadline: the next iteration has to
            // observe it and stop rather than wake up after the server has
            // already settled the promise.
            let sleep_ms = interval.min(promise.timeout_at - now).max(0) as u64;
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            interval = interval
                .saturating_mul(2)
                .min(self.worker.max_poll_interval);
        }
    }

    /// Start the DAG run, treating "already exists" as success.
    ///
    /// The canonical `start`: idempotent, keyed by `run_id`, and a duplicate is
    /// success rather than an error.
    async fn start(
        &self,
        target: &Target,
        request: &TriggerRequest,
        run_id: &str,
    ) -> Result<(), AirflowError> {
        let mut body = json!({
            "dag_run_id": run_id,
            "conf": request.conf,
        });
        if let Some(note) = &request.note {
            body["note"] = json!(note);
        }
        // Airflow 3 requires the key to be present (null means "run now");
        // Airflow 2 defaults it when omitted.
        if target.deployment.api_version != "v1" || request.logical_date.is_some() {
            body["logical_date"] = request.logical_date.clone().unwrap_or(Value::Null);
        }

        let url = format!(
            "{}/dags/{}/dagRuns",
            target.deployment.api_base(),
            urlencode(&request.dag)
        );
        let (status, payload) = self
            .request(target, reqwest::Method::POST, &url, Some(body))
            .await?;

        let dag = &request.dag;
        match status {
            // 409 is the recovery path, not an error: an earlier delivery
            // already triggered this run, so re-attach and monitor it.
            200 | 201 | 409 => {
                tracing::info!(task_id = %self.task.id, dag_id = %dag, run_id, status, "airflow: DAG run triggered or re-attached");
                Ok(())
            }
            404 => Err(AirflowError::permanent(
                "not_found",
                format!("no DAG '{dag}'"),
            )),
            401 | 403 => Err(AirflowError::permanent(
                "unauthorized",
                format!("credentials rejected by '{}'", target.name),
            )),
            // Every other 4xx is the request's fault and retrying cannot fix it.
            400..=499 => Err(AirflowError::permanent(
                "invalid_request",
                format!("trigger rejected: {payload}"),
            )),
            _ => Err(AirflowError::transient(format!(
                "trigger '{dag}': HTTP {status}: {payload}"
            ))),
        }
    }

    /// One status check. The canonical `check`.
    async fn check(
        &self,
        target: &Target,
        dag: &str,
        run_id: &str,
    ) -> Result<RunState, AirflowError> {
        let url = format!(
            "{}/dags/{}/dagRuns/{}",
            target.deployment.api_base(),
            urlencode(dag),
            urlencode(run_id)
        );
        let (status, payload) = self
            .request(target, reqwest::Method::GET, &url, None)
            .await?;

        match status {
            200 => Ok(classify_run(payload, run_id)),
            // The run is gone — cleared or deleted. Re-triggering would break
            // the one-promise-one-run contract, and waiting will not bring it
            // back.
            404 => Err(AirflowError::permanent(
                "not_found",
                format!("DAG run '{run_id}' no longer exists"),
            )),
            401 | 403 => Err(AirflowError::permanent(
                "unauthorized",
                format!("credentials rejected by '{}'", target.name),
            )),
            _ => Err(AirflowError::transient(format!(
                "get run '{run_id}': HTTP {status}: {payload}"
            ))),
        }
    }

    /// One authenticated request, with a token refresh on a mid-flight 401.
    async fn request(
        &self,
        target: &Target,
        method: reqwest::Method,
        url: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), AirflowError> {
        let mut req = self.worker.client.request(method.clone(), url);
        req = target.deployment.authenticate(req);
        if let Some(ref b) = body {
            req = req.json(b);
        }
        let resp = req.send().await.map_err(|e| {
            // Connection refused before the first byte and a timeout after the
            // last are indistinguishable here, so the request may or may not
            // have been applied. Transient is the only safe classification —
            // and it is safe because create is idempotent.
            AirflowError::transient(format!("{method} {url}: {e}"))
        })?;
        let status = resp.status().as_u16();
        let payload = resp.json::<Value>().await.unwrap_or(Value::Null);
        Ok((status, payload))
    }

    fn run_summary(&self, target: &Target, dag: &str, run: &Value) -> Value {
        let run_id = run
            .get("dag_run_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        json!({
            "id": run_id,
            "state": run.get("state").cloned().unwrap_or(Value::Null),
            "startedAt": run.get("start_date").cloned().unwrap_or(Value::Null),
            "endedAt": run.get("end_date").cloned().unwrap_or(Value::Null),
            "url": target.deployment.run_url(dag, run_id),
        })
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

/// Decode and validate `param.data` into a [`TriggerRequest`].
///
/// One shape, and only one. There is no SDK-envelope unwrapping: `{func, args,
/// version}` is the SDK's convention for SDK functions, and an integration is
/// not one. Accepting both would mean two shapes to document and test, and an
/// ambiguity — a legitimate request with a `func` field would be silently
/// misread as an envelope.
///
/// Every failure here is permanent. A promise's param is immutable, so a
/// request that is malformed now is malformed on every redelivery; the message
/// names what was wrong, because the promise value is the only channel back to
/// whoever sent it.
fn decode_param(data: Option<&str>) -> Result<TriggerRequest, AirflowError> {
    let bad = |what: &str| AirflowError::permanent("invalid_request", what.to_string());

    let encoded = data
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad("param is required: at least { \"dag\": \"...\" }"))?;
    let bytes = b64_decode(encoded).ok_or_else(|| bad("param.data is not base64"))?;
    let text = String::from_utf8(bytes).map_err(|_| bad("param.data is not UTF-8"))?;

    let request: TriggerRequest = serde_json::from_str(&text)
        .map_err(|e| AirflowError::permanent("invalid_request", format!("param.data: {e}")))?;
    if request.dag.trim().is_empty() {
        return Err(bad("dag must not be empty"));
    }
    Ok(request)
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
    fn address_is_just_a_deployment() {
        assert_eq!(parse_address("airflow://prod").unwrap(), "prod");
    }

    #[test]
    fn address_keeps_the_authority_verbatim() {
        // A deployment name is a config key, not a hostname: no lowercasing,
        // and a port-looking suffix is part of the name.
        assert_eq!(
            parse_address("airflow://Airflow.Internal:8080").unwrap(),
            "Airflow.Internal:8080"
        );
    }

    #[test]
    fn address_rejects_a_path() {
        // The old form put the DAG here. Rejecting it makes the move loud
        // instead of silently triggering the wrong thing.
        let err = parse_address("airflow://prod/dags/etl_daily").unwrap_err();
        assert!(err.contains("takes no path"), "{err}");
    }

    #[test]
    fn address_rejects_wrong_shape() {
        for addr in ["airflow://", "airflow:///", "bash://prod", "not a url"] {
            assert!(parse_address(addr).is_err(), "expected rejection: {addr}");
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

    fn param(v: Value) -> Option<String> {
        Some(b64_encode(&v.to_string()))
    }

    #[test]
    fn param_needs_only_a_dag() {
        let got = decode_param(param(json!({"dag": "etl_daily"})).as_deref()).unwrap();
        assert_eq!(got.dag, "etl_daily");
        assert_eq!(got.conf, json!({}));
        assert!(got.note.is_none() && got.logical_date.is_none());
    }

    #[test]
    fn param_carries_the_optional_fields() {
        let got = decode_param(
            param(json!({"dag": "d", "conf": {"a": 1}, "note": "n", "logicalDate": null}))
                .as_deref(),
        )
        .unwrap();
        assert_eq!(got.conf, json!({"a": 1}));
        assert_eq!(got.note.as_deref(), Some("n"));
    }

    #[test]
    fn param_is_required() {
        for data in [None, Some("")] {
            match decode_param(data) {
                Err(AirflowError::Permanent { kind, .. }) => assert_eq!(kind, "invalid_request"),
                other => panic!("expected a permanent error, got {other:?}"),
            }
        }
    }

    #[test]
    fn param_rejects_a_missing_or_empty_dag() {
        for v in [json!({}), json!({"dag": ""}), json!({"dag": "   "})] {
            assert!(
                decode_param(param(v.clone()).as_deref()).is_err(),
                "expected rejection: {v}"
            );
        }
    }

    #[test]
    fn param_rejects_unknown_fields() {
        // A typo is a rejection naming the field, not a silently dropped
        // setting.
        let err = decode_param(param(json!({"dag": "d", "conff": {}})).as_deref()).unwrap_err();
        match err {
            AirflowError::Permanent { message, .. } => {
                assert!(message.contains("conff"), "{message}")
            }
            other => panic!("expected a permanent error, got {other:?}"),
        }
    }

    #[test]
    fn param_does_not_unwrap_the_sdk_envelope() {
        // `{func, args, version}` is not this integration's schema, and a
        // request that happens to have a `func` field must not be re-read as
        // one.
        let envelope = json!({"func": "trigger", "args": [{"dag": "d"}], "version": 1});
        assert!(decode_param(param(envelope).as_deref()).is_err());
    }

    #[test]
    fn malformed_param_is_permanent_not_transient() {
        for data in [
            "!!!not base64!!!",
            &b64_encode("not json"),
            &b64_encode("[1,2,3]"),
        ] {
            match decode_param(Some(data)) {
                Err(AirflowError::Permanent { kind, .. }) => assert_eq!(kind, "invalid_request"),
                other => panic!("expected a permanent error for {data}, got {other:?}"),
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
