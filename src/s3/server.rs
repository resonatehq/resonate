//! `ResonateServer` over S3.
//!
//! # Contract
//!
//! The adapter between the protocol and the shell: resolve `now`, deserialize,
//! validate, route. It answers the same statuses `Server::dispatch` does
//! (`server.rs:424-500`) because it is the same decision tree — only the state
//! behind it is different. The differential suite holds the two byte-equal.
//!
//! Two routing notes worth stating outright:
//!
//! - **Reads go through the applier too.** Reading a promise whose deadline has
//!   passed must settle it, which is a real write; and a read that changes
//!   nothing costs no S3 operations at all on a cache hit, by the write law. So
//!   there is nothing to gain from a separate read path and a correctness bug
//!   waiting in one.
//! - **Every operation is single-origin, `task.fence` included.** Its validator
//!   requires the fenced action's id to share the task's origin, so one
//!   document always covers the check and the action and the fence is atomic.
//!   Without that rule the fence would need a two-object commit, which the
//!   single-document design deliberately does not have.
//!
//! # Dependencies
//!
//! [`S3Server::build`] wires the whole backend — store, cache, outbox,
//! applier, schedule service, timer poller, scan service — and it is the one
//! constructor for production and for tests, so the differential suite
//! exercises the graph `main` builds rather than a lookalike.
//!
//! # Dependants
//!
//! `main`'s backend selection, the differential suite, and the live-bucket
//! smoke tests, all through the `ResonateServer` port.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::Value;
use validator::Validate;

use crate::core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRegisterCallbackData,
    PromiseRegisterListenerData, PromiseSearchData, PromiseSettleData, RequestEnvelope,
    ResponseEnvelope, ScheduleCreateData, ScheduleDeleteData, ScheduleGetData, ScheduleSearchData,
    TaskAcquireData, TaskContinueData, TaskCreateData, TaskFenceData, TaskFulfillData, TaskGetData,
    TaskHaltData, TaskHeartbeatData, TaskReleaseData, TaskSearchData, TaskSuspendData,
};
use crate::core::{ResonateRouter, ResonateServer, Unavailable};
use crate::kernel::state::{KernelCfg, Reply, Req};
use crate::util;

use super::applier::{ApplierCfg, ApplierPool, KeySpace};
use super::cache::{DocCache, MemDocCache};
use super::outbox::Outbox;
use super::scan::ScanService;
use super::schedules::ScheduleService;
use super::store::Store;
use super::timer_queue::TimerQueue;
use super::timerd::{ScheduleFirer, Timerd, TimerdCfg};

/// The origin a promise or task id belongs to: everything before the first
/// `':'`. This is the routing key, and the reason one document can answer any
/// single operation.
fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

/// Everything needed to stand up the backend.
#[derive(Debug, Clone)]
pub struct S3ServerCfg {
    pub keys: KeySpace,
    pub applier: ApplierCfg,
    pub timerd: TimerdCfg,
    pub cache_capacity: usize,
    /// Whether `debug.*` operations and `resonate:debug_time` are honoured.
    pub debug: bool,
    /// Whether the search operations are answered. Each search reads every
    /// document in the store, so this is opt-in.
    pub search: bool,
    /// Advertised in every `execute` message.
    pub server_url: String,
}

impl Default for S3ServerCfg {
    fn default() -> Self {
        Self {
            keys: KeySpace::new("", 4),
            applier: ApplierCfg::default(),
            timerd: TimerdCfg::default(),
            cache_capacity: 10_000,
            debug: false,
            search: false,
            server_url: String::new(),
        }
    }
}

pub struct S3Server {
    applier: Arc<ApplierPool>,
    timerd: Arc<Timerd>,
    scan: ScanService,
    schedules: Arc<ScheduleService>,
    outbox: Arc<Outbox>,
    store: Arc<dyn Store>,
    keys: KeySpace,
    debug: bool,
    search: bool,
    /// Set by `debug.start`. Shared with the timer poller, which is the point:
    /// with the loop paused, `debug.tick` is the only thing that moves time.
    debug_mode: Arc<AtomicBool>,
}

impl S3Server {
    /// Wire the whole backend over `store`.
    ///
    /// One constructor for production and for tests, so the differential suite
    /// exercises the same graph `main` builds.
    pub fn build(
        store: Arc<dyn Store>,
        router: Option<Arc<dyn ResonateRouter>>,
        cfg: S3ServerCfg,
    ) -> Arc<Self> {
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(cfg.cache_capacity));
        let outbox = Arc::new(Outbox::new(router, cfg.server_url.clone()));
        let timers = Arc::new(TimerQueue::new());
        let applier = Arc::new(ApplierPool::new(
            Arc::clone(&store),
            Arc::clone(&cache),
            Arc::clone(&outbox),
            Arc::clone(&timers),
            cfg.keys.clone(),
            cfg.applier.clone(),
        ));
        let schedules = Arc::new(ScheduleService::new(
            Arc::clone(&store),
            Arc::clone(&applier),
            Arc::clone(&timers),
            cfg.keys.clone(),
        ));
        let timerd = Arc::new(Timerd::new(
            Arc::clone(&store),
            Arc::clone(&applier),
            Some(Arc::clone(&schedules) as Arc<dyn ScheduleFirer>),
            Arc::clone(&timers),
            cfg.keys.clone(),
            cfg.timerd.clone(),
        ));
        let scan = ScanService::new(
            Arc::clone(&store),
            cache,
            Arc::clone(&schedules),
            Arc::clone(&outbox),
            cfg.keys.clone(),
        );
        Arc::new(Self {
            applier,
            timerd,
            scan,
            schedules,
            outbox,
            store,
            keys: cfg.keys,
            debug: cfg.debug,
            search: cfg.search,
            debug_mode: Arc::new(AtomicBool::new(false)),
        })
    }

    /// A ready-to-drive backend over an in-process store. Tests and the
    /// differential suite.
    pub fn in_memory(cfg: S3ServerCfg) -> Arc<Self> {
        Self::build(
            Arc::new(super::store::ObjectStoreAdapter::in_memory()),
            None,
            cfg,
        )
    }

    pub fn timerd(&self) -> &Arc<Timerd> {
        &self.timerd
    }

    /// Whether the store answers at all — what `/ready` reports.
    pub async fn store_reachable(&self) -> bool {
        match self.store.list(&self.keys.doc_prefix(), 1).await {
            Ok(_) => true,
            Err(e) => {
                tracing::error!(error = %e, "Readiness check failed: object store unavailable");
                false
            }
        }
    }

    /// The flag the timer poller watches. `debug.start` sets it.
    pub fn debug_mode(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.debug_mode)
    }

    pub fn kernel_cfg(&self) -> KernelCfg {
        self.applier.cfg_kernel()
    }

    async fn dispatch(&self, req: &RequestEnvelope, now: i64) -> Result<Reply, Unavailable> {
        let data = &req.data;
        match req.kind.as_str() {
            // Gated first: every search reads the whole store, so a deployment
            // that turned them off must refuse them before anything is listed.
            "promise.search" | "task.search" | "schedule.search" if !self.search => {
                Ok(Reply::err(403, "Search operations are disabled"))
            }

            // --- promises ---------------------------------------------------
            "promise.get" => {
                let r: PromiseGetData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::PromiseGet(r), now).await
            }
            "promise.create" => {
                let r: PromiseCreateData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier
                    .submit(&origin, Req::PromiseCreate(r), now)
                    .await
            }
            "promise.settle" => {
                let r: PromiseSettleData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier
                    .submit(&origin, Req::PromiseSettle(r), now)
                    .await
            }
            "promise.register_callback" => {
                let r: PromiseRegisterCallbackData = parsed!(data);
                let origin = origin_of(&r.awaiter).to_string();
                self.applier
                    .submit(&origin, Req::PromiseRegisterCallback(r), now)
                    .await
            }
            "promise.register_listener" => {
                let r: PromiseRegisterListenerData = parsed!(data);
                let origin = origin_of(&r.awaited).to_string();
                self.applier
                    .submit(&origin, Req::PromiseRegisterListener(r), now)
                    .await
            }
            "promise.search" => {
                let r: PromiseSearchData = parsed!(data);
                self.scan.search_promises(&r).await
            }

            // --- tasks ------------------------------------------------------
            "task.get" => {
                let r: TaskGetData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::TaskGet(r), now).await
            }
            "task.create" => {
                let r: TaskCreateData = parsed!(data);
                let origin = origin_of(&r.action.data.id).to_string();
                self.applier.submit(&origin, Req::TaskCreate(r), now).await
            }
            "task.acquire" => {
                let r: TaskAcquireData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::TaskAcquire(r), now).await
            }
            "task.release" => {
                let r: TaskReleaseData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::TaskRelease(r), now).await
            }
            "task.fulfill" => {
                let r: TaskFulfillData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::TaskFulfill(r), now).await
            }
            "task.suspend" => {
                let r: TaskSuspendData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::TaskSuspend(r), now).await
            }
            "task.fence" => {
                // The validator requires the action's id (when it has one) to
                // share the task's origin, so one document covers the check and
                // the action and the fence is atomic.
                let r: TaskFenceData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier
                    .submit(
                        &origin,
                        Req::TaskFence {
                            data: r,
                            corr_id: req.head.corr_id.clone(),
                        },
                        now,
                    )
                    .await
            }
            "task.heartbeat" => {
                let r: TaskHeartbeatData = parsed!(data);
                // The validator requires a non-empty batch sharing one origin.
                let origin = origin_of(&r.tasks[0].id).to_string();
                self.applier
                    .submit(&origin, Req::TaskHeartbeat(r), now)
                    .await
            }
            "task.halt" => {
                let r: TaskHaltData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier.submit(&origin, Req::TaskHalt(r), now).await
            }
            "task.continue" => {
                let r: TaskContinueData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.applier
                    .submit(&origin, Req::TaskContinue(r), now)
                    .await
            }
            "task.search" => {
                let r: TaskSearchData = parsed!(data);
                self.scan.search_tasks(&r).await
            }

            // --- schedules --------------------------------------------------
            "schedule.get" => {
                let r: ScheduleGetData = parsed!(data);
                self.schedules.get(&r.id).await
            }
            "schedule.create" => {
                let r: ScheduleCreateData = parsed!(data);
                self.schedules.create(&r, now).await
            }
            "schedule.delete" => {
                let r: ScheduleDeleteData = parsed!(data);
                self.schedules.delete(&r.id).await
            }
            "schedule.search" => {
                let r: ScheduleSearchData = parsed!(data);
                self.scan.search_schedules(&r).await
            }

            // --- debug ------------------------------------------------------
            "debug.start" | "debug.stop" | "debug.reset" | "debug.snap" | "debug.tick"
                if !self.debug =>
            {
                Ok(Reply::err(403, "Debug operations are disabled"))
            }
            "debug.start" => {
                self.debug_mode.store(true, Ordering::SeqCst);
                self.outbox.set_paused(true);
                tracing::info!("Debug mode started — background loops paused");
                Ok(Reply::status(200, Value::Object(serde_json::Map::new())))
            }
            "debug.stop" => {
                self.debug_mode.store(false, Ordering::SeqCst);
                self.outbox.set_paused(false);
                // Whatever queued up while paused goes now, rather than
                // waiting for the next message to push it along.
                self.outbox.deliver().await;
                tracing::info!("Debug mode stopped — background loops resumed");
                Ok(Reply::status(200, Value::Object(serde_json::Map::new())))
            }
            "debug.reset" => self.reset().await,
            "debug.snap" => {
                let snapshot = self.scan.snapshot().await?;
                Ok(Reply::status(
                    200,
                    serde_json::to_value(snapshot).expect("a snapshot serializes"),
                ))
            }
            "debug.tick" => self.tick(req, now).await,

            other => Ok(Reply::err(400, &format!("Unknown operation: {other}"))),
        }
    }

    /// Delete everything, then forget everything that referred to it.
    async fn reset(&self) -> Result<Reply, Unavailable> {
        for prefix in [
            self.keys.doc_prefix(),
            self.keys.sched_prefix(),
            self.keys.timer_prefix(),
        ] {
            self.store
                .delete_prefix(&prefix)
                .await
                .map_err(|e| Unavailable::new(e.to_string()))?;
        }
        // Order matters: the objects are gone, so anything still holding a
        // document or a queued message is holding a ghost.
        self.applier.reset().await;
        self.outbox.clear();
        tracing::warn!("Debug reset: all data cleared");
        Ok(Reply::status(200, Value::Object(serde_json::Map::new())))
    }

    /// `debug.tick`: sweep until nothing is due.
    ///
    /// The SQL backends' tick processes *every* expired timeout in one
    /// transaction, so a single capped round would not be equivalent. Rounds
    /// terminate because each one either fires something and re-arms it
    /// strictly later than `now`, or returns zero.
    async fn tick(&self, req: &RequestEnvelope, now: i64) -> Result<Reply, Unavailable> {
        let time = match req.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => return Ok(Reply::err(400, "Missing or invalid 'time' field")),
        };
        if let Some(debug_time) = req.head.debug_time {
            if debug_time != time {
                return Ok(Reply::err(400, "resonate:debug_time must equal data.time"));
            }
        }
        let _ = now;
        // A bound, not a limit: a round that fires nothing ends the loop, so
        // hitting this means something is re-arming into the past.
        const MAX_ROUNDS: usize = 10_000;
        for round in 0..MAX_ROUNDS {
            if self.timerd.round(time).await? == 0 {
                return Ok(Reply::status(200, Value::Array(vec![])));
            }
            debug_assert!(
                round + 1 < MAX_ROUNDS,
                "a tick that never settles means a deadline re-armed into the past"
            );
        }
        Err(Unavailable::new("tick did not converge"))
    }
}

/// Deserialize and validate `data`, or return the 400 the SQL handlers return.
///
/// A macro because each arm binds a different type and needs to `return` the
/// rejection from `dispatch`, which a generic function cannot do.
macro_rules! parsed {
    ($data:expr) => {
        match parse($data) {
            Ok(r) => r,
            Err(reply) => return Ok(reply),
        }
    };
}
use parsed;

fn parse<T: DeserializeOwned + Validate>(data: &Value) -> Result<T, Reply> {
    let parsed: T = serde_json::from_value(data.clone())
        .map_err(|e| Reply::err(400, &format!("Invalid request: {e}")))?;
    parsed
        .validate()
        .map_err(|e| Reply::err(400, &format_validation_errors(&e)))?;
    Ok(parsed)
}

#[async_trait]
impl ResonateServer for S3Server {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        // Debug-time overrides are gated by config, so a caller cannot move the
        // server's clock. The gate is here rather than at the HTTP edge so
        // every caller of the port is subject to it.
        let debug_time = if self.debug {
            req.head.debug_time
        } else {
            None
        };
        let now = util::resolve_time(debug_time);
        let reply = self.dispatch(req, now).await?;
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            reply.status,
            reply.data,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{RequestHead, SUPPORTED_VERSIONS};
    use serde_json::json;

    const W: &str = "http://worker:9999";
    const PID: &str = "pid-1";
    const TTL: i64 = 60_000;

    fn server(debug: bool) -> Arc<S3Server> {
        S3Server::in_memory(S3ServerCfg {
            keys: KeySpace::new("p", 4),
            debug,
            search: true,
            server_url: "http://server:8001".into(),
            ..Default::default()
        })
    }

    fn envelope(kind: &str, data: Value) -> RequestEnvelope {
        RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: "corr-1".into(),
                version: SUPPORTED_VERSIONS[0].into(),
                auth: None,
                debug_time: None,
            },
            data,
        }
    }

    async fn send(s: &Arc<S3Server>, kind: &str, data: Value, now: i64) -> ResponseEnvelope {
        let mut req = envelope(kind, data);
        req.head.debug_time = Some(now);
        s.process(&req).await.expect("in-process backend answers")
    }

    /// A debug server with the background loops paused, as the differential
    /// suite drives it.
    async fn started() -> Arc<S3Server> {
        let s = server(true);
        assert_eq!(send(&s, "debug.start", json!({}), 0).await.head.status, 200);
        s
    }

    #[tokio::test]
    async fn a_response_carries_the_kind_and_correlation_id_back() {
        let s = started().await;
        let resp = send(&s, "promise.get", json!({ "id": "o:a" }), 1_000).await;
        assert_eq!(resp.kind, "promise.get");
        assert_eq!(resp.head.corr_id, "corr-1");
        assert_eq!(resp.head.status, 404);
        assert_eq!(resp.head.version, crate::core::types::PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn an_unknown_operation_is_a_400() {
        let s = started().await;
        let resp = send(&s, "promise.frobnicate", json!({}), 0).await;
        assert_eq!(resp.head.status, 400);
        assert_eq!(resp.data, json!("Unknown operation: promise.frobnicate"));
    }

    #[tokio::test]
    async fn malformed_data_is_a_400() {
        let s = started().await;
        let resp = send(&s, "promise.get", json!({ "wrong": "shape" }), 0).await;
        assert_eq!(resp.head.status, 400);
        assert!(resp.data.as_str().unwrap().starts_with("Invalid request:"));
    }

    #[tokio::test]
    async fn a_validation_failure_is_a_400_with_the_validators_message() {
        let s = started().await;
        // Same origin required for a callback.
        let resp = send(
            &s,
            "promise.register_callback",
            json!({ "awaited": "other:a", "awaiter": "o:b" }),
            0,
        )
        .await;
        assert_eq!(resp.head.status, 400);
        assert_eq!(
            resp.data,
            json!("Awaiter and awaited must belong to the same origin")
        );
    }

    #[tokio::test]
    async fn the_whole_promise_lifecycle_answers_as_the_protocol_says() {
        let s = started().await;
        let created = send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 500_000, "param": {}, "tags": {} }),
            1_000,
        )
        .await;
        assert_eq!(created.head.status, 200);
        assert_eq!(created.data["promise"]["state"], "pending");

        let got = send(&s, "promise.get", json!({ "id": "o:a" }), 1_500).await;
        assert_eq!(got.data, created.data);

        let settled = send(
            &s,
            "promise.settle",
            json!({ "id": "o:a", "state": "resolved", "value": {} }),
            2_000,
        )
        .await;
        assert_eq!(settled.head.status, 200);
        assert_eq!(settled.data["promise"]["settledAt"], 2_000);
    }

    #[tokio::test]
    async fn a_task_is_claimed_and_fulfilled_through_the_port() {
        let s = started().await;
        let created = send(
            &s,
            "task.create",
            json!({ "pid": PID, "ttl": TTL, "action": {
                "kind": "promise.create", "head": {}, "data": {
                    "id": "o:t", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } } } }),
            1_000,
        )
        .await;
        assert_eq!(created.head.status, 200);
        assert_eq!(created.data["task"]["version"], 1);

        let fulfilled = send(
            &s,
            "task.fulfill",
            json!({ "id": "o:t", "version": 1, "action": {
                "kind": "promise.settle", "head": {}, "data": {
                    "id": "o:t", "state": "resolved", "value": {} } } }),
            2_000,
        )
        .await;
        assert_eq!(fulfilled.head.status, 200);
        assert_eq!(fulfilled.data["promise"]["state"], "resolved");
    }

    #[tokio::test]
    async fn a_fenced_action_carries_the_requests_correlation_id() {
        let s = started().await;
        send(
            &s,
            "task.create",
            json!({ "pid": PID, "ttl": TTL, "action": {
                "kind": "promise.create", "head": {}, "data": {
                    "id": "o:t", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } } } }),
            1_000,
        )
        .await;
        let fenced = send(
            &s,
            "task.fence",
            json!({ "id": "o:t", "version": 1, "action": {
                "kind": "promise.create", "head": {}, "data": {
                    "id": "o:child", "timeoutAt": 500_000, "param": {}, "tags": {} } } }),
            2_000,
        )
        .await;
        assert_eq!(fenced.head.status, 200);
        assert_eq!(fenced.data["action"]["head"]["corrId"], "corr-1");
        assert_eq!(fenced.data["action"]["head"]["status"], 200);
    }

    #[tokio::test]
    async fn a_suspend_that_cannot_wait_answers_300() {
        let s = started().await;
        send(
            &s,
            "task.create",
            json!({ "pid": PID, "ttl": TTL, "action": {
                "kind": "promise.create", "head": {}, "data": {
                    "id": "o:t", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } } } }),
            1_000,
        )
        .await;
        send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 500_000, "param": {}, "tags": {} }),
            1_000,
        )
        .await;
        send(
            &s,
            "promise.settle",
            json!({ "id": "o:a", "state": "resolved", "value": {} }),
            1_500,
        )
        .await;
        let resp = send(
            &s,
            "task.suspend",
            json!({ "id": "o:t", "version": 1, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": "o:a", "awaiter": "o:t" } }] }),
            2_000,
        )
        .await;
        assert_eq!(resp.head.status, 300);
        assert_eq!(resp.data["preload"], json!([]));
    }

    #[tokio::test]
    async fn operations_on_different_origins_reach_different_documents() {
        let s = started().await;
        for origin in ["alpha", "beta"] {
            send(
                &s,
                "promise.create",
                json!({ "id": format!("{origin}:a"), "timeoutAt": 500_000,
                        "param": {}, "tags": {} }),
                1_000,
            )
            .await;
        }
        let snap = send(&s, "debug.snap", json!({}), 1_000).await;
        assert_eq!(
            snap.data["promises"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["alpha:a", "beta:a"]
        );
    }

    // --- fencing across origins -------------------------------------------

    /// Claim `diff:t` and create a promise in a *different* origin, so a fence
    /// naming it would have to span two documents.
    async fn with_cross_origin_target(s: &Arc<S3Server>) {
        send(
            s,
            "task.create",
            json!({ "pid": PID, "ttl": TTL, "action": {
                "kind": "promise.create", "head": {}, "data": {
                    "id": "diff:t", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } } } }),
            1_000,
        )
        .await;
        send(
            s,
            "promise.create",
            json!({ "id": "elsewhere", "timeoutAt": 500_000, "param": {}, "tags": {} }),
            1_000,
        )
        .await;
    }

    #[tokio::test]
    async fn a_cross_origin_fence_is_a_400() {
        // The validator requires the fenced action to share the task's origin;
        // anything else would need a two-object commit this design does not
        // have. Both action kinds are refused the same way.
        let s = started().await;
        with_cross_origin_target(&s).await;
        for action in [
            json!({ "kind": "promise.settle", "head": {}, "data": {
                "id": "elsewhere", "state": "resolved", "value": {} } }),
            json!({ "kind": "promise.create", "head": {}, "data": {
                "id": "brand-new", "timeoutAt": 500_000, "param": {}, "tags": {} } }),
        ] {
            let resp = send(
                &s,
                "task.fence",
                json!({ "id": "diff:t", "version": 1, "action": action }),
                2_000,
            )
            .await;
            assert_eq!(resp.head.status, 400);
            assert_eq!(resp.data, json!("Action must belong to the task's origin"));
        }
        // Nothing moved in either document.
        let got = send(&s, "promise.get", json!({ "id": "elsewhere" }), 2_000).await;
        assert_eq!(got.data["promise"]["state"], "pending");
        assert_eq!(
            send(&s, "promise.get", json!({ "id": "brand-new" }), 2_000)
                .await
                .head
                .status,
            404
        );
    }

    #[tokio::test]
    async fn a_cross_origin_fence_is_refused_before_the_task_is_looked_up() {
        // Validation runs before any state is read, on every backend alike: a
        // cross-origin fence on an unknown task is the validator's 400, not a
        // 404.
        let s = started().await;
        let resp = send(
            &s,
            "task.fence",
            json!({ "id": "diff:missing", "version": 1, "action": {
                "kind": "promise.settle", "head": {}, "data": {
                    "id": "elsewhere", "state": "resolved", "value": {} } } }),
            2_000,
        )
        .await;
        assert_eq!(resp.head.status, 400);
        assert_eq!(resp.data, json!("Action must belong to the task's origin"));
    }

    // --- searches ---------------------------------------------------------

    #[tokio::test]
    async fn every_search_kind_is_routed() {
        let s = started().await;
        for kind in ["promise.search", "task.search", "schedule.search"] {
            let resp = send(&s, kind, json!({ "limit": 10 }), 0).await;
            assert_eq!(resp.head.status, 200, "{kind}");
        }
    }

    #[tokio::test]
    async fn searches_are_refused_by_default() {
        // `search` defaults to false: a deployment opts in.
        let s = S3Server::in_memory(S3ServerCfg {
            keys: KeySpace::new("p", 4),
            server_url: "http://server:8001".into(),
            ..Default::default()
        });
        for kind in ["promise.search", "task.search", "schedule.search"] {
            let resp = send(&s, kind, json!({ "limit": 10 }), 0).await;
            assert_eq!(resp.head.status, 403, "{kind}");
            assert_eq!(resp.data, json!("Search operations are disabled"));
        }
        // Everything that reads one document at a time still answers.
        let resp = send(&s, "promise.get", json!({ "id": "o:a" }), 0).await;
        assert_eq!(resp.head.status, 404);
    }

    #[tokio::test]
    async fn a_schedule_is_created_read_and_deleted() {
        let s = started().await;
        let data = json!({
            "id": "s0", "cron": "* * * * *", "promiseId": "p-{{.id}}-{{.timestamp}}",
            "promiseTimeout": 600_000, "promiseParam": {},
            "promiseTags": { "resonate:target": W }
        });
        let created = send(&s, "schedule.create", data, 1_000).await;
        assert_eq!(created.head.status, 200);
        let got = send(&s, "schedule.get", json!({ "id": "s0" }), 1_000).await;
        assert_eq!(got.data, created.data);
        assert_eq!(
            send(&s, "schedule.delete", json!({ "id": "s0" }), 1_000)
                .await
                .head
                .status,
            200
        );
        assert_eq!(
            send(&s, "schedule.get", json!({ "id": "s0" }), 1_000)
                .await
                .head
                .status,
            404
        );
    }

    // --- debug ------------------------------------------------------------

    #[tokio::test]
    async fn debug_operations_are_refused_when_debug_is_off() {
        let s = server(false);
        for kind in [
            "debug.start",
            "debug.stop",
            "debug.reset",
            "debug.snap",
            "debug.tick",
        ] {
            let resp = send(&s, kind, json!({ "time": 1 }), 0).await;
            assert_eq!(resp.head.status, 403, "{kind}");
            assert_eq!(resp.data, json!("Debug operations are disabled"));
        }
    }

    #[tokio::test]
    async fn debug_time_is_ignored_when_debug_is_off() {
        let s = server(false);
        // A caller cannot move the server's clock, so a promise with a deadline
        // in 1970 is born timed out however the header is set.
        let resp = send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 1, "param": {}, "tags": {} }),
            0,
        )
        .await;
        assert_eq!(resp.data["promise"]["state"], "rejected_timedout");
    }

    #[tokio::test]
    async fn a_tick_without_a_time_is_a_400() {
        let s = started().await;
        let resp = send(&s, "debug.tick", json!({}), 0).await;
        assert_eq!(resp.head.status, 400);
        assert_eq!(resp.data, json!("Missing or invalid 'time' field"));
    }

    #[tokio::test]
    async fn a_tick_whose_time_disagrees_with_the_header_is_a_400() {
        let s = started().await;
        let mut req = envelope("debug.tick", json!({ "time": 5 }));
        req.head.debug_time = Some(9);
        let resp = s.process(&req).await.unwrap();
        assert_eq!(resp.head.status, 400);
        assert_eq!(resp.data, json!("resonate:debug_time must equal data.time"));
    }

    #[tokio::test]
    async fn a_tick_sweeps_every_origin_that_is_due() {
        let s = started().await;
        for origin in ["alpha", "beta", "gamma"] {
            send(
                &s,
                "promise.create",
                json!({ "id": format!("{origin}:a"), "timeoutAt": 5_000,
                        "param": {}, "tags": { "resonate:target": W } }),
                1_000,
            )
            .await;
        }
        let resp = send(&s, "debug.tick", json!({ "time": 9_000 }), 9_000).await;
        assert_eq!(resp.head.status, 200);
        assert_eq!(resp.data, json!([]));

        let snap = send(&s, "debug.snap", json!({}), 9_000).await;
        for promise in snap.data["promises"].as_array().unwrap() {
            assert_eq!(promise["state"], "rejected_timedout");
        }
        assert!(snap.data["promiseTimeouts"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_tick_fires_a_due_schedule() {
        let s = started().await;
        let created = send(
            &s,
            "schedule.create",
            json!({
                "id": "s0", "cron": "* * * * *", "promiseId": "p-{{.timestamp}}",
                "promiseTimeout": 600_000, "promiseParam": {},
                "promiseTags": { "resonate:target": W }
            }),
            1_000,
        )
        .await;
        let due = created.data["schedule"]["nextRunAt"].as_i64().unwrap();
        send(&s, "debug.tick", json!({ "time": due }), due).await;

        let snap = send(&s, "debug.snap", json!({}), due).await;
        let ids: Vec<&str> = snap.data["promises"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec![format!("p-{due}").as_str()]);
    }

    #[tokio::test]
    async fn messages_queue_while_debug_is_started_and_appear_in_the_snapshot() {
        let s = started().await;
        send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } }),
            1_000,
        )
        .await;
        let snap = send(&s, "debug.snap", json!({}), 1_000).await;
        let messages = snap.data["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["address"], W);
        assert_eq!(messages[0]["message"]["kind"], "execute");
        assert_eq!(messages[0]["message"]["data"]["task"]["id"], "o:a");
    }

    #[tokio::test]
    async fn stopping_debug_delivers_what_queued_up() {
        let s = started().await;
        send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } }),
            1_000,
        )
        .await;
        assert_eq!(
            send(&s, "debug.snap", json!({}), 1_000).await.data["messages"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        send(&s, "debug.stop", json!({}), 1_000).await;
        assert!(
            send(&s, "debug.snap", json!({}), 1_000).await.data["messages"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_reset_clears_promises_schedules_timers_and_messages() {
        let s = started().await;
        send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } }),
            1_000,
        )
        .await;
        send(
            &s,
            "schedule.create",
            json!({
                "id": "s0", "cron": "* * * * *", "promiseId": "p-{{.id}}",
                "promiseTimeout": 600_000, "promiseParam": {},
                "promiseTags": { "resonate:target": W }
            }),
            1_000,
        )
        .await;
        assert_eq!(
            send(&s, "debug.reset", json!({}), 1_000).await.head.status,
            200
        );

        let snap = send(&s, "debug.snap", json!({}), 1_000).await;
        for key in [
            "promises",
            "tasks",
            "messages",
            "promiseTimeouts",
            "taskTimeouts",
            "callbacks",
            "listeners",
        ] {
            assert!(
                snap.data[key].as_array().unwrap().is_empty(),
                "{key} survived the reset"
            );
        }
        assert_eq!(
            send(&s, "schedule.search", json!({}), 1_000).await.data["schedules"],
            json!([])
        );
        // And a request after the reset does not see a cached ghost.
        assert_eq!(
            send(&s, "promise.get", json!({ "id": "o:a" }), 1_000)
                .await
                .head
                .status,
            404
        );
    }

    #[tokio::test]
    async fn a_reset_leaves_the_store_empty_under_the_prefix() {
        let s = started().await;
        send(
            &s,
            "promise.create",
            json!({ "id": "o:a", "timeoutAt": 500_000, "param": {},
                    "tags": { "resonate:target": W } }),
            1_000,
        )
        .await;
        send(&s, "debug.reset", json!({}), 1_000).await;
        assert!(s.store.list("p", 100).await.unwrap().is_empty());
    }
}
