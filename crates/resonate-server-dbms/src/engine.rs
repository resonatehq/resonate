//! The engine: every state transition a Resonate server can make.
//!
//! One method per protocol operation, each opening a transaction, applying the
//! transition, and shaping a response. The three SQL implementations behind
//! [`Storage`] differ in dialect, not in what a transition means — which is
//! what the differential, run in lock step against [`oracle`](crate::oracle),
//! exists to keep true.
//!
//! This is the layer `ResonateEngine` will name. Today it is concrete.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRegisterCallbackData,
    PromiseRegisterListenerData, PromiseResponseData, PromiseSearchData, PromiseSearchResponseData,
    PromiseSettleData, PromiseState, RequestEnvelope, ResponseEnvelope, ScheduleCreateData,
    ScheduleDeleteData, ScheduleGetData, ScheduleResponseData, ScheduleSearchData,
    ScheduleSearchResponseData, TaskAcquireData, TaskAcquireResponseData, TaskContinueData,
    TaskCreateData, TaskCreateResponseData, TaskFenceData, TaskFenceResponseData, TaskFulfillData,
    TaskFulfillResponseData, TaskGetData, TaskHaltData, TaskHeartbeatData, TaskRecord,
    TaskReleaseData, TaskResponseData, TaskSearchData, TaskSearchResponseData, TaskState,
    TaskSuspendData, TaskSuspendPreloadData,
};
use resonate_core::util;

use crate::Db;
use crate::StorageResult;
use serde_json::Value;
use validator::Validate;

use crate::{
    PromiseCreateParams, PromiseSettleParams, ScheduleCreateParams, Storage, StorageError,
    TaskAcquireParams, TaskCreateParams, TaskFenceCreateParams, TaskFenceSettleParams,
    TaskFulfillParams,
};

/// Durable state, and every transition over it.
///
/// The storage handle is private: everything callers need is a method here, so
/// that how a transition reaches the database stays this type's business. That
/// is what lets the internals change — which they are about to.
pub struct Engine {
    storage: Arc<Storage>,
    /// Whether `debug.*` operations are permitted at all.
    debug: bool,
    /// Set by `debug.start` / `debug.stop`; pauses the background loops so a
    /// test can drive the clock with `debug.tick`.
    debug_mode: AtomicBool,
}

impl Engine {
    pub fn new(storage: Arc<Storage>, debug: bool) -> Self {
        Self {
            storage,
            debug,
            debug_mode: AtomicBool::new(false),
        }
    }

    /// Is the engine paused? `debug.start` sets this, and the background loops
    /// honour it so a test can drive the clock with `debug.tick` instead of
    /// racing wall time.
    pub fn is_paused(&self) -> bool {
        self.debug_mode.load(Ordering::SeqCst)
    }

    /// Lightweight liveness probe.
    pub async fn ping(&self) -> StorageResult<()> {
        self.storage.query(|db| db.ping()).await
    }

    /// One tick of the timer wheel: the three timeout sweeps, then expired
    /// schedules. Returns how many schedules fired, for the caller to record.
    pub async fn tick(&self, now: i64) -> StorageResult<usize> {
        self.storage
            .transact(move |db| process_all_timeouts(db, now))
            .await
    }

    /// Claim a batch of outgoing messages.
    ///
    /// Temporary. Once a transition returns what it emitted, nothing is queued
    /// and this goes away with the outbox tables.
    pub async fn take_outgoing(
        &self,
        batch_size: i64,
    ) -> StorageResult<(Vec<crate::OutgoingExecute>, Vec<crate::OutgoingUnblock>)> {
        self.storage
            .transact(move |db| db.take_outgoing(batch_size))
            .await
    }

    pub async fn dispatch(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let kind = req.kind.as_str();

        match kind {
            // === Promise operations ===
            "promise.get" => self.op_promise_get(req, now).await,
            "promise.create" => self.op_promise_create(req, now).await,
            "promise.settle" => self.op_promise_settle(req, now).await,
            "promise.register_callback" => self.op_promise_register_callback(req, now).await,
            "promise.register_listener" => self.op_promise_register_listener(req, now).await,
            "promise.search" => self.op_promise_search(req, now).await,

            // === Task operations ===
            "task.get" => self.op_task_get(req, now).await,
            "task.create" => self.op_task_create(req, now).await,
            "task.acquire" => self.op_task_acquire(req, now).await,
            "task.release" => self.op_task_release(req, now).await,
            "task.fulfill" => self.op_task_fulfill(req, now).await,
            "task.suspend" => self.op_task_suspend(req, now).await,
            "task.fence" => self.op_task_fence(req, now).await,
            "task.heartbeat" => self.op_task_heartbeat(req, now).await,
            "task.halt" => self.op_task_halt(req, now).await,
            "task.continue" => self.op_task_continue(req, now).await,
            "task.search" => self.op_task_search(req, now).await,

            // === Schedule operations ===
            "schedule.get" => self.op_schedule_get(req, now).await,
            "schedule.create" => self.op_schedule_create(req, now).await,
            "schedule.delete" => self.op_schedule_delete(req).await,
            "schedule.search" => self.op_schedule_search(req).await,

            // === Debug operations ===
            "debug.start" | "debug.stop" | "debug.reset" | "debug.snap" | "debug.tick"
                if !self.debug =>
            {
                ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    403,
                    "Debug operations are disabled",
                )
            }
            "debug.start" => {
                self.debug_mode.store(true, Ordering::SeqCst);
                tracing::info!("Debug mode started — background loops paused");
                ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    200,
                    Value::Object(serde_json::Map::new()),
                )
            }
            "debug.stop" => {
                self.debug_mode.store(false, Ordering::SeqCst);
                tracing::info!("Debug mode stopped — background loops resumed");
                ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    200,
                    Value::Object(serde_json::Map::new()),
                )
            }
            "debug.reset" => self.op_debug_reset(req).await,
            "debug.snap" => self.op_debug_snap(req).await,
            "debug.tick" => self.op_debug_tick(req).await,

            _ => {
                tracing::warn!(kind = %kind, "Invalid request: unknown operation");
                ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    &format!("Unknown operation: {}", kind),
                )
            }
        }
    }

    // ============================================================================
    // Promise operations
    // ============================================================================

    async fn op_promise_get(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: PromiseGetData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                match db.promise_get(&r.id)? {
                    Some(promise) => {
                        tracing::debug!(
                            promise_id = %r.id,
                            state = %promise.state,
                            "Promise found"
                        );
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &PromiseResponseData { promise },
                        ))
                    }
                    None => {
                        tracing::debug!(promise_id = %r.id, "Promise not found");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Promise not found",
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_promise_create(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: PromiseCreateData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let address = r.tags.get("resonate:target").map(|s| s.as_str());
                if let Some(addr) = address {
                    if !resonate_core::is_valid_address(addr) {
                        tracing::warn!(
                            promise_id = %r.id,
                            address = addr,
                            "Promise create rejected: invalid resonate:target address"
                        );
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid resonate:target address",
                        ));
                    }
                }
                db.try_timeout(&[&r.id], now)?;
                let tags_json = serde_json::to_string(&r.tags).unwrap();
                let already_timedout = now >= r.timeout_at;
                let (state, created_at, settled_at) = if already_timedout {
                    let state = if r.tags.get("resonate:timer").map(|v| v.as_str()) == Some("true") {
                        tracing::debug!(promise_id = %r.id, "Promise created already timedout (timer: resolved immediately)");
                        PromiseState::Resolved
                    } else {
                        tracing::debug!(promise_id = %r.id, "Promise created already timedout");
                        PromiseState::RejectedTimedout
                    };
                    (state, r.timeout_at, Some(r.timeout_at))
                } else {
                    (PromiseState::Pending, now, None)
                };
                let param_headers_json = r
                    .param
                    .headers
                    .as_ref()
                    .map(|h| serde_json::to_string(h).unwrap());
                let result = db.promise_create(&PromiseCreateParams {
                    id: &r.id,
                    state: state.as_str(),
                    param_headers: param_headers_json.as_deref(),
                    param_data: r.param.data.as_deref(),
                    tags: &tags_json,
                    timeout_at: r.timeout_at,
                    created_at,
                    settled_at,
                    already_timedout,
                    address,
                })?;
                if result.was_created {
                    tracing::info!(
                        promise_id = %result.promise.id,
                        state = %result.promise.state,
                        timeout_at = result.promise.timeout_at,
                        target = address.unwrap_or("none"),
                        already_timedout = already_timedout,
                        "Promise created"
                    );
                } else {
                    tracing::debug!(
                        promise_id = %result.promise.id,
                        state = %result.promise.state,
                        "Promise create: already exists (idempotent)"
                    );
                }
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &PromiseResponseData { promise: result.promise },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(StorageError::InvalidInput(msg)) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Invalid request: {}", msg),
            ),
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_promise_settle(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: PromiseSettleData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                let value_headers_json = r
                    .value
                    .headers
                    .as_ref()
                    .map(|h| serde_json::to_string(h).unwrap());
                let result = db.promise_settle(&PromiseSettleParams {
                    id: &r.id,
                    state: r.state.as_str(),
                    value_headers: value_headers_json.as_deref(),
                    value_data: r.value.data.as_deref(),
                    settled_at: now,
                })?;
                match result.promise {
                    Some(promise) => {
                        assert_ne!(
                            promise.state,
                            PromiseState::Pending,
                            "invariant: returning 200 but promise is still pending"
                        );
                        if result.was_settled {
                            tracing::info!(
                                promise_id = %promise.id,
                                state = %promise.state,
                                "Promise settled"
                            );
                        } else {
                            tracing::debug!(
                                promise_id = %promise.id,
                                current_state = %promise.state,
                                requested_state = %r.state,
                                "Promise settle: already settled (idempotent)"
                            );
                        }
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &PromiseResponseData { promise },
                        ))
                    }
                    None => {
                        tracing::debug!(promise_id = %r.id, "Promise settle: promise not found");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Promise not found",
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_promise_register_callback(
        &self,
        req: &RequestEnvelope,
        now: i64,
    ) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: PromiseRegisterCallbackData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.awaited, &r.awaiter], now)?;
                let result = db.promise_register_callback(&r.awaited, &r.awaiter, now)?;
                let p_awaited = match result.awaited {
                    Some(p) => p,
                    None => {
                        tracing::debug!(promise_id = %r.awaited, "Callback registration: awaited promise not found");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Awaited promise not found",
                        ))
                    }
                };
                let p_awaiter = match result.awaiter {
                    Some(p) => p,
                    None => {
                        tracing::debug!(promise_id = %r.awaiter, "Callback registration: awaiter promise not found");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            422,
                            "Awaiter promise not found",
                        ))
                    }
                };
                if !p_awaiter.tags.contains_key("resonate:target") {
                    tracing::debug!(awaiter = %r.awaiter, "Callback registration rejected: awaiter has no resonate:target");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        422,
                        "Awaiter promise has no resonate:target tag",
                    ));
                }
                tracing::info!(
                    awaited = %r.awaited,
                    awaiter = %r.awaiter,
                    awaited_state = %p_awaited.state,
                    "Callback registered"
                );
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &PromiseResponseData { promise: p_awaited },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_promise_register_listener(
        &self,
        req: &RequestEnvelope,
        now: i64,
    ) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: PromiseRegisterListenerData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                if !resonate_core::is_valid_address(&r.address) {
                    tracing::warn!(
                        awaited = %r.awaited,
                        address = %r.address,
                        "Listener registration rejected: invalid address"
                    );
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid listener address",
                    ));
                }
                db.try_timeout(&[&r.awaited], now)?;
                match db.promise_register_listener(&r.awaited, &r.address)? {
                    Some(promise) => {
                        tracing::info!(
                            awaited = %r.awaited,
                            address = %r.address,
                            promise_state = %promise.state,
                            "Listener registered"
                        );
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &PromiseResponseData { promise },
                        ))
                    }
                    None => {
                        tracing::debug!(awaited = %r.awaited, "Listener registration: awaited promise not found");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Awaited promise not found",
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_promise_search(&self, req: &RequestEnvelope, _now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: PromiseSearchData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let tags_json = r.tags.as_ref().map(|t| serde_json::to_string(t).unwrap());
                let limit = match r.limit {
                    Some(n) if n > 1000 => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid 'limit' — must be between 1 and 1000",
                        ))
                    }
                    Some(n) => n,
                    None => 100,
                };
                let state_str = r.state.map(|s| s.as_str());
                let results = db.promise_search(
                    state_str,
                    tags_json.as_deref(),
                    r.cursor.as_deref(),
                    limit + 1,
                )?;
                let has_more = results.len() as i64 > limit;
                let promises: Vec<_> = results.into_iter().take(limit as usize).collect();
                let next_cursor = if has_more {
                    promises.last().map(|p| p.id.clone())
                } else {
                    None
                };
                tracing::debug!(
                    found = promises.len(),
                    has_more = has_more,
                    "Promise search completed"
                );
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &PromiseSearchResponseData {
                        promises,
                        cursor: next_cursor,
                    },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    // ============================================================================
    // Task operations
    // ============================================================================

    async fn op_task_get(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskGetData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                match db.task_get(&r.id)? {
                    Some(task) => {
                        tracing::debug!(
                            task_id = %r.id,
                            state = %task.state,
                            version = task.version,
                            "Task found"
                        );
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskResponseData { task },
                        ))
                    }
                    None => {
                        tracing::debug!(task_id = %r.id, "Task not found");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_create(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskCreateData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let action_data = &r.action.data;
                let action_id = &action_data.id;
                if let Some(addr) = action_data.tags.get("resonate:target") {
                    if !resonate_core::is_valid_address(addr) {
                        tracing::warn!(
                            task_id = %action_id,
                            address = %addr,
                            "Task create rejected: invalid resonate:target address"
                        );
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid resonate:target address",
                        ));
                    }
                }
                db.try_timeout(&[action_id], now)?;
                // Lock preamble: ensures CTE and subsequent reads see
                // current state under READ COMMITTED.
                let _ = db.lock_for_update(action_id)?;
                let tags_json = serde_json::to_string(&action_data.tags).unwrap();
                let already_timedout = now >= action_data.timeout_at;
                let (p_state, created_at, settled_at) = if already_timedout {
                    let p_state =
                        if action_data.tags.get("resonate:timer").map(|v| v.as_str()) == Some("true") {
                            tracing::debug!(task_id = %action_id, "Task create: already timedout (timer: resolved immediately)");
                            PromiseState::Resolved
                        } else {
                            tracing::debug!(task_id = %action_id, "Task create: already timedout");
                            PromiseState::RejectedTimedout
                        };
                    (
                        p_state,
                        action_data.timeout_at,
                        Some(action_data.timeout_at),
                    )
                } else {
                    (PromiseState::Pending, now, None)
                };
                let param_headers_json = action_data
                    .param
                    .headers
                    .as_ref()
                    .map(|h| serde_json::to_string(h).unwrap());
                let res = db.task_create(&TaskCreateParams {
                    promise_id: action_id,
                    state: p_state.as_str(),
                    param_headers: param_headers_json.as_deref(),
                    param_data: action_data.param.data.as_deref(),
                    tags: &tags_json,
                    timeout_at: action_data.timeout_at,
                    created_at,
                    settled_at,
                    already_timedout,
                    ttl: r.ttl,
                    pid: &r.pid,
                })?;

                // If the promise is settled, process callbacks as a separate
                // statement. This fires any callbacks registered by concurrent
                // transactions (e.g. task.suspend) that committed after
                // try_timeout's snapshot but before now.
                if res.promise.state != PromiseState::Pending {
                    db.process_callbacks(action_id, now)?;
                }

                // When the CTE created the task, use CTE result directly.
                if res.task_created {
                    let task_state_str = res.task_state.expect("invariant: task_state is Some when task_created");
                    let task_state = task_state_str.parse::<TaskState>().expect("invariant: task_state is a valid TaskState");
                    assert!(res.promise.state != PromiseState::Pending || task_state != TaskState::Fulfilled, "invariant: pending promise with fulfilled task");
                    assert!(res.promise.state == PromiseState::Pending || task_state == TaskState::Fulfilled, "invariant: settled promise with non-fulfilled task");
                    // Acquired tasks start at version 1 (first claim), fulfilled at 0
                    let task_version = if task_state == TaskState::Acquired { 1 } else { 0 };
                    let task = TaskRecord {
                        id: action_id.to_string(),
                        state: task_state,
                        version: task_version,
                        resumes: 0,
                        ttl: if task_state == TaskState::Fulfilled { None } else { Some(r.ttl) },
                        pid: if task_state == TaskState::Fulfilled { None } else { Some(r.pid.to_string()) },
                    };
                    let preload = if task_state == TaskState::Acquired {
                        db.compute_preload(action_id)?
                    } else {
                        vec![]
                    };
                    return Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskCreateResponseData {
                            task,
                            promise: res.promise,
                            preload,
                        },
                    ));
                }

                // CTE didn't create the task (promise already existed).
                // Branch on the state/version surfaced by the CTE.
                match (res.task_state.as_deref(), res.task_version) {
                    (Some("fulfilled"), version) => {
                        assert_ne!(res.promise.state, PromiseState::Pending, "invariant: pending promise with fulfilled task");
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskCreateResponseData {
                                task: TaskRecord {
                                    id: action_id.to_string(),
                                    state: TaskState::Fulfilled,
                                    version: version.unwrap_or(0),
                                    resumes: 0,
                                    ttl: None,
                                    pid: None,
                                },
                                promise: res.promise,
                                preload: vec![],
                            },
                        ))
                    }
                    (Some("pending"), Some(version)) => {
                        let acquire_result = db.task_acquire(&TaskAcquireParams {
                            task_id: action_id,
                            version,
                            time: now,
                            ttl: r.ttl,
                            pid: &r.pid,
                        })?;
                        if acquire_result.was_acquired {
                            let task = TaskRecord {
                                id: action_id.to_string(),
                                state: TaskState::Acquired,
                                version: version + 1,
                                resumes: 0,
                                ttl: Some(r.ttl),
                                pid: Some(r.pid.to_string()),
                            };
                            assert_eq!(res.promise.state, PromiseState::Pending, "invariant: settled promise with non-fulfilled task");
                            assert_eq!(acquire_result.task_version, Some(version + 1), "invariant: acquired task version must be version + 1");
                            let preload = db.compute_preload(action_id)?;
                            Ok(ResponseEnvelope::success(
                                kind_str.clone(),
                                corr_id.clone(),
                                &TaskCreateResponseData {
                                    task,
                                    promise: res.promise,
                                    preload,
                                },
                            ))
                        } else if acquire_result.task_state == Some(TaskState::Fulfilled) {
                            let promise = acquire_result.promise.expect("fulfilled task must have a promise");
                            assert_ne!(promise.state, PromiseState::Pending, "invariant: fulfilled task cannot have a pending promise");
                            Ok(ResponseEnvelope::success(
                                kind_str.clone(),
                                corr_id.clone(),
                                &TaskCreateResponseData {
                                    task: TaskRecord {
                                        id: action_id.to_string(),
                                        state: TaskState::Fulfilled,
                                        version: acquire_result.task_version.expect("invariant: fulfilled task must have a version"),
                                        resumes: 0,
                                        ttl: None,
                                        pid: None,
                                    },
                                    promise,
                                    preload: vec![],
                                },
                            ))
                        } else {
                            assert!(acquire_result.task_state.is_some(), "invariant: non-acquired result must have a task state");
                            assert!(acquire_result.task_version.is_some(), "invariant: non-acquired result must have a task version");
                            assert!(
                                acquire_result.task_state.unwrap() != TaskState::Pending || acquire_result.task_version.unwrap() != version,
                                "invariant: task state must not be pending or version must differ from request"
                            );
                            Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                409,
                                "Already exists",
                            ))
                        }
                    }
                    (None, _) => Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        422,
                        "The promise does not have a resonate:target tag",
                    )),
                    _ => Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        409,
                        "Already exists",
                    )),
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(StorageError::InvalidInput(msg)) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Invalid request: {}", msg),
            ),
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_acquire(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskAcquireData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                let result = db.task_acquire(&TaskAcquireParams {
                    task_id: &r.id,
                    version: r.version,
                    time: now,
                    ttl: r.ttl,
                    pid: &r.pid,
                })?;
                match result.promise {
                    None => {
                        tracing::debug!(task_id = %r.id, "Task acquire: task not found");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        ))
                    }
                    Some(promise) => {
                        assert!(result.task_state.is_some(), "invariant: acquired result must have a task state");
                        assert!(result.task_version.is_some(), "invariant: acquired result must have a task version");
                        assert!(
                            result.task_state.unwrap() != TaskState::Pending || result.task_version.unwrap() != r.version,
                            "invariant: task state must not be pending or version must differ from request"
                        );
                        if !result.was_acquired {
                            let state = result.task_state.unwrap();
                            let version = result.task_version.unwrap();
                            if state != TaskState::Pending {
                                tracing::debug!(
                                    task_id = %r.id,
                                    current_state = %state,
                                    "Task acquire rejected: not pending"
                                );
                                return Ok(ResponseEnvelope::error(
                                    kind_str.clone(),
                                    corr_id.clone(),
                                    409,
                                    "Task is not pending",
                                ));
                            }
                            tracing::debug!(
                                task_id = %r.id,
                                expected_version = r.version,
                                actual_version = version,
                                "Task acquire rejected: version mismatch"
                            );
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                409,
                                "Version mismatch",
                            ));
                        }
                        assert_eq!(result.task_version, Some(r.version + 1), "invariant: acquired task version must be request version + 1");
                        // Use known values — no separate task_get that could
                        // see stale state from concurrent transactions.
                        let task = TaskRecord {
                            id: r.id.to_string(),
                            state: TaskState::Acquired,
                            version: r.version + 1,
                            resumes: 0,
                            ttl: Some(r.ttl),
                            pid: Some(r.pid.to_string()),
                        };
                        let preload = db.compute_preload(&r.id)?;
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskAcquireResponseData {
                                task,
                                promise,
                                preload,
                            },
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_release(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskReleaseData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                let (_, task_exists) = db.lock_for_update(&r.id)?;
                if !task_exists {
                    tracing::debug!(task_id = %r.id, "Task release: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ));
                }
                let result = db.task_release(&r.id, r.version, now, db.task_retry_timeout())?;
                if result.task_released {
                    tracing::info!(task_id = %r.id, version = r.version, "Task released back to pending");
                    return Ok(ResponseEnvelope::new(
                        kind_str.clone(),
                        corr_id.clone(),
                        200,
                        serde_json::json!({}),
                    ));
                }
                if !result.task_exists {
                    tracing::debug!(task_id = %r.id, "Task release: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ));
                }
                tracing::debug!(task_id = %r.id, version = r.version, "Task release rejected: version mismatch or invalid state");
                Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task version mismatch or invalid state",
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_fulfill(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskFulfillData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let action_data = &r.action.data;
                db.try_timeout(&[&action_data.id], now)?;
                // Lock preamble: lock promise + task to prevent stale snapshot
                // in fulfillment CTE.
                let (_, task_exists) = db.lock_for_update(&r.id)?;
                if !task_exists {
                    tracing::debug!(task_id = %r.id, "Task fulfill: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ));
                }
                let value_headers_json = action_data
                    .value
                    .headers
                    .as_ref()
                    .map(|h| serde_json::to_string(h).unwrap());
                let result = db.task_fulfill(&TaskFulfillParams {
                    task_id: &r.id,
                    version: r.version,
                    promise_id: &r.id,
                    state: action_data.state.as_str(),
                    value_headers: value_headers_json.as_deref(),
                    value_data: action_data.value.data.as_deref(),
                    settled_at: now,
                })?;
                if !result.task_exists {
                    tracing::debug!(task_id = %r.id, "Task fulfill: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ));
                }
                if !result.task_fulfilled {
                    tracing::debug!(task_id = %r.id, version = r.version, "Task fulfill rejected: version mismatch or invalid state");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        409,
                        "Task version mismatch or invalid state",
                    ));
                }
                let promise = result.promise.expect("invariant: task exists implies promise exists");
                assert!(result.task_fulfilled, "invariant: returning 200 but task is not fulfilled");
                assert_ne!(promise.state, PromiseState::Pending, "invariant: returning 200 but promise is still pending");
                tracing::info!(
                    task_id = %r.id,
                    version = r.version,
                    promise_state = %promise.state,
                    "Task fulfilled and promise settled"
                );
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &TaskFulfillResponseData { promise },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_suspend(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskSuspendData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let awaited_ids: Vec<String> =
                    r.actions.iter().map(|a| a.data.awaited.clone()).collect();
                let mut timeout_ids: Vec<&str> = vec![&r.id];
                for aid in &awaited_ids {
                    timeout_ids.push(aid.as_str());
                }
                // Lock the task row BEFORE try_timeout to prevent
                // try_timeout from fulfilling it via promise timeout.
                let (_, task_exists) = db.lock_for_update(&r.id)?;
                db.try_timeout(&timeout_ids, now)?;
                let mut seen = std::collections::HashSet::new();
                let unique_awaited: Vec<&str> = awaited_ids
                    .iter()
                    .filter(|id| seen.insert(id.as_str()))
                    .map(|s| s.as_str())
                    .collect();
                let result = db.task_suspend(&r.id, r.version, &unique_awaited)?;
                if !result.task_matched {
                    // Use lock_for_update result — no separate task_get that
                    // could see a concurrent task creation.
                    if !task_exists {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        ));
                    }
                    tracing::debug!(
                        task_id = %r.id,
                        version = r.version,
                        "Task suspend rejected: not acquired or version mismatch"
                    );
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        409,
                        "Task is not acquired or version mismatch",
                    ));
                }
                if result.missing_count > 0 {
                    tracing::debug!(
                        task_id = %r.id,
                        missing_count = result.missing_count,
                        "Task suspend rejected: awaited promise(s) not found"
                    );
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        422,
                        "Awaited promise not found",
                    ));
                }
                if result.was_suspended {
                    tracing::info!(
                        task_id = %r.id,
                        version = r.version,
                        awaited_count = unique_awaited.len(),
                        "Task suspended, waiting on promises"
                    );
                    return Ok(ResponseEnvelope::new(
                        kind_str.clone(),
                        corr_id.clone(),
                        200,
                        serde_json::json!({}),
                    ));
                }
                // Immediate resume (settled awaited promises)
                tracing::info!(
                    task_id = %r.id,
                    version = r.version,
                    "Task suspend: immediate resume, awaited promises already settled"
                );
                let preload = db.compute_preload(&r.id)?;
                Ok(ResponseEnvelope::new(
                    kind_str.clone(),
                    corr_id.clone(),
                    300,
                    serde_json::to_value(&TaskSuspendPreloadData { preload }).unwrap(),
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_fence(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskFenceData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let action_kind = &r.action.kind;
                let action_data = &r.action.data;
                let action_id = action_data["id"].as_str().unwrap_or("");
                db.try_timeout(&[&r.id, action_id], now)?;
                // Lock preamble: ensures fence check sees current task state.
                let _ = db.lock_for_update(&r.id)?;

                match action_kind.as_str() {
                    "promise.create" => {
                        let create_data: PromiseCreateData =
                            match serde_json::from_value(action_data.clone()) {
                                Ok(d) => d,
                                Err(e) => {
                                    return Ok(ResponseEnvelope::error(
                                        kind_str.clone(),
                                        corr_id.clone(),
                                        400,
                                        &format!("Invalid action data: {}", e),
                                    ))
                                }
                            };
                        if let Err(e) = create_data.validate() {
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                400,
                                &format_validation_errors(&e),
                            ));
                        }
                        let tags_json = serde_json::to_string(&create_data.tags).unwrap();
                        let already_timedout = now >= create_data.timeout_at;
                        let address = create_data.tags.get("resonate:target").map(|s| s.as_str());
                        if let Some(addr) = address {
                            if !resonate_core::is_valid_address(addr) {
                                tracing::warn!(
                                    task_id = %r.id,
                                    address = addr,
                                    "Task fence rejected: invalid resonate:target address in fenced promise.create"
                                );
                                return Ok(ResponseEnvelope::error(
                                    kind_str.clone(),
                                    corr_id.clone(),
                                    400,
                                    "Invalid resonate:target address",
                                ));
                            }
                        }
                        let (p_state, created_at, settled_at) = if already_timedout {
                            let p_state = if create_data.tags.get("resonate:timer").map(|v| v.as_str())
                                == Some("true")
                            {
                                PromiseState::Resolved
                            } else {
                                PromiseState::RejectedTimedout
                            };
                            (
                                p_state,
                                create_data.timeout_at,
                                Some(create_data.timeout_at),
                            )
                        } else {
                            (PromiseState::Pending, now, None)
                        };
                        let param_headers_json = create_data
                            .param
                            .headers
                            .as_ref()
                            .map(|h| serde_json::to_string(h).unwrap());
                        let result = db.task_fence_create(&TaskFenceCreateParams {
                            task_id: &r.id,
                            version: r.version,
                            promise_id: &create_data.id,
                            state: p_state.as_str(),
                            param_headers: param_headers_json.as_deref(),
                            param_data: create_data.param.data.as_deref(),
                            tags: &tags_json,
                            timeout_at: create_data.timeout_at,
                            created_at,
                            settled_at,
                            already_timedout,
                            address,
                        })?;
                        if !result.task_exists {
                            tracing::debug!(task_id = %r.id, fenced_action = "promise.create", "Task fence rejected: task not found");
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                404,
                                "Task not found",
                            ));
                        }
                        if !result.fence_ok {
                            tracing::debug!(task_id = %r.id, version = r.version, fenced_action = "promise.create", "Task fence rejected: version mismatch");
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                409,
                                "Version mismatch",
                            ));
                        }
                        tracing::info!(
                            task_id = %r.id,
                            version = r.version,
                            fenced_action = "promise.create",
                            promise_id = %create_data.id,
                            "Task fence: promise.create executed"
                        );
                        let p = result.promise.expect("invariant: promise.create result must have a promise");
                        let inner_data = serde_json::json!({ "promise": p });
                        let inner_envelope = serde_json::json!({
                            "kind": action_kind,
                            "head": { "corrId": corr_id, "status": 200, "version": "2026-04-01" },
                            "data": inner_data,
                        });
                        let preload = db.compute_preload(&r.id)?;
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskFenceResponseData {
                                action: inner_envelope,
                                preload,
                            },
                        ))
                    }
                    "promise.settle" => {
                        let settle_data: PromiseSettleData =
                            match serde_json::from_value(action_data.clone()) {
                                Ok(d) => d,
                                Err(e) => {
                                    return Ok(ResponseEnvelope::error(
                                        kind_str.clone(),
                                        corr_id.clone(),
                                        400,
                                        &format!("Invalid action data: {}", e),
                                    ))
                                }
                            };
                        if let Err(e) = settle_data.validate() {
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                400,
                                &format_validation_errors(&e),
                            ));
                        }
                        let value_headers_json = settle_data
                            .value
                            .headers
                            .as_ref()
                            .map(|h| serde_json::to_string(h).unwrap());
                        let result = db.task_fence_settle(&TaskFenceSettleParams {
                            task_id: &r.id,
                            version: r.version,
                            promise_id: &settle_data.id,
                            state: settle_data.state.as_str(),
                            value_headers: value_headers_json.as_deref(),
                            value_data: settle_data.value.data.as_deref(),
                            settled_at: now,
                        })?;
                        if !result.task_exists {
                            tracing::debug!(task_id = %r.id, fenced_action = "promise.settle", "Task fence rejected: task not found");
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                404,
                                "Task not found",
                            ));
                        }
                        if !result.fence_ok {
                            tracing::debug!(task_id = %r.id, version = r.version, fenced_action = "promise.settle", "Task fence rejected: version mismatch");
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                409,
                                "Version mismatch",
                            ));
                        }
                        tracing::info!(
                            task_id = %r.id,
                            version = r.version,
                            fenced_action = "promise.settle",
                            promise_id = %settle_data.id,
                            settle_state = %settle_data.state,
                            "Task fence: promise.settle executed"
                        );
                        let inner_status = if result.promise.is_some() { 200 } else { 404 };
                        let inner_data = match &result.promise {
                            Some(p) => {
                                assert_ne!(p.state, PromiseState::Pending, "invariant: returning 200 but promise is still pending");
                                serde_json::json!({ "promise": p })
                            }
                            None => serde_json::json!("Promise not found"),
                        };
                        let inner_envelope = serde_json::json!({
                            "kind": action_kind,
                            "head": { "corrId": corr_id, "status": inner_status, "version": "2026-04-01" },
                            "data": inner_data,
                        });
                        let preload = db.compute_preload(&r.id)?;
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskFenceResponseData {
                                action: inner_envelope,
                                preload,
                            },
                        ))
                    }
                    _ => {
                        tracing::warn!(
                            task_id = %r.id,
                            action_kind = %action_kind,
                            "Task fence rejected: invalid fence action kind"
                        );
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid fence action kind",
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_heartbeat(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskHeartbeatData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let task_pairs: Vec<(&str, i64)> =
                    r.tasks.iter().map(|t| (t.id.as_str(), t.version)).collect();
                db.task_heartbeat(&r.pid, &task_pairs, now)?;
                tracing::debug!(
                    pid = %r.pid,
                    task_count = task_pairs.len(),
                    "Task heartbeat processed"
                );
                Ok(ResponseEnvelope::new(
                    kind_str.clone(),
                    corr_id.clone(),
                    200,
                    serde_json::json!({}),
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_halt(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskHaltData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                let result = db.task_halt(&r.id)?;
                if !result.task_exists {
                    tracing::debug!(task_id = %r.id, "Task halt: not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                } else if result.task_fulfilled {
                    tracing::debug!(task_id = %r.id, "Task halt rejected: already fulfilled");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        409,
                        "Task is fulfilled",
                    ))
                } else {
                    tracing::info!(task_id = %r.id, "Task halted");
                    Ok(ResponseEnvelope::new(
                        kind_str.clone(),
                        corr_id.clone(),
                        200,
                        serde_json::json!({}),
                    ))
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_continue(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskContinueData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                db.try_timeout(&[&r.id], now)?;
                let result = db.task_continue(&r.id, now)?;
                if !result.task_exists {
                    tracing::debug!(task_id = %r.id, "Task continue: not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                } else if result.continued {
                    tracing::info!(task_id = %r.id, "Task continued from halted state");
                    Ok(ResponseEnvelope::new(
                        kind_str.clone(),
                        corr_id.clone(),
                        200,
                        serde_json::json!({}),
                    ))
                } else {
                    tracing::debug!(task_id = %r.id, "Task continue rejected: not halted");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        409,
                        "Task is not halted",
                    ))
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_task_search(&self, req: &RequestEnvelope, _now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: TaskSearchData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let limit = match r.limit {
                    Some(n) if n > 1000 => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid 'limit' — must be between 1 and 1000",
                        ))
                    }
                    Some(n) => n,
                    None => 100,
                };
                let state_str = r.state.map(|s| s.as_str());
                let results = db.task_search(state_str, r.cursor.as_deref(), limit + 1)?;
                let has_more = results.len() as i64 > limit;
                let tasks: Vec<_> = results.into_iter().take(limit as usize).collect();
                let next_cursor = if has_more {
                    tasks.last().map(|t| t.id.clone())
                } else {
                    None
                };
                tracing::debug!(
                    found = tasks.len(),
                    has_more = has_more,
                    "Task search completed"
                );
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &TaskSearchResponseData {
                        tasks,
                        cursor: next_cursor,
                    },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    // ============================================================================
    // Schedule operations
    // ============================================================================

    async fn op_schedule_get(&self, req: &RequestEnvelope, _now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: ScheduleGetData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                match db.schedule_get(&r.id)? {
                    Some(schedule) => {
                        tracing::debug!(
                            schedule_id = %r.id,
                            cron = %schedule.cron,
                            next_run_at = schedule.next_run_at,
                            "Schedule found"
                        );
                        Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &ScheduleResponseData { schedule },
                        ))
                    }
                    None => {
                        tracing::debug!(schedule_id = %r.id, "Schedule not found");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Schedule not found",
                        ))
                    }
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_schedule_create(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: ScheduleCreateData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                // Every promise this schedule fires carries the target, so it is
                // held to the same standard promise create holds a target to.
                if let Some(addr) = r.promise_tags.get("resonate:target") {
                    if !resonate_core::is_valid_address(addr) {
                        tracing::warn!(
                            schedule_id = %r.id,
                            address = addr,
                            "Schedule create rejected: invalid resonate:target address"
                        );
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid resonate:target address",
                        ));
                    }
                }
                if !util::is_valid_cron(&r.cron) {
                    tracing::warn!(
                        schedule_id = %r.id,
                        cron = %r.cron,
                        "Schedule create rejected: invalid cron expression"
                    );
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid cron expression",
                    ));
                }
                let promise_tags_json = serde_json::to_string(&r.promise_tags).unwrap();
                let next_run_at = util::compute_next_cron(&r.cron, now);
                let promise_param_headers_json = r
                    .promise_param
                    .headers
                    .as_ref()
                    .map(|h| serde_json::to_string(h).unwrap());
                let schedule = db.schedule_create(&ScheduleCreateParams {
                    id: &r.id,
                    cron: &r.cron,
                    promise_id: &r.promise_id,
                    promise_timeout: r.promise_timeout,
                    promise_param_headers: promise_param_headers_json.as_deref(),
                    promise_param_data: r.promise_param.data.as_deref(),
                    promise_tags: &promise_tags_json,
                    created_at: now,
                    next_run_at,
                })?;
                tracing::info!(
                    schedule_id = %schedule.id,
                    cron = %schedule.cron,
                    next_run_at = schedule.next_run_at,
                    "Schedule created"
                );
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &ScheduleResponseData { schedule },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_schedule_delete(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: ScheduleDeleteData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                if db.schedule_delete(&r.id)? {
                    tracing::info!(schedule_id = %r.id, "Schedule deleted");
                    Ok(ResponseEnvelope::new(
                        kind_str.clone(),
                        corr_id.clone(),
                        200,
                        serde_json::json!({}),
                    ))
                } else {
                    tracing::debug!(schedule_id = %r.id, "Schedule delete: not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Schedule not found",
                    ))
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    async fn op_schedule_search(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        match self
            .storage
            .transact(move |db| {
                let r: ScheduleSearchData = match serde_json::from_value(data.clone()) {
                    Ok(d) => d,
                    Err(e) => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format!("Invalid request: {}", e),
                        ))
                    }
                };
                if let Err(e) = r.validate() {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    ));
                }
                let tags_json = r.tags.as_ref().map(|t| serde_json::to_string(t).unwrap());
                let limit = match r.limit {
                    Some(n) if n > 1000 => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            "Invalid 'limit' — must be between 1 and 1000",
                        ))
                    }
                    Some(n) => n,
                    None => 10,
                };
                let schedules =
                    db.schedule_search(tags_json.as_deref(), r.cursor.as_deref(), limit + 1)?;
                let limit_usize = limit as usize;
                let has_more = schedules.len() > limit_usize;
                let result_schedules: Vec<_> = schedules.into_iter().take(limit_usize).collect();
                let next_cursor = if has_more {
                    result_schedules.last().map(|s| s.id.clone())
                } else {
                    None
                };
                tracing::debug!(
                    found = result_schedules.len(),
                    has_more = has_more,
                    "Schedule search completed"
                );
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &ScheduleSearchResponseData {
                        schedules: result_schedules,
                        cursor: next_cursor,
                    },
                ))
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            ),
        }
    }

    // ============================================================================
    // Debug operations
    // ============================================================================

    async fn op_debug_reset(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        match self.storage.transact(move |db| db.debug_reset()).await {
            Ok(()) => {
                tracing::warn!("Debug reset: all data cleared");
                ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    200,
                    Value::Object(serde_json::Map::new()),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "Debug reset failed");
                ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    500,
                    &format!("Reset failed: {}", e),
                )
            }
        }
    }

    async fn op_debug_snap(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        match self.storage.query(move |db| db.snap()).await {
            Ok(snapshot) => {
                let data = serde_json::to_value(snapshot).unwrap_or(Value::Null);
                ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, data)
            }
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Snap failed: {}", e),
            ),
        }
    }

    async fn op_debug_tick(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let time = match req.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Missing or invalid 'time' field",
                )
            }
        };
        if let Some(debug_time) = req.head.debug_time {
            if debug_time != time {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "resonate:debug_time must equal data.time",
                );
            }
        }

        match self
            .storage
            .transact(move |db| process_all_timeouts(db, time).map(|_| ()))
            .await
        {
            Ok(_) => ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                Value::Array(vec![]),
            ),
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Tick failed: {}", e),
            ),
        }
    }
}

/// Run one tick: the three timeout sweeps, then expired schedules.
///
/// Returns how many schedules fired, so the caller can record it — this crate
/// deliberately has no opinion about metrics.
pub fn process_all_timeouts(db: &dyn Db, time: i64) -> StorageResult<usize> {
    // Run the three tick CTE statements (promise timeouts, task retry, task lease)
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time)?;

    // Process expired schedules (application-level cron computation)
    process_schedule_timeouts(db, time)
}

/// Process expired schedule timeouts.
fn process_schedule_timeouts(db: &dyn Db, time: i64) -> StorageResult<usize> {
    let expired = db.get_expired_schedule_timeouts(time)?;
    let mut fired = 0usize;

    for (schedule_id, fired_at) in &expired {
        let schedule = match db.schedule_get(schedule_id)? {
            Some(s) => s,
            None => continue,
        };

        let next_run_at = util::compute_next_cron(&schedule.cron, *fired_at);

        let mut promise_tags = schedule.promise_tags.clone();
        promise_tags.insert("resonate:schedule".to_string(), schedule_id.clone());

        let promise_id = schedule
            .promise_id
            .replace("{{.id}}", schedule_id)
            .replace("{{.timestamp}}", &fired_at.to_string());
        promise_tags.insert("resonate:origin".to_string(), promise_id.clone());
        promise_tags.insert("resonate:branch".to_string(), promise_id.clone());
        promise_tags.insert("resonate:parent".to_string(), promise_id.clone());
        promise_tags.insert("resonate:prefix".to_string(), promise_id.clone());

        match db.process_schedule_timeout(
            schedule_id,
            *fired_at,
            next_run_at,
            time,
            &promise_tags,
        )? {
            Some(_) => {
                tracing::info!(
                    schedule_id = %schedule_id,
                    fired_at = fired_at,
                    next_run_at = next_run_at,
                    "Schedule fired"
                );
                fired += 1;
            }
            None => {
                // Idempotency guard fired or schedule was deleted — skip.
            }
        }
    }

    Ok(fired)
}
