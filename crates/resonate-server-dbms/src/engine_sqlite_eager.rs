//! The SQLite engine, sweeping eagerly.
//!
//! A second complete implementation of the protocol over SQLite, on the same
//! schema, differing from [`crate::engine_sqlite`] in one thing: which
//! promises the timeout sweep is responsible for. It is a copy of that file,
//! kept a copy on purpose — `diff engine_sqlite.rs engine_sqlite_eager.rs` is
//! how the difference between two admissible schedules is meant to be read,
//! and a shared abstraction with a mode flag would hide exactly what is being
//! compared. The cost is that a change to one must be made to the other; the
//! differential is what notices when it was not.
//!
//! # What differs, and why it is allowed to
//!
//! The abstract machine's `processPromiseTimeout` is `touchObject id now` — a
//! materialising read, the same thing a request performs on its way past the
//! row. So *when* it fires is a schedule the specification leaves free:
//! `valid/lean/real.lean` records the trace checker accepting both an eager
//! server and a lazy one on the same run.
//!
//! What is not free is *which* promises a fair scheduler is obliged to look
//! at. `04-theorems/liveness.lean` keys `enabledInternal` on
//! `otype.awaitable` — "a deadline is owed an observation exactly when someone
//! can be blocked on it". [`crate::engine_sqlite`] arms `resonate:target`
//! instead, which is `okind == Task` and strictly narrower: an external,
//! untargeted promise — the kind a task suspends on — is on no queue there and
//! times out only when a request next names it. This file arms
//! [`resonate_core::types::is_external`], which is the specification's rule.
//!
//! An `.internal` promise is on neither queue in either file, and that is not
//! an omission: nobody may await it, so nobody is owed the observation.
//!
//! # The whole of the difference
//!
//! Four `WHERE` clauses and three arming sites. The clauses are the queue —
//! for the sweep, for `upcoming`, and for the snapshot's own report of it, so
//! the three cannot drift. The arming sites are what a transition announces to
//! a timer, which must match the rows it wrote.
//!
//! Everything below this header is [`crate::engine_sqlite`] verbatim.
//!
//! A complete implementation of the protocol over SQLite: it parses and
//! validates a request, applies the transition in its own SQL, and shapes the
//! response — with no `Db` trait between the two halves and no shared engine
//! above them.
//!
//! That is what removes the outbox. `outgoing_execute` and `outgoing_unblock`
//! existed to carry a message across the `Db` boundary: the statement that
//! emitted one had no way to hand it back, so it left a row for a pump to find
//! 100 ms later. Here the statement's caller *is* the engine, so a message is
//! returned in [`Output::messages`] and delivered by whoever called `process`.
//!
//! The schema is `persistence_sqlite.rs`'s, minus those two tables. See that
//! module for what the promise row's columns replaced; the collapse is the
//! same, and this file changes only where the messages go.
//!
//! # How emissions are collected
//!
//! [`SqliteDb`] carries an `emitted` list and every statement that would have
//! written an outbox row pushes onto it instead. `transact` returns it
//! alongside the operation's own result. The operation bodies never mention
//! it — an emission is a side effect of a transition, and threading it through
//! 21 return types would say otherwise.

use rusqlite::{params, Connection};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use validator::Validate;

use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRegisterCallbackData,
    PromiseRegisterListenerData, PromiseResponseData, PromiseSearchData, PromiseSearchResponseData,
    PromiseSettleData, RequestEnvelope, ResponseEnvelope, ScheduleCreateData, ScheduleDeleteData,
    ScheduleGetData, ScheduleResponseData, ScheduleSearchData, ScheduleSearchResponseData,
    TaskAcquireData, TaskAcquireResponseData, TaskContinueData, TaskCreateData,
    TaskCreateResponseData, TaskFenceData, TaskFenceResponseData, TaskFulfillData,
    TaskFulfillResponseData, TaskGetData, TaskHaltData, TaskHeartbeatData, TaskReleaseData,
    TaskResponseData, TaskSearchData, TaskSearchResponseData, TaskSuspendData,
    TaskSuspendPreloadData,
};
use resonate_core::util;

use crate::engine_port::{Input, Outgoing, Output, ResonateEngine, Scheduled, Timeout};
use crate::StorageError;

use super::{
    PromiseCreateParams, PromiseCreateResult, PromiseSettleParams, PromiseSettleResult,
    RegisterCallbackResult, ScheduleCreateParams, StorageResult, TaskAcquireParams,
    TaskAcquireResult, TaskContinueResult, TaskCreateParams, TaskCreateResult,
    TaskFenceCreateParams, TaskFenceResult, TaskFenceSettleParams, TaskFulfillParams,
    TaskFulfillResult, TaskHaltResult, TaskReleaseResult, TaskSuspendResult,
};
use resonate_core::types::{
    PromiseRecord, PromiseState, PromiseValue, ScheduleRecord, Snapshot, SnapshotCallback,
    SnapshotListener, SnapshotMessage, SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskRecord,
    TaskState,
};

/// `is_external` where the caller holds the tags as the JSON it is about to
/// insert rather than as a map.
///
/// The one function this file has that `engine_sqlite` does not, and it exists
/// only because `promise_create` takes its tags as a `&str`. Malformed JSON
/// answers `false`, which is the narrow queue's answer — the INSERT beside it
/// would have failed the same way.
fn awaitable_json(tags: &str) -> bool {
    resonate_core::types::is_external(&serde_json::from_str(tags).unwrap_or_default())
}

fn parse_promise_state(s: &str) -> PromiseState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt promise state in DB: {}", e))
}

fn parse_task_state(s: &str) -> TaskState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt task state in DB: {}", e))
}

/// Set the connection pragmas, then migrate.
///
/// The pragmas are per-connection settings rather than schema, so they stay
/// here; everything that describes the tables lives in `migrations/sqlite`.
/// A migration error is returned, not logged: a server whose schema did not
/// migrate must not serve.
pub fn init_db(conn: &mut Connection, migrate: bool) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
        ",
    )?;
    crate::migrate::run_rusqlite(conn, &MIGRATOR, migrate)?;
    Ok(())
}

/// The same embedded migrator sqlx would run, applied through rusqlite —
/// see [`crate::migrate`] for why the executor differs and nothing else does.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

pub struct SqliteEagerEngine {
    conn: Arc<Mutex<Connection>>,
    task_retry_timeout: i64,
    preload_limit: u32,
    debug: bool,
}

impl SqliteEagerEngine {
    pub fn open(
        path: &str,
        task_retry_timeout: i64,
        preload_limit: u32,
        migrate: bool,
        debug: bool,
    ) -> rusqlite::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn, migrate)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            task_retry_timeout,
            preload_limit,
            debug,
        })
    }

    /// Run one transition, and hand back what it emitted along with its result.
    ///
    /// The emissions are dropped if the transaction rolls back, which is the
    /// atomicity the port promises: state and messages commit together or not
    /// at all. An outbox got that for free by being a table; here it is the
    /// `?` on `commit` and the fact that `emitted` never leaves this scope on
    /// the error path.
    async fn transact<F, T>(&self, f: F) -> StorageResult<(T, Vec<Outgoing>, Vec<Scheduled>)>
    where
        F: FnMut(&SqliteDb) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(feature = "concurrency-stress")]
        tokio::task::yield_now().await;

        let mut f = f;
        let conn = Arc::clone(&self.conn);
        let task_retry_timeout = self.task_retry_timeout;
        let preload_limit = self.preload_limit;
        tokio::task::block_in_place(|| {
            // Use unwrap_or_else to recover from poisoned mutex (a prior panic
            // while holding the lock). The connection itself is still valid.
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let tx = conn.unchecked_transaction()?;
            let db = SqliteDb {
                conn: &tx,
                task_retry_timeout,
                preload_limit,
                emitted: RefCell::new(Vec::new()),
                armed: RefCell::new(Vec::new()),
            };
            let result = f(&db)?;
            let emitted = db.emitted.into_inner();
            let armed = db.armed.into_inner();
            tx.commit()?;
            Ok((result, emitted, armed))
        })
    }

    /// One operation: run it, and turn a storage failure into a response.
    ///
    /// Every op has the same tail, so it lives here once. `InvalidInput` maps
    /// to 400 for all of them — SQLite never raises it (it has no id-length
    /// limit to trip), so this is the shape rather than a behaviour.
    async fn run<F>(&self, req: &RequestEnvelope, f: F) -> Output
    where
        F: FnMut(&SqliteDb) -> StorageResult<ResponseEnvelope> + Send + 'static,
    {
        match self.transact(f).await {
            Ok((response, messages, timeouts)) => Output {
                response: Some(response),
                messages,
                timeouts,
            },
            Err(StorageError::InvalidInput(msg)) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Invalid request: {}", msg),
            )),
            Err(e) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Internal error: {}", e),
            )),
        }
    }

    /// A read. Nothing is emitted, so nothing comes back but the result.
    async fn query<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&SqliteDb) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(feature = "concurrency-stress")]
        tokio::task::yield_now().await;

        let mut f = f;
        let conn = Arc::clone(&self.conn);
        let task_retry_timeout = self.task_retry_timeout;
        let preload_limit = self.preload_limit;
        tokio::task::block_in_place(|| {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let db = SqliteDb {
                conn: &conn,
                task_retry_timeout,
                preload_limit,
                emitted: RefCell::new(Vec::new()),
                armed: RefCell::new(Vec::new()),
            };
            f(&db)
        })
    }

    pub async fn dispatch(&self, req: &RequestEnvelope, now: i64) -> Output {
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
            "debug.reset" | "debug.snap" | "debug.tick" if !self.debug => {
                Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    403,
                    "Debug operations are disabled",
                ))
            }
            "debug.reset" => self.op_debug_reset(req).await,
            "debug.snap" => self.op_debug_snap(req).await,
            "debug.tick" => self.op_debug_tick(req).await,

            _ => {
                tracing::warn!(kind = %kind, "Invalid request: unknown operation");
                Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    &format!("Unknown operation: {}", kind),
                ))
            }
        }
    }

    // ============================================================================
    // Promise operations
    // ============================================================================

    async fn op_promise_get(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_promise_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_promise_settle(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_promise_register_callback(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
                if !resonate_core::types::is_external(&p_awaited.tags) {
                    tracing::debug!(awaited = %r.awaited, "Callback registration rejected: awaited is not awaitable");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        422,
                        "Awaited promise is not awaitable",
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
    }

    async fn op_promise_register_listener(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
                        if !resonate_core::types::is_external(&promise.tags) {
                            tracing::debug!(awaited = %r.awaited, "Listener registration rejected: awaited is not awaitable");
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                422,
                                "Awaited promise is not awaitable",
                            ));
                        }
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
    }

    async fn op_promise_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    // ============================================================================
    // Task operations
    // ============================================================================

    async fn op_task_get(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
                    // Every branch computes it: preload is branch-scoped, not
                    // lifecycle-scoped, so a fulfilled task's siblings are as
                    // real as an acquired one's.
                    let preload = db.compute_preload(action_id)?;
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
                                preload: db.compute_preload(action_id)?,
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
                                    preload: db.compute_preload(action_id)?,
                                },
                            ))
                        } else {
                            assert!(acquire_result.task_state.is_some(), "invariant: non-acquired result must have a task state");
                            assert!(acquire_result.task_version.is_some(), "invariant: non-acquired result must have a task version");
                            // Commented out, not deleted: this fired as a 500 under concurrent
                            // load. It claims a lost acquire implies the row moved on, but another
                            // request can return the task to `pending` at the same version between
                            // the acquire and this read — so the state it calls impossible is
                            // reachable, and a race that the next line already answers with a 409
                            // became an internal error instead.
                            // assert!(
                            //     acquire_result.task_state.unwrap() != TaskState::Pending || acquire_result.task_version.unwrap() != version,
                            //     "invariant: task state must not be pending or version must differ from request"
                            // );
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
    }

    async fn op_task_acquire(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
                    assert!(
                        result.task_state.is_some(),
                        "invariant: acquired result must have a task state"
                    );
                    assert!(
                        result.task_version.is_some(),
                        "invariant: acquired result must have a task version"
                    );
                    // Commented out, not deleted: this fired as a 500 under concurrent
                    // load. It claims a lost acquire implies the row moved on, but another
                    // request can return the task to `pending` at the same version between
                    // the acquire and this read — so the state it calls impossible is
                    // reachable, and a race that the next line already answers with a 409
                    // became an internal error instead.
                    // assert!(
                    //     result.task_state.unwrap() != TaskState::Pending || result.task_version.unwrap() != r.version,
                    //     "invariant: task state must not be pending or version must differ from request"
                    // );
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
                    assert_eq!(
                        result.task_version,
                        Some(r.version + 1),
                        "invariant: acquired task version must be request version + 1"
                    );
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
    }

    async fn op_task_release(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_fulfill(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_suspend(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
            // Duplicates are refused by validation, so the list is already
            // unique — no deduplication on the way to storage.
            let awaited: Vec<&str> = awaited_ids.iter().map(|s| s.as_str()).collect();
            let result = db.task_suspend(&r.id, r.version, &awaited)?;
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
            if result.non_awaitable_count > 0 {
                tracing::debug!(
                    task_id = %r.id,
                    non_awaitable_count = result.non_awaitable_count,
                    "Task suspend rejected: awaited promise(s) not awaitable"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    422,
                    "Awaited promise is not awaitable",
                ));
            }
            if result.was_suspended {
                tracing::info!(
                    task_id = %r.id,
                    version = r.version,
                    awaited_count = awaited.len(),
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
    }

    async fn op_task_fence(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_heartbeat(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_halt(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_continue(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_task_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    // ============================================================================
    // Schedule operations
    // ============================================================================

    async fn op_schedule_get(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_schedule_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_schedule_delete(&self, req: &RequestEnvelope) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    async fn op_schedule_search(&self, req: &RequestEnvelope) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
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
    }

    // ============================================================================
    // Debug operations
    // ============================================================================

    async fn op_debug_reset(&self, req: &RequestEnvelope) -> Output {
        Output::response(match self.transact(move |db| db.debug_reset()).await {
            Ok(((), _, _)) => {
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
        })
    }

    async fn op_debug_snap(&self, req: &RequestEnvelope) -> Output {
        Output::response(match self.query(move |db| db.snap()).await {
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
        })
    }

    /// The sweep, and every message it emits.
    ///
    /// This is the one debug op that emits: redispatching a pending task and
    /// firing a schedule both produce execute messages, and under the outbox
    /// they were left for the pump. Here they ride out on the tick's own
    /// `Output`, which is why the caller must deliver them.
    async fn op_debug_tick(&self, req: &RequestEnvelope) -> Output {
        let time = match req.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => {
                return Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Missing or invalid 'time' field",
                ))
            }
        };
        if let Some(debug_time) = req.head.debug_time {
            if debug_time != time {
                return Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "resonate:debug_time must equal data.time",
                ));
            }
        }

        match self
            .transact(move |db| process_all_timeouts(db, time).map(|_| ()))
            .await
        {
            Ok(((), messages, timeouts)) => Output {
                response: Some(ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    200,
                    Value::Array(vec![]),
                )),
                messages,
                timeouts,
            },
            Err(e) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Tick failed: {}", e),
            )),
        }
    }
}

struct SqliteDb<'a> {
    conn: &'a rusqlite::Connection,
    task_retry_timeout: i64,
    preload_limit: u32,
    /// What this transition has emitted so far.
    ///
    /// `RefCell` because every operation takes `&SqliteDb` — an emission is a
    /// side effect of a transition, not something its signature should carry.
    emitted: RefCell<Vec<Outgoing>>,
    /// What deadlines this transition armed or moved.
    ///
    /// Unlike `emitted`, this is a hint and nothing more: the deadline is a
    /// column on the promise row, committed with the state change, and the
    /// sweep will find it whether or not it is reported here. Missing one
    /// costs latency, never correctness — which is why only arming is
    /// reported and disarming is not. A stale entry fires into a no-op.
    armed: RefCell<Vec<Scheduled>>,
}

impl SqliteDb<'_> {
    /// Queue a message for the caller of `process`.
    ///
    /// This is the whole of what `INSERT INTO outgoing_execute` used to do,
    /// minus the row, the pump and the 100 ms.
    fn emit(&self, message: Outgoing) {
        self.emitted.borrow_mut().push(message);
    }

    /// Report a deadline this transition just wrote.
    ///
    /// Targeted by construction: every call site has the row in hand, so this
    /// never scans for what is armed — it states what was armed.
    fn arm(&self, at: i64, timeout: Timeout) {
        self.armed.borrow_mut().push(Scheduled { at, timeout });
    }

    /// A promise joins the eager sweep when anyone can block on it — the queue
    /// is `state = 'pending' AND <awaitable>`, so an `.internal` promise times
    /// out lazily through `try_timeout` and has no deadline to announce.
    ///
    /// `engine_sqlite` passes `targeted` here; this file passes `awaitable`.
    /// That is the parameter, and the whole of the arming difference.
    fn arm_promise_timeout(&self, promise_id: &str, timeout_at: i64, awaitable: bool) {
        if awaitable {
            self.arm(
                timeout_at,
                Timeout::PromiseTimeout {
                    promise_id: promise_id.to_string(),
                },
            );
        }
    }

    fn arm_retry(&self, task_id: &str, at: i64) {
        self.arm(
            at,
            Timeout::TaskRetryTimeout {
                task_id: task_id.to_string(),
            },
        );
    }

    fn arm_lease(&self, task_id: &str, pid: &str, at: i64) {
        self.arm(
            at,
            Timeout::TaskLeaseTimeout {
                task_id: task_id.to_string(),
                pid: pid.to_string(),
            },
        );
    }

    /// Emit an execute message for `task_id`, if it has somewhere to go.
    ///
    /// Every redispatch path needs the task's current version and target, and
    /// a task whose promise carries no `resonate:target` has neither an
    /// address nor anything to send. The retry deadline is armed separately by
    /// the caller, because the two do not always coincide: an untargeted task
    /// is redispatched to nobody but still waits on a deadline.
    fn emit_execute(&self, task_id: &str) -> rusqlite::Result<()> {
        let (version, target): (i64, Option<String>) = self.conn.query_row(
            "SELECT task_version, target FROM promises WHERE id = ?1",
            params![task_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if let Some(address) = target {
            self.emit(Outgoing::Execute {
                address,
                task_id: task_id.to_string(),
                version,
            });
        }
        Ok(())
    }
}

// === Settlement chain helpers (multi-statement within the transaction) ===

/// SettlementEnqueued: fulfill task, drop its timeout, delete callbacks by awaiter.
///
/// Fulfilling and dropping the timeout were two statements against two tables;
/// they are one row now, so they are one `SET`. Clearing `retry_timeout_at`/`lease_timeout_at`
/// is what deleting the `task_timeouts` row used to be, and `ttl`/`pid` go with
/// the lease that just ended.
fn settlement_enqueued(tx: &rusqlite::Connection, promise_id: &str) -> rusqlite::Result<bool> {
    let fulfilled = tx.execute(
        "UPDATE promises SET task_state = 'fulfilled',
                             retry_timeout_at = NULL, lease_timeout_at = NULL, ttl = NULL, pid = NULL
         WHERE id = ?1 AND task_state IS NOT NULL AND task_state != 'fulfilled'",
        params![promise_id],
    )? > 0;
    if fulfilled {
        tx.execute(
            "DELETE FROM callbacks WHERE awaiter_id = ?1",
            params![promise_id],
        )?;
    }
    Ok(fulfilled)
}

/// ResumptionEnqueued: mark callbacks ready, resume suspended tasks, insert outgoing
fn resumption_enqueued(
    db: &SqliteDb,
    awaited_id: &str,
    time: i64,
    task_retry_timeout: i64,
    exclude_fulfilled: Option<&[String]>,
) -> rusqlite::Result<()> {
    let tx = db.conn;
    // Mark callbacks ready
    tx.execute(
        "UPDATE callbacks SET ready = true WHERE awaited_id = ?1",
        params![awaited_id],
    )?;

    // Find awaiter IDs that need resuming (suspended tasks whose callbacks just
    // became ready). The task is on the promise row now, so the join is to
    // `promises` and reads `task_state`.
    let mut stmt = tx.prepare(
        "SELECT DISTINCT c.awaiter_id FROM callbacks c
         JOIN promises p ON p.id = c.awaiter_id
         WHERE c.awaited_id = ?1 AND c.ready = true AND p.task_state = 'suspended'",
    )?;
    let awaiter_ids: Vec<String> = {
        let mut rows = stmt.query(params![awaited_id])?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            if let Some(excluded) = exclude_fulfilled {
                if excluded.contains(&id) {
                    continue;
                }
            }
            ids.push(id);
        }
        ids
    };

    for awaiter_id in &awaiter_ids {
        // Resume: set to pending (version unchanged — only claim bumps version)
        // and move the task onto the retry queue. Writing `retry_timeout_at` and
        // clearing `lease_timeout_at` is the whole of what switching the timeout row
        // from type 1 to type 0 used to be.
        let updated = tx.execute(
            "UPDATE promises SET task_state = 'pending', retry_timeout_at = ?2,
                                 lease_timeout_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_state = 'suspended'",
            params![awaiter_id, time + task_retry_timeout],
        )?;
        if updated > 0 {
            db.arm_retry(awaiter_id, time + task_retry_timeout);
            db.emit_execute(awaiter_id)?;
        }
    }
    Ok(())
}

/// ListenerUnblocked: emit one unblock message per listener, then drop them.
///
/// The outbox stored `(promise_id, address)` and joined back to `promises` when
/// the pump ran, so the promise a listener saw was the one at delivery time.
/// Here it is captured at emission time — the same record, because this runs
/// inside the settlement that just made it final and a settled promise does not
/// change again.
fn listener_unblocked(db: &SqliteDb, promise_id: &str) -> rusqlite::Result<()> {
    let tx = db.conn;
    let addresses: Vec<String> = {
        let mut stmt = tx.prepare("SELECT address FROM listeners WHERE promise_id = ?1")?;
        let mut rows = stmt.query(params![promise_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row.get(0)?);
        }
        out
    };
    if addresses.is_empty() {
        return Ok(());
    }

    let mut stmt = tx.prepare(
        "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?1",
    )?;
    let mut rows = stmt.query(params![promise_id])?;
    if let Some(row) = rows.next()? {
        let promise = row_to_promise(row)?;
        for address in addresses {
            db.emit(Outgoing::Unblock {
                address,
                promise: promise.clone(),
            });
        }
    }

    tx.execute(
        "DELETE FROM listeners WHERE promise_id = ?1",
        params![promise_id],
    )?;
    Ok(())
}

/// Full settlement chain: settle promise + SettlementEnqueued + ResumptionEnqueued + ListenerUnblocked
#[allow(clippy::too_many_arguments)]
fn settle_promise(
    db: &SqliteDb,
    id: &str,
    state: &str,
    value_headers: Option<&str>,
    value_data: Option<&str>,
    settled_at: i64,
    time: i64,
    task_retry_timeout: i64,
) -> rusqlite::Result<bool> {
    let updated = db.conn.execute(
        "UPDATE promises SET state = ?2, value_headers = ?3, value_data = ?4, settled_at = ?5 WHERE id = ?1 AND state = 'pending'",
        params![id, state, value_headers, value_data, settled_at],
    )?;
    if updated == 0 {
        return Ok(false);
    }

    // No promise timeout to delete: the queue is `state = 'pending'`, and the
    // UPDATE above just took this row out of it.
    settlement_enqueued(db.conn, id)?;
    resumption_enqueued(db, id, time, task_retry_timeout, None)?;
    listener_unblocked(db, id)?;
    Ok(true)
}

impl<'a> SqliteDb<'a> {
    fn task_retry_timeout(&self) -> i64 {
        self.task_retry_timeout
    }

    fn lock_for_update(&self, id: &str) -> StorageResult<(bool, bool)> {
        let promise_exists = self.conn.query_row(
            "SELECT COUNT(*) FROM promises WHERE id = ?1",
            params![id],
            |r| r.get::<_, i64>(0),
        )? > 0;
        let task_exists = self.conn.query_row(
            "SELECT COUNT(*) FROM promises WHERE id = ?1 AND task_state IS NOT NULL",
            params![id],
            |r| r.get::<_, i64>(0),
        )? > 0;
        Ok((promise_exists, task_exists))
    }

    /// Nothing to do, and that is a property of SQLite rather than an omission.
    ///
    /// Postgres and MySQL run a statement here so the transition sees callbacks
    /// a concurrent transaction registered after its own snapshot opened. A
    /// `SqliteEagerEngine` holds one `Connection` behind a `Mutex`, so no other
    /// transaction can have committed anything while this one runs — there is
    /// nothing for a second look to find.
    fn process_callbacks(&self, promise_id: &str, time: i64) -> StorageResult<()> {
        let _ = (promise_id, time);
        Ok(())
    }

    fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids_json = serde_json::to_string(ids).unwrap();
        // Find expired promises from the ID set
        let mut stmt = self.conn.prepare(
            "SELECT id, is_timer, timeout_at FROM promises
             WHERE id IN (SELECT value FROM json_each(?1))
               AND state = 'pending' AND timeout_at <= ?2
             ORDER BY id",
        )?;
        let expired: Vec<(String, bool, i64)> = {
            let mut rows = stmt.query(params![ids_json, time])?;
            let mut results = Vec::new();
            while let Some(row) = rows.next()? {
                results.push((row.get(0)?, row.get(1)?, row.get(2)?));
            }
            results
        };

        if expired.is_empty() {
            return Ok(());
        }

        // Settle each expired promise
        let mut fulfilled_ids = Vec::new();
        for (id, is_timer, timeout_at) in &expired {
            let new_state = if *is_timer {
                "resolved"
            } else {
                "rejected_timedout"
            };
            self.conn.execute(
                "UPDATE promises SET state = ?2, settled_at = ?3 WHERE id = ?1 AND state = 'pending'",
                params![id, new_state, timeout_at],
            )?;

            // SettlementEnqueued
            if settlement_enqueued(self.conn, id)? {
                fulfilled_ids.push(id.clone());
            }
        }

        // ResumptionEnqueued for each expired
        for (id, _, _) in &expired {
            resumption_enqueued(
                self,
                id,
                time,
                self.task_retry_timeout,
                Some(&fulfilled_ids),
            )?;
        }

        // ListenerUnblocked for each expired
        for (id, _, _) in &expired {
            listener_unblocked(self, id)?;
        }

        Ok(())
    }

    fn promise_get(&self, id: &str) -> StorageResult<Option<PromiseRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_promise(row)?)),
            None => Ok(None),
        }
    }

    fn promise_create(&self, params: &PromiseCreateParams) -> StorageResult<PromiseCreateResult> {
        let PromiseCreateParams {
            id,
            state,
            param_headers,
            param_data,
            tags,
            timeout_at,
            created_at,
            settled_at,
            already_timedout,
            address,
        } = *params;
        // Idempotent insert
        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at],
        )?;

        let was_created = inserted > 0;
        if was_created {
            // Creating a task is now an UPDATE of the row that was just
            // inserted, and `task_state IS NULL` is the guard that used to be
            // `INSERT OR IGNORE INTO tasks`: a promise carries at most one task,
            // and only the first writer gets to install it. No promise timeout
            // is written either way — `state = 'pending' AND <awaitable>` is
            // the queue, and the INSERT above already put the row in it.
            //
            // The announcement stands here rather than on the targeted branch
            // below, which is where `engine_sqlite` makes it: an untargeted
            // external promise never reaches that branch, and on this queue it
            // has a deadline to announce. A row already past its deadline is
            // settled, so it joins no queue and announces nothing.
            if !already_timedout {
                self.arm_promise_timeout(id, timeout_at, awaitable_json(tags));
            }
            if already_timedout {
                // Already timed out — create fulfilled task if resonate:target
                if address.is_some() {
                    self.conn.execute(
                        "UPDATE promises SET task_state = 'fulfilled', task_version = 0
                         WHERE id = ?1 AND task_state IS NULL",
                        params![id],
                    )?;
                }
            } else if let Some(addr) = address {
                // TaskInfraCreated
                let created = self.conn.execute(
                    "UPDATE promises SET task_state = 'pending', task_version = 0, retry_timeout_at = ?2
                     WHERE id = ?1 AND task_state IS NULL",
                    params![id, created_at + self.task_retry_timeout],
                )? > 0;
                if created {
                    self.arm_retry(id, created_at + self.task_retry_timeout);
                    self.emit(Outgoing::Execute {
                        address: addr.to_string(),
                        task_id: id.to_string(),
                        version: 0,
                    });
                }
            }
        }

        Ok(PromiseCreateResult {
            was_created,
            promise: self.promise_get(id)?.unwrap(),
        })
    }

    fn promise_settle(&self, params: &PromiseSettleParams) -> StorageResult<PromiseSettleResult> {
        let PromiseSettleParams {
            id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;
        let was_settled = settle_promise(
            self,
            id,
            state,
            value_headers,
            value_data,
            settled_at,
            settled_at,
            self.task_retry_timeout,
        )?;

        Ok(PromiseSettleResult {
            was_settled,
            promise: self.promise_get(id)?,
        })
    }

    fn promise_register_callback(
        &self,
        awaited_id: &str,
        awaiter_id: &str,
        time: i64,
    ) -> StorageResult<RegisterCallbackResult> {
        let awaited = self.promise_get(awaited_id)?;
        let awaiter = self.promise_get(awaiter_id)?;

        // An awaited that may not be awaited is refused by the caller, so
        // nothing below may write — neither the callback row nor the direct
        // resume, which would otherwise wake the awaiter for a registration
        // that never happened.
        if let Some(ref pa) = awaited {
            if !resonate_core::types::is_external(&pa.tags) {
                return Ok(RegisterCallbackResult { awaited, awaiter });
            }
        }

        // Insert callback only if both pending and awaiter has target
        if let (Some(ref pa), Some(ref pw)) = (&awaited, &awaiter) {
            if pa.state == PromiseState::Pending
                && pw.state == PromiseState::Pending
                && pw.tags.contains_key("resonate:target")
            {
                self.conn.execute(
                    "INSERT OR IGNORE INTO callbacks (awaited_id, awaiter_id) VALUES (?1, ?2)",
                    params![awaited_id, awaiter_id],
                )?;
            }
        }

        // Direct resume if awaited is already settled
        if let Some(ref pa) = awaited {
            if pa.state != PromiseState::Pending {
                // Resume awaiter if suspended (version unchanged — only claim bumps version)
                let updated = self.conn.execute(
                    "UPDATE promises SET task_state = 'pending', retry_timeout_at = ?2,
                                         lease_timeout_at = NULL, ttl = NULL, pid = NULL
                     WHERE id = ?1 AND task_state = 'suspended'",
                    params![awaiter_id, time + self.task_retry_timeout],
                )?;
                if updated > 0 {
                    self.arm_retry(awaiter_id, time + self.task_retry_timeout);
                    self.emit_execute(awaiter_id)?;
                }

                // EnqueueResume #96/#97: insert ready callback for pending/acquired awaiters
                self.conn.execute(
                    "INSERT OR IGNORE INTO callbacks (awaited_id, awaiter_id, ready)
                     SELECT ?1, ?2, true
                     WHERE EXISTS (
                       SELECT 1 FROM promises WHERE id = ?2 AND task_state IN ('pending', 'acquired')
                     )",
                    params![awaited_id, awaiter_id],
                )?;
            }
        }

        Ok(RegisterCallbackResult { awaited, awaiter })
    }

    fn promise_register_listener(
        &self,
        awaited_id: &str,
        address: &str,
    ) -> StorageResult<Option<PromiseRecord>> {
        let promise = self.promise_get(awaited_id)?;
        if let Some(ref p) = promise {
            // A listener is an obligation, and the server owes an observation
            // only where someone can be blocked. Refused by the caller with a
            // 422, so nothing is written here.
            if p.state == PromiseState::Pending && resonate_core::types::is_external(&p.tags) {
                self.conn.execute(
                    "INSERT OR IGNORE INTO listeners (promise_id, address) VALUES (?1, ?2)",
                    params![awaited_id, address],
                )?;
            }
        }
        Ok(promise)
    }

    fn promise_search(
        &self,
        state: Option<&str>,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<PromiseRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at
             FROM promises
             WHERE (?1 IS NULL OR state = ?1)
               AND (?2 IS NULL OR NOT EXISTS (
                 SELECT key, value FROM json_each(?2) EXCEPT SELECT key, value FROM json_each(tags)
               ))
               AND (?3 IS NULL OR id > ?3)
             ORDER BY id ASC LIMIT ?4",
        )?;
        let mut rows = stmt.query(params![state, tags, cursor, limit])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push(row_to_promise(row)?);
        }
        Ok(results)
    }

    fn task_get(&self, id: &str) -> StorageResult<Option<TaskRecord>> {
        // `task_state IS NOT NULL` is the row's membership in what was the
        // `tasks` table; `ttl`/`pid` belong to the lease, so they read as NULL
        // for anything but an acquired task — which is what the old
        // `timeout_type = 1` guard said.
        let mut stmt = self.conn.prepare(
            "SELECT id, task_state, task_version,
                    CASE WHEN task_state = 'acquired' THEN ttl ELSE NULL END,
                    CASE WHEN task_state = 'acquired' THEN pid ELSE NULL END
             FROM promises WHERE id = ?1 AND task_state IS NOT NULL",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => {
                let task_id: String = row.get(0)?;
                let resumes = get_resumes(self.conn, &task_id)?;
                let state_str: String = row.get(1)?;
                Ok(Some(TaskRecord {
                    id: task_id,
                    state: parse_task_state(&state_str),
                    version: row.get(2)?,
                    resumes,
                    ttl: row.get(3)?,
                    pid: row.get(4)?,
                }))
            }
            None => Ok(None),
        }
    }

    fn task_create(&self, params: &TaskCreateParams) -> StorageResult<TaskCreateResult> {
        let TaskCreateParams {
            promise_id,
            state,
            param_headers,
            param_data,
            tags,
            timeout_at,
            created_at,
            settled_at,
            already_timedout,
            ttl,
            pid,
        } = *params;

        let promise_inserted = self.conn.execute(
            "INSERT OR IGNORE INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![promise_id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at],
        )? > 0;

        let promise = self
            .promise_get(promise_id)?
            .unwrap_or_else(|| unreachable!("promise missing after insert in task_create"));

        if promise_inserted {
            // task.create claims the task at birth, so the lease columns are
            // written with the state that owns them: `lease_timeout_at`/`ttl`/`pid`
            // are the type-1 timeout row, and only an acquired task has one.
            let task_state = if already_timedout {
                "fulfilled"
            } else {
                "acquired"
            };
            let task_version: i64 = if task_state == "acquired" { 1 } else { 0 };
            let inserted = if already_timedout {
                self.conn.execute(
                    "UPDATE promises SET task_state = 'fulfilled', task_version = 0
                     WHERE id = ?1 AND task_state IS NULL",
                    params![promise_id],
                )? > 0
            } else {
                self.conn.execute(
                    "UPDATE promises SET task_state = 'acquired', task_version = 1,
                                         lease_timeout_at = ?2, ttl = ?3, pid = ?4
                     WHERE id = ?1 AND task_state IS NULL",
                    params![promise_id, created_at + ttl, ttl, pid],
                )? > 0
            };
            if inserted {
                if !already_timedout {
                    self.arm_promise_timeout(
                        promise_id,
                        timeout_at,
                        resonate_core::types::is_external(&promise.tags),
                    );
                    self.arm_lease(promise_id, pid, created_at + ttl);
                }
                return Ok(TaskCreateResult {
                    promise,
                    task_created: true,
                    task_state: Some(task_state.to_string()),
                    task_version: Some(task_version),
                });
            }
        }

        // Promise already existed — do NOT acquire here.
        // The server handler will try to acquire as a separate step,
        // consistent with the PostgreSQL path.
        let task_row = self.task_get(promise_id)?;
        Ok(TaskCreateResult {
            promise,
            task_created: false,
            task_state: task_row.as_ref().map(|t| t.state.to_string()),
            task_version: task_row.as_ref().map(|t| t.version),
        })
    }

    fn task_acquire(&self, params: &TaskAcquireParams) -> StorageResult<TaskAcquireResult> {
        let TaskAcquireParams {
            task_id,
            version,
            time,
            ttl,
            pid,
        } = *params;
        // Claiming the task and taking the lease are one write now: the
        // type-0 row becomes a type-1 row by clearing `retry_timeout_at` and setting
        // `lease_timeout_at`, `ttl` and `pid`.
        let updated = self.conn.execute(
            "UPDATE promises SET task_state = 'acquired', task_version = task_version + 1,
                                 retry_timeout_at = NULL, lease_timeout_at = ?3, ttl = ?4, pid = ?5
             WHERE id = ?1 AND task_version = ?2 AND task_state = 'pending'",
            params![task_id, version, time + ttl, ttl, pid],
        )?;

        let promise = self.promise_get(task_id)?;
        let task = self.task_get(task_id)?;
        if promise.is_none() || task.is_none() {
            return Ok(TaskAcquireResult {
                promise: None,
                was_acquired: false,
                task_state: None,
                task_version: None,
            });
        }

        if updated > 0 {
            self.arm_lease(task_id, pid, time + ttl);
            // Clean up ready callbacks from previous suspension
            self.conn.execute(
                "DELETE FROM callbacks WHERE awaiter_id = ?1 AND ready = true",
                params![task_id],
            )?;
        }

        let (task_state, task_version) =
            task.map_or((None, None), |t| (Some(t.state), Some(t.version)));
        Ok(TaskAcquireResult {
            promise,
            was_acquired: updated > 0,
            task_state,
            task_version,
        })
    }

    fn task_fence_create(&self, params: &TaskFenceCreateParams) -> StorageResult<TaskFenceResult> {
        let TaskFenceCreateParams {
            task_id,
            version,
            promise_id,
            state,
            param_headers,
            param_data,
            tags,
            timeout_at,
            created_at,
            settled_at,
            already_timedout,
            address,
        } = *params;
        // Fence check
        let task = self.task_get(task_id)?;
        let task_exists = task.is_some();
        let fence_ok = task.is_some_and(|t| t.state == TaskState::Acquired && t.version == version);

        if !fence_ok {
            return Ok(TaskFenceResult {
                task_exists,
                fence_ok,
                promise: None,
            });
        }

        // Execute inner promise.create
        let result = self.promise_create(&PromiseCreateParams {
            id: promise_id,
            state,
            param_headers,
            param_data,
            tags,
            timeout_at,
            created_at,
            settled_at,
            already_timedout,
            address,
        })?;

        Ok(TaskFenceResult {
            task_exists,
            fence_ok,
            promise: Some(result.promise),
        })
    }

    fn task_fence_settle(&self, params: &TaskFenceSettleParams) -> StorageResult<TaskFenceResult> {
        let TaskFenceSettleParams {
            task_id,
            version,
            promise_id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;
        let task = self.task_get(task_id)?;
        let task_exists = task.is_some();
        let fence_ok = task.is_some_and(|t| t.state == TaskState::Acquired && t.version == version);

        if !fence_ok {
            return Ok(TaskFenceResult {
                task_exists,
                fence_ok,
                promise: None,
            });
        }

        // Execute settlement
        settle_promise(
            self,
            promise_id,
            state,
            value_headers,
            value_data,
            settled_at,
            settled_at,
            self.task_retry_timeout,
        )?;

        let promise = self.promise_get(promise_id)?;
        Ok(TaskFenceResult {
            task_exists,
            fence_ok,
            promise,
        })
    }

    fn task_heartbeat(&self, pid: &str, tasks: &[(&str, i64)], time: i64) -> StorageResult<()> {
        for &(task_id, version) in tasks {
            if task_id.is_empty() {
                continue;
            }

            // Push the lease out only if the task is acquired at the right
            // version by the right pid. The two EXISTS subqueries against
            // `tasks` are now three predicates on the row being updated.
            // The last predicate is the promise-liveness guard: a heartbeat on
            // a task whose promise is pending-but-expired is a no-op. This is
            // the one operation that does not sweep first, so without it the
            // lease would be extended in the window before the wheel reaches
            // the row.
            // RETURNING, because the new deadline is `?1 + ttl` and `ttl` is
            // a column: the caller cannot compute what was written without
            // reading it. The one statement here that needs the row back.
            let mut stmt = self.conn.prepare(
                "UPDATE promises SET lease_timeout_at = ?1 + ttl
                 WHERE id = ?2 AND pid = ?3 AND task_version = ?4 AND task_state = 'acquired'
                   AND (state != 'pending' OR timeout_at > ?1)
                 RETURNING lease_timeout_at",
            )?;
            let mut rows = stmt.query(params![time, task_id, pid, version])?;
            if let Some(row) = rows.next()? {
                let lease_timeout_at: i64 = row.get(0)?;
                self.arm_lease(task_id, pid, lease_timeout_at);
            }
        }
        Ok(())
    }

    fn task_suspend(
        &self,
        task_id: &str,
        version: i64,
        awaited_ids: &[&str],
    ) -> StorageResult<TaskSuspendResult> {
        // Check task state
        let task = self.task_get(task_id)?;
        let task_matched = task
            .as_ref()
            .is_some_and(|t| t.state == TaskState::Acquired && t.version == version);
        if !task_matched {
            return Ok(TaskSuspendResult {
                task_matched: false,
                was_suspended: false,
                missing_count: 0,
                non_awaitable_count: 0,
            });
        }

        // Check each awaited promise — count missing, non-awaitable, and settled
        let mut found_count = 0;
        let mut non_awaitable_count = 0;
        let mut has_settled = false;
        for aid in awaited_ids {
            if let Some(p) = self.promise_get(aid)? {
                found_count += 1;
                if !resonate_core::types::is_external(&p.tags) {
                    non_awaitable_count += 1;
                }
                if p.state != PromiseState::Pending {
                    has_settled = true;
                }
            }
        }

        let missing_count = awaited_ids.len() as i32 - found_count;

        // Can only suspend if: task matched, every awaited present and
        // awaitable, all pending
        let can_suspend = missing_count == 0 && non_awaitable_count == 0 && !has_settled;

        if can_suspend {
            // Clear stale ready callbacks from a prior resume before registering new ones
            self.conn.execute(
                "DELETE FROM callbacks WHERE awaiter_id = ?1 AND ready = true",
                params![task_id],
            )?;
            // Register callbacks for all awaited
            for aid in awaited_ids {
                self.conn.execute(
                    "INSERT OR IGNORE INTO callbacks (awaited_id, awaiter_id) VALUES (?1, ?2)",
                    params![aid, task_id],
                )?;
            }

            // Suspend the task. A suspended task is on neither timeout queue,
            // which is what deleting its `task_timeouts` row used to say.
            self.conn.execute(
                "UPDATE promises SET task_state = 'suspended',
                                     retry_timeout_at = NULL, lease_timeout_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ?1 AND task_version = ?2 AND task_state = 'acquired'",
                params![task_id, version],
            )?;

            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: true,
                missing_count: 0,
                non_awaitable_count: 0,
            })
        } else if missing_count == 0 && non_awaitable_count == 0 {
            // Immediate resume — has_settled is true, delete ready callbacks
            self.conn.execute(
                "DELETE FROM callbacks WHERE awaiter_id = ?1 AND ready = true",
                params![task_id],
            )?;
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count: 0,
                non_awaitable_count: 0,
            })
        } else {
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count,
                non_awaitable_count,
            })
        }
    }

    fn task_fulfill(&self, params: &TaskFulfillParams) -> StorageResult<TaskFulfillResult> {
        let TaskFulfillParams {
            task_id,
            version,
            promise_id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;
        // Fulfill the task, and with it drop the lease.
        let task_fulfilled = self.conn.execute(
            "UPDATE promises SET task_state = 'fulfilled',
                                 retry_timeout_at = NULL, lease_timeout_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_version = ?2 AND task_state = 'acquired'",
            params![task_id, version],
        )? > 0;

        if task_fulfilled {
            // Settle the promise
            settle_promise(
                self,
                promise_id,
                state,
                value_headers,
                value_data,
                settled_at,
                settled_at,
                self.task_retry_timeout,
            )?;

            // Delete callbacks where this task is the awaiter
            self.conn.execute(
                "DELETE FROM callbacks WHERE awaiter_id = ?1",
                params![task_id],
            )?;
        }

        let task_exists = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM promises WHERE id = ?1 AND task_state IS NOT NULL)",
            params![task_id],
            |r| r.get::<_, bool>(0),
        )?;
        Ok(TaskFulfillResult {
            task_exists,
            task_fulfilled,
            promise: self.promise_get(promise_id)?,
        })
    }

    fn task_release(
        &self,
        task_id: &str,
        version: i64,
        time: i64,
        ttl: i64,
    ) -> StorageResult<TaskReleaseResult> {
        // Handing the task back moves it from the lease queue to the retry
        // queue: `lease_timeout_at` out, `retry_timeout_at` in.
        let task_released = self.conn.execute(
            "UPDATE promises SET task_state = 'pending', retry_timeout_at = ?3,
                                 lease_timeout_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_version = ?2 AND task_state = 'acquired'",
            params![task_id, version, time + ttl],
        )? > 0;

        if task_released {
            self.arm_retry(task_id, time + ttl);
            self.emit_execute(task_id)?;
        }
        let task_exists = self.conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM promises WHERE id = ?1 AND task_state IS NOT NULL)",
            params![task_id],
            |r| r.get(0),
        )?;
        Ok(TaskReleaseResult {
            task_released,
            task_exists,
        })
    }

    fn task_halt(&self, task_id: &str) -> StorageResult<TaskHaltResult> {
        // Halting and dropping the timeout were two statements; they are one
        // row now. The separate DELETE was guarded on the task ending up
        // halted, which is exactly this UPDATE's own WHERE clause.
        self.conn.execute(
            "UPDATE promises SET task_state = 'halted',
                                 retry_timeout_at = NULL, lease_timeout_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_state IS NOT NULL
               AND task_state NOT IN ('fulfilled', 'halted')",
            params![task_id],
        )?;
        let row = self.conn.query_row(
            "SELECT
               EXISTS (SELECT 1 FROM promises WHERE id = ?1 AND task_state IS NOT NULL) AS task_exists,
               EXISTS (SELECT 1 FROM promises WHERE id = ?1 AND task_state = 'fulfilled') AS task_fulfilled",
            params![task_id],
            |r| Ok(TaskHaltResult {
                task_exists: r.get(0)?,
                task_fulfilled: r.get(1)?,
            }),
        )?;
        Ok(row)
    }

    fn task_continue(&self, task_id: &str, time: i64) -> StorageResult<TaskContinueResult> {
        // A halted task carries no timeout, so putting it back on the retry
        // queue is the same write that makes it pending again.
        let continued = self.conn.execute(
            "UPDATE promises SET task_state = 'pending', retry_timeout_at = ?2
             WHERE id = ?1 AND task_state = 'halted'",
            params![task_id, time + self.task_retry_timeout],
        )? > 0;

        if continued {
            self.arm_retry(task_id, time + self.task_retry_timeout);
            self.emit_execute(task_id)?;
        }

        let task_exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM promises WHERE id = ?1 AND task_state IS NOT NULL)",
            params![task_id],
            |r| r.get(0),
        )?;
        Ok(TaskContinueResult {
            task_exists,
            continued,
        })
    }

    fn task_search(
        &self,
        state: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<TaskRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT p.id, p.task_state, p.task_version,
                    CASE WHEN p.task_state = 'acquired' THEN p.ttl ELSE NULL END,
                    CASE WHEN p.task_state = 'acquired' THEN p.pid ELSE NULL END,
                    COALESCE((SELECT COUNT(*) FROM callbacks c WHERE c.awaiter_id = p.id AND c.ready = true), 0) AS resumes
             FROM promises p
             WHERE p.task_state IS NOT NULL
               AND (?1 IS NULL OR p.task_state = ?1) AND (?2 IS NULL OR p.id > ?2)
             ORDER BY p.id ASC LIMIT ?3",
        )?;
        let mut rows = stmt.query(params![state, cursor, limit])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let state_str: String = row.get(1)?;
            results.push(TaskRecord {
                id: row.get(0)?,
                state: parse_task_state(&state_str),
                version: row.get(2)?,
                ttl: row.get::<_, Option<i64>>(3).ok().flatten(),
                pid: row.get::<_, Option<String>>(4).ok().flatten(),
                resumes: row.get(5)?,
            });
        }
        Ok(results)
    }

    fn compute_preload(&self, promise_id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let branch: Option<String> = self
            .conn
            .query_row(
                "SELECT branch_id FROM promises WHERE id = ?1",
                params![promise_id],
                |r| r.get(0),
            )
            .ok()
            .flatten();
        let branch = match branch {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };
        let mut stmt = self.conn.prepare(
            "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at
             FROM promises WHERE branch_id = ?1 AND id != ?2 ORDER BY id ASC LIMIT ?3",
        )?;
        let mut rows = stmt.query(params![branch, promise_id, self.preload_limit])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push(row_to_promise(row)?);
        }
        Ok(results)
    }

    fn schedule_get(&self, id: &str) -> StorageResult<Option<ScheduleRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at FROM schedules WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_schedule(row)?)),
            None => Ok(None),
        }
    }

    fn schedule_create(&self, params: &ScheduleCreateParams) -> StorageResult<ScheduleRecord> {
        let ScheduleCreateParams {
            id,
            cron,
            promise_id,
            promise_timeout,
            promise_param_headers,
            promise_param_data,
            promise_tags,
            created_at,
            next_run_at,
        } = *params;
        let created = self.conn.execute(
            "INSERT OR IGNORE INTO schedules (id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at],
        )? > 0;
        // No schedule timeout to insert: `next_run_at` on the row above is it.
        // Only a create that actually happened arms one — an idempotent
        // re-create leaves the existing next_run_at where it was.
        if created {
            self.arm(
                next_run_at,
                Timeout::ScheduleDue {
                    schedule_id: id.to_string(),
                },
            );
        }
        Ok(self.schedule_get(id)?.unwrap())
    }

    fn schedule_delete(&self, id: &str) -> StorageResult<bool> {
        Ok(self
            .conn
            .execute("DELETE FROM schedules WHERE id = ?1", params![id])?
            > 0)
    }

    fn schedule_search(
        &self,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at
             FROM schedules WHERE (?1 IS NULL OR NOT EXISTS (
               SELECT key, value FROM json_each(?1) EXCEPT SELECT key, value FROM json_each(promise_tags)
             )) AND (?2 IS NULL OR id > ?2) ORDER BY id ASC LIMIT ?3",
        )?;
        let mut rows = stmt.query(params![tags, cursor, limit])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push(row_to_schedule(row)?);
        }
        Ok(results)
    }

    /// The nearest deadlines the tables hold, soonest first.
    ///
    /// The four queues are four columns, so this is a union of four index
    /// scans, each with the same predicate its sweep statement uses. It is the
    /// only read here that goes looking for what is armed — everything else
    /// reports what it just wrote — and it exists so a timer can fill itself
    /// after a restart rather than waiting to be told.
    ///
    /// Overdue rows are not excluded. They sort first, and a restarting timer
    /// wants exactly those.
    fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        let mut stmt = self.conn.prepare(
            "SELECT deadline, kind, id, pid FROM (
                 SELECT timeout_at AS deadline, 'promise' AS kind, id AS id, NULL AS pid
                   FROM promises WHERE state = 'pending'
                     AND (target IS NOT NULL OR is_timer
                          OR json_extract(tags, '$.resonate:scope') = 'global'
                          OR json_extract(tags, '$.resonate:external') = 'true')
                 UNION ALL
                 SELECT retry_timeout_at, 'retry', id, NULL
                   FROM promises WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL
                 UNION ALL
                 SELECT lease_timeout_at, 'lease', id, pid
                   FROM promises WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL
                 UNION ALL
                 SELECT next_run_at, 'schedule', id, NULL FROM schedules
             )
             ORDER BY deadline ASC, id ASC
             LIMIT ?1",
        )?;
        let mut rows = stmt.query(params![limit as i64])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let at: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let id: String = row.get(2)?;
            let pid: Option<String> = row.get(3)?;
            if let Some(timeout) = Timeout::from_parts(&kind, id, pid) {
                out.push(Scheduled { at, timeout });
            }
        }
        Ok(out)
    }

    fn get_expired_schedule_timeouts(
        &self,
        time: i64,
        only: Option<&str>,
    ) -> StorageResult<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, next_run_at FROM schedules
             WHERE next_run_at <= ?1 AND (?2 IS NULL OR id = ?2)
             ORDER BY id",
        )?;
        let mut rows = stmt.query(params![time, only])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let timeout_at: i64 = row.get(1)?;
            results.push((id, timeout_at));
        }
        Ok(results)
    }

    fn process_schedule_timeout(
        &self,
        schedule_id: &str,
        fired_at: i64,
        next_run_at: i64,
        time: i64,
        promise_tags: &std::collections::HashMap<String, String>,
    ) -> StorageResult<Option<ScheduleRecord>> {
        // Step 1: Guard check — idempotency. `next_run_at` is the queue, so
        // the guard reads the schedule row rather than a timeout row: a second
        // caller for the same `fired_at` finds it already advanced.
        let guard_exists: bool = self.conn.query_row(
            "SELECT COUNT(*) FROM schedules WHERE id = ?1 AND next_run_at = ?2",
            params![schedule_id, fired_at],
            |row| row.get::<_, i64>(0),
        )? > 0;
        if !guard_exists {
            return Ok(None);
        }

        // Step 2: Fetch schedule
        let schedule = match self.schedule_get(schedule_id)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Step 3: Extract resonate:target address
        let address = schedule.promise_tags.get("resonate:target").cloned();

        // Step 4: Build promise ID and timeout
        let promise_id = schedule
            .promise_id
            .replace("{{.id}}", &schedule.id)
            .replace("{{.timestamp}}", &fired_at.to_string());
        let promise_timeout_at = fired_at + schedule.promise_timeout;
        let already_timedout = time >= promise_timeout_at;
        let is_timer = promise_tags.get("resonate:timer").map(|v| v.as_str()) == Some("true");
        let (state, settled_at, created_at): (&str, Option<i64>, i64) = if already_timedout {
            let s = if is_timer {
                "resolved"
            } else {
                "rejected_timedout"
            };
            (s, Some(promise_timeout_at), fired_at)
        } else {
            ("pending", None, fired_at)
        };

        // Step 5: Create promise
        let ph = schedule
            .promise_param
            .headers
            .as_ref()
            .map(|h| serde_json::to_string(h).unwrap());
        let tags_json = serde_json::to_string(promise_tags).unwrap();
        self.conn.execute(
            "INSERT OR IGNORE INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![promise_id, state, ph, schedule.promise_param.data, tags_json, promise_timeout_at, created_at, settled_at],
        )?;
        let promise_inserted = self.conn.changes() > 0;

        if promise_inserted {
            // Step 6 is gone with `promise_timeouts`; the INSERT above already
            // put a pending, awaitable promise on the queue.
            if !already_timedout {
                self.arm_promise_timeout(
                    &promise_id,
                    promise_timeout_at,
                    resonate_core::types::is_external(promise_tags),
                );
            }
            if already_timedout {
                // Promise is immediately settled — create fulfilled task if resonate:target is set
                if address.is_some() {
                    self.conn.execute(
                        "UPDATE promises SET task_state = 'fulfilled', task_version = 0
                         WHERE id = ?1 AND task_state IS NULL",
                        params![promise_id],
                    )?;
                }
            } else if let Some(addr) = &address {
                // Step 7: Create task infrastructure if resonate:target is set
                let created = self.conn.execute(
                    "UPDATE promises SET task_state = 'pending', task_version = 0, retry_timeout_at = ?2
                     WHERE id = ?1 AND task_state IS NULL",
                    params![promise_id, time + self.task_retry_timeout],
                )? > 0;
                if created {
                    self.arm_retry(&promise_id, time + self.task_retry_timeout);
                    self.emit(Outgoing::Execute {
                        address: addr.clone(),
                        task_id: promise_id.clone(),
                        version: 0,
                    });
                }
            }
        }

        // Step 8: Advance schedule — which re-arms its own deadline
        self.conn.execute(
            "UPDATE schedules SET last_run_at = ?1, next_run_at = ?2 WHERE id = ?3",
            params![fired_at, next_run_at, schedule_id],
        )?;
        self.arm(
            next_run_at,
            Timeout::ScheduleDue {
                schedule_id: schedule_id.to_string(),
            },
        );

        // Step 9 is gone: advancing the schedule above advanced the queue.

        // Step 10: Return updated schedule
        self.schedule_get(schedule_id)
    }

    #[allow(dead_code)] // the liveness probe the server will call
    fn ping(&self) -> StorageResult<()> {
        self.conn.execute_batch("SELECT 1")?;
        Ok(())
    }

    fn debug_reset(&self) -> StorageResult<()> {
        self.conn.execute_batch(
            "DELETE FROM listeners; DELETE FROM callbacks;
             DELETE FROM promises; DELETE FROM schedules;",
        )?;
        Ok(())
    }

    /// Fire expired timeouts, either all of them or one named.
    ///
    /// `only` is what makes the precise form precise. Each statement below
    /// already selects the rows of one queue past their deadline; naming a
    /// timeout adds `AND id = ?` to the statement for its own queue and skips
    /// the other two entirely. Everything downstream — the settle, the
    /// emissions, the re-arm — is the same code on the same row, so the narrow
    /// form cannot drift from the sweep: it is the sweep, over one row.
    ///
    /// A named timeout whose deadline has moved or whose row has settled
    /// matches nothing and does nothing, which is the idempotency the port
    /// promises.
    fn process_timeouts(&self, time: i64, only: Option<&Timeout>) -> StorageResult<()> {
        let selected = |kind: &str| match only {
            None => Some(None),
            Some(t) if t.kind() == kind => Some(Some(t.id())),
            Some(_) => None,
        };

        // Statement 1: Process expired promise timeouts.
        //
        // The predicate is the whole of what `promise_timeouts` held: rows
        // entered on create and left on settle. Here it is every promise
        // anyone can block on — `is_external`'s four tags, as SQL — where
        // `engine_sqlite` restricts it to `target IS NOT NULL`. `target` and
        // `is_timer` are stored generated columns; the other two are read out
        // of `tags`, which costs nothing the partial index on
        // `(timeout_at) WHERE state = 'pending'` was not already paying.
        //
        // An `.internal` promise is still excluded, and still times out
        // lazily through `try_timeout` — the same transition, read off a
        // request instead of off the sweep.
        let expired_ids: Vec<String> = match selected("promise") {
            None => Vec::new(),
            Some(id) => {
                let mut stmt = self.conn.prepare(
                    "SELECT id FROM promises
                     WHERE state = 'pending'
                       AND (target IS NOT NULL OR is_timer
                            OR json_extract(tags, '$.resonate:scope') = 'global'
                            OR json_extract(tags, '$.resonate:external') = 'true') AND timeout_at <= ?1
                       AND (?2 IS NULL OR id = ?2)
                     ORDER BY id",
                )?;
                let mut rows = stmt.query(params![time, id])?;
                let mut r = Vec::new();
                while let Some(row) = rows.next()? {
                    r.push(row.get(0)?);
                }
                r
            }
        };

        // Phase 1: Settle all expired promises
        let mut fulfilled_ids = Vec::new();
        for id in &expired_ids {
            self.conn.execute(
                "UPDATE promises SET state = CASE WHEN is_timer THEN 'resolved' ELSE 'rejected_timedout' END, settled_at = timeout_at WHERE id = ?1 AND state = 'pending'",
                params![id],
            )?;
        }

        // Phase 2: SettlementEnqueued for all
        for id in &expired_ids {
            if settlement_enqueued(self.conn, id)? {
                fulfilled_ids.push(id.clone());
            }
        }

        // Phase 3: ResumptionEnqueued + ListenerUnblocked
        for id in &expired_ids {
            resumption_enqueued(
                self,
                id,
                time,
                self.task_retry_timeout,
                Some(&fulfilled_ids),
            )?;
            listener_unblocked(self, id)?;
        }

        // Statement 2: Process expired task retry deadlines — what was
        // `timeout_type = 0`, now a non-NULL `retry_timeout_at` on a pending task.
        let retry_ids: Vec<String> = match selected("retry") {
            None => Vec::new(),
            Some(id) => {
                let mut stmt = self.conn.prepare(
                    "SELECT id FROM promises
                     WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL AND retry_timeout_at <= ?1
                       AND (?2 IS NULL OR id = ?2)
                     ORDER BY id",
                )?;
                let mut rows = stmt.query(params![time, id])?;
                let mut r = Vec::new();
                while let Some(row) = rows.next()? {
                    r.push(row.get(0)?);
                }
                r
            }
        };

        for id in &retry_ids {
            self.conn.execute(
                "UPDATE promises SET retry_timeout_at = ?1 + ?3, pid = NULL WHERE id = ?2",
                params![time, id, self.task_retry_timeout],
            )?;
            self.arm_retry(id, time + self.task_retry_timeout);
            self.emit_execute(id)?;
        }

        // Statement 3: Process expired leases — what was `timeout_type = 1`,
        // now a non-NULL `lease_timeout_at` on an acquired task. The holder went
        // away; hand the task back to the retry queue.
        let lease_ids: Vec<String> = match selected("lease") {
            None => Vec::new(),
            Some(id) => {
                let mut stmt = self.conn.prepare(
                    "SELECT id FROM promises
                     WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL AND lease_timeout_at <= ?1
                       AND (?2 IS NULL OR id = ?2)
                     ORDER BY id",
                )?;
                let mut rows = stmt.query(params![time, id])?;
                let mut r = Vec::new();
                while let Some(row) = rows.next()? {
                    r.push(row.get(0)?);
                }
                r
            }
        };

        for id in &lease_ids {
            self.conn.execute(
                "UPDATE promises SET task_state = 'pending', retry_timeout_at = ?1 + ?3,
                                     lease_timeout_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ?2",
                params![time, id, self.task_retry_timeout],
            )?;
            self.arm_retry(id, time + self.task_retry_timeout);
            self.emit_execute(id)?;
        }

        Ok(())
    }

    fn snap(&self) -> StorageResult<Snapshot> {
        let conn = self.conn;

        let mut stmt = conn.prepare("SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises ORDER BY id")?;
        let promises: Vec<PromiseRecord> = {
            let mut rows = stmt.query([])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(row_to_promise(row)?);
            }
            r
        };

        // Every section below is a projection of the one table now. The
        // predicates are the membership rules the deleted tables carried.
        let mut stmt = conn.prepare(
            "SELECT id, timeout_at FROM promises
             WHERE state = 'pending'
               AND (target IS NOT NULL OR is_timer
                    OR json_extract(tags, '$.resonate:scope') = 'global'
                    OR json_extract(tags, '$.resonate:external') = 'true')
             ORDER BY id",
        )?;
        let promise_timeouts: Vec<SnapshotPromiseTimeout> = {
            let mut rows = stmt.query([])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(SnapshotPromiseTimeout {
                    id: row.get(0)?,
                    timeout: row.get(1)?,
                });
            }
            r
        };

        let mut stmt = conn.prepare("SELECT awaiter_id, awaited_id FROM callbacks WHERE NOT ready ORDER BY awaiter_id, awaited_id")?;
        let callbacks: Vec<SnapshotCallback> = {
            let mut rows = stmt.query([])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(SnapshotCallback {
                    awaiter: row.get(0)?,
                    awaited: row.get(1)?,
                });
            }
            r
        };

        let mut stmt =
            conn.prepare("SELECT promise_id, address FROM listeners ORDER BY promise_id, address")?;
        let listeners: Vec<SnapshotListener> = {
            let mut rows = stmt.query([])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(SnapshotListener {
                    promise_id: row.get(0)?,
                    address: row.get(1)?,
                });
            }
            r
        };

        let mut stmt = conn.prepare(
            "SELECT id, task_state, task_version,
                    CASE WHEN task_state = 'acquired' THEN ttl ELSE NULL END,
                    CASE WHEN task_state = 'acquired' THEN pid ELSE NULL END
             FROM promises WHERE task_state IS NOT NULL ORDER BY id",
        )?;
        let tasks: Vec<TaskRecord> = {
            let mut rows = stmt.query([])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                let task_id: String = row.get(0)?;
                let resumes = get_resumes(conn, &task_id)?;
                let state_str: String = row.get(1)?;
                r.push(TaskRecord {
                    id: task_id,
                    state: parse_task_state(&state_str),
                    version: row.get(2)?,
                    resumes,
                    ttl: row.get(3)?,
                    pid: row.get(4)?,
                });
            }
            r
        };

        // One row per task at most, as before: the two deadlines are mutually
        // exclusive because each is live only in the state that owns it.
        let mut stmt = conn.prepare(
            "SELECT id, 0 AS timeout_type, retry_timeout_at AS timeout_at FROM promises
               WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL
             UNION ALL
             SELECT id, 1 AS timeout_type, lease_timeout_at AS timeout_at FROM promises
               WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL
             ORDER BY id",
        )?;
        let task_timeouts: Vec<SnapshotTaskTimeout> = {
            let mut rows = stmt.query([])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(SnapshotTaskTimeout {
                    id: row.get(0)?,
                    timeout_type: row.get(1)?,
                    timeout: row.get(2)?,
                });
            }
            r
        };

        // Nothing queued, so nothing to report. `Snapshot::messages` is what a
        // backend still holding an outbox shows; here the messages left with
        // the transitions that emitted them, and the differential compares
        // those instead.
        let messages: Vec<SnapshotMessage> = Vec::new();

        Ok(Snapshot {
            promises,
            promise_timeouts,
            callbacks,
            listeners,
            tasks,
            task_timeouts,
            messages,
        })
    }
}

/// Get resumes count (number of ready callbacks) for a task
fn get_resumes(tx: &rusqlite::Connection, task_id: &str) -> rusqlite::Result<i64> {
    tx.query_row(
        "SELECT COUNT(*) FROM callbacks WHERE awaiter_id = ?1 AND ready = true",
        params![task_id],
        |row| row.get(0),
    )
}

// === Row mapping helpers ===

fn row_to_promise(row: &rusqlite::Row) -> rusqlite::Result<PromiseRecord> {
    row_to_promise_offset(row, 0)
}

fn row_to_promise_offset(row: &rusqlite::Row, offset: usize) -> rusqlite::Result<PromiseRecord> {
    let param_headers: Option<String> = row.get(offset + 2)?;
    let param_data: Option<String> = row.get(offset + 3)?;
    let value_headers: Option<String> = row.get(offset + 4)?;
    let value_data: Option<String> = row.get(offset + 5)?;
    let tags_str: String = row.get(offset + 6)?;

    let state_str: String = row.get(offset + 1)?;
    Ok(PromiseRecord {
        id: row.get(offset)?,
        state: parse_promise_state(&state_str),
        param: PromiseValue {
            headers: param_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: param_data,
        },
        value: PromiseValue {
            headers: value_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: value_data,
        },
        tags: serde_json::from_str(&tags_str).unwrap_or_default(),
        timeout_at: row.get(offset + 7)?,
        created_at: row.get(offset + 8)?,
        settled_at: row.get(offset + 9)?,
    })
}

fn row_to_schedule(row: &rusqlite::Row) -> rusqlite::Result<ScheduleRecord> {
    let param_headers: Option<String> = row.get(4)?;
    let param_data: Option<String> = row.get(5)?;
    let tags_str: String = row.get(6)?;

    Ok(ScheduleRecord {
        id: row.get(0)?,
        cron: row.get(1)?,
        promise_id: row.get(2)?,
        promise_timeout: row.get(3)?,
        promise_param: PromiseValue {
            headers: param_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: param_data,
        },
        promise_tags: serde_json::from_str(&tags_str).unwrap_or_default(),
        created_at: row.get(7)?,
        next_run_at: row.get(8)?,
        last_run_at: row.get(9)?,
    })
}

// ---------------------------------------------------------------------------
// How the four collapsed tables map onto statements here
// ---------------------------------------------------------------------------
//
//   tasks             INSERT INTO tasks (id, state) VALUES (?, 'pending')
//                       -> UPDATE promises SET task_state = 'pending' WHERE id = ?
//                     JOIN tasks t ON t.id = p.id     -> same row, drop the join
//                     t.state / t.version             -> task_state / task_version
//                     a promise with no task          -> task_state IS NULL
//
//   task_timeouts     timeout_type = 0 -> retry_timeout_at, timeout_type = 1 -> lease_timeout_at.
//                     Two nullable columns, so "which queue" is which column is
//                     non-null rather than a discriminator value. process_id and
//                     ttl became pid and ttl on the promise. Every statement that
//                     deleted the row now nulls the pair, and every statement
//                     that flipped timeout_type now writes one and clears the
//                     other — which is why fulfilling a task, dropping its
//                     timeout and clearing its lease are one UPDATE here.
//
//   promise_timeouts  Gone. The queue is `state = 'pending' AND <awaitable>`,
//                     which is what rows entering on create and leaving on
//                     settle amounted to; idx_promises_timeout_at is the
//                     index the table carried. Internal promises are not on
//                     it and never were — they time out lazily, through
//                     try_timeout.
//
//   schedule_timeouts Gone: `next_run_at` already is the queue, and
//                     process_schedule_timeout's idempotency guard reads the
//                     schedule row it is about to advance.

/// One tick of the timer wheel: the three timeout sweeps, then expired
/// schedules. Returns how many schedules fired, for the caller to record.
fn process_all_timeouts(db: &SqliteDb, time: i64) -> StorageResult<usize> {
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time, None)?;
    process_schedule_timeouts(db, time, None)
}

/// Process expired schedule timeouts.
fn process_schedule_timeouts(db: &SqliteDb, time: i64, only: Option<&str>) -> StorageResult<usize> {
    let expired = db.get_expired_schedule_timeouts(time, only)?;
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

#[async_trait]
impl ResonateEngine for SqliteEagerEngine {
    async fn process(&self, input: Input<'_>, now: i64) -> Output {
        match input {
            Input::External(req) => self.dispatch(req, now).await,
            Input::Internal(timeout) => self.fire(timeout, now).await,
        }
    }

    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        self.transact(move |db| process_all_timeouts(db, now)).await
    }

    async fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        self.query(move |db| db.upcoming(limit)).await
    }

    async fn ping(&self) -> StorageResult<()> {
        self.query(|db| db.ping()).await
    }

    fn returns_messages(&self) -> bool {
        true
    }
}

impl SqliteEagerEngine {
    /// Fire one timeout the system asked of itself.
    ///
    /// Per-timeout rather than a sweep: each variant runs only the statement
    /// that timeout belongs to, so a wheel that knows exactly what is due does
    /// exactly that much work. Every variant is a no-op if the deadline has
    /// moved or the state has changed underneath it, which is `Internal` being
    /// idempotent — the wheel and the sweep will both fire the same timeout
    /// and neither knows about the other.
    async fn fire(&self, timeout: Timeout, now: i64) -> Output {
        let swept = match timeout {
            Timeout::ScheduleDue { schedule_id } => {
                self.transact(move |db| {
                    process_schedule_timeouts(db, now, Some(&schedule_id)).map(|_| ())
                })
                .await
            }
            other => {
                self.transact(move |db| db.process_timeouts(now, Some(&other)))
                    .await
            }
        };
        match swept {
            Ok(((), messages, timeouts)) => Output {
                response: None,
                messages,
                timeouts,
            },
            Err(e) => {
                tracing::error!(error = %e, "Timeout sweep failed");
                Output::default()
            }
        }
    }
}
