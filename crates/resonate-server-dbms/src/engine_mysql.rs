//! The MySQL engine.
//!
//! A complete implementation of the protocol over MySQL: it parses and
//! validates a request, applies the transition in its own SQL, and shapes the
//! response — with no `Db` trait between the two halves and no shared engine
//! above them.
//!
//! There is no outbox. MySQL applies a transition as a sequence of statements
//! rather than one CTE, so — as in `engine_sqlite.rs` — a message is pushed
//! onto the transition's emission list at the point the statement that owes it
//! runs, and `transact` hands the list back. `engine_postgres.rs` is the one
//! that has to work for this, because there the whole transition is a single
//! statement.
//!
//! See `persistence_sqlite.rs` for what the promise row's columns replaced;
//! the collapse is the same.

use super::{
    PromiseCreateParams, PromiseCreateResult, PromiseSettleParams, PromiseSettleResult,
    RegisterCallbackResult, ScheduleCreateParams, StorageError, StorageResult, TaskAcquireParams,
    TaskAcquireResult, TaskContinueResult, TaskCreateParams, TaskCreateResult,
    TaskFenceCreateParams, TaskFenceResult, TaskFenceSettleParams, TaskFulfillParams,
    TaskFulfillResult, TaskHaltResult, TaskReleaseResult, TaskSuspendResult,
};
use async_trait::async_trait;
use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRecord,
    PromiseRegisterCallbackData, PromiseRegisterListenerData, PromiseResponseData,
    PromiseSearchData, PromiseSearchResponseData, PromiseSettleData, PromiseState, PromiseValue,
    RequestEnvelope, ResponseEnvelope, ScheduleCreateData, ScheduleDeleteData, ScheduleGetData,
    ScheduleRecord, ScheduleResponseData, ScheduleSearchData, ScheduleSearchResponseData, Snapshot,
    SnapshotCallback, SnapshotListener, SnapshotMessage, SnapshotPromiseTimeout,
    SnapshotTaskTimeout, TaskAcquireData, TaskAcquireResponseData, TaskContinueData,
    TaskCreateData, TaskCreateResponseData, TaskFenceData, TaskFenceResponseData, TaskFulfillData,
    TaskFulfillResponseData, TaskGetData, TaskHaltData, TaskHeartbeatData, TaskRecord,
    TaskReleaseData, TaskResponseData, TaskSearchData, TaskSearchResponseData, TaskState,
    TaskSuspendData, TaskSuspendPreloadData,
};
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::{MySqlPool, Row};
use std::cell::{RefCell, UnsafeCell};
use validator::Validate;

use crate::engine_port::{Input, Outgoing, Output, ResonateEngine, Scheduled, Timeout};
use resonate_core::util;

pub struct MysqlEngine {
    pool: MySqlPool,
    task_retry_timeout: i64,
    /// Whether `debug.*` operations are permitted at all.
    debug: bool,
}

const CREATE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS promises (
  id VARCHAR(255) NOT NULL,
  state VARCHAR(50) NOT NULL DEFAULT 'pending',
  param_headers LONGTEXT,
  param_data LONGTEXT,
  value_headers LONGTEXT,
  value_data LONGTEXT,
  tags LONGTEXT NOT NULL,
  target VARCHAR(255) GENERATED ALWAYS AS (tags->>'$."resonate:target"') STORED,
  origin VARCHAR(255) GENERATED ALWAYS AS (tags->>'$."resonate:origin"') STORED,
  branch VARCHAR(255) GENERATED ALWAYS AS (tags->>'$."resonate:branch"') STORED,
  timer BOOLEAN GENERATED ALWAYS AS (COALESCE(tags->>'$."resonate:timer"', '') = 'true') STORED NOT NULL,
  timeout_at BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  settled_at BIGINT,

  -- was the `tasks` table. NULL task_state means this promise has no task,
  -- which is what `LEFT JOIN tasks` used to express.
  task_state VARCHAR(50) NULL,
  task_version INT NOT NULL DEFAULT 0,

  -- was `task_timeouts`, whose timeout_type discriminated two queues.
  -- Two nullable columns say the same thing without the row.
  retry_at BIGINT NULL,
  expires_at BIGINT NULL,
  ttl BIGINT NULL,
  pid VARCHAR(255) NULL,

  PRIMARY KEY (id),
  INDEX idx_promises_timeout_at (timeout_at),
  INDEX idx_promises_target (target),
  INDEX idx_promises_branch (branch),
  -- `promise_timeouts` is gone: a pending, targeted promise past its
  -- timeout_at is exactly the queue, and idx_promises_timeout_at is the index
  -- the table carried.
  INDEX idx_promises_retry_at (retry_at ASC, id ASC),
  INDEX idx_promises_expires_at (expires_at ASC, id ASC),
  INDEX idx_promises_pid (pid),
  CONSTRAINT promises_state_check CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout')),
  CONSTRAINT promises_task_state_check CHECK (task_state IS NULL OR task_state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled'))
);

CREATE TABLE IF NOT EXISTS callbacks (
  awaited_id VARCHAR(255) NOT NULL,
  awaiter_id VARCHAR(255) NOT NULL,
  ready BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (awaited_id, awaiter_id),
  INDEX idx_callbacks_awaiter_id (awaiter_id),
  FOREIGN KEY (awaited_id) REFERENCES promises (id) ON DELETE CASCADE,
  FOREIGN KEY (awaiter_id) REFERENCES promises (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS listeners (
  promise_id VARCHAR(255) NOT NULL,
  address VARCHAR(255) NOT NULL,
  PRIMARY KEY (promise_id, address),
  FOREIGN KEY (promise_id) REFERENCES promises (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schedules (
  id VARCHAR(255) NOT NULL,
  cron TEXT NOT NULL,
  promise_id VARCHAR(255) NOT NULL,
  promise_timeout BIGINT NOT NULL,
  promise_param_headers LONGTEXT,
  promise_param_data LONGTEXT,
  promise_tags LONGTEXT NOT NULL,
  created_at BIGINT NOT NULL,
  next_run_at BIGINT NOT NULL,
  last_run_at BIGINT,
  PRIMARY KEY (id),
  -- `schedule_timeouts` is gone: next_run_at already is the queue.
  INDEX idx_schedules_next_run_at (next_run_at ASC, id ASC)
);
"#;

impl MysqlEngine {
    pub async fn connect(
        url: &str,
        pool_size: u32,
        task_retry_timeout: i64,
        debug: bool,
    ) -> Result<Self, sqlx::Error> {
        // Use READ COMMITTED so every statement sees the latest committed row version.
        // REPEATABLE READ's consistent snapshot causes promise_get to miss rows created
        // by concurrent transactions after the snapshot was established, returning 404
        // for promises that actually exist.
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(pool_size)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await?;
        Ok(Self {
            pool,
            task_retry_timeout,
            debug,
        })
    }

    pub async fn init(&self) -> Result<(), sqlx::Error> {
        for stmt in CREATE_SCHEMA_SQL.split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                sqlx::raw_sql(stmt).execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    /// Run one transition, and hand back what it emitted along with its result.
    ///
    /// A retried attempt starts with an empty list, so a message is never
    /// emitted for an attempt that did not commit.
    async fn transact<F, T>(&self, f: F) -> StorageResult<(T, Vec<Outgoing>, Vec<Scheduled>)>
    where
        F: FnMut(&MysqlDb) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        // On deadlock (1213) or lock wait timeout (1205), retry once immediately.
        // If the retry also fails, return Serialization error (maps to 503).
        let max_retries: u32 = 1;

        let mut f = f;
        for attempt in 0..=max_retries {
            #[cfg(feature = "concurrency-stress")]
            tokio::task::yield_now().await;

            let tx = self.pool.begin().await.map_err(StorageError::from)?;

            let task_retry_timeout = self.task_retry_timeout;
            let (result, emitted, armed, tx) = tokio::task::block_in_place(|| {
                let db = MysqlDb {
                    tx: UnsafeCell::new(tx),
                    task_retry_timeout,
                    emitted: RefCell::new(Vec::new()),
                    armed: RefCell::new(Vec::new()),
                };

                #[cfg(feature = "concurrency-stress")]
                {
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .subsec_nanos();
                    std::thread::sleep(std::time::Duration::from_micros((nanos % 1000) as u64 + 1));
                }

                let result = f(&db);
                let emitted = db.emitted.into_inner();
                let armed = db.armed.into_inner();
                let tx = db.tx.into_inner();
                (result, emitted, armed, tx)
            });

            // If business logic failed with a serialization error, retry.
            // Other errors propagate immediately (tx is dropped → auto-rollback).
            let result = match result {
                Ok(v) => v,
                Err(StorageError::Serialization) => {
                    if attempt < max_retries {
                        tracing::warn!(
                            attempt = attempt + 1,
                            "Serialization failure (1213/1205) in query, retrying"
                        );
                        continue;
                    } else {
                        tracing::warn!(
                            "Serialization failure (1213/1205) in query after retry, returning 503"
                        );
                        return Err(StorageError::Serialization);
                    }
                }
                Err(e) => return Err(e),
            };

            match tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(tx.commit())
            }) {
                Ok(_) => return Ok((result, emitted, armed)),
                Err(e) => {
                    let mysql_err = e
                        .as_database_error()
                        .and_then(|dbe| dbe.code().map(|c| c.to_string()));
                    if mysql_err.as_deref() == Some("1213") || mysql_err.as_deref() == Some("1205")
                    {
                        if attempt < max_retries {
                            tracing::warn!(
                                attempt = attempt + 1,
                                "Serialization failure (1213/1205) at commit, retrying"
                            );
                            continue;
                        } else {
                            tracing::warn!(
                                "Serialization failure (1213/1205) at commit after retry, returning 503"
                            );
                            return Err(StorageError::Serialization);
                        }
                    }
                    return Err(StorageError::from(e));
                }
            }
        }

        unreachable!("transact loop completed without returning")
    }

    /// One operation: run it, and turn a storage failure into a response.
    ///
    /// Same tail for all 21, so it lives here once. `InvalidInput` maps to 400
    /// — MySQL raises it for an id past VARCHAR(255).
    async fn run<F>(&self, req: &RequestEnvelope, f: F) -> Output
    where
        F: FnMut(&MysqlDb) -> StorageResult<ResponseEnvelope> + Send + 'static,
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
            Err(StorageError::Serialization) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                503,
                "Serialization failure, please retry",
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
        F: FnMut(&MysqlDb) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.transact(f).await.map(|(v, _, _)| v)
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

/// Wraps a MySQL transaction for use within the synchronous `Db` trait.
///
/// Uses `UnsafeCell` for interior mutability: `Db` trait methods take `&self`,
/// but `sqlx` requires `&mut Transaction` for query execution. This is safe because:
/// - `MysqlDb` is created and dropped within a single `block_in_place` call
/// - Only one `&MysqlDb` reference exists at a time (no aliasing)
/// - The `UnsafeCell` is never accessed concurrently
struct MysqlDb<'a> {
    tx: UnsafeCell<sqlx::Transaction<'a, sqlx::MySql>>,
    task_retry_timeout: i64,
    /// What this transition has emitted so far. See `engine_sqlite.rs`.
    emitted: RefCell<Vec<Outgoing>>,
    /// What deadlines this transition armed or moved. A hint; see
    /// `engine_sqlite.rs`.
    armed: RefCell<Vec<Scheduled>>,
}

impl<'a> MysqlDb<'a> {
    /// Returns a mutable reference to the underlying transaction.
    ///
    /// # Safety
    /// Safe because `MysqlDb` is only used within a single synchronous closure
    /// in `transact()` — there is no concurrent or aliased access to the `UnsafeCell`.
    #[allow(clippy::mut_from_ref)]
    fn tx(&self) -> &mut sqlx::Transaction<'a, sqlx::MySql> {
        unsafe { &mut *self.tx.get() }
    }

    /// Queue a message for the caller of `process`.
    fn emit(&self, message: Outgoing) {
        self.emitted.borrow_mut().push(message);
    }

    /// Report a deadline this transition just wrote.
    fn arm(&self, at: i64, timeout: Timeout) {
        self.armed.borrow_mut().push(Scheduled { at, timeout });
    }

    fn arm_promise_timeout(&self, promise_id: &str, timeout_at: i64, targeted: bool) {
        if targeted {
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

    /// Emit an unblock message carrying the promise as it now stands.
    ///
    /// The outbox stored `(promise_id, address)` and joined back to `promises`
    /// at delivery time; captured here instead, inside the settlement that just
    /// made the promise final.
    fn emit_unblock(&self, promise_id: &str, address: &str) -> StorageResult<()> {
        let row = rt_block_on(
            sqlx::query(
                "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?",
            )
            .bind(promise_id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        if let Some(row) = row {
            self.emit(Outgoing::Unblock {
                address: address.to_string(),
                promise: row_to_promise(&row),
            });
        }
        Ok(())
    }

    /// Emit an execute message for `task_id`, if it has somewhere to go.
    fn emit_execute(&self, task_id: &str) -> StorageResult<()> {
        let row = rt_block_on(
            sqlx::query("SELECT task_version, target FROM promises WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        if let Some(row) = row {
            if let Some(address) = row.get::<Option<String>, _>("target") {
                self.emit(Outgoing::Execute {
                    address,
                    task_id: task_id.to_string(),
                    version: row.get::<i32, _>("task_version") as i64,
                });
            }
        }
        Ok(())
    }

    /// Marks the owning task fulfilled, drops its timeout, and deletes callbacks
    /// registered by it (as awaiter). Used when a promise that owns a task is settled.
    ///
    /// The fulfil and the timeout delete were two statements against two
    /// tables; they are one row now, so they are one `SET` with a `CASE` — the
    /// deadline columns clear unconditionally, exactly as the `DELETE` did,
    /// while `task_state` moves only when it is a task that is not yet
    /// fulfilled.
    fn settlement_enqueued(&self, task_id: &str) -> StorageResult<()> {
        rt_block_on(
            sqlx::query(
                "UPDATE promises SET
                   task_state = CASE WHEN task_state IS NOT NULL AND task_state != 'fulfilled'
                                THEN 'fulfilled' ELSE task_state END,
                   retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ?",
            )
            .bind(task_id)
            .execute(self.tx().as_mut()),
        )?;
        rt_block_on(
            sqlx::query("DELETE FROM callbacks WHERE awaiter_id = ?")
                .bind(task_id)
                .execute(self.tx().as_mut()),
        )?;
        Ok(())
    }

    /// Finds all suspended tasks waiting on `awaited_id` via not-yet-ready callbacks,
    /// marks those callbacks ready, resumes the tasks, and queues outgoing execute
    /// messages and task timeouts.
    fn resumption_enqueued(&self, awaited_id: &str, time: i64) -> StorageResult<()> {
        let trt = self.task_retry_timeout;

        // Snapshot resumed tasks BEFORE marking callbacks ready. The task is
        // on the promise row now, so the join is to `promises`.
        let rows = rt_block_on(
            sqlx::query(
                "SELECT p.id, p.task_version AS version FROM promises p
                 JOIN callbacks c ON c.awaiter_id = p.id
                 WHERE c.awaited_id = ? AND c.ready = false AND p.task_state = 'suspended'",
            )
            .bind(awaited_id)
            .fetch_all(self.tx().as_mut()),
        )?;

        // Mark all callbacks for this promise as ready
        rt_block_on(
            sqlx::query("UPDATE callbacks SET ready = true WHERE awaited_id = ?")
                .bind(awaited_id)
                .execute(self.tx().as_mut()),
        )?;

        for row in &rows {
            let task_id: String = row.get("id");

            // Resuming and re-arming the retry deadline are one write: writing
            // `retry_at` and clearing `expires_at` is what flipping the
            // timeout row from type 1 to type 0 used to be.
            rt_block_on(
                sqlx::query(
                    "UPDATE promises SET task_state = 'pending', retry_at = ?,
                                         expires_at = NULL, ttl = NULL, pid = NULL
                     WHERE id = ? AND task_state = 'suspended'",
                )
                .bind(time + trt)
                .bind(&task_id)
                .execute(self.tx().as_mut()),
            )?;

            self.arm_retry(&task_id, time + trt);
            self.emit_execute(&task_id)?;
        }
        Ok(())
    }

    /// Settles a list of already-updated promise IDs and runs the full cascade:
    /// delete promise timeouts, fulfill owning tasks, mark callbacks ready,
    /// resume suspended tasks, notify listeners.
    fn batch_settle_cascade(&self, expired_ids: &[String], time: i64) -> StorageResult<()> {
        if expired_ids.is_empty() {
            return Ok(());
        }
        let trt = self.task_retry_timeout;
        let ph = |n: usize| -> String { vec!["?"; n].join(", ") };
        let n = expired_ids.len();

        // Nothing to delete from `promise_timeouts`: the settling UPDATE the
        // caller has already run took these rows out of the queue.

        // Fulfill owning tasks and drop their timeouts — same row, so the
        // three statements this replaces are one `SET`.
        {
            let sql = format!(
                "UPDATE promises SET
                   task_state = CASE WHEN task_state IS NOT NULL AND task_state != 'fulfilled'
                                THEN 'fulfilled' ELSE task_state END,
                   retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
                 WHERE id IN ({})",
                ph(n)
            );
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        // Delete callbacks where expired task is the awaiter
        {
            let sql = format!("DELETE FROM callbacks WHERE awaiter_id IN ({})", ph(n));
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        // Snapshot suspended tasks waiting on any expired promise (exclude newly-fulfilled tasks)
        let resumed_rows = {
            let sql = format!(
                "SELECT p.id, p.task_version AS version FROM promises p
                 JOIN callbacks c ON c.awaiter_id = p.id
                 WHERE c.awaited_id IN ({}) AND c.ready = false AND p.task_state = 'suspended'
                   AND p.id NOT IN ({})",
                ph(n),
                ph(n)
            );
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.fetch_all(self.tx().as_mut()))?
        };

        // Mark callbacks ready (exclude awaiter_ids that are now fulfilled)
        {
            let sql = format!(
                "UPDATE callbacks SET ready = true
                 WHERE awaited_id IN ({}) AND awaiter_id NOT IN ({})",
                ph(n),
                ph(n)
            );
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        // Resume each suspended task
        for row in &resumed_rows {
            let task_id: String = row.get("id");

            rt_block_on(
                sqlx::query(
                    "UPDATE promises SET task_state = 'pending', retry_at = ?,
                                         expires_at = NULL, ttl = NULL, pid = NULL
                     WHERE id = ? AND task_state = 'suspended'",
                )
                .bind(time + trt)
                .bind(&task_id)
                .execute(self.tx().as_mut()),
            )?;
            self.arm_retry(&task_id, time + trt);
            self.emit_execute(&task_id)?;
        }

        // Collect and insert outgoing unblock messages, then delete listeners
        {
            let sql = format!(
                "SELECT promise_id, address FROM listeners WHERE promise_id IN ({})",
                ph(n)
            );
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            let listener_rows = rt_block_on(q.fetch_all(self.tx().as_mut()))?;

            for row in &listener_rows {
                let pid: String = row.get("promise_id");
                let addr: String = row.get("address");
                self.emit_unblock(&pid, &addr)?;
            }
        }

        {
            let sql = format!("DELETE FROM listeners WHERE promise_id IN ({})", ph(n));
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        Ok(())
    }

    /// Queues outgoing unblock messages for all listeners on the promise,
    /// then deletes those listeners.
    fn listener_unblocked(&self, promise_id: &str) -> StorageResult<()> {
        let listeners = rt_block_on(
            sqlx::query("SELECT address FROM listeners WHERE promise_id = ?")
                .bind(promise_id)
                .fetch_all(self.tx().as_mut()),
        )?;

        for row in &listeners {
            let address: String = row.get("address");
            self.emit_unblock(promise_id, &address)?;
        }

        rt_block_on(
            sqlx::query("DELETE FROM listeners WHERE promise_id = ?")
                .bind(promise_id)
                .execute(self.tx().as_mut()),
        )?;
        Ok(())
    }
}

fn rt_block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

fn parse_promise_state(s: &str) -> PromiseState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt promise state in DB: {}", e))
}

fn parse_task_state(s: &str) -> TaskState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt task state in DB: {}", e))
}

fn row_to_promise(row: &MySqlRow) -> PromiseRecord {
    let param_headers: Option<String> = row.get("param_headers");
    let value_headers: Option<String> = row.get("value_headers");
    let tags_str: String = row.get("tags");
    let state_str: String = row.get("state");

    PromiseRecord {
        id: row.get("id"),
        state: parse_promise_state(&state_str),
        param: PromiseValue {
            headers: param_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: row.get("param_data"),
        },
        value: PromiseValue {
            headers: value_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: row.get("value_data"),
        },
        tags: serde_json::from_str(&tags_str).unwrap_or_default(),
        timeout_at: row.get("timeout_at"),
        created_at: row.get("created_at"),
        settled_at: row.get("settled_at"),
    }
}

fn row_to_schedule(row: &MySqlRow) -> ScheduleRecord {
    let param_headers: Option<String> = row.get("promise_param_headers");
    let tags_str: String = row.get("promise_tags");

    ScheduleRecord {
        id: row.get("id"),
        cron: row.get("cron"),
        promise_id: row.get("promise_id"),
        promise_timeout: row.get("promise_timeout"),
        promise_param: PromiseValue {
            headers: param_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: row.get("promise_param_data"),
        },
        promise_tags: serde_json::from_str(&tags_str).unwrap_or_default(),
        created_at: row.get("created_at"),
        next_run_at: row.get("next_run_at"),
        last_run_at: row.get("last_run_at"),
    }
}

#[allow(dead_code)]
fn row_to_task(row: &MySqlRow) -> TaskRecord {
    let resumes: i32 = row.get("resumes");
    TaskRecord {
        id: row.get("id"),
        state: parse_task_state(&row.get::<String, _>("state")),
        version: row.get::<i32, _>("version") as i64,
        resumes: resumes as i64,
        ttl: row.get("ttl"),
        pid: row.get("pid"),
    }
}

// ============================================================================
// Db implementation — stubbed; every method returns todo!()
// ============================================================================

impl MysqlDb<'_> {
    fn task_retry_timeout(&self) -> i64 {
        self.task_retry_timeout
    }

    fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let ph = |n: usize| -> String { vec!["?"; n].join(", ") };
        let n = ids.len();

        // Find which of the given promises are expired and still pending
        let expired_rows = {
            let sql = format!(
                "SELECT id FROM promises WHERE id IN ({}) AND state = 'pending' AND timeout_at <= ?",
                ph(n)
            );
            let mut q = sqlx::query(&sql);
            for id in &ids {
                q = q.bind(id.as_str());
            }
            q = q.bind(time);
            rt_block_on(q.fetch_all(self.tx().as_mut()))?
        };

        if expired_rows.is_empty() {
            return Ok(());
        }

        let expired_ids: Vec<String> = expired_rows
            .iter()
            .map(|r| r.get::<String, _>("id"))
            .collect();
        let m = expired_ids.len();

        // Settle them
        {
            let sql = format!(
                "UPDATE promises
                 SET state = CASE WHEN timer THEN 'resolved' ELSE 'rejected_timedout' END,
                     settled_at = timeout_at
                 WHERE id IN ({}) AND state = 'pending' AND timeout_at <= ?",
                ph(m)
            );
            let mut q = sqlx::query(&sql);
            for id in &expired_ids {
                q = q.bind(id.as_str());
            }
            q = q.bind(time);
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        self.batch_settle_cascade(&expired_ids, time)
    }

    fn lock_for_update(&self, id: &str) -> StorageResult<(bool, bool)> {
        // One row, so one lock: the promise and its task were never separately
        // lockable in the first place.
        let row = rt_block_on(
            sqlx::query(
                "SELECT (task_state IS NOT NULL) AS has_task FROM promises WHERE id = ? FOR UPDATE",
            )
            .bind(id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        match row {
            Some(r) => Ok((true, r.get::<i8, _>("has_task") != 0)),
            None => Ok((false, false)),
        }
    }

    fn process_callbacks(&self, promise_id: &str, time: i64) -> StorageResult<()> {
        let settled = rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ? AND state != 'pending'")
                .bind(promise_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        if settled.is_some() {
            self.resumption_enqueued(promise_id, time)?;
        }
        Ok(())
    }

    fn promise_get(&self, id: &str) -> StorageResult<Option<PromiseRecord>> {
        let row = rt_block_on(
            sqlx::query(
                "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(row.as_ref().map(row_to_promise))
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
        if id.len() > 255 {
            return Err(StorageError::InvalidInput(
                "id exceeds maximum length of 255 characters".to_string(),
            ));
        }
        let trt = self.task_retry_timeout;

        let res = rt_block_on(
            sqlx::query(
                "INSERT IGNORE INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(state)
            .bind(param_headers)
            .bind(param_data)
            .bind(tags)
            .bind(timeout_at)
            .bind(created_at)
            .bind(settled_at)
            .execute(self.tx().as_mut()),
        )?;

        let was_created = res.rows_affected() > 0;
        if was_created {
            // No promise timeout to write: `state = 'pending' AND target IS NOT
            // NULL` is the queue, and the INSERT above already put the row in
            // it. Creating the task is an UPDATE of that same row, and
            // `task_state IS NULL` is the guard `INSERT IGNORE INTO tasks`
            // used to be — a promise carries at most one task.
            if let Some(addr) = address {
                let task_state = if already_timedout {
                    "fulfilled"
                } else {
                    "pending"
                };
                let task_res = rt_block_on(
                    sqlx::query(
                        "UPDATE promises SET task_state = ?, task_version = 0,
                                             retry_at = CASE WHEN ? THEN NULL ELSE ? END
                         WHERE id = ? AND task_state IS NULL",
                    )
                    .bind(task_state)
                    .bind(already_timedout)
                    .bind(created_at + trt)
                    .bind(id)
                    .execute(self.tx().as_mut()),
                )?;

                if task_res.rows_affected() > 0 && !already_timedout {
                    self.arm_promise_timeout(id, timeout_at, true);
                    self.arm_retry(id, created_at + trt);
                    self.emit(Outgoing::Execute {
                        address: addr.to_string(),
                        task_id: id.to_string(),
                        version: 0,
                    });
                }
            }
        }

        // Return canonical record (INSERT IGNORE is idempotent — always SELECT to get state)
        let promise = self.promise_get(id)?.ok_or_else(|| {
            StorageError::Backend(format!("promise not found after create: {}", id))
        })?;
        Ok(PromiseCreateResult {
            was_created,
            promise,
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

        // Statement 1: acquire lock — blocks concurrent task.suspend etc.
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ? FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 2: try to settle
        let res = rt_block_on(
            sqlx::query(
                "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ?
                 WHERE id = ? AND state = 'pending'",
            )
            .bind(state)
            .bind(value_headers)
            .bind(value_data)
            .bind(settled_at)
            .bind(id)
            .execute(self.tx().as_mut()),
        )?;

        let was_settled = res.rows_affected() > 0;

        if was_settled {
            // No promise timeout to delete — the UPDATE above took this row
            // out of the queue.
            // Fulfill owning task (same id), drop its timeout, delete its callbacks-as-awaiter
            self.settlement_enqueued(id)?;
            // Mark callbacks-as-awaited ready, resume suspended tasks, queue outgoing
            self.resumption_enqueued(id, settled_at)?;
            // Notify listeners
            self.listener_unblocked(id)?;
        }

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
        let trt = self.task_retry_timeout;

        // Lock both promises (ORDER BY id for consistent lock ordering to prevent deadlocks)
        let rows = rt_block_on(
            sqlx::query(
                "SELECT id, state, target, tags FROM promises WHERE id IN (?, ?) ORDER BY id FOR UPDATE",
            )
            .bind(awaited_id)
            .bind(awaiter_id)
            .fetch_all(self.tx().as_mut()),
        )?;

        let mut awaited_state: Option<String> = None;
        let mut awaiter_state: Option<String> = None;
        let mut awaiter_target: Option<String> = None;
        let mut awaited_awaitable = false;

        for row in &rows {
            let rid: String = row.get("id");
            let rstate: String = row.get("state");
            let rtarget: Option<String> = row.get("target");
            if rid == awaited_id {
                awaited_state = Some(rstate);
                let tags: String = row.get("tags");
                awaited_awaitable =
                    serde_json::from_str::<std::collections::HashMap<String, String>>(&tags)
                        .map(|t| resonate_core::types::is_external(&t))
                        .unwrap_or(false);
            } else {
                awaiter_state = Some(rstate);
                awaiter_target = rtarget;
            }
        }

        // An awaited that may not be awaited is refused by the caller with a
        // 422, so neither arm below may run: not the link, and not the direct
        // resume, which would wake the awaiter for a registration that never
        // happened.
        let arms = if awaited_awaitable {
            (awaited_state.clone(), awaiter_state.clone())
        } else {
            (None, None)
        };
        match (&arms.0, &arms.1) {
            (Some(as_), Some(aw_))
                if as_ == "pending" && aw_ == "pending" && awaiter_target.is_some() =>
            {
                // Insert callback if awaited is pending and awaiter is a runnable task-promise
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO callbacks (awaited_id, awaiter_id) VALUES (?, ?)",
                    )
                    .bind(awaited_id)
                    .bind(awaiter_id)
                    .execute(self.tx().as_mut()),
                )?;
            }
            (Some(as_), _) if as_ != "pending" => {
                // Awaited already settled — directly resume the awaiter task if suspended
                let upd = rt_block_on(
                    sqlx::query(
                        "UPDATE promises SET task_state = 'pending', retry_at = ?,
                                             expires_at = NULL, ttl = NULL, pid = NULL
                         WHERE id = ? AND task_state = 'suspended'",
                    )
                    .bind(time + trt)
                    .bind(awaiter_id)
                    .execute(self.tx().as_mut()),
                )?;
                // Only enqueue the execute message if the task was actually transitioned
                if upd.rows_affected() > 0 {
                    self.arm_retry(awaiter_id, time + trt);
                    self.emit_execute(awaiter_id)?;
                }

                // EnqueueResume #96/#97: insert ready callback for pending/acquired awaiters
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO callbacks (awaited_id, awaiter_id, ready)
                         SELECT ?, ?, true FROM promises
                         WHERE id = ? AND task_state IN ('pending', 'acquired')",
                    )
                    .bind(awaited_id)
                    .bind(awaiter_id)
                    .bind(awaiter_id)
                    .execute(self.tx().as_mut()),
                )?;
            }
            _ => {}
        }

        Ok(RegisterCallbackResult {
            awaited: self.promise_get(awaited_id)?,
            awaiter: self.promise_get(awaiter_id)?,
        })
    }

    fn promise_register_listener(
        &self,
        awaited_id: &str,
        address: &str,
    ) -> StorageResult<Option<PromiseRecord>> {
        let row = rt_block_on(
            sqlx::query("SELECT id, state FROM promises WHERE id = ? FOR UPDATE")
                .bind(awaited_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        let promise_state: Option<String> = row.as_ref().map(|r| r.get("state"));

        if promise_state.as_deref() == Some("pending") {
            rt_block_on(
                sqlx::query("INSERT IGNORE INTO listeners (promise_id, address) VALUES (?, ?)")
                    .bind(awaited_id)
                    .bind(address)
                    .execute(self.tx().as_mut()),
            )?;
        }

        self.promise_get(awaited_id)
    }

    fn promise_search(
        &self,
        state: Option<&str>,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<PromiseRecord>> {
        let rows = rt_block_on(
            sqlx::query(
                "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at
                 FROM promises
                 WHERE (? IS NULL OR state = ?)
                   AND (? IS NULL OR JSON_CONTAINS(tags, ?))
                   AND (? IS NULL OR id > ?)
                 ORDER BY id ASC
                 LIMIT ?",
            )
            .bind(state).bind(state)
            .bind(tags).bind(tags)
            .bind(cursor).bind(cursor)
            .bind(limit)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    fn task_get(&self, id: &str) -> StorageResult<Option<TaskRecord>> {
        let row = rt_block_on(
            sqlx::query(
                "SELECT p.id, p.task_state AS state, p.task_version AS version,
                   CASE WHEN p.task_state = 'acquired' THEN p.ttl ELSE NULL END AS ttl,
                   CASE WHEN p.task_state = 'acquired' THEN p.pid ELSE NULL END AS pid,
                   COALESCE(
                     (SELECT CAST(COUNT(*) AS SIGNED) FROM callbacks c WHERE c.awaiter_id = p.id AND c.ready = true),
                     0
                   ) AS resumes
                 FROM promises p WHERE p.id = ? AND p.task_state IS NOT NULL",
            )
            .bind(id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        match row {
            Some(r) => {
                let resumes: i64 = r.get("resumes");
                Ok(Some(TaskRecord {
                    id: r.get("id"),
                    state: parse_task_state(&r.get::<String, _>("state")),
                    version: r.get::<i32, _>("version") as i64,
                    resumes,
                    ttl: r.get("ttl"),
                    pid: r.get("pid"),
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
        if promise_id.len() > 255 {
            return Err(StorageError::InvalidInput(
                "id exceeds maximum length of 255 characters".to_string(),
            ));
        }
        // No retry deadline here: task.create claims the task at birth, so it
        // goes straight onto the lease queue.
        let task_initial_state = if already_timedout {
            "fulfilled"
        } else {
            "acquired"
        };
        let task_initial_version: i32 = if already_timedout { 0 } else { 1 };

        // Insert promise (idempotent)
        let promise_res = rt_block_on(
            sqlx::query(
                "INSERT IGNORE INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            ).bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags)
             .bind(timeout_at).bind(created_at).bind(settled_at)
             .execute(self.tx().as_mut())
        )?;
        let promise_inserted = promise_res.rows_affected() > 0;

        let mut task_created = false;

        if promise_inserted {
            // task.create claims the task at birth, so the lease columns are
            // written with the state that owns them — no intermediate retry
            // deadline to insert and then upgrade.
            let task_res = rt_block_on(
                sqlx::query(
                    "UPDATE promises SET task_state = ?, task_version = ?,
                                         expires_at = CASE WHEN ? THEN NULL ELSE ? END,
                                         ttl = CASE WHEN ? THEN NULL ELSE ? END,
                                         pid = CASE WHEN ? THEN NULL ELSE ? END
                     WHERE id = ? AND task_state IS NULL",
                )
                .bind(task_initial_state)
                .bind(task_initial_version)
                .bind(already_timedout)
                .bind(created_at + ttl)
                .bind(already_timedout)
                .bind(ttl)
                .bind(already_timedout)
                .bind(pid)
                .bind(promise_id)
                .execute(self.tx().as_mut()),
            )?;
            task_created = task_res.rows_affected() > 0;
        }

        let promise = self
            .promise_get(promise_id)?
            .unwrap_or_else(|| unreachable!("promise missing after insert in task_create"));

        if task_created {
            if !already_timedout {
                self.arm_promise_timeout(
                    promise_id,
                    timeout_at,
                    promise.tags.contains_key("resonate:target"),
                );
                self.arm_lease(promise_id, pid, created_at + ttl);
            }
            return Ok(TaskCreateResult {
                promise,
                task_created: true,
                task_state: Some(task_initial_state.to_string()),
                task_version: Some(task_initial_version as i64),
            });
        }

        let task_row = rt_block_on(
            sqlx::query(
                "SELECT task_state, task_version FROM promises WHERE id = ? AND task_state IS NOT NULL",
            )
            .bind(promise_id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(TaskCreateResult {
            promise,
            task_created: false,
            task_state: task_row.as_ref().map(|r| r.get::<String, _>("task_state")),
            task_version: task_row
                .as_ref()
                .map(|r| r.get::<i32, _>("task_version") as i64),
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

        // Claiming the task and taking the lease are one write now: the type-0
        // deadline becomes a type-1 one by clearing `retry_at` and setting
        // `expires_at`, `ttl` and `pid`.
        let res = rt_block_on(
            sqlx::query(
                "UPDATE promises SET task_state = 'acquired', task_version = task_version + 1,
                                     retry_at = NULL, expires_at = ?, ttl = ?, pid = ?
                 WHERE id = ? AND task_version = ? AND task_state = 'pending'",
            )
            .bind(time + ttl)
            .bind(ttl)
            .bind(pid)
            .bind(task_id)
            .bind(version as i32)
            .execute(self.tx().as_mut()),
        )?;
        let was_acquired = res.rows_affected() > 0;

        if was_acquired {
            self.arm_lease(task_id, pid, time + ttl);
            rt_block_on(
                sqlx::query("DELETE FROM callbacks WHERE awaiter_id = ? AND ready = true")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;
        }

        let promise = rt_block_on(
            sqlx::query(
                "SELECT p.id, p.state, p.param_headers, p.param_data, p.value_headers, p.value_data,
                        p.tags, p.timeout_at, p.created_at, p.settled_at
                 FROM promises p WHERE p.id = ? AND p.task_state IS NOT NULL"
            ).bind(task_id).fetch_optional(self.tx().as_mut())
        )?;
        let task_row = rt_block_on(
            sqlx::query(
                "SELECT task_state, task_version FROM promises WHERE id = ? AND task_state IS NOT NULL",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        let task_state = task_row
            .as_ref()
            .map(|r| parse_task_state(&r.get::<String, _>("task_state")));
        let task_version = task_row
            .as_ref()
            .map(|r| r.get::<i32, _>("task_version") as i64);

        Ok(TaskAcquireResult {
            promise: promise.as_ref().map(row_to_promise),
            was_acquired,
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
        let trt = self.task_retry_timeout;

        let task_row = rt_block_on(
            sqlx::query(
                "SELECT id, task_state, task_version FROM promises WHERE id = ? AND task_state IS NOT NULL",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;

        let task_exists = task_row.is_some();
        let fence_ok = task_row.as_ref().is_some_and(|r| {
            let s: String = r.get("task_state");
            let v: i32 = r.get("task_version");
            s == "acquired" && v == version as i32
        });

        let promise = if fence_ok {
            let res = rt_block_on(
                sqlx::query(
                    "INSERT IGNORE INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                ).bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags)
                 .bind(timeout_at).bind(created_at).bind(settled_at)
                 .execute(self.tx().as_mut())
            )?;

            if res.rows_affected() > 0 {
                if let Some(addr) = address {
                    let task_state = if already_timedout {
                        "fulfilled"
                    } else {
                        "pending"
                    };
                    let task_res = rt_block_on(
                        sqlx::query(
                            "UPDATE promises SET task_state = ?, task_version = 0,
                                                 retry_at = CASE WHEN ? THEN NULL ELSE ? END
                             WHERE id = ? AND task_state IS NULL",
                        )
                        .bind(task_state)
                        .bind(already_timedout)
                        .bind(created_at + trt)
                        .bind(promise_id)
                        .execute(self.tx().as_mut()),
                    )?;
                    if task_res.rows_affected() > 0 && !already_timedout {
                        self.arm_promise_timeout(promise_id, timeout_at, true);
                        self.arm_retry(promise_id, created_at + trt);
                        self.emit(Outgoing::Execute {
                            address: addr.to_string(),
                            task_id: promise_id.to_string(),
                            version: 0,
                        });
                    }
                }
            }
            self.promise_get(promise_id)?
        } else {
            self.promise_get(promise_id)?
        };

        Ok(TaskFenceResult {
            task_exists,
            fence_ok,
            promise,
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

        let task_row = rt_block_on(
            sqlx::query(
                "SELECT id, task_state, task_version FROM promises WHERE id = ? AND task_state IS NOT NULL",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;

        let task_exists = task_row.is_some();
        let fence_ok = task_row.as_ref().is_some_and(|r| {
            let s: String = r.get("task_state");
            let v: i32 = r.get("task_version");
            s == "acquired" && v == version as i32
        });

        if fence_ok {
            // Lock the promise
            rt_block_on(
                sqlx::query("SELECT id FROM promises WHERE id = ? FOR UPDATE")
                    .bind(promise_id)
                    .fetch_optional(self.tx().as_mut()),
            )?;

            let res = rt_block_on(
                sqlx::query(
                    "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ?
                     WHERE id = ? AND state = 'pending'"
                ).bind(state).bind(value_headers).bind(value_data).bind(settled_at).bind(promise_id)
                 .execute(self.tx().as_mut())
            )?;

            if res.rows_affected() > 0 {
                self.settlement_enqueued(promise_id)?;
                self.resumption_enqueued(promise_id, settled_at)?;
                self.listener_unblocked(promise_id)?;
            }
        }

        Ok(TaskFenceResult {
            task_exists,
            fence_ok,
            promise: self.promise_get(promise_id)?,
        })
    }

    fn task_heartbeat(&self, pid: &str, tasks: &[(&str, i64)], time: i64) -> StorageResult<()> {
        for (task_id, version) in tasks {
            // The three-table join collapses: the lease, the task and the
            // promise it guards are all this one row.
            let res = rt_block_on(
                sqlx::query(
                    "UPDATE promises SET expires_at = ? + ttl
                     WHERE id = ? AND task_version = ? AND task_state = 'acquired' AND pid = ?
                       AND (state != 'pending' OR timeout_at > ?)",
                )
                .bind(time)
                .bind(task_id)
                .bind(*version as i32)
                .bind(pid)
                .bind(time)
                .execute(self.tx().as_mut()),
            )?;
            // The new deadline is `? + ttl` and `ttl` is a column, so it has to
            // be read back. MySQL has no RETURNING, so this is a second
            // statement — taken only when the heartbeat actually landed, which
            // keeps the no-op case (the common one on a stale task) free.
            if res.rows_affected() > 0 {
                let row = rt_block_on(
                    sqlx::query("SELECT expires_at FROM promises WHERE id = ?")
                        .bind(task_id)
                        .fetch_optional(self.tx().as_mut()),
                )?;
                if let Some(row) = row {
                    if let Some(at) = row.get::<Option<i64>, _>("expires_at") {
                        self.arm_lease(task_id, pid, at);
                    }
                }
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
        let awaited_ids: Vec<String> = awaited_ids.iter().map(|s| s.to_string()).collect();

        // 1. Lock all awaited promises in id order (deadlock prevention)
        if !awaited_ids.is_empty() {
            let placeholders = awaited_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT id FROM promises WHERE id IN ({}) ORDER BY id FOR UPDATE",
                placeholders
            );
            let mut q = sqlx::query(&sql);
            for id in &awaited_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.fetch_all(self.tx().as_mut()))?;
        }

        // 2. Lock the task — the same row as its promise
        rt_block_on(
            sqlx::query(
                "SELECT id FROM promises WHERE id = ? AND task_state IS NOT NULL FOR UPDATE",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;

        // 3. Check task version/state
        let task_row = rt_block_on(
            sqlx::query(
                "SELECT task_state, task_version FROM promises WHERE id = ? AND task_state IS NOT NULL",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        let task_matched = task_row.as_ref().is_some_and(|r| {
            r.get::<String, _>("task_state") == "acquired"
                && r.get::<i32, _>("task_version") == version as i32
        });
        if !task_matched {
            return Ok(TaskSuspendResult {
                task_matched: false,
                was_suspended: false,
                missing_count: 0,
                non_awaitable_count: 0,
            });
        }

        // 4. Count missing awaited promises
        let missing_count = if awaited_ids.is_empty() {
            0i32
        } else {
            let placeholders = awaited_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM promises WHERE id IN ({})",
                placeholders
            );
            let mut q = sqlx::query(&sql);
            for id in &awaited_ids {
                q = q.bind(id.as_str());
            }
            let row = rt_block_on(q.fetch_one(self.tx().as_mut()))?;
            let found: i64 = row.get("cnt");
            (awaited_ids.len() as i64 - found) as i32
        };
        if missing_count > 0 {
            return Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count,
                non_awaitable_count: 0,
            });
        }

        // 4b. Count awaited promises that may not be awaited — the three tags
        // of `resonate_core::types::is_external`, two of them already stored
        // as generated columns.
        let non_awaitable_count = if awaited_ids.is_empty() {
            0i32
        } else {
            let placeholders = awaited_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM promises WHERE id IN ({}) \
                 AND NOT (COALESCE(tags->>'$.\"resonate:scope\"', '') = 'global' \
                          OR COALESCE(tags->>'$.\"resonate:external\"', '') = 'true' \
                          OR target IS NOT NULL OR timer)",
                placeholders
            );
            let mut q = sqlx::query(&sql);
            for id in &awaited_ids {
                q = q.bind(id.as_str());
            }
            let row = rt_block_on(q.fetch_one(self.tx().as_mut()))?;
            row.get::<i64, _>("cnt") as i32
        };
        if non_awaitable_count > 0 {
            return Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count: 0,
                non_awaitable_count,
            });
        }

        // 5. Count already-settled awaited promises
        let settled_count: i64 = if awaited_ids.is_empty() {
            0
        } else {
            let placeholders = awaited_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM promises WHERE id IN ({}) AND state != 'pending'",
                placeholders
            );
            let mut q = sqlx::query(&sql);
            for id in &awaited_ids {
                q = q.bind(id.as_str());
            }
            let row = rt_block_on(q.fetch_one(self.tx().as_mut()))?;
            row.get("cnt")
        };

        if settled_count == 0 {
            // 6. Can suspend: clear stale ready callbacks, insert new ones, transition task
            rt_block_on(
                sqlx::query("DELETE FROM callbacks WHERE awaiter_id = ? AND ready = true")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;
            for awaited_id in &awaited_ids {
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO callbacks (awaited_id, awaiter_id) VALUES (?, ?)",
                    )
                    .bind(awaited_id.as_str())
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
                )?;
            }
            // A suspended task is on neither timeout queue, which is what
            // deleting its `task_timeouts` row used to say.
            rt_block_on(
                sqlx::query(
                    "UPDATE promises SET task_state = 'suspended',
                                         retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
                     WHERE id = ? AND task_version = ? AND task_state = 'acquired'",
                )
                .bind(task_id)
                .bind(version as i32)
                .execute(self.tx().as_mut()),
            )?;
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: true,
                missing_count: 0,
                non_awaitable_count: 0,
            })
        } else {
            // 7. Cannot suspend — at least one awaited promise is already settled.
            // Delete ready callbacks (re-entry cleanup, same semantics as postgres).
            rt_block_on(
                sqlx::query("DELETE FROM callbacks WHERE awaiter_id = ? AND ready = true")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count: 0,
                non_awaitable_count: 0,
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

        // 1. Lock: promise first, then task (consistent ordering prevents deadlocks)
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ? FOR UPDATE")
                .bind(promise_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let task_lock = rt_block_on(
            sqlx::query(
                "SELECT id FROM promises WHERE id = ? AND task_state IS NOT NULL FOR UPDATE",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        let task_exists = task_lock.is_some();

        // 2. Fulfill the task (version + state guard), and with it drop the lease
        let task_res = rt_block_on(
            sqlx::query(
                "UPDATE promises SET task_state = 'fulfilled',
                                     retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ? AND task_version = ? AND task_state = 'acquired'",
            )
            .bind(task_id)
            .bind(version as i32)
            .execute(self.tx().as_mut()),
        )?;
        let task_fulfilled = task_res.rows_affected() > 0;

        if task_fulfilled {
            // 3. Clean up task infrastructure
            rt_block_on(
                sqlx::query("DELETE FROM callbacks WHERE awaiter_id = ?")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;

            // 4. Settle the promise
            let promise_res = rt_block_on(
                sqlx::query(
                    "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ?
                     WHERE id = ? AND state = 'pending'"
                ).bind(state).bind(value_headers).bind(value_data).bind(settled_at).bind(promise_id)
                 .execute(self.tx().as_mut())
            )?;

            if promise_res.rows_affected() > 0 {
                // 5. No promise timeout to delete — settling took the row out
                //    of the queue.
                // 6. Resume suspended tasks waiting on this promise + notify listeners
                self.resumption_enqueued(promise_id, settled_at)?;
                self.listener_unblocked(promise_id)?;
            }
        }

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
        // queue: `expires_at` out, `retry_at` in.
        let res = rt_block_on(
            sqlx::query(
                "UPDATE promises SET task_state = 'pending', retry_at = ? + ?,
                                     expires_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ? AND task_version = ? AND task_state = 'acquired'",
            )
            .bind(time)
            .bind(ttl)
            .bind(task_id)
            .bind(version as i32)
            .execute(self.tx().as_mut()),
        )?;
        let task_released = res.rows_affected() > 0;

        if task_released {
            self.arm_retry(task_id, time + ttl);
            self.emit_execute(task_id)?;
        }
        let task_exists = rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ? AND task_state IS NOT NULL")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?
        .is_some();
        Ok(TaskReleaseResult {
            task_released,
            task_exists,
        })
    }

    fn task_halt(&self, task_id: &str) -> StorageResult<TaskHaltResult> {
        let row = rt_block_on(
            sqlx::query(
                "SELECT id, task_state FROM promises WHERE id = ? AND task_state IS NOT NULL FOR UPDATE",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;

        let task_exists = row.is_some();
        let task_state: Option<String> = row.as_ref().map(|r| r.get("task_state"));
        let task_fulfilled = task_state.as_deref() == Some("fulfilled");

        if task_exists && !task_fulfilled && task_state.as_deref() != Some("halted") {
            // Halting and dropping the timeout are one row, so one statement.
            rt_block_on(
                sqlx::query(
                    "UPDATE promises SET task_state = 'halted',
                                         retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
                     WHERE id = ?",
                )
                .bind(task_id)
                .execute(self.tx().as_mut()),
            )?;
        }

        Ok(TaskHaltResult {
            task_exists,
            task_fulfilled,
        })
    }

    fn task_continue(&self, task_id: &str, time: i64) -> StorageResult<TaskContinueResult> {
        let trt = self.task_retry_timeout;

        // Lock the task first — the same row as its promise
        rt_block_on(
            sqlx::query(
                "SELECT id FROM promises WHERE id = ? AND task_state IS NOT NULL FOR UPDATE",
            )
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;

        // A halted task carries no deadline, so putting it back on the retry
        // queue is the same write that makes it pending again.
        let res = rt_block_on(
            sqlx::query(
                "UPDATE promises SET task_state = 'pending', retry_at = ?
                 WHERE id = ? AND task_state = 'halted'",
            )
            .bind(time + trt)
            .bind(task_id)
            .execute(self.tx().as_mut()),
        )?;
        let continued = res.rows_affected() > 0;

        if continued {
            self.arm_retry(task_id, time + trt);
            self.emit_execute(task_id)?;
        }

        let task_exists = rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ? AND task_state IS NOT NULL")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?
        .is_some();

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
        let rows = rt_block_on(
            sqlx::query(
                "SELECT p.id, p.task_state AS state, p.task_version AS version,
                   CASE WHEN p.task_state = 'acquired' THEN p.ttl ELSE NULL END AS ttl,
                   CASE WHEN p.task_state = 'acquired' THEN p.pid ELSE NULL END AS pid,
                   COALESCE(
                     (SELECT CAST(COUNT(*) AS SIGNED) FROM callbacks c WHERE c.awaiter_id = p.id AND c.ready = true),
                     0
                   ) AS resumes
                 FROM promises p
                 WHERE p.task_state IS NOT NULL
                   AND (? IS NULL OR p.task_state = ?) AND (? IS NULL OR p.id > ?)
                 ORDER BY p.id ASC LIMIT ?",
            )
            .bind(state).bind(state)
            .bind(cursor).bind(cursor)
            .bind(limit)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows
            .iter()
            .map(|r| {
                let resumes: i64 = r.get("resumes");
                TaskRecord {
                    id: r.get("id"),
                    state: parse_task_state(&r.get::<String, _>("state")),
                    version: r.get::<i32, _>("version") as i64,
                    resumes,
                    ttl: r.get("ttl"),
                    pid: r.get("pid"),
                }
            })
            .collect())
    }

    fn compute_preload(&self, promise_id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let branch_row = rt_block_on(
            sqlx::query("SELECT branch FROM promises WHERE id = ?")
                .bind(promise_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let branch: Option<String> = branch_row.and_then(|r| r.get("branch"));
        let branch = match branch {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let rows = rt_block_on(
            sqlx::query(
                "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at
                 FROM promises WHERE branch = ? AND id != ? ORDER BY id ASC",
            )
            .bind(&branch)
            .bind(promise_id)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    fn schedule_get(&self, id: &str) -> StorageResult<Option<ScheduleRecord>> {
        let row = rt_block_on(
            sqlx::query(
                "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at FROM schedules WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(row.as_ref().map(row_to_schedule))
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

        let res = rt_block_on(
            sqlx::query(
                "INSERT IGNORE INTO schedules
                 (id, cron, promise_id, promise_timeout, promise_param_headers,
                  promise_param_data, promise_tags, created_at, next_run_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(cron)
            .bind(promise_id)
            .bind(promise_timeout)
            .bind(promise_param_headers)
            .bind(promise_param_data)
            .bind(promise_tags)
            .bind(created_at)
            .bind(next_run_at)
            .execute(self.tx().as_mut()),
        )?;
        // Only a create that actually happened arms a deadline — an idempotent
        // re-create leaves the existing next_run_at where it was.
        if res.rows_affected() > 0 {
            self.arm(
                next_run_at,
                Timeout::ScheduleDue {
                    schedule_id: id.to_string(),
                },
            );
        }

        // No schedule timeout to insert: `next_run_at` on the row above is it.
        let _ = res;

        Ok(self
            .schedule_get(id)?
            .unwrap_or_else(|| panic!("schedule not found after create: {}", id)))
    }

    fn schedule_delete(&self, id: &str) -> StorageResult<bool> {
        let res = rt_block_on(
            sqlx::query("DELETE FROM schedules WHERE id = ?")
                .bind(id)
                .execute(self.tx().as_mut()),
        )?;
        Ok(res.rows_affected() > 0)
    }

    fn schedule_search(
        &self,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let rows = rt_block_on(
            sqlx::query(
                "SELECT id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at, last_run_at
                 FROM schedules
                 WHERE (? IS NULL OR JSON_CONTAINS(promise_tags, ?))
                   AND (? IS NULL OR id > ?)
                 ORDER BY id ASC LIMIT ?",
            )
            .bind(tags).bind(tags)
            .bind(cursor).bind(cursor)
            .bind(limit)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows.iter().map(row_to_schedule).collect())
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
        let rows = rt_block_on(
            sqlx::query(
                // The kind is an integer, not the label the other two backends
                // select. A string literal in a UNION carries the connection's
                // collation, the columns carry the table's, and MySQL refuses
                // to unify them — "Illegal mix of collations". An integer has
                // no collation, so the discriminant travels as a number and is
                // named on the Rust side.
                //
                // `pid` is a bare NULL for the same reason: `CAST(NULL AS
                // CHAR)` would introduce a collation of its own, where an
                // untyped NULL takes the one the third branch supplies.
                "SELECT deadline, kind, id, pid FROM (
                     SELECT timeout_at AS deadline, 0 AS kind, id AS id, NULL AS pid
                       FROM promises WHERE state = 'pending' AND target IS NOT NULL
                     UNION ALL
                     SELECT retry_at, 1, id, NULL
                       FROM promises WHERE task_state = 'pending' AND retry_at IS NOT NULL
                     UNION ALL
                     SELECT expires_at, 2, id, pid
                       FROM promises WHERE task_state = 'acquired' AND expires_at IS NOT NULL
                     UNION ALL
                     SELECT next_run_at, 3, id, NULL FROM schedules
                 ) d
                 ORDER BY deadline ASC, id ASC
                 LIMIT ?",
            )
            .bind(limit as i64)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let at: i64 = r.get("deadline");
                let kind = match r.get::<i64, _>("kind") {
                    0 => "promise",
                    1 => "retry",
                    2 => "lease",
                    _ => "schedule",
                };
                let id: String = r.get("id");
                let pid: Option<String> = r.get("pid");
                Timeout::from_parts(kind, id, pid).map(|timeout| Scheduled { at, timeout })
            })
            .collect())
    }

    fn get_expired_schedule_timeouts(
        &self,
        time: i64,
        only: Option<&str>,
    ) -> StorageResult<Vec<(String, i64)>> {
        let rows = rt_block_on(
            sqlx::query(
                "SELECT id, next_run_at FROM schedules
                 WHERE next_run_at <= ? AND (? IS NULL OR id = ?)
                 ORDER BY id",
            )
            .bind(time)
            .bind(only)
            .bind(only)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows
            .iter()
            .map(|r| (r.get::<String, _>("id"), r.get::<i64, _>("next_run_at")))
            .collect())
    }

    fn process_schedule_timeout(
        &self,
        schedule_id: &str,
        fired_at: i64,
        next_run_at: i64,
        time: i64,
        promise_tags: &std::collections::HashMap<String, String>,
    ) -> StorageResult<Option<ScheduleRecord>> {
        let trt = self.task_retry_timeout;
        let promise_tags_json = serde_json::to_string(promise_tags).unwrap();

        // 1. Verify the schedule has not already been advanced past this
        //    firing. `next_run_at` is the queue, so the idempotency guard reads
        //    the schedule row it is about to advance.
        let timeout_row = rt_block_on(
            sqlx::query("SELECT id FROM schedules WHERE id = ? AND next_run_at = ?")
                .bind(schedule_id)
                .bind(fired_at)
                .fetch_optional(self.tx().as_mut()),
        )?;
        if timeout_row.is_none() {
            return Ok(None);
        }

        // 2. Load schedule
        let schedule_row = rt_block_on(
            sqlx::query(
                "SELECT id, promise_id, promise_timeout, promise_param_headers, promise_param_data
                 FROM schedules WHERE id = ?",
            )
            .bind(schedule_id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        let schedule_row = match schedule_row {
            Some(r) => r,
            None => return Ok(None),
        };

        // 3. Template substitution (in Rust)
        let promise_id_template: String = schedule_row.get("promise_id");
        let computed_promise_id = promise_id_template
            .replace("{{.id}}", schedule_id)
            .replace("{{.timestamp}}", &fired_at.to_string());
        let promise_timeout: i64 = schedule_row.get("promise_timeout");
        let computed_timeout_at = fired_at + promise_timeout;
        let param_headers: Option<String> = schedule_row.get("promise_param_headers");
        let param_data: Option<String> = schedule_row.get("promise_param_data");

        // 4. Address from promise_tags
        let address = promise_tags.get("resonate:target").cloned();

        let already_timedout = time >= computed_timeout_at;
        let is_timer = promise_tags.get("resonate:timer").map(|v| v.as_str()) == Some("true");
        let (state, settled_at, created_at): (&str, Option<i64>, i64) = if already_timedout {
            let s = if is_timer {
                "resolved"
            } else {
                "rejected_timedout"
            };
            (s, Some(computed_timeout_at), fired_at)
        } else {
            ("pending", None, fired_at)
        };

        // 5. Create promise (idempotent)
        let promise_res = rt_block_on(
            sqlx::query(
                "INSERT IGNORE INTO promises
                 (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&computed_promise_id)
            .bind(state)
            .bind(param_headers.as_deref())
            .bind(param_data.as_deref())
            .bind(&promise_tags_json)
            .bind(computed_timeout_at)
            .bind(created_at)
            .bind(settled_at)
            .execute(self.tx().as_mut()),
        )?;

        if promise_res.rows_affected() > 0 {
            // 6a is gone with `promise_timeouts`: the INSERT above already put
            // a pending, targeted promise on the queue.
            if already_timedout {
                // Promise is immediately settled — create fulfilled task if resonate:target is set
                if address.is_some() {
                    rt_block_on(
                        sqlx::query(
                            "UPDATE promises SET task_state = 'fulfilled', task_version = 0
                             WHERE id = ? AND task_state IS NULL",
                        )
                        .bind(&computed_promise_id)
                        .execute(self.tx().as_mut()),
                    )?;
                }
            } else if let Some(ref addr) = address {
                // 6b. Task infrastructure if address is present
                let task_res = rt_block_on(
                    sqlx::query(
                        "UPDATE promises SET task_state = 'pending', task_version = 0, retry_at = ?
                         WHERE id = ? AND task_state IS NULL",
                    )
                    .bind(time + trt)
                    .bind(&computed_promise_id)
                    .execute(self.tx().as_mut()),
                )?;
                if task_res.rows_affected() > 0 {
                    self.arm_promise_timeout(&computed_promise_id, computed_timeout_at, true);
                    self.arm_retry(&computed_promise_id, time + trt);
                    self.emit(Outgoing::Execute {
                        address: addr.to_string(),
                        task_id: computed_promise_id.clone(),
                        version: 0,
                    });
                }
            }
        }

        // 7. Update schedule
        rt_block_on(
            sqlx::query("UPDATE schedules SET last_run_at = ?, next_run_at = ? WHERE id = ?")
                .bind(fired_at)
                .bind(next_run_at)
                .bind(schedule_id)
                .execute(self.tx().as_mut()),
        )?;
        self.arm(
            next_run_at,
            Timeout::ScheduleDue {
                schedule_id: schedule_id.to_string(),
            },
        );

        // 8 is gone: advancing the schedule above advanced the queue.

        // 9. Return updated schedule record
        self.schedule_get(schedule_id)
    }

    /// Fire expired timeouts, either all of them or one named.
    ///
    /// `only` is what makes the precise form precise, and it costs one bound
    /// parameter: every statement below already selects the rows of one queue
    /// past their deadline, and `? IS NULL OR id = ?` narrows that to a single
    /// id. A named timeout runs the statement for its own queue and skips the
    /// other two, so the narrow form is the sweep restricted to one row rather
    /// than a second implementation of it.
    fn process_timeouts(&self, time: i64, only: Option<&Timeout>) -> StorageResult<()> {
        let trt = self.task_retry_timeout;
        let selected = |kind: &str| match only {
            None => Some(None::<&str>),
            Some(t) if t.kind() == kind => Some(Some(t.id())),
            Some(_) => None,
        };

        // Statement 1: Expire all pending promises with timeout_at <= time
        // (with resonate:target).
        //
        // `state = 'pending' AND target IS NOT NULL` is the whole of what
        // `promise_timeouts` held: rows entered on create and left on settle,
        // and only a targeted promise was ever swept eagerly. Untargeted ones
        // still time out lazily, through `try_timeout`.
        let expired_rows = match selected("promise") {
            None => Vec::new(),
            Some(id) => rt_block_on(
                sqlx::query(
                    "SELECT id FROM promises
                     WHERE state = 'pending' AND target IS NOT NULL AND timeout_at <= ?
                       AND (? IS NULL OR id = ?)",
                )
                .bind(time)
                .bind(id)
                .bind(id)
                .fetch_all(self.tx().as_mut()),
            )?,
        };

        if !expired_rows.is_empty() {
            let expired_ids: Vec<String> = expired_rows
                .iter()
                .map(|r| r.get::<String, _>("id"))
                .collect();
            let n = expired_ids.len();
            let ph = |k: usize| -> String { vec!["?"; k].join(", ") };

            {
                let sql = format!(
                    "UPDATE promises
                     SET state = CASE WHEN timer THEN 'resolved' ELSE 'rejected_timedout' END,
                         settled_at = timeout_at
                     WHERE id IN ({})",
                    ph(n)
                );
                let mut q = sqlx::query(&sql);
                for id in &expired_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }

            self.batch_settle_cascade(&expired_ids, time)?;
        }

        // Statement 2: Process expired task retry deadlines — what was
        // `timeout_type = 0`, now a non-NULL `retry_at` on a pending task.
        let retry_rows = match selected("retry") {
            None => Vec::new(),
            Some(id) => rt_block_on(
                sqlx::query(
                    "SELECT id FROM promises
                     WHERE task_state = 'pending' AND retry_at IS NOT NULL AND retry_at <= ?
                       AND (? IS NULL OR id = ?)",
                )
                .bind(time)
                .bind(id)
                .bind(id)
                .fetch_all(self.tx().as_mut()),
            )?,
        };

        if !retry_rows.is_empty() {
            let retry_ids: Vec<String> = retry_rows
                .iter()
                .map(|r| r.get::<String, _>("id"))
                .collect();
            let n = retry_ids.len();
            let ph = |k: usize| -> String { vec!["?"; k].join(", ") };

            {
                let sql = format!(
                    "UPDATE promises SET retry_at = ? + ?, pid = NULL WHERE id IN ({})",
                    ph(n)
                );
                let mut q = sqlx::query(&sql).bind(time).bind(trt);
                for id in &retry_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }

            for id in &retry_ids {
                self.arm_retry(id, time + trt);
                self.emit_execute(id)?;
            }
        }

        // Statement 3: Process expired leases — what was `timeout_type = 1`,
        // now a non-NULL `expires_at` on an acquired task. The holder went
        // away; hand the task back to the retry queue.
        let lease_rows = match selected("lease") {
            None => Vec::new(),
            Some(id) => rt_block_on(
                sqlx::query(
                    "SELECT id FROM promises
                     WHERE task_state = 'acquired' AND expires_at IS NOT NULL AND expires_at <= ?
                       AND (? IS NULL OR id = ?)",
                )
                .bind(time)
                .bind(id)
                .bind(id)
                .fetch_all(self.tx().as_mut()),
            )?,
        };

        if !lease_rows.is_empty() {
            let lease_ids: Vec<String> = lease_rows
                .iter()
                .map(|r| r.get::<String, _>("id"))
                .collect();
            let n = lease_ids.len();
            let ph = |k: usize| -> String { vec!["?"; k].join(", ") };

            {
                // Handing the task back and moving it between queues are the
                // same row, so the two statements this replaces are one `SET`.
                let sql = format!(
                    "UPDATE promises SET task_state = 'pending', retry_at = ? + ?,
                                         expires_at = NULL, ttl = NULL, pid = NULL
                     WHERE id IN ({})",
                    ph(n)
                );
                let mut q = sqlx::query(&sql).bind(time).bind(trt);
                for id in &lease_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }

            for id in &lease_ids {
                self.arm_retry(id, time + trt);
                self.emit_execute(id)?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)] // the liveness probe the server will call
    fn ping(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql("SELECT 1").execute(self.tx().as_mut()))?;
        Ok(())
    }

    fn debug_reset(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql("DELETE FROM listeners").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM callbacks").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM schedules").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM promises").execute(self.tx().as_mut()))?;
        Ok(())
    }

    fn snap(&self) -> StorageResult<Snapshot> {
        let promise_rows = rt_block_on(
            sqlx::query(
                "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises ORDER BY id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let promises: Vec<PromiseRecord> = promise_rows.iter().map(row_to_promise).collect();

        // Every section below is a projection of the one table now. The
        // predicates are the membership rules the deleted tables carried.
        let pt_rows = rt_block_on(
            sqlx::query(
                "SELECT id, timeout_at FROM promises
                 WHERE state = 'pending' AND target IS NOT NULL ORDER BY id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let promise_timeouts: Vec<SnapshotPromiseTimeout> = pt_rows
            .iter()
            .map(|r| SnapshotPromiseTimeout {
                id: r.get("id"),
                timeout: r.get("timeout_at"),
            })
            .collect();

        let cb_rows = rt_block_on(
            sqlx::query(
                "SELECT awaiter_id, awaited_id FROM callbacks WHERE NOT ready ORDER BY awaiter_id, awaited_id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let callbacks: Vec<SnapshotCallback> = cb_rows
            .iter()
            .map(|r| SnapshotCallback {
                awaiter: r.get("awaiter_id"),
                awaited: r.get("awaited_id"),
            })
            .collect();

        let li_rows = rt_block_on(
            sqlx::query("SELECT promise_id, address FROM listeners ORDER BY promise_id, address")
                .fetch_all(self.tx().as_mut()),
        )?;
        let listeners: Vec<SnapshotListener> = li_rows
            .iter()
            .map(|r| SnapshotListener {
                promise_id: r.get("promise_id"),
                address: r.get("address"),
            })
            .collect();

        let task_rows = rt_block_on(
            sqlx::query(
                "SELECT p.id, p.task_state AS state, p.task_version AS version,
                   CASE WHEN p.task_state = 'acquired' THEN p.ttl ELSE NULL END AS ttl,
                   CASE WHEN p.task_state = 'acquired' THEN p.pid ELSE NULL END AS pid,
                   COALESCE(
                     (SELECT CAST(COUNT(*) AS SIGNED) FROM callbacks c WHERE c.awaiter_id = p.id AND c.ready = true),
                     0
                   ) AS resumes
                 FROM promises p WHERE p.task_state IS NOT NULL ORDER BY p.id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let tasks: Vec<TaskRecord> = task_rows
            .iter()
            .map(|r| {
                let resumes: i64 = r.get("resumes");
                TaskRecord {
                    id: r.get("id"),
                    state: parse_task_state(&r.get::<String, _>("state")),
                    version: r.get::<i32, _>("version") as i64,
                    resumes,
                    ttl: r.get("ttl"),
                    pid: r.get("pid"),
                }
            })
            .collect();

        // One row per task at most, as before: the two deadlines are mutually
        // exclusive because each is live only in the state that owns it.
        let tt_rows = rt_block_on(
            sqlx::query(
                "SELECT id, CAST(0 AS SIGNED) AS timeout_type, retry_at AS timeout_at FROM promises
                   WHERE task_state = 'pending' AND retry_at IS NOT NULL
                 UNION ALL
                 SELECT id, CAST(1 AS SIGNED) AS timeout_type, expires_at AS timeout_at FROM promises
                   WHERE task_state = 'acquired' AND expires_at IS NOT NULL
                 ORDER BY id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let task_timeouts: Vec<SnapshotTaskTimeout> = tt_rows
            .iter()
            .map(|r| {
                let tt: i64 = r.get("timeout_type");
                SnapshotTaskTimeout {
                    id: r.get("id"),
                    timeout_type: tt as i32,
                    timeout: r.get("timeout_at"),
                }
            })
            .collect();

        // Nothing queued, so nothing to report — the messages left with the
        // transitions that emitted them. See `engine_sqlite.rs`.
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

/// One tick of the timer wheel: the three timeout sweeps, then expired
/// schedules. Returns how many schedules fired, for the caller to record.
fn process_all_timeouts(db: &MysqlDb, time: i64) -> StorageResult<usize> {
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time, None)?;
    process_schedule_timeouts(db, time, None)
}

/// Process expired schedule timeouts.
fn process_schedule_timeouts(db: &MysqlDb, time: i64, only: Option<&str>) -> StorageResult<usize> {
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

        if db
            .process_schedule_timeout(schedule_id, *fired_at, next_run_at, time, &promise_tags)?
            .is_some()
        {
            tracing::info!(
                schedule_id = %schedule_id,
                fired_at = fired_at,
                next_run_at = next_run_at,
                "Schedule fired"
            );
            fired += 1;
        }
    }

    Ok(fired)
}

#[async_trait]
impl ResonateEngine for MysqlEngine {
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

impl MysqlEngine {
    /// Fire one timeout the system asked of itself. See `engine_sqlite.rs`.
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
