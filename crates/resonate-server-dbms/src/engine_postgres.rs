//! The PostgreSQL engine.
//!
//! A complete implementation of the protocol over PostgreSQL: it parses and
//! validates a request, applies the transition in its own SQL, and shapes the
//! response — with no `Db` trait between the two halves and no shared engine
//! above them.
//!
//! A promise is one row, and what sits beside it is `schedules`. There is no
//! `outbox`: a message is not something a transition leaves behind for a pump
//! to find, it is something the transition returns.
//!
//! # Emitting from inside a CTE
//!
//! This is the one place the collapse costs something. Every operation is a
//! single statement, and that single round trip is the property the CTE design
//! exists for — so emissions cannot be a second query. Each `INSERT INTO
//! outbox ... RETURNING key` CTE became a plain `SELECT` producing the message
//! instead, and the statement's final `SELECT` aggregates them into one JSON
//! column with [`emitted_json`]. Same round trip, and the messages come back
//! on the same row as the response.
//!
//! See `persistence_sqlite.rs` for what the promise row's columns replaced;
//! the collapse is the same, minus `callbacks`/`listeners`/`resumes`, which
//! are TEXT[] columns here because Postgres has arrays.

use crate::engine_port::{Input, Outgoing, Output, ResonateEngine, Scheduled, Timeout};
use crate::{
    PromiseCreateParams, PromiseCreateResult, PromiseSettleParams, PromiseSettleResult,
    RegisterCallbackResult, ScheduleCreateParams, StorageError, StorageResult, TaskAcquireParams,
    TaskAcquireResult, TaskContinueResult, TaskCreateParams, TaskCreateResult,
    TaskFenceCreateParams, TaskFenceResult, TaskFenceSettleParams, TaskFulfillParams,
    TaskFulfillResult, TaskHaltResult, TaskReleaseResult, TaskSuspendResult,
};
use async_trait::async_trait;
use serde_json::Value;
use validator::Validate;

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
use resonate_core::util;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use std::cell::{RefCell, UnsafeCell};

pub struct PostgresEngine {
    pool: PgPool,
    task_retry_timeout: i64,
    preload_limit: u32,
    /// Whether `debug.*` operations are permitted at all.
    debug: bool,
}

/// The promise columns every read projects. `param_headers`/`value_headers` are
/// `NOT NULL DEFAULT '{}'` here (the catalogue's
/// `well_formed_promise_pending_has_no_value` compares against `'{}'::jsonb`),
/// so `NULLIF` restores the wire-level distinction the API draws between
/// "no headers" and "headers present".
const P_COLS: &str =
    "id, state, NULLIF(param_headers, '{}'::jsonb)::text AS param_headers, param_data, \
                      NULLIF(value_headers, '{}'::jsonb)::text AS value_headers, value_data, \
                      tags::text, timeout_at, created_at, settled_at";

/// Same projection, qualified — for statements that alias the table.
fn p_cols(alias: &str) -> String {
    format!(
        "{a}.id, {a}.state, NULLIF({a}.param_headers, '{{}}'::jsonb)::text AS param_headers, {a}.param_data, \
         NULLIF({a}.value_headers, '{{}}'::jsonb)::text AS value_headers, {a}.value_data, \
         {a}.tags::text, {a}.timeout_at, {a}.created_at, {a}.settled_at",
        a = alias
    )
}

/// The columns every emission CTE produces, so several can be `UNION ALL`ed
/// into one list regardless of which kind they carry.
const MSG_COLS: &str = "kind, address, task_id, version, promise";

/// Aggregate the named emission CTEs into one JSON column on the result row.
///
/// A scalar subquery rather than a join, because the statement's own result is
/// one row (or a small fixed set) and the messages are a list beside it, not a
/// dimension of it.
fn emitted_json(ctes: &[&str]) -> String {
    let parts: Vec<String> = ctes
        .iter()
        .map(|c| format!("SELECT {MSG_COLS} FROM {c}"))
        .collect();
    format!(
        "(SELECT COALESCE(json_agg(json_build_object(\
           'kind', kind, 'address', address, 'task_id', task_id, \
           'version', version, 'promise', promise) \
           ORDER BY kind, address, task_id), '[]'::json) \
         FROM ({}) e) AS messages",
        parts.join(" UNION ALL ")
    )
}

/// Arguments to `resonate._promise_json`, in declaration order.
const PROMISE_JSON_ARGS: &str = "id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at";

fn promise_json(alias: &str) -> String {
    let args: Vec<String> = PROMISE_JSON_ARGS
        .split(", ")
        .map(|c| format!("{}.{}", alias, c))
        .collect();
    format!("resonate._promise_json({})", args.join(", "))
}

impl PostgresEngine {
    pub async fn connect(
        url: &str,
        pool_size: u32,
        task_retry_timeout: i64,
        preload_limit: u32,
        debug: bool,
    ) -> Result<Self, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(pool_size)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET search_path TO resonate, public")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await?;
        Ok(Self {
            pool,
            task_retry_timeout,
            preload_limit,
            debug,
        })
    }

    /// Migrate the schema, constraints and all.
    ///
    /// One entry point, because there is one schema and one mechanism. The
    /// constraints are statements in the same migration, so a database
    /// carrying the tables carries the invariants too, and no configuration
    /// can start a server that enforces fewer of them. An error here stops
    /// startup: a server whose schema did not migrate must not serve.
    pub async fn init(&self, migrate: bool) -> Result<(), sqlx::Error> {
        // The migrator's own bookkeeping table follows `search_path`, so the
        // schema has to exist before it runs.
        sqlx::raw_sql("CREATE SCHEMA IF NOT EXISTS resonate")
            .execute(&self.pool)
            .await?;
        let migrator = sqlx::migrate!("./migrations/postgres");
        // COUNT over a table that may not exist yet: a missing table is an
        // empty database, which is the always-create case.
        let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        crate::migrate::may_apply(applied as usize, migrator.iter().count(), migrate)
            .map_err(|e| sqlx::Error::Configuration(Box::new(e)))?;
        migrator.run(&self.pool).await.map_err(|e| match e {
            // The initial schema was edited after this database was created.
            sqlx::migrate::MigrateError::VersionMismatch(v) => sqlx::Error::Configuration(
                Box::new(crate::migrate::MigrateError(crate::migrate::stale_schema(
                    &format!("migration {v} was applied with a different checksum"),
                ))),
            ),
            other => sqlx::Error::Migrate(Box::new(other)),
        })
    }

    /// Run one transition, and hand back what it emitted along with its result.
    ///
    /// The emissions are dropped if the transaction rolls back or a
    /// serialization retry restarts it — a retried attempt starts with an
    /// empty list, so a message is never emitted twice for one attempt that
    /// did not commit. That is the atomicity the port promises, which an
    /// outbox got for free by being a table.
    async fn transact<F, T>(&self, f: F) -> StorageResult<(T, Vec<Outgoing>, Vec<Scheduled>)>
    where
        F: FnMut(&PostgresDb) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        // One retry, unconditionally, as MySQL does. A serialization failure
        // means the transaction aborted with nothing committed — the emissions
        // above are dropped with it — so re-running the closure from scratch
        // is safe, and `promise_create` raises this error itself precisely to
        // ask for that. It used to be gated on a `serializable` flag that
        // every one of the seven call sites passed `false`, so the retry never
        // ran and the request the code asked to retry became a 503 instead.
        const MAX_RETRIES: u32 = 1;

        let mut f = f;
        for attempt in 0..=MAX_RETRIES {
            #[cfg(feature = "concurrency-stress")]
            tokio::task::yield_now().await;

            // READ COMMITTED, the connection default, and what the
            // single-round-trip CTEs are written against: they take their own
            // row locks with `FOR UPDATE` rather than relying on the isolation
            // level to serialize them.
            let tx = self.pool.begin().await.map_err(StorageError::from)?;

            let task_retry_timeout = self.task_retry_timeout;
            let preload_limit = self.preload_limit;
            let (result, emitted, armed, tx) = tokio::task::block_in_place(|| {
                let db = PostgresDb {
                    tx: UnsafeCell::new(tx),
                    task_retry_timeout,
                    preload_limit,
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

            let result = match result {
                Ok(v) => v,
                Err(StorageError::Serialization) => {
                    if attempt < MAX_RETRIES {
                        tracing::warn!(
                            attempt = attempt + 1,
                            "Serialization failure (40001) in query, retrying"
                        );
                        continue;
                    }
                    return Err(StorageError::Serialization);
                }
                Err(e) => return Err(e),
            };

            match tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(tx.commit())
            }) {
                Ok(_) => return Ok((result, emitted, armed)),
                Err(e) => {
                    let pg_err = e
                        .as_database_error()
                        .and_then(|dbe| dbe.code().map(|c| c.to_string()));
                    if pg_err.as_deref() == Some("40001") || pg_err.as_deref() == Some("40P01") {
                        if attempt < MAX_RETRIES {
                            continue;
                        }
                        return Err(StorageError::Serialization);
                    }
                    return Err(StorageError::from(e));
                }
            }
        }

        unreachable!("transact loop completed without returning")
    }

    /// One operation: run it, and turn a storage failure into a response.
    ///
    /// Same tail for all 21, so it lives here once. `Serialization` maps to
    /// 503 — a CTE snapshot race committed nothing, and the caller may retry.
    async fn run<F>(&self, req: &RequestEnvelope, f: F) -> Output
    where
        F: FnMut(&PostgresDb) -> StorageResult<ResponseEnvelope> + Send + 'static,
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
        F: FnMut(&PostgresDb) -> StorageResult<T> + Send + 'static,
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
                    awaitable: resonate_core::types::is_awaitable(&r.tags),
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
                if !resonate_core::types::is_awaitable(&p_awaited.tags) {
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
                        if !resonate_core::types::is_awaitable(&promise.tags) {
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
                            awaitable: resonate_core::types::is_awaitable(&create_data.tags),
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

/// Wraps a PostgreSQL transaction for use within the synchronous `Db` trait.
/// Same `UnsafeCell` rationale as `persistence_postgres::PostgresDb`.
struct PostgresDb<'a> {
    tx: UnsafeCell<sqlx::Transaction<'a, sqlx::Postgres>>,
    task_retry_timeout: i64,
    preload_limit: u32,
    /// What this transition has emitted so far. See `engine_sqlite.rs`
    /// — same reasoning, and the same reason it is not in a return type.
    emitted: RefCell<Vec<Outgoing>>,
    /// What deadlines this transition armed or moved. A hint; see
    /// `engine_sqlite.rs`.
    armed: RefCell<Vec<Scheduled>>,
}

impl<'a> PostgresDb<'a> {
    #[allow(clippy::mut_from_ref)]
    fn tx(&self) -> &mut sqlx::Transaction<'a, sqlx::Postgres> {
        unsafe { &mut *self.tx.get() }
    }

    /// Report a deadline this transition just wrote.
    fn arm(&self, at: i64, timeout: Timeout) {
        self.armed.borrow_mut().push(Scheduled { at, timeout });
    }

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

    /// Absorb a statement's messages, and arm a retry deadline for each task it
    /// redispatched.
    ///
    /// Every execute message this backend emits accompanies a task whose retry
    /// deadline the same statement just wrote — a resumed awaiter, a released
    /// task, a redispatched one, a newly created one. So the fan-out case,
    /// which is the only one where the armed rows are not the rows the
    /// statement returns, needs no extra SQL: the emission already names them.
    /// `at` is per statement, because the deadline each one writes differs.
    fn absorb_and_arm_retries(&self, row: &PgRow, at: i64) -> Vec<String> {
        let before = self.emitted.borrow().len();
        self.absorb(row);
        let armed: Vec<String> = self.emitted.borrow()[before..]
            .iter()
            .filter_map(|m| match m {
                Outgoing::Execute { task_id, .. } => Some(task_id.clone()),
                Outgoing::Unblock { .. } => None,
            })
            .collect();
        for task_id in &armed {
            self.arm_retry(task_id, at);
        }
        armed
    }

    /// Take the `messages` column off a statement's result row.
    ///
    /// Every statement that can emit carries one, built by `emitted_json`. A
    /// row without the column is a statement that cannot emit, and is ignored.
    fn absorb(&self, row: &PgRow) {
        let Ok(value) = row.try_get::<serde_json::Value, _>("messages") else {
            return;
        };
        let Some(items) = value.as_array() else {
            return;
        };
        let mut out = self.emitted.borrow_mut();
        for m in items {
            let address = m
                .get("address")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            match m.get("kind").and_then(|v| v.as_str()) {
                Some("execute") => out.push(Outgoing::Execute {
                    address: address.to_string(),
                    task_id: m
                        .get("task_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    version: m.get("version").and_then(|v| v.as_i64()).unwrap_or(0),
                }),
                Some("unblock") => {
                    if let Some(promise) = m.get("promise") {
                        if let Ok(promise) =
                            serde_json::from_value::<PromiseRecord>(promise.clone())
                        {
                            out.push(Outgoing::Unblock {
                                address: address.to_string(),
                                promise,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
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

fn row_to_promise(row: &sqlx::postgres::PgRow) -> PromiseRecord {
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

fn row_to_task(r: &sqlx::postgres::PgRow) -> TaskRecord {
    let resumes: Vec<String> = r.get("resumes");
    TaskRecord {
        id: r.get("id"),
        state: parse_task_state(&r.get::<String, _>("task_state")),
        version: r.get::<i32, _>("task_version") as i64,
        resumes: resumes.len() as i64,
        ttl: r.get("ttl"),
        pid: r.get("pid"),
    }
}

fn row_to_schedule(row: &sqlx::postgres::PgRow) -> ScheduleRecord {
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

// ============================================================================
// Shared SQL fragments
//
// Templates use `:NAME` placeholders substituted by `fill` rather than
// `format!`, so SQL array literals (`'{}'`) need no brace escaping.
// ============================================================================

fn fill(template: &str, subs: &[(&str, &str)]) -> String {
    let mut out = template.to_string();
    for (k, v) in subs {
        out = out.replace(k, v);
    }
    out
}

/// The half of the settlement cascade that lives on the settling row itself.
///
/// Stands in for `fulfilled_task`, `deleted_ttimeout`, the awaiter-side
/// `deleted_callbacks` and `deleted_listeners` — four CTEs in the multi-table
/// backend, one `SET` list here, because they all target the same row.
///
/// `:FULFILLED` is the predicate "this settlement also fulfils the row's task".
const SETTLE_SELF: &str = "
    task_state = CASE WHEN :FULFILLED THEN 'fulfilled' ELSE p.task_state END,
    retry_timeout_at   = CASE WHEN :FULFILLED THEN NULL ELSE p.retry_timeout_at END,
    lease_timeout_at = CASE WHEN :FULFILLED THEN NULL ELSE p.lease_timeout_at END,
    ttl        = CASE WHEN :FULFILLED THEN NULL ELSE p.ttl END,
    pid        = CASE WHEN :FULFILLED THEN NULL ELSE p.pid END,
    resumes    = CASE WHEN :FULFILLED THEN '{}' ELSE p.resumes END,
    callbacks   = '{}',
    listeners  = '{}'";

/// The half of the settlement cascade that fans out to *other* rows.
///
/// Merges `marked_ready` + `resumed_tasks` (awaited side) with
/// `deleted_callbacks` (awaiter side) into one `UPDATE`: in a two-promise await
/// cycle a single row is both, and two CTEs updating it would be undefined.
///
/// `:AWAITERS` is a scalar subquery yielding the awaiter ids to wake (or NULL
/// when the settlement did not fire); `:FULFILLED` says whether the settling
/// row's own task was fulfilled and so must be unlinked from everything it was
/// itself blocked on.
///
/// `suspended_awaiters` is read from the pre-update snapshot rather than from
/// the `UPDATE`'s `RETURNING`, because `RETURNING` yields post-update values and
/// the outbox needs to know *which* awaiters were suspended. The multi-table
/// backend gets this from `resumed_tasks RETURNING`, which is re-checked under
/// EPQ; this snapshot read is not. The exposure is a concurrent write to an
/// awaiter row between this statement's snapshot and its row locks — see the
/// module docs on lock scope.
const SETTLE_FANOUT: &str = "
suspended_awaiters AS (
  SELECT id, task_version, target FROM promises
  WHERE id = ANY(:AWAITERS) AND task_state = 'suspended'
),
fanout AS (
  UPDATE promises q SET
    callbacks = CASE WHEN :FULFILLED THEN array_remove(q.callbacks, :AWAITED) ELSE q.callbacks END,
    resumes = CASE WHEN q.id = ANY(:AWAITERS) AND NOT (q.resumes @> ARRAY[:AWAITED])
                THEN q.resumes || :AWAITED ELSE q.resumes END,
    task_state = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN 'pending' ELSE q.task_state END,
    retry_timeout_at = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN :TIME + :TRT ELSE q.retry_timeout_at END,
    lease_timeout_at = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN NULL ELSE q.lease_timeout_at END,
    ttl = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN NULL ELSE q.ttl END,
    pid = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN NULL ELSE q.pid END
  WHERE q.id <> :AWAITED
    AND ( q.id = ANY(:AWAITERS)
          OR (:FULFILLED AND q.callbacks @> ARRAY[:AWAITED]) )
  RETURNING q.id
),
emit_resume AS (
  SELECT 'execute'::text AS kind, s.target AS address, s.id AS task_id,
         s.task_version::int AS version, NULL::jsonb AS promise
  FROM suspended_awaiters s WHERE s.target IS NOT NULL
)";

/// Queue one `unblock` message per listener of the row `:SRC` just settled.
/// `:SRC` must be a CTE with the post-settlement promise columns; `:LISTENERS`
/// a scalar subquery yielding the listener addresses as they were *before* the
/// settlement cleared them.
const SETTLE_UNBLOCK: &str = "
emit_unblock AS (
  SELECT 'unblock'::text AS kind, l AS address, NULL::text AS task_id,
         NULL::int AS version, :PROMISE_JSON AS promise
  FROM :SRC u CROSS JOIN LATERAL unnest(COALESCE(:LISTENERS, '{}')) AS l
)";

fn settle_self(fulfilled: &str) -> String {
    fill(SETTLE_SELF, &[(":FULFILLED", fulfilled)])
}

fn settle_fanout(awaited: &str, awaiters: &str, fulfilled: &str, time: &str, trt: i64) -> String {
    // `x = ANY((SELECT ...))` parses as the *subquery* form of ANY, which
    // compares text against text[]. Wrapping the scalar subquery in COALESCE
    // makes it an ordinary array expression, and gives the "settlement did not
    // fire" case an empty array rather than NULL.
    let awaiters = String::from("COALESCE(") + awaiters + ", '{}'::text[])";
    fill(
        SETTLE_FANOUT,
        &[
            (":AWAITED", awaited),
            (":AWAITERS", &awaiters),
            (":FULFILLED", fulfilled),
            (":TIME", time),
            (":TRT", &trt.to_string()),
        ],
    )
}

fn settle_unblock(src: &str, listeners: &str) -> String {
    fill(
        SETTLE_UNBLOCK,
        &[
            (":SRC", src),
            (":LISTENERS", listeners),
            (":PROMISE_JSON", &promise_json("u")),
        ],
    )
}

/// The batch settlement cascade, shared by `try_timeout` (explicit id list) and
/// `process_timeouts` (the sweep queue). `selection` is the WHERE clause that
/// picks the rows to expire; it may reference the `promises` table directly.
///
/// This is the one place where the collapse costs something: expiring N
/// promises may touch a row that is both an expiring promise's awaiter and
/// another's, so `marked_ready` becomes an aggregate (`ready_agg`) rather than
/// a plain `UPDATE ... WHERE awaited_id IN (...)`.
fn expire_batch_sql(selection: &str, time_param: &str, trt: i64) -> String {
    let self_set = settle_self("(p.task_state IS NOT NULL AND p.task_state <> 'fulfilled')");
    fill(
        "
WITH expired AS (
  SELECT id, callbacks, listeners, task_state FROM promises
  WHERE :SELECTION
  FOR UPDATE
),
-- The same rows, unlocked, for the emissions to read.
--
-- `expired` cannot serve them: the final SELECT references the emission CTEs,
-- which would re-evaluate `expired`, whose FOR UPDATE then finds rows this
-- same command has already settled and yields nothing — silently dropping
-- every unblock message. A plain scan sees the statement's snapshot, which is
-- exactly the pre-settlement state the listeners live in.
expired_snap AS (
  SELECT id, listeners FROM promises WHERE :SELECTION
),
fulfilled AS (
  SELECT id FROM expired WHERE task_state IS NOT NULL AND task_state <> 'fulfilled'
),
fulfilled_ids AS (
  SELECT COALESCE(array_agg(id), '{}') AS ids FROM fulfilled
),
-- marked_ready, aggregated: one awaiter may be woken by several expiring promises
ready_agg AS (
  SELECT aw AS awaiter, array_agg(DISTINCT e.id) AS awaited_ids
  FROM expired e CROSS JOIN LATERAL unnest(e.callbacks) aw
  WHERE aw NOT IN (SELECT id FROM fulfilled)
  GROUP BY aw
),
suspended_awaiters AS (
  SELECT p.id, p.task_version, p.target FROM promises p
  WHERE p.task_state = 'suspended' AND p.id IN (SELECT awaiter FROM ready_agg)
),
updated_expired AS (
  UPDATE promises p SET
    state = CASE WHEN p.is_timer THEN 'resolved' ELSE 'rejected_timedout' END,
    settled_at = p.timeout_at,
    :SELF_SET
  WHERE p.id IN (SELECT id FROM expired)
  RETURNING p.*
),
emit_unblock AS (
  SELECT 'unblock'::text AS kind, l AS address, NULL::text AS task_id,
         NULL::int AS version, :PROMISE_JSON AS promise
  FROM updated_expired u
  JOIN expired_snap e ON e.id = u.id
  CROSS JOIN LATERAL unnest(e.listeners) AS l
),
fanout AS (
  UPDATE promises q SET
    callbacks = (SELECT COALESCE(array_agg(b), '{}') FROM unnest(q.callbacks) b
                WHERE b NOT IN (SELECT id FROM fulfilled)),
    resumes = q.resumes || COALESCE((SELECT r.awaited_ids FROM ready_agg r WHERE r.awaiter = q.id), '{}'),
    task_state = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN 'pending' ELSE q.task_state END,
    retry_timeout_at = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN :TIME + :TRT ELSE q.retry_timeout_at END,
    lease_timeout_at = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN NULL ELSE q.lease_timeout_at END,
    ttl = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN NULL ELSE q.ttl END,
    pid = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN NULL ELSE q.pid END
  WHERE q.id NOT IN (SELECT id FROM expired)
    AND ( EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
          OR q.callbacks && (SELECT ids FROM fulfilled_ids) )
  RETURNING q.id
),
emit_resume AS (
  SELECT 'execute'::text AS kind, s.target AS address, s.id AS task_id,
         s.task_version::int AS version, NULL::jsonb AS promise
  FROM suspended_awaiters s WHERE s.target IS NOT NULL
)
SELECT :MESSAGES",
        &[
            (":SELECTION", selection),
            (":SELF_SET", &self_set),
            (":PROMISE_JSON", &promise_json("u")),
            (":TIME", time_param),
            (":TRT", &trt.to_string()),
            (":MESSAGES", &emitted_json(&["emit_unblock", "emit_resume"])),
        ],
    )
}

// ============================================================================
// Db implementation — one row per promise
// ============================================================================

impl PostgresDb<'_> {
    fn task_retry_timeout(&self) -> i64 {
        self.task_retry_timeout
    }

    // Ghost operation — runs before every user operation.
    fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let sql = expire_batch_sql(
            "id = ANY($1) AND state = 'pending' AND timeout_at <= $2",
            "$2",
            self.task_retry_timeout,
        );
        let row = rt_block_on(
            sqlx::query(&sql)
                .bind(&ids)
                .bind(time)
                .fetch_optional(self.tx().as_mut()),
        )?;
        if let Some(row) = row {
            self.absorb_and_arm_retries(&row, time + self.task_retry_timeout);
        }
        Ok(())
    }

    // Lock preamble. One row now, where the multi-table backend locked the
    // promise row and then the task row.
    fn lock_for_update(&self, id: &str) -> StorageResult<(bool, bool)> {
        let row = rt_block_on(
            sqlx::query("SELECT (task_state IS NOT NULL) AS has_task FROM promises WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        match row {
            Some(r) => Ok((true, r.get::<bool, _>("has_task"))),
            None => Ok((false, false)),
        }
    }

    // Fire callbacks for an already-settled promise, as its own statement so it
    // gets a fresh READ COMMITTED snapshot and sees callbacks committed by
    // concurrent transactions.
    fn process_callbacks(&self, promise_id: &str, time: i64) -> StorageResult<()> {
        let fanout = settle_fanout(
            "$1",
            "(SELECT b.callbacks FROM before b)",
            "false",
            "$2",
            self.task_retry_timeout,
        );
        let sql = format!(
            "
            WITH before AS (
              SELECT id, callbacks FROM promises WHERE id = $1 AND state <> 'pending'
            ),
            cleared AS (
              UPDATE promises SET callbacks = '{{}}'
              WHERE id = $1 AND EXISTS (SELECT 1 FROM before)
              RETURNING id
            ),
            {fanout}
            SELECT {messages}",
            messages = emitted_json(&["emit_resume"])
        );
        let row = rt_block_on(
            sqlx::query(&sql)
                .bind(promise_id)
                .bind(time)
                .fetch_optional(self.tx().as_mut()),
        )?;
        if let Some(row) = row {
            self.absorb_and_arm_retries(&row, time + self.task_retry_timeout);
        }
        Ok(())
    }

    // P-01: promise.get
    fn promise_get(&self, id: &str) -> StorageResult<Option<PromiseRecord>> {
        let row = rt_block_on(
            sqlx::query(&format!("SELECT {P_COLS} FROM promises WHERE id = $1"))
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(row.as_ref().map(row_to_promise))
    }

    // P-02: promise.create
    //
    // Five CTEs in the multi-table backend — promise, promise_timeout, task,
    // task_timeout, outgoing_execute — collapse to one INSERT plus the outbox.
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
            awaitable,
        } = *params;
        let trt = self.task_retry_timeout;

        let rows = rt_block_on(sqlx::query(&format!("
            WITH inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at,
                                    task_state, task_version, retry_timeout_at)
              VALUES ($1, $2, COALESCE($3::jsonb, '{{}}'), $4, $5::jsonb, $6, $7, $8,
                      CASE WHEN $10::text IS NOT NULL
                           THEN (CASE WHEN $9 THEN 'fulfilled' ELSE 'pending' END) END,
                      0,
                      CASE WHEN $10::text IS NOT NULL AND NOT $9 THEN $7 + {trt} END)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            emit_new AS (
              SELECT 'execute'::text AS kind, $10::text AS address, p.id AS task_id,
                     0::int AS version, NULL::jsonb AS promise
              FROM inserted_or_skipped_promise p WHERE p.task_state = 'pending'
            ),
            result AS (
              SELECT *, TRUE AS was_created FROM inserted_or_skipped_promise
              UNION ALL
              SELECT *, FALSE AS was_created FROM promises
              WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
            )
            SELECT {P_COLS}, was_created, {messages} FROM result
        ", messages = emitted_json(&["emit_new"])))
            .bind(id).bind(state).bind(param_headers).bind(param_data).bind(tags)  // $1-$5
            .bind(timeout_at).bind(created_at).bind(settled_at)                     // $6-$8
            .bind(already_timedout).bind(address)                                   // $9-$10
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            // CTE snapshot race: a concurrent INSERT committed after our
            // snapshot, so the UNION ALL fallback saw neither row. Nothing was
            // committed — signal the caller to retry.
            return Err(StorageError::Serialization);
        }
        self.absorb_and_arm_retries(&rows[0], created_at + trt);
        let was_created: bool = rows[0].get("was_created");
        if was_created && !already_timedout {
            self.arm_promise_timeout(id, timeout_at, awaitable);
        }
        Ok(PromiseCreateResult {
            was_created,
            promise: row_to_promise(&rows[0]),
        })
    }

    // P-03: promise.settle — lock preamble + one cascade statement
    fn promise_settle(&self, params: &PromiseSettleParams) -> StorageResult<PromiseSettleResult> {
        let PromiseSettleParams {
            id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;

        // Statement 1: acquire the row lock — blocks until a concurrent
        // task.suspend writing our `callbacks` finishes.
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 2: fresh snapshot, so `before` sees those awaiters.
        let self_set = settle_self(
            "(SELECT b.task_state IS NOT NULL AND b.task_state <> 'fulfilled' FROM before b)",
        );
        let unblock = settle_unblock("updated_promise", "(SELECT b.listeners FROM before b)");
        let fanout = settle_fanout(
            "$1",
            "(SELECT CASE WHEN b.state = 'pending' THEN b.callbacks END FROM before b)",
            "(SELECT b.state = 'pending' AND b.task_state IS NOT NULL AND b.task_state <> 'fulfilled' FROM before b)",
            "$5",
            self.task_retry_timeout,
        );

        let rows = rt_block_on(sqlx::query(&format!("
            WITH before AS (
              SELECT id, state, task_state, callbacks, listeners FROM promises WHERE id = $1
            ),
            updated_promise AS (
              UPDATE promises p
              SET state = $2, value_headers = COALESCE($3::jsonb, '{{}}'), value_data = $4, settled_at = $5,
                  {self_set}
              WHERE p.id = $1 AND p.state = 'pending'
              RETURNING p.*
            ),
            {unblock},
            {fanout},
            result AS (
              SELECT *, true AS was_settled FROM updated_promise
              UNION ALL
              SELECT *, false AS was_settled FROM promises
              WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT {P_COLS}, was_settled, {messages} FROM result
        ", messages = emitted_json(&["emit_unblock", "emit_resume"])))
            .bind(id).bind(state).bind(value_headers).bind(value_data).bind(settled_at)
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(PromiseSettleResult {
                was_settled: false,
                promise: None,
            });
        }
        let row = &rows[0];
        self.absorb_and_arm_retries(row, settled_at + self.task_retry_timeout);
        Ok(PromiseSettleResult {
            was_settled: row.get("was_settled"),
            promise: Some(row_to_promise(row)),
        })
    }

    // P-04: promise.register_callback
    fn promise_register_callback(
        &self,
        awaited_id: &str,
        awaiter_id: &str,
        time: i64,
    ) -> StorageResult<RegisterCallbackResult> {
        let trt = self.task_retry_timeout;
        let rows = rt_block_on(sqlx::query(&format!("
            WITH awaited AS (
              SELECT * FROM promises WHERE id = $1 FOR UPDATE
            ),
            awaiter AS (
              SELECT * FROM promises WHERE id = $2 FOR UPDATE
            ),
            -- An awaited that may not be awaited is refused by the caller with
            -- a 422, so nothing below may write for it: not the link, and not
            -- the direct resume, which would wake the awaiter for a
            -- registration that never happened.
            awaitable AS (
              SELECT EXISTS (SELECT 1 FROM awaited WHERE awaitable) AS ok
            ),
            -- link: awaited still pending and awaitable, awaiter targeted and pending
            linked AS (
              UPDATE promises p SET callbacks = p.callbacks || $2
              WHERE p.id = $1
                AND NOT (p.callbacks @> ARRAY[$2])
                AND (SELECT ok FROM awaitable)
                AND EXISTS (SELECT 1 FROM awaited WHERE state = 'pending')
                AND EXISTS (SELECT 1 FROM awaiter WHERE target IS NOT NULL AND state = 'pending')
              RETURNING p.id
            ),
            -- direct resume: awaited already settled. A suspended awaiter is
            -- woken; a pending/acquired one only records the ready callback.
            resumed AS (
              UPDATE promises p SET
                task_state = CASE WHEN p.task_state = 'suspended' THEN 'pending' ELSE p.task_state END,
                retry_timeout_at   = CASE WHEN p.task_state = 'suspended' THEN $3 + {trt} ELSE p.retry_timeout_at END,
                lease_timeout_at = CASE WHEN p.task_state = 'suspended' THEN NULL ELSE p.lease_timeout_at END,
                ttl        = CASE WHEN p.task_state = 'suspended' THEN NULL ELSE p.ttl END,
                pid        = CASE WHEN p.task_state = 'suspended' THEN NULL ELSE p.pid END,
                -- 'suspended' too: the row is being woken in this same
                -- statement, so the pre-update state is what this CASE sees,
                -- and a woken awaiter records the resume that woke it — which
                -- is what SQLite and MySQL do by marking the callback ready
                -- after their resume UPDATE.
                resumes    = CASE WHEN p.task_state IN ('pending', 'acquired', 'suspended')
                                    AND NOT (p.resumes @> ARRAY[$1])
                                  THEN p.resumes || $1 ELSE p.resumes END
              WHERE p.id = $2
                AND p.task_state IN ('pending', 'acquired', 'suspended')
                AND (SELECT ok FROM awaitable)
                AND EXISTS (SELECT 1 FROM awaited WHERE state <> 'pending')
              RETURNING p.id, p.task_version, p.target,
                        (SELECT a.task_state FROM awaiter a) AS prev_task_state
            ),
            -- Read from the pre-update snapshot, not from `resumed`.
            --
            -- `outbox_resume` was a data-modifying CTE, so it ran on its own
            -- and the final SELECT never depended on the UPDATE. A plain CTE
            -- does not: referencing it pulls `resumed` into the final scan,
            -- and `awaiter`'s FOR UPDATE then finds a row this same command
            -- has already updated and yields nothing for it — losing the
            -- awaiter from the result entirely. The emission is a function of
            -- the pre-state anyway: a suspended, targeted awaiter of a settled
            -- promise, at a version the resume does not change.
            emit_resume AS (
              SELECT 'execute'::text AS kind, a.target AS address, a.id AS task_id,
                     a.task_version::int AS version, NULL::jsonb AS promise
              FROM awaiter a
              WHERE a.task_state = 'suspended' AND a.target IS NOT NULL
                AND (SELECT ok FROM awaitable)
                AND EXISTS (SELECT 1 FROM awaited WHERE state <> 'pending')
            )
            SELECT 'awaited' AS type, {awaited_cols}, {messages} FROM awaited
            UNION ALL
            SELECT 'awaiter' AS type, {awaiter_cols}, {messages} FROM awaiter
        ",
            awaited_cols = p_cols("awaited"),
            awaiter_cols = p_cols("awaiter"),
            messages = emitted_json(&["emit_resume"]),
        ))
            .bind(awaited_id).bind(awaiter_id).bind(time)
            .fetch_all(self.tx().as_mut()))?;

        if let Some(row) = rows.first() {
            self.absorb_and_arm_retries(row, time + trt);
        }
        let mut awaited = None;
        let mut awaiter = None;
        for row in &rows {
            let typ: String = row.get("type");
            let promise = row_to_promise(row);
            match typ.as_str() {
                "awaited" => awaited = Some(promise),
                "awaiter" => awaiter = Some(promise),
                _ => {}
            }
        }
        Ok(RegisterCallbackResult { awaited, awaiter })
    }

    // P-05: promise.register_listener
    fn promise_register_listener(
        &self,
        awaited_id: &str,
        address: &str,
    ) -> StorageResult<Option<PromiseRecord>> {
        let rows = rt_block_on(
            sqlx::query(&format!(
                "
            WITH locked_promise AS (
              SELECT * FROM promises WHERE id = $1 FOR UPDATE
            ),
            -- A listener is an obligation, and `awaitable` is where the server
            -- owes an observation. Refused by the caller with a 422, so nothing
            -- is written for a promise that may not be awaited.
            linked AS (
              UPDATE promises p SET listeners = p.listeners || $2
              WHERE p.id = $1
                AND NOT (p.listeners @> ARRAY[$2])
                AND EXISTS (SELECT 1 FROM locked_promise WHERE state = 'pending' AND awaitable)
              RETURNING p.id
            )
            SELECT {cols} FROM locked_promise",
                cols = p_cols("locked_promise")
            ))
            .bind(awaited_id)
            .bind(address)
            .fetch_all(self.tx().as_mut()),
        )?;

        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(row_to_promise(&rows[0])))
    }

    // P-06: promise.search
    fn promise_search(
        &self,
        state: Option<&str>,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<PromiseRecord>> {
        let rows = rt_block_on(
            sqlx::query(&format!(
                "SELECT {P_COLS} FROM promises
                 WHERE ($1::text IS NULL OR state = $1)
                   AND ($2::jsonb IS NULL OR tags @> $2::jsonb)
                   AND ($3::text IS NULL OR id > $3)
                 ORDER BY id ASC LIMIT $4"
            ))
            .bind(state)
            .bind(tags)
            .bind(cursor)
            .bind(limit)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    // T-01: task.get — `resumes` is a local array now, not a COUNT over a join
    fn task_get(&self, id: &str) -> StorageResult<Option<TaskRecord>> {
        let row = rt_block_on(
            sqlx::query(
                "SELECT id, task_state, task_version, ttl, pid, resumes
                 FROM promises WHERE id = $1 AND task_state IS NOT NULL",
            )
            .bind(id)
            .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(row.as_ref().map(row_to_task))
    }

    // T-02: task.create
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
        let task_initial_state = if already_timedout {
            "fulfilled"
        } else {
            "acquired"
        };

        let rows = rt_block_on(sqlx::query(&format!("
            WITH inserted_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at,
                                    task_state, task_version, lease_timeout_at, ttl, pid)
              VALUES ($1, $2, COALESCE($3::jsonb, '{{}}'), $4, $5::jsonb, $6, $7, $8,
                      $12, CASE WHEN $12 = 'acquired' THEN 1 ELSE 0 END,
                      CASE WHEN NOT $9 THEN $7 + $10 END,
                      CASE WHEN NOT $9 THEN $10 END,
                      CASE WHEN NOT $9 THEN $11 END)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            promise AS (
              SELECT * FROM inserted_promise
              UNION ALL
              SELECT * FROM promises WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_promise)
            )
            SELECT {cols},
              EXISTS (SELECT 1 FROM inserted_promise) AS task_created,
              p.task_state, p.task_version
            FROM promise p
        ", cols = p_cols("p")))
            .bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags) // $1-$5
            .bind(timeout_at).bind(created_at).bind(settled_at)                            // $6-$8
            .bind(already_timedout).bind(ttl).bind(pid).bind(task_initial_state)           // $9-$12
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Err(StorageError::Serialization);
        }
        let row = &rows[0];
        let promise = row_to_promise(row);
        let task_created: bool = row.get("task_created");

        if task_created {
            if !already_timedout {
                self.arm_promise_timeout(
                    promise_id,
                    timeout_at,
                    resonate_core::types::is_awaitable(&promise.tags),
                );
                self.arm_lease(promise_id, pid, created_at + ttl);
            }
            return Ok(TaskCreateResult {
                promise,
                task_created: true,
                task_state: Some(task_initial_state.to_string()),
                task_version: Some(if already_timedout { 0 } else { 1 }),
            });
        }

        Ok(TaskCreateResult {
            promise,
            task_created: false,
            task_state: row
                .try_get::<Option<String>, _>("task_state")
                .ok()
                .flatten(),
            task_version: row
                .try_get::<Option<i32>, _>("task_version")
                .ok()
                .flatten()
                .map(|v| v as i64),
        })
    }

    // T-03: task.acquire
    fn task_acquire(&self, params: &TaskAcquireParams) -> StorageResult<TaskAcquireResult> {
        let TaskAcquireParams {
            task_id,
            version,
            time,
            ttl,
            pid,
        } = *params;
        let rows = rt_block_on(sqlx::query(&format!("
            WITH before AS (
              SELECT id, task_state, task_version FROM promises WHERE id = $1 AND task_state IS NOT NULL
            ),
            acquired_task AS (
              UPDATE promises p SET
                task_state = 'acquired', task_version = p.task_version + 1,
                lease_timeout_at = $3 + $4, ttl = $4, pid = $5, retry_timeout_at = NULL,
                resumes = '{{}}'                    -- deleted_ready_callbacks
              WHERE p.id = $1 AND p.task_version = $2 AND p.task_state = 'pending'
              RETURNING p.id, p.task_state, p.task_version
            )
            SELECT {cols},
              COALESCE(a.task_state, b.task_state)     AS task_state,
              COALESCE(a.task_version, b.task_version) AS task_version,
              (a.id IS NOT NULL)                       AS was_acquired
            FROM before b
            JOIN promises p ON p.id = b.id
            LEFT JOIN acquired_task a ON a.id = b.id
        ", cols = p_cols("p")))
            .bind(task_id).bind(version as i32).bind(time).bind(ttl).bind(pid)
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(TaskAcquireResult {
                promise: None,
                was_acquired: false,
                task_state: None,
                task_version: None,
            });
        }
        let row = &rows[0];
        let task_state: String = row.get("task_state");
        let was_acquired: bool = row.get("was_acquired");
        if was_acquired {
            self.arm_lease(task_id, pid, time + ttl);
        }
        Ok(TaskAcquireResult {
            promise: Some(row_to_promise(row)),
            was_acquired,
            task_state: Some(parse_task_state(&task_state)),
            task_version: Some(row.get::<i32, _>("task_version") as i64),
        })
    }

    // T-04: task.fence (create variant) — fence on one row, insert another
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
            awaitable,
        } = *params;
        let trt = self.task_retry_timeout;

        let rows = rt_block_on(sqlx::query(&format!("
            WITH fence_check AS (
              SELECT id, task_state, task_version FROM promises WHERE id = $1 AND task_state IS NOT NULL
            ),
            fence_ok AS (
              SELECT EXISTS (SELECT 1 FROM fence_check WHERE task_state = 'acquired' AND task_version = $2) AS ok
            ),
            inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at,
                                    task_state, task_version, retry_timeout_at)
              SELECT $3, $4, COALESCE($5::jsonb, '{{}}'), $6, $7::jsonb, $8, $9, $10,
                     CASE WHEN $12::text IS NOT NULL
                          THEN (CASE WHEN $11::bool THEN 'fulfilled' ELSE 'pending' END) END,
                     0,
                     CASE WHEN $12::text IS NOT NULL AND NOT $11::bool THEN $9 + {trt} END
              WHERE (SELECT ok FROM fence_ok)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            emit_new AS (
              SELECT 'execute'::text AS kind, $12::text AS address, p.id AS task_id,
                     0::int AS version, NULL::jsonb AS promise
              FROM inserted_or_skipped_promise p WHERE p.task_state = 'pending'
            ),
            result AS (
              SELECT * FROM inserted_or_skipped_promise
              UNION ALL
              SELECT * FROM promises
              WHERE id = $3 AND (SELECT ok FROM fence_ok)
                AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
            )
            SELECT
              EXISTS (SELECT 1 FROM fence_check) AS task_exists,
              (SELECT ok FROM fence_ok) AS fence_ok,
              EXISTS (SELECT 1 FROM inserted_or_skipped_promise p
                      WHERE p.state = 'pending') AS promise_pending_created,
              {cols}, {messages}
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        ", cols = p_cols("r"), messages = emitted_json(&["emit_new"])))
            .bind(task_id).bind(version as i32)                                            // $1-$2
            .bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags)  // $3-$7
            .bind(timeout_at).bind(created_at).bind(settled_at)                             // $8-$10
            .bind(already_timedout).bind(address)                                           // $11-$12
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Err(StorageError::Serialization);
        }
        let row = &rows[0];
        self.absorb_and_arm_retries(row, created_at + trt);
        let promise_id_val: Option<String> = row.get("id");
        // A promise joins the eager sweep when this statement created it
        // still pending — the CTE says so directly, because "got a retry
        // deadline" no longer names the same set: an awaitable, untargeted
        // promise is armed with no retry to ride on.
        let promise_pending_created: bool = row.get("promise_pending_created");
        if promise_pending_created {
            self.arm_promise_timeout(promise_id, timeout_at, awaitable);
        }
        Ok(TaskFenceResult {
            task_exists: row.get("task_exists"),
            fence_ok: row.get("fence_ok"),
            promise: promise_id_val.map(|_| row_to_promise(row)),
        })
    }

    // T-04: task.fence (settle variant) — fence on one row, settlement cascade on another
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

        let self_set = settle_self(
            "(SELECT b.task_state IS NOT NULL AND b.task_state <> 'fulfilled' FROM before b)",
        );
        let unblock = settle_unblock("updated_promise", "(SELECT b.listeners FROM before b)");
        let fanout = settle_fanout(
            "$3",
            "(SELECT CASE WHEN b.state = 'pending' THEN b.callbacks END FROM before b)",
            "(SELECT b.state = 'pending' AND b.task_state IS NOT NULL AND b.task_state <> 'fulfilled' FROM before b)",
            "$7",
            self.task_retry_timeout,
        );

        let rows = rt_block_on(sqlx::query(&format!("
            WITH fence_check AS (
              SELECT id, task_state, task_version FROM promises WHERE id = $1 AND task_state IS NOT NULL
            ),
            fence_ok AS (
              SELECT EXISTS (SELECT 1 FROM fence_check WHERE task_state = 'acquired' AND task_version = $2) AS ok
            ),
            locked_promise AS (
              SELECT * FROM promises WHERE id = $3 AND (SELECT ok FROM fence_ok) FOR UPDATE
            ),
            before AS (
              SELECT id, state, task_state, callbacks, listeners FROM locked_promise
            ),
            updated_promise AS (
              UPDATE promises p
              SET state = $4, value_headers = COALESCE($5::jsonb, '{{}}'), value_data = $6, settled_at = $7,
                  {self_set}
              WHERE p.id = $3 AND p.state = 'pending' AND (SELECT ok FROM fence_ok)
                AND EXISTS (SELECT 1 FROM locked_promise)
              RETURNING p.*
            ),
            {unblock},
            {fanout},
            result AS (
              SELECT * FROM updated_promise
              UNION ALL
              SELECT * FROM locked_promise WHERE NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT
              EXISTS (SELECT 1 FROM fence_check) AS task_exists,
              (SELECT ok FROM fence_ok) AS fence_ok,
              {cols}, {messages}
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        ", cols = p_cols("r"), messages = emitted_json(&["emit_unblock", "emit_resume"])))
            .bind(task_id).bind(version as i32)                                                     // $1-$2
            .bind(promise_id).bind(state).bind(value_headers).bind(value_data).bind(settled_at)     // $3-$7
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(TaskFenceResult {
                task_exists: false,
                fence_ok: false,
                promise: None,
            });
        }
        let row = &rows[0];
        self.absorb_and_arm_retries(row, settled_at + self.task_retry_timeout);
        let promise_id_val: Option<String> = row.get("id");
        Ok(TaskFenceResult {
            task_exists: row.get("task_exists"),
            fence_ok: row.get("fence_ok"),
            promise: promise_id_val.map(|_| row_to_promise(row)),
        })
    }

    // T-05: task.heartbeat — extend the lease of every task this pid still holds
    fn task_heartbeat(&self, pid: &str, tasks: &[(&str, i64)], time: i64) -> StorageResult<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = tasks.iter().map(|(id, _)| id.to_string()).collect();
        let versions: Vec<i32> = tasks.iter().map(|(_, v)| *v as i32).collect();

        // RETURNING, because the new deadline is `$3 + p.ttl` and `ttl` is a
        // column: the caller cannot compute what was written without reading
        // it back. The only statement in this backend that needed new SQL to
        // announce its deadline.
        let rows = rt_block_on(
            sqlx::query(
                "
            WITH task_data AS (
              SELECT unnest($1::text[]) AS id, unnest($2::int[]) AS version
            )
            UPDATE promises p SET lease_timeout_at = $3 + p.ttl
            FROM task_data td
            WHERE p.id = td.id AND p.task_version = td.version
              AND p.task_state = 'acquired' AND p.pid = $4
            -- The promise-liveness guard: a heartbeat on a task whose promise
            -- is pending-but-expired is a no-op. This is the one operation
            -- that does not sweep first, so without it the lease would be
            -- extended in the window before the wheel reaches the row.
              AND (p.state != 'pending' OR p.timeout_at > $3)
            RETURNING p.id, p.lease_timeout_at
        ",
            )
            .bind(&ids)
            .bind(&versions)
            .bind(time)
            .bind(pid)
            .fetch_all(self.tx().as_mut()),
        )?;
        for row in &rows {
            let id: String = row.get("id");
            let lease_timeout_at: i64 = row.get("lease_timeout_at");
            self.arm_lease(&id, pid, lease_timeout_at);
        }
        Ok(())
    }

    // T-06: task.suspend
    fn task_suspend(
        &self,
        task_id: &str,
        version: i64,
        awaited_ids: &[&str],
    ) -> StorageResult<TaskSuspendResult> {
        let awaited: Vec<String> = awaited_ids.iter().map(|s| s.to_string()).collect();

        // Statement 1: lock every row this touches, lowest id first, so a
        // concurrent settle taking the same rows cannot deadlock with us.
        let mut lock_ids: Vec<String> = awaited.clone();
        lock_ids.push(task_id.to_string());
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ANY($1) ORDER BY id FOR UPDATE")
                .bind(&lock_ids)
                .fetch_all(self.tx().as_mut()),
        )?;

        // Statement 2: fresh snapshot — sees everything committed before the locks.
        let rows = rt_block_on(sqlx::query("
            WITH me AS (
              SELECT id, task_state, task_version FROM promises WHERE id = $1 AND task_state IS NOT NULL
            ),
            matched AS (
              SELECT EXISTS (SELECT 1 FROM me WHERE task_version = $2 AND task_state = 'acquired') AS ok
            ),
            awaited AS (
              SELECT id, state, awaitable
              FROM promises WHERE id = ANY($3) AND (SELECT ok FROM matched)
            ),
            missing AS (
              SELECT (COALESCE(array_length($3::text[], 1), 0) - COUNT(*)::INT) AS cnt FROM awaited
            ),
            -- `awaitable` is the generated column of the same four tags as
            -- `resonate_core::types::is_awaitable`. A promise nothing outside
            -- its own execution can settle may not be awaited.
            non_awaitable AS (
              SELECT COUNT(*)::INT AS cnt FROM awaited WHERE NOT awaitable
            ),
            can_suspend AS (
              SELECT 1 WHERE (SELECT ok FROM matched)
                AND (SELECT cnt FROM missing) = 0
                AND (SELECT cnt FROM non_awaitable) = 0
                AND NOT EXISTS (SELECT 1 FROM awaited WHERE state <> 'pending')
            ),
            -- link the awaited rows (other than the task's own, handled below)
            linked AS (
              UPDATE promises p SET callbacks = p.callbacks || $1
              WHERE p.id = ANY($3) AND p.id <> $1
                AND NOT (p.callbacks @> ARRAY[$1])
                AND EXISTS (SELECT 1 FROM can_suspend)
              RETURNING p.id
            ),
            suspended AS (
              UPDATE promises p SET
                task_state = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN 'suspended' ELSE p.task_state END,
                retry_timeout_at   = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.retry_timeout_at END,
                lease_timeout_at = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.lease_timeout_at END,
                ttl        = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.ttl END,
                pid        = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.pid END,
                -- deleted_ready_callbacks: fires on a version match even when
                -- the suspend itself is refused because an awaited promise settled
                resumes    = CASE WHEN (SELECT ok FROM matched) AND (SELECT cnt FROM missing) = 0
                                    AND (SELECT cnt FROM non_awaitable) = 0
                               THEN '{}' ELSE p.resumes END,
                callbacks   = CASE WHEN $1 = ANY($3) AND EXISTS (SELECT 1 FROM can_suspend)
                                    AND NOT (p.callbacks @> ARRAY[$1])
                               THEN p.callbacks || $1 ELSE p.callbacks END
              WHERE p.id = $1
                AND ((SELECT ok FROM matched) AND (SELECT cnt FROM missing) = 0
                     AND (SELECT cnt FROM non_awaitable) = 0)
              RETURNING p.id
            )
            SELECT
              (SELECT ok FROM matched) AS task_matched,
              EXISTS (SELECT 1 FROM can_suspend) AS was_suspended,
              (SELECT cnt FROM missing) AS missing_count,
              (SELECT cnt FROM non_awaitable) AS non_awaitable_count
        ")
            .bind(task_id).bind(version as i32).bind(&awaited)
            .fetch_one(self.tx().as_mut()))?;

        Ok(TaskSuspendResult {
            task_matched: rows.get("task_matched"),
            was_suspended: rows.get("was_suspended"),
            missing_count: rows.get("missing_count"),
            non_awaitable_count: rows.get("non_awaitable_count"),
        })
    }

    // T-07: task.fulfill — the task and the promise are the same row, so the
    // multi-table backend's `fulfilled_acquired_task` and `updated_promise`
    // must become one UPDATE.
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
        debug_assert_eq!(
            task_id, promise_id,
            "task.fulfill assumes the task and its promise are one row"
        );

        // Statement 1: lock preamble.
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = $1 FOR UPDATE")
                .bind(promise_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // `fulfilled` here is the task transition, which also drives the
        // promise settlement — hence one shared guard.
        let guard = "(SELECT b.task_state = 'acquired' AND b.task_version = $2 FROM before b)";
        let settle_guard =
            "(SELECT b.task_state = 'acquired' AND b.task_version = $2 AND b.state = 'pending' FROM before b)";
        let self_set = settle_self(guard);
        let unblock = settle_unblock("updated_promise", "(SELECT b.listeners FROM before b)");
        let fanout = settle_fanout(
            "$3",
            &format!("(SELECT CASE WHEN {settle_guard} THEN b.callbacks END FROM before b)"),
            guard,
            "$7",
            self.task_retry_timeout,
        );

        let rows = rt_block_on(sqlx::query(&format!("
            WITH before AS (
              SELECT id, state, task_state, task_version, callbacks, listeners FROM promises WHERE id = $3
            ),
            updated_promise AS (
              UPDATE promises p
              SET state = CASE WHEN p.state = 'pending' THEN $4 ELSE p.state END,
                  value_headers = CASE WHEN p.state = 'pending' THEN COALESCE($5::jsonb, '{{}}') ELSE p.value_headers END,
                  value_data    = CASE WHEN p.state = 'pending' THEN $6 ELSE p.value_data END,
                  settled_at    = CASE WHEN p.state = 'pending' THEN $7 ELSE p.settled_at END,
                  {self_set}
              WHERE p.id = $3 AND p.task_state = 'acquired' AND p.task_version = $2
              RETURNING p.*
            ),
            {unblock},
            {fanout},
            result AS (
              SELECT * FROM updated_promise
              UNION ALL
              SELECT * FROM promises WHERE id = $3 AND NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT {cols},
              EXISTS (SELECT 1 FROM updated_promise) AS task_fulfilled,
              (SELECT b.task_state IS NOT NULL FROM before b) AS task_exists,
              {messages}
            FROM result r
        ", cols = p_cols("r"), messages = emitted_json(&["emit_unblock", "emit_resume"])))
            .bind(task_id).bind(version as i32)                                                 // $1-$2
            .bind(promise_id).bind(state).bind(value_headers).bind(value_data).bind(settled_at) // $3-$7
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(TaskFulfillResult {
                task_exists: false,
                task_fulfilled: false,
                promise: None,
            });
        }
        let row = &rows[0];
        self.absorb_and_arm_retries(row, settled_at + self.task_retry_timeout);
        Ok(TaskFulfillResult {
            task_exists: row
                .try_get::<Option<bool>, _>("task_exists")
                .ok()
                .flatten()
                .unwrap_or(false),
            task_fulfilled: row.get("task_fulfilled"),
            promise: Some(row_to_promise(row)),
        })
    }

    // T-08: task.release
    fn task_release(
        &self,
        task_id: &str,
        version: i64,
        time: i64,
        ttl: i64,
    ) -> StorageResult<TaskReleaseResult> {
        let row = rt_block_on(
            sqlx::query(
                "
            WITH released_task AS (
              UPDATE promises p SET
                task_state = 'pending', retry_timeout_at = $3 + $4,
                lease_timeout_at = NULL, ttl = NULL, pid = NULL
              WHERE p.id = $1 AND p.task_version = $2 AND p.task_state = 'acquired'
              RETURNING p.id, p.task_version, p.target
            ),
            emit_released AS (
              SELECT 'execute'::text AS kind, t.target AS address, t.id AS task_id,
                     t.task_version::int AS version, NULL::jsonb AS promise
              FROM released_task t WHERE t.target IS NOT NULL
            )
            SELECT
              EXISTS (SELECT 1 FROM released_task) AS task_released,
              EXISTS (SELECT 1 FROM promises WHERE id = $1 AND task_state IS NOT NULL) AS task_exists,
              :MESSAGES
        "
            .replace(":MESSAGES", &emitted_json(&["emit_released"]))
            .as_str(),
            )
            .bind(task_id)
            .bind(version as i32)
            .bind(time)
            .bind(ttl)
            .fetch_one(self.tx().as_mut()),
        )?;

        self.absorb_and_arm_retries(&row, time + ttl);
        Ok(TaskReleaseResult {
            task_released: row.get("task_released"),
            task_exists: row.get("task_exists"),
        })
    }

    // T-09: task.halt
    fn task_halt(&self, task_id: &str) -> StorageResult<TaskHaltResult> {
        let row = rt_block_on(
            sqlx::query(
                "
            WITH locked_task AS (
              SELECT id, task_state FROM promises WHERE id = $1 AND task_state IS NOT NULL FOR UPDATE
            ),
            halted_task AS (
              UPDATE promises p SET
                task_state = 'halted', retry_timeout_at = NULL, lease_timeout_at = NULL, ttl = NULL, pid = NULL
              WHERE p.id = $1 AND p.task_state IS NOT NULL
                AND p.task_state NOT IN ('fulfilled', 'halted')
              RETURNING p.id
            )
            SELECT
              EXISTS (SELECT 1 FROM locked_task) AS task_exists,
              EXISTS (SELECT 1 FROM locked_task WHERE task_state = 'fulfilled') AS task_fulfilled
        ",
            )
            .bind(task_id)
            .fetch_one(self.tx().as_mut()),
        )?;

        Ok(TaskHaltResult {
            task_exists: row.get("task_exists"),
            task_fulfilled: row.get("task_fulfilled"),
        })
    }

    // T-10: task.continue
    fn task_continue(&self, task_id: &str, time: i64) -> StorageResult<TaskContinueResult> {
        let trt = self.task_retry_timeout;
        let row = rt_block_on(
            sqlx::query(&format!(
                "
            WITH locked_task AS (
              SELECT id, task_state, task_version, target FROM promises
              WHERE id = $1 AND task_state IS NOT NULL FOR UPDATE
            ),
            continued_task AS (
              UPDATE promises p SET task_state = 'pending', retry_timeout_at = $2 + {trt}
              WHERE p.id = $1 AND p.task_state = 'halted'
              RETURNING p.id, p.task_version, p.target
            ),
            -- From the snapshot, not from `continued_task` — see
            -- `promise_register_callback` for why.
            emit_continued AS (
              SELECT 'execute'::text AS kind, t.target AS address, t.id AS task_id,
                     t.task_version::int AS version, NULL::jsonb AS promise
              FROM locked_task t WHERE t.task_state = 'halted' AND t.target IS NOT NULL
            )
            SELECT
              EXISTS (SELECT 1 FROM locked_task) AS task_exists,
              EXISTS (SELECT 1 FROM continued_task) AS continued,
              {messages}
        ",
                messages = emitted_json(&["emit_continued"])
            ))
            .bind(task_id)
            .bind(time)
            .fetch_one(self.tx().as_mut()),
        )?;

        self.absorb_and_arm_retries(&row, time + trt);
        Ok(TaskContinueResult {
            task_exists: row.get("task_exists"),
            continued: row.get("continued"),
        })
    }

    // T-11: task.search
    fn task_search(
        &self,
        state: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<TaskRecord>> {
        let rows = rt_block_on(
            sqlx::query(
                "SELECT id, task_state, task_version, ttl, pid, resumes FROM promises
                 WHERE task_state IS NOT NULL
                   AND ($1::text IS NULL OR task_state = $1)
                   AND ($2::text IS NULL OR id > $2)
                 ORDER BY id ASC LIMIT $3",
            )
            .bind(state)
            .bind(cursor)
            .bind(limit)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows.iter().map(row_to_task).collect())
    }

    fn compute_preload(&self, promise_id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let rows = rt_block_on(
            sqlx::query(&format!(
                "SELECT {P_COLS} FROM promises
                 WHERE branch_id = (SELECT branch_id FROM promises WHERE id = $1)
                   AND branch_id IS NOT NULL AND id <> $1
                 ORDER BY id ASC LIMIT $2"
            ))
            .bind(promise_id)
            .bind(self.preload_limit as i64)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    // S-01: schedule.get
    fn schedule_get(&self, id: &str) -> StorageResult<Option<ScheduleRecord>> {
        let row = rt_block_on(sqlx::query(
            "SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{}'::jsonb)::text AS promise_param_headers,
                    promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
             FROM schedules WHERE id = $1")
            .bind(id).fetch_optional(self.tx().as_mut()))?;
        Ok(row.as_ref().map(row_to_schedule))
    }

    // S-03: schedule.create — schedule_timeouts is gone, next_run_at *is* the queue
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

        let row = rt_block_on(sqlx::query("
            WITH inserted_or_skipped_schedule AS (
              INSERT INTO schedules (id, cron, promise_id, promise_timeout, promise_param_headers,
                                     promise_param_data, promise_tags, created_at, next_run_at)
              VALUES ($1, $2, $3, $4, COALESCE($5::jsonb, '{}'), $6, $7::jsonb, $8, $9)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            result AS (
              SELECT * FROM inserted_or_skipped_schedule
              UNION ALL
              SELECT * FROM schedules WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_schedule)
            )
            SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{}'::jsonb)::text AS promise_param_headers,
                   promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at,
                   EXISTS (SELECT 1 FROM inserted_or_skipped_schedule) AS was_created
            FROM result
        ")
            .bind(id).bind(cron).bind(promise_id).bind(promise_timeout)
            .bind(promise_param_headers).bind(promise_param_data).bind(promise_tags)
            .bind(created_at).bind(next_run_at)
            .fetch_one(self.tx().as_mut()))?;

        // Only a create that actually happened arms a deadline — an idempotent
        // re-create leaves the existing next_run_at where it was.
        if row.get::<bool, _>("was_created") {
            self.arm(
                next_run_at,
                Timeout::ScheduleDue {
                    schedule_id: id.to_string(),
                },
            );
        }
        Ok(row_to_schedule(&row))
    }

    // S-04: schedule.delete
    fn schedule_delete(&self, id: &str) -> StorageResult<bool> {
        let res = rt_block_on(
            sqlx::query("DELETE FROM schedules WHERE id = $1")
                .bind(id)
                .execute(self.tx().as_mut()),
        )?;
        Ok(res.rows_affected() > 0)
    }

    // S-05: schedule.search
    fn schedule_search(
        &self,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let rows = rt_block_on(sqlx::query(
            "SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{}'::jsonb)::text AS promise_param_headers,
                    promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
             FROM schedules
             WHERE ($1::jsonb IS NULL OR promise_tags @> $1::jsonb) AND ($2::text IS NULL OR id > $2)
             ORDER BY id ASC LIMIT $3")
            .bind(tags).bind(cursor).bind(limit).fetch_all(self.tx().as_mut()))?;
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
                "SELECT deadline, kind, id, pid FROM (
                     SELECT timeout_at AS deadline, 'promise' AS kind, id AS id, NULL::text AS pid
                       FROM promises WHERE state = 'pending' AND awaitable
                     UNION ALL
                     SELECT retry_timeout_at, 'retry', id, NULL
                       FROM promises WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL
                     UNION ALL
                     SELECT lease_timeout_at, 'lease', id, pid
                       FROM promises WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL
                     UNION ALL
                     SELECT next_run_at, 'schedule', id, NULL FROM schedules
                 ) d
                 ORDER BY deadline ASC, id ASC
                 LIMIT $1",
            )
            .bind(limit as i64)
            .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let at: i64 = r.get("deadline");
                let kind: String = r.get("kind");
                let id: String = r.get("id");
                let pid: Option<String> = r.get("pid");
                Timeout::from_parts(&kind, id, pid).map(|timeout| Scheduled { at, timeout })
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
                 WHERE next_run_at <= $1 AND ($2::text IS NULL OR id = $2)
                 ORDER BY id",
            )
            .bind(time)
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
        // $1=schedule_id, $2=fired_at, $3=next_run_at, $4=promise_tags, $5=time
        let rows = rt_block_on(sqlx::query(&format!("
            WITH schedule AS (
              SELECT *,
                REPLACE(REPLACE(promise_id, '{{{{.id}}}}', id), '{{{{.timestamp}}}}', CAST($2 AS TEXT)) AS computed_promise_id,
                ($2 + promise_timeout) AS computed_timeout_at,
                (promise_tags->>'resonate:target') AS address,
                ($5 >= ($2 + promise_timeout)) AS already_timedout
              FROM schedules
              WHERE id = $1 AND next_run_at = $2
            ),
            inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at,
                                    task_state, task_version, retry_timeout_at)
              SELECT s.computed_promise_id,
                CASE WHEN s.already_timedout
                     THEN (CASE WHEN ($4::jsonb->>'resonate:timer') = 'true' THEN 'resolved' ELSE 'rejected_timedout' END)
                     ELSE 'pending' END,
                COALESCE(s.promise_param_headers, '{{}}'), s.promise_param_data, $4::jsonb,
                s.computed_timeout_at, $2,
                CASE WHEN s.already_timedout THEN s.computed_timeout_at END,
                CASE WHEN s.address IS NOT NULL
                     THEN (CASE WHEN s.already_timedout THEN 'fulfilled' ELSE 'pending' END) END,
                0,
                CASE WHEN s.address IS NOT NULL AND NOT s.already_timedout THEN $5 + {trt} END
              FROM schedule s
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            emit_new AS (
              SELECT 'execute'::text AS kind, s.address AS address, p.id AS task_id,
                     0::int AS version, NULL::jsonb AS promise
              FROM inserted_or_skipped_promise p, schedule s
              WHERE p.task_state = 'pending'
            ),
            updated_schedule AS (
              UPDATE schedules SET last_run_at = $2, next_run_at = $3
              WHERE id = $1 AND next_run_at = $2
              RETURNING *
            )
            SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{{}}'::jsonb)::text AS promise_param_headers,
                   promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at,
                   EXISTS (SELECT 1 FROM inserted_or_skipped_promise p
                           WHERE p.state = 'pending') AS promise_pending_created,
                   {messages}
            FROM updated_schedule
        ", messages = emitted_json(&["emit_new"])))
            .bind(schedule_id).bind(fired_at).bind(next_run_at).bind(promise_tags_json).bind(time)
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(None);
        }
        self.absorb_and_arm_retries(&rows[0], time + trt);
        // The schedule advanced, so its own deadline moved. The promise this
        // firing created has one too, if it is still pending and awaitable —
        // asked of the CTE directly, because "got a retry deadline" no longer
        // names the same set: a cron that mints timers arms with no retry to
        // ride on.
        let schedule = row_to_schedule(&rows[0]);
        let promise_pending_created: bool = rows[0].get("promise_pending_created");
        if promise_pending_created {
            let computed_promise_id = schedule
                .promise_id
                .replace("{{.id}}", schedule_id)
                .replace("{{.timestamp}}", &fired_at.to_string());
            self.arm_promise_timeout(
                &computed_promise_id,
                fired_at + schedule.promise_timeout,
                resonate_core::types::is_awaitable(promise_tags),
            );
        }
        self.arm(
            schedule.next_run_at,
            Timeout::ScheduleDue {
                schedule_id: schedule_id.to_string(),
            },
        );
        Ok(Some(schedule))
    }

    #[allow(dead_code)] // the liveness probe the server will call
    fn ping(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql("SELECT 1").execute(self.tx().as_mut()))?;
        Ok(())
    }

    fn debug_reset(&self) -> StorageResult<()> {
        rt_block_on(
            sqlx::raw_sql("TRUNCATE promises, schedules CASCADE").execute(self.tx().as_mut()),
        )?;
        Ok(())
    }

    // Timeout processing — three sequential statements, as in the multi-table
    // backend. Statement 1 is the same cascade as `try_timeout`, driven by the
    // sweep predicate instead of an explicit id list.
    /// Fire expired timeouts, either all of them or one named.
    ///
    /// `only` is what makes the precise form precise, and it costs one bound
    /// parameter: every statement below already selects the rows of one queue
    /// past their deadline, and `$2` narrows that to a single id. A named
    /// timeout runs the statement for its own queue and skips the other two,
    /// so the narrow form is the sweep restricted to one row rather than a
    /// second implementation of it.
    ///
    /// `$2::text IS NULL` is the full sweep. The cast is load-bearing: without
    /// it Postgres cannot infer the parameter's type in a comparison against
    /// `NULL`.
    fn process_timeouts(&self, time: i64, only: Option<&Timeout>) -> StorageResult<()> {
        let trt = self.task_retry_timeout;
        let selected = |kind: &str| match only {
            None => Some(None::<String>),
            Some(t) if t.kind() == kind => Some(Some(t.id().to_string())),
            Some(_) => None,
        };

        // Statement 1: expired promises.
        //
        // `state = 'pending' AND awaitable` is the whole of what
        // promise_timeouts held: rows enter on create and leave on settle, and
        // only awaitable promises are ever swept eagerly — a deadline is owed
        // as a write exactly where someone can be waiting to observe it.
        // Internal promises still time out lazily, through `try_timeout`.
        if let Some(id) = selected("promise") {
            let sql = expire_batch_sql(
                "state = 'pending' AND awaitable AND timeout_at <= $1
                 AND ($2::text IS NULL OR id = $2)",
                "$1",
                trt,
            );
            let expired_row = rt_block_on(
                sqlx::query(&sql)
                    .bind(time)
                    .bind(&id)
                    .fetch_optional(self.tx().as_mut()),
            )?;
            if let Some(row) = expired_row {
                self.absorb_and_arm_retries(&row, time + trt);
            }
        }

        // Statement 2: expired task retry deadlines — re-enqueue the execute
        // message and push the deadline out.
        if let Some(id) = selected("retry") {
            let retry_row = rt_block_on(
                sqlx::query(&format!(
                    "
            WITH expired_retry AS (
              SELECT id, task_version, target FROM promises
              WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL AND retry_timeout_at <= $1
                AND ($2::text IS NULL OR id = $2)
              FOR UPDATE
            ),
            updated_retry AS (
              UPDATE promises SET retry_timeout_at = $1 + {trt}, pid = NULL
              WHERE id IN (SELECT id FROM expired_retry)
              RETURNING id
            ),
            emit_retry AS (
              SELECT 'execute'::text AS kind, e.target AS address, e.id AS task_id,
                     e.task_version::int AS version, NULL::jsonb AS promise
              FROM expired_retry e WHERE e.target IS NOT NULL
            )
            SELECT {messages}
        ",
                    messages = emitted_json(&["emit_retry"])
                ))
                .bind(time)
                .bind(&id)
                .fetch_optional(self.tx().as_mut()),
            )?;
            if let Some(row) = retry_row {
                self.absorb_and_arm_retries(&row, time + trt);
            }
        }

        // Statement 3: expired leases — the holder went away, hand the task back.
        if let Some(id) = selected("lease") {
            let lease_row = rt_block_on(
                sqlx::query(&format!(
                    "
            WITH expired_lease AS (
              SELECT id, task_version, target FROM promises
              WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL AND lease_timeout_at <= $1
                AND ($2::text IS NULL OR id = $2)
              FOR UPDATE
            ),
            released AS (
              UPDATE promises SET
                task_state = 'pending', retry_timeout_at = $1 + {trt},
                lease_timeout_at = NULL, ttl = NULL, pid = NULL
              WHERE id IN (SELECT id FROM expired_lease)
              RETURNING id
            ),
            emit_released AS (
              SELECT 'execute'::text AS kind, e.target AS address, e.id AS task_id,
                     e.task_version::int AS version, NULL::jsonb AS promise
              FROM expired_lease e WHERE e.target IS NOT NULL
            )
            SELECT {messages}
        ",
                    messages = emitted_json(&["emit_released"])
                ))
                .bind(time)
                .bind(&id)
                .fetch_optional(self.tx().as_mut()),
            )?;
            if let Some(row) = lease_row {
                self.absorb_and_arm_retries(&row, time + trt);
            }
        }

        Ok(())
    }

    // D-04: debug.snap — every section is now a projection of the one table
    fn snap(&self) -> StorageResult<Snapshot> {
        let promise_rows = rt_block_on(
            sqlx::query(&format!("SELECT {P_COLS} FROM promises ORDER BY id"))
                .fetch_all(self.tx().as_mut()),
        )?;
        let promises: Vec<PromiseRecord> = promise_rows.iter().map(row_to_promise).collect();

        let pt_rows = rt_block_on(
            sqlx::query(
                "SELECT id, timeout_at FROM promises
                 WHERE state = 'pending' AND awaitable ORDER BY id",
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

        // Non-ready callbacks only — the ready ones live in `resumes`.
        let cb_rows = rt_block_on(
            sqlx::query(
                "SELECT aw AS awaiter_id, id AS awaited_id
                 FROM promises CROSS JOIN LATERAL unnest(callbacks) AS aw
                 ORDER BY aw, id",
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
            sqlx::query(
                "SELECT id AS promise_id, l AS address
                 FROM promises CROSS JOIN LATERAL unnest(listeners) AS l
                 ORDER BY id, l",
            )
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
                "SELECT id, task_state, task_version, ttl, pid, resumes
                 FROM promises WHERE task_state IS NOT NULL ORDER BY id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let tasks: Vec<TaskRecord> = task_rows.iter().map(row_to_task).collect();

        let tt_rows = rt_block_on(
            sqlx::query(
                "SELECT id, 0 AS timeout_type, retry_timeout_at AS timeout_at FROM promises
                   WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL
                 UNION ALL
                 SELECT id, 1 AS timeout_type, lease_timeout_at AS timeout_at FROM promises
                   WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL
                 ORDER BY id",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        let task_timeouts: Vec<SnapshotTaskTimeout> = tt_rows
            .iter()
            .map(|r| SnapshotTaskTimeout {
                id: r.get("id"),
                timeout_type: r.get::<i32, _>("timeout_type"),
                timeout: r.get("timeout_at"),
            })
            .collect();

        // Nothing queued, so nothing to report — the messages left with the
        // transitions that emitted them. See `persistence_sqlite.rs`.
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
fn process_all_timeouts(db: &PostgresDb, time: i64) -> StorageResult<usize> {
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time, None)?;
    process_schedule_timeouts(db, time, None)
}

/// Process expired schedule timeouts.
fn process_schedule_timeouts(
    db: &PostgresDb,
    time: i64,
    only: Option<&str>,
) -> StorageResult<usize> {
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
impl ResonateEngine for PostgresEngine {
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

impl PostgresEngine {
    /// Fire one timeout the system asked of itself. See `persistence_sqlite.rs`.
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
