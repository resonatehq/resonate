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

use async_trait::async_trait;
use resonate_sql::engine::{Engine, Input, Outgoing, Output, Scheduled, Timeout};
use resonate_sql::{
    PromiseCreateParams, PromiseCreateResult, PromiseSettleParams, PromiseSettleResult,
    RegisterCallbackResult, ScheduleCreateParams, StorageError, StorageResult, TaskAcquireParams,
    TaskAcquireResult, TaskContinueResult, TaskCreateParams, TaskCreateResult,
    TaskFenceCreateParams, TaskFenceResult, TaskFenceSettleParams, TaskFulfillParams,
    TaskFulfillResult, TaskHaltResult, TaskReleaseResult, TaskSuspendResult,
};
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
use resonate_core::ui;
use resonate_core::util;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use std::sync::Arc;

pub struct PostgresEngine {
    pool: PgPool,
    /// The hot statements' text, built once. See [`PostgresDb::cached`].
    sql: SqlCache,
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
/// A statement that finds rows by id never states a partial index's predicate
/// in plain words beside the id: `task_state = 'acquired'`, `task_state =
/// 'pending'`, `task_state IS NOT NULL`, `state = 'pending' AND external`. Each
/// is the predicate of an index whose key starts with something else (a
/// deadline, the task state), and a generic plan that matches it may walk that
/// whole index with `id` as a filter instead of probing the primary key —
/// `task.fulfill` did, at 2 ms a call. `COALESCE(task_state, '')` and
/// `state || ''` say the same and match no index.
///
/// The promise columns of a CTE that unions a written row with a locked one,
/// for `P_COLS` to project from.
const RESULT_COLS: &str = "id, state, param_headers, param_data, value_headers, value_data, \
                           tags, timeout_at, created_at, settled_at";

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

/// A promise row as the wire JSON an unblock message and a preload carry —
/// `resonate._promise_json` in the schema, written out inline.
///
/// Inline because Postgres did not inline the function: every call ran as a
/// nested SQL function execution, and a preload of ten siblings paid for ten
/// of them. Field names and omissions are `PromiseRecord`'s serde, and the
/// schema function stays the one definition of that shape this mirrors.
fn promise_json(alias: &str) -> String {
    format!(
        "jsonb_strip_nulls(jsonb_build_object(\
           'id', {a}.id, 'state', {a}.state, \
           'param', jsonb_strip_nulls(jsonb_build_object(\
              'headers', NULLIF({a}.param_headers, '{{}}'::jsonb), 'data', {a}.param_data)), \
           'value', jsonb_strip_nulls(jsonb_build_object(\
              'headers', NULLIF({a}.value_headers, '{{}}'::jsonb), 'data', {a}.value_data)), \
           'tags', {a}.tags, 'timeoutAt', {a}.timeout_at, \
           'createdAt', {a}.created_at, 'settledAt', {a}.settled_at))",
        a = alias
    )
}

/// Parse and validate a request's `data`, or the 400 that says why not.
fn parse_req<T>(data: &Value, kind: &str, corr_id: &str) -> Result<T, ResponseEnvelope>
where
    T: serde::de::DeserializeOwned + Validate,
{
    let r: T = serde_json::from_value(data.clone()).map_err(|e| {
        ResponseEnvelope::error(
            kind.to_string(),
            corr_id.to_string(),
            400,
            &format!("Invalid request: {}", e),
        )
    })?;
    r.validate().map_err(|e| {
        ResponseEnvelope::error(
            kind.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )
    })?;
    Ok(r)
}

/// A task's preload, as a JSON column of the statement that answers the
/// request — the same rows `compute_preload` reads in a statement of its own,
/// without the round trip.
///
/// `branch` is the task's branch and `task` its id, both SQL expressions.
/// `changed`, when given, names a CTE holding the one row the statement
/// wrote: a statement reads its own snapshot, which has the row as it was
/// (or not at all), so the written row replaces it here. Only the promise
/// columns appear in a preload, and only the written row's changed, so the
/// overlay is the whole of what a separate read afterwards would see.
fn preload_sql(branch: &str, task: &str, changed: Option<&str>, limit: u32) -> String {
    const COLS: &str = "id, state, param_headers, param_data, value_headers, value_data, \
                        tags, timeout_at, created_at, settled_at, branch_id";
    let rows = match changed {
        None => format!("SELECT {COLS} FROM promises WHERE branch_id = {branch} AND id <> {task}"),
        Some(c) => format!(
            "SELECT {COLS} FROM promises WHERE branch_id = {branch} AND id <> {task}
               AND id NOT IN (SELECT id FROM {c})
             UNION ALL
             SELECT {COLS} FROM {c} WHERE branch_id = {branch} AND id <> {task}"
        ),
    };
    format!(
        "(SELECT COALESCE(json_agg(t.pj ORDER BY t.id), '[]'::json) FROM (
            SELECT x.id, {pj} AS pj FROM ({rows}) x ORDER BY x.id LIMIT {limit}
          ) t)",
        pj = promise_json("x"),
    )
}

/// The preload column back as records. Absent or NULL — the statement did
/// not reach the point of computing one — is an empty list.
fn preload_from(row: &PgRow) -> Vec<PromiseRecord> {
    row.try_get::<Option<Value>, _>("preload")
        .ok()
        .flatten()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default()
}

impl PostgresEngine {
    pub async fn connect(
        url: &str,
        pool_size: u32,
        task_retry_timeout: i64,
        preload_limit: u32,
        debug: bool,
    ) -> Result<Self, sqlx::Error> {
        // Room for every statement text this engine prepares. The console's
        // reads vary with their filters and sort (some two hundred texts), and
        // with sqlx's default of 100 a busy console evicted the hot statements,
        // each of which costs milliseconds to plan again.
        let options: sqlx::postgres::PgConnectOptions = url.parse()?;
        let options = options.statement_cache_capacity(1024);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(pool_size)
            // No ping before every checkout. sqlx's default spends a round
            // trip per request proving a pooled connection is alive, which on
            // this engine was a fifth of the round trips a read made. A
            // connection that died is reported by the statement that finds
            // it, as a 500 for that one request, and the pool replaces it.
            .test_before_acquire(false)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Generic plans, always. Every statement here is
                    // prepared once per connection and selects through keys
                    // whose plan does not depend on the values bound, so a
                    // custom plan buys nothing — but Postgres re-plans the
                    // first five executions of every statement and keeps
                    // re-planning any whose generic estimate looks worse,
                    // and planning one of these CTEs costs a millisecond
                    // where executing it costs a tenth of that. Every plan
                    // this forces was read with EXPLAIN against a table of
                    // millions of rows; none of them scans it.
                    sqlx::query("SET search_path TO resonate, public")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET plan_cache_mode TO force_generic_plan")
                        .execute(&mut *conn)
                        .await?;
                    // Durable commits, whatever the cluster's default. This is
                    // not tuning: a transition commits together with the
                    // messages it returns, and a message goes out as soon as
                    // the commit returns. With `off` a commit can return
                    // before it is on disk, so a crash can lose a task whose
                    // execute message a worker has already received.
                    sqlx::query("SET synchronous_commit TO on")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;
        Ok(Self {
            pool,
            sql: SqlCache::default(),
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
        let migrator = sqlx::migrate!("./migrations");
        // COUNT over a table that may not exist yet: a missing table is an
        // empty database, which is the always-create case.
        let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        resonate_sql::migrate::may_apply(applied as usize, migrator.iter().count(), migrate)
            .map_err(|e| sqlx::Error::Configuration(Box::new(e)))?;
        let migrated = migrator.run(&self.pool).await.map_err(|e| match e {
            // The initial schema was edited after this database was created.
            sqlx::migrate::MigrateError::VersionMismatch(v) => {
                sqlx::Error::Configuration(Box::new(resonate_sql::migrate::MigrateError(
                    resonate_sql::migrate::stale_schema(&format!(
                        "migration {v} was applied with a different checksum"
                    )),
                )))
            }
            other => sqlx::Error::Migrate(Box::new(other)),
        });
        if migrated.is_ok() {
            self.check_settings().await;
        }
        migrated
    }

    /// Warn about cluster settings this engine runs badly under.
    ///
    /// Nothing here is changed — they are the cluster's, shared with every
    /// database on it, and settable only by a superuser or a restart — but a
    /// deployment that leaves them at their defaults pays for it on every
    /// request, so it should hear about it once, at startup. What each costs
    /// is measured in the README's "PostgreSQL settings".
    async fn check_settings(&self) {
        let rows: Vec<(String, String)> = match sqlx::query_as(
            "SELECT name, setting FROM pg_settings WHERE name IN \
             ('max_wal_size', 'wal_compression', 'checkpoint_timeout', 'shared_buffers')",
        )
        .fetch_all(&self.pool)
        .await
        {
            Ok(rows) => rows,
            Err(_) => return,
        };
        let get = |name: &str| {
            rows.iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
        };
        let num = |name: &str| get(name).and_then(|v| v.parse::<i64>().ok());
        // pg_settings units: max_wal_size MB, checkpoint_timeout s,
        // shared_buffers 8 kB pages.
        if num("max_wal_size").is_some_and(|mb| mb < 4096) {
            tracing::warn!(
                max_wal_size_mb = num("max_wal_size"),
                "max_wal_size is under 4GB: checkpoints come often, and every page a \
                 transition touches first after one is written to WAL whole"
            );
        }
        if num("checkpoint_timeout").is_some_and(|s| s < 900) {
            tracing::warn!(
                checkpoint_timeout_s = num("checkpoint_timeout"),
                "checkpoint_timeout is under 15min: see max_wal_size"
            );
        }
        if get("wal_compression") == Some("off") {
            tracing::warn!(
                "wal_compression is off: full-page images dominate this engine's WAL; \
                 lz4 cut it from ~10KB to ~1KB per request"
            );
        }
        if num("shared_buffers").is_some_and(|pages| pages < 131_072) {
            tracing::warn!(
                shared_buffers_mb = num("shared_buffers").map(|p| p / 128),
                "shared_buffers is under 1GB: the promises table and its indexes \
                 should stay in memory"
            );
        }
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
        F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, T> + Send,
        T: Send + 'static,
    {
        self.execute(false, f).await
    }

    /// [`Self::transact`], or — `autocommit` — the same closure on a bare
    /// connection with no transaction around it.
    ///
    /// Autocommit is for a closure that issues exactly one statement that
    /// writes, or none: that statement is its own transaction, so it is
    /// atomic, and what it emitted is what it committed. A serialization
    /// failure or deadlock aborted that one statement and nothing else, so
    /// the retry below is as safe here as for a transaction.
    async fn execute<F, T>(
        &self,
        autocommit: bool,
        f: F,
    ) -> StorageResult<(T, Vec<Outgoing>, Vec<Scheduled>)>
    where
        F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, T> + Send,
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
            let tx = if autocommit {
                Conn::Auto(self.pool.acquire().await.map_err(StorageError::from)?)
            } else {
                Conn::Tx(self.pool.begin().await.map_err(StorageError::from)?)
            };

            #[cfg(feature = "concurrency-stress")]
            {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos();
                tokio::time::sleep(std::time::Duration::from_micros((nanos % 1000) as u64 + 1))
                    .await;
            }

            let db = PostgresDb {
                conn: tokio::sync::Mutex::new(tx),
                sql: self.sql.clone(),
                task_retry_timeout: self.task_retry_timeout,
                preload_limit: self.preload_limit,
                emitted: std::sync::Mutex::new(Vec::new()),
                armed: std::sync::Mutex::new(Vec::new()),
            };
            let result = f(&db).await;
            let emitted = db.emitted.into_inner().expect("emitted");
            let armed = db.armed.into_inner().expect("armed");
            let tx = db.conn.into_inner();

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

            let tx = match tx {
                Conn::Tx(tx) => tx,
                // Each statement committed as it ran.
                Conn::Auto(_) => return Ok((result, emitted, armed)),
            };
            match tx.commit().await {
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
        F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, ResponseEnvelope> + Send,
    {
        match self.transact(f).await {
            Ok((response, messages, timeouts)) => Output {
                response: Some(response),
                messages,
                timeouts,
            },
            Err(e) => self.fail(req, e),
        }
    }

    /// An operation that is one statement on every path, run in autocommit.
    async fn run_auto<F>(&self, req: &RequestEnvelope, f: F) -> Output
    where
        F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, ResponseEnvelope> + Send,
    {
        match self.execute(true, f).await {
            Ok((response, messages, timeouts)) => Output {
                response: Some(response),
                messages,
                timeouts,
            },
            Err(e) => self.fail(req, e),
        }
    }

    /// One operation with a fast path: `fast` first, as one autocommit
    /// statement, and `slow` — the full transaction — only if `fast` declines.
    ///
    /// `fast` answers `None` when the statement it ran found a promise it
    /// names pending past its deadline. Such a promise must be timed out
    /// before the operation sees it, which takes the expiry cascade and then
    /// the operation, more than one statement; so every fast statement is
    /// guarded to write nothing when it finds one, and declining is safe —
    /// nothing was committed, nothing was emitted, and `slow` starts from
    /// exactly the state `fast` saw. The common case, nothing expired, is one
    /// round trip instead of five or six.
    async fn run_fast<FF, F>(&self, req: &RequestEnvelope, fast: FF, slow: F) -> Output
    where
        FF: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, Option<ResponseEnvelope>> + Send,
        F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, ResponseEnvelope> + Send,
    {
        match self.execute(true, fast).await {
            Ok((Some(response), messages, timeouts)) => Output {
                response: Some(response),
                messages,
                timeouts,
            },
            Ok((None, messages, _)) => {
                debug_assert!(messages.is_empty(), "a declined fast path emitted");
                self.run(req, slow).await
            }
            Err(e) => self.fail(req, e),
        }
    }

    fn fail(&self, req: &RequestEnvelope, e: StorageError) -> Output {
        match e {
            StorageError::InvalidInput(msg) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Invalid request: {}", msg),
            )),
            StorageError::Serialization => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                503,
                "Serialization failure, please retry",
            )),
            e => Output::response(ResponseEnvelope::error(
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
        F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, T> + Send,
        T: Send + 'static,
    {
        // Autocommit: under READ COMMITTED a transaction gives a run of reads
        // no common snapshot, so it would buy nothing but two round trips.
        self.execute(true, f).await.map(|(v, _, _)| v)
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

            // === Console operations (read-only) ===
            "ui.executions.search" => self.op_ui_executions_search(req, now).await,
            "ui.execution.get" => self.op_ui_execution_get(req, now).await,
            "ui.schedules.search" => self.op_ui_schedules_search(req, now).await,

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
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(async move {
                    let r: PromiseGetData = match parse_req(&data, &kind_str, &corr_id) {
                        Ok(r) => r,
                        Err(e) => return Ok(Some(e)),
                    };
                    Ok(match db.promise_get(&r.id).await? {
                        None => Some(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Promise not found",
                        )),
                        Some(p) if p.state == PromiseState::Pending && p.timeout_at <= now => None,
                        Some(promise) => Some(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &PromiseResponseData { promise },
                        )),
                    })
                })
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                db.try_timeout(&[&r.id], now).await?;
                match db.promise_get(&r.id).await? {
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
        })
        .await
    }

    async fn op_promise_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(async move {
                    promise_create_body(db, &data, &kind_str, &corr_id, now, true).await
                })
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                promise_create_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_promise_settle(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(
                    async move { settle_body(db, &data, &kind_str, &corr_id, now, true).await },
                )
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                settle_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_promise_register_callback(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(
                    async move { callback_body(db, &data, &kind_str, &corr_id, now, true).await },
                )
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                callback_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_promise_register_listener(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(
                    async move { listener_body(db, &data, &kind_str, &corr_id, now, true).await },
                )
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                listener_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_promise_search(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        // One read: no transaction around it.
        self.run_auto(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                let results = db
                    .promise_search(
                        state_str,
                        tags_json.as_deref(),
                        r.cursor.as_deref(),
                        limit + 1,
                        now,
                    )
                    .await?;
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
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(async move {
                    let r: TaskGetData = match parse_req(&data, &kind_str, &corr_id) {
                        Ok(r) => r,
                        Err(e) => return Ok(Some(e)),
                    };
                    Ok(match db.task_get_probe(&r.id, now).await? {
                        Some((_, true)) => None,
                        Some((Some(task), false)) => Some(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskResponseData { task },
                        )),
                        None | Some((None, false)) => Some(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        )),
                    })
                })
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                db.try_timeout(&[&r.id], now).await?;
                match db.task_get(&r.id).await? {
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
        })
        .await
    }

    async fn op_task_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(
                    async move { create_body(db, &data, &kind_str, &corr_id, now, true).await },
                )
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                create_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_task_acquire(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(async move {
                    let r: TaskAcquireData = match parse_req(&data, &kind_str, &corr_id) {
                        Ok(r) => r,
                        Err(e) => return Ok(Some(e)),
                    };
                    let (result, expired, preload) = db
                        .task_acquire_guarded(
                            &TaskAcquireParams {
                                task_id: &r.id,
                                version: r.version,
                                time: now,
                                ttl: r.ttl,
                                pid: &r.pid,
                            },
                            true,
                        )
                        .await?;
                    if expired {
                        return Ok(None);
                    }
                    let Some(promise) = result.promise else {
                        return Ok(Some(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        )));
                    };
                    if !result.was_acquired {
                        let msg = if result.task_state != Some(TaskState::Pending) {
                            "Task is not pending"
                        } else {
                            "Version mismatch"
                        };
                        return Ok(Some(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            msg,
                        )));
                    }
                    tracing::debug!(task_id = %r.id, version = r.version + 1, "Task acquired");
                    Ok(Some(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskAcquireResponseData {
                            task: TaskRecord {
                                id: r.id.to_string(),
                                state: TaskState::Acquired,
                                version: r.version + 1,
                                resumes: 0,
                                ttl: Some(r.ttl),
                                pid: Some(r.pid.to_string()),
                            },
                            promise,
                            preload,
                        },
                    )))
                })
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                db.try_timeout(&[&r.id], now).await?;
                let result = db
                    .task_acquire(&TaskAcquireParams {
                        task_id: &r.id,
                        version: r.version,
                        time: now,
                        ttl: r.ttl,
                        pid: &r.pid,
                    })
                    .await?;
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
                        let preload = db.compute_preload(&r.id).await?;
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
        })
        .await
    }

    async fn op_task_release(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| { let data = data.clone(); let kind_str = kind_str.clone(); let corr_id = corr_id.clone(); Box::pin(async move {
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
                db.try_timeout(&[&r.id], now).await?;
                let (_, task_exists) = db.lock_for_update(&r.id).await?;
                if !task_exists {
                    tracing::debug!(task_id = %r.id, "Task release: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ));
                }
                let result = db.task_release(&r.id, r.version, now, db.task_retry_timeout()).await?;
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
            }) })
            .await
    }

    async fn op_task_fulfill(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(
                    async move { fulfill_body(db, &data, &kind_str, &corr_id, now, true).await },
                )
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                fulfill_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_task_suspend(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(
                    async move { suspend_body(db, &data, &kind_str, &corr_id, now, true).await },
                )
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                suspend_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_task_fence(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        let fast = {
            let (data, kind_str, corr_id) = (data.clone(), kind_str.clone(), corr_id.clone());
            db_fn(move |db| {
                let data = data.clone();
                let kind_str = kind_str.clone();
                let corr_id = corr_id.clone();
                Box::pin(async move { fence_body(db, &data, &kind_str, &corr_id, now, true).await })
            })
        };
        self.run_fast(req, fast, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                fence_body(db, &data, &kind_str, &corr_id, now, false)
                    .await
                    .map(|r| r.expect("invariant: the slow path never declines"))
            })
        })
        .await
    }

    async fn op_task_heartbeat(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        // One statement, whatever the request holds: it is its own
        // transaction, so no BEGIN and no COMMIT around it.
        self.run_auto(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                db.task_heartbeat(&r.pid, &task_pairs, now).await?;
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
        })
        .await
    }

    async fn op_task_halt(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                db.try_timeout(&[&r.id], now).await?;
                let result = db.task_halt(&r.id).await?;
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
        })
        .await
    }

    async fn op_task_continue(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                db.try_timeout(&[&r.id], now).await?;
                let result = db.task_continue(&r.id, now).await?;
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
        })
        .await
    }

    async fn op_task_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        // One read: no transaction around it.
        self.run_auto(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                let results = db
                    .task_search(state_str, r.cursor.as_deref(), limit + 1)
                    .await?;
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
        // One read: no transaction around it.
        self.run_auto(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                match db.schedule_get(&r.id).await? {
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
        })
        .await
    }

    async fn op_schedule_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                let schedule = db
                    .schedule_create(&ScheduleCreateParams {
                        id: &r.id,
                        cron: &r.cron,
                        promise_id: &r.promise_id,
                        promise_timeout: r.promise_timeout,
                        promise_param_headers: promise_param_headers_json.as_deref(),
                        promise_param_data: r.promise_param.data.as_deref(),
                        promise_tags: &promise_tags_json,
                        created_at: now,
                        next_run_at,
                    })
                    .await?;
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
        })
        .await
    }

    async fn op_schedule_delete(&self, req: &RequestEnvelope) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                if db.schedule_delete(&r.id).await? {
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
        })
        .await
    }

    async fn op_schedule_search(&self, req: &RequestEnvelope) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        // One read: no transaction around it.
        self.run_auto(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
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
                let schedules = db
                    .schedule_search(tags_json.as_deref(), r.cursor.as_deref(), limit + 1)
                    .await?;
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
        })
        .await
    }

    // ============================================================================
    // Console operations — the read-only `ui.*` namespace
    // ============================================================================
    //
    // Additive: remove these three arms and the server still serves. What they
    // add is read *shape* — root-ness, sorting, one tree in one answer — none
    // of which the worker protocol was written to express.

    async fn op_ui_executions_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                let q = match resonate_sql::ui_resolve::<ui::ExecutionsSearchData, _>(
                    &data,
                    &kind_str,
                    &corr_id,
                    |d| d.resolve(),
                ) {
                    Ok(q) => q,
                    Err(resp) => return Ok(resp),
                };
                db.custom_plans().await?;
                let rows = db.ui_executions_search(&q).await?;
                let total = if q.count_total {
                    Some(db.ui_executions_count(&q).await?)
                } else {
                    None
                };
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &ui::finish_executions_page(&q, rows, total),
                ))
            })
        })
        .await
    }

    async fn op_ui_execution_get(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                let q = match resonate_sql::ui_resolve::<ui::ExecutionGetData, _>(
                    &data,
                    &kind_str,
                    &corr_id,
                    |d| d.resolve(),
                ) {
                    Ok(q) => q,
                    Err(resp) => return Ok(resp),
                };
                db.custom_plans().await?;
                let rows = db.ui_execution_nodes(&q).await?;
                match ui::build_execution(&q, rows) {
                    Ok(view) => Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &view,
                    )),
                    Err(e) => Ok(e.to_response(kind_str.clone(), corr_id.clone())),
                }
            })
        })
        .await
    }

    async fn op_ui_schedules_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let data = req.data.clone();
        let kind_str = req.kind.clone();
        let corr_id = req.head.corr_id.clone();
        self.run(req, move |db| {
            let data = data.clone();
            let kind_str = kind_str.clone();
            let corr_id = corr_id.clone();
            Box::pin(async move {
                let q = match resonate_sql::ui_resolve::<ui::SchedulesSearchData, _>(
                    &data,
                    &kind_str,
                    &corr_id,
                    |d| d.resolve(),
                ) {
                    Ok(q) => q,
                    Err(resp) => return Ok(resp),
                };
                db.custom_plans().await?;
                let rows = db.ui_schedules_search(&q).await?;
                let total = if q.count_total {
                    Some(db.ui_schedules_count(&q).await?)
                } else {
                    None
                };
                Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &ui::finish_schedules_page(&q, rows, total),
                ))
            })
        })
        .await
    }

    // ============================================================================
    // Debug operations
    // ============================================================================

    async fn op_debug_reset(&self, req: &RequestEnvelope) -> Output {
        Output::response(
            match self
                .transact(move |db| Box::pin(async move { db.debug_reset().await }))
                .await
            {
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
            },
        )
    }

    async fn op_debug_snap(&self, req: &RequestEnvelope) -> Output {
        Output::response(
            match self
                .query(move |db| Box::pin(async move { db.snap().await }))
                .await
            {
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
            },
        )
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
            .transact(move |db| {
                Box::pin(async move { process_all_timeouts(db, time).await.map(|_| ()) })
            })
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

/// Where a `PostgresDb`'s statements go.
///
/// A transaction for an operation of several statements; a bare pooled
/// connection, in autocommit, for an operation that is one statement — which
/// is atomic on its own, and so needs neither the `BEGIN` nor the `COMMIT`
/// round trip.
enum Conn<'a> {
    Tx(sqlx::Transaction<'a, sqlx::Postgres>),
    Auto(sqlx::pool::PoolConnection<sqlx::Postgres>),
}

impl Conn<'_> {
    fn pg(&mut self) -> &mut sqlx::PgConnection {
        match self {
            Conn::Tx(tx) => tx,
            Conn::Auto(conn) => conn,
        }
    }
}

/// Statement text by call site, shared by every operation of one engine.
type SqlCache = Arc<std::sync::Mutex<std::collections::HashMap<&'static str, Arc<str>>>>;

/// What an operation's closure returns: the operation, as a future borrowing
/// the `PostgresDb` it runs on.
type DbFut<'d, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<T>> + Send + 'd>>;

/// Fix a closure's signature to the one `execute` takes. A closure stored in a
/// `let` before it is passed cannot have its higher-ranked signature inferred
/// from the call; passing it through here gives it the bound up front.
fn db_fn<F, T>(f: F) -> F
where
    F: for<'d> FnMut(&'d PostgresDb<'static>) -> DbFut<'d, T>,
{
    f
}

/// One operation's connection and what it has emitted so far.
///
/// Every method is async and runs on the caller's task: no thread is parked
/// waiting for a statement, and no runtime worker is handed off to keep the
/// scheduler moving while one is. The locks are never contended — one task
/// owns a `PostgresDb` for its whole life — and exist to make it `Sync`, so
/// a future holding `&PostgresDb` across an await is `Send`.
struct PostgresDb<'a> {
    conn: tokio::sync::Mutex<Conn<'a>>,
    sql: SqlCache,
    task_retry_timeout: i64,
    preload_limit: u32,
    /// What this transition has emitted so far. See `engine_sqlite.rs`
    /// — same reasoning, and the same reason it is not in a return type.
    emitted: std::sync::Mutex<Vec<Outgoing>>,
    /// What deadlines this transition armed or moved. A hint; see
    /// `engine_sqlite.rs`.
    armed: std::sync::Mutex<Vec<Scheduled>>,
}

impl<'a> PostgresDb<'a> {
    /// A statement's text, built by `build` the first time `key` is asked for
    /// and shared after.
    ///
    /// The hot statements are several kilobytes assembled from fragments, and
    /// depend on nothing but the engine's configuration — so the assembling,
    /// a dozen passes of `format!` and `replace`, was the same work on every
    /// request. `key` names the call site; one site, one text.
    fn cached(&self, key: &'static str, build: impl FnOnce() -> String) -> Arc<str> {
        if let Some(sql) = self.sql.lock().expect("sql cache").get(key) {
            return sql.clone();
        }
        let sql: Arc<str> = build().into();
        self.sql.lock().expect("sql cache").insert(key, sql.clone());
        sql
    }

    /// The connection, for one statement.
    async fn tx(&self) -> tokio::sync::MutexGuard<'_, Conn<'a>> {
        self.conn.lock().await
    }

    /// Report a deadline this transition just wrote.
    fn arm(&self, at: i64, timeout: Timeout) {
        self.armed
            .lock()
            .expect("armed")
            .push(Scheduled { at, timeout });
    }

    /// Announce a promise deadline the queue holds.
    ///
    /// The queue is `state = 'pending' AND external`: every pending promise
    /// that is not internal — one a listener or an awaiter can wait on, or
    /// whose own task is redispatched — is swept eagerly. An internal promise
    /// arms nothing: it times out lazily, the first time a request names it.
    /// Callers arm exactly when they created a pending, external promise.
    fn arm_promise_timeout(&self, promise_id: &str, timeout_at: i64) {
        self.arm(
            timeout_at,
            Timeout::PromiseTimeout {
                promise_id: promise_id.to_string(),
            },
        );
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
        let before = self.emitted.lock().expect("emitted").len();
        self.absorb(row);
        let armed: Vec<String> = self.emitted.lock().expect("emitted")[before..]
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
        let mut out = self.emitted.lock().expect("emitted");
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

/// A keyset cursor as two bindable halves. `None` for both when there is no
/// cursor, which is what the `IS NULL OR` in every keyset predicate reads.
fn split_keyset(after: Option<&ui::Keyset>) -> (Option<i64>, Option<String>) {
    match after {
        Some(k) => (Some(k.key), Some(k.id.clone())),
        None => (None, None),
    }
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
/// `suspended_awaiters` is read before the `UPDATE` rather than from its
/// `RETURNING`, because `RETURNING` yields post-update values and the emission
/// needs to know *which* awaiters were suspended. It reads them locked, so at
/// their latest version: a concurrent suspend that committed while this
/// statement waited is seen, as the multi-table backend saw it through
/// `resumed_tasks RETURNING` under EPQ.
const SETTLE_FANOUT: &str = "
-- The awaiters, locked, so each is read at its latest version: one that
-- suspended while this statement waited for its own lock is seen as
-- suspended, and woken with a message. Read from the snapshot instead it
-- would be woken in `fanout` (which sees the latest version) but sent
-- nothing, and wait out its retry deadline. No state filter here: a filter
-- is applied to the snapshot version before the lock, and would drop exactly
-- that row; `emit_resume` filters the locked versions instead.
awaiter_rows AS (
  SELECT id, task_version, target, task_state FROM promises
  WHERE id = ANY(:AWAITERS)
  FOR UPDATE
),
suspended_awaiters AS (
  SELECT id, task_version, target FROM awaiter_rows WHERE task_state = 'suspended'
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
          OR (:FULFILLED AND q.callbacks <> '{}' AND q.callbacks @> ARRAY[:AWAITED]) )
    -- Read the awaiters (and take their locks) before writing them: a locked
    -- read after this UPDATE would find its own rewrite and yield nothing.
    AND (SELECT count(*) FROM awaiter_rows) >= 0
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
-- The sets a row is looked up *by* are arrays computed once, so each
-- `= ANY(...)` and `&&` is an InitPlan parameter (the `::text[]` is what makes
-- `ANY((SELECT ..))` the array form rather than the subquery form) that the
-- planner pushes into an index: the fan-out reaches the rows it names through
-- the primary key and the GIN index on `callbacks`, and never scans the
-- table. The sets a row is tested *against* — `NOT IN` below — stay subqueries,
-- which Postgres hashes: `x <> ALL(array)` compares against every element, and
-- across a large batch that was quadratic (4.2 s for 20k rows against a 40k
-- array, against 30 ms hashed). When nothing expired, every set is empty and
-- the statement touches no row at all.
expired_ids AS (
  SELECT COALESCE(array_agg(id), '{}') AS ids FROM expired
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
ready_ids AS (
  SELECT COALESCE(array_agg(awaiter), '{}') AS ids FROM ready_agg
),
-- The awaiters to wake, locked so they are read at their latest version — see
-- `SETTLE_FANOUT`. An awaiter that is itself expiring is settled, not woken.
awaiter_rows AS (
  SELECT p.id, p.task_version, p.target, p.task_state FROM promises p
  WHERE p.id = ANY((SELECT ids FROM ready_ids)::text[])
    AND p.id NOT IN (SELECT id FROM expired)
  FOR UPDATE
),
suspended_awaiters AS (
  SELECT id, task_version, target FROM awaiter_rows WHERE task_state = 'suspended'
),
updated_expired AS (
  UPDATE promises p SET
    state = CASE WHEN p.is_timer THEN 'resolved' ELSE 'rejected_timedout' END,
    settled_at = p.timeout_at,
    :SELF_SET
  WHERE p.id = ANY((SELECT ids FROM expired_ids)::text[])
  RETURNING p.*
),
emit_unblock AS (
  SELECT 'unblock'::text AS kind, l AS address, NULL::text AS task_id,
         NULL::int AS version, :PROMISE_JSON AS promise
  FROM updated_expired u
  JOIN expired_snap e ON e.id = u.id
  CROSS JOIN LATERAL unnest(e.listeners) AS l
),
-- The rows the fan-out writes, each with what it is owed, joined once. A
-- correlated lookup per row (`SELECT .. FROM ready_agg WHERE awaiter = q.id`
-- in the SET list) is a scan of the whole aggregate for every row written:
-- 30 s for a batch of 20k.
fanout_targets AS (
  SELECT q.id, r.awaited_ids
  FROM promises q
  LEFT JOIN ready_agg r ON r.awaiter = q.id
  WHERE ( q.id = ANY((SELECT ids FROM ready_ids)::text[])
          OR (q.callbacks <> '{}' AND q.callbacks && (SELECT ids FROM fulfilled_ids)) )
    AND q.id NOT IN (SELECT id FROM expired)
),
fanout AS (
  UPDATE promises q SET
    callbacks = CASE WHEN q.callbacks && (SELECT ids FROM fulfilled_ids)
                  THEN (SELECT COALESCE(array_agg(b), '{}') FROM unnest(q.callbacks) b
                        WHERE b NOT IN (SELECT id FROM fulfilled))
                  ELSE q.callbacks END,
    resumes = q.resumes || COALESCE(t.awaited_ids, '{}'),
    task_state = CASE WHEN q.task_state = 'suspended' AND t.awaited_ids IS NOT NULL
                   THEN 'pending' ELSE q.task_state END,
    retry_timeout_at = CASE WHEN q.task_state = 'suspended' AND t.awaited_ids IS NOT NULL
                   THEN :TIME + :TRT ELSE q.retry_timeout_at END,
    lease_timeout_at = CASE WHEN q.task_state = 'suspended' AND t.awaited_ids IS NOT NULL
                   THEN NULL ELSE q.lease_timeout_at END,
    ttl = CASE WHEN q.task_state = 'suspended' AND t.awaited_ids IS NOT NULL
                   THEN NULL ELSE q.ttl END,
    pid = CASE WHEN q.task_state = 'suspended' AND t.awaited_ids IS NOT NULL
                   THEN NULL ELSE q.pid END
  FROM fanout_targets t
  WHERE t.id = q.id
    -- Read the awaiters (and take their locks) before writing them.
    AND (SELECT count(*) FROM awaiter_rows) >= 0
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

/// `task.create`, both ways — see `fence_body`. The fast path is the one
/// statement that inserts the promise with its task acquired; anything else
/// (the promise exists, or is created already timed out) declines to the
/// transaction.
async fn create_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: TaskCreateData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
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
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                "Invalid resonate:target address",
            )));
        }
    }
    if !fast {
        db.try_timeout(&[action_id], now).await?;
        // Lock preamble: ensures CTE and subsequent reads see
        // current state under READ COMMITTED.
        let _ = db.lock_for_update(action_id).await?;
    }
    let tags_json = serde_json::to_string(&action_data.tags).unwrap();
    let already_timedout = now >= action_data.timeout_at;
    // The fast path creates, or declines: a promise created already
    // settled owes its callbacks a separate statement, and one that
    // exists already may need timing out first.
    if fast && already_timedout {
        return Ok(None);
    }
    let (p_state, created_at, settled_at) = if already_timedout {
        let p_state = if action_data.tags.get("resonate:timer").map(|v| v.as_str()) == Some("true")
        {
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
    let (res, created_preload) = db
        .task_create(&TaskCreateParams {
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
        })
        .await?;
    if fast && !res.task_created {
        return Ok(None);
    }

    // If the promise is settled, process callbacks as a separate
    // statement. This fires any callbacks registered by concurrent
    // transactions (e.g. task.suspend) that committed after
    // try_timeout's snapshot but before now.
    if res.promise.state != PromiseState::Pending {
        db.process_callbacks(action_id, now).await?;
    }

    // When the CTE created the task, use CTE result directly.
    if res.task_created {
        let task_state_str = res
            .task_state
            .expect("invariant: task_state is Some when task_created");
        let task_state = task_state_str
            .parse::<TaskState>()
            .expect("invariant: task_state is a valid TaskState");
        assert!(
            res.promise.state != PromiseState::Pending || task_state != TaskState::Fulfilled,
            "invariant: pending promise with fulfilled task"
        );
        assert!(
            res.promise.state == PromiseState::Pending || task_state == TaskState::Fulfilled,
            "invariant: settled promise with non-fulfilled task"
        );
        // Acquired tasks start at version 1 (first claim), fulfilled at 0
        let task_version = if task_state == TaskState::Acquired {
            1
        } else {
            0
        };
        let task = TaskRecord {
            id: action_id.to_string(),
            state: task_state,
            version: task_version,
            resumes: 0,
            ttl: if task_state == TaskState::Fulfilled {
                None
            } else {
                Some(r.ttl)
            },
            pid: if task_state == TaskState::Fulfilled {
                None
            } else {
                Some(r.pid.to_string())
            },
        };
        // Every branch computes it: preload is branch-scoped, not
        // lifecycle-scoped, so a fulfilled task's siblings are as
        // real as an acquired one's.
        let preload = created_preload;
        return Ok(Some(ResponseEnvelope::success(
            kind_str.to_string(),
            corr_id.to_string(),
            &TaskCreateResponseData {
                task,
                promise: res.promise,
                preload,
            },
        )));
    }

    // CTE didn't create the task (promise already existed).
    // Branch on the state/version surfaced by the CTE.
    match (res.task_state.as_deref(), res.task_version) {
        (Some("fulfilled"), version) => {
            assert_ne!(
                res.promise.state,
                PromiseState::Pending,
                "invariant: pending promise with fulfilled task"
            );
            Ok(Some(ResponseEnvelope::success(
                kind_str.to_string(),
                corr_id.to_string(),
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
                    preload: db.compute_preload(action_id).await?,
                },
            )))
        }
        (Some("pending"), Some(version)) => {
            let acquire_result = db
                .task_acquire(&TaskAcquireParams {
                    task_id: action_id,
                    version,
                    time: now,
                    ttl: r.ttl,
                    pid: &r.pid,
                })
                .await?;
            if acquire_result.was_acquired {
                let task = TaskRecord {
                    id: action_id.to_string(),
                    state: TaskState::Acquired,
                    version: version + 1,
                    resumes: 0,
                    ttl: Some(r.ttl),
                    pid: Some(r.pid.to_string()),
                };
                assert_eq!(
                    res.promise.state,
                    PromiseState::Pending,
                    "invariant: settled promise with non-fulfilled task"
                );
                assert_eq!(
                    acquire_result.task_version,
                    Some(version + 1),
                    "invariant: acquired task version must be version + 1"
                );
                let preload = db.compute_preload(action_id).await?;
                Ok(Some(ResponseEnvelope::success(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    &TaskCreateResponseData {
                        task,
                        promise: res.promise,
                        preload,
                    },
                )))
            } else if acquire_result.task_state == Some(TaskState::Fulfilled) {
                let promise = acquire_result
                    .promise
                    .expect("fulfilled task must have a promise");
                assert_ne!(
                    promise.state,
                    PromiseState::Pending,
                    "invariant: fulfilled task cannot have a pending promise"
                );
                Ok(Some(ResponseEnvelope::success(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    &TaskCreateResponseData {
                        task: TaskRecord {
                            id: action_id.to_string(),
                            state: TaskState::Fulfilled,
                            version: acquire_result
                                .task_version
                                .expect("invariant: fulfilled task must have a version"),
                            resumes: 0,
                            ttl: None,
                            pid: None,
                        },
                        promise,
                        preload: db.compute_preload(action_id).await?,
                    },
                )))
            } else {
                assert!(
                    acquire_result.task_state.is_some(),
                    "invariant: non-acquired result must have a task state"
                );
                assert!(
                    acquire_result.task_version.is_some(),
                    "invariant: non-acquired result must have a task version"
                );
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
                Ok(Some(ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    409,
                    "Already exists",
                )))
            }
        }
        (None, _) => Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            422,
            "The promise does not have a resonate:target tag",
        ))),
        _ => Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            409,
            "Already exists",
        ))),
    }
}

/// `task.suspend`, both ways — see `fence_body`.
async fn suspend_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: TaskSuspendData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    let awaited_ids: Vec<String> = r.actions.iter().map(|a| a.data.awaited.clone()).collect();
    let mut timeout_ids: Vec<&str> = vec![&r.id];
    for aid in &awaited_ids {
        timeout_ids.push(aid.as_str());
    }
    // The slow path: lock the task row BEFORE try_timeout to prevent
    // try_timeout from fulfilling it via promise timeout. The fast
    // path runs no try_timeout; its statement declines instead.
    let mut task_exists = false;
    if !fast {
        task_exists = db.lock_for_update(&r.id).await?.1;
        db.try_timeout(&timeout_ids, now).await?;
    }
    // Duplicates are refused by validation, so the list is already
    // unique — no deduplication on the way to storage.
    let awaited: Vec<&str> = awaited_ids.iter().map(|s| s.as_str()).collect();
    let (result, exists, expired, preload) = db
        .task_suspend_guarded(&r.id, r.version, &awaited, fast.then_some(now))
        .await?;
    if expired {
        return Ok(None);
    }
    if fast {
        task_exists = exists;
    }
    if !result.task_matched {
        // Use lock_for_update result — no separate task_get that
        // could see a concurrent task creation.
        if !task_exists {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                404,
                "Task not found",
            )));
        }
        tracing::debug!(
            task_id = %r.id,
            version = r.version,
            "Task suspend rejected: not acquired or version mismatch"
        );
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            409,
            "Task is not acquired or version mismatch",
        )));
    }
    if result.missing_count > 0 {
        tracing::debug!(
            task_id = %r.id,
            missing_count = result.missing_count,
            "Task suspend rejected: awaited promise(s) not found"
        );
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            422,
            "Awaited promise not found",
        )));
    }
    if result.non_awaitable_count > 0 {
        tracing::debug!(
            task_id = %r.id,
            non_awaitable_count = result.non_awaitable_count,
            "Task suspend rejected: awaited promise(s) not awaitable"
        );
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            422,
            "Awaited promise is not awaitable",
        )));
    }
    if result.was_suspended {
        tracing::info!(
            task_id = %r.id,
            version = r.version,
            awaited_count = awaited.len(),
            "Task suspended, waiting on promises"
        );
        return Ok(Some(ResponseEnvelope::new(
            kind_str.to_string(),
            corr_id.to_string(),
            200,
            serde_json::json!({}),
        )));
    }
    // Immediate resume (settled awaited promises)
    tracing::info!(
        task_id = %r.id,
        version = r.version,
        "Task suspend: immediate resume, awaited promises already settled"
    );
    Ok(Some(ResponseEnvelope::new(
        kind_str.to_string(),
        corr_id.to_string(),
        300,
        serde_json::to_value(&TaskSuspendPreloadData { preload }).unwrap(),
    )))
}

/// `promise.settle`, both ways — see `fence_body`.
async fn settle_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: PromiseSettleData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    if !fast {
        db.try_timeout(&[&r.id], now).await?;
    }
    let value_headers_json = r
        .value
        .headers
        .as_ref()
        .map(|h| serde_json::to_string(h).unwrap());
    let (result, expired) = db
        .promise_settle(
            &PromiseSettleParams {
                id: &r.id,
                state: r.state.as_str(),
                value_headers: value_headers_json.as_deref(),
                value_data: r.value.data.as_deref(),
                settled_at: now,
            },
            fast.then_some(now),
        )
        .await?;
    if expired {
        return Ok(None);
    }
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
            Ok(Some(ResponseEnvelope::success(
                kind_str.to_string(),
                corr_id.to_string(),
                &PromiseResponseData { promise },
            )))
        }
        None => {
            tracing::debug!(promise_id = %r.id, "Promise settle: promise not found");
            Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                404,
                "Promise not found",
            )))
        }
    }
}

/// `promise.register_listener`, both ways — see `fence_body`.
async fn listener_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: PromiseRegisterListenerData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    if !resonate_core::is_valid_address(&r.address) {
        tracing::warn!(
            awaited = %r.awaited,
            address = %r.address,
            "Listener registration rejected: invalid address"
        );
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            "Invalid listener address",
        )));
    }
    if !fast {
        db.try_timeout(&[&r.awaited], now).await?;
    }
    let (found, expired) = db
        .promise_register_listener(&r.awaited, &r.address, fast.then_some(now))
        .await?;
    if expired {
        return Ok(None);
    }
    match found {
        Some(promise) => {
            if !resonate_core::types::is_external(&promise.tags) {
                tracing::debug!(awaited = %r.awaited, "Listener registration rejected: awaited is not awaitable");
                return Ok(Some(ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    422,
                    "Awaited promise is not awaitable",
                )));
            }
            tracing::info!(
                awaited = %r.awaited,
                address = %r.address,
                promise_state = %promise.state,
                "Listener registered"
            );
            Ok(Some(ResponseEnvelope::success(
                kind_str.to_string(),
                corr_id.to_string(),
                &PromiseResponseData { promise },
            )))
        }
        None => {
            tracing::debug!(awaited = %r.awaited, "Listener registration: awaited promise not found");
            Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                404,
                "Awaited promise not found",
            )))
        }
    }
}

/// `promise.register_callback`, both ways — see `fence_body`.
async fn callback_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: PromiseRegisterCallbackData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    if !fast {
        db.try_timeout(&[&r.awaited, &r.awaiter], now).await?;
    }
    let (result, expired) = db
        .promise_register_callback(&r.awaited, &r.awaiter, now, fast.then_some(now))
        .await?;
    if expired {
        return Ok(None);
    }
    let p_awaited = match result.awaited {
        Some(p) => p,
        None => {
            tracing::debug!(promise_id = %r.awaited, "Callback registration: awaited promise not found");
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                404,
                "Awaited promise not found",
            )));
        }
    };
    let p_awaiter = match result.awaiter {
        Some(p) => p,
        None => {
            tracing::debug!(promise_id = %r.awaiter, "Callback registration: awaiter promise not found");
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                422,
                "Awaiter promise not found",
            )));
        }
    };
    if !p_awaiter.tags.contains_key("resonate:target") {
        tracing::debug!(awaiter = %r.awaiter, "Callback registration rejected: awaiter has no resonate:target");
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            422,
            "Awaiter promise has no resonate:target tag",
        )));
    }
    if !resonate_core::types::is_external(&p_awaited.tags) {
        tracing::debug!(awaited = %r.awaited, "Callback registration rejected: awaited is not awaitable");
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            422,
            "Awaited promise is not awaitable",
        )));
    }
    tracing::info!(
        awaited = %r.awaited,
        awaiter = %r.awaiter,
        awaited_state = %p_awaited.state,
        "Callback registered"
    );
    Ok(Some(ResponseEnvelope::success(
        kind_str.to_string(),
        corr_id.to_string(),
        &PromiseResponseData { promise: p_awaited },
    )))
}

/// `promise.create`, both ways — see `fence_body`. The insert is the one
/// statement either way; only an existing promise past its deadline declines.
async fn promise_create_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: PromiseCreateData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    let address = r.tags.get("resonate:target").map(|s| s.as_str());
    if let Some(addr) = address {
        if !resonate_core::is_valid_address(addr) {
            tracing::warn!(
                promise_id = %r.id,
                address = addr,
                "Promise create rejected: invalid resonate:target address"
            );
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                "Invalid resonate:target address",
            )));
        }
    }
    if !fast {
        db.try_timeout(&[&r.id], now).await?;
    }
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
    let result = db
        .promise_create(&PromiseCreateParams {
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
        })
        .await?;
    // The fast path's insert wrote nothing if the promise exists;
    // one that exists pending past its deadline must be timed out
    // before it is answered, which is the transaction's job.
    if fast
        && !result.was_created
        && result.promise.state == PromiseState::Pending
        && result.promise.timeout_at <= now
    {
        return Ok(None);
    }
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
    Ok(Some(ResponseEnvelope::success(
        kind_str.to_string(),
        corr_id.to_string(),
        &PromiseResponseData {
            promise: result.promise,
        },
    )))
}

/// `task.fulfill`, both ways — see `fence_body`. The fast path is one
/// autocommit statement that locks, checks the fence and settles; it declines
/// if the promise is pending past its deadline.
async fn fulfill_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: TaskFulfillData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    let action_data = &r.action.data;
    if !fast {
        db.try_timeout(&[&action_data.id], now).await?;
        // Lock preamble: lock promise + task to prevent stale snapshot
        // in fulfillment CTE.
        let (_, task_exists) = db.lock_for_update(&r.id).await?;
        if !task_exists {
            tracing::debug!(task_id = %r.id, "Task fulfill: task not found");
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                404,
                "Task not found",
            )));
        }
    }
    let value_headers_json = action_data
        .value
        .headers
        .as_ref()
        .map(|h| serde_json::to_string(h).unwrap());
    let (result, expired) = db
        .task_fulfill_guarded(
            &TaskFulfillParams {
                task_id: &r.id,
                version: r.version,
                promise_id: &r.id,
                state: action_data.state.as_str(),
                value_headers: value_headers_json.as_deref(),
                value_data: action_data.value.data.as_deref(),
                settled_at: now,
            },
            fast.then_some(now),
        )
        .await?;
    if expired {
        return Ok(None);
    }
    if !result.task_exists {
        tracing::debug!(task_id = %r.id, "Task fulfill: task not found");
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            404,
            "Task not found",
        )));
    }
    if !result.task_fulfilled {
        tracing::debug!(task_id = %r.id, version = r.version, "Task fulfill rejected: version mismatch or invalid state");
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            409,
            "Task version mismatch or invalid state",
        )));
    }
    let promise = result
        .promise
        .expect("invariant: task exists implies promise exists");
    assert!(
        result.task_fulfilled,
        "invariant: returning 200 but task is not fulfilled"
    );
    assert_ne!(
        promise.state,
        PromiseState::Pending,
        "invariant: returning 200 but promise is still pending"
    );
    tracing::info!(
        task_id = %r.id,
        version = r.version,
        promise_state = %promise.state,
        "Task fulfilled and promise settled"
    );
    Ok(Some(ResponseEnvelope::success(
        kind_str.to_string(),
        corr_id.to_string(),
        &TaskFulfillResponseData { promise },
    )))
}

/// A 400 the slow path gives only after it has run the expiry cascade: the
/// fast path, which has not, declines rather than answer without it.
fn unless_fast(fast: bool, response: ResponseEnvelope) -> Option<ResponseEnvelope> {
    (!fast).then_some(response)
}

/// `task.fence`, both ways: `fast` is one autocommit statement that declines
/// (`None`) on a pending promise past its deadline; otherwise the expiry
/// cascade and the lock preamble run first, in a transaction, and it never
/// declines. One body, so the two cannot answer differently.
async fn fence_body(
    db: &PostgresDb<'_>,
    data: &Value,
    kind_str: &str,
    corr_id: &str,
    now: i64,
    fast: bool,
) -> StorageResult<Option<ResponseEnvelope>> {
    let r: TaskFenceData = match serde_json::from_value(data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Ok(Some(ResponseEnvelope::error(
                kind_str.to_string(),
                corr_id.to_string(),
                400,
                &format!("Invalid request: {}", e),
            )))
        }
    };
    if let Err(e) = r.validate() {
        return Ok(Some(ResponseEnvelope::error(
            kind_str.to_string(),
            corr_id.to_string(),
            400,
            &format_validation_errors(&e),
        )));
    }
    let action_kind = &r.action.kind;
    let action_data = &r.action.data;
    let action_id = action_data["id"].as_str().unwrap_or("");
    if !fast {
        db.try_timeout(&[&r.id, action_id], now).await?;
        // Lock preamble: ensures fence check sees current task state.
        let _ = db.lock_for_update(&r.id).await?;
    }

    match action_kind.as_str() {
        "promise.create" => {
            let create_data: PromiseCreateData = match serde_json::from_value(action_data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(unless_fast(
                        fast,
                        ResponseEnvelope::error(
                            kind_str.to_string(),
                            corr_id.to_string(),
                            400,
                            &format!("Invalid action data: {}", e),
                        ),
                    ))
                }
            };
            if let Err(e) = create_data.validate() {
                return Ok(unless_fast(
                    fast,
                    ResponseEnvelope::error(
                        kind_str.to_string(),
                        corr_id.to_string(),
                        400,
                        &format_validation_errors(&e),
                    ),
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
                    return Ok(unless_fast(
                        fast,
                        ResponseEnvelope::error(
                            kind_str.to_string(),
                            corr_id.to_string(),
                            400,
                            "Invalid resonate:target address",
                        ),
                    ));
                }
            }
            let (p_state, created_at, settled_at) = if already_timedout {
                let p_state =
                    if create_data.tags.get("resonate:timer").map(|v| v.as_str()) == Some("true") {
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
            let (result, expired, preload) = db
                .task_fence_create_guarded(
                    &TaskFenceCreateParams {
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
                    },
                    fast.then_some(now),
                )
                .await?;
            if expired {
                return Ok(None);
            }
            if !result.task_exists {
                tracing::debug!(task_id = %r.id, fenced_action = "promise.create", "Task fence rejected: task not found");
                return Ok(Some(ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    404,
                    "Task not found",
                )));
            }
            if !result.fence_ok {
                tracing::debug!(task_id = %r.id, version = r.version, fenced_action = "promise.create", "Task fence rejected: version mismatch");
                return Ok(Some(ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    409,
                    "Version mismatch",
                )));
            }
            tracing::info!(
                task_id = %r.id,
                version = r.version,
                fenced_action = "promise.create",
                promise_id = %create_data.id,
                "Task fence: promise.create executed"
            );
            let p = result
                .promise
                .expect("invariant: promise.create result must have a promise");
            let inner_data = serde_json::json!({ "promise": p });
            let inner_envelope = serde_json::json!({
                "kind": action_kind,
                "head": { "corrId": corr_id, "status": 200, "version": "2026-04-01" },
                "data": inner_data,
            });
            Ok(Some(ResponseEnvelope::success(
                kind_str.to_string(),
                corr_id.to_string(),
                &TaskFenceResponseData {
                    action: inner_envelope,
                    preload,
                },
            )))
        }
        "promise.settle" => {
            let settle_data: PromiseSettleData = match serde_json::from_value(action_data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(unless_fast(
                        fast,
                        ResponseEnvelope::error(
                            kind_str.to_string(),
                            corr_id.to_string(),
                            400,
                            &format!("Invalid action data: {}", e),
                        ),
                    ))
                }
            };
            if let Err(e) = settle_data.validate() {
                return Ok(unless_fast(
                    fast,
                    ResponseEnvelope::error(
                        kind_str.to_string(),
                        corr_id.to_string(),
                        400,
                        &format_validation_errors(&e),
                    ),
                ));
            }
            let value_headers_json = settle_data
                .value
                .headers
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap());
            let (result, expired, preload) = db
                .task_fence_settle_guarded(
                    &TaskFenceSettleParams {
                        task_id: &r.id,
                        version: r.version,
                        promise_id: &settle_data.id,
                        state: settle_data.state.as_str(),
                        value_headers: value_headers_json.as_deref(),
                        value_data: settle_data.value.data.as_deref(),
                        settled_at: now,
                    },
                    fast.then_some(now),
                )
                .await?;
            if expired {
                return Ok(None);
            }
            if !result.task_exists {
                tracing::debug!(task_id = %r.id, fenced_action = "promise.settle", "Task fence rejected: task not found");
                return Ok(Some(ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    404,
                    "Task not found",
                )));
            }
            if !result.fence_ok {
                tracing::debug!(task_id = %r.id, version = r.version, fenced_action = "promise.settle", "Task fence rejected: version mismatch");
                return Ok(Some(ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    409,
                    "Version mismatch",
                )));
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
                    assert_ne!(
                        p.state,
                        PromiseState::Pending,
                        "invariant: returning 200 but promise is still pending"
                    );
                    serde_json::json!({ "promise": p })
                }
                None => serde_json::json!("Promise not found"),
            };
            let inner_envelope = serde_json::json!({
                "kind": action_kind,
                "head": { "corrId": corr_id, "status": inner_status, "version": "2026-04-01" },
                "data": inner_data,
            });
            Ok(Some(ResponseEnvelope::success(
                kind_str.to_string(),
                corr_id.to_string(),
                &TaskFenceResponseData {
                    action: inner_envelope,
                    preload,
                },
            )))
        }
        _ => {
            tracing::warn!(
                task_id = %r.id,
                action_kind = %action_kind,
                "Task fence rejected: invalid fence action kind"
            );
            Ok(unless_fast(
                fast,
                ResponseEnvelope::error(
                    kind_str.to_string(),
                    corr_id.to_string(),
                    400,
                    "Invalid fence action kind",
                ),
            ))
        }
    }
}

// ============================================================================
// Db implementation — one row per promise
// ============================================================================

impl PostgresDb<'_> {
    fn task_retry_timeout(&self) -> i64 {
        self.task_retry_timeout
    }

    // Ghost operation — runs before every user operation.
    async fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let sql = self.cached("try_timeout", || {
            expire_batch_sql(
                "id = ANY($1) AND state = 'pending' AND timeout_at <= $2",
                "$2",
                self.task_retry_timeout,
            )
        });
        let row = sqlx::query(&sql)
            .bind(&ids)
            .bind(time)
            .fetch_optional(self.tx().await.pg())
            .await?;
        if let Some(row) = row {
            self.absorb_and_arm_retries(&row, time + self.task_retry_timeout);
        }
        Ok(())
    }

    // Lock preamble. One row now, where the multi-table backend locked the
    // promise row and then the task row.
    async fn lock_for_update(&self, id: &str) -> StorageResult<(bool, bool)> {
        let row = sqlx::query(
            "SELECT (task_state IS NOT NULL) AS has_task FROM promises WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(self.tx().await.pg())
        .await?;
        match row {
            Some(r) => Ok((true, r.get::<bool, _>("has_task"))),
            None => Ok((false, false)),
        }
    }

    // Fire callbacks for an already-settled promise, as its own statement so it
    // gets a fresh READ COMMITTED snapshot and sees callbacks committed by
    // concurrent transactions.
    async fn process_callbacks(&self, promise_id: &str, time: i64) -> StorageResult<()> {
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
        let row = sqlx::query(&sql)
            .bind(promise_id)
            .bind(time)
            .fetch_optional(self.tx().await.pg())
            .await?;
        if let Some(row) = row {
            self.absorb_and_arm_retries(&row, time + self.task_retry_timeout);
        }
        Ok(())
    }

    // P-01: promise.get
    async fn promise_get(&self, id: &str) -> StorageResult<Option<PromiseRecord>> {
        let sql = self.cached("promise_get", || {
            format!("SELECT {P_COLS} FROM promises WHERE id = $1")
        });
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.tx().await.pg())
            .await?;
        Ok(row.as_ref().map(row_to_promise))
    }

    // P-02: promise.create
    //
    // Five CTEs in the multi-table backend — promise, promise_timeout, task,
    // task_timeout, outgoing_execute — collapse to one INSERT plus the outbox.
    async fn promise_create(
        &self,
        params: &PromiseCreateParams<'_>,
    ) -> StorageResult<PromiseCreateResult> {
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
        let trt = self.task_retry_timeout;

        let sql = self.cached("promise_create", || {
            format!("
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
        ", messages = emitted_json(&["emit_new"]))
        });
        let rows = sqlx::query(&sql)
            .bind(id)
            .bind(state)
            .bind(param_headers)
            .bind(param_data)
            .bind(tags) // $1-$5
            .bind(timeout_at)
            .bind(created_at)
            .bind(settled_at) // $6-$8
            .bind(already_timedout)
            .bind(address) // $9-$10
            .fetch_all(self.tx().await.pg())
            .await?;

        if rows.is_empty() {
            // CTE snapshot race: a concurrent INSERT committed after our
            // snapshot, so the UNION ALL fallback saw neither row. Nothing was
            // committed — signal the caller to retry.
            return Err(StorageError::Serialization);
        }
        self.absorb_and_arm_retries(&rows[0], created_at + trt);
        let was_created: bool = rows[0].get("was_created");
        if was_created && !already_timedout && resonate_sql::external_tags(tags) {
            self.arm_promise_timeout(id, timeout_at);
        }
        Ok(PromiseCreateResult {
            was_created,
            promise: row_to_promise(&rows[0]),
        })
    }

    // P-03: promise.settle — lock preamble + one cascade statement
    async fn promise_settle(
        &self,
        params: &PromiseSettleParams<'_>,
        guard: Option<i64>,
    ) -> StorageResult<(PromiseSettleResult, bool)> {
        let PromiseSettleParams {
            id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;

        // One statement. `locked` takes the row lock — it waits out a
        // concurrent task.suspend writing our `callbacks`, then reads the
        // row's latest version, those awaiters included — which is what a
        // lock statement and a second, fresh-snapshot statement were for.
        // `guard`: with `Some(now)`, the statement writes nothing and reports
        // `declined` if the promise is pending past its deadline — the
        // transaction times it out first — or if the row changed after this
        // statement's snapshot was taken, which means it waited for the lock
        // and its other reads (the awaiters it would wake) may be stale. The
        // transaction's statements run after the lock, so they cannot be.
        let sql = self.cached("promise_settle", || {
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

            format!("
            WITH locked AS (
              SELECT *,
                     ($6::bigint IS NOT NULL AND (
                        (state = 'pending' AND timeout_at <= $6)
                        OR ctid <> (SELECT s.ctid FROM promises s WHERE s.id = $1))) AS declined
              FROM promises WHERE id = $1
              FOR UPDATE
            ),
            before AS (
              SELECT * FROM locked WHERE NOT declined
            ),
            updated_promise AS (
              UPDATE promises p
              SET state = $2, value_headers = COALESCE($3::jsonb, '{{}}'), value_data = $4, settled_at = $5,
                  {self_set}
              WHERE p.id = $1 AND p.state = 'pending' AND EXISTS (SELECT 1 FROM before)
              RETURNING p.*
            ),
            {unblock},
            {fanout},
            result AS (
              SELECT {RESULT_COLS}, true AS was_settled FROM updated_promise
              UNION ALL
              SELECT {RESULT_COLS}, false AS was_settled FROM locked
              WHERE NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT {P_COLS}, was_settled,
              COALESCE((SELECT declined FROM locked), false) AS declined,
              {messages}
            FROM result
        ", messages = emitted_json(&["emit_unblock", "emit_resume"]))
        });
        let rows = sqlx::query(&sql)
            .bind(id)
            .bind(state)
            .bind(value_headers)
            .bind(value_data)
            .bind(settled_at)
            .bind(guard)
            .fetch_all(self.tx().await.pg())
            .await?;

        let none = PromiseSettleResult {
            was_settled: false,
            promise: None,
        };
        let Some(row) = rows.first() else {
            return Ok((none, false));
        };
        if row.get::<bool, _>("declined") {
            return Ok((none, true));
        }
        self.absorb_and_arm_retries(row, settled_at + self.task_retry_timeout);
        Ok((
            PromiseSettleResult {
                was_settled: row.get("was_settled"),
                promise: Some(row_to_promise(row)),
            },
            false,
        ))
    }

    // P-04: promise.register_callback
    async fn promise_register_callback(
        &self,
        awaited_id: &str,
        awaiter_id: &str,
        time: i64,
        guard: Option<i64>,
    ) -> StorageResult<(RegisterCallbackResult, bool)> {
        let trt = self.task_retry_timeout;
        let sql = self.cached("promise_register_callback", || {
            format!("
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
            -- The fast path's guard: with `$4`, nothing below writes if
            -- either promise is pending past its deadline.
            expired AS (
              SELECT $4::bigint IS NOT NULL AND EXISTS (
                SELECT 1 FROM awaited WHERE state = 'pending' AND timeout_at <= $4
                UNION ALL
                SELECT 1 FROM awaiter WHERE state = 'pending' AND timeout_at <= $4
              ) AS hit
            ),
            awaitable AS (
              SELECT EXISTS (SELECT 1 FROM awaited WHERE external)
                     AND NOT (SELECT hit FROM expired) AS ok
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
            SELECT 'awaited' AS type, {awaited_cols}, {messages},
                   (SELECT hit FROM expired) AS expired FROM awaited
            UNION ALL
            SELECT 'awaiter' AS type, {awaiter_cols}, {messages},
                   (SELECT hit FROM expired) AS expired FROM awaiter
        ",
            awaited_cols = p_cols("awaited"),
            awaiter_cols = p_cols("awaiter"),
            messages = emitted_json(&["emit_resume"]),
        )
        });
        let rows = sqlx::query(&sql)
            .bind(awaited_id)
            .bind(awaiter_id)
            .bind(time)
            .bind(guard)
            .fetch_all(self.tx().await.pg())
            .await?;

        if let Some(row) = rows.first() {
            if row.get::<bool, _>("expired") {
                return Ok((
                    RegisterCallbackResult {
                        awaited: None,
                        awaiter: None,
                    },
                    true,
                ));
            }
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
        Ok((RegisterCallbackResult { awaited, awaiter }, false))
    }

    // P-05: promise.register_listener
    async fn promise_register_listener(
        &self,
        awaited_id: &str,
        address: &str,
        guard: Option<i64>,
    ) -> StorageResult<(Option<PromiseRecord>, bool)> {
        let sql = self.cached("promise_register_listener", || {
            format!(
                "
            WITH locked_promise AS (
              SELECT * FROM promises WHERE id = $1 FOR UPDATE
            ),
            -- A listener is an obligation, and `external` is where the server
            -- owes an observation. Refused by the caller with a 422, so nothing
            -- is written for a promise that may not be awaited.
            linked AS (
              UPDATE promises p SET listeners = p.listeners || $2
              WHERE p.id = $1
                AND NOT (p.listeners @> ARRAY[$2])
                AND EXISTS (SELECT 1 FROM locked_promise WHERE state = 'pending' AND external
                              AND NOT ($3::bigint IS NOT NULL AND timeout_at <= $3))
              RETURNING p.id
            )
            SELECT {cols},
              ($3::bigint IS NOT NULL AND state = 'pending' AND timeout_at <= $3) AS expired
            FROM locked_promise",
                cols = p_cols("locked_promise")
            )
        });
        let rows = sqlx::query(&sql)
            .bind(awaited_id)
            .bind(address)
            .bind(guard)
            .fetch_all(self.tx().await.pg())
            .await?;

        match rows.first() {
            None => Ok((None, false)),
            Some(row) if row.get::<bool, _>("expired") => Ok((None, true)),
            Some(row) => Ok((Some(row_to_promise(row)), false)),
        }
    }

    // P-06: promise.search
    /// Search by effective state at `now`: the filter is
    /// `resonate_sql::effective_state_sql`, and every record comes back
    /// projected, so a pending row past its deadline neither fills a
    /// "pending" page nor reads as pending on any other.
    async fn promise_search(
        &self,
        state: Option<&str>,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
        now: i64,
    ) -> StorageResult<Vec<PromiseRecord>> {
        // `now` is bound, never formatted in: a statement whose text changed
        // with the clock was a new prepared statement on every call, and each
        // one pushed a hot statement out of the connection's cache. Only the
        // states that depend on the deadline mention `$4`, so it is bound
        // only when it appears.
        let sql = format!(
            "SELECT {P_COLS} FROM promises
             WHERE {}
               AND ($1::jsonb IS NULL OR tags @> $1::jsonb)
               AND id > COALESCE($2::text, '')
             ORDER BY id ASC LIMIT $3",
            resonate_sql::effective_state_sql_at(state, "$4::bigint")
        );
        let q = sqlx::query(&sql).bind(tags).bind(cursor).bind(limit);
        let q = if sql.contains("$4") { q.bind(now) } else { q };
        let rows = q.fetch_all(self.tx().await.pg()).await?;
        Ok(rows
            .iter()
            .map(|row| {
                let mut p = row_to_promise(row);
                p.project(now);
                p
            })
            .collect())
    }

    // T-01: task.get — `resumes` is a local array now, not a COUNT over a join
    async fn task_get(&self, id: &str) -> StorageResult<Option<TaskRecord>> {
        let row = sqlx::query(
            "SELECT id, task_state, task_version, ttl, pid, resumes
                 FROM promises WHERE id = $1 AND COALESCE(task_state, '') <> ''",
        )
        .bind(id)
        .fetch_optional(self.tx().await.pg())
        .await?;
        Ok(row.as_ref().map(row_to_task))
    }

    // T-02: task.create
    /// `task.create`'s statement, and — when it created the task — the
    /// preload, computed in the same statement over the new row's branch.
    async fn task_create(
        &self,
        params: &TaskCreateParams<'_>,
    ) -> StorageResult<(TaskCreateResult, Vec<PromiseRecord>)> {
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

        let sql = self.cached("task_create", || {
            format!("
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
              p.task_state, p.task_version,
              CASE WHEN EXISTS (SELECT 1 FROM inserted_promise) THEN {preload} END AS preload
            FROM promise p
        ", cols = p_cols("p"),
           preload = preload_sql("(SELECT branch_id FROM inserted_promise)", "$1", None, self.preload_limit))
        });
        let rows = sqlx::query(&sql)
            .bind(promise_id)
            .bind(state)
            .bind(param_headers)
            .bind(param_data)
            .bind(tags) // $1-$5
            .bind(timeout_at)
            .bind(created_at)
            .bind(settled_at) // $6-$8
            .bind(already_timedout)
            .bind(ttl)
            .bind(pid)
            .bind(task_initial_state) // $9-$12
            .fetch_all(self.tx().await.pg())
            .await?;

        if rows.is_empty() {
            return Err(StorageError::Serialization);
        }
        let row = &rows[0];
        let promise = row_to_promise(row);
        let task_created: bool = row.get("task_created");

        if task_created {
            if !already_timedout {
                if resonate_core::types::is_external(&promise.tags) {
                    self.arm_promise_timeout(promise_id, timeout_at);
                }
                self.arm_lease(promise_id, pid, created_at + ttl);
            }
            return Ok((
                TaskCreateResult {
                    promise,
                    task_created: true,
                    task_state: Some(task_initial_state.to_string()),
                    task_version: Some(if already_timedout { 0 } else { 1 }),
                },
                preload_from(row),
            ));
        }

        Ok((
            TaskCreateResult {
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
            },
            Vec::new(),
        ))
    }

    // T-03: task.acquire
    async fn task_acquire(
        &self,
        params: &TaskAcquireParams<'_>,
    ) -> StorageResult<TaskAcquireResult> {
        self.task_acquire_guarded(params, false)
            .await
            .map(|(r, _, _)| r)
    }

    /// `task.acquire` as one statement, its preload included.
    ///
    /// `guard`: write nothing if the promise is pending past its deadline,
    /// and say so in the second value — the fast path's contract. Without it
    /// the statement is what the slow path runs after `try_timeout`.
    async fn task_acquire_guarded(
        &self,
        params: &TaskAcquireParams<'_>,
        guard: bool,
    ) -> StorageResult<(TaskAcquireResult, bool, Vec<PromiseRecord>)> {
        let TaskAcquireParams {
            task_id,
            version,
            time,
            ttl,
            pid,
        } = *params;
        let sql = self.cached("task_acquire_guarded", || {
            format!(
                "
            WITH before AS (
              SELECT id, task_state, task_version, branch_id,
                     (state = 'pending' AND timeout_at <= $3) AS expired
              FROM promises WHERE id = $1 AND COALESCE(task_state, '') <> ''
            ),
            acquired_task AS (
              UPDATE promises p SET
                task_state = 'acquired', task_version = p.task_version + 1,
                lease_timeout_at = $3 + $4, ttl = $4, pid = $5, retry_timeout_at = NULL,
                resumes = '{{}}'                    -- deleted_ready_callbacks
              WHERE p.id = $1 AND p.task_version = $2 AND COALESCE(p.task_state, '') = 'pending'
                AND NOT ($6 AND p.state = 'pending' AND p.timeout_at <= $3)
              RETURNING p.id, p.task_state, p.task_version
            )
            SELECT {cols},
              COALESCE(a.task_state, b.task_state)     AS task_state,
              COALESCE(a.task_version, b.task_version) AS task_version,
              (a.id IS NOT NULL)                       AS was_acquired,
              b.expired,
              CASE WHEN a.id IS NOT NULL THEN {preload} END AS preload
            FROM before b
            JOIN promises p ON p.id = b.id
            LEFT JOIN acquired_task a ON a.id = b.id
        ",
                cols = p_cols("p"),
                preload = preload_sql("b.branch_id", "$1", None, self.preload_limit)
            )
        });
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .bind(version as i32)
            .bind(time)
            .bind(ttl)
            .bind(pid)
            .bind(guard)
            .fetch_all(self.tx().await.pg())
            .await?;

        if rows.is_empty() {
            return Ok((
                TaskAcquireResult {
                    promise: None,
                    was_acquired: false,
                    task_state: None,
                    task_version: None,
                },
                false,
                Vec::new(),
            ));
        }
        let row = &rows[0];
        let task_state: String = row.get("task_state");
        let was_acquired: bool = row.get("was_acquired");
        let expired: bool = row.get("expired");
        if was_acquired {
            self.arm_lease(task_id, pid, time + ttl);
        }
        Ok((
            TaskAcquireResult {
                promise: Some(row_to_promise(row)),
                was_acquired,
                task_state: Some(parse_task_state(&task_state)),
                task_version: Some(row.get::<i32, _>("task_version") as i64),
            },
            expired,
            preload_from(row),
        ))
    }

    /// `task.get` with the one fact the fast path needs beside it: whether
    /// the promise is pending past its deadline. `None` when there is no row
    /// at all; a row that is no task comes back with no record.
    async fn task_get_probe(
        &self,
        id: &str,
        now: i64,
    ) -> StorageResult<Option<(Option<TaskRecord>, bool)>> {
        let row = sqlx::query(
            "SELECT id, task_state, task_version, ttl, pid, resumes,
                        (state = 'pending' AND timeout_at <= $2) AS expired
                 FROM promises WHERE id = $1",
        )
        .bind(id)
        .bind(now)
        .fetch_optional(self.tx().await.pg())
        .await?;
        Ok(row.map(|r| {
            let task = r
                .get::<Option<String>, _>("task_state")
                .map(|_| row_to_task(&r));
            (task, r.get::<bool, _>("expired"))
        }))
    }

    // T-04: task.fence (create variant) — fence on one row, insert another
    /// The fenced `promise.create` as one statement, preload included.
    ///
    /// The task row is locked by the statement itself (`FOR UPDATE` in
    /// `fence_check`), so under READ COMMITTED the fence is checked against
    /// the row's latest version and holds until commit — the lock preamble's
    /// job, without its round trip. `guard` is the fast path's: `Some(now)`
    /// writes nothing if the task's promise or the one being created is
    /// pending past its deadline, and reports it.
    async fn task_fence_create_guarded(
        &self,
        params: &TaskFenceCreateParams<'_>,
        guard: Option<i64>,
    ) -> StorageResult<(TaskFenceResult, bool, Vec<PromiseRecord>)> {
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

        let sql = self.cached("task_fence_create_guarded", || {
            format!("
            WITH fence_check AS (
              SELECT id, task_state, task_version, branch_id,
                     ctid <> (SELECT s.ctid FROM promises s WHERE s.id = $1) AS stale
              FROM promises
              WHERE id = $1 AND COALESCE(task_state, '') <> ''
              FOR UPDATE
            ),
            -- Why the fast path declines: a promise past its deadline, or a
            -- task row that changed after this statement's snapshot.
            expired AS (
              SELECT $13::bigint IS NOT NULL AND EXISTS (
                SELECT 1 FROM promises
                WHERE id IN ($1, $3) AND state = 'pending' AND (timeout_at + 0) <= $13
              ) OR ($13::bigint IS NOT NULL AND COALESCE((SELECT stale FROM fence_check), false))
              AS hit
            ),
            fence_ok AS (
              SELECT EXISTS (SELECT 1 FROM fence_check WHERE task_state = 'acquired' AND task_version = $2)
                     AND NOT (SELECT hit FROM expired) AS ok
            ),
            -- Every fence rewrites its task row, even to the same values.
            -- A fence that only locked it would leave no trace: one that
            -- committed after this statement's snapshot, while this statement
            -- waited for the lock, would have written siblings this statement
            -- cannot see. Rewritten, the row's version moves, `fence_check`
            -- reports `stale`, and the fast path declines. (Not when the
            -- fenced promise is the task's own: one statement must not
            -- update a row twice.)
            bumped AS (
              UPDATE promises SET task_version = task_version
              WHERE id = $1 AND $1 <> $3 AND (SELECT ok FROM fence_ok)
              RETURNING id
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
              (SELECT hit FROM expired) AS expired,
              EXISTS (SELECT 1 FROM inserted_or_skipped_promise) AS was_created,
              {cols}, {messages},
              CASE WHEN r.id IS NOT NULL THEN {preload} END AS preload
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        ", cols = p_cols("r"), messages = emitted_json(&["emit_new"]),
           preload = preload_sql("(SELECT branch_id FROM fence_check)", "$1",
                                 Some("inserted_or_skipped_promise"), self.preload_limit))
        });
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .bind(version as i32) // $1-$2
            .bind(promise_id)
            .bind(state)
            .bind(param_headers)
            .bind(param_data)
            .bind(tags) // $3-$7
            .bind(timeout_at)
            .bind(created_at)
            .bind(settled_at) // $8-$10
            .bind(already_timedout)
            .bind(address) // $11-$12
            .bind(guard) // $13
            .fetch_all(self.tx().await.pg())
            .await?;

        if rows.is_empty() {
            return Err(StorageError::Serialization);
        }
        let row = &rows[0];
        let expired: bool = row.get("expired");
        if expired {
            return Ok((
                TaskFenceResult {
                    task_exists: false,
                    fence_ok: false,
                    promise: None,
                },
                true,
                Vec::new(),
            ));
        }
        let promise_id_val: Option<String> = row.get("id");
        if row.get::<bool, _>("fence_ok") && promise_id_val.is_none() {
            // The insert conflicted with a row this statement's snapshot
            // cannot see — a concurrent create committed in between. Nothing
            // was written; run it again and it will.
            return Err(StorageError::Serialization);
        }
        self.absorb_and_arm_retries(row, created_at + trt);
        // The INSERT put the row on the queue if it is pending and external;
        // that is the deadline to announce, task or no task.
        let was_created: bool = row.get("was_created");
        if was_created && !already_timedout && resonate_sql::external_tags(tags) {
            self.arm_promise_timeout(promise_id, timeout_at);
        }
        Ok((
            TaskFenceResult {
                task_exists: row.get("task_exists"),
                fence_ok: row.get("fence_ok"),
                promise: promise_id_val.map(|_| row_to_promise(row)),
            },
            false,
            preload_from(row),
        ))
    }

    // T-04: task.fence (settle variant) — fence on one row, settlement cascade on another
    /// The fenced `promise.settle` as one statement, preload included — the
    /// same shape as `task_fence_create_guarded`. The settled promise is
    /// locked inside the statement too (`locked_promise`), so `before` reads
    /// its latest version, awaiters registered a moment ago included.
    async fn task_fence_settle_guarded(
        &self,
        params: &TaskFenceSettleParams<'_>,
        guard: Option<i64>,
    ) -> StorageResult<(TaskFenceResult, bool, Vec<PromiseRecord>)> {
        let TaskFenceSettleParams {
            task_id,
            version,
            promise_id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;

        let sql = self.cached("task_fence_settle_guarded", || {
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

            format!("
            WITH fence_check AS (
              SELECT id, task_state, task_version, branch_id,
                     ctid <> (SELECT s.ctid FROM promises s WHERE s.id = $1) AS stale
              FROM promises
              WHERE id = $1 AND COALESCE(task_state, '') <> ''
              FOR UPDATE
            ),
            -- Why the fast path declines: a promise past its deadline, or a
            -- task row that changed after this statement's snapshot.
            expired AS (
              SELECT $8::bigint IS NOT NULL AND EXISTS (
                SELECT 1 FROM promises
                WHERE id IN ($1, $3) AND state = 'pending' AND (timeout_at + 0) <= $8
              ) OR ($8::bigint IS NOT NULL AND COALESCE((SELECT stale FROM fence_check), false))
              AS hit
            ),
            fence_ok AS (
              SELECT EXISTS (SELECT 1 FROM fence_check WHERE task_state = 'acquired' AND task_version = $2)
                     AND NOT (SELECT hit FROM expired) AS ok
            ),
            -- `stale` here: the promise changed after the snapshot — an
            -- awaiter suspended on it while this statement waited — and the
            -- awaiters' rows this statement would wake are read as they were.
            locked_promise AS (
              SELECT *, ($8::bigint IS NOT NULL
                         AND ctid <> (SELECT s.ctid FROM promises s WHERE s.id = $3)) AS stale
              FROM promises WHERE id = $3 AND (SELECT ok FROM fence_ok) FOR UPDATE
            ),
            before AS (
              SELECT id, state, task_state, callbacks, listeners FROM locked_promise WHERE NOT stale
            ),
            updated_promise AS (
              UPDATE promises p
              SET state = $4, value_headers = COALESCE($5::jsonb, '{{}}'), value_data = $6, settled_at = $7,
                  {self_set}
              WHERE p.id = $3 AND p.state = 'pending' AND (SELECT ok FROM fence_ok)
                AND EXISTS (SELECT 1 FROM before)
              RETURNING p.*
            ),
            {unblock},
            {fanout},
            -- Every fence rewrites its task row, even to the same values.
            -- A fence that only locked it would leave no trace: one that
            -- committed after this statement's snapshot, while this statement
            -- waited for the lock, would have written siblings this statement
            -- cannot see. Rewritten, the row's version moves, `fence_check`
            -- reports `stale`, and the fast path declines. (Not when the
            -- fenced promise is the task's own: one statement must not
            -- update a row twice.)
            bumped AS (
              UPDATE promises SET task_version = task_version
              WHERE id = $1 AND $1 <> $3 AND (SELECT ok FROM fence_ok)
                AND NOT EXISTS (SELECT 1 FROM locked_promise WHERE stale)
                -- Nor when the fan-out rewrites it (the fenced task awaits
                -- the promise it settles): one statement must not update a
                -- row twice — Postgres keeps one write and silently drops the
                -- other — and the fan-out's write moves the version anyway.
                -- Reading `fanout` here also runs it first.
                AND NOT EXISTS (SELECT 1 FROM fanout WHERE fanout.id = $1)
              RETURNING id
            ),
            result AS (
              SELECT {RESULT_COLS} FROM updated_promise
              UNION ALL
              SELECT {RESULT_COLS} FROM locked_promise WHERE NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT
              EXISTS (SELECT 1 FROM fence_check) AS task_exists,
              (SELECT ok FROM fence_ok) AS fence_ok,
              (SELECT hit FROM expired)
                OR COALESCE((SELECT stale FROM locked_promise), false) AS expired,
              {cols}, {messages},
              CASE WHEN (SELECT ok FROM fence_ok) THEN {preload} END AS preload
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        ", cols = p_cols("r"), messages = emitted_json(&["emit_unblock", "emit_resume"]),
           preload = preload_sql("(SELECT branch_id FROM fence_check)", "$1",
                                 Some("updated_promise"), self.preload_limit))
        });
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .bind(version as i32) // $1-$2
            .bind(promise_id)
            .bind(state)
            .bind(value_headers)
            .bind(value_data)
            .bind(settled_at) // $3-$7
            .bind(guard) // $8
            .fetch_all(self.tx().await.pg())
            .await?;

        if rows.is_empty() {
            return Ok((
                TaskFenceResult {
                    task_exists: false,
                    fence_ok: false,
                    promise: None,
                },
                false,
                Vec::new(),
            ));
        }
        let row = &rows[0];
        if row.get::<bool, _>("expired") {
            return Ok((
                TaskFenceResult {
                    task_exists: false,
                    fence_ok: false,
                    promise: None,
                },
                true,
                Vec::new(),
            ));
        }
        self.absorb_and_arm_retries(row, settled_at + self.task_retry_timeout);
        let promise_id_val: Option<String> = row.get("id");
        Ok((
            TaskFenceResult {
                task_exists: row.get("task_exists"),
                fence_ok: row.get("fence_ok"),
                promise: promise_id_val.map(|_| row_to_promise(row)),
            },
            false,
            preload_from(row),
        ))
    }

    // T-05: task.heartbeat — extend the lease of every task this pid still holds
    async fn task_heartbeat(
        &self,
        pid: &str,
        tasks: &[(&str, i64)],
        time: i64,
    ) -> StorageResult<()> {
        if tasks.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = tasks.iter().map(|(id, _)| id.to_string()).collect();
        let versions: Vec<i32> = tasks.iter().map(|(_, v)| *v as i32).collect();

        // RETURNING, because the new deadline is `$3 + p.ttl` and `ttl` is a
        // column: the caller cannot compute what was written without reading
        // it back. The only statement in this backend that needed new SQL to
        // announce its deadline.
        let rows = sqlx::query(
            "
            WITH task_data AS (
              SELECT unnest($1::text[]) AS id, unnest($2::int[]) AS version
            )
            UPDATE promises p SET lease_timeout_at = $3 + p.ttl
            FROM task_data td
            WHERE p.id = td.id AND p.task_version = td.version
              AND COALESCE(p.task_state, '') = 'acquired' AND p.pid = $4
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
        .fetch_all(self.tx().await.pg())
        .await?;
        for row in &rows {
            let id: String = row.get("id");
            let lease_timeout_at: i64 = row.get("lease_timeout_at");
            self.arm_lease(&id, pid, lease_timeout_at);
        }
        Ok(())
    }

    // T-06: task.suspend
    /// `task.suspend` as one statement: locks, checks, links and suspends, and
    /// computes the preload an immediate resume answers with.
    ///
    /// Every row it touches is locked by `locked`, awaited promises first and
    /// the task last. That is the order a settle takes them in — its own row,
    /// then the awaiters it wakes — so a child settling while its parent
    /// suspends on it waits rather than deadlocks. Every decision is made in
    /// `facts`, an aggregate over the locked rows: an aggregate reads its input
    /// to the end before it yields, so the locked versions are read in full
    /// before either UPDATE below writes one of them.
    ///
    /// `guard`: with `Some(now)`, nothing is written if any of these promises
    /// is pending past its deadline. Returns the result, whether the task
    /// exists, whether the guard tripped, and the preload.
    async fn task_suspend_guarded(
        &self,
        task_id: &str,
        version: i64,
        awaited_ids: &[&str],
        guard: Option<i64>,
    ) -> StorageResult<(TaskSuspendResult, bool, bool, Vec<PromiseRecord>)> {
        let awaited: Vec<String> = awaited_ids.iter().map(|s| s.to_string()).collect();
        let mut lock_ids: Vec<String> = awaited.clone();
        lock_ids.push(task_id.to_string());

        let sql = self.cached("task_suspend_guarded", || {
            format!("
            WITH locked AS (
              SELECT id, state, external, task_state, task_version, branch_id,
                     -- Decline (with `$4`): past its deadline, or changed after
                     -- the snapshot — this statement waited for its lock, and
                     -- the preload it would read may predate that writer.
                     ($4::bigint IS NOT NULL AND (
                        (state = 'pending' AND timeout_at <= $4)
                        OR ctid <> (SELECT s.ctid FROM promises s WHERE s.id = p.id))) AS expired
              FROM promises p WHERE id = ANY($5)
              ORDER BY (id = $1), id
              FOR UPDATE
            ),
            facts AS (
              SELECT
                COALESCE(bool_or(id = $1 AND task_state IS NOT NULL), false) AS task_exists,
                COALESCE(bool_or(id = $1 AND task_state = 'acquired' AND task_version = $2), false) AS ok,
                COALESCE(bool_or(expired), false) AS expired,
                (SELECT branch_id FROM locked WHERE id = $1) AS branch_id,
                COUNT(*) FILTER (WHERE id = ANY($3))::INT AS found,
                COUNT(*) FILTER (WHERE id = ANY($3) AND NOT external)::INT AS non_awaitable,
                COALESCE(bool_or(id = ANY($3) AND state <> 'pending'), false) AS any_settled
              FROM locked
            ),
            -- What the multi-statement form called `matched`, `missing`,
            -- `non_awaitable` and `can_suspend`. The awaited rows count only
            -- when the task matched, as they did there.
            verdict AS (
              SELECT task_exists, expired, branch_id,
                ok AND NOT expired AS ok,
                COALESCE(array_length($3::text[], 1), 0) - CASE WHEN ok THEN found ELSE 0 END AS missing,
                CASE WHEN ok THEN non_awaitable ELSE 0 END AS non_awaitable,
                any_settled
              FROM facts
            ),
            can_suspend AS (
              SELECT 1 FROM verdict
              WHERE ok AND missing = 0 AND non_awaitable = 0 AND NOT any_settled
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
                resumes    = '{{}}',
                callbacks   = CASE WHEN $1 = ANY($3) AND EXISTS (SELECT 1 FROM can_suspend)
                                    AND NOT (p.callbacks @> ARRAY[$1])
                               THEN p.callbacks || $1 ELSE p.callbacks END
              WHERE p.id = $1
                AND (SELECT ok AND missing = 0 AND non_awaitable = 0 FROM verdict)
              RETURNING p.id
            )
            SELECT
              v.ok AS task_matched,
              EXISTS (SELECT 1 FROM can_suspend) AS was_suspended,
              v.missing AS missing_count,
              v.non_awaitable AS non_awaitable_count,
              v.task_exists,
              v.expired,
              CASE WHEN v.ok AND v.missing = 0 AND v.non_awaitable = 0
                        AND NOT EXISTS (SELECT 1 FROM can_suspend)
                   THEN {preload} END AS preload
            FROM verdict v
        ", preload = preload_sql("v.branch_id", "$1", None, self.preload_limit))
        });
        let row = sqlx::query(&sql)
            .bind(task_id)
            .bind(version as i32)
            .bind(&awaited)
            .bind(guard)
            .bind(&lock_ids)
            .fetch_one(self.tx().await.pg())
            .await?;

        Ok((
            TaskSuspendResult {
                task_matched: row.get("task_matched"),
                was_suspended: row.get("was_suspended"),
                missing_count: row.get("missing_count"),
                non_awaitable_count: row.get("non_awaitable_count"),
            },
            row.get("task_exists"),
            row.get("expired"),
            preload_from(&row),
        ))
    }

    // T-07: task.fulfill — the task and the promise are the same row, so the
    // multi-table backend's `fulfilled_acquired_task` and `updated_promise`
    // must become one UPDATE.
    /// `task.fulfill` as one statement.
    ///
    /// The row is locked by the statement itself — `locked` is `FOR UPDATE`,
    /// so after waiting out a concurrent writer it reads the row's latest
    /// version, callbacks registered a moment ago included — which is what the
    /// lock preamble's separate statement was for. `guard`: with `Some(now)`
    /// nothing is written, and the second value says so, if the promise is
    /// pending past its deadline or the row changed after the statement's
    /// snapshot — it waited for the lock, so what it read of other rows (the
    /// awaiters it would wake) may predate the writer it waited for.
    async fn task_fulfill_guarded(
        &self,
        params: &TaskFulfillParams<'_>,
        guard: Option<i64>,
    ) -> StorageResult<(TaskFulfillResult, bool)> {
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

        // `fulfilled` here is the task transition, which also drives the
        // promise settlement — hence one shared guard.
        let guard_sql =
            "(SELECT b.task_state = 'acquired' AND b.task_version = $2 AND NOT b.declined FROM before b)";
        let settle_guard =
            "(SELECT b.task_state = 'acquired' AND b.task_version = $2 AND NOT b.declined \
             AND b.state = 'pending' FROM before b)";
        let sql = self.cached("task_fulfill_guarded", || {
            let self_set = settle_self(guard_sql);
            let unblock = settle_unblock("updated_promise", "(SELECT b.listeners FROM before b)");
            let fanout = settle_fanout(
                "$3",
                &format!("(SELECT CASE WHEN {settle_guard} THEN b.callbacks END FROM before b)"),
                guard_sql,
                "$7",
                self.task_retry_timeout,
            );

            format!("
            WITH locked AS (
              SELECT *,
                     ($8::bigint IS NOT NULL AND (
                        (state = 'pending' AND timeout_at <= $8)
                        OR ctid <> (SELECT s.ctid FROM promises s WHERE s.id = $3))) AS declined
              FROM promises WHERE id = $3
              FOR UPDATE
            ),
            before AS (
              SELECT * FROM locked
            ),
            updated_promise AS (
              UPDATE promises p
              SET state = CASE WHEN p.state = 'pending' THEN $4 ELSE p.state END,
                  value_headers = CASE WHEN p.state = 'pending' THEN COALESCE($5::jsonb, '{{}}') ELSE p.value_headers END,
                  value_data    = CASE WHEN p.state = 'pending' THEN $6 ELSE p.value_data END,
                  settled_at    = CASE WHEN p.state = 'pending' THEN $7 ELSE p.settled_at END,
                  {self_set}
              -- The fence is checked on `locked`, which holds the row's lock
              -- and so its latest version: said here instead, `task_state =
              -- 'acquired'` matched the lease index's predicate and the update
              -- was planned as a walk of that whole index (2 ms, 380 buffers).
              WHERE p.id = $3
                AND EXISTS (SELECT 1 FROM locked
                            WHERE NOT declined AND task_state = 'acquired' AND task_version = $2)
              RETURNING p.*
            ),
            {unblock},
            {fanout},
            result AS (
              SELECT {RESULT_COLS} FROM updated_promise
              UNION ALL
              SELECT {RESULT_COLS} FROM locked WHERE NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT {cols},
              EXISTS (SELECT 1 FROM updated_promise) AS task_fulfilled,
              (SELECT b.task_state IS NOT NULL FROM before b) AS task_exists,
              COALESCE((SELECT b.declined FROM before b), false) AS declined,
              {messages}
            FROM result r
        ", cols = p_cols("r"), messages = emitted_json(&["emit_unblock", "emit_resume"]))
        });
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .bind(version as i32) // $1-$2
            .bind(promise_id)
            .bind(state)
            .bind(value_headers)
            .bind(value_data)
            .bind(settled_at) // $3-$7
            .bind(guard) // $8
            .fetch_all(self.tx().await.pg())
            .await?;

        if rows.is_empty() {
            return Ok((
                TaskFulfillResult {
                    task_exists: false,
                    task_fulfilled: false,
                    promise: None,
                },
                false,
            ));
        }
        let row = &rows[0];
        if row.get::<bool, _>("declined") {
            return Ok((
                TaskFulfillResult {
                    task_exists: false,
                    task_fulfilled: false,
                    promise: None,
                },
                true,
            ));
        }
        self.absorb_and_arm_retries(row, settled_at + self.task_retry_timeout);
        Ok((
            TaskFulfillResult {
                task_exists: row
                    .try_get::<Option<bool>, _>("task_exists")
                    .ok()
                    .flatten()
                    .unwrap_or(false),
                task_fulfilled: row.get("task_fulfilled"),
                promise: Some(row_to_promise(row)),
            },
            false,
        ))
    }

    // T-08: task.release
    async fn task_release(
        &self,
        task_id: &str,
        version: i64,
        time: i64,
        ttl: i64,
    ) -> StorageResult<TaskReleaseResult> {
        let row = (
            sqlx::query(
                "
            WITH released_task AS (
              UPDATE promises p SET
                task_state = 'pending', retry_timeout_at = $3 + $4,
                lease_timeout_at = NULL, ttl = NULL, pid = NULL
              WHERE p.id = $1 AND p.task_version = $2 AND COALESCE(p.task_state, '') = 'acquired'
              RETURNING p.id, p.task_version, p.target
            ),
            emit_released AS (
              SELECT 'execute'::text AS kind, t.target AS address, t.id AS task_id,
                     t.task_version::int AS version, NULL::jsonb AS promise
              FROM released_task t WHERE t.target IS NOT NULL
            )
            SELECT
              EXISTS (SELECT 1 FROM released_task) AS task_released,
              EXISTS (SELECT 1 FROM promises WHERE id = $1 AND COALESCE(task_state, '') <> '') AS task_exists,
              :MESSAGES
        "
            .replace(":MESSAGES", &emitted_json(&["emit_released"]))
            .as_str(),
            )
            .bind(task_id)
            .bind(version as i32)
            .bind(time)
            .bind(ttl)
            .fetch_one(self.tx().await.pg())
        ).await?;

        self.absorb_and_arm_retries(&row, time + ttl);
        Ok(TaskReleaseResult {
            task_released: row.get("task_released"),
            task_exists: row.get("task_exists"),
        })
    }

    // T-09: task.halt
    async fn task_halt(&self, task_id: &str) -> StorageResult<TaskHaltResult> {
        let row = (
            sqlx::query(
                "
            WITH locked_task AS (
              SELECT id, task_state FROM promises WHERE id = $1 AND COALESCE(task_state, '') <> '' FOR UPDATE
            ),
            halted_task AS (
              UPDATE promises p SET
                task_state = 'halted', retry_timeout_at = NULL, lease_timeout_at = NULL, ttl = NULL, pid = NULL
              WHERE p.id = $1 AND COALESCE(p.task_state, '') <> ''
                AND p.task_state NOT IN ('fulfilled', 'halted')
              RETURNING p.id
            )
            SELECT
              EXISTS (SELECT 1 FROM locked_task) AS task_exists,
              EXISTS (SELECT 1 FROM locked_task WHERE task_state = 'fulfilled') AS task_fulfilled
        ",
            )
            .bind(task_id)
            .fetch_one(self.tx().await.pg())
        ).await?;

        Ok(TaskHaltResult {
            task_exists: row.get("task_exists"),
            task_fulfilled: row.get("task_fulfilled"),
        })
    }

    // T-10: task.continue
    async fn task_continue(&self, task_id: &str, time: i64) -> StorageResult<TaskContinueResult> {
        let trt = self.task_retry_timeout;
        let row = sqlx::query(&format!(
            "
            WITH locked_task AS (
              SELECT id, task_state, task_version, target FROM promises
              WHERE id = $1 AND COALESCE(task_state, '') <> '' FOR UPDATE
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
        .fetch_one(self.tx().await.pg())
        .await?;

        self.absorb_and_arm_retries(&row, time + trt);
        Ok(TaskContinueResult {
            task_exists: row.get("task_exists"),
            continued: row.get("continued"),
        })
    }

    // T-11: task.search
    async fn task_search(
        &self,
        state: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<TaskRecord>> {
        // One text per shape, and the cursor as `id > COALESCE(..)` rather
        // than `$n IS NULL OR id > $n`: a generic plan cannot use an index for
        // a condition that might not apply, so the old form walked the primary
        // key filtering every row, and restarted from the first row on every
        // page (165 ms a page at 1.7M rows). `''` sorts before every id.
        let rows = match state {
            Some(state) => {
                sqlx::query(
                    "SELECT id, task_state, task_version, ttl, pid, resumes FROM promises
                     WHERE task_state = $1 AND id > COALESCE($2::text, '')
                     ORDER BY id ASC LIMIT $3",
                )
                .bind(state)
                .bind(cursor)
                .bind(limit)
                .fetch_all(self.tx().await.pg())
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT id, task_state, task_version, ttl, pid, resumes FROM promises
                     WHERE task_state IS NOT NULL AND id > COALESCE($1::text, '')
                     ORDER BY id ASC LIMIT $2",
                )
                .bind(cursor)
                .bind(limit)
                .fetch_all(self.tx().await.pg())
                .await?
            }
        };
        Ok(rows.iter().map(row_to_task).collect())
    }

    async fn compute_preload(&self, promise_id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let rows = sqlx::query(&format!(
            "SELECT {P_COLS} FROM promises
                 WHERE branch_id = (SELECT branch_id FROM promises WHERE id = $1)
                   AND branch_id IS NOT NULL AND id <> $1
                 ORDER BY id ASC LIMIT $2"
        ))
        .bind(promise_id)
        .bind(self.preload_limit as i64)
        .fetch_all(self.tx().await.pg())
        .await?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    // S-01: schedule.get
    async fn schedule_get(&self, id: &str) -> StorageResult<Option<ScheduleRecord>> {
        let row = sqlx::query(
            "SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{}'::jsonb)::text AS promise_param_headers,
                    promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
             FROM schedules WHERE id = $1")
            .bind(id).fetch_optional(self.tx().await.pg()).await?;
        Ok(row.as_ref().map(row_to_schedule))
    }

    // S-03: schedule.create — schedule_timeouts is gone, next_run_at *is* the queue
    async fn schedule_create(
        &self,
        params: &ScheduleCreateParams<'_>,
    ) -> StorageResult<ScheduleRecord> {
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

        let row = sqlx::query("
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
            .fetch_one(self.tx().await.pg()).await?;

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
    async fn schedule_delete(&self, id: &str) -> StorageResult<bool> {
        let res = sqlx::query("DELETE FROM schedules WHERE id = $1")
            .bind(id)
            .execute(self.tx().await.pg())
            .await?;
        Ok(res.rows_affected() > 0)
    }

    // S-05: schedule.search
    async fn schedule_search(
        &self,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let rows = sqlx::query(
            "SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{}'::jsonb)::text AS promise_param_headers,
                    promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
             FROM schedules
             WHERE ($1::jsonb IS NULL OR promise_tags @> $1::jsonb) AND id > COALESCE($2::text, '')
             ORDER BY id ASC LIMIT $3")
            .bind(tags).bind(cursor).bind(limit).fetch_all(self.tx().await.pg()).await?;
        Ok(rows.iter().map(row_to_schedule).collect())
    }

    // === Console reads ===
    //
    // Three statements, one per screen. Everything a client can vary — sort,
    // direction, cursor, limits — is resolved in `resonate_core::ui` before it
    // gets here, so what these build is a `format!` over constants and a bind
    // list, never caller text.

    /// The executions list: root promises, sorted, one keyset page.
    ///
    /// `id = origin_id` is root-ness — an id is `<origin>:<lineage>`, so a
    /// promise with no lineage is a root. `origin_id` is a stored generated
    /// column with its own index, so this is a comparison rather than a scan
    /// of the tags.
    async fn ui_executions_search(
        &self,
        q: &ui::ExecutionsQuery,
    ) -> StorageResult<Vec<PromiseRecord>> {
        let (expr, cmp, dir) = (q.sort.key.expr(), q.sort.dir.cmp_sql(), q.sort.dir.sql());
        let sql = format!(
            "SELECT {P_COLS} FROM promises
             WHERE id = origin_id{states}
               AND ($1::text IS NULL OR id >= $1)
               AND ($2::text IS NULL OR id < $2)
               AND ($3::jsonb IS NULL OR tags @> $3::jsonb)
               AND ($4::bigint IS NULL OR created_at >= $4)
               AND ($5::bigint IS NULL OR created_at <= $5)
               AND ($6::bigint IS NULL OR ({expr}, id) {cmp} ($6::bigint, $7::text))
             ORDER BY {expr} {dir}, id {dir}
             LIMIT $8",
            states = q.states_sql(),
        );
        let (after_key, after_id) = split_keyset(q.after.as_ref());
        let rows = sqlx::query(&sql)
            .bind(q.id_from.as_deref())
            .bind(q.id_to.as_deref())
            .bind(q.tags_json.as_deref())
            .bind(q.created_from)
            .bind(q.created_to)
            .bind(after_key)
            .bind(after_id)
            .bind(q.fetch + 1)
            .fetch_all(self.tx().await.pg())
            .await?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    /// How many executions match, ignoring the cursor — the "of 240" a page
    /// number is useless without.
    async fn ui_executions_count(&self, q: &ui::ExecutionsQuery) -> StorageResult<i64> {
        let sql = format!(
            "SELECT COUNT(*) AS n FROM promises
             WHERE id = origin_id{states}
               AND ($1::text IS NULL OR id >= $1)
               AND ($2::text IS NULL OR id < $2)
               AND ($3::jsonb IS NULL OR tags @> $3::jsonb)
               AND ($4::bigint IS NULL OR created_at >= $4)
               AND ($5::bigint IS NULL OR created_at <= $5)",
            states = q.states_sql(),
        );
        let row = sqlx::query(&sql)
            .bind(q.id_from.as_deref())
            .bind(q.id_to.as_deref())
            .bind(q.tags_json.as_deref())
            .bind(q.created_from)
            .bind(q.created_to)
            .fetch_one(self.tx().await.pg())
            .await?;
        Ok(row.get::<i64, _>("n"))
    }

    /// One execution, whole: every promise sharing the root's origin, with the
    /// task columns that sit on the same row.
    ///
    /// This is the request that replaces `resonate-ui`'s recursive fan-out —
    /// one indexed read on `origin_id` instead of one round trip per level,
    /// re-run every 5s.
    async fn ui_execution_nodes(&self, q: &ui::ExecutionQuery) -> StorageResult<Vec<ui::NodeRow>> {
        let rows = sqlx::query(&format!(
            "SELECT {P_COLS}, task_state, task_version,
                        cardinality(resumes) AS resumes,
                        ttl, pid, retry_timeout_at, lease_timeout_at
                 FROM promises
                 WHERE origin_id = $1
                 ORDER BY created_at ASC, id ASC
                 LIMIT $2"
        ))
        .bind(&q.root_id)
        .bind(q.max_nodes + 1)
        .fetch_all(self.tx().await.pg())
        .await?;
        Ok(rows
            .iter()
            .map(|row| {
                let task_state: Option<String> = row.get("task_state");
                ui::NodeRow {
                    promise: row_to_promise(row),
                    task_state: task_state.as_deref().map(parse_task_state),
                    task_version: row.get::<i32, _>("task_version") as i64,
                    resumes: row.get::<i32, _>("resumes") as i64,
                    ttl: row.get("ttl"),
                    pid: row.get("pid"),
                    retry_timeout_at: row.get("retry_timeout_at"),
                    lease_timeout_at: row.get("lease_timeout_at"),
                }
            })
            .collect())
    }

    async fn ui_schedules_search(
        &self,
        q: &ui::SchedulesQuery,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let (expr, cmp, dir) = (q.sort.key.expr(), q.sort.dir.cmp_sql(), q.sort.dir.sql());
        let sql = format!(
            "SELECT id, cron, promise_id, promise_timeout,
                    NULLIF(promise_param_headers, '{{}}'::jsonb)::text AS promise_param_headers,
                    promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
             FROM schedules
             WHERE ($1::text IS NULL OR id >= $1)
               AND ($2::text IS NULL OR id < $2)
               AND ($3::jsonb IS NULL OR promise_tags @> $3::jsonb)
               AND ($4::bigint IS NULL OR ({expr}, id) {cmp} ($4::bigint, $5::text))
             ORDER BY {expr} {dir}, id {dir}
             LIMIT $6"
        );
        let (after_key, after_id) = split_keyset(q.after.as_ref());
        let rows = sqlx::query(&sql)
            .bind(q.id_from.as_deref())
            .bind(q.id_to.as_deref())
            .bind(q.tags_json.as_deref())
            .bind(after_key)
            .bind(after_id)
            .bind(q.limit + 1)
            .fetch_all(self.tx().await.pg())
            .await?;
        Ok(rows.iter().map(row_to_schedule).collect())
    }

    async fn ui_schedules_count(&self, q: &ui::SchedulesQuery) -> StorageResult<i64> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS n FROM schedules
                 WHERE ($1::text IS NULL OR id >= $1)
                   AND ($2::text IS NULL OR id < $2)
                   AND ($3::jsonb IS NULL OR promise_tags @> $3::jsonb)",
        )
        .bind(q.id_from.as_deref())
        .bind(q.id_to.as_deref())
        .bind(q.tags_json.as_deref())
        .fetch_one(self.tx().await.pg())
        .await?;
        Ok(row.get::<i64, _>("n"))
    }

    /// Plan this transaction's statements for the values they are bound to.
    ///
    /// The console's reads are the one place a generic plan is wrong: their
    /// `LIMIT` and keyset bounds are parameters, and planned blind Postgres
    /// sorts every execution to return a page of 25 rather than walking
    /// `idx_promises_roots` backwards for 25 entries (measured: 325 ms against
    /// 0.2 ms at 470k executions). One extra round trip, on a console request.
    async fn custom_plans(&self) -> StorageResult<()> {
        sqlx::query("SET LOCAL plan_cache_mode TO force_custom_plan")
            .execute(self.tx().await.pg())
            .await?;
        Ok(())
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
    async fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        let rows = sqlx::query(
            // Each branch is its own top-N over the index whose key is
            // `(deadline, id)` and whose predicate is the branch's WHERE,
            // so each reads at most `$1` index entries in order and the
            // outer sort merges four short lists. Without the inner
            // LIMITs Postgres may sort every armed row to keep `$1` of
            // them — the full read the sweep used to be.
            "SELECT deadline, kind, id, pid FROM (
                     (SELECT timeout_at AS deadline, 'promise' AS kind, id, NULL::text AS pid
                        FROM promises WHERE state = 'pending' AND external
                        ORDER BY timeout_at, id LIMIT $1)
                     UNION ALL
                     (SELECT retry_timeout_at, 'retry', id, NULL
                        FROM promises WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL
                        ORDER BY retry_timeout_at, id LIMIT $1)
                     UNION ALL
                     (SELECT lease_timeout_at, 'lease', id, pid
                        FROM promises WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL
                        ORDER BY lease_timeout_at, id LIMIT $1)
                     UNION ALL
                     (SELECT next_run_at, 'schedule', id, NULL FROM schedules
                        ORDER BY next_run_at, id LIMIT $1)
                 ) d
                 ORDER BY deadline ASC, id ASC
                 LIMIT $1",
        )
        .bind(limit as i64)
        .fetch_all(self.tx().await.pg())
        .await?;
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

    /// Schedules due at `time`: every one (`None`, the schedule index walked up
    /// to `time`), or the named ones through the primary key — two statements
    /// for the same reason `process_timeouts` has two.
    async fn get_expired_schedule_timeouts(
        &self,
        time: i64,
        only: Option<&[String]>,
    ) -> StorageResult<Vec<(String, i64)>> {
        let rows = match only {
            None => {
                sqlx::query(
                    "SELECT id, next_run_at FROM schedules WHERE next_run_at <= $1 ORDER BY id",
                )
                .bind(time)
                .fetch_all(self.tx().await.pg())
                .await?
            }
            Some(ids) => {
                sqlx::query(
                    "SELECT id, next_run_at FROM schedules
                     WHERE id = ANY($2) AND (next_run_at + 0) <= $1 ORDER BY id",
                )
                .bind(time)
                .bind(ids)
                .fetch_all(self.tx().await.pg())
                .await?
            }
        };
        Ok(rows
            .iter()
            .map(|r| (r.get::<String, _>("id"), r.get::<i64, _>("next_run_at")))
            .collect())
    }

    async fn process_schedule_timeout(
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
        let rows = sqlx::query(&format!("
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
                   EXISTS (SELECT 1 FROM inserted_or_skipped_promise) AS promise_created,
                   (SELECT computed_promise_id FROM schedule) AS computed_promise_id,
                   (SELECT already_timedout FROM schedule) AS promise_already_timedout,
                   {messages}
            FROM updated_schedule
        ", messages = emitted_json(&["emit_new"])))
            .bind(schedule_id).bind(fired_at).bind(next_run_at).bind(promise_tags_json).bind(time)
            .fetch_all(self.tx().await.pg()).await?;

        if rows.is_empty() {
            return Ok(None);
        }
        self.absorb_and_arm_retries(&rows[0], time + trt);
        // The schedule advanced, so its own deadline moved. The promise this
        // firing created has one too, if it is pending and external — task or
        // no task.
        let schedule = row_to_schedule(&rows[0]);
        let promise_created: bool = rows[0].get("promise_created");
        let promise_already_timedout: bool = rows[0].get("promise_already_timedout");
        if promise_created
            && !promise_already_timedout
            && resonate_core::types::is_external(promise_tags)
        {
            let computed_promise_id: String = rows[0].get("computed_promise_id");
            self.arm_promise_timeout(&computed_promise_id, fired_at + schedule.promise_timeout);
        }
        self.arm(
            schedule.next_run_at,
            Timeout::ScheduleDue {
                schedule_id: schedule_id.to_string(),
            },
        );
        Ok(Some(schedule))
    }

    async fn debug_reset(&self) -> StorageResult<()> {
        sqlx::query("TRUNCATE promises, schedules CASCADE")
            .execute(self.tx().await.pg())
            .await?;
        Ok(())
    }

    /// One of `process_timeouts`' statements: `$1` the time, `$2` the ids when
    /// the precise form names some.
    async fn fire_statement(
        &self,
        sql: &str,
        time: i64,
        ids: &Option<Vec<String>>,
    ) -> StorageResult<()> {
        // A named batch goes in chunks. A burst — a wheel's worth of leases
        // expiring after an outage — would otherwise be one statement holding
        // thousands of row locks, with per-row tests against arrays as long
        // as the batch.
        const CHUNK: usize = 256;
        let chunks: Vec<Option<&[String]>> = match ids {
            Some(ids) => ids.chunks(CHUNK).map(Some).collect(),
            None => vec![None],
        };
        for chunk in chunks {
            let q = sqlx::query(sql).bind(time);
            let q = match chunk {
                Some(ids) => q.bind(ids),
                None => q,
            };
            if let Some(row) = q.fetch_optional(self.tx().await.pg()).await? {
                self.absorb_and_arm_retries(&row, time + self.task_retry_timeout);
            }
        }
        Ok(())
    }

    /// Fire expired timeouts, either every one that is due or the ones named.
    ///
    /// The precise form's predicates never say `task_state = '...'` or
    /// `state = 'pending' AND external` plainly: those are the predicates of
    /// the deadline indexes, keyed `(deadline, id)`, and a statement that
    /// matches one may be planned through it — `id` its second column, so a
    /// walk of the whole index — instead of through the primary key. The
    /// `COALESCE` / `|| ''` spellings mean the same and match no index.
    ///
    /// Two forms of each statement, never one statement with a switch in it.
    /// `($2::text IS NULL OR id = $2)` reads like one statement, but once a
    /// prepared statement goes generic Postgres must plan it for both values
    /// at once, which means it cannot use the primary key: the precise form
    /// became a range scan over every overdue row of the queue. So the named
    /// form selects through `id = ANY($2)` — the primary key, one probe per
    /// id — and the deadline is `timeout_at + 0`, an expression no index
    /// carries, so the planner is never tempted into the deadline index
    /// instead. The full form is the deadline index walked up to `$1`, and
    /// only `debug.tick` asks for it: there is no background sweep.
    ///
    /// A batch of named timeouts is one statement per queue, not one
    /// transaction per timeout, so a burst of deadlines coming due together
    /// costs three round trips rather than three per deadline.
    async fn process_timeouts(&self, time: i64, only: Option<&[Timeout]>) -> StorageResult<()> {
        let trt = self.task_retry_timeout;
        // `None`: the full form. `Some(ids)`: the precise form over `ids`,
        // skipped when nothing of that kind was named.
        let selected = |kind: &str| -> Option<Option<Vec<String>>> {
            match only {
                None => Some(None),
                Some(ts) => {
                    let ids: Vec<String> = ts
                        .iter()
                        .filter(|t| t.kind() == kind)
                        .map(|t| t.id().to_string())
                        .collect();
                    (!ids.is_empty()).then_some(Some(ids))
                }
            }
        };

        // Statement 1: expired promises.
        //
        // `state = 'pending' AND external` is the whole of what
        // `promise_timeouts` held: rows entered on create and left on settle.
        // Every pending promise that is not internal is fired eagerly;
        // internal ones time out lazily, on read.
        if let Some(ids) = selected("promise") {
            let selection = match ids {
                None => "state = 'pending' AND external AND timeout_at <= $1",
                Some(_) => {
                    "id = ANY($2) AND (state || '') = 'pending' AND external AND (timeout_at + 0) <= $1"
                }
            };
            let key = if ids.is_some() {
                "expire_precise"
            } else {
                "expire_full"
            };
            let sql = self.cached(key, || expire_batch_sql(selection, "$1", trt));
            self.fire_statement(&sql, time, &ids).await?;
        }

        // Statement 2: expired task retry deadlines — re-enqueue the execute
        // message and push the deadline out.
        if let Some(ids) = selected("retry") {
            let selection = match ids {
                None => "task_state = 'pending' AND retry_timeout_at <= $1",
                Some(_) => {
                    "id = ANY($2) AND COALESCE(task_state, '') = 'pending' AND (retry_timeout_at + 0) <= $1"
                }
            };
            let key = if ids.is_some() {
                "retry_precise"
            } else {
                "retry_full"
            };
            let sql = self.cached(key, || {
                format!(
                    "
            WITH expired_retry AS (
              SELECT id, task_version, target FROM promises
              WHERE {selection}
              FOR UPDATE
            ),
            updated_retry AS (
              UPDATE promises SET retry_timeout_at = $1 + {trt}, pid = NULL
              WHERE id = ANY((SELECT COALESCE(array_agg(id), '{{}}') FROM expired_retry)::text[])
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
                )
            });
            self.fire_statement(&sql, time, &ids).await?;
        }

        // Statement 3: expired leases — the holder went away, hand the task back.
        if let Some(ids) = selected("lease") {
            let selection = match ids {
                None => "task_state = 'acquired' AND lease_timeout_at <= $1",
                Some(_) => {
                    "id = ANY($2) AND COALESCE(task_state, '') = 'acquired' AND (lease_timeout_at + 0) <= $1"
                }
            };
            let key = if ids.is_some() {
                "lease_precise"
            } else {
                "lease_full"
            };
            let sql = self.cached(key, || {
                format!(
                    "
            WITH expired_lease AS (
              SELECT id, task_version, target FROM promises
              WHERE {selection}
              FOR UPDATE
            ),
            released AS (
              UPDATE promises SET
                task_state = 'pending', retry_timeout_at = $1 + {trt},
                lease_timeout_at = NULL, ttl = NULL, pid = NULL
              WHERE id = ANY((SELECT COALESCE(array_agg(id), '{{}}') FROM expired_lease)::text[])
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
                )
            });
            self.fire_statement(&sql, time, &ids).await?;
        }

        Ok(())
    }

    // D-04: debug.snap — every section is now a projection of the one table
    async fn snap(&self) -> StorageResult<Snapshot> {
        let promise_rows = sqlx::query(&format!("SELECT {P_COLS} FROM promises ORDER BY id"))
            .fetch_all(self.tx().await.pg())
            .await?;
        let promises: Vec<PromiseRecord> = promise_rows.iter().map(row_to_promise).collect();

        let pt_rows = sqlx::query(
            "SELECT id, timeout_at FROM promises
                 WHERE state = 'pending' AND external ORDER BY id",
        )
        .fetch_all(self.tx().await.pg())
        .await?;
        let promise_timeouts: Vec<SnapshotPromiseTimeout> = pt_rows
            .iter()
            .map(|r| SnapshotPromiseTimeout {
                id: r.get("id"),
                timeout: r.get("timeout_at"),
            })
            .collect();

        // Non-ready callbacks only — the ready ones live in `resumes`.
        let cb_rows = sqlx::query(
            "SELECT aw AS awaiter_id, id AS awaited_id
                 FROM promises CROSS JOIN LATERAL unnest(callbacks) AS aw
                 ORDER BY aw, id",
        )
        .fetch_all(self.tx().await.pg())
        .await?;
        let callbacks: Vec<SnapshotCallback> = cb_rows
            .iter()
            .map(|r| SnapshotCallback {
                awaiter: r.get("awaiter_id"),
                awaited: r.get("awaited_id"),
            })
            .collect();

        let li_rows = sqlx::query(
            "SELECT id AS promise_id, l AS address
                 FROM promises CROSS JOIN LATERAL unnest(listeners) AS l
                 ORDER BY id, l",
        )
        .fetch_all(self.tx().await.pg())
        .await?;
        let listeners: Vec<SnapshotListener> = li_rows
            .iter()
            .map(|r| SnapshotListener {
                promise_id: r.get("promise_id"),
                address: r.get("address"),
            })
            .collect();

        let task_rows = sqlx::query(
            "SELECT id, task_state, task_version, ttl, pid, resumes
                 FROM promises WHERE task_state IS NOT NULL ORDER BY id",
        )
        .fetch_all(self.tx().await.pg())
        .await?;
        let tasks: Vec<TaskRecord> = task_rows.iter().map(row_to_task).collect();

        let tt_rows = sqlx::query(
            "SELECT id, 0 AS timeout_type, retry_timeout_at AS timeout_at FROM promises
                   WHERE task_state = 'pending' AND retry_timeout_at IS NOT NULL
                 UNION ALL
                 SELECT id, 1 AS timeout_type, lease_timeout_at AS timeout_at FROM promises
                   WHERE task_state = 'acquired' AND lease_timeout_at IS NOT NULL
                 ORDER BY id",
        )
        .fetch_all(self.tx().await.pg())
        .await?;
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
async fn process_all_timeouts(db: &PostgresDb<'_>, time: i64) -> StorageResult<usize> {
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time, None).await?;
    process_schedule_timeouts(db, time, None).await
}

/// Process expired schedule timeouts.
async fn process_schedule_timeouts(
    db: &PostgresDb<'_>,
    time: i64,
    only: Option<&[String]>,
) -> StorageResult<usize> {
    let expired = db.get_expired_schedule_timeouts(time, only).await?;
    let mut fired = 0usize;

    for (schedule_id, fired_at) in &expired {
        let schedule = match db.schedule_get(schedule_id).await? {
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
            .process_schedule_timeout(schedule_id, *fired_at, next_run_at, time, &promise_tags)
            .await?
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
impl Engine for PostgresEngine {
    async fn process(&self, input: Input<'_>, now: i64) -> Output {
        match input {
            Input::External(req) => self.dispatch(req, now).await,
            Input::Internal(timeout) => self.fire_all(vec![timeout], now).await,
        }
    }

    async fn fire(&self, timeouts: Vec<Timeout>, now: i64) -> Output {
        self.fire_all(timeouts, now).await
    }

    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        self.transact(move |db| Box::pin(async move { process_all_timeouts(db, now).await }))
            .await
    }

    async fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        self.query(move |db| Box::pin(async move { db.upcoming(limit).await }))
            .await
    }

    fn returns_messages(&self) -> bool {
        true
    }
}

impl PostgresEngine {
    /// Fire the timeouts the system asked of itself, all in one transaction:
    /// one statement per queue, whatever the batch holds.
    ///
    /// Atomic and idempotent as a batch, which is what each was alone: every
    /// statement re-checks the deadline against the row, so a timeout that
    /// has moved or settled is skipped, and a failed batch committed nothing
    /// and is found again by the timer's next refresh.
    async fn fire_all(&self, timeouts: Vec<Timeout>, now: i64) -> Output {
        if timeouts.is_empty() {
            return Output::default();
        }
        let swept = self
            .transact(move |db| {
                let timeouts = timeouts.clone();
                Box::pin(async move {
                    db.process_timeouts(now, Some(&timeouts)).await?;
                    let schedules: Vec<String> = timeouts
                        .iter()
                        .filter_map(|t| match t {
                            Timeout::ScheduleDue { schedule_id } => Some(schedule_id.clone()),
                            _ => None,
                        })
                        .collect();
                    if !schedules.is_empty() {
                        process_schedule_timeouts(db, now, Some(&schedules)).await?;
                    }
                    Ok(())
                })
            })
            .await;
        match swept {
            Ok(((), messages, timeouts)) => Output {
                response: None,
                messages,
                timeouts,
            },
            Err(e) => {
                tracing::error!(error = %e, "Firing timeouts failed; the timer's refresh finds them again");
                Output::default()
            }
        }
    }
}

// ─── The plugin ──────────────────────────────────────────────────────────────

use resonate_plugin::{ConfigError, ResonateServer, ServerDependencies, ServerPlugin, Settings};
use serde::{Deserialize, Serialize};

/// This server, as a plugin. The one thing a binary names to run on PostgreSQL.
pub static PLUGIN: ServerPlugin = ServerPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Everything under `[servers.server_postgres]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Connection URL. Required — there is no sensible default host.
    #[serde(default)]
    pub url: String,

    /// Connection pool size.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,

    /// Apply pending migrations to an existing database.
    ///
    /// An empty database is always created. Beyond that, a schema behind the
    /// binary is a deployment decision, not a startup default: without this the
    /// server refuses to start and names what is pending, rather than running
    /// DDL nobody asked for on a restart.
    #[serde(default)]
    pub migrate: bool,

    /// How long a pending task waits before it is redispatched (ms).
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: i64,

    /// The externally reachable URL, stamped into every emitted message so a
    /// worker knows where to call back.
    #[serde(default)]
    pub server_url: String,

    /// How many deadlines the in-memory timer holds. Bigger buys a longer
    /// horizon, not correctness — the sweep covers whatever falls outside.
    #[serde(default = "default_wheel_capacity")]
    pub wheel_capacity: usize,

    /// How often the timer re-reads the durable deadlines (ms), and the longest
    /// it will sleep. This is the staleness bound for a deadline another
    /// instance armed.
    #[serde(default = "default_wheel_refresh")]
    pub wheel_refresh: u64,
}

fn default_preload_limit() -> u32 {
    10
}
fn default_retry_timeout() -> i64 {
    30_000
}
fn default_wheel_capacity() -> usize {
    8192
}
fn default_wheel_refresh() -> u64 {
    30_000
}
fn default_pool_size() -> u32 {
    10
}

impl Default for Config {
    fn default() -> Self {
        Self {
            url: String::new(),
            pool_size: default_pool_size(),
            preload_limit: default_preload_limit(),
            migrate: false,
            retry_timeout: default_retry_timeout(),
            server_url: String::new(),
            wheel_capacity: default_wheel_capacity(),
            wheel_refresh: default_wheel_refresh(),
        }
    }
}

/// Read `[servers.server_postgres]` and build the server. Nothing is opened here — the
/// database is `init`'s, like every other port's resource.
fn configure(
    settings: &Settings<'_>,
    deps: ServerDependencies,
) -> Result<std::sync::Arc<dyn ResonateServer>, ConfigError> {
    let config: Config = settings.extract()?;
    if config.url.is_empty() {
        return Err(settings.reject("url", "a connection URL is required"));
    }

    let options = resonate_sql::server::Options {
        server_url: config.server_url.clone(),
        wheel_capacity: config.wheel_capacity,
        wheel_refresh: config.wheel_refresh,
        // No sweep. Every deadline is a row in an index ordered by it, and the
        // timer walks the front of those indexes every `wheel_refresh`: that
        // walk is the recovery path, for a restart, for another instance's
        // deadlines and for a wheel that overflowed.
        sweep_interval: 0,
    };
    let open = config.clone();
    Ok(resonate_sql::server::Server::new(
        Box::new(move |debug| {
            Box::pin(async move {
                let engine = PostgresEngine::connect(
                    &open.url,
                    open.pool_size,
                    open.retry_timeout,
                    open.preload_limit,
                    debug,
                )
                .await
                .map_err(|e| resonate_plugin::Unavailable::new(format!("cannot connect: {e}")))?;
                engine
                    .init(open.migrate)
                    .await
                    .map_err(|e| resonate_plugin::Unavailable::new(format!("schema: {e}")))?;
                Ok(std::sync::Arc::new(engine) as std::sync::Arc<dyn resonate_sql::engine::Engine>)
            })
        }),
        deps.router,
        options,
    ))
}
