//! Single-table PostgreSQL backend (experimental).
//!
//! The default Postgres backend (`persistence_postgres.rs`) spreads one logical
//! promise across eight tables: `promises`, `promise_timeouts`, `tasks`,
//! `task_timeouts`, `callbacks`, `listeners`, `outgoing_execute` and
//! `outgoing_unblock`. Every one of them is keyed by — or joins straight back
//! to — the promise id, so each user operation fans out into a CTE touching six
//! or seven tables, and every read-modify-write has to lock several rows in a
//! consistent order to stay correct under READ COMMITTED.
//!
//! Here a promise is one row. What is left beside it is an `outbox` (a message
//! is not a promise attribute — a settled promise can owe several of them) and
//! `schedules` (a separate id space, and a genuinely different entity).
//! `schedule_timeouts` is gone: its `(timeout_at, id)` was a verbatim copy of
//! `(next_run_at, id)`.
//!
//! Mapping from the multi-table schema:
//!
//! | multi-table                          | single-table                                      |
//! |--------------------------------------|---------------------------------------------------|
//! | `promises`                           | `id, state, param_*, value_*, tags, *_at`         |
//! | `promise_timeouts(timeout_at, id)`   | *derived*: `state='pending' AND target IS NOT NULL`|
//! | `tasks(id, state, version)`          | `task_state` (NULL ⟺ no task), `task_version`     |
//! | `task_timeouts` type 0 (retry)       | `retry_at`   (live ⟺ `task_state='pending'`)      |
//! | `task_timeouts` type 1 (lease)       | `expires_at` (live ⟺ `task_state='acquired'`), `ttl`, `pid` |
//! | `callbacks(awaited, awaiter, false)` | `callbacks TEXT[]` on the **awaited** row           |
//! | `callbacks(awaited, awaiter, true)`  | `resumes  TEXT[]` on the **awaiter** row           |
//! | `listeners(promise_id, address)`     | `listeners TEXT[]`                                 |
//! | `outgoing_execute(id, version, addr)`| `outbox` row, `key = 'e:'||task_id`                |
//! | `outgoing_unblock(promise_id, addr)` | `outbox` row, `key = 'u:'||promise_id||':'||addr`  |
//!
//! Column names, the `resonate` schema and the shape of `outbox` follow
//! `constraints-all.sql`, so that file applies to this schema unchanged. Set
//! `RESONATE_STORAGE__POSTGRES__CONSTRAINTS=true` (or call `init_with_constraints`)
//! to install the catalogue and have the database enforce it.
//!
//! # The one structural constraint the collapse imposes
//!
//! **Two CTEs in one statement may not update the same row.** In the
//! multi-table schema `fulfilled_task`, `deleted_ttimeout`, `deleted_callbacks`
//! and `updated_promise` are four independent CTEs that merely share an id;
//! here they must become one `UPDATE ... SET` with `CASE` expressions. The
//! same applies to fan-out: `marked_ready`, `resumed_tasks` and the awaiter-side
//! `deleted_callbacks` are merged into a single fan-out `UPDATE`, because an
//! await cycle (A awaits B, B awaits A) would otherwise have two CTEs collide
//! on one row — which Postgres leaves undefined. See `SETTLE_FANOUT`.

use super::{
    Db, OutgoingExecute, OutgoingUnblock, PromiseCreateParams, PromiseCreateResult,
    PromiseSettleParams, PromiseSettleResult, RegisterCallbackResult, ScheduleCreateParams,
    StorageError, StorageResult, TaskAcquireParams, TaskAcquireResult, TaskContinueResult,
    TaskCreateParams, TaskCreateResult, TaskFenceCreateParams, TaskFenceResult,
    TaskFenceSettleParams, TaskFulfillParams, TaskFulfillResult, TaskHaltResult, TaskReleaseResult,
    TaskSuspendResult,
};
use crate::core::types::{
    PromiseRecord, PromiseState, PromiseValue, ScheduleRecord, Snapshot, SnapshotCallback,
    SnapshotListener, SnapshotMessage, SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskRecord,
    TaskState,
};
use sqlx::{PgPool, Row};
use std::cell::UnsafeCell;

pub struct PostgresSingleStorage {
    pool: PgPool,
    task_retry_timeout: i64,
}

pub const CREATE_SCHEMA_SQL: &str = include_str!("../../config/postgres/single-table.sql");
pub const CONSTRAINTS_SQL: &str =
    include_str!("../../config/postgres/single-table-constraints.sql");

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

/// Arguments to `resonate._promise_json`, in declaration order.
const PROMISE_JSON_ARGS: &str = "id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at";

fn promise_json(alias: &str) -> String {
    let args: Vec<String> = PROMISE_JSON_ARGS
        .split(", ")
        .map(|c| format!("{}.{}", alias, c))
        .collect();
    format!("resonate._promise_json({})", args.join(", "))
}

impl PostgresSingleStorage {
    pub async fn connect(
        url: &str,
        pool_size: u32,
        task_retry_timeout: i64,
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
        })
    }

    pub async fn init(&self) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(CREATE_SCHEMA_SQL).execute(&self.pool).await?;
        Ok(())
    }

    /// Install the schema and then the constraint catalogue, so the database
    /// enforces every representable entry of the specification.
    ///
    /// `skip` names constraints to leave off — the escape hatch for exploring
    /// which catalogue entries the current server does not yet uphold: install
    /// everything, see what fires, add it to `skip`, repeat.
    pub async fn init_with_constraints(&self, skip: &[String]) -> Result<(), sqlx::Error> {
        self.init().await?;
        let sql = filter_constraints(CONSTRAINTS_SQL, skip);
        sqlx::raw_sql(&sql).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn transact<F, T>(&self, f: F, serializable: bool) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let max_retries: u32 = if serializable { 1 } else { 0 };

        let mut f = f;
        for attempt in 0..=max_retries {
            #[cfg(feature = "concurrency-stress")]
            tokio::task::yield_now().await;

            let mut tx = self.pool.begin().await.map_err(StorageError::from)?;
            if serializable {
                sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    .execute(tx.as_mut())
                    .await
                    .map_err(StorageError::from)?;
            }

            let task_retry_timeout = self.task_retry_timeout;
            let (result, tx) = tokio::task::block_in_place(|| {
                let db = PostgresSingleDb {
                    tx: UnsafeCell::new(tx),
                    task_retry_timeout,
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
                let tx = db.tx.into_inner();
                (result, tx)
            });

            let result = match result {
                Ok(v) => v,
                Err(StorageError::Serialization) => {
                    if attempt < max_retries {
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
                Ok(_) => return Ok(result),
                Err(e) => {
                    let pg_err = e
                        .as_database_error()
                        .and_then(|dbe| dbe.code().map(|c| c.to_string()));
                    if pg_err.as_deref() == Some("40001") || pg_err.as_deref() == Some("40P01") {
                        if attempt < max_retries {
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

    pub async fn query<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.transact(f, false).await
    }
}

/// Wraps a PostgreSQL transaction for use within the synchronous `Db` trait.
/// Same `UnsafeCell` rationale as `persistence_postgres::PostgresDb`.
struct PostgresSingleDb<'a> {
    tx: UnsafeCell<sqlx::Transaction<'a, sqlx::Postgres>>,
    task_retry_timeout: i64,
}

impl<'a> PostgresSingleDb<'a> {
    #[allow(clippy::mut_from_ref)]
    fn tx(&self) -> &mut sqlx::Transaction<'a, sqlx::Postgres> {
        unsafe { &mut *self.tx.get() }
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

/// Drop every statement that mentions one of the named constraints, so the rest
/// of the catalogue can still be installed. Statement-level, not line-level:
/// the catalogue is one `DROP IF EXISTS` plus one `ADD` per constraint.
fn filter_constraints(sql: &str, skip: &[String]) -> String {
    if skip.is_empty() {
        return sql.to_string();
    }
    sql.split(';')
        .filter(|stmt| {
            !skip
                .iter()
                .any(|name| stmt.contains(&format!("CONSTRAINT {}", name)))
        })
        .collect::<Vec<_>>()
        .join(";")
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
    retry_at   = CASE WHEN :FULFILLED THEN NULL ELSE p.retry_at END,
    expires_at = CASE WHEN :FULFILLED THEN NULL ELSE p.expires_at END,
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
    retry_at = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN :TIME + :TRT ELSE q.retry_at END,
    expires_at = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN NULL ELSE q.expires_at END,
    ttl = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN NULL ELSE q.ttl END,
    pid = CASE WHEN q.id = ANY(:AWAITERS) AND q.task_state = 'suspended'
                THEN NULL ELSE q.pid END
  WHERE q.id <> :AWAITED
    AND ( q.id = ANY(:AWAITERS)
          OR (:FULFILLED AND q.callbacks @> ARRAY[:AWAITED]) )
  RETURNING q.id
),
outbox_resume AS (
  INSERT INTO outbox (key, kind, address, task_id, version)
  SELECT 'e:' || s.id, 'execute', s.target, s.id, s.task_version
  FROM suspended_awaiters s WHERE s.target IS NOT NULL
  ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
  RETURNING key
)";

/// Queue one `unblock` message per listener of the row `:SRC` just settled.
/// `:SRC` must be a CTE with the post-settlement promise columns; `:LISTENERS`
/// a scalar subquery yielding the listener addresses as they were *before* the
/// settlement cleared them.
const SETTLE_UNBLOCK: &str = "
outbox_unblock AS (
  INSERT INTO outbox (key, kind, address, promise)
  SELECT 'u:' || u.id || ':' || l, 'unblock', l, :PROMISE_JSON
  FROM :SRC u CROSS JOIN LATERAL unnest(COALESCE(:LISTENERS, '{}')) AS l
  ON CONFLICT (key) DO NOTHING
  RETURNING key
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
outbox_unblock AS (
  INSERT INTO outbox (key, kind, address, promise)
  SELECT 'u:' || u.id || ':' || l, 'unblock', l, :PROMISE_JSON
  FROM updated_expired u
  JOIN expired e ON e.id = u.id
  CROSS JOIN LATERAL unnest(e.listeners) AS l
  ON CONFLICT (key) DO NOTHING
  RETURNING key
),
fanout AS (
  UPDATE promises q SET
    callbacks = (SELECT COALESCE(array_agg(b), '{}') FROM unnest(q.callbacks) b
                WHERE b NOT IN (SELECT id FROM fulfilled)),
    resumes = q.resumes || COALESCE((SELECT r.awaited_ids FROM ready_agg r WHERE r.awaiter = q.id), '{}'),
    task_state = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN 'pending' ELSE q.task_state END,
    retry_at = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN :TIME + :TRT ELSE q.retry_at END,
    expires_at = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN NULL ELSE q.expires_at END,
    ttl = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN NULL ELSE q.ttl END,
    pid = CASE WHEN q.task_state = 'suspended' AND EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
                   THEN NULL ELSE q.pid END
  WHERE q.id NOT IN (SELECT id FROM expired)
    AND ( EXISTS (SELECT 1 FROM ready_agg r WHERE r.awaiter = q.id)
          OR q.callbacks && (SELECT ids FROM fulfilled_ids) )
  RETURNING q.id
),
outbox_resume AS (
  INSERT INTO outbox (key, kind, address, task_id, version)
  SELECT 'e:' || s.id, 'execute', s.target, s.id, s.task_version
  FROM suspended_awaiters s WHERE s.target IS NOT NULL
  ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
  RETURNING key
)
SELECT 1",
        &[
            (":SELECTION", selection),
            (":SELF_SET", &self_set),
            (":PROMISE_JSON", &promise_json("u")),
            (":TIME", time_param),
            (":TRT", &trt.to_string()),
        ],
    )
}

// ============================================================================
// Db implementation — one row per promise
// ============================================================================

impl Db for PostgresSingleDb<'_> {
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
        rt_block_on(
            sqlx::query(&sql)
                .bind(&ids)
                .bind(time)
                .fetch_optional(self.tx().as_mut()),
        )?;
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
            SELECT 1"
        );
        rt_block_on(
            sqlx::query(&sql)
                .bind(promise_id)
                .bind(time)
                .fetch_optional(self.tx().as_mut()),
        )?;
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
        } = *params;
        let trt = self.task_retry_timeout;

        let rows = rt_block_on(sqlx::query(&format!("
            WITH inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at,
                                    task_state, task_version, retry_at)
              VALUES ($1, $2, COALESCE($3::jsonb, '{{}}'), $4, $5::jsonb, $6, $7, $8,
                      CASE WHEN $10::text IS NOT NULL
                           THEN (CASE WHEN $9 THEN 'fulfilled' ELSE 'pending' END) END,
                      0,
                      CASE WHEN $10::text IS NOT NULL AND NOT $9 THEN $7 + {trt} END)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            outbox_new AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || p.id, 'execute', $10::text, p.id, 0
              FROM inserted_or_skipped_promise p WHERE p.task_state = 'pending'
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            ),
            result AS (
              SELECT *, TRUE AS was_created FROM inserted_or_skipped_promise
              UNION ALL
              SELECT *, FALSE AS was_created FROM promises
              WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
            )
            SELECT {P_COLS}, was_created FROM result
        "))
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
        let was_created: bool = rows[0].get("was_created");
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
            SELECT {P_COLS}, was_settled FROM result
        "))
            .bind(id).bind(state).bind(value_headers).bind(value_data).bind(settled_at)
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(PromiseSettleResult {
                was_settled: false,
                promise: None,
            });
        }
        let row = &rows[0];
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
            -- link: awaited still pending, awaiter targeted and pending
            linked AS (
              UPDATE promises p SET callbacks = p.callbacks || $2
              WHERE p.id = $1
                AND NOT (p.callbacks @> ARRAY[$2])
                AND EXISTS (SELECT 1 FROM awaited WHERE state = 'pending')
                AND EXISTS (SELECT 1 FROM awaiter WHERE target IS NOT NULL AND state = 'pending')
              RETURNING p.id
            ),
            -- direct resume: awaited already settled. A suspended awaiter is
            -- woken; a pending/acquired one only records the ready callback.
            resumed AS (
              UPDATE promises p SET
                task_state = CASE WHEN p.task_state = 'suspended' THEN 'pending' ELSE p.task_state END,
                retry_at   = CASE WHEN p.task_state = 'suspended' THEN $3 + {trt} ELSE p.retry_at END,
                expires_at = CASE WHEN p.task_state = 'suspended' THEN NULL ELSE p.expires_at END,
                ttl        = CASE WHEN p.task_state = 'suspended' THEN NULL ELSE p.ttl END,
                pid        = CASE WHEN p.task_state = 'suspended' THEN NULL ELSE p.pid END,
                resumes    = CASE WHEN p.task_state IN ('pending', 'acquired')
                                    AND NOT (p.resumes @> ARRAY[$1])
                                  THEN p.resumes || $1 ELSE p.resumes END
              WHERE p.id = $2
                AND p.task_state IN ('pending', 'acquired', 'suspended')
                AND EXISTS (SELECT 1 FROM awaited WHERE state <> 'pending')
              RETURNING p.id, p.task_version, p.target,
                        (SELECT a.task_state FROM awaiter a) AS prev_task_state
            ),
            outbox_resume AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || r.id, 'execute', r.target, r.id, r.task_version
              FROM resumed r WHERE r.prev_task_state = 'suspended' AND r.target IS NOT NULL
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            )
            SELECT 'awaited' AS type, {awaited_cols} FROM awaited
            UNION ALL
            SELECT 'awaiter' AS type, {awaiter_cols} FROM awaiter
        ",
            awaited_cols = p_cols("awaited"),
            awaiter_cols = p_cols("awaiter"),
        ))
            .bind(awaited_id).bind(awaiter_id).bind(time)
            .fetch_all(self.tx().as_mut()))?;

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
            linked AS (
              UPDATE promises p SET listeners = p.listeners || $2
              WHERE p.id = $1
                AND NOT (p.listeners @> ARRAY[$2])
                AND EXISTS (SELECT 1 FROM locked_promise WHERE state = 'pending')
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
                                    task_state, task_version, expires_at, ttl, pid)
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
                expires_at = $3 + $4, ttl = $4, pid = $5, retry_at = NULL,
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
        Ok(TaskAcquireResult {
            promise: Some(row_to_promise(row)),
            was_acquired: row.get("was_acquired"),
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
                                    task_state, task_version, retry_at)
              SELECT $3, $4, COALESCE($5::jsonb, '{{}}'), $6, $7::jsonb, $8, $9, $10,
                     CASE WHEN $12::text IS NOT NULL
                          THEN (CASE WHEN $11::bool THEN 'fulfilled' ELSE 'pending' END) END,
                     0,
                     CASE WHEN $12::text IS NOT NULL AND NOT $11::bool THEN $9 + {trt} END
              WHERE (SELECT ok FROM fence_ok)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            outbox_new AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || p.id, 'execute', $12::text, p.id, 0
              FROM inserted_or_skipped_promise p WHERE p.task_state = 'pending'
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
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
              {cols}
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        ", cols = p_cols("r")))
            .bind(task_id).bind(version as i32)                                            // $1-$2
            .bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags)  // $3-$7
            .bind(timeout_at).bind(created_at).bind(settled_at)                             // $8-$10
            .bind(already_timedout).bind(address)                                           // $11-$12
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Err(StorageError::Serialization);
        }
        let row = &rows[0];
        let promise_id_val: Option<String> = row.get("id");
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
              {cols}
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        ", cols = p_cols("r")))
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

        rt_block_on(
            sqlx::query(
                "
            WITH task_data AS (
              SELECT unnest($1::text[]) AS id, unnest($2::int[]) AS version
            )
            UPDATE promises p SET expires_at = $3 + p.ttl
            FROM task_data td
            WHERE p.id = td.id AND p.task_version = td.version
              AND p.task_state = 'acquired' AND p.pid = $4
            -- TODO (carried over from the multi-table backend): also require the
            -- promise to be live, so a heartbeat on a task whose promise already
            -- timed out is a no-op:  AND p.state = 'pending' AND p.timeout_at > $3
        ",
            )
            .bind(&ids)
            .bind(&versions)
            .bind(time)
            .bind(pid)
            .execute(self.tx().as_mut()),
        )?;
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
              SELECT id, state FROM promises WHERE id = ANY($3) AND (SELECT ok FROM matched)
            ),
            missing AS (
              SELECT (COALESCE(array_length($3::text[], 1), 0) - COUNT(*)::INT) AS cnt FROM awaited
            ),
            can_suspend AS (
              SELECT 1 WHERE (SELECT ok FROM matched)
                AND (SELECT cnt FROM missing) = 0
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
                retry_at   = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.retry_at END,
                expires_at = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.expires_at END,
                ttl        = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.ttl END,
                pid        = CASE WHEN EXISTS (SELECT 1 FROM can_suspend) THEN NULL ELSE p.pid END,
                -- deleted_ready_callbacks: fires on a version match even when
                -- the suspend itself is refused because an awaited promise settled
                resumes    = CASE WHEN (SELECT ok FROM matched) AND (SELECT cnt FROM missing) = 0
                               THEN '{}' ELSE p.resumes END,
                callbacks   = CASE WHEN $1 = ANY($3) AND EXISTS (SELECT 1 FROM can_suspend)
                                    AND NOT (p.callbacks @> ARRAY[$1])
                               THEN p.callbacks || $1 ELSE p.callbacks END
              WHERE p.id = $1
                AND ((SELECT ok FROM matched) AND (SELECT cnt FROM missing) = 0)
              RETURNING p.id
            )
            SELECT
              (SELECT ok FROM matched) AS task_matched,
              EXISTS (SELECT 1 FROM can_suspend) AS was_suspended,
              (SELECT cnt FROM missing) AS missing_count
        ")
            .bind(task_id).bind(version as i32).bind(&awaited)
            .fetch_one(self.tx().as_mut()))?;

        Ok(TaskSuspendResult {
            task_matched: rows.get("task_matched"),
            was_suspended: rows.get("was_suspended"),
            missing_count: rows.get("missing_count"),
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
            "single-table task.fulfill assumes the task and its promise are one row"
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
              (SELECT b.task_state IS NOT NULL FROM before b) AS task_exists
            FROM result r
        ", cols = p_cols("r")))
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
                task_state = 'pending', retry_at = $3 + $4,
                expires_at = NULL, ttl = NULL, pid = NULL
              WHERE p.id = $1 AND p.task_version = $2 AND p.task_state = 'acquired'
              RETURNING p.id, p.task_version, p.target
            ),
            outbox_released AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || t.id, 'execute', t.target, t.id, t.task_version
              FROM released_task t WHERE t.target IS NOT NULL
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            )
            SELECT
              EXISTS (SELECT 1 FROM released_task) AS task_released,
              EXISTS (SELECT 1 FROM promises WHERE id = $1 AND task_state IS NOT NULL) AS task_exists
        ",
            )
            .bind(task_id)
            .bind(version as i32)
            .bind(time)
            .bind(ttl)
            .fetch_one(self.tx().as_mut()),
        )?;

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
                task_state = 'halted', retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
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
              SELECT id, task_state FROM promises WHERE id = $1 AND task_state IS NOT NULL FOR UPDATE
            ),
            continued_task AS (
              UPDATE promises p SET task_state = 'pending', retry_at = $2 + {trt}
              WHERE p.id = $1 AND p.task_state = 'halted'
              RETURNING p.id, p.task_version, p.target
            ),
            outbox_continued AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || t.id, 'execute', t.target, t.id, t.task_version
              FROM continued_task t WHERE t.target IS NOT NULL
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            )
            SELECT
              EXISTS (SELECT 1 FROM locked_task) AS task_exists,
              EXISTS (SELECT 1 FROM continued_task) AS continued
        "
            ))
            .bind(task_id)
            .bind(time)
            .fetch_one(self.tx().as_mut()),
        )?;

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
                 ORDER BY id ASC"
            ))
            .bind(promise_id)
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
                   promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
            FROM result
        ")
            .bind(id).bind(cron).bind(promise_id).bind(promise_timeout)
            .bind(promise_param_headers).bind(promise_param_data).bind(promise_tags)
            .bind(created_at).bind(next_run_at)
            .fetch_one(self.tx().as_mut()))?;

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

    fn get_expired_schedule_timeouts(&self, time: i64) -> StorageResult<Vec<(String, i64)>> {
        let rows = rt_block_on(
            sqlx::query("SELECT id, next_run_at FROM schedules WHERE next_run_at <= $1")
                .bind(time)
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
                                    task_state, task_version, retry_at)
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
            outbox_new AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || p.id, 'execute', s.address, p.id, 0
              FROM inserted_or_skipped_promise p, schedule s
              WHERE p.task_state = 'pending'
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            ),
            updated_schedule AS (
              UPDATE schedules SET last_run_at = $2, next_run_at = $3
              WHERE id = $1 AND next_run_at = $2
              RETURNING *
            )
            SELECT id, cron, promise_id, promise_timeout, NULLIF(promise_param_headers, '{{}}'::jsonb)::text AS promise_param_headers,
                   promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
            FROM updated_schedule
        "))
            .bind(schedule_id).bind(fired_at).bind(next_run_at).bind(promise_tags_json).bind(time)
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(row_to_schedule(&rows[0])))
    }

    fn ping(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql("SELECT 1").execute(self.tx().as_mut()))?;
        Ok(())
    }

    fn debug_reset(&self) -> StorageResult<()> {
        rt_block_on(
            sqlx::raw_sql("TRUNCATE outbox, promises, schedules CASCADE")
                .execute(self.tx().as_mut()),
        )?;
        Ok(())
    }

    // Timeout processing — three sequential statements, as in the multi-table
    // backend. Statement 1 is the same cascade as `try_timeout`, driven by the
    // sweep predicate instead of an explicit id list.
    fn process_timeouts(&self, time: i64) -> StorageResult<()> {
        let trt = self.task_retry_timeout;

        // Statement 1: expired promises.
        //
        // `state = 'pending' AND target IS NOT NULL` is the whole of what
        // promise_timeouts held: rows enter on create and leave on settle, and
        // only targeted promises are ever swept eagerly.
        let sql = expire_batch_sql(
            "state = 'pending' AND target IS NOT NULL AND timeout_at <= $1",
            "$1",
            trt,
        );
        rt_block_on(
            sqlx::query(&sql)
                .bind(time)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 2: expired task retry deadlines — re-enqueue the execute
        // message and push the deadline out.
        rt_block_on(
            sqlx::query(&format!(
                "
            WITH expired_retry AS (
              SELECT id, task_version, target FROM promises
              WHERE task_state = 'pending' AND retry_at IS NOT NULL AND retry_at <= $1
              FOR UPDATE
            ),
            updated_retry AS (
              UPDATE promises SET retry_at = $1 + {trt}, pid = NULL
              WHERE id IN (SELECT id FROM expired_retry)
              RETURNING id
            ),
            outbox_retry AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || e.id, 'execute', e.target, e.id, e.task_version
              FROM expired_retry e WHERE e.target IS NOT NULL
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            )
            SELECT 1
        "
            ))
            .bind(time)
            .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 3: expired leases — the holder went away, hand the task back.
        rt_block_on(
            sqlx::query(&format!(
                "
            WITH expired_lease AS (
              SELECT id, task_version, target FROM promises
              WHERE task_state = 'acquired' AND expires_at IS NOT NULL AND expires_at <= $1
              FOR UPDATE
            ),
            released AS (
              UPDATE promises SET
                task_state = 'pending', retry_at = $1 + {trt},
                expires_at = NULL, ttl = NULL, pid = NULL
              WHERE id IN (SELECT id FROM expired_lease)
              RETURNING id
            ),
            outbox_released AS (
              INSERT INTO outbox (key, kind, address, task_id, version)
              SELECT 'e:' || e.id, 'execute', e.target, e.id, e.task_version
              FROM expired_lease e WHERE e.target IS NOT NULL
              ON CONFLICT (key) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version
              RETURNING key
            )
            SELECT 1
        "
            ))
            .bind(time)
            .fetch_optional(self.tx().as_mut()),
        )?;

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
                "SELECT id, 0 AS timeout_type, retry_at AS timeout_at FROM promises
                   WHERE task_state = 'pending' AND retry_at IS NOT NULL
                 UNION ALL
                 SELECT id, 1 AS timeout_type, expires_at AS timeout_at FROM promises
                   WHERE task_state = 'acquired' AND expires_at IS NOT NULL
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

        let mut messages: Vec<SnapshotMessage> = Vec::new();
        let exec_rows = rt_block_on(
            sqlx::query(
                "SELECT task_id, version, address FROM outbox WHERE kind = 'execute' ORDER BY key",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        for r in &exec_rows {
            let id: String = r.get("task_id");
            let version: i32 = r.get("version");
            let address: String = r.get("address");
            messages.push(SnapshotMessage {
                address,
                message: serde_json::json!({
                    "kind": "execute", "head": {},
                    "data": { "task": { "id": id, "version": version } }
                }),
            });
        }

        let unblock_rows = rt_block_on(
            sqlx::query("SELECT address, promise FROM outbox WHERE kind = 'unblock' ORDER BY key")
                .fetch_all(self.tx().as_mut()),
        )?;
        for r in &unblock_rows {
            let address: String = r.get("address");
            let promise: serde_json::Value = r.get("promise");
            messages.push(SnapshotMessage {
                address,
                message: serde_json::json!({
                    "kind": "unblock", "head": {}, "data": { "promise": promise }
                }),
            });
        }

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

    fn take_outgoing(
        &self,
        batch_size: i64,
    ) -> StorageResult<(Vec<OutgoingExecute>, Vec<OutgoingUnblock>)> {
        let exec_rows = rt_block_on(
            sqlx::query(
                "WITH batch AS (
                   DELETE FROM outbox
                   WHERE key IN (SELECT key FROM outbox WHERE kind = 'execute' LIMIT $1)
                   RETURNING task_id, version, address
                 )
                 SELECT * FROM batch",
            )
            .bind(batch_size)
            .fetch_all(self.tx().as_mut()),
        )?;
        let execute_msgs: Vec<OutgoingExecute> = exec_rows
            .iter()
            .map(|r| OutgoingExecute {
                id: r.get("task_id"),
                version: r.get::<i32, _>("version") as i64,
                address: r.get("address"),
            })
            .collect();

        let unblock_rows = rt_block_on(
            sqlx::query(
                "WITH batch AS (
                   DELETE FROM outbox
                   WHERE key IN (SELECT key FROM outbox WHERE kind = 'unblock' LIMIT $1)
                   RETURNING address, promise
                 )
                 SELECT * FROM batch",
            )
            .bind(batch_size)
            .fetch_all(self.tx().as_mut()),
        )?;
        let unblock_msgs: Vec<OutgoingUnblock> = unblock_rows
            .iter()
            .map(|r| {
                let promise: serde_json::Value = r.get("promise");
                OutgoingUnblock {
                    address: r.get("address"),
                    promise: serde_json::from_value(promise)
                        .expect("outbox unblock payload is a PromiseRecord"),
                }
            })
            .collect();

        Ok((execute_msgs, unblock_msgs))
    }
}
