use super::{
    Db, OutgoingExecute, OutgoingUnblock, PromiseCreateParams, PromiseCreateResult,
    PromiseSettleParams, PromiseSettleResult, RegisterCallbackResult, ScheduleCreateParams,
    StorageError, StorageResult, TaskAcquireParams, TaskAcquireResult, TaskContinueResult,
    TaskCreateParams, TaskCreateResult, TaskFenceCreateParams, TaskFenceResult,
    TaskFenceSettleParams, TaskFulfillParams, TaskFulfillResult, TaskHaltResult, TaskReleaseResult,
    TaskSuspendResult,
};
use crate::types::{
    PromiseRecord, PromiseState, PromiseValue, ScheduleRecord, Snapshot, SnapshotCallback,
    SnapshotListener, SnapshotMessage, SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskRecord,
    TaskState,
};
use sqlx::{PgPool, Row};
use std::cell::UnsafeCell;

pub struct PostgresStorage {
    pool: PgPool,
    task_retry_timeout: i64,
}

const CREATE_SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS promises (
  id TEXT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout')),
  param_headers JSONB,
  param_data TEXT,
  value_headers JSONB,
  value_data TEXT,
  tags JSONB NOT NULL DEFAULT '{}',
  target TEXT GENERATED ALWAYS AS (tags->>'resonate:target') STORED,
  origin TEXT GENERATED ALWAYS AS (tags->>'resonate:origin') STORED,
  branch TEXT GENERATED ALWAYS AS (tags->>'resonate:branch') STORED,
  timer BOOLEAN NOT NULL GENERATED ALWAYS AS (COALESCE(tags->>'resonate:timer', '') = 'true') STORED,
  timeout_at BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  settled_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_promises_timeout_at ON promises (timeout_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_promises_target ON promises (target) WHERE target IS NOT NULL;

CREATE TABLE IF NOT EXISTS promise_timeouts (
  timeout_at BIGINT NOT NULL,
  id TEXT PRIMARY KEY REFERENCES promises(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_promise_timeouts_timeout_at_id ON promise_timeouts (timeout_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS callbacks (
  awaited_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  awaiter_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  ready BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (awaited_id, awaiter_id)
);
CREATE INDEX IF NOT EXISTS idx_callbacks_awaiter_id ON callbacks (awaiter_id);
CREATE INDEX IF NOT EXISTS idx_callbacks_ready ON callbacks (awaiter_id) WHERE ready = true;

CREATE TABLE IF NOT EXISTS listeners (
  promise_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  address TEXT NOT NULL,
  PRIMARY KEY (promise_id, address)
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY REFERENCES promises(id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled')),
  version INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS task_timeouts (
  timeout_at BIGINT NOT NULL,
  id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  timeout_type SMALLINT NOT NULL DEFAULT 0,
  process_id TEXT,
  ttl BIGINT NOT NULL DEFAULT 30000
);
CREATE INDEX IF NOT EXISTS idx_task_timeouts_timeout_at_id ON task_timeouts (timeout_at ASC, id ASC);
CREATE INDEX IF NOT EXISTS idx_task_timeouts_process_id ON task_timeouts (process_id) WHERE process_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_timeouts_timeout_type ON task_timeouts (timeout_type, timeout_at ASC);

CREATE TABLE IF NOT EXISTS outgoing_execute (
  id TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  version INT NOT NULL,
  address TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outgoing_unblock (
  promise_id TEXT NOT NULL REFERENCES promises(id) ON DELETE CASCADE,
  address TEXT NOT NULL,
  PRIMARY KEY (promise_id, address)
);

CREATE TABLE IF NOT EXISTS schedules (
  id TEXT PRIMARY KEY,
  cron TEXT NOT NULL,
  promise_id TEXT NOT NULL,
  promise_timeout BIGINT NOT NULL,
  promise_param_headers JSONB,
  promise_param_data TEXT,
  promise_tags JSONB NOT NULL DEFAULT '{}',
  created_at BIGINT NOT NULL,
  next_run_at BIGINT NOT NULL,
  last_run_at BIGINT
);

CREATE TABLE IF NOT EXISTS schedule_timeouts (
  timeout_at BIGINT NOT NULL,
  id TEXT PRIMARY KEY REFERENCES schedules(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_schedule_timeouts_timeout_at_id ON schedule_timeouts (timeout_at ASC, id ASC);
";

impl PostgresStorage {
    pub async fn connect(
        url: &str,
        pool_size: u32,
        task_retry_timeout: i64,
    ) -> Result<Self, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(pool_size)
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

    pub async fn transact<F, T>(&self, f: F, serializable: bool) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        // On serialization failure (40001), retry once immediately.
        // If the retry also fails, return Serialization error (maps to 503).
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
                let db = PostgresDb {
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

            // If business logic failed with a serialization error, retry.
            // Other errors propagate immediately (tx is dropped → auto-rollback).
            let result = match result {
                Ok(v) => v,
                Err(StorageError::Serialization) => {
                    if attempt < max_retries {
                        tracing::warn!(
                            attempt = attempt + 1,
                            "Serialization failure (40001) in query, retrying"
                        );
                        continue;
                    } else {
                        tracing::warn!(
                            "Serialization failure (40001) in query after retry, returning 503"
                        );
                        return Err(StorageError::Serialization);
                    }
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
                            tracing::warn!(
                                attempt = attempt + 1,
                                "Serialization failure (40001) at commit, retrying"
                            );
                            continue;
                        } else {
                            tracing::warn!(
                                "Serialization failure (40001) at commit after retry, returning 503"
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

    pub async fn query<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.transact(f, false).await
    }
}

/// Wraps a PostgreSQL transaction for use within the synchronous `Db` trait.
///
/// Uses `UnsafeCell` for interior mutability: `Db` trait methods take `&self`,
/// but `sqlx` requires `&mut Transaction` for query execution. This is safe because:
/// - `PostgresDb` is created and dropped within a single `block_in_place` call
/// - Only one `&PostgresDb` reference exists at a time (no aliasing)
/// - The `UnsafeCell` is never accessed concurrently
struct PostgresDb<'a> {
    tx: UnsafeCell<sqlx::Transaction<'a, sqlx::Postgres>>,
    task_retry_timeout: i64,
}

impl<'a> PostgresDb<'a> {
    /// Returns a mutable reference to the underlying transaction.
    ///
    /// # Safety
    /// Safe because `PostgresDb` is only used within a single synchronous closure
    /// in `transact()` — there is no concurrent or aliased access to the `UnsafeCell`.
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

fn row_to_promise_indexed(row: &sqlx::postgres::PgRow, offset: usize) -> PromiseRecord {
    let param_headers: Option<String> = row.get(offset + 2);
    let value_headers: Option<String> = row.get(offset + 4);
    let tags_str: String = row.get(offset + 6);
    let state_str: String = row.get(offset + 1);

    PromiseRecord {
        id: row.get(offset),
        state: parse_promise_state(&state_str),
        param: PromiseValue {
            headers: param_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: row.get(offset + 3),
        },
        value: PromiseValue {
            headers: value_headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: row.get(offset + 5),
        },
        tags: serde_json::from_str(&tags_str).unwrap_or_default(),
        timeout_at: row.get(offset + 7),
        created_at: row.get(offset + 8),
        settled_at: row.get(offset + 9),
    }
}

// ============================================================================
// Db implementation — single CTE query per operation
// ============================================================================

impl Db for PostgresDb<'_> {
    fn task_retry_timeout(&self) -> i64 {
        self.task_retry_timeout
    }

    // tryTimeout.sql — ghost operation before every user operation
    fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let trt = self.task_retry_timeout;

        rt_block_on(sqlx::query(&format!("
            WITH expired AS (
              UPDATE promises p
              SET state = CASE WHEN p.timer THEN 'resolved' ELSE 'rejected_timedout' END,
                  settled_at = p.timeout_at
              WHERE p.id = ANY($1) AND p.state = 'pending' AND p.timeout_at <= $2
              RETURNING *
            ),
            -- @snippet deleted_ptimeout(id=expired.id)
            deleted_ptimeout AS (
              DELETE FROM promise_timeouts WHERE id IN (SELECT id FROM expired) RETURNING id
            ),
            -- @cluster SettlementEnqueued
            -- @snippet fulfilled_task(id=expired.id)
            fulfilled_task AS (
              UPDATE tasks SET state = 'fulfilled'
              WHERE id IN (SELECT id FROM expired) AND state != 'fulfilled'
              RETURNING id
            ),
            -- @snippet deleted_ttimeout(id=fulfilled_task.id)
            deleted_ttimeout AS (
              DELETE FROM task_timeouts WHERE id IN (SELECT id FROM fulfilled_task) RETURNING id
            ),
            -- @snippet deleted_callbacks(awaiter_id=fulfilled_task.id)
            deleted_callbacks AS (
              DELETE FROM callbacks WHERE awaiter_id IN (SELECT id FROM fulfilled_task) RETURNING awaited_id
            ),
            -- @cluster ResumptionEnqueued
            -- @snippet marked_ready(awaited_id=expired.id, exclusion=fulfilled_task)
            marked_ready AS (
              UPDATE callbacks SET ready = true
              WHERE awaited_id IN (SELECT id FROM expired)
                AND awaiter_id NOT IN (SELECT id FROM fulfilled_task)
              RETURNING awaiter_id
            ),
            -- @snippet resumed_tasks(source=marked_ready, exclusion=fulfilled_task)
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT awaiter_id FROM marked_ready)
                AND state = 'suspended'
                AND id NOT IN (SELECT id FROM fulfilled_task)
              RETURNING id, version
            ),
            -- @snippet inserted_or_updated_ttimeout_resumed(time_base=$time)
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($2 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_resumed()
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            -- @cluster ListenerUnblocked
            -- @snippet inserted_outgoing_unblock(id=expired.id)
            inserted_outgoing_unblock AS (
              INSERT INTO outgoing_unblock (promise_id, address)
              SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id IN (SELECT id FROM expired)
              RETURNING promise_id
            ),
            -- @snippet deleted_listeners(promise_id=expired.id)
            deleted_listeners AS (
              DELETE FROM listeners WHERE promise_id IN (SELECT id FROM expired) RETURNING promise_id
            )
            SELECT 1
        ")).bind(&ids).bind(time).fetch_optional(self.tx().as_mut()))?;
        Ok(())
    }

    // Process settlement chain for a promise ID.
    // Fires any pending callbacks (marks ready, resumes suspended tasks).
    // This is a separate statement so it gets a fresh READ COMMITTED
    // snapshot, seeing all callbacks committed by concurrent transactions.
    fn process_callbacks(&self, promise_id: &str, time: i64) -> StorageResult<()> {
        let trt = self.task_retry_timeout;
        rt_block_on(sqlx::query(&format!("
            WITH settled_promise AS (
              SELECT id FROM promises WHERE id = $1 AND state != 'pending'
            ),
            marked_ready AS (
              UPDATE callbacks SET ready = true
              WHERE awaited_id = $1 AND EXISTS (SELECT 1 FROM settled_promise)
              RETURNING awaiter_id
            ),
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT awaiter_id FROM marked_ready)
                AND state = 'suspended'
              RETURNING id, version
            ),
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($2 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            )
            SELECT 1
        ")).bind(promise_id).bind(time).fetch_optional(self.tx().as_mut()))?;
        Ok(())
    }

    // Lock preamble: acquire FOR UPDATE locks on promise and task rows.
    // Returns (promise_exists, task_exists).
    fn lock_for_update(&self, id: &str) -> StorageResult<(bool, bool)> {
        let p = rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let t = rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        Ok((p.is_some(), t.is_some()))
    }

    // P-01: promise.get
    fn promise_get(&self, id: &str) -> StorageResult<Option<PromiseRecord>> {
        let row = rt_block_on(
            sqlx::query("SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at FROM promises WHERE id = $1")
                .bind(id).fetch_optional(self.tx().as_mut())
        )?;
        Ok(row.as_ref().map(row_to_promise))
    }

    // P-02: promise.create — single CTE
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
            -- @snippet inserted_or_skipped_promise(id=$id)
            WITH inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
              VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6, $7, $8)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            -- @snippet inserted_or_skipped_ptimeout(id=$id, guard=inserted_or_skipped_promise)
            inserted_or_skipped_ptimeout AS (
              INSERT INTO promise_timeouts (timeout_at, id)
              SELECT $6, $1
              WHERE NOT $9 AND EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            -- @cluster TaskInfraCreated
            -- @snippet inserted_or_skipped_task(id=$id, guard=inserted_or_skipped_promise)
            inserted_or_skipped_task AS (
              INSERT INTO tasks (id, state)
              SELECT $1, CASE WHEN $9 THEN 'fulfilled' ELSE 'pending' END
              WHERE $10::text IS NOT NULL AND EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            -- @snippet inserted_or_skipped_ttimeout_new(time_base=$created_at, id=$id)
            inserted_or_skipped_ttimeout_new AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($7 + {trt}), $1, 0, {trt}
              WHERE NOT $9 AND EXISTS (SELECT 1 FROM inserted_or_skipped_task)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_new(id=$id, address=$address)
            inserted_or_updated_outgoing_new AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT $1, 0, $10::text
              WHERE NOT $9 AND EXISTS (SELECT 1 FROM inserted_or_skipped_task)
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            result AS (
              SELECT *, TRUE AS was_created FROM inserted_or_skipped_promise
              UNION ALL
              SELECT *, FALSE AS was_created FROM promises WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
            )
            SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at, was_created FROM result
        "))
            .bind(id).bind(state).bind(param_headers).bind(param_data).bind(tags)       // $1-$5
            .bind(timeout_at).bind(created_at).bind(settled_at)                           // $6-$8
            .bind(already_timedout).bind(address)                                         // $9-$10
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            // CTE snapshot race: a concurrent INSERT committed after our snapshot;
            // the UNION ALL fallback SELECT saw neither the insert nor the existing row.
            // Nothing was committed — signal the caller to retry.
            return Err(StorageError::Serialization);
        }
        let was_created: bool = rows[0].get("was_created");
        Ok(PromiseCreateResult {
            was_created,
            promise: row_to_promise(&rows[0]),
        })
    }

    // P-03: promise.settle — lock preamble + CTE
    // Under READ COMMITTED, the FOR UPDATE lock is acquired in a separate statement
    // so that the settlement CTE gets a fresh snapshot that includes any callbacks
    // inserted by concurrent transactions (e.g. task.suspend) that were waiting on this lock.
    fn promise_settle(&self, params: &PromiseSettleParams) -> StorageResult<PromiseSettleResult> {
        let PromiseSettleParams {
            id,
            state,
            value_headers,
            value_data,
            settled_at,
        } = *params;
        let trt = self.task_retry_timeout;

        // Statement 1: acquire lock — blocks until concurrent task.suspend etc. finish
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = $1 FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 2: fresh snapshot — sees all callbacks committed before this point
        let rows = rt_block_on(sqlx::query(&format!("
            -- @snippet updated_promise(id=$id, guard=locked_promise)
            WITH updated_promise AS (
              UPDATE promises p
              SET state = $2, value_headers = $3::jsonb, value_data = $4, settled_at = $5
              WHERE p.id = $1 AND p.state = 'pending'
              RETURNING p.*
            ),
            -- @snippet deleted_ptimeout(id=$id, guard=updated_promise)
            deleted_ptimeout AS (
              DELETE FROM promise_timeouts WHERE id = $1 AND EXISTS (SELECT 1 FROM updated_promise) RETURNING id
            ),
            -- @cluster SettlementEnqueued
            -- @snippet fulfilled_task(id=$id, guard=updated_promise)
            fulfilled_task AS (
              UPDATE tasks SET state = 'fulfilled'
              WHERE id = $1 AND state != 'fulfilled' AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING id
            ),
            -- @snippet deleted_ttimeout(id=$id, guard=fulfilled_task)
            deleted_ttimeout AS (
              DELETE FROM task_timeouts WHERE id = $1 AND EXISTS (SELECT 1 FROM fulfilled_task) RETURNING id
            ),
            -- @snippet deleted_callbacks(awaiter_id=$id, guard=fulfilled_task)
            deleted_callbacks AS (
              DELETE FROM callbacks WHERE awaiter_id = $1 AND EXISTS (SELECT 1 FROM fulfilled_task) RETURNING awaited_id
            ),
            -- @cluster ResumptionEnqueued
            -- @snippet marked_ready(awaited_id=$id, guard=updated_promise)
            marked_ready AS (
              UPDATE callbacks SET ready = true
              WHERE awaited_id = $1 AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING awaiter_id
            ),
            -- @snippet resumed_tasks(source=marked_ready)
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT awaiter_id FROM marked_ready) AND state = 'suspended'
              RETURNING id, version
            ),
            -- @snippet inserted_or_updated_ttimeout_resumed(time_base=$settled_at)
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($5 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_resumed()
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            -- @cluster ListenerUnblocked
            -- @snippet inserted_outgoing_unblock(id=$id, guard=updated_promise)
            inserted_outgoing_unblock AS (
              INSERT INTO outgoing_unblock (promise_id, address)
              SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id = $1 AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING promise_id
            ),
            -- @snippet deleted_listeners(promise_id=$id, guard=updated_promise)
            deleted_listeners AS (
              DELETE FROM listeners WHERE promise_id = $1 AND EXISTS (SELECT 1 FROM updated_promise) RETURNING promise_id
            ),
            result AS (
              SELECT *, true AS was_settled FROM updated_promise
              UNION ALL
              SELECT *, false AS was_settled FROM promises WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at, was_settled FROM result
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
        let was_settled: bool = row.get("was_settled");
        Ok(PromiseSettleResult {
            was_settled,
            promise: Some(row_to_promise(row)),
        })
    }

    // P-04: promise.register_callback — single CTE
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
            inserted_or_skipped_callback AS (
              INSERT INTO callbacks (awaited_id, awaiter_id)
              SELECT $1, $2
              WHERE EXISTS (SELECT 1 FROM awaited WHERE state = 'pending')
                AND EXISTS (SELECT 1 FROM awaiter WHERE target IS NOT NULL AND state = 'pending')
              ON CONFLICT (awaited_id, awaiter_id) DO NOTHING
              RETURNING awaited_id
            ),
            -- @cluster ResumptionEnqueued (variant: direct resume, no marked_ready)
            -- @snippet resumed_tasks(source=direct, condition=awaited.state != 'pending')
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id = $2 AND state = 'suspended'
                AND EXISTS (SELECT 1 FROM awaited WHERE state != 'pending')
              RETURNING id, version
            ),
            -- @snippet inserted_or_updated_ttimeout_resumed(time_base=$time)
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($3 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_resumed()
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            )
            SELECT 'awaited' AS type, id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at FROM awaited
            UNION ALL
            SELECT 'awaiter' AS type, id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at FROM awaiter
        "))
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

    // P-05: promise.register_listener — single CTE
    fn promise_register_listener(
        &self,
        awaited_id: &str,
        address: &str,
    ) -> StorageResult<Option<PromiseRecord>> {
        let rows = rt_block_on(sqlx::query("
            -- @snippet locked_promise(id=$awaited_id)
            WITH locked_promise AS (
              SELECT * FROM promises WHERE id = $1 FOR UPDATE
            ),
            -- @snippet inserted_or_skipped_listener(promise_id=$awaited_id, address=$address, guard=locked_promise)
            inserted_or_skipped_listener AS (
              INSERT INTO listeners (promise_id, address)
              SELECT $1, $2
              WHERE EXISTS (SELECT 1 FROM locked_promise WHERE state = 'pending')
              ON CONFLICT (promise_id, address) DO NOTHING
              RETURNING promise_id
            )
            SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at FROM locked_promise
        ")
            .bind(awaited_id).bind(address)
            .fetch_all(self.tx().as_mut()))?;

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
            sqlx::query("SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at
                         FROM promises WHERE ($1::text IS NULL OR state = $1) AND ($2::jsonb IS NULL OR tags @> $2::jsonb) AND ($3::text IS NULL OR id > $3) ORDER BY id ASC LIMIT $4")
                .bind(state).bind(tags).bind(cursor).bind(limit).fetch_all(self.tx().as_mut())
        )?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    // T-01: task.get — single query with resumes subquery
    fn task_get(&self, id: &str) -> StorageResult<Option<TaskRecord>> {
        let row = rt_block_on(sqlx::query("
            SELECT t.id, t.state, t.version,
              CASE WHEN tt.timeout_type = 1 THEN tt.ttl ELSE NULL END AS ttl,
              CASE WHEN tt.timeout_type = 1 THEN tt.process_id ELSE NULL END AS pid,
              COALESCE(
                (SELECT COUNT(*)::INT FROM callbacks c WHERE c.awaiter_id = t.id AND c.ready = true),
                0
              ) AS resumes
            FROM tasks t LEFT JOIN task_timeouts tt ON tt.id = t.id WHERE t.id = $1
        ").bind(id).fetch_optional(self.tx().as_mut()))?;

        match row {
            Some(r) => {
                let resumes: i32 = r.get("resumes");
                Ok(Some(TaskRecord {
                    id: r.get("id"),
                    state: parse_task_state(&r.get::<String, _>("state")),
                    version: r.get::<i32, _>("version") as i64,
                    resumes: resumes as i64,
                    ttl: r.get("ttl"),
                    pid: r.get("pid"),
                }))
            }
            None => Ok(None),
        }
    }

    // T-02: task.create — single CTE
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

        // Statement 1: Promise creation CTE — inserts promise + task (if new),
        // and sets up the lease timeout atomically within the CTE.
        let rows = rt_block_on(sqlx::query("
            WITH inserted_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
              VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6, $7, $8)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            inserted_ptimeout AS (
              INSERT INTO promise_timeouts (timeout_at, id)
              SELECT $6, $1
              WHERE NOT $9 AND EXISTS (SELECT 1 FROM inserted_promise)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            inserted_task AS (
              INSERT INTO tasks (id, state, version)
              SELECT $1, $12, CASE WHEN $12 = 'acquired' THEN 1 ELSE 0 END
              WHERE EXISTS (SELECT 1 FROM inserted_promise)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            inserted_ttimeout AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, process_id, ttl)
              SELECT ($7 + $10), $1, 1, $11, $10
              WHERE NOT $9 AND EXISTS (SELECT 1 FROM inserted_task)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            promise AS (
              SELECT * FROM inserted_promise
              UNION ALL
              SELECT * FROM promises WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_promise)
            )
            SELECT
              p.id, p.state, p.param_headers::text, p.param_data, p.value_headers::text, p.value_data, p.tags::text, p.timeout_at, p.created_at, p.settled_at,
              EXISTS (SELECT 1 FROM inserted_task) AS task_created,
              COALESCE((SELECT state FROM inserted_task), t.state)    AS task_state,
              COALESCE((SELECT version FROM inserted_task), t.version) AS task_version
            FROM promise p
            LEFT JOIN tasks t ON t.id = p.id
        ")
            .bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags) // $1-$5
            .bind(timeout_at).bind(created_at).bind(settled_at)                             // $6-$8
            .bind(already_timedout).bind(ttl).bind(pid).bind(task_initial_state)             // $9-$12
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            // CTE snapshot race: a concurrent INSERT committed after our snapshot;
            // the UNION ALL fallback SELECT saw neither the insert nor the existing row.
            // Nothing was committed — signal the caller to retry.
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

        // Promise already existed, task not created.
        // Acquire is done by the handler as a separate statement (fresh snapshot).
        Ok(TaskCreateResult {
            promise,
            task_created: false,
            task_state: row.try_get::<String, _>("task_state").ok(),
            task_version: row.try_get::<i32, _>("task_version").ok().map(|v| v as i64),
        })
    }

    // T-03: task.acquire — single CTE
    fn task_acquire(&self, params: &TaskAcquireParams) -> StorageResult<TaskAcquireResult> {
        let TaskAcquireParams {
            task_id,
            version,
            time,
            ttl,
            pid,
        } = *params;
        let rows = rt_block_on(sqlx::query("
            WITH acquired_task AS (
              UPDATE tasks SET state = 'acquired', version = version + 1
              WHERE id = $1 AND version = $2 AND state = 'pending'
              RETURNING *
            ),
            inserted_or_updated_ttimeout_acquire AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, process_id, ttl)
              SELECT ($3 + $4), $1, 1, $5, $4
              WHERE EXISTS (SELECT 1 FROM acquired_task)
              ON CONFLICT (id) DO UPDATE SET timeout_at = ($3 + $4), timeout_type = 1, process_id = $5, ttl = $4
              RETURNING id
            ),
            deleted_ready_callbacks AS (
              DELETE FROM callbacks
              WHERE awaiter_id = $1 AND ready = true AND EXISTS (SELECT 1 FROM acquired_task)
              RETURNING *
            ),
            invoked AS (
              SELECT p.*,
                COALESCE(at.state, t.state)     AS task_state,
                COALESCE(at.version, t.version) AS task_version,
                (at.id IS NOT NULL)             AS was_acquired
              FROM promises p
              JOIN tasks t ON t.id = p.id
              LEFT JOIN acquired_task at ON at.id = p.id
              WHERE p.id = $1
            )
            SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at, task_state, task_version, was_acquired FROM invoked
        ")
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
        let was_acquired: bool = row.get("was_acquired");
        let task_state: String = row.get("task_state");
        let task_version: i64 = row.get::<i32, _>("task_version") as i64;
        Ok(TaskAcquireResult {
            promise: Some(row_to_promise(row)),
            was_acquired,
            task_state: Some(parse_task_state(&task_state)),
            task_version: Some(task_version),
        })
    }

    // T-04: task.fence (create variant) — single CTE
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
            -- @cluster FenceGuarded
            -- @snippet fence_guarded(task_id=$task_id, version=$version)
            WITH fence_check AS (
              SELECT id, state, version FROM tasks WHERE id = $1
            ),
            fence_ok AS (
              SELECT EXISTS (SELECT 1 FROM fence_check WHERE state = 'acquired' AND version = $2) AS ok
            ),
            -- @snippet inserted_or_skipped_promise(id=$promise_id, guard=fence_ok)
            inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
              SELECT $3, $4, $5::jsonb, $6, $7::jsonb, $8, $9, $10
              WHERE (SELECT ok FROM fence_ok)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            -- @snippet inserted_or_skipped_ptimeout(id=$promise_id, guard=inserted_or_skipped_promise)
            inserted_or_skipped_ptimeout AS (
              INSERT INTO promise_timeouts (timeout_at, id)
              SELECT $8, $3
              WHERE NOT $11 AND EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            -- @cluster TaskInfraCreated
            -- @snippet inserted_or_skipped_task(id=$promise_id, guard=inserted_or_skipped_promise)
            inserted_or_skipped_task AS (
              INSERT INTO tasks (id, state)
              SELECT $3, CASE WHEN $11::bool THEN 'fulfilled' ELSE 'pending' END
              WHERE $12::text IS NOT NULL AND EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            -- @snippet inserted_or_skipped_ttimeout_new(time_base=$created_at, id=$promise_id)
            inserted_or_skipped_ttimeout_new AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($9 + {trt}), $3, 0, {trt}
              WHERE NOT $11::bool AND EXISTS (SELECT 1 FROM inserted_or_skipped_task)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_new(id=$promise_id, address=$address)
            inserted_or_updated_outgoing_new AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT $3, 0, $12::text
              WHERE NOT $11::bool AND EXISTS (SELECT 1 FROM inserted_or_skipped_task)
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            result AS (
              SELECT * FROM inserted_or_skipped_promise
              UNION ALL
              SELECT * FROM promises WHERE id = $3 AND (SELECT ok FROM fence_ok) AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_promise)
            )
            SELECT
              EXISTS (SELECT 1 FROM fence_check) AS task_exists,
              (SELECT ok FROM fence_ok) AS fence_ok,
              r.id, r.state, r.param_headers::text, r.param_data, r.value_headers::text, r.value_data, r.tags::text, r.timeout_at, r.created_at, r.settled_at
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        "))
            .bind(task_id).bind(version as i32)                                              // $1-$2
            .bind(promise_id).bind(state).bind(param_headers).bind(param_data).bind(tags)    // $3-$7
            .bind(timeout_at).bind(created_at).bind(settled_at)                               // $8-$10
            .bind(already_timedout).bind(address)                                             // $11-$12
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Err(StorageError::Serialization);
        }
        let row = &rows[0];
        let task_exists: bool = row.get("task_exists");
        let fence_ok: bool = row.get("fence_ok");
        let promise_id_val: Option<String> = row.get("id");
        let promise = promise_id_val.map(|_| row_to_promise(row));

        Ok(TaskFenceResult {
            task_exists,
            fence_ok,
            promise,
        })
    }

    // T-04: task.fence (settle variant) — single CTE
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
        let trt = self.task_retry_timeout;

        let rows = rt_block_on(sqlx::query(&format!("
            -- @cluster FenceGuarded
            -- @snippet fence_guarded(task_id=$task_id, version=$version)
            WITH fence_check AS (
              SELECT id, state, version FROM tasks WHERE id = $1
            ),
            fence_ok AS (
              SELECT EXISTS (SELECT 1 FROM fence_check WHERE state = 'acquired' AND version = $2) AS ok
            ),
            -- @snippet locked_promise(id=$promise_id, guard=fence_ok)
            locked_promise AS (
              SELECT * FROM promises WHERE id = $3 AND (SELECT ok FROM fence_ok) FOR UPDATE
            ),
            -- @snippet updated_promise(id=$promise_id, guard=locked_promise+fence_ok)
            updated_promise AS (
              UPDATE promises p
              SET state = $4, value_headers = $5::jsonb, value_data = $6, settled_at = $7
              FROM locked_promise
              WHERE p.id = $3 AND p.state = 'pending' AND (SELECT ok FROM fence_ok)
              RETURNING p.*
            ),
            -- @snippet deleted_ptimeout(id=$promise_id, guard=updated_promise)
            deleted_ptimeout AS (
              DELETE FROM promise_timeouts WHERE id = $3 AND EXISTS (SELECT 1 FROM updated_promise) RETURNING id
            ),
            -- @cluster SettlementEnqueued
            -- @snippet fulfilled_task(id=$promise_id, guard=updated_promise)
            fulfilled_task AS (
              UPDATE tasks SET state = 'fulfilled'
              WHERE id = $3 AND state != 'fulfilled' AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING id
            ),
            -- @snippet deleted_ttimeout(id=$promise_id, guard=fulfilled_task)
            deleted_ttimeout AS (
              DELETE FROM task_timeouts WHERE id = $3 AND EXISTS (SELECT 1 FROM fulfilled_task) RETURNING id
            ),
            -- @snippet deleted_callbacks(awaiter_id=$promise_id, guard=fulfilled_task)
            deleted_callbacks AS (
              DELETE FROM callbacks WHERE awaiter_id = $3 AND EXISTS (SELECT 1 FROM fulfilled_task) RETURNING awaited_id
            ),
            -- @cluster ResumptionEnqueued
            -- @snippet marked_ready(awaited_id=$promise_id, guard=updated_promise)
            marked_ready AS (
              UPDATE callbacks SET ready = true
              WHERE awaited_id = $3 AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING awaiter_id
            ),
            -- @snippet resumed_tasks(source=marked_ready)
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT awaiter_id FROM marked_ready) AND state = 'suspended'
              RETURNING id, version
            ),
            -- @snippet inserted_or_updated_ttimeout_resumed(time_base=$settled_at)
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($7 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_resumed()
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            -- @cluster ListenerUnblocked
            -- @snippet inserted_outgoing_unblock(id=$promise_id, guard=updated_promise)
            inserted_outgoing_unblock AS (
              INSERT INTO outgoing_unblock (promise_id, address)
              SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id = $3 AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING promise_id
            ),
            -- @snippet deleted_listeners(promise_id=$promise_id, guard=updated_promise)
            deleted_listeners AS (
              DELETE FROM listeners WHERE promise_id = $3 AND EXISTS (SELECT 1 FROM updated_promise) RETURNING promise_id
            ),
            result AS (
              SELECT * FROM updated_promise
              UNION ALL
              SELECT * FROM locked_promise WHERE NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT
              EXISTS (SELECT 1 FROM fence_check) AS task_exists,
              (SELECT ok FROM fence_ok) AS fence_ok,
              r.id, r.state, r.param_headers::text, r.param_data, r.value_headers::text, r.value_data, r.tags::text, r.timeout_at, r.created_at, r.settled_at
            FROM (SELECT 1) AS dummy
            LEFT JOIN result r ON true
        "))
            .bind(task_id).bind(version as i32)                                       // $1-$2
            .bind(promise_id).bind(state).bind(value_headers).bind(value_data).bind(settled_at)  // $3-$7
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(TaskFenceResult {
                task_exists: false,
                fence_ok: false,
                promise: None,
            });
        }
        let row = &rows[0];
        let task_exists: bool = row.get("task_exists");
        let fence_ok: bool = row.get("fence_ok");
        let promise_id_val: Option<String> = row.get("id");
        let promise = promise_id_val.map(|_| row_to_promise(row));
        Ok(TaskFenceResult {
            task_exists,
            fence_ok,
            promise,
        })
    }

    // T-05: task.heartbeat — single CTE with unnest
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
            ),
            updated_ttimeout AS (
              UPDATE task_timeouts tt
              SET timeout_at = $3 + tt.ttl
              FROM task_data td
              JOIN tasks t ON t.id = td.id AND t.version = td.version AND t.state = 'acquired'
              -- TODO: also guard that the promise is still active so heartbeats on tasks whose
              -- promise has already timed out are no-ops. Add after the tasks JOIN:
              --   JOIN promises p ON p.id = td.id AND p.state = 'pending' AND p.timeout_at > $3
              WHERE tt.id = td.id AND tt.process_id = $4
              RETURNING tt.id
            )
            SELECT 1
        ",
            )
            .bind(&ids)
            .bind(&versions)
            .bind(time)
            .bind(pid)
            .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(())
    }

    // T-06: task.suspend — single CTE
    fn task_suspend(
        &self,
        task_id: &str,
        version: i64,
        awaited_ids: &[&str],
    ) -> StorageResult<TaskSuspendResult> {
        let awaited_ids: Vec<String> = awaited_ids.iter().map(|s| s.to_string()).collect();

        // Statement 1: acquire locks — promises first, then task (consistent lock ordering)
        rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ANY($1) FOR UPDATE")
                .bind(&awaited_ids)
                .fetch_all(self.tx().as_mut()),
        )?;
        rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = $1 FOR UPDATE")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 2: fresh snapshot — sees all callbacks/timeouts committed before this point
        let rows = rt_block_on(sqlx::query("
            WITH locked_task AS (
              SELECT * FROM tasks WHERE id = $1
            ),
            locked_promises AS (
              SELECT * FROM promises
              WHERE id = ANY($3)
                AND EXISTS (SELECT 1 FROM locked_task WHERE version = $2 AND state = 'acquired')
            ),
            missing_count AS (
              SELECT (COALESCE(array_length($3::TEXT[], 1), 0) - COUNT(*)::INT) AS cnt
              FROM locked_promises
            ),
            settled_promises AS (
              SELECT id FROM locked_promises WHERE state != 'pending'
            ),
            can_suspend AS (
              SELECT 1
              WHERE EXISTS (SELECT 1 FROM locked_task WHERE version = $2 AND state = 'acquired')
                AND (SELECT cnt FROM missing_count) = 0
                AND NOT EXISTS (SELECT 1 FROM settled_promises)
            ),
            inserted_callbacks AS (
              INSERT INTO callbacks (awaited_id, awaiter_id)
              SELECT id, $1 FROM locked_promises
              WHERE EXISTS (SELECT 1 FROM can_suspend)
              ON CONFLICT (awaited_id, awaiter_id) DO NOTHING
              RETURNING awaited_id
            ),
            deleted_ttimeout_suspend AS (
              DELETE FROM task_timeouts WHERE id = $1
                AND EXISTS (SELECT 1 FROM can_suspend)
              RETURNING id
            ),
            suspended_task AS (
              UPDATE tasks SET state = 'suspended'
              WHERE id = $1 AND version = $2 AND state = 'acquired'
                AND EXISTS (SELECT 1 FROM can_suspend)
              RETURNING state
            ),
            deleted_ready_callbacks AS (
              DELETE FROM callbacks
              WHERE awaiter_id = $1 AND ready = true
                AND EXISTS (SELECT 1 FROM locked_task WHERE version = $2 AND state = 'acquired')
                AND (SELECT cnt FROM missing_count) = 0
              RETURNING *
            )
            SELECT
              EXISTS (SELECT 1 FROM locked_task WHERE version = $2 AND state = 'acquired') AS task_matched,
              EXISTS (SELECT 1 FROM suspended_task) AS was_suspended,
              (SELECT cnt FROM missing_count) AS missing_count
        ")
            .bind(task_id).bind(version as i32).bind(&awaited_ids)
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(TaskSuspendResult {
                task_matched: false,
                was_suspended: false,
                missing_count: 0,
            });
        }
        let row = &rows[0];
        let task_matched: bool = row.get("task_matched");
        let was_suspended: bool = row.get("was_suspended");
        let missing_count: i32 = row.get("missing_count");
        Ok(TaskSuspendResult {
            task_matched,
            was_suspended,
            missing_count,
        })
    }

    // T-07: task.fulfill — lock preamble + CTE
    // Under READ COMMITTED, lock the task and promise first so the CTE gets a fresh
    // snapshot that includes callbacks/timeouts from concurrent transactions.
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
        let trt = self.task_retry_timeout;

        // Statement 1: lock preamble — promise first, then task (consistent lock ordering)
        rt_block_on(
            sqlx::query(
                "
            WITH lock_promise AS (
              SELECT id FROM promises WHERE id = $1 FOR UPDATE
            ),
            lock_task AS (
              SELECT id FROM tasks WHERE id = $2 FOR UPDATE
            )
            SELECT 1
        ",
            )
            .bind(promise_id)
            .bind(task_id)
            .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 2: fresh snapshot — sees all callbacks/timeouts committed before this point
        let rows = rt_block_on(sqlx::query(&format!("
            WITH fulfilled_acquired_task AS (
              UPDATE tasks SET state = 'fulfilled'
              WHERE id = $1 AND version = $2 AND state = 'acquired'
              RETURNING *
            ),
            -- @snippet deleted_ttimeout(id=$task_id, guard=fulfilled_acquired_task)
            deleted_ttimeout AS (
              DELETE FROM task_timeouts WHERE id = $1 AND EXISTS (SELECT 1 FROM fulfilled_acquired_task) RETURNING id
            ),
            -- @snippet updated_promise(id=$promise_id, guard=fulfilled_acquired_task)
            updated_promise AS (
              UPDATE promises p
              SET state = $4, value_headers = $5::jsonb, value_data = $6, settled_at = $7
              WHERE p.id = $3 AND p.state = 'pending' AND EXISTS (SELECT 1 FROM fulfilled_acquired_task)
              RETURNING p.*
            ),
            -- @snippet deleted_ptimeout(id=$promise_id, guard=fulfilled_acquired_task)
            deleted_ptimeout AS (
              DELETE FROM promise_timeouts WHERE id = $3 AND EXISTS (SELECT 1 FROM fulfilled_acquired_task) RETURNING id
            ),
            -- @snippet deleted_callbacks(awaiter_id=$task_id, guard=fulfilled_acquired_task)
            deleted_callbacks AS (
              DELETE FROM callbacks WHERE awaiter_id = $1 AND EXISTS (SELECT 1 FROM fulfilled_acquired_task) RETURNING awaited_id
            ),
            -- @cluster ResumptionEnqueued
            -- @snippet marked_ready(awaited_id=$promise_id, guard=updated_promise)
            marked_ready AS (
              UPDATE callbacks SET ready = true
              WHERE awaited_id = $3 AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING awaiter_id
            ),
            -- @snippet resumed_tasks(source=marked_ready)
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT awaiter_id FROM marked_ready) AND state = 'suspended'
              RETURNING id, version
            ),
            -- @snippet inserted_or_updated_ttimeout_resumed(time_base=$settled_at)
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($7 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_resumed()
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            -- @cluster ListenerUnblocked
            -- @snippet inserted_outgoing_unblock(id=$promise_id, guard=updated_promise)
            inserted_outgoing_unblock AS (
              INSERT INTO outgoing_unblock (promise_id, address)
              SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id = $3 AND EXISTS (SELECT 1 FROM updated_promise)
              RETURNING promise_id
            ),
            -- @snippet deleted_listeners(promise_id=$promise_id, guard=updated_promise)
            deleted_listeners AS (
              DELETE FROM listeners WHERE promise_id = $3 AND EXISTS (SELECT 1 FROM updated_promise) RETURNING promise_id
            ),
            result AS (
              SELECT *, EXISTS (SELECT 1 FROM fulfilled_acquired_task) AS task_fulfilled, EXISTS (SELECT 1 FROM tasks WHERE id = $1) AS task_exists FROM updated_promise
              UNION ALL
              SELECT *, EXISTS (SELECT 1 FROM fulfilled_acquired_task) AS task_fulfilled, EXISTS (SELECT 1 FROM tasks WHERE id = $1) AS task_exists FROM promises WHERE id = $3 AND NOT EXISTS (SELECT 1 FROM updated_promise)
            )
            SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at, task_fulfilled, task_exists FROM result
        "))
            .bind(task_id).bind(version as i32)                                       // $1-$2
            .bind(promise_id).bind(state).bind(value_headers).bind(value_data).bind(settled_at)  // $3-$7
            .fetch_all(self.tx().as_mut()))?;

        if rows.is_empty() {
            return Ok(TaskFulfillResult {
                task_exists: false,
                task_fulfilled: false,
                promise: None,
            });
        }
        let row = &rows[0];
        let task_fulfilled: bool = row.get("task_fulfilled");
        let task_exists: bool = row.get("task_exists");
        Ok(TaskFulfillResult {
            task_exists,
            task_fulfilled,
            promise: Some(row_to_promise(row)),
        })
    }

    // T-08: task.release — single CTE
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
              UPDATE tasks SET state = 'pending'
              WHERE id = $1 AND version = $2 AND state = 'acquired'
              RETURNING *
            ),
            updated_ttimeout AS (
              UPDATE task_timeouts
              SET timeout_type = 0, timeout_at = $3 + $4, process_id = NULL, ttl = $4
              WHERE id = $1 AND EXISTS (SELECT 1 FROM released_task)
              RETURNING id
            ),
            -- @snippet inserted_or_updated_outgoing_released()
            inserted_or_updated_outgoing_released AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target
              FROM released_task t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            )
            SELECT
              EXISTS (SELECT 1 FROM released_task) AS task_released,
              EXISTS (SELECT 1 FROM tasks WHERE id = $1) AS task_exists
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

    // T-09: task.halt — single CTE
    fn task_halt(&self, task_id: &str) -> StorageResult<TaskHaltResult> {
        let row = rt_block_on(
            sqlx::query(
                "
            WITH locked_task AS (
              SELECT * FROM tasks WHERE id = $1 FOR UPDATE
            ),
            halted_task AS (
              UPDATE tasks SET state = 'halted'
              WHERE id = $1 AND state NOT IN ('fulfilled', 'halted')
              RETURNING id
            ),
            deleted_ttimeout AS (
              DELETE FROM task_timeouts
              WHERE id = $1 AND EXISTS (SELECT 1 FROM halted_task)
              RETURNING id
            )
            SELECT
              EXISTS (SELECT 1 FROM locked_task) AS task_exists,
              EXISTS (SELECT 1 FROM locked_task WHERE state = 'fulfilled') AS task_fulfilled
        ",
            )
            .bind(task_id)
            .fetch_one(self.tx().as_mut()),
        )?;

        Ok(TaskHaltResult {
            task_exists: row.get::<bool, _>("task_exists"),
            task_fulfilled: row.get::<bool, _>("task_fulfilled"),
        })
    }

    // T-10: task.continue — single CTE
    fn task_continue(&self, task_id: &str, time: i64) -> StorageResult<TaskContinueResult> {
        let trt = self.task_retry_timeout;
        let row = rt_block_on(
            sqlx::query(&format!(
                "
            WITH locked_task AS (
              SELECT * FROM tasks WHERE id = $1 FOR UPDATE
            ),
            continued_task AS (
              UPDATE tasks SET state = 'pending'
              WHERE id = $1 AND state = 'halted'
              RETURNING id, version
            ),
            inserted_ttimeout AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($2 + {trt}), $1, 0, {trt}
              WHERE EXISTS (SELECT 1 FROM continued_task)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            inserted_or_updated_outgoing AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target
              FROM continued_task t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
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
            task_exists: row.get::<bool, _>("task_exists"),
            continued: row.get::<bool, _>("continued"),
        })
    }

    // T-11: task.search — single query with resumes subquery
    fn task_search(
        &self,
        state: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<TaskRecord>> {
        let rows = rt_block_on(sqlx::query("
            SELECT t.id, t.state, t.version,
              CASE WHEN tt.timeout_type = 1 THEN tt.ttl ELSE NULL END AS ttl,
              CASE WHEN tt.timeout_type = 1 THEN tt.process_id ELSE NULL END AS pid,
              COALESCE(
                (SELECT COUNT(*)::INT FROM callbacks c WHERE c.awaiter_id = t.id AND c.ready = true),
                0
              ) AS resumes
            FROM tasks t LEFT JOIN task_timeouts tt ON tt.id = t.id
            WHERE ($1::text IS NULL OR t.state = $1) AND ($2::text IS NULL OR t.id > $2)
            ORDER BY t.id ASC LIMIT $3
        ")
            .bind(state).bind(cursor).bind(limit).fetch_all(self.tx().as_mut()))?;

        Ok(rows
            .iter()
            .map(|r| {
                let resumes: i32 = r.get("resumes");
                TaskRecord {
                    id: r.get("id"),
                    state: parse_task_state(&r.get::<String, _>("state")),
                    version: r.get::<i32, _>("version") as i64,
                    resumes: resumes as i64,
                    ttl: r.get("ttl"),
                    pid: r.get("pid"),
                }
            })
            .collect())
    }

    fn compute_preload(&self, promise_id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let branch_row = rt_block_on(
            sqlx::query("SELECT branch FROM promises WHERE id = $1")
                .bind(promise_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let branch: Option<String> = branch_row.and_then(|r| r.get("branch"));
        let branch = match branch {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let rows = rt_block_on(sqlx::query(
            "SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at
             FROM promises WHERE branch = $1 AND id != $2 ORDER BY id ASC")
            .bind(&branch).bind(promise_id).fetch_all(self.tx().as_mut()))?;
        Ok(rows.iter().map(row_to_promise).collect())
    }

    // S-01: schedule.get
    fn schedule_get(&self, id: &str) -> StorageResult<Option<ScheduleRecord>> {
        let row = rt_block_on(sqlx::query(
            "SELECT id, cron, promise_id, promise_timeout, promise_param_headers::text, promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at FROM schedules WHERE id = $1")
            .bind(id).fetch_optional(self.tx().as_mut()))?;
        Ok(row.as_ref().map(row_to_schedule))
    }

    // S-03: schedule.create — single CTE
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

        let rows = rt_block_on(sqlx::query("
            WITH inserted_or_skipped_schedule AS (
              INSERT INTO schedules (id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at)
              VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8, $9)
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            inserted_or_skipped_stimeout AS (
              INSERT INTO schedule_timeouts (timeout_at, id)
              SELECT $9, $1
              WHERE EXISTS (SELECT 1 FROM inserted_or_skipped_schedule)
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            result AS (
              SELECT * FROM inserted_or_skipped_schedule
              UNION ALL
              SELECT * FROM schedules WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM inserted_or_skipped_schedule)
            )
            SELECT id, cron, promise_id, promise_timeout, promise_param_headers::text, promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at FROM result
        ")
            .bind(id).bind(cron).bind(promise_id).bind(promise_timeout)
            .bind(promise_param_headers).bind(promise_param_data).bind(promise_tags)
            .bind(created_at).bind(next_run_at)
            .fetch_one(self.tx().as_mut()))?;

        Ok(row_to_schedule(&rows))
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
            "SELECT id, cron, promise_id, promise_timeout, promise_param_headers::text, promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at
             FROM schedules WHERE ($1::jsonb IS NULL OR promise_tags @> $1::jsonb) AND ($2::text IS NULL OR id > $2) ORDER BY id ASC LIMIT $3")
            .bind(tags).bind(cursor).bind(limit).fetch_all(self.tx().as_mut()))?;
        Ok(rows.iter().map(row_to_schedule).collect())
    }

    fn get_expired_schedule_timeouts(&self, time: i64) -> StorageResult<Vec<(String, i64)>> {
        let rows = rt_block_on(
            sqlx::query("SELECT id, timeout_at FROM schedule_timeouts WHERE timeout_at <= $1")
                .bind(time)
                .fetch_all(self.tx().as_mut()),
        )?;
        Ok(rows
            .iter()
            .map(|r| (r.get::<String, _>("id"), r.get::<i64, _>("timeout_at")))
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
            WITH fired_stimeout AS (
              SELECT st.id
              FROM schedule_timeouts st
              WHERE st.id = $1 AND st.timeout_at = $2
            ),
            schedule AS (
              SELECT *,
                REPLACE(REPLACE(promise_id, '{{{{.id}}}}', id), '{{{{.timestamp}}}}', CAST($2 AS TEXT)) AS computed_promise_id,
                ($2 + promise_timeout) AS computed_timeout_at,
                (promise_tags->>'resonate:target') AS address,
                ($5 >= ($2 + promise_timeout)) AS already_timedout
              FROM schedules
              WHERE id = $1
                AND EXISTS (SELECT 1 FROM fired_stimeout)
            ),
            inserted_or_skipped_promise AS (
              INSERT INTO promises (id, state, param_headers, param_data, tags, timeout_at, created_at, settled_at)
              SELECT s.computed_promise_id,
                CASE WHEN s.already_timedout THEN CASE WHEN ($4::jsonb->>'resonate:timer') = 'true' THEN 'resolved' ELSE 'rejected_timedout' END ELSE 'pending' END,
                s.promise_param_headers, s.promise_param_data, $4::jsonb,
                s.computed_timeout_at,
                $2,
                CASE WHEN s.already_timedout THEN s.computed_timeout_at ELSE NULL END
              FROM schedule s
              ON CONFLICT (id) DO NOTHING
              RETURNING *
            ),
            inserted_or_skipped_ptimeout AS (
              INSERT INTO promise_timeouts (timeout_at, id)
              SELECT p.timeout_at, p.id FROM inserted_or_skipped_promise p
              WHERE p.state = 'pending'
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            inserted_or_skipped_task AS (
              INSERT INTO tasks (id, state)
              SELECT p.id,
                CASE WHEN p.state != 'pending' THEN 'fulfilled' ELSE 'pending' END
              FROM inserted_or_skipped_promise p
              WHERE EXISTS (SELECT 1 FROM schedule s WHERE s.address IS NOT NULL)
              ON CONFLICT (id) DO NOTHING
              RETURNING id, state
            ),
            inserted_or_skipped_ttimeout AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($2 + {trt}), t.id, 0, {trt}
              FROM inserted_or_skipped_task t
              WHERE t.state = 'pending'
              ON CONFLICT (id) DO NOTHING
              RETURNING id
            ),
            inserted_or_updated_outgoing AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, 0, s.address
              FROM inserted_or_skipped_task t, schedule s
              WHERE t.state = 'pending'
              ON CONFLICT (id) DO UPDATE
                SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            updated_schedule AS (
              UPDATE schedules SET last_run_at = $2, next_run_at = $3
              WHERE id = $1
                AND EXISTS (SELECT 1 FROM fired_stimeout)
              RETURNING *
            ),
            updated_stimeout AS (
              UPDATE schedule_timeouts SET timeout_at = $3
              WHERE id = $1
                AND EXISTS (SELECT 1 FROM fired_stimeout)
              RETURNING id
            )
            SELECT id, cron, promise_id, promise_timeout, promise_param_headers::text, promise_param_data, promise_tags::text, created_at, next_run_at, last_run_at FROM updated_schedule
        ", trt = trt))
            .bind(schedule_id)       // $1
            .bind(fired_at)          // $2
            .bind(next_run_at)       // $3
            .bind(promise_tags_json) // $4
            .bind(time)              // $5
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

    // D-02: debug.reset
    fn debug_reset(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql(
            "TRUNCATE outgoing_unblock, outgoing_execute, task_timeouts, listeners, callbacks, promise_timeouts, tasks, promises, schedule_timeouts, schedules CASCADE"
        ).execute(self.tx().as_mut()))?;
        Ok(())
    }

    // Timeout processing — three sequential CTE statements
    fn process_timeouts(&self, time: i64) -> StorageResult<()> {
        let trt = self.task_retry_timeout;
        // Statement 1: Process expired promise timeouts
        rt_block_on(sqlx::query(&format!("
            WITH expired AS (
              UPDATE promises p
              SET state = CASE WHEN p.timer THEN 'resolved' ELSE 'rejected_timedout' END,
                  settled_at = p.timeout_at
              WHERE p.state = 'pending' AND p.timeout_at <= $1
              RETURNING *
            ),
            deleted_ptimeout AS (
              DELETE FROM promise_timeouts WHERE id IN (SELECT id FROM expired) RETURNING id
            ),
            fulfilled_task AS (
              UPDATE tasks SET state = 'fulfilled'
              WHERE id IN (SELECT id FROM expired) AND state != 'fulfilled'
              RETURNING id
            ),
            deleted_ttimeout AS (
              DELETE FROM task_timeouts WHERE id IN (SELECT id FROM fulfilled_task) RETURNING id
            ),
            deleted_callbacks AS (
              DELETE FROM callbacks WHERE awaiter_id IN (SELECT id FROM fulfilled_task) RETURNING awaited_id
            ),
            marked_ready AS (
              UPDATE callbacks SET ready = true
              WHERE awaited_id IN (SELECT id FROM expired)
                AND awaiter_id NOT IN (SELECT id FROM fulfilled_task)
              RETURNING awaiter_id
            ),
            resumed_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT awaiter_id FROM marked_ready)
                AND state = 'suspended'
                AND id NOT IN (SELECT id FROM fulfilled_task)
              RETURNING id, version
            ),
            inserted_or_updated_ttimeout_resumed AS (
              INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl)
              SELECT ($1 + {trt}), id, 0, {trt} FROM resumed_tasks
              ON CONFLICT (id) DO UPDATE SET timeout_at = EXCLUDED.timeout_at, timeout_type = 0, process_id = NULL, ttl = {trt}
              RETURNING id
            ),
            inserted_or_updated_outgoing_resumed AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target FROM resumed_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            ),
            inserted_outgoing_unblock AS (
              INSERT INTO outgoing_unblock (promise_id, address)
              SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id IN (SELECT id FROM expired)
              RETURNING promise_id
            ),
            deleted_listeners AS (
              DELETE FROM listeners WHERE promise_id IN (SELECT id FROM expired) RETURNING promise_id
            )
            SELECT id, state FROM expired
        ")).bind(time).fetch_all(self.tx().as_mut()))?;

        // Statement 2: Process expired task retry timeouts (type 0)
        rt_block_on(
            sqlx::query(&format!(
                "
            WITH expired_retry AS (
              SELECT tt.id FROM task_timeouts tt JOIN tasks t ON t.id = tt.id
              WHERE tt.timeout_type = 0 AND tt.timeout_at <= $1 AND t.state = 'pending'
              FOR UPDATE OF t, tt
            ),
            updated_retry_ttimeout AS (
              UPDATE task_timeouts SET timeout_at = $1 + {trt}, process_id = NULL
              WHERE id IN (SELECT id FROM expired_retry)
              RETURNING id
            ),
            inserted_outgoing_retry AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target
              FROM expired_retry er JOIN tasks t ON t.id = er.id JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            )
            SELECT 1
        "
            ))
            .bind(time)
            .fetch_optional(self.tx().as_mut()),
        )?;

        // Statement 3: Process expired task lease timeouts (type 1)
        rt_block_on(sqlx::query(&format!("
            WITH expired_lease AS (
              SELECT tt.id FROM task_timeouts tt JOIN tasks t ON t.id = tt.id
              WHERE tt.timeout_type = 1 AND tt.timeout_at <= $1 AND t.state = 'acquired'
              FOR UPDATE OF t, tt
            ),
            released_tasks AS (
              UPDATE tasks SET state = 'pending'
              WHERE id IN (SELECT id FROM expired_lease)
              RETURNING id, version
            ),
            updated_lease_ttimeout AS (
              UPDATE task_timeouts SET timeout_at = $1 + {trt}, timeout_type = 0, process_id = NULL, ttl = {trt}
              WHERE id IN (SELECT id FROM expired_lease)
              RETURNING id
            ),
            inserted_outgoing_released AS (
              INSERT INTO outgoing_execute (id, version, address)
              SELECT t.id, t.version, p.target
              FROM released_tasks t JOIN promises p ON p.id = t.id
              ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address
              RETURNING id
            )
            SELECT 1
        ")).bind(time).fetch_optional(self.tx().as_mut()))?;

        Ok(())
    }

    // D-04: debug.snap — multiple simple queries
    fn snap(&self) -> StorageResult<Snapshot> {
        // TODO: set REPEATABLE READ so all snap queries see a single consistent snapshot and
        // mid-snap interleaving from concurrent writes cannot produce an inconsistent picture.
        // Add before the first query:
        //   rt_block_on(sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        //       .execute(self.tx().as_mut()))?;
        let promise_rows = rt_block_on(sqlx::query(
            "SELECT id, state, param_headers::text, param_data, value_headers::text, value_data, tags::text, timeout_at, created_at, settled_at FROM promises ORDER BY id")
            .fetch_all(self.tx().as_mut()))?;
        let promises: Vec<PromiseRecord> = promise_rows.iter().map(row_to_promise).collect();

        let pt_rows = rt_block_on(
            sqlx::query("SELECT id, timeout_at FROM promise_timeouts ORDER BY id")
                .fetch_all(self.tx().as_mut()),
        )?;
        let promise_timeouts: Vec<SnapshotPromiseTimeout> = pt_rows
            .iter()
            .map(|r| SnapshotPromiseTimeout {
                id: r.get("id"),
                timeout: r.get("timeout_at"),
            })
            .collect();

        let cb_rows = rt_block_on(sqlx::query("SELECT awaiter_id, awaited_id FROM callbacks WHERE NOT ready ORDER BY awaiter_id, awaited_id").fetch_all(self.tx().as_mut()))?;
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

        let task_rows = rt_block_on(sqlx::query("
            SELECT t.id, t.state, t.version,
              CASE WHEN tt.timeout_type = 1 THEN tt.ttl ELSE NULL END AS ttl,
              CASE WHEN tt.timeout_type = 1 THEN tt.process_id ELSE NULL END AS pid,
              COALESCE(
                (SELECT COUNT(*)::INT FROM callbacks c WHERE c.awaiter_id = t.id AND c.ready = true),
                0
              ) AS resumes
            FROM tasks t LEFT JOIN task_timeouts tt ON tt.id = t.id ORDER BY t.id
        ").fetch_all(self.tx().as_mut()))?;
        let tasks: Vec<TaskRecord> = task_rows
            .iter()
            .map(|r| {
                let resumes: i32 = r.get("resumes");
                TaskRecord {
                    id: r.get("id"),
                    state: parse_task_state(&r.get::<String, _>("state")),
                    version: r.get::<i32, _>("version") as i64,
                    resumes: resumes as i64,
                    ttl: r.get("ttl"),
                    pid: r.get("pid"),
                }
            })
            .collect();

        let tt_rows = rt_block_on(
            sqlx::query("SELECT id, timeout_type, timeout_at FROM task_timeouts ORDER BY id")
                .fetch_all(self.tx().as_mut()),
        )?;
        let task_timeouts: Vec<SnapshotTaskTimeout> = tt_rows
            .iter()
            .map(|r| {
                let tt: i16 = r.get("timeout_type");
                SnapshotTaskTimeout {
                    id: r.get("id"),
                    timeout_type: tt as i32,
                    timeout: r.get("timeout_at"),
                }
            })
            .collect();

        let mut messages: Vec<SnapshotMessage> = Vec::new();
        let exec_rows = rt_block_on(
            sqlx::query("SELECT id, version, address FROM outgoing_execute ORDER BY id")
                .fetch_all(self.tx().as_mut()),
        )?;
        for r in &exec_rows {
            let id: String = r.get("id");
            let version: i32 = r.get("version");
            let address: String = r.get("address");
            messages.push(SnapshotMessage { address, message: serde_json::json!({ "kind": "execute", "head": {}, "data": { "task": { "id": id, "version": version } } }) });
        }
        let unblock_rows = rt_block_on(sqlx::query(
            "SELECT ou.promise_id, ou.address, p.id, p.state, p.param_headers::text, p.param_data, p.value_headers::text, p.value_data, p.tags::text, p.timeout_at, p.created_at, p.settled_at
             FROM outgoing_unblock ou JOIN promises p ON p.id = ou.promise_id ORDER BY ou.promise_id, ou.address")
            .fetch_all(self.tx().as_mut()))?;
        for r in &unblock_rows {
            let address: String = r.get::<String, _>(1);
            let promise = row_to_promise_indexed(r, 2);
            messages.push(SnapshotMessage { address, message: serde_json::json!({ "kind": "unblock", "head": {}, "data": { "promise": promise } }) });
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
        // Atomically delete and return a batch of execute messages
        let exec_rows = rt_block_on(
            sqlx::query(
                "WITH batch AS (
                DELETE FROM outgoing_execute WHERE id IN (SELECT id FROM outgoing_execute LIMIT $1)
                RETURNING id, version, address
            )
            SELECT * FROM batch",
            )
            .bind(batch_size)
            .fetch_all(self.tx().as_mut()),
        )?;
        let execute_msgs: Vec<OutgoingExecute> = exec_rows
            .iter()
            .map(|r| {
                let v: i32 = r.get("version");
                OutgoingExecute {
                    id: r.get("id"),
                    version: v as i64,
                    address: r.get("address"),
                }
            })
            .collect();

        // Atomically delete and return a batch of unblock messages, joining with promises for payload
        let unblock_rows = rt_block_on(sqlx::query(
            "WITH batch AS (
                DELETE FROM outgoing_unblock WHERE (promise_id, address) IN (SELECT promise_id, address FROM outgoing_unblock LIMIT $1)
                RETURNING promise_id, address
            )
            SELECT b.address, p.id, p.state, p.param_headers::text, p.param_data, p.value_headers::text, p.value_data, p.tags::text, p.timeout_at, p.created_at, p.settled_at
            FROM batch b JOIN promises p ON p.id = b.promise_id"
        ).bind(batch_size).fetch_all(self.tx().as_mut()))?;
        let unblock_msgs: Vec<OutgoingUnblock> = unblock_rows
            .iter()
            .map(|r| OutgoingUnblock {
                address: r.get::<String, _>(0),
                promise: row_to_promise_indexed(r, 1),
            })
            .collect();

        Ok((execute_msgs, unblock_msgs))
    }
}
