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
use sqlx::mysql::MySqlRow;
use sqlx::{MySqlPool, Row};
use std::cell::UnsafeCell;

pub struct MysqlStorage {
    pool: MySqlPool,
    task_retry_timeout: i64,
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
  PRIMARY KEY (id),
  INDEX idx_promises_timeout_at (timeout_at),
  INDEX idx_promises_target (target),
  CONSTRAINT promises_state_check CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout'))
);

CREATE TABLE IF NOT EXISTS promise_timeouts (
  timeout_at BIGINT NOT NULL,
  id VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_promise_timeouts_timeout_at_id (timeout_at ASC, id ASC),
  FOREIGN KEY (id) REFERENCES promises (id) ON DELETE CASCADE
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

CREATE TABLE IF NOT EXISTS tasks (
  id VARCHAR(255) NOT NULL,
  state VARCHAR(50) NOT NULL DEFAULT 'pending',
  version INT NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES promises (id) ON DELETE CASCADE,
  CONSTRAINT tasks_state_check CHECK (state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled'))
);

CREATE TABLE IF NOT EXISTS task_timeouts (
  timeout_at BIGINT NOT NULL,
  id VARCHAR(255) NOT NULL,
  timeout_type SMALLINT NOT NULL DEFAULT 0,
  process_id VARCHAR(255),
  ttl BIGINT NOT NULL DEFAULT 30000,
  PRIMARY KEY (id),
  INDEX idx_task_timeouts_timeout_at_id (timeout_at ASC, id ASC),
  INDEX idx_task_timeouts_process_id (process_id),
  INDEX idx_task_timeouts_timeout_type (timeout_type, timeout_at ASC),
  FOREIGN KEY (id) REFERENCES tasks (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS outgoing_execute (
  id VARCHAR(255) NOT NULL,
  version INT NOT NULL,
  address VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (id) REFERENCES tasks (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS outgoing_unblock (
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
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS schedule_timeouts (
  timeout_at BIGINT NOT NULL,
  id VARCHAR(255) NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_schedule_timeouts_timeout_at_id (timeout_at ASC, id ASC),
  FOREIGN KEY (id) REFERENCES schedules (id) ON DELETE CASCADE
);
"#;

impl MysqlStorage {
    pub async fn connect(
        url: &str,
        pool_size: u32,
        task_retry_timeout: i64,
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

    pub async fn transact<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
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
            let (result, tx) = tokio::task::block_in_place(|| {
                let db = MysqlDb {
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
                Ok(_) => return Ok(result),
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

    pub async fn query<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.transact(f).await
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

    /// Marks the owning task fulfilled, deletes its timeout, and deletes callbacks
    /// registered by it (as awaiter). Used when a promise that owns a task is settled.
    fn settlement_enqueued(&self, task_id: &str) -> StorageResult<()> {
        rt_block_on(
            sqlx::query(
                "UPDATE tasks SET state = 'fulfilled' WHERE id = ? AND state != 'fulfilled'",
            )
            .bind(task_id)
            .execute(self.tx().as_mut()),
        )?;
        rt_block_on(
            sqlx::query("DELETE FROM task_timeouts WHERE id = ?")
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

        // Snapshot resumed tasks BEFORE marking callbacks ready
        let rows = rt_block_on(
            sqlx::query(
                "SELECT t.id, t.version FROM tasks t
                 JOIN callbacks c ON c.awaiter_id = t.id
                 WHERE c.awaited_id = ? AND c.ready = false AND t.state = 'suspended'",
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
            let version: i32 = row.get("version");

            rt_block_on(
                sqlx::query(
                    "UPDATE tasks SET state = 'pending' WHERE id = ? AND state = 'suspended'",
                )
                .bind(&task_id)
                .execute(self.tx().as_mut()),
            )?;

            rt_block_on(
                sqlx::query(
                    "INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)
                     ON DUPLICATE KEY UPDATE timeout_at = VALUES(timeout_at), timeout_type = 0, process_id = NULL, ttl = VALUES(ttl)",
                )
                .bind(time + trt)
                .bind(&task_id)
                .bind(trt)
                .execute(self.tx().as_mut()),
            )?;

            rt_block_on(
                sqlx::query(
                    "INSERT INTO outgoing_execute (id, version, address)
                     SELECT ?, ?, target FROM promises WHERE id = ?
                     ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                )
                .bind(&task_id)
                .bind(version)
                .bind(&task_id)
                .execute(self.tx().as_mut()),
            )?;
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

        // Delete promise timeouts
        {
            let sql = format!("DELETE FROM promise_timeouts WHERE id IN ({})", ph(n));
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        // Fulfill owning tasks (same id as promise)
        {
            let sql = format!(
                "UPDATE tasks SET state = 'fulfilled' WHERE id IN ({}) AND state != 'fulfilled'",
                ph(n)
            );
            let mut q = sqlx::query(&sql);
            for id in expired_ids {
                q = q.bind(id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        // Delete task timeouts for fulfilled tasks
        {
            let sql = format!("DELETE FROM task_timeouts WHERE id IN ({})", ph(n));
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
                "SELECT t.id, t.version FROM tasks t
                 JOIN callbacks c ON c.awaiter_id = t.id
                 WHERE c.awaited_id IN ({}) AND c.ready = false AND t.state = 'suspended'
                   AND t.id NOT IN ({})",
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
            let version: i32 = row.get("version");

            rt_block_on(
                sqlx::query(
                    "UPDATE tasks SET state = 'pending' WHERE id = ? AND state = 'suspended'",
                )
                .bind(&task_id)
                .execute(self.tx().as_mut()),
            )?;
            rt_block_on(
                sqlx::query(
                    "INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)
                     ON DUPLICATE KEY UPDATE timeout_at = VALUES(timeout_at), timeout_type = 0,
                                             process_id = NULL, ttl = VALUES(ttl)"
                ).bind(time + trt).bind(&task_id).bind(trt).execute(self.tx().as_mut())
            )?;
            rt_block_on(
                sqlx::query(
                    "INSERT INTO outgoing_execute (id, version, address)
                     SELECT ?, ?, target FROM promises WHERE id = ?
                     ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                )
                .bind(&task_id)
                .bind(version)
                .bind(&task_id)
                .execute(self.tx().as_mut()),
            )?;
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
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO outgoing_unblock (promise_id, address) VALUES (?, ?)",
                    )
                    .bind(&pid)
                    .bind(&addr)
                    .execute(self.tx().as_mut()),
                )?;
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
            rt_block_on(
                sqlx::query(
                    "INSERT IGNORE INTO outgoing_unblock (promise_id, address) VALUES (?, ?)",
                )
                .bind(promise_id)
                .bind(&address)
                .execute(self.tx().as_mut()),
            )?;
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

fn row_to_promise_offset(row: &MySqlRow, offset: usize) -> PromiseRecord {
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
// Db implementation — stubbed; every method returns todo!()
// ============================================================================

impl Db for MysqlDb<'_> {
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
        let p = rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE id = ? FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let t = rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = ? FOR UPDATE")
                .bind(id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        Ok((p.is_some(), t.is_some()))
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
            // New promise — set up timeout and optional task infrastructure
            if !already_timedout {
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO promise_timeouts (timeout_at, id) VALUES (?, ?)",
                    )
                    .bind(timeout_at)
                    .bind(id)
                    .execute(self.tx().as_mut()),
                )?;
            }

            if let Some(addr) = address {
                let task_state = if already_timedout {
                    "fulfilled"
                } else {
                    "pending"
                };
                let task_res = rt_block_on(
                    sqlx::query("INSERT IGNORE INTO tasks (id, state) VALUES (?, ?)")
                        .bind(id)
                        .bind(task_state)
                        .execute(self.tx().as_mut()),
                )?;

                if task_res.rows_affected() > 0 && !already_timedout {
                    rt_block_on(
                        sqlx::query(
                            "INSERT IGNORE INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)",
                        )
                        .bind(created_at + trt)
                        .bind(id)
                        .bind(trt)
                        .execute(self.tx().as_mut()),
                    )?;
                    rt_block_on(
                        sqlx::query(
                            "INSERT INTO outgoing_execute (id, version, address) VALUES (?, 0, ?)
                             ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                        )
                        .bind(id)
                        .bind(addr)
                        .execute(self.tx().as_mut()),
                    )?;
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
            // Delete promise timeout
            rt_block_on(
                sqlx::query("DELETE FROM promise_timeouts WHERE id = ?")
                    .bind(id)
                    .execute(self.tx().as_mut()),
            )?;
            // Fulfill owning task (same id), delete its timeout, delete its callbacks-as-awaiter
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
                "SELECT id, state, target FROM promises WHERE id IN (?, ?) ORDER BY id FOR UPDATE",
            )
            .bind(awaited_id)
            .bind(awaiter_id)
            .fetch_all(self.tx().as_mut()),
        )?;

        let mut awaited_state: Option<String> = None;
        let mut awaiter_state: Option<String> = None;
        let mut awaiter_target: Option<String> = None;

        for row in &rows {
            let rid: String = row.get("id");
            let rstate: String = row.get("state");
            let rtarget: Option<String> = row.get("target");
            if rid == awaited_id {
                awaited_state = Some(rstate);
            } else {
                awaiter_state = Some(rstate);
                awaiter_target = rtarget;
            }
        }

        match (&awaited_state, &awaiter_state) {
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
                        "UPDATE tasks SET state = 'pending' WHERE id = ? AND state = 'suspended'",
                    )
                    .bind(awaiter_id)
                    .execute(self.tx().as_mut()),
                )?;
                // Only enqueue timeout and execute message if the task was actually transitioned
                if upd.rows_affected() > 0 {
                    rt_block_on(
                        sqlx::query(
                            "INSERT INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)
                             ON DUPLICATE KEY UPDATE timeout_at = VALUES(timeout_at), timeout_type = 0, process_id = NULL, ttl = VALUES(ttl)",
                        )
                        .bind(time + trt)
                        .bind(awaiter_id)
                        .bind(trt)
                        .execute(self.tx().as_mut()),
                    )?;
                    rt_block_on(
                        sqlx::query(
                            "INSERT INTO outgoing_execute (id, version, address)
                             SELECT t.id, t.version, p.target FROM tasks t JOIN promises p ON p.id = t.id
                             WHERE t.id = ?
                             ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                        )
                        .bind(awaiter_id)
                        .execute(self.tx().as_mut()),
                    )?;
                }
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
                "SELECT t.id, t.state, t.version,
                   CASE WHEN tt.timeout_type = 1 THEN tt.ttl ELSE NULL END AS ttl,
                   CASE WHEN tt.timeout_type = 1 THEN tt.process_id ELSE NULL END AS pid,
                   COALESCE(
                     (SELECT CAST(COUNT(*) AS SIGNED) FROM callbacks c WHERE c.awaiter_id = t.id AND c.ready = true),
                     0
                   ) AS resumes
                 FROM tasks t LEFT JOIN task_timeouts tt ON tt.id = t.id WHERE t.id = ?",
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
        let trt = self.task_retry_timeout;
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
            if !already_timedout {
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO promise_timeouts (timeout_at, id) VALUES (?, ?)",
                    )
                    .bind(timeout_at)
                    .bind(promise_id)
                    .execute(self.tx().as_mut()),
                )?;
            }
            let task_res = rt_block_on(
                sqlx::query("INSERT IGNORE INTO tasks (id, state, version) VALUES (?, ?, ?)")
                    .bind(promise_id)
                    .bind(task_initial_state)
                    .bind(task_initial_version)
                    .execute(self.tx().as_mut()),
            )?;
            task_created = task_res.rows_affected() > 0;

            if task_created && !already_timedout {
                // Insert initial retry timeout (type 0); will be replaced by lease below
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)"
                    ).bind(created_at + trt).bind(promise_id).bind(trt)
                     .execute(self.tx().as_mut())
                )?;
            }
        }

        let promise = self
            .promise_get(promise_id)?
            .unwrap_or_else(|| unreachable!("promise missing after insert in task_create"));

        if task_created && !already_timedout {
            // Upgrade to lease timeout (type 1) with the worker's ttl and pid
            rt_block_on(
                sqlx::query(
                    "INSERT INTO task_timeouts (timeout_at, id, timeout_type, process_id, ttl)
                     VALUES (?, ?, 1, ?, ?)
                     ON DUPLICATE KEY UPDATE timeout_at = VALUES(timeout_at), timeout_type = 1,
                                             process_id = VALUES(process_id), ttl = VALUES(ttl)",
                )
                .bind(created_at + ttl)
                .bind(promise_id)
                .bind(pid)
                .bind(ttl)
                .execute(self.tx().as_mut()),
            )?;
            return Ok(TaskCreateResult {
                promise,
                task_created: true,
                task_state: Some(task_initial_state.to_string()),
                task_version: Some(task_initial_version as i64),
            });
        }

        let task_row = rt_block_on(
            sqlx::query("SELECT state, version FROM tasks WHERE id = ?")
                .bind(promise_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        Ok(TaskCreateResult {
            promise,
            task_created: false,
            task_state: task_row.as_ref().map(|r| r.get::<String, _>("state")),
            task_version: task_row.as_ref().map(|r| r.get::<i32, _>("version") as i64),
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

        let res = rt_block_on(
            sqlx::query(
                "UPDATE tasks SET state = 'acquired', version = version + 1
                 WHERE id = ? AND version = ? AND state = 'pending'",
            )
            .bind(task_id)
            .bind(version as i32)
            .execute(self.tx().as_mut()),
        )?;
        let was_acquired = res.rows_affected() > 0;

        if was_acquired {
            rt_block_on(
                sqlx::query(
                    "INSERT INTO task_timeouts (timeout_at, id, timeout_type, process_id, ttl)
                     VALUES (?, ?, 1, ?, ?)
                     ON DUPLICATE KEY UPDATE timeout_at = VALUES(timeout_at), timeout_type = 1,
                                             process_id = VALUES(process_id), ttl = VALUES(ttl)",
                )
                .bind(time + ttl)
                .bind(task_id)
                .bind(pid)
                .bind(ttl)
                .execute(self.tx().as_mut()),
            )?;
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
                 FROM promises p JOIN tasks t ON t.id = p.id WHERE p.id = ?"
            ).bind(task_id).fetch_optional(self.tx().as_mut())
        )?;
        let task_row = rt_block_on(
            sqlx::query("SELECT state, version FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let task_state = task_row
            .as_ref()
            .map(|r| parse_task_state(&r.get::<String, _>("state")));
        let task_version = task_row.as_ref().map(|r| r.get::<i32, _>("version") as i64);

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
            sqlx::query("SELECT id, state, version FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        let task_exists = task_row.is_some();
        let fence_ok = task_row.as_ref().is_some_and(|r| {
            let s: String = r.get("state");
            let v: i32 = r.get("version");
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
                if !already_timedout {
                    rt_block_on(
                        sqlx::query(
                            "INSERT IGNORE INTO promise_timeouts (timeout_at, id) VALUES (?, ?)",
                        )
                        .bind(timeout_at)
                        .bind(promise_id)
                        .execute(self.tx().as_mut()),
                    )?;
                }
                if let Some(addr) = address {
                    let task_state = if already_timedout {
                        "fulfilled"
                    } else {
                        "pending"
                    };
                    let task_res = rt_block_on(
                        sqlx::query("INSERT IGNORE INTO tasks (id, state) VALUES (?, ?)")
                            .bind(promise_id)
                            .bind(task_state)
                            .execute(self.tx().as_mut()),
                    )?;
                    if task_res.rows_affected() > 0 && !already_timedout {
                        rt_block_on(
                            sqlx::query(
                                "INSERT IGNORE INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)"
                            ).bind(created_at + trt).bind(promise_id).bind(trt)
                             .execute(self.tx().as_mut())
                        )?;
                        rt_block_on(
                            sqlx::query(
                                "INSERT INTO outgoing_execute (id, version, address) VALUES (?, 0, ?)
                                 ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)"
                            ).bind(promise_id).bind(addr).execute(self.tx().as_mut())
                        )?;
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
            sqlx::query("SELECT id, state, version FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        let task_exists = task_row.is_some();
        let fence_ok = task_row.as_ref().is_some_and(|r| {
            let s: String = r.get("state");
            let v: i32 = r.get("version");
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
                rt_block_on(
                    sqlx::query("DELETE FROM promise_timeouts WHERE id = ?")
                        .bind(promise_id)
                        .execute(self.tx().as_mut()),
                )?;
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
            rt_block_on(
                sqlx::query(
                    "UPDATE task_timeouts tt
                     JOIN tasks t ON t.id = tt.id
                     JOIN promises p ON p.id = t.id
                     SET tt.timeout_at = ? + tt.ttl
                     WHERE tt.id = ? AND t.version = ? AND t.state = 'acquired' AND tt.process_id = ?
                       AND (p.state != 'pending' OR p.timeout_at > ?)"
                ).bind(time).bind(task_id).bind(*version as i32).bind(pid).bind(time)
                 .execute(self.tx().as_mut())
            )?;
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

        // 2. Lock the task
        rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = ? FOR UPDATE")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        // 3. Check task version/state
        let task_row = rt_block_on(
            sqlx::query("SELECT state, version FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let task_matched = task_row.as_ref().is_some_and(|r| {
            r.get::<String, _>("state") == "acquired"
                && r.get::<i32, _>("version") == version as i32
        });
        if !task_matched {
            return Ok(TaskSuspendResult {
                task_matched: false,
                was_suspended: false,
                missing_count: 0,
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
            rt_block_on(
                sqlx::query("DELETE FROM task_timeouts WHERE id = ?")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;
            rt_block_on(
                sqlx::query(
                    "UPDATE tasks SET state = 'suspended' WHERE id = ? AND version = ? AND state = 'acquired'"
                ).bind(task_id).bind(version as i32).execute(self.tx().as_mut())
            )?;
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: true,
                missing_count: 0,
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
            sqlx::query("SELECT id FROM tasks WHERE id = ? FOR UPDATE")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;
        let task_exists = task_lock.is_some();

        // 2. Fulfill the task (version + state guard)
        let task_res = rt_block_on(
            sqlx::query(
                "UPDATE tasks SET state = 'fulfilled' WHERE id = ? AND version = ? AND state = 'acquired'"
            ).bind(task_id).bind(version as i32).execute(self.tx().as_mut())
        )?;
        let task_fulfilled = task_res.rows_affected() > 0;

        if task_fulfilled {
            // 3. Clean up task infrastructure
            rt_block_on(
                sqlx::query("DELETE FROM task_timeouts WHERE id = ?")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;
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
                // 5. Delete promise timeout
                rt_block_on(
                    sqlx::query("DELETE FROM promise_timeouts WHERE id = ?")
                        .bind(promise_id)
                        .execute(self.tx().as_mut()),
                )?;
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
        let res = rt_block_on(
            sqlx::query(
                "UPDATE tasks SET state = 'pending' WHERE id = ? AND version = ? AND state = 'acquired'"
            ).bind(task_id).bind(version as i32).execute(self.tx().as_mut())
        )?;
        let task_released = res.rows_affected() > 0;

        if task_released {
            rt_block_on(
                sqlx::query(
                    "UPDATE task_timeouts SET timeout_type = 0, timeout_at = ? + ?, process_id = NULL, ttl = ?
                     WHERE id = ?"
                ).bind(time).bind(ttl).bind(ttl).bind(task_id)
                 .execute(self.tx().as_mut())
            )?;
            rt_block_on(
                sqlx::query(
                    "INSERT INTO outgoing_execute (id, version, address)
                     SELECT t.id, t.version, p.target FROM tasks t JOIN promises p ON p.id = t.id
                     WHERE t.id = ?
                     ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                )
                .bind(task_id)
                .execute(self.tx().as_mut()),
            )?;
        }
        let task_exists = rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = ?")
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
            sqlx::query("SELECT id, state FROM tasks WHERE id = ? FOR UPDATE")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        let task_exists = row.is_some();
        let task_state: Option<String> = row.as_ref().map(|r| r.get("state"));
        let task_fulfilled = task_state.as_deref() == Some("fulfilled");

        if task_exists && !task_fulfilled && task_state.as_deref() != Some("halted") {
            rt_block_on(
                sqlx::query("UPDATE tasks SET state = 'halted' WHERE id = ?")
                    .bind(task_id)
                    .execute(self.tx().as_mut()),
            )?;
            rt_block_on(
                sqlx::query("DELETE FROM task_timeouts WHERE id = ?")
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

        // Lock the task first
        rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = ? FOR UPDATE")
                .bind(task_id)
                .fetch_optional(self.tx().as_mut()),
        )?;

        let res = rt_block_on(
            sqlx::query("UPDATE tasks SET state = 'pending' WHERE id = ? AND state = 'halted'")
                .bind(task_id)
                .execute(self.tx().as_mut()),
        )?;
        let continued = res.rows_affected() > 0;

        if continued {
            rt_block_on(
                sqlx::query(
                    "INSERT IGNORE INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)"
                ).bind(time + trt).bind(task_id).bind(trt)
                 .execute(self.tx().as_mut())
            )?;
            rt_block_on(
                sqlx::query(
                    "INSERT INTO outgoing_execute (id, version, address)
                     SELECT t.id, t.version, p.target FROM tasks t JOIN promises p ON p.id = t.id WHERE t.id = ?
                     ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)"
                ).bind(task_id).execute(self.tx().as_mut())
            )?;
        }

        let task_exists = rt_block_on(
            sqlx::query("SELECT id FROM tasks WHERE id = ?")
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
                "SELECT t.id, t.state, t.version,
                   CASE WHEN tt.timeout_type = 1 THEN tt.ttl ELSE NULL END AS ttl,
                   CASE WHEN tt.timeout_type = 1 THEN tt.process_id ELSE NULL END AS pid,
                   COALESCE(
                     (SELECT CAST(COUNT(*) AS SIGNED) FROM callbacks c WHERE c.awaiter_id = t.id AND c.ready = true),
                     0
                   ) AS resumes
                 FROM tasks t LEFT JOIN task_timeouts tt ON tt.id = t.id
                 WHERE (? IS NULL OR t.state = ?) AND (? IS NULL OR t.id > ?)
                 ORDER BY t.id ASC LIMIT ?",
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

        if res.rows_affected() > 0 {
            rt_block_on(
                sqlx::query("INSERT IGNORE INTO schedule_timeouts (timeout_at, id) VALUES (?, ?)")
                    .bind(next_run_at)
                    .bind(id)
                    .execute(self.tx().as_mut()),
            )?;
        }

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

    fn get_expired_schedule_timeouts(&self, time: i64) -> StorageResult<Vec<(String, i64)>> {
        let rows = rt_block_on(
            sqlx::query("SELECT id, timeout_at FROM schedule_timeouts WHERE timeout_at <= ?")
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

        // 1. Verify schedule timeout matches
        let timeout_row = rt_block_on(
            sqlx::query("SELECT id FROM schedule_timeouts WHERE id = ? AND timeout_at = ?")
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
            if already_timedout {
                // Promise is immediately settled — create fulfilled task if resonate:target is set
                if address.is_some() {
                    rt_block_on(
                        sqlx::query("INSERT IGNORE INTO tasks (id, state) VALUES (?, 'fulfilled')")
                            .bind(&computed_promise_id)
                            .execute(self.tx().as_mut()),
                    )?;
                }
            } else {
                // 6a. Promise timeout
                rt_block_on(
                    sqlx::query(
                        "INSERT IGNORE INTO promise_timeouts (timeout_at, id) VALUES (?, ?)",
                    )
                    .bind(computed_timeout_at)
                    .bind(&computed_promise_id)
                    .execute(self.tx().as_mut()),
                )?;

                // 6b. Task infrastructure if address is present
                if let Some(ref addr) = address {
                    let task_res = rt_block_on(
                        sqlx::query("INSERT IGNORE INTO tasks (id, state) VALUES (?, 'pending')")
                            .bind(&computed_promise_id)
                            .execute(self.tx().as_mut()),
                    )?;
                    if task_res.rows_affected() > 0 {
                        rt_block_on(
                            sqlx::query(
                                "INSERT IGNORE INTO task_timeouts (timeout_at, id, timeout_type, ttl) VALUES (?, ?, 0, ?)"
                            ).bind(fired_at + trt).bind(&computed_promise_id).bind(trt)
                             .execute(self.tx().as_mut())
                        )?;
                        rt_block_on(
                            sqlx::query(
                                "INSERT INTO outgoing_execute (id, version, address) VALUES (?, 0, ?)
                                 ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)"
                            ).bind(&computed_promise_id).bind(addr)
                             .execute(self.tx().as_mut())
                        )?;
                    }
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

        // 8. Update schedule timeout
        rt_block_on(
            sqlx::query("UPDATE schedule_timeouts SET timeout_at = ? WHERE id = ?")
                .bind(next_run_at)
                .bind(schedule_id)
                .execute(self.tx().as_mut()),
        )?;

        // 9. Return updated schedule record
        self.schedule_get(schedule_id)
    }

    fn process_timeouts(&self, time: i64) -> StorageResult<()> {
        let trt = self.task_retry_timeout;

        // Statement 1: Expire all pending promises with timeout_at <= time
        let expired_rows = rt_block_on(
            sqlx::query("SELECT id FROM promises WHERE state = 'pending' AND timeout_at <= ?")
                .bind(time)
                .fetch_all(self.tx().as_mut()),
        )?;

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

        // Statement 2: Process expired task retry timeouts (type 0, pending tasks)
        let retry_rows = rt_block_on(
            sqlx::query(
                "SELECT tt.id FROM task_timeouts tt JOIN tasks t ON t.id = tt.id
                 WHERE tt.timeout_type = 0 AND tt.timeout_at <= ? AND t.state = 'pending'",
            )
            .bind(time)
            .fetch_all(self.tx().as_mut()),
        )?;

        if !retry_rows.is_empty() {
            let retry_ids: Vec<String> = retry_rows
                .iter()
                .map(|r| r.get::<String, _>("id"))
                .collect();
            let n = retry_ids.len();
            let ph = |k: usize| -> String { vec!["?"; k].join(", ") };

            {
                let sql = format!(
                    "UPDATE task_timeouts SET timeout_at = ? + ?, process_id = NULL WHERE id IN ({})",
                    ph(n)
                );
                let mut q = sqlx::query(&sql).bind(time).bind(trt);
                for id in &retry_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }

            {
                let sql = format!(
                    "INSERT INTO outgoing_execute (id, version, address)
                     SELECT t.id, t.version, p.target FROM tasks t JOIN promises p ON p.id = t.id
                     WHERE t.id IN ({})
                     ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                    ph(n)
                );
                let mut q = sqlx::query(&sql);
                for id in &retry_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }
        }

        // Statement 3: Process expired task lease timeouts (type 1, acquired tasks)
        let lease_rows = rt_block_on(
            sqlx::query(
                "SELECT tt.id FROM task_timeouts tt JOIN tasks t ON t.id = tt.id
                 WHERE tt.timeout_type = 1 AND tt.timeout_at <= ? AND t.state = 'acquired'",
            )
            .bind(time)
            .fetch_all(self.tx().as_mut()),
        )?;

        if !lease_rows.is_empty() {
            let lease_ids: Vec<String> = lease_rows
                .iter()
                .map(|r| r.get::<String, _>("id"))
                .collect();
            let n = lease_ids.len();
            let ph = |k: usize| -> String { vec!["?"; k].join(", ") };

            {
                let sql = format!("UPDATE tasks SET state = 'pending' WHERE id IN ({})", ph(n));
                let mut q = sqlx::query(&sql);
                for id in &lease_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }

            {
                let sql = format!(
                    "UPDATE task_timeouts
                     SET timeout_at = ? + ?, timeout_type = 0, process_id = NULL, ttl = ?
                     WHERE id IN ({})",
                    ph(n)
                );
                let mut q = sqlx::query(&sql).bind(time).bind(trt).bind(trt);
                for id in &lease_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }

            {
                let sql = format!(
                    "INSERT INTO outgoing_execute (id, version, address)
                     SELECT t.id, t.version, p.target FROM tasks t JOIN promises p ON p.id = t.id
                     WHERE t.id IN ({})
                     ON DUPLICATE KEY UPDATE version = VALUES(version), address = VALUES(address)",
                    ph(n)
                );
                let mut q = sqlx::query(&sql);
                for id in &lease_ids {
                    q = q.bind(id.as_str());
                }
                rt_block_on(q.execute(self.tx().as_mut()))?;
            }
        }

        Ok(())
    }

    fn ping(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql("SELECT 1").execute(self.tx().as_mut()))?;
        Ok(())
    }

    fn debug_reset(&self) -> StorageResult<()> {
        rt_block_on(sqlx::raw_sql("DELETE FROM outgoing_unblock").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM outgoing_execute").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM task_timeouts").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM listeners").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM callbacks").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM schedule_timeouts").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM promise_timeouts").execute(self.tx().as_mut()))?;
        rt_block_on(sqlx::raw_sql("DELETE FROM tasks").execute(self.tx().as_mut()))?;
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
                "SELECT t.id, t.state, t.version,
                   CASE WHEN tt.timeout_type = 1 THEN tt.ttl ELSE NULL END AS ttl,
                   CASE WHEN tt.timeout_type = 1 THEN tt.process_id ELSE NULL END AS pid,
                   COALESCE(
                     (SELECT CAST(COUNT(*) AS SIGNED) FROM callbacks c WHERE c.awaiter_id = t.id AND c.ready = true),
                     0
                   ) AS resumes
                 FROM tasks t LEFT JOIN task_timeouts tt ON tt.id = t.id ORDER BY t.id",
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
            messages.push(SnapshotMessage {
                address,
                message: serde_json::json!({ "kind": "execute", "head": {}, "data": { "task": { "id": id, "version": version } } }),
            });
        }

        let unblock_rows = rt_block_on(
            sqlx::query(
                "SELECT ou.promise_id, ou.address, p.id, p.state, p.param_headers, p.param_data, p.value_headers, p.value_data, p.tags, p.timeout_at, p.created_at, p.settled_at
                 FROM outgoing_unblock ou JOIN promises p ON p.id = ou.promise_id
                 ORDER BY ou.promise_id, ou.address",
            )
            .fetch_all(self.tx().as_mut()),
        )?;
        for r in &unblock_rows {
            let address: String = r.get::<String, _>(1);
            let promise = row_to_promise_offset(r, 2);
            messages.push(SnapshotMessage {
                address,
                message: serde_json::json!({ "kind": "unblock", "head": {}, "data": { "promise": promise } }),
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
        // Atomically claim execute messages: SELECT-lock then DELETE
        let exec_rows = rt_block_on(
            sqlx::query("SELECT id, version, address FROM outgoing_execute LIMIT ? FOR UPDATE")
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

        if !execute_msgs.is_empty() {
            let ph = vec!["?"; execute_msgs.len()].join(", ");
            let sql = format!("DELETE FROM outgoing_execute WHERE id IN ({})", ph);
            let mut q = sqlx::query(&sql);
            for m in &execute_msgs {
                q = q.bind(m.id.as_str());
            }
            rt_block_on(q.execute(self.tx().as_mut()))?;
        }

        // Step 1: Lock and capture unblock message keys
        let unblock_key_rows = rt_block_on(
            sqlx::query("SELECT promise_id, address FROM outgoing_unblock LIMIT ? FOR UPDATE")
                .bind(batch_size)
                .fetch_all(self.tx().as_mut()),
        )?;

        let unblock_keys: Vec<(String, String)> = unblock_key_rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("promise_id"),
                    r.get::<String, _>("address"),
                )
            })
            .collect();

        let unblock_msgs: Vec<OutgoingUnblock> = if unblock_keys.is_empty() {
            Vec::new()
        } else {
            // Step 2: Fetch full promise data for each key
            let promise_ids: Vec<String> =
                unblock_keys.iter().map(|(pid, _)| pid.clone()).collect();
            let ph = vec!["?"; promise_ids.len()].join(", ");
            let sql = format!(
                "SELECT id, state, param_headers, param_data, value_headers, value_data,
                        tags, timeout_at, created_at, settled_at
                 FROM promises WHERE id IN ({})",
                ph
            );
            let mut q = sqlx::query(&sql);
            for pid in &promise_ids {
                q = q.bind(pid.as_str());
            }
            let promise_rows = rt_block_on(q.fetch_all(self.tx().as_mut()))?;

            // Build a map from promise_id to PromiseRecord
            let mut promise_map: std::collections::HashMap<String, PromiseRecord> =
                std::collections::HashMap::new();
            for row in &promise_rows {
                let rec = row_to_promise(row);
                promise_map.insert(rec.id.clone(), rec);
            }

            unblock_keys
                .iter()
                .filter_map(|(pid, addr)| {
                    promise_map.get(pid).map(|p| OutgoingUnblock {
                        address: addr.clone(),
                        promise: p.clone(),
                    })
                })
                .collect()
        };

        // Step 3: Delete claimed unblock messages
        if !unblock_keys.is_empty() {
            for (pid, addr) in &unblock_keys {
                rt_block_on(
                    sqlx::query(
                        "DELETE FROM outgoing_unblock WHERE promise_id = ? AND address = ?",
                    )
                    .bind(pid.as_str())
                    .bind(addr.as_str())
                    .execute(self.tx().as_mut()),
                )?;
            }
        }

        Ok((execute_msgs, unblock_msgs))
    }
}
