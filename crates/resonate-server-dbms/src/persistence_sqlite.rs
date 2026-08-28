//! The SQLite backend.
//!
//! A promise is one row. A task is columns on that row, and the timeout queues
//! are derived from it rather than stored beside it. The table on the left is
//! the relational shape this replaced — worth keeping, because every predicate
//! below is a membership rule one of those tables used to carry:
//!
//! | was                                 | is                                                  |
//! |-------------------------------------|-----------------------------------------------------|
//! | `promises`                          | `id, state, param_*, value_*, tags, *_at`           |
//! | `promise_timeouts(timeout_at, id)`  | *derived*: `state = 'pending' AND target IS NOT NULL`|
//! | `tasks(id, state, version)`         | `task_state` (NULL ⟺ no task), `task_version`       |
//! | `task_timeouts` type 0 (retry)      | `retry_at`   (live ⟺ `task_state = 'pending'`)      |
//! | `task_timeouts` type 1 (lease)      | `expires_at` (live ⟺ `task_state = 'acquired'`), `ttl`, `pid` |
//! | `schedule_timeouts(timeout_at, id)` | `schedules.next_run_at`                             |
//!
//! Ten tables became six. `callbacks` and `listeners` stay relational, and so
//! do the two outgoing tables: Postgres folds the first two into TEXT[] columns
//! and the last two into an `outbox`, but SQLite has no array type — 43 array
//! operations have no equivalent here that is not a worse JSON one — and with
//! the arrays gone there is nothing for one outbox table to simplify.
//!
//! # What a derived queue costs
//!
//! A row leaves a queue by no longer matching its predicate, not by being
//! deleted, so a stale deadline can outlive the state that owned it. The
//! predicates above are written to make that harmless: `retry_at` left over
//! from a task that has since been acquired is invisible, because the retry
//! sweep also demands `task_state = 'pending'`. Writers clear the column
//! anyway on every transition, so the two agree even when only one is
//! consulted.
//!
//! One case the derivation does not reproduce exactly, shared with Postgres and
//! MySQL: `task.create` used to put every promise it created on the eager
//! sweep, targeted or not, while `state = 'pending' AND target IS NOT NULL`
//! admits only targeted ones. An untargeted `task.create` promise therefore
//! times out lazily, through `try_timeout` — the way an untargeted
//! `promise.create` always has — rather than through `process_timeouts`.

use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};

use super::{
    Db, OutgoingExecute, OutgoingUnblock, PromiseCreateParams, PromiseCreateResult,
    PromiseSettleParams, PromiseSettleResult, RegisterCallbackResult, ScheduleCreateParams,
    StorageResult, TaskAcquireParams, TaskAcquireResult, TaskContinueResult, TaskCreateParams,
    TaskCreateResult, TaskFenceCreateParams, TaskFenceResult, TaskFenceSettleParams,
    TaskFulfillParams, TaskFulfillResult, TaskHaltResult, TaskReleaseResult, TaskSuspendResult,
};
use resonate_core::types::{
    PromiseRecord, PromiseState, PromiseValue, ScheduleRecord, Snapshot, SnapshotCallback,
    SnapshotListener, SnapshotMessage, SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskRecord,
    TaskState,
};

fn parse_promise_state(s: &str) -> PromiseState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt promise state in DB: {}", e))
}

fn parse_task_state(s: &str) -> TaskState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt task state in DB: {}", e))
}

/// Initialize database: set pragmas and create schema
pub fn init_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
        ",
    )?;
    create_schema(conn)?;
    Ok(())
}

fn create_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS promises (
          id TEXT PRIMARY KEY,
          state TEXT NOT NULL DEFAULT 'pending'
            CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout')),
          param_headers TEXT,
          param_data TEXT,
          value_headers TEXT,
          value_data TEXT,
          tags TEXT NOT NULL DEFAULT '{}',
          target TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:target')) STORED,
          origin TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:origin')) STORED,
          branch TEXT GENERATED ALWAYS AS (json_extract(tags, '$.resonate:branch')) STORED,
          timer BOOLEAN NOT NULL GENERATED ALWAYS AS (COALESCE(json_extract(tags, '$.resonate:timer'), '') = 'true') STORED,
          timeout_at BIGINT NOT NULL,
          created_at BIGINT NOT NULL,
          settled_at BIGINT,

          -- was the `tasks` table. NULL task_state means this promise has no
          -- task, which is what `LEFT JOIN tasks` used to express.
          task_state TEXT
            CHECK (task_state IS NULL OR task_state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled')),
          task_version INT NOT NULL DEFAULT 0,

          -- was `task_timeouts`, whose timeout_type discriminated two queues.
          -- Two nullable columns say the same thing without the row.
          retry_at   BIGINT,   -- type 0: redispatch a pending task
          expires_at BIGINT,   -- type 1: an acquired task's lease
          ttl        BIGINT,
          pid        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_promises_timeout_at ON promises (timeout_at) WHERE state = 'pending';
        CREATE INDEX IF NOT EXISTS idx_promises_target ON promises (target) WHERE target IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_promises_branch ON promises (branch) WHERE branch IS NOT NULL;

        -- `promise_timeouts` is gone: a pending promise past its timeout_at is
        -- exactly the queue, and the partial index above is the same index the
        -- table carried.
        CREATE INDEX IF NOT EXISTS idx_promises_retry_at ON promises (retry_at ASC, id ASC) WHERE retry_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_promises_expires_at ON promises (expires_at ASC, id ASC) WHERE expires_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_promises_pid ON promises (pid) WHERE pid IS NOT NULL;

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

        CREATE TABLE IF NOT EXISTS outgoing_execute (
          id TEXT PRIMARY KEY REFERENCES promises(id) ON DELETE CASCADE,
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
          promise_param_headers TEXT,
          promise_param_data TEXT,
          promise_tags TEXT NOT NULL DEFAULT '{}',
          created_at BIGINT NOT NULL,
          next_run_at BIGINT NOT NULL,
          last_run_at BIGINT
        );
        -- `schedule_timeouts` is gone: next_run_at already is the queue.
        CREATE INDEX IF NOT EXISTS idx_schedules_next_run_at ON schedules (next_run_at ASC, id ASC);
        ",
    )?;
    Ok(())
}

pub struct SqliteStorage {
    conn: Arc<Mutex<Connection>>,
    task_retry_timeout: i64,
}

impl SqliteStorage {
    pub fn open(path: &str, task_retry_timeout: i64) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        init_db(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            task_retry_timeout,
        })
    }

    pub async fn transact<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(feature = "concurrency-stress")]
        tokio::task::yield_now().await;

        let mut f = f;
        let conn = Arc::clone(&self.conn);
        let task_retry_timeout = self.task_retry_timeout;
        tokio::task::block_in_place(|| {
            // Use unwrap_or_else to recover from poisoned mutex (a prior panic
            // while holding the lock). The connection itself is still valid.
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let tx = conn.unchecked_transaction()?;
            let db = SqliteDb {
                conn: &tx,
                task_retry_timeout,
            };
            let result = f(&db)?;
            tx.commit()?;
            Ok(result)
        })
    }

    pub async fn query<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(feature = "concurrency-stress")]
        tokio::task::yield_now().await;

        let mut f = f;
        let conn = Arc::clone(&self.conn);
        let task_retry_timeout = self.task_retry_timeout;
        tokio::task::block_in_place(|| {
            let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
            let db = SqliteDb {
                conn: &conn,
                task_retry_timeout,
            };
            f(&db)
        })
    }
}

struct SqliteDb<'a> {
    conn: &'a rusqlite::Connection,
    task_retry_timeout: i64,
}

// === Settlement chain helpers (multi-statement within the transaction) ===

/// SettlementEnqueued: fulfill task, drop its timeout, delete callbacks by awaiter.
///
/// Fulfilling and dropping the timeout were two statements against two tables;
/// they are one row now, so they are one `SET`. Clearing `retry_at`/`expires_at`
/// is what deleting the `task_timeouts` row used to be, and `ttl`/`pid` go with
/// the lease that just ended.
fn settlement_enqueued(tx: &rusqlite::Connection, promise_id: &str) -> rusqlite::Result<bool> {
    let fulfilled = tx.execute(
        "UPDATE promises SET task_state = 'fulfilled',
                             retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
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
    tx: &rusqlite::Connection,
    awaited_id: &str,
    time: i64,
    task_retry_timeout: i64,
    exclude_fulfilled: Option<&[String]>,
) -> rusqlite::Result<()> {
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
        // and move the task onto the retry queue. Writing `retry_at` and
        // clearing `expires_at` is the whole of what switching the timeout row
        // from type 1 to type 0 used to be.
        let updated = tx.execute(
            "UPDATE promises SET task_state = 'pending', retry_at = ?2,
                                 expires_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_state = 'suspended'",
            params![awaiter_id, time + task_retry_timeout],
        )?;
        if updated > 0 {
            // Insert/update outgoing execute
            let (version, target): (i64, Option<String>) = tx.query_row(
                "SELECT task_version, target FROM promises WHERE id = ?1",
                params![awaiter_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if let Some(target) = target {
                tx.execute(
                    "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3)
                     ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                    params![awaiter_id, version, target],
                )?;
            }
        }
    }
    Ok(())
}

/// ListenerUnblocked: insert outgoing unblock messages, delete listeners
fn listener_unblocked(tx: &rusqlite::Connection, promise_id: &str) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outgoing_unblock (promise_id, address)
         SELECT l.promise_id, l.address FROM listeners l WHERE l.promise_id = ?1
         ON CONFLICT DO NOTHING",
        params![promise_id],
    )?;
    tx.execute(
        "DELETE FROM listeners WHERE promise_id = ?1",
        params![promise_id],
    )?;
    Ok(())
}

/// Full settlement chain: settle promise + SettlementEnqueued + ResumptionEnqueued + ListenerUnblocked
#[allow(clippy::too_many_arguments)]
fn settle_promise(
    tx: &rusqlite::Connection,
    id: &str,
    state: &str,
    value_headers: Option<&str>,
    value_data: Option<&str>,
    settled_at: i64,
    time: i64,
    task_retry_timeout: i64,
) -> rusqlite::Result<bool> {
    let updated = tx.execute(
        "UPDATE promises SET state = ?2, value_headers = ?3, value_data = ?4, settled_at = ?5 WHERE id = ?1 AND state = 'pending'",
        params![id, state, value_headers, value_data, settled_at],
    )?;
    if updated == 0 {
        return Ok(false);
    }

    // No promise timeout to delete: the queue is `state = 'pending'`, and the
    // UPDATE above just took this row out of it.
    settlement_enqueued(tx, id)?;
    resumption_enqueued(tx, id, time, task_retry_timeout, None)?;
    listener_unblocked(tx, id)?;
    Ok(true)
}

impl<'a> Db for SqliteDb<'a> {
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

    fn process_callbacks(&self, _promise_id: &str, _time: i64) -> StorageResult<()> {
        Ok(())
    }

    fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids_json = serde_json::to_string(ids).unwrap();
        // Find expired promises from the ID set
        let mut stmt = self.conn.prepare(
            "SELECT id, timer, timeout_at FROM promises
             WHERE id IN (SELECT value FROM json_each(?1))
               AND state = 'pending' AND timeout_at <= ?2",
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
        for (id, timer, timeout_at) in &expired {
            let new_state = if *timer {
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
                self.conn,
                id,
                time,
                self.task_retry_timeout,
                Some(&fulfilled_ids),
            )?;
        }

        // ListenerUnblocked for each expired
        for (id, _, _) in &expired {
            listener_unblocked(self.conn, id)?;
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
            // is written either way — `state = 'pending' AND target IS NOT NULL`
            // is the queue, and the INSERT above already put the row in it.
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
                    "UPDATE promises SET task_state = 'pending', task_version = 0, retry_at = ?2
                     WHERE id = ?1 AND task_state IS NULL",
                    params![id, created_at + self.task_retry_timeout],
                )? > 0;
                if created {
                    self.conn.execute(
                        "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, 0, ?2)
                         ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                        params![id, addr],
                    )?;
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
            self.conn,
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
                    "UPDATE promises SET task_state = 'pending', retry_at = ?2,
                                         expires_at = NULL, ttl = NULL, pid = NULL
                     WHERE id = ?1 AND task_state = 'suspended'",
                    params![awaiter_id, time + self.task_retry_timeout],
                )?;
                if updated > 0 {
                    let (version, target): (i64, Option<String>) = self.conn.query_row(
                        "SELECT task_version, target FROM promises WHERE id = ?1",
                        params![awaiter_id],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )?;
                    if let Some(target) = target {
                        self.conn.execute(
                            "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3)
                             ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                            params![awaiter_id, version, target],
                        )?;
                    }
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
            if p.state == PromiseState::Pending {
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
            // written with the state that owns them: `expires_at`/`ttl`/`pid`
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
                                         expires_at = ?2, ttl = ?3, pid = ?4
                     WHERE id = ?1 AND task_state IS NULL",
                    params![promise_id, created_at + ttl, ttl, pid],
                )? > 0
            };
            if inserted {
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
        // type-0 row becomes a type-1 row by clearing `retry_at` and setting
        // `expires_at`, `ttl` and `pid`.
        let updated = self.conn.execute(
            "UPDATE promises SET task_state = 'acquired', task_version = task_version + 1,
                                 retry_at = NULL, expires_at = ?3, ttl = ?4, pid = ?5
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
            self.conn,
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
            // TODO: also guard that the promise is still active
            // (`state = 'pending' AND timeout_at > ?1`), so heartbeats on tasks
            // whose promise has already timed out are no-ops.
            self.conn.execute(
                "UPDATE promises SET expires_at = ?1 + ttl
                 WHERE id = ?2 AND pid = ?3 AND task_version = ?4 AND task_state = 'acquired'",
                params![time, task_id, pid, version],
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
            });
        }

        // Check each awaited promise — count missing and settled
        let mut found_count = 0;
        let mut has_settled = false;
        for aid in awaited_ids {
            if let Some(p) = self.promise_get(aid)? {
                found_count += 1;
                if p.state != PromiseState::Pending {
                    has_settled = true;
                }
            }
        }

        let missing_count = awaited_ids.len() as i32 - found_count;

        // Can only suspend if: task matched, no missing, all pending
        let can_suspend = missing_count == 0 && !has_settled;

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
                                     retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ?1 AND task_version = ?2 AND task_state = 'acquired'",
                params![task_id, version],
            )?;

            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: true,
                missing_count: 0,
            })
        } else if missing_count == 0 {
            // Immediate resume — has_settled is true, delete ready callbacks
            self.conn.execute(
                "DELETE FROM callbacks WHERE awaiter_id = ?1 AND ready = true",
                params![task_id],
            )?;
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count: 0,
            })
        } else {
            Ok(TaskSuspendResult {
                task_matched: true,
                was_suspended: false,
                missing_count,
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
                                 retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_version = ?2 AND task_state = 'acquired'",
            params![task_id, version],
        )? > 0;

        if task_fulfilled {
            // Settle the promise
            settle_promise(
                self.conn,
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
        // queue: `expires_at` out, `retry_at` in.
        let task_released = self.conn.execute(
            "UPDATE promises SET task_state = 'pending', retry_at = ?3,
                                 expires_at = NULL, ttl = NULL, pid = NULL
             WHERE id = ?1 AND task_version = ?2 AND task_state = 'acquired'",
            params![task_id, version, time + ttl],
        )? > 0;

        if task_released {
            // Insert outgoing execute
            let (new_version, target): (i64, Option<String>) = self.conn.query_row(
                "SELECT task_version, target FROM promises WHERE id = ?1",
                params![task_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if let Some(target) = target {
                self.conn.execute(
                    "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3)
                     ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                    params![task_id, new_version, target],
                )?;
            }
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
                                 retry_at = NULL, expires_at = NULL, ttl = NULL, pid = NULL
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
            "UPDATE promises SET task_state = 'pending', retry_at = ?2
             WHERE id = ?1 AND task_state = 'halted'",
            params![task_id, time + self.task_retry_timeout],
        )? > 0;

        if continued {
            let (version, target): (i64, Option<String>) = self.conn.query_row(
                "SELECT task_version, target FROM promises WHERE id = ?1",
                params![task_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if let Some(target) = target {
                self.conn.execute(
                    "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3)
                     ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                    params![task_id, version, target],
                )?;
            }
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
                "SELECT branch FROM promises WHERE id = ?1",
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
             FROM promises WHERE branch = ?1 AND id != ?2 ORDER BY id ASC",
        )?;
        let mut rows = stmt.query(params![branch, promise_id])?;
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
        self.conn.execute(
            "INSERT OR IGNORE INTO schedules (id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![id, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, created_at, next_run_at],
        )?;
        // No schedule timeout to insert: `next_run_at` on the row above is it.
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

    fn get_expired_schedule_timeouts(&self, time: i64) -> StorageResult<Vec<(String, i64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, next_run_at FROM schedules WHERE next_run_at <= ?1")?;
        let mut rows = stmt.query(params![time])?;
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
            // put a pending, targeted promise on the queue.
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
                    "UPDATE promises SET task_state = 'pending', task_version = 0, retry_at = ?2
                     WHERE id = ?1 AND task_state IS NULL",
                    params![promise_id, time + self.task_retry_timeout],
                )? > 0;
                if created {
                    self.conn.execute(
                        "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, 0, ?2)
                         ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                        params![promise_id, addr],
                    )?;
                }
            }
        }

        // Step 8: Advance schedule
        self.conn.execute(
            "UPDATE schedules SET last_run_at = ?1, next_run_at = ?2 WHERE id = ?3",
            params![fired_at, next_run_at, schedule_id],
        )?;

        // Step 9 is gone: advancing the schedule above advanced the queue.

        // Step 10: Return updated schedule
        self.schedule_get(schedule_id)
    }

    fn ping(&self) -> StorageResult<()> {
        self.conn.execute_batch("SELECT 1")?;
        Ok(())
    }

    fn debug_reset(&self) -> StorageResult<()> {
        self.conn.execute_batch(
            "DELETE FROM outgoing_unblock; DELETE FROM outgoing_execute;
             DELETE FROM listeners; DELETE FROM callbacks;
             DELETE FROM promises; DELETE FROM schedules;",
        )?;
        Ok(())
    }

    fn process_timeouts(&self, time: i64) -> StorageResult<()> {
        // Statement 1: Process expired promise timeouts.
        //
        // `state = 'pending' AND target IS NOT NULL` is the whole of what
        // `promise_timeouts` held: rows entered on create and left on settle,
        // and only a targeted promise was ever swept eagerly. Untargeted ones
        // still time out lazily, through `try_timeout`.
        let mut stmt = self.conn.prepare(
            "SELECT id FROM promises
             WHERE state = 'pending' AND target IS NOT NULL AND timeout_at <= ?1",
        )?;
        let expired_ids: Vec<String> = {
            let mut rows = stmt.query(params![time])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(row.get(0)?);
            }
            r
        };

        // Phase 1: Settle all expired promises
        let mut fulfilled_ids = Vec::new();
        for id in &expired_ids {
            self.conn.execute(
                "UPDATE promises SET state = CASE WHEN timer THEN 'resolved' ELSE 'rejected_timedout' END, settled_at = timeout_at WHERE id = ?1 AND state = 'pending'",
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
                self.conn,
                id,
                time,
                self.task_retry_timeout,
                Some(&fulfilled_ids),
            )?;
            listener_unblocked(self.conn, id)?;
        }

        // Statement 2: Process expired task retry deadlines — what was
        // `timeout_type = 0`, now a non-NULL `retry_at` on a pending task.
        let mut stmt = self.conn.prepare(
            "SELECT id FROM promises
             WHERE task_state = 'pending' AND retry_at IS NOT NULL AND retry_at <= ?1",
        )?;
        let retry_ids: Vec<String> = {
            let mut rows = stmt.query(params![time])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(row.get(0)?);
            }
            r
        };

        for id in &retry_ids {
            self.conn.execute(
                "UPDATE promises SET retry_at = ?1 + ?3, pid = NULL WHERE id = ?2",
                params![time, id, self.task_retry_timeout],
            )?;
            let (version, target): (i64, Option<String>) = self
                .conn
                .query_row(
                    "SELECT task_version, target FROM promises WHERE id = ?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or((0, None));
            if let Some(target) = target {
                self.conn.execute(
                    "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3)
                     ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                    params![id, version, target],
                )?;
            }
        }

        // Statement 3: Process expired leases — what was `timeout_type = 1`,
        // now a non-NULL `expires_at` on an acquired task. The holder went
        // away; hand the task back to the retry queue.
        let mut stmt = self.conn.prepare(
            "SELECT id FROM promises
             WHERE task_state = 'acquired' AND expires_at IS NOT NULL AND expires_at <= ?1",
        )?;
        let lease_ids: Vec<String> = {
            let mut rows = stmt.query(params![time])?;
            let mut r = Vec::new();
            while let Some(row) = rows.next()? {
                r.push(row.get(0)?);
            }
            r
        };

        for id in &lease_ids {
            self.conn.execute(
                "UPDATE promises SET task_state = 'pending', retry_at = ?1 + ?3,
                                     expires_at = NULL, ttl = NULL, pid = NULL
                 WHERE id = ?2",
                params![time, id, self.task_retry_timeout],
            )?;
            let (version, target): (i64, Option<String>) = self
                .conn
                .query_row(
                    "SELECT task_version, target FROM promises WHERE id = ?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap_or((0, None));
            if let Some(target) = target {
                self.conn.execute(
                    "INSERT INTO outgoing_execute (id, version, address) VALUES (?1, ?2, ?3)
                     ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, address = EXCLUDED.address",
                    params![id, version, target],
                )?;
            }
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
             WHERE state = 'pending' AND target IS NOT NULL ORDER BY id",
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
            "SELECT id, 0 AS timeout_type, retry_at AS timeout_at FROM promises
               WHERE task_state = 'pending' AND retry_at IS NOT NULL
             UNION ALL
             SELECT id, 1 AS timeout_type, expires_at AS timeout_at FROM promises
               WHERE task_state = 'acquired' AND expires_at IS NOT NULL
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

        let mut messages: Vec<SnapshotMessage> = Vec::new();

        let mut stmt =
            conn.prepare("SELECT id, version, address FROM outgoing_execute ORDER BY id")?;
        {
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let id: String = row.get(0)?;
                let version: i64 = row.get(1)?;
                let address: String = row.get(2)?;
                messages.push(SnapshotMessage { address, message: serde_json::json!({ "kind": "execute", "head": {}, "data": { "task": { "id": id, "version": version } } }) });
            }
        }

        let mut stmt = conn.prepare(
            "SELECT ou.promise_id, ou.address, p.id, p.state, p.param_headers, p.param_data, p.value_headers, p.value_data, p.tags, p.timeout_at, p.created_at, p.settled_at
             FROM outgoing_unblock ou JOIN promises p ON p.id = ou.promise_id ORDER BY ou.promise_id, ou.address"
        )?;
        {
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let _promise_id: String = row.get(0)?;
                let address: String = row.get(1)?;
                let promise = row_to_promise_offset(row, 2)?;
                messages.push(SnapshotMessage { address, message: serde_json::json!({ "kind": "unblock", "head": {}, "data": { "promise": promise } }) });
            }
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
        let mut execute_msgs = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "DELETE FROM outgoing_execute WHERE rowid IN (SELECT rowid FROM outgoing_execute LIMIT ?1) RETURNING id, version, address"
            )?;
            let mut rows = stmt.query(params![batch_size])?;
            while let Some(row) = rows.next()? {
                execute_msgs.push(OutgoingExecute {
                    id: row.get(0)?,
                    version: row.get(1)?,
                    address: row.get(2)?,
                });
            }
        }

        // Delete a batch of unblock messages, then join with promises for payload.
        // SQLite doesn't support DELETE in CTE WITH clauses, so we use two steps
        // within the same transaction: DELETE RETURNING, then SELECT per row.
        let mut deleted_unblocks: Vec<(String, String)> = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "DELETE FROM outgoing_unblock WHERE rowid IN (SELECT rowid FROM outgoing_unblock LIMIT ?1) RETURNING promise_id, address"
            )?;
            let mut rows = stmt.query(params![batch_size])?;
            while let Some(row) = rows.next()? {
                deleted_unblocks.push((row.get(0)?, row.get(1)?));
            }
        }

        let mut unblock_msgs = Vec::new();
        for (promise_id, address) in deleted_unblocks {
            let mut stmt = self.conn.prepare(
                "SELECT id, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at FROM promises WHERE id = ?1"
            )?;
            let mut rows = stmt.query(params![promise_id])?;
            if let Some(row) = rows.next()? {
                let promise = row_to_promise_offset(row, 0)?;
                unblock_msgs.push(OutgoingUnblock { address, promise });
            }
        }

        Ok((execute_msgs, unblock_msgs))
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
//   task_timeouts     timeout_type = 0 -> retry_at, timeout_type = 1 -> expires_at.
//                     Two nullable columns, so "which queue" is which column is
//                     non-null rather than a discriminator value. process_id and
//                     ttl became pid and ttl on the promise. Every statement that
//                     deleted the row now nulls the pair, and every statement
//                     that flipped timeout_type now writes one and clears the
//                     other — which is why fulfilling a task, dropping its
//                     timeout and clearing its lease are one UPDATE here.
//
//   promise_timeouts  Gone. The queue is `state = 'pending' AND target IS NOT
//                     NULL`, which is what rows entering on create and leaving
//                     on settle amounted to; idx_promises_timeout_at is the
//                     index the table carried. Untargeted promises were never
//                     swept eagerly and still are not — they time out lazily,
//                     through try_timeout.
//
//   schedule_timeouts Gone: `next_run_at` already is the queue, and
//                     process_schedule_timeout's idempotency guard reads the
//                     schedule row it is about to advance.
