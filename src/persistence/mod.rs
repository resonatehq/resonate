pub mod persistence_mysql;
pub mod persistence_postgres;
pub mod persistence_sqlite;

use std::collections::HashMap;

use crate::core::types::{PromiseRecord, ScheduleRecord, Snapshot, TaskRecord, TaskState};

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// A backend-agnostic storage error. Carries the formatted error message
    /// without exposing the underlying driver type (rusqlite, sqlx, etc.).
    Backend(String),
    /// Serialization conflict — retries exhausted, nothing was committed.
    /// The caller should return 503 (not 500) to indicate a retriable no-op.
    Serialization,
    /// The request contains a field that violates a storage-level constraint
    /// (e.g. a VARCHAR(255) column in MySQL). The caller should return 400.
    InvalidInput(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Backend(msg) => write!(f, "Storage error: {}", msg),
            StorageError::Serialization => write!(f, "Serialization conflict"),
            StorageError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
        }
    }
}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        StorageError::Backend(e.to_string())
    }
}

/// Is this driver error code a retriable write conflict?
///
/// | code    | backend  | meaning                          |
/// |---------|----------|----------------------------------|
/// | `40001` | Postgres | serialization failure            |
/// | `40P01` | Postgres | deadlock detected                |
/// | `1213`  | MySQL    | deadlock found                   |
/// | `1205`  | MySQL    | lock wait timeout exceeded       |
///
/// All four mean nothing was committed and the transaction can be replayed.
///
/// This predicate was previously written out in three places that disagreed:
/// `From<sqlx::Error>` recognised only the Postgres pair, while each backend's
/// commit path re-checked its own pair inline. The consequence was that a
/// MySQL deadlock *inside a query* converted to `Backend` rather than
/// `Serialization` — so MySQL's in-query retry arm could never match, and the
/// conflict surfaced to the client as a non-retriable error instead of being
/// retried. One pure function, one table, one set of tests.
pub fn is_serialization_conflict(code: Option<&str>) -> bool {
    matches!(code, Some("40001" | "40P01" | "1213" | "1205"))
}

/// Whether an `sqlx` error is a retriable write conflict.
pub fn is_sqlx_serialization_conflict(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db_err| db_err.code())
        .is_some_and(|code| is_serialization_conflict(Some(code.as_ref())))
}

impl From<sqlx::Error> for StorageError {
    fn from(e: sqlx::Error) -> Self {
        if is_sqlx_serialization_conflict(&e) {
            return StorageError::Serialization;
        }
        StorageError::Backend(e.to_string())
    }
}

// === Result types for CTE-based operations ===

pub struct PromiseCreateResult {
    /// Whether the promise was newly inserted (false = already existed).
    pub was_created: bool,
    pub promise: PromiseRecord,
}

pub struct PromiseSettleResult {
    /// Whether the promise was actually transitioned from pending to settled.
    pub was_settled: bool,
    /// `None` when the promise was not found in the database.
    pub promise: Option<PromiseRecord>,
}

pub struct RegisterCallbackResult {
    pub awaited: Option<PromiseRecord>,
    pub awaiter: Option<PromiseRecord>,
}

pub struct TaskCreateResult {
    pub promise: PromiseRecord,
    pub task_created: bool,
    pub task_state: Option<String>,
    pub task_version: Option<i64>,
}

pub struct TaskAcquireResult {
    pub promise: Option<PromiseRecord>,
    pub was_acquired: bool,
    pub task_state: Option<TaskState>,
    pub task_version: Option<i64>,
}

pub struct TaskFenceResult {
    pub task_exists: bool,
    pub fence_ok: bool,
    pub promise: Option<PromiseRecord>,
}

pub struct TaskSuspendResult {
    pub task_matched: bool,
    pub was_suspended: bool,
    pub missing_count: i32,
}

pub struct TaskReleaseResult {
    pub task_released: bool,
    pub task_exists: bool,
}

pub struct TaskFulfillResult {
    pub task_exists: bool,
    /// Whether the task was actually transitioned to fulfilled.
    pub task_fulfilled: bool,
    /// `None` when the promise was not found in the database.
    pub promise: Option<PromiseRecord>,
}

pub struct TaskHaltResult {
    pub task_exists: bool,
    pub task_fulfilled: bool,
}

pub struct TaskContinueResult {
    pub task_exists: bool,
    pub continued: bool,
}

pub struct OutgoingExecute {
    pub id: String,
    pub version: i64,
    pub address: String,
}

pub struct OutgoingUnblock {
    pub address: String,
    pub promise: PromiseRecord,
}

// === Parameter structs for Db trait methods ===

pub struct PromiseCreateParams<'a> {
    pub id: &'a str,
    pub state: &'a str,
    pub param_headers: Option<&'a str>,
    pub param_data: Option<&'a str>,
    pub tags: &'a str,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub already_timedout: bool,
    pub address: Option<&'a str>,
}

pub struct PromiseSettleParams<'a> {
    pub id: &'a str,
    pub state: &'a str,
    pub value_headers: Option<&'a str>,
    pub value_data: Option<&'a str>,
    pub settled_at: i64,
}

pub struct TaskCreateParams<'a> {
    pub promise_id: &'a str,
    pub state: &'a str,
    pub param_headers: Option<&'a str>,
    pub param_data: Option<&'a str>,
    pub tags: &'a str,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub already_timedout: bool,
    pub ttl: i64,
    pub pid: &'a str,
}

pub struct TaskAcquireParams<'a> {
    pub task_id: &'a str,
    pub version: i64,
    pub time: i64,
    pub ttl: i64,
    pub pid: &'a str,
}

pub struct TaskFenceCreateParams<'a> {
    pub task_id: &'a str,
    pub version: i64,
    pub promise_id: &'a str,
    pub state: &'a str,
    pub param_headers: Option<&'a str>,
    pub param_data: Option<&'a str>,
    pub tags: &'a str,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub already_timedout: bool,
    pub address: Option<&'a str>,
}

pub struct TaskFenceSettleParams<'a> {
    pub task_id: &'a str,
    pub version: i64,
    pub promise_id: &'a str,
    pub state: &'a str,
    pub value_headers: Option<&'a str>,
    pub value_data: Option<&'a str>,
    pub settled_at: i64,
}

pub struct TaskFulfillParams<'a> {
    pub task_id: &'a str,
    pub version: i64,
    pub promise_id: &'a str,
    pub state: &'a str,
    pub value_headers: Option<&'a str>,
    pub value_data: Option<&'a str>,
    pub settled_at: i64,
}

pub struct ScheduleCreateParams<'a> {
    pub id: &'a str,
    pub cron: &'a str,
    pub promise_id: &'a str,
    pub promise_timeout: i64,
    pub promise_param_headers: Option<&'a str>,
    pub promise_param_data: Option<&'a str>,
    pub promise_tags: &'a str,
    pub created_at: i64,
    pub next_run_at: i64,
}

/// The Db trait — CTE-based operations within a transaction
pub trait Db {
    /// Returns the configured task retry timeout in milliseconds.
    /// Used wherever a pending task timeout is inserted or reset.
    fn task_retry_timeout(&self) -> i64;

    // Ghost operation — runs before every user operation
    fn try_timeout(&self, ids: &[&str], time: i64) -> StorageResult<()>;
    fn lock_for_update(&self, id: &str) -> StorageResult<(bool, bool)>;
    fn process_callbacks(&self, promise_id: &str, time: i64) -> StorageResult<()>;

    // === Promise operations ===
    fn promise_get(&self, id: &str) -> StorageResult<Option<PromiseRecord>>;

    fn promise_create(&self, params: &PromiseCreateParams) -> StorageResult<PromiseCreateResult>;

    fn promise_settle(&self, params: &PromiseSettleParams) -> StorageResult<PromiseSettleResult>;

    fn promise_register_callback(
        &self,
        awaited_id: &str,
        awaiter_id: &str,
        time: i64,
    ) -> StorageResult<RegisterCallbackResult>;

    fn promise_register_listener(
        &self,
        awaited_id: &str,
        address: &str,
    ) -> StorageResult<Option<PromiseRecord>>;

    fn promise_search(
        &self,
        state: Option<&str>,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<PromiseRecord>>;

    // === Task operations ===
    fn task_get(&self, id: &str) -> StorageResult<Option<TaskRecord>>;

    fn task_create(&self, params: &TaskCreateParams) -> StorageResult<TaskCreateResult>;

    fn task_acquire(&self, params: &TaskAcquireParams) -> StorageResult<TaskAcquireResult>;

    fn task_fence_create(&self, params: &TaskFenceCreateParams) -> StorageResult<TaskFenceResult>;

    fn task_fence_settle(&self, params: &TaskFenceSettleParams) -> StorageResult<TaskFenceResult>;

    fn task_heartbeat(&self, pid: &str, tasks: &[(&str, i64)], time: i64) -> StorageResult<()>;

    fn task_suspend(
        &self,
        task_id: &str,
        version: i64,
        awaited_ids: &[&str],
    ) -> StorageResult<TaskSuspendResult>;

    fn task_fulfill(&self, params: &TaskFulfillParams) -> StorageResult<TaskFulfillResult>;

    fn task_release(
        &self,
        task_id: &str,
        version: i64,
        time: i64,
        ttl: i64,
    ) -> StorageResult<TaskReleaseResult>;

    fn task_halt(&self, task_id: &str) -> StorageResult<TaskHaltResult>;

    fn task_continue(&self, task_id: &str, time: i64) -> StorageResult<TaskContinueResult>;

    fn task_search(
        &self,
        state: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<TaskRecord>>;

    fn compute_preload(&self, promise_id: &str) -> StorageResult<Vec<PromiseRecord>>;

    // === Schedule operations ===
    fn schedule_get(&self, id: &str) -> StorageResult<Option<ScheduleRecord>>;

    fn schedule_create(&self, params: &ScheduleCreateParams) -> StorageResult<ScheduleRecord>;

    fn schedule_delete(&self, id: &str) -> StorageResult<bool>;

    fn schedule_search(
        &self,
        tags: Option<&str>,
        cursor: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<ScheduleRecord>>;

    fn get_expired_schedule_timeouts(&self, time: i64) -> StorageResult<Vec<(String, i64)>>;

    fn process_schedule_timeout(
        &self,
        schedule_id: &str,
        fired_at: i64,
        next_run_at: i64,
        time: i64,
        promise_tags: &HashMap<String, String>,
    ) -> StorageResult<Option<ScheduleRecord>>;

    // === Timeout processing ===
    fn process_timeouts(&self, time: i64) -> StorageResult<()>;

    // === Readiness check ===
    /// Lightweight storage probe: executes `SELECT 1` to verify the backend is responsive.
    fn ping(&self) -> StorageResult<()>;

    // === Debug operations ===
    fn debug_reset(&self) -> StorageResult<()>;
    fn snap(&self) -> StorageResult<Snapshot>;

    // === Outgoing messages (for background delivery) ===
    /// Atomically claim and delete a batch of outgoing messages using DELETE ... RETURNING.
    /// Guarantees at-most-once delivery: messages are removed before delivery is attempted.
    fn take_outgoing(
        &self,
        batch_size: i64,
    ) -> StorageResult<(Vec<OutgoingExecute>, Vec<OutgoingUnblock>)>;
}

/// A storage backend that fails every transaction with a chosen error.
///
/// The [`Db`] trait is the seam, but [`Storage`] is a closed enum of three
/// real databases — so until this existed there was no way to construct a
/// [`Server`](crate::server::Server) that returns a given [`StorageError`].
/// Every error arm in `server.rs` was therefore unreachable from a test, which
/// is precisely why `StorageError::Serialization` went unmapped: producing one
/// required a genuinely racing Postgres.
///
/// It fails at the `transact`/`query` boundary rather than inside a [`Db`]
/// method, which is exactly where the real backends surface a serialization
/// failure after exhausting their retries.
pub struct FailingStorage {
    error: StorageError,
}

impl FailingStorage {
    pub fn new(error: StorageError) -> Self {
        Self { error }
    }

    pub fn error(&self) -> StorageError {
        self.error.clone()
    }
}

/// Enum-based storage to avoid trait object limitations with generic methods
pub enum Storage {
    Sqlite(persistence_sqlite::SqliteStorage),
    Postgres(persistence_postgres::PostgresStorage),
    Mysql(persistence_mysql::MysqlStorage),
    /// A backend that fails every operation. Test-only; never constructed by
    /// `main.rs`, which selects a backend from `config.storage.type`.
    Failing(FailingStorage),
}

impl Storage {
    pub async fn transact<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            Storage::Sqlite(s) => s.transact(f).await,
            Storage::Postgres(p) => p.transact(f, false).await,
            Storage::Mysql(m) => m.transact(f).await,
            Storage::Failing(s) => Err(s.error()),
        }
    }

    pub async fn query<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            Storage::Sqlite(s) => s.query(f).await,
            Storage::Postgres(p) => p.query(f).await,
            Storage::Mysql(m) => m.query(f).await,
            Storage::Failing(s) => Err(s.error()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- retriable write conflicts ----
    //
    // The predicate is a closed table, so this is its whole contract. It is a
    // pure function of a driver code, which is what makes the retry decision
    // testable without inducing a real deadlock against a live database.

    #[test]
    fn postgres_conflict_codes_are_retriable() {
        assert!(
            is_serialization_conflict(Some("40001")),
            "serialization failure"
        );
        assert!(
            is_serialization_conflict(Some("40P01")),
            "deadlock detected"
        );
    }

    /// The regression: these were recognised only by MySQL's *commit* path, so
    /// a deadlock inside a query converted to `Backend` and was never retried.
    #[test]
    fn mysql_conflict_codes_are_retriable() {
        assert!(is_serialization_conflict(Some("1213")), "deadlock found");
        assert!(is_serialization_conflict(Some("1205")), "lock wait timeout");
    }

    #[test]
    fn every_backends_conflict_codes_are_recognised_by_one_predicate() {
        // The point of the shared predicate: the query path and the commit
        // path of both backends agree, because there is only one table.
        for code in ["40001", "40P01", "1213", "1205"] {
            assert!(is_serialization_conflict(Some(code)), "{code} should retry");
        }
    }

    #[test]
    fn ordinary_errors_are_not_retriable() {
        for code in ["23505", "42P01", "1062", "", "40002", "121"] {
            assert!(
                !is_serialization_conflict(Some(code)),
                "{code} must not retry"
            );
        }
        assert!(!is_serialization_conflict(None), "a driverless error");
    }

    // ---- the storage error vocabulary ----

    #[test]
    fn storage_errors_describe_themselves() {
        assert_eq!(
            StorageError::Serialization.to_string(),
            "Serialization conflict"
        );
        assert_eq!(
            StorageError::InvalidInput("too long".into()).to_string(),
            "Invalid input: too long"
        );
        assert_eq!(
            StorageError::Backend("boom".into()).to_string(),
            "Storage error: boom"
        );
    }

    #[test]
    fn a_failing_storage_returns_the_error_it_was_given() {
        for error in [
            StorageError::Serialization,
            StorageError::InvalidInput("bad".into()),
            StorageError::Backend("boom".into()),
        ] {
            assert_eq!(FailingStorage::new(error.clone()).error(), error);
        }
    }
}
