pub mod persistence_postgres;
pub mod persistence_sqlite;

use crate::types::{PromiseRecord, ScheduleRecord, Snapshot, TaskRecord, TaskState};

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    /// A backend-agnostic storage error. Carries the formatted error message
    /// without exposing the underlying driver type (rusqlite, sqlx, etc.).
    Backend(String),
    /// Serialization conflict — retries exhausted, nothing was committed.
    /// The caller should return 503 (not 500) to indicate a retriable no-op.
    Serialization,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Backend(msg) => write!(f, "Storage error: {}", msg),
            StorageError::Serialization => write!(f, "Serialization conflict"),
        }
    }
}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        StorageError::Backend(e.to_string())
    }
}

impl From<sqlx::Error> for StorageError {
    fn from(e: sqlx::Error) -> Self {
        // Detect serialization failures (40001) and deadlocks (40P01) from within queries.
        // Both mean nothing was committed and the transaction can be safely retried.
        if let Some(db_err) = e.as_database_error() {
            let code = db_err.code().map(|c| c.to_string());
            if code.as_deref() == Some("40001") || code.as_deref() == Some("40P01") {
                return StorageError::Serialization;
            }
        }
        StorageError::Backend(e.to_string())
    }
}

// === Result types for CTE-based operations ===

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
    pub task_acquired: bool,
    pub task_state: Option<String>,
    pub task_version: Option<i64>,
}

pub struct TaskAcquireResult {
    pub promise: Option<PromiseRecord>,
    pub was_acquired: bool,
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

pub struct TaskFulfillResult {
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
    pub state: Option<TaskState>,
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

    fn promise_create(&self, params: &PromiseCreateParams) -> StorageResult<PromiseRecord>;

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

    fn task_create(&self, params: &TaskCreateParams) -> StorageResult<Option<TaskCreateResult>>;

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

    fn task_release(&self, task_id: &str, version: i64, time: i64, ttl: i64)
        -> StorageResult<bool>;

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

/// Enum-based storage to avoid trait object limitations with generic methods
pub enum Storage {
    Sqlite(persistence_sqlite::SqliteStorage),
    Postgres(persistence_postgres::PostgresStorage),
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
        }
    }

    pub async fn transact_serializable<F, T>(&self, f: F) -> StorageResult<T>
    where
        F: FnMut(&dyn Db) -> StorageResult<T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            Storage::Sqlite(s) => s.transact(f).await,
            Storage::Postgres(p) => p.transact(f, true).await,
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
        }
    }
}
