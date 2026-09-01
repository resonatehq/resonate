//! Resonate's durable state, over a relational database.
//!
//! Holds the [`engine_port::ResonateEngine`] contract — every transition the
//! system makes — and the three implementations of it, one per SQL dialect.
//! Each is complete on its own: it parses a request, applies the transition in
//! its own SQL, shapes the response, and returns the messages it emitted.
//!
//! There is no shared engine over a storage trait. Lifting the state machine
//! into shared Rust would cost Postgres its single-round-trip CTE, which is
//! the property that makes it fast; what keeps three implementations honest is
//! the differential, run in lock step against [`oracle`].
//!
//! What is left here is what all three genuinely share: the error type, the
//! parameter structs an operation passes down, and the record types.

#[cfg(feature = "mysql")]
pub mod engine_mysql;
pub mod engine_port;
#[cfg(feature = "postgres")]
pub mod engine_postgres;
#[cfg(feature = "sqlite")]
pub mod engine_sqlite;
pub mod migrate;
pub mod oracle;

use resonate_core::types::{PromiseRecord, ResponseEnvelope, TaskState};
use resonate_core::ui::UiError;

/// Parse and resolve a `ui.*` request's `data`, rendering either failure as
/// the response it is.
///
/// Shared by the four engines because the answer must not differ between them:
/// a limit one backend clamps and another refuses would be a protocol with two
/// meanings. What is left per engine is the one statement its dialect needs.
pub fn ui_resolve<T, Q>(
    data: &serde_json::Value,
    kind: &str,
    corr_id: &str,
    resolve: impl FnOnce(T) -> Result<Q, UiError>,
) -> Result<Q, ResponseEnvelope>
where
    T: serde::de::DeserializeOwned,
{
    let parsed: T = serde_json::from_value(data.clone()).map_err(|e| {
        UiError::InvalidRequest(e.to_string()).to_response(kind.into(), corr_id.into())
    })?;
    resolve(parsed).map_err(|e| e.to_response(kind.into(), corr_id.into()))
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug)]
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

#[cfg(feature = "sqlite")]
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
    /// Awaited promises that exist but may not be awaited — see
    /// `resonate_core::types::is_external`. Refused with the same 422 as a
    /// missing one, and counted apart only so the message can say which.
    pub non_awaitable_count: i32,
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

// === Parameter structs, shared by the three engines ===

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
