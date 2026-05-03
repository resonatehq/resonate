use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use validator::Validate;

// --- State Enums ---
// These match the Zod enums in the canonical types.ts exactly.

/// Promise states: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout"
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromiseState {
    Pending,
    Resolved,
    Rejected,
    RejectedCanceled,
    RejectedTimedout,
}

impl PromiseState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PromiseState::Pending => "pending",
            PromiseState::Resolved => "resolved",
            PromiseState::Rejected => "rejected",
            PromiseState::RejectedCanceled => "rejected_canceled",
            PromiseState::RejectedTimedout => "rejected_timedout",
        }
    }
}

impl fmt::Display for PromiseState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PromiseState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(PromiseState::Pending),
            "resolved" => Ok(PromiseState::Resolved),
            "rejected" => Ok(PromiseState::Rejected),
            "rejected_canceled" => Ok(PromiseState::RejectedCanceled),
            "rejected_timedout" => Ok(PromiseState::RejectedTimedout),
            _ => Err(format!("invalid promise state: {}", s)),
        }
    }
}

/// Task states: "pending" | "acquired" | "suspended" | "halted" | "fulfilled"
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Acquired,
    Suspended,
    Halted,
    Fulfilled,
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskState::Pending => "pending",
            TaskState::Acquired => "acquired",
            TaskState::Suspended => "suspended",
            TaskState::Halted => "halted",
            TaskState::Fulfilled => "fulfilled",
        }
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(TaskState::Pending),
            "acquired" => Ok(TaskState::Acquired),
            "suspended" => Ok(TaskState::Suspended),
            "halted" => Ok(TaskState::Halted),
            "fulfilled" => Ok(TaskState::Fulfilled),
            _ => Err(format!("invalid task state: {}", s)),
        }
    }
}

/// Settle states (used in promise.settle and task.fulfill): "resolved" | "rejected" | "rejected_canceled"
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettleState {
    Resolved,
    Rejected,
    RejectedCanceled,
}

impl SettleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            SettleState::Resolved => "resolved",
            SettleState::Rejected => "rejected",
            SettleState::RejectedCanceled => "rejected_canceled",
        }
    }

    /// Convert to the corresponding PromiseState.
    #[allow(dead_code)]
    pub fn to_promise_state(self) -> PromiseState {
        match self {
            SettleState::Resolved => PromiseState::Resolved,
            SettleState::Rejected => PromiseState::Rejected,
            SettleState::RejectedCanceled => PromiseState::RejectedCanceled,
        }
    }
}

impl fmt::Display for SettleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Protocol version used in responses
pub const PROTOCOL_VERSION: &str = "2026-04-01";
// Versions accepted in requests.
pub const SUPPORTED_VERSIONS: &[&str] = &["2026-04-01"];

// --- Request Envelope ---

#[derive(Debug, Clone, Deserialize)]
pub struct RequestEnvelope {
    pub kind: String,
    pub head: RequestHead,
    pub data: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RequestHead {
    #[serde(rename = "corrId")]
    pub corr_id: String,
    #[allow(dead_code)]
    pub version: String,
    #[allow(dead_code)]
    pub auth: Option<String>,
    #[serde(rename = "resonate:debug_time")]
    pub debug_time: Option<i64>,
}

// --- Response Envelope ---

#[derive(Debug, Serialize)]
pub struct ResponseEnvelope {
    pub kind: String,
    pub head: ResponseHead,
    pub data: Value,
}

#[derive(Debug, Serialize)]
pub struct ResponseHead {
    #[serde(rename = "corrId")]
    pub corr_id: String,
    pub status: i32,
    pub version: String,
}

// --- Outgoing Message Types ---

#[derive(Debug, Serialize)]
pub struct MessageHead {
    #[serde(rename = "serverUrl")]
    pub server_url: String,
}

#[derive(Debug, Serialize)]
pub struct ExecuteMsg {
    pub kind: String,
    pub head: MessageHead,
    pub data: ExecuteMsgData,
}

#[derive(Debug, Serialize)]
pub struct ExecuteMsgData {
    pub task: ExecuteMsgTask,
}

#[derive(Debug, Serialize)]
pub struct ExecuteMsgTask {
    pub id: String,
    pub version: i64,
}

// --- Record Types ---

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PromiseValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<std::collections::HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromiseRecord {
    pub id: String,
    pub state: PromiseState,
    pub param: PromiseValue,
    pub value: PromiseValue,
    pub tags: std::collections::HashMap<String, String>,
    #[serde(rename = "timeoutAt")]
    pub timeout_at: i64,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "settledAt", skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub state: TaskState,
    pub version: i64,
    #[serde(default)]
    pub resumes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub id: String,
    pub cron: String,
    #[serde(rename = "promiseId")]
    pub promise_id: String,
    #[serde(rename = "promiseTimeout")]
    pub promise_timeout: i64,
    #[serde(rename = "promiseParam")]
    pub promise_param: PromiseValue,
    #[serde(rename = "promiseTags")]
    pub promise_tags: std::collections::HashMap<String, String>,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "nextRunAt")]
    pub next_run_at: i64,
    #[serde(rename = "lastRunAt", skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<i64>,
}

// --- Request Data Structs ---
// These mirror the `data` field of each request kind in types.ts.

#[derive(Debug, Deserialize, Validate)]
pub struct PromiseGetData {
    #[validate(length(min = 1, message = "Promise ID is required"))]
    pub id: String,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
#[validate(schema(function = "validate_promise_create_data"))]
pub struct PromiseCreateData {
    #[validate(length(min = 1, message = "Promise ID is required"))]
    pub id: String,
    #[serde(rename = "timeoutAt")]
    #[validate(range(min = 0, message = "TimeoutAt must be a non-negative integer"))]
    pub timeout_at: i64,
    #[serde(default)]
    pub param: PromiseValue,
    #[serde(default)]
    pub tags: std::collections::HashMap<String, String>,
}

fn validate_promise_create_data(
    data: &PromiseCreateData,
) -> Result<(), validator::ValidationError> {
    if data.id.contains('\0') {
        return Err(validator::ValidationError::new("null_bytes")
            .with_message("Promise ID must not contain null bytes".into()));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Validate)]
pub struct PromiseSettleData {
    #[validate(length(min = 1, message = "Promise ID is required"))]
    pub id: String,
    pub state: SettleState,
    #[serde(default)]
    pub value: PromiseValue,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
#[validate(schema(function = "validate_callback_data"))]
pub struct PromiseRegisterCallbackData {
    #[validate(length(min = 1, message = "Awaited promise ID is required"))]
    pub awaited: String,
    #[validate(length(min = 1, message = "Awaiter promise ID is required"))]
    pub awaiter: String,
}

fn validate_callback_data(
    data: &PromiseRegisterCallbackData,
) -> Result<(), validator::ValidationError> {
    if data.awaited == data.awaiter {
        return Err(validator::ValidationError::new("awaited_equals_awaiter")
            .with_message("Awaited and awaiter must be different promises".into()));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Validate)]
pub struct PromiseRegisterListenerData {
    #[validate(length(min = 1, message = "Awaited promise ID is required"))]
    pub awaited: String,
    #[validate(length(min = 1, message = "Address is required"))]
    pub address: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct PromiseSearchData {
    pub state: Option<PromiseState>,
    pub tags: Option<std::collections::HashMap<String, String>>,
    #[validate(range(min = 1, message = "Limit must be a positive integer"))]
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskGetData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
}

#[derive(Debug, Deserialize, Validate)]
#[validate(schema(function = "validate_task_create_data"))]
pub struct TaskCreateData {
    #[validate(length(min = 1, message = "Process ID is required"))]
    pub pid: String,
    #[validate(range(min = 1, message = "TTL must be a positive integer"))]
    pub ttl: i64,
    #[validate(nested)]
    pub action: TaskCreateAction,
}

fn validate_task_create_data(data: &TaskCreateData) -> Result<(), validator::ValidationError> {
    if !data.action.data.tags.contains_key("resonate:target") {
        return Err(validator::ValidationError::new("missing_target")
            .with_message("Action must have a resonate:target tag".into()));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct TaskCreateAction {
    #[allow(dead_code)]
    pub kind: String,
    #[allow(dead_code)]
    pub head: serde_json::Value,
    #[validate(nested)]
    pub data: PromiseCreateData,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskAcquireData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
    #[validate(range(min = 0, message = "Version must be a non-negative integer"))]
    pub version: i64,
    #[validate(length(min = 1, message = "Process ID is required"))]
    pub pid: String,
    #[validate(range(min = 1, message = "TTL must be a positive integer"))]
    pub ttl: i64,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskReleaseData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
    #[validate(range(min = 0, message = "Version must be a non-negative integer"))]
    pub version: i64,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct TaskSuspendAction {
    #[allow(dead_code)]
    pub kind: String,
    #[allow(dead_code)]
    pub head: serde_json::Value,
    #[validate(nested)]
    pub data: PromiseRegisterCallbackData,
}

#[derive(Debug, Deserialize, Validate)]
#[validate(schema(function = "validate_task_suspend_data"))]
pub struct TaskSuspendData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
    #[validate(range(min = 0, message = "Version must be a non-negative integer"))]
    pub version: i64,
    #[validate(length(min = 1, message = "Actions array cannot be empty"))]
    #[validate(nested)]
    pub actions: Vec<TaskSuspendAction>,
}

fn validate_task_suspend_data(data: &TaskSuspendData) -> Result<(), validator::ValidationError> {
    for action in &data.actions {
        if action.data.awaiter != data.id {
            return Err(validator::ValidationError::new("awaiter_mismatch")
                .with_message("All action awaiter IDs must match the task ID".into()));
        }
        if action.data.awaited == data.id {
            return Err(validator::ValidationError::new("awaited_is_self")
                .with_message("Action awaited promise must not equal the task ID".into()));
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct TaskFulfillActionData {
    #[validate(length(min = 1, message = "Promise ID is required"))]
    pub id: String,
    pub state: SettleState,
    #[serde(default)]
    pub value: PromiseValue,
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct TaskFulfillAction {
    #[allow(dead_code)]
    pub kind: String,
    #[allow(dead_code)]
    pub head: serde_json::Value,
    #[validate(nested)]
    pub data: TaskFulfillActionData,
}

#[derive(Debug, Deserialize, Validate)]
#[validate(schema(function = "validate_task_fulfill_data"))]
pub struct TaskFulfillData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
    #[validate(range(min = 0, message = "Version must be a non-negative integer"))]
    pub version: i64,
    #[validate(nested)]
    pub action: TaskFulfillAction,
}

fn validate_task_fulfill_data(data: &TaskFulfillData) -> Result<(), validator::ValidationError> {
    if data.action.data.id != data.id {
        return Err(validator::ValidationError::new("action_id_mismatch")
            .with_message("Action ID must match the task ID".into()));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct TaskFenceActionData {
    pub kind: String,
    #[allow(dead_code)]
    pub head: Option<serde_json::Value>,
    pub data: serde_json::Value,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskFenceData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
    #[validate(range(min = 0, message = "Version must be a non-negative integer"))]
    pub version: i64,
    pub action: TaskFenceActionData,
}

#[derive(Debug, Deserialize)]
pub struct TaskHeartbeatTask {
    pub id: String,
    pub version: i64,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskHeartbeatData {
    #[validate(length(min = 1, message = "Process ID is required"))]
    pub pid: String,
    pub tasks: Vec<TaskHeartbeatTask>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskHaltData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskContinueData {
    #[validate(length(min = 1, message = "Task ID is required"))]
    pub id: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct TaskSearchData {
    pub state: Option<TaskState>,
    #[validate(range(min = 1, message = "Limit must be a positive integer"))]
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct ScheduleGetData {
    #[validate(length(min = 1, message = "Schedule ID is required"))]
    pub id: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct ScheduleCreateData {
    #[validate(length(min = 1, message = "Schedule ID is required"))]
    pub id: String,
    #[validate(length(min = 1, message = "Cron expression is required"))]
    pub cron: String,
    #[serde(rename = "promiseId")]
    #[validate(length(min = 1, message = "Promise ID template is required"))]
    pub promise_id: String,
    #[serde(rename = "promiseTimeout")]
    #[validate(range(min = 0, message = "Promise timeout must be a non-negative integer"))]
    pub promise_timeout: i64,
    #[serde(rename = "promiseParam", default)]
    pub promise_param: PromiseValue,
    #[serde(rename = "promiseTags", default)]
    pub promise_tags: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct ScheduleDeleteData {
    #[validate(length(min = 1, message = "Schedule ID is required"))]
    pub id: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct ScheduleSearchData {
    pub tags: Option<std::collections::HashMap<String, String>>,
    #[validate(range(min = 1, message = "Limit must be a positive integer"))]
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

// --- Response Data Structs ---
// These mirror the `data` field of each successful (200) response in types.ts.

#[derive(Debug, Serialize)]
pub struct PromiseResponseData {
    pub promise: PromiseRecord,
}

#[derive(Debug, Serialize)]
pub struct PromiseSearchResponseData {
    pub promises: Vec<PromiseRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TaskResponseData {
    pub task: TaskRecord,
}

#[derive(Debug, Serialize)]
pub struct TaskCreateResponseData {
    pub task: TaskRecord,
    pub promise: PromiseRecord,
    pub preload: Vec<PromiseRecord>,
}

#[derive(Debug, Serialize)]
pub struct TaskAcquireResponseData {
    pub task: TaskRecord,
    pub promise: PromiseRecord,
    pub preload: Vec<PromiseRecord>,
}

#[derive(Debug, Serialize)]
pub struct TaskSuspendPreloadData {
    pub preload: Vec<PromiseRecord>,
}

#[derive(Debug, Serialize)]
pub struct TaskFulfillResponseData {
    pub promise: PromiseRecord,
}

#[derive(Debug, Serialize)]
pub struct TaskFenceResponseData {
    pub action: Value,
    pub preload: Vec<PromiseRecord>,
}

#[derive(Debug, Serialize)]
pub struct TaskSearchResponseData {
    pub tasks: Vec<TaskRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ScheduleResponseData {
    pub schedule: ScheduleRecord,
}

#[derive(Debug, Serialize)]
pub struct ScheduleSearchResponseData {
    pub schedules: Vec<ScheduleRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

// --- Snapshot types (debug.snap) ---

#[derive(Debug, Serialize)]
pub struct SnapshotPromiseTimeout {
    pub id: String,
    pub timeout: i64,
}

#[derive(Debug, Serialize)]
pub struct SnapshotCallback {
    pub awaiter: String,
    pub awaited: String,
}

#[derive(Debug, Serialize)]
pub struct SnapshotListener {
    #[serde(rename = "id")]
    pub promise_id: String,
    pub address: String,
}

#[derive(Debug, Serialize)]
pub struct SnapshotTaskTimeout {
    pub id: String,
    #[serde(rename = "type")]
    pub timeout_type: i32,
    pub timeout: i64,
}

#[derive(Debug, Serialize)]
pub struct SnapshotMessage {
    pub address: String,
    pub message: Value,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub promises: Vec<PromiseRecord>,
    #[serde(rename = "promiseTimeouts")]
    pub promise_timeouts: Vec<SnapshotPromiseTimeout>,
    pub callbacks: Vec<SnapshotCallback>,
    pub listeners: Vec<SnapshotListener>,
    pub tasks: Vec<TaskRecord>,
    #[serde(rename = "taskTimeouts")]
    pub task_timeouts: Vec<SnapshotTaskTimeout>,
    pub messages: Vec<SnapshotMessage>,
}

// --- Validation helper ---

/// Format validation errors into a single human-readable string.
pub fn format_validation_errors(errors: &validator::ValidationErrors) -> String {
    let mut messages = Vec::new();
    for (field, field_errors) in errors.field_errors() {
        for error in field_errors {
            if let Some(msg) = &error.message {
                messages.push(msg.to_string());
            } else {
                messages.push(format!("Invalid value for '{}'", field));
            }
        }
    }
    // Also check struct-level errors
    for error in errors.errors().values() {
        match error {
            validator::ValidationErrorsKind::Struct(inner) => {
                messages.push(format_validation_errors(inner));
            }
            validator::ValidationErrorsKind::List(map) => {
                for inner in map.values() {
                    messages.push(format_validation_errors(inner));
                }
            }
            validator::ValidationErrorsKind::Field(field_errors) => {
                for error in field_errors {
                    if let Some(msg) = &error.message {
                        messages.push(msg.to_string());
                    }
                }
            }
        }
    }
    messages.dedup();
    if messages.is_empty() {
        "Validation failed".to_string()
    } else {
        messages.join("; ")
    }
}

// --- Helper to build response ---

impl ResponseEnvelope {
    pub fn new(kind: String, corr_id: String, status: i32, data: Value) -> Self {
        Self {
            kind,
            head: ResponseHead {
                corr_id,
                status,
                version: PROTOCOL_VERSION.to_string(),
            },
            data,
        }
    }

    pub fn error(kind: String, corr_id: String, status: i32, message: &str) -> Self {
        Self::new(kind, corr_id, status, Value::String(message.to_string()))
    }

    pub fn success<T: Serialize>(kind: String, corr_id: String, data: &T) -> Self {
        Self::new(kind, corr_id, 200, serde_json::to_value(data).unwrap())
    }
}
