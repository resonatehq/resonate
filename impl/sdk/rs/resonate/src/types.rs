use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// SHARED TYPES
// =============================================================================

/// The wire format for data crossing the durability boundary.
///
/// On the wire, `data` is a base64-encoded JSON string (or undefined).
/// Internally after decoding by the Codec, `data` holds the deserialized value.
///
/// Mirrors the TS type:
/// ```ts
/// type Value = { headers?: Record<string, string>; data?: any };
/// ```
#[derive(Debug, Clone, Default, Serialize)]
pub struct Value {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl Value {
    /// Get a reference to the data field, defaulting to `serde_json::Value::Null` if absent.
    pub fn data_as_ref(&self) -> &serde_json::Value {
        static NULL: serde_json::Value = serde_json::Value::Null;
        self.data.as_ref().unwrap_or(&NULL)
    }

    /// Get the data field, defaulting to `serde_json::Value::Null` if absent.
    pub fn data_or_null(&self) -> serde_json::Value {
        self.data.clone().unwrap_or(serde_json::Value::Null)
    }

    /// Get headers, defaulting to empty map if absent.
    pub fn headers_or_empty(&self) -> HashMap<String, String> {
        self.headers.clone().unwrap_or_default()
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        match v {
            serde_json::Value::Null => Ok(Value::default()),
            serde_json::Value::Object(map) => {
                let headers: Option<HashMap<String, String>> = map
                    .get("headers")
                    .and_then(|h| serde_json::from_value(h.clone()).ok());
                let data = map.get("data").cloned();
                Ok(Value { headers, data })
            }
            // If it's not an object, treat the raw value as `data`
            other => Ok(Value {
                headers: None,
                data: Some(other),
            }),
        }
    }
}

// =============================================================================
// RECORDS
// =============================================================================

/// The state of a durable promise.
///
/// Mirrors the TS type:
/// ```ts
/// state: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PromiseState {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "resolved")]
    Resolved,
    #[serde(rename = "rejected")]
    Rejected,
    #[serde(rename = "rejected_canceled")]
    RejectedCanceled,
    #[serde(rename = "rejected_timedout")]
    RejectedTimedout,
}

/// A durable promise record as stored by the server.
///
/// Mirrors the TS type:
/// ```ts
/// type PromiseRecord = {
///   id: string; state: PromiseState; param: Value; value: Value;
///   tags: Record<string, string>; timeoutAt: number;
///   createdAt: number; settledAt?: number;
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromiseRecord {
    pub id: String,
    pub state: PromiseState,
    #[serde(default)]
    pub param: Value,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub timeout_at: i64,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub settled_at: Option<i64>,
}

/// The state of a task.
///
/// Mirrors the TS type:
/// ```ts
/// state: "pending" | "acquired" | "suspended" | "halted" | "fulfilled"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "acquired")]
    Acquired,
    #[serde(rename = "suspended")]
    Suspended,
    #[serde(rename = "halted")]
    Halted,
    #[serde(rename = "fulfilled")]
    Fulfilled,
}

/// A task record as returned by the server.
///
/// Mirrors the TS type:
/// ```ts
/// type TaskRecord = {
///   id: string;
///   state: "pending" | "acquired" | "suspended" | "halted" | "fulfilled";
///   version: number;
///   resumes: string[] | number | boolean;
///   ttl?: number;
///   pid?: string;
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub state: TaskState,
    pub version: i64,
    /// Resumes can be an array of strings, a number, or a boolean.
    #[serde(default)]
    pub resumes: serde_json::Value,
    #[serde(default)]
    pub ttl: Option<i64>,
    #[serde(default)]
    pub pid: Option<String>,
}

/// A schedule record as returned by the server.
///
/// Mirrors the TS type:
/// ```ts
/// type ScheduleRecord = {
///   id: string; cron: string; promiseId: string;
///   promiseTimeout: number; promiseParam: Value;
///   promiseTags: Record<string, string>;
///   createdAt: number; nextRunAt: number; lastRunAt?: number;
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRecord {
    pub id: String,
    pub cron: String,
    pub promise_id: String,
    pub promise_timeout: i64,
    #[serde(default)]
    pub promise_param: Value,
    #[serde(default)]
    pub promise_tags: HashMap<String, String>,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub next_run_at: i64,
    #[serde(default)]
    pub last_run_at: Option<i64>,
}

// =============================================================================
// REQUEST TYPES
// =============================================================================

/// How to settle a durable promise.
///
/// Mirrors the TS type:
/// ```ts
/// state: "resolved" | "rejected" | "rejected_canceled"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettleState {
    #[serde(rename = "resolved")]
    Resolved,
    #[serde(rename = "rejected")]
    Rejected,
    #[serde(rename = "rejected_canceled")]
    RejectedCanceled,
}

/// Request to create a durable promise (`promise.create` data payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromiseCreateReq {
    pub id: String,
    pub timeout_at: i64,
    pub param: Value,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl PromiseCreateReq {
    /// Create a minimal placeholder request (used when serialization fails at construction time).
    pub(crate) fn default_with_id(id: &str) -> Self {
        Self {
            id: id.to_string(),
            timeout_at: 0,
            param: Value {
                headers: None,
                data: None,
            },
            tags: HashMap::new(),
        }
    }
}

/// Request to settle a durable promise (`promise.settle` data payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromiseSettleReq {
    pub id: String,
    pub state: SettleState,
    pub value: Value,
}

/// A promise register callback request (`promise.register_callback` data payload).
///
/// Used inside `task.suspend` actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromiseRegisterCallbackData {
    pub awaited: String,
    pub awaiter: String,
}

// =============================================================================
// SDK-INTERNAL TYPES (not part of the wire protocol)
// =============================================================================

/// The result of executing a durable function.
#[derive(Debug)]
pub enum Outcome {
    /// Function completed successfully or with an error.
    Done(crate::error::Result<serde_json::Value>),
    /// Function cannot proceed — it has unresolved remote dependencies.
    Suspended { remote_todos: Vec<String> },
}

/// The kind of durable function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableKind {
    /// Leaf function — no sub-tasks, always completes.
    Function,
    /// Workflow function — can call ctx.run/rpc, may suspend.
    Workflow,
}

/// Parsed task data from the root promise param.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskData {
    pub func: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

/// Execution status returned from Core methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Done,
    Suspended,
}
