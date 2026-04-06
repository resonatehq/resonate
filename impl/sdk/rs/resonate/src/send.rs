use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::types::{
    PromiseCreateReq, PromiseRecord, PromiseRegisterCallbackData, PromiseSettleReq, ScheduleRecord,
    TaskRecord,
};

/// Protocol version string sent in all requests.
const PROTOCOL_VERSION: &str = "2025-01-15";

// ═══════════════════════════════════════════════════════════════════
//  Public result types
// ═══════════════════════════════════════════════════════════════════

/// Result of acquiring a task.
#[derive(Debug, Clone)]
pub struct TaskAcquireResult {
    pub task: TaskRecord,
    pub promise: PromiseRecord,
    pub preload: Vec<PromiseRecord>,
}

/// Result of creating a task (same structure as acquire).
pub type TaskCreateResult = TaskAcquireResult;

/// Result of suspending a task — either actually suspended or redirected.
#[derive(Debug, Clone)]
pub enum SuspendResult {
    Suspended,
    Redirect { preload: Vec<PromiseRecord> },
}

/// Result of a fenced operation.
#[derive(Debug, Clone)]
pub struct TaskFenceResult {
    pub promise: PromiseRecord,
    pub preload: Vec<PromiseRecord>,
}

/// Reference to a task for heartbeat.
#[derive(Debug, Clone, Serialize)]
pub struct TaskRef {
    pub id: String,
    pub version: i64,
}

/// Result of a task search.
#[derive(Debug, Clone)]
pub struct TaskSearchResult {
    pub tasks: Vec<TaskRecord>,
    pub cursor: Option<String>,
}

/// Result of a promise search.
#[derive(Debug, Clone)]
pub struct PromiseSearchResult {
    pub promises: Vec<PromiseRecord>,
    pub cursor: Option<String>,
}

/// Result of a schedule search.
#[derive(Debug, Clone)]
pub struct ScheduleSearchResult {
    pub schedules: Vec<ScheduleRecord>,
    pub cursor: Option<String>,
}

/// Request to create a schedule.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleCreateReq {
    pub id: String,
    pub cron: String,
    pub promise_id: String,
    pub promise_timeout: i64,
    pub promise_param: crate::types::Value,
    pub promise_tags: std::collections::HashMap<String, String>,
}

// ═══════════════════════════════════════════════════════════════════
//  Sender — typed interface over Transport
// ═══════════════════════════════════════════════════════════════════

/// The Sender wraps a Transport and provides typed methods for each
/// server operation. It handles JSON serialization, response parsing,
/// and error conversion so callers never deal with raw JSON.
///
/// Cloning is cheap — Transport wraps `Arc<dyn Network>`.
#[derive(Clone)]
pub struct Sender {
    transport: Transport,
    auth: Option<String>,
}

impl Sender {
    pub fn new(transport: Transport, auth: Option<String>) -> Self {
        Self { transport, auth }
    }

    /// Acquire a task, returning the task, root promise, and any preloaded promises.
    pub async fn task_acquire(
        &self,
        id: &str,
        version: i64,
        pid: &str,
        ttl: i64,
    ) -> Result<TaskAcquireResult> {
        let req = Request::TaskAcquire {
            id: id.to_string(),
            version,
            pid: pid.to_string(),
            ttl,
        };
        let (_, data) = self.send_request(&req).await?;
        parse_task_acquire(&data)
    }

    /// Fulfill a task by settling its root promise.
    /// Returns the settled promise.
    pub async fn task_fulfill(
        &self,
        id: &str,
        version: i64,
        action: PromiseSettleReq,
    ) -> Result<PromiseRecord> {
        let req = Request::TaskFulfill {
            id: id.to_string(),
            version,
            action,
        };
        let (_, data) = self.send_request(&req).await?;
        parse_promise(&data)
    }

    /// Suspend a task, registering callbacks for awaited promises.
    /// Returns whether the task was actually suspended or redirected.
    pub async fn task_suspend(
        &self,
        id: &str,
        version: i64,
        actions: Vec<PromiseRegisterCallbackData>,
    ) -> Result<SuspendResult> {
        let req = Request::TaskSuspend {
            id: id.to_string(),
            version,
            actions,
        };
        let (status, data) = self.send_request(&req).await?;
        parse_suspend_result(status, &data)
    }

    /// Release a task (give up the lock without fulfilling).
    pub async fn task_release(&self, id: &str, version: i64) -> Result<()> {
        let req = Request::TaskRelease {
            id: id.to_string(),
            version,
        };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Get a task by ID.
    pub async fn task_get(&self, id: &str) -> Result<TaskRecord> {
        let req = Request::TaskGet { id: id.to_string() };
        let (_, data) = self.send_request(&req).await?;
        let task = data
            .get("task")
            .ok_or_else(|| Error::DecodingError("missing 'task' in response".into()))?;
        TaskRecord::deserialize(task)
            .map_err(|e| Error::DecodingError(format!("invalid task record: {}", e)))
    }

    /// Create a task and its associated promise.
    pub async fn task_create(
        &self,
        pid: &str,
        ttl: i64,
        action: PromiseCreateReq,
    ) -> Result<TaskCreateResult> {
        let req = Request::TaskCreate {
            pid: pid.to_string(),
            ttl,
            action,
        };
        let (_, data) = self.send_request(&req).await?;
        parse_task_acquire(&data)
    }

    /// Halt a task, preventing it from being acquired or making progress.
    pub async fn task_halt(&self, id: &str) -> Result<()> {
        let req = Request::TaskHalt { id: id.to_string() };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Continue a halted task, transitioning it back to pending.
    pub async fn task_continue(&self, id: &str) -> Result<()> {
        let req = Request::TaskContinue { id: id.to_string() };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Execute a promise operation only if the task's lease is still valid.
    pub async fn task_fence(
        &self,
        id: &str,
        version: i64,
        action: serde_json::Value,
    ) -> Result<TaskFenceResult> {
        let req = Request::TaskFence {
            id: id.to_string(),
            version,
            action,
        };
        let (_, data) = self.send_request(&req).await?;
        // The response data.action contains a nested response with its own kind/head/data
        // We extract the promise from the action response
        let empty = serde_json::json!({});
        let action_resp = data.get("action").unwrap_or(&empty);
        let promise_val = action_resp
            .get("data")
            .and_then(|d| d.get("promise"))
            .ok_or_else(|| {
                Error::DecodingError("missing promise in fence action response".into())
            })?;
        let promise = PromiseRecord::deserialize(promise_val).map_err(|e| {
            Error::DecodingError(format!("invalid promise in fence response: {}", e))
        })?;
        let preload = parse_preloaded(&data);
        Ok(TaskFenceResult { promise, preload })
    }

    /// Extend the lease for one or more tasks.
    pub async fn task_heartbeat(&self, pid: &str, tasks: Vec<TaskRef>) -> Result<()> {
        let req = Request::TaskHeartbeat {
            pid: pid.to_string(),
            tasks,
        };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Search for tasks matching criteria.
    pub async fn task_search(
        &self,
        state: Option<&str>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<TaskSearchResult> {
        let req = Request::TaskSearch {
            state: state.map(|s| s.to_string()),
            limit,
            cursor: cursor.map(|c| c.to_string()),
        };
        let (_, data) = self.send_request(&req).await?;
        let tasks_val = data.get("tasks").and_then(|v| v.as_array());
        let tasks = tasks_val
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| TaskRecord::deserialize(v).ok())
                    .collect()
            })
            .unwrap_or_default();
        let cursor = data
            .get("cursor")
            .and_then(|c| c.as_str())
            .map(|s| s.to_string());
        Ok(TaskSearchResult { tasks, cursor })
    }

    /// Get a promise by ID.
    pub async fn promise_get(&self, id: &str) -> Result<PromiseRecord> {
        let req = Request::PromiseGet { id: id.to_string() };
        let (_, data) = self.send_request(&req).await?;
        parse_promise(&data)
    }

    /// Create a durable promise.
    pub async fn promise_create(&self, req: PromiseCreateReq) -> Result<PromiseRecord> {
        let request = Request::PromiseCreate(req);
        let (_, data) = self.send_request(&request).await?;
        parse_promise(&data)
    }

    /// Settle (resolve/reject) a durable promise.
    pub async fn promise_settle(&self, req: PromiseSettleReq) -> Result<PromiseRecord> {
        let request = Request::PromiseSettle(req);
        let (_, data) = self.send_request(&request).await?;
        parse_promise(&data)
    }

    /// Register a callback between two promises.
    pub async fn promise_register_callback(
        &self,
        awaited: &str,
        awaiter: &str,
    ) -> Result<PromiseRecord> {
        let req = Request::PromiseRegisterCallback(PromiseRegisterCallbackData {
            awaited: awaited.to_string(),
            awaiter: awaiter.to_string(),
        });
        let (_, data) = self.send_request(&req).await?;
        parse_promise(&data)
    }

    /// Register a listener for a promise.
    pub async fn promise_register_listener(
        &self,
        awaited: &str,
        address: &str,
    ) -> Result<PromiseRecord> {
        let req = Request::PromiseRegisterListener {
            awaited: awaited.to_string(),
            address: address.to_string(),
        };
        let (_, data) = self.send_request(&req).await?;
        parse_promise(&data)
    }

    /// Search for promises matching criteria.
    pub async fn promise_search(
        &self,
        state: Option<&str>,
        tags: Option<std::collections::HashMap<String, String>>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<PromiseSearchResult> {
        let req = Request::PromiseSearch {
            state: state.map(|s| s.to_string()),
            tags,
            limit,
            cursor: cursor.map(|c| c.to_string()),
        };
        let (_, data) = self.send_request(&req).await?;
        let promises_val = data.get("promises").and_then(|v| v.as_array());
        let promises = promises_val
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| PromiseRecord::deserialize(v).ok())
                    .collect()
            })
            .unwrap_or_default();
        let cursor = data
            .get("cursor")
            .and_then(|c| c.as_str())
            .map(|s| s.to_string());
        Ok(PromiseSearchResult { promises, cursor })
    }

    /// Get a schedule by ID.
    pub async fn schedule_get(&self, id: &str) -> Result<ScheduleRecord> {
        let req = Request::ScheduleGet { id: id.to_string() };
        let (_, data) = self.send_request(&req).await?;
        let schedule = data
            .get("schedule")
            .ok_or_else(|| Error::DecodingError("missing 'schedule' in response".into()))?;
        ScheduleRecord::deserialize(schedule)
            .map_err(|e| Error::DecodingError(format!("invalid schedule record: {}", e)))
    }

    /// Create a schedule.
    pub async fn schedule_create(&self, req: ScheduleCreateReq) -> Result<ScheduleRecord> {
        let request = Request::ScheduleCreate(req);
        let (_, data) = self.send_request(&request).await?;
        let schedule = data
            .get("schedule")
            .ok_or_else(|| Error::DecodingError("missing 'schedule' in response".into()))?;
        ScheduleRecord::deserialize(schedule)
            .map_err(|e| Error::DecodingError(format!("invalid schedule record: {}", e)))
    }

    /// Delete a schedule.
    pub async fn schedule_delete(&self, id: &str) -> Result<()> {
        let req = Request::ScheduleDelete { id: id.to_string() };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Search for schedules.
    pub async fn schedule_search(
        &self,
        tags: Option<std::collections::HashMap<String, String>>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ScheduleSearchResult> {
        let req = Request::ScheduleSearch {
            tags,
            limit,
            cursor: cursor.map(|c| c.to_string()),
        };
        let (_, data) = self.send_request(&req).await?;
        let schedules_val = data.get("schedules").and_then(|v| v.as_array());
        let schedules = schedules_val
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| ScheduleRecord::deserialize(v).ok())
                    .collect()
            })
            .unwrap_or_default();
        let cursor = data
            .get("cursor")
            .and_then(|c| c.as_str())
            .map(|s| s.to_string());
        Ok(ScheduleSearchResult { schedules, cursor })
    }

    /// Internal: serialize a Request, wrap in protocol envelope, send via transport,
    /// validate the response envelope, and return (status, data).
    ///
    /// Returns `Err` for HTTP error status codes (≥400).
    /// Returns `Ok((status, data))` for success/redirect (200, 201, 300).
    async fn send_request(&self, req: &Request) -> Result<(u16, serde_json::Value)> {
        // Serialize the Request enum (includes "kind" tag and data fields)
        let mut req_json = serde_json::to_value(req)?;

        // Extract "kind" from the serialized value
        let kind = req_json
            .as_object()
            .and_then(|o| o.get("kind"))
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .to_string();

        // Remove "kind" — remaining fields become the "data" payload
        if let Some(obj) = req_json.as_object_mut() {
            obj.remove("kind");
        }

        // Wrap nested action/actions fields in protocol sub-envelopes.
        // The protocol requires these to be full { kind, head, data } objects.
        wrap_nested_actions(&kind, &mut req_json, &self.auth);

        // Build protocol envelope
        let corr_id = format!("sr-{}", crate::now_ms());
        let mut head = serde_json::Map::new();
        head.insert("corrId".into(), serde_json::Value::String(corr_id));
        head.insert("version".into(), serde_json::Value::String(PROTOCOL_VERSION.into()));
        if let Some(ref token) = self.auth {
            head.insert("auth".into(), serde_json::Value::String(token.clone()));
        }
        let envelope = serde_json::json!({
            "kind": kind,
            "head": head,
            "data": req_json,
        });

        // Send through transport (validates kind + corrId match)
        let resp = self.transport.send(envelope).await?;

        // Extract status from response head
        let status = resp
            .get("head")
            .and_then(|h| h.get("status"))
            .and_then(|s| s.as_u64())
            .unwrap_or(200) as u16;

        // Extract data from response
        let data = resp.get("data").cloned().unwrap_or(serde_json::json!({}));

        // Check for error status codes
        if status >= 400 {
            let message = if let Some(s) = data.as_str() {
                s.to_string()
            } else if let Some(err) = data.get("error").and_then(|e| e.as_str()) {
                err.to_string()
            } else {
                format!("server error (status {})", status)
            };
            return Err(Error::ServerError {
                code: status,
                message,
            });
        }

        Ok((status, data))
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Nested action envelope wrapping
// ═══════════════════════════════════════════════════════════════════

/// Wrap a flat data object into a protocol sub-envelope: `{ kind, head: { corrId, version[, auth] }, data }`.
fn make_sub_envelope(kind: &str, data: serde_json::Value, auth: &Option<String>) -> serde_json::Value {
    let mut head = serde_json::Map::new();
    head.insert("corrId".into(), serde_json::Value::String(format!("sr-{}", crate::now_ms())));
    head.insert("version".into(), serde_json::Value::String(PROTOCOL_VERSION.into()));
    if let Some(token) = auth {
        head.insert("auth".into(), serde_json::Value::String(token.clone()));
    }
    serde_json::json!({
        "kind": kind,
        "head": head,
        "data": data,
    })
}

/// Post-process the serialized request data to wrap nested `action`/`actions`
/// fields in full protocol sub-envelopes, as required by the protocol spec.
fn wrap_nested_actions(kind: &str, data: &mut serde_json::Value, auth: &Option<String>) {
    match kind {
        // task.create: action is a PromiseCreateReq → wrap as "promise.create"
        "task.create" => {
            if let Some(action) = data.get("action").cloned() {
                if action.get("kind").is_none() {
                    data["action"] = make_sub_envelope("promise.create", action, auth);
                }
            }
        }
        // task.fulfill: action is a PromiseSettleReq → wrap as "promise.settle"
        "task.fulfill" => {
            if let Some(action) = data.get("action").cloned() {
                if action.get("kind").is_none() {
                    data["action"] = make_sub_envelope("promise.settle", action, auth);
                }
            }
        }
        // task.suspend: actions is an array of PromiseRegisterCallbackReq
        //   → wrap each as "promise.register_callback"
        "task.suspend" => {
            if let Some(actions) = data.get("actions").and_then(|v| v.as_array()).cloned() {
                let wrapped: Vec<serde_json::Value> = actions
                    .into_iter()
                    .map(|a| {
                        if a.get("kind").is_none() {
                            make_sub_envelope("promise.register_callback", a, auth)
                        } else {
                            a
                        }
                    })
                    .collect();
                data["actions"] = serde_json::Value::Array(wrapped);
            }
        }
        // task.fence: action can be PromiseCreateReq or PromiseSettleReq
        //   → detect which one by checking for "state" field (settle has it, create doesn't)
        "task.fence" => {
            if let Some(action) = data.get("action").cloned() {
                if action.get("kind").is_none() {
                    let sub_kind = if action.get("state").is_some() {
                        "promise.settle"
                    } else {
                        "promise.create"
                    };
                    data["action"] = make_sub_envelope(sub_kind, action, auth);
                }
            }
        }
        _ => {}
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Request enum (internal serialization format)
// ═══════════════════════════════════════════════════════════════════

/// Request types sent to the Resonate server.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind")]
pub(crate) enum Request {
    #[serde(rename = "promise.get")]
    PromiseGet { id: String },

    #[serde(rename = "promise.create")]
    PromiseCreate(PromiseCreateReq),

    #[serde(rename = "promise.settle")]
    PromiseSettle(PromiseSettleReq),

    #[serde(rename = "promise.register_callback")]
    PromiseRegisterCallback(PromiseRegisterCallbackData),

    #[serde(rename = "promise.register_listener")]
    PromiseRegisterListener { awaited: String, address: String },

    #[serde(rename = "promise.search")]
    PromiseSearch {
        #[serde(skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tags: Option<std::collections::HashMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },

    #[serde(rename = "task.get")]
    TaskGet { id: String },

    #[serde(rename = "task.create")]
    TaskCreate {
        pid: String,
        ttl: i64,
        action: PromiseCreateReq,
    },

    #[serde(rename = "task.acquire")]
    TaskAcquire {
        id: String,
        version: i64,
        pid: String,
        ttl: i64,
    },

    #[serde(rename = "task.fulfill")]
    TaskFulfill {
        id: String,
        version: i64,
        action: PromiseSettleReq,
    },

    #[serde(rename = "task.suspend")]
    TaskSuspend {
        id: String,
        version: i64,
        actions: Vec<PromiseRegisterCallbackData>,
    },

    #[serde(rename = "task.release")]
    TaskRelease { id: String, version: i64 },

    #[serde(rename = "task.halt")]
    TaskHalt { id: String },

    #[serde(rename = "task.continue")]
    TaskContinue { id: String },

    #[serde(rename = "task.fence")]
    TaskFence {
        id: String,
        version: i64,
        action: serde_json::Value,
    },

    #[serde(rename = "task.heartbeat")]
    TaskHeartbeat { pid: String, tasks: Vec<TaskRef> },

    #[serde(rename = "task.search")]
    TaskSearch {
        #[serde(skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },

    #[serde(rename = "schedule.get")]
    ScheduleGet { id: String },

    #[serde(rename = "schedule.create")]
    ScheduleCreate(ScheduleCreateReq),

    #[serde(rename = "schedule.delete")]
    ScheduleDelete { id: String },

    #[serde(rename = "schedule.search")]
    ScheduleSearch {
        #[serde(skip_serializing_if = "Option::is_none")]
        tags: Option<std::collections::HashMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
    },
}

// ═══════════════════════════════════════════════════════════════════
//  Response parsing helpers (internal)
// ═══════════════════════════════════════════════════════════════════

/// Parse a promise record from a server JSON response.
fn parse_promise(json: &serde_json::Value) -> Result<PromiseRecord> {
    let promise = json
        .get("promise")
        .ok_or_else(|| Error::DecodingError("missing 'promise' in response".into()))?;
    PromiseRecord::deserialize(promise)
        .map_err(|e| Error::DecodingError(format!("invalid promise record: {}", e)))
}

/// Parse a task.acquire response.
fn parse_task_acquire(json: &serde_json::Value) -> Result<TaskAcquireResult> {
    let task_val = json
        .get("task")
        .ok_or_else(|| Error::DecodingError("missing 'task' in task.acquire response".into()))?;
    let task: TaskRecord = TaskRecord::deserialize(task_val)
        .map_err(|e| Error::DecodingError(format!("invalid task in task.acquire: {}", e)))?;

    let promise_val = json
        .get("promise")
        .ok_or_else(|| Error::DecodingError("missing 'promise' in task.acquire response".into()))?;
    let promise: PromiseRecord = PromiseRecord::deserialize(promise_val)
        .map_err(|e| Error::DecodingError(format!("invalid promise in task.acquire: {}", e)))?;

    let preload = parse_preloaded(json);
    Ok(TaskAcquireResult {
        task,
        promise,
        preload,
    })
}

/// Parse a task.suspend response — either Suspended (200) or Redirect (300).
fn parse_suspend_result(status: u16, data: &serde_json::Value) -> Result<SuspendResult> {
    if status == 300 {
        Ok(SuspendResult::Redirect {
            preload: parse_preloaded(data),
        })
    } else {
        Ok(SuspendResult::Suspended)
    }
}

/// Extract preloaded promises from a response.
fn parse_preloaded(json: &serde_json::Value) -> Vec<PromiseRecord> {
    json.get("preload")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| PromiseRecord::deserialize(v).ok())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{LocalNetwork, Network};
    use crate::types::{
        PromiseCreateReq, PromiseRegisterCallbackData, PromiseSettleReq, SettleState, Value,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_sender(net: Arc<LocalNetwork>) -> Sender {
        Sender::new(Transport::new(net), None)
    }

    // ── Serialization: verify "kind" uses "subject.verb" format ─────

    #[test]
    fn promise_create_serializes_with_dot_kind() {
        let req = Request::PromiseCreate(PromiseCreateReq {
            id: "p1".into(),
            timeout_at: 999,
            param: Value::default(),
            tags: HashMap::new(),
        });
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "promise.create");
        assert_eq!(json["id"], "p1");
        assert_eq!(json["timeoutAt"], 999);
    }

    #[test]
    fn promise_settle_serializes_with_dot_kind() {
        let req = Request::PromiseSettle(PromiseSettleReq {
            id: "p1".into(),
            state: SettleState::Resolved,
            value: Value::default(),
        });
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "promise.settle");
        assert_eq!(json["id"], "p1");
        assert_eq!(json["state"], "resolved");
    }

    #[test]
    fn task_acquire_serializes_with_dot_kind() {
        let req = Request::TaskAcquire {
            id: "t1".into(),
            version: 2,
            pid: "p1".into(),
            ttl: 60000,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.acquire");
        assert_eq!(json["id"], "t1");
        assert_eq!(json["version"], 2);
        assert_eq!(json["pid"], "p1");
        assert_eq!(json["ttl"], 60000);
    }

    #[test]
    fn task_fulfill_serializes_with_dot_kind_and_nested_action() {
        let req = Request::TaskFulfill {
            id: "t1".into(),
            version: 2,
            action: PromiseSettleReq {
                id: "p1".into(),
                state: SettleState::Resolved,
                value: Value::default(),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.fulfill");
        assert_eq!(json["id"], "t1");
        assert_eq!(json["version"], 2);
        assert_eq!(json["action"]["id"], "p1");
        assert_eq!(json["action"]["state"], "resolved");
    }

    #[test]
    fn task_suspend_serializes_with_dot_kind() {
        let req = Request::TaskSuspend {
            id: "t1".into(),
            version: 1,
            actions: vec![
                PromiseRegisterCallbackData {
                    awaited: "dep-a".into(),
                    awaiter: "t1".into(),
                },
                PromiseRegisterCallbackData {
                    awaited: "dep-b".into(),
                    awaiter: "t1".into(),
                },
            ],
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.suspend");
        assert_eq!(json["id"], "t1");
        assert_eq!(json["version"], 1);
        assert_eq!(json["actions"].as_array().unwrap().len(), 2);
        assert_eq!(json["actions"][0]["awaited"], "dep-a");
        assert_eq!(json["actions"][0]["awaiter"], "t1");
    }

    #[test]
    fn task_release_serializes_with_dot_kind() {
        let req = Request::TaskRelease {
            id: "t1".into(),
            version: 3,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.release");
        assert_eq!(json["id"], "t1");
        assert_eq!(json["version"], 3);
    }

    // ── Round-trip: Sender methods through LocalNetwork ─────────────

    #[tokio::test]
    async fn promise_create_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("test".into()), None));
        let sender = test_sender(net);

        let req = PromiseCreateReq {
            id: "rt-p1".into(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        let record = sender.promise_create(req).await.unwrap();
        assert_eq!(record.id, "rt-p1");
        assert_eq!(record.state, crate::types::PromiseState::Pending);
    }

    /// Helper to extract data from raw LocalNetwork envelope response
    fn resp_data(resp_str: &str) -> serde_json::Value {
        let resp: serde_json::Value = serde_json::from_str(resp_str).unwrap();
        resp.get("data").cloned().unwrap_or(resp)
    }

    #[tokio::test]
    async fn task_acquire_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // First create a task via raw network (proper envelope format)
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "rt-p2", "timeoutAt": i64::MAX,
                        "param": {"data": "test"}, "tags": {},
                    },
                },
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let rdata = resp_data(&resp_str);
        let task_id = rdata["task"]["id"].as_str().unwrap().to_string();

        // Release the task so we can re-acquire
        let release_req = serde_json::json!({
            "kind": "task.release",
            "head": { "corrId": "c2", "version": "2025-01-15" },
            "data": { "id": task_id, "version": 0 },
        });
        net.send(release_req.to_string()).await.unwrap();

        // Now acquire via Sender
        let sender = test_sender(net);
        let result = sender
            .task_acquire(&task_id, 1, "pid1", 60000)
            .await
            .unwrap();
        assert_eq!(result.promise.id, "rt-p2");
    }

    #[tokio::test]
    async fn task_fulfill_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // Create a task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "rt-p3", "timeoutAt": i64::MAX,
                        "param": {"data": "test"}, "tags": {},
                    },
                },
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let rdata = resp_data(&resp_str);
        let task_id = rdata["task"]["id"].as_str().unwrap().to_string();

        // Fulfill via Sender
        let sender = test_sender(net);
        let promise = sender
            .task_fulfill(
                &task_id,
                0,
                PromiseSettleReq {
                    id: "rt-p3".into(),
                    state: SettleState::Resolved,
                    value: Value {
                        headers: None,
                        data: Some(serde_json::json!("result")),
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(promise.id, "rt-p3");
    }

    #[tokio::test]
    async fn task_suspend_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // Create a task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "rt-p4", "timeoutAt": i64::MAX,
                        "param": {"data": "test"}, "tags": {},
                    },
                },
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let rdata = resp_data(&resp_str);
        let task_id = rdata["task"]["id"].as_str().unwrap().to_string();

        // Suspend via Sender
        let sender = test_sender(net);
        let result = sender
            .task_suspend(
                &task_id,
                0,
                vec![PromiseRegisterCallbackData {
                    awaited: "dep-a".into(),
                    awaiter: task_id.clone(),
                }],
            )
            .await
            .unwrap();
        assert!(matches!(result, SuspendResult::Suspended));
    }

    #[tokio::test]
    async fn task_release_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // Create a task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "rt-p5", "timeoutAt": i64::MAX, "param": {}, "tags": {},
                    },
                },
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let rdata = resp_data(&resp_str);
        let task_id = rdata["task"]["id"].as_str().unwrap().to_string();

        // Release via Sender
        let sender = test_sender(net);
        sender.task_release(&task_id, 0).await.unwrap();
    }

    // ── Auth token in envelope head ──────────────────────────────────

    #[tokio::test]
    async fn envelope_head_contains_auth_when_token_provided() {
        use crate::test_utils::TestHarness;

        let harness = TestHarness::new();
        let sender = harness.build_sender_with_auth(Some("my-secret-token".into()));

        let req = PromiseCreateReq {
            id: "auth-p1".into(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        sender.promise_create(req).await.unwrap();

        let envelopes = harness.raw_sent_json().await;
        assert_eq!(envelopes.len(), 1);
        let head = &envelopes[0]["head"];
        assert_eq!(head["auth"], "my-secret-token");
        assert!(head.get("corrId").is_some());
        assert!(head.get("version").is_some());
    }

    #[tokio::test]
    async fn envelope_head_omits_auth_when_no_token() {
        use crate::test_utils::TestHarness;

        let harness = TestHarness::new();
        let sender = harness.build_sender_with_auth(None);

        let req = PromiseCreateReq {
            id: "no-auth-p1".into(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        sender.promise_create(req).await.unwrap();

        let envelopes = harness.raw_sent_json().await;
        assert_eq!(envelopes.len(), 1);
        let head = &envelopes[0]["head"];
        assert!(head.get("auth").is_none());
    }

    #[tokio::test]
    async fn sub_envelope_head_contains_auth_when_token_provided() {
        use crate::test_utils::TestHarness;

        let harness = TestHarness::new();

        // Pre-create the promise that the task action references
        harness
            .add_promise(crate::types::PromiseRecord {
                id: "sub-p1".into(),
                state: crate::types::PromiseState::Pending,
                timeout_at: i64::MAX,
                param: Value::default(),
                value: Value::default(),
                tags: HashMap::new(),
                created_at: 0,
                settled_at: None,
            })
            .await;

        let sender = harness.build_sender_with_auth(Some("sub-token".into()));

        // task.create wraps action in a sub-envelope
        let action = PromiseCreateReq {
            id: "sub-p1".into(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        let _ = sender.task_create("test-pid", 60000, action).await;

        let envelopes = harness.raw_sent_json().await;
        assert!(!envelopes.is_empty());
        let sub_head = &envelopes[0]["data"]["action"]["head"];
        assert_eq!(sub_head["auth"], "sub-token");
    }
}
