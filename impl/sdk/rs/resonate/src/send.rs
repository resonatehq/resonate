use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::types::{
    PromiseCreateReq, PromiseRecord, PromiseRegisterCallbackData, PromiseSettleReq, ScheduleRecord,
    TaskRecord,
};

use crate::PROTOCOL_VERSION;

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

/// Result of task creation when conflict is expected.
#[derive(Debug, Clone)]
pub enum TaskCreateOutcome {
    /// Task was successfully created and acquired.
    Created(TaskCreateResult),
    /// Promise already exists (409 conflict). Contains the existing promise.
    Conflict(PromiseRecord),
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
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
            version: i64,
            pid: &'a str,
            ttl: i64,
        }
        let (_, data) = self
            .send_envelope("task.acquire", &Data { id, version, pid, ttl }, false)
            .await?;
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
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
            version: i64,
            action: SubEnvelope<'a, &'a PromiseSettleReq>,
        }
        let data_payload = Data {
            id,
            version,
            action: SubEnvelope {
                kind: "promise.settle",
                head: self.make_head(),
                data: &action,
            },
        };
        let (_, data) = self.send_envelope("task.fulfill", &data_payload, false).await?;
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
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
            version: i64,
            actions: Vec<SubEnvelope<'a, &'a PromiseRegisterCallbackData>>,
        }
        let wrapped: Vec<_> = actions
            .iter()
            .map(|a| SubEnvelope {
                kind: "promise.register_callback",
                head: self.make_head(),
                data: a,
            })
            .collect();
        let data_payload = Data {
            id,
            version,
            actions: wrapped,
        };
        let (status, data) = self.send_envelope("task.suspend", &data_payload, false).await?;
        parse_suspend_result(status, &data)
    }

    /// Release a task (give up the lock without fulfilling).
    pub async fn task_release(&self, id: &str, version: i64) -> Result<()> {
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
            version: i64,
        }
        self.send_envelope("task.release", &Data { id, version }, false)
            .await?;
        Ok(())
    }

    /// Get a task by ID.
    pub async fn task_get(&self, id: &str) -> Result<TaskRecord> {
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
        }
        let (_, data) = self
            .send_envelope("task.get", &Data { id }, false)
            .await?;
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
        let (_, data) = self
            .send_task_create(pid, ttl, &action, false)
            .await?;
        parse_task_acquire(&data)
    }

    /// Create a task and its associated promise, returning `Conflict` on 409.
    ///
    /// Unlike `task_create`, this method does not fail on 409 (promise already exists).
    /// Instead, it returns the existing promise data so the caller can build a handle.
    pub async fn task_create_or_conflict(
        &self,
        pid: &str,
        ttl: i64,
        action: PromiseCreateReq,
    ) -> Result<TaskCreateOutcome> {
        let (status, data) = self
            .send_task_create(pid, ttl, &action, true)
            .await?;
        if status == 409 {
            let promise = parse_promise(&data)?;
            Ok(TaskCreateOutcome::Conflict(promise))
        } else {
            let result = parse_task_acquire(&data)?;
            Ok(TaskCreateOutcome::Created(result))
        }
    }

    /// Halt a task, preventing it from being acquired or making progress.
    pub async fn task_halt(&self, id: &str) -> Result<()> {
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
        }
        self.send_envelope("task.halt", &Data { id }, false).await?;
        Ok(())
    }

    /// Continue a halted task, transitioning it back to pending.
    pub async fn task_continue(&self, id: &str) -> Result<()> {
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
        }
        self.send_envelope("task.continue", &Data { id }, false)
            .await?;
        Ok(())
    }

    /// Execute a promise operation only if the task's lease is still valid.
    pub async fn task_fence(
        &self,
        id: &str,
        version: i64,
        action: serde_json::Value,
    ) -> Result<TaskFenceResult> {
        // Detect sub-kind: if "state" field present → settle, otherwise → create
        let sub_kind = if action.get("state").is_some() {
            "promise.settle"
        } else {
            "promise.create"
        };
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
            version: i64,
            action: SubEnvelope<'a, &'a serde_json::Value>,
        }
        let data_payload = Data {
            id,
            version,
            action: SubEnvelope {
                kind: sub_kind,
                head: self.make_head(),
                data: &action,
            },
        };
        let (_, data) = self.send_envelope("task.fence", &data_payload, false).await?;
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
        #[derive(Serialize)]
        struct Data<'a> {
            pid: &'a str,
            tasks: &'a [TaskRef],
        }
        self.send_envelope("task.heartbeat", &Data { pid, tasks: &tasks }, false)
            .await?;
        Ok(())
    }

    /// Search for tasks matching criteria.
    pub async fn task_search(
        &self,
        state: Option<&str>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<TaskSearchResult> {
        #[derive(Serialize)]
        struct Data<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            state: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            cursor: Option<&'a str>,
        }
        let (_, data) = self
            .send_envelope("task.search", &Data { state, limit, cursor }, false)
            .await?;
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
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
        }
        let (_, data) = self
            .send_envelope("promise.get", &Data { id }, false)
            .await?;
        parse_promise(&data)
    }

    /// Create a durable promise.
    pub async fn promise_create(&self, req: PromiseCreateReq) -> Result<PromiseRecord> {
        let (_, data) = self
            .send_envelope("promise.create", &req, false)
            .await?;
        parse_promise(&data)
    }

    /// Settle (resolve/reject) a durable promise.
    pub async fn promise_settle(&self, req: PromiseSettleReq) -> Result<PromiseRecord> {
        let (_, data) = self
            .send_envelope("promise.settle", &req, false)
            .await?;
        parse_promise(&data)
    }

    /// Register a callback between two promises.
    pub async fn promise_register_callback(
        &self,
        awaited: &str,
        awaiter: &str,
    ) -> Result<PromiseRecord> {
        #[derive(Serialize)]
        struct Data<'a> {
            awaited: &'a str,
            awaiter: &'a str,
        }
        let (_, data) = self
            .send_envelope(
                "promise.register_callback",
                &Data { awaited, awaiter },
                false,
            )
            .await?;
        parse_promise(&data)
    }

    /// Register a listener for a promise.
    pub async fn promise_register_listener(
        &self,
        awaited: &str,
        address: &str,
    ) -> Result<PromiseRecord> {
        #[derive(Serialize)]
        struct Data<'a> {
            awaited: &'a str,
            address: &'a str,
        }
        let (_, data) = self
            .send_envelope(
                "promise.register_listener",
                &Data { awaited, address },
                false,
            )
            .await?;
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
        #[derive(Serialize)]
        struct Data<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            state: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            tags: Option<&'a std::collections::HashMap<String, String>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            cursor: Option<&'a str>,
        }
        let (_, data) = self
            .send_envelope(
                "promise.search",
                &Data {
                    state,
                    tags: tags.as_ref(),
                    limit,
                    cursor,
                },
                false,
            )
            .await?;
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
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
        }
        let (_, data) = self
            .send_envelope("schedule.get", &Data { id }, false)
            .await?;
        let schedule = data
            .get("schedule")
            .ok_or_else(|| Error::DecodingError("missing 'schedule' in response".into()))?;
        ScheduleRecord::deserialize(schedule)
            .map_err(|e| Error::DecodingError(format!("invalid schedule record: {}", e)))
    }

    /// Create a schedule.
    pub async fn schedule_create(&self, req: ScheduleCreateReq) -> Result<ScheduleRecord> {
        let (_, data) = self
            .send_envelope("schedule.create", &req, false)
            .await?;
        let schedule = data
            .get("schedule")
            .ok_or_else(|| Error::DecodingError("missing 'schedule' in response".into()))?;
        ScheduleRecord::deserialize(schedule)
            .map_err(|e| Error::DecodingError(format!("invalid schedule record: {}", e)))
    }

    /// Delete a schedule.
    pub async fn schedule_delete(&self, id: &str) -> Result<()> {
        #[derive(Serialize)]
        struct Data<'a> {
            id: &'a str,
        }
        self.send_envelope("schedule.delete", &Data { id }, false)
            .await?;
        Ok(())
    }

    /// Search for schedules.
    pub async fn schedule_search(
        &self,
        tags: Option<std::collections::HashMap<String, String>>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ScheduleSearchResult> {
        #[derive(Serialize)]
        struct Data<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            tags: Option<&'a std::collections::HashMap<String, String>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            cursor: Option<&'a str>,
        }
        let (_, data) = self
            .send_envelope(
                "schedule.search",
                &Data {
                    tags: tags.as_ref(),
                    limit,
                    cursor,
                },
                false,
            )
            .await?;
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

    // ═══════════════════════════════════════════════════════════════
    //  Internal helpers
    // ═══════════════════════════════════════════════════════════════

    fn make_head(&self) -> Head<'_> {
        Head {
            corr_id: format!("sr-{}", crate::now_ms()),
            version: PROTOCOL_VERSION,
            auth: self.auth.as_deref(),
        }
    }

    /// Shared helper for task_create and task_create_or_conflict.
    async fn send_task_create(
        &self,
        pid: &str,
        ttl: i64,
        action: &PromiseCreateReq,
        allow_409: bool,
    ) -> Result<(u16, serde_json::Value)> {
        #[derive(Serialize)]
        struct Data<'a> {
            pid: &'a str,
            ttl: i64,
            action: SubEnvelope<'a, &'a PromiseCreateReq>,
        }
        let data_payload = Data {
            pid,
            ttl,
            action: SubEnvelope {
                kind: "promise.create",
                head: self.make_head(),
                data: action,
            },
        };
        self.send_envelope("task.create", &data_payload, allow_409).await
    }

    /// Serialize an envelope directly to a JSON string and send it.
    /// One allocation (the final String) — no intermediate Value tree.
    async fn send_envelope<D: Serialize>(
        &self,
        kind: &str,
        data: &D,
        allow_409: bool,
    ) -> Result<(u16, serde_json::Value)> {
        let head = self.make_head();
        let corr_id = head.corr_id.clone();
        let envelope = Envelope {
            kind,
            head,
            data,
        };
        let body = serde_json::to_string(&envelope)?;
        let resp = self.transport.send(kind, &corr_id, &body).await?;

        let status = resp
            .get("head")
            .and_then(|h| h.get("status"))
            .and_then(|s| s.as_u64())
            .unwrap_or(200) as u16;

        let data = resp.get("data").cloned().unwrap_or(serde_json::json!({}));

        if status >= 400 && !(allow_409 && status == 409) {
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
//  Typed envelope structs — serialize directly to wire format
// ═══════════════════════════════════════════════════════════════════

#[derive(Serialize)]
struct Envelope<'a, D: Serialize> {
    kind: &'a str,
    head: Head<'a>,
    data: &'a D,
}

#[derive(Serialize)]
struct Head<'a> {
    #[serde(rename = "corrId")]
    corr_id: String,
    version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<&'a str>,
}

#[derive(Serialize)]
struct SubEnvelope<'a, D: Serialize> {
    kind: &'a str,
    head: Head<'a>,
    data: D,
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

    // ── Serialization: verify envelope wire format ─────

    #[test]
    fn envelope_serializes_correct_wire_format() {
        let data = PromiseCreateReq {
            id: "p1".into(),
            timeout_at: 999,
            param: Value::default(),
            tags: HashMap::new(),
        };
        let envelope = Envelope {
            kind: "promise.create",
            head: Head {
                corr_id: "test-corr".into(),
                version: "2025-01-15",
                auth: None,
            },
            data: &data,
        };
        let json: serde_json::Value = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["kind"], "promise.create");
        assert_eq!(json["head"]["corrId"], "test-corr");
        assert_eq!(json["head"]["version"], "2025-01-15");
        assert_eq!(json["data"]["id"], "p1");
        assert_eq!(json["data"]["timeoutAt"], 999);
        // auth should be absent when None
        assert!(json["head"].get("auth").is_none());
    }

    #[test]
    fn sub_envelope_serializes_nested_action() {
        let action = PromiseSettleReq {
            id: "p1".into(),
            state: SettleState::Resolved,
            value: Value::default(),
        };
        let sub = SubEnvelope {
            kind: "promise.settle",
            head: Head {
                corr_id: "sub-corr".into(),
                version: "2025-01-15",
                auth: Some("token123"),
            },
            data: &action,
        };
        let json: serde_json::Value = serde_json::to_value(&sub).unwrap();
        assert_eq!(json["kind"], "promise.settle");
        assert_eq!(json["head"]["corrId"], "sub-corr");
        assert_eq!(json["head"]["auth"], "token123");
        assert_eq!(json["data"]["id"], "p1");
        assert_eq!(json["data"]["state"], "resolved");
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
