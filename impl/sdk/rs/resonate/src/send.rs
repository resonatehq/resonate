use serde::Deserialize;

use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::types::{PromiseCreateReq, PromiseRecord, PromiseSettleReq};

// ═══════════════════════════════════════════════════════════════════
//  Public result types
// ═══════════════════════════════════════════════════════════════════

/// Result of acquiring a task.
#[derive(Debug, Clone)]
pub struct TaskAcquireResult {
    pub root_promise: PromiseRecord,
    pub preloaded: Vec<PromiseRecord>,
}

/// Result of suspending a task — either actually suspended or redirected.
#[derive(Debug, Clone)]
pub enum SuspendResult {
    Suspended,
    Redirect { preloaded: Vec<PromiseRecord> },
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
}

impl Sender {
    pub fn new(transport: Transport) -> Self {
        Self { transport }
    }

    /// Acquire a task, returning the root promise and any preloaded promises.
    pub async fn task_acquire(&self, task_id: &str) -> Result<TaskAcquireResult> {
        let req = Request::TaskAcquire {
            task_id: task_id.to_string(),
        };
        let json = self.send_request(&req).await?;
        parse_task_acquire(&json)
    }

    /// Fulfill a task by settling its root promise.
    pub async fn task_fulfill(&self, task_id: &str, settle: PromiseSettleReq) -> Result<()> {
        let req = Request::TaskFulfill {
            task_id: task_id.to_string(),
            settle,
        };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Suspend a task, registering callbacks for awaited promises.
    /// Returns whether the task was actually suspended or redirected.
    pub async fn task_suspend(
        &self,
        task_id: &str,
        callbacks: Vec<String>,
    ) -> Result<SuspendResult> {
        let req = Request::TaskSuspend {
            task_id: task_id.to_string(),
            callbacks,
        };
        let json = self.send_request(&req).await?;
        parse_suspend_result(&json)
    }

    /// Release a task (give up the lock without fulfilling).
    pub async fn task_release(&self, task_id: &str) -> Result<()> {
        let req = Request::TaskRelease {
            task_id: task_id.to_string(),
        };
        self.send_request(&req).await?;
        Ok(())
    }

    /// Create a durable promise.
    pub async fn promise_create(&self, req: PromiseCreateReq) -> Result<PromiseRecord> {
        let request = Request::PromiseCreate(req);
        let json = self.send_request(&request).await?;
        parse_promise(&json)
    }

    /// Settle (resolve/reject) a durable promise.
    pub async fn promise_settle(&self, req: PromiseSettleReq) -> Result<PromiseRecord> {
        let request = Request::PromiseSettle(req);
        let json = self.send_request(&request).await?;
        parse_promise(&json)
    }

    /// Internal: serialize a Request, send via transport, return raw JSON response.
    async fn send_request(&self, req: &Request) -> Result<serde_json::Value> {
        let req_json = serde_json::to_value(req)?;
        self.transport.send(req_json).await
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Request enum (internal serialization format)
// ═══════════════════════════════════════════════════════════════════

/// Request types sent to the Resonate server.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind")]
pub(crate) enum Request {
    #[serde(rename = "promise.create")]
    PromiseCreate(PromiseCreateReq),

    #[serde(rename = "promise.settle")]
    PromiseSettle(PromiseSettleReq),

    #[serde(rename = "task.acquire")]
    TaskAcquire {
        #[serde(rename = "taskId")]
        task_id: String,
    },

    #[serde(rename = "task.fulfill")]
    TaskFulfill {
        #[serde(rename = "taskId")]
        task_id: String,
        settle: PromiseSettleReq,
    },

    #[serde(rename = "task.suspend")]
    TaskSuspend {
        #[serde(rename = "taskId")]
        task_id: String,
        callbacks: Vec<String>,
    },

    #[serde(rename = "task.release")]
    TaskRelease {
        #[serde(rename = "taskId")]
        task_id: String,
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
    let promise = json.get("promise").ok_or_else(|| {
        Error::DecodingError("missing 'promise' in task.acquire response".into())
    })?;
    let root_promise: PromiseRecord = PromiseRecord::deserialize(promise)
        .map_err(|e| Error::DecodingError(format!("invalid promise in task.acquire: {}", e)))?;
    let preloaded = parse_preloaded(json);
    Ok(TaskAcquireResult {
        root_promise,
        preloaded,
    })
}

/// Parse a task.suspend response — either Suspended or Redirect.
fn parse_suspend_result(json: &serde_json::Value) -> Result<SuspendResult> {
    let is_redirect = json
        .get("redirect")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || json.get("status").and_then(|v| v.as_u64()) == Some(300);

    if is_redirect {
        Ok(SuspendResult::Redirect {
            preloaded: parse_preloaded(json),
        })
    } else {
        Ok(SuspendResult::Suspended)
    }
}

/// Extract preloaded promises from a response (accepts both "preload" and "preloaded" keys).
fn parse_preloaded(json: &serde_json::Value) -> Vec<PromiseRecord> {
    json.get("preload")
        .or_else(|| json.get("preloaded"))
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
    use crate::types::{PromiseCreateReq, PromiseSettleReq, SettleState, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_sender(net: Arc<LocalNetwork>) -> Sender {
        Sender::new(Transport::new(net))
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
            task_id: "t1".into(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.acquire");
        assert_eq!(json["taskId"], "t1");
    }

    #[test]
    fn task_fulfill_serializes_with_dot_kind_and_nested_settle() {
        let req = Request::TaskFulfill {
            task_id: "t1".into(),
            settle: PromiseSettleReq {
                id: "p1".into(),
                state: SettleState::Resolved,
                value: Value::default(),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.fulfill");
        assert_eq!(json["taskId"], "t1");
        assert_eq!(json["settle"]["id"], "p1");
        assert_eq!(json["settle"]["state"], "resolved");
    }

    #[test]
    fn task_suspend_serializes_with_dot_kind() {
        let req = Request::TaskSuspend {
            task_id: "t1".into(),
            callbacks: vec!["dep-a".into(), "dep-b".into()],
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.suspend");
        assert_eq!(json["taskId"], "t1");
        assert_eq!(json["callbacks"], serde_json::json!(["dep-a", "dep-b"]));
    }

    #[test]
    fn task_release_serializes_with_dot_kind() {
        let req = Request::TaskRelease {
            task_id: "t1".into(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "task.release");
        assert_eq!(json["taskId"], "t1");
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

    #[tokio::test]
    async fn task_acquire_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // First create a task via raw network
        let create_req = serde_json::json!({
            "kind": "task.create",
            "corrId": "c1",
            "pid": "pid1",
            "ttl": 60000,
            "promise": {
                "id": "rt-p2",
                "timeoutAt": i64::MAX,
                "param": {"data": "test"},
                "tags": {},
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let create_resp: serde_json::Value = serde_json::from_str(&resp_str).unwrap();
        let task_id = create_resp["task"]["id"].as_str().unwrap().to_string();

        // Release the task so we can re-acquire
        let release_req = serde_json::json!({
            "kind": "task.release",
            "corrId": "c2",
            "taskId": task_id,
        });
        net.send(release_req.to_string()).await.unwrap();

        // Now acquire via Sender
        let sender = test_sender(net);
        let result = sender.task_acquire(&task_id).await.unwrap();
        assert_eq!(result.root_promise.id, "rt-p2");
    }

    #[tokio::test]
    async fn task_fulfill_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // Create a task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "corrId": "c1",
            "pid": "pid1",
            "ttl": 60000,
            "promise": {
                "id": "rt-p3",
                "timeoutAt": i64::MAX,
                "param": {"data": "test"},
                "tags": {},
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let create_resp: serde_json::Value = serde_json::from_str(&resp_str).unwrap();
        let task_id = create_resp["task"]["id"].as_str().unwrap().to_string();

        // Fulfill via Sender
        let sender = test_sender(net);
        sender
            .task_fulfill(
                &task_id,
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
    }

    #[tokio::test]
    async fn task_suspend_roundtrip() {
        let net = Arc::new(LocalNetwork::new(Some("pid1".into()), None));

        // Create a task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "corrId": "c1",
            "pid": "pid1",
            "ttl": 60000,
            "promise": {
                "id": "rt-p4",
                "timeoutAt": i64::MAX,
                "param": {"data": "test"},
                "tags": {},
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let create_resp: serde_json::Value = serde_json::from_str(&resp_str).unwrap();
        let task_id = create_resp["task"]["id"].as_str().unwrap().to_string();

        // Suspend via Sender
        let sender = test_sender(net);
        let result = sender
            .task_suspend(&task_id, vec!["dep-a".into()])
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
            "corrId": "c1",
            "pid": "pid1",
            "ttl": 60000,
            "promise": {
                "id": "rt-p5",
                "timeoutAt": i64::MAX,
            },
        });
        let resp_str = net.send(create_req.to_string()).await.unwrap();
        let create_resp: serde_json::Value = serde_json::from_str(&resp_str).unwrap();
        let task_id = create_resp["task"]["id"].as_str().unwrap().to_string();

        // Release via Sender
        let sender = test_sender(net);
        sender.task_release(&task_id).await.unwrap();
    }
}
