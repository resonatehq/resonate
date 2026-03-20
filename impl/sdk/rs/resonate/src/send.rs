use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::types::{PromiseCreateReq, PromiseRecord, PromiseSettleReq};

/// Request types sent to the Resonate server.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind")]
pub enum Request {
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

/// Response types received from the Resonate server.
///
/// The server echoes back the request kind (e.g., `"promise.create"`, `"task.acquire"`).
/// Because response shapes vary by operation, deserialization is handled manually
/// by `parse_response` rather than via serde's tag dispatch.
#[derive(Debug, Clone)]
pub enum Response {
    Promise(PromiseRecord),

    TaskAcquireResult {
        root_promise: PromiseRecord,
        preloaded: Vec<PromiseRecord>,
    },

    Suspended,

    Redirect {
        preloaded: Vec<PromiseRecord>,
    },
}

/// Parse a server JSON response into a `Response`, using the originating `Request`
/// to determine which response shape to expect.
///
/// The server always echoes the request kind back (e.g. `"promise.create"`,
/// `"task.acquire"`). The response body structure depends on the operation.
pub fn parse_response(req: &Request, json: &serde_json::Value) -> Result<Response> {
    match req {
        Request::PromiseCreate(_) | Request::PromiseSettle(_) => {
            let promise = json.get("promise")
                .ok_or_else(|| Error::DecodingError("missing 'promise' in response".into()))?;
            let record: PromiseRecord = PromiseRecord::deserialize(promise)
                .map_err(|e| Error::DecodingError(format!("invalid promise record: {}", e)))?;
            Ok(Response::Promise(record))
        }

        Request::TaskAcquire { .. } => {
            let promise = json.get("promise")
                .ok_or_else(|| Error::DecodingError("missing 'promise' in task.acquire response".into()))?;
            let root_promise: PromiseRecord = PromiseRecord::deserialize(promise)
                .map_err(|e| Error::DecodingError(format!("invalid promise in task.acquire: {}", e)))?;
            let preloaded = json.get("preload")
                .or_else(|| json.get("preloaded"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| PromiseRecord::deserialize(v).ok())
                        .collect()
                })
                .unwrap_or_default();
            Ok(Response::TaskAcquireResult { root_promise, preloaded })
        }

        Request::TaskFulfill { .. } | Request::TaskRelease { .. } => {
            Ok(Response::Suspended) // Ack — no meaningful data to extract
        }

        Request::TaskSuspend { .. } => {
            // Check for redirect (status 300 in TS SDK, or "redirect": true in local network)
            let is_redirect = json.get("redirect").and_then(|v| v.as_bool()).unwrap_or(false)
                || json.get("status").and_then(|v| v.as_u64()) == Some(300);
            if is_redirect {
                let preloaded = json.get("preload")
                    .or_else(|| json.get("preloaded"))
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| PromiseRecord::deserialize(v).ok())
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(Response::Redirect { preloaded })
            } else {
                Ok(Response::Suspended)
            }
        }
    }
}

/// The Send function type alias.
/// Wraps an HTTP client and handles serialization, deserialization, and error conversion.
pub type SendFn = Arc<
    dyn Fn(Request) -> Pin<Box<dyn Future<Output = Result<Response>> + Send>> + Send + Sync,
>;

/// Build a Send function from a reqwest client and base URL.
pub fn build_send(client: reqwest::Client, base_url: &str) -> SendFn {
    let base = base_url.to_string();
    Arc::new(move |req: Request| {
        let client = client.clone();
        let base = base.clone();
        Box::pin(async move {
            let url = match &req {
                Request::PromiseCreate(_) => format!("{}/promises", base),
                Request::PromiseSettle(r) => format!("{}/promises/{}", base, r.id),
                Request::TaskAcquire { task_id } => {
                    format!("{}/tasks/{}/acquire", base, task_id)
                }
                Request::TaskFulfill { task_id, .. } => {
                    format!("{}/tasks/{}/fulfill", base, task_id)
                }
                Request::TaskSuspend { task_id, .. } => {
                    format!("{}/tasks/{}/suspend", base, task_id)
                }
                Request::TaskRelease { task_id } => {
                    format!("{}/tasks/{}/release", base, task_id)
                }
            };

            let method = match &req {
                Request::PromiseCreate(_) => reqwest::Method::POST,
                Request::PromiseSettle(_) => reqwest::Method::PATCH,
                Request::TaskAcquire { .. } => reqwest::Method::POST,
                Request::TaskFulfill { .. } => reqwest::Method::POST,
                Request::TaskSuspend { .. } => reqwest::Method::POST,
                Request::TaskRelease { .. } => reqwest::Method::POST,
            };

            let res = client
                .request(method, &url)
                .json(&req)
                .send()
                .await?;

            let status = res.status();
            let text = res.text().await?;

            if !status.is_success() && status.as_u16() != 409 {
                return Err(Error::ServerError {
                    code: status.as_u16(),
                    message: text,
                });
            }

            let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                Error::ServerError {
                    code: status.as_u16(),
                    message: format!("invalid response JSON: {} (body: {})", e, text),
                }
            })?;

            parse_response(&req, &json)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::Network;
    use crate::types::{PromiseCreateReq, PromiseSettleReq, SettleState, Value};
    use std::collections::HashMap;

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
        // settle should be a nested object, not flattened
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

    // ── Round-trip: Request → LocalNetwork → parse_response ─────────

    #[tokio::test]
    async fn promise_create_roundtrip_through_local_network() {
        let net = crate::network::LocalNetwork::new(Some("test".into()), None);

        let req = Request::PromiseCreate(PromiseCreateReq {
            id: "rt-p1".into(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        });
        let req_json = serde_json::to_value(&req).unwrap();
        let resp_str = net.send(serde_json::to_string(&req_json).unwrap()).await.unwrap();
        let resp_json: serde_json::Value = serde_json::from_str(&resp_str).unwrap();

        let resp = parse_response(&req, &resp_json).unwrap();
        match resp {
            Response::Promise(record) => {
                assert_eq!(record.id, "rt-p1");
                assert_eq!(record.state, crate::types::PromiseState::Pending);
            }
            other => panic!("expected Promise, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn task_acquire_roundtrip_through_local_network() {
        let net = crate::network::LocalNetwork::new(Some("pid1".into()), None);

        // First create a task
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

        // Now acquire via the Request enum path
        let req = Request::TaskAcquire {
            task_id: task_id.clone(),
        };
        let req_json = serde_json::to_value(&req).unwrap();
        let resp_str = net.send(serde_json::to_string(&req_json).unwrap()).await.unwrap();
        let resp_json: serde_json::Value = serde_json::from_str(&resp_str).unwrap();

        let resp = parse_response(&req, &resp_json).unwrap();
        match resp {
            Response::TaskAcquireResult { root_promise, .. } => {
                assert_eq!(root_promise.id, "rt-p2");
            }
            other => panic!("expected TaskAcquireResult, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn task_fulfill_roundtrip_through_local_network() {
        let net = crate::network::LocalNetwork::new(Some("pid1".into()), None);

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

        // Fulfill via the Request enum path
        let req = Request::TaskFulfill {
            task_id: task_id.clone(),
            settle: PromiseSettleReq {
                id: "rt-p3".into(),
                state: SettleState::Resolved,
                value: Value {
                    headers: None,
                    data: Some(serde_json::json!("result")),
                },
            },
        };
        let req_json = serde_json::to_value(&req).unwrap();
        let resp_str = net.send(serde_json::to_string(&req_json).unwrap()).await.unwrap();
        let resp_json: serde_json::Value = serde_json::from_str(&resp_str).unwrap();

        let resp = parse_response(&req, &resp_json).unwrap();
        // Fulfill returns Suspended (ack)
        assert!(matches!(resp, Response::Suspended));
    }

    #[tokio::test]
    async fn task_suspend_roundtrip_through_local_network() {
        let net = crate::network::LocalNetwork::new(Some("pid1".into()), None);

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

        // Suspend via the Request enum path
        let req = Request::TaskSuspend {
            task_id: task_id.clone(),
            callbacks: vec!["dep-a".into()],
        };
        let req_json = serde_json::to_value(&req).unwrap();
        let resp_str = net.send(serde_json::to_string(&req_json).unwrap()).await.unwrap();
        let resp_json: serde_json::Value = serde_json::from_str(&resp_str).unwrap();

        let resp = parse_response(&req, &resp_json).unwrap();
        assert!(matches!(resp, Response::Suspended));
    }

    #[tokio::test]
    async fn task_release_roundtrip_through_local_network() {
        let net = crate::network::LocalNetwork::new(Some("pid1".into()), None);

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

        // Release via the Request enum path
        let req = Request::TaskRelease {
            task_id: task_id.clone(),
        };
        let req_json = serde_json::to_value(&req).unwrap();
        let resp_str = net.send(serde_json::to_string(&req_json).unwrap()).await.unwrap();
        let resp_json: serde_json::Value = serde_json::from_str(&resp_str).unwrap();

        let resp = parse_response(&req, &resp_json).unwrap();
        assert!(matches!(resp, Response::Suspended));
    }
}
