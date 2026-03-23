use std::sync::Arc;

use crate::error::{Error, Result};
use crate::network::Network;

/// The Transport wraps a Network to add JSON serialization, deserialization,
/// and correlation validation. Resonate and its sub-components use the
/// transport—never the raw network.
#[derive(Clone)]
pub struct Transport {
    network: Arc<dyn Network>,
}

/// A parsed incoming message from the network.
///
/// Mirrors the TS types:
/// ```ts
/// type ExecuteMsg = { kind: "execute"; head: MessageHead; data: { task: { id: string; version: number } } };
/// type UnblockMsg = { kind: "unblock"; head: MessageHead; data: { promise: PromiseRecord } };
/// ```
///
/// NOTE: The Rust LocalNetwork currently emits `"type"` as the tag key
/// while the TS SDK uses `"kind"`. We accept both for compatibility.
#[derive(Debug, Clone)]
pub enum Message {
    Execute(ExecuteMsg),
    Unblock(UnblockMsg),
}

impl<'de> serde::Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = serde_json::Value::deserialize(deserializer)?;
        // Accept both "kind" (TS/server) and "type" (Rust LocalNetwork) as the tag
        let tag = v
            .get("kind")
            .or_else(|| v.get("type"))
            .and_then(|k| k.as_str())
            .unwrap_or("");

        match tag {
            "execute" => {
                let msg = ExecuteMsg::from_json(&v)
                    .ok_or_else(|| serde::de::Error::custom("invalid execute message"))?;
                Ok(Message::Execute(msg))
            }
            "unblock" => {
                let msg = UnblockMsg::from_json(&v)
                    .ok_or_else(|| serde::de::Error::custom("invalid unblock message"))?;
                Ok(Message::Unblock(msg))
            }
            other => Err(serde::de::Error::custom(format!(
                "unknown message kind: {}",
                other
            ))),
        }
    }
}

/// Execute message — server tells this worker to run a task.
///
/// TS shape: `{ kind: "execute", head: { serverUrl? }, data: { task: { id, version } } }`
/// Rust LocalNetwork shape: `{ type: "execute", taskId: "..." }`
///
/// We accept both formats.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecuteMsg {
    /// Task ID — extracted from `data.task.id` (TS) or `taskId` (Rust local).
    #[serde(default)]
    pub task_id: String,
    /// Task version — from `data.task.version` (TS). Default 0 for local.
    #[serde(default)]
    pub version: i64,
}

impl ExecuteMsg {
    /// Parse from a raw JSON value, handling both TS and local formats.
    pub fn from_json(v: &serde_json::Value) -> Option<Self> {
        // TS format: { data: { task: { id, version } } }
        if let Some(data) = v.get("data") {
            if let Some(task) = data.get("task") {
                let id = task
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let version = task.get("version").and_then(|v| v.as_i64()).unwrap_or(0);
                return Some(ExecuteMsg {
                    task_id: id,
                    version,
                });
            }
        }
        // Local format: { taskId: "..." }
        if let Some(id) = v.get("taskId").and_then(|v| v.as_str()) {
            return Some(ExecuteMsg {
                task_id: id.to_string(),
                version: 0,
            });
        }
        None
    }
}

/// Unblock message — a promise this worker is waiting on has been settled.
///
/// TS shape: `{ kind: "unblock", head: { serverUrl? }, data: { promise: PromiseRecord } }`
/// Rust LocalNetwork shape: `{ type: "unblock", promise: { ... } }`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnblockMsg {
    /// The settled promise — from `data.promise` (TS) or `promise` (Rust local).
    #[serde(default)]
    pub promise: serde_json::Value,
}

impl UnblockMsg {
    /// Parse from a raw JSON value, handling both TS and local formats.
    pub fn from_json(v: &serde_json::Value) -> Option<Self> {
        // TS format: { data: { promise: {...} } }
        if let Some(data) = v.get("data") {
            if let Some(promise) = data.get("promise") {
                return Some(UnblockMsg {
                    promise: promise.clone(),
                });
            }
        }
        // Local format: { promise: {...} }
        if let Some(promise) = v.get("promise") {
            return Some(UnblockMsg {
                promise: promise.clone(),
            });
        }
        None
    }
}

impl Transport {
    /// Build a Transport from a Network.
    pub fn new(network: Arc<dyn Network>) -> Self {
        Self { network }
    }

    /// Send a typed request through the network, returning the parsed response.
    /// Validates that `response.kind == request.kind` and `response.corrId == request.corrId`.
    pub async fn send(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        let req_kind = request
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .to_string();
        let req_corr_id = request.get("corrId").cloned();

        let req_str = serde_json::to_string(&request)?;
        tracing::debug!(direction = "send_req", body = %req_str, "transport");

        let resp_str = self.network.send(req_str).await?;
        tracing::debug!(direction = "send_res", body = %resp_str, "transport");

        let response: serde_json::Value = serde_json::from_str(&resp_str)
            .map_err(|e| Error::DecodingError(format!("invalid response JSON: {}", e)))?;

        // Validate kind matches
        let resp_kind = response.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if resp_kind != req_kind {
            return Err(Error::ServerError {
                code: 500,
                message: format!(
                    "response kind mismatch: expected '{}', got '{}'",
                    req_kind, resp_kind
                ),
            });
        }

        // Validate corrId matches
        if let Some(ref expected_corr) = req_corr_id {
            let resp_corr = response.get("corrId");
            if resp_corr != Some(expected_corr) {
                return Err(Error::ServerError {
                    code: 500,
                    message: format!(
                        "response corrId mismatch: expected {:?}, got {:?}",
                        expected_corr, resp_corr
                    ),
                });
            }
        }

        Ok(response)
    }

    /// Register a callback for incoming messages. Parses JSON → Message,
    /// discards invalid messages, forwards valid ones.
    pub fn recv(&self, callback: Box<dyn Fn(Message) + Send + Sync>) {
        self.network.recv(Box::new(move |raw: String| {
            match serde_json::from_str::<Message>(&raw) {
                Ok(msg) => {
                    tracing::debug!(direction = "recv", body = %raw, "transport");
                    callback(msg)
                }
                Err(e) => {
                    tracing::warn!(error = %e, raw = %raw, "failed to parse incoming message");
                }
            }
        }));
    }

    /// Access the underlying network.
    pub fn network(&self) -> &Arc<dyn Network> {
        &self.network
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::LocalNetwork;

    #[tokio::test]
    async fn transport_send_and_validate() {
        let net = Arc::new(LocalNetwork::new(Some("test".into()), None));
        let transport = Transport::new(net);

        let req = serde_json::json!({
            "kind": "promise.create",
            "corrId": "abc123",
            "promise": {
                "id": "p1",
                "timeoutAt": i64::MAX,
            },
        });

        let resp = transport.send(req).await.unwrap();
        assert_eq!(resp["kind"], "promise.create");
        assert_eq!(resp["corrId"], "abc123");
        assert_eq!(resp["promise"]["id"], "p1");
    }
}
