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
/// type ExecuteMsg = { kind: "execute"; data: { task: { id: string; version: number } } };
/// type UnblockMsg = { kind: "unblock"; data: { promise: PromiseRecord } };
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum Message {
    #[serde(rename = "execute")]
    Execute(ExecuteMsg),
    #[serde(rename = "unblock")]
    Unblock(UnblockMsg),
}

/// Execute message — server tells this worker to run a task.
///
/// JSON shape: `{ kind: "execute", data: { task: { id, version } } }`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecuteMsg {
    pub data: ExecuteData,
}

impl ExecuteMsg {
    /// Task ID — shorthand for `data.task.id`.
    pub fn task_id(&self) -> &str {
        &self.data.task.id
    }
    /// Task version — shorthand for `data.task.version`.
    pub fn version(&self) -> i64 {
        self.data.task.version
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecuteData {
    pub task: TaskRef,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskRef {
    pub id: String,
    #[serde(default)]
    pub version: i64,
}

/// Unblock message — a promise this worker is waiting on has been settled.
///
/// JSON shape: `{ kind: "unblock", data: { promise: PromiseRecord } }`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnblockMsg {
    pub data: UnblockData,
}

impl UnblockMsg {
    /// The settled promise — shorthand for `data.promise`.
    pub fn promise(&self) -> &serde_json::Value {
        &self.data.promise
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnblockData {
    pub promise: serde_json::Value,
}

/// Extract the `data` portion from a protocol envelope response.
pub fn response_data(resp: &serde_json::Value) -> Result<&serde_json::Value> {
    resp.get("data")
        .ok_or_else(|| Error::DecodingError("response missing 'data' envelope field".into()))
}

/// Extract the `head.status` from a protocol envelope response.
pub fn response_status(resp: &serde_json::Value) -> Result<u64> {
    resp.get("head")
        .and_then(|h| h.get("status"))
        .and_then(|s| s.as_u64())
        .ok_or_else(|| Error::DecodingError("response missing 'head.status' envelope field".into()))
}

impl Transport {
    /// Build a Transport from a Network.
    pub fn new(network: Arc<dyn Network>) -> Self {
        Self { network }
    }

    /// Send a typed request through the network, returning the parsed response.
    /// Validates that `response.kind == request.kind` and correlation IDs match.
    /// Expects protocol envelope format: `{ kind, head: { corrId, ... }, data: { ... } }`.
    pub async fn send(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        let req_kind = request
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .to_string();

        let req_corr_id = request
            .get("head")
            .and_then(|h| h.get("corrId"))
            .cloned();

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
            let resp_corr = response
                .get("head")
                .and_then(|h| h.get("corrId"));
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
    async fn transport_send_and_validate_envelope_format() {
        let net = Arc::new(LocalNetwork::new(Some("test".into()), None));
        let transport = Transport::new(net);

        // Envelope format request
        let req = serde_json::json!({
            "kind": "promise.create",
            "head": {
                "corrId": "env123",
                "version": "2025-01-15",
            },
            "data": {
                "id": "p2",
                "timeoutAt": i64::MAX,
                "param": {},
                "tags": {},
            },
        });

        let resp = transport.send(req).await.unwrap();
        assert_eq!(resp["kind"], "promise.create");
        assert_eq!(resp["head"]["corrId"], "env123");
        assert_eq!(resp["data"]["promise"]["id"], "p2");
    }
}
