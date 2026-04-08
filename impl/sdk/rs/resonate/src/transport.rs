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

    /// Send an already-serialized request through the network, returning the parsed response.
    /// Validates that `response.kind == kind` and `response.head.corrId == corr_id`.
    pub async fn send(
        &self,
        kind: &str,
        corr_id: &str,
        body: &str,
    ) -> Result<serde_json::Value> {
        tracing::debug!(direction = "send_req", body = %body, "transport");

        let resp_str = self.network.send(body.to_owned()).await?;
        tracing::debug!(direction = "send_res", body = %resp_str, "transport");

        let response: serde_json::Value = serde_json::from_str(&resp_str).map_err(|e| {
            Error::DecodingError(format!("invalid response JSON: {e}, resp: {resp_str}"))
        })?;

        // Validate kind matches
        let resp_kind = response
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("");
        if resp_kind != kind {
            return Err(Error::ServerError {
                code: 500,
                message: format!(
                    "response kind mismatch: expected '{}', got '{}'",
                    kind, resp_kind
                ),
            });
        }

        // Validate corrId matches
        let resp_corr = response
            .get("head")
            .and_then(|h| h.get("corrId"))
            .and_then(|c| c.as_str())
            .unwrap_or("");
        if resp_corr != corr_id {
            return Err(Error::ServerError {
                code: 500,
                message: format!(
                    "response corrId mismatch: expected '{}', got '{}'",
                    corr_id, resp_corr
                ),
            });
        }

        Ok(response)
    }

    /// Convenience: serialize a `serde_json::Value` envelope and send it.
    /// Extracts `kind` and `head.corrId` from the value before delegating to [`send`].
    pub async fn send_json(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        let kind = request
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("");
        let corr_id = request
            .get("head")
            .and_then(|h| h.get("corrId"))
            .and_then(|c| c.as_str())
            .unwrap_or("");
        let body = serde_json::to_string(&request)?;
        self.send(kind, corr_id, &body).await
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

        // Envelope format request — pre-serialized
        let body = serde_json::json!({
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
        let body_str = serde_json::to_string(&body).unwrap();

        let resp = transport
            .send("promise.create", "env123", &body_str)
            .await
            .unwrap();
        assert_eq!(resp["kind"], "promise.create");
        assert_eq!(resp["head"]["corrId"], "env123");
        assert_eq!(resp["data"]["promise"]["id"], "p2");
    }
}
