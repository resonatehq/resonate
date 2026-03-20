use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

use crate::codec::{deserialize_error, Codec};
use crate::error::{Error, Result};
use crate::transport::Transport;

/// A handle to a durable promise, returned from `begin_run`, `begin_rpc`, and `get`.
/// Allows non-blocking observation and eventual awaiting of a durable promise.
pub struct ResonateHandle<T> {
    pub id: String,
    state: HandleState,
    transport: Transport,
    unicast: String,
    codec: Codec,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for ResonateHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResonateHandle")
            .field("id", &self.id)
            .finish()
    }
}

enum HandleState {
    /// Promise is still pending — we need to wait for completion.
    Pending,
    /// Promise already completed with this value.
    Completed(serde_json::Value),
    /// Promise already failed/rejected with this value.
    Failed(serde_json::Value),
    /// Promise was resolved via a subscription channel.
    Channel(Option<oneshot::Receiver<PromiseResult>>),
}

#[derive(Debug)]
pub(crate) struct PromiseResult {
    pub state: String,
    pub value: serde_json::Value,
}

impl<T: DeserializeOwned> ResonateHandle<T> {
    pub(crate) fn new(
        id: String,
        promise_state: &str,
        promise_value: serde_json::Value,
        transport: Transport,
        unicast: String,
        codec: Codec,
    ) -> Self {
        let state = match promise_state {
            "resolved" => HandleState::Completed(promise_value),
            "rejected" | "rejected_canceled" | "rejected_timedout" => {
                HandleState::Failed(promise_value)
            }
            _ => HandleState::Pending,
        };

        Self {
            id,
            state,
            transport,
            unicast,
            codec,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn from_channel(
        id: String,
        rx: oneshot::Receiver<PromiseResult>,
        transport: Transport,
        unicast: String,
        codec: Codec,
    ) -> Self {
        Self {
            id,
            state: HandleState::Channel(Some(rx)),
            transport,
            unicast,
            codec,
            _phantom: PhantomData,
        }
    }

    /// Block until the promise completes, return the result or error.
    pub async fn result(&mut self) -> Result<T> {
        match std::mem::replace(&mut self.state, HandleState::Pending) {
            HandleState::Completed(v) => {
                let decoded: T = serde_json::from_value(v)?;
                Ok(decoded)
            }
            HandleState::Failed(v) => Err(deserialize_error(v)),
            HandleState::Pending => {
                // Register listener and wait
                let promise = self.register_listener().await?;
                let state = promise
                    .get("state")
                    .and_then(|s| s.as_str())
                    .unwrap_or("pending");

                match state {
                    "resolved" => {
                        let value = promise.get("value").cloned().unwrap_or_default();
                        let decoded_val = self.decode_value(&value)?;
                        let result: T = serde_json::from_value(decoded_val)?;
                        Ok(result)
                    }
                    "rejected" => {
                        let value = promise.get("value").cloned().unwrap_or_default();
                        let decoded_val = self.decode_value(&value)?;
                        Err(deserialize_error(decoded_val))
                    }
                    "rejected_canceled" => Err(Error::Application {
                        message: "Promise canceled".to_string(),
                    }),
                    "rejected_timedout" => Err(Error::Timeout),
                    _ => Err(Error::Application {
                        message: format!("unexpected promise state: {}", state),
                    }),
                }
            }
            HandleState::Channel(rx) => {
                let rx = rx.ok_or_else(|| Error::Application {
                    message: "handle already consumed".to_string(),
                })?;
                let result = rx.await.map_err(|_| Error::Application {
                    message: "subscription channel dropped".to_string(),
                })?;
                match result.state.as_str() {
                    "resolved" => {
                        let decoded_val = self.decode_value(&result.value)?;
                        let val: T = serde_json::from_value(decoded_val)?;
                        Ok(val)
                    }
                    "rejected" => {
                        let decoded_val = self.decode_value(&result.value)?;
                        Err(deserialize_error(decoded_val))
                    }
                    "rejected_canceled" => Err(Error::Application {
                        message: "Promise canceled".to_string(),
                    }),
                    "rejected_timedout" => Err(Error::Timeout),
                    other => Err(Error::Application {
                        message: format!("unexpected promise state: {}", other),
                    }),
                }
            }
        }
    }

    /// Check if the promise is done (non-blocking).
    pub async fn done(&self) -> Result<bool> {
        match &self.state {
            HandleState::Completed(_) | HandleState::Failed(_) => Ok(true),
            HandleState::Pending => {
                let promise = self.register_listener_once().await?;
                let state = promise
                    .get("state")
                    .and_then(|s| s.as_str())
                    .unwrap_or("pending");
                Ok(state != "pending")
            }
            HandleState::Channel(_) => Ok(false),
        }
    }

    /// Register a listener for this promise and return the current state.
    async fn register_listener(&self) -> Result<serde_json::Value> {
        loop {
            let resp = self.register_listener_once().await?;
            let state = resp
                .get("state")
                .and_then(|s| s.as_str())
                .unwrap_or("pending");
            if state != "pending" {
                return Ok(resp);
            }
            // Wait before retrying
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }

    /// Single attempt to register a listener.
    async fn register_listener_once(&self) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "promise.registerListener",
            "corrId": format!("rl-{}", now_ms()),
            "awaited": self.id,
            "address": self.unicast,
        });

        let resp = self.transport.send(req).await?;
        let promise = resp.get("promise").cloned().unwrap_or_default();
        Ok(promise)
    }

    /// Decode value field from a promise (may be base64-encoded).
    fn decode_value(&self, value: &serde_json::Value) -> Result<serde_json::Value> {
        // Try to decode the value.data field through codec
        if let Some(data) = value.get("data") {
            if let Some(s) = data.as_str() {
                if !s.is_empty() {
                    let decoded: Option<serde_json::Value> = self.codec.decode(
                        &crate::types::Value {
                            headers: None,
                            data: Some(serde_json::Value::String(s.to_string())),
                        },
                    )?;
                    return Ok(decoded.unwrap_or(serde_json::Value::Null));
                }
            }
            return Ok(data.clone());
        }
        Ok(value.clone())
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
