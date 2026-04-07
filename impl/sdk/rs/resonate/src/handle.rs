use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::codec::{deserialize_error, Codec};
use crate::error::{Error, Result};
use crate::types::PromiseState;

/// A handle to a durable promise, returned from `Resonate::run`, `Resonate::rpc`, and `get`.
/// Allows non-blocking observation and eventual awaiting of a durable promise.
pub struct ResonateHandle<T> {
    pub id: String,
    rx: watch::Receiver<Option<PromiseResult>>,
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

#[derive(Clone, Debug)]
pub(crate) struct PromiseResult {
    pub state: PromiseState,
    pub value: serde_json::Value,
}

impl<T: DeserializeOwned> ResonateHandle<T> {
    pub(crate) fn new(
        id: String,
        rx: watch::Receiver<Option<PromiseResult>>,
        codec: Codec,
    ) -> Self {
        Self {
            id,
            rx,
            codec,
            _phantom: PhantomData,
        }
    }

    /// Block until the promise completes, return the result or error.
    pub async fn result(&mut self) -> Result<T> {
        let guard = self
            .rx
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| Error::Application {
                message: "promise channel closed".into(),
            })?;
        let result = guard.as_ref().unwrap().clone();
        drop(guard);
        self.decode_result(&result)
    }

    /// Check if the promise is done (non-blocking).
    pub async fn done(&self) -> Result<bool> {
        Ok(self.rx.borrow().is_some())
    }

    /// Decode a PromiseResult into the final T or error.
    fn decode_result(&self, result: &PromiseResult) -> Result<T> {
        match result.state {
            PromiseState::Resolved => {
                let decoded_val = self.decode_value(&result.value)?;
                let val: T = serde_json::from_value(decoded_val)?;
                Ok(val)
            }
            PromiseState::Rejected => {
                let decoded_val = self.decode_value(&result.value)?;
                Err(deserialize_error(decoded_val))
            }
            PromiseState::RejectedCanceled => Err(Error::Application {
                message: "Promise canceled".to_string(),
            }),
            PromiseState::RejectedTimedout => Err(Error::Timeout),
            PromiseState::Pending => Err(Error::Application {
                message: "promise still pending".to_string(),
            }),
        }
    }

    /// Decode value field from a promise (may be base64-encoded).
    fn decode_value(&self, value: &serde_json::Value) -> Result<serde_json::Value> {
        // Try to decode the value.data field through codec
        if let Some(data) = value.get("data") {
            if let Some(s) = data.as_str() {
                if !s.is_empty() {
                    let decoded: Option<serde_json::Value> =
                        self.codec.decode(&crate::types::Value {
                            headers: None,
                            data: Some(serde_json::Value::String(s.to_string())),
                        })?;
                    return Ok(decoded.unwrap_or(serde_json::Value::Null));
                }
            }
            return Ok(data.clone());
        }
        Ok(value.clone())
    }
}
