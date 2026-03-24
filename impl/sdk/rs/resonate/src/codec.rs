use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::{PromiseRecord, Value};

/// Encryption trait for codec (default: no-op).
pub trait Encryptor: Send + Sync {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>>;
}

/// No-op encryptor (passthrough).
pub struct NoopEncryptor;
impl Encryptor for NoopEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Handles encoding/decoding of values for the durability boundary.
///
/// Encode: Rust value → JSON → encrypt → base64 → Value { headers, data }
/// Decode: Value { headers, data } → base64 → decrypt → JSON → Rust value
#[derive(Clone)]
pub struct Codec {
    encryptor: Arc<dyn Encryptor>,
}

impl std::fmt::Debug for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Codec").finish()
    }
}

impl Codec {
    pub fn new(encryptor: Arc<dyn Encryptor>) -> Self {
        Self { encryptor }
    }

    /// Encode a serializable value into the wire format.
    pub fn encode(&self, value: &impl Serialize) -> Result<Value> {
        let json_val = serde_json::to_value(value)?;
        if json_val.is_null() {
            return Ok(Value {
                headers: None,
                data: Some(serde_json::Value::String(String::new())),
            });
        }
        let json_str = serde_json::to_string(&json_val)?;
        let encrypted = self.encryptor.encrypt(json_str.as_bytes())?;
        let b64 = BASE64.encode(&encrypted);
        Ok(Value {
            headers: None,
            data: Some(serde_json::Value::String(b64)),
        })
    }

    /// Decode a wire-format value back into a Rust type.
    pub fn decode<T: DeserializeOwned>(&self, value: &Value) -> Result<Option<T>> {
        let data = value.data_as_ref();
        let s = match data {
            serde_json::Value::String(s) if s.is_empty() => return Ok(None),
            serde_json::Value::String(s) => s,
            serde_json::Value::Null => return Ok(None),
            _ => return Err(Error::DecodingError("expected string or null data".into())),
        };
        let bytes = BASE64.decode(s)?;
        let decrypted = self.encryptor.decrypt(&bytes)?;
        let json_str = String::from_utf8(decrypted)?;
        let decoded: T = serde_json::from_str(&json_str)?;
        Ok(Some(decoded))
    }

    /// Decode an entire PromiseRecord's param and value fields, consuming the original.
    pub fn decode_promise(&self, promise: PromiseRecord) -> Result<PromiseRecord> {
        let decoded_param_data: serde_json::Value = self
            .decode(&promise.param)?
            .unwrap_or(serde_json::Value::Null);
        let decoded_value_data: serde_json::Value = self
            .decode(&promise.value)?
            .unwrap_or(serde_json::Value::Null);

        Ok(PromiseRecord {
            id: promise.id,
            state: promise.state,
            timeout_at: promise.timeout_at,
            param: Value {
                headers: promise.param.headers,
                data: Some(decoded_param_data),
            },
            value: Value {
                headers: promise.value.headers,
                data: Some(decoded_value_data),
            },
            tags: promise.tags,
            created_at: promise.created_at,
            settled_at: promise.settled_at,
        })
    }

    /// Decode a promise from a raw JSON value (as returned by the network).
    /// Parses the JSON into a PromiseRecord, then decodes param/value fields.
    pub fn decode_promise_from_json(&self, json: &serde_json::Value) -> Result<PromiseRecord> {
        let record: PromiseRecord = PromiseRecord::deserialize(json)
            .map_err(|e| Error::DecodingError(format!("invalid promise JSON: {}", e)))?;
        self.decode_promise(record)
    }
}

/// Encode an error for durable storage.
pub fn encode_error(err: &Error) -> serde_json::Value {
    serde_json::json!({
        "__type": "error",
        "message": err.to_string(),
    })
}

/// Decode an entire PromiseRecord's param and value fields, returning decoded copies.
/// (Public alias for testing convenience.)
impl Codec {
    /// Check if a string is valid base64.
    pub fn is_valid_base64(s: &str) -> bool {
        BASE64.decode(s).is_ok()
    }
}

/// Deserialize an error value from a rejected promise.
pub fn deserialize_error(value: serde_json::Value) -> Error {
    if let Some(obj) = value.as_object() {
        if let Some(msg) = obj.get("message").and_then(|m| m.as_str()) {
            return Error::Application {
                message: msg.to_string(),
            };
        }
    }
    Error::Application {
        message: format!("unknown error: {}", value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PromiseRecord, PromiseState};
    use std::collections::HashMap;

    fn codec() -> Codec {
        Codec::new(Arc::new(NoopEncryptor))
    }

    // ── encode/decode roundtrip for primitives ──────────────────────

    #[test]
    fn roundtrip_integer() {
        let c = codec();
        let encoded = c.encode(&42).unwrap();
        let decoded: Option<i64> = c.decode(&encoded).unwrap();
        assert_eq!(decoded, Some(42));
    }

    #[test]
    fn roundtrip_string() {
        let c = codec();
        let encoded = c.encode(&"hello").unwrap();
        let decoded: Option<String> = c.decode(&encoded).unwrap();
        assert_eq!(decoded, Some("hello".to_string()));
    }

    #[test]
    fn roundtrip_bool() {
        let c = codec();
        let encoded = c.encode(&true).unwrap();
        let decoded: Option<bool> = c.decode(&encoded).unwrap();
        assert_eq!(decoded, Some(true));
    }

    // ── encode/decode roundtrip for objects ──────────────────────────

    #[test]
    fn roundtrip_object() {
        let c = codec();
        let obj = serde_json::json!({"func": "f", "args": [1, "two"]});
        let encoded = c.encode(&obj).unwrap();
        let decoded: Option<serde_json::Value> = c.decode(&encoded).unwrap();
        assert_eq!(decoded, Some(obj));
    }

    // ── encode/decode roundtrip for arrays ──────────────────────────

    #[test]
    fn roundtrip_array() {
        let c = codec();
        let arr = serde_json::json!([1, 2, 3]);
        let encoded = c.encode(&arr).unwrap();
        let decoded: Option<serde_json::Value> = c.decode(&encoded).unwrap();
        assert_eq!(decoded, Some(arr));
    }

    // ── encode None/unit produces empty data ────────────────────────

    #[test]
    fn encode_null_produces_empty_data() {
        let c = codec();
        let encoded = c.encode(&serde_json::Value::Null).unwrap();
        assert_eq!(
            encoded.data_or_null(),
            serde_json::Value::String(String::new())
        );

        let decoded: Option<serde_json::Value> = c.decode(&encoded).unwrap();
        assert!(decoded.is_none());
    }

    // ── encode produces base64 string ───────────────────────────────

    #[test]
    fn encode_produces_valid_base64() {
        let c = codec();
        let encoded = c.encode(&"hello").unwrap();
        let data_str = encoded.data_or_null();
        let data_str = data_str.as_str().unwrap();
        assert!(Codec::is_valid_base64(data_str));

        // Decoding base64 should produce valid JSON
        let bytes = BASE64.decode(data_str).unwrap();
        let json_str = String::from_utf8(bytes).unwrap();
        let _: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    }

    // ── decode_promise decodes both param and value ─────────────────

    #[test]
    fn decode_promise_decodes_param_and_value() {
        let c = codec();
        let param_encoded = c.encode(&serde_json::json!({"func": "f"})).unwrap();
        let value_encoded = c.encode(&serde_json::json!({"result": 42})).unwrap();

        let record = PromiseRecord {
            id: "test".to_string(),
            state: PromiseState::Resolved,
            timeout_at: 0,
            param: param_encoded,
            value: value_encoded,
            tags: HashMap::new(),
            created_at: 0,
            settled_at: Some(1),
        };

        let decoded = c.decode_promise(record).unwrap();
        assert_eq!(
            decoded.param.data_or_null(),
            serde_json::json!({"func": "f"})
        );
        assert_eq!(
            decoded.value.data_or_null(),
            serde_json::json!({"result": 42})
        );
    }

    // ── decode invalid base64 returns error ─────────────────────────

    #[test]
    fn decode_invalid_base64_returns_error() {
        let c = codec();
        let bad_value = Value {
            headers: None,
            data: Some(serde_json::Value::String("not-base64!!!".to_string())),
        };
        let result: Result<Option<serde_json::Value>> = c.decode(&bad_value);
        assert!(result.is_err());
    }

    // ── encode error produces correct shape ─────────────────────────

    #[test]
    fn encode_error_produces_correct_shape() {
        let err = Error::Application {
            message: "boom".to_string(),
        };
        let encoded = encode_error(&err);
        assert_eq!(encoded["__type"], "error");
        assert_eq!(encoded["message"], "application error: boom");
    }
}
