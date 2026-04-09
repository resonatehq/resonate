use dashmap::DashMap;
use serde::Serialize;

use crate::codec::Codec;
use crate::error::Result;
use crate::send::Sender;
use crate::types::{PromiseCreateReq, PromiseRecord, PromiseSettleReq, PromiseState, SettleState};

/// The two durable operations the SDK needs. Built from Sender + Codec.
/// Maintains an internal cache of decoded PromiseRecords.
/// Shared via Arc — all contexts in an execution tree use the same instance.
pub struct Effects {
    sender: Sender,
    codec: Codec,
    cache: DashMap<String, PromiseRecord>,
}

impl Effects {
    /// Build Effects from a Sender, Codec, and optional preloaded promises.
    pub fn new(sender: Sender, codec: Codec, preload: Vec<PromiseRecord>) -> Self {
        let map = DashMap::new();
        for p in preload {
            if let Ok(decoded) = codec.decode_promise(p) {
                map.insert(decoded.id.clone(), decoded);
            }
        }

        Self {
            sender,
            codec,
            cache: map,
        }
    }

    /// Create a durable promise. Returns the decoded PromiseRecord.
    /// This is idempotent — if the promise already exists, it returns the existing record.
    pub async fn create_promise(&self, req: PromiseCreateReq) -> Result<PromiseRecord> {
        // 1. Check cache
        if let Some(cached) = self.cache.get(&req.id) {
            return Ok(cached.clone());
        }

        // 2. Encode param via codec
        let encoded_param = self.codec.encode(req.param.data_as_ref())?;
        let encoded_req = PromiseCreateReq {
            id: req.id.clone(),
            timeout_at: req.timeout_at,
            param: encoded_param,
            tags: req.tags.clone(),
        };

        // validation tracing
        let invocation = match encoded_req.tags.get("resonate:scope").map(String::as_str) {
            Some("local") => "run",
            Some("global") => "rpc",
            _ => "unknown",
        };
        tracing::info!(
            target: "resonate::validation",
            promise_id = %encoded_req.id,
            invocation,
            "promise_create_request"
        );

        // 3. Send request
        let record = self.sender.promise_create(encoded_req).await?;

        // 4. Decode response, cache, return
        let decoded = self.codec.decode_promise(record)?;
        self.cache.insert(decoded.id.clone(), decoded.clone());

        tracing::info!(
            target: "resonate::validation",
            promise_id = %decoded.id,
            invocation,
            state = ?decoded.state,
            "promise_create_response"
        );
        Ok(decoded)
    }

    /// Settle a durable promise with a result.
    pub async fn settle_promise<T: Serialize>(
        &self,
        id: &str,
        result: &Result<T>,
    ) -> Result<PromiseRecord> {
        // 1. Check cache — if already settled, return cached
        if let Some(cached) = self.cache.get(id) {
            if cached.state != PromiseState::Pending {
                return Ok(cached.clone());
            }
        }

        // 2. Build settle request
        let (state, value_data) = match result {
            Ok(val) => (SettleState::Resolved, serde_json::to_value(val)?),
            Err(err) => (SettleState::Rejected, crate::codec::encode_error(err)),
        };

        // 3. Encode value via codec
        let encoded_value = self.codec.encode(&value_data)?;

        let req = PromiseSettleReq {
            id: id.to_string(),
            state,
            value: encoded_value,
        };

        // 4. Send request
        tracing::info!(
            target: "resonate::validation",
            promise_id = %req.id,
            state = ?req.state,
            "promise_settle_request"
        );
        let record = self.sender.promise_settle(req).await?;

        // 5. Decode response, cache, return
        let decoded = self.codec.decode_promise(record)?;
        tracing::info!(
            target: "resonate::validation",
            promise_id = %decoded.id,
            state = ?decoded.state,
            "promise_settle_response"
        );
        self.cache.insert(decoded.id.clone(), decoded.clone());
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use crate::types::{PromiseCreateReq, PromiseState, Value};
    use std::collections::HashMap;

    // ── create_promise ──────────────────────────────────────────────

    #[tokio::test]
    async fn create_returns_cached_promise_from_preload_without_hitting_network() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![pending_promise("p1")]);

        let req = PromiseCreateReq {
            id: "p1".to_string(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        let record = effects.create_promise(req).await.unwrap();
        assert_eq!(record.state, PromiseState::Pending);
        assert_eq!(harness.get_send_count(), 0);
    }

    #[tokio::test]
    async fn create_hits_network_when_promise_not_in_preload() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        let req = PromiseCreateReq {
            id: "p2".to_string(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        let record = effects.create_promise(req).await.unwrap();
        assert_eq!(record.state, PromiseState::Pending);
        assert_eq!(harness.get_send_count(), 1);
    }

    #[tokio::test]
    async fn create_adds_to_cache_second_call_uses_cache() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        let req = PromiseCreateReq {
            id: "p3".to_string(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        effects.create_promise(req.clone()).await.unwrap();
        assert_eq!(harness.get_send_count(), 1);

        let record = effects.create_promise(req).await.unwrap();
        assert_eq!(record.state, PromiseState::Pending);
        assert_eq!(harness.get_send_count(), 1);
    }

    // ── settle_promise ──────────────────────────────────────────────

    #[tokio::test]
    async fn settle_returns_cached_when_already_settled_in_preload() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![resolved_promise("s1", serde_json::json!(42))]);

        let record = effects
            .settle_promise("s1", &Ok(serde_json::json!(99)))
            .await
            .unwrap();
        assert_eq!(record.state, PromiseState::Resolved);
        assert_eq!(harness.get_send_count(), 0);
    }

    #[tokio::test]
    async fn settle_hits_network_when_preloaded_promise_is_pending() {
        let harness = TestHarness::new();
        // Seed the stub so settle can find it
        harness.add_promise(pending_promise("s2")).await;
        let effects = harness.build_effects(vec![pending_promise("s2")]);

        let record = effects
            .settle_promise("s2", &Ok(serde_json::json!("ok")))
            .await
            .unwrap();
        assert_eq!(record.state, PromiseState::Resolved);
        assert_eq!(harness.get_send_count(), 1);
    }

    #[tokio::test]
    async fn settle_updates_cache_second_settle_is_cached() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        // Create the promise first
        let req = PromiseCreateReq {
            id: "s3".to_string(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        effects.create_promise(req).await.unwrap();
        assert_eq!(harness.get_send_count(), 1);

        // Settle it
        effects
            .settle_promise("s3", &Ok(serde_json::json!("done")))
            .await
            .unwrap();
        assert_eq!(harness.get_send_count(), 2);

        // Second settle should use cache
        let record = effects
            .settle_promise("s3", &Ok(serde_json::json!("done")))
            .await
            .unwrap();
        assert_eq!(record.state, PromiseState::Resolved);
        assert_eq!(harness.get_send_count(), 2);
    }

    #[tokio::test]
    async fn settle_hits_network_when_promise_not_in_cache() {
        let harness = TestHarness::new();
        // Seed stub directly (not in preload/cache)
        harness.add_promise(pending_promise("s4")).await;
        let effects = harness.build_effects(vec![]);

        let record = effects
            .settle_promise("s4", &Ok(serde_json::json!("ok")))
            .await
            .unwrap();
        assert_eq!(record.state, PromiseState::Resolved);
        assert_eq!(harness.get_send_count(), 1);
    }

    // ── cached promise values ───────────────────────────────────────

    #[tokio::test]
    async fn preloaded_pending_promise_has_decoded_param() {
        let harness = TestHarness::new();
        let param = serde_json::json!({"func": "f", "args": []});
        let effects = harness.build_effects(vec![pending_promise_with_param("v1", param.clone())]);

        let req = PromiseCreateReq {
            id: "v1".to_string(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        let record = effects.create_promise(req).await.unwrap();
        assert_eq!(record.param.data_or_null(), param);
    }

    #[tokio::test]
    async fn preloaded_resolved_promise_has_decoded_value() {
        let harness = TestHarness::new();
        let val = serde_json::json!({"answer": 42});
        let effects = harness.build_effects(vec![resolved_promise("v2", val.clone())]);

        let record = effects
            .settle_promise("v2", &Ok(serde_json::json!(0)))
            .await
            .unwrap();
        assert_eq!(record.value.data_or_null(), val);
    }

    #[tokio::test]
    async fn promise_created_via_network_has_correct_decoded_values() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        let param_data = serde_json::json!({"func": "myFunc", "args": [1, "two"]});
        let req = PromiseCreateReq {
            id: "v3".to_string(),
            timeout_at: i64::MAX,
            param: crate::types::Value {
                headers: None,
                data: Some(param_data.clone()),
            },
            tags: HashMap::new(),
        };
        effects.create_promise(req.clone()).await.unwrap();

        // Second call from cache
        let record = effects.create_promise(req).await.unwrap();
        assert_eq!(record.param.data_or_null(), param_data);
    }

    #[tokio::test]
    async fn promise_settled_via_network_has_correct_decoded_values() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        let req = PromiseCreateReq {
            id: "v4".to_string(),
            timeout_at: i64::MAX,
            param: Value::default(),
            tags: HashMap::new(),
        };
        effects.create_promise(req).await.unwrap();

        let val = serde_json::json!({"result": "success", "count": 7});
        effects
            .settle_promise("v4", &Ok(val.clone()))
            .await
            .unwrap();

        // Second settle from cache
        let record = effects
            .settle_promise("v4", &Ok(val.clone()))
            .await
            .unwrap();
        assert_eq!(record.value.data_or_null(), val);
    }

    #[tokio::test]
    async fn multiple_preloaded_promises_each_have_correct_values() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![
            pending_promise_with_param("m1", serde_json::json!({"func": "f", "args": []})),
            resolved_promise("m2", serde_json::json!("hello")),
            resolved_promise("m3", serde_json::json!([1, 2, 3])),
        ]);

        let r1 = effects
            .create_promise(PromiseCreateReq {
                id: "m1".to_string(),
                timeout_at: i64::MAX,
                param: Value::default(),
                tags: HashMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(r1.state, PromiseState::Pending);
        assert_eq!(
            r1.param.data_or_null(),
            serde_json::json!({"func": "f", "args": []})
        );

        let r2 = effects
            .settle_promise("m2", &Ok(serde_json::json!(0)))
            .await
            .unwrap();
        assert_eq!(r2.state, PromiseState::Resolved);
        assert_eq!(r2.value.data_or_null(), serde_json::json!("hello"));

        let r3 = effects
            .settle_promise("m3", &Ok(serde_json::json!(0)))
            .await
            .unwrap();
        assert_eq!(r3.state, PromiseState::Resolved);
        assert_eq!(r3.value.data_or_null(), serde_json::json!([1, 2, 3]));

        assert_eq!(harness.get_send_count(), 0);
    }
}
