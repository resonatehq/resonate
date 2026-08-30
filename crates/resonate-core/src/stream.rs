//! Ephemeral streams — the request algebra, without the delivery.
//!
//! A stream is a best-effort side channel attached to a durable promise. A
//! producer opens one with `stream.bos`, writes chunks with `stream.put`, and
//! closes it with `stream.eos`, naming the count so a consumer can tell a
//! finished stream from a truncated one. Nothing here is durable and nothing
//! here is retried: the promise carries the result, and a consumer that finds a
//! gap reads it rather than guessing.
//!
//! **These three operations are no-ops today.** The requests are admitted,
//! validated and answered — a client can speak the whole protocol against a
//! server that does nothing with it — but no chunk reaches anyone, because
//! working out who is interested and how they are reached is a separate
//! question from what the operations *are*. Keeping the two apart is the point:
//! the request shapes settle first, uncontaminated by routing.
//!
//! It lives in `core`, beside the types it validates, so that every
//! [`ResonateServer`](crate::ResonateServer) answers these kinds identically.
//! The trait promises its implementations are interchangeable; two hand-written
//! copies of a no-op would be two chances to drift apart.

use serde::de::DeserializeOwned;
use validator::Validate;

use crate::types::{
    format_validation_errors, RequestEnvelope, ResponseEnvelope, StreamBosData, StreamEosData,
    StreamPutData,
};

/// The kinds this module answers.
///
/// Named once so an adapter with its own opinion about them — authorization,
/// metrics, a client that offers them as methods — can ask rather than
/// hard-code the list.
pub const KINDS: &[&str] = &["stream.bos", "stream.put", "stream.eos"];

/// Is `kind` a stream operation?
pub fn is_stream_kind(kind: &str) -> bool {
    KINDS.contains(&kind)
}

/// Answer a stream request, or `None` if this is not one.
///
/// `None` is what lets a caller chain this ahead of its own dispatch without
/// having to know the list: a server calls it first, returns what it gets, and
/// otherwise carries on into the operations it does implement.
///
/// The answer is a real one — `data` is parsed against the operation's struct
/// and validated, so a malformed stream request is refused with 400 exactly as
/// it will be once delivery exists. Only the effect is missing.
pub fn process(req: &RequestEnvelope) -> Option<ResponseEnvelope> {
    let outcome = match req.kind.as_str() {
        "stream.bos" => admit::<StreamBosData>(req),
        "stream.put" => admit::<StreamPutData>(req),
        "stream.eos" => admit::<StreamEosData>(req),
        _ => return None,
    };
    Some(match outcome {
        // Nothing to report: a stream request that was accepted produced no
        // state to read back and, for now, no delivery to describe.
        Ok(()) => ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            serde_json::json!({}),
        ),
        Err(resp) => resp,
    })
}

/// Parse and validate a request's `data`, discarding it.
///
/// Discarding is the whole behaviour today, and it is deliberately the *last*
/// step rather than the first: skipping the parse would make these kinds
/// accept anything, and every client written against them would encode
/// something subtly wrong that only surfaces when delivery lands.
fn admit<T: DeserializeOwned + Validate>(req: &RequestEnvelope) -> Result<(), ResponseEnvelope> {
    let bad = |message: String| {
        ResponseEnvelope::error(req.kind.clone(), req.head.corr_id.clone(), 400, &message)
    };
    let data: T = serde_json::from_value(req.data.clone())
        .map_err(|e| bad(format!("Invalid request: {}", e)))?;
    data.validate()
        .map_err(|e| bad(format_validation_errors(&e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RequestHead, PROTOCOL_VERSION};
    use serde_json::json;

    fn req(kind: &str, data: serde_json::Value) -> RequestEnvelope {
        RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: "c1".to_string(),
                version: PROTOCOL_VERSION.to_string(),
                auth: None,
                debug_time: None,
            },
            data,
        }
    }

    fn status(kind: &str, data: serde_json::Value) -> i32 {
        process(&req(kind, data))
            .expect("a stream kind is answered")
            .head
            .status
    }

    #[test]
    fn a_non_stream_kind_is_not_answered() {
        assert!(process(&req("promise.get", json!({ "id": "p1" }))).is_none());
        assert!(process(&req("stream.flush", json!({}))).is_none());
    }

    #[test]
    fn a_well_formed_request_succeeds_and_says_nothing() {
        let resp = process(&req(
            "stream.put",
            json!({ "origin": "f", "promiseId": "f:1", "offset": 0, "body": "aGk=" }),
        ))
        .unwrap();
        assert_eq!(resp.head.status, 200);
        assert_eq!(resp.data, json!({}));
        assert_eq!(resp.kind, "stream.put");
        assert_eq!(resp.head.corr_id, "c1");
    }

    #[test]
    fn every_stream_kind_is_answered() {
        assert_eq!(
            status("stream.bos", json!({ "origin": "f", "promiseId": "f:1" })),
            200
        );
        assert_eq!(
            status(
                "stream.put",
                json!({ "origin": "f", "promiseId": "f:1", "offset": 3, "body": "" })
            ),
            200
        );
        assert_eq!(
            status(
                "stream.eos",
                json!({ "origin": "f", "promiseId": "f:1", "count": 4 })
            ),
            200
        );
        for kind in KINDS {
            assert!(is_stream_kind(kind));
        }
    }

    #[test]
    fn the_root_may_stream_for_itself() {
        assert_eq!(
            status("stream.bos", json!({ "origin": "f", "promiseId": "f" })),
            200
        );
    }

    #[test]
    fn a_promise_outside_the_named_origin_is_refused() {
        assert_eq!(
            status("stream.bos", json!({ "origin": "f", "promiseId": "g:1" })),
            400
        );
    }

    #[test]
    fn missing_and_malformed_fields_are_refused() {
        // Absent field.
        assert_eq!(
            status(
                "stream.put",
                json!({ "origin": "f", "promiseId": "f:1", "offset": 0 })
            ),
            400
        );
        // Empty id.
        assert_eq!(
            status("stream.bos", json!({ "origin": "", "promiseId": "f:1" })),
            400
        );
        // Wrong type.
        assert_eq!(
            status(
                "stream.eos",
                json!({ "origin": "f", "promiseId": "f:1", "count": "4" })
            ),
            400
        );
        // An index is a position, never negative; a count is a total, likewise.
        assert_eq!(
            status(
                "stream.put",
                json!({ "origin": "f", "promiseId": "f:1", "offset": -1, "body": "" })
            ),
            400
        );
        assert_eq!(
            status(
                "stream.eos",
                json!({ "origin": "f", "promiseId": "f:1", "count": -1 })
            ),
            400
        );
    }

    #[test]
    fn bos_carries_optional_headers() {
        assert_eq!(
            status(
                "stream.bos",
                json!({ "origin": "f", "promiseId": "f:1", "headers": { "content-type": "text/plain" } })
            ),
            200
        );
    }
}
