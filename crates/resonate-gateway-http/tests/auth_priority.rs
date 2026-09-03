//! Where the token comes from, and which one wins.
//!
//! The protocol carries auth in the envelope; HTTP carries it in a header. A
//! caller may send either, the SDKs send both, and the server sees one field
//! either way — so what is worth pinning down is the precedence, and that
//! nothing downstream can tell which door the token came through.

use std::sync::{Arc, Mutex};

// axum comes from `resonate-plugin`, so a build has one of it — see that
// crate's re-export for why.
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};
use resonate_gateway_http::routes::{api_routes, AppState};
use resonate_plugin::axum::body::Body;
use resonate_plugin::axum::http::{Request, StatusCode};
use resonate_plugin::axum::Router;
use serde_json::json;
use tower::ServiceExt;

/// Records the `head.auth` it was handed, which is the whole question here.
#[derive(Default)]
struct Recorder(Mutex<Option<Option<String>>>);

#[async_trait::async_trait]
impl ResonateServer for Recorder {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        *self.0.lock().unwrap() = Some(req.head.auth.clone());
        Ok(ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            "ok",
        ))
    }
}

/// The token the server was given, for a request carrying `auth` in the
/// envelope, in the header, in both, or in neither.
async fn seen_by_server(envelope_auth: Option<&str>, header: Option<&str>) -> Option<String> {
    let recorder = Arc::new(Recorder::default());
    let app: Router = api_routes().with_state(AppState {
        server: Arc::clone(&recorder) as Arc<dyn ResonateServer>,
        // No `AuthConfig`: this is about which token reaches the server, not
        // whether it verifies. Turning verification on would reject every
        // token here and hide the difference.
        auth: None,
    });

    let mut head = json!({ "corrId": "c1", "version": "2026-04-01" });
    if let Some(token) = envelope_auth {
        head["auth"] = json!(token);
    }
    let mut req = Request::builder().method("POST").uri("/");
    if let Some(value) = header {
        req = req.header("authorization", value);
    }
    let body = json!({ "kind": "promises.create", "head": head, "data": {} });
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .expect("infallible");
    assert_eq!(
        res.status(),
        StatusCode::OK,
        "the request must reach the server"
    );

    let seen = recorder.0.lock().unwrap().clone();
    seen.expect("the server was called")
}

/// The case this exists for: a caller holding a bearer token and no envelope
/// auth — a browser, or anything speaking HTTP before it speaks the protocol.
#[tokio::test]
async fn a_header_fills_an_envelope_that_left_auth_empty() {
    assert_eq!(
        seen_by_server(None, Some("Bearer from-header")).await,
        Some("from-header".to_string())
    );
}

/// The envelope is the protocol's own field, so it wins. The SDKs send both,
/// which makes this the ordinary path rather than the tiebreak it looks like.
#[tokio::test]
async fn the_envelope_wins_when_both_carry_a_token() {
    assert_eq!(
        seen_by_server(Some("from-envelope"), Some("Bearer from-header")).await,
        Some("from-envelope".to_string())
    );
}

#[tokio::test]
async fn an_envelope_token_arrives_untouched_with_no_header() {
    assert_eq!(
        seen_by_server(Some("from-envelope"), None).await,
        Some("from-envelope".to_string())
    );
}

/// Not a token this endpoint knows how to read. Guessing at it would hand the
/// verifier a base64 credential and call the failure an invalid token.
#[tokio::test]
async fn another_scheme_is_not_read_as_a_bearer_token() {
    assert_eq!(seen_by_server(None, Some("Basic abc123")).await, None);
}

/// Unchanged: no token anywhere is still no token, and `auth_check` is what
/// turns that into a 401 when auth is configured.
#[tokio::test]
async fn nothing_anywhere_stays_nothing() {
    assert_eq!(seen_by_server(None, None).await, None);
}
