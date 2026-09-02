//! The console's routes, driven the way a browser drives them.
//!
//! Two things are worth pinning down here and nowhere else: that the built app
//! actually comes out of the binary over HTTP, and that the boundary holds —
//! `ui.*` is answered on the console's route and refused on the worker's.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use resonate_core::ResonateServer;
use resonate_gateway_web::{ConsoleState, MOUNT, RPC_PATH};
use resonate_server_dbms::oracle::SharedOracle;
use serde_json::{json, Value};
use tower::ServiceExt;

fn console() -> Router {
    let server: Arc<dyn ResonateServer> = Arc::new(SharedOracle::with_preload_limit(10));
    resonate_gateway_web::routes::<()>(
        &resonate_gateway_web::Config::default(),
        ConsoleState { server, auth: None },
    )
    .expect("the console is enabled by default")
}

async fn get(app: &Router, path: &str) -> (StatusCode, Vec<(String, String)>, String) {
    let res = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .expect("infallible");
    let status = res.status();
    let headers = res
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
        .collect();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&body).to_string())
}

async fn post(app: &Router, path: &str, envelope: Value) -> (StatusCode, Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(envelope.to_string()))
                .unwrap(),
        )
        .await
        .expect("infallible");
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

fn envelope(kind: &str, data: Value) -> Value {
    json!({
        "kind": kind,
        "head": { "corrId": "c1", "version": "2026-04-01" },
        "data": data,
    })
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

#[tokio::test]
async fn the_app_shell_is_served_from_the_binary() {
    let app = console();
    for path in [MOUNT, "/console/"] {
        let (status, headers, body) = get(&app, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(header(&headers, "content-type"), "text/html; charset=utf-8");
        // The shell names hashed asset files, so it must never be cached.
        assert_eq!(header(&headers, "cache-control"), "no-cache");
        assert!(body.contains("Resonate Console"), "{path}");
    }
}

#[tokio::test]
async fn every_client_route_resolves_to_the_shell() {
    // Deep links are the point: an execution's URL has to survive being pasted
    // into a fresh tab, where the server sees the path before the router does.
    let app = console();
    for path in [
        "/console/executions",
        "/console/executions/checkout.order-8842",
        "/console/executions/checkout.order-8842?step=checkout.order-8842:2",
        // A dot in a path is not an extension: ids are full of them.
        "/console/executions/sync.stripe-hourly.44",
        "/console/schedules",
        "/console/settings",
    ] {
        let (status, _, body) = get(&app, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(body.contains("Resonate Console"), "{path}");
    }
}

#[tokio::test]
async fn assets_are_served_and_the_hashed_ones_are_cached_forever() {
    let app = console();
    let (status, headers, body) = get(&app, "/console/fonts/inter-latin.woff2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(header(&headers, "content-type"), "font/woff2");
    assert!(!body.is_empty());

    // The shell links its scripts by hashed name; find one and fetch it.
    let (_, _, shell) = get(&app, "/console/").await;
    let script = shell
        .split('"')
        .find(|s| s.starts_with("/console/app/immutable/") && s.ends_with(".js"))
        .expect("the shell links at least one script");
    let (status, headers, _) = get(&app, script).await;
    assert_eq!(status, StatusCode::OK, "{script}");
    assert_eq!(
        header(&headers, "content-type"),
        "text/javascript; charset=utf-8"
    );
    assert_eq!(
        header(&headers, "cache-control"),
        "public, max-age=31536000, immutable"
    );
}

#[tokio::test]
async fn a_missing_file_is_a_404_and_not_the_shell() {
    // Answering HTML to a request for a script is the failure that costs an
    // hour to diagnose, so a path that looks like a file never falls back.
    let app = console();
    for path in [
        "/console/app/immutable/nope.js",
        "/console/missing.css",
        "/console/x.woff2",
    ] {
        let (status, _, body) = get(&app, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert!(!body.contains("Resonate Console"), "{path}");
    }
}

#[tokio::test]
async fn the_api_root_sends_a_browser_to_the_console() {
    let app = console();
    let (status, headers, _) = get(&app, "/").await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(header(&headers, "location"), "/console/");
}

#[tokio::test]
async fn the_console_endpoint_answers_the_ui_requests() {
    let app = console();
    let (status, body) = post(
        &app,
        RPC_PATH,
        envelope("ui.executions.search", json!({ "countTotal": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["head"]["corrId"], "c1");
    assert_eq!(body["data"]["items"], json!([]));
    assert_eq!(body["data"]["total"], 0);

    let (status, body) = post(&app, RPC_PATH, envelope("ui.schedules.search", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["items"], json!([]));

    let (status, body) = post(
        &app,
        RPC_PATH,
        envelope("ui.execution.get", json!({ "id": "nope" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["data"]["error"], "not_found");
}

#[tokio::test]
async fn the_console_endpoint_also_carries_the_one_write() {
    // Cancel is `promise.settle`, the real request — there is no `ui.*` request
    // that mutates, so the console's endpoint cannot be limited to `ui.*`.
    let app = console();
    let (status, _) = post(
        &app,
        RPC_PATH,
        envelope(
            "promise.create",
            json!({
                "id": "checkout.order-1",
                "timeoutAt": 9_999_999_999_999i64,
                "tags": { "resonate:scope": "global" }
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = post(
        &app,
        RPC_PATH,
        envelope(
            "promise.settle",
            json!({ "id": "checkout.order-1", "state": "rejected_canceled" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = post(&app, RPC_PATH, envelope("ui.executions.search", json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["items"][0]["id"], "checkout.order-1");
    assert_eq!(body["data"]["items"][0]["state"], "rejected_canceled");
}

#[tokio::test]
async fn a_malformed_envelope_is_refused_at_the_edge() {
    let app = console();
    let res = console()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(RPC_PATH)
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    let (status, _) = post(&app, RPC_PATH, envelope("", json!({}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_disabled_console_serves_nothing() {
    let server: Arc<dyn ResonateServer> = Arc::new(SharedOracle::with_preload_limit(10));
    let routes = resonate_gateway_web::routes::<()>(
        &resonate_gateway_web::Config {
            enabled: false,
            redirect_root: true,
        },
        ConsoleState { server, auth: None },
    );
    assert!(routes.is_none(), "nothing to merge when it is off");
}

// --- composition ------------------------------------------------------------
//
// The console is merged into the gateway's own router and served on its port,
// so the two have to fit: no path collides, and the boundary between the two
// endpoints is real rather than a comment.

fn merged() -> Router {
    let oracle = Arc::new(SharedOracle::with_preload_limit(10));
    let server: Arc<dyn ResonateServer> = oracle.clone();
    let weak: std::sync::Weak<dyn ResonateServer> = Arc::downgrade(&oracle) as _;
    let poll_registry = Arc::new(resonate_transport_http_poll::PollRegistry::new(
        weak,
        resonate_transport_http_poll::Config {
            enabled: true,
            max_connections: 4,
            buffer_size: 4,
            keepalive_interval_ms: 0,
        },
    ));
    let console = resonate_gateway_web::routes::<resonate_gateway_http::AppState>(
        &resonate_gateway_web::Config::default(),
        ConsoleState {
            server: server.clone(),
            auth: None,
        },
    )
    .expect("enabled");

    // This is the merge the composition root performs. It panics if the two
    // routers claim the same method on the same path — `POST /` and `GET /`
    // are the pair that has to stay apart.
    resonate_gateway_http::routes::api_routes()
        .merge(resonate_gateway_http::routes::poll_routes())
        .merge(console)
        .with_state(resonate_gateway_http::AppState {
            server,
            auth: None,
            poll_registry,
        })
}

#[tokio::test]
async fn the_console_and_the_protocol_share_one_port() {
    let app = merged();

    // The worker endpoint, unchanged.
    let (status, body) = post(
        &app,
        "/",
        envelope("promise.get", json!({ "id": "nothing" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    // The console, on the same router.
    let (status, _, html) = get(&app, "/console/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Resonate Console"));

    // And health still answers.
    let (status, _, _) = get(&app, "/health").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_worker_endpoint_refuses_the_console_namespace() {
    // The boundary, stated as a test: `ui.*` is answered on the console's route
    // and nowhere else, so an SDK cannot come to depend on a request that
    // exists to draw a table.
    let app = merged();
    for kind in resonate_gateway_web::ui_kinds() {
        let (status, body) = post(&app, "/", envelope(kind, json!({}))).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{kind} must not be served at /"
        );
        assert!(
            body["data"]
                .as_str()
                .unwrap_or_default()
                .contains("console's own endpoint"),
            "{kind}: {body}"
        );

        // The same request, on the console's route, is *answered* — with a
        // page, or with this namespace's own structured refusal. An empty
        // server has no execution called `absent`, and `not_found` is an
        // answer to the question, not a refusal to hear it.
        let (status, body) = post(&app, RPC_PATH, envelope(kind, ui_probe(kind))).await;
        assert!(
            status.is_success() || body["data"]["error"] == "not_found",
            "{kind} must be served at {RPC_PATH}: {status} {body}"
        );
    }
}

/// The minimum `data` each console request needs against an empty server.
fn ui_probe(kind: &str) -> Value {
    match kind {
        "ui.execution.get" => json!({ "id": "absent" }),
        _ => json!({}),
    }
}
