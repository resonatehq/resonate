//! The routes, their state, and the handlers behind them.
//!
//! Everything here is the translation between HTTP and the Resonate protocol:
//! bytes to an envelope, an envelope to a
//! [`ResonateServer::process`](resonate_core::ResonateServer::process) call,
//! and a response envelope back to a status and a body. No protocol decisions
//! are made here — what the protocol admits is `core`'s, and what an operation
//! does is the server's.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{any, get, post},
    Json, Router,
};
use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};

use resonate_auth::{auth_check, AuthConfig};
use resonate_core::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_core::{ui, ResonateServer};

lazy_static! {
    /// Requests by kind and status. Registered into prometheus' default
    /// registry, which is what the binary's `/metrics` endpoint gathers — so
    /// these appear alongside the engine's counters without the two crates
    /// having to share a registry handle.
    pub static ref REQUEST_TOTAL: CounterVec = register_counter_vec!(
        "resonate_request_total",
        "Total number of requests by kind and status",
        &["kind", "status"]
    )
    .unwrap();

    /// Request latency by kind.
    pub static ref REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "resonate_request_duration_seconds",
        "Request duration in seconds by kind",
        &["kind"]
    )
    .unwrap();
}

// === Shared application state ===

/// What every handler can reach.
///
/// Ports, not implementations: the server is `dyn ResonateServer`, so this
/// crate fronts the in-process engine, the reference model or a client for a
/// remote server without knowing which.
///
#[derive(Clone)]
pub struct AppState {
    pub server: Arc<dyn ResonateServer>,
    pub auth: Option<Arc<AuthConfig>>,
}

// Sub-state for API handlers — the server, and whether to authenticate.
#[derive(Clone)]
pub struct ApiState {
    pub server: Arc<dyn ResonateServer>,
    pub auth: Option<Arc<AuthConfig>>,
}

impl axum::extract::FromRef<AppState> for ApiState {
    fn from_ref(state: &AppState) -> Self {
        ApiState {
            server: state.server.clone(),
            auth: state.auth.clone(),
        }
    }
}

/// API routes: the RPC endpoint, the readiness probe, and the legacy paths.
pub fn api_routes() -> Router<AppState> {
    Router::new()
        .route("/", post(handle_api))
        .route("/ready", get(handle_ready))
        .route("/promises", any(handle_legacy))
        .route("/promises/*path", any(handle_legacy))
        .route("/schedules", any(handle_legacy))
        .route("/schedules/*path", any(handle_legacy))
        .route("/tasks", any(handle_legacy))
        .route("/tasks/*path", any(handle_legacy))
}

async fn handle_legacy() -> impl IntoResponse {
    tracing::warn!(
        "Legacy endpoint hit — this path is no longer supported. \
        Please update to the latest SDK."
    );
    (
        StatusCode::GONE,
        Json(serde_json::json!({
            "error": "This endpoint is no longer supported. Please update to the latest SDK."
        })),
    )
}

/// The one probe: this process is running and serving.
///
/// It used to ask the server, which pinged its storage, so a pod whose database
/// had gone away answered 503. Nothing here can tell that any more — the port
/// has no `ready` — so this is liveness, and `/health` alongside it said the
/// same thing twice.
async fn handle_ready() -> StatusCode {
    StatusCode::OK
}

fn into_response(resp: ResponseEnvelope) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let code = axum::http::StatusCode::from_u16(resp.head.status as u16)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    (code, Json(resp))
}

async fn handle_api(
    State(api_state): State<ApiState>,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let server = &api_state.server;
    let start = std::time::Instant::now();

    // Parse and validate at the edge. A body that is not a request never
    // reaches the server: `core` decides what the protocol admits and how the
    // rejection reads, and this renders it — which is the only part that is
    // HTTP's. `salvage_context` digs out what it can from bytes that would not
    // parse, so even that answer can be correlated.
    let req: RequestEnvelope = match types::parse_and_validate(&body) {
        Ok(req) => req,
        Err(invalid) => {
            let (kind, corr_id) = types::salvage_context(&body);
            tracing::warn!(kind = %kind, corr_id = %corr_id, reason = %invalid, "Invalid request");
            return into_response(invalid.to_response(kind, corr_id));
        }
    };

    let kind = req.kind.clone();
    let corr_id = req.head.corr_id.clone();

    // The console's namespace is not served here.
    //
    // `ui.*` is a read model shaped for one screen at a time, answered on the
    // console's own route (`resonate_gateway_web`). Refusing it at the worker
    // endpoint is what keeps the two apart: an SDK cannot come to depend on a
    // request that exists to draw a table, and the read model can change
    // without touching the protocol workers speak. The engines would answer it
    // — this is the boundary, not a missing implementation, so it says so.
    if ui::is_ui_kind(&kind) {
        tracing::warn!(kind = %kind, corr_id = %corr_id, "Console request refused on the worker endpoint");
        return into_response(ResponseEnvelope::error(
            kind,
            corr_id,
            404,
            "Console requests ('ui.*') are served on the console's own endpoint, not here",
        ));
    }

    // Log incoming request at the application protocol level
    tracing::info!(
        kind = %kind,
        corr_id = %corr_id,
        "Received request"
    );

    if let Some(auth) = &api_state.auth {
        if let Err(err_response) = auth_check(auth, &req) {
            let status = err_response.head.status.to_string();
            let elapsed_ms = start.elapsed().as_millis();
            tracing::warn!(
                kind = %kind,
                corr_id = %corr_id,
                status = %status,
                elapsed_ms = elapsed_ms,
                "Request rejected by auth"
            );
            REQUEST_TOTAL.with_label_values(&[&kind, &status]).inc();
            REQUEST_DURATION
                .with_label_values(&[&kind])
                .observe(start.elapsed().as_secs_f64());
            return into_response(*err_response);
        }
    }

    let response = match server.process(&req).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!(kind = %kind, corr_id = %corr_id, error = %e, "Server unavailable");
            ResponseEnvelope::error(kind.clone(), corr_id.clone(), 503, &e.to_string())
        }
    };
    let status = response.head.status.to_string();
    let elapsed_ms = start.elapsed().as_millis();

    // Log response outcome — level depends on status
    if response.head.status >= 500 {
        tracing::error!(
            kind = %kind,
            corr_id = %corr_id,
            status = response.head.status,
            elapsed_ms = elapsed_ms,
            "Request failed with internal error"
        );
    } else if response.head.status >= 400 {
        tracing::warn!(
            kind = %kind,
            corr_id = %corr_id,
            status = response.head.status,
            elapsed_ms = elapsed_ms,
            "Request rejected"
        );
    } else {
        tracing::info!(
            kind = %kind,
            corr_id = %corr_id,
            status = response.head.status,
            elapsed_ms = elapsed_ms,
            "Request completed"
        );
    }

    REQUEST_TOTAL.with_label_values(&[&kind, &status]).inc();
    REQUEST_DURATION
        .with_label_values(&[&kind])
        .observe(start.elapsed().as_secs_f64());
    into_response(response)
}
