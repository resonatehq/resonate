use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{any, get, post},
    Json, Router,
};
use serde_json::Value;

use crate::auth;
use crate::config::Config;
use crate::metrics;
use async_trait::async_trait;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope, SUPPORTED_VERSIONS};
use resonate_core::util;
use resonate_core::{ResonateServer, Unavailable};
use resonate_server_dbms::engine::Engine;
use resonate_server_dbms::Storage;
use resonate_transport_http_poll::PollRegistry;

/// The running server — owns configuration, storage, and auth.
pub struct Server {
    pub config: Config,
    pub auth: Option<auth::AuthConfig>,
    /// Durable state and every transition over it. The server validates,
    /// hands over, and shapes what comes back.
    pub engine: Engine,
}

impl Server {
    pub fn new(config: Config, auth: Option<auth::AuthConfig>, storage: Storage) -> Self {
        Self {
            engine: Engine::new(Arc::new(storage), config.debug),
            config,
            auth,
        }
    }
}

// === Shared application state ===

#[derive(Clone)]
pub struct AppState {
    pub server: Arc<Server>,
    pub poll_registry: Arc<PollRegistry>,
    pub sse_shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

// Sub-state for API handlers — only needs the server.
#[derive(Clone)]
pub struct ApiState {
    pub server: Arc<Server>,
}

impl axum::extract::FromRef<AppState> for ApiState {
    fn from_ref(state: &AppState) -> Self {
        ApiState {
            server: state.server.clone(),
        }
    }
}

// Sub-state for poll handler — needs server (for auth) and poll registry.
#[derive(Clone)]
pub struct PollState {
    pub server: Arc<Server>,
    pub poll_registry: Arc<PollRegistry>,
    pub sse_shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl axum::extract::FromRef<AppState> for PollState {
    fn from_ref(state: &AppState) -> Self {
        PollState {
            server: state.server.clone(),
            poll_registry: state.poll_registry.clone(),
            sse_shutdown_rx: state.sse_shutdown_rx.clone(),
        }
    }
}

/// API routes: RPC endpoint, health, readiness.
pub fn api_routes() -> Router<AppState> {
    Router::new()
        .route("/", post(handle_api))
        .route("/health", get(handle_health))
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

/// Poll transport routes: SSE endpoint for workers.
pub fn poll_routes() -> Router<AppState> {
    Router::new().route("/poll/:group/:id", get(handle_poll))
}

async fn handle_health() -> StatusCode {
    StatusCode::OK
}

async fn handle_ready(State(state): State<ApiState>) -> StatusCode {
    match state.server.engine.storage.query(|db| db.ping()).await {
        Ok(()) => StatusCode::OK,
        Err(e) => {
            tracing::error!(error = %e, "Readiness check failed: storage database unavailable");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

fn into_response(resp: ResponseEnvelope) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let code = axum::http::StatusCode::from_u16(resp.head.status as u16)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    (code, Json(resp))
}

/// Best-effort extraction of `kind` and `corrId` from a raw JSON body for
/// error responses when full deserialization fails.
fn extract_error_context(body: &[u8]) -> (String, String) {
    let kind;
    let corr_id;
    if let Ok(raw) = serde_json::from_slice::<Value>(body) {
        kind = raw
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        corr_id = raw
            .get("head")
            .and_then(|h| h.get("corrId"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string();
    } else {
        kind = "unknown".to_string();
        corr_id = "0".to_string();
    }
    (kind, corr_id)
}

async fn handle_api(
    State(api_state): State<ApiState>,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let state = &api_state.server;
    let start = std::time::Instant::now();
    // Deserialize the envelope using serde. On failure, attempt to extract
    // kind from the raw JSON so the error response can include it.
    let req: RequestEnvelope = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            let (kind, corr_id) = extract_error_context(&body);
            tracing::warn!(
                kind = %kind,
                error = %e,
                "Invalid request envelope: deserialization failed"
            );
            return into_response(ResponseEnvelope::error(
                kind,
                corr_id,
                400,
                &format!("Invalid request envelope: {}", e),
            ));
        }
    };

    let kind = req.kind.clone();
    let corr_id = req.head.corr_id.clone();

    // Reject empty kind (serde accepts "" as a valid String)
    if kind.is_empty() {
        tracing::warn!(corr_id = %corr_id, "Invalid request: empty 'kind' field");
        return into_response(ResponseEnvelope::error(
            kind,
            corr_id,
            400,
            "Missing or invalid 'kind' field — must be a non-empty string",
        ));
    }

    // Reject non-object data (serde deserializes any JSON value into Value)
    if !req.data.is_object() {
        tracing::warn!(kind = %kind, corr_id = %corr_id, "Invalid request: 'data' is not an object");
        return into_response(ResponseEnvelope::error(
            kind,
            corr_id,
            400,
            "Invalid 'data' field — must be an object",
        ));
    }

    // Validate protocol version
    if !SUPPORTED_VERSIONS.contains(&req.head.version.as_str()) {
        tracing::warn!(kind = %kind, corr_id = %corr_id, version = %req.head.version, "Invalid request: unsupported protocol version");
        return into_response(ResponseEnvelope::error(
            kind,
            corr_id,
            400,
            &format!(
                "Unsupported protocol version '{}', supported versions: {:?}",
                req.head.version, SUPPORTED_VERSIONS
            ),
        ));
    }

    // Log incoming request at the application protocol level
    tracing::info!(
        kind = %kind,
        corr_id = %corr_id,
        "Received request"
    );

    if let Some(auth) = &state.auth {
        if let Err(err_response) = auth::auth_check(auth, &req) {
            let status = err_response.head.status.to_string();
            let elapsed_ms = start.elapsed().as_millis();
            tracing::warn!(
                kind = %kind,
                corr_id = %corr_id,
                status = %status,
                elapsed_ms = elapsed_ms,
                "Request rejected by auth"
            );
            metrics::REQUEST_TOTAL
                .with_label_values(&[&kind, &status])
                .inc();
            metrics::REQUEST_DURATION
                .with_label_values(&[&kind])
                .observe(start.elapsed().as_secs_f64());
            return into_response(*err_response);
        }
    }

    let response = match state.process(&req).await {
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

    metrics::REQUEST_TOTAL
        .with_label_values(&[&kind, &status])
        .inc();
    metrics::REQUEST_DURATION
        .with_label_values(&[&kind])
        .observe(start.elapsed().as_secs_f64());
    into_response(response)
}

async fn handle_poll(
    State(poll_state): State<PollState>,
    headers: axum::http::HeaderMap,
    Path((group, id)): Path<(String, String)>,
) -> Response {
    // Authenticate when auth is configured.
    if let Some(auth) = &poll_state.server.auth {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));

        if auth::auth_check_token(auth, token).is_err() {
            tracing::warn!(group = %group, id = %id, "Poll connection rejected: unauthorized");
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

    tracing::info!(group = %group, id = %id, "Poll SSE connection requested");
    let registry = &poll_state.poll_registry;

    let rx = registry.register(&group, &id).await;

    match rx {
        Some((conn, mut rx)) => {
            tracing::info!(
                group = %group,
                id = %id,
                conn_id = conn.conn_id,
                "Poll SSE connection established"
            );
            let mut sse_shutdown = poll_state.sse_shutdown_rx.clone();
            let stream = async_stream::stream! {
                let _guard = PollGuard {
                    registry: poll_state.poll_registry.clone(),
                    group: group.clone(),
                    conn_id: conn.conn_id,
                };
                loop {
                    // Check synchronously first (no await — Ref is not held across a yield).
                    if *sse_shutdown.borrow() {
                        break;
                    }
                    tokio::select! {
                        biased;
                        result = sse_shutdown.changed() => {
                            // Sender dropped or value changed; check if shutdown fired.
                            if result.is_err() || *sse_shutdown.borrow() {
                                break;
                            }
                        }
                        msg = rx.recv() => {
                            match msg {
                                Some(msg) => yield Ok::<_, std::convert::Infallible>(Event::default().data(msg)),
                                None => break,
                            }
                        }
                    }
                }
            };

            Sse::new(stream).into_response()
        }
        None => {
            tracing::warn!(group = %group, id = %id, "Poll connection rejected: at capacity");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Poll registration at capacity",
            )
                .into_response()
        }
    }
}

struct PollGuard {
    registry: Arc<PollRegistry>,
    group: String,
    conn_id: u64,
}

impl Drop for PollGuard {
    fn drop(&mut self) {
        let registry = self.registry.clone();
        let group = self.group.clone();
        let conn_id = self.conn_id;
        tokio::spawn(async move {
            registry.deregister(&group, conn_id).await;
        });
    }
}

#[async_trait]
impl ResonateServer for Server {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        // Debug-time overrides are gated by config, so a caller cannot move the
        // server's clock. The gate lives here rather than at the HTTP edge so
        // that every caller of the port is subject to it.
        let debug_time = if self.config.debug {
            req.head.debug_time
        } else {
            None
        };
        Ok(self
            .engine
            .dispatch(req, util::resolve_time(debug_time))
            .await)
    }
}
