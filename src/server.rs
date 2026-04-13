use std::sync::atomic::{AtomicBool, Ordering};
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
use crate::persistence::{
    PromiseCreateParams, PromiseSettleParams, ScheduleCreateParams, Storage, TaskAcquireParams,
    TaskCreateParams, TaskFenceCreateParams, TaskFenceSettleParams, TaskFulfillParams,
};
use crate::processing::processing_timeouts;
use crate::transport::transport_http_poll::PollRegistry;
use crate::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRegisterCallbackData,
    PromiseRegisterListenerData, PromiseResponseData, PromiseSearchData, PromiseSearchResponseData,
    PromiseSettleData, PromiseState, RequestEnvelope, ResponseEnvelope, ScheduleCreateData,
    ScheduleDeleteData, ScheduleGetData, ScheduleResponseData, ScheduleSearchData,
    ScheduleSearchResponseData, TaskAcquireData, TaskAcquireResponseData, TaskContinueData,
    TaskCreateData, TaskCreateResponseData, TaskFenceData, TaskFenceResponseData, TaskFulfillData,
    TaskFulfillResponseData, TaskGetData, TaskHaltData, TaskHeartbeatData, TaskRecord,
    TaskReleaseData, TaskResponseData, TaskSearchData, TaskSearchResponseData, TaskState,
    TaskSuspendData, TaskSuspendPreloadData, SUPPORTED_VERSIONS,
};
use crate::util;
use validator::Validate;

/// The running server — owns configuration, storage, and auth.
pub struct Server {
    pub config: Config,
    pub storage: Arc<Storage>,
    pub auth: Option<auth::AuthConfig>,
    pub debug_mode: AtomicBool,
}

impl Server {
    pub fn new(config: Config, auth: Option<auth::AuthConfig>, storage: Storage) -> Self {
        Self {
            config,
            storage: Arc::new(storage),
            auth,
            debug_mode: AtomicBool::new(false),
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
    match state.server.storage.query(|db| db.ping()).await {
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
    let mut req: RequestEnvelope = match serde_json::from_slice(&body) {
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

    // Gate debug_time behind config
    if !state.config.debug {
        req.head.debug_time = None;
    }

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

    let now = util::resolve_time(req.head.debug_time);

    let response = dispatch(state, &req, now).await;
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

async fn dispatch(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let kind = req.kind.as_str();

    match kind {
        // === Promise operations ===
        "promise.get" => op_promise_get(state, req, now).await,
        "promise.create" => op_promise_create(state, req, now).await,
        "promise.settle" => op_promise_settle(state, req, now).await,
        "promise.register_callback" => op_promise_register_callback(state, req, now).await,
        "promise.register_listener" => op_promise_register_listener(state, req, now).await,
        "promise.search" => op_promise_search(state, req, now).await,

        // === Task operations ===
        "task.get" => op_task_get(state, req, now).await,
        "task.create" => op_task_create(state, req, now).await,
        "task.acquire" => op_task_acquire(state, req, now).await,
        "task.release" => op_task_release(state, req, now).await,
        "task.fulfill" => op_task_fulfill(state, req, now).await,
        "task.suspend" => op_task_suspend(state, req, now).await,
        "task.fence" => op_task_fence(state, req, now).await,
        "task.heartbeat" => op_task_heartbeat(state, req, now).await,
        "task.halt" => op_task_halt(state, req, now).await,
        "task.continue" => op_task_continue(state, req, now).await,
        "task.search" => op_task_search(state, req, now).await,

        // === Schedule operations ===
        "schedule.get" => op_schedule_get(state, req, now).await,
        "schedule.create" => op_schedule_create(state, req, now).await,
        "schedule.delete" => op_schedule_delete(state, req).await,
        "schedule.search" => op_schedule_search(state, req).await,

        // === Debug operations ===
        "debug.start" | "debug.stop" | "debug.reset" | "debug.snap" | "debug.tick"
            if !state.config.debug =>
        {
            ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                403,
                "Debug operations are disabled",
            )
        }
        "debug.start" => {
            state.debug_mode.store(true, Ordering::SeqCst);
            tracing::info!("Debug mode started — background loops paused");
            ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                Value::Object(serde_json::Map::new()),
            )
        }
        "debug.stop" => {
            state.debug_mode.store(false, Ordering::SeqCst);
            tracing::info!("Debug mode stopped — background loops resumed");
            ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                Value::Object(serde_json::Map::new()),
            )
        }
        "debug.reset" => op_debug_reset(state, req).await,
        "debug.snap" => op_debug_snap(state, req).await,
        "debug.tick" => op_debug_tick(state, req).await,

        _ => {
            tracing::warn!(kind = %kind, "Invalid request: unknown operation");
            ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Unknown operation: {}", kind),
            )
        }
    }
}

// ============================================================================
// Promise operations
// ============================================================================

async fn op_promise_get(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: PromiseGetData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            match db.promise_get(&r.id)? {
                Some(promise) => {
                    tracing::debug!(
                        promise_id = %r.id,
                        state = %promise.state,
                        "Promise found"
                    );
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &PromiseResponseData { promise },
                    ))
                }
                None => {
                    tracing::debug!(promise_id = %r.id, "Promise not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Promise not found",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_promise_create(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: PromiseCreateData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let address = r.tags.get("resonate:target").map(|s| s.as_str());
            if let Some(addr) = address {
                if !crate::transport::is_valid_address(addr) {
                    tracing::warn!(
                        promise_id = %r.id,
                        address = addr,
                        "Promise create rejected: invalid resonate:target address"
                    );
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid resonate:target address",
                    ));
                }
            }
            db.try_timeout(&[&r.id], now)?;
            let tags_json = serde_json::to_string(&r.tags).unwrap();
            let already_timedout = now >= r.timeout_at;
            let (state, created_at, settled_at) = if already_timedout {
                let state = if r.tags.get("resonate:timer").map(|v| v.as_str()) == Some("true") {
                    tracing::debug!(promise_id = %r.id, "Promise created already timedout (timer: resolved immediately)");
                    PromiseState::Resolved
                } else {
                    tracing::debug!(promise_id = %r.id, "Promise created already timedout");
                    PromiseState::RejectedTimedout
                };
                (state, r.timeout_at, Some(r.timeout_at))
            } else {
                (PromiseState::Pending, now, None)
            };
            let param_headers_json = r
                .param
                .headers
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap());
            let promise = db.promise_create(&PromiseCreateParams {
                id: &r.id,
                state: state.as_str(),
                param_headers: param_headers_json.as_deref(),
                param_data: r.param.data.as_deref(),
                tags: &tags_json,
                timeout_at: r.timeout_at,
                created_at,
                settled_at,
                already_timedout,
                address,
            })?;
            let is_new = promise.created_at == created_at;
            if is_new {
                tracing::info!(
                    promise_id = %promise.id,
                    state = %promise.state,
                    timeout_at = promise.timeout_at,
                    target = address.unwrap_or("none"),
                    already_timedout = already_timedout,
                    "Promise created"
                );
            } else {
                tracing::debug!(
                    promise_id = %promise.id,
                    state = %promise.state,
                    "Promise create: already exists (idempotent)"
                );
            }
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &PromiseResponseData { promise },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_promise_settle(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: PromiseSettleData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            let value_headers_json = r
                .value
                .headers
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap());
            let result = db.promise_settle(&PromiseSettleParams {
                id: &r.id,
                state: r.state.as_str(),
                value_headers: value_headers_json.as_deref(),
                value_data: r.value.data.as_deref(),
                settled_at: now,
            })?;
            match result.promise {
                Some(promise) => {
                    if !result.was_settled && promise.state == PromiseState::Pending {
                        tracing::debug!(promise_id = %r.id, "Promise settle: TOCTOU race detected, treating as not found");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Promise not found",
                        ));
                    }
                    if result.was_settled {
                        tracing::info!(
                            promise_id = %promise.id,
                            state = %promise.state,
                            "Promise settled"
                        );
                    } else {
                        tracing::debug!(
                            promise_id = %promise.id,
                            current_state = %promise.state,
                            requested_state = %r.state,
                            "Promise settle: already settled (idempotent)"
                        );
                    }
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &PromiseResponseData { promise },
                    ))
                }
                None => {
                    tracing::debug!(promise_id = %r.id, "Promise settle: promise not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Promise not found",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_promise_register_callback(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: PromiseRegisterCallbackData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.awaited, &r.awaiter], now)?;
            let result = db.promise_register_callback(&r.awaited, &r.awaiter, now)?;
            let p_awaited = match result.awaited {
                Some(p) => p,
                None => {
                    tracing::debug!(promise_id = %r.awaited, "Callback registration: awaited promise not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Awaited promise not found",
                    ))
                }
            };
            let p_awaiter = match result.awaiter {
                Some(p) => p,
                None => {
                    tracing::debug!(promise_id = %r.awaiter, "Callback registration: awaiter promise not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        422,
                        "Awaiter promise not found",
                    ))
                }
            };
            if !p_awaiter.tags.contains_key("resonate:target") {
                tracing::debug!(awaiter = %r.awaiter, "Callback registration rejected: awaiter has no resonate:target");
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    422,
                    "Awaiter promise has no resonate:target tag",
                ));
            }
            tracing::info!(
                awaited = %r.awaited,
                awaiter = %r.awaiter,
                awaited_state = %p_awaited.state,
                "Callback registered"
            );
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &PromiseResponseData { promise: p_awaited },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_promise_register_listener(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: PromiseRegisterListenerData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            if !crate::transport::is_valid_address(&r.address) {
                tracing::warn!(
                    awaited = %r.awaited,
                    address = %r.address,
                    "Listener registration rejected: invalid address"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    "Invalid listener address",
                ));
            }
            db.try_timeout(&[&r.awaited], now)?;
            match db.promise_register_listener(&r.awaited, &r.address)? {
                Some(promise) => {
                    tracing::info!(
                        awaited = %r.awaited,
                        address = %r.address,
                        promise_state = %promise.state,
                        "Listener registered"
                    );
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &PromiseResponseData { promise },
                    ))
                }
                None => {
                    tracing::debug!(awaited = %r.awaited, "Listener registration: awaited promise not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Awaited promise not found",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_promise_search(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    _now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: PromiseSearchData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let tags_json = r.tags.as_ref().map(|t| serde_json::to_string(t).unwrap());
            let limit = match r.limit {
                Some(n) if n > 1000 => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid 'limit' — must be between 1 and 1000",
                    ))
                }
                Some(n) => n,
                None => 100,
            };
            let state_str = r.state.map(|s| s.as_str());
            let results = db.promise_search(
                state_str,
                tags_json.as_deref(),
                r.cursor.as_deref(),
                limit + 1,
            )?;
            let has_more = results.len() as i64 > limit;
            let promises: Vec<_> = results.into_iter().take(limit as usize).collect();
            let next_cursor = if has_more {
                promises.last().map(|p| p.id.clone())
            } else {
                None
            };
            tracing::debug!(
                found = promises.len(),
                has_more = has_more,
                "Promise search completed"
            );
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &PromiseSearchResponseData {
                    promises,
                    cursor: next_cursor,
                },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

// ============================================================================
// Task operations
// ============================================================================

async fn op_task_get(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskGetData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            match db.task_get(&r.id)? {
                Some(task) => {
                    tracing::debug!(
                        task_id = %r.id,
                        state = %task.state,
                        version = task.version,
                        "Task found"
                    );
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskResponseData { task },
                    ))
                }
                None => {
                    tracing::debug!(task_id = %r.id, "Task not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_create(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskCreateData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let action_data = &r.action.data;
            let action_id = &action_data.id;
            if let Some(addr) = action_data.tags.get("resonate:target") {
                if !crate::transport::is_valid_address(addr) {
                    tracing::warn!(
                        task_id = %action_id,
                        address = %addr,
                        "Task create rejected: invalid resonate:target address"
                    );
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid resonate:target address",
                    ));
                }
            }
            db.try_timeout(&[action_id], now)?;
            // Lock preamble: ensures CTE and subsequent reads see
            // current state under READ COMMITTED.
            let _ = db.lock_for_update(action_id)?;
            let tags_json = serde_json::to_string(&action_data.tags).unwrap();
            let already_timedout = now >= action_data.timeout_at;
            let (p_state, created_at, settled_at) = if already_timedout {
                let p_state =
                    if action_data.tags.get("resonate:timer").map(|v| v.as_str()) == Some("true") {
                        tracing::debug!(task_id = %action_id, "Task create: already timedout (timer: resolved immediately)");
                        PromiseState::Resolved
                    } else {
                        tracing::debug!(task_id = %action_id, "Task create: already timedout");
                        PromiseState::RejectedTimedout
                    };
                (
                    p_state,
                    action_data.timeout_at,
                    Some(action_data.timeout_at),
                )
            } else {
                (PromiseState::Pending, now, None)
            };
            let param_headers_json = action_data
                .param
                .headers
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap());
            let result = db.task_create(&TaskCreateParams {
                promise_id: action_id,
                state: p_state.as_str(),
                param_headers: param_headers_json.as_deref(),
                param_data: action_data.param.data.as_deref(),
                tags: &tags_json,
                timeout_at: action_data.timeout_at,
                created_at,
                settled_at,
                already_timedout,
                ttl: r.ttl,
                pid: &r.pid,
            })?;
            if result.is_none() {
                tracing::debug!(task_id = %action_id, "Task create: underlying promise not found");
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    404,
                    "Promise not found",
                ));
            }
            let res = result.unwrap();

            // If the promise is settled, process callbacks as a separate
            // statement. This fires any callbacks registered by concurrent
            // transactions (e.g. task.suspend) that committed after
            // try_timeout's snapshot but before now.
            if res.promise.state != PromiseState::Pending {
                db.process_callbacks(action_id, now)?;
            }

            // When the CTE created the task, use CTE result directly.
            if res.task_created {
                let task_state_str = res.task_state.unwrap_or_default();
                let task_state = task_state_str.parse::<TaskState>().unwrap_or(TaskState::Acquired);
                // Acquired tasks start at version 1 (first claim), fulfilled at 0
                let task_version = if task_state == TaskState::Acquired { 1 } else { 0 };
                let task = TaskRecord {
                    id: action_id.to_string(),
                    state: task_state,
                    version: task_version,
                    resumes: 0,
                    ttl: if task_state == TaskState::Fulfilled { None } else { Some(r.ttl) },
                    pid: if task_state == TaskState::Fulfilled { None } else { Some(r.pid.to_string()) },
                };
                let preload = db.compute_preload(action_id)?;
                return Ok(ResponseEnvelope::success(
                    kind_str.clone(),
                    corr_id.clone(),
                    &TaskCreateResponseData {
                        task,
                        promise: res.promise,
                        preload,
                    },
                ));
            }

            // CTE didn't create the task (promise already existed).
            // Statement 2: try to acquire as a SEPARATE statement —
            // gets a fresh READ COMMITTED snapshot that sees all
            // concurrent commits (e.g. task.release making it pending).
            let acquire_result = db.task_acquire(&TaskAcquireParams {
                task_id: action_id,
                version: 0,
                time: now,
                ttl: r.ttl,
                pid: &r.pid,
            })?;
            if !acquire_result.was_acquired {
                // Acquire with version 0 failed. Read current state
                // (fresh snapshot) and retry with actual version.
                let task = db.task_get(action_id)?;
                match &task {
                    None => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            422,
                            "Promise exists without a target task",
                        ));
                    }
                    Some(t) if t.state == TaskState::Fulfilled => {
                        let preload = db.compute_preload(action_id)?;
                        return Ok(ResponseEnvelope::success(
                            kind_str.clone(),
                            corr_id.clone(),
                            &TaskCreateResponseData {
                                task: t.clone(),
                                promise: res.promise,
                                preload,
                            },
                        ));
                    }
                    Some(t) if t.state == TaskState::Pending => {
                        let retry = db.task_acquire(&TaskAcquireParams {
                            task_id: action_id,
                            version: t.version,
                            time: now,
                            ttl: r.ttl,
                            pid: &r.pid,
                        })?;
                        if retry.was_acquired {
                            let task = TaskRecord {
                                id: action_id.to_string(),
                                state: TaskState::Acquired,
                                version: t.version + 1,
                                resumes: 0,
                                ttl: Some(r.ttl),
                                pid: Some(r.pid.to_string()),
                            };
                            let preload = db.compute_preload(action_id)?;
                            return Ok(ResponseEnvelope::success(
                                kind_str.clone(),
                                corr_id.clone(),
                                &TaskCreateResponseData {
                                    task,
                                    promise: res.promise,
                                    preload,
                                },
                            ));
                        }
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            "Already exists",
                        ));
                    }
                    _ => {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            "Already exists",
                        ));
                    }
                }
            }

            // Acquired with version 1 (first claim)
            let task = TaskRecord {
                id: action_id.to_string(),
                state: TaskState::Acquired,
                version: 1,
                resumes: 0,
                ttl: Some(r.ttl),
                pid: Some(r.pid.to_string()),
            };
            let preload = db.compute_preload(action_id)?;
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &TaskCreateResponseData {
                    task,
                    promise: res.promise,
                    preload,
                },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_acquire(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskAcquireData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            let result = db.task_acquire(&TaskAcquireParams {
                task_id: &r.id,
                version: r.version,
                time: now,
                ttl: r.ttl,
                pid: &r.pid,
            })?;
            match result.promise {
                None => {
                    tracing::debug!(task_id = %r.id, "Task acquire: task not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                }
                Some(promise) => {
                    if !result.was_acquired {
                        let task = db.task_get(&r.id)?;
                        if let Some(t) = task {
                            if t.state != TaskState::Pending {
                                tracing::debug!(
                                    task_id = %r.id,
                                    current_state = %t.state,
                                    "Task acquire rejected: not pending"
                                );
                                return Ok(ResponseEnvelope::error(
                                    kind_str.clone(),
                                    corr_id.clone(),
                                    409,
                                    "Task is not pending",
                                ));
                            }
                            if t.version != r.version {
                                tracing::debug!(
                                    task_id = %r.id,
                                    expected_version = r.version,
                                    actual_version = t.version,
                                    "Task acquire rejected: version mismatch"
                                );
                                return Ok(ResponseEnvelope::error(
                                    kind_str.clone(),
                                    corr_id.clone(),
                                    409,
                                    "Version mismatch",
                                ));
                            }
                        }
                        tracing::debug!(
                            task_id = %r.id,
                            "Task acquire rejected: could not acquire (concurrent modification)"
                        );
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            "Task is not pending",
                        ));
                    }
                    // Use known values — no separate task_get that could
                    // see stale state from concurrent transactions.
                    let task = TaskRecord {
                        id: r.id.to_string(),
                        state: TaskState::Acquired,
                        version: r.version + 1,
                        resumes: 0,
                        ttl: Some(r.ttl),
                        pid: Some(r.pid.to_string()),
                    };
                    let preload = db.compute_preload(&r.id)?;
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskAcquireResponseData {
                            task,
                            promise,
                            preload,
                        },
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_release(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskReleaseData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            let task = match db.task_get(&r.id)? {
                Some(t) => t,
                None => {
                    tracing::debug!(task_id = %r.id, "Task release: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                }
            };
            if task.state != TaskState::Acquired {
                tracing::debug!(
                    task_id = %r.id,
                    current_state = %task.state,
                    "Task release rejected: task is not acquired"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task is not acquired",
                ));
            }
            if task.version != r.version {
                tracing::debug!(
                    task_id = %r.id,
                    expected_version = r.version,
                    actual_version = task.version,
                    "Task release rejected: version mismatch"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Version mismatch",
                ));
            }
            let released = db.task_release(&r.id, r.version, now, db.task_retry_timeout())?;
            if !released {
                tracing::debug!(task_id = %r.id, version = r.version, "Task release rejected: version mismatch or invalid state");
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task version mismatch or invalid state",
                ));
            }
            tracing::info!(task_id = %r.id, version = r.version, "Task released back to pending");
            Ok(ResponseEnvelope::new(
                kind_str.clone(),
                corr_id.clone(),
                200,
                serde_json::json!({}),
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_fulfill(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskFulfillData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let action_data = &r.action.data;
            db.try_timeout(&[&action_data.id], now)?;
            // Lock preamble: lock promise + task to prevent stale snapshot
            // in precondition check and fulfillment CTE.
            let _ = db.lock_for_update(&r.id)?;
            let task = match db.task_get(&r.id)? {
                Some(t) => t,
                None => {
                    tracing::debug!(task_id = %r.id, "Task fulfill: task not found");
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                }
            };
            if task.state != TaskState::Acquired {
                tracing::debug!(
                    task_id = %r.id,
                    current_state = %task.state,
                    "Task fulfill rejected: task is not acquired"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task is not acquired",
                ));
            }
            if task.version != r.version {
                tracing::debug!(
                    task_id = %r.id,
                    expected_version = r.version,
                    actual_version = task.version,
                    "Task fulfill rejected: version mismatch"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Version mismatch",
                ));
            }
            let value_headers_json = action_data
                .value
                .headers
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap());
            let result = db.task_fulfill(&TaskFulfillParams {
                task_id: &r.id,
                version: r.version,
                promise_id: &r.id,
                state: action_data.state.as_str(),
                value_headers: value_headers_json.as_deref(),
                value_data: action_data.value.data.as_deref(),
                settled_at: now,
            })?;
            if !result.task_fulfilled {
                tracing::debug!(task_id = %r.id, version = r.version, "Task fulfill rejected: version mismatch or invalid state");
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task version mismatch or invalid state",
                ));
            }
            match result.promise {
                Some(promise) => {
                    tracing::info!(
                        task_id = %r.id,
                        version = r.version,
                        promise_state = %promise.state,
                        "Task fulfilled and promise settled"
                    );
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskFulfillResponseData { promise },
                    ))
                }
                None => {
                    tracing::warn!(task_id = %r.id, "Task fulfilled but promise not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Promise not found",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_suspend(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskSuspendData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let awaited_ids: Vec<String> =
                r.actions.iter().map(|a| a.data.awaited.clone()).collect();
            let mut timeout_ids: Vec<&str> = vec![&r.id];
            for aid in &awaited_ids {
                timeout_ids.push(aid.as_str());
            }
            // Lock the task row BEFORE try_timeout to prevent
            // try_timeout from fulfilling it via promise timeout.
            let (_, task_exists) = db.lock_for_update(&r.id)?;
            db.try_timeout(&timeout_ids, now)?;
            let mut seen = std::collections::HashSet::new();
            let unique_awaited: Vec<&str> = awaited_ids
                .iter()
                .filter(|id| seen.insert(id.as_str()))
                .map(|s| s.as_str())
                .collect();
            let result = db.task_suspend(&r.id, r.version, &unique_awaited)?;
            if !result.task_matched {
                // Use lock_for_update result — no separate task_get that
                // could see a concurrent task creation.
                if !task_exists {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ));
                }
                tracing::debug!(
                    task_id = %r.id,
                    version = r.version,
                    "Task suspend rejected: not acquired or version mismatch"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task is not acquired or version mismatch",
                ));
            }
            if result.missing_count > 0 {
                tracing::debug!(
                    task_id = %r.id,
                    missing_count = result.missing_count,
                    "Task suspend rejected: awaited promise(s) not found"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    422,
                    "Awaited promise not found",
                ));
            }
            if result.was_suspended {
                tracing::info!(
                    task_id = %r.id,
                    version = r.version,
                    awaited_count = unique_awaited.len(),
                    "Task suspended, waiting on promises"
                );
                return Ok(ResponseEnvelope::new(
                    kind_str.clone(),
                    corr_id.clone(),
                    200,
                    serde_json::json!({}),
                ));
            }
            // Immediate resume (settled awaited promises)
            tracing::info!(
                task_id = %r.id,
                version = r.version,
                "Task suspend: immediate resume, awaited promises already settled"
            );
            let preload = db.compute_preload(&r.id)?;
            Ok(ResponseEnvelope::new(
                kind_str.clone(),
                corr_id.clone(),
                300,
                serde_json::to_value(&TaskSuspendPreloadData { preload }).unwrap(),
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_fence(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskFenceData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let action_kind = &r.action.kind;
            let action_data = &r.action.data;
            let action_id = action_data["id"].as_str().unwrap_or("");
            db.try_timeout(&[&r.id, action_id], now)?;
            // Lock preamble: ensures fence check sees current task state.
            let _ = db.lock_for_update(&r.id)?;

            match action_kind.as_str() {
                "promise.create" => {
                    let create_data: PromiseCreateData =
                        match serde_json::from_value(action_data.clone()) {
                            Ok(d) => d,
                            Err(e) => {
                                return Ok(ResponseEnvelope::error(
                                    kind_str.clone(),
                                    corr_id.clone(),
                                    400,
                                    &format!("Invalid action data: {}", e),
                                ))
                            }
                        };
                    if let Err(e) = create_data.validate() {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format_validation_errors(&e),
                        ));
                    }
                    let tags_json = serde_json::to_string(&create_data.tags).unwrap();
                    let already_timedout = now >= create_data.timeout_at;
                    let address = create_data.tags.get("resonate:target").map(|s| s.as_str());
                    if let Some(addr) = address {
                        if !crate::transport::is_valid_address(addr) {
                            tracing::warn!(
                                task_id = %r.id,
                                address = addr,
                                "Task fence rejected: invalid resonate:target address in fenced promise.create"
                            );
                            return Ok(ResponseEnvelope::error(
                                kind_str.clone(),
                                corr_id.clone(),
                                400,
                                "Invalid resonate:target address",
                            ));
                        }
                    }
                    let (p_state, created_at, settled_at) = if already_timedout {
                        let p_state = if create_data.tags.get("resonate:timer").map(|v| v.as_str())
                            == Some("true")
                        {
                            PromiseState::Resolved
                        } else {
                            PromiseState::RejectedTimedout
                        };
                        (
                            p_state,
                            create_data.timeout_at,
                            Some(create_data.timeout_at),
                        )
                    } else {
                        (PromiseState::Pending, now, None)
                    };
                    let param_headers_json = create_data
                        .param
                        .headers
                        .as_ref()
                        .map(|h| serde_json::to_string(h).unwrap());
                    let result = db.task_fence_create(&TaskFenceCreateParams {
                        task_id: &r.id,
                        version: r.version,
                        promise_id: &create_data.id,
                        state: p_state.as_str(),
                        param_headers: param_headers_json.as_deref(),
                        param_data: create_data.param.data.as_deref(),
                        tags: &tags_json,
                        timeout_at: create_data.timeout_at,
                        created_at,
                        settled_at,
                        already_timedout,
                        address,
                    })?;
                    if !result.task_exists {
                        tracing::debug!(task_id = %r.id, fenced_action = "promise.create", "Task fence rejected: task not found");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        ));
                    }
                    if !result.fence_ok {
                        tracing::debug!(task_id = %r.id, version = r.version, fenced_action = "promise.create", "Task fence rejected: version mismatch");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            "Version mismatch",
                        ));
                    }
                    tracing::info!(
                        task_id = %r.id,
                        version = r.version,
                        fenced_action = "promise.create",
                        promise_id = %create_data.id,
                        "Task fence: promise.create executed"
                    );
                    let inner_status = if result.promise.is_some() { 200 } else { 404 };
                    let inner_data = match &result.promise {
                        Some(p) => serde_json::json!({ "promise": p }),
                        None => serde_json::json!("Promise not found"),
                    };
                    let inner_envelope = serde_json::json!({
                        "kind": action_kind,
                        "head": { "corrId": corr_id, "status": inner_status, "version": "2026-04-01" },
                        "data": inner_data,
                    });
                    let preload = db.compute_preload(&r.id)?;
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskFenceResponseData {
                            action: inner_envelope,
                            preload,
                        },
                    ))
                }
                "promise.settle" => {
                    let settle_data: PromiseSettleData =
                        match serde_json::from_value(action_data.clone()) {
                            Ok(d) => d,
                            Err(e) => {
                                return Ok(ResponseEnvelope::error(
                                    kind_str.clone(),
                                    corr_id.clone(),
                                    400,
                                    &format!("Invalid action data: {}", e),
                                ))
                            }
                        };
                    if let Err(e) = settle_data.validate() {
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            400,
                            &format_validation_errors(&e),
                        ));
                    }
                    let value_headers_json = settle_data
                        .value
                        .headers
                        .as_ref()
                        .map(|h| serde_json::to_string(h).unwrap());
                    let result = db.task_fence_settle(&TaskFenceSettleParams {
                        task_id: &r.id,
                        version: r.version,
                        promise_id: &settle_data.id,
                        state: settle_data.state.as_str(),
                        value_headers: value_headers_json.as_deref(),
                        value_data: settle_data.value.data.as_deref(),
                        settled_at: now,
                    })?;
                    if !result.task_exists {
                        tracing::debug!(task_id = %r.id, fenced_action = "promise.settle", "Task fence rejected: task not found");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            404,
                            "Task not found",
                        ));
                    }
                    if !result.fence_ok {
                        tracing::debug!(task_id = %r.id, version = r.version, fenced_action = "promise.settle", "Task fence rejected: version mismatch");
                        return Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            "Version mismatch",
                        ));
                    }
                    tracing::info!(
                        task_id = %r.id,
                        version = r.version,
                        fenced_action = "promise.settle",
                        promise_id = %settle_data.id,
                        settle_state = %settle_data.state,
                        "Task fence: promise.settle executed"
                    );
                    let inner_status = if result.promise.is_some() { 200 } else { 404 };
                    let inner_data = match &result.promise {
                        Some(p) => serde_json::json!({ "promise": p }),
                        None => serde_json::json!("Promise not found"),
                    };
                    let inner_envelope = serde_json::json!({
                        "kind": action_kind,
                        "head": { "corrId": corr_id, "status": inner_status, "version": "2026-04-01" },
                        "data": inner_data,
                    });
                    let preload = db.compute_preload(&r.id)?;
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &TaskFenceResponseData {
                            action: inner_envelope,
                            preload,
                        },
                    ))
                }
                _ => {
                    tracing::warn!(
                        task_id = %r.id,
                        action_kind = %action_kind,
                        "Task fence rejected: invalid fence action kind"
                    );
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid fence action kind",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_heartbeat(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskHeartbeatData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let task_pairs: Vec<(&str, i64)> =
                r.tasks.iter().map(|t| (t.id.as_str(), t.version)).collect();
            db.task_heartbeat(&r.pid, &task_pairs, now)?;
            tracing::debug!(
                pid = %r.pid,
                task_count = task_pairs.len(),
                "Task heartbeat processed"
            );
            Ok(ResponseEnvelope::new(
                kind_str.clone(),
                corr_id.clone(),
                200,
                serde_json::json!({}),
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_halt(state: &Arc<Server>, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskHaltData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            let result = db.task_halt(&r.id)?;
            if !result.task_exists {
                tracing::debug!(task_id = %r.id, "Task halt: not found");
                Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    404,
                    "Task not found",
                ))
            } else if result.task_fulfilled {
                tracing::debug!(task_id = %r.id, "Task halt rejected: already fulfilled");
                Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    409,
                    "Task is fulfilled",
                ))
            } else {
                tracing::info!(task_id = %r.id, "Task halted");
                Ok(ResponseEnvelope::new(
                    kind_str.clone(),
                    corr_id.clone(),
                    200,
                    serde_json::json!({}),
                ))
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_continue(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskContinueData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            db.try_timeout(&[&r.id], now)?;
            let result = db.task_continue(&r.id, now)?;
            match result.state {
                None => {
                    tracing::debug!(task_id = %r.id, "Task continue: not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Task not found",
                    ))
                }
                Some(_state) => {
                    if result.continued {
                        tracing::info!(task_id = %r.id, "Task continued from halted state");
                        Ok(ResponseEnvelope::new(
                            kind_str.clone(),
                            corr_id.clone(),
                            200,
                            serde_json::json!({}),
                        ))
                    } else {
                        tracing::debug!(task_id = %r.id, "Task continue rejected: not halted");
                        Ok(ResponseEnvelope::error(
                            kind_str.clone(),
                            corr_id.clone(),
                            409,
                            "Task is not halted",
                        ))
                    }
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_task_search(state: &Arc<Server>, req: &RequestEnvelope, _now: i64) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: TaskSearchData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let limit = match r.limit {
                Some(n) if n > 1000 => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid 'limit' — must be between 1 and 1000",
                    ))
                }
                Some(n) => n,
                None => 100,
            };
            let state_str = r.state.map(|s| s.as_str());
            let results = db.task_search(state_str, r.cursor.as_deref(), limit + 1)?;
            let has_more = results.len() as i64 > limit;
            let tasks: Vec<_> = results.into_iter().take(limit as usize).collect();
            let next_cursor = if has_more {
                tasks.last().map(|t| t.id.clone())
            } else {
                None
            };
            tracing::debug!(
                found = tasks.len(),
                has_more = has_more,
                "Task search completed"
            );
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &TaskSearchResponseData {
                    tasks,
                    cursor: next_cursor,
                },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

// ============================================================================
// Schedule operations
// ============================================================================

async fn op_schedule_get(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    _now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: ScheduleGetData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            match db.schedule_get(&r.id)? {
                Some(schedule) => {
                    tracing::debug!(
                        schedule_id = %r.id,
                        cron = %schedule.cron,
                        next_run_at = schedule.next_run_at,
                        "Schedule found"
                    );
                    Ok(ResponseEnvelope::success(
                        kind_str.clone(),
                        corr_id.clone(),
                        &ScheduleResponseData { schedule },
                    ))
                }
                None => {
                    tracing::debug!(schedule_id = %r.id, "Schedule not found");
                    Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        404,
                        "Schedule not found",
                    ))
                }
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_schedule_create(
    state: &Arc<Server>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: ScheduleCreateData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            if !util::is_valid_cron(&r.cron) {
                tracing::warn!(
                    schedule_id = %r.id,
                    cron = %r.cron,
                    "Schedule create rejected: invalid cron expression"
                );
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    "Invalid cron expression",
                ));
            }
            let promise_tags_json = serde_json::to_string(&r.promise_tags).unwrap();
            let next_run_at = util::compute_next_cron(&r.cron, now);
            let promise_param_headers_json = r
                .promise_param
                .headers
                .as_ref()
                .map(|h| serde_json::to_string(h).unwrap());
            let schedule = db.schedule_create(&ScheduleCreateParams {
                id: &r.id,
                cron: &r.cron,
                promise_id: &r.promise_id,
                promise_timeout: r.promise_timeout,
                promise_param_headers: promise_param_headers_json.as_deref(),
                promise_param_data: r.promise_param.data.as_deref(),
                promise_tags: &promise_tags_json,
                created_at: now,
                next_run_at,
            })?;
            tracing::info!(
                schedule_id = %schedule.id,
                cron = %schedule.cron,
                next_run_at = schedule.next_run_at,
                "Schedule created"
            );
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &ScheduleResponseData { schedule },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_schedule_delete(state: &Arc<Server>, req: &RequestEnvelope) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: ScheduleDeleteData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            if db.schedule_delete(&r.id)? {
                tracing::info!(schedule_id = %r.id, "Schedule deleted");
                Ok(ResponseEnvelope::new(
                    kind_str.clone(),
                    corr_id.clone(),
                    200,
                    serde_json::json!({}),
                ))
            } else {
                tracing::debug!(schedule_id = %r.id, "Schedule delete: not found");
                Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    404,
                    "Schedule not found",
                ))
            }
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

async fn op_schedule_search(state: &Arc<Server>, req: &RequestEnvelope) -> ResponseEnvelope {
    let data = req.data.clone();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();
    match state
        .storage
        .transact(move |db| {
            let r: ScheduleSearchData = match serde_json::from_value(data.clone()) {
                Ok(d) => d,
                Err(e) => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        &format!("Invalid request: {}", e),
                    ))
                }
            };
            if let Err(e) = r.validate() {
                return Ok(ResponseEnvelope::error(
                    kind_str.clone(),
                    corr_id.clone(),
                    400,
                    &format_validation_errors(&e),
                ));
            }
            let tags_json = r.tags.as_ref().map(|t| serde_json::to_string(t).unwrap());
            let limit = match r.limit {
                Some(n) if n > 1000 => {
                    return Ok(ResponseEnvelope::error(
                        kind_str.clone(),
                        corr_id.clone(),
                        400,
                        "Invalid 'limit' — must be between 1 and 1000",
                    ))
                }
                Some(n) => n,
                None => 10,
            };
            let schedules =
                db.schedule_search(tags_json.as_deref(), r.cursor.as_deref(), limit + 1)?;
            let limit_usize = limit as usize;
            let has_more = schedules.len() > limit_usize;
            let result_schedules: Vec<_> = schedules.into_iter().take(limit_usize).collect();
            let next_cursor = if has_more {
                result_schedules.last().map(|s| s.id.clone())
            } else {
                None
            };
            tracing::debug!(
                found = result_schedules.len(),
                has_more = has_more,
                "Schedule search completed"
            );
            Ok(ResponseEnvelope::success(
                kind_str.clone(),
                corr_id.clone(),
                &ScheduleSearchResponseData {
                    schedules: result_schedules,
                    cursor: next_cursor,
                },
            ))
        })
        .await
    {
        Ok(resp) => resp,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Internal error: {}", e),
        ),
    }
}

// ============================================================================
// Debug operations
// ============================================================================

async fn op_debug_reset(state: &Arc<Server>, req: &RequestEnvelope) -> ResponseEnvelope {
    match state.storage.transact(move |db| db.debug_reset()).await {
        Ok(()) => {
            tracing::warn!("Debug reset: all data cleared");
            ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                Value::Object(serde_json::Map::new()),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "Debug reset failed");
            ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Reset failed: {}", e),
            )
        }
    }
}

async fn op_debug_snap(state: &Arc<Server>, req: &RequestEnvelope) -> ResponseEnvelope {
    match state.storage.query(move |db| db.snap()).await {
        Ok(snapshot) => {
            let data = serde_json::to_value(snapshot).unwrap_or(Value::Null);
            ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, data)
        }
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Snap failed: {}", e),
        ),
    }
}

async fn op_debug_tick(state: &Arc<Server>, req: &RequestEnvelope) -> ResponseEnvelope {
    let time = match req.data.get("time").and_then(|v| v.as_i64()) {
        Some(t) => t,
        None => {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                "Missing or invalid 'time' field",
            )
        }
    };

    match state
        .storage
        .transact(move |db| processing_timeouts::process_all_timeouts(db, time))
        .await
    {
        Ok(_) => ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            Value::Array(vec![]),
        ),
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            500,
            &format!("Tick failed: {}", e),
        ),
    }
}
