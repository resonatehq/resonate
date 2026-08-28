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

use crate::auth;
use crate::config::Config;
use crate::metrics;
use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, UnblockMsg, UnblockMsgData,
    UnblockMsgHead,
};
use resonate_core::util;
use resonate_core::{ResonateServer, Unavailable};
use resonate_server_dbms::engine_port::{
    Input, Outgoing, Output, ResonateEngine, Scheduled, Timeout,
};

use crate::deadlines::DeadlineTimer;
use resonate_transport_http_poll::PollRegistry;

/// The running server — owns configuration, the engine, auth, and the router.
pub struct Server {
    pub config: Config,
    pub auth: Option<auth::AuthConfig>,
    /// Durable state and every transition over it. The server validates,
    /// hands over, and shapes what comes back.
    pub engine: Arc<dyn ResonateEngine>,
    /// Where a transition's messages go.
    ///
    /// A server without one could not deliver anything it produced, so it is
    /// not optional and not late-bound: `deliver` has a router or the server
    /// does not exist. The ring — server holds router, router holds workers,
    /// worker holds server — is closed by `Arc::new_cyclic` at startup, which
    /// puts the one weak link on the workers' handle, where it belongs: a
    /// worker outliving its server is a real condition at shutdown, whereas a
    /// server without a router was only ever an artifact of the wiring order.
    router: Arc<dyn ResonateRouter>,
    /// The near future, in memory.
    ///
    /// Every deadline a transition arms is merged here, and the timer asks the
    /// engine for the one it names the moment it comes due. A cache, not a
    /// record: what it holds is a bounded prefix of what one process has heard
    /// about, and the sweep in `processing_timeouts` is what covers the rest.
    timer: DeadlineTimer,
}

impl Server {
    pub fn new(
        config: Config,
        auth: Option<auth::AuthConfig>,
        engine: Arc<dyn ResonateEngine>,
        router: Arc<dyn ResonateRouter>,
        timer: DeadlineTimer,
    ) -> Self {
        Self {
            engine,
            config,
            auth,
            router,
            timer,
        }
    }

    /// Start the timer and wait for it to be seeded.
    ///
    /// Separate from construction because seeding reads the database, and
    /// because the timer's own callbacks point back here: nothing can run until
    /// the server is behind an `Arc`.
    pub async fn start_timer(&self) {
        self.timer.init().await;
    }

    /// Stop the timer task.
    pub async fn stop_timer(&self) {
        self.timer.stop().await;
    }

    /// Hand the timer the deadlines a transition just armed.
    ///
    /// Cheap and lossy on purpose: a send onto a queue, no waiting, and a
    /// dropped batch costs latency rather than correctness — the durable row
    /// committed with the transition, and the sweep still finds it. The wheel
    /// decides what is worth keeping, so everything is offered to it.
    pub fn arm(&self, timeouts: Vec<Scheduled>) {
        // A paused engine is on a clock `debug.tick` drives, and the wheel is
        // on the wall clock. Feeding one to the other would fill it with
        // deadlines that are due at a fictional instant, and `fire` would
        // decline every one of them anyway. Backfill re-reads the world when
        // the engine resumes.
        if timeouts.is_empty() || self.engine.is_paused() {
            return;
        }
        self.timer.merge(
            timeouts
                .into_iter()
                .map(crate::deadlines::scheduled_to_entry)
                .collect(),
        );
    }

    /// Fire deadlines the timer says have come due.
    ///
    /// One `Internal` per deadline, which is the narrow form: the engine acts
    /// on the row that timeout names and nothing else. Firing is a hint, so
    /// each of these may find the deadline has moved or the row has settled and
    /// do nothing — that is `Internal` being idempotent, and it is what lets
    /// this run alongside a sweep that will fire the same deadlines.
    ///
    /// What comes back is treated exactly like a request's output: messages go
    /// to the router, and a deadline a firing armed goes straight back into the
    /// timer, which is how a redispatched task keeps its retry deadline live
    /// without a round trip through the sweep.
    pub async fn fire(&self, timeouts: Vec<Timeout>) {
        if self.engine.is_paused() {
            return;
        }
        let now = util::system_time_ms();
        for timeout in timeouts {
            let out = self.engine.process(Input::Internal(timeout), now).await;
            self.deliver(out.messages).await;
            self.arm(out.timeouts);
        }
    }

    /// Deliver what a transition emitted.
    ///
    /// This is what the message pump used to do on a 100 ms poll, over rows a
    /// transition had left in an outbox. There is no queue between the two any
    /// more: a message goes out as soon as the transaction that produced it has
    /// committed.
    ///
    /// Delivery is best-effort, as it was: a failed route is logged and the
    /// attempt is lost. An execute message comes back — the task stays pending
    /// and its retry timeout re-emits it — and an unblock message does not,
    /// which is the behaviour the outbox had too, since the pump deleted before
    /// it delivered.
    pub async fn deliver(&self, messages: Vec<Outgoing>) {
        if messages.is_empty() {
            return;
        }
        let server_url = self.config.server.url.clone().unwrap_or_default();
        for msg in messages {
            let (address, payload) = match msg {
                Outgoing::Execute {
                    address,
                    task_id,
                    version,
                } => {
                    metrics::MESSAGES_TOTAL
                        .with_label_values(&["execute"])
                        .inc();
                    tracing::info!(kind = "execute", task_id = %task_id, version, address = %address, "Dispatching execute message");
                    (
                        address,
                        Message::Execute(ExecuteMsg {
                            kind: "execute".to_string(),
                            head: MessageHead {
                                server_url: server_url.clone(),
                            },
                            data: ExecuteMsgData {
                                task: ExecuteMsgTask {
                                    id: task_id,
                                    version,
                                },
                            },
                        }),
                    )
                }
                Outgoing::Unblock { address, promise } => {
                    metrics::MESSAGES_TOTAL
                        .with_label_values(&["unblock"])
                        .inc();
                    tracing::info!(kind = "unblock", promise_id = %promise.id, promise_state = %promise.state, address = %address, "Dispatching unblock message");
                    (
                        address,
                        Message::Unblock(UnblockMsg {
                            kind: "unblock".to_string(),
                            head: UnblockMsgHead {},
                            data: UnblockMsgData { promise },
                        }),
                    )
                }
            };
            if let Err(e) = self.router.route(&address, &payload).await {
                tracing::warn!(address = %address, error = %e, "Message not delivered");
            }
        }
    }
}

// === Shared application state ===

#[derive(Clone)]
pub struct AppState {
    pub server: Arc<Server>,
    pub poll_registry: Arc<PollRegistry>,
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
}

impl axum::extract::FromRef<AppState> for PollState {
    fn from_ref(state: &AppState) -> Self {
        PollState {
            server: state.server.clone(),
            poll_registry: state.poll_registry.clone(),
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
    match state.server.engine.ping().await {
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

async fn handle_api(
    State(api_state): State<ApiState>,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, Json<ResponseEnvelope>) {
    let state = &api_state.server;
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
        Some((conn_id, mut rx)) => {
            tracing::info!(
                group = %group,
                id = %id,
                conn_id = conn_id,
                "Poll SSE connection established"
            );
            // The stream ends when the channel closes, and nothing else. A
            // client disconnecting drops this response; the transport stopping
            // clears its registry, which drops the only sender. There is no
            // shutdown signal to keep in step with, because the thing that owns
            // the connection is the thing that ends it.
            let stream = async_stream::stream! {
                let _guard = PollGuard {
                    registry: poll_state.poll_registry.clone(),
                    group: group.clone(),
                    conn_id,
                };
                while let Some(msg) = rx.recv().await {
                    yield Ok::<_, std::convert::Infallible>(Event::default().data(msg));
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
        let Output {
            response,
            messages,
            timeouts,
        } = self
            .engine
            .process(Input::External(req), util::resolve_time(debug_time))
            .await;
        // Deliver after the transition has committed, never before: the engine
        // returns only what its transaction actually wrote.
        self.deliver(messages).await;
        self.arm(timeouts);
        Ok(response.expect("invariant: External input always yields a response"))
    }
}
