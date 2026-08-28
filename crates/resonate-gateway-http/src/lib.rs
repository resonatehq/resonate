//! The HTTP gateway: axum in front of a [`ResonateServer`].
//!
//! An adapter, in the same sense as the transports: it speaks one wire format
//! and knows nothing about promises, tasks or schedules. Requests arrive as
//! bytes, become envelopes, and go to a `dyn ResonateServer`; what comes back
//! becomes a status and a body.
//!
//! `routes` holds that translation. This file holds the socket and the task
//! that serves it — the part with a lifecycle to coordinate, which is what
//! [`ResonateGateway`] exists to name.

pub mod routes;

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN},
    HeaderValue, Method, StatusCode,
};
use axum::response::IntoResponse;
use axum::Json;
use resonate_auth::AuthConfig;
use resonate_core::types::ResponseEnvelope;
use resonate_core::{ResonateGateway, ResonateServer, Unavailable};
use resonate_transport_http_poll::PollRegistry;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

pub use routes::{AppState, PollState};

/// Where to listen, and how to behave once we do.
#[derive(Debug, Clone)]
pub struct HttpGatewayConfig {
    pub bind: String,
    pub port: u16,
    /// The URL clients reach this server on, for logging only.
    pub url: String,
    /// Origins to allow. Empty disables CORS; `*` is permissive.
    pub cors_allow_origins: Vec<String>,
    /// Abort the process when a handler panics, rather than answering 500.
    ///
    /// For a single-process store — SQLite — a panic mid-transaction can leave
    /// in-memory state the next request would read. Aborting is the safer
    /// failure. A server whose state is in a database elsewhere answers 500 and
    /// carries on.
    pub abort_on_panic: bool,
}

/// axum, hosting the Resonate protocol over HTTP.
pub struct HttpGateway {
    config: HttpGatewayConfig,
    /// Held so the server outlives every request it can still accept. Strong,
    /// unlike a worker's handle: nothing points back at a gateway, so there is
    /// no cycle to break. Unused directly — the routes carry their own handle —
    /// but the ownership is the point.
    #[allow(dead_code)]
    server: Arc<dyn ResonateServer>,
    /// The application, built in `new` and taken by `init` — which is what
    /// makes `init` the only place a socket is bound.
    app: Mutex<Option<axum::Router>>,
    /// Set by `stop` to release the graceful-shutdown future.
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl HttpGateway {
    /// Build the gateway. Nothing is bound and nothing runs until `init`.
    ///
    /// `auth` is `None` when authentication is disabled. `poll_registry` is the
    /// poll transport, which needs an endpoint to hand its connections out
    /// through — see [`routes::AppState`] for why it arrives here rather than
    /// being something this crate owns.
    pub fn new(
        server: Arc<dyn ResonateServer>,
        auth: Option<Arc<AuthConfig>>,
        poll_registry: Arc<PollRegistry>,
        config: HttpGatewayConfig,
    ) -> Self {
        let state = routes::AppState {
            server: Arc::clone(&server),
            auth,
            poll_registry,
        };
        let app = build_app(state, &config);
        Self {
            config,
            server,
            app: Mutex::new(Some(app)),
            shutdown: Mutex::new(None),
            task: Mutex::new(None),
        }
    }
}

/// Routes, state and layers, in the order a request meets them.
fn build_app(state: routes::AppState, config: &HttpGatewayConfig) -> axum::Router {
    let abort_on_panic = config.abort_on_panic;
    let mut app = routes::api_routes()
        .merge(routes::poll_routes())
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(
            move |err: Box<dyn std::any::Any + Send + 'static>| {
                let message = if let Some(s) = err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "internal server error".to_string()
                };
                tracing::error!(message = %message, "panic in request handler");
                if abort_on_panic {
                    std::process::abort();
                }
                let body =
                    ResponseEnvelope::error("unknown".to_string(), "0".to_string(), 500, &message);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
            },
        ))
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .make_span_with(
                    tower_http::trace::DefaultMakeSpan::new().level(tracing::Level::INFO),
                )
                .on_response(
                    tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO),
                )
                .on_failure(
                    tower_http::trace::DefaultOnFailure::new().level(tracing::Level::ERROR),
                ),
        )
        .with_state(state);
    if let Some(layer) = cors_layer(&config.cors_allow_origins) {
        app = app.layer(layer);
    }
    app
}

fn cors_layer(allow_origins: &[String]) -> Option<tower_http::cors::CorsLayer> {
    if allow_origins.is_empty() {
        return None;
    }
    let layer = if allow_origins.iter().any(|o| o == "*") {
        tower_http::cors::CorsLayer::permissive()
    } else {
        let origins: Vec<HeaderValue> = allow_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        tower_http::cors::CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers([ORIGIN, CONTENT_LENGTH, CONTENT_TYPE, AUTHORIZATION])
    };
    Some(layer)
}

#[async_trait]
impl ResonateGateway for HttpGateway {
    async fn init(&self) -> Result<(), Unavailable> {
        let Some(app) = self.app.lock().await.take() else {
            return Ok(()); // already started
        };

        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| Unavailable::new(format!("failed to bind {addr}: {e}")))?;

        let (tx, rx) = oneshot::channel();
        *self.shutdown.lock().await = Some(tx);

        tracing::info!(
            bind = %self.config.bind,
            port = self.config.port,
            server_url = %self.config.url,
            "Server listening"
        );

        let handle = tokio::spawn(async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    // An error means `stop` dropped the sender rather than
                    // sending, which is still a request to stop.
                    let _ = rx.await;
                })
                .await;
            if let Err(e) = served {
                tracing::error!(error = %e, "HTTP gateway stopped with an error");
            }
        });
        *self.task.lock().await = Some(handle);
        Ok(())
    }

    /// Close the listener and wait for what is in flight.
    ///
    /// Called last, after every worker has stopped — which is what makes the
    /// wait finite. A poll transport's SSE streams are long-lived responses
    /// axum's graceful shutdown would otherwise wait on forever; by now that
    /// transport has dropped its senders and every one of those streams has
    /// ended on its own.
    async fn stop(&self) -> Result<(), Unavailable> {
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
        let handle = self.task.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }
}
