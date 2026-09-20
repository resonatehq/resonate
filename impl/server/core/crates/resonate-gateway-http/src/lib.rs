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

// axum comes from `resonate-plugin`, so a build has one of it — see that
// crate's re-export for why.
use resonate_plugin::axum;

use async_trait::async_trait;
use axum::http::{
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN},
    HeaderValue, Method, StatusCode,
};
use axum::response::IntoResponse;
use axum::Json;
use resonate_core::types::ResponseEnvelope;
use resonate_core::{ResonateGateway, ResonateServer, Unavailable};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use routes::AppState;

/// This gateway, as a plugin. The worker-facing HTTP edge.
pub static PLUGIN: resonate_plugin::GatewayPlugin =
    resonate_plugin::GatewayPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[gateways.gateway_http]`, and build the gateway unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::GatewayDependencies,
) -> Result<Option<Arc<dyn ResonateGateway>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    Ok(Some(Arc::new(HttpGateway::new(
        deps.server,
        deps.routes,
        config,
    ))))
}

/// Where to listen, and how to behave once we do.
///
/// Plain data, like every transport's `Config`, so it deserializes straight
/// out of a config file. Nothing here is read from disk or opened; that is
/// [`HttpGateway::init`]'s.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Serve at all.
    #[serde(default = "yes")]
    pub enabled: bool,

    /// Where to listen [default: 0.0.0.0:8001].
    ///
    /// One string, like every other listening plugin's. It used to be a host
    /// and a port in two fields, which made this the one edge configured
    /// differently from the other three for no reason anybody could name.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Origins to allow. Empty disables CORS; `*` is permissive.
    #[serde(default)]
    pub cors_allow_origins: Vec<String>,

    /// Authentication. Absent means every request is accepted.
    ///
    /// The key it names is read by [`HttpGateway::init`] — a bad path is a
    /// startup failure, not a request that later cannot be authenticated.
    #[serde(default)]
    pub auth: Option<resonate_auth::Config>,

    /// WorkOS authentication. Absent means WorkOS auth is off.
    ///
    /// Mutually exclusive with `auth`: a gateway verifies tokens one way, and
    /// naming both is a startup error rather than a silent precedence.
    #[serde(default)]
    pub workos: Option<resonate_auth::workos::Config>,

    /// Abort the process when a handler panics, rather than answering 500.
    ///
    /// For a single-process store — SQLite — a panic mid-transaction can leave
    /// in-memory state the next request would read. Aborting is the safer
    /// failure. A server whose state is in a database elsewhere answers 500 and
    /// carries on.
    #[serde(default)]
    pub abort_on_panic: bool,
}

fn yes() -> bool {
    true
}

fn default_bind() -> String {
    "0.0.0.0:8001".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_bind(),
            cors_allow_origins: Vec::new(),
            auth: None,
            workos: None,
            abort_on_panic: false,
        }
    }
}

/// The listener, once `init` has bound it, and the switch that ends it.
struct Serving {
    task: JoinHandle<()>,
    shutdown: oneshot::Sender<()>,
}

/// axum, hosting the Resonate protocol over HTTP.
pub struct HttpGateway {
    config: Config,
    /// Held so the server outlives every request it can still accept, and
    /// handed to the routes when `init` builds them. Strong, unlike a worker's
    /// handle: nothing points back at a gateway, so there is no cycle to break.
    server: Arc<dyn ResonateServer>,
    /// What every other plugin asked to have served here.
    ///
    /// This gateway owns the only listener, so it owns everyone's HTTP: the
    /// poll transport's SSE endpoint, the console, and whatever a worker
    /// registers for a callback. They are merged in `init` — see there for why
    /// it cannot be earlier.
    routes: Arc<resonate_plugin::Routes>,
    serving: std::sync::Mutex<Option<Serving>>,
}

impl HttpGateway {
    /// Build the gateway. Nothing is bound and nothing runs until `init`.
    pub fn new(
        server: Arc<dyn ResonateServer>,
        routes: Arc<resonate_plugin::Routes>,
        config: Config,
    ) -> Self {
        Self {
            config,
            server,
            routes,
            serving: std::sync::Mutex::new(None),
        }
    }
}

/// Routes, state and layers, in the order a request meets them.
///
/// `extra` is what every other plugin registered. Merged before the layers, so
/// a registered route gets the same panic guard, the same tracing and the same
/// CORS as the protocol's own — a route served here is served on the same terms
/// as everything else, not through a hole beside them.
fn build_app(
    state: routes::AppState,
    config: &Config,
    extra: Vec<(String, axum::Router)>,
) -> axum::Router {
    let abort_on_panic = config.abort_on_panic;
    let mut merged = routes::api_routes().with_state(state);
    for (plugin, router) in extra {
        tracing::info!(plugin = %plugin, "Serving routes for plugin");
        // Panics on a path collision, naming neither side — which is why the
        // line above names the plugin whose routes are going in.
        merged = merged.merge(router);
    }
    let mut app = merged
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
        );
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

/// Which policy this gateway will verify tokens against.
///
/// Separate from `init` because it is the whole of the decision and none of
/// the I/O it triggers: a test can ask what a config means without a socket.
/// Reading the key material still happens here, so a bad path is a startup
/// failure like the rest.
fn auth_mode(config: &Config) -> Result<Option<resonate_auth::AuthMode>, String> {
    match (&config.auth, &config.workos) {
        (Some(_), Some(_)) => {
            Err("auth and workos cannot both be configured — choose exactly one mode".to_string())
        }
        (Some(cfg), None) => Ok(Some(resonate_auth::AuthMode::Jwt(Arc::new(cfg.load()?)))),
        (None, Some(cfg)) => Ok(Some(resonate_auth::AuthMode::WorkOs(
            resonate_auth::workos::WorkOsClient::new(cfg.load()?),
        ))),
        (None, None) => {
            tracing::info!("Auth disabled — all requests accepted");
            Ok(None)
        }
    }
}

#[async_trait]
impl ResonateGateway for HttpGateway {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        // Reading the key material is this crate's, not its caller's: it
        // touches the disk and it can fail, which is what `init` is for. A bad
        // key path stops the process here rather than surfacing later as a
        // request nobody can authenticate.
        let auth = auth_mode(&self.config).map_err(Unavailable::new)?;
        // Every other plugin's routes, built now that the auth policy exists.
        // This is why registration is a builder and not a router: a route that
        // must authenticate the same way as the protocol cannot be built before
        // the key has been read, and reading it is what `init` is for.
        //
        // It is also why this happens here rather than in `configure`: by the
        // time any gateway starts, every plugin has been configured, so nothing
        // can register after this drains the list.
        let extra: Vec<(String, axum::Router)> = self
            .routes
            .take()
            .into_iter()
            .map(|(plugin, build)| (plugin, build(auth.clone())))
            .collect();

        let app = build_app(
            routes::AppState {
                server: Arc::clone(&self.server),
                auth,
            },
            &self.config,
            extra,
        );

        let listener = tokio::net::TcpListener::bind(&self.config.bind)
            .await
            .map_err(|e| {
                Unavailable::new(format!(
                    "http gateway cannot bind {}: {e}",
                    self.config.bind
                ))
            })?;

        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(async move {
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
        *self.serving.lock().expect("http gateway serving mutex") =
            Some(Serving { task, shutdown: tx });
        tracing::info!(bind = %self.config.bind, "Server listening");
        Ok(())
    }

    /// Close the listener and wait for what is in flight.
    ///
    /// Called last, after the server and the workers have stopped, so a client
    /// gets a 503 rather than a closed socket while in-flight work drains.
    /// Every response this gateway serves is a request/response pair, so the
    /// wait is bounded by the slowest one.
    async fn stop(&self) -> Result<(), Unavailable> {
        // Out of the guard before the await: a std MutexGuard is not Send.
        let serving = self
            .serving
            .lock()
            .expect("http gateway serving mutex")
            .take();
        if let Some(serving) = serving {
            let _ = serving.shutdown.send(());
            let _ = serving.task.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workos(api_key: Option<&str>, org_id: Option<&str>) -> resonate_auth::workos::Config {
        resonate_auth::workos::Config {
            api_key: api_key.map(str::to_owned),
            org_id: org_id.map(str::to_owned),
            base_url: "https://api.workos.com".to_string(),
        }
    }

    #[test]
    fn no_section_means_no_policy() {
        assert!(auth_mode(&Config::default()).unwrap().is_none());
    }

    /// One edge verifies tokens one way. Two policies would make the answer
    /// depend on an order nobody wrote down, so startup refuses instead.
    #[test]
    fn naming_both_modes_is_refused() {
        let config = Config {
            auth: Some(resonate_auth::Config {
                publickey: "none".into(),
                iss: None,
                aud: None,
            }),
            workos: Some(workos(Some("sk_test"), Some("org_abc"))),
            ..Default::default()
        };
        let Err(err) = auth_mode(&config) else {
            panic!("two policies is not a policy");
        };
        assert!(err.contains("cannot both be configured"), "{err}");
    }

    #[test]
    fn a_jwt_section_selects_local_verification() {
        let config = Config {
            auth: Some(resonate_auth::Config {
                publickey: "none".into(),
                iss: None,
                aud: None,
            }),
            ..Default::default()
        };
        assert!(matches!(
            auth_mode(&config).unwrap(),
            Some(resonate_auth::AuthMode::Jwt(_))
        ));
    }

    #[test]
    fn a_workos_section_selects_remote_validation() {
        let config = Config {
            workos: Some(workos(Some("sk_test"), Some("org_abc"))),
            ..Default::default()
        };
        assert!(matches!(
            auth_mode(&config).unwrap(),
            Some(resonate_auth::AuthMode::WorkOs(_))
        ));
    }

    /// A WorkOS section is a request for WorkOS auth, so an incomplete one is
    /// a startup failure rather than a policy that admits everything.
    #[test]
    fn an_incomplete_workos_section_fails_startup() {
        for (api_key, org_id, missing) in [
            (None, Some("org_abc"), "api_key"),
            (Some("sk_test"), None, "org_id"),
        ] {
            let config = Config {
                workos: Some(workos(api_key, org_id)),
                ..Default::default()
            };
            let Err(err) = auth_mode(&config) else {
                panic!("an incomplete WorkOS section must not start");
            };
            assert!(err.contains(missing), "{err}");
        }
    }
}
