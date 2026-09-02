//! The web console: a built single-page app, embedded, and the route that
//! answers its requests.
//!
//! # What this crate is
//!
//! A [`ResonateGateway`](resonate_core::ResonateGateway), like every other
//! edge: it binds its own socket, on its own port, and enforces its own auth.
//! It used to hand a set of routes to the HTTP gateway to merge, which meant
//! the composition root had to know that one plugin was routes rather than a
//! gateway, and had to hand them to another. A plugin owns what it owns.
//!
//! What it also owns is the boundary. The console's `ui.*` requests are
//! answered **here and nowhere else**: the worker route refuses the whole
//! namespace (see `resonate_gateway_http::routes`), so the read model the
//! console needs cannot be reached through the API workers use, and can change
//! without touching the protocol they depend on.
//!
//! ```text
//!   GET  /                     → 303 to the console
//!   GET  /console, /console/   → the app shell
//!   GET  /console/<anything>   → an embedded asset, or the shell (SPA fallback)
//!   POST /console/rpc          → one envelope in, one envelope out
//! ```
//!
//! # The assets
//!
//! `assets/` is the SvelteKit build, committed. `cargo build` alone produces
//! the shipping binary — no node on the build machine, no network, nothing to
//! install — and `make console` is what regenerates it after a change to
//! `ui/`. Fonts are vendored for the same reason the console is embedded at
//! all: it has to work on an air-gapped install.
//!
//! # Writes
//!
//! The console reads. What it writes it writes with the worker protocol's own
//! requests — cancel is `promise.settle`, invoke is `promise.create` — so there
//! is no console-only way to change anything, and no second code path to keep
//! honest. Note that this route accepts those requests from anyone the auth
//! policy admits: hiding a button in the app would not change that, which is
//! why the button is not hidden.
//!
//! # Authentication
//!
//! The same check the worker route applies, when auth is configured at all: a
//! console request carries `head.auth` like any other, and is verified by
//! `resonate_auth::auth_check`. There is no login and no session here — the
//! token is typed into Settings and kept in the browser — so a server with
//! auth on serves a console that works only once an operator supplies a token.
//! That is the honest state of it, not a design.

use std::sync::Arc;

// axum comes from `resonate-plugin`, so a build has one of it — see that
// crate's re-export for why.
use resonate_plugin::axum;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use resonate_auth::{auth_check, AuthConfig};
use resonate_core::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_core::{ui, ResonateServer};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};

/// Where the console is mounted. Baked into the built app (SvelteKit's
/// `paths.base`), so it is a constant here rather than a setting.
pub const MOUNT: &str = "/console";

/// The console's own endpoint. `ui.*` is answered here and on no other route.
pub const RPC_PATH: &str = "/console/rpc";

#[derive(Embed)]
#[folder = "assets/"]
struct Assets;

/// Whether the console is served at all.
///
/// Plain data, like every gateway's `Config`, so it deserializes straight out
/// of a config file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Serve the console. On by default: it is compiled in either way, and a
    /// binary that carries a console nobody can reach is a surprise.
    #[serde(default = "yes")]
    pub enabled: bool,

    /// Where to listen [default: 0.0.0.0:8003].
    ///
    /// Its own port, because a plugin owns what it owns. It used to be merged
    /// into the HTTP gateway's router, which meant the composition root had to
    /// know that the console was routes rather than a gateway, and had to hand
    /// one plugin's routes to another.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Who may reach it. Its own, because it enforces it.
    #[serde(default)]
    pub auth: Option<resonate_auth::Config>,

    /// Abort the process when a handler panics, rather than answering 500.
    ///
    /// The same setting the HTTP gateway carries, and for the same reason: this
    /// edge writes. `promise.settle` (cancel) and `promise.create` (invoke)
    /// arrive here as the protocol's own requests, so for a single-process
    /// store a panic mid-transaction can leave in-memory state the next request
    /// would read. A guarantee with one write path inside it and one outside is
    /// not a guarantee.
    #[serde(default)]
    pub abort_on_panic: bool,

    /// Answer `GET /` with a redirect to the console.
    ///
    /// The API's root is `POST /`, so a `GET` there is a person with a
    /// browser. Sending them to the console is the useful answer; turning it
    /// off leaves `GET /` a 405 as before.
    #[serde(default = "yes")]
    pub redirect_root: bool,
}

fn yes() -> bool {
    true
}

fn default_bind() -> String {
    "0.0.0.0:8003".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_bind(),
            auth: None,
            abort_on_panic: false,
            redirect_root: true,
        }
    }
}

/// What the console's handlers can reach: the same server the worker route
/// puts requests to, and the same auth policy.
#[derive(Clone)]
pub struct ConsoleState {
    pub server: Arc<dyn ResonateServer>,
    pub auth: Option<Arc<AuthConfig>>,
}

/// Turn a panic in a handler into a 500, or into an abort.
///
/// The same layer the HTTP gateway applies, because this edge carries the same
/// writes — see [`Config::abort_on_panic`]. Written inline rather than returned
/// from a helper: `CatchPanicLayer::custom` is generic over the responder, and
/// an `impl Trait` return loses the bounds `Router::layer` needs.
macro_rules! panic_guard {
    ($abort:expr) => {{
        let abort = $abort;
        tower_http::catch_panic::CatchPanicLayer::custom(
            move |err: Box<dyn std::any::Any + Send + 'static>| {
                let message = if let Some(s) = err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "internal server error".to_string()
                };
                tracing::error!(message = %message, "panic in a console handler");
                if abort {
                    std::process::abort();
                }
                let body =
                    ResponseEnvelope::error("unknown".to_string(), "0".to_string(), 500, &message);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
            },
        )
    }};
}

/// The console's routes, with its state and its panic guard already applied.
///
/// Its one caller in production is this crate's own [`ResonateGateway::init`];
/// the tests are the others. It stays generic in the host router's state, and
/// public, because that costs nothing and is what makes the routes testable
/// without a socket.
///
/// Returns `None` when the console is disabled, so a caller gets an option
/// rather than having to decide what an empty router means.
pub fn routes<S>(config: &Config, state: ConsoleState) -> Option<Router<S>>
where
    S: Clone + Send + Sync + 'static,
{
    if !config.enabled {
        tracing::info!("Web console disabled");
        return None;
    }
    let mut router = Router::new()
        .route(RPC_PATH, post(handle_rpc))
        .route(MOUNT, get(handle_index))
        .route("/console/", get(handle_index))
        .route("/console/*path", get(handle_asset));
    if config.redirect_root {
        router = router.route("/", get(handle_root));
    }
    tracing::info!(mount = MOUNT, "Web console enabled");
    Some(
        router
            .layer(panic_guard!(config.abort_on_panic))
            .with_state(state),
    )
}

/// A browser at the API's root wants the console.
async fn handle_root() -> Response {
    (
        StatusCode::SEE_OTHER,
        [(header::LOCATION, "/console/")],
        "The Resonate console is at /console/\n",
    )
        .into_response()
}

async fn handle_index() -> Response {
    serve("index.html")
}

/// An asset, or the shell.
///
/// Anything the build did not produce is a client-side route — `/console/executions/
/// checkout.order-8842` is a page — so it gets the shell and the router in the
/// browser resolves it. What must *not* fall back is a request for a file the
/// build should have produced: answering HTML to a request for a script is the
/// failure mode that costs an hour to diagnose.
///
/// So the test is the extension, and only the extensions this handler would
/// have served. A dot in a path means nothing here — promise ids are full of
/// them, and `checkout.order-8842` is a page, not a missing asset.
async fn handle_asset(Path(path): Path<String>) -> Response {
    let trimmed = path.trim_start_matches('/');
    if Assets::get(trimmed).is_some() {
        return serve(trimmed);
    }
    let last = trimmed.rsplit('/').next().unwrap_or("");
    if let Some((_, ext)) = last.rsplit_once('.') {
        if ASSET_EXTENSIONS.contains(&ext) {
            tracing::warn!(path = %trimmed, "Console asset not found");
            return (StatusCode::NOT_FOUND, "Not found\n").into_response();
        }
    }
    serve("index.html")
}

/// The extensions the build emits. A path ending in one of these is a file
/// request, and a missing file is a 404 rather than a page.
const ASSET_EXTENSIONS: &[&str] = &[
    "js", "mjs", "css", "map", "json", "svg", "woff", "woff2", "png", "webp", "ico", "html", "txt",
];

fn serve(path: &str) -> Response {
    let Some(file) = Assets::get(path) else {
        tracing::error!(
            path = %path,
            "Console asset missing from the binary — was `make console` run?"
        );
        return (
            StatusCode::NOT_FOUND,
            "The console was not built into this binary.\n",
        )
            .into_response();
    };
    let mime = content_type(path);
    let mut response = (StatusCode::OK, file.data.into_owned()).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime));
    // Everything under `app/immutable` is content-hashed by the build, so it
    // can be cached forever; the shell names those files and must not be.
    let cache = if path.starts_with("app/immutable/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    response
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "png" => "image/png",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "txt" | "map" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// One envelope in, one envelope out.
///
/// The same translation the worker route performs, and deliberately the same
/// code path underneath: parse and validate at the edge, authorize, put it to
/// the server. The one difference is which kinds arrive — `ui.*` reaches a
/// server only through here — and this route does not restrict itself to them,
/// because the console's single write action is `promise.settle`, the real
/// request, not a console-shaped alias for it.
async fn handle_rpc(
    State(state): State<ConsoleState>,
    body: Bytes,
) -> (StatusCode, Json<ResponseEnvelope>) {
    let req: RequestEnvelope = match types::parse_and_validate(&body) {
        Ok(req) => req,
        Err(invalid) => {
            let (kind, corr_id) = types::salvage_context(&body);
            tracing::warn!(kind = %kind, corr_id = %corr_id, reason = %invalid, "Invalid console request");
            return render(invalid.to_response(kind, corr_id));
        }
    };

    let kind = req.kind.clone();
    let corr_id = req.head.corr_id.clone();

    if let Some(auth) = &state.auth {
        if let Err(rejection) = auth_check(auth, &req) {
            tracing::warn!(kind = %kind, corr_id = %corr_id, "Console request rejected by auth");
            return render(*rejection);
        }
    }

    let response = match state.server.process(&req).await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::error!(kind = %kind, corr_id = %corr_id, error = %e, "Server unavailable");
            ResponseEnvelope::error(kind, corr_id, 503, &e.to_string())
        }
    };
    render(response)
}

fn render(resp: ResponseEnvelope) -> (StatusCode, Json<ResponseEnvelope>) {
    let code =
        StatusCode::from_u16(resp.head.status as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (code, Json(resp))
}

/// The kinds this route exists for, for a caller that wants to say so.
pub fn ui_kinds() -> &'static [&'static str] {
    ui::KINDS
}

// ─── The gateway ─────────────────────────────────────────────────────────────

/// This console, as a plugin. Its own listener, its own port, its own policy.
pub static PLUGIN: resonate_plugin::GatewayPlugin =
    resonate_plugin::GatewayPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[gateways.gateway_web]`, and build it unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::GatewayDependencies,
) -> Result<Option<Arc<dyn resonate_plugin::ResonateGateway>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    Ok(Some(Arc::new(Console {
        config,
        server: deps.server,
        serving: std::sync::Mutex::new(None),
    })))
}

struct Serving {
    task: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

/// The console, serving itself.
struct Console {
    config: Config,
    server: Arc<dyn ResonateServer>,
    serving: std::sync::Mutex<Option<Serving>>,
}

#[async_trait::async_trait]
impl resonate_plugin::ResonateGateway for Console {
    async fn init(&self, _debug: bool) -> Result<(), resonate_plugin::Unavailable> {
        let auth = match &self.config.auth {
            Some(cfg) => Some(Arc::new(cfg.load().map_err(|e| {
                resonate_plugin::Unavailable::new(format!("console auth: {e}"))
            })?)),
            None => None,
        };
        let app: Router<()> = routes(
            &self.config,
            ConsoleState {
                server: Arc::clone(&self.server),
                auth,
            },
        )
        .expect("enabled was checked in configure");

        let listener = tokio::net::TcpListener::bind(&self.config.bind)
            .await
            .map_err(|e| {
                resonate_plugin::Unavailable::new(format!(
                    "console cannot bind {}: {e}",
                    self.config.bind
                ))
            })?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let served = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = rx.await;
            });
            if let Err(e) = served.await {
                tracing::error!(error = %e, "Console listener stopped");
            }
        });
        *self.serving.lock().expect("console serving mutex") = Some(Serving { task, shutdown: tx });
        tracing::info!(bind = %self.config.bind, mount = MOUNT, "Console listening");
        Ok(())
    }

    async fn stop(&self) -> Result<(), resonate_plugin::Unavailable> {
        // Out of the guard before the await: a std MutexGuard is not Send.
        let serving = self.serving.lock().expect("console serving mutex").take();
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

    /// The build output has to actually be in the binary. A console that
    /// compiles and serves a 404 is the failure this catches.
    #[test]
    fn the_built_console_is_embedded() {
        let index = Assets::get("index.html").expect("index.html is embedded");
        let html = std::str::from_utf8(&index.data).expect("utf-8");
        assert!(
            html.contains("<title>Resonate Console</title>"),
            "{html:.400}"
        );
        // SvelteKit's base path is baked in at build time; if it drifts from
        // MOUNT every asset link 404s.
        assert!(
            html.contains("/console/app/immutable/"),
            "assets are mounted at {MOUNT}"
        );

        assert!(
            Assets::get("fonts/inter-latin.woff2").is_some(),
            "Inter is self-hosted so the console works air-gapped"
        );
        assert!(Assets::get("favicon.svg").is_some());
        assert!(
            Assets::iter().any(|p| p.starts_with("app/immutable/") && p.ends_with(".js")),
            "the app's own scripts are embedded"
        );
    }

    #[test]
    fn assets_are_served_with_the_type_and_cache_a_browser_needs() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("app/immutable/nodes/0.abc.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("fonts/inter-latin.woff2"), "font/woff2");
        assert_eq!(content_type("favicon.svg"), "image/svg+xml");
        assert_eq!(content_type("noextension"), "application/octet-stream");
    }
}
