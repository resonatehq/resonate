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

    // No `bind`, no `auth` and no `abort_on_panic`. The console serves routes,
    // not a socket: the gateway that owns the listener owns the address, the
    // policy that admits a request, and the panic guard over the handlers.
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

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
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

/// The console's routes, with its state already applied.
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
    let mut router = axum::Router::new()
        .route(RPC_PATH, post(handle_rpc))
        .route(MOUNT, get(handle_index))
        .route("/console/", get(handle_index))
        .route("/console/*path", get(handle_asset));
    if config.redirect_root {
        router = router.route("/", get(handle_root));
    }
    tracing::info!(mount = MOUNT, "Web console enabled");
    Some(router.with_state(state))
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

// ─── The plugin ───────────────────────────────────────────────────────────────

/// The console, as a plugin.
///
/// A [`GatewayPlugin`](resonate_plugin::GatewayPlugin) because it is an edge —
/// requests arrive from outside — but not one that listens. It registers its
/// routes and the HTTP gateway serves them, so the console is on the same port
/// and the same origin as the protocol it reads. One port, one process, one
/// origin: no CORS to configure, no second address to publish, and a browser
/// that reaches the server reaches the console.
pub static PLUGIN: resonate_plugin::GatewayPlugin =
    resonate_plugin::GatewayPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[gateways.gateway_web]`, and register the console unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::GatewayDependencies,
) -> Result<Option<Arc<dyn resonate_plugin::ResonateGateway>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        tracing::info!("Web console disabled");
        return Ok(None);
    }

    let server = Arc::clone(&deps.server);
    deps.routes.add(PLUGIN.id(), move |auth| {
        // `expect`: `enabled` was checked three lines up, and nothing can have
        // changed it — the config is owned by this closure.
        routes(&config, ConsoleState { server, auth }).expect("the console is enabled")
    });

    // `Some`, though there is nothing to start. The composition root reads
    // `None` as "this plugin turned itself off" and says so in the log, which
    // would be a lie about a console that is serving. What comes back is the
    // console as a thing that exists, with the trait's own do-nothing `init`
    // and `stop`: it has no socket and no task, so there is nothing to drive.
    Ok(Some(Arc::new(Console)))
}

/// The console, as something the composition root can hold.
///
/// No `init` and no `stop` — the defaults do nothing, which is the whole
/// truth about a plugin whose routes are served by another plugin's listener.
struct Console;

impl resonate_plugin::ResonateGateway for Console {}

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
