//! The Resonate plugin surface.
//!
//! What a plugin announces, how it is configured, and how a binary assembles
//! one. This is the crate every plugin pins, so it is deliberately small:
//! manifests, three plugin shapes, a registry, and the configuration loader. No
//! transport, no storage, no runtime.
//!
//! A plugin is one of three things, and the kind decides how a binary composes
//! it. A [`ServerPlugin`] answers protocol requests and is *selected*, one per
//! binary. A [`WorkerPlugin`] consumes what the server emits and is
//! *registered*, keyed by the address schemes it claims. A [`GatewayPlugin`]
//! accepts requests from outside and is registered too, switched on or off
//! independently.
//!
//! Server, worker, gateway — the order they are built in, and the order
//! everything in this crate is written in.
//!
//! # Writing one
//!
//! One `static`, and nothing else is required:
//!
//! ```ignore
//! pub static PLUGIN: WorkerPlugin = WorkerPlugin::new(
//!     env!("CARGO_PKG_NAME"),
//!     &["kafka"],   // resonate-worker-kafka  →  [workers.worker_kafka]
//!     |settings, deps| {
//!         let config: Config = settings.extract()?;
//!         if !config.enabled {
//!             return Ok(None);
//!         }
//!         if config.brokers.is_empty() {
//!             return Err(settings.reject("brokers", "at least one broker is required"));
//!         }
//!         Ok(Some(Arc::new(KafkaTransport::new(deps.server, config))))
//!     },
//! );
//! ```
//!
//! Identity is data and building is behaviour, so reading a plugin never
//! requires running it. `configure` is sync and side-effect-free — a pool, a
//! schema, a session, a bound port all belong in the thing's own `init`, which
//! every port has and which the composition root awaits. Defaults are the `#[serde(default)]` on the plugin's own
//! `Config` — a section nobody has configured reads as an empty one — so nothing
//! is declared twice and the loader does not need to know the registry. The
//! typed `Config` is captured by the closure and never named outside the
//! plugin's own crate: the framework holds a [`ResonateWorker`] and nothing
//! else.
//!
//! # Startup order
//!
//! Construction is sync and cheap, and hands each plugin the one thing it talks
//! to; `init` is where anything that can fail or block happens, and where the
//! debug flag arrives. Every port has the same pair, so the sequence is: build
//! everything, then start it, in that order.
//!
//! ```ignore
//! // 1. The router, empty.
//! let router = Arc::new(Router::new());   // the message router, not axum's
//!
//! // 2. The server, handed that router. Nothing is connected yet.
//! let deps = ServerDependencies::new(Arc::clone(&router) as _);
//! let server = (chosen.configure)(&config.server(&chosen.id()), deps)?;
//!
//! // 3. The workers, each downgrading the server that now exists.
//! let mut routes = HashMap::new();
//! for plugin in registry.workers() {
//!     let deps = WorkerDependencies::new(Arc::downgrade(&server));
//!     let Some(worker) = (plugin.configure)(&config.worker(&plugin.id()), deps)? else {
//!         continue; // turned itself off
//!     };
//!     for scheme in plugin.schemes {
//!         routes.insert(scheme.to_string(), Arc::clone(&worker));
//!     }
//! }
//!
//! // 4. Install them. The router is complete from here and never changes again.
//! router.install(routes);
//!
//! // 5. The gateways, holding the server strongly. Nothing is bound yet.
//! let gateways = /* (plugin.configure)(&config.gateway(&plugin.id()), deps)? */;
//!
//! // 6. Start, in the order things were built.
//! router.init(debug).await?;   // and every worker it holds
//! server.init(debug).await?;
//! for gateway in &gateways { gateway.init(debug).await?; }
//! ```
//!
//! Init order is construction order — one rule, no special case to remember —
//! and it is also the order that works: a worker that upgrades its handle finds
//! a server that can already answer, and a gateway binds only once everything
//! behind it can serve what it accepts. Accepting a request the rest of the
//! process cannot serve is worse than not accepting it, so the gateways are
//! last.
//!
//! The workers are started by the router, because it is the only thing that
//! knows a worker's scheme — which is what a startup failure has to name. It
//! iterates its own table, and one worker claiming two schemes gets one `init`
//! per scheme, which is what a worker registered twice has always got.
//!
//! Step 4 is what closes the cycle — server holds router, router holds workers,
//! workers hold server — and the router is the only participant that starts
//! incomplete. So nothing is ever handed a value that does not exist yet, and no
//! plugin is constructed before the thing it is given.
//!
//! The `Weak` at step 3 is not about ordering; the server is right there. It is
//! that a strong handle back would close that cycle in `Arc`s, and nothing in it
//! would ever be dropped. Upgrade per message: a failed upgrade means the server
//! is gone and there is no work worth doing.
//!
//! Shutdown is not the mirror image, and the exception is load-bearing: a
//! gateway's graceful drain can be waiting on a long-lived response that only a
//! worker's `stop` releases, so stopping the gateways first would deadlock. The
//! server stops first — its timer is the only thing that can still hand it work
//! of its own — then the workers, through the router that holds them, and the
//! gateways last. Which also means a client gets a 503 rather than a closed
//! socket while in-flight work drains.
//!
//! # Assembling a binary
//!
//! The composition root is the *user's* crate, not the server's. Cargo is the
//! plugin manager: name the plugins you want as dependencies, name them again
//! in [`Registry`], and only those are resolved, downloaded or compiled.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> std::process::ExitCode {
//!     resonate_base::main(
//!         Registry::new()
//!             .server(&resonate_server_postgres::PLUGIN)
//!             .worker(&acme_worker_kafka::PLUGIN)
//!             .gateway(&resonate_gateway_http::PLUGIN),
//!         Options::default().default_server("server_postgres"),
//!     )
//!     .await
//! }
//! ```

pub mod config;
pub mod error;
pub mod plugin;
pub mod registry;
pub mod routes;

pub use registry::Registry;

// Server, worker, gateway: the order a binary builds them in, and the order
// everything in this crate is written in. Skipped, because rustfmt would sort
// these alphabetically and put the gateway first.
pub use plugin::id_from_crate;

#[rustfmt::skip]
pub use plugin::{
    ServerDependencies, ServerPlugin,
    WorkerDependencies, WorkerPlugin,
    GatewayDependencies, GatewayPlugin,
};

pub use config::{Configuration, Loader, Settings};
pub use error::{ConfigError, RegistryError};
pub use routes::{RouteBuilder, Routes};

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two.
#[rustfmt::skip]
pub use resonate_core::{ResonateServer, ResonateWorker, ResonateGateway, ResonateRouter};

// And what implementing one of them requires: the wire types a plugin handles,
// and the error it reports. Without these a plugin crate would still have to
// name resonate-core, and the pair would have to version together anyway.
pub use resonate_core::{types, Unavailable};

/// HTTP, for any plugin that serves it.
///
/// Re-exported for the same reason as `prometheus` below: so a build has one
/// version of it by construction. A plugin that listens — a gateway, or a
/// worker with a callback route — owns its socket and builds its own
/// `axum::Router`, and two semver-major axums in the graph would make those
/// routers different types that no shared helper could take.
///
/// It also means a plugin crate names one dependency. `resonate-plugin` is
/// already the crate every plugin pins; the HTTP it serves comes with it.
///
/// ```ignore
/// use resonate_plugin::axum;   // in lib.rs; `use crate::axum;` in a submodule
///
/// async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
///     let app = axum::Router::new().route("/callback/:id", axum::routing::post(handle));
///     let listener = tokio::net::TcpListener::bind(&self.config.bind)
///         .await
///         .map_err(|e| Unavailable::new(format!("cannot bind {}: {e}", self.config.bind)))?;
///     tokio::spawn(async move { axum::serve(listener, app).await });
///     Ok(())
/// }
/// ```
pub use axum;

/// Metrics, for any plugin that wants them.
///
/// Re-exported rather than depended on directly, and that is the whole point:
/// `register_*!` writes into prometheus' process-wide default registry and the
/// `/metrics` gateway reads the same one, so two semver-major versions of
/// prometheus in the graph would mean two registries and half the counters
/// silently missing — no error, no log line, just absent series. Going through
/// this re-export means there is one version by construction.
///
/// A plugin declares whatever it wants; nothing central lists them. Prefix the
/// name with the plugin id so two plugins cannot collide, and touch each
/// counter in `init` if it should be present at zero rather than appearing on
/// first use.
///
/// ```ignore
/// lazy_static! {
///     static ref LWT_RETRIES: Counter = resonate_plugin::prometheus::register_counter!(
///         "resonate_scylladb_lwt_retries_total",
///         "Conditional writes retried after a failed compare-and-set"
///     ).unwrap();
/// }
/// ```
pub use prometheus;
