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
//!     "kafka",
//!     env!("CARGO_PKG_NAME"),
//!     &["kafka"],
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
//! let router = Arc::new(Dispatcher::new());
//!
//! // 2. The server, handed that router. Nothing is connected yet.
//! let deps = ServerDependencies::new(Arc::clone(&router) as _);
//! let server = (chosen.configure)(&config.server(chosen.id), deps)?;
//!
//! // 3. The workers, each downgrading the server that now exists.
//! //    Two collections, because they answer different questions: `workers` is
//! //    what was built, `routes` is how to reach it. One worker can claim
//! //    several schemes, so it appears once in the first and several times in
//! //    the second.
//! let mut workers = Vec::new();
//! let mut routes = HashMap::new();
//! for plugin in registry.workers() {
//!     let deps = WorkerDependencies::new(Arc::downgrade(&server));
//!     let Some(worker) = (plugin.configure)(&config.worker(plugin.id), deps)? else {
//!         continue; // turned itself off
//!     };
//!     for scheme in plugin.schemes {
//!         routes.insert(scheme.to_string(), Arc::clone(&worker));
//!     }
//!     workers.push(worker);
//! }
//!
//! // 4. Install them. The router is complete from here and never changes again.
//! router.install(routes);
//!
//! // 5. The gateways, holding the server strongly. Nothing is bound yet.
//! let gateways = /* (plugin.configure)(&config.gateway(plugin.id), deps)? */;
//!
//! // 6. Start, in the order things were built.
//! router.init(debug).await?;
//! server.init(debug).await?;
//! for worker in &workers   { worker.init(debug).await?; }
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
//! Each thing is started by whoever built it, which is this loop. A router does
//! not drive its workers' lifecycle just because it holds them — and the routing
//! table is the wrong thing to iterate for it, because a worker claiming two
//! schemes is in there twice and would be started twice.
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
//! worker's `stop` releases, so stopping the gateway first would deadlock. The
//! workers stop, then the server, then the router, and the gateway last — which
//! also means a client gets a 503 rather than a closed socket while in-flight
//! work drains.
//!
//! # Assembling a binary
//!
//! The composition root is the *user's* crate, not the server's. Cargo is the
//! plugin manager: name the plugins you want as dependencies, name them again
//! in [`Registry`], and only those are resolved, downloaded or compiled.
//!
//! ```ignore
//! fn main() -> ExitCode {
//!     resonate::run(Registry::new()
//!         .server(&resonate_server_dbms::SQLITE)
//!         .worker(&resonate_worker_kafka::PLUGIN)
//!         .gateway(&resonate_gateway_http::PLUGIN))
//! }
//! ```

pub mod config;
pub mod error;
pub mod plugin;
pub mod registry;

pub use registry::Registry;

// Server, worker, gateway: the order a binary builds them in, and the order
// everything in this crate is written in. Skipped, because rustfmt would sort
// these alphabetically and put the gateway first.
#[rustfmt::skip]
pub use plugin::{
    ServerDependencies, ServerPlugin,
    WorkerDependencies, WorkerPlugin,
    GatewayDependencies, GatewayPlugin,
};

pub use config::{Configuration, Loader, Settings};
pub use error::{ConfigError, RegistryError};

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two.
#[rustfmt::skip]
pub use resonate_core::{ResonateServer, ResonateWorker, ResonateGateway, ResonateRouter};
