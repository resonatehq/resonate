//! The Resonate plugin surface.
//!
//! What a plugin announces, how it is configured, and how a binary assembles
//! one. This is the crate every plugin pins, so it is deliberately small:
//! manifests, three plugin shapes, a registry, and the configuration loader. No
//! transport, no storage, no runtime.
//!
//! A plugin is one of three things, and the kind decides how a binary composes
//! it: a [`ServerPlugin`] answers protocol requests and is *selected*, one per
//! binary; a [`WorkerPlugin`] consumes what the server emits and is
//! *registered*, keyed by the address schemes it claims; a [`GatewayPlugin`]
//! accepts requests from outside and is registered too, switched on or off
//! independently.
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
//!     |settings| {
//!         let config: Config = settings.extract()?;
//!         if !config.enabled {
//!             return Ok(None);
//!         }
//!         if config.brokers.is_empty() {
//!             return Err(settings.reject("brokers", "at least one broker is required"));
//!         }
//!         Ok(Some(Box::new(move |deps: WorkerDependencies| {
//!             Arc::new(KafkaTransport::new(deps.server, config)) as Arc<dyn ResonateWorker>
//!         })))
//!     },
//! );
//! ```
//!
//! Identity is data and the factory is behaviour, so reading a plugin never
//! requires running it. Defaults are the `#[serde(default)]` on the plugin's own
//! `Config` — a section nobody has configured reads as an empty one — so nothing
//! is declared twice and the loader does not need to know the registry. The
//! typed `Config` is captured by the closure and never named outside the
//! plugin's own crate: the framework holds a [`ResonateWorker`] and nothing
//! else.
//!
//! # Startup order
//!
//! The three kinds are built in one order, and it is the whole of what a plugin
//! may assume about when it is called:
//!
//! ```ignore
//! // 1. The router, empty.
//! let router = Arc::new(Dispatcher::new());
//!
//! // 2. The server, handed that router.
//! let connect = (chosen.configure)(&config.server(chosen.id))?;
//! let build = connect().await?;
//! let server = build(ServerDependencies::new(Arc::clone(&router) as _, debug));
//!
//! // 3. The workers, each downgrading the server that now exists.
//! let mut workers = HashMap::new();
//! for plugin in registry.workers() {
//!     let Some(build) = (plugin.configure)(&config.worker(plugin.id))? else {
//!         continue; // turned itself off
//!     };
//!     let worker = build(WorkerDependencies::new(Arc::downgrade(&server)));
//!     for scheme in plugin.schemes {
//!         workers.insert(scheme.to_string(), Arc::clone(&worker));
//!     }
//! }
//!
//! // 4. Install them. The router is complete from here and never changes again.
//! router.install(workers);
//!
//! // 5. The gateways, holding the server strongly. They bind last, because
//! //    accepting a request the rest of the process cannot serve is worse than
//! //    not accepting it.
//! ```
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
//! Nothing can route between steps 2 and 4 — no gateway is listening and no
//! background loop has started.
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
//!         .gateway(&resonate_gateway_http::PLUGIN)
//!         .worker(&resonate_worker_kafka::PLUGIN))
//! }
//! ```

pub mod config;
pub mod error;
pub mod plugin;
pub mod registry;

pub use config::{Configuration, Loader, Settings};
pub use error::{ConfigError, RegistryError, StartupError};
pub use plugin::{
    GatewayDependencies, GatewayFactory, GatewayPlugin, ServerConnect, ServerDependencies,
    ServerFactory, ServerPlugin, WorkerDependencies, WorkerFactory, WorkerPlugin,
};
pub use registry::Registry;

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two.
pub use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
