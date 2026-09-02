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
//!         Ok(Some(Box::new(move |ctx: &WorkerCtx| {
//!             Arc::new(KafkaTransport::new(ctx.server.clone(), config)) as Arc<dyn ResonateWorker>
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
//! The three kinds have to be built in one order, and it is the reason a worker
//! holds its server weakly:
//!
//! ```text
//! 1. the router, empty
//! 2. the server        — handed the router
//! 3. the workers       — handed a Weak to the server, which now exists
//! 4. install the workers into the router
//! 5. the gateways      — handed a strong Arc to the server; they bind last
//! ```
//!
//! Step 4 is what closes it. The router is the only thing that starts
//! incomplete, so nothing needs a handle to a value that does not exist yet, and
//! no plugin is constructed before what it is handed. The `Weak` at step 3 is
//! not about ordering — the server is right there — it is that the server holds
//! the router and the router holds the workers, so a strong handle back would be
//! a cycle nothing in could ever be dropped from.
//!
//! Nothing can route between steps 2 and 4: no gateway is listening and no
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

pub use config::{Loaded, Loader, Settings};
pub use error::{ConfigError, RegistryError, StartupError};
pub use plugin::{
    GatewayCtx, GatewayFactory, GatewayPlugin, ServerConnect, ServerCtx, ServerFactory,
    ServerPlugin, WorkerCtx, WorkerFactory, WorkerPlugin,
};
pub use registry::Registry;

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two.
pub use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
