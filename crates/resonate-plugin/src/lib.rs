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
//!     Manifest::new("kafka", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
//!         .with_summary("Deliver by producing to a Kafka topic")
//!         .with_schemes(&["kafka"]),
//!     || serde_json::to_value(Config::default()).unwrap(),
//!     |settings| {
//!         let config: Config = settings.extract()?;
//!         if config.brokers.is_empty() {
//!             return Err(settings.reject("brokers", "at least one broker is required"));
//!         }
//!         Ok(Box::new(move |ctx: &WorkerCtx| {
//!             Arc::new(KafkaTransport::new(ctx.server.clone(), config)) as Arc<dyn ResonateWorker>
//!         }))
//!     },
//! );
//! ```
//!
//! The [`Manifest`] is data and the factory is behaviour, split so that reading
//! a plugin never requires running it. The typed `Config` is captured by the
//! closure and never named outside the plugin's own crate — the framework holds
//! a [`ResonateWorker`] and nothing else.
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
pub mod handle;
pub mod manifest;
pub mod plugin;
pub mod registry;

pub use config::{Loaded, Loader, Settings, ENABLED};
pub use error::{ConfigError, RegistryError, StartupError};
pub use handle::ServerHandle;
pub use manifest::{Kind, Manifest};
pub use plugin::{
    GatewayCtx, GatewayFactory, GatewayPlugin, ServerConnect, ServerCtx, ServerFactory,
    ServerPlugin, WorkerCtx, WorkerFactory, WorkerPlugin,
};
pub use registry::Registry;

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two.
pub use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
