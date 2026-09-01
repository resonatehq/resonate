//! The Resonate plugin surface.
//!
//! What a plugin announces, how it is configured, and how a binary assembles
//! one. This is the crate four hundred plugins pin, so it is deliberately
//! small and deliberately boring: manifests, four plugin shapes, a registry,
//! and the configuration loader. No transport, no storage, no runtime.
//!
//! # The ring
//!
//! Resonate is four ports in one cycle:
//!
//! ```text
//!    N                1              1              N
//! ┌─────────┐    ┌────────┐    ┌────────┐    ┌─────────┐
//! │ Gateway │───▶│ Server │───▶│ Router │───▶│ Worker  │
//! └─────────┘    └────────┘    └────────┘    └─────────┘
//!                     ▲                            │
//!                     └────────────────────────────┘
//!                          Weak — the one back-edge
//! ```
//!
//! [`ResonateServer`](resonate_core::ResonateServer) appears twice because it
//! is the only port that is both an entry and a return. Everything else
//! follows from that, which is why a plugin declares only which port it *is*:
//!
//! - what it is handed, and whether that handle is strong or weak
//!   ([`Port::consumes`], [`Port::holds_weakly`]);
//! - whether the binary has one of it or many ([`Port::is_singleton`]) — which
//!   is why a server is *selected* by name and workers are *registered* by
//!   scheme;
//! - where its configuration lives ([`Manifest::config_key`]).
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
//! The [`Manifest`] is pure data and the factory is behaviour, split so that
//! reading a plugin never requires running it: `resonate plugins` prints a row
//! for a plugin whose configuration is wrong, which is exactly when someone is
//! looking. The typed `Config` is captured by the closure and never named
//! outside the plugin's own crate — the framework holds a port trait object
//! and nothing else.
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
//!         .worker(&resonate_worker_kafka::PLUGIN)
//!         .server(&resonate_server_dbms::SQLITE))
//! }
//! ```

pub mod config;
pub mod error;
pub mod manifest;
pub mod plugin;
pub mod registry;

pub use config::{Loaded, Loader, OwnedSettings, Settings, ACTIVE, ENABLED};
pub use error::{ConfigError, RegistryError, StartupError};
pub use manifest::{Manifest, Port};
pub use plugin::{
    BoxFuture, Env, GatewayCtx, GatewayFactory, GatewayPlugin, RouterCtx, RouterFactory,
    RouterPlugin, ServerConnect, ServerCtx, ServerFactory, ServerPlugin, WorkerCtx, WorkerFactory,
    WorkerPlugin,
};
pub use registry::{Entry, Registry};

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two — and so the pair that makes up the plugin ABI
// versions together.
pub use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
