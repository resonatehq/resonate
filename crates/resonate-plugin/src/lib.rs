//! The Resonate worker plugin surface.
//!
//! What a worker plugin announces, how it is configured, and how a binary
//! assembles one. This is the crate every plugin pins, so it is deliberately
//! small: a manifest, a plugin, a registry, and the configuration loader. No
//! transport, no storage, no runtime.
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
//!         .worker(&resonate_worker_kafka::PLUGIN))
//! }
//! ```

pub mod config;
pub mod error;
pub mod manifest;
pub mod plugin;
pub mod registry;

/// The configuration section every worker plugin lives under.
pub const SECTION: &str = "transports";

pub use config::{Loaded, Loader, Settings, ENABLED};
pub use error::{ConfigError, RegistryError};
pub use manifest::Manifest;
pub use plugin::{WorkerCtx, WorkerFactory, WorkerPlugin};
pub use registry::Registry;

// The port traits a plugin implements, re-exported so a plugin crate names one
// dependency rather than two.
pub use resonate_core::{ResonateServer, ResonateWorker};
