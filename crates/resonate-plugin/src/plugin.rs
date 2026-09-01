//! The four kinds of plugin, and what each is handed when it is built.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};

use crate::config::Settings;
use crate::error::{ConfigError, StartupError};
use crate::manifest::{Manifest, Port};

/// What the server owns rather than any plugin, handed to plugins that need it.
///
/// Non-exhaustive on purpose: this is the one place the framework can grow
/// without every plugin recompiling against a new field.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Env {
    /// The process-wide debug flag: the clock belongs to the caller, so
    /// nothing may start work that runs on wall time.
    pub debug: bool,
    /// `tasks.lease_timeout` — what an in-process worker requests when it
    /// acquires, unless it has an opinion of its own.
    pub task_lease_timeout: i64,
    /// `tasks.retry_timeout` — how long a pending task waits to be redispatched.
    pub task_retry_timeout: i64,
    /// The externally reachable URL of this server, stamped into the messages
    /// it emits so a worker knows where to call back.
    pub server_url: Option<String>,
}

impl Env {
    pub fn new(
        debug: bool,
        task_lease_timeout: i64,
        task_retry_timeout: i64,
        server_url: Option<String>,
    ) -> Self {
        Self {
            debug,
            task_lease_timeout,
            task_retry_timeout,
            server_url,
        }
    }
}

/// A future a plugin hands back, boxed so the plugin structs stay plain data.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ─── Worker ───────────────────────────────────────────────────────────────────

/// What a worker is handed. Weak, because this is the edge that closes the ring.
#[non_exhaustive]
pub struct WorkerCtx {
    /// Upgrade per message. A failed upgrade means the server is gone and
    /// there is no work worth doing.
    pub server: Weak<dyn ResonateServer>,
    pub env: Env,
}

impl WorkerCtx {
    pub fn new(server: Weak<dyn ResonateServer>, env: Env) -> Self {
        Self { server, env }
    }
}

/// Phase two: build the worker. Sync and infallible, because it runs inside
/// the expression that closes the ring, where nothing may fail or await.
pub type WorkerFactory = Box<dyn FnOnce(&WorkerCtx) -> Arc<dyn ResonateWorker> + Send>;

/// A plugin that consumes what a server emits.
///
/// A `static` with no `impl` block: less ceremony than a trait for someone
/// writing their first plugin, and `const`-constructible, so the manifest is
/// data in the binary rather than something built at startup.
pub struct WorkerPlugin {
    pub manifest: Manifest,
    /// This plugin's settings when the operator has said nothing. Seeded into
    /// the configuration's defaults layer, so the server's own defaults do not
    /// have to name a single plugin.
    pub defaults: fn() -> serde_json::Value,
    /// Phase one: extract and validate. Runs before anything is constructed,
    /// so a bad setting is a startup error rather than a message that later
    /// goes quietly nowhere. The typed `Config` is captured in the returned
    /// closure and never has to be named outside the plugin's own crate.
    pub configure: fn(&Settings<'_>) -> Result<WorkerFactory, ConfigError>,
}

impl WorkerPlugin {
    pub const PORT: Port = Port::Worker;

    pub const fn new(
        manifest: Manifest,
        defaults: fn() -> serde_json::Value,
        configure: fn(&Settings<'_>) -> Result<WorkerFactory, ConfigError>,
    ) -> Self {
        Self {
            manifest,
            defaults,
            configure,
        }
    }

    pub fn config_key(&self) -> String {
        self.manifest.config_key(Port::Worker)
    }
}

// ─── Router ───────────────────────────────────────────────────────────────────

/// What a router is handed: every worker this binary registered, by scheme.
#[non_exhaustive]
pub struct RouterCtx {
    pub workers: HashMap<String, Arc<dyn ResonateWorker>>,
    pub env: Env,
}

impl RouterCtx {
    pub fn new(workers: HashMap<String, Arc<dyn ResonateWorker>>, env: Env) -> Self {
        Self { workers, env }
    }
}

pub type RouterFactory = Box<dyn FnOnce(RouterCtx) -> Arc<dyn ResonateRouter> + Send>;

/// A plugin that resolves an address to a worker.
///
/// The fourth node, and the one that has had no plugin: routing policy —
/// best-effort, retrying, dead-lettering, fanning out — is a choice, and
/// leaving it in the composition root made it a property of the binary.
pub struct RouterPlugin {
    pub manifest: Manifest,
    pub defaults: fn() -> serde_json::Value,
    pub configure: fn(&Settings<'_>) -> Result<RouterFactory, ConfigError>,
}

impl RouterPlugin {
    pub const PORT: Port = Port::Router;

    pub const fn new(
        manifest: Manifest,
        defaults: fn() -> serde_json::Value,
        configure: fn(&Settings<'_>) -> Result<RouterFactory, ConfigError>,
    ) -> Self {
        Self {
            manifest,
            defaults,
            configure,
        }
    }

    pub fn config_key(&self) -> String {
        self.manifest.config_key(Port::Router)
    }
}

// ─── Server ───────────────────────────────────────────────────────────────────

/// What a server is handed: the router it delivers through, and a handle to
/// itself for anything that has to call back in before it exists.
#[non_exhaustive]
pub struct ServerCtx {
    pub router: Arc<dyn ResonateRouter>,
    /// Dangling while the factory runs — the ring is not closed yet — and live
    /// by the time anything routes. Store it; do not upgrade it here.
    pub this: Weak<dyn ResonateServer>,
    pub env: Env,
}

impl ServerCtx {
    pub fn new(router: Arc<dyn ResonateRouter>, this: Weak<dyn ResonateServer>, env: Env) -> Self {
        Self { router, this, env }
    }
}

/// Phase three: build the server, inside the expression that closes the ring.
pub type ServerFactory = Box<dyn FnOnce(&ServerCtx) -> Arc<dyn ResonateServer> + Send>;

/// Phase two: acquire whatever the server needs — a connection pool, a schema,
/// a session. Async and fallible, and run before the ring is closed, so a
/// database that will not answer is a startup failure and not a half-built
/// process.
pub type ServerConnect =
    Box<dyn FnOnce(Env) -> BoxFuture<'static, Result<ServerFactory, StartupError>> + Send>;

/// A plugin that answers Resonate protocol requests.
///
/// The unit of pluggability, not the storage underneath it: what varies is how
/// a request becomes a response, and whatever internal shape a plugin uses to
/// get there — an engine, a client to somewhere else, a model in memory — is
/// its own business and stays inside its own crate.
///
/// Three phases rather than two, because connecting is asynchronous and
/// fallible while closing the ring is neither.
pub struct ServerPlugin {
    pub manifest: Manifest,
    pub defaults: fn() -> serde_json::Value,
    pub configure: fn(&Settings<'_>) -> Result<ServerConnect, ConfigError>,
}

impl ServerPlugin {
    pub const PORT: Port = Port::Server;

    pub const fn new(
        manifest: Manifest,
        defaults: fn() -> serde_json::Value,
        configure: fn(&Settings<'_>) -> Result<ServerConnect, ConfigError>,
    ) -> Self {
        Self {
            manifest,
            defaults,
            configure,
        }
    }

    pub fn config_key(&self) -> String {
        self.manifest.config_key(Port::Server)
    }
}

// ─── Gateway ──────────────────────────────────────────────────────────────────

/// What a gateway is handed. Strong, unlike a worker: a gateway is not inside
/// the ring, and it keeps its server alive for exactly as long as it can still
/// accept a request.
#[non_exhaustive]
pub struct GatewayCtx {
    pub server: Arc<dyn ResonateServer>,
    pub env: Env,
}

impl GatewayCtx {
    pub fn new(server: Arc<dyn ResonateServer>, env: Env) -> Self {
        Self { server, env }
    }
}

pub type GatewayFactory = Box<dyn FnOnce(GatewayCtx) -> Arc<dyn ResonateGateway> + Send>;

/// A plugin that accepts requests from outside and puts them to the server.
pub struct GatewayPlugin {
    pub manifest: Manifest,
    pub defaults: fn() -> serde_json::Value,
    pub configure: fn(&Settings<'_>) -> Result<GatewayFactory, ConfigError>,
}

impl GatewayPlugin {
    pub const PORT: Port = Port::Gateway;

    pub const fn new(
        manifest: Manifest,
        defaults: fn() -> serde_json::Value,
        configure: fn(&Settings<'_>) -> Result<GatewayFactory, ConfigError>,
    ) -> Self {
        Self {
            manifest,
            defaults,
            configure,
        }
    }

    pub fn config_key(&self) -> String {
        self.manifest.config_key(Port::Gateway)
    }
}

impl std::fmt::Debug for WorkerPlugin {
    /// The manifest, which is the printable half. A factory has nothing to
    /// show and a log line wants the name anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPlugin")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RouterPlugin {
    /// The manifest, which is the printable half. A factory has nothing to
    /// show and a log line wants the name anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterPlugin")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ServerPlugin {
    /// The manifest, which is the printable half. A factory has nothing to
    /// show and a log line wants the name anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerPlugin")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for GatewayPlugin {
    /// The manifest, which is the printable half. A factory has nothing to
    /// show and a log line wants the name anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayPlugin")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}
