//! The three kinds of plugin, and what each is handed when it is built.
//!
//! Each is a `static` with no `impl` block — less ceremony than a trait for
//! someone writing their first plugin, and `const`-constructible, so a plugin's
//! identity is data in the binary rather than something built at startup. (A
//! trait could not carry `const ID` through `dyn`, so it would be three methods
//! and an impl block where these are four fields.)

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};

use crate::config::Settings;
use crate::error::{ConfigError, StartupError};

// ─── Worker ──────────────────────────────────────────────────────────────────

/// What a worker is handed.
///
/// The server handle is [`Weak`] deliberately: a router holds its workers and a
/// server holds its router, so a strong handle back would close a reference
/// cycle and nothing in it would ever be dropped. Upgrade per message; a failed
/// upgrade means the server is gone and there is no work worth doing.
#[non_exhaustive]
pub struct WorkerCtx {
    pub server: Weak<dyn ResonateServer>,
    /// `tasks.lease_timeout` — what an in-process worker requests when it
    /// acquires, unless it has an opinion of its own. Server-owned, so it
    /// cannot come out of the plugin's own settings.
    pub task_lease_timeout: i64,
}

impl WorkerCtx {
    pub fn new(server: Weak<dyn ResonateServer>, task_lease_timeout: i64) -> Self {
        Self {
            server,
            task_lease_timeout,
        }
    }
}

/// Build the worker. Sync and infallible: everything that can fail belongs in
/// `configure`, and everything that starts background work belongs in the
/// worker's own `init`.
pub type WorkerFactory = Box<dyn FnOnce(&WorkerCtx) -> Arc<dyn ResonateWorker> + Send>;

/// A plugin that consumes what a server emits.
pub struct WorkerPlugin {
    /// The name this plugin is known by: its configuration key
    /// (`transports.<id>`), its `--set` path, its log field.
    pub id: &'static str,
    /// `env!("CARGO_PKG_NAME")`. What a collision has to name, because the
    /// person who can fix one is the person assembling the binary.
    pub krate: &'static str,
    /// The address schemes this worker claims.
    pub schemes: &'static [&'static str],
    /// Extract and validate. Runs before anything is constructed, so a bad
    /// setting is a startup error rather than a message that later goes quietly
    /// nowhere. The typed `Config` is captured by the returned closure and never
    /// named outside the plugin's own crate.
    ///
    /// `None` means this plugin's own configuration turned it off: it is not
    /// registered, and the router reports its schemes as undeliverable.
    pub configure: fn(&Settings<'_>) -> Result<Option<WorkerFactory>, ConfigError>,
}

impl WorkerPlugin {
    pub const fn new(
        id: &'static str,
        krate: &'static str,
        schemes: &'static [&'static str],
        configure: fn(&Settings<'_>) -> Result<Option<WorkerFactory>, ConfigError>,
    ) -> Self {
        Self {
            id,
            krate,
            schemes,
            configure,
        }
    }
}

// ─── Server ──────────────────────────────────────────────────────────────────

/// What a server is handed: the router it delivers through, and a handle to
/// itself, which its workers already hold.
#[non_exhaustive]
pub struct ServerCtx {
    pub router: Arc<dyn ResonateRouter>,
    /// The place this server is about to fill. Store it; the handle is not yet
    /// fulfilled while the factory runs.
    pub this: Weak<dyn ResonateServer>,
    /// The process-wide debug flag: the clock belongs to the caller, so nothing
    /// may start work that runs on wall time.
    pub debug: bool,
}

impl ServerCtx {
    pub fn new(
        router: Arc<dyn ResonateRouter>,
        this: Weak<dyn ResonateServer>,
        debug: bool,
    ) -> Self {
        Self {
            router,
            this,
            debug,
        }
    }
}

/// Build the server, once the router and its workers exist.
pub type ServerFactory = Box<dyn FnOnce(&ServerCtx) -> Arc<dyn ResonateServer> + Send>;

/// Acquire what the server needs — a connection pool, a schema, a session.
/// Async and fallible, and run before anything else, so a database that will not
/// answer is a startup failure rather than a half-built process.
pub type ServerConnect = Box<
    dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<ServerFactory, StartupError>> + Send>>
        + Send,
>;

/// A plugin that answers Resonate protocol requests.
///
/// The unit of pluggability, not the storage underneath it: whatever internal
/// shape a plugin uses to turn a request into a response — an engine, a client
/// to somewhere else, a model in memory — is its own business and stays inside
/// its own crate.
///
/// Three phases where the others have two, because connecting is asynchronous
/// and fallible while the rest of the wiring is neither. And no `Option`: a
/// binary has one server, chosen by name, so switching it off is not a thing to
/// express.
pub struct ServerPlugin {
    /// Its configuration key is `servers.<id>`, and `servers.active` is how one
    /// is chosen.
    pub id: &'static str,
    pub krate: &'static str,
    pub configure: fn(&Settings<'_>) -> Result<ServerConnect, ConfigError>,
}

impl ServerPlugin {
    pub const fn new(
        id: &'static str,
        krate: &'static str,
        configure: fn(&Settings<'_>) -> Result<ServerConnect, ConfigError>,
    ) -> Self {
        Self {
            id,
            krate,
            configure,
        }
    }
}

// ─── Gateway ─────────────────────────────────────────────────────────────────

/// What a gateway is handed. Strong, unlike a worker: a gateway is not in the
/// reference cycle, and it keeps its server alive for exactly as long as it can
/// still accept a request.
#[non_exhaustive]
pub struct GatewayCtx {
    pub server: Arc<dyn ResonateServer>,
}

impl GatewayCtx {
    pub fn new(server: Arc<dyn ResonateServer>) -> Self {
        Self { server }
    }
}

/// Build the gateway. Binding a port belongs in its `init`, not here — a
/// gateway is the last thing to start.
pub type GatewayFactory = Box<dyn FnOnce(GatewayCtx) -> Arc<dyn ResonateGateway> + Send>;

/// A plugin that accepts requests from outside and puts them to the server.
pub struct GatewayPlugin {
    /// Its configuration key is `gateways.<id>`.
    pub id: &'static str,
    pub krate: &'static str,
    /// `None` means this plugin's own configuration turned it off.
    pub configure: fn(&Settings<'_>) -> Result<Option<GatewayFactory>, ConfigError>,
}

impl GatewayPlugin {
    pub const fn new(
        id: &'static str,
        krate: &'static str,
        configure: fn(&Settings<'_>) -> Result<Option<GatewayFactory>, ConfigError>,
    ) -> Self {
        Self {
            id,
            krate,
            configure,
        }
    }
}

macro_rules! debug_by_id {
    ($($t:ty),*) => {$(
        impl std::fmt::Debug for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($t))
                    .field("id", &self.id)
                    .field("krate", &self.krate)
                    .finish_non_exhaustive()
            }
        }
    )*};
}

debug_by_id!(WorkerPlugin, ServerPlugin, GatewayPlugin);
