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

/// Build the worker, given the server it reports back to.
///
/// Sync and infallible: everything that can fail belongs in `configure`, and
/// everything that starts background work belongs in the worker's own `init`.
/// The handle is the only thing a worker cannot get from its own settings —
/// everything else it needs it declares, with its own defaults.
///
/// [`Weak`] deliberately: a router holds its workers and a server holds its
/// router, so a strong handle back would close a reference cycle and nothing in
/// it would ever be dropped. Upgrade per message; a failed upgrade means the
/// server is gone and there is no work worth doing.
pub type WorkerFactory =
    Box<dyn FnOnce(Weak<dyn ResonateServer>) -> Arc<dyn ResonateWorker> + Send>;

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

/// What a server is handed: the two things it cannot read out of its own
/// settings.
///
/// The router exists before the server and is still empty — its workers are
/// installed once the server they hold a handle to exists. A server that needs a
/// handle to *itself* (an engine-backed one arms its timer with one) makes it
/// inside its own crate, where the concrete type is known.
///
/// `debug` is process-wide rather than any plugin's setting, and a server is the
/// only kind with nowhere else to receive it: a worker and a gateway are handed
/// it by their own `init`.
#[non_exhaustive]
pub struct ServerCtx {
    pub router: Arc<dyn ResonateRouter>,
    /// The clock belongs to the caller, so nothing may start work that runs on
    /// wall time.
    pub debug: bool,
}

impl ServerCtx {
    pub fn new(router: Arc<dyn ResonateRouter>, debug: bool) -> Self {
        Self { router, debug }
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

/// Build the gateway, given the server it puts requests to.
///
/// Strong, unlike a worker: a gateway is not in the reference cycle, and it
/// keeps its server alive for exactly as long as it can still accept a request.
/// Binding a port belongs in its `init`, not here — a gateway is the last thing
/// to start.
pub type GatewayFactory =
    Box<dyn FnOnce(Arc<dyn ResonateServer>) -> Arc<dyn ResonateGateway> + Send>;

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
