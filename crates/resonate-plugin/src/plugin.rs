//! The three kinds of plugin, and what each is handed when it is built.
//!
//! Each is a `static` with no `impl` block — less ceremony than a trait for
//! someone writing their first plugin, and `const`-constructible, so a plugin's
//! identity is data in the binary rather than something built at startup. (A
//! trait could not carry `const ID` through `dyn`, so it would be three methods
//! and an impl block where these are four fields.)
//!
//! Everything here is `#[non_exhaustive]`, so `new` is the only way to build one
//! and reading a field is the only way to use one. That is what lets any of
//! these grow later: a plugin reads `deps.server` and is unaffected by a
//! dependency added beside it, and no plugin can have written a struct literal
//! or an exhaustive destructure that a new field would break.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};

use crate::config::Settings;
use crate::error::{ConfigError, StartupError};

// ─── Worker ──────────────────────────────────────────────────────────────────

/// What the composition root gives a worker: everything it needs and cannot
/// read out of its own settings.
///
/// Which is one thing. Anything else a worker needs, it declares in its own
/// section with its own default.
///
/// [`Weak`] deliberately: a router holds its workers and a server holds its
/// router, so a strong handle back would close a reference cycle and nothing in
/// it would ever be dropped. Upgrade per message; a failed upgrade means the
/// server is gone and there is no work worth doing.
#[non_exhaustive]
pub struct WorkerDependencies {
    pub server: Weak<dyn ResonateServer>,
}

impl WorkerDependencies {
    pub fn new(server: Weak<dyn ResonateServer>) -> Self {
        Self { server }
    }
}

/// A plugin that consumes what a server emits.
// The `configure` signature below is long, and naming it would only move it
// somewhere a reader has to go and look. It is the one thing a plugin author
// has to understand, so it is written where they will read it.
#[allow(clippy::type_complexity)]
#[non_exhaustive]
pub struct WorkerPlugin {
    /// The name this plugin is known by: its configuration key
    /// (`transports.<id>`), its `--set` path, its log field.
    pub id: &'static str,
    /// `env!("CARGO_PKG_NAME")`. What a collision has to name, because the
    /// person who can fix one is the person assembling the binary.
    pub krate: &'static str,
    /// The address schemes this worker claims.
    pub schemes: &'static [&'static str],
    /// Read this plugin's settings and build it.
    ///
    /// Nothing is deferred: a worker's one dependency exists by the time this
    /// runs. Whatever starts background work belongs in the worker's own `init`,
    /// not here, so this stays cheap and side-effect-free.
    ///
    /// The typed `Config` never leaves the plugin's crate — what comes back is a
    /// [`ResonateWorker`] and nothing else. `None` means this plugin's own
    /// configuration turned it off: it is not registered, and the router reports
    /// its schemes as undeliverable.
    pub configure: fn(
        &Settings<'_>,
        WorkerDependencies,
    ) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError>,
}

impl WorkerPlugin {
    #[allow(clippy::type_complexity)]
    pub const fn new(
        id: &'static str,
        krate: &'static str,
        schemes: &'static [&'static str],
        configure: fn(
            &Settings<'_>,
            WorkerDependencies,
        ) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError>,
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

/// What the composition root gives a server: the two things it needs and cannot
/// read out of its own settings.
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
pub struct ServerDependencies {
    pub router: Arc<dyn ResonateRouter>,
    /// The clock belongs to the caller, so nothing may start work that runs on
    /// wall time.
    pub debug: bool,
}

impl ServerDependencies {
    pub fn new(router: Arc<dyn ResonateRouter>, debug: bool) -> Self {
        Self { router, debug }
    }
}

/// A server under construction: connecting, and whatever it does with what it
/// connected to.
///
/// The one place a future appears in this crate. A server is the only kind that
/// does I/O to come into existence — a pool, a schema, a session — and it is
/// fallible in a way that is not the operator's config being wrong, which is why
/// it carries a [`StartupError`] rather than a [`ConfigError`].
pub type ServerFuture =
    Pin<Box<dyn Future<Output = Result<Arc<dyn ResonateServer>, StartupError>> + Send>>;

/// A plugin that answers Resonate protocol requests.
///
/// The unit of pluggability, not the storage underneath it: whatever internal
/// shape a plugin uses to turn a request into a response — an engine, a client
/// to somewhere else, a model in memory — is its own business and stays inside
/// its own crate.
///
/// The only kind that is asynchronous to build, and the only one with no
/// `Option`: a binary has one server, chosen by name, so switching it off is not
/// a thing to express.
// The `configure` signature below is long, and naming it would only move it
// somewhere a reader has to go and look. It is the one thing a plugin author
// has to understand, so it is written where they will read it.
#[allow(clippy::type_complexity)]
#[non_exhaustive]
pub struct ServerPlugin {
    /// Its configuration key is `servers.<id>`, and `servers.active` is how one
    /// is chosen.
    pub id: &'static str,
    pub krate: &'static str,
    /// Read the settings, then connect.
    ///
    /// Two steps rather than one, and the split is the two ways this can fail:
    /// settings are read synchronously, so a bad one is reported before a socket
    /// is opened, and the future that follows owns what it read.
    pub configure: fn(&Settings<'_>, ServerDependencies) -> Result<ServerFuture, ConfigError>,
}

impl ServerPlugin {
    pub const fn new(
        id: &'static str,
        krate: &'static str,
        configure: fn(&Settings<'_>, ServerDependencies) -> Result<ServerFuture, ConfigError>,
    ) -> Self {
        Self {
            id,
            krate,
            configure,
        }
    }
}

// ─── Gateway ─────────────────────────────────────────────────────────────────

/// What the composition root gives a gateway.
///
/// Strong, unlike a worker: a gateway is not in the reference cycle, and it
/// keeps its server alive for exactly as long as it can still accept a request.
#[non_exhaustive]
pub struct GatewayDependencies {
    pub server: Arc<dyn ResonateServer>,
}

impl GatewayDependencies {
    pub fn new(server: Arc<dyn ResonateServer>) -> Self {
        Self { server }
    }
}

/// A plugin that accepts requests from outside and puts them to the server.
// The `configure` signature below is long, and naming it would only move it
// somewhere a reader has to go and look. It is the one thing a plugin author
// has to understand, so it is written where they will read it.
#[allow(clippy::type_complexity)]
#[non_exhaustive]
pub struct GatewayPlugin {
    /// Its configuration key is `gateways.<id>`.
    pub id: &'static str,
    pub krate: &'static str,
    /// Read this plugin's settings and build it. Binding a port belongs in the
    /// gateway's own `init`, not here — a gateway is the last thing to start.
    ///
    /// `None` means this plugin's own configuration turned it off.
    pub configure: fn(
        &Settings<'_>,
        GatewayDependencies,
    ) -> Result<Option<Arc<dyn ResonateGateway>>, ConfigError>,
}

impl GatewayPlugin {
    #[allow(clippy::type_complexity)]
    pub const fn new(
        id: &'static str,
        krate: &'static str,
        configure: fn(
            &Settings<'_>,
            GatewayDependencies,
        ) -> Result<Option<Arc<dyn ResonateGateway>>, ConfigError>,
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
