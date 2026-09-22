//! The three kinds of plugin, and what each is handed when it is built.
//!
//! Each plugin is handed exactly one thing: what it talks to. A server is handed
//! its router, a worker and a gateway are handed the server. Everything else
//! either comes out of the plugin's own settings, or is handed to it by its
//! `init` — the process-wide debug flag included, which is why it appears in no
//! dependency struct.
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

use std::sync::{Arc, Weak};

use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};

use crate::config::Settings;
use crate::error::ConfigError;
use crate::routes::Routes;

/// A plugin's identity, from its crate name: `resonate-transport-http-poll`
/// becomes `transport_http_poll`.
///
/// Derived rather than declared, because two fields that have to agree
/// eventually will not. The `resonate-` prefix goes because every crate here
/// carries it and it would say nothing; what is left is the role and the
/// thing — which is the crate name doing its job, so the key repeats it rather
/// than inventing a second vocabulary. A crate outside this naming keeps its
/// whole name: `acme-kafka` is `acme_kafka`.
pub fn id_from_crate(krate: &str) -> String {
    krate
        .strip_prefix("resonate-")
        .unwrap_or(krate)
        .replace('-', "_")
}

// ─── Server ──────────────────────────────────────────────────────────────────

/// What the composition root gives a server: the router it delivers through.
///
/// The router exists before the server and is still empty — its workers are
/// installed once the server they hold a handle to exists. A server that needs a
/// handle to *itself* (an engine-backed one arms its timer with one) makes it
/// inside its own crate, where the concrete type is known.
#[non_exhaustive]
pub struct ServerDependencies {
    pub router: Arc<dyn ResonateRouter>,
}

impl ServerDependencies {
    pub fn new(router: Arc<dyn ResonateRouter>) -> Self {
        Self { router }
    }
}

/// A plugin that answers Resonate protocol requests.
///
/// The unit of pluggability, not the storage underneath it: whatever internal
/// shape a plugin uses to turn a request into a response — an engine, a client
/// to somewhere else, a model in memory — is its own business and stays inside
/// its own crate.
///
/// Selected rather than switched on: a binary has exactly one.
// The `configure` signature below is long, and naming it would only move it
// somewhere a reader has to go and look. It is the one thing a plugin author
// has to understand, so it is written where they will read it.
#[allow(clippy::type_complexity)]
#[non_exhaustive]
pub struct ServerPlugin {
    pub krate: &'static str,
    /// Read this plugin's settings and build it.
    ///
    /// Sync, like every other kind: connecting is `init`'s, so a plugin that
    /// needs a pool, a schema or a session opens it there and fails there. This
    /// stays cheap, side-effect-free, and the only place a bad setting is
    /// reported.
    ///
    /// No `Option`, unlike the other two: a binary has one server, chosen by
    /// name, so switching it off is not a thing to express.
    pub configure:
        fn(&Settings<'_>, ServerDependencies) -> Result<Arc<dyn ResonateServer>, ConfigError>,
}

impl ServerPlugin {
    /// Where this plugin's settings live, and what it is called everywhere
    /// else. See [`id_from_crate`].
    pub fn id(&self) -> String {
        id_from_crate(self.krate)
    }

    #[allow(clippy::type_complexity)]
    pub const fn new(
        krate: &'static str,
        configure: fn(
            &Settings<'_>,
            ServerDependencies,
        ) -> Result<Arc<dyn ResonateServer>, ConfigError>,
    ) -> Self {
        Self { krate, configure }
    }
}

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
    /// Where to put an HTTP route this worker needs served — a callback
    /// endpoint, a long-poll connection, anything a peer dials rather than
    /// receives. See [`Routes`]: a worker never binds a socket of its own.
    pub routes: Arc<Routes>,
}

impl WorkerDependencies {
    pub fn new(server: Weak<dyn ResonateServer>, routes: Arc<Routes>) -> Self {
        Self { server, routes }
    }
}

/// A plugin that consumes what a server emits.
// The `configure` signature below is long, and naming it would only move it
// somewhere a reader has to go and look. It is the one thing a plugin author
// has to understand, so it is written where they will read it.
#[allow(clippy::type_complexity)]
#[non_exhaustive]
pub struct WorkerPlugin {
    /// `env!("CARGO_PKG_NAME")`. Both this plugin's identity and what a
    /// collision has to name, because the person who can fix one is the person
    /// assembling the binary.
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
    /// Where this plugin's settings live, and what it is called everywhere
    /// else. See [`id_from_crate`].
    pub fn id(&self) -> String {
        id_from_crate(self.krate)
    }

    #[allow(clippy::type_complexity)]
    pub const fn new(
        krate: &'static str,
        schemes: &'static [&'static str],
        configure: fn(
            &Settings<'_>,
            WorkerDependencies,
        ) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError>,
    ) -> Self {
        Self {
            krate,
            schemes,
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
    /// The routes every plugin registered. A gateway that serves HTTP takes
    /// them in its `init` and merges them into its own router; one that does
    /// not — the metrics endpoint — leaves them alone.
    pub routes: Arc<Routes>,
}

impl GatewayDependencies {
    pub fn new(server: Arc<dyn ResonateServer>, routes: Arc<Routes>) -> Self {
        Self { server, routes }
    }
}

/// A plugin that accepts requests from outside and puts them to the server.
// The `configure` signature below is long, and naming it would only move it
// somewhere a reader has to go and look. It is the one thing a plugin author
// has to understand, so it is written where they will read it.
#[allow(clippy::type_complexity)]
#[non_exhaustive]
pub struct GatewayPlugin {
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
    /// Where this plugin's settings live, and what it is called everywhere
    /// else. See [`id_from_crate`].
    pub fn id(&self) -> String {
        id_from_crate(self.krate)
    }

    #[allow(clippy::type_complexity)]
    pub const fn new(
        krate: &'static str,
        configure: fn(
            &Settings<'_>,
            GatewayDependencies,
        ) -> Result<Option<Arc<dyn ResonateGateway>>, ConfigError>,
    ) -> Self {
        Self { krate, configure }
    }
}

macro_rules! debug_by_id {
    ($($t:ty),*) => {$(
        impl std::fmt::Debug for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($t))
                    .field("krate", &self.krate)
                    .finish_non_exhaustive()
            }
        }
    )*};
}

debug_by_id!(ServerPlugin, WorkerPlugin, GatewayPlugin);
