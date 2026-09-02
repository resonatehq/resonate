//! The composition root.
//!
//! A Resonate server is a [`Registry`] of plugins and a `run`. This crate is
//! what turns the first into the second: it reads the configuration, builds
//! server, workers and gateways in that order, starts them, waits, and stops
//! them in the order that drains rather than deadlocks.
//!
//! It names no plugin. Which plugins exist is the *binary's* to say — a
//! dependency in its `Cargo.toml` and a line in its registry — which is what
//! lets a build carry four plugins rather than four hundred, and lets a plugin
//! be published by someone who never touches this repository.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> std::process::ExitCode {
//!     resonate_base::main(
//!         Registry::new()
//!             .server(&resonate_server_sqlite::PLUGIN)
//!             .worker(&resonate_worker_kafka::PLUGIN)
//!             .gateway(&resonate_gateway_http::PLUGIN),
//!         Options::default().default_server("server_sqlite"),
//!     )
//!     .await
//! }
//! ```

pub mod router;

use std::collections::HashMap;
use std::sync::Arc;

use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
use resonate_plugin::{
    Configuration, GatewayDependencies, Loader, Registry, ServerDependencies, WorkerDependencies,
};
use serde::{Deserialize, Serialize};

pub use router::Router;

/// How to find the configuration, and what to do when it says nothing.
///
/// Not settings — those are the operator's, and live in the file. These are the
/// binary's own decisions: where to look, and which server it defaults to when
/// it carries more than one.
#[derive(Debug, Clone)]
pub struct Options {
    /// The config file to read. Missing is not an error.
    pub file: std::path::PathBuf,
    /// Environment prefix, `RESONATE_` by convention.
    pub env_prefix: String,
    /// `--set key=value`, highest precedence. The key space is the config's, so
    /// this covers every plugin that exists or ever will.
    pub overrides: Vec<(String, String)>,
    /// What `servers.active` falls back to.
    pub default_server: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            file: "resonate.toml".into(),
            env_prefix: "RESONATE_".to_string(),
            overrides: Vec::new(),
            default_server: String::new(),
        }
    }
}

impl Options {
    pub fn file(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.file = path.into();
        self
    }

    pub fn env_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.env_prefix = prefix.into();
        self
    }

    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.overrides.push((key.into(), value.into()));
        self
    }

    pub fn default_server(mut self, id: impl Into<String>) -> Self {
        self.default_server = id.into();
        self
    }
}

/// The settings that belong to the process rather than to any one plugin.
///
/// Everything else is under `servers.*`, `workers.*` or `gateways.*` and is
/// read by the plugin it belongs to. These three are read here because nothing
/// else could: they are about the process itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Process {
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_level")]
    pub level: String,

    /// Debug mode: the clock belongs to the caller. Handed to every plugin's
    /// `init`, which is the only place it appears in the plugin surface.
    #[serde(default)]
    pub debug: bool,

    /// How long to wait for in-flight work when stopping (ms).
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout: u64,
}

fn default_level() -> String {
    "info".to_string()
}

fn default_shutdown_timeout() -> u64 {
    10_000
}

impl Default for Process {
    fn default() -> Self {
        Self {
            level: default_level(),
            debug: false,
            shutdown_timeout: default_shutdown_timeout(),
        }
    }
}

/// Run a server until it is asked to stop, and report to the shell.
///
/// The whole of a custom binary's `main`, which is the point: everything that
/// varies is in the registry it is handed.
pub async fn main(registry: Registry, options: Options) -> std::process::ExitCode {
    match run(registry, options).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // Tracing may not be up yet — a bad config fails before it is — so
            // this goes to stderr as well as to the log.
            tracing::error!("{e}");
            eprintln!("Fatal: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Build it, start it, wait, stop it.
pub async fn run(registry: Registry, options: Options) -> Result<(), String> {
    // What is wrong with this *set* of plugins is knowable from the plugins
    // alone — before any configuration is read, and before anything is built.
    registry.check().map_err(|errors| {
        errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    })?;

    let config = load(&options)?;
    let process: Process = config.extract().map_err(|e| e.to_string())?;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&process.level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let debug = process.debug;
    if debug {
        tracing::info!(
            "Debug mode enabled — the clock belongs to the caller: debug.* is \
             answered, head.debug_time is honoured, and nothing runs on wall time"
        );
    }

    let running = build(&registry, &config, &options)?;
    running.start(debug).await?;
    shutdown_signal().await;
    running
        .stop(std::time::Duration::from_millis(process.shutdown_timeout))
        .await;
    tracing::info!("Resonate Server stopped");
    Ok(())
}

/// The layers, in the order each wins over the last.
fn load(options: &Options) -> Result<Configuration, String> {
    let mut loader = Loader::new().file(&options.file).env(&options.env_prefix);
    for (key, value) in &options.overrides {
        loader = loader.set(key, value).map_err(|e| e.to_string())?;
    }
    Ok(loader.load())
}

/// A built server: everything constructed, nothing started.
///
/// Held together so `start` and `stop` are the only two orders anyone has to
/// get right, and both are written down once.
pub struct Running {
    router: Arc<Router>,
    server: Arc<dyn ResonateServer>,
    gateways: Vec<Arc<dyn ResonateGateway>>,
}

impl Running {
    /// The server this binary was pointed at, for a caller that wants to reach
    /// it without a gateway.
    pub fn server(&self) -> &Arc<dyn ResonateServer> {
        &self.server
    }
}

/// Build everything, in order, connecting nothing.
///
/// Not a cycle. The router is the only participant that starts incomplete —
/// step 3 is what closes the loop — so nothing is ever handed a value that does
/// not exist yet, and every `configure` is cheap and side-effect free.
pub fn build(
    registry: &Registry,
    config: &Configuration,
    options: &Options,
) -> Result<Running, String> {
    // 1. The router, empty.
    let router = Arc::new(Router::new());

    // 2. The server this binary was pointed at. Nothing is connected yet —
    //    opening the database is `init`'s, like every other port's resource.
    let active = config.active_server(&options.default_server);
    let chosen = registry.select_server(&active).map_err(|e| e.to_string())?;
    let server = (chosen.configure)(
        &config.server(&chosen.id()),
        ServerDependencies::new(Arc::clone(&router) as Arc<dyn ResonateRouter>),
    )
    .map_err(|e| e.to_string())?;
    tracing::info!(server = %chosen.id(), "Server plugin selected");

    // 3. The workers, each downgrading the server that now exists.
    //
    //    One map, not two: the router is handed how to reach a worker, and
    //    `init` is driven from the plugin loop below rather than from the
    //    routing table — a worker claiming two schemes is in the table twice
    //    and would otherwise be started twice.
    let mut routes: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
    for plugin in registry.workers() {
        let deps = WorkerDependencies::new(Arc::downgrade(&server));
        let Some(worker) =
            (plugin.configure)(&config.worker(&plugin.id()), deps).map_err(|e| e.to_string())?
        else {
            tracing::info!(worker = %plugin.id(), "Worker plugin disabled");
            continue;
        };
        for scheme in plugin.schemes {
            routes.insert((*scheme).to_string(), Arc::clone(&worker));
        }
        tracing::info!(worker = %plugin.id(), schemes = ?plugin.schemes, "Worker plugin registered");
    }

    // 4. Install them. The router is complete from here and never changes again.
    router
        .install(routes)
        .map_err(|e| format!("the router is built here and nowhere else: {e}"))?;

    // 5. The gateways, holding the server strongly. Nothing is bound yet.
    let mut gateways: Vec<Arc<dyn ResonateGateway>> = Vec::new();
    for plugin in registry.gateways() {
        let deps = GatewayDependencies::new(Arc::clone(&server));
        let Some(gateway) =
            (plugin.configure)(&config.gateway(&plugin.id()), deps).map_err(|e| e.to_string())?
        else {
            tracing::info!(gateway = %plugin.id(), "Gateway plugin disabled");
            continue;
        };
        gateways.push(gateway);
        tracing::info!(gateway = %plugin.id(), "Gateway plugin registered");
    }

    Ok(Running {
        router,
        server,
        gateways,
    })
}

impl Running {
    /// Start, in the order things were built.
    ///
    /// One rule, no special case to remember — and it is also the order that
    /// works: a worker that upgrades its handle finds a server that can already
    /// answer, and a gateway binds only once everything behind it can serve
    /// what it accepts. Accepting a request the rest of the process cannot
    /// serve is worse than not accepting it, so the gateways are last.
    pub async fn start(&self, debug: bool) -> Result<(), String> {
        self.router
            .init(debug)
            .await
            .map_err(|e| format!("a worker failed to start: {e}"))?;
        self.server
            .init(debug)
            .await
            .map_err(|e| format!("the server failed to start: {e}"))?;
        for gateway in &self.gateways {
            gateway
                .init(debug)
                .await
                .map_err(|e| format!("a gateway failed to start: {e}"))?;
        }
        Ok(())
    }

    /// Stop, which is not the mirror image of `start`.
    ///
    /// The exception is load-bearing. A gateway's graceful drain can be waiting
    /// on a long-lived response — an SSE stream — that only a worker's `stop`
    /// releases, so stopping the gateways first would deadlock. It also means a
    /// client gets a 503 rather than a closed socket while in-flight work
    /// drains.
    ///
    /// Nothing here is fatal: the process is on its way down, and one plugin
    /// refusing to drain is no reason to leave the others running.
    pub async fn stop(&self, timeout: std::time::Duration) {
        tracing::info!("Shutting down, draining background tasks...");
        let drain = async {
            // The server first: its timer is the only thing that can still hand
            // it work of its own, and `stop` drains what it spawned behind it.
            if let Err(e) = self.server.stop().await {
                tracing::warn!(error = %e, "server did not stop cleanly");
            }
            // Then the workers, through the router that holds them. What feeds
            // them has stopped, so this drains what is already in flight rather
            // than racing new deliveries.
            let _ = self.router.stop().await;
            // The gateways last. See above.
            for gateway in &self.gateways {
                if let Err(e) = gateway.stop().await {
                    tracing::warn!(error = %e, "gateway did not stop cleanly");
                }
            }
        };
        if tokio::time::timeout(timeout, drain).await.is_err() {
            tracing::warn!("Background tasks did not finish within shutdown timeout, forcing exit");
        }
    }
}

/// Wait for SIGINT or SIGTERM.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, initiating graceful shutdown..."),
        _ = terminate => tracing::info!("Received SIGTERM, initiating graceful shutdown..."),
    }
}
