mod cli;
mod config;

mod mcp;
mod metrics;

mod router;

use std::sync::Arc;

use axum::{routing::get, Router};
use clap::{Parser, Subcommand};
use config::Config;
// A server with no storage engine cannot serve, and the alternative is a
// `match` whose only arm returns an error — which compiles, but reports itself
// as an unreachable statement and an unused binding rather than as the
// configuration mistake it is. The library has no such requirement: with every
// engine off it still carries the oracle.
#[cfg(not(any(feature = "sqlite", feature = "postgres", feature = "mysql")))]
compile_error!(
    "at least one storage engine must be enabled: --features sqlite, postgres, or mysql"
);

use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
use resonate_gateway_http::{Config as GatewayConfig, HttpGateway};
use resonate_gateway_web as console;
#[cfg(feature = "mysql")]
use resonate_server_mysql::MysqlEngine;
#[cfg(feature = "postgres")]
use resonate_server_postgres::PostgresEngine;
#[cfg(feature = "sqlite")]
use resonate_server_sqlite::SqliteEngine;
use resonate_sql::engine::Engine;
use resonate_transport_http_poll::PollRegistry;
use std::collections::HashMap;

#[derive(Parser)]
#[command(
    name = "resonate",
    about = "Resonate Server — durable promise engine",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Resonate server
    Serve(Box<cli::ServeArgs>),
    /// Start the Resonate server with in-memory storage (ephemeral, for development)
    Dev(Box<cli::DevArgs>),
    /// Promise operations
    #[command(alias = "promise")]
    Promises(cli::PromiseArgs),
    /// Task operations
    #[command(alias = "task")]
    Tasks(cli::TaskArgs),
    /// Schedule operations
    #[command(alias = "schedule")]
    Schedules(cli::ScheduleArgs),
    /// Invoke a function via a durable promise
    Invoke(cli::InvokeArgs),
    /// Display the call-graph tree rooted at a promise ID
    Tree(cli::TreeArgs),
    /// Start the Resonate MCP server (stdio transport)
    Mcp(Box<cli::McpArgs>),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Promises(args) => {
            cli::run_promises(args).await;
        }
        Commands::Tasks(args) => {
            cli::run_tasks(args).await;
        }
        Commands::Schedules(args) => {
            cli::run_schedules(args).await;
        }
        Commands::Invoke(args) => {
            cli::run_invoke(args).await;
        }
        Commands::Tree(args) => {
            cli::run_tree(args).await;
        }
        Commands::Mcp(args) => {
            cli::run_mcp(args).await;
        }
        Commands::Serve(args) => {
            let config = match Config::load() {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Fatal: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            };
            let config = args.apply(config);
            if let Err(e) = run_server(config).await {
                // Tracing may not be initialized if the error occurred
                // before tracing setup, so also write to stderr.
                tracing::error!("{e}");
                eprintln!("Fatal: {e}");
                return std::process::ExitCode::FAILURE;
            }
        }
        Commands::Dev(args) => {
            let config = match Config::load() {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Fatal: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            };
            let config = args.apply(config);
            if let Err(e) = run_server(config).await {
                tracing::error!("{e}");
                eprintln!("Fatal: {e}");
                return std::process::ExitCode::FAILURE;
            }
        }
    }
    std::process::ExitCode::SUCCESS
}

async fn run_server(config: Config) -> Result<(), String> {
    // Initialize tracing
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.level));

    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    tracing::info!(port = config.server.port, "Resonate Server starting");
    tracing::info!(
        timeout_poll_interval_ms = config.timeouts.poll_interval,
        task_retry_timeout_ms = config.tasks.retry_timeout,
        task_lease_timeout_ms = config.tasks.lease_timeout,
        "Operational config"
    );
    if config.debug {
        tracing::info!(
            "Debug mode enabled — the clock belongs to the caller: debug.* is \
             answered, head.debug_time is honoured, and nothing runs on wall time"
        );
    }

    // Validate storage config
    if config.storage.storage_type == "postgres" && config.storage.postgres.url.is_none() {
        return Err("storage.type=postgres requires RESONATE_STORAGE__POSTGRES__URL".into());
    }
    if config.storage.storage_type == "mysql" && config.storage.mysql.url.is_none() {
        return Err("MySQL storage selected but no URL configured. Set --storage-mysql-url or RESONATE_STORAGE__MYSQL__URL".to_string());
    }

    // Validate poll config (buffer_size=0 panics in tokio::mpsc::channel)
    if config.transports.http_poll.buffer_size == 0 {
        return Err("http_poll.buffer_size must be at least 1".into());
    }
    if config.transports.http_poll.max_connections == 0 {
        return Err("http_poll.max_connections must be at least 1".into());
    }

    // Backend selection. Each is a complete engine, not a storage handle
    // behind a shared one.
    let engine: Arc<dyn Engine> = match config.storage.storage_type.as_str() {
        #[cfg(feature = "postgres")]
        "postgres" => {
            let url = config.storage.postgres.url.as_ref().unwrap();
            let pool_size = config.storage.postgres.pool_size;
            tracing::info!("Using PostgreSQL backend");
            tracing::info!(pool_size = pool_size, "PostgreSQL pool configured");
            let pg = PostgresEngine::connect(
                url,
                pool_size,
                config.tasks.retry_timeout,
                config.storage.postgres.preload_limit,
                config.debug,
            )
            .await
            .map_err(|e| format!("Failed to connect to Postgres: {e}"))?;
            pg.init(config.storage.postgres.migrate)
                .await
                .map_err(|e| format!("Failed to initialize Postgres schema: {e}"))?;
            tracing::info!("PostgreSQL initialized");
            Arc::new(pg)
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let url = config.storage.mysql.url.as_deref().unwrap();
            let pool_size = config.storage.mysql.pool_size;
            let mysql = MysqlEngine::connect(
                url,
                pool_size,
                config.tasks.retry_timeout,
                config.storage.mysql.preload_limit,
                config.debug,
            )
            .await
            .map_err(|e| format!("MySQL connection failed: {e}"))?;
            mysql
                .init(config.storage.mysql.migrate)
                .await
                .map_err(|e| format!("MySQL init failed: {e}"))?;
            Arc::new(mysql)
        }
        #[cfg(feature = "sqlite")]
        _ => {
            let path = &config.storage.sqlite.path;
            tracing::info!(path = %path, "Using SQLite backend");
            let sqlite = SqliteEngine::open(
                path,
                config.tasks.retry_timeout,
                config.storage.sqlite.preload_limit,
                config.storage.sqlite.migrate,
                config.debug,
            )
            .map_err(|e| format!("Failed to open SQLite database: {e}"))?;
            tracing::info!("SQLite initialized");
            Arc::new(sqlite)
        }
        // Without SQLite there is no catch-all engine, so an unrecognised or
        // uncompiled backend has to be refused rather than fallen back on.
        #[cfg(not(feature = "sqlite"))]
        other => {
            return Err(format!(
                "storage backend '{other}' is not compiled into this build"
            ))
        }
    };

    let port = config.server.port;
    let bind = config.server.bind.clone();
    let poll_max_connections = config.transports.http_poll.max_connections;
    let poll_buffer_size = config.transports.http_poll.buffer_size;
    let shutdown_timeout = std::time::Duration::from_millis(config.server.shutdown_timeout);
    let is_sqlite = config.storage.storage_type == "sqlite";
    let config_debug = config.debug;

    // Build transports
    tracing::info!(
        http_push_connect_timeout_ms = config.transports.http_push.connect_timeout,
        http_push_request_timeout_ms = config.transports.http_push.request_timeout,
        http_poll_max_connections = poll_max_connections,
        http_poll_buffer_size = poll_buffer_size,
        "Transport config"
    );

    // Built in order, not in a cycle. The router is the only thing that starts
    // incomplete, so nothing is ever handed a value that does not exist yet.
    //
    // 1. The router, empty.
    let dispatcher = Arc::new(router::Router::new());
    let router: Arc<dyn ResonateRouter> = dispatcher.clone();

    // 2. The server, handed that router. Nothing is connected or started yet.
    let state = resonate_sql::server::Server::new(
        engine,
        router,
        resonate_sql::server::Options {
            server_url: config.server.url.clone().unwrap_or_default(),
            wheel_capacity: config.timeouts.wheel_capacity,
            wheel_refresh: config.timeouts.wheel_refresh,
            sweep_interval: config.timeouts.poll_interval,
        },
    );

    // 3. The workers, each downgrading the server that now exists.
    let server_handle: std::sync::Weak<dyn ResonateServer> = Arc::downgrade(&state) as _;
    let poll_registry = Arc::new(PollRegistry::new(
        server_handle.clone(),
        config.transports.http_poll.clone(),
    ));

    // Scheme -> worker. A disabled transport is simply not registered, and
    // the router reports its addresses as undeliverable.
    let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();

    if config.transports.http_push.enabled {
        let worker: Arc<dyn ResonateWorker> =
            Arc::new(resonate_transport_http_push::HttpPushTransport::new(
                server_handle.clone(),
                config.transports.http_push.clone(),
            ));
        for scheme in resonate_transport_http_push::SCHEMES {
            workers.insert((*scheme).to_string(), Arc::clone(&worker));
        }
    } else {
        tracing::info!("HTTP push transport disabled");
    }

    if config.transports.http_poll.enabled {
        workers.insert(
            resonate_transport_http_poll::SCHEME.to_string(),
            poll_registry.clone(),
        );
    } else {
        tracing::info!("HTTP poll transport disabled");
    }

    if config.transports.gcps.enabled {
        workers.insert(
            resonate_transport_gcps::SCHEME.to_string(),
            Arc::new(resonate_transport_gcps::GcpsPubSubTransport::new(
                server_handle.clone(),
                config.transports.gcps.clone(),
            )),
        );
    }

    if config.transports.bash_exec.enabled {
        workers.insert(
            resonate_worker_bash::SCHEME.to_string(),
            Arc::new(resonate_worker_bash::BashExecTransport::new(
                server_handle.clone(),
                config.transports.bash_exec.clone(),
                config.tasks.lease_timeout,
            )),
        );
    }

    // 4. Install them. The router is complete from here and never changes again.
    dispatcher
        .install(workers)
        .expect("the router is built here and nowhere else");

    // Start every worker before anything can route to one. A worker that
    // cannot start is a startup failure, not a message that quietly goes
    // nowhere later. Nothing has routed yet: the listener is not up and the
    // background loops are not spawned.
    // 6. Start, in the order things were built. The server's timer and sweep
    //    are its own now: `init` seeds one and spawns the other.
    let debug = config_debug;
    dispatcher.init(debug).await.map_err(|e| e.to_string())?;
    state.init(debug).await.map_err(|e| e.to_string())?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut handles = Vec::new();

    let metrics_port = config.observability.metrics_port;
    if metrics_port > 0 {
        let metrics_shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            let metrics_app = Router::new().route("/metrics", get(metrics::metrics_handler));
            match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", metrics_port)).await {
                Ok(listener) => {
                    tracing::info!(port = metrics_port, "Metrics server listening");
                    let _ = axum::serve(listener, metrics_app)
                        .with_graceful_shutdown(async move {
                            let mut rx = metrics_shutdown;
                            let _ = rx.wait_for(|v| *v).await;
                        })
                        .await;
                }
                Err(e) => {
                    tracing::error!(port = metrics_port, error = %e, "Failed to bind metrics port");
                }
            }
        }));
    }

    // The gateway is built here and listens below. `new` binds nothing, so
    // construction order does not matter; `init` is what opens the socket, and
    // that has to come after the workers and the timer — accepting a request
    // the rest of the process cannot yet serve is worse than not accepting it.
    if !config.server.cors.allow_origins.is_empty() {
        tracing::info!(origins = ?config.server.cors.allow_origins, "CORS enabled");
    }
    // The console is routes, not a gateway: it binds nothing and has no
    // lifecycle of its own, so it is merged into the listener that already
    // exists rather than opening a second one. One port, one origin — which is
    // also why it needs no CORS.
    // Built during the gateway's `init`, which is when the auth key has been
    // read: the console applies the same policy as the worker endpoint, so it
    // cannot be a way around it.
    let console_config = config.console.clone();
    let console_server = Arc::clone(&state) as Arc<dyn ResonateServer>;
    let console_enabled = console_config.enabled;
    let build_console = move |auth| {
        console::routes(
            &console_config,
            console::ConsoleState {
                server: console_server,
                auth,
            },
        )
        .unwrap_or_default()
    };

    let mut gateway_impl = HttpGateway::new(
        Arc::clone(&state) as Arc<dyn ResonateServer>,
        poll_registry,
        GatewayConfig {
            bind: bind.clone(),
            port,
            url: config.server.url.clone(),
            cors_allow_origins: config.server.cors.allow_origins.clone(),
            // Carried through, not interpreted: the gateway reads the key in
            // `init`, and a bad path fails startup there.
            auth: config.auth.clone(),
            // SQLite lives in this process, so a panic mid-transaction can
            // leave state the next request would read.
            abort_on_panic: is_sqlite,
        },
    );
    if console_enabled {
        gateway_impl = gateway_impl.with_routes(build_console);
    }
    let gateway: Arc<dyn ResonateGateway> = Arc::new(gateway_impl);
    gateway
        .init(debug)
        .await
        .map_err(|e| format!("HTTP gateway failed to start: {e}"))?;

    shutdown_signal().await;

    // Shutdown, in the reverse of the order things became able to do work —
    // with one exception, at the end.
    tracing::info!("Shutting down, draining background tasks...");
    let _ = shutdown_tx.send(true);

    let drain = async {
        // The timer first: it is the only thing that can still hand the engine
        // work of its own, and stopping it means nothing new arrives while the
        // loops below drain.
        // The server first: its timer is the only thing that can still hand the
        // engine work of its own, and `stop` drains its sweep behind it.
        let _ = state.stop().await;
        for handle in handles {
            let _ = handle.await;
        }
        // Then the workers, through the router that holds them: the loops that
        // feed them have stopped, so this drains what is already in flight
        // rather than racing new deliveries. It is also what ends the poll
        // transport's SSE streams, by dropping the senders they read from.
        let _ = dispatcher.stop().await;
        // The gateway last, not first. Refusing connections while in-flight
        // work is still draining would give clients a closed socket where a
        // 503 would do — and stopping it first would deadlock, because its
        // graceful shutdown waits on the very SSE streams only the step above
        // can release.
        if let Err(e) = gateway.stop().await {
            tracing::warn!(error = %e, "HTTP gateway did not stop cleanly");
        }
    };

    if tokio::time::timeout(shutdown_timeout, drain).await.is_err() {
        tracing::warn!("Background tasks did not finish within shutdown timeout, forcing exit");
    }

    tracing::info!("Resonate Server stopped");
    Ok(())
}

/// Wait for SIGINT or SIGTERM to initiate graceful shutdown.
async fn shutdown_signal() {
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
