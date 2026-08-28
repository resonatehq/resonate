mod cli;
mod config;
mod deadlines;
mod mcp;
mod metrics;
mod processing;
mod server;
mod transport;

use std::sync::Arc;

use axum::{routing::get, Router};
use clap::{Parser, Subcommand};
use config::Config;
use resonate_core::{ResonateGateway, ResonateRouter, ResonateServer, ResonateWorker};
use resonate_gateway_http::{Config as GatewayConfig, HttpGateway};
use resonate_server_dbms::{
    engine_mysql::MysqlEngine, engine_port::ResonateEngine, engine_postgres::PostgresEngine,
    engine_sqlite::SqliteEngine,
};
use resonate_transport_http_poll::PollRegistry;
use server::Server;
use std::collections::HashMap;

/// The transports, handed back out of the wiring closure.
///
/// The server owns the router and the router owns the workers, but two things
/// are still needed here: the poll registry, which the HTTP layer serves
/// directly, and the workers by scheme, which have to be started before the
/// listener comes up and stopped after it goes down.
struct Transports {
    poll_registry: Arc<PollRegistry>,
    by_scheme: Vec<(String, Arc<dyn ResonateWorker>)>,
}

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
        tracing::info!("Debug mode enabled — debug operations allowed, background loops paused");
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
    let engine: Arc<dyn ResonateEngine> = match config.storage.storage_type.as_str() {
        "postgres" => {
            let url = config.storage.postgres.url.as_ref().unwrap();
            let pool_size = config.storage.postgres.pool_size;
            tracing::info!("Using PostgreSQL backend");
            tracing::info!(pool_size = pool_size, "PostgreSQL pool configured");
            let pg =
                PostgresEngine::connect(url, pool_size, config.tasks.retry_timeout, config.debug)
                    .await
                    .map_err(|e| format!("Failed to connect to Postgres: {e}"))?;
            pg.init()
                .await
                .map_err(|e| format!("Failed to initialize Postgres schema: {e}"))?;
            tracing::info!("PostgreSQL initialized");
            Arc::new(pg)
        }
        "mysql" => {
            let url = config.storage.mysql.url.as_deref().unwrap();
            let pool_size = config.storage.mysql.pool_size;
            let mysql =
                MysqlEngine::connect(url, pool_size, config.tasks.retry_timeout, config.debug)
                    .await
                    .map_err(|e| format!("MySQL connection failed: {e}"))?;
            mysql
                .init()
                .await
                .map_err(|e| format!("MySQL init failed: {e}"))?;
            Arc::new(mysql)
        }
        _ => {
            let path = &config.storage.sqlite.path;
            tracing::info!(path = %path, "Using SQLite backend");
            let sqlite = SqliteEngine::open(path, config.tasks.retry_timeout, config.debug)
                .map_err(|e| format!("Failed to open SQLite database: {e}"))?;
            tracing::info!("SQLite initialized");
            Arc::new(sqlite)
        }
    };

    let port = config.server.port;
    let bind = config.server.bind.clone();
    let poll_max_connections = config.transports.http_poll.max_connections;
    let poll_buffer_size = config.transports.http_poll.buffer_size;
    let shutdown_timeout = std::time::Duration::from_millis(config.server.shutdown_timeout);
    let is_sqlite = config.storage.storage_type == "sqlite";

    // Build transports
    tracing::info!(
        http_push_connect_timeout_ms = config.transports.http_push.connect_timeout,
        http_push_request_timeout_ms = config.transports.http_push.request_timeout,
        http_poll_max_connections = poll_max_connections,
        http_poll_buffer_size = poll_buffer_size,
        "Transport config"
    );

    // What the closure below builds but the server does not own: the poll
    // registry, which the HTTP layer also needs, and the workers by scheme,
    // which have to be started and later stopped.
    let mut transports: Option<Transports> = None;

    // The ring is closed here, in one expression: the server holds the router,
    // the router holds the workers, and every worker holds the server.
    //
    // Weak, because that last link points back up the ownership chain — this
    // owns the server, and a strong handle would mean nothing in the ring,
    // server or storage or background task, was ever dropped. An in-process
    // worker calls the server directly through it; a remote worker uses it to
    // report a delivery failure rather than dropping the message.
    //
    // `new_cyclic` hands out that weak handle before the server exists, which
    // is what lets the router be a constructor argument instead of something
    // set afterwards. No worker upgrades it during construction — they only
    // store it — so it is still dangling here and live by the time anything
    // routes.
    let state = Arc::new_cyclic(|weak: &std::sync::Weak<Server>| {
        let server_handle: std::sync::Weak<dyn ResonateServer> = weak.clone();

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

        transports = Some(Transports {
            poll_registry,
            by_scheme: workers
                .iter()
                .map(|(scheme, worker)| (scheme.clone(), Arc::clone(worker)))
                .collect(),
        });

        let router: Arc<dyn ResonateRouter> =
            Arc::new(transport::TransportDispatcher::new(workers));
        // The timer's callbacks point back at the server too, so it is built
        // from the same weak handle and in the same expression. Nothing runs
        // until `start_timer` below.
        let timer = deadlines::build(&config.timeouts, weak.clone());
        Server::new(config, engine, router, timer)
    });

    let Transports {
        poll_registry,
        by_scheme: started,
    } = transports.expect("the closure above always sets it");

    // Start every worker before anything can route to one. A worker that
    // cannot start is a startup failure, not a message that quietly goes
    // nowhere later. Nothing has routed yet: the listener is not up and the
    // background loops are not spawned.
    for (scheme, worker) in &started {
        worker
            .init()
            .await
            .map_err(|e| format!("transport '{scheme}' failed to start: {e}"))?;
    }

    // Seed the timer from the durable deadlines before anything can arm one.
    // `start_timer` returns only once that first read has landed, so a deadline
    // that is already due fires immediately rather than waiting for a sweep.
    state.start_timer().await;
    tracing::info!(
        wheel_capacity = state.config.timeouts.wheel_capacity,
        wheel_refresh_ms = state.config.timeouts.wheel_refresh,
        sweep_interval_ms = state.config.timeouts.poll_interval,
        "Timer started"
    );

    // Spawn background loops
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut handles = Vec::new();

    let timeout_state = Arc::clone(&state);
    let timeout_shutdown = shutdown_rx.clone();
    handles.push(tokio::spawn(async move {
        processing::processing_timeouts::timeout_processing_loop(timeout_state, timeout_shutdown)
            .await;
    }));

    let metrics_port = state.config.observability.metrics_port;
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
    if !state.config.server.cors.allow_origins.is_empty() {
        tracing::info!(origins = ?state.config.server.cors.allow_origins, "CORS enabled");
    }
    let gateway: Arc<dyn ResonateGateway> = Arc::new(HttpGateway::new(
        Arc::clone(&state) as Arc<dyn ResonateServer>,
        poll_registry,
        GatewayConfig {
            bind: bind.clone(),
            port,
            url: state.config.server.url.clone(),
            cors_allow_origins: state.config.server.cors.allow_origins.clone(),
            // Carried through, not interpreted: the gateway reads the key in
            // `init`, and a bad path fails startup there.
            auth: state.config.auth.clone(),
            // SQLite lives in this process, so a panic mid-transaction can
            // leave state the next request would read.
            abort_on_panic: is_sqlite,
        },
    ));
    gateway
        .init()
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
        state.stop_timer().await;
        for handle in handles {
            let _ = handle.await;
        }
        // Then the workers: the loops that feed them have stopped, so this
        // drains what is already in flight rather than racing new deliveries.
        // This is also what ends the poll transport's SSE streams, by dropping
        // the senders they read from.
        for (_, worker) in started {
            if let Err(e) = worker.stop().await {
                tracing::warn!(error = %e, "transport did not stop cleanly");
            }
        }
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
