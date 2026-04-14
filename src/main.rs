mod auth;
mod cli;
mod config;

mod metrics;
mod persistence;
mod processing;
mod server;
mod transport;
mod types;
mod util;

use std::sync::Arc;

use axum::{routing::get, Router};
use clap::{Parser, Subcommand};
use config::Config;
use persistence::{persistence_sqlite::SqliteStorage, Storage};
use server::Server;
use transport::transport_http_poll::PollRegistry;

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
        message_poll_interval_ms = config.messages.poll_interval,
        message_batch_size = config.messages.batch_size,
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

    // Validate poll config (buffer_size=0 panics in tokio::mpsc::channel)
    if config.transports.http_poll.buffer_size == 0 {
        return Err("http_poll.buffer_size must be at least 1".into());
    }
    if config.transports.http_poll.max_connections == 0 {
        return Err("http_poll.max_connections must be at least 1".into());
    }

    // Load auth configuration
    let auth_config = match &config.auth {
        Some(auth_cfg) => {
            let key = if auth_cfg.publickey == "none" {
                tracing::warn!("Auth enabled — unsigned mode (no signature verification)");
                None
            } else {
                let vk = auth::load_public_key(&auth_cfg.publickey).map_err(|e| e.to_string())?;
                tracing::info!(key = %auth_cfg.publickey, "Auth enabled");
                Some(vk)
            };
            if let Some(iss) = &auth_cfg.iss {
                tracing::info!(issuer = %iss, "Auth issuer configured");
            }
            if let Some(aud) = &auth_cfg.aud {
                tracing::info!(audience = %aud, "Auth audience configured");
            }
            Some(auth::AuthConfig {
                key,
                iss: auth_cfg.iss.clone(),
                aud: auth_cfg.aud.clone(),
            })
        }
        None => {
            tracing::info!("Auth disabled — all requests accepted");
            None
        }
    };

    // Backend selection
    let storage = match config.storage.storage_type.as_str() {
        "postgres" => {
            let url = config.storage.postgres.url.as_ref().unwrap();
            let pool_size = config.storage.postgres.pool_size;
            tracing::info!("Using PostgreSQL backend");
            tracing::info!(pool_size = pool_size, "PostgreSQL pool configured");
            let pg = persistence::persistence_postgres::PostgresStorage::connect(
                url,
                pool_size,
                config.tasks.retry_timeout,
            )
            .await
            .map_err(|e| format!("Failed to connect to Postgres: {e}"))?;
            pg.init()
                .await
                .map_err(|e| format!("Failed to initialize Postgres schema: {e}"))?;
            tracing::info!("PostgreSQL initialized");
            Storage::Postgres(pg)
        }
        _ => {
            let path = &config.storage.sqlite.path;
            tracing::info!(path = %path, "Using SQLite backend");
            let sqlite = SqliteStorage::open(path, config.tasks.retry_timeout)
                .map_err(|e| format!("Failed to open SQLite database: {e}"))?;
            tracing::info!("SQLite initialized");
            Storage::Sqlite(sqlite)
        }
    };

    let port = config.server.port;
    let bind = config.server.bind.clone();
    let poll_max_connections = config.transports.http_poll.max_connections;
    let poll_buffer_size = config.transports.http_poll.buffer_size;
    let shutdown_timeout = std::time::Duration::from_millis(config.server.shutdown_timeout);
    let state = Arc::new(Server::new(config, auth_config, storage));

    // Build transports
    tracing::info!(
        http_push_connect_timeout_ms = state.config.transports.http_push.connect_timeout,
        http_push_request_timeout_ms = state.config.transports.http_push.request_timeout,
        http_poll_max_connections = poll_max_connections,
        http_poll_buffer_size = poll_buffer_size,
        "Transport config"
    );
    let poll_registry = Arc::new(PollRegistry::new(poll_max_connections, poll_buffer_size));
    let http_push = Arc::new(transport::transport_http_push::HttpPushTransport::new(
        std::time::Duration::from_millis(state.config.transports.http_push.connect_timeout),
        std::time::Duration::from_millis(state.config.transports.http_push.request_timeout),
    ));
    let gcps = match &state.config.transports.gcps {
        Some(_gcps_config) => {
            tracing::info!("GCP Pub/Sub transport enabled");
            Some(Arc::new(
                transport::transport_gcps::GcpsPubSubTransport::new(),
            ))
        }
        None => None,
    };
    let dispatcher = Arc::new(transport::TransportDispatcher::new(
        http_push,
        poll_registry.clone(),
        gcps,
    ));

    // Spawn background loops
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut handles = Vec::new();

    let timeout_state = Arc::clone(&state);
    let timeout_shutdown = shutdown_rx.clone();
    handles.push(tokio::spawn(async move {
        processing::processing_timeouts::timeout_processing_loop(timeout_state, timeout_shutdown)
            .await;
    }));

    let message_state = Arc::clone(&state);
    let message_shutdown = shutdown_rx.clone();
    let message_dispatcher = Arc::clone(&dispatcher);
    handles.push(tokio::spawn(async move {
        processing::processing_messages::message_processing_loop(
            message_state,
            message_dispatcher,
            message_shutdown,
        )
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

    // Build HTTP router
    let effective_url = state.config.server.url.clone().unwrap_or_default();
    let (sse_shutdown_tx, sse_shutdown_rx) = tokio::sync::watch::channel(false);
    let app_state = server::AppState {
        server: state,
        poll_registry,
        sse_shutdown_rx,
    };
    let app = server::api_routes()
        .merge(server::poll_routes())
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .make_span_with(
                    tower_http::trace::DefaultMakeSpan::new().level(tracing::Level::INFO),
                )
                .on_response(
                    tower_http::trace::DefaultOnResponse::new().level(tracing::Level::INFO),
                )
                .on_failure(
                    tower_http::trace::DefaultOnFailure::new().level(tracing::Level::ERROR),
                ),
        )
        .with_state(app_state);
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", bind, port))
        .await
        .map_err(|e| format!("Failed to bind to {}:{}: {e}", bind, port))?;

    tracing::info!(bind = %bind, port = port, server_url = %effective_url, "Server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = sse_shutdown_tx.send(true);
        })
        .await
        .map_err(|e| format!("Server error: {e}"))?;

    // Shutdown
    tracing::info!("HTTP server stopped, draining background tasks...");
    let _ = shutdown_tx.send(true);

    let drain = async {
        for handle in handles {
            let _ = handle.await;
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
