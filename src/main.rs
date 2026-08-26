mod auth;
mod cli;
mod config;
mod core;
// The binary re-declares the crate's modules rather than depending on the
// library, so its dead-code analysis sees everything the *binary* does not
// reach as unused — which for these two is most of their surface, since they
// exist to be driven by the library's tests and by the differential suite.
#[allow(dead_code, unused_imports)]
mod kernel;
mod mcp;
mod metrics;
mod persistence;
mod processing;
#[allow(dead_code, unused_imports)]
mod s3;
mod server;
mod transport;
mod util;

use std::sync::Arc;

use crate::core::types::ResponseEnvelope;
use axum::{
    http::{
        header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, ORIGIN},
        HeaderValue, Method, StatusCode,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::{Parser, Subcommand};
use config::Config;
use core::{ResonateRouter, ResonateServer, ResonateWorker};
use persistence::{persistence_mysql::MysqlStorage, persistence_sqlite::SqliteStorage, Storage};
use server::Server;
use std::collections::HashMap;
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

/// The active backend. Only the inbound port is common to both arms; the SQL
/// one also needs its concrete `Server` for the two background loops that read
/// storage directly.
enum Backend {
    Sql(Arc<Server>),
    S3(Arc<s3::server::S3Server>),
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
            Some(Arc::new(auth::AuthConfig {
                key,
                iss: auth_cfg.iss.clone(),
                aud: auth_cfg.aud.clone(),
            }))
        }
        None => {
            tracing::info!("Auth disabled — all requests accepted");
            None
        }
    };

    let port = config.server.port;
    let bind = config.server.bind.clone();
    let poll_max_connections = config.transports.http_poll.max_connections;
    let poll_buffer_size = config.transports.http_poll.buffer_size;
    let shutdown_timeout = std::time::Duration::from_millis(config.server.shutdown_timeout);
    // Only SQLite aborts on a panic in a handler: it is the one backend whose
    // in-process connection a poisoned handler could leave inconsistent.
    let is_sqlite = config.storage.storage_type == "sqlite";
    let late_router = Arc::new(s3::outbox::LateRouter::new());

    // Backend selection.
    //
    // The SQL backends share one `Server` over a `Db`; S3 is a fourth
    // implementation of the inbound port with no `Db` under it at all, so the
    // two arms produce different types and only the port is common.
    let backend = match config.storage.storage_type.as_str() {
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
            Backend::Sql(Arc::new(Server::new(config.clone(), Storage::Postgres(pg))))
        }
        "mysql" => {
            let url = config.storage.mysql.url.as_deref().unwrap();
            let pool_size = config.storage.mysql.pool_size;
            let mysql = MysqlStorage::connect(url, pool_size, config.tasks.retry_timeout)
                .await
                .map_err(|e| format!("MySQL connection failed: {e}"))?;
            mysql
                .init()
                .await
                .map_err(|e| format!("MySQL init failed: {e}"))?;
            Backend::Sql(Arc::new(Server::new(config.clone(), Storage::Mysql(mysql))))
        }
        "s3" => {
            let s3 = &config.storage.s3;
            let bucket = s3.bucket.as_deref().expect("validated at load");
            tracing::info!(
                bucket = bucket,
                prefix = %s3.prefix,
                timer_shards = s3.timer_shards,
                "Using S3 backend"
            );
            tracing::warn!(
                "The S3 backend requires real conditional writes (If-Match / \
                 If-None-Match). S3, R2, GCS and Azure qualify; MinIO, B2 and \
                 Spaces do not and will silently lose writes."
            );
            let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
            if let Some(region) = &s3.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &s3.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if s3.allow_http {
                builder = builder.with_allow_http(true);
            }
            let store = builder
                .build()
                .map_err(|e| format!("Failed to configure S3 store: {e}"))?;
            let store: Arc<dyn s3::store::Store> =
                Arc::new(s3::store::ObjectStoreAdapter::new(store));
            // The router does not exist yet — its workers need a handle to the
            // server being built here — so the outbox gets a placeholder that is
            // filled in once the router is up, below.
            Backend::S3(s3::server::S3Server::build(
                store,
                Some(Arc::clone(&late_router) as Arc<dyn ResonateRouter>),
                s3::server::S3ServerCfg {
                    keys: s3::applier::KeySpace::new(s3.prefix.clone(), s3.timer_shards),
                    applier: s3::applier::ApplierCfg {
                        max_cas_retries: s3.max_cas_retries,
                        kernel: kernel::KernelCfg {
                            retry_timeout: config.tasks.retry_timeout,
                        },
                        ..Default::default()
                    },
                    timerd: s3::timerd::TimerdCfg::default(),
                    cache_capacity: s3.cache_capacity,
                    debug: config.debug,
                    search: s3.search_enabled,
                    server_url: config.server.url.clone().unwrap_or_default(),
                },
            ))
        }
        _ => {
            let path = &config.storage.sqlite.path;
            tracing::info!(path = %path, "Using SQLite backend");
            let sqlite = SqliteStorage::open(path, config.tasks.retry_timeout)
                .map_err(|e| format!("Failed to open SQLite database: {e}"))?;
            tracing::info!("SQLite initialized");
            Backend::Sql(Arc::new(Server::new(config.clone(), Storage::Sqlite(sqlite))))
        }
    };

    // Build transports
    tracing::info!(
        http_push_connect_timeout_ms = config.transports.http_push.connect_timeout,
        http_push_request_timeout_ms = config.transports.http_push.request_timeout,
        http_poll_max_connections = poll_max_connections,
        http_poll_buffer_size = poll_buffer_size,
        "Transport config"
    );
    // Every worker holds a handle to the server: an in-process worker calls it
    // directly, and a remote worker uses it to report a delivery failure rather
    // than dropping the message. `Server` never holds the router, so this stays
    // a DAG.
    let server: Arc<dyn ResonateServer> = match &backend {
        Backend::Sql(s) => Arc::clone(s) as Arc<dyn ResonateServer>,
        Backend::S3(s) => Arc::clone(s) as Arc<dyn ResonateServer>,
    };
    let ready: Arc<dyn server::ReadinessProbe> = match &backend {
        Backend::Sql(s) => Arc::clone(s) as Arc<dyn server::ReadinessProbe>,
        Backend::S3(s) => Arc::clone(s) as Arc<dyn server::ReadinessProbe>,
    };

    let poll_registry = Arc::new(PollRegistry::new(
        Arc::clone(&server),
        poll_max_connections,
        poll_buffer_size,
    ));
    let connect_timeout =
        std::time::Duration::from_millis(config.transports.http_push.connect_timeout);
    let request_timeout =
        std::time::Duration::from_millis(config.transports.http_push.request_timeout);

    // Scheme -> worker. A disabled transport is simply not registered, and the
    // router reports its addresses as undeliverable.
    let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();

    if config.transports.http_push.enabled {
        let outbound_auth = match &config.transports.http_push.auth {
            Some(auth_cfg) => {
                let mode_label = format!("{:?}", auth_cfg.mode);
                let auth = transport::transport_http_push::Auth::from_config(auth_cfg);
                tracing::info!(mode = %mode_label, "HTTP push outbound auth enabled");
                auth
            }
            None => {
                tracing::debug!("HTTP push outbound auth: none");
                transport::transport_http_push::Auth::None
            }
        };
        let worker: Arc<dyn ResonateWorker> =
            Arc::new(transport::transport_http_push::HttpPushTransport::new(
                Arc::clone(&server),
                connect_timeout,
                request_timeout,
                outbound_auth,
                config.transports.http_push.concurrency,
            ));
        workers.insert("http".to_string(), Arc::clone(&worker));
        workers.insert("https".to_string(), worker);
    } else {
        tracing::info!("HTTP push transport disabled");
    }

    if config.transports.http_poll.enabled {
        workers.insert("poll".to_string(), poll_registry.clone());
    } else {
        tracing::info!("HTTP poll transport disabled");
    }

    if config.transports.gcps.enabled {
        tracing::info!(
            concurrency = config.transports.gcps.concurrency,
            timeout_ms = config.transports.gcps.timeout,
            "GCP Pub/Sub transport enabled"
        );
        workers.insert(
            "gcps".to_string(),
            Arc::new(transport::transport_gcps::GcpsPubSubTransport::new(
                Arc::clone(&server),
                config.transports.gcps.concurrency,
                std::time::Duration::from_millis(config.transports.gcps.timeout),
            )),
        );
    }

    if config.transports.bash_exec.enabled {
        tracing::info!("Bash exec transport enabled (local + docker + tensorlake)");
        workers.insert(
            "bash".to_string(),
            Arc::new(transport::transport_exec_bash::BashExecTransport::new(
                Arc::clone(&server),
                config
                    .transports
                    .bash_exec
                    .resolve_lease_timeout(&config.tasks),
            )),
        );
    }

    let router: Arc<dyn ResonateRouter> = Arc::new(transport::TransportDispatcher::new(workers));
    // Close the knot: the outbox has been holding a placeholder since the
    // backend was built, and nothing has been delivered through it yet.
    late_router.bind(Arc::clone(&router));

    // Spawn background loops
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut handles = Vec::new();

    match &backend {
        Backend::Sql(state) => {
            let timeout_state = Arc::clone(state);
            let timeout_shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                processing::processing_timeouts::timeout_processing_loop(
                    timeout_state,
                    timeout_shutdown,
                )
                .await;
            }));

            let message_state = Arc::clone(state);
            let message_shutdown = shutdown_rx.clone();
            let message_router = Arc::clone(&router);
            handles.push(tokio::spawn(async move {
                processing::processing_messages::message_processing_loop(
                    message_state,
                    message_router,
                    message_shutdown,
                )
                .await;
            }));
        }
        Backend::S3(s3_server) => {
            // One loop, not two: the timer loop fires armed deadlines from
            // memory (listing the store only to seed itself at startup), and
            // the outbox delivers as it is written rather than being drained
            // by a second loop.
            handles.push(Arc::clone(s3_server.timerd()).spawn(
                s3_server.debug_mode(),
                shutdown_rx.clone(),
            ));
        }
    }

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

    // Build HTTP router
    let effective_url = config.server.url.clone().unwrap_or_default();
    let (sse_shutdown_tx, sse_shutdown_rx) = tokio::sync::watch::channel(false);

    let cors_layer = build_cors_layer(&config.server.cors.allow_origins);
    if !config.server.cors.allow_origins.is_empty() {
        tracing::info!(
            origins = ?config.server.cors.allow_origins,
            "CORS enabled"
        );
    }

    let app_state = server::AppState {
        server: Arc::clone(&server),
        ready,
        auth: auth_config,
        poll_registry,
        sse_shutdown_rx,
    };
    let mut app = server::api_routes()
        .merge(server::poll_routes())
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(
            move |err: Box<dyn std::any::Any + Send + 'static>| {
                let message = if let Some(s) = err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "internal server error".to_string()
                };
                tracing::error!(message = %message, "panic in request handler");
                if is_sqlite {
                    std::process::abort();
                }
                let body =
                    ResponseEnvelope::error("unknown".to_string(), "0".to_string(), 500, &message);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
            },
        ))
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
    if let Some(layer) = cors_layer {
        app = app.layer(layer);
    }
    let app = app;
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

fn build_cors_layer(allow_origins: &[String]) -> Option<tower_http::cors::CorsLayer> {
    if allow_origins.is_empty() {
        return None;
    }
    let layer = if allow_origins.iter().any(|o| o == "*") {
        tower_http::cors::CorsLayer::permissive()
    } else {
        let origins: Vec<HeaderValue> = allow_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        tower_http::cors::CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers([ORIGIN, CONTENT_LENGTH, CONTENT_TYPE, AUTHORIZATION])
    };
    Some(layer)
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
