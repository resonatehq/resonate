//! The Resonate server, as this repository ships it.
//!
//! Everything that makes it *this* server is in [`registry`]: a list of plugins,
//! one line each. Everything else — reading the configuration, building in
//! order, starting, stopping — is `resonate-base`, which names no plugin at all.
//!
//! Which is what makes a custom build small. A binary that carries a different
//! set of plugins is this file with a different list, and nothing else:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> std::process::ExitCode {
//!     resonate_base::main(
//!         Registry::new()
//!             .server(&resonate_server_postgres::PLUGIN)
//!             .worker(&acme_worker_kafka::PLUGIN)
//!             .gateway(&resonate_gateway_http::PLUGIN),
//!         Options::default().default_server("server_postgres"),
//!     )
//!     .await
//! }
//! ```

mod serve;

use clap::{Parser, Subcommand};
use resonate_base::Registry;

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
    Serve(Box<serve::ServeArgs>),
    /// Start the Resonate server with in-memory storage (ephemeral, for development)
    Dev(Box<serve::DevArgs>),
    /// Promise operations
    #[command(alias = "promise")]
    Promises(resonate_cli::PromiseArgs),
    /// Task operations
    #[command(alias = "task")]
    Tasks(resonate_cli::TaskArgs),
    /// Schedule operations
    #[command(alias = "schedule")]
    Schedules(resonate_cli::ScheduleArgs),
    /// Invoke a function via a durable promise
    Invoke(resonate_cli::InvokeArgs),
    /// Display the call-graph tree rooted at a promise ID
    Tree(resonate_cli::TreeArgs),
    /// Start the Resonate MCP server (stdio transport)
    Mcp(Box<resonate_cli::McpArgs>),
}

/// Everything this binary carries.
///
/// The one place plugins are named, and the only thing that changes when the
/// set changes: a dependency in Cargo.toml, and a line here. Server, worker,
/// gateway — the order they are built in.
fn registry() -> Registry {
    Registry::new()
        .server(&resonate_server_sqlite::PLUGIN)
        .server(&resonate_server_postgres::PLUGIN)
        .server(&resonate_server_mysql::PLUGIN)
        .server(&resonate_server_scylladb::PLUGIN)
        .worker(&resonate_transport_http_push::PLUGIN)
        .worker(&resonate_transport_http_poll::PLUGIN)
        .worker(&resonate_transport_gcps::PLUGIN)
        .worker(&resonate_worker_bash::PLUGIN)
        .gateway(&resonate_gateway_http::PLUGIN)
        .gateway(&resonate_gateway_web::PLUGIN)
        .gateway(&resonate_gateway_metrics::PLUGIN)
}

/// What `servers.active` falls back to.
const DEFAULT_SERVER: &str = "server_sqlite";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Promises(args) => {
            resonate_cli::run_promises(args).await;
        }
        Commands::Tasks(args) => {
            resonate_cli::run_tasks(args).await;
        }
        Commands::Schedules(args) => {
            resonate_cli::run_schedules(args).await;
        }
        Commands::Invoke(args) => {
            resonate_cli::run_invoke(args).await;
        }
        Commands::Tree(args) => {
            resonate_cli::run_tree(args).await;
        }
        Commands::Mcp(args) => {
            resonate_cli::run_mcp(args).await;
        }
        Commands::Serve(args) => {
            let registry = registry();
            let options = args
                .options(&server_ids(&registry))
                .default_server(DEFAULT_SERVER);
            return resonate_base::main(registry, options).await;
        }
        Commands::Dev(args) => {
            let registry = registry();
            let options = args
                .options(&server_ids(&registry))
                .default_server(DEFAULT_SERVER);
            return resonate_base::main(registry, options).await;
        }
    }
    std::process::ExitCode::SUCCESS
}

/// The server plugins this binary carries, for the flags that are about *the*
/// server rather than about one of them by name.
fn server_ids(registry: &Registry) -> Vec<String> {
    registry.servers().iter().map(|p| p.id()).collect()
}
