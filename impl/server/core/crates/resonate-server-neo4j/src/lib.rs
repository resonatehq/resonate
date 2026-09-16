//! Resonate's durable state, over Neo4j.
//!
//! One `:Promise` node per promise, with the task's columns on the same node,
//! and two relationship types beside it: `AWAITS`, which carries what the
//! relational engines keep in their `callbacks` and `resumes` arrays, and
//! `CHILD_OF`, the `resonate:parent` tag as an edge so a graph tool can draw
//! the call tree. See `schema.cypher`.
//!
//! The semantics are the Postgres engine's, transition for transition: lazy
//! expiry for internal promises, eager deadlines for targeted ones, the same
//! deadline projections, the same messages. Where Postgres writes one CTE this
//! engine reads the nodes a transition touches under Neo4j's write locks,
//! decides in Rust, and writes back — every operation is one Bolt transaction,
//! and the transaction is what makes a settle and its fan-out atomic.
//!
//! # The shell is a copy
//!
//! `engine.rs`, `server.rs`, `deadlines.rs`, `sweep.rs` and `errors.rs` are
//! byte-for-byte copies of `resonate-server-scylladb`'s, and `metrics.rs`
//! differs in the two metric names only. That is deliberate: a third copy
//! rather than a shared crate, so the copies can be diffed and every
//! divergence between the engine-backed servers read off:
//!
//! ```sh
//! for f in engine.rs server.rs deadlines.rs sweep.rs metrics.rs errors.rs; do
//!   diff crates/resonate-server-scylladb/src/$f crates/resonate-server-neo4j/src/$f
//! done
//! ```
//!
//! What is this crate's own is everything under `db.rs`, `ops_*.rs`,
//! `timeouts.rs` and `snap.rs`: the Cypher, and the Rust that decides between
//! reading it and writing it.
//!
//! # Locking
//!
//! Neo4j runs read-committed and takes a write lock on a node when a statement
//! modifies it. A `MATCH ... WHERE p.state = 'pending' SET ...` is therefore
//! not safe against a concurrent writer: the predicate is evaluated before the
//! lock. So every transition begins by *locking* the nodes it will decide on —
//! `SET p.rev = p.rev + 1 RETURN p.*` — which acquires the lock before the
//! read the `RETURN` performs, and holds it to commit. That is `SELECT ... FOR
//! UPDATE`, and it is what lets the Rust between the read and the write reason
//! about state nothing else is changing. Two transactions after the same nodes
//! in different orders deadlock; Neo4j detects it, the driver reports a
//! transient error, and the shell retries the whole transaction once before
//! answering 503 — exactly the Postgres engine's `40001` path.

mod db;
mod deadlines;
pub mod engine;
mod errors;
mod metrics;
mod ops_promise;
mod ops_schedule;
mod ops_task;
mod ops_ui;
mod server;
mod snap;
mod sweep;
mod timeouts;

pub use errors::{StorageError, StorageResult};

use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use neo4rs::{query, ConfigBuilder, Graph};
use serde::{Deserialize, Serialize};

use crate::db::{map_err, Tx};
use crate::engine::{Engine, Input, Outgoing, Output, Scheduled};
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};

/// The embedded schema, applied statement by statement when `migrate` asks.
const SCHEMA_CYPHER: &str = include_str!("../schema.cypher");

/// Everything under `[servers.server_neo4j]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Bolt URI: `bolt://host:7687`, `neo4j://host:7687`, or the `+s` /
    /// `+ssc` forms for TLS.
    #[serde(default = "default_uri")]
    pub uri: String,
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// The database to use. Neo4j's default is `neo4j`.
    #[serde(default = "default_database")]
    pub database: String,
    /// Connection pool size.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    /// Rows the driver pulls per round trip.
    #[serde(default = "default_fetch_size")]
    pub fetch_size: usize,
    /// Apply the embedded constraints and indexes on connect.
    ///
    /// Defaults to on, unlike the SQL servers' `migrate`: the schema here is
    /// `IF NOT EXISTS` constraints and indexes, so re-applying it is a no-op
    /// and there is no migration history to get ahead of.
    #[serde(default = "default_migrate")]
    pub migrate: bool,
    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,
    /// How long a pending task waits before it is redispatched (ms).
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: i64,
    /// The externally reachable URL, stamped into every emitted message.
    #[serde(default)]
    pub server_url: String,
    /// How many deadlines the in-memory timer holds.
    #[serde(default = "default_wheel_capacity")]
    pub wheel_capacity: usize,
    /// How often the timer re-reads the durable deadlines (ms).
    #[serde(default = "default_wheel_refresh")]
    pub wheel_refresh: u64,
    /// The backstop scan interval (ms).
    #[serde(default = "default_sweep_interval")]
    pub sweep_interval: u64,
}

fn default_uri() -> String {
    "bolt://localhost:7687".to_string()
}
fn default_user() -> String {
    "neo4j".to_string()
}
fn default_database() -> String {
    "neo4j".to_string()
}
fn default_pool_size() -> usize {
    16
}
fn default_fetch_size() -> usize {
    500
}
fn default_migrate() -> bool {
    true
}
fn default_preload_limit() -> u32 {
    10
}
fn default_retry_timeout() -> i64 {
    30_000
}
fn default_wheel_capacity() -> usize {
    8192
}
fn default_wheel_refresh() -> u64 {
    30_000
}
fn default_sweep_interval() -> u64 {
    60_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            uri: default_uri(),
            user: default_user(),
            password: String::new(),
            database: default_database(),
            pool_size: default_pool_size(),
            fetch_size: default_fetch_size(),
            migrate: default_migrate(),
            preload_limit: default_preload_limit(),
            retry_timeout: default_retry_timeout(),
            server_url: String::new(),
            wheel_capacity: default_wheel_capacity(),
            wheel_refresh: default_wheel_refresh(),
            sweep_interval: default_sweep_interval(),
        }
    }
}

/// The Neo4j engine: a connection pool and the two limits every transition
/// reads.
pub struct Neo4jEngine {
    pub(crate) graph: Graph,
    pub(crate) task_retry_timeout: i64,
    pub(crate) preload_limit: u32,
    /// Whether `debug.*` operations are permitted at all.
    pub(crate) debug: bool,
}

/// The future one transaction's body returns. Boxed, because the body is a
/// closure the retry loop may call twice.
pub(crate) type TxFuture<'a, T> = Pin<Box<dyn Future<Output = StorageResult<T>> + Send + 'a>>;

impl Neo4jEngine {
    /// Open the pool. Nothing is sent until `init` — the driver connects
    /// lazily, so a bad address is `init`'s to report.
    ///
    /// `debug` is the process-wide flag, which gates the `debug.*` operations.
    pub async fn connect(cfg: &Config, debug: bool) -> StorageResult<Self> {
        let config = ConfigBuilder::default()
            .uri(cfg.uri.as_str())
            .user(cfg.user.as_str())
            .password(cfg.password.as_str())
            .db(cfg.database.as_str())
            .max_connections(cfg.pool_size.max(1))
            .fetch_size(cfg.fetch_size.max(1))
            .build()
            .map_err(map_err)?;
        let graph = Graph::connect(config).await.map_err(map_err)?;
        Ok(Self {
            graph,
            task_retry_timeout: cfg.retry_timeout,
            preload_limit: cfg.preload_limit,
            debug,
        })
    }

    /// Reach the database, and when `migrate`, apply the schema.
    ///
    /// The ping comes first so a wrong URI or password is reported as what it
    /// is rather than as a failure to create an index.
    pub async fn init(&self, migrate: bool) -> StorageResult<()> {
        self.graph
            .run(query("RETURN 1"))
            .await
            .map_err(|e| StorageError::Backend(format!("neo4j connect: {e}")))?;
        if migrate {
            for stmt in schema_statements() {
                self.graph
                    .run(query(&stmt))
                    .await
                    .map_err(|e| StorageError::Backend(format!("apply schema: {e}: {stmt}")))?;
            }
        }
        Ok(())
    }

    /// One transaction: begin, run the body, commit; and once more from the
    /// top when a conditional write lost its race.
    ///
    /// A deadlock or a constraint violation means the transaction rolled back
    /// with nothing committed, and what the body emitted is dropped with it —
    /// the messages and deadlines come back only from an attempt that
    /// committed, which is the atomicity the port promises.
    ///
    /// Three retries where the SQL engines take one. A transaction here holds
    /// its node locks across several round trips rather than for one
    /// statement, so two workers on one call tree — a parent suspending on
    /// its children while a child fulfils and fans out to the parent — meet in
    /// a deadlock more often than they do on Postgres. Each loser rolls back
    /// and runs again; only after the last does the caller see a 503.
    pub(crate) async fn transact<'c, T, F>(
        &self,
        f: F,
    ) -> StorageResult<(T, Vec<Outgoing>, Vec<Scheduled>)>
    where
        F: for<'a> Fn(&'a mut Tx<'c>) -> TxFuture<'a, T>,
    {
        const MAX_RETRIES: u32 = 3;
        for attempt in 0..=MAX_RETRIES {
            let txn = self.graph.start_txn().await.map_err(map_err)?;
            let mut tx: Tx<'c> = Tx::new(txn, self.task_retry_timeout, self.preload_limit);
            let result = f(&mut tx).await;
            match result {
                Ok(value) => {
                    let (txn, emitted, armed) = tx.into_parts();
                    match txn.commit().await.map_err(map_err) {
                        Ok(()) => return Ok((value, emitted, armed)),
                        Err(StorageError::Serialization) if attempt < MAX_RETRIES => {
                            tracing::warn!(
                                attempt = attempt + 1,
                                "Transaction lost its race at commit, retrying"
                            );
                            backoff(attempt).await;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(StorageError::Serialization) => {
                    tx.abort().await;
                    if attempt < MAX_RETRIES {
                        tracing::warn!(
                            attempt = attempt + 1,
                            "Transaction lost its race, retrying"
                        );
                        backoff(attempt).await;
                        continue;
                    }
                    return Err(StorageError::Serialization);
                }
                Err(e) => {
                    tx.abort().await;
                    return Err(e);
                }
            }
        }
        unreachable!("transact loop completed without returning")
    }

    /// One operation: run it, and turn a storage failure into a response.
    ///
    /// Same tail for every operation, so it lives here once. `Serialization`
    /// maps to 503 — nothing committed, and the caller may retry.
    pub(crate) async fn run<'c, F>(&self, req: &RequestEnvelope, f: F) -> Output
    where
        F: for<'a> Fn(&'a mut Tx<'c>) -> TxFuture<'a, ResponseEnvelope>,
    {
        match self.transact(f).await {
            Ok((response, messages, timeouts)) => Output {
                response: Some(response),
                messages,
                timeouts,
            },
            Err(StorageError::InvalidInput(msg)) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Invalid request: {}", msg),
            )),
            Err(StorageError::Serialization) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                503,
                "Serialization failure, please retry",
            )),
            Err(e) => {
                tracing::error!(kind = %req.kind, error = %e, "Storage error");
                Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    500,
                    &format!("Internal error: {}", e),
                ))
            }
        }
    }

    /// A read. Nothing is emitted, so nothing comes back but the result.
    pub(crate) async fn query<'c, T, F>(&self, f: F) -> StorageResult<T>
    where
        F: for<'a> Fn(&'a mut Tx<'c>) -> TxFuture<'a, T>,
    {
        self.transact(f).await.map(|(v, _, _)| v)
    }

    pub async fn dispatch(&self, req: &RequestEnvelope, now: i64) -> Output {
        match req.kind.as_str() {
            "promise.get" => self.op_promise_get(req, now).await,
            "promise.create" => self.op_promise_create(req, now).await,
            "promise.settle" => self.op_promise_settle(req, now).await,
            "promise.register_callback" => self.op_promise_register_callback(req, now).await,
            "promise.register_listener" => self.op_promise_register_listener(req, now).await,
            "promise.search" => self.op_promise_search(req, now).await,

            "task.get" => self.op_task_get(req, now).await,
            "task.create" => self.op_task_create(req, now).await,
            "task.acquire" => self.op_task_acquire(req, now).await,
            "task.release" => self.op_task_release(req, now).await,
            "task.fulfill" => self.op_task_fulfill(req, now).await,
            "task.suspend" => self.op_task_suspend(req, now).await,
            "task.fence" => self.op_task_fence(req, now).await,
            "task.heartbeat" => self.op_task_heartbeat(req, now).await,
            "task.halt" => self.op_task_halt(req, now).await,
            "task.continue" => self.op_task_continue(req, now).await,
            "task.search" => self.op_task_search(req, now).await,

            "schedule.get" => self.op_schedule_get(req, now).await,
            "schedule.create" => self.op_schedule_create(req, now).await,
            "schedule.delete" => self.op_schedule_delete(req).await,
            "schedule.search" => self.op_schedule_search(req).await,

            "ui.executions.search" => self.op_ui_executions_search(req, now).await,
            "ui.execution.get" => self.op_ui_execution_get(req, now).await,
            "ui.schedules.search" => self.op_ui_schedules_search(req, now).await,

            "debug.reset" | "debug.snap" | "debug.tick" if !self.debug => {
                Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    403,
                    "Debug operations are disabled",
                ))
            }
            "debug.reset" => self.op_debug_reset(req).await,
            "debug.snap" => self.op_debug_snap(req, now).await,
            "debug.tick" => self.op_debug_tick(req).await,

            _ => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Unknown request kind: {}", req.kind),
            )),
        }
    }
}

/// A short, randomised pause before a retry.
///
/// Two transactions that deadlocked and both retry at once tend to deadlock
/// again; a few milliseconds of jitter, doubling per attempt, is what breaks
/// the tie. Bounded well below a request timeout — the point is to spread the
/// losers, not to wait for anything.
async fn backoff(attempt: u32) {
    let ceiling = 8u64 << attempt.min(4);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let millis = 1 + nanos % ceiling;
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
}

/// Split the embedded schema into statements: Neo4j runs one per query, and
/// schema statements cannot share a transaction with data.
fn schema_statements() -> Vec<String> {
    SCHEMA_CYPHER
        .split(';')
        .map(|raw| {
            raw.lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string()
        })
        .filter(|stmt| !stmt.is_empty())
        .collect()
}

#[async_trait]
impl Engine for Neo4jEngine {
    async fn process(&self, input: Input<'_>, now: i64) -> Output {
        match input {
            Input::External(req) => self.dispatch(req, now).await,
            Input::Internal(timeout) => self.fire(timeout, now).await,
        }
    }

    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        self.transact(|tx| Box::pin(timeouts::process_all_timeouts(tx, now)))
            .await
    }

    async fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        self.query(|tx| Box::pin(timeouts::upcoming(tx, limit)))
            .await
    }
}

// ─── The plugin ──────────────────────────────────────────────────────────────

use resonate_plugin::{ConfigError, ResonateServer, ServerDependencies, ServerPlugin, Settings};

/// This server, as a plugin. The one thing a binary names to run on Neo4j.
pub static PLUGIN: ServerPlugin = ServerPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[servers.server_neo4j]` and build the server. Nothing is opened
/// here — the pool is `init`'s, like every other port's resource.
fn configure(
    settings: &Settings<'_>,
    deps: ServerDependencies,
) -> Result<std::sync::Arc<dyn ResonateServer>, ConfigError> {
    let config: Config = settings.extract()?;
    if config.uri.is_empty() {
        return Err(settings.reject("uri", "a Bolt URI is required"));
    }
    if config.pool_size < 1 {
        return Err(settings.reject("pool_size", "must be at least 1"));
    }
    let options = server::Options {
        server_url: config.server_url.clone(),
        wheel_capacity: config.wheel_capacity,
        wheel_refresh: config.wheel_refresh,
        sweep_interval: config.sweep_interval,
    };
    let open = config.clone();
    Ok(server::Server::new(
        Box::new(move |debug| {
            Box::pin(async move {
                let engine = Neo4jEngine::connect(&open, debug).await.map_err(|e| {
                    resonate_plugin::Unavailable::new(format!("cannot connect: {e}"))
                })?;
                engine
                    .init(open.migrate)
                    .await
                    .map_err(|e| resonate_plugin::Unavailable::new(format!("schema: {e}")))?;
                Ok(std::sync::Arc::new(engine) as std::sync::Arc<dyn engine::Engine>)
            })
        }),
        deps.router,
        options,
    ))
}
