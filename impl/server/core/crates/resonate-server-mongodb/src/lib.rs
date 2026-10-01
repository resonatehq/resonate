//! Resonate's durable state, over MongoDB.
//!
//! One document per promise in `promises`, with the task's fields in the same
//! document, and one per schedule in `schedules`. What the relational engines
//! keep in their `callbacks` and `resumes` arrays is kept in arrays here too:
//! `callbacks` on the awaited promise, `resumes` on the awaiting task. See
//! [`indexes`] for the schema, which is nothing but indexes.
//!
//! The semantics are the Postgres engine's, transition for transition: lazy
//! expiry for internal promises, eager deadlines for external ones, the same
//! deadline projections, the same messages. Where Postgres writes one CTE this
//! engine reads the documents a transition touches, decides in Rust, and
//! writes back — every operation is one multi-document transaction, and the
//! transaction is what makes a settle and its fan-out atomic. Transactions
//! need a replica set (one member is enough) or a sharded cluster.
//!
//! # The shell is a copy
//!
//! `engine.rs`, `server.rs`, `deadlines.rs`, `sweep.rs` and `errors.rs` are
//! byte-for-byte copies of `resonate-server-scylladb`'s, and `metrics.rs`
//! differs in the two metric names only — the same deliberate copy the Neo4j
//! server makes, so the copies can be diffed:
//!
//! ```sh
//! for f in engine.rs server.rs deadlines.rs sweep.rs metrics.rs errors.rs; do
//!   diff crates/resonate-server-scylladb/src/$f crates/resonate-server-mongodb/src/$f
//! done
//! ```
//!
//! What is this crate's own is everything under `db.rs`, `ops_*.rs`,
//! `timeouts.rs` and `snap.rs`: the filters and updates, and the Rust that
//! decides between reading and writing.
//!
//! # Locking
//!
//! A MongoDB transaction reads a snapshot and takes no read locks, so a
//! `find` followed by an `update` is not safe against a concurrent writer: the
//! decision would be made on a read nothing protects. So every transition
//! begins by *writing* the documents it will decide on — `$inc: {rev: 1}`,
//! returning the document after — and from then on any other transaction
//! that writes them fails with a write conflict. That is `SELECT ... FOR
//! UPDATE`, except that the loser fails at once instead of waiting: it is
//! aborted with a `TransientTransactionError`, and the shell retries the
//! whole transaction with jittered backoff before answering 503 — the
//! Postgres engine's `40001` path, taken more often. Because nobody waits,
//! nobody deadlocks, and lock order does not matter.
//!
//! The arrays are what make this sound for the fan-out: linking an awaiter
//! writes the awaited promise's document, so a link and a concurrent settle of
//! the same promise conflict, and one of them runs again on the other's
//! result.
//!
//! # Sharding
//!
//! With `shard = true`, `promises` is sharded by hashed `origin`, so a call
//! tree lives on one shard and a transition within it is a single-shard
//! transaction. Every per-document filter names the origin beside the id
//! (`db::key`) so `mongos` routes it to that shard, and a task keeps an
//! `awaiting` array — the reverse of `callbacks` — so fulfilling it writes to
//! the promises it awaited and no others. See the README.

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
use mongodb::bson::{doc, Document};
use mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT;
use mongodb::options::{ClientOptions, IndexOptions, ReadConcern, WriteConcern};
use mongodb::{Client, Collection, IndexModel};
use serde::{Deserialize, Serialize};

use crate::db::{map_err, Tx};
use crate::engine::{Engine, Input, Outgoing, Output, Scheduled};
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};

/// Everything under `[servers.server_mongodb]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Connection string: `mongodb://host:27017/?replicaSet=rs0`, or
    /// `mongodb+srv://...`. Credentials and TLS go here too.
    #[serde(default = "default_uri")]
    pub uri: String,
    /// The database holding the `promises` and `schedules` collections.
    /// Unset, it is the one the connection string names
    /// (`mongodb://host/<database>?...`), and failing that `resonate`.
    #[serde(default)]
    pub database: Option<String>,
    /// Connection pool size.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    /// Create the indexes on connect.
    ///
    /// Defaults to on, like Neo4j's: creating an index that already exists
    /// with the same definition is a no-op, and there is no migration history
    /// to get ahead of.
    #[serde(default = "default_migrate")]
    pub migrate: bool,
    /// Shard `promises` by hashed `origin` on connect (with `migrate`).
    ///
    /// Needs a sharded cluster: the URI must name `mongos` routers. Every
    /// promise of one call tree then lives on one shard, so a transition
    /// within a tree — a settle and its fan-out, a suspend on its children —
    /// is a single-shard transaction, and only an await across trees costs a
    /// two-phase commit. Hashed, because root ids that grow with time would
    /// otherwise all land on the last range. `schedules` stays unsharded.
    #[serde(default)]
    pub shard: bool,
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
    /// The backstop scan interval (ms). Ten minutes by default: the timer
    /// fires every deadline it holds as it comes due, one transaction each,
    /// and its backfill re-reads the nearest ones every `wheel_refresh`, so
    /// the sweep only catches what overflowed the wheel. Each run is a scan
    /// of every deadline queue, on every shard when sharded, so it should run
    /// rarely.
    #[serde(default = "default_sweep_interval")]
    pub sweep_interval: u64,
}

fn default_uri() -> String {
    "mongodb://localhost:27017/?replicaSet=rs0".to_string()
}
fn default_pool_size() -> u32 {
    16
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
    600_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            uri: default_uri(),
            database: None,
            pool_size: default_pool_size(),
            migrate: default_migrate(),
            shard: false,
            preload_limit: default_preload_limit(),
            retry_timeout: default_retry_timeout(),
            server_url: String::new(),
            wheel_capacity: default_wheel_capacity(),
            wheel_refresh: default_wheel_refresh(),
            sweep_interval: default_sweep_interval(),
        }
    }
}

/// The MongoDB engine: a client and the two limits every transition reads.
pub struct MongoDbEngine {
    pub(crate) client: Client,
    pub(crate) promises: Collection<Document>,
    pub(crate) schedules: Collection<Document>,
    pub(crate) task_retry_timeout: i64,
    pub(crate) preload_limit: u32,
    /// Whether `init` shards `promises`.
    pub(crate) shard: bool,
    /// Whether `debug.*` operations are permitted at all.
    pub(crate) debug: bool,
}

/// The future one transaction's body returns. Boxed, because the body is a
/// closure the retry loop may call more than once.
pub(crate) type TxFuture<'a, T> = Pin<Box<dyn Future<Output = StorageResult<T>> + Send + 'a>>;

/// How many times a transaction that lost a race runs again before the caller
/// sees a 503.
///
/// Many more than Neo4j's three. A write conflict here fails the loser at
/// once rather than making it wait for the winner to commit, so under
/// contention — a parent suspending on its children while a child fulfils
/// and fans out to the parent — a transaction can lose several times in a row
/// before the documents it wants are quiet. Each loss costs a few
/// milliseconds of backoff, not a lock wait.
const MAX_RETRIES: u32 = 12;

/// How many times a commit whose outcome is unknown is retried. Committing is
/// idempotent per session; running the body again is not.
const MAX_COMMIT_RETRIES: u32 = 3;

impl MongoDbEngine {
    /// Build the client. Nothing is sent until `init` — the driver connects
    /// lazily, so a bad address is `init`'s to report.
    ///
    /// `debug` is the process-wide flag, which gates the `debug.*` operations.
    pub async fn connect(cfg: &Config, debug: bool) -> StorageResult<Self> {
        let mut options = ClientOptions::parse(cfg.uri.as_str())
            .await
            .map_err(|e| StorageError::Backend(format!("mongodb uri: {e}")))?;
        options.max_pool_size = Some(cfg.pool_size.max(1));
        if options.app_name.is_none() {
            options.app_name = Some("resonate".to_string());
        }
        let database = cfg
            .database
            .clone()
            .or_else(|| options.default_database.clone())
            .unwrap_or_else(|| "resonate".to_string());
        let client = Client::with_options(options).map_err(map_err)?;
        let db = client.database(&database);
        Ok(Self {
            promises: db.collection("promises"),
            schedules: db.collection("schedules"),
            client,
            task_retry_timeout: cfg.retry_timeout,
            preload_limit: cfg.preload_limit,
            shard: cfg.shard,
            debug,
        })
    }

    /// Reach the database, check that it can run transactions, and when
    /// `migrate`, create the indexes.
    ///
    /// The ping comes first so a wrong URI or password is reported as what it
    /// is rather than as a failure to create an index; the topology check
    /// second, so a standalone `mongod` is refused here, in words, rather
    /// than on the first request.
    pub async fn init(&self, migrate: bool) -> StorageResult<()> {
        let admin = self.client.database("admin");
        let hello = admin
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(|e| StorageError::Backend(format!("mongodb connect: {e}")))?;
        let sharded = hello.get_str("msg").is_ok_and(|m| m == "isdbgrid");
        let replicated = sharded || hello.get_str("setName").is_ok();
        if self.shard && !sharded {
            return Err(StorageError::Backend(
                "mongodb: shard = true needs a sharded cluster; the URI must name mongos"
                    .to_string(),
            ));
        }
        if !replicated {
            return Err(StorageError::Backend(
                "mongodb: transactions need a replica set or a sharded cluster; \
                 start mongod with --replSet (one member is enough) and run rs.initiate()"
                    .to_string(),
            ));
        }
        if migrate {
            for (coll, index) in indexes(&self.promises, &self.schedules) {
                let name = index
                    .options
                    .as_ref()
                    .and_then(|o| o.name.clone())
                    .unwrap_or_default();
                coll.create_index(index)
                    .await
                    .map_err(|e| StorageError::Backend(format!("create index {name}: {e}")))?;
            }
            if self.shard {
                self.shard_promises(&admin).await?;
            }
        }
        Ok(())
    }

    /// Shard `promises` by hashed `origin`. Idempotent: sharding a collection
    /// again with the same key is a no-op, and a different key is an error
    /// worth stopping for.
    ///
    /// The hashed index is created first, so this works on a collection that
    /// already holds documents as well as on an empty one.
    async fn shard_promises(&self, admin: &mongodb::Database) -> StorageResult<()> {
        let key = doc! { "origin": "hashed" };
        self.promises
            .create_index(
                IndexModel::builder()
                    .keys(key.clone())
                    .options(
                        IndexOptions::builder()
                            .name("resonate_promise_shard".to_string())
                            .build(),
                    )
                    .build(),
            )
            .await
            .map_err(|e| StorageError::Backend(format!("create shard key index: {e}")))?;
        let ns = self.promises.namespace().to_string();
        admin
            .run_command(doc! { "shardCollection": ns.as_str(), "key": key })
            .await
            .map_err(|e| StorageError::Backend(format!("shard {ns}: {e}")))?;
        tracing::info!(collection = %ns, "Promises sharded by hashed origin");
        Ok(())
    }

    /// One transaction: begin, run the body, commit; and again from the top
    /// when a write lost its race.
    ///
    /// A write conflict or a duplicate key means the transaction was aborted
    /// with nothing committed, and what the body emitted is dropped with it —
    /// the messages and deadlines come back only from an attempt that
    /// committed, which is the atomicity the port promises.
    ///
    /// Snapshot reads and majority writes: the body decides on one consistent
    /// view, and what it commits survives a failover.
    pub(crate) async fn transact<'c, T, F>(
        &self,
        f: F,
    ) -> StorageResult<(T, Vec<Outgoing>, Vec<Scheduled>)>
    where
        F: for<'a> Fn(&'a mut Tx<'c>) -> TxFuture<'a, T>,
    {
        let mut session = self.client.start_session().await.map_err(map_err)?;
        for attempt in 0..=MAX_RETRIES {
            session
                .start_transaction()
                .read_concern(ReadConcern::snapshot())
                .write_concern(WriteConcern::majority())
                .await
                .map_err(map_err)?;
            let mut tx: Tx<'c> = Tx::new(
                session,
                self.promises.clone(),
                self.schedules.clone(),
                self.task_retry_timeout,
                self.preload_limit,
            );
            let result = f(&mut tx).await;
            match result {
                Ok(value) => {
                    let (s, emitted, armed) = tx.into_parts();
                    session = s;
                    match commit(&mut session).await {
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
                Err(e) => {
                    session = tx.into_session();
                    // Best effort: after a failed statement the server has
                    // usually aborted the transaction already.
                    let _ = session.abort_transaction().await;
                    match e {
                        StorageError::Serialization if attempt < MAX_RETRIES => {
                            tracing::debug!(
                                attempt = attempt + 1,
                                "Transaction lost its race, retrying"
                            );
                            backoff(attempt).await;
                            continue;
                        }
                        e => return Err(e),
                    }
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

/// Commit, retrying a commit whose outcome is unknown.
///
/// `UnknownTransactionCommitResult` means the commit may or may not have
/// happened — a network error mid-commit, a failover. Committing again on the
/// same session is idempotent, so that is retried; running the body again is
/// not, so a commit that stays unknown is a 500, never a replay.
async fn commit(session: &mut mongodb::ClientSession) -> StorageResult<()> {
    let mut tries = 0;
    loop {
        match session.commit_transaction().await {
            Ok(()) => return Ok(()),
            Err(e)
                if e.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                    && tries < MAX_COMMIT_RETRIES =>
            {
                tries += 1;
                tracing::warn!(tries, error = %e, "Commit outcome unknown, retrying commit");
            }
            Err(e) if e.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) => {
                return Err(StorageError::Backend(format!(
                    "commit outcome unknown: {e}"
                )));
            }
            Err(e) => return Err(map_err(e)),
        }
    }
}

/// A short, randomised pause before a retry.
///
/// Two transactions that conflicted and both retry at once tend to conflict
/// again; a few milliseconds of jitter, doubling per attempt, is what breaks
/// the tie. Bounded well below a request timeout — the point is to spread the
/// losers, not to wait for anything.
async fn backoff(attempt: u32) {
    let ceiling = 4u64 << attempt.min(5);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let millis = 1 + nanos % ceiling;
    tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
}

/// The schema: every index, on the collection it belongs to.
///
/// The four deadline queues are four partial indexes, as they are in
/// Postgres: each holds exactly the documents whose deadline is live, so a
/// sweep reads the queue and nothing else.
fn indexes<'a>(
    promises: &'a Collection<Document>,
    schedules: &'a Collection<Document>,
) -> Vec<(&'a Collection<Document>, IndexModel)> {
    fn index(name: &str, keys: Document, partial: Option<Document>) -> IndexModel {
        IndexModel::builder()
            .keys(keys)
            .options(
                IndexOptions::builder()
                    .name(name.to_string())
                    .partial_filter_expression(partial)
                    .build(),
            )
            .build()
    }
    vec![
        // Promise deadlines: pending and external.
        (
            promises,
            index(
                "resonate_promise_timeout",
                doc! { "timeout_at": 1, "_id": 1 },
                Some(doc! { "state": "pending", "external": true }),
            ),
        ),
        // Retry deadlines: pending tasks.
        (
            promises,
            index(
                "resonate_task_retry",
                doc! { "retry_timeout_at": 1, "_id": 1 },
                Some(doc! { "task_state": "pending" }),
            ),
        ),
        // Leases: acquired tasks.
        (
            promises,
            index(
                "resonate_task_lease",
                doc! { "lease_timeout_at": 1, "_id": 1 },
                Some(doc! { "task_state": "acquired" }),
            ),
        ),
        // The branch siblings a task response preloads.
        (
            promises,
            index(
                "resonate_promise_branch",
                doc! { "branch_id": 1, "_id": 1 },
                None,
            ),
        ),
        // The console's tree: every promise of one execution shares an origin.
        (
            promises,
            index(
                "resonate_promise_origin",
                doc! { "origin": 1, "created_at": 1, "_id": 1 },
                None,
            ),
        ),
        // The console's executions list: roots, by creation.
        (
            promises,
            index(
                "resonate_promise_roots",
                doc! { "created_at": 1, "_id": 1 },
                Some(doc! { "root": true }),
            ),
        ),
        // `task.search`.
        (
            promises,
            index(
                "resonate_task_state",
                doc! { "task_state": 1, "_id": 1 },
                None,
            ),
        ),
        // Tag containment.
        (
            promises,
            index(
                "resonate_promise_tags",
                doc! { "tags.k": 1, "tags.v": 1 },
                None,
            ),
        ),
        // A schedule's `next_run_at` is its queue.
        (
            schedules,
            index(
                "resonate_schedule_next_run",
                doc! { "next_run_at": 1, "_id": 1 },
                None,
            ),
        ),
        (
            schedules,
            index(
                "resonate_schedule_tags",
                doc! { "promise_tags.k": 1, "promise_tags.v": 1 },
                None,
            ),
        ),
    ]
}

#[async_trait]
impl Engine for MongoDbEngine {
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

/// This server, as a plugin. The one thing a binary names to run on MongoDB.
pub static PLUGIN: ServerPlugin = ServerPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[servers.server_mongodb]` and build the server. Nothing is opened
/// here — the client is `init`'s, like every other port's resource.
fn configure(
    settings: &Settings<'_>,
    deps: ServerDependencies,
) -> Result<std::sync::Arc<dyn ResonateServer>, ConfigError> {
    let config: Config = settings.extract()?;
    if config.uri.is_empty() {
        return Err(settings.reject("uri", "a MongoDB connection string is required"));
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
                let engine = MongoDbEngine::connect(&open, debug).await.map_err(|e| {
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
