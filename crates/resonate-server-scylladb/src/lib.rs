//! Resonate's durable state, over ScyllaDB.
//!
//! The mechanics are `resonate-on-scylladb`'s, carried over deliberately:
//! one `promises` table partitioned by origin so a transition and its
//! fanout are a single-partition LWT or conditional batch; bucketed,
//! sharded timeout queues written through the pre-insert protocol; and an
//! EAGER deadline for every pending promise, internal included — where the
//! relational engines stay lazy. The differential is the experiment that
//! decides whether that difference is observable.
//!
//! What is not Go's: messages are returned, not dispatched (`Output`
//! carries them), `now` is a parameter, and the scan loops are driven by
//! the server through `tick`/`process(Internal)` rather than by their own
//! goroutines. The queries underneath are the same queries.

mod db;
mod ops_promise;
mod ops_schedule;
mod ops_task;
mod snap;
mod timeouts;
mod tls;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use scylla::client::caching_session::CachingSession;
use scylla::client::session_builder::SessionBuilder;
use serde::{Deserialize, Serialize};

use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_server_dbms::engine_port::{Input, Outgoing, Output, ResonateEngine, Scheduled};
use resonate_server_dbms::{StorageError, StorageResult};

/// The embedded schema, applied statement by statement when `migrate` asks.
const SCHEMA_CQL: &str = include_str!("../schema.cql");

/// Between a task redispatch and the next execute message: the follow-up
/// deadline every retry path arms. Same constant, same meaning, as the
/// other engines' `task_retry_timeout` — carried in config here too.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Seed hosts, bare or host:port.
    #[serde(default = "default_hosts")]
    pub hosts: Vec<String>,
    /// CQL port for hosts without one AND for gossip-discovered peers.
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_keyspace")]
    pub keyspace: String,
    /// CREATE KEYSPACE replication clause, e.g.
    /// "{'class': 'NetworkTopologyStrategy', 'dc1': 3}". Empty = server default.
    #[serde(default)]
    pub replication: String,
    /// Apply the embedded schema (CREATE ... IF NOT EXISTS) on connect.
    /// False = verify nothing, assume schema provisioned out of band.
    #[serde(default)]
    pub migrate: bool,
    /// Timeout-queue bucket width in ms.
    #[serde(default = "default_bucket_width")]
    pub bucket_width: i64,
    /// Past buckets each scan covers, in addition to the current one.
    #[serde(default = "default_bucket_lookback")]
    pub bucket_lookback: i64,
    /// Queue-table shard count; entries land on fnv32a(id) % shards.
    #[serde(default = "default_shards")]
    pub shards: i16,
    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,
    /// Worker heartbeat TTL in ms — how long a dead instance keeps its
    /// shard assignment. Must exceed the server's sweep interval, or live
    /// workers expire between their own heartbeats.
    #[serde(default = "default_worker_ttl")]
    pub worker_ttl: i64,
    /// TLS to the cluster.
    #[serde(default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Encrypt but skip certificate verification — for dev clusters with
    /// private-CA certs, the SCYLLADB_TLS_INSECURE the Go deployment used.
    #[serde(default)]
    pub insecure: bool,
    /// PEM file with the CA to trust instead of the WebPKI roots.
    #[serde(default)]
    pub ca_cert: Option<String>,
}

fn default_hosts() -> Vec<String> {
    vec!["localhost".to_string()]
}
fn default_keyspace() -> String {
    "resonate".to_string()
}
fn default_bucket_width() -> i64 {
    3_600_000
}
fn default_bucket_lookback() -> i64 {
    1
}
fn default_shards() -> i16 {
    1
}
fn default_preload_limit() -> u32 {
    10
}
fn default_worker_ttl() -> i64 {
    180_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hosts: default_hosts(),
            port: 0,
            username: String::new(),
            password: String::new(),
            keyspace: default_keyspace(),
            replication: String::new(),
            migrate: false,
            bucket_width: default_bucket_width(),
            bucket_lookback: default_bucket_lookback(),
            shards: default_shards(),
            preload_limit: default_preload_limit(),
            worker_ttl: default_worker_ttl(),
            tls: TlsConfig::default(),
        }
    }
}

pub struct ScyllaEngine {
    pub(crate) session: Arc<CachingSession>,
    pub(crate) bucket_width: i64,
    pub(crate) bucket_lookback: i64,
    pub(crate) shards: i16,
    pub(crate) task_retry_timeout: i64,
    pub(crate) preload_limit: u32,
    pub(crate) worker_ttl: i64,
    pub(crate) worker_id: uuid::Uuid,
    pub(crate) debug: bool,
}

impl ScyllaEngine {
    /// Open a session and, when `migrate`, apply the embedded schema. The
    /// keyspace is created if missing either way — a keyspace-bound session
    /// cannot exist without one, and creating an empty keyspace is the same
    /// "empty database is always created" policy the SQL engines follow.
    pub async fn connect(
        cfg: &Config,
        task_retry_timeout: i64,
        debug: bool,
    ) -> StorageResult<Self> {
        let mut builder = SessionBuilder::new().known_nodes(&cfg.hosts);
        if cfg.port != 0 {
            let with_ports: Vec<String> = cfg
                .hosts
                .iter()
                .map(|h| {
                    if h.contains(':') {
                        h.clone()
                    } else {
                        format!("{}:{}", h, cfg.port)
                    }
                })
                .collect();
            builder = SessionBuilder::new().known_nodes(&with_ports);
        }
        if !cfg.username.is_empty() || !cfg.password.is_empty() {
            builder = builder.user(cfg.username.clone(), cfg.password.clone());
        }
        if cfg.tls.enabled {
            builder = builder.tls_context(Some(tls::context(&cfg.tls)?));
        }
        // Explicit, not inherited: Quorum for reads and writes, Serial for
        // the LWT Paxos round — the settings this engine's mechanics assume,
        // stated rather than left to whatever the driver ships.
        let profile = scylla::client::execution_profile::ExecutionProfile::builder()
            .consistency(scylla::statement::Consistency::Quorum)
            .serial_consistency(Some(scylla::statement::SerialConsistency::Serial))
            .build();
        builder = builder.default_execution_profile_handle(profile.into_handle());
        let session = builder
            .build()
            .await
            .map_err(|e| StorageError::Backend(format!("scylladb connect: {e}")))?;

        let replication = if cfg.replication.is_empty() {
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        } else {
            cfg.replication.as_str()
        };
        // Tablets refuse LWT, and LWT is this engine's entire write
        // mechanism — the keyspace opts out explicitly on ScyllaDB versions
        // that would otherwise default to them.
        let create = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {} AND tablets = {{'enabled': false}}",
            cfg.keyspace, replication
        );
        session
            .query_unpaged(create, ())
            .await
            .map_err(|e| StorageError::Backend(format!("create keyspace: {e}")))?;
        session
            .use_keyspace(cfg.keyspace.clone(), false)
            .await
            .map_err(|e| StorageError::Backend(format!("use keyspace: {e}")))?;

        if cfg.migrate {
            for stmt in schema_statements() {
                session
                    .query_unpaged(stmt.clone(), ())
                    .await
                    .map_err(|e| StorageError::Backend(format!("apply schema: {e}: {stmt}")))?;
            }
        }

        Ok(Self {
            session: Arc::new(CachingSession::from(session, 256)),
            bucket_width: cfg.bucket_width.max(1),
            bucket_lookback: cfg.bucket_lookback.max(0),
            shards: cfg.shards.max(1),
            task_retry_timeout,
            preload_limit: cfg.preload_limit,
            worker_ttl: cfg.worker_ttl.max(1_000),
            worker_id: uuid::Uuid::new_v4(),
            debug,
        })
    }

    pub(crate) fn bucket_for(&self, t: i64) -> i64 {
        t.div_euclid(self.bucket_width)
    }

    /// fnv32a(id) % shards, like the Go implementation — keyed on the
    /// entity id, not the origin, so one call graph's deadlines spread.
    pub(crate) fn shard_for(&self, id: &str) -> i16 {
        if self.shards <= 1 {
            return 0;
        }
        let mut h: u32 = 0x811c_9dc5;
        for b in id.as_bytes() {
            h ^= *b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        (h % self.shards as u32) as i16
    }

    /// Buckets a scan at `t` covers: the current one plus the lookback.
    pub(crate) fn buckets_to_scan(&self, t: i64) -> Vec<i64> {
        let cur = self.bucket_for(t);
        let start = (cur - self.bucket_lookback).max(0);
        (start..=cur).collect()
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

/// Split the embedded schema into statements, skipping CREATE KEYSPACE and
/// USE (handled by `connect`) and comment lines — the Go loader, verbatim.
fn schema_statements() -> Vec<String> {
    let mut out = Vec::new();
    for raw in SCHEMA_CQL.split(';') {
        let stmt: String = raw
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let stmt = stmt.trim().to_string();
        if stmt.is_empty() {
            continue;
        }
        let upper = stmt.to_uppercase();
        if upper.starts_with("CREATE KEYSPACE") || upper.starts_with("USE ") {
            continue;
        }
        out.push(stmt);
    }
    out
}

/// The origin is everything before the id's first ':' — the partition key,
/// and spec-level ("an id is an origin and a suffix").
pub(crate) fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

/// What one transition accumulates: the messages it emitted and the
/// deadlines it armed. The engine writes queue rows for every pending
/// promise (eager, like Go); the HINTS it reports follow the protocol —
/// awaitable only — so the wheel and the differential see the same
/// announcements every engine makes.
#[derive(Default)]
pub(crate) struct Ctx {
    pub messages: Vec<Outgoing>,
    pub armed: Vec<Scheduled>,
}

pub(crate) type Tags = HashMap<String, String>;

#[async_trait]
impl ResonateEngine for ScyllaEngine {
    async fn process(&self, input: Input<'_>, now: i64) -> Output {
        match input {
            Input::External(req) => self.dispatch(req, now).await,
            Input::Internal(timeout) => self.process_internal(timeout, now).await,
        }
    }

    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        self.tick_impl(now).await
    }

    async fn upcoming(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        self.upcoming_impl(limit).await
    }

    async fn ping(&self) -> StorageResult<()> {
        self.session
            .get_session()
            .query_unpaged("SELECT release_version FROM system.local", ())
            .await
            .map_err(|e| StorageError::Backend(format!("ping: {e}")))?;
        Ok(())
    }

    fn returns_messages(&self) -> bool {
        true
    }
}
