//! This server, as a plugin.
//!
//! Reads `[servers.server_kafka]`, defers every connection to `init`, and stops
//! from the port's own lifecycle — so the composition root treats it like any
//! other server plugin.
//!
//! With no `brokers` the node runs alone over an in-process log: every
//! partition, nothing durable. That is a development and test mode, and
//! startup says so loudly. With no `data_dir` the local copies live in memory
//! and every start restores from Kafka.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};
use resonate_server_blob::kernel::state::KernelCfg;
use resonate_server_blob::sender::Sender;

use crate::directory::kafka::{GroupDirectory, GroupDirectoryCfg};
use crate::directory::{Directory, NoDirectory};
use crate::local::mem::MemLocal;
use crate::local::rocks::{RocksCfg, RocksLocal};
use crate::local::LocalStore;
use crate::log::kafka::{KafkaCfg, KafkaLog};
use crate::log::mem::MemLog;
use crate::log::{Log, Topic};
use crate::membership::kafka::{GroupCfg, KafkaMembership};
use crate::membership::{Membership, StaticMembership};
use crate::node::{Node, NodeCfg};
use crate::partition::PartitionCfg;
use crate::peer::{HttpPeers, LocalPeers, Peers};

/// This server, as a plugin.
pub static PLUGIN: resonate_plugin::ServerPlugin =
    resonate_plugin::ServerPlugin::new(env!("CARGO_PKG_NAME"), configure);

fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::ServerDependencies,
) -> Result<resonate_plugin::Configured, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if config.partitions == 0 {
        return Err(settings.reject("partitions", "must be at least 1 (got 0)"));
    }
    if config.brokers.is_some() && config.node_id.trim().is_empty() {
        return Err(settings.reject("node_id", "every node in a cluster needs a unique id"));
    }
    if config.max_batch == 0 {
        return Err(settings.reject("max_batch", "must be at least 1 (got 0)"));
    }
    // A placeholder: nothing reads the roster yet, and the node still forwards
    // between nodes itself. Kafka's own roster — its partition table and
    // directory — replaces this.
    Ok(resonate_plugin::Configured::single(Arc::new(
        KafkaServer::new(config, deps.router),
    )))
}

/// Everything under `[servers.server_kafka]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// `bootstrap.servers`. Unset: one node over an in-process log.
    #[serde(default)]
    pub brokers: Option<String>,

    /// Partitions of the promise and schedule topics. Fixed for the life of
    /// the deployment: an origin's partition is a function of it.
    #[serde(default = "default_partitions")]
    pub partitions: u32,

    /// Topics are `<topic_prefix>.promises` and `.schedules`.
    #[serde(default = "default_prefix")]
    pub topic_prefix: String,

    /// The consumer group. Defaults to the topic prefix.
    #[serde(default)]
    pub group_id: Option<String>,

    /// Transactional ids are `<txn_prefix>-p<partition>`. Defaults to the
    /// topic prefix.
    #[serde(default)]
    pub txn_prefix: Option<String>,

    /// Replication factor for topics this server creates.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i32,

    /// Create the topics when they do not exist.
    #[serde(default = "default_true")]
    pub create_topics: bool,

    /// This node's id: unique in the cluster.
    #[serde(default = "default_node_id")]
    pub node_id: String,

    /// Where this node listens for requests forwarded by other nodes. Internal
    /// traffic only; keep it off the public network.
    #[serde(default = "default_peer_bind")]
    pub peer_bind: String,

    /// The URL other nodes reach `peer_bind` at.
    #[serde(default)]
    pub peer_url: String,

    /// How often the owner directory asks the group who owns what, in ms. A
    /// failed forward asks at once.
    #[serde(default = "default_directory_refresh_ms")]
    pub directory_refresh_ms: u64,

    /// A shared secret every peer request must carry.
    #[serde(default)]
    pub peer_token: Option<String>,

    /// Directory for the local RocksDB copy. Unset: in memory.
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    /// RocksDB block cache, shared by every partition, in MiB.
    #[serde(default = "default_block_cache_mb")]
    pub block_cache_mb: usize,

    /// RocksDB memtable budget, shared by every partition, in MiB.
    #[serde(default = "default_write_buffer_mb")]
    pub write_buffer_mb: usize,

    /// How long the group waits on a silent node, in ms.
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u64,

    /// `group.instance.id` for static membership.
    #[serde(default)]
    pub instance_id: Option<String>,

    /// How long a revoked partition's local copy is kept, in seconds.
    #[serde(default = "default_drop_grace_secs")]
    pub drop_grace_secs: u64,

    /// The most requests one commit round decides.
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,

    /// Whether the search operations are answered. Each reads every record.
    #[serde(default)]
    pub search_enabled: bool,

    /// The URL a worker is told to call back on.
    #[serde(default)]
    pub server_url: String,

    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,

    /// How long a pending task waits before its dispatch is re-sent.
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: i64,

    /// Extra librdkafka properties for every client: security, tuning.
    #[serde(default)]
    pub librdkafka: BTreeMap<String, String>,
}

fn default_partitions() -> u32 {
    64
}
fn default_prefix() -> String {
    "resonate".into()
}
fn default_replication_factor() -> i32 {
    3
}
fn default_true() -> bool {
    true
}
fn default_node_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "node-0".into())
}
fn default_peer_bind() -> String {
    "0.0.0.0:8002".into()
}
fn default_directory_refresh_ms() -> u64 {
    2_000
}
fn default_block_cache_mb() -> usize {
    256
}
fn default_write_buffer_mb() -> usize {
    128
}
fn default_session_timeout_ms() -> u64 {
    10_000
}
fn default_drop_grace_secs() -> u64 {
    15 * 60
}
fn default_max_batch() -> usize {
    512
}
fn default_preload_limit() -> u32 {
    10
}
fn default_retry_timeout() -> i64 {
    60_000
}

impl Default for Config {
    fn default() -> Self {
        serde_json::from_value(serde_json::json!({})).expect("every field has a default")
    }
}

type Wiring = (
    Arc<dyn Log>,
    Arc<dyn Membership>,
    Arc<dyn Directory>,
    Arc<dyn Peers>,
);

pub struct KafkaServer {
    config: Config,
    router: Arc<dyn ResonateRouter>,
    inner: OnceLock<Arc<Node>>,
    peer_stop: tokio::sync::watch::Sender<bool>,
    peer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl KafkaServer {
    pub fn new(config: Config, router: Arc<dyn ResonateRouter>) -> Self {
        Self {
            config,
            router,
            inner: OnceLock::new(),
            peer_stop: tokio::sync::watch::channel(false).0,
            peer: Mutex::new(None),
        }
    }

    fn started(&self) -> Result<&Arc<Node>, Unavailable> {
        self.inner
            .get()
            .ok_or_else(|| Unavailable::new("the kafka server has not been started"))
    }

    fn open_local(&self) -> Result<Arc<dyn LocalStore>, Unavailable> {
        match &self.config.data_dir {
            Some(dir) => {
                std::fs::create_dir_all(dir).map_err(|e| {
                    Unavailable::new(format!("cannot create {}: {e}", dir.display()))
                })?;
                let store = RocksLocal::open(
                    dir,
                    &RocksCfg {
                        block_cache_bytes: self.config.block_cache_mb << 20,
                        write_buffer_bytes: self.config.write_buffer_mb << 20,
                    },
                )
                .map_err(Unavailable::new)?;
                Ok(store)
            }
            None => Ok(MemLocal::new()),
        }
    }
}

#[async_trait]
impl ResonateServer for KafkaServer {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let c = &self.config;
        let (log, membership, directory, peers): Wiring = match &c.brokers {
            None => {
                tracing::warn!(
                    "servers.server_kafka.brokers is not set — one node over an \
                         in-process log. Nothing survives this process."
                );
                (
                    MemLog::new(c.partitions),
                    StaticMembership::new(c.partitions),
                    Arc::new(NoDirectory),
                    LocalPeers::new(),
                )
            }
            Some(brokers) => {
                let prefix = c.topic_prefix.clone();
                let kafka = KafkaCfg {
                    brokers: brokers.clone(),
                    topic_prefix: prefix.clone(),
                    txn_prefix: c.txn_prefix.clone().unwrap_or_else(|| prefix.clone()),
                    partitions: c.partitions,
                    replication_factor: c.replication_factor,
                    create_topics: c.create_topics,
                    properties: c.librdkafka.clone(),
                    node_id: c.node_id.clone(),
                    ..Default::default()
                };
                tracing::info!(brokers = %brokers, partitions = c.partitions, node = %c.node_id, "Using Kafka backend");
                if c.peer_url.is_empty() {
                    tracing::warn!(
                        "servers.server_kafka.peer_url is not set — requests for \
                             partitions this node does not own cannot be forwarded"
                    );
                }
                let log = KafkaLog::connect(kafka.clone())
                    .await
                    .map_err(|e| Unavailable::new(e.to_string()))?;
                let group_id = c.group_id.clone().unwrap_or(prefix);
                // Nodes find each other through the group: who is assigned
                // what, and where each member says it can be reached.
                let directory = GroupDirectory::start(GroupDirectoryCfg {
                    brokers: brokers.clone(),
                    group_id: group_id.clone(),
                    topic: kafka.topic(Topic::Promises),
                    refresh: Duration::from_millis(c.directory_refresh_ms),
                    timeout: Duration::from_secs(10),
                    properties: c.librdkafka.clone(),
                })
                .map_err(Unavailable::new)?;
                let membership = KafkaMembership::new(
                    kafka,
                    GroupCfg {
                        group_id,
                        session_timeout: Duration::from_millis(c.session_timeout_ms),
                        instance_id: c.instance_id.clone(),
                        peer_url: c.peer_url.clone(),
                    },
                );
                let peers = Arc::new(HttpPeers::new(
                    Duration::from_secs(30),
                    c.peer_token.clone(),
                ));
                (log, membership, directory, peers)
            }
        };

        let node = Node::new(
            NodeCfg {
                node_id: c.node_id.clone(),
                partition: PartitionCfg {
                    kernel: KernelCfg {
                        retry_timeout: c.retry_timeout,
                        preload_limit: c.preload_limit,
                        server_url: c.server_url.clone(),
                    },
                    max_batch: c.max_batch,
                    ..Default::default()
                },
                debug,
                search: c.search_enabled,
                drop_grace: Duration::from_secs(c.drop_grace_secs),
                ..Default::default()
            },
            log,
            self.open_local()?,
            Arc::new(Sender::new(Arc::clone(&self.router), debug)),
            membership,
            directory,
            peers,
        );

        if c.brokers.is_some() {
            let handle = crate::peer::serve(
                &c.peer_bind,
                Arc::downgrade(&node),
                c.peer_token.clone(),
                self.peer_stop.subscribe(),
            )
            .await?;
            *self.peer.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        }
        if debug {
            tracing::warn!(
                "Debug mode — no timer loops, and messages are held for debug.snap. \
                 Time advances only through debug.tick."
            );
        }
        node.start().await?;
        let _ = self.inner.set(node);
        Ok(())
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        if let Some(node) = self.inner.get() {
            node.stop().await;
        }
        let _ = self.peer_stop.send(true);
        let handle = self.peer.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.started()?.process(req).await
    }

    async fn ready(&self) -> bool {
        match self.inner.get() {
            Some(node) => node.is_ready().await,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoRouter;

    #[async_trait]
    impl ResonateRouter for NoRouter {
        async fn route(
            &self,
            _address: &str,
            _msg: &resonate_core::types::Message,
        ) -> Result<(), Unavailable> {
            Ok(())
        }
    }

    fn deps() -> resonate_plugin::ServerDependencies {
        resonate_plugin::ServerDependencies::new(
            Arc::new(NoRouter) as Arc<dyn ResonateRouter>,
            resonate_plugin::Routes::new(),
        )
    }

    fn settings(pairs: &[(&str, &str)]) -> resonate_plugin::Configuration {
        let mut loader = resonate_plugin::Loader::new();
        for (k, v) in pairs {
            loader = loader.set(k, v).unwrap();
        }
        loader.load()
    }

    #[test]
    fn its_id_comes_from_its_crate_name() {
        assert_eq!(PLUGIN.id(), "server_kafka");
    }

    #[test]
    fn a_section_nobody_wrote_gets_this_crate_s_defaults() {
        let config = settings(&[]);
        assert!((PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).is_ok());
        assert_eq!(Config::default().partitions, 64);
    }

    #[test]
    fn zero_partitions_is_refused_at_startup() {
        let config = settings(&[("servers.server_kafka.partitions", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()) else {
            panic!("zero partitions would divide by zero when routing");
        };
        assert_eq!(err.key, "servers.server_kafka.partitions");
    }

    /// `configure` opens nothing: no brokers are dialled until `init`.
    #[test]
    fn configure_touches_no_broker() {
        let config = settings(&[("servers.server_kafka.brokers", "127.0.0.1:1")]);
        assert!((PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).is_ok());
    }

    /// With no brokers it runs alone in memory: the whole lifecycle.
    #[tokio::test]
    async fn it_starts_answers_and_stops_in_memory() {
        let config = settings(&[("servers.server_kafka.partitions", "4")]);
        let server = (PLUGIN.configure)(&config.server(&PLUGIN.id()), deps())
            .unwrap()
            .server;
        assert!(!server.ready().await, "not ready before init");
        server.init(true).await.expect("in-memory needs nothing");
        assert!(server.ready().await);

        let req: RequestEnvelope = serde_json::from_value(serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "1", "version": resonate_core::types::SUPPORTED_VERSIONS[0], "resonate:debug_time": 1000 },
            "data": { "id": "o:a", "timeoutAt": 50000, "param": {}, "tags": {} }
        }))
        .unwrap();
        let resp = server.process(&req).await.unwrap();
        assert_eq!(resp.head.status, 200);

        server.stop().await.expect("stops cleanly");
    }

    #[tokio::test]
    async fn stop_is_safe_when_init_never_ran() {
        let config = settings(&[]);
        let server = (PLUGIN.configure)(&config.server(&PLUGIN.id()), deps())
            .unwrap()
            .server;
        server
            .stop()
            .await
            .expect("nothing to stop is not an error");
    }
}
