//! What a binary's plugin set has to satisfy, and how settings reach a plugin.

use std::sync::Arc;

use async_trait::async_trait;
#[rustfmt::skip]
use resonate_plugin::{
    ServerDependencies, ServerPlugin, WorkerDependencies, WorkerPlugin, GatewayPlugin,
    ResonateServer, ResonateWorker, ResonateRouter,
    ConfigError, RegistryError,
    Loader, Registry, Settings,
};
use serde::{Deserialize, Serialize};

// ─── A server ────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Serialize, Deserialize)]
struct SqliteConfig {
    #[serde(default)]
    path: String,
}

static SQLITE: ServerPlugin = ServerPlugin::new("resonate-server-sqlite", |settings, _deps| {
    let _config: SqliteConfig = settings.extract()?;
    // Nothing is opened here. A real engine connects in `init`, like every
    // other port, and reports failure from there.
    Ok(Arc::new(Unstarted) as Arc<dyn ResonateServer>)
});

/// A server that has been built and not started. Standing in for one whose
/// `init` would open a pool.
struct Unstarted;

#[async_trait]
impl ResonateServer for Unstarted {
    async fn init(&self, _debug: bool) -> Result<(), resonate_core::Unavailable> {
        Err(resonate_core::Unavailable::new("not a real engine"))
    }

    async fn process(
        &self,
        _req: &resonate_core::types::RequestEnvelope,
    ) -> Result<resonate_core::types::ResponseEnvelope, resonate_core::Unavailable> {
        unreachable!("never started")
    }
}

// ─── A worker, written the way a third party would write one ────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct KafkaConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    brokers: Vec<String>,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
}

fn default_concurrency() -> usize {
    100
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            brokers: Vec::new(),
            concurrency: default_concurrency(),
        }
    }
}

struct KafkaWorker {
    config: KafkaConfig,
}

#[async_trait]
impl ResonateWorker for KafkaWorker {
    async fn process(
        &self,
        _address: &str,
        _msg: &resonate_core::types::Message,
    ) -> Result<(), resonate_core::Unavailable> {
        // Reads the config it captured at configure time, which is the whole
        // point of the factory closure owning it.
        if self.config.brokers.is_empty() {
            return Err(resonate_core::Unavailable::new("no brokers configured"));
        }
        Ok(())
    }
}

fn kafka_configure(
    settings: &Settings<'_>,
    _deps: WorkerDependencies,
) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError> {
    let config: KafkaConfig = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    if config.concurrency == 0 {
        // Zero permits means every message queues forever, so it is refused at
        // startup rather than hung on later — and the plugin says so itself, in
        // the one place that knows.
        return Err(settings.reject("concurrency", "must be at least 1 (got 0)"));
    }
    Ok(Some(Arc::new(KafkaWorker { config })))
}

/// The dangling handle a worker is given in these tests. A real one is a
/// `Weak` to the server built at step 2.
fn no_server() -> WorkerDependencies {
    WorkerDependencies::new(std::sync::Weak::<Dead>::new() as std::sync::Weak<dyn ResonateServer>)
}

static KAFKA: WorkerPlugin =
    WorkerPlugin::new("resonate-worker-kafka", &["kafka"], kafka_configure);

/// A second plugin claiming the same scheme, for the collision check.
static KAFKA_RIVAL: WorkerPlugin =
    WorkerPlugin::new("other-kafka-crate", &["kafka"], kafka_configure);

/// A worker that claims nothing, so nothing could ever route to it.
static MUTE: WorkerPlugin = WorkerPlugin::new("resonate-worker-mute", &[], kafka_configure);

// ─── A gateway ───────────────────────────────────────────────────────────────

static HTTP: GatewayPlugin = GatewayPlugin::new("resonate-gateway-http", |_settings, _deps| {
    unreachable!("not built in this test")
});

// ─── The set of plugins a binary carries ─────────────────────────────────────

#[test]
fn a_scheme_claimed_twice_is_refused_before_anything_runs() {
    let errors = Registry::new()
        .server(&SQLITE)
        .worker(&KAFKA)
        .worker(&KAFKA_RIVAL)
        .check()
        .expect_err("two workers cannot share a scheme");

    let collision = errors
        .iter()
        .find(|e| matches!(e, RegistryError::DuplicateScheme { .. }))
        .expect("the collision is reported");
    let rendered = collision.to_string();
    // Both crates are named: the operator cannot fix this, the person
    // assembling the binary can, and they need to know which two to choose
    // between.
    assert!(rendered.contains("resonate-worker-kafka"), "{rendered}");
    assert!(rendered.contains("other-kafka-crate"), "{rendered}");
}

#[test]
fn a_worker_claiming_no_scheme_is_refused() {
    let errors = Registry::new()
        .server(&SQLITE)
        .worker(&MUTE)
        .check()
        .expect_err("unreachable worker");
    assert!(errors
        .iter()
        .any(|e| matches!(e, RegistryError::NoSchemes { .. })));
}

#[test]
fn one_id_claimed_twice_is_refused() {
    let errors = Registry::new()
        .server(&SQLITE)
        .worker(&KAFKA)
        .worker(&KAFKA)
        .check()
        .expect_err("one id, two plugins");
    assert!(errors
        .iter()
        .any(|e| matches!(e, RegistryError::DuplicateId { .. })));
}

#[test]
fn a_binary_with_no_server_is_not_a_server() {
    let errors = Registry::new()
        .worker(&KAFKA)
        .check()
        .expect_err("a worker and no server is not a server");
    assert!(errors.iter().any(|e| matches!(e, RegistryError::NoServer)));
}

#[test]
fn a_whole_binary_checks_out() {
    Registry::new()
        .server(&SQLITE)
        .worker(&KAFKA)
        .gateway(&HTTP)
        .check()
        .expect("a server, a worker and a gateway");
}

#[test]
fn selecting_a_server_the_binary_does_not_carry_names_what_it_does() {
    let registry = Registry::new().server(&SQLITE);
    let Err(err) = registry.select_server("server_scylladb") else {
        panic!("scylladb was never registered");
    };
    let rendered = err.to_string();
    // Not misconfigured — *not compiled in*, and the message says which, rather
    // than falling through to whichever backend happens to be the catch-all.
    assert!(
        rendered.contains("not compiled into this binary"),
        "{rendered}"
    );
    assert!(rendered.contains("server_scylladb"), "{rendered}");
    assert!(rendered.contains("server_sqlite"), "{rendered}");
    assert_eq!(
        registry.select_server("server_sqlite").unwrap().id(),
        "server_sqlite"
    );
}

// ─── Configuration ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct CoreConfig {
    level: String,
}

fn loader() -> Loader {
    Loader::new().defaults(CoreConfig {
        level: "info".to_string(),
    })
}

#[test]
fn a_plugin_nobody_configured_gets_its_own_defaults() {
    // Nothing was seeded on its behalf — the section is absent entirely, and
    // serde's own defaults on KafkaConfig fill it in. One source of truth for a
    // default, rather than a struct and a snapshot of the same struct.
    let loaded = loader().load();
    let config: KafkaConfig = loaded.worker(&KAFKA.id()).extract().unwrap();
    assert_eq!(config, KafkaConfig::default());
    assert_eq!(loaded.extract::<CoreConfig>().unwrap().level, "info");
}

#[test]
fn a_plugin_turns_itself_off() {
    // `enabled` is the plugin's own field, so nothing in the framework has an
    // opinion about what it is called or what it defaults to.
    let loaded = loader().load();
    assert!(
        (KAFKA.configure)(&loaded.worker(&KAFKA.id()), no_server())
            .unwrap()
            .is_none(),
        "off by its own default"
    );

    let loaded = loader()
        .set("workers.worker_kafka.enabled", "true")
        .unwrap()
        .load();
    assert!((KAFKA.configure)(&loaded.worker(&KAFKA.id()), no_server())
        .unwrap()
        .is_some());
}

#[test]
fn set_carries_types_not_just_strings() {
    let loaded = loader()
        .set("workers.worker_kafka.concurrency", "8")
        .unwrap()
        .set("workers.worker_kafka.brokers", r#"["a:9092", "b:9092"]"#)
        .unwrap()
        // Unquoted, so it is meant as a string.
        .set("level", "debug")
        .unwrap()
        .load();

    let config: KafkaConfig = loaded.worker(&KAFKA.id()).extract().unwrap();
    assert_eq!(config.concurrency, 8);
    assert_eq!(config.brokers, vec!["a:9092", "b:9092"]);
    assert_eq!(loaded.extract::<CoreConfig>().unwrap().level, "debug");
}

#[test]
fn a_plugin_validates_its_own_settings_and_says_where_they_came_from() {
    let loaded = loader()
        .set("workers.worker_kafka.enabled", "true")
        .unwrap()
        .set("workers.worker_kafka.concurrency", "0")
        .unwrap()
        .load();

    let Err(err) = (KAFKA.configure)(&loaded.worker(&KAFKA.id()), no_server()) else {
        panic!("zero concurrency is the plugin's own rule");
    };
    assert_eq!(err.key, "workers.worker_kafka.concurrency");
    assert!(err.message.contains("at least 1"), "{}", err.message);
    // Provenance survives: the same bad number from a file and from a flag are
    // different problems to go and fix.
    assert!(err.source.is_some(), "the source of the value is reported");
}

#[test]
fn a_server_is_selected_by_name() {
    let loaded = loader().load();
    assert_eq!(loaded.active_server("server_sqlite"), "server_sqlite");

    let loaded = loader()
        .set("servers.active", "server_scylladb")
        .unwrap()
        .load();
    assert_eq!(loaded.active_server("server_sqlite"), "server_scylladb");
}

#[test]
fn the_typed_config_never_leaves_the_plugins_crate() {
    // The factory closure owns it; what comes back out is a port trait object.
    let loaded = loader()
        .set("workers.worker_kafka.enabled", "true")
        .unwrap()
        .set("workers.worker_kafka.brokers", r#"["a:9092"]"#)
        .unwrap()
        .load();

    let worker: Arc<dyn ResonateWorker> =
        (KAFKA.configure)(&loaded.worker(&KAFKA.id()), no_server())
            .unwrap()
            .unwrap();
    // Nothing about KafkaConfig is reachable from here, which is the point.
    let _ = worker;
}

/// A `ResonateServer` that never exists — only its `Weak` is used, to stand in
/// for the handle a worker is given before the server is in place.
struct Dead;

#[async_trait]
impl ResonateServer for Dead {
    async fn process(
        &self,
        _req: &resonate_core::types::RequestEnvelope,
    ) -> Result<resonate_core::types::ResponseEnvelope, resonate_core::Unavailable> {
        unreachable!("never constructed")
    }
}

#[tokio::test]
async fn connecting_is_init_not_configure() {
    // A plugin that opens something does it in `init`, whichever kind it is, so
    // construction stays sync and cheap and a failure to start is reported from
    // one place. Configuring this server succeeds; starting it does not.
    let loaded = loader().load();
    let server = (SQLITE.configure)(
        &loaded.server(&SQLITE.id()),
        ServerDependencies::new(Arc::new(NoRoute) as Arc<dyn ResonateRouter>),
    )
    .expect("its settings are fine");

    let err = server
        .init(false)
        .await
        .expect_err("the engine it would open is not there");
    assert!(err.message.contains("not a real engine"), "{}", err.message);
}

/// A router that never delivers. The server under test never emits anything.
struct NoRoute;

#[async_trait]
impl ResonateRouter for NoRoute {
    async fn route(
        &self,
        _address: &str,
        _msg: &resonate_core::types::Message,
    ) -> Result<(), resonate_core::Unavailable> {
        unreachable!("nothing is routed in this test")
    }
}

#[test]
fn the_id_is_the_crate_name() {
    // One field, not two that have to agree. The `resonate-` prefix goes
    // because every crate here carries it; the role stays, because the crate
    // name saying what the thing does is the point.
    assert_eq!(SQLITE.id(), "server_sqlite");
    assert_eq!(KAFKA.id(), "worker_kafka");
    assert_eq!(HTTP.id(), "gateway_http");
    // A crate outside the naming keeps its whole name.
    assert_eq!(KAFKA_RIVAL.id(), "other_kafka_crate");
    assert_eq!(
        resonate_plugin::id_from_crate("resonate-transport-http-poll"),
        "transport_http_poll"
    );
}
