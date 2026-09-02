//! What a binary's plugin set has to satisfy, and how settings reach a plugin.

use std::sync::Arc;

use async_trait::async_trait;
use resonate_plugin::{
    ConfigError, GatewayPlugin, Loader, Registry, RegistryError, ResonateServer, ResonateWorker,
    ServerPlugin, Settings, StartupError, WorkerFactory, WorkerPlugin,
};
use serde::{Deserialize, Serialize};

// ─── A plugin, written the way a third party would write one ─────────────────

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
    async fn send(
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

fn kafka_configure(settings: &Settings<'_>) -> Result<Option<WorkerFactory>, ConfigError> {
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
    Ok(Some(Box::new(move |_server| {
        Arc::new(KafkaWorker { config }) as Arc<dyn ResonateWorker>
    })))
}

static KAFKA: WorkerPlugin = WorkerPlugin::new(
    "kafka",
    "resonate-worker-kafka",
    &["kafka"],
    kafka_configure,
);

/// A second plugin claiming the same scheme, for the collision check.
static KAFKA_RIVAL: WorkerPlugin = WorkerPlugin::new(
    "kafkaesque",
    "other-kafka-crate",
    &["kafka"],
    kafka_configure,
);

/// A worker that claims nothing, so nothing could ever route to it.
static MUTE: WorkerPlugin = WorkerPlugin::new("mute", "resonate-worker-mute", &[], kafka_configure);

#[derive(Debug, Default, Serialize, Deserialize)]
struct SqliteConfig {
    #[serde(default)]
    path: String,
}

static SQLITE: ServerPlugin = ServerPlugin::new("sqlite", "resonate-server-dbms", |settings| {
    let _config: SqliteConfig = settings.extract()?;
    Ok(Box::new(|| {
        Box::pin(async { Err(StartupError::new("sqlite", "not a real engine")) })
    }))
});

static HTTP: GatewayPlugin = GatewayPlugin::new("http", "resonate-gateway-http", |_settings| {
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
        .gateway(&HTTP)
        .worker(&KAFKA)
        .check()
        .expect("a server, a gateway and a worker");
}

#[test]
fn selecting_a_server_the_binary_does_not_carry_names_what_it_does() {
    let registry = Registry::new().server(&SQLITE);
    let Err(err) = registry.select_server("scylladb") else {
        panic!("scylladb was never registered");
    };
    let rendered = err.to_string();
    // Not misconfigured — *not compiled in*, and the message says which, rather
    // than falling through to whichever backend happens to be the catch-all.
    assert!(
        rendered.contains("not compiled into this binary"),
        "{rendered}"
    );
    assert!(rendered.contains("scylladb"), "{rendered}");
    assert!(rendered.contains("sqlite"), "{rendered}");
    assert_eq!(registry.select_server("sqlite").unwrap().id, "sqlite");
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
    let config: KafkaConfig = loaded.worker("kafka").extract().unwrap();
    assert_eq!(config, KafkaConfig::default());
    assert_eq!(loaded.extract::<CoreConfig>().unwrap().level, "info");
}

#[test]
fn a_plugin_turns_itself_off() {
    // `enabled` is the plugin's own field, so nothing in the framework has an
    // opinion about what it is called or what it defaults to.
    let loaded = loader().load();
    assert!(
        (KAFKA.configure)(&loaded.worker("kafka"))
            .unwrap()
            .is_none(),
        "off by its own default"
    );

    let loaded = loader()
        .set("transports.kafka.enabled", "true")
        .unwrap()
        .load();
    assert!((KAFKA.configure)(&loaded.worker("kafka"))
        .unwrap()
        .is_some());
}

#[test]
fn set_carries_types_not_just_strings() {
    let loaded = loader()
        .set("transports.kafka.concurrency", "8")
        .unwrap()
        .set("transports.kafka.brokers", r#"["a:9092", "b:9092"]"#)
        .unwrap()
        // Unquoted, so it is meant as a string.
        .set("level", "debug")
        .unwrap()
        .load();

    let config: KafkaConfig = loaded.worker("kafka").extract().unwrap();
    assert_eq!(config.concurrency, 8);
    assert_eq!(config.brokers, vec!["a:9092", "b:9092"]);
    assert_eq!(loaded.extract::<CoreConfig>().unwrap().level, "debug");
}

#[test]
fn a_plugin_validates_its_own_settings_and_says_where_they_came_from() {
    let loaded = loader()
        .set("transports.kafka.enabled", "true")
        .unwrap()
        .set("transports.kafka.concurrency", "0")
        .unwrap()
        .load();

    let Err(err) = (KAFKA.configure)(&loaded.worker("kafka")) else {
        panic!("zero concurrency is the plugin's own rule");
    };
    assert_eq!(err.key, "transports.kafka.concurrency");
    assert!(err.message.contains("at least 1"), "{}", err.message);
    // Provenance survives: the same bad number from a file and from a flag are
    // different problems to go and fix.
    assert!(err.source.is_some(), "the source of the value is reported");
}

#[test]
fn a_server_is_selected_by_name() {
    let loaded = loader().load();
    assert_eq!(loaded.active_server("sqlite"), "sqlite");

    let loaded = loader().set("servers.active", "scylladb").unwrap().load();
    assert_eq!(loaded.active_server("sqlite"), "scylladb");
}

#[test]
fn the_typed_config_never_leaves_the_plugins_crate() {
    // The factory closure owns it; what comes back out is a port trait object.
    let loaded = loader()
        .set("transports.kafka.enabled", "true")
        .unwrap()
        .set("transports.kafka.brokers", r#"["a:9092"]"#)
        .unwrap()
        .load();

    let factory = (KAFKA.configure)(&loaded.worker("kafka")).unwrap().unwrap();
    let worker: Arc<dyn ResonateWorker> =
        factory(std::sync::Weak::<Dead>::new() as std::sync::Weak<dyn ResonateServer>);
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
