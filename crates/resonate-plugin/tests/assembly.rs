//! What a binary's plugin set has to satisfy, and how settings reach a plugin.
//!
//! Every check here is one that used to be impossible: without a registry
//! there is no set of valid keys to compare a config file against, and no set
//! of compiled-in plugins to compare a selection against.

use std::sync::Arc;

use async_trait::async_trait;
use resonate_plugin::{
    ConfigError, Env, Manifest, Port, Registry, RegistryError, ResonateServer, ResonateWorker,
    Settings, WorkerCtx, WorkerFactory, WorkerPlugin,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

// ─── A plugin, written the way a third party would write one ─────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct KafkaConfig {
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

fn kafka_configure(settings: &Settings<'_>) -> Result<WorkerFactory, ConfigError> {
    let config: KafkaConfig = settings.extract()?;
    if config.concurrency == 0 {
        // Zero permits means every message queues forever, so it is refused at
        // startup rather than hung on later — and the plugin says so itself,
        // in the one place that knows.
        return Err(settings.reject("concurrency", "must be at least 1 (got 0)"));
    }
    Ok(Box::new(move |_ctx: &WorkerCtx| {
        Arc::new(KafkaWorker { config }) as Arc<dyn ResonateWorker>
    }))
}

static KAFKA: WorkerPlugin = WorkerPlugin::new(
    Manifest::new("kafka", "resonate-worker-kafka", "1.0.0")
        .with_summary("Deliver by producing to a Kafka topic")
        .with_schemes(&["kafka"]),
    || serde_json::to_value(KafkaConfig::default()).unwrap(),
    kafka_configure,
);

/// A second plugin that claims the same scheme, for the collision checks.
static KAFKA_RIVAL: WorkerPlugin = WorkerPlugin::new(
    Manifest::new("kafkaesque", "other-kafka-crate", "0.1.0").with_schemes(&["kafka"]),
    || json!({}),
    kafka_configure,
);

/// A worker that claims nothing, so nothing could ever route to it.
static MUTE: WorkerPlugin = WorkerPlugin::new(
    Manifest::new("mute", "resonate-worker-mute", "0.1.0"),
    || json!({}),
    kafka_configure,
);

// ─── The set of plugins a binary carries ─────────────────────────────────────

#[test]
fn a_scheme_claimed_twice_is_refused_before_anything_runs() {
    let errors = Registry::new()
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
        .worker(&MUTE)
        .check()
        .expect_err("unreachable worker");
    assert!(errors
        .iter()
        .any(|e| matches!(e, RegistryError::NoSchemes { .. })));
}

#[test]
fn one_id_at_one_port_is_refused() {
    let errors = Registry::new()
        .worker(&KAFKA)
        .worker(&KAFKA)
        .check()
        .expect_err("one id, two plugins");
    assert!(errors
        .iter()
        .any(|e| matches!(e, RegistryError::DuplicateId { .. })));
}

#[test]
fn selecting_a_server_the_binary_does_not_carry_names_what_it_does() {
    let registry = Registry::new().worker(&KAFKA);
    let err = registry
        .select_server("scylladb")
        .expect_err("no server plugins were registered");
    let rendered = err.to_string();
    // The distinction the old string match could not make: not misconfigured,
    // *not compiled in* — and the message says which is which.
    assert!(
        rendered.contains("not compiled into this binary"),
        "{rendered}"
    );
    assert!(rendered.contains("scylladb"), "{rendered}");
}

#[test]
fn the_registry_reports_what_it_can_route() {
    let registry = Registry::new().worker(&KAFKA);
    assert_eq!(registry.schemes(), vec![("kafka", "kafka")]);
    assert_eq!(registry.ids(Port::Worker), vec!["kafka".to_string()]);
    let entry = registry.entries().into_iter().next().unwrap();
    assert_eq!(entry.config_key(), "transports.kafka");
    assert_eq!(
        entry.manifest.summary,
        "Deliver by producing to a Kafka topic"
    );
}

// ─── Configuration ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct CoreConfig {
    level: String,
}

fn loader(registry: &Registry) -> resonate_plugin::Loader {
    resonate_plugin::Loader::new(registry).defaults(CoreConfig {
        level: "info".to_string(),
    })
}

#[test]
fn defaults_are_assembled_from_the_registry() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry).load();

    // The server's own defaults name no plugin; this value came from the
    // plugin's crate, through its manifest.
    let config: KafkaConfig = loaded.settings(Port::Worker, "kafka").extract().unwrap();
    assert_eq!(config, KafkaConfig::default());
    assert_eq!(loaded.extract::<CoreConfig>().unwrap().level, "info");
}

#[test]
fn enabled_is_the_frameworks_field_not_the_plugins() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry).load();
    // KafkaConfig has no `enabled` field at all — the flag is hoisted, so the
    // registration loop is uniform and no plugin repeats it.
    assert!(loaded.settings(Port::Worker, "kafka").enabled(true));

    let loaded = loader(&registry)
        .set("transports.kafka.enabled", "false")
        .unwrap()
        .load();
    assert!(!loaded.settings(Port::Worker, "kafka").enabled(true));
}

#[test]
fn set_carries_types_not_just_strings() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .set("transports.kafka.concurrency", "8")
        .unwrap()
        .set("transports.kafka.brokers", r#"["a:9092", "b:9092"]"#)
        .unwrap()
        // Unquoted, so it is meant as a string.
        .set("level", "debug")
        .unwrap()
        .load();

    let config: KafkaConfig = loaded.settings(Port::Worker, "kafka").extract().unwrap();
    assert_eq!(config.concurrency, 8);
    assert_eq!(config.brokers, vec!["a:9092", "b:9092"]);
    assert_eq!(loaded.extract::<CoreConfig>().unwrap().level, "debug");
}

#[test]
fn a_plugin_validates_its_own_settings_and_says_where_they_came_from() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .set("transports.kafka.concurrency", "0")
        .unwrap()
        .load();

    let Err(err) = (KAFKA.configure)(&loaded.settings(Port::Worker, "kafka")) else {
        panic!("zero concurrency is the plugin's own rule");
    };
    assert_eq!(err.key, "transports.kafka.concurrency");
    assert!(err.message.contains("at least 1"), "{}", err.message);
    // Provenance survives: the same bad number from a file and from a flag are
    // different problems to go and fix.
    assert!(err.source.is_some(), "the source of the value is reported");
}

#[test]
fn a_misspelled_key_is_reported_rather_than_ignored() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .set("transports.kafkaa.concurrency", "8")
        .unwrap()
        .set("transports.kafka.concurrency", "8")
        .unwrap()
        .load();

    let unknown = loaded.unknown_keys(&registry, &["level"]);
    assert_eq!(
        unknown,
        vec!["transports.kafkaa".to_string()],
        "a typo'd section reaches the server today and is silently never read"
    );
}

#[test]
fn a_singletons_selection_key_is_not_mistaken_for_a_plugin() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .set("servers.active", "sqlite")
        .unwrap()
        .load();
    assert!(loaded.unknown_keys(&registry, &["level"]).is_empty());
    assert_eq!(loaded.selected(Port::Server, "sqlite"), "sqlite");
}

#[test]
fn the_typed_config_never_leaves_the_plugins_crate() {
    // The factory closure owns it; what comes back out is a port trait object.
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .set("transports.kafka.brokers", r#"["a:9092"]"#)
        .unwrap()
        .load();

    let factory = (KAFKA.configure)(&loaded.settings(Port::Worker, "kafka")).unwrap();
    let ctx = WorkerCtx::new(
        std::sync::Weak::<Dead>::new() as std::sync::Weak<dyn ResonateServer>,
        Env::new(false, 15_000, 30_000, None),
    );
    let worker: Arc<dyn ResonateWorker> = factory(&ctx);
    // Nothing about KafkaConfig is reachable from here, which is the point.
    let _ = worker;
}

/// A `ResonateServer` that never exists — only its `Weak` is used, to stand in
/// for the handle a worker is given before the ring is closed.
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

// ─── Migrating a key ─────────────────────────────────────────────────────────

#[test]
fn a_retired_key_still_works_and_says_so() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .alias("messages.kafka", "transports.kafka")
        .set("messages.kafka.concurrency", "4")
        .unwrap()
        .load();

    let config: KafkaConfig = loaded.settings(Port::Worker, "kafka").extract().unwrap();
    assert_eq!(config.concurrency, 4, "a deployed config keeps working");
    assert_eq!(
        loaded.deprecated_keys(),
        &[("messages.kafka".to_string(), "transports.kafka".to_string())],
        "and is named once at startup rather than silently honoured forever"
    );
}

#[test]
fn the_current_key_wins_when_a_config_names_both() {
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .alias("messages.kafka", "transports.kafka")
        .set("messages.kafka.concurrency", "4")
        .unwrap()
        .set("transports.kafka.concurrency", "9")
        .unwrap()
        .load();

    let config: KafkaConfig = loaded.settings(Port::Worker, "kafka").extract().unwrap();
    assert_eq!(config.concurrency, 9);
    assert!(
        loaded.deprecated_keys().is_empty(),
        "nothing was carried, so there is nothing to warn about"
    );
}

#[test]
fn an_alias_does_not_outrank_a_plugins_own_default() {
    // The failure this guards: a legacy key that is merely *absent* must not
    // shadow the defaults layer, and a default at the new key must not shadow
    // a value someone actually wrote at the old one.
    let registry = Registry::new().worker(&KAFKA);
    let loaded = loader(&registry)
        .alias("messages.kafka", "transports.kafka")
        .load();
    let config: KafkaConfig = loaded.settings(Port::Worker, "kafka").extract().unwrap();
    assert_eq!(config.concurrency, default_concurrency());
    assert!(loaded.deprecated_keys().is_empty());
}
