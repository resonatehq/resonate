//! Layered configuration, assembled from the registry rather than restated.
//!
//! The server's own defaults name no plugin. Every plugin contributes its own
//! defaults through its `defaults` function, and reads its settings back at a
//! key derived from its id — so a config file, a `RESONATE_*` variable and a
//! `--set` path cannot drift apart from each other or from the plugin.
//!
//! No figment type appears in this module's public API. The layering is
//! figment's; the surface plugin crates pin is not.

use serde::de::DeserializeOwned;

use figment::{
    providers::{Env as FigEnv, Format, Serialized, Toml},
    Figment, Provider,
};

use crate::error::ConfigError;

/// Builds the layered configuration: defaults, then a file, then the
/// environment, then explicit overrides. Each layer wins over the last.
///
/// It does not know the registry. A plugin's defaults are the `#[serde(default)]`
/// on its own `Config` — one source of truth rather than a struct and a JSON
/// snapshot of the same struct — and [`Settings::extract`] reads a section that
/// is absent as an empty one, so a plugin nobody has configured gets its own
/// defaults without anything being seeded on its behalf.
#[derive(Default)]
pub struct Loader {
    figment: Figment,
}

impl Loader {
    pub fn new() -> Self {
        Self::default()
    }

    /// The server's own defaults — everything that is not a plugin.
    pub fn defaults<T: serde::Serialize>(mut self, core: T) -> Self {
        self.figment = Figment::from(Serialized::defaults(core)).merge(self.figment);
        self
    }

    /// An optional TOML file. Missing is not an error — a server with no config
    /// file runs on defaults and the environment.
    pub fn file(mut self, path: impl AsRef<std::path::Path>) -> Self {
        self.figment = self.figment.merge(Toml::file(path.as_ref()));
        self
    }

    /// `PREFIX_SECTION__ID__FIELD`, double underscore for nesting.
    pub fn env(mut self, prefix: &str) -> Self {
        self.figment = self.figment.merge(FigEnv::prefixed(prefix).split("__"));
        self
    }

    /// One `--set key=value`. Highest precedence, and the reason the CLI needs
    /// no flag per plugin field: the key space is the config's, so it covers
    /// every plugin that exists or ever will.
    ///
    /// The value is parsed as TOML, so `8`, `true` and `["a", "b"]` arrive as a
    /// number, a bool and a list rather than as strings.
    pub fn set(mut self, key: &str, value: &str) -> Result<Self, ConfigError> {
        // A dotted TOML key is exactly the path syntax `--set` already uses, so
        // one line of TOML expresses the whole assignment — including a list or
        // a table, which a string-valued override could not carry.
        let assignment = format!("{key} = {value}");
        self.figment = match Toml::string(&assignment).data() {
            Ok(_) => self.figment.merge(Toml::string(&assignment)),
            // Unquoted, so it was meant as a string: `--set level=debug`.
            Err(_) => {
                let quoted = format!("{key} = {}", toml_quote(value));
                Toml::string(&quoted)
                    .data()
                    .map_err(|e| ConfigError::from_source(key, e.to_string(), "--set"))?;
                self.figment.merge(Toml::string(&quoted))
            }
        };
        Ok(self)
    }

    pub fn load(self) -> Loaded {
        Loaded {
            figment: self.figment,
        }
    }
}

/// Configuration, merged and ready to be handed out one plugin at a time.
pub struct Loaded {
    figment: Figment,
}

impl Loaded {
    /// `servers.<id>`.
    pub fn server(&self, id: &str) -> Settings<'_> {
        self.at("servers", id)
    }

    /// `transports.<id>`.
    pub fn worker(&self, id: &str) -> Settings<'_> {
        self.at("transports", id)
    }

    /// `gateways.<id>`.
    pub fn gateway(&self, id: &str) -> Settings<'_> {
        self.at("gateways", id)
    }

    /// Which server this configuration points at, or the fallback when it says
    /// nothing.
    pub fn active_server(&self, fallback: &str) -> String {
        self.figment
            .extract_inner::<String>("servers.active")
            .unwrap_or_else(|_| fallback.to_string())
    }

    /// The server's own configuration — everything that is not a plugin's.
    pub fn extract<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        self.figment.extract().map_err(|e| to_config_error("", &e))
    }

    fn at(&self, section: &str, id: &str) -> Settings<'_> {
        Settings {
            figment: &self.figment,
            key: format!("{section}.{id}"),
        }
    }
}

/// One plugin's slice of the configuration.
pub struct Settings<'a> {
    figment: &'a Figment,
    key: String,
}

impl Settings<'_> {
    /// Where this plugin's settings live — `transports.kafka`. Worth naming in
    /// an error: it is what the operator has to go and edit.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// This plugin's whole `Config`, deserialized.
    ///
    /// The typed value never leaves the plugin's crate: it is captured by the
    /// factory closure returned alongside it, so the framework holds a port
    /// trait object and nothing else.
    pub fn extract<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        // An empty table underneath, so a plugin nobody has configured still
        // has a section and serde's own defaults fill it in. Without this every
        // plugin would have to hand the loader a snapshot of its own defaults,
        // which is the same values written down twice.
        Figment::from(Serialized::default(&self.key, figment::value::Dict::new()))
            .merge(self.figment)
            .extract_inner(&self.key)
            .map_err(|e| to_config_error(&self.key, &e))
    }

    /// Reject a value the plugin's own rules refuse, naming where it came from.
    pub fn reject(&self, field: &str, message: impl Into<String>) -> ConfigError {
        let key = format!("{}.{}", self.key, field);
        match self.figment.find_metadata(&key) {
            Some(md) => ConfigError::from_source(key.clone(), message, describe(md)),
            None => ConfigError::new(key, message),
        }
    }
}

/// TOML-quote a bare string so `--set level=debug` means what it looks like.
fn toml_quote(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

fn describe(md: &figment::Metadata) -> String {
    match &md.source {
        Some(src) => format!("{} {}", md.name, src),
        None => md.name.to_string(),
    }
}

fn to_config_error(base: &str, e: &figment::Error) -> ConfigError {
    let path = e.path.join(".");
    let key = match (base.is_empty(), path.is_empty()) {
        (true, true) => "configuration".to_string(),
        (true, false) => path,
        (false, true) => base.to_string(),
        (false, false) => format!("{base}.{path}"),
    };
    ConfigError {
        key,
        message: e.kind.to_string(),
        source: e.metadata.as_ref().map(describe),
    }
}
