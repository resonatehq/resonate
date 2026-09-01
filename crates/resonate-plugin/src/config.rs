//! Layered configuration, assembled from the registry rather than restated.
//!
//! The server's own defaults name no plugin. Every plugin contributes its own
//! defaults layer through [`Manifest`](crate::Manifest) and its `defaults`
//! function, and reads its settings back out at a key derived from its port and
//! its id — so a config file, a `RESONATE_*` variable and a `--set` path cannot
//! drift apart from each other or from the crate.
//!
//! No figment type appears in this module's public API. The layering is
//! figment's; the surface 400 plugin crates pin is not.

use serde::de::DeserializeOwned;

use figment::{
    providers::{Env as FigEnv, Format, Serialized, Toml},
    Figment, Provider,
};

use crate::error::ConfigError;
use crate::manifest::Port;
use crate::registry::Registry;

/// The key under a singleton port's section that names the active plugin:
/// `servers.active`, `routers.active`.
pub const ACTIVE: &str = "active";

/// The framework-owned key under every plugin's section.
///
/// Hoisted out of plugin configuration so the registration loop is uniform and
/// no plugin repeats the field. A singleton port ignores it — those are
/// selected by name, not switched on.
pub const ENABLED: &str = "enabled";

/// Builds the layered configuration: defaults, then a file, then the
/// environment, then explicit overrides. Each layer wins over the last.
pub struct Loader {
    /// What plugins and the server declare when nobody has said anything.
    defaults: Figment,
    /// What someone actually wrote: a file, the environment, a `--set`.
    ///
    /// Kept apart from the defaults rather than collapsed into one figment,
    /// because "was this set, or is it just the default?" is a question two of
    /// the things below have to ask, and metadata cannot answer it reliably.
    explicit: Figment,
    aliases: Vec<(String, String)>,
}

impl Loader {
    /// Start from the defaults every registered plugin declares.
    ///
    /// This is the layer that used to be a `Config::default()` naming every
    /// plugin by hand. Now it is assembled, so a plugin the binary does not
    /// carry contributes nothing and cannot be configured by accident.
    pub fn new(registry: &Registry) -> Self {
        let mut defaults = Figment::new();
        for (port, id, plugin_defaults, default_enabled) in registry.default_layers() {
            let key = format!("{}.{}", port.section(), id);
            defaults = defaults.merge(Serialized::default(&key, plugin_defaults));
            if !port.is_singleton() {
                defaults = defaults.merge(Serialized::default(
                    &format!("{key}.{ENABLED}"),
                    default_enabled,
                ));
            }
        }
        Self {
            defaults,
            explicit: Figment::new(),
            aliases: Vec::new(),
        }
    }

    /// Merge the server's own defaults — everything that is not a plugin.
    pub fn defaults<T: serde::Serialize>(mut self, core: T) -> Self {
        // Under the plugin layer, not over it: a plugin owns its own section
        // and the server's defaults must not reach into it.
        self.defaults = Figment::from(Serialized::defaults(core)).merge(self.defaults);
        self
    }

    /// An optional TOML file. Missing is not an error — a server with no
    /// config file runs on defaults and the environment.
    pub fn file(mut self, path: impl AsRef<std::path::Path>) -> Self {
        self.explicit = self.explicit.merge(Toml::file(path.as_ref()));
        self
    }

    /// `PREFIX_SECTION__ID__FIELD`, double underscore for nesting.
    pub fn env(mut self, prefix: &str) -> Self {
        self.explicit = self.explicit.merge(FigEnv::prefixed(prefix).split("__"));
        self
    }

    /// One `--set key=value`. Highest precedence, and the reason the CLI does
    /// not need a flag per plugin field: the key space is the config's, so it
    /// covers every plugin that exists or ever will.
    ///
    /// The value is parsed as TOML, so `8`, `true` and `["a", "b"]` arrive as
    /// a number, a bool and a list rather than as strings.
    pub fn set(mut self, key: &str, value: &str) -> Result<Self, ConfigError> {
        // A dotted TOML key is exactly the path syntax `--set` already uses, so
        // one line of TOML expresses the whole assignment — including a list or
        // a table, which a string-valued override could not carry.
        let assignment = format!("{key} = {value}");
        self.explicit = match Toml::string(&assignment).data() {
            Ok(_) => self.explicit.merge(Toml::string(&assignment)),
            // Unquoted, so it was meant as a string: `--set level=debug`.
            Err(_) => {
                let quoted = format!("{key} = {}", toml_quote(value));
                Toml::string(&quoted)
                    .data()
                    .map_err(|e| ConfigError::from_source(key, e.to_string(), "--set"))?;
                self.explicit.merge(Toml::string(&quoted))
            }
        };
        Ok(self)
    }

    /// Accept a key this server used to read, at the key it reads now.
    ///
    /// Deriving a plugin's config key from its port and id is what stops the
    /// file, the environment variable and the `--set` path drifting apart —
    /// but a server already deployed has the old key in its files. The alias
    /// fills in only when nothing was written at the new key, so a config that
    /// names both is not ambiguous: the current name wins.
    pub fn alias(mut self, old: &str, new: &str) -> Self {
        self.aliases.push((old.to_string(), new.to_string()));
        self
    }

    pub fn load(self) -> Loaded {
        let Self {
            defaults,
            explicit,
            aliases,
        } = self;

        let mut carried = Figment::new();
        let mut deprecated = Vec::new();
        for (old, new) in &aliases {
            let Ok(value) = explicit.find_value(old) else {
                continue;
            };
            if explicit.find_value(new).is_ok() {
                continue;
            }
            carried = carried.merge(Serialized::default(new, value));
            deprecated.push((old.clone(), new.clone()));
        }

        Loaded {
            figment: defaults.merge(carried).merge(explicit),
            deprecated,
        }
    }
}

/// Configuration, merged and ready to be handed out one plugin at a time.
pub struct Loaded {
    figment: Figment,
    deprecated: Vec<(String, String)>,
}

impl Loaded {
    /// Deprecated keys this configuration actually used, and what replaced
    /// them. Worth one warning at startup each: silence is how a key nobody
    /// reads any more goes on looking like it works.
    pub fn deprecated_keys(&self) -> &[(String, String)] {
        &self.deprecated
    }

    /// This plugin's slice of the configuration.
    pub fn settings(&self, port: Port, id: &str) -> Settings<'_> {
        Settings {
            figment: &self.figment,
            key: format!("{}.{}", port.section(), id),
        }
    }

    /// Extract the server's own configuration — everything that is not a
    /// plugin's.
    pub fn extract<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        self.figment.extract().map_err(|e| to_config_error("", &e))
    }

    /// Which plugin a singleton port has been pointed at, or the fallback when
    /// configuration says nothing.
    pub fn selected(&self, port: Port, fallback: &str) -> String {
        self.figment
            .extract_inner::<String>(&format!("{}.{}", port.section(), ACTIVE))
            .unwrap_or_else(|_| fallback.to_string())
    }

    /// Settings that reached the server but that nothing will ever read.
    ///
    /// Only answerable because a registry exists: without one there is no set
    /// of valid keys to compare against, which is why a misspelled
    /// `RESONATE_TRANSPORTS__GCPS__CONCURENCY` is silently ignored today.
    /// `core` is the server's own top-level keys.
    pub fn unknown_keys(&self, registry: &Registry, core: &[&str]) -> Vec<String> {
        let Ok(root) = self.figment.extract::<figment::value::Dict>() else {
            return Vec::new();
        };
        let sections = [Port::Gateway, Port::Server, Port::Router, Port::Worker];
        let mut unknown = Vec::new();

        for (key, value) in &root {
            if core.contains(&key.as_str()) {
                continue;
            }
            let Some(port) = sections.iter().find(|p| p.section() == key) else {
                unknown.push(key.clone());
                continue;
            };
            let Some(dict) = value.as_dict() else {
                unknown.push(key.clone());
                continue;
            };
            let known = registry.ids(*port);
            for id in dict.keys() {
                if port.is_singleton() && id == ACTIVE {
                    continue;
                }
                if !known.iter().any(|k| k == id) {
                    unknown.push(format!("{key}.{id}"));
                }
            }
        }
        unknown.sort();
        unknown
    }
}

/// One plugin's slice of the configuration, and where each value came from.
pub struct Settings<'a> {
    figment: &'a Figment,
    key: String,
}

impl<'a> Settings<'a> {
    /// Where this plugin's settings live — `transports.kafka`. Worth naming in
    /// an error message: it is what the operator has to go and edit.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// This plugin's whole `Config`, deserialized.
    ///
    /// The typed value never leaves the plugin's crate: it is captured by the
    /// factory closure returned alongside it, so the framework holds a port
    /// trait object and nothing else.
    pub fn extract<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        self.figment
            .extract_inner(&self.key)
            .map_err(|e| to_config_error(&self.key, &e))
    }

    /// One field, for a plugin that wants to read a value without a struct.
    pub fn extract_at<T: DeserializeOwned>(&self, field: &str) -> Result<T, ConfigError> {
        let key = format!("{}.{}", self.key, field);
        self.figment
            .extract_inner(&key)
            .map_err(|e| to_config_error(&key, &e))
    }

    /// The framework-owned `enabled` flag.
    pub fn enabled(&self, default: bool) -> bool {
        self.extract_at::<bool>(ENABLED).unwrap_or(default)
    }

    /// Where a value was read from — a file, an environment variable, a
    /// `--set` — so an error can send someone to the right place rather than
    /// naming a number.
    pub fn source_of(&self, field: &str) -> Option<String> {
        source_of(self.figment, &format!("{}.{}", self.key, field))
    }

    /// Reject a value the plugin's own rules refuse, naming where it came from.
    pub fn reject(&self, field: &str, message: impl Into<String>) -> ConfigError {
        let key = format!("{}.{}", self.key, field);
        match source_of(self.figment, &key) {
            Some(src) => ConfigError::from_source(key, message, src),
            None => ConfigError::new(key, message),
        }
    }
}

/// A [`Settings`] that owns its configuration, for tests and for a plugin
/// exercised outside a server.
pub struct OwnedSettings {
    figment: Figment,
    key: String,
}

impl OwnedSettings {
    /// Settings over nothing but the values given.
    pub fn new(key: impl Into<String>, value: serde_json::Value) -> Self {
        let key = key.into();
        Self {
            figment: Figment::from(Serialized::default(&key, value)),
            key,
        }
    }

    pub fn as_settings(&self) -> Settings<'_> {
        Settings {
            figment: &self.figment,
            key: self.key.clone(),
        }
    }
}

fn source_of(figment: &Figment, key: &str) -> Option<String> {
    let md = figment.find_metadata(key)?;
    Some(match &md.source {
        Some(src) => format!("{} {}", md.name, src),
        None => md.name.to_string(),
    })
}

fn to_config_error(base: &str, e: &figment::Error) -> ConfigError {
    let path = e.path.join(".");
    let key = match (base.is_empty(), path.is_empty()) {
        (true, true) => "configuration".to_string(),
        (true, false) => path,
        (false, true) => base.to_string(),
        (false, false) => format!("{base}.{path}"),
    };
    let source = e.metadata.as_ref().map(|md| match &md.source {
        Some(src) => format!("{} {}", md.name, src),
        None => md.name.to_string(),
    });
    ConfigError {
        key,
        message: e.kind.to_string(),
        source,
    }
}

/// TOML-quote a bare string so `--set level=debug` means what it looks like.
fn toml_quote(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}
