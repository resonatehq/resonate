//! What a plugin reports when it cannot be configured, and what a *set* of
//! plugins reports when it does not hang together.

/// A plugin's settings were missing, malformed, or invalid.
///
/// Carries where the value came from, not just what was wrong with it: the same
/// bad number reached the server from a file, an environment variable or a
/// `--set`, and only one of those is worth telling someone to go and fix.
#[derive(Debug, Clone)]
pub struct ConfigError {
    /// The setting, as a config-file path — `transports.kafka.brokers`.
    pub key: String,
    /// What is wrong with it, in the plugin's own words.
    pub message: String,
    /// Where the offending value was read from, when that is known.
    pub source: Option<String>,
}

impl ConfigError {
    pub fn new(key: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            message: message.into(),
            source: None,
        }
    }

    pub fn from_source(
        key: impl Into<String>,
        message: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            message: message.into(),
            source: Some(source.into()),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.key, self.message)?;
        if let Some(src) = &self.source {
            write!(f, " (from {src})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

/// Something is wrong with the *set* of plugins a binary was assembled from,
/// rather than with any one of them.
///
/// Every one is answerable from manifests alone — no configuration, nothing
/// constructed — so a build can be checked before it is compiled.
#[derive(Debug, Clone)]
pub enum RegistryError {
    /// Two plugins of one kind answer to one id.
    DuplicateId {
        kind: &'static str,
        id: String,
        krates: (String, String),
    },
    /// A binary assembled with no server is not a server.
    NoServer,
    /// Configuration selected a server this binary does not carry. The
    /// distinction that matters: not compiled in, rather than misconfigured.
    NotCompiledIn {
        requested: String,
        available: Vec<String>,
    },
    /// Two workers claim one address scheme. Never resolved by registration
    /// order: whichever won would be a coin flip nobody could read off the
    /// config.
    DuplicateScheme {
        scheme: String,
        krates: (String, String),
    },
    /// A worker registered without claiming anything, so nothing could ever
    /// route to it.
    NoSchemes { id: String, krate: String },
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::DuplicateId {
                kind,
                id,
                krates: (a, b),
            } => write!(f, "two {kind} plugins answer to '{id}': {a} and {b}"),
            RegistryError::NoServer => {
                write!(f, "this binary was assembled without a server")
            }
            RegistryError::NotCompiledIn {
                requested,
                available,
            } => write!(
                f,
                "server '{requested}' is not compiled into this binary; it carries: {}",
                if available.is_empty() {
                    "none".to_string()
                } else {
                    available.join(", ")
                }
            ),
            RegistryError::DuplicateScheme {
                scheme,
                krates: (a, b),
            } => write!(
                f,
                "'{scheme}://' is claimed by both {a} and {b} — a binary can carry only one"
            ),
            RegistryError::NoSchemes { id, krate } => write!(
                f,
                "worker '{id}' ({krate}) claims no address scheme, so nothing could route to it"
            ),
        }
    }
}

impl std::error::Error for RegistryError {}
