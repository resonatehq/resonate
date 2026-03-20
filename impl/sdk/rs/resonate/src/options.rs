use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::network::Network;

/// Per-call configuration options (fully resolved).
#[derive(Debug, Clone)]
pub struct Options {
    /// Custom tags for the promise.
    pub tags: HashMap<String, String>,
    /// Target for RPC routing (resolved via network.match).
    pub target: String,
    /// Timeout duration (added to current time for timeoutAt).
    pub timeout: Duration,
    /// Function version.
    pub version: u32,
    /// Retry policy.
    pub retry_policy: Option<RetryPolicy>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            tags: HashMap::new(),
            target: "default".to_string(),
            timeout: Duration::from_secs(86_400), // 24 hours (24 * 60 * 60), matches TS SDK
            version: 0,
            retry_policy: None,
        }
    }
}

/// User-provided partial options. Any field left as `None` gets a default in `OptionsBuilder::build()`.
#[derive(Debug, Clone, Default)]
pub struct PartialOptions {
    /// Custom tags for the promise.
    pub tags: Option<HashMap<String, String>>,
    /// Target for RPC routing. Bare names are resolved via `network.match()`;
    /// URL targets (containing `://`) pass through unchanged.
    pub target: Option<String>,
    /// Timeout duration.
    pub timeout: Option<Duration>,
    /// Function version.
    pub version: Option<u32>,
    /// Retry policy.
    pub retry_policy: Option<RetryPolicy>,
}

/// Retry policy configuration.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub delay: Duration,
    pub backoff_factor: f64,
}

/// Builder for resolving and merging options.
#[derive(Clone)]
pub struct OptionsBuilder {
    network: Arc<dyn Network>,
    id_prefix: String,
}

impl OptionsBuilder {
    pub fn new(network: Arc<dyn Network>, id_prefix: String) -> Self {
        Self { network, id_prefix }
    }

    /// Prepend the configured prefix to an ID.
    pub fn prefix_id(&self, id: &str) -> String {
        if self.id_prefix.is_empty() {
            id.to_string()
        } else {
            format!("{}{}", self.id_prefix, id)
        }
    }

    /// Merge user-provided partial options with defaults and resolve the target
    /// through `network.match()`.
    ///
    /// - If `target` is `None`, defaults to `"default"`.
    /// - If `target` looks like a URL (contains `://`), it passes through unchanged.
    /// - Otherwise, `target` is resolved via `network.match(target)`.
    pub fn build(&self, opts: Option<PartialOptions>) -> Options {
        let defaults = Options::default();
        match opts {
            None => {
                let resolved_target = self.resolve_target(&defaults.target);
                Options {
                    target: resolved_target,
                    ..defaults
                }
            }
            Some(partial) => {
                let raw_target = partial.target.unwrap_or(defaults.target);
                let resolved_target = self.resolve_target(&raw_target);

                Options {
                    tags: partial.tags.unwrap_or(defaults.tags),
                    target: resolved_target,
                    timeout: partial.timeout.unwrap_or(defaults.timeout),
                    version: partial.version.unwrap_or(defaults.version),
                    retry_policy: partial.retry_policy.or(defaults.retry_policy),
                }
            }
        }
    }

    /// Resolve a target string: URLs pass through, bare names go through `network.match()`.
    fn resolve_target(&self, target: &str) -> String {
        if is_url(target) {
            target.to_string()
        } else {
            self.network.r#match(target)
        }
    }
}

/// Check if a string looks like a URL (has a scheme with "://").
/// Mirrors the TS `util.isUrl` check.
pub(crate) fn is_url(s: &str) -> bool {
    s.contains("://")
}
