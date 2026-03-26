use std::collections::HashMap;
use std::time::Duration;

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

/// Retry policy configuration.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub delay: Duration,
    pub backoff_factor: f64,
}

/// Helper for ID prefixing.
#[derive(Clone)]
pub struct OptionsBuilder {
    id_prefix: String,
}

impl OptionsBuilder {
    pub fn new(id_prefix: String) -> Self {
        Self { id_prefix }
    }

    /// Prepend the configured prefix to an ID.
    pub fn prefix_id(&self, id: &str) -> String {
        if self.id_prefix.is_empty() {
            id.to_string()
        } else {
            format!("{}{}", self.id_prefix, id)
        }
    }
}

/// Check if a string looks like a URL (has a scheme with "://").
/// Mirrors the TS `util.isUrl` check.
pub(crate) fn is_url(s: &str) -> bool {
    s.contains("://")
}
