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
}

impl Default for Options {
    fn default() -> Self {
        Self {
            tags: HashMap::new(),
            target: "default".to_string(),
            timeout: Duration::from_secs(86_400), // 24 hours (24 * 60 * 60), matches TS SDK
            version: 0,
        }
    }
}

/// Check if a string looks like a URL (has a scheme with "://").
/// Mirrors the TS `util.isUrl` check.
pub(crate) fn is_url(s: &str) -> bool {
    s.contains("://")
}
