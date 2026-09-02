use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Top-level configuration.
///
/// Layered loading order: defaults -> resonate.toml (optional) -> env vars.
/// Each layer overrides the previous.
///
/// Environment variables use `RESONATE_` prefix with double-underscore nesting:
///   RESONATE_SERVER__PORT=3000
///   RESONATE_STORAGE__TYPE=postgres
///   RESONATE_STORAGE__POSTGRES__URL=postgres://...
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Log level: debug, info, warn, error
    #[serde(default = "default_level")]
    pub level: String,

    /// Enable debug mode
    #[serde(default)]
    pub debug: bool,

    /// Server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Storage backend configuration
    #[serde(default)]
    pub storage: StorageConfig,

    /// Authentication configuration. Absent = auth disabled.
    ///
    /// The crate's own type, like every transport's: `resonate-auth` describes
    /// its configuration, reads its own key material, and this only carries the
    /// section from the file to it.
    #[serde(default)]
    pub auth: Option<resonate_auth::Config>,

    /// Task configuration
    #[serde(default)]
    pub tasks: TasksConfig,

    /// Timeout processing configuration
    #[serde(default)]
    pub timeouts: TimeoutsConfig,

    /// Transport configuration
    #[serde(default)]
    pub transports: TransportsConfig,

    /// Observability configuration
    #[serde(default)]
    pub observability: ObservabilityConfig,

    /// The web console.
    ///
    /// The crate's own type, like auth and every transport: this only carries
    /// the section from the file to the thing that reads it.
    #[serde(default)]
    pub console: resonate_gateway_web::Config,
}

fn default_level() -> String {
    "info".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CorsConfig {
    /// Allowed origins. Empty = CORS disabled. Use ["*"] for permissive access.
    #[serde(default)]
    pub allow_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP server host
    #[serde(default = "default_host")]
    pub host: String,

    /// HTTP server port
    #[serde(default = "default_port")]
    pub port: u16,

    /// Bind address
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Graceful shutdown timeout (ms)
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout: u64,

    /// External server URL included in response headers.
    /// Defaults to http://{host}:{port} if not set.
    #[serde(default)]
    pub url: Option<String>,

    /// CORS configuration
    #[serde(default)]
    pub cors: CorsConfig,
}

fn default_host() -> String {
    "localhost".to_string()
}
fn default_port() -> u16 {
    8001
}
fn default_bind() -> String {
    "0.0.0.0".to_string()
}
fn default_shutdown_timeout() -> u64 {
    10000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            bind: default_bind(),
            shutdown_timeout: default_shutdown_timeout(),
            url: None,
            cors: CorsConfig::default(),
        }
    }
}

/// Storage backend configuration.
///
/// The `type` field selects the active backend ("sqlite", "postgres", or "mysql").
/// Backend-specific settings are in the `sqlite`, `postgres`, and `mysql` sub-structs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Active backend: "sqlite", "postgres", or "mysql"
    #[serde(default = "default_storage_type", rename = "type")]
    pub storage_type: String,

    /// SQLite-specific configuration
    #[serde(default)]
    pub sqlite: SqliteConfig,

    /// PostgreSQL-specific configuration
    #[serde(default)]
    pub postgres: PostgresConfig,

    /// MySQL-specific configuration
    #[serde(default)]
    pub mysql: MysqlConfig,
}

fn default_storage_type() -> String {
    "sqlite".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    /// Path to SQLite database file
    #[serde(default = "default_db_path")]
    pub path: String,

    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,

    /// Apply pending migrations to an existing database.
    ///
    /// An empty database is always created. Beyond that, a schema behind the
    /// binary is a deployment decision, not a startup default: without this
    /// the server refuses to start and names what is pending, rather than
    /// running DDL nobody asked for on a restart.
    #[serde(default)]
    pub migrate: bool,
}

fn default_db_path() -> String {
    "resonate.db".to_string()
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: default_db_path(),
            preload_limit: default_preload_limit(),
            migrate: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL connection URL
    #[serde(default)]
    pub url: Option<String>,

    /// Connection pool size
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,

    /// Apply pending migrations to an existing database. See `SqliteConfig`.
    #[serde(default)]
    pub migrate: bool,
}

fn default_pool_size() -> u32 {
    10
}

fn default_preload_limit() -> u32 {
    10
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: None,
            pool_size: default_pool_size(),
            preload_limit: default_preload_limit(),
            migrate: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlConfig {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,

    /// Apply pending migrations to an existing database. See `SqliteConfig`.
    #[serde(default)]
    pub migrate: bool,
}

impl Default for MysqlConfig {
    fn default() -> Self {
        Self {
            url: None,
            pool_size: default_pool_size(),
            preload_limit: default_preload_limit(),
            migrate: false,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_type: default_storage_type(),
            sqlite: SqliteConfig::default(),
            postgres: PostgresConfig::default(),
            mysql: MysqlConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TasksConfig {
    /// Default task lease timeout (ms)
    #[serde(default = "default_lease_timeout")]
    pub lease_timeout: i64,

    /// Default pending task retry timeout (ms)
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: i64,
}

fn default_lease_timeout() -> i64 {
    15000
}
fn default_retry_timeout() -> i64 {
    30000
}

impl Default for TasksConfig {
    fn default() -> Self {
        Self {
            lease_timeout: default_lease_timeout(),
            retry_timeout: default_retry_timeout(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutsConfig {
    /// Background timeout scan interval (ms)
    ///
    /// The last resort, and no longer the mechanism. The in-memory timer fires
    /// a deadline the moment it comes due, and `wheel_refresh` re-reads the
    /// durable deadlines more often than this does — so a deadline another
    /// instance armed is picked up by the refresh, not here. What is left for
    /// this scan is what neither sees: a deadline past the wheel's horizon on a
    /// server holding more than `wheel_capacity` of them.
    ///
    /// It was 1s when it was the only thing firing timeouts. At a minute it is
    /// still an order of magnitude tighter than the deadlines it guards, and
    /// with the timer running most of these scans find nothing at all.
    #[serde(default = "default_timeout_poll_interval")]
    pub poll_interval: u64,

    /// How many deadlines the in-memory timer holds.
    ///
    /// The horizon is whatever this many nearest deadlines reach; anything
    /// further out is the sweep's until the wheel drains down to it. Bigger
    /// costs memory and buys a longer horizon, not correctness.
    #[serde(default = "default_wheel_capacity")]
    pub wheel_capacity: usize,

    /// How often the timer re-reads the durable deadlines (ms).
    ///
    /// Also the longest it will sleep. This is the staleness bound: a deadline
    /// armed by another instance is invisible to this one until the next
    /// refresh, and the sweep is what fires it in the meantime.
    #[serde(default = "default_wheel_refresh")]
    pub wheel_refresh: u64,
}

fn default_timeout_poll_interval() -> u64 {
    60_000
}

fn default_wheel_capacity() -> usize {
    8192
}

fn default_wheel_refresh() -> u64 {
    30_000
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            poll_interval: default_timeout_poll_interval(),
            wheel_capacity: default_wheel_capacity(),
            wheel_refresh: default_wheel_refresh(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransportsConfig {
    /// HTTP push transport configuration
    #[serde(default)]
    pub http_push: resonate_transport_http_push::Config,

    /// HTTP poll (SSE) transport configuration
    #[serde(default)]
    pub http_poll: resonate_transport_http_poll::Config,

    /// Google Cloud Pub/Sub transport configuration
    #[serde(default)]
    pub gcps: resonate_transport_gcps::Config,

    /// Bash execution transport configuration
    #[serde(default)]
    pub bash_exec: resonate_worker_bash::Config,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Prometheus metrics port (0 = disabled)
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,

    /// OpenTelemetry OTLP endpoint
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,
}

fn default_metrics_port() -> u16 {
    9090
}
fn default_otlp_endpoint() -> String {
    "localhost:4317".to_string()
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_port: default_metrics_port(),
            otlp_endpoint: default_otlp_endpoint(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level: default_level(),
            debug: false,
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            auth: None,
            tasks: TasksConfig::default(),
            timeouts: TimeoutsConfig::default(),
            transports: TransportsConfig::default(),
            observability: ObservabilityConfig::default(),
            console: resonate_gateway_web::Config::default(),
        }
    }
}

impl Config {
    /// Load configuration using layered merging:
    /// 1. Defaults
    /// 2. resonate.toml (optional)
    /// 3. Environment variables (RESONATE_* prefix, double-underscore nesting)
    ///
    /// Env var examples:
    ///   RESONATE_SERVER__PORT=3000
    ///   RESONATE_STORAGE__TYPE=postgres
    ///   RESONATE_STORAGE__POSTGRES__URL=postgres://...
    pub fn load() -> Result<Self, String> {
        let mut figment = Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(Toml::file("resonate.toml"))
            .merge(Env::prefixed("RESONATE_").split("__"));

        // Support standard OTEL env var (no RESONATE_ prefix)
        if let Ok(val) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            figment = figment.merge(Serialized::default("observability.otlp_endpoint", val));
        }

        let config: Config = figment
            .extract()
            .map_err(|e| format!("Configuration error: {e}"))?;

        config.validate()?;

        Ok(config)
    }

    /// Validate semantic constraints that serde/figment cannot express.
    fn validate(&self) -> Result<(), String> {
        // Validate storage type
        match self.storage.storage_type.as_str() {
            "sqlite" | "postgres" | "mysql" => {}
            other => {
                return Err(format!(
                    "Unknown storage backend: '{}'. Valid options are 'sqlite', 'postgres', and 'mysql'.",
                    other
                ));
            }
        }

        // Validate transport concurrency caps. A value of 0 sizes the delivery
        // semaphore to zero permits, so the dispatcher could never acquire a
        // slot — every message would queue and then block the processing loop
        // forever. Reject it up front rather than hang silently at runtime.
        if self.transports.http_push.concurrency == 0 {
            return Err("transports.http_push.concurrency must be at least 1 (got 0)".to_string());
        }
        if self.transports.gcps.concurrency == 0 {
            return Err("transports.gcps.concurrency must be at least 1 (got 0)".to_string());
        }

        // `task.acquire` validates `ttl >= 1`, so a non-positive lease would
        // make every acquire fail with a 400 and the worker would silently
        // never run anything. Reject it here instead.
        //
        // Both the override and the value it falls back to: guarding only the
        // override would leave `tasks.lease_timeout = 0` reaching `task.acquire`
        // through `resolve_lease_timeout`, which is the same failure.
        if self.tasks.lease_timeout < 1 {
            return Err(format!(
                "tasks.lease_timeout must be at least 1 (got {})",
                self.tasks.lease_timeout
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_positive_task_lease_timeout_is_rejected() {
        // It is what bash_exec.lease_timeout falls back to, so it reaches
        // `task.acquire` as the ttl and must clear the same bar.
        for ttl in [0, -1] {
            let mut config = Config::default();
            config.tasks.lease_timeout = ttl;
            let err = config.validate().expect_err("would 400 every acquire");
            assert!(err.contains("tasks.lease_timeout"), "{err}");
        }
    }

    #[test]
    fn default_config_is_valid() {
        Config::default()
            .validate()
            .expect("default config should validate");
    }

    #[test]
    fn rejects_zero_http_push_concurrency() {
        let mut config = Config::default();
        config.transports.http_push.concurrency = 0;
        let err = config
            .validate()
            .expect_err("zero concurrency must be rejected");
        assert!(
            err.contains("http_push.concurrency"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_zero_gcps_concurrency() {
        let mut config = Config::default();
        config.transports.gcps.concurrency = 0;
        let err = config
            .validate()
            .expect_err("zero concurrency must be rejected");
        assert!(err.contains("gcps.concurrency"), "unexpected error: {err}");
    }
}
