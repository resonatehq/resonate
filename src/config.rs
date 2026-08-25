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
    #[serde(default)]
    pub auth: Option<AuthConfig>,

    /// Task configuration
    #[serde(default)]
    pub tasks: TasksConfig,

    /// Timeout processing configuration
    #[serde(default)]
    pub timeouts: TimeoutsConfig,

    /// Message delivery configuration
    #[serde(default)]
    pub messages: MessagesConfig,

    /// Transport configuration
    #[serde(default)]
    pub transports: TransportsConfig,

    /// Observability configuration
    #[serde(default)]
    pub observability: ObservabilityConfig,
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
/// The `type` field selects the active backend ("sqlite", "postgres", "mysql",
/// or "s3"). Backend-specific settings live in the matching sub-struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Active backend: "sqlite", "postgres", "mysql", or "s3"
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

    /// S3-specific configuration
    #[serde(default)]
    pub s3: S3Config,
}

fn default_storage_type() -> String {
    "sqlite".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    /// Path to SQLite database file
    #[serde(default = "default_db_path")]
    pub path: String,
}

fn default_db_path() -> String {
    "resonate.db".to_string()
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: default_db_path(),
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
}

fn default_pool_size() -> u32 {
    10
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: None,
            pool_size: default_pool_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlConfig {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl Default for MysqlConfig {
    fn default() -> Self {
        Self {
            url: None,
            pool_size: default_pool_size(),
        }
    }
}

/// S3 backend configuration.
///
/// **The store must implement real conditional writes.** The whole design rests
/// on `If-Match` and `If-None-Match: *`: S3, R2, GCS and Azure have them, and
/// MinIO, B2 and Spaces do not — pointed at one of those, this backend silently
/// loses writes rather than failing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// Bucket holding every object. Required when `type = "s3"`.
    #[serde(default)]
    pub bucket: Option<String>,

    /// Region. Defaults to whatever the environment or instance metadata says.
    #[serde(default)]
    pub region: Option<String>,

    /// Endpoint override, for an S3-compatible service.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Allow a plain-HTTP endpoint. Only for a local test service.
    #[serde(default)]
    pub allow_http: bool,

    /// Prefix under which every key lives, so one bucket can hold several
    /// deployments.
    #[serde(default)]
    pub prefix: String,

    /// How many prefixes the timer keys are spread across.
    ///
    /// Timer keys carry their deadline, so they increase monotonically — the
    /// classic S3 hot-prefix anti-pattern. Sharding spreads the writes.
    #[serde(default = "default_timer_shards")]
    pub timer_shards: u32,

    /// Documents held in the read cache.
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize,

    /// How many times a batch is re-decided after losing a race before the
    /// caller is told there is no answer.
    #[serde(default = "default_max_cas_retries")]
    pub max_cas_retries: u32,
}

fn default_timer_shards() -> u32 {
    4
}
fn default_cache_capacity() -> usize {
    10_000
}
fn default_max_cas_retries() -> u32 {
    8
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            endpoint: None,
            allow_http: false,
            prefix: String::new(),
            timer_shards: default_timer_shards(),
            cache_capacity: default_cache_capacity(),
            max_cas_retries: default_max_cas_retries(),
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
            s3: S3Config::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Public key for JWT verification.
    /// Set to "none" to accept unsigned tokens (debug/testing).
    /// Set to a file path to verify signatures against a PEM key.
    pub publickey: String,

    /// Expected issuer (`iss` claim).
    #[serde(default)]
    pub iss: Option<String>,

    /// Expected audience (`aud` claim).
    #[serde(default)]
    pub aud: Option<String>,
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
    #[serde(default = "default_timeout_poll_interval")]
    pub poll_interval: u64,
}

fn default_timeout_poll_interval() -> u64 {
    1000
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            poll_interval: default_timeout_poll_interval(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagesConfig {
    /// Background message delivery scan interval (ms)
    #[serde(default = "default_message_poll_interval")]
    pub poll_interval: u64,

    /// Max messages to claim per delivery cycle
    #[serde(default = "default_message_batch_size")]
    pub batch_size: i64,
}

fn default_message_poll_interval() -> u64 {
    100
}
fn default_message_batch_size() -> i64 {
    100
}

impl Default for MessagesConfig {
    fn default() -> Self {
        Self {
            poll_interval: default_message_poll_interval(),
            batch_size: default_message_batch_size(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransportsConfig {
    /// HTTP push transport configuration
    #[serde(default)]
    pub http_push: HttpPushConfig,

    /// HTTP poll (SSE) transport configuration
    #[serde(default)]
    pub http_poll: HttpPollConfig,

    /// Google Cloud Pub/Sub transport configuration
    #[serde(default)]
    pub gcps: GcpsConfig,

    /// Bash execution transport configuration
    #[serde(default)]
    pub bash_exec: BashExecConfig,
}

/// Bash execution transport configuration.
///
/// When `enabled`, the bash:// scheme is routable via three backends:
/// - `bash://`                       → local bash
/// - `bash://docker/<image>`         → docker run --rm <image> bash -c <script>
/// - `bash://tensorlake/<image>`     → Tensorlake Sandboxes API (needs TENSORLAKE_API_KEY)
///
/// Scripts are always inline (carried in `param.data`); named scripts are not supported.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BashExecConfig {
    /// Enable the bash:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// Lease TTL (ms) this worker requests when acquiring a task, and the basis
    /// for its heartbeat interval (a third of it).
    ///
    /// The lease has to outlast the script: if it expires the task is
    /// redispatched to another worker while this one is still running.
    ///
    /// Unset means "follow `tasks.lease_timeout`" — the server-wide default
    /// this worker used before it had a setting of its own. Set it when scripts
    /// here run longer than tasks generally do.
    #[serde(default)]
    pub lease_timeout: Option<i64>,
}

impl BashExecConfig {
    /// The lease TTL to request, falling back to the server-wide task default.
    pub fn resolve_lease_timeout(&self, tasks: &TasksConfig) -> i64 {
        self.lease_timeout.unwrap_or(tasks.lease_timeout)
    }
}

/// Google Cloud Pub/Sub transport configuration.
/// Authentication uses Application Default Credentials (ADC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcpsConfig {
    /// Enable the gcps:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// Default GCP project ID. Used when the address doesn't specify a project.
    #[serde(default)]
    pub project: Option<String>,

    /// Max concurrent GCP Pub/Sub deliveries
    #[serde(default = "default_gcps_concurrency")]
    pub concurrency: usize,

    /// Per-publish timeout (ms)
    #[serde(default = "default_gcps_timeout")]
    pub timeout: u64,
}

fn default_gcps_concurrency() -> usize {
    256
}

fn default_gcps_timeout() -> u64 {
    10000
}

impl Default for GcpsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            project: None,
            concurrency: default_gcps_concurrency(),
            timeout: default_gcps_timeout(),
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPushConfig {
    /// Enable the http:// / https:// address scheme [default: true]
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Max concurrent HTTP push deliveries
    #[serde(default = "default_http_push_concurrency")]
    pub concurrency: usize,

    /// HTTP connect timeout (ms)
    #[serde(default = "default_http_push_connect_timeout")]
    pub connect_timeout: u64,

    /// HTTP request timeout (ms)
    #[serde(default = "default_http_push_request_timeout")]
    pub request_timeout: u64,

    /// Outbound auth for HTTP push deliveries.
    /// Absent (default) = no auth attached to outbound requests.
    #[serde(default)]
    pub auth: Option<HttpPushAuthConfig>,
}

fn default_http_push_concurrency() -> usize {
    256
}
fn default_http_push_connect_timeout() -> u64 {
    10000
}
fn default_http_push_request_timeout() -> u64 {
    180000
}

impl Default for HttpPushConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            concurrency: default_http_push_concurrency(),
            connect_timeout: default_http_push_connect_timeout(),
            request_timeout: default_http_push_request_timeout(),
            auth: None,
        }
    }
}

/// Outbound authentication for HTTP push deliveries.
///
/// Example config:
/// ```toml
/// [transports.http_push.auth]
/// mode = "gcp"
/// # audience = "https://my-function.example.com"  # optional; defaults to delivery URL
/// ```
///
/// Equivalent env vars (double-underscore nesting):
///   RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__MODE=gcp
///   RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__AUDIENCE=https://...
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPushAuthConfig {
    /// Auth mode. Default: `none`.
    #[serde(default)]
    pub mode: HttpPushAuthMode,

    /// Static bearer token. Used only when `mode = "bearer"`.
    /// Falls back to the `RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__TOKEN` env var.
    #[serde(default)]
    pub token: Option<String>,

    /// GCP audience override. Used only when `mode = "gcp"`.
    /// When absent, each delivery target URL is used as its own audience.
    #[serde(default)]
    pub audience: Option<String>,

    /// Header name to set. Default: `"Authorization"`.
    #[serde(default = "default_auth_header")]
    pub header: String,
}

fn default_auth_header() -> String {
    "Authorization".to_string()
}

impl Default for HttpPushAuthConfig {
    fn default() -> Self {
        Self {
            mode: HttpPushAuthMode::default(),
            token: None,
            audience: None,
            header: default_auth_header(),
        }
    }
}

/// Outbound auth mode for HTTP push deliveries.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HttpPushAuthMode {
    /// No auth header. Default.
    #[default]
    None,
    /// Static `Authorization: Bearer <token>`.
    Bearer,
    /// GCP OIDC ID token via the GCP metadata server.
    Gcp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPollConfig {
    /// Enable the poll:// (SSE) address scheme [default: true]
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum number of concurrent poll (SSE) connections
    #[serde(default = "default_http_poll_max_connections")]
    pub max_connections: usize,

    /// Channel buffer size for each poll (SSE) connection
    #[serde(default = "default_http_poll_buffer_size")]
    pub buffer_size: usize,
}

fn default_http_poll_max_connections() -> usize {
    1000
}
fn default_http_poll_buffer_size() -> usize {
    100
}

impl Default for HttpPollConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_connections: default_http_poll_max_connections(),
            buffer_size: default_http_poll_buffer_size(),
        }
    }
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
            messages: MessagesConfig::default(),
            transports: TransportsConfig::default(),
            observability: ObservabilityConfig::default(),
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
            "s3" => {
                if self.storage.s3.bucket.as_deref().unwrap_or("").is_empty() {
                    return Err(
                        "S3 storage selected but no bucket configured. Set --storage-s3-bucket or RESONATE_STORAGE__S3__BUCKET".to_string(),
                    );
                }
                if self.storage.s3.timer_shards == 0 {
                    return Err("storage.s3.timer_shards must be at least 1 (got 0)".to_string());
                }
            }
            other => {
                return Err(format!(
                    "Unknown storage backend: '{}'. Valid options are 'sqlite', 'postgres', 'mysql', and 's3'.",
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
        if let Some(ttl) = self.transports.bash_exec.lease_timeout {
            if ttl < 1 {
                return Err(format!(
                    "transports.bash_exec.lease_timeout must be at least 1 (got {ttl})"
                ));
            }
        }
        if self.tasks.lease_timeout < 1 {
            return Err(format!(
                "tasks.lease_timeout must be at least 1 (got {})",
                self.tasks.lease_timeout
            ));
        }

        // The S3 backend deletes a fired timer key only after sweeping it, which
        // is safe only while a re-armed deadline is strictly later than the
        // instant it was swept at. A non-positive retry timeout would re-arm
        // into the past and the sweep would delete the key it had just written,
        // stranding the origin.
        if self.tasks.retry_timeout < 1 {
            return Err(format!(
                "tasks.retry_timeout must be at least 1 (got {})",
                self.tasks.retry_timeout
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_config() -> Config {
        let mut config = Config::default();
        config.storage.storage_type = "s3".to_string();
        config.storage.s3.bucket = Some("my-bucket".to_string());
        config
    }

    #[test]
    fn s3_storage_needs_a_bucket() {
        let mut config = s3_config();
        config.storage.s3.bucket = None;
        let err = config.validate().expect_err("rejected");
        assert!(err.contains("no bucket configured"), "{err}");

        config.storage.s3.bucket = Some(String::new());
        assert!(config.validate().is_err(), "an empty bucket is no bucket");

        config.storage.s3.bucket = Some("my-bucket".into());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn s3_storage_needs_at_least_one_timer_shard() {
        // Zero shards would leave the timer prefix unreadable: there would be
        // no prefix to list.
        let mut config = s3_config();
        config.storage.s3.timer_shards = 0;
        let err = config.validate().expect_err("rejected");
        assert!(err.contains("timer_shards"), "{err}");
    }

    #[test]
    fn the_retry_timeout_must_be_positive() {
        // The S3 poller deletes a fired timer key only after sweeping it, which
        // is safe only while the re-armed deadline is strictly later than the
        // sweep. A zero retry timeout would re-arm into the past.
        let mut config = Config::default();
        config.tasks.retry_timeout = 0;
        let err = config.validate().expect_err("rejected");
        assert!(err.contains("retry_timeout"), "{err}");
    }

    #[test]
    fn an_unknown_storage_type_names_every_valid_one() {
        let mut config = Config::default();
        config.storage.storage_type = "cassandra".to_string();
        let err = config.validate().expect_err("rejected");
        assert!(err.contains("s3"), "{err}");
    }

    #[test]
    fn s3_defaults_are_usable_as_they_stand() {
        let s3 = S3Config::default();
        assert_eq!(s3.timer_shards, 4);
        assert_eq!(s3.cache_capacity, 10_000);
        assert_eq!(s3.max_cas_retries, 8);
        assert_eq!(s3.prefix, "");
        assert!(!s3.allow_http);
    }

    #[test]
    fn bash_lease_timeout_defaults_to_the_server_task_lease() {
        let mut config = Config::default();
        config.tasks.lease_timeout = 120_000;
        assert_eq!(config.transports.bash_exec.lease_timeout, None);
        assert_eq!(
            config
                .transports
                .bash_exec
                .resolve_lease_timeout(&config.tasks),
            120_000,
            "unset means follow tasks.lease_timeout, including when that is customised"
        );
    }

    #[test]
    fn bash_lease_timeout_overrides_the_server_task_lease() {
        let mut config = Config::default();
        config.tasks.lease_timeout = 15_000;
        config.transports.bash_exec.lease_timeout = Some(600_000);
        assert_eq!(
            config
                .transports
                .bash_exec
                .resolve_lease_timeout(&config.tasks),
            600_000
        );
    }

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
    fn non_positive_bash_lease_timeout_is_rejected() {
        for ttl in [0, -1] {
            let mut config = Config::default();
            config.transports.bash_exec.lease_timeout = Some(ttl);
            let err = config
                .validate()
                .expect_err("task.acquire would 400 on every acquire");
            assert!(err.contains("bash_exec.lease_timeout"), "{err}");
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
