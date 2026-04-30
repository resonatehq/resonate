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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BashExecConfig {
    /// Enable the bash:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// Root directory for named scripts (bash:///relative/path.sh).
    /// Not required if only inline scripts are used.
    #[serde(default)]
    pub root_dir: Option<String>,

    /// Working directory for named script execution.
    /// "<root>"   — CWD is set to root_dir (default)
    /// "<script>" — CWD is set to the directory containing the script
    /// any path   — CWD is set to that literal path
    #[serde(default = "default_working_dir")]
    pub working_dir: String,
}

fn default_working_dir() -> String {
    "<root>".to_string()
}

impl Default for BashExecConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            root_dir: None,
            working_dir: default_working_dir(),
        }
    }
}

/// Google Cloud Pub/Sub transport configuration.
/// Authentication uses Application Default Credentials (ADC).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GcpsConfig {
    /// Enable the gcps:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// Default GCP project ID. Used when the address doesn't specify a project.
    #[serde(default)]
    pub project: Option<String>,
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
    16
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

        // Validate storage type
        match config.storage.storage_type.as_str() {
            "sqlite" | "postgres" | "mysql" => {}
            other => {
                return Err(format!(
                    "Unknown storage backend: '{}'. Valid options are 'sqlite', 'postgres', and 'mysql'.",
                    other
                ));
            }
        }

        Ok(config)
    }
}
