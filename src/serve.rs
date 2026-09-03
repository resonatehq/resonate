//! The `serve` and `dev` flags, and what they override.
//!
//! Every flag here is one configuration key. `--server-port 3000` is
//! `gateways.gateway_http.bind = "0.0.0.0:3000"`, and `--set` says the same
//! thing in the general case — which is what lets a binary carrying a plugin
//! this repository has never heard of configure it without a flag being added
//! for it.
//!
//! This is the top level's, not the CLI client's: the keys are the composition
//! root's own vocabulary, and a build with a different set of plugins has a
//! different set of them.

use clap::Args;

// ---- Serve args ----

/// Common CLI flags shared between `serve` and `dev`.
///
/// Every one of these is an override on a configuration key, and that is all
/// they are: `--server-port 3000` is `gateways.gateway_http.bind =
/// "0.0.0.0:3000"`, and `--set gateways.gateway_http.bind=0.0.0.0:3000` says
/// the same thing. The gateway listens on one address, so `--server-host` and
/// `--server-port` are two halves of one key rather than two keys of their
/// own. Precedence: defaults < the file < the environment < these.
///
/// `--set` is the general case, and the reason there is no flag per plugin
/// field: a binary carrying a plugin this repository has never heard of
/// configures it the same way as one that ships here. The named flags below are
/// the handful worth typing.
#[derive(Args, Default)]
pub struct CommonArgs {
    // --- Where the configuration comes from ---
    /// Configuration file [default: resonate.toml]
    #[arg(long = "config", value_name = "PATH")]
    pub config: Option<String>,

    /// Override any configuration key (repeatable): --set workers.worker_kafka.brokers='["a:9092"]'
    ///
    /// The value is read as TOML, so numbers, booleans and lists arrive as
    /// themselves; anything else is a string.
    #[arg(long = "set", value_name = "KEY=VALUE")]
    pub set: Vec<String>,

    // --- Top-level ---
    /// Log level: debug, info, warn, error [default: info]
    #[arg(long)]
    pub level: Option<String>,

    /// Enable debug mode
    #[arg(long)]
    pub debug: bool,

    // --- Server ---
    /// HTTP server host [default: localhost]
    #[arg(long = "server-host")]
    pub host: Option<String>,

    /// HTTP server port [default: 8001]
    #[arg(long = "server-port")]
    pub port: Option<u16>,

    /// Bind address [default: 0.0.0.0]
    #[arg(long = "server-bind")]
    pub bind: Option<String>,

    /// Graceful shutdown timeout (ms) [default: 10000]
    #[arg(long = "server-shutdown-timeout", value_name = "MS")]
    pub shutdown_timeout: Option<u64>,

    /// External server URL included in response headers [default: http://{host}:{port}]
    #[arg(long = "server-url", value_name = "URL")]
    pub url: Option<String>,

    // --- CORS ---
    /// Allowed CORS origins (repeatable; use "*" for permissive access)
    #[arg(long = "server-cors-allow-origin", value_name = "ORIGIN")]
    pub cors_allow_origins: Vec<String>,

    // --- Storage ---
    /// Storage backend: sqlite, postgres, mysql or scylladb [default: sqlite]
    #[arg(long = "storage-type")]
    pub storage_type: Option<String>,

    /// PostgreSQL connection URL
    #[arg(long = "storage-postgres-url", value_name = "URL")]
    pub postgres_url: Option<String>,

    /// PostgreSQL connection pool size [default: 10]
    #[arg(long = "storage-postgres-pool-size", value_name = "N")]
    pub postgres_pool_size: Option<u32>,

    /// MySQL connection URL
    #[arg(long = "storage-mysql-url", value_name = "URL")]
    pub mysql_url: Option<String>,

    /// MySQL connection pool size [default: 10]
    #[arg(long = "storage-mysql-pool-size", value_name = "N")]
    pub mysql_pool_size: Option<u32>,

    // --- Auth ---
    /// Public key for JWT verification (enables auth; use "none" for unsigned mode)
    #[arg(long = "auth-publickey", value_name = "KEY")]
    pub auth_publickey: Option<String>,

    /// Expected JWT issuer claim
    #[arg(long = "auth-iss", value_name = "ISS")]
    pub auth_iss: Option<String>,

    /// Expected JWT audience claim
    #[arg(long = "auth-aud", value_name = "AUD")]
    pub auth_aud: Option<String>,

    // --- WorkOS auth ---
    /// The server's own WorkOS secret key (enables WorkOS auth)
    #[arg(long = "workos-api-key", value_name = "KEY")]
    pub workos_api_key: Option<String>,

    /// WorkOS organization every client API key must belong to
    #[arg(long = "workos-org-id", value_name = "ORG")]
    pub workos_org_id: Option<String>,

    /// WorkOS API base URL [default: https://api.workos.com]
    #[arg(long = "workos-base-url", value_name = "URL")]
    pub workos_base_url: Option<String>,

    // --- Tasks ---
    /// Task lease timeout (ms) [default: 15000]
    #[arg(long = "tasks-lease-timeout", value_name = "MS")]
    pub tasks_lease_timeout: Option<i64>,

    /// Pending task retry timeout (ms) [default: 30000]
    #[arg(long = "tasks-retry-timeout", value_name = "MS")]
    pub tasks_retry_timeout: Option<i64>,

    // --- HTTP Push ---
    /// Enable/disable HTTP push transport [default: true]
    #[arg(long = "transports-http-push-enabled", value_name = "BOOL")]
    pub transports_http_push_enabled: Option<bool>,

    /// Max concurrent HTTP push deliveries [default: 16]
    #[arg(long = "transports-http-push-concurrency", value_name = "N")]
    pub transports_http_push_concurrency: Option<usize>,

    /// HTTP push connect timeout (ms) [default: 10000]
    #[arg(long = "transports-http-push-connect-timeout", value_name = "MS")]
    pub transports_http_push_connect_timeout: Option<u64>,

    /// HTTP push request timeout (ms) [default: 180000]
    #[arg(long = "transports-http-push-request-timeout", value_name = "MS")]
    pub transports_http_push_request_timeout: Option<u64>,

    // --- HTTP Poll/SSE ---
    /// Enable/disable HTTP poll (SSE) transport [default: true]
    #[arg(long = "transports-http-poll-enabled", value_name = "BOOL")]
    pub transports_http_poll_enabled: Option<bool>,

    /// Max concurrent poll (SSE) connections [default: 1000]
    #[arg(long = "transports-http-poll-max-connections", value_name = "N")]
    pub transports_http_poll_max_connections: Option<usize>,

    /// Channel buffer size per poll connection [default: 100]
    #[arg(long = "transports-http-poll-buffer-size", value_name = "N")]
    pub transports_http_poll_buffer_size: Option<usize>,

    /// Keepalive interval for poll (SSE) connections, in ms [default: none]
    #[arg(long = "transports-http-poll-keepalive-interval", value_name = "MS")]
    pub transports_http_poll_keepalive_interval_ms: Option<u64>,

    // --- HTTP Push Auth ---
    /// Outbound auth mode for HTTP push deliveries: none, bearer, gcp [default: none]
    #[arg(long = "transports-http-push-auth-mode", value_name = "MODE")]
    pub transports_http_push_auth_mode: Option<String>,

    /// Static bearer token for HTTP push auth (mode=bearer)
    #[arg(long = "transports-http-push-auth-token", value_name = "TOKEN")]
    pub transports_http_push_auth_token: Option<String>,

    /// GCP audience for HTTP push auth (mode=gcp; defaults to delivery target URL)
    #[arg(long = "transports-http-push-auth-aud", value_name = "URL")]
    pub transports_http_push_auth_audience: Option<String>,

    /// Authorization header name for HTTP push auth [default: Authorization]
    #[arg(long = "transports-http-push-auth-header", value_name = "HEADER")]
    pub transports_http_push_auth_header: Option<String>,

    // --- GCP Pub/Sub ---
    /// Enable/disable GCP Pub/Sub transport [default: false]
    #[arg(long = "transports-gcps-enabled", value_name = "BOOL")]
    pub transports_gcps_enabled: Option<bool>,

    /// GCP project ID
    #[arg(long = "transports-gcps-project", value_name = "PROJECT")]
    pub transports_gcps_project: Option<String>,

    /// Max concurrent GCP Pub/Sub deliveries [default: 16]
    #[arg(long = "transports-gcps-concurrency", value_name = "N")]
    pub transports_gcps_concurrency: Option<usize>,

    /// GCP Pub/Sub per-publish timeout (ms) [default: 10000]
    #[arg(long = "transports-gcps-timeout", value_name = "MS")]
    pub transports_gcps_timeout: Option<u64>,

    // --- Bash Exec ---
    /// Enable/disable bash exec transport [default: false]
    #[arg(long = "transports-bash-exec-enabled", value_name = "BOOL")]
    pub transports_bash_exec_enabled: Option<bool>,

    // --- Console ---
    /// Enable/disable the web console [default: true]
    #[arg(long = "console-enabled", value_name = "BOOL")]
    pub console_enabled: Option<bool>,

    // --- Observability ---
    /// Address the Prometheus endpoint listens on [default: 0.0.0.0:9090]
    #[arg(long = "observability-metrics-bind", value_name = "ADDR")]
    pub observability_metrics_bind: Option<String>,

    /// Enable/disable the Prometheus endpoint [default: true]
    #[arg(long = "observability-metrics-enabled", value_name = "BOOL")]
    pub observability_metrics_enabled: Option<bool>,
}

/// Collects `key = value` overrides, so each flag below is one line.
#[derive(Default)]
struct Overrides(Vec<(String, String)>);

impl Overrides {
    /// Set a scalar key — a number or a bool — when the flag was given.
    ///
    /// The value is written as TOML, and `Display` is already TOML for both.
    fn maybe<T: std::fmt::Display>(&mut self, key: &str, value: Option<T>) {
        if let Some(v) = value {
            self.0.push((key.to_string(), v.to_string()));
        }
    }

    /// Set a string key, quoted, when the flag was given.
    ///
    /// Quoted here rather than left to the loader's "did it parse as TOML?"
    /// rule, because for a string flag that rule is wrong: a GCP project id of
    /// `123456789` or a bearer token of `12345` parses as an integer and then
    /// fails to deserialize into a `String`, naming a key the operator did
    /// write with a type error they did not cause.
    fn maybe_str(&mut self, key: &str, value: Option<impl Into<String>>) {
        if let Some(v) = value {
            self.0.push((key.to_string(), toml_string(&v.into())));
        }
    }

    /// Set a key to a value that is already TOML.
    fn set(&mut self, key: &str, value: impl Into<String>) {
        self.0.push((key.to_string(), value.into()));
    }

    /// Set a string key to a value that is already known.
    fn set_str(&mut self, key: &str, value: &str) {
        self.0.push((key.to_string(), toml_string(value)));
    }
}

/// A TOML basic string, so nothing about the value can be read as syntax.
fn toml_string(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

impl CommonArgs {
    /// Turn the flags into configuration overrides.
    ///
    /// `servers` is what this binary was compiled with — the flags that are
    /// about *the* server (`--server-url`, `--tasks-retry-timeout`) are written
    /// to every one of them, because which is active is not settled until the
    /// file and the environment have been read too. A section that is never the
    /// active one is never read, so writing it costs nothing.
    fn overrides(self, servers: &[String]) -> Vec<(String, String)> {
        let mut o = Overrides::default();

        o.maybe_str("level", self.level);
        if self.debug {
            o.set("debug", "true");
        }
        o.maybe("shutdown_timeout", self.shutdown_timeout);

        // --- servers ---
        if let Some(v) = &self.storage_type {
            let id = if v.starts_with("server_") {
                v.clone()
            } else {
                format!("server_{v}")
            };
            o.set_str("servers.active", &id);
        }
        o.maybe_str("servers.server_postgres.url", self.postgres_url.clone());
        o.maybe("servers.server_postgres.pool_size", self.postgres_pool_size);
        o.maybe_str("servers.server_mysql.url", self.mysql_url.clone());
        o.maybe("servers.server_mysql.pool_size", self.mysql_pool_size);

        // The URL a worker is told to call back on. The server stamps it into
        // every execute message, so it is the server's setting — the gateway
        // used to carry a copy of it that nothing read, and two settings that
        // can disagree are worse than one.
        let url = self.url.clone().or_else(|| {
            match (&self.host, self.port) {
                // Derived only when something was said about it. A host and a
                // port that both came from the file are the file's business.
                (None, None) => None,
                (host, port) => Some(format!(
                    "http://{}:{}",
                    host.clone().unwrap_or_else(|| "localhost".to_string()),
                    port.unwrap_or(8001)
                )),
            }
        });
        for id in servers {
            o.maybe_str(&format!("servers.{id}.server_url"), url.clone());
            o.maybe(
                &format!("servers.{id}.retry_timeout"),
                self.tasks_retry_timeout,
            );
        }

        // --- workers ---
        o.maybe(
            "workers.transport_http_push.enabled",
            self.transports_http_push_enabled,
        );
        o.maybe(
            "workers.transport_http_push.concurrency",
            self.transports_http_push_concurrency,
        );
        o.maybe(
            "workers.transport_http_push.connect_timeout",
            self.transports_http_push_connect_timeout,
        );
        o.maybe(
            "workers.transport_http_push.request_timeout",
            self.transports_http_push_request_timeout,
        );
        o.maybe_str(
            "workers.transport_http_push.auth.mode",
            self.transports_http_push_auth_mode,
        );
        o.maybe_str(
            "workers.transport_http_push.auth.token",
            self.transports_http_push_auth_token,
        );
        o.maybe_str(
            "workers.transport_http_push.auth.audience",
            self.transports_http_push_auth_audience,
        );
        o.maybe_str(
            "workers.transport_http_push.auth.header",
            self.transports_http_push_auth_header,
        );

        o.maybe(
            "workers.transport_http_poll.enabled",
            self.transports_http_poll_enabled,
        );
        o.maybe(
            "workers.transport_http_poll.max_connections",
            self.transports_http_poll_max_connections,
        );
        o.maybe(
            "workers.transport_http_poll.buffer_size",
            self.transports_http_poll_buffer_size,
        );
        o.maybe(
            "workers.transport_http_poll.keepalive_interval_ms",
            self.transports_http_poll_keepalive_interval_ms,
        );

        o.maybe(
            "workers.transport_gcps.enabled",
            self.transports_gcps_enabled,
        );
        o.maybe_str(
            "workers.transport_gcps.project",
            self.transports_gcps_project,
        );
        o.maybe(
            "workers.transport_gcps.concurrency",
            self.transports_gcps_concurrency,
        );
        o.maybe(
            "workers.transport_gcps.timeout",
            self.transports_gcps_timeout,
        );

        o.maybe(
            "workers.worker_bash.enabled",
            self.transports_bash_exec_enabled,
        );
        o.maybe(
            "workers.worker_bash.lease_timeout",
            self.tasks_lease_timeout,
        );

        // --- gateways ---
        //
        // One key, from two flags: the gateway listens on one address like
        // every other listening plugin, and `--server-bind` and `--server-port`
        // are two halves of it.
        if self.bind.is_some() || self.port.is_some() {
            o.set_str(
                "gateways.gateway_http.bind",
                &format!(
                    "{}:{}",
                    self.bind.clone().unwrap_or_else(|| "0.0.0.0".to_string()),
                    self.port.unwrap_or(8001)
                ),
            );
        }
        if !self.cors_allow_origins.is_empty() {
            let list = self
                .cors_allow_origins
                .iter()
                .map(|s| toml_string(s))
                .collect::<Vec<_>>()
                .join(", ");
            o.set(
                "gateways.gateway_http.cors_allow_origins",
                format!("[{list}]"),
            );
        }
        o.maybe("gateways.gateway_web.enabled", self.console_enabled);
        o.maybe(
            "gateways.gateway_metrics.enabled",
            self.observability_metrics_enabled,
        );
        o.maybe_str(
            "gateways.gateway_metrics.bind",
            self.observability_metrics_bind,
        );

        // One policy, on the one edge that admits a request. The console and
        // the poll transport are routes on this gateway's listener, so they are
        // behind this key rather than each carrying one of their own.
        //
        // `--auth-iss` and `--auth-aud` narrow a policy rather than turning one
        // on, so they only mean anything alongside a key — but they are written
        // whether or not this invocation supplied one, because the key may have
        // come from the file or the environment. Writing only the claims used
        // to drop them silently in exactly that case.
        o.maybe_str(
            "gateways.gateway_http.auth.publickey",
            self.auth_publickey.clone(),
        );
        o.maybe_str("gateways.gateway_http.auth.iss", self.auth_iss.clone());
        o.maybe_str("gateways.gateway_http.auth.aud", self.auth_aud.clone());

        // The other mode, on the same edge. Naming both is refused by the
        // gateway at startup rather than resolved by a precedence nobody wrote
        // down.
        o.maybe_str(
            "gateways.gateway_http.workos.api_key",
            self.workos_api_key.clone(),
        );
        o.maybe_str(
            "gateways.gateway_http.workos.org_id",
            self.workos_org_id.clone(),
        );
        o.maybe_str(
            "gateways.gateway_http.workos.base_url",
            self.workos_base_url.clone(),
        );

        // Whatever the named flags do not cover.
        for assignment in self.set {
            match assignment.split_once('=') {
                Some((key, value)) => o.set(key.trim(), value),
                None => o.set(assignment.trim(), "true"),
            }
        }

        o.0
    }
}

/// CLI flags for the `serve` subcommand.
#[derive(Args, Default)]
pub struct ServeArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// SQLite database file path [default: resonate.db]
    #[arg(long = "storage-sqlite-path", value_name = "PATH")]
    pub sqlite_path: Option<String>,
}

impl ServeArgs {
    /// Where to read the configuration from, and what these flags override.
    pub fn options(self, servers: &[String]) -> resonate_base::Options {
        let file = self.common.config.clone();
        let sqlite_path = self.sqlite_path;
        let mut options = base_options(file, self.common.overrides(servers));
        if let Some(path) = sqlite_path {
            options = options.set("servers.server_sqlite.path", path);
        }
        options
    }
}

/// CLI flags for the `dev` subcommand (same as `serve` but defaults SQLite to `:memory:`).
#[derive(Args, Default)]
pub struct DevArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// SQLite database file path [default: :memory:]
    #[arg(long = "storage-sqlite-path", value_name = "PATH")]
    pub sqlite_path: Option<String>,
}

impl DevArgs {
    /// The same, with the one difference `dev` is for: storage that does not
    /// outlive the process.
    pub fn options(self, servers: &[String]) -> resonate_base::Options {
        let file = self.common.config.clone();
        let sqlite_path = self.sqlite_path;
        base_options(file, self.common.overrides(servers)).set(
            "servers.server_sqlite.path",
            sqlite_path.unwrap_or_else(|| ":memory:".to_string()),
        )
    }
}

/// The layers every subcommand reads, before its own flags.
fn base_options(file: Option<String>, overrides: Vec<(String, String)>) -> resonate_base::Options {
    let mut options = resonate_base::Options::default()
        .file(file.unwrap_or_else(|| "resonate.toml".to_string()))
        .env_prefix("RESONATE_");
    for (key, value) in overrides {
        options = options.set(key, value);
    }
    options
}
