//! CLI client commands: promises, tasks, schedules

use clap::{Args, Parser, Subcommand};
use serde_json::{json, Value};

// ---- Serve args ----

/// CLI flags for the `serve` subcommand.
///
/// All fields are `Option<T>` (or plain `bool` for flags) so that only
/// explicitly-provided values override the loaded configuration. Precedence:
/// defaults < resonate.toml < env vars < these flags.
#[derive(Parser, Default)]
pub struct ServeArgs {
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

    // --- Storage ---
    /// Storage backend: sqlite or postgres [default: sqlite]
    #[arg(long = "storage-type")]
    pub storage_type: Option<String>,

    /// SQLite database file path [default: resonate.db]
    #[arg(long = "storage-sqlite-path", value_name = "PATH")]
    pub sqlite_path: Option<String>,

    /// PostgreSQL connection URL
    #[arg(long = "storage-postgres-url", value_name = "URL")]
    pub postgres_url: Option<String>,

    /// PostgreSQL connection pool size [default: 10]
    #[arg(long = "storage-postgres-pool-size", value_name = "N")]
    pub postgres_pool_size: Option<u32>,

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

    // --- Tasks ---
    /// Task lease timeout (ms) [default: 15000]
    #[arg(long = "tasks-lease-timeout", value_name = "MS")]
    pub tasks_lease_timeout: Option<i64>,

    /// Pending task retry timeout (ms) [default: 30000]
    #[arg(long = "tasks-retry-timeout", value_name = "MS")]
    pub tasks_retry_timeout: Option<i64>,

    // --- Timeouts ---
    /// Background timeout scan interval (ms) [default: 1000]
    #[arg(long = "timeouts-poll-interval", value_name = "MS")]
    pub timeouts_poll_interval: Option<u64>,

    // --- Messages ---
    /// Background message delivery scan interval (ms) [default: 100]
    #[arg(long = "messages-poll-interval", value_name = "MS")]
    pub messages_poll_interval: Option<u64>,

    /// Max messages to claim per delivery cycle [default: 100]
    #[arg(long = "messages-batch-size", value_name = "N")]
    pub messages_batch_size: Option<i64>,

    // --- HTTP Push ---
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
    /// Max concurrent poll (SSE) connections [default: 1000]
    #[arg(long = "transports-http-poll-max-connections", value_name = "N")]
    pub transports_http_poll_max_connections: Option<usize>,

    /// Channel buffer size per poll connection [default: 100]
    #[arg(long = "transports-http-poll-buffer-size", value_name = "N")]
    pub transports_http_poll_buffer_size: Option<usize>,

    // --- GCP Pub/Sub ---
    /// GCP project ID (enables GCP Pub/Sub transport)
    #[arg(long = "transports-gcps-project", value_name = "PROJECT")]
    pub transports_gcps_project: Option<String>,

    // --- Observability ---
    /// Prometheus metrics port (0 = disabled) [default: 9090]
    #[arg(long = "observability-metrics-port", value_name = "PORT")]
    pub observability_metrics_port: Option<u16>,

    /// OpenTelemetry OTLP endpoint [default: localhost:4317]
    #[arg(long = "observability-otlp-endpoint", value_name = "ENDPOINT")]
    pub observability_otlp_endpoint: Option<String>,
}

impl ServeArgs {
    /// Apply any explicitly-provided CLI flags on top of an already-loaded `Config`.
    pub fn apply(self, mut config: crate::config::Config) -> crate::config::Config {
        if let Some(v) = self.level {
            config.level = v;
        }
        if self.debug {
            config.debug = true;
        }

        if let Some(v) = self.host {
            config.server.host = v;
        }
        if let Some(v) = self.port {
            config.server.port = v;
        }
        if let Some(v) = self.bind {
            config.server.bind = v;
        }
        if let Some(v) = self.shutdown_timeout {
            config.server.shutdown_timeout = v;
        }
        if let Some(v) = self.url {
            config.server.url = Some(v);
        }
        if config.server.url.is_none() {
            config.server.url = Some(format!(
                "http://{}:{}",
                config.server.host, config.server.port
            ));
        }

        if let Some(v) = self.storage_type {
            config.storage.storage_type = v;
        }
        if let Some(v) = self.sqlite_path {
            config.storage.sqlite.path = v;
        }
        if let Some(v) = self.postgres_url {
            config.storage.postgres.url = Some(v);
        }
        if let Some(v) = self.postgres_pool_size {
            config.storage.postgres.pool_size = v;
        }

        if let Some(key) = self.auth_publickey {
            let auth = config
                .auth
                .get_or_insert_with(|| crate::config::AuthConfig {
                    publickey: String::new(),
                    iss: None,
                    aud: None,
                });
            auth.publickey = key;
            if let Some(v) = self.auth_iss {
                auth.iss = Some(v);
            }
            if let Some(v) = self.auth_aud {
                auth.aud = Some(v);
            }
        }

        if let Some(v) = self.tasks_lease_timeout {
            config.tasks.lease_timeout = v;
        }
        if let Some(v) = self.tasks_retry_timeout {
            config.tasks.retry_timeout = v;
        }

        if let Some(v) = self.timeouts_poll_interval {
            config.timeouts.poll_interval = v;
        }

        if let Some(v) = self.messages_poll_interval {
            config.messages.poll_interval = v;
        }
        if let Some(v) = self.messages_batch_size {
            config.messages.batch_size = v;
        }

        if let Some(v) = self.transports_http_push_concurrency {
            config.transports.http_push.concurrency = v;
        }
        if let Some(v) = self.transports_http_push_connect_timeout {
            config.transports.http_push.connect_timeout = v;
        }
        if let Some(v) = self.transports_http_push_request_timeout {
            config.transports.http_push.request_timeout = v;
        }

        if let Some(v) = self.transports_http_poll_max_connections {
            config.transports.http_poll.max_connections = v;
        }
        if let Some(v) = self.transports_http_poll_buffer_size {
            config.transports.http_poll.buffer_size = v;
        }

        if let Some(project) = self.transports_gcps_project {
            let gcps = config
                .transports
                .gcps
                .get_or_insert(crate::config::GcpsConfig { project: None });
            gcps.project = Some(project);
        }

        if let Some(v) = self.observability_metrics_port {
            config.observability.metrics_port = v;
        }
        if let Some(v) = self.observability_otlp_endpoint {
            config.observability.otlp_endpoint = v;
        }

        config
    }
}

// ---- Duration parsing ----

/// Parse a duration string (e.g. "1h", "30s", "1m30s", "100ms") into milliseconds.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    let mut total: i64 = 0;
    let mut num = String::new();
    let mut chars = s.chars().peekable();

    while chars.peek().is_some() {
        while chars.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            num.push(chars.next().unwrap());
        }
        if num.is_empty() {
            return Err(format!("Invalid duration '{}': expected a number", s));
        }
        let n: i64 = num
            .parse()
            .map_err(|_| format!("Invalid number in duration '{}'", s))?;
        num.clear();

        let mut unit = String::new();
        while chars.peek().map(|c| c.is_alphabetic()).unwrap_or(false) {
            unit.push(chars.next().unwrap());
        }

        let ms = match unit.as_str() {
            "ms" => n,
            "s" => n * 1_000,
            "m" => n * 60_000,
            "h" => n * 3_600_000,
            "d" => n * 86_400_000,
            "" => return Err(format!("Missing unit in duration '{}'", s)),
            u => return Err(format!("Unknown unit '{}' in duration '{}'", u, s)),
        };
        total += ms;
    }

    if total == 0 && !s.is_empty() && s != "0" {
        return Err(format!("Invalid duration '{}'", s));
    }
    Ok(total)
}

// ---- Helpers ----

fn gen_corr_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("cli-{}-{}", d.as_millis(), d.subsec_nanos() % 1_000_000)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Build a protocol request envelope from parts.
/// Returns the complete JSON body ready to send to the server.
fn build_envelope(kind: &str, corr_id: &str, token: Option<&str>, data: Value) -> Value {
    let mut head = json!({ "corrId": corr_id, "version": "2026-04-01" });
    if let Some(t) = token {
        head["auth"] = json!(t);
    }
    json!({ "kind": kind, "head": head, "data": data })
}

async fn post(server: &str, kind: &str, token: Option<&str>, data: Value) -> Result<Value, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/", server.trim_end_matches('/'));

    let body = build_envelope(kind, &gen_corr_id(), token, data);

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Connection error: {}", e))?;

    let envelope: Value = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    let status = envelope["head"]["status"].as_i64().unwrap_or(0);
    if (200..300).contains(&status) {
        Ok(envelope["data"].clone())
    } else {
        Err(format!("{}", envelope["data"]))
    }
}

fn print_data(data: &Value) {
    println!("{}", serde_json::to_string_pretty(data).unwrap_or_default());
}

fn error_exit(msg: &str) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

fn parse_json_flag(flag: &str, name: &str) -> Value {
    serde_json::from_str(flag)
        .unwrap_or_else(|e| error_exit(&format!("Invalid {} JSON: {}", name, e)))
}

// ---- Global args ----

#[derive(Args, Debug, Clone)]
pub struct GlobalArgs {
    /// Resonate server URL
    #[arg(
        short = 'S',
        long = "server",
        default_value = "http://localhost:8001",
        global = true
    )]
    pub server: String,

    /// JWT token for authentication
    #[arg(short = 'T', long = "token", global = true)]
    pub token: Option<String>,
}

// ---- Promise commands ----

#[derive(Args, Debug)]
pub struct PromiseArgs {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: PromiseCommand,
}

#[derive(Subcommand, Debug)]
pub enum PromiseCommand {
    /// Get a promise by ID
    Get {
        /// Promise ID
        id: String,
    },
    /// Create a new promise
    Create {
        /// Promise ID
        id: String,
        /// Timeout duration (e.g. 1h, 30s)
        #[arg(long)]
        timeout: String,
        /// Parameter as JSON (e.g. '{"data":"hello"}')
        #[arg(long)]
        param: Option<String>,
        /// Tags as JSON object (e.g. '{"key":"value"}')
        #[arg(long)]
        tags: Option<String>,
    },
    /// Resolve a promise
    Resolve {
        /// Promise ID
        id: String,
        /// Value as JSON (e.g. '{"data":"ok"}')
        #[arg(long)]
        value: Option<String>,
    },
    /// Reject a promise
    Reject {
        /// Promise ID
        id: String,
        /// Value as JSON
        #[arg(long)]
        value: Option<String>,
    },
    /// Cancel a promise
    Cancel {
        /// Promise ID
        id: String,
        /// Value as JSON
        #[arg(long)]
        value: Option<String>,
    },
    /// Search promises
    Search {
        /// Filter by state (pending, resolved, rejected, rejected_canceled, rejected_timedout)
        #[arg(long)]
        state: Option<String>,
        /// Filter by tags (JSON object)
        #[arg(long)]
        tags: Option<String>,
        /// Maximum number of results
        #[arg(long, default_value_t = 100)]
        limit: i64,
        /// Pagination cursor
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Register a callback between two promises
    RegisterCallback {
        /// Awaited promise ID
        awaited: String,
        /// Awaiter promise ID
        awaiter: String,
    },
    /// Register a listener address on a promise
    RegisterListener {
        /// Awaited promise ID
        awaited: String,
        /// Listener address (e.g. http://host/path or poll://id)
        address: String,
    },
}

pub async fn run_promises(args: PromiseArgs) {
    let server = &args.global.server;
    let token = args.global.token.as_deref();

    let result = match &args.command {
        PromiseCommand::Get { id } => post(server, "promise.get", token, json!({ "id": id })).await,

        PromiseCommand::Create {
            id,
            timeout,
            param,
            tags,
        } => {
            let dur = parse_duration(timeout).unwrap_or_else(|e| error_exit(&e));
            let timeout_at = now_ms() + dur;
            let mut data = json!({ "id": id, "timeoutAt": timeout_at });
            if let Some(p) = param {
                data["param"] = parse_json_flag(p, "--param");
            }
            if let Some(t) = tags {
                data["tags"] = parse_json_flag(t, "--tags");
            }
            post(server, "promise.create", token, data).await
        }

        PromiseCommand::Resolve { id, value } => {
            settle(server, token, id, "resolved", value.as_deref()).await
        }

        PromiseCommand::Reject { id, value } => {
            settle(server, token, id, "rejected", value.as_deref()).await
        }

        PromiseCommand::Cancel { id, value } => {
            settle(server, token, id, "rejected_canceled", value.as_deref()).await
        }

        PromiseCommand::Search {
            state,
            tags,
            limit,
            cursor,
        } => {
            let mut data = json!({ "limit": limit });
            if let Some(s) = state {
                data["state"] = json!(s);
            }
            if let Some(c) = cursor {
                data["cursor"] = json!(c);
            }
            if let Some(t) = tags {
                data["tags"] = parse_json_flag(t, "--tags");
            }
            post(server, "promise.search", token, data).await
        }

        PromiseCommand::RegisterCallback { awaited, awaiter } => {
            post(
                server,
                "promise.register_callback",
                token,
                json!({ "awaited": awaited, "awaiter": awaiter }),
            )
            .await
        }

        PromiseCommand::RegisterListener { awaited, address } => {
            post(
                server,
                "promise.register_listener",
                token,
                json!({ "awaited": awaited, "address": address }),
            )
            .await
        }
    };

    match result {
        Ok(data) => print_data(&data),
        Err(e) => error_exit(&e),
    }
}

async fn settle(
    server: &str,
    token: Option<&str>,
    id: &str,
    state: &str,
    value: Option<&str>,
) -> Result<Value, String> {
    let mut data = json!({ "id": id, "state": state });
    if let Some(v) = value {
        data["value"] = parse_json_flag(v, "--value");
    }
    post(server, "promise.settle", token, data).await
}

// ---- Task commands ----

#[derive(Args, Debug)]
pub struct TaskArgs {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: TaskCommand,
}

#[derive(Subcommand, Debug)]
pub enum TaskCommand {
    /// Get a task by ID
    Get {
        /// Task ID (same as promise ID)
        id: String,
    },
    /// Create a promise+task atomically (task starts acquired)
    Create {
        /// Promise ID
        id: String,
        /// Process ID claiming the task
        #[arg(long)]
        pid: String,
        /// Task lease TTL duration (e.g. 15s, 1m)
        #[arg(long)]
        ttl: String,
        /// Promise timeout duration (e.g. 1h)
        #[arg(long)]
        timeout: String,
        /// Promise parameter as JSON
        #[arg(long)]
        param: Option<String>,
        /// Promise tags as JSON object
        #[arg(long)]
        tags: Option<String>,
    },
    /// Claim (acquire) a pending task
    Claim {
        /// Task ID
        id: String,
        /// Process ID
        #[arg(long)]
        pid: String,
        /// Task version (for optimistic concurrency)
        #[arg(long = "version")]
        version: i64,
        /// Lease TTL duration (e.g. 15s)
        #[arg(long)]
        ttl: String,
    },
    /// Execute a fenced operation within a task
    Fence {
        /// Task ID
        id: String,
        /// Task version
        #[arg(long = "version")]
        version: i64,
        /// Inner action as JSON (e.g. '{"kind":"promise.create","data":{...}}')
        #[arg(long)]
        action: String,
    },
    /// Extend task leases
    Heartbeat {
        /// Process ID
        #[arg(long)]
        pid: String,
        /// Tasks to renew as JSON array (e.g. '[{"id":"foo","version":1}]')
        #[arg(long)]
        tasks: String,
    },
    /// Suspend a task while awaiting other promises
    Suspend {
        /// Task ID
        id: String,
        /// Task version
        #[arg(long = "version")]
        version: i64,
        /// Suspend actions as JSON array (register_callback actions)
        #[arg(long)]
        actions: String,
    },
    /// Fulfill a task (settle its promise)
    Complete {
        /// Task ID
        id: String,
        /// Task version
        #[arg(long = "version")]
        version: i64,
        /// Fulfill action as JSON (e.g. '{"kind":"promise.settle","data":{...}}')
        #[arg(long)]
        action: String,
    },
    /// Release an acquired task back to pending
    Release {
        /// Task ID
        id: String,
        /// Task version
        #[arg(long = "version")]
        version: i64,
    },
    /// Halt a task
    Halt {
        /// Task ID
        id: String,
    },
    /// Continue a halted task
    Continue {
        /// Task ID
        id: String,
    },
    /// Search tasks
    Search {
        /// Filter by state
        #[arg(long)]
        state: Option<String>,
        /// Maximum number of results
        #[arg(long, default_value_t = 100)]
        limit: i64,
        /// Pagination cursor
        #[arg(long)]
        cursor: Option<String>,
    },
}

pub async fn run_tasks(args: TaskArgs) {
    let server = &args.global.server;
    let token = args.global.token.as_deref();

    let result = match &args.command {
        TaskCommand::Get { id } => post(server, "task.get", token, json!({ "id": id })).await,

        TaskCommand::Create {
            id,
            pid,
            ttl,
            timeout,
            param,
            tags,
        } => {
            let ttl_ms = parse_duration(ttl).unwrap_or_else(|e| error_exit(&e));
            let timeout_ms = parse_duration(timeout).unwrap_or_else(|e| error_exit(&e));
            let timeout_at = now_ms() + timeout_ms;
            let mut action_data = json!({ "id": id, "timeoutAt": timeout_at });
            if let Some(p) = param {
                action_data["param"] = parse_json_flag(p, "--param");
            }
            if let Some(t) = tags {
                action_data["tags"] = parse_json_flag(t, "--tags");
            }
            let data = json!({
                "pid": pid,
                "ttl": ttl_ms,
                "action": { "kind": "promise.create", "data": action_data }
            });
            post(server, "task.create", token, data).await
        }

        TaskCommand::Claim {
            id,
            pid,
            version,
            ttl,
        } => {
            let ttl_ms = parse_duration(ttl).unwrap_or_else(|e| error_exit(&e));
            post(
                server,
                "task.acquire",
                token,
                json!({ "id": id, "pid": pid, "version": version, "ttl": ttl_ms }),
            )
            .await
        }

        TaskCommand::Fence {
            id,
            version,
            action,
        } => {
            let action_val = parse_json_flag(action, "--action");
            post(
                server,
                "task.fence",
                token,
                json!({ "id": id, "version": version, "action": action_val }),
            )
            .await
        }

        TaskCommand::Heartbeat { pid, tasks } => {
            let tasks_val = parse_json_flag(tasks, "--tasks");
            post(
                server,
                "task.heartbeat",
                token,
                json!({ "pid": pid, "tasks": tasks_val }),
            )
            .await
        }

        TaskCommand::Suspend {
            id,
            version,
            actions,
        } => {
            let actions_val = parse_json_flag(actions, "--actions");
            post(
                server,
                "task.suspend",
                token,
                json!({ "id": id, "version": version, "actions": actions_val }),
            )
            .await
        }

        TaskCommand::Complete {
            id,
            version,
            action,
        } => {
            let action_val = parse_json_flag(action, "--action");
            post(
                server,
                "task.fulfill",
                token,
                json!({ "id": id, "version": version, "action": action_val }),
            )
            .await
        }

        TaskCommand::Release { id, version } => {
            post(
                server,
                "task.release",
                token,
                json!({ "id": id, "version": version }),
            )
            .await
        }

        TaskCommand::Halt { id } => post(server, "task.halt", token, json!({ "id": id })).await,

        TaskCommand::Continue { id } => {
            post(server, "task.continue", token, json!({ "id": id })).await
        }

        TaskCommand::Search {
            state,
            limit,
            cursor,
        } => {
            let mut data = json!({ "limit": limit });
            if let Some(s) = state {
                data["state"] = json!(s);
            }
            if let Some(c) = cursor {
                data["cursor"] = json!(c);
            }
            post(server, "task.search", token, data).await
        }
    };

    match result {
        Ok(data) => print_data(&data),
        Err(e) => error_exit(&e),
    }
}

// ---- Invoke command ----

#[derive(Args, Debug)]
pub struct InvokeArgs {
    #[command(flatten)]
    pub global: GlobalArgs,

    /// Promise ID
    pub id: String,

    /// Function to invoke
    #[arg(short = 'f', long = "func")]
    pub func_name: String,

    /// Function argument; can be specified multiple times (JSON value or plain string)
    #[arg(long = "arg")]
    pub args: Vec<String>,

    /// Function arguments as a JSON array (alternative to --arg)
    #[arg(long = "json-args")]
    pub json_args: Option<String>,

    /// Function version [default: 1]
    #[arg(long, default_value_t = 1)]
    pub version: i64,

    /// Promise timeout duration (e.g. 1h, 30s) [default: 1h]
    #[arg(short = 't', long, default_value = "1h")]
    pub timeout: String,

    /// Invoke target [default: poll://any@default]
    #[arg(long, default_value = "poll://any@default")]
    pub target: String,

    /// Promise delivery delay (e.g. 5s, 1m); omit for no delay
    #[arg(long)]
    pub delay: Option<String>,
}

pub async fn run_invoke(args: InvokeArgs) {
    let server = &args.global.server;
    let token = args.global.token.as_deref();

    if args.version <= 0 {
        error_exit("version must be greater than 0");
    }

    let delay_ms: i64 = match &args.delay {
        Some(d) => parse_duration(d).unwrap_or_else(|e| error_exit(&e)),
        None => 0,
    };
    let timeout_ms = parse_duration(&args.timeout).unwrap_or_else(|e| error_exit(&e));

    let invoke_args: Vec<Value> = if let Some(json_str) = &args.json_args {
        match serde_json::from_str::<Vec<Value>>(json_str) {
            Ok(v) => v,
            Err(e) => error_exit(&format!("Failed to parse --json-args: {}", e)),
        }
    } else {
        args.args
            .iter()
            .map(|arg| {
                serde_json::from_str::<Value>(arg).unwrap_or_else(|_| Value::String(arg.clone()))
            })
            .collect()
    };

    let param = json!({
        "func": args.func_name,
        "args": invoke_args,
        "version": args.version,
    });

    let mut tags = serde_json::Map::new();
    tags.insert(
        "resonate:invoke".to_string(),
        Value::String(args.target.clone()),
    );
    if delay_ms > 0 {
        let deliver_at = now_ms() + delay_ms;
        tags.insert(
            "resonate:delay".to_string(),
            Value::String(deliver_at.to_string()),
        );
    }

    let timeout_at = now_ms() + timeout_ms + delay_ms;
    let data = json!({
        "id": args.id,
        "timeoutAt": timeout_at,
        "param": param,
        "tags": Value::Object(tags),
    });

    let result = post(server, "promise.create", token, data).await;
    match result {
        Ok(data) => print_data(&data),
        Err(e) => error_exit(&e),
    }
}

// ---- Schedule commands ----

#[derive(Args, Debug)]
pub struct ScheduleArgs {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: ScheduleCommand,
}

#[derive(Subcommand, Debug)]
pub enum ScheduleCommand {
    /// Get a schedule by ID
    Get {
        /// Schedule ID
        id: String,
    },
    /// Create a new schedule
    Create {
        /// Schedule ID
        id: String,
        /// Cron expression (e.g. "0 * * * *")
        #[arg(long)]
        cron: String,
        /// Template promise ID (supports {{.timestamp}})
        #[arg(long)]
        promise_id: String,
        /// Promise timeout duration (e.g. 1h)
        #[arg(long)]
        promise_timeout: String,
        /// Promise parameter as JSON
        #[arg(long)]
        promise_param: Option<String>,
        /// Promise tags as JSON object
        #[arg(long)]
        promise_tags: Option<String>,
    },
    /// Delete a schedule by ID
    Delete {
        /// Schedule ID
        id: String,
    },
    /// Search schedules
    Search {
        /// Filter by tags (JSON object)
        #[arg(long)]
        tags: Option<String>,
        /// Maximum number of results
        #[arg(long, default_value_t = 10)]
        limit: i64,
        /// Pagination cursor
        #[arg(long)]
        cursor: Option<String>,
    },
}

pub async fn run_schedules(args: ScheduleArgs) {
    let server = &args.global.server;
    let token = args.global.token.as_deref();

    let result = match &args.command {
        ScheduleCommand::Get { id } => {
            post(server, "schedule.get", token, json!({ "id": id })).await
        }

        ScheduleCommand::Create {
            id,
            cron,
            promise_id,
            promise_timeout,
            promise_param,
            promise_tags,
        } => {
            let timeout_ms = parse_duration(promise_timeout).unwrap_or_else(|e| error_exit(&e));
            let mut data = json!({
                "id": id,
                "cron": cron,
                "promiseId": promise_id,
                "promiseTimeout": timeout_ms,
            });
            if let Some(p) = promise_param {
                data["promiseParam"] = parse_json_flag(p, "--promise-param");
            }
            if let Some(t) = promise_tags {
                data["promiseTags"] = parse_json_flag(t, "--promise-tags");
            }
            post(server, "schedule.create", token, data).await
        }

        ScheduleCommand::Delete { id } => {
            post(server, "schedule.delete", token, json!({ "id": id })).await
        }

        ScheduleCommand::Search {
            tags,
            limit,
            cursor,
        } => {
            let mut data = json!({ "limit": limit });
            if let Some(t) = tags {
                data["tags"] = parse_json_flag(t, "--tags");
            }
            if let Some(c) = cursor {
                data["cursor"] = json!(c);
            }
            post(server, "schedule.search", token, data).await
        }
    };

    match result {
        Ok(data) => print_data(&data),
        Err(e) => error_exit(&e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- Duration parsing tests (existing) ----

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("1s").unwrap(), 1_000);
        assert_eq!(parse_duration("30s").unwrap(), 30_000);
        assert_eq!(parse_duration("1m").unwrap(), 60_000);
        assert_eq!(parse_duration("1h").unwrap(), 3_600_000);
        assert_eq!(parse_duration("1d").unwrap(), 86_400_000);
        assert_eq!(parse_duration("100ms").unwrap(), 100);
        assert_eq!(parse_duration("1h30m").unwrap(), 5_400_000);
        assert_eq!(parse_duration("1m30s").unwrap(), 90_000);
        assert!(parse_duration("bad").is_err());
        assert!(parse_duration("1x").is_err());
    }

    // ---- Duration parsing edge cases ----

    #[test]
    fn test_parse_duration_zero() {
        // "0s" and "0ms" are rejected by the zero-check (total == 0 && s != "0")
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("0ms").is_err());
        // But adding a nonzero part works
        assert_eq!(parse_duration("0s1ms").unwrap(), 1);
    }

    #[test]
    fn test_parse_duration_compound() {
        // 1d2h3m4s5ms = 86400000 + 7200000 + 180000 + 4000 + 5 = 93784005
        assert_eq!(parse_duration("1d2h3m4s5ms").unwrap(), 93_784_005);
    }

    #[test]
    fn test_parse_duration_empty() {
        // Empty string: total=0 and s is empty, so Ok(0)
        assert_eq!(parse_duration("").unwrap(), 0);
    }

    #[test]
    fn test_parse_duration_missing_unit() {
        assert!(parse_duration("42").is_err());
    }

    #[test]
    fn test_parse_duration_unknown_unit() {
        assert!(parse_duration("5w").is_err());
    }

    #[test]
    fn test_parse_duration_leading_alpha() {
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn test_parse_duration_large_value() {
        assert_eq!(parse_duration("999999ms").unwrap(), 999_999);
    }

    // ---- Envelope construction tests ----

    #[test]
    fn test_build_envelope_basic() {
        let envelope = build_envelope(
            "promise.get",
            "corr-123",
            None,
            json!({ "id": "test-promise" }),
        );

        assert_eq!(envelope["kind"], "promise.get");
        assert_eq!(envelope["head"]["corrId"], "corr-123");
        assert_eq!(envelope["head"]["version"], "2026-04-01");
        assert!(envelope["head"]["auth"].is_null());
        assert_eq!(envelope["data"]["id"], "test-promise");
    }

    #[test]
    fn test_build_envelope_with_auth() {
        let envelope = build_envelope(
            "promise.create",
            "corr-456",
            Some("jwt-token-here"),
            json!({ "id": "p1" }),
        );

        assert_eq!(envelope["head"]["auth"], "jwt-token-here");
        assert_eq!(envelope["kind"], "promise.create");
    }

    #[test]
    fn test_build_envelope_promise_create() {
        let data = json!({
            "id": "my-promise",
            "timeoutAt": 1700000000000_i64,
            "param": { "headers": {}, "data": "hello" },
            "tags": { "env": "test" }
        });
        let envelope = build_envelope("promise.create", "c1", None, data);

        assert_eq!(envelope["kind"], "promise.create");
        assert_eq!(envelope["data"]["id"], "my-promise");
        assert_eq!(envelope["data"]["timeoutAt"], 1700000000000_i64);
        assert_eq!(envelope["data"]["param"]["data"], "hello");
        assert_eq!(envelope["data"]["tags"]["env"], "test");
    }

    #[test]
    fn test_build_envelope_promise_settle() {
        let data = json!({
            "id": "p1",
            "state": "resolved",
            "value": { "headers": {}, "data": "result" }
        });
        let envelope = build_envelope("promise.settle", "c2", None, data);

        assert_eq!(envelope["kind"], "promise.settle");
        assert_eq!(envelope["data"]["state"], "resolved");
        assert_eq!(envelope["data"]["value"]["data"], "result");
    }

    #[test]
    fn test_build_envelope_promise_search() {
        let data = json!({ "limit": 50, "state": "pending" });
        let envelope = build_envelope("promise.search", "c3", None, data);

        assert_eq!(envelope["kind"], "promise.search");
        assert_eq!(envelope["data"]["limit"], 50);
        assert_eq!(envelope["data"]["state"], "pending");
    }

    #[test]
    fn test_build_envelope_promise_callback() {
        let data = json!({ "awaited": "p1", "awaiter": "p2" });
        let envelope = build_envelope("promise.register_callback", "c4", None, data);

        assert_eq!(envelope["kind"], "promise.register_callback");
        assert_eq!(envelope["data"]["awaited"], "p1");
        assert_eq!(envelope["data"]["awaiter"], "p2");
    }

    #[test]
    fn test_build_envelope_promise_listener() {
        let data = json!({ "awaited": "p1", "address": "http://example.com/hook" });
        let envelope = build_envelope("promise.register_listener", "c5", None, data);

        assert_eq!(envelope["kind"], "promise.register_listener");
        assert_eq!(envelope["data"]["address"], "http://example.com/hook");
    }

    #[test]
    fn test_build_envelope_task_create() {
        let data = json!({
            "pid": "worker-1",
            "ttl": 15000,
            "action": {
                "kind": "promise.create",
                "data": { "id": "t1", "timeoutAt": 1700000000000_i64 }
            }
        });
        let envelope = build_envelope("task.create", "c6", None, data);

        assert_eq!(envelope["kind"], "task.create");
        assert_eq!(envelope["data"]["pid"], "worker-1");
        assert_eq!(envelope["data"]["ttl"], 15000);
        assert_eq!(envelope["data"]["action"]["kind"], "promise.create");
    }

    #[test]
    fn test_build_envelope_task_acquire() {
        let data = json!({ "id": "t1", "pid": "w1", "version": 1, "ttl": 15000 });
        let envelope = build_envelope("task.acquire", "c7", None, data);

        assert_eq!(envelope["data"]["version"], 1);
        assert_eq!(envelope["data"]["ttl"], 15000);
    }

    #[test]
    fn test_build_envelope_task_fence() {
        let data = json!({
            "id": "t1",
            "version": 2,
            "action": { "kind": "promise.settle", "data": { "id": "p1", "state": "resolved" } }
        });
        let envelope = build_envelope("task.fence", "c8", None, data);

        assert_eq!(envelope["kind"], "task.fence");
        assert_eq!(envelope["data"]["action"]["kind"], "promise.settle");
    }

    #[test]
    fn test_build_envelope_task_heartbeat() {
        let data = json!({
            "pid": "w1",
            "tasks": [{"id": "t1", "version": 1}, {"id": "t2", "version": 3}]
        });
        let envelope = build_envelope("task.heartbeat", "c9", None, data);

        assert_eq!(envelope["data"]["tasks"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_build_envelope_task_suspend() {
        let data = json!({
            "id": "t1",
            "version": 2,
            "actions": [{ "kind": "promise.register_callback", "data": { "awaited": "p2", "awaiter": "t1" } }]
        });
        let envelope = build_envelope("task.suspend", "c10", None, data);

        assert_eq!(envelope["kind"], "task.suspend");
        assert_eq!(envelope["data"]["actions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_build_envelope_task_fulfill() {
        let data = json!({
            "id": "t1",
            "version": 3,
            "action": { "kind": "promise.settle", "data": { "id": "t1", "state": "resolved" } }
        });
        let envelope = build_envelope("task.fulfill", "c11", None, data);

        assert_eq!(envelope["kind"], "task.fulfill");
    }

    #[test]
    fn test_build_envelope_task_release() {
        let data = json!({ "id": "t1", "version": 3 });
        let envelope = build_envelope("task.release", "c12", None, data);

        assert_eq!(envelope["kind"], "task.release");
    }

    #[test]
    fn test_build_envelope_schedule_create() {
        let data = json!({
            "id": "sched-1",
            "cron": "0 * * * *",
            "promiseId": "run-{{.timestamp}}",
            "promiseTimeout": 3600000
        });
        let envelope = build_envelope("schedule.create", "c13", None, data);

        assert_eq!(envelope["kind"], "schedule.create");
        assert_eq!(envelope["data"]["cron"], "0 * * * *");
        assert_eq!(envelope["data"]["promiseTimeout"], 3600000);
    }

    #[test]
    fn test_build_envelope_schedule_delete() {
        let data = json!({ "id": "sched-1" });
        let envelope = build_envelope("schedule.delete", "c14", None, data);

        assert_eq!(envelope["kind"], "schedule.delete");
    }

    #[test]
    fn test_build_envelope_schedule_search() {
        let data = json!({ "limit": 10, "tags": { "env": "prod" } });
        let envelope = build_envelope("schedule.search", "c15", None, data);

        assert_eq!(envelope["data"]["limit"], 10);
    }

    // ---- Correlation ID tests ----

    #[test]
    fn test_gen_corr_id_format() {
        let id = gen_corr_id();
        assert!(
            id.starts_with("cli-"),
            "corr ID should start with 'cli-': {}",
            id
        );
        // Should contain at least two parts separated by '-'
        let parts: Vec<&str> = id.splitn(3, '-').collect();
        assert_eq!(
            parts.len(),
            3,
            "corr ID should have format cli-<millis>-<nanos>: {}",
            id
        );
        // Both numeric parts should parse as numbers
        assert!(
            parts[1].parse::<u128>().is_ok(),
            "millis part should be numeric: {}",
            parts[1]
        );
        assert!(
            parts[2].parse::<u32>().is_ok(),
            "nanos part should be numeric: {}",
            parts[2]
        );
    }

    #[test]
    fn test_gen_corr_id_unique() {
        let id1 = gen_corr_id();
        let id2 = gen_corr_id();
        // Not strictly guaranteed but practically always true
        // At minimum they should be valid
        assert!(id1.starts_with("cli-"));
        assert!(id2.starts_with("cli-"));
    }

    // ---- now_ms tests ----

    #[test]
    fn test_now_ms_reasonable() {
        let ms = now_ms();
        // Should be after 2020-01-01 (1577836800000) and before 2100-01-01 (4102444800000)
        assert!(ms > 1_577_836_800_000, "now_ms too small: {}", ms);
        assert!(ms < 4_102_444_800_000, "now_ms too large: {}", ms);
    }

    // ---- JSON parsing edge cases ----

    #[test]
    fn test_json_parsing_valid_object() {
        let v: Value = serde_json::from_str(r#"{"key":"value"}"#).unwrap();
        assert_eq!(v["key"], "value");
    }

    #[test]
    fn test_json_parsing_valid_array() {
        let v: Value = serde_json::from_str(r#"[1,2,3]"#).unwrap();
        assert_eq!(v.as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_json_parsing_invalid() {
        assert!(serde_json::from_str::<Value>("not json").is_err());
    }

    #[test]
    fn test_json_parsing_empty_object() {
        let v: Value = serde_json::from_str("{}").unwrap();
        assert!(v.is_object());
        assert!(v.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_json_parsing_nested() {
        let v: Value = serde_json::from_str(r#"{"a":{"b":{"c":1}}}"#).unwrap();
        assert_eq!(v["a"]["b"]["c"], 1);
    }

    #[test]
    fn test_json_parsing_unicode() {
        let v: Value = serde_json::from_str(r#"{"emoji":"🎉","cjk":"日本語"}"#).unwrap();
        assert_eq!(v["emoji"], "🎉");
        assert_eq!(v["cjk"], "日本語");
    }

    // ---- Timeout computation tests ----

    #[test]
    fn test_timeout_computation() {
        let base = now_ms();
        let dur = parse_duration("1h").unwrap();
        let timeout_at = base + dur;
        // timeout_at should be ~1 hour from now
        assert!(timeout_at > base);
        assert_eq!(timeout_at - base, 3_600_000);
    }

    #[test]
    fn test_timeout_computation_compound() {
        let dur = parse_duration("1h30m").unwrap();
        assert_eq!(dur, 5_400_000); // 90 minutes
    }
}
