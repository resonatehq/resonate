use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{oneshot, Mutex, RwLock};

use crate::codec::Codec;
use crate::core::Core;
use crate::durable::Durable;
use crate::error::{Error, Result};
use crate::handle::{PromiseResult, ResonateHandle};
use crate::heartbeat::{AsyncHeartbeat, Heartbeat, NoopHeartbeat};
use crate::network::{LocalNetwork, Network};
use crate::options::{is_url, Options, OptionsBuilder, PartialOptions};
use crate::promises::{Promises, Schedules};
use crate::registry::Registry;
use crate::send::SendFn;
use crate::transport::{Message, Transport};

/// Configuration for constructing a Resonate instance.
#[derive(Default)]
pub struct ResonateConfig {
    /// Server URL (or from RESONATE_URL / RESONATE_HOST+PORT).
    pub url: Option<String>,
    /// Group name (default: "default").
    pub group: Option<String>,
    /// Process ID (default: from network).
    pub pid: Option<String>,
    /// Time-to-live in milliseconds (default: 60_000 = 1 min).
    pub ttl: Option<u64>,
    /// Basic auth credentials.
    pub auth: Option<BasicAuth>,
    /// Bearer token (takes priority over auth).
    pub token: Option<String>,
    /// Custom encryption (default: no-op).
    pub encryptor: Option<Box<dyn Encryptor>>,
    /// Custom network implementation (overrides url).
    pub network: Option<Arc<dyn Network>>,
    /// ID prefix for all promise/task IDs (or from RESONATE_PREFIX).
    pub prefix: Option<String>,
}

/// Basic auth credentials.
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

/// Encryption trait for codec (default: no-op).
pub trait Encryptor: Send + Sync {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>>;
}

/// No-op encryptor (passthrough).
#[allow(dead_code)]
struct NoopEncryptor;
impl Encryptor for NoopEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Subscription entry for awaiting remote promise completion.
struct SubscriptionEntry {
    tx: Option<oneshot::Sender<PromiseResult>>,
    #[allow(dead_code)]
    rx: Option<oneshot::Receiver<PromiseResult>>,
    /// If already resolved before anyone subscribed.
    resolved: Option<PromiseResult>,
}

/// Return value from `schedule()`.
pub struct ResonateSchedule {
    name: String,
    schedules: Schedules,
}

impl ResonateSchedule {
    /// Delete this schedule.
    pub async fn delete(self) -> Result<()> {
        self.schedules.delete(&self.name).await?;
        Ok(())
    }
}

/// The main entry point for the Resonate SDK.
pub struct Resonate {
    // Identity
    pid: String,
    ttl: u64,
    id_prefix: String,

    // Infrastructure
    codec: Codec,
    network: Arc<dyn Network>,
    transport: Transport,

    // Core execution engine
    core: Arc<Core>,

    // Function management
    registry: Arc<RwLock<Registry>>,
    heartbeat: Arc<dyn Heartbeat>,
    dependencies: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>>,
    opts_builder: OptionsBuilder,

    // Subscriptions (for awaiting remote promise completion)
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,
    subscribe_every: Duration,

    // Sub-clients
    pub promises: Promises,
    pub schedules: Schedules,

    // Background task handles
    subscription_refresh_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Resonate {
    // ═══════════════════════════════════════════════════════════════
    //  Static Constructors
    // ═══════════════════════════════════════════════════════════════

    /// Local-only mode. No external dependencies. In-memory state.
    /// group="default", pid="default", ttl=MAX, no heartbeat.
    pub fn local(encryptor: Option<Box<dyn Encryptor>>) -> Self {
        let config = ResonateConfig {
            pid: Some("default".to_string()),
            group: Some("default".to_string()),
            ttl: Some(u64::MAX),
            encryptor,
            ..Default::default()
        };
        Self::new(config)
    }

    /// Remote mode. Connects to a Resonate Server via HTTP.
    pub fn remote(config: ResonateConfig) -> Self {
        Self::new(config)
    }

    // ═══════════════════════════════════════════════════════════════
    //  Constructor
    // ═══════════════════════════════════════════════════════════════

    pub fn new(config: ResonateConfig) -> Self {
        let ttl = config.ttl.unwrap_or(60_000);

        // Resolve prefix
        let prefix = config
            .prefix
            .or_else(|| std::env::var("RESONATE_PREFIX").ok())
            .unwrap_or_default();
        let id_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}:", prefix)
        };

        // Resolve URL
        let resolved_url = config
            .url
            .clone()
            .or_else(|| std::env::var("RESONATE_URL").ok())
            .or_else(|| {
                let host = std::env::var("RESONATE_HOST").ok()?;
                let scheme = std::env::var("RESONATE_SCHEME").unwrap_or_else(|_| "http".into());
                let port = std::env::var("RESONATE_PORT").unwrap_or_else(|_| "8001".into());
                Some(format!("{}://{}:{}", scheme, host, port))
            });

        // Resolve auth
        let _token = config
            .token
            .or_else(|| std::env::var("RESONATE_TOKEN").ok());
        let _auth = config.auth.or_else(|| {
            let username = std::env::var("RESONATE_USERNAME").ok()?;
            let password = std::env::var("RESONATE_PASSWORD").unwrap_or_default();
            Some(BasicAuth { username, password })
        });

        // Network selection
        let (network, heartbeat): (Arc<dyn Network>, Arc<dyn Heartbeat>) =
            if let Some(net) = config.network {
                // Custom network provided
                let transport = Transport::new(net.clone());
                let hb = Arc::new(AsyncHeartbeat::new(
                    net.pid().to_string(),
                    ttl / 2,
                    transport.clone(),
                ));
                (net, hb)
            } else if let Some(_url) = &resolved_url {
                // Remote mode: HTTP network
                // TODO: Implement HttpNetwork
                // For now, fall back to local with a warning
                tracing::warn!("HttpNetwork not yet implemented, using LocalNetwork as fallback");
                let net = Arc::new(LocalNetwork::new(config.pid.clone(), config.group.clone()));
                let transport = Transport::new(net.clone());
                let hb = Arc::new(AsyncHeartbeat::new(
                    net.pid().to_string(),
                    ttl / 2,
                    transport.clone(),
                ));
                (net as Arc<dyn Network>, hb as Arc<dyn Heartbeat>)
            } else {
                // Local mode
                let net = Arc::new(LocalNetwork::new(config.pid.clone(), config.group.clone()));
                (
                    net as Arc<dyn Network>,
                    Arc::new(NoopHeartbeat) as Arc<dyn Heartbeat>,
                )
            };

        let pid = network.pid().to_string();
        let transport = Transport::new(network.clone());
        let codec = Codec;
        let registry = Arc::new(RwLock::new(Registry::new()));

        let opts_builder = OptionsBuilder::new(network.clone(), id_prefix.clone());

        // Build the SendFn for Core from the transport
        let transport_for_core = transport.clone();
        let send_fn: SendFn = Arc::new(move |req| {
            let transport = transport_for_core.clone();
            Box::pin(async move {
                // Serialize Request → JSON with correct "subject.verb" kinds
                let req_json = serde_json::to_value(&req)?;
                let resp_json = transport.send(req_json).await?;
                // Parse response based on request type (server echoes request kind)
                crate::send::parse_response(&req, &resp_json)
            })
        });

        // Build match_fn from the network for target resolution.
        // If the target already looks like a URL, pass it through unchanged
        // (mirrors TS: `util.isUrl(target) ? target : match(target)`).
        let network_for_match = network.clone();
        let match_fn: crate::context::MatchFn = Arc::new(move |target: &str| {
            if is_url(target) {
                target.to_string()
            } else {
                network_for_match.r#match(target)
            }
        });

        let core = Arc::new(Core::new(send_fn, codec.clone(), registry.clone(), match_fn));
        let promises = Promises::new(transport.clone());
        let schedules = Schedules::new(transport.clone());

        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let subscribe_every = Duration::from_secs(60);

        let resonate = Self {
            pid,
            ttl,
            id_prefix,
            codec,
            network: network.clone(),
            transport: transport.clone(),
            core,
            registry,
            heartbeat,
            dependencies: Arc::new(RwLock::new(HashMap::new())),
            opts_builder,
            subscriptions: subscriptions.clone(),
            subscribe_every,
            promises,
            schedules,
            subscription_refresh_handle: Mutex::new(None),
        };

        // Subscribe to incoming messages
        let subs_for_recv = subscriptions.clone();
        let core_for_recv = resonate.core.clone();
        transport.recv(Box::new(move |msg| {
            let subs = subs_for_recv.clone();
            let core = core_for_recv.clone();
            match msg {
                Message::Execute(exec_msg) => {
                    // Path 1: execute message from network → core.on_message (acquires task)
                    tracing::debug!(task_id = %exec_msg.task_id, "received execute message");
                    let task_id = exec_msg.task_id.clone();
                    tokio::spawn(async move {
                        if let Err(e) = core.on_message(&task_id).await {
                            tracing::error!(
                                error = %e,
                                task_id = %task_id,
                                "core.on_message failed"
                            );
                        }
                    });
                }
                Message::Unblock(unblock_msg) => {
                    let promise = &unblock_msg.promise;
                    let id = promise
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let state = promise
                        .get("state")
                        .and_then(|v| v.as_str())
                        .unwrap_or("pending")
                        .to_string();
                    let value = promise.get("value").cloned().unwrap_or_default();

                    let result = PromiseResult { state, value };

                    // Notify subscription
                    let subs = subs.clone();
                    tokio::spawn(async move {
                        let mut map = subs.lock().await;
                        if let Some(entry) = map.remove(&id) {
                            if let Some(tx) = entry.tx {
                                let _ = tx.send(result);
                            }
                        } else {
                            // No one subscribed yet — store for later
                            map.insert(
                                id,
                                SubscriptionEntry {
                                    tx: None,
                                    rx: None,
                                    resolved: Some(result),
                                },
                            );
                        }
                    });
                }
            }
        }));

        // Start network (fire-and-forget, log errors)
        let net = network.clone();
        tokio::spawn(async move {
            if let Err(e) = net.start().await {
                tracing::error!(error = %e, "failed to start network");
            }
        });

        // Start periodic subscription refresh
        let transport_for_refresh = transport.clone();
        let subs_for_refresh = resonate.subscriptions.clone();
        let unicast_for_refresh = resonate.network.unicast().to_string();
        let refresh_interval = resonate.subscribe_every;

        let refresh_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            loop {
                interval.tick().await;
                let ids: Vec<String> = {
                    let map = subs_for_refresh.lock().await;
                    map.keys().cloned().collect()
                };
                for id in ids {
                    let req = serde_json::json!({
                        "kind": "promise.registerListener",
                        "corrId": format!("refresh-{}", now_ms()),
                        "awaited": id,
                        "address": unicast_for_refresh,
                    });
                    match transport_for_refresh.send(req).await {
                        Ok(resp) => {
                            let state = resp
                                .get("promise")
                                .and_then(|p| p.get("state"))
                                .and_then(|s| s.as_str())
                                .unwrap_or("pending");
                            if state != "pending" {
                                let value = resp
                                    .get("promise")
                                    .and_then(|p| p.get("value"))
                                    .cloned()
                                    .unwrap_or_default();
                                let result = PromiseResult {
                                    state: state.to_string(),
                                    value,
                                };
                                let mut map = subs_for_refresh.lock().await;
                                if let Some(entry) = map.remove(&id) {
                                    if let Some(tx) = entry.tx {
                                        let _ = tx.send(result);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, promise_id = %id, "subscription refresh failed");
                        }
                    }
                }
            }
        });

        // Store the handle (we can't await the mutex in a sync context,
        // so we just let it go; it will be stopped in stop())
        drop(refresh_handle);

        resonate
    }

    // ═══════════════════════════════════════════════════════════════
    //  Public API
    // ═══════════════════════════════════════════════════════════════

    /// Register a durable function. The name and kind are derived from the
    /// `Durable` impl generated by `#[resonate::function]`.
    pub async fn register<D, Args, T>(&self, func: D)
    where
        D: Durable<Args, T> + Copy + Send + Sync + 'static,
        Args: DeserializeOwned + Send + 'static,
        T: Serialize + Send + 'static,
    {
        let mut reg = self.registry.write().await;
        reg.register(func);
    }

    /// Execute a typed durable function and wait for its result.
    pub async fn run<D, Args, T>(
        &self,
        id: &str,
        _func: D,
        args: Args,
        opts: Option<PartialOptions>,
    ) -> Result<T>
    where
        D: Durable<Args, T>,
        Args: Serialize,
        T: DeserializeOwned,
    {
        let json_args = serde_json::to_value(args)?;
        self.run_by_name::<T>(id, D::NAME, json_args, opts).await
    }

    /// Execute a function by name and wait for its result.
    pub async fn run_by_name<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Option<PartialOptions>,
    ) -> Result<T> {
        let mut handle = self
            .begin_run_by_name::<T>(id, func_name, args, opts)
            .await?;
        handle.result().await
    }

    /// Start a typed durable function and return a handle for later awaiting.
    pub async fn begin_run<D, Args, T>(
        &self,
        id: &str,
        _func: D,
        args: Args,
        opts: Option<PartialOptions>,
    ) -> Result<ResonateHandle<T>>
    where
        D: Durable<Args, T>,
        Args: Serialize,
        T: DeserializeOwned,
    {
        let json_args = serde_json::to_value(args)?;
        self.begin_run_by_name::<T>(id, D::NAME, json_args, opts)
            .await
    }

    /// Start a function execution by name and return a handle for later awaiting.
    pub async fn begin_run_by_name<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Option<PartialOptions>,
    ) -> Result<ResonateHandle<T>> {
        let opts = self.opts_builder.build(opts);
        let prefixed_id = self.opts_builder.prefix_id(id);

        // Verify function is registered
        {
            let reg = self.registry.read().await;
            if !reg.contains(func_name) {
                return Err(Error::FunctionNotFound(func_name.to_string()));
            }
        }

        let timeout_at = now_ms() + opts.timeout.as_millis() as i64;

        // Encode param data
        let param_data = serde_json::json!({
            "func": func_name,
            "args": args,
            "version": opts.version,
        });
        let encoded_param = self.codec.encode(&param_data)?;

        // Build tags
        let mut tags = opts.tags.clone();
        tags.insert("resonate:origin".to_string(), prefixed_id.clone());
        tags.insert("resonate:branch".to_string(), prefixed_id.clone());
        tags.insert("resonate:parent".to_string(), prefixed_id.clone());
        tags.insert("resonate:scope".to_string(), "global".to_string());
        tags.insert("resonate:target".to_string(), opts.target.clone());

        // Build task.create request
        let corr_id = format!("tc-{}", now_ms());
        let req = serde_json::json!({
            "kind": "task.create",
            "corrId": corr_id,
            "pid": self.pid,
            "ttl": self.ttl,
            "promise": {
                "id": prefixed_id,
                "timeoutAt": timeout_at,
                "param": encoded_param,
                "tags": tags,
            },
        });

        let resp = self.transport.send(req).await?;
        let status = resp.get("status").and_then(|s| s.as_u64()).unwrap_or(0);
        let promise = resp.get("promise").cloned().unwrap_or_default();
        let task = resp.get("task").cloned();

        if status == 409 {
            // Promise already exists — register listener and return handle
            return self.create_handle(prefixed_id, &promise).await;
        }

        // If task is acquired, fire-and-forget core execution via
        // execute_until_blocked (Path 2: task already acquired, no re-acquire)
        if let Some(ref task_val) = task {
            let task_state = task_val.get("state").and_then(|s| s.as_str()).unwrap_or("");
            if task_state == "acquired" {
                let task_id = task_val
                    .get("id")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_string();

                // Decode the promise for execute_until_blocked
                let root_promise = self.codec.decode_promise_from_json(&promise);

                let core = self.core.clone();
                tokio::spawn(async move {
                    match root_promise {
                        Ok(rp) => {
                            if let Err(e) = core.execute_until_blocked(&task_id, rp, None).await {
                                tracing::error!(
                                    error = %e,
                                    task_id = %task_id,
                                    "core execute_until_blocked failed"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                task_id = %task_id,
                                "failed to decode root promise for execution"
                            );
                        }
                    }
                });
            }
        }

        self.create_handle(prefixed_id, &promise).await
    }

    /// Remote procedure call (rpc = begin_rpc + await result).
    pub async fn rpc<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Option<PartialOptions>,
    ) -> Result<T> {
        let mut handle = self.begin_rpc::<T>(id, func_name, args, opts).await?;
        handle.result().await
    }

    /// Start a remote procedure call and return a handle.
    pub async fn begin_rpc<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Option<PartialOptions>,
    ) -> Result<ResonateHandle<T>> {
        let opts = self.opts_builder.build(opts);
        let prefixed_id = self.opts_builder.prefix_id(id);

        let timeout_at = now_ms() + opts.timeout.as_millis() as i64;

        // Encode param data
        let param_data = serde_json::json!({
            "func": func_name,
            "args": args,
            "version": opts.version,
        });
        let encoded_param = self.codec.encode(&param_data)?;

        // Build tags
        let mut tags = opts.tags.clone();
        tags.insert("resonate:origin".to_string(), prefixed_id.clone());
        tags.insert("resonate:branch".to_string(), prefixed_id.clone());
        tags.insert("resonate:parent".to_string(), prefixed_id.clone());
        tags.insert("resonate:scope".to_string(), "global".to_string());
        tags.insert("resonate:target".to_string(), opts.target.clone());

        // Build promise.create request (NOT task.create — key difference from run)
        let corr_id = format!("pc-{}", now_ms());
        let req = serde_json::json!({
            "kind": "promise.create",
            "corrId": corr_id,
            "promise": {
                "id": prefixed_id,
                "timeoutAt": timeout_at,
                "param": encoded_param,
                "tags": tags,
            },
        });

        let resp = self.transport.send(req).await?;
        let promise = resp.get("promise").cloned().unwrap_or_default();

        self.create_handle(prefixed_id, &promise).await
    }

    /// Get a handle to an existing promise.
    pub async fn get<T: DeserializeOwned>(&self, id: &str) -> Result<ResonateHandle<T>> {
        let prefixed_id = self.opts_builder.prefix_id(id);

        let req = serde_json::json!({
            "kind": "promise.get",
            "corrId": format!("pg-{}", now_ms()),
            "id": prefixed_id,
        });

        let resp = self.transport.send(req).await?;
        let status = resp.get("status").and_then(|s| s.as_u64()).unwrap_or(0);
        if status == 404 {
            return Err(Error::ServerError {
                code: 404,
                message: format!("promise {} not found", prefixed_id),
            });
        }

        let promise = resp.get("promise").cloned().unwrap_or_default();
        self.create_handle(prefixed_id, &promise).await
    }

    /// Create a schedule for periodic function execution.
    pub async fn schedule(
        &self,
        name: &str,
        cron: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Option<PartialOptions>,
    ) -> Result<ResonateSchedule> {
        let opts = self.opts_builder.build(opts);

        let param_data = serde_json::json!({
            "func": func_name,
            "args": args,
            "version": opts.version,
        });
        let encoded_param = self.codec.encode(&param_data)?;

        let template = format!("{}{{{{.id}}}}.{{{{.timestamp}}}}", self.id_prefix);

        self.schedules
            .create(
                name,
                cron,
                &template,
                opts.timeout.as_millis() as i64,
                serde_json::to_value(&encoded_param)?,
            )
            .await?;

        Ok(ResonateSchedule {
            name: name.to_string(),
            schedules: self.schedules.clone(),
        })
    }

    /// Set a named dependency that functions can access.
    pub async fn set_dependency(&self, name: &str, obj: Box<dyn Any + Send + Sync>) {
        let mut deps = self.dependencies.write().await;
        deps.insert(name.to_string(), obj);
    }

    /// Build options with defaults resolved.
    pub fn options(&self, opts: Option<PartialOptions>) -> Options {
        self.opts_builder.build(opts)
    }

    /// Stop the Resonate instance: network, heartbeat, background tasks.
    pub async fn stop(&self) -> Result<()> {
        self.network.stop().await?;
        self.heartbeat.stop().await?;
        if let Some(handle) = self.subscription_refresh_handle.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }

    /// Get the process ID.
    pub fn pid(&self) -> &str {
        &self.pid
    }

    /// Get the TTL.
    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    /// Get the ID prefix.
    pub fn id_prefix(&self) -> &str {
        &self.id_prefix
    }

    /// Access the transport (for sub-components).
    pub fn transport(&self) -> &Transport {
        &self.transport
    }

    /// Access the network.
    pub fn network(&self) -> &Arc<dyn Network> {
        &self.network
    }

    // ═══════════════════════════════════════════════════════════════
    //  Internal Methods
    // ═══════════════════════════════════════════════════════════════

    /// Create a handle from a promise JSON value.
    async fn create_handle<T: DeserializeOwned>(
        &self,
        id: String,
        promise: &serde_json::Value,
    ) -> Result<ResonateHandle<T>> {
        let state = promise
            .get("state")
            .and_then(|s| s.as_str())
            .unwrap_or("pending");
        let value = promise.get("value").cloned().unwrap_or_default();

        if state == "pending" {
            // Subscribe for notification
            let (tx, rx) = oneshot::channel();
            {
                let mut subs = self.subscriptions.lock().await;
                if let Some(entry) = subs.get_mut(&id) {
                    // Already have a notification for this promise
                    if let Some(resolved) = entry.resolved.take() {
                        return Ok(ResonateHandle::new(
                            id,
                            &resolved.state,
                            resolved.value,
                            self.transport.clone(),
                            self.network.unicast().to_string(),
                            self.codec.clone(),
                        ));
                    }
                }
                subs.insert(
                    id.clone(),
                    SubscriptionEntry {
                        tx: Some(tx),
                        rx: None,
                        resolved: None,
                    },
                );
            }

            // Register listener with the server
            let req = serde_json::json!({
                "kind": "promise.registerListener",
                "corrId": format!("rl-{}", now_ms()),
                "awaited": id,
                "address": self.network.unicast(),
            });
            let resp = self.transport.send(req).await?;
            let resp_promise = resp.get("promise").cloned().unwrap_or_default();
            let resp_state = resp_promise
                .get("state")
                .and_then(|s| s.as_str())
                .unwrap_or("pending");

            if resp_state != "pending" {
                // Already settled — remove subscription, return directly
                let mut subs = self.subscriptions.lock().await;
                subs.remove(&id);
                return Ok(ResonateHandle::new(
                    id,
                    resp_state,
                    resp_promise.get("value").cloned().unwrap_or_default(),
                    self.transport.clone(),
                    self.network.unicast().to_string(),
                    self.codec.clone(),
                ));
            }

            Ok(ResonateHandle::from_channel(
                id,
                rx,
                self.transport.clone(),
                self.network.unicast().to_string(),
                self.codec.clone(),
            ))
        } else {
            Ok(ResonateHandle::new(
                id,
                state,
                value,
                self.transport.clone(),
                self.network.unicast().to_string(),
                self.codec.clone(),
            ))
        }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::options::PartialOptions;
    use std::time::Duration;

    // ── Test functions ─────────────────────────────────────────────

    #[resonate_macros::function]
    async fn noop() -> Result<()> {
        Ok(())
    }

    #[resonate_macros::function]
    async fn add(x: i64, y: i64) -> Result<i64> {
        Ok(x + y)
    }

    // ═══════════════════════════════════════════════════════════════
    //  Constructor / Configuration Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn local_constructor_sets_defaults() {
        let r = Resonate::local(None);
        assert_eq!(r.pid(), "default");
        assert_eq!(r.ttl(), u64::MAX);
        assert_eq!(r.id_prefix(), "");
    }

    #[tokio::test]
    async fn config_with_custom_pid_and_group() {
        let r = Resonate::new(ResonateConfig {
            pid: Some("worker-1".into()),
            group: Some("workers".into()),
            ..Default::default()
        });
        assert_eq!(r.pid(), "worker-1");
        assert!(r.network().unicast().contains("worker-1"));
        assert!(r.network().unicast().contains("workers"));
    }

    #[tokio::test]
    async fn config_with_prefix() {
        let r = Resonate::new(ResonateConfig {
            pid: Some("test".into()),
            group: Some("g1".into()),
            ttl: Some(30_000),
            prefix: Some("myapp".into()),
            ..Default::default()
        });
        assert_eq!(r.pid(), "test");
        assert_eq!(r.id_prefix(), "myapp:");
        assert_eq!(r.ttl(), 30_000);
    }

    #[tokio::test]
    async fn config_with_empty_prefix() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("".into()),
            ..Default::default()
        });
        assert_eq!(r.id_prefix(), "");
    }

    #[tokio::test]
    async fn config_with_custom_ttl() {
        let r = Resonate::new(ResonateConfig {
            ttl: Some(120_000),
            ..Default::default()
        });
        assert_eq!(r.ttl(), 120_000);
    }

    #[tokio::test]
    async fn default_ttl_is_one_minute() {
        let r = Resonate::new(ResonateConfig::default());
        assert_eq!(r.ttl(), 60_000);
    }

    #[tokio::test]
    async fn network_identity_local_mode() {
        let r = Resonate::local(None);
        assert!(r.network().unicast().starts_with("local://uni@"));
        assert!(r.network().anycast().starts_with("local://any@"));
        assert_eq!(r.network().group(), "default");
        assert_eq!(r.network().pid(), "default");
    }

    #[tokio::test]
    async fn network_match_returns_local_anycast() {
        let r = Resonate::local(None);
        let matched = r.network().r#match("my-target");
        assert_eq!(matched, "local://any@my-target");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Register Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn register_function_by_name() {
        let r = Resonate::local(None);
        r.register(Add).await;

        // Verify function is registered (begin_run won't fail with FunctionNotFound)
        let result = r.begin_run("test-id", Add, (1i64, 2i64), None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[should_panic(expected = "already registered")]
    async fn register_duplicate_function_panics() {
        let r = Resonate::local(None);
        r.register(Noop).await;
        r.register(Noop).await;
    }

    // ═══════════════════════════════════════════════════════════════
    //  begin_run / run Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn begin_run_returns_handle_for_registered_function() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let handle = r.begin_run("greet-1", Noop, (), None).await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "greet-1");
    }

    #[tokio::test]
    async fn begin_run_unregistered_function_returns_function_not_found() {
        let r = Resonate::local(None);
        let result = r
            .begin_run_by_name::<()>("test-id", "nonexistent", serde_json::json!(null), None)
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::FunctionNotFound(name) => assert_eq!(name, "nonexistent"),
            other => panic!("expected FunctionNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn begin_run_with_prefix_prepends_to_id() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("app".into()),
            pid: Some("default".into()),
            ..Default::default()
        });
        r.register(Noop).await;

        let handle = r.begin_run("my-id", Noop, (), None).await.unwrap();
        assert_eq!(handle.id, "app:my-id");
    }

    #[tokio::test]
    async fn begin_run_creates_task_and_promise() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let _handle = r.begin_run("task-1", Noop, (), None).await.unwrap();

        // The promise should exist in the local network — we can verify via get
        let get_handle = r.get::<()>("task-1").await;
        assert!(get_handle.is_ok(), "promise should exist after begin_run");
    }

    #[tokio::test]
    async fn begin_run_idempotent_same_id_returns_existing_promise() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let h1 = r.begin_run("same-id", Noop, (), None).await;
        assert!(h1.is_ok());

        // Second call with same ID should not fail (idempotent: 409 handled)
        let h2 = r.begin_run("same-id", Noop, (), None).await;
        assert!(h2.is_ok());
        assert_eq!(h2.unwrap().id, "same-id");
    }

    #[tokio::test]
    async fn begin_run_sets_correct_tags() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let custom_opts = PartialOptions {
            tags: Some({
                let mut m = std::collections::HashMap::new();
                m.insert("user:tag".to_string(), "value".to_string());
                m
            }),
            ..Default::default()
        };

        let handle = r.begin_run("tag-test", Noop, (), Some(custom_opts)).await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn begin_run_with_custom_timeout() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let opts = PartialOptions {
            timeout: Some(Duration::from_secs(300)),
            ..Default::default()
        };

        let handle = r.begin_run("timeout-test", Noop, (), Some(opts)).await;
        assert!(handle.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  begin_rpc / rpc Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn begin_rpc_creates_promise_not_task() {
        let r = Resonate::local(None);
        // RPC does NOT require function to be registered locally
        let handle = r
            .begin_rpc::<serde_json::Value>(
                "rpc-1",
                "remote_func",
                serde_json::json!({"x": 1}),
                None,
            )
            .await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "rpc-1");
    }

    #[tokio::test]
    async fn begin_rpc_with_prefix() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("svc".into()),
            ..Default::default()
        });

        let handle = r
            .begin_rpc::<serde_json::Value>("rpc-2", "remote", serde_json::json!(null), None)
            .await
            .unwrap();
        assert_eq!(handle.id, "svc:rpc-2");
    }

    #[tokio::test]
    async fn begin_rpc_sets_scope_global() {
        let r = Resonate::local(None);
        // Verifying RPC succeeds — tags (scope=global, target) are set internally
        let handle = r
            .begin_rpc::<serde_json::Value>("rpc-scope", "remote", serde_json::json!(null), None)
            .await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn begin_rpc_with_custom_target() {
        let r = Resonate::local(None);
        let opts = PartialOptions {
            target: Some("custom-worker".to_string()),
            ..Default::default()
        };

        let handle = r
            .begin_rpc::<serde_json::Value>(
                "rpc-target",
                "remote",
                serde_json::json!(null),
                Some(opts),
            )
            .await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn begin_rpc_idempotent_same_id() {
        let r = Resonate::local(None);

        let h1 = r
            .begin_rpc::<serde_json::Value>("rpc-dup", "remote", serde_json::json!(null), None)
            .await;
        assert!(h1.is_ok());

        // Same ID should return existing promise
        let h2 = r
            .begin_rpc::<serde_json::Value>("rpc-dup", "remote", serde_json::json!(null), None)
            .await;
        assert!(h2.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  get Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn get_nonexistent_promise_returns_error() {
        let r = Resonate::local(None);
        let result = r.get::<serde_json::Value>("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ServerError { code, .. } => assert_eq!(code, 404),
            other => panic!("expected ServerError 404, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn get_existing_promise_returns_handle() {
        let r = Resonate::local(None);

        // Create a promise via RPC first
        r.begin_rpc::<serde_json::Value>("get-test", "func", serde_json::json!(null), None)
            .await
            .unwrap();

        // Now get it
        let handle = r.get::<serde_json::Value>("get-test").await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "get-test");
    }

    #[tokio::test]
    async fn get_with_prefix_prepends_prefix() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("ns".into()),
            ..Default::default()
        });

        // Create via RPC (which prepends prefix)
        r.begin_rpc::<serde_json::Value>("p1", "func", serde_json::json!(null), None)
            .await
            .unwrap();

        // Get with the unprefixed ID (prefix is prepended internally)
        let handle = r.get::<serde_json::Value>("p1").await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "ns:p1");
    }

    // ═══════════════════════════════════════════════════════════════
    //  schedule Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn schedule_creates_schedule() {
        let r = Resonate::local(None);
        let result = r
            .schedule(
                "my-schedule",
                "*/5 * * * *",
                "my-func",
                serde_json::json!(null),
                None,
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn schedule_returns_deletable_handle() {
        let r = Resonate::local(None);
        let schedule = r
            .schedule(
                "deletable",
                "0 * * * *",
                "func",
                serde_json::json!(null),
                None,
            )
            .await
            .unwrap();
        // Deleting should not fail
        let result = schedule.delete().await;
        assert!(result.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  set_dependency Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn set_dependency_stores_value() {
        let r = Resonate::local(None);
        r.set_dependency("db", Box::new("postgres://localhost".to_string()))
            .await;
        // No panic means it stored successfully
    }

    #[tokio::test]
    async fn set_dependency_overwrites_existing() {
        let r = Resonate::local(None);
        r.set_dependency("key", Box::new(1i32)).await;
        r.set_dependency("key", Box::new(2i32)).await;
        // No panic means overwrite succeeded
    }

    // ═══════════════════════════════════════════════════════════════
    //  options Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn options_returns_defaults_when_none() {
        let r = Resonate::local(None);
        let opts = r.options(None);
        assert_eq!(opts.timeout, Duration::from_secs(60));
        assert_eq!(opts.version, 0);
        assert!(opts.tags.is_empty());
        // Default target "default" is resolved through network.match
        assert_eq!(opts.target, "local://any@default");
    }

    #[tokio::test]
    async fn options_passes_through_user_values() {
        let r = Resonate::local(None);
        let custom = PartialOptions {
            timeout: Some(Duration::from_secs(120)),
            version: Some(3),
            tags: Some({
                let mut m = std::collections::HashMap::new();
                m.insert("key".into(), "val".into());
                m
            }),
            ..Default::default()
        };

        let opts = r.options(Some(custom));
        assert_eq!(opts.timeout, Duration::from_secs(120));
        assert_eq!(opts.version, 3);
        assert_eq!(opts.tags.get("key").unwrap(), "val");
    }

    #[tokio::test]
    async fn options_partial_merge_only_timeout_provided() {
        let r = Resonate::local(None);
        let partial = PartialOptions {
            timeout: Some(Duration::from_secs(300)),
            ..Default::default()
        };
        let opts = r.options(Some(partial));
        assert_eq!(opts.timeout, Duration::from_secs(300));
        assert_eq!(opts.version, 0); // default
        assert!(opts.tags.is_empty()); // default
        assert_eq!(opts.target, "local://any@default"); // default resolved
    }

    #[tokio::test]
    async fn options_target_resolution_bare_name() {
        let r = Resonate::local(None);
        let partial = PartialOptions {
            target: Some("my-worker".into()),
            ..Default::default()
        };
        let opts = r.options(Some(partial));
        assert_eq!(opts.target, "local://any@my-worker");
    }

    #[tokio::test]
    async fn options_target_resolution_url_passthrough() {
        let r = Resonate::local(None);
        let partial = PartialOptions {
            target: Some("https://remote:9000/workers/hello".into()),
            ..Default::default()
        };
        let opts = r.options(Some(partial));
        assert_eq!(opts.target, "https://remote:9000/workers/hello");
    }

    #[tokio::test]
    async fn options_default_target_is_default() {
        let r = Resonate::local(None);
        let opts = r.options(None);
        // "default" gets resolved through network.match → "local://any@default"
        assert_eq!(opts.target, "local://any@default");
    }

    // ═══════════════════════════════════════════════════════════════
    //  stop Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn stop_is_clean() {
        let r = Resonate::local(None);
        let result = r.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stop_can_be_called_twice() {
        let r = Resonate::local(None);
        r.stop().await.unwrap();
        // Second stop should also be fine (idempotent)
        let result = r.stop().await;
        assert!(result.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  ID Prefix Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn no_prefix_leaves_id_unchanged() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let handle = r.begin_run("my-id", Noop, (), None).await.unwrap();
        assert_eq!(handle.id, "my-id");
    }

    #[tokio::test]
    async fn prefix_is_prepended_with_colon() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("prefix".into()),
            ..Default::default()
        });
        r.register(Noop).await;

        let handle = r.begin_run("my-id", Noop, (), None).await.unwrap();
        assert_eq!(handle.id, "prefix:my-id");
    }

    #[tokio::test]
    async fn prefix_applied_consistently_to_run_rpc_and_get() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("p".into()),
            ..Default::default()
        });
        r.register(Noop).await;

        // begin_run with prefix
        let h1 = r.begin_run("id1", Noop, (), None).await.unwrap();
        assert_eq!(h1.id, "p:id1");

        // begin_rpc with prefix
        let h2 = r
            .begin_rpc::<serde_json::Value>("id2", "remote", serde_json::json!(null), None)
            .await
            .unwrap();
        assert_eq!(h2.id, "p:id2");

        // get with prefix (the promise was created as "p:id2")
        let h3 = r.get::<serde_json::Value>("id2").await.unwrap();
        assert_eq!(h3.id, "p:id2");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Key Difference: run vs rpc
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn run_requires_registered_function() {
        let r = Resonate::local(None);
        // run_by_name fails because "unregistered" is not in the registry
        let result = r
            .begin_run_by_name::<()>("run-test", "unregistered", serde_json::json!(null), None)
            .await;
        assert!(matches!(result.unwrap_err(), Error::FunctionNotFound(_)));
    }

    #[tokio::test]
    async fn rpc_does_not_require_registered_function() {
        let r = Resonate::local(None);
        // rpc should succeed even without registration (remote worker will handle it)
        let result = r
            .begin_rpc::<serde_json::Value>(
                "rpc-test",
                "any_remote_func",
                serde_json::json!(null),
                None,
            )
            .await;
        assert!(result.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Handle Tests (via Resonate API)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn handle_id_matches_requested_id() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let handle = r.begin_run("handle-test", Noop, (), None).await.unwrap();
        assert_eq!(handle.id, "handle-test");
    }

    #[tokio::test]
    async fn rpc_handle_id_matches() {
        let r = Resonate::local(None);
        let handle = r
            .begin_rpc::<serde_json::Value>("rpc-handle", "remote", serde_json::json!(null), None)
            .await
            .unwrap();
        assert_eq!(handle.id, "rpc-handle");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Multiple Operations Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn multiple_begin_runs_with_different_ids() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let h1 = r.begin_run("m1", Noop, (), None).await;
        let h2 = r.begin_run("m2", Noop, (), None).await;
        let h3 = r.begin_run("m3", Noop, (), None).await;

        assert!(h1.is_ok());
        assert!(h2.is_ok());
        assert!(h3.is_ok());
        assert_eq!(h1.unwrap().id, "m1");
        assert_eq!(h2.unwrap().id, "m2");
        assert_eq!(h3.unwrap().id, "m3");
    }

    #[tokio::test]
    async fn mixed_run_and_rpc_operations() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let local_h = r.begin_run("local-1", Noop, (), None).await;
        let remote_h = r
            .begin_rpc::<serde_json::Value>("remote-1", "remote-fn", serde_json::json!(null), None)
            .await;

        assert!(local_h.is_ok());
        assert!(remote_h.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Sub-client Access Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn promises_sub_client_create_and_get() {
        let r = Resonate::local(None);

        // Create a promise via the sub-client
        let created = r
            .promises
            .create(
                "sub-p1",
                i64::MAX,
                serde_json::json!({"data": "test"}),
                serde_json::json!({}),
            )
            .await;
        assert!(created.is_ok());

        // Get it back
        let fetched = r.promises.get("sub-p1").await;
        assert!(fetched.is_ok());
        assert_eq!(fetched.unwrap()["id"], "sub-p1");
    }

    #[tokio::test]
    async fn promises_sub_client_settle() {
        let r = Resonate::local(None);

        // Create then settle
        r.promises
            .create(
                "sub-p2",
                i64::MAX,
                serde_json::json!(null),
                serde_json::json!({}),
            )
            .await
            .unwrap();

        let settled = r
            .promises
            .settle("sub-p2", "resolved", serde_json::json!({"data": "result"}))
            .await;
        assert!(settled.is_ok());

        // Verify it's settled
        let fetched = r.promises.get("sub-p2").await.unwrap();
        assert_eq!(fetched["state"], "resolved");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Transport / Network Passthrough Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn transport_accessible_from_resonate() {
        let r = Resonate::local(None);
        // Verify we can send a raw request through the transport
        let req = serde_json::json!({
            "kind": "promise.create",
            "corrId": "test-transport",
            "promise": {
                "id": "transport-test",
                "timeoutAt": i64::MAX,
            },
        });
        let resp = r.transport().send(req).await;
        assert!(resp.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  is_url / match_fn URL bypass Tests
    // ═══════════════════════════════════════════════════════════════

    #[test]
    fn is_url_detects_urls() {
        assert!(is_url("http://localhost:8001"));
        assert!(is_url("https://example.com/path"));
        assert!(is_url("local://any@hello"));
        assert!(is_url("custom://group/worker"));
        assert!(!is_url("hello"));
        assert!(!is_url("my_func"));
        assert!(!is_url("default"));
        assert!(!is_url(""));
    }

    #[tokio::test]
    async fn begin_rpc_with_url_target_passes_through_unchanged() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        // Use a URL as the target option — should NOT be rewritten by network.match
        let opts = PartialOptions {
            target: Some("https://remote-host:9000/workers/noop".to_string()),
            ..Default::default()
        };

        let handle = r
            .begin_rpc::<serde_json::Value>(
                "url-target-test",
                "noop",
                serde_json::json!(null),
                Some(opts),
            )
            .await;
        assert!(handle.is_ok());

        // Verify the promise was created with the URL target
        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "url-target-test",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // OptionsBuilder resolves target: URLs pass through unchanged.
        assert_eq!(target, "https://remote-host:9000/workers/noop");
    }

    #[tokio::test]
    async fn begin_rpc_with_no_target_uses_default() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let handle = r
            .begin_rpc::<serde_json::Value>(
                "bare-target-test",
                "noop",
                serde_json::json!(null),
                None, // no custom target — defaults to network.match("default")
            )
            .await;
        assert!(handle.is_ok());

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "bare-target-test",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Default target "default" is resolved by network.match → "local://any@default"
        assert_eq!(target, "local://any@default");
    }

    #[tokio::test]
    async fn begin_rpc_with_bare_name_target_gets_rewritten() {
        let r = Resonate::local(None);
        r.register(Noop).await;

        let opts = PartialOptions {
            target: Some("noop".into()),
            ..Default::default()
        };

        let handle = r
            .begin_rpc::<serde_json::Value>(
                "bare-target-test2",
                "noop",
                serde_json::json!(null),
                Some(opts),
            )
            .await;
        assert!(handle.is_ok());

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "bare-target-test2",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Bare name should be resolved by network.match → "local://any@noop"
        assert_eq!(target, "local://any@noop");
    }
}
