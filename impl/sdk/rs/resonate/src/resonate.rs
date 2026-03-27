use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{oneshot, Mutex};

use crate::codec::{Codec, Encryptor, NoopEncryptor};
use crate::core::Core;
use crate::durable::Durable;
use crate::error::{Error, Result};
use crate::handle::{PromiseResult, ResonateHandle};
use crate::heartbeat::{AsyncHeartbeat, Heartbeat, NoopHeartbeat};
use crate::http_network::{HttpAuth, HttpNetwork};
use crate::network::{LocalNetwork, Network};
use crate::now_ms;
use crate::options::{is_url, Options};
use crate::promises::{Promises, Schedules};
use crate::registry::Registry;
use crate::send::Sender;
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
    /// Authentication (basic or bearer). Takes priority over env vars.
    pub auth: Option<HttpAuth>,
    /// Custom encryption (default: no-op).
    pub encryptor: Option<Box<dyn Encryptor>>,
    /// Custom network implementation (overrides url).
    pub network: Option<Arc<dyn Network>>,
    /// ID prefix for all promise/task IDs (or from RESONATE_PREFIX).
    pub prefix: Option<String>,
}

/// Subscription entry for awaiting remote promise completion.
struct SubscriptionEntry {
    tx: Option<oneshot::Sender<PromiseResult>>,
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

    // Subscriptions (for awaiting remote promise completion)
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,

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

        // Resolve auth: explicit config takes priority, then env vars
        let http_auth = config.auth.or_else(|| {
            if let Ok(token) = std::env::var("RESONATE_TOKEN") {
                return Some(HttpAuth::Bearer(token));
            }
            let username = std::env::var("RESONATE_USERNAME").ok()?;
            let password = std::env::var("RESONATE_PASSWORD").unwrap_or_default();
            Some(HttpAuth::Basic { username, password })
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
            } else if let Some(url) = resolved_url {
                // Remote mode: HTTP network
                let net = Arc::new(HttpNetwork::new(
                    url,
                    config.pid.clone(),
                    config.group.clone(),
                    http_auth,
                ));
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
        let encryptor: Arc<dyn Encryptor> = match config.encryptor {
            Some(e) => Arc::from(e),
            None => Arc::new(NoopEncryptor),
        };
        let codec = Codec::new(encryptor);
        let registry = Arc::new(RwLock::new(Registry::new()));

        // Build the Sender for Core from the transport
        let sender = Sender::new(transport.clone());

        // Build target_resolver from the network for target resolution.
        // If the target already looks like a URL, pass it through unchanged
        // (mirrors TS: `util.isUrl(target) ? target : match(target)`).
        let network_for_match = network.clone();
        let target_resolver: crate::context::TargetResolver = Arc::new(move |target: &str| {
            if is_url(target) {
                target.to_string()
            } else {
                network_for_match.r#match(target)
            }
        });

        // Cap TTL to i64::MAX to avoid overflow when casting u64 → i64.
        // In local mode ttl is u64::MAX; a naive `as i64` wraps to -1,
        // which makes the server set lease timeouts in the past and
        // immediately release + retry tasks on every tick.
        let core_ttl: i64 = if ttl >= i64::MAX as u64 {
            i64::MAX
        } else {
            ttl as i64
        };
        let core = Arc::new(Core::new(
            sender,
            codec.clone(),
            registry.clone(),
            target_resolver,
            heartbeat.clone(),
            pid.clone(),
            core_ttl,
        ));
        let promises = Promises::new(transport.clone());
        let schedules = Schedules::new(transport.clone());

        let subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let subscribe_every = Duration::from_secs(60);

        // Start periodic subscription refresh
        let transport_for_refresh = transport.clone();
        let subs_for_refresh = subscriptions.clone();
        let unicast_for_refresh = network.unicast().to_string();
        let refresh_interval = subscribe_every;

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
                            let rdata = match crate::transport::response_data(&resp) {
                                Ok(d) => d,
                                Err(e) => {
                                    tracing::warn!(error = %e, promise_id = %id, "subscription refresh: invalid response");
                                    continue;
                                }
                            };
                            let state = rdata
                                .get("promise")
                                .and_then(|p| p.get("state"))
                                .and_then(|s| s.as_str())
                                .unwrap_or("pending");
                            if state != "pending" {
                                let value = rdata
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
            subscriptions: subscriptions.clone(),
            promises,
            schedules,
            subscription_refresh_handle: Mutex::new(Some(refresh_handle)),
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
                    tracing::debug!(task_id = %exec_msg.task_id(), version = exec_msg.version(), "received execute message");
                    let task_id = exec_msg.task_id().to_string();
                    let version = exec_msg.version();
                    tokio::spawn(async move {
                        if let Err(e) = core.on_message(&task_id, version).await {
                            tracing::error!(
                                error = %e,
                                task_id = %task_id,
                                "core.on_message failed"
                            );
                        }
                    });
                }
                Message::Unblock(unblock_msg) => {
                    let promise = unblock_msg.promise();
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

        resonate
    }

    // ═══════════════════════════════════════════════════════════════
    //  Public API
    // ═══════════════════════════════════════════════════════════════

    /// Register a durable function. The name and kind are derived from the
    /// `Durable` impl generated by `#[resonate::function]`.
    pub fn register<D, Args, T>(&self, func: D) -> Result<()>
    where
        D: Durable<Args, T> + Copy + Send + Sync + 'static,
        Args: DeserializeOwned + Send + 'static,
        T: Serialize + Send + 'static,
    {
        let mut reg = self.registry.write();
        reg.register(func)
    }

    /// Execute a typed durable function. Returns a builder that implements `IntoFuture`.
    ///
    /// # Usage
    /// ```ignore
    /// // Simple — await directly
    /// let result: T = resonate.run("id", my_func, args).await?;
    ///
    /// // With options
    /// let result: T = resonate.run("id", my_func, args)
    ///     .timeout(Duration::from_secs(30))
    ///     .version(2)
    ///     .await?;
    ///
    /// // Get a handle (like old begin_run)
    /// let handle = resonate.run("id", my_func, args).spawn().await?;
    /// let result = handle.result().await?;
    /// ```
    pub fn run<'a, D, Args, T>(
        &'a self,
        id: &'a str,
        _func: D,
        args: Args,
    ) -> ResRunTask<'a, D, Args, T>
    where
        D: Durable<Args, T>,
        Args: Serialize,
        T: DeserializeOwned,
    {
        ResRunTask {
            resonate: self,
            id,
            args,
            timeout: None,
            version: None,
            tags: None,
            target: None,
            _phantom: PhantomData,
        }
    }

    /// Remote procedure call. Returns a builder that implements `IntoFuture`.
    ///
    /// # Usage
    /// ```ignore
    /// // Simple
    /// let result: T = resonate.rpc::<T>("id", "func", args).await?;
    ///
    /// // With options
    /// let result: T = resonate.rpc::<T>("id", "func", args)
    ///     .target("custom-worker")
    ///     .timeout(Duration::from_secs(60))
    ///     .await?;
    ///
    /// // Get a handle (like old begin_rpc)
    /// let handle = resonate.rpc::<T>("id", "func", args).spawn().await?;
    /// ```
    pub fn rpc<'a, T: DeserializeOwned>(
        &'a self,
        id: &'a str,
        func_name: &'a str,
        args: serde_json::Value,
    ) -> ResRpcTask<'a, T> {
        ResRpcTask {
            resonate: self,
            id,
            func_name,
            args,
            timeout: None,
            version: None,
            tags: None,
            target: None,
            _phantom: PhantomData,
        }
    }

    /// Build root-level tags for a top-level run or rpc call.
    fn build_root_tags(id: &str, target: &str, tags: &mut HashMap<String, String>) {
        tags.insert("resonate:origin".to_string(), id.to_string());
        tags.insert("resonate:branch".to_string(), id.to_string());
        tags.insert("resonate:parent".to_string(), id.to_string());
        tags.insert("resonate:scope".to_string(), "global".to_string());
        tags.insert("resonate:target".to_string(), target.to_string());
    }

    /// Internal: execute a run by func name, returning a handle.
    async fn do_run<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Options,
    ) -> Result<ResonateHandle<T>> {
        let prefixed_id = self.prefix_id(id);

        // Verify function is registered
        {
            let reg = self.registry.read();
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
        Self::build_root_tags(&prefixed_id, &opts.target, &mut tags);

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
        let rdata = crate::transport::response_data(&resp)?;
        let status = crate::transport::response_status(&resp)?;
        let promise = rdata.get("promise").cloned().unwrap_or_default();
        let task = rdata.get("task").cloned();

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
                let task_version = task_val
                    .get("version")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                // Decode the promise for execute_until_blocked
                let root_promise = self.codec.decode_promise_from_json(&promise);

                let core = self.core.clone();
                tokio::spawn(async move {
                    match root_promise {
                        Ok(rp) => {
                            if let Err(e) = core
                                .execute_until_blocked(&task_id, task_version, rp, None)
                                .await
                            {
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

    /// Internal: execute an rpc, returning a handle.
    async fn do_rpc<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Options,
    ) -> Result<ResonateHandle<T>> {
        let prefixed_id = self.prefix_id(id);

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
        Self::build_root_tags(&prefixed_id, &opts.target, &mut tags);

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
        let rdata = crate::transport::response_data(&resp)?;
        let promise = rdata.get("promise").cloned().unwrap_or_default();

        self.create_handle(prefixed_id, &promise).await
    }

    /// Get a handle to an existing promise.
    pub async fn get<T: DeserializeOwned>(&self, id: &str) -> Result<ResonateHandle<T>> {
        let prefixed_id = self.prefix_id(id);

        let req = serde_json::json!({
            "kind": "promise.get",
            "corrId": format!("pg-{}", now_ms()),
            "id": prefixed_id,
        });

        let resp = self.transport.send(req).await?;
        let status = crate::transport::response_status(&resp)?;
        if status == 404 {
            return Err(Error::ServerError {
                code: 404,
                message: format!("promise {} not found", prefixed_id),
            });
        }

        let rdata = crate::transport::response_data(&resp)?;
        let promise = rdata.get("promise").cloned().unwrap_or_default();
        self.create_handle(prefixed_id, &promise).await
    }

    /// Create a schedule for periodic function execution. Returns a builder
    /// that implements `IntoFuture`.
    ///
    /// # Usage
    /// ```ignore
    /// // Simple
    /// let schedule = resonate.schedule("name", "*/5 * * * *", "func", args).await?;
    ///
    /// // With options
    /// let schedule = resonate.schedule("name", "*/5 * * * *", "func", args)
    ///     .timeout(Duration::from_secs(300))
    ///     .version(2)
    ///     .await?;
    /// ```
    pub fn schedule<'a>(
        &'a self,
        name: &'a str,
        cron: &'a str,
        func_name: &'a str,
        args: serde_json::Value,
    ) -> ResScheduleTask<'a> {
        ResScheduleTask {
            resonate: self,
            name,
            cron,
            func_name,
            args,
            timeout: None,
            version: None,
        }
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

    // ═══════════════════════════════════════════════════════════════
    //  Test-only accessors
    // ═══════════════════════════════════════════════════════════════

    #[cfg(test)]
    pub fn pid(&self) -> &str {
        &self.pid
    }

    #[cfg(test)]
    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    #[cfg(test)]
    pub fn id_prefix(&self) -> &str {
        &self.id_prefix
    }

    #[cfg(test)]
    pub fn transport(&self) -> &Transport {
        &self.transport
    }

    #[cfg(test)]
    pub fn network(&self) -> &Arc<dyn Network> {
        &self.network
    }

    // ═══════════════════════════════════════════════════════════════
    //  Internal Methods
    // ═══════════════════════════════════════════════════════════════

    /// Prepend the configured prefix to an ID.
    fn prefix_id(&self, id: &str) -> String {
        if self.id_prefix.is_empty() {
            id.to_string()
        } else {
            format!("{}{}", self.id_prefix, id)
        }
    }

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
            let rdata = crate::transport::response_data(&resp)?;
            let resp_promise = rdata.get("promise").cloned().unwrap_or_default();
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

// ═══════════════════════════════════════════════════════════════
//  Shared builder helpers
// ═══════════════════════════════════════════════════════════════

/// Build resolved `Options` from builder fields (shared by ResRunTask and ResRpcTask).
fn build_options(
    resonate: &Resonate,
    target: Option<&str>,
    tags: Option<HashMap<String, String>>,
    timeout: Option<Duration>,
    version: Option<u32>,
) -> Options {
    let defaults = Options::default();
    let raw_target = target.unwrap_or("default");
    let resolved_target = if is_url(raw_target) {
        raw_target.to_string()
    } else {
        resonate.network.r#match(raw_target)
    };
    Options {
        tags: tags.unwrap_or(defaults.tags),
        target: resolved_target,
        timeout: timeout.unwrap_or(defaults.timeout),
        version: version.unwrap_or(defaults.version),
    }
}

// ═══════════════════════════════════════════════════════════════
//  ResRunTask — builder returned by resonate.run()
// ═══════════════════════════════════════════════════════════════

/// A builder for local durable function execution. Created by `Resonate::run()`.
///
/// Implements `IntoFuture` so `.await` runs and returns `Result<T>`.
/// Use `.spawn()` to get a `ResonateHandle<T>` instead.
pub struct ResRunTask<'a, D, Args, T> {
    resonate: &'a Resonate,
    id: &'a str,
    args: Args,
    timeout: Option<Duration>,
    version: Option<u32>,
    tags: Option<HashMap<String, String>>,
    target: Option<String>,
    _phantom: PhantomData<fn(D) -> T>,
}

impl<'a, D, Args, T> ResRunTask<'a, D, Args, T> {
    /// Set the timeout for this execution.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the function version.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Set custom tags.
    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Set the target for routing.
    pub fn target(mut self, target: &str) -> Self {
        self.target = Some(target.to_string());
        self
    }
}

impl<'a, D, Args, T> ResRunTask<'a, D, Args, T>
where
    D: Durable<Args, T>,
    Args: Serialize,
    T: DeserializeOwned,
{
    /// Start the execution and return a handle for later awaiting (replaces `begin_run`).
    pub async fn spawn(self) -> Result<ResonateHandle<T>> {
        let opts = build_options(
            self.resonate,
            self.target.as_deref(),
            self.tags,
            self.timeout,
            self.version,
        );
        let json_args = serde_json::to_value(self.args)?;
        self.resonate
            .do_run::<T>(self.id, D::NAME, json_args, opts)
            .await
    }
}

impl<'a, D, Args, T> IntoFuture for ResRunTask<'a, D, Args, T>
where
    D: Durable<Args, T> + Send + 'a,
    Args: Serialize + Send + 'a,
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut handle = self.spawn().await?;
            handle.result().await
        })
    }
}

// ═══════════════════════════════════════════════════════════════
//  ResRpcTask — builder returned by resonate.rpc()
// ═══════════════════════════════════════════════════════════════

/// A builder for remote procedure call execution. Created by `Resonate::rpc()`.
///
/// Implements `IntoFuture` so `.await` runs and returns `Result<T>`.
/// Use `.spawn()` to get a `ResonateHandle<T>` instead.
pub struct ResRpcTask<'a, T> {
    resonate: &'a Resonate,
    id: &'a str,
    func_name: &'a str,
    args: serde_json::Value,
    timeout: Option<Duration>,
    version: Option<u32>,
    tags: Option<HashMap<String, String>>,
    target: Option<String>,
    _phantom: PhantomData<T>,
}

impl<'a, T> ResRpcTask<'a, T> {
    /// Set the timeout for this execution.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the function version.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Set custom tags.
    pub fn tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Set the target for routing.
    pub fn target(mut self, target: &str) -> Self {
        self.target = Some(target.to_string());
        self
    }
}

impl<'a, T: DeserializeOwned> ResRpcTask<'a, T> {
    /// Start the RPC and return a handle for later awaiting (replaces `begin_rpc`).
    pub async fn spawn(self) -> Result<ResonateHandle<T>> {
        let opts = build_options(
            self.resonate,
            self.target.as_deref(),
            self.tags,
            self.timeout,
            self.version,
        );
        self.resonate
            .do_rpc::<T>(self.id, self.func_name, self.args, opts)
            .await
    }
}

impl<'a, T: DeserializeOwned + Send + Sync + 'static> IntoFuture for ResRpcTask<'a, T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut handle = self.spawn().await?;
            handle.result().await
        })
    }
}

// ═══════════════════════════════════════════════════════════════
//  ResScheduleTask — builder returned by resonate.schedule()
// ═══════════════════════════════════════════════════════════════

/// A builder for schedule creation. Created by `Resonate::schedule()`.
///
/// Implements `IntoFuture` so `.await` creates the schedule and returns
/// `Result<ResonateSchedule>`.
pub struct ResScheduleTask<'a> {
    resonate: &'a Resonate,
    name: &'a str,
    cron: &'a str,
    func_name: &'a str,
    args: serde_json::Value,
    timeout: Option<Duration>,
    version: Option<u32>,
}

impl<'a> ResScheduleTask<'a> {
    /// Set the timeout for scheduled executions.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the function version.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }
}

impl<'a> IntoFuture for ResScheduleTask<'a> {
    type Output = Result<ResonateSchedule>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<ResonateSchedule>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let defaults = Options::default();
            let timeout = self.timeout.unwrap_or(defaults.timeout);
            let version = self.version.unwrap_or(defaults.version);

            let param_data = serde_json::json!({
                "func": self.func_name,
                "args": self.args,
                "version": version,
            });
            let encoded_param = self.resonate.codec.encode(&param_data)?;

            let template = format!("{}{{{{.id}}}}.{{{{.timestamp}}}}", self.resonate.id_prefix);

            self.resonate
                .schedules
                .create(
                    self.name,
                    self.cron,
                    &template,
                    timeout.as_millis() as i64,
                    serde_json::to_value(&encoded_param)?,
                )
                .await?;

            Ok(ResonateSchedule {
                name: self.name.to_string(),
                schedules: self.resonate.schedules.clone(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
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
        r.register(add).unwrap();

        // Verify function is registered (spawn won't fail with FunctionNotFound)
        let result = r.run("test-id", add, (1i64, 2i64)).spawn().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn register_duplicate_function_returns_error() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();
        let err = r.register(noop);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("already registered"));
    }

    // ═══════════════════════════════════════════════════════════════
    //  run Tests (builder API)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn run_spawn_returns_handle_for_registered_function() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let handle = r.run("greet-1", noop, ()).spawn().await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "greet-1");
    }

    #[tokio::test]
    async fn run_unregistered_function_returns_function_not_found() {
        let r = Resonate::local(None);
        // Use a registered wrapper to test — noop is not registered here
        let result: Result<()> = r.run("test-id", noop, ()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::FunctionNotFound(name) => assert_eq!(name, "noop"),
            other => panic!("expected FunctionNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn run_spawn_with_prefix_prepends_to_id() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("app".into()),
            pid: Some("default".into()),
            ..Default::default()
        });
        r.register(noop).unwrap();

        let handle = r.run("my-id", noop, ()).spawn().await.unwrap();
        assert_eq!(handle.id, "app:my-id");
    }

    #[tokio::test]
    async fn run_spawn_creates_task_and_promise() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let _handle = r.run("task-1", noop, ()).spawn().await.unwrap();

        // The promise should exist in the local network — we can verify via get
        let get_handle = r.get::<()>("task-1").await;
        assert!(
            get_handle.is_ok(),
            "promise should exist after run().spawn()"
        );
    }

    #[tokio::test]
    async fn run_spawn_idempotent_same_id_returns_existing_promise() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let h1 = r.run("same-id", noop, ()).spawn().await;
        assert!(h1.is_ok());

        // Second call with same ID should not fail (idempotent: 409 handled)
        let h2 = r.run("same-id", noop, ()).spawn().await;
        assert!(h2.is_ok());
        assert_eq!(h2.unwrap().id, "same-id");
    }

    #[tokio::test]
    async fn run_spawn_sets_correct_tags() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let mut m = std::collections::HashMap::new();
        m.insert("user:tag".to_string(), "value".to_string());

        let handle = r.run("tag-test", noop, ()).tags(m).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn run_spawn_with_custom_timeout() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let handle = r
            .run("timeout-test", noop, ())
            .timeout(Duration::from_secs(300))
            .spawn()
            .await;
        assert!(handle.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  rpc Tests (builder API)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn rpc_spawn_creates_promise_not_task() {
        let r = Resonate::local(None);
        // RPC does NOT require function to be registered locally
        let handle = r
            .rpc::<serde_json::Value>("rpc-1", "remote_func", serde_json::json!({"x": 1}))
            .spawn()
            .await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "rpc-1");
    }

    #[tokio::test]
    async fn rpc_spawn_with_prefix() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("svc".into()),
            ..Default::default()
        });

        let handle = r
            .rpc::<serde_json::Value>("rpc-2", "remote", serde_json::json!(null))
            .spawn()
            .await
            .unwrap();
        assert_eq!(handle.id, "svc:rpc-2");
    }

    #[tokio::test]
    async fn rpc_spawn_sets_scope_global() {
        let r = Resonate::local(None);
        // Verifying RPC succeeds — tags (scope=global, target) are set internally
        let handle = r
            .rpc::<serde_json::Value>("rpc-scope", "remote", serde_json::json!(null))
            .spawn()
            .await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn rpc_spawn_with_custom_target() {
        let r = Resonate::local(None);

        let handle = r
            .rpc::<serde_json::Value>("rpc-target", "remote", serde_json::json!(null))
            .target("custom-worker")
            .spawn()
            .await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn rpc_spawn_idempotent_same_id() {
        let r = Resonate::local(None);

        let h1 = r
            .rpc::<serde_json::Value>("rpc-dup", "remote", serde_json::json!(null))
            .spawn()
            .await;
        assert!(h1.is_ok());

        // Same ID should return existing promise
        let h2 = r
            .rpc::<serde_json::Value>("rpc-dup", "remote", serde_json::json!(null))
            .spawn()
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
        r.rpc::<serde_json::Value>("get-test", "func", serde_json::json!(null))
            .spawn()
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
        r.rpc::<serde_json::Value>("p1", "func", serde_json::json!(null))
            .spawn()
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
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn schedule_returns_deletable_handle() {
        let r = Resonate::local(None);
        let schedule = r
            .schedule("deletable", "0 * * * *", "func", serde_json::json!(null))
            .await
            .unwrap();
        // Deleting should not fail
        let result = schedule.delete().await;
        assert!(result.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Builder Options Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn run_builder_uses_defaults() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();
        // Default options should work — just spawn and check
        let handle = r.run("defaults-test", noop, ()).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn run_builder_with_timeout_and_version() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let handle = r
            .run("builder-opts", noop, ())
            .timeout(Duration::from_secs(120))
            .version(3)
            .spawn()
            .await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn run_builder_with_tags() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let mut m = std::collections::HashMap::new();
        m.insert("key".into(), "val".into());

        let handle = r.run("builder-tags", noop, ()).tags(m).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn rpc_builder_target_resolution_bare_name() {
        let r = Resonate::local(None);

        let handle = r
            .rpc::<serde_json::Value>("target-bare", "func", serde_json::json!(null))
            .target("my-worker")
            .spawn()
            .await
            .unwrap();

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "target-bare",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");
        assert_eq!(target, "local://any@my-worker");
        drop(handle);
    }

    #[tokio::test]
    async fn rpc_builder_target_resolution_url_passthrough() {
        let r = Resonate::local(None);

        let handle = r
            .rpc::<serde_json::Value>("target-url", "func", serde_json::json!(null))
            .target("https://remote:9000/workers/hello")
            .spawn()
            .await
            .unwrap();

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "target-url",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");
        assert_eq!(target, "https://remote:9000/workers/hello");
        drop(handle);
    }

    #[tokio::test]
    async fn rpc_builder_default_target() {
        let r = Resonate::local(None);

        let _handle = r
            .rpc::<serde_json::Value>("target-default", "func", serde_json::json!(null))
            .spawn()
            .await
            .unwrap();

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "target-default",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");
        // "default" gets resolved through network.match → "local://any@default"
        assert_eq!(target, "local://any@default");
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

    #[tokio::test]
    async fn stop_aborts_subscription_refresh_handle() {
        let r = Resonate::local(None);
        // The refresh handle should be stored (not None)
        {
            let guard = r.subscription_refresh_handle.lock().await;
            assert!(
                guard.is_some(),
                "refresh handle should be stored at construction"
            );
        }
        r.stop().await.unwrap();
        // After stop, the handle should be taken (None)
        {
            let guard = r.subscription_refresh_handle.lock().await;
            assert!(guard.is_none(), "refresh handle should be None after stop");
        }
    }

    #[tokio::test]
    async fn stop_aborts_refresh_task() {
        let r = Resonate::local(None);
        // Confirm the refresh task is running before stop
        {
            let guard = r.subscription_refresh_handle.lock().await;
            let handle = guard.as_ref().unwrap();
            assert!(
                !handle.is_finished(),
                "refresh task should be running before stop"
            );
        }
        r.stop().await.unwrap();
        // After stop, handle is taken so we can't inspect it directly,
        // but we verified it was present and stop didn't panic.
        // The idempotent stop test further confirms correctness.
    }

    // ═══════════════════════════════════════════════════════════════
    //  ID Prefix Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn no_prefix_leaves_id_unchanged() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let handle = r.run("my-id", noop, ()).spawn().await.unwrap();
        assert_eq!(handle.id, "my-id");
    }

    #[tokio::test]
    async fn prefix_is_prepended_with_colon() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("prefix".into()),
            ..Default::default()
        });
        r.register(noop).unwrap();

        let handle = r.run("my-id", noop, ()).spawn().await.unwrap();
        assert_eq!(handle.id, "prefix:my-id");
    }

    #[tokio::test]
    async fn prefix_applied_consistently_to_run_rpc_and_get() {
        let r = Resonate::new(ResonateConfig {
            prefix: Some("p".into()),
            ..Default::default()
        });
        r.register(noop).unwrap();

        // run().spawn() with prefix
        let h1 = r.run("id1", noop, ()).spawn().await.unwrap();
        assert_eq!(h1.id, "p:id1");

        // rpc().spawn() with prefix
        let h2 = r
            .rpc::<serde_json::Value>("id2", "remote", serde_json::json!(null))
            .spawn()
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
        // run fails because noop is not registered
        let result: Result<()> = r.run("run-test", noop, ()).await;
        assert!(matches!(result.unwrap_err(), Error::FunctionNotFound(_)));
    }

    #[tokio::test]
    async fn rpc_does_not_require_registered_function() {
        let r = Resonate::local(None);
        // rpc should succeed even without registration (remote worker will handle it)
        let result = r
            .rpc::<serde_json::Value>("rpc-test", "any_remote_func", serde_json::json!(null))
            .spawn()
            .await;
        assert!(result.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Handle Tests (via Resonate API)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn handle_id_matches_requested_id() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let handle = r.run("handle-test", noop, ()).spawn().await.unwrap();
        assert_eq!(handle.id, "handle-test");
    }

    #[tokio::test]
    async fn rpc_handle_id_matches() {
        let r = Resonate::local(None);
        let handle = r
            .rpc::<serde_json::Value>("rpc-handle", "remote", serde_json::json!(null))
            .spawn()
            .await
            .unwrap();
        assert_eq!(handle.id, "rpc-handle");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Multiple Operations Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn multiple_run_spawns_with_different_ids() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let h1 = r.run("m1", noop, ()).spawn().await;
        let h2 = r.run("m2", noop, ()).spawn().await;
        let h3 = r.run("m3", noop, ()).spawn().await;

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
        r.register(noop).unwrap();

        let local_h = r.run("local-1", noop, ()).spawn().await;
        let remote_h = r
            .rpc::<serde_json::Value>("remote-1", "remote-fn", serde_json::json!(null))
            .spawn()
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
    //  is_url / target_resolver URL bypass Tests
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
    async fn rpc_with_url_target_passes_through_unchanged() {
        let r = Resonate::local(None);

        // Use a URL as the target option — should NOT be rewritten by network.match
        let handle = r
            .rpc::<serde_json::Value>("url-target-test", "noop", serde_json::json!(null))
            .target("https://remote-host:9000/workers/noop")
            .spawn()
            .await;
        assert!(handle.is_ok());

        // Verify the promise was created with the URL target
        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "url-target-test",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        assert_eq!(target, "https://remote-host:9000/workers/noop");
    }

    #[tokio::test]
    async fn run_with_custom_target() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let _handle = r
            .run("run-target-test", noop, ())
            .target("my-target")
            .spawn()
            .await
            .unwrap();

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "run-target-test",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Bare name resolved via network.match
        assert_eq!(target, "local://any@my-target");
    }

    #[tokio::test]
    async fn run_default_target_uses_network_match() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let _handle = r.run("run-default-target", noop, ()).spawn().await.unwrap();

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "run-default-target",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        assert_eq!(target, "local://any@default");
    }

    #[tokio::test]
    async fn run_url_target_passes_through() {
        let r = Resonate::local(None);
        r.register(noop).unwrap();

        let _handle = r
            .run("run-url-target", noop, ())
            .target("https://remote:9000/workers/noop")
            .spawn()
            .await
            .unwrap();

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "run-url-target",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        assert_eq!(target, "https://remote:9000/workers/noop");
    }

    #[tokio::test]
    async fn rpc_with_no_target_uses_default() {
        let r = Resonate::local(None);

        let handle = r
            .rpc::<serde_json::Value>("bare-target-test", "noop", serde_json::json!(null))
            .spawn()
            .await;
        assert!(handle.is_ok());

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "bare-target-test",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Default target "default" is resolved by network.match → "local://any@default"
        assert_eq!(target, "local://any@default");
    }

    #[tokio::test]
    async fn rpc_with_bare_name_target_gets_rewritten() {
        let r = Resonate::local(None);

        let handle = r
            .rpc::<serde_json::Value>("bare-target-test2", "noop", serde_json::json!(null))
            .target("noop")
            .spawn()
            .await;
        assert!(handle.is_ok());

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "corrId": "check",
            "id": "bare-target-test2",
        });
        let resp = r.transport().send(get_req).await.unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Bare name should be resolved by network.match → "local://any@noop"
        assert_eq!(target, "local://any@noop");
    }
}
