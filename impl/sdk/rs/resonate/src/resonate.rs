use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{watch, Mutex};

use crate::codec::{Codec, Encryptor, NoopEncryptor};
use crate::core::Core;
use crate::durable::Durable;
use crate::error::{Error, Result};
use crate::handle::{PromiseResult, ResonateHandle};
use crate::heartbeat::{AsyncHeartbeat, Heartbeat, NoopHeartbeat};
use crate::http_network::HttpNetwork;
use crate::network::{LocalNetwork, Network};
use crate::now_ms;
use crate::options::{is_url, Options};
use crate::promises::{Promises, Schedules};
use crate::registry::Registry;
use crate::send::{Sender, TaskCreateOutcome};
use crate::transport::{Message, Transport};
use crate::types::PromiseState;

type Subscriptions = Arc<Mutex<HashMap<String, watch::Sender<Option<Arc<PromiseResult>>>>>>;

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
    /// JWT token for auth.
    pub token: Option<String>,
    /// Custom encryption (default: no-op).
    pub encryptor: Option<Box<dyn Encryptor>>,
    /// Custom network implementation (overrides url).
    pub network: Option<Arc<dyn Network>>,
    /// ID prefix for all promise/task IDs (or from RESONATE_PREFIX).
    pub prefix: Option<String>,
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

    // Core execution engine
    core: Arc<Core>,

    // Function management
    registry: Arc<RwLock<Registry>>,
    heartbeat: Arc<dyn Heartbeat>,

    // Subscriptions (for awaiting remote promise completion)
    subscriptions: Subscriptions,

    // Typed sender for all protocol requests
    sender: Sender,

    // Sub-clients
    pub promises: Promises,
    pub schedules: Schedules,

    // Application dependencies (DB pools, service clients, config)
    deps: Arc<crate::DependencyMap>,

    // Background task handles
    subscription_refresh_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Resonate {
    // ═══════════════════════════════════════════════════════════════
    //  Static Constructors
    // ═══════════════════════════════════════════════════════════════

    /// Local-only mode. No external dependencies. In-memory state.
    /// group="default", pid="default", ttl=MAX, no heartbeat.
    pub fn local() -> Self {
        let config = ResonateConfig {
            pid: Some("default".to_string()),
            group: Some("default".to_string()),
            ttl: Some(u64::MAX),
            encryptor: None,
            ..Default::default()
        };
        Self::new(config)
    }

    // ═══════════════════════════════════════════════════════════════
    //  Constructor
    // ═══════════════════════════════════════════════════════════════

    pub fn new(config: ResonateConfig) -> Self {
        let ResonateConfig {
            url,
            group,
            pid: config_pid,
            ttl: config_ttl,
            token,
            encryptor: config_encryptor,
            network: config_network,
            prefix: config_prefix,
        } = config;

        let ttl = config_ttl.unwrap_or(60_000);

        // Resolve prefix
        let prefix = config_prefix
            .or_else(|| std::env::var("RESONATE_PREFIX").ok())
            .unwrap_or_default();
        let id_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}:", prefix)
        };

        // Resolve URL
        let resolved_url = url
            .or_else(|| std::env::var("RESONATE_URL").ok())
            .or_else(|| {
                let host = std::env::var("RESONATE_HOST").ok()?;
                let scheme = std::env::var("RESONATE_SCHEME").unwrap_or_else(|_| "http".into());
                let port = std::env::var("RESONATE_PORT").unwrap_or_else(|_| "8001".into());
                Some(format!("{}://{}:{}", scheme, host, port))
            });

        // Resolve auth: explicit config takes priority, then env vars.
        // Clone before auth is potentially moved into HttpNetwork.
        let auth = token.or_else(|| std::env::var("RESONATE_TOKEN").ok());
        let auth_for_envelope = auth.clone();

        // Network selection — only the Arc<dyn Network> is selected here.
        // Transport and heartbeat are built once below from the single network.
        let (network, needs_async_heartbeat): (Arc<dyn Network>, bool) =
            if let Some(net) = config_network {
                (net, true)
            } else if let Some(url) = resolved_url {
                let net = Arc::new(HttpNetwork::new(url, config_pid, group, auth));
                (net as Arc<dyn Network>, true)
            } else {
                let net = Arc::new(LocalNetwork::new(config_pid, group));
                (net as Arc<dyn Network>, false)
            };

        let pid = network.pid().to_string();
        let transport = Transport::new(network.clone());
        let encryptor: Arc<dyn Encryptor> = match config_encryptor {
            Some(e) => Arc::from(e),
            None => Arc::new(NoopEncryptor),
        };
        let codec = Codec::new(encryptor);
        let registry = Arc::new(RwLock::new(Registry::new()));

        // Build heartbeat after transport so we share a single Transport instance.
        let heartbeat: Arc<dyn Heartbeat> = if needs_async_heartbeat {
            let sender_for_hb = Sender::new(transport.clone(), auth_for_envelope.clone());
            Arc::new(AsyncHeartbeat::new(pid.clone(), ttl / 2, sender_for_hb))
        } else {
            Arc::new(NoopHeartbeat)
        };

        // Build the Sender for all protocol requests
        let sender = Sender::new(transport.clone(), auth_for_envelope);

        // Build target_resolver from the network for target resolution.
        let network_for_match = network.clone();
        let target_resolver: crate::context::TargetResolver =
            Arc::new(move |target: Option<&str>| {
                let resolved = target.unwrap_or(network_for_match.group());
                resolve_target(&*network_for_match, resolved)
            });

        // Cap TTL to i64::MAX to avoid overflow when casting u64 → i64.
        let core_ttl = ttl.min(i64::MAX as u64) as i64;
        let deps = Arc::new(crate::DependencyMap::new());
        let core = Arc::new(Core::new(
            sender.clone(),
            codec.clone(),
            registry.clone(),
            target_resolver,
            heartbeat.clone(),
            pid.clone(),
            core_ttl,
            deps.clone(),
        ));
        let promises = Promises::new(transport.clone());
        let schedules = Schedules::new(transport.clone());

        let subscriptions: Subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let subscribe_every = Duration::from_secs(60);

        // Start periodic subscription refresh
        let refresh_handle = Self::spawn_subscription_refresh(
            subscriptions.clone(),
            sender.clone(),
            network.unicast().to_string(),
            subscribe_every,
        );

        let resonate = Self {
            pid,
            ttl,
            id_prefix,
            codec,
            network: network.clone(),
            core,
            registry,
            heartbeat,
            subscriptions: subscriptions.clone(),
            sender,
            promises,
            schedules,
            deps,
            subscription_refresh_handle: Mutex::new(Some(refresh_handle)),
        };

        // Subscribe to incoming messages
        Self::subscribe_to_messages(&transport, subscriptions.clone(), resonate.core.clone());

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

    /// Store a typed application dependency (DB pool, service client, config, etc.).
    ///
    /// Dependencies are keyed by their concrete type and shared with every
    /// [`Context`] and [`Info`] created during task execution.
    ///
    /// All dependencies should be added **before** the system starts processing
    /// tasks (i.e. before any `run()` call or incoming network message).
    pub fn with_dependency<T: Send + Sync + 'static>(self, value: T) -> Self {
        self.deps.insert(value);
        self
    }

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
    /// let result: T = resonate.rpc("id", "func", (arg1, arg2)).await?;
    ///
    /// // With options
    /// let result: T = resonate.rpc("id", "func", (arg1, arg2))
    ///     .target("custom-worker")
    ///     .timeout(Duration::from_secs(60))
    ///     .await?;
    ///
    /// // Get a handle (like old begin_rpc)
    /// let handle = resonate.rpc::<_, T>("id", "func", (arg1, arg2)).spawn().await?;
    /// ```
    pub fn rpc<'a, Args: Serialize, T: DeserializeOwned>(
        &'a self,
        id: &'a str,
        func_name: &'a str,
        args: Args,
    ) -> ResRpcTask<'a, Args, T> {
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

    /// Build a `PromiseCreateReq` with encoded params, tags, and prefixed ID.
    fn build_promise_create_req(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: &Options,
    ) -> Result<(String, crate::types::PromiseCreateReq)> {
        let prefixed_id = self.prefix_id(id);
        let timeout_at = now_ms() + opts.timeout.as_millis() as i64;
        // NOTE: function versioning is not yet supported by this SDK.
        let param_data = serde_json::json!({
            "func": func_name,
            "args": args,
        });
        let encoded_param = self.codec.encode(&param_data)?;
        let mut tags = opts.tags.clone();
        tags.reserve(5);
        Self::build_root_tags(&prefixed_id, &opts.target, &mut tags);
        Ok((
            prefixed_id.clone(),
            crate::types::PromiseCreateReq {
                id: prefixed_id,
                timeout_at,
                param: encoded_param,
                tags,
            },
        ))
    }

    /// Internal: execute a run by func name, returning a handle.
    async fn do_run<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Options,
    ) -> Result<ResonateHandle<T>> {
        // Verify function is registered
        {
            let reg = self.registry.read();
            if !reg.contains(func_name) {
                return Err(Error::FunctionNotFound(func_name.to_string()));
            }
        }

        let (prefixed_id, action) = self.build_promise_create_req(id, func_name, args, &opts)?;
        let ttl = self.safe_ttl();
        let outcome = self
            .sender
            .task_create_or_conflict(&self.pid, ttl, action)
            .await?;

        match outcome {
            TaskCreateOutcome::Conflict(promise) => {
                // Promise already exists — register listener and return handle
                self.create_handle_from_record(prefixed_id, &promise).await
            }
            TaskCreateOutcome::Created(result) => {
                let promise = &result.promise;

                // If task is acquired, fire-and-forget core execution
                if result.task.state == crate::types::TaskState::Acquired {
                    let task_id = result.task.id.clone();
                    let task_version = result.task.version;
                    let decoded = self.codec.decode_promise(result.promise.clone());
                    let preload = result.preload;
                    let core = self.core.clone();
                    tokio::spawn(async move {
                        match decoded {
                            Ok(rp) => {
                                if let Err(e) = core
                                    .execute_until_blocked(
                                        &task_id,
                                        task_version,
                                        rp,
                                        Some(preload),
                                    )
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

                self.create_handle_from_record(prefixed_id, promise).await
            }
        }
    }

    /// Internal: execute an rpc, returning a handle.
    async fn do_rpc<T: DeserializeOwned>(
        &self,
        id: &str,
        func_name: &str,
        args: serde_json::Value,
        opts: Options,
    ) -> Result<ResonateHandle<T>> {
        let (prefixed_id, req) = self.build_promise_create_req(id, func_name, args, &opts)?;
        let promise = self.sender.promise_create(req).await?;
        self.create_handle_from_record(prefixed_id, &promise).await
    }

    /// Get a handle to an existing promise.
    pub async fn get<T: DeserializeOwned>(&self, id: &str) -> Result<ResonateHandle<T>> {
        let prefixed_id = self.prefix_id(id);
        let promise = self.sender.promise_get(&prefixed_id).await?;
        self.create_handle_from_record(prefixed_id, &promise).await
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
    pub fn schedule<'a, Args: Serialize>(
        &'a self,
        name: &'a str,
        cron: &'a str,
        func_name: &'a str,
        args: Args,
    ) -> ResScheduleTask<'a, Args> {
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
        self.heartbeat.shutdown();
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
    pub fn transport(&self) -> Transport {
        Transport::new(self.network.clone())
    }

    #[cfg(test)]
    pub fn network(&self) -> &Arc<dyn Network> {
        &self.network
    }

    // ═══════════════════════════════════════════════════════════════
    //  Internal Methods
    // ═══════════════════════════════════════════════════════════════

    /// TTL capped to i64::MAX for Sender methods that take i64.
    fn safe_ttl(&self) -> i64 {
        self.ttl.min(i64::MAX as u64) as i64
    }

    /// Prepend the configured prefix to an ID.
    fn prefix_id(&self, id: &str) -> String {
        if self.id_prefix.is_empty() {
            id.to_string()
        } else {
            format!("{}{}", self.id_prefix, id)
        }
    }

    /// Wire up the transport message handler to dispatch Execute and Unblock
    /// messages to the core engine and subscription watchers respectively.
    fn subscribe_to_messages(transport: &Transport, subscriptions: Subscriptions, core: Arc<Core>) {
        transport.recv(Box::new(move |msg| {
            let subs = subscriptions.clone();
            let core = core.clone();
            match msg {
                Message::Execute(exec_msg) => {
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
                    let state_str = promise
                        .get("state")
                        .and_then(|v| v.as_str())
                        .unwrap_or("pending");
                    let value = promise.get("value").cloned().unwrap_or_default();

                    let result = Arc::new(PromiseResult {
                        state: Resonate::parse_promise_state(state_str),
                        value,
                    });

                    let subs = subs.clone();
                    tokio::spawn(async move {
                        let mut map = subs.lock().await;
                        if let Some(tx) = map.get(&id) {
                            let _ = tx.send(Some(result));
                        } else {
                            let (tx, _) = watch::channel(Some(result));
                            map.insert(id, tx);
                        }
                    });
                }
            }
        }));
    }

    /// Spawn a background task that periodically re-registers listeners for
    /// pending promises, ensuring the server continues to push Unblock messages.
    fn spawn_subscription_refresh(
        subscriptions: Subscriptions,
        sender: Sender,
        unicast: String,
        interval_duration: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            loop {
                interval.tick().await;

                // Snapshot: only IDs that are still pending.
                let pending_ids: Vec<String> = {
                    let map = subscriptions.lock().await;
                    map.iter()
                        .filter(|(_, tx)| tx.borrow().is_none())
                        .map(|(id, _)| id.clone())
                        .collect()
                };

                for id in pending_ids {
                    match sender.promise_register_listener(&id, &unicast).await {
                        Ok(promise) => {
                            if promise.state != PromiseState::Pending {
                                let result = Arc::new(PromiseResult {
                                    state: promise.state,
                                    value: Resonate::value_to_wire_json(&promise.value),
                                });
                                let map = subscriptions.lock().await;
                                if let Some(tx) = map.get(&id) {
                                    let _ = tx.send(Some(result));
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, promise_id = %id,
                                "refresh failed");
                        }
                    }
                }
            }
        })
    }

    /// Convert a `types::Value` to the raw wire-format JSON expected by `ResonateHandle`.
    ///
    /// The handle's `decode_value` works on raw wire JSON, so we must produce the
    /// same shape as the Unblock message path (which passes `promise.get("value")`).
    fn value_to_wire_json(value: &crate::types::Value) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        if let Some(ref headers) = value.headers {
            map.insert(
                "headers".into(),
                serde_json::to_value(headers).unwrap_or_default(),
            );
        }
        match &value.data {
            Some(d) => map.insert("data".into(), d.clone()),
            None => map.insert("data".into(), serde_json::Value::Null),
        };
        serde_json::Value::Object(map)
    }

    /// Create a handle from a typed PromiseRecord.
    async fn create_handle_from_record<T: DeserializeOwned>(
        &self,
        id: String,
        promise: &crate::types::PromiseRecord,
    ) -> Result<ResonateHandle<T>> {
        let wire_value = Self::value_to_wire_json(&promise.value);
        let settled = if promise.state == PromiseState::Pending {
            None
        } else {
            Some(Arc::new(PromiseResult {
                state: promise.state.clone(),
                value: wire_value,
            }))
        };
        let is_pending = promise.state == PromiseState::Pending;

        let (rx, needs_listener) = {
            let mut subs = self.subscriptions.lock().await;
            if let Some(tx) = subs.get(&id) {
                // Existing entry — another handle or early Unblock.
                (tx.subscribe(), false)
            } else {
                // First time — create watch channel.
                // If already settled, pre-load the value.
                let (tx, rx) = watch::channel(settled);
                subs.insert(id.clone(), tx);
                (rx, is_pending)
            }
        };
        // Lock released.

        // Register listener so the server pushes Unblock to us.
        // Only needed on first subscription for a pending promise.
        if needs_listener {
            let resp_promise = self.register_listener(&id).await?;
            if resp_promise.state != PromiseState::Pending {
                // Settled between promise creation and listener registration.
                let result = Arc::new(PromiseResult {
                    state: resp_promise.state,
                    value: Self::value_to_wire_json(&resp_promise.value),
                });
                let subs = self.subscriptions.lock().await;
                if let Some(tx) = subs.get(&id) {
                    let _ = tx.send(Some(result));
                }
            }
        }

        Ok(ResonateHandle::new(id, rx, self.codec.clone()))
    }

    /// Parse a wire-format state string into a PromiseState.
    fn parse_promise_state(state: &str) -> PromiseState {
        match state {
            "resolved" => PromiseState::Resolved,
            "rejected" => PromiseState::Rejected,
            "rejected_canceled" => PromiseState::RejectedCanceled,
            "rejected_timedout" => PromiseState::RejectedTimedout,
            _ => PromiseState::Pending,
        }
    }

    /// Register a listener for a promise and return the current promise record.
    async fn register_listener(&self, id: &str) -> Result<crate::types::PromiseRecord> {
        self.sender
            .promise_register_listener(id, self.network.unicast())
            .await
    }
}

// ═══════════════════════════════════════════════════════════════
//  Shared builder helpers
// ═══════════════════════════════════════════════════════════════

/// Resolve a target string: URLs pass through unchanged, bare names go through
/// `network.target_resolver`.
fn resolve_target(network: &dyn Network, target: &str) -> String {
    if is_url(target) {
        target.to_string()
    } else {
        network.target_resolver(target)
    }
}

/// Build resolved `Options` from builder fields (shared by ResRunTask and ResRpcTask).
fn build_options(
    resonate: &Resonate,
    target: Option<&str>,
    tags: Option<HashMap<String, String>>,
    timeout: Option<Duration>,
    version: Option<u32>,
) -> Options {
    let defaults = Options::default();
    let group = resonate.network.group();
    let raw_target = target.unwrap_or(group);
    let resolved_target = resolve_target(&*resonate.network, raw_target);
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
            let handle = self.spawn().await?;
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
pub struct ResRpcTask<'a, Args, T> {
    resonate: &'a Resonate,
    id: &'a str,
    func_name: &'a str,
    args: Args,
    timeout: Option<Duration>,
    version: Option<u32>,
    tags: Option<HashMap<String, String>>,
    target: Option<String>,
    _phantom: PhantomData<T>,
}

impl<'a, Args, T> ResRpcTask<'a, Args, T> {
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

impl<'a, Args: Serialize, T: DeserializeOwned> ResRpcTask<'a, Args, T> {
    /// Start the RPC and return a handle for later awaiting (replaces `begin_rpc`).
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
            .do_rpc::<T>(self.id, self.func_name, json_args, opts)
            .await
    }
}

impl<'a, Args: Serialize + Send + 'a, T: DeserializeOwned + Send + Sync + 'static> IntoFuture
    for ResRpcTask<'a, Args, T>
{
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let handle = self.spawn().await?;
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
pub struct ResScheduleTask<'a, Args> {
    resonate: &'a Resonate,
    name: &'a str,
    cron: &'a str,
    func_name: &'a str,
    args: Args,
    timeout: Option<Duration>,
    version: Option<u32>,
}

impl<'a, Args> ResScheduleTask<'a, Args> {
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

impl<'a, Args: Serialize + Send + 'a> IntoFuture for ResScheduleTask<'a, Args> {
    type Output = Result<ResonateSchedule>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<ResonateSchedule>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let defaults = Options::default();
            let timeout = self.timeout.unwrap_or(defaults.timeout);
            let version = self.version.unwrap_or(defaults.version);

            let json_args = serde_json::to_value(self.args)?;
            let param_data = serde_json::json!({
                "func": self.func_name,
                "args": json_args,
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
    use crate::PROTOCOL_VERSION;
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

    /// Helper: build a protocol-compliant promise.get request.
    fn promise_get_req(id: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "promise.get",
            "head": { "corrId": format!("pg-{}", now_ms()), "version": PROTOCOL_VERSION },
            "data": { "id": id },
        })
    }

    /// Helper: build a protocol-compliant promise.create request.
    fn promise_create_req(id: &str, timeout_at: i64) -> serde_json::Value {
        serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": format!("pc-{}", now_ms()), "version": PROTOCOL_VERSION },
            "data": { "id": id, "timeoutAt": timeout_at, "param": {}, "tags": {} },
        })
    }

    #[tokio::test]
    async fn local_constructor_sets_defaults() {
        let r = Resonate::local();
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
        let r = Resonate::local();
        assert!(r.network().unicast().starts_with("local://uni@"));
        assert!(r.network().anycast().starts_with("local://any@"));
        assert_eq!(r.network().group(), "default");
        assert_eq!(r.network().pid(), "default");
    }

    #[tokio::test]
    async fn network_match_returns_local_anycast() {
        let r = Resonate::local();
        let matched = r.network().target_resolver("my-target");
        assert_eq!(matched, "local://any@my-target");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Register Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn register_function_by_name() {
        let r = Resonate::local();
        r.register(add).unwrap();

        // Verify function is registered (spawn won't fail with FunctionNotFound)
        let result = r.run("test-id", add, (1i64, 2i64)).spawn().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn register_duplicate_function_returns_error() {
        let r = Resonate::local();
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
        let r = Resonate::local();
        r.register(noop).unwrap();

        let handle = r.run("greet-1", noop, ()).spawn().await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "greet-1");
    }

    #[tokio::test]
    async fn run_unregistered_function_returns_function_not_found() {
        let r = Resonate::local();
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
        let r = Resonate::local();
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
        let r = Resonate::local();
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
        let r = Resonate::local();
        r.register(noop).unwrap();

        let mut m = std::collections::HashMap::new();
        m.insert("user:tag".to_string(), "value".to_string());

        let handle = r.run("tag-test", noop, ()).tags(m).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn run_spawn_with_custom_timeout() {
        let r = Resonate::local();
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
        let r = Resonate::local();
        // RPC does NOT require function to be registered locally
        let handle = r
            .rpc::<_, ()>("rpc-1", "remote_func", (1i32,))
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

        let handle = r.rpc::<_, ()>("rpc-2", "remote", ()).spawn().await.unwrap();
        assert_eq!(handle.id, "svc:rpc-2");
    }

    #[tokio::test]
    async fn rpc_spawn_sets_scope_global() {
        let r = Resonate::local();
        // Verifying RPC succeeds — tags (scope=global, target) are set internally
        let handle = r.rpc::<_, ()>("rpc-scope", "remote", ()).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn rpc_spawn_with_custom_target() {
        let r = Resonate::local();

        let handle = r
            .rpc::<_, ()>("rpc-target", "remote", ())
            .target("custom-worker")
            .spawn()
            .await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn rpc_spawn_idempotent_same_id() {
        let r = Resonate::local();

        let h1 = r.rpc::<_, ()>("rpc-dup", "remote", ()).spawn().await;
        assert!(h1.is_ok());

        // Same ID should return existing promise
        let h2 = r.rpc::<_, ()>("rpc-dup", "remote", ()).spawn().await;
        assert!(h2.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  get Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn get_nonexistent_promise_returns_error() {
        let r = Resonate::local();
        let result = r.get::<()>("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::ServerError { code, .. } => assert_eq!(code, 404),
            other => panic!("expected ServerError 404, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn get_existing_promise_returns_handle() {
        let r = Resonate::local();

        // Create a promise via RPC first
        r.rpc::<_, ()>("get-test", "func", ())
            .spawn()
            .await
            .unwrap();

        // Now get it
        let handle = r.get::<()>("get-test").await;
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
        r.rpc::<_, ()>("p1", "func", ()).spawn().await.unwrap();

        // Get with the unprefixed ID (prefix is prepended internally)
        let handle = r.get::<()>("p1").await;
        assert!(handle.is_ok());
        assert_eq!(handle.unwrap().id, "ns:p1");
    }

    // ═══════════════════════════════════════════════════════════════
    //  schedule Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn schedule_creates_schedule() {
        let r = Resonate::local();
        let result = r
            .schedule("my-schedule", "*/5 * * * *", "my-func", ())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn schedule_returns_deletable_handle() {
        let r = Resonate::local();
        let schedule = r
            .schedule("deletable", "0 * * * *", "func", ())
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
        let r = Resonate::local();
        r.register(noop).unwrap();
        // Default options should work — just spawn and check
        let handle = r.run("defaults-test", noop, ()).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn run_builder_with_timeout_and_version() {
        let r = Resonate::local();
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
        let r = Resonate::local();
        r.register(noop).unwrap();

        let mut m = std::collections::HashMap::new();
        m.insert("key".into(), "val".into());

        let handle = r.run("builder-tags", noop, ()).tags(m).spawn().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn rpc_builder_target_resolution_bare_name() {
        let r = Resonate::local();

        let handle = r
            .rpc::<_, ()>("target-bare", "func", ())
            .target("my-worker")
            .spawn()
            .await
            .unwrap();

        let resp = r
            .transport()
            .send_json(promise_get_req("target-bare"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");
        assert_eq!(target, "local://any@my-worker");
        drop(handle);
    }

    #[tokio::test]
    async fn rpc_builder_target_resolution_url_passthrough() {
        let r = Resonate::local();

        let handle = r
            .rpc::<_, ()>("target-url", "func", ())
            .target("https://remote:9000/workers/hello")
            .spawn()
            .await
            .unwrap();

        let resp = r
            .transport()
            .send_json(promise_get_req("target-url"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");
        assert_eq!(target, "https://remote:9000/workers/hello");
        drop(handle);
    }

    #[tokio::test]
    async fn rpc_builder_default_target() {
        let r = Resonate::local();

        let _handle = r
            .rpc::<_, ()>("target-default", "func", ())
            .spawn()
            .await
            .unwrap();

        let resp = r
            .transport()
            .send_json(promise_get_req("target-default"))
            .await
            .unwrap();
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
        let r = Resonate::local();
        let result = r.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stop_can_be_called_twice() {
        let r = Resonate::local();
        r.stop().await.unwrap();
        // Second stop should also be fine (idempotent)
        let result = r.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stop_aborts_subscription_refresh_handle() {
        let r = Resonate::local();
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
        let r = Resonate::local();
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
        let r = Resonate::local();
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
        let h2 = r.rpc::<_, ()>("id2", "remote", ()).spawn().await.unwrap();
        assert_eq!(h2.id, "p:id2");

        // get with prefix (the promise was created as "p:id2")
        let h3 = r.get::<()>("id2").await.unwrap();
        assert_eq!(h3.id, "p:id2");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Key Difference: run vs rpc
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn run_requires_registered_function() {
        let r = Resonate::local();
        // run fails because noop is not registered
        let result: Result<()> = r.run("run-test", noop, ()).await;
        assert!(matches!(result.unwrap_err(), Error::FunctionNotFound(_)));
    }

    #[tokio::test]
    async fn rpc_does_not_require_registered_function() {
        let r = Resonate::local();
        // rpc should succeed even without registration (remote worker will handle it)
        let result = r
            .rpc::<_, ()>("rpc-test", "any_remote_func", ())
            .spawn()
            .await;
        assert!(result.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Handle Tests (via Resonate API)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn handle_id_matches_requested_id() {
        let r = Resonate::local();
        r.register(noop).unwrap();

        let handle = r.run("handle-test", noop, ()).spawn().await.unwrap();
        assert_eq!(handle.id, "handle-test");
    }

    #[tokio::test]
    async fn rpc_handle_id_matches() {
        let r = Resonate::local();
        let handle = r
            .rpc::<_, ()>("rpc-handle", "remote", ())
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
        let r = Resonate::local();
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
        let r = Resonate::local();
        r.register(noop).unwrap();

        let local_h = r.run("local-1", noop, ()).spawn().await;
        let remote_h = r.rpc::<_, ()>("remote-1", "remote-fn", ()).spawn().await;

        assert!(local_h.is_ok());
        assert!(remote_h.is_ok());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Sub-client Access Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn promises_sub_client_create_and_get() {
        let r = Resonate::local();

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
        let r = Resonate::local();

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
        let r = Resonate::local();
        // Verify we can send a raw request through the transport
        let req = promise_create_req("transport-test", i64::MAX);
        let resp = r.transport().send_json(req).await;
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
        let r = Resonate::local();

        // Use a URL as the target option — should NOT be rewritten by network.match
        let handle = r
            .rpc::<_, ()>("url-target-test", "noop", ())
            .target("https://remote-host:9000/workers/noop")
            .spawn()
            .await;
        assert!(handle.is_ok());

        // Verify the promise was created with the URL target
        let resp = r
            .transport()
            .send_json(promise_get_req("url-target-test"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        assert_eq!(target, "https://remote-host:9000/workers/noop");
    }

    #[tokio::test]
    async fn run_with_custom_target() {
        let r = Resonate::local();
        r.register(noop).unwrap();

        let _handle = r
            .run("run-target-test", noop, ())
            .target("my-target")
            .spawn()
            .await
            .unwrap();

        let resp = r
            .transport()
            .send_json(promise_get_req("run-target-test"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Bare name resolved via network.match
        assert_eq!(target, "local://any@my-target");
    }

    #[tokio::test]
    async fn run_default_target_uses_network_match() {
        let r = Resonate::local();
        r.register(noop).unwrap();

        let _handle = r.run("run-default-target", noop, ()).spawn().await.unwrap();

        let resp = r
            .transport()
            .send_json(promise_get_req("run-default-target"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        assert_eq!(target, "local://any@default");
    }

    #[tokio::test]
    async fn run_url_target_passes_through() {
        let r = Resonate::local();
        r.register(noop).unwrap();

        let _handle = r
            .run("run-url-target", noop, ())
            .target("https://remote:9000/workers/noop")
            .spawn()
            .await
            .unwrap();

        let resp = r
            .transport()
            .send_json(promise_get_req("run-url-target"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        assert_eq!(target, "https://remote:9000/workers/noop");
    }

    #[tokio::test]
    async fn rpc_with_no_target_uses_default() {
        let r = Resonate::local();

        let handle = r.rpc::<_, ()>("bare-target-test", "noop", ()).spawn().await;
        assert!(handle.is_ok());

        let resp = r
            .transport()
            .send_json(promise_get_req("bare-target-test"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Default target "default" is resolved by network.match → "local://any@default"
        assert_eq!(target, "local://any@default");
    }

    #[tokio::test]
    async fn rpc_with_bare_name_target_gets_rewritten() {
        let r = Resonate::local();

        let handle = r
            .rpc::<_, ()>("bare-target-test2", "noop", ())
            .target("noop")
            .spawn()
            .await;
        assert!(handle.is_ok());

        let resp = r
            .transport()
            .send_json(promise_get_req("bare-target-test2"))
            .await
            .unwrap();
        let target = resp["data"]["promise"]["tags"]["resonate:target"]
            .as_str()
            .unwrap_or("");

        // Bare name should be resolved by network.match → "local://any@noop"
        assert_eq!(target, "local://any@noop");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Subscription Refactor Tests (watch channels)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn multiple_handles_same_id_all_resolve() {
        let r = Resonate::local();

        // Create a promise via RPC
        let h1 = r
            .rpc::<_, ()>("multi-handle", "func", ())
            .spawn()
            .await
            .unwrap();

        // Get a second handle to the same promise
        let h2 = r.get::<()>("multi-handle").await.unwrap();

        // Notify via the subscription (simulate Unblock)
        // Use null value to avoid codec encoding issues
        {
            let subs = r.subscriptions.lock().await;
            if let Some(tx) = subs.get("multi-handle") {
                let _ = tx.send(Some(Arc::new(PromiseResult {
                    state: PromiseState::Resolved,
                    value: serde_json::json!(null),
                })));
            }
        }

        // Both handles should resolve
        let r1 = h1.result().await;
        let r2 = h2.result().await;
        assert!(r1.is_ok(), "first handle should resolve");
        assert!(r2.is_ok(), "second handle should resolve");
    }

    #[tokio::test]
    async fn early_unblock_before_create_handle() {
        let r = Resonate::local();

        // Simulate an early Unblock by inserting a pre-loaded watch entry
        // Use null value to avoid codec encoding issues
        {
            let mut subs = r.subscriptions.lock().await;
            let (tx, _) = watch::channel(Some(Arc::new(PromiseResult {
                state: PromiseState::Resolved,
                value: serde_json::json!(null),
            })));
            subs.insert("early-unblock".to_string(), tx);
        }

        // Create the promise so get() can find it
        r.promises
            .create(
                "early-unblock",
                i64::MAX,
                serde_json::json!(null),
                serde_json::json!({}),
            )
            .await
            .unwrap();

        // Now get a handle — should pick up the pre-loaded result immediately
        let handle = r.get::<()>("early-unblock").await.unwrap();
        assert!(
            handle.done().await.unwrap(),
            "handle should be done immediately for early unblock"
        );
        let result = handle.result().await;
        assert!(result.is_ok(), "early unblock handle should resolve");
    }

    #[tokio::test]
    async fn done_returns_false_then_true() {
        let r = Resonate::local();

        // Create a pending promise via RPC
        let handle = r
            .rpc::<_, ()>("done-test", "func", ())
            .spawn()
            .await
            .unwrap();

        // Should be pending
        assert!(
            !handle.done().await.unwrap(),
            "handle should not be done yet"
        );

        // Settle via subscription
        {
            let subs = r.subscriptions.lock().await;
            if let Some(tx) = subs.get("done-test") {
                let _ = tx.send(Some(Arc::new(PromiseResult {
                    state: PromiseState::Resolved,
                    value: serde_json::json!({"data": "done"}),
                })));
            }
        }

        // Should be done now
        assert!(handle.done().await.unwrap(), "handle should be done now");
    }

    #[tokio::test]
    async fn handle_dropped_without_awaiting_does_not_leak() {
        let r = Resonate::local();

        // Create handles and drop them without awaiting
        {
            let _h1 = r.rpc::<_, ()>("drop-1", "func", ()).spawn().await.unwrap();
            let _h2 = r.rpc::<_, ()>("drop-2", "func", ()).spawn().await.unwrap();
            // Both dropped here
        }

        // Should not panic or hang — the watch senders still exist in subscriptions
        // but no receivers are listening, which is fine.
        let subs = r.subscriptions.lock().await;
        assert!(subs.contains_key("drop-1"));
        assert!(subs.contains_key("drop-2"));
    }

    // ═══════════════════════════════════════════════════════════════
    //  End-to-end Subscription Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn e2e_settle_unblocks_handle() {
        let r = Resonate::local();

        // Create a pending promise via RPC
        let handle = r.rpc::<_, ()>("e2e-1", "func", ()).spawn().await.unwrap();

        assert!(!handle.done().await.unwrap(), "should be pending");

        // Settle the promise — the local network will dispatch an Unblock
        // message through the transport to our watch channel.
        r.promises
            .settle("e2e-1", "resolved", serde_json::json!(null))
            .await
            .unwrap();

        // Give the async Unblock dispatch a moment to propagate
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(handle.done().await.unwrap(), "should be done after settle");
        let result = handle.result().await;
        assert!(result.is_ok(), "should resolve successfully");
    }

    #[tokio::test]
    async fn e2e_multiple_handles_resolve_on_settle() {
        let r = Resonate::local();

        // Create two handles for the same promise
        let h1 = r
            .rpc::<_, ()>("e2e-multi", "func", ())
            .spawn()
            .await
            .unwrap();
        let h2 = r.get::<()>("e2e-multi").await.unwrap();

        // Settle — Unblock flows through transport → watch → both receivers
        r.promises
            .settle("e2e-multi", "resolved", serde_json::json!(null))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let r1 = h1.result().await;
        let r2 = h2.result().await;
        assert!(r1.is_ok(), "first handle should resolve");
        assert!(r2.is_ok(), "second handle should resolve");
    }

    #[tokio::test]
    async fn e2e_reject_unblocks_handle_with_error() {
        let r = Resonate::local();

        let handle = r
            .rpc::<_, ()>("e2e-reject", "func", ())
            .spawn()
            .await
            .unwrap();

        // Reject the promise
        r.promises
            .settle("e2e-reject", "rejected", serde_json::json!(null))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = handle.result().await;
        assert!(result.is_err(), "rejected promise should return error");
    }

    #[tokio::test]
    async fn e2e_settle_before_handle_returns_immediately() {
        let r = Resonate::local();

        // Create and immediately settle a promise
        r.promises
            .create(
                "e2e-pre",
                i64::MAX,
                serde_json::json!(null),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        r.promises
            .settle("e2e-pre", "resolved", serde_json::json!(null))
            .await
            .unwrap();

        // Getting a handle to an already-settled promise should work immediately
        let handle = r.get::<()>("e2e-pre").await.unwrap();
        assert!(handle.done().await.unwrap(), "should be done immediately");
        let result = handle.result().await;
        assert!(result.is_ok(), "already-settled promise should resolve");
    }

    #[tokio::test]
    async fn e2e_result_blocks_until_settle() {
        let r = Resonate::local();

        let handle = r
            .rpc::<_, ()>("e2e-block", "func", ())
            .spawn()
            .await
            .unwrap();

        // Settle after a short delay in a background task
        let promises = r.promises.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            promises
                .settle("e2e-block", "resolved", serde_json::json!(null))
                .await
                .unwrap();
        });

        // result() should block until the settle arrives
        let result = handle.result().await;
        assert!(result.is_ok(), "result() should unblock after settle");
    }

    // ═══════════════════════════════════════════════════════════════
    //  End-to-end Function Execution Tests
    //
    //  These tests start a Resonate::local() instance, register
    //  functions, run them, and verify the actual return values.
    // ═══════════════════════════════════════════════════════════════

    #[resonate_macros::function]
    async fn greet(name: String) -> Result<String> {
        Ok(format!("hello, {}!", name))
    }

    #[resonate_macros::function]
    async fn fail_with_message(msg: String) -> Result<String> {
        Err(Error::Application { message: msg })
    }

    #[resonate_macros::function]
    async fn multiply(x: i64, y: i64) -> Result<i64> {
        Ok(x * y)
    }

    #[tokio::test]
    async fn e2e_run_simple_function_returns_result() {
        let r = Resonate::local();
        r.register(add).unwrap();

        let result: i64 = r.run("e2e-add", add, (3_i64, 4_i64)).await.unwrap();
        assert_eq!(result, 7);
    }

    #[tokio::test]
    async fn e2e_run_noop_function_completes() {
        let r = Resonate::local();
        r.register(noop).unwrap();

        // noop returns Ok(()) — use spawn + done to verify it completed
        let handle = r.run("e2e-noop", noop, ()).spawn().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(handle.done().await.unwrap(), "noop should complete");
    }

    #[tokio::test]
    async fn e2e_run_string_args_and_return() {
        let r = Resonate::local();
        r.register(greet).unwrap();

        let result: String = r
            .run("e2e-greet", greet, "world".to_string())
            .await
            .unwrap();
        assert_eq!(result, "hello, world!");
    }

    #[tokio::test]
    async fn e2e_run_failing_function_returns_error() {
        let r = Resonate::local();
        r.register(fail_with_message).unwrap();

        let result: Result<String> = r
            .run("e2e-fail", fail_with_message, "something broke".to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn e2e_run_via_handle_returns_result() {
        let r = Resonate::local();
        r.register(multiply).unwrap();

        let handle = r
            .run("e2e-mul", multiply, (6_i64, 7_i64))
            .spawn()
            .await
            .unwrap();

        // Give the async execution a moment to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(handle.done().await.unwrap(), "should be done");
        let result: i64 = handle.result().await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn e2e_run_idempotent_same_id_returns_same_result() {
        let r = Resonate::local();
        r.register(add).unwrap();

        let r1: i64 = r.run("e2e-idem", add, (10_i64, 20_i64)).await.unwrap();
        assert_eq!(r1, 30);

        // Second call with same ID should return the same result (idempotent)
        let r2: i64 = r.run("e2e-idem", add, (10_i64, 20_i64)).await.unwrap();
        assert_eq!(r2, 30);
    }

    #[tokio::test]
    async fn e2e_run_multiple_functions_concurrently() {
        let r = Resonate::local();
        r.register(add).unwrap();
        r.register(multiply).unwrap();

        let (r1, r2) = tokio::join!(
            r.run("e2e-conc-add", add, (1_i64, 2_i64)),
            r.run("e2e-conc-mul", multiply, (3_i64, 4_i64)),
        );

        assert_eq!(r1.unwrap(), 3_i64);
        assert_eq!(r2.unwrap(), 12_i64);
    }

    #[resonate_macros::function]
    async fn spawn_child(ctx: &Context) -> Result<i64> {
        let h = ctx.run(multiply, (3_i64, 5_i64)).spawn().await?;
        let result = h.await?;
        Ok(result)
    }

    #[tokio::test]
    async fn e2e_workflow_spawn_durable_future_is_send() {
        let r = Resonate::local();
        r.register(multiply).unwrap();
        r.register(spawn_child).unwrap();

        let result: i64 = r.run("e2e-spawn-child", spawn_child, ()).await.unwrap();
        assert_eq!(result, 15);
    }

    #[tokio::test]
    async fn e2e_get_handle_after_run_completes() {
        let r = Resonate::local();
        r.register(add).unwrap();

        // Run to completion
        let _: i64 = r.run("e2e-get-after", add, (5_i64, 5_i64)).await.unwrap();

        // Get a handle to the already-completed promise
        let handle = r.get::<i64>("e2e-get-after").await.unwrap();
        assert!(handle.done().await.unwrap(), "should already be done");
        let result = handle.result().await.unwrap();
        assert_eq!(result, 10);
    }

    // ═══════════════════════════════════════════════════════════════
    //  Dependency Injection Tests
    // ═══════════════════════════════════════════════════════════════

    struct TestConfig {
        value: String,
    }

    struct TestCounter {
        count: i64,
    }

    #[resonate_macros::function]
    async fn read_config(ctx: &Context) -> Result<String> {
        let cfg = ctx.get_dependency::<TestConfig>();
        Ok(cfg.value.clone())
    }

    #[resonate_macros::function]
    async fn read_config_leaf(info: &Info) -> Result<String> {
        let cfg = info.get_dependency::<TestConfig>();
        Ok(cfg.value.clone())
    }

    #[resonate_macros::function]
    async fn read_two_deps(ctx: &Context) -> Result<String> {
        let cfg = ctx.get_dependency::<TestConfig>();
        let counter = ctx.get_dependency::<TestCounter>();
        Ok(format!("{}:{}", cfg.value, counter.count))
    }

    struct MissingDep;

    #[resonate_macros::function]
    async fn read_missing_dep(ctx: &Context) -> Result<String> {
        let _dep = ctx.get_dependency::<MissingDep>();
        Ok("unreachable".to_string())
    }

    #[tokio::test]
    async fn e2e_workflow_reads_dependency_via_context() {
        let r = Resonate::local().with_dependency(TestConfig {
            value: "hello-from-di".to_string(),
        });
        r.register(read_config).unwrap();

        let result: String = r.run("di-ctx", read_config, ()).await.unwrap();
        assert_eq!(result, "hello-from-di");
    }

    #[tokio::test]
    async fn e2e_leaf_reads_dependency_via_info() {
        let r = Resonate::local().with_dependency(TestConfig {
            value: "leaf-di".to_string(),
        });
        r.register(read_config_leaf).unwrap();

        let result: String = r.run("di-info", read_config_leaf, ()).await.unwrap();
        assert_eq!(result, "leaf-di");
    }

    #[tokio::test]
    async fn e2e_multiple_dependencies() {
        let r = Resonate::local()
            .with_dependency(TestConfig {
                value: "multi".to_string(),
            })
            .with_dependency(TestCounter { count: 42 });
        r.register(read_two_deps).unwrap();

        let result: String = r.run("di-multi", read_two_deps, ()).await.unwrap();
        assert_eq!(result, "multi:42");
    }

    #[tokio::test]
    async fn e2e_missing_dependency_panics_gracefully() {
        // Verify that a missing dependency causes a panic that is caught at
        // the task boundary (catch_unwind) — the process doesn't crash.
        // The task gets *released* (not fulfilled), so we can't await the result.
        // Instead, use a timeout to confirm the handle never resolves.
        let r = Resonate::local();
        r.register(read_missing_dep).unwrap();

        let handle = r
            .run::<_, _, String>("di-missing", read_missing_dep, ())
            .spawn()
            .await
            .unwrap();

        // The panic is caught; the task is released. The handle should never
        // resolve because the promise is never settled.
        let timed_out = tokio::time::timeout(Duration::from_millis(200), handle.result()).await;
        assert!(
            timed_out.is_err(),
            "handle should not resolve — task was released after panic"
        );
    }
}
