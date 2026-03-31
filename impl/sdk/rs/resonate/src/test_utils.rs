//! Shared test infrastructure for all test modules.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::codec::{Codec, NoopEncryptor};
use crate::context::Context;
use crate::effects::Effects;
use crate::error;
use crate::send::Sender;
use crate::transport::Transport;
use crate::types::{PromiseRecord, PromiseState, SettleState, Value};

fn test_codec() -> Codec {
    Codec::new(Arc::new(NoopEncryptor))
}

/// A stub task for Core tests.
#[derive(Clone)]
pub struct StubTask {
    pub root_promise: PromiseRecord,
    pub preloaded: Vec<PromiseRecord>,
}

/// In-memory promise store that mimics the Resonate server.
pub struct StubNetwork {
    pub promises: HashMap<String, PromiseRecord>,
    pub tasks: HashMap<String, StubTask>,
    /// Controls what task.suspend returns. If true, returns Redirect.
    pub suspend_returns_redirect: bool,
    /// How many times redirect has been returned (to avoid infinite loops).
    pub redirect_count: usize,
    pub max_redirects: usize,
}

impl StubNetwork {
    pub fn new() -> Self {
        Self {
            promises: HashMap::new(),
            tasks: HashMap::new(),
            suspend_returns_redirect: false,
            redirect_count: 0,
            max_redirects: 1,
        }
    }

    /// Handle a JSON request string and return a JSON response string.
    /// Accepts both envelope and flat formats. Always returns envelope format.
    fn handle_request(&mut self, req_json: &serde_json::Value) -> serde_json::Value {
        // Unwrap envelope if present
        let (kind, corr_id, data) =
            if req_json.get("head").is_some() && req_json.get("data").is_some() {
                let kind = req_json
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .unwrap_or("")
                    .to_string();
                let corr_id = req_json
                    .get("head")
                    .and_then(|h| h.get("corrId"))
                    .cloned()
                    .unwrap_or_default();
                let data = req_json
                    .get("data")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));
                (kind, corr_id, data)
            } else {
                let kind = req_json
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .unwrap_or("")
                    .to_string();
                let corr_id = req_json.get("corrId").cloned().unwrap_or_default();
                (kind, corr_id, req_json.clone())
            };

        let (status, resp_data) = match kind.as_str() {
            "promise.create" => (200, self.handle_promise_create(&data)),
            "promise.settle" => (200, self.handle_promise_settle(&data)),
            "task.acquire" => {
                let task_id = data
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !self.tasks.contains_key(task_id) {
                    (
                        404,
                        serde_json::json!(format!("task {} not found", task_id)),
                    )
                } else {
                    (200, self.handle_task_acquire(task_id))
                }
            }
            "task.fulfill" => (200, self.handle_task_fulfill(&data)),
            "task.suspend" => {
                let (s, d) = self.handle_task_suspend(&data);
                (s, d)
            }
            "task.release" => (200, self.handle_task_release(&data)),
            _ => (
                400,
                serde_json::json!(format!("unknown request kind: {}", kind)),
            ),
        };

        serde_json::json!({
            "kind": kind,
            "head": {
                "corrId": corr_id,
                "status": status,
                "version": "2025-01-15",
            },
            "data": resp_data,
        })
    }

    fn handle_promise_create(&mut self, data: &serde_json::Value) -> serde_json::Value {
        let id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");

        // Idempotent: if exists, return existing
        if let Some(existing) = self.promises.get(id) {
            return serde_json::json!({
                "promise": promise_to_json(existing),
            });
        }

        let timeout_at = data
            .get("timeoutAt")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX);
        let param = data
            .get("param")
            .cloned()
            .unwrap_or(serde_json::json!({"headers": null, "data": null}));
        let tags: HashMap<String, String> = data
            .get("tags")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let record = PromiseRecord {
            id: id.to_string(),
            state: PromiseState::Pending,
            timeout_at,
            param: serde_json::from_value(param).unwrap_or_default(),
            value: Value::default(),
            tags,
            created_at: 0,
            settled_at: None,
        };
        self.promises.insert(id.to_string(), record.clone());
        serde_json::json!({
            "promise": promise_to_json(&record),
        })
    }

    fn handle_promise_settle(&mut self, data: &serde_json::Value) -> serde_json::Value {
        let id = data.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let state_str = data
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("resolved");
        let value = data
            .get("value")
            .cloned()
            .unwrap_or(serde_json::json!({"headers": null, "data": null}));

        let settle_state = match state_str {
            "resolved" => SettleState::Resolved,
            "rejected" => SettleState::Rejected,
            _ => SettleState::RejectedCanceled,
        };
        let promise_state = match settle_state {
            SettleState::Resolved => PromiseState::Resolved,
            SettleState::Rejected | SettleState::RejectedCanceled => PromiseState::Rejected,
        };

        if let Some(p) = self.promises.get_mut(id) {
            if p.state != PromiseState::Pending {
                return serde_json::json!({
                    "promise": promise_to_json(p),
                });
            }
            p.state = promise_state;
            p.value = serde_json::from_value(value).unwrap_or_default();
            p.settled_at = Some(1);
            return serde_json::json!({
                "promise": promise_to_json(p),
            });
        }

        // Promise not found — create and settle it
        let record = PromiseRecord {
            id: id.to_string(),
            state: promise_state,
            timeout_at: i64::MAX,
            param: Value::default(),
            value: serde_json::from_value(value).unwrap_or_default(),
            tags: HashMap::new(),
            created_at: 0,
            settled_at: Some(1),
        };
        self.promises.insert(id.to_string(), record.clone());
        serde_json::json!({
            "promise": promise_to_json(&record),
        })
    }

    fn handle_task_acquire(&self, task_id: &str) -> serde_json::Value {
        if let Some(task) = self.tasks.get(task_id) {
            let preloaded: Vec<serde_json::Value> =
                task.preloaded.iter().map(promise_to_json).collect();
            serde_json::json!({
                "task": {
                    "id": task_id,
                    "state": "acquired",
                    "version": 0,
                    "resumes": [],
                },
                "promise": promise_to_json(&task.root_promise),
                "preload": preloaded,
            })
        } else {
            // Shouldn't reach here — 404 is handled in handle_request
            serde_json::json!({})
        }
    }

    fn handle_task_fulfill(&self, data: &serde_json::Value) -> serde_json::Value {
        // Return the promise from the action.
        // The action may be a full envelope { kind, head, data } or flat { id, state, value }.
        let raw_action = data.get("action");
        let action = raw_action.map(|a| unwrap_sub_envelope(a));
        let id = action
            .as_ref()
            .and_then(|a| a.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let state = action
            .as_ref()
            .and_then(|a| a.get("state"))
            .and_then(|v| v.as_str())
            .unwrap_or("resolved");
        let value = action
            .as_ref()
            .and_then(|a| a.get("value"))
            .cloned()
            .unwrap_or_default();

        serde_json::json!({
            "promise": {
                "id": id,
                "state": state,
                "param": {},
                "value": value,
                "tags": {},
                "timeoutAt": i64::MAX,
                "createdAt": 0,
                "settledAt": 1,
            }
        })
    }

    /// Returns (status_code, data)
    fn handle_task_suspend(&mut self, _data: &serde_json::Value) -> (u16, serde_json::Value) {
        if self.suspend_returns_redirect && self.redirect_count < self.max_redirects {
            self.redirect_count += 1;
            (300, serde_json::json!({ "preload": [] }))
        } else {
            (200, serde_json::json!({}))
        }
    }

    fn handle_task_release(&self, _data: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({})
    }
}

/// Unwrap a sub-envelope `{ kind, head, data }` into just its `data` portion.
/// If it's not an envelope, return as-is.
fn unwrap_sub_envelope(val: &serde_json::Value) -> serde_json::Value {
    if val.get("kind").is_some() && val.get("head").is_some() && val.get("data").is_some() {
        val.get("data").cloned().unwrap_or(val.clone())
    } else {
        val.clone()
    }
}

/// Convert a PromiseRecord to JSON matching the server response format.
fn promise_to_json(p: &PromiseRecord) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "state": match p.state {
            PromiseState::Pending => "pending",
            PromiseState::Resolved => "resolved",
            PromiseState::Rejected => "rejected",
            PromiseState::RejectedCanceled => "rejected_canceled",
            PromiseState::RejectedTimedout => "rejected_timedout",
        },
        "param": {
            "headers": p.param.headers,
            "data": p.param.data,
        },
        "value": {
            "headers": p.value.headers,
            "data": p.value.data,
        },
        "tags": p.tags,
        "timeoutAt": p.timeout_at,
        "createdAt": p.created_at,
        "settledAt": p.settled_at,
    })
}

/// A Network implementation backed by StubNetwork for testing.
/// Tracks all sent requests and their count.
struct StubNetworkAdapter {
    network: Arc<Mutex<StubNetwork>>,
    send_count: Arc<AtomicUsize>,
    sent_json: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[async_trait::async_trait]
impl crate::network::Network for StubNetworkAdapter {
    fn pid(&self) -> &str {
        "test-pid"
    }
    fn group(&self) -> &str {
        "test-group"
    }
    fn unicast(&self) -> &str {
        "test-unicast"
    }
    fn anycast(&self) -> &str {
        "test-anycast"
    }
    async fn start(&self) -> error::Result<()> {
        Ok(())
    }
    async fn stop(&self) -> error::Result<()> {
        Ok(())
    }

    async fn send(&self, req: String) -> error::Result<String> {
        self.send_count.fetch_add(1, Ordering::SeqCst);

        let req_json: serde_json::Value = serde_json::from_str(&req)
            .map_err(|e| error::Error::DecodingError(format!("invalid request JSON: {}", e)))?;

        self.sent_json.lock().await.push(req_json.clone());

        let mut net = self.network.lock().await;
        let resp = net.handle_request(&req_json);
        Ok(serde_json::to_string(&resp).unwrap())
    }

    fn recv(&self, _callback: Box<dyn Fn(String) + Send + Sync>) {
        // No-op for tests
    }

    fn target_resolver(&self, target: &str) -> String {
        target.to_string()
    }
}

/// Test harness wrapping a StubNetwork with tracking.
pub struct TestHarness {
    network: Arc<Mutex<StubNetwork>>,
    send_count: Arc<AtomicUsize>,
    sent_json: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl TestHarness {
    pub fn new() -> Self {
        Self {
            network: Arc::new(Mutex::new(StubNetwork::new())),
            send_count: Arc::new(AtomicUsize::new(0)),
            sent_json: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_send_count(&self) -> usize {
        self.send_count.load(Ordering::SeqCst)
    }

    pub async fn add_promise(&self, record: PromiseRecord) {
        let mut net = self.network.lock().await;
        net.promises.insert(record.id.clone(), record);
    }

    pub async fn add_task(
        &self,
        task_id: &str,
        root_promise: PromiseRecord,
        preloaded: Vec<PromiseRecord>,
    ) {
        let mut net = self.network.lock().await;
        // Also add root promise to the promise store
        net.promises
            .insert(root_promise.id.clone(), root_promise.clone());
        net.tasks.insert(
            task_id.to_string(),
            StubTask {
                root_promise,
                preloaded,
            },
        );
    }

    pub async fn set_suspend_returns_redirect(&self, val: bool) {
        let mut net = self.network.lock().await;
        net.suspend_returns_redirect = val;
    }

    pub async fn set_max_redirects(&self, max: usize) {
        let mut net = self.network.lock().await;
        net.max_redirects = max;
    }

    /// Build a Sender backed by the StubNetwork.
    pub fn build_sender(&self) -> Sender {
        let adapter = StubNetworkAdapter {
            network: self.network.clone(),
            send_count: self.send_count.clone(),
            sent_json: self.sent_json.clone(),
        };
        let transport = Transport::new(Arc::new(adapter));
        Sender::new(transport)
    }

    /// Build Effects from the stub, with optional preloaded promises.
    pub fn build_effects(&self, preload: Vec<PromiseRecord>) -> Effects {
        Effects::new(self.build_sender(), test_codec(), preload)
    }

    /// Return the sent requests as flattened JSON (for test assertions).
    /// Unwraps envelope format: merges `data` fields and `kind` into a flat object.
    /// Also unwraps nested sub-envelopes in `action`/`actions` fields.
    pub async fn sent_requests_json(&self) -> Vec<serde_json::Value> {
        self.sent_json
            .lock()
            .await
            .iter()
            .map(|req| {
                if req.get("head").is_some() && req.get("data").is_some() {
                    // Envelope format — flatten for test convenience
                    let mut flat = req
                        .get("data")
                        .and_then(|d| d.as_object())
                        .cloned()
                        .unwrap_or_default();
                    if let Some(kind) = req.get("kind") {
                        flat.insert("kind".to_string(), kind.clone());
                    }
                    // Also unwrap nested sub-envelopes in action/actions
                    if let Some(action) = flat.get("action").cloned() {
                        flat.insert("action".to_string(), unwrap_sub_envelope(&action));
                    }
                    if let Some(actions) = flat.get("actions").and_then(|v| v.as_array()).cloned() {
                        let unwrapped: Vec<serde_json::Value> = actions
                            .iter()
                            .map(|a| unwrap_sub_envelope(a))
                            .collect();
                        flat.insert("actions".to_string(), serde_json::Value::Array(unwrapped));
                    }
                    serde_json::Value::Object(flat)
                } else {
                    req.clone()
                }
            })
            .collect()
    }

    /// Settle a promise in the stub directly (simulating remote completion).
    pub async fn settle_promise_in_stub(&self, id: &str, value: serde_json::Value) {
        let codec = test_codec();
        let encoded = codec.encode(&value).unwrap();
        let mut net = self.network.lock().await;
        if let Some(p) = net.promises.get_mut(id) {
            p.state = PromiseState::Resolved;
            p.value = encoded;
            p.settled_at = Some(1);
        } else {
            net.promises.insert(
                id.to_string(),
                PromiseRecord {
                    id: id.to_string(),
                    state: PromiseState::Resolved,
                    timeout_at: i64::MAX,
                    param: Value::default(),
                    value: encoded,
                    tags: HashMap::new(),
                    created_at: 0,
                    settled_at: Some(1),
                },
            );
        }
    }
}

/// Helper to create a pending PromiseRecord (with encoded param).
pub fn pending_promise(id: &str) -> PromiseRecord {
    let codec = test_codec();
    PromiseRecord {
        id: id.to_string(),
        state: PromiseState::Pending,
        timeout_at: i64::MAX,
        param: codec
            .encode(&serde_json::json!({"func": "test", "args": []}))
            .unwrap(),
        value: Value::default(),
        tags: HashMap::new(),
        created_at: 0,
        settled_at: None,
    }
}

/// Helper to create a pending PromiseRecord with a specific param.
pub fn pending_promise_with_param(id: &str, param: serde_json::Value) -> PromiseRecord {
    let codec = test_codec();
    PromiseRecord {
        id: id.to_string(),
        state: PromiseState::Pending,
        timeout_at: i64::MAX,
        param: codec.encode(&param).unwrap(),
        value: Value::default(),
        tags: HashMap::new(),
        created_at: 0,
        settled_at: None,
    }
}

/// Helper to create a resolved PromiseRecord (with encoded value).
pub fn resolved_promise(id: &str, value: serde_json::Value) -> PromiseRecord {
    let codec = test_codec();
    PromiseRecord {
        id: id.to_string(),
        state: PromiseState::Resolved,
        timeout_at: i64::MAX,
        param: Value::default(),
        value: codec.encode(&value).unwrap(),
        tags: HashMap::new(),
        created_at: 0,
        settled_at: Some(1),
    }
}

/// Helper to create a rejected PromiseRecord (with encoded error value).
#[allow(dead_code)]
pub fn rejected_promise(id: &str, message: &str) -> PromiseRecord {
    let codec = test_codec();
    let err_val = serde_json::json!({"__type": "error", "message": message});
    PromiseRecord {
        id: id.to_string(),
        state: PromiseState::Rejected,
        timeout_at: i64::MAX,
        param: Value::default(),
        value: codec.encode(&err_val).unwrap(),
        tags: HashMap::new(),
        created_at: 0,
        settled_at: Some(1),
    }
}

/// A no-op target resolver for tests (returns target unchanged).
pub fn test_target_resolver() -> crate::context::TargetResolver {
    std::sync::Arc::new(|target: &str| target.to_string())
}

/// Build a root Context for testing with mock effects.
pub fn test_context(id: &str, effects: Effects) -> Context {
    Context::root(
        id.to_string(),
        i64::MAX,
        "test".to_string(),
        effects,
        test_target_resolver(),
    )
}

/// Build a root Context for testing with a specific function name.
#[allow(dead_code)]
pub fn test_context_with_func(id: &str, func_name: &str, effects: Effects) -> Context {
    Context::root(
        id.to_string(),
        i64::MAX,
        func_name.to_string(),
        effects,
        test_target_resolver(),
    )
}

/// Build a root Context for testing with a custom target resolver.
#[allow(dead_code)]
pub fn test_context_with_match(
    id: &str,
    effects: Effects,
    target_resolver: crate::context::TargetResolver,
) -> Context {
    Context::root(
        id.to_string(),
        i64::MAX,
        "test".to_string(),
        effects,
        target_resolver,
    )
}

/// Build a root Context for testing with a specific timeout_at.
#[allow(dead_code)]
pub fn test_context_with_timeout(id: &str, timeout_at: i64, effects: Effects) -> Context {
    Context::root(
        id.to_string(),
        timeout_at,
        "test".to_string(),
        effects,
        test_target_resolver(),
    )
}

/// Finalize a context into an Outcome after a workflow function has been called.
/// Call this after running operations on the context to determine Done vs Suspended.
pub async fn finalize_context(
    ctx: &Context,
    result: error::Result<serde_json::Value>,
) -> crate::types::Outcome {
    let flush_remote = ctx.flush_local_work().await;
    let mut remote_todos = ctx.take_remote_todos().await;
    remote_todos.extend(flush_remote);

    if remote_todos.is_empty() {
        crate::types::Outcome::Done(result)
    } else {
        crate::types::Outcome::Suspended { remote_todos }
    }
}
