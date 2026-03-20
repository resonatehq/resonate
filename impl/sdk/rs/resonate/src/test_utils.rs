//! Shared test infrastructure for all test modules.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::codec::Codec;
use crate::context::Context;
use crate::effects::Effects;
use crate::error;
use crate::send::{Request, Response, SendFn};
use crate::types::{
    PromiseRecord, PromiseState, SettleState, Value,
};

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
}

/// Test harness wrapping a StubNetwork with tracking.
pub struct TestHarness {
    pub network: Arc<Mutex<StubNetwork>>,
    pub send_count: Arc<AtomicUsize>,
    pub sent_requests: Arc<Mutex<Vec<Request>>>,
}

impl TestHarness {
    pub fn new() -> Self {
        Self {
            network: Arc::new(Mutex::new(StubNetwork::new())),
            send_count: Arc::new(AtomicUsize::new(0)),
            sent_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_send_count(&self) -> usize {
        self.send_count.load(Ordering::SeqCst)
    }

    pub async fn add_promise(&self, record: PromiseRecord) {
        let mut net = self.network.lock().await;
        net.promises.insert(record.id.clone(), record);
    }

    pub async fn add_task(&self, task_id: &str, root_promise: PromiseRecord, preloaded: Vec<PromiseRecord>) {
        let mut net = self.network.lock().await;
        // Also add root promise to the promise store
        net.promises.insert(root_promise.id.clone(), root_promise.clone());
        net.tasks.insert(task_id.to_string(), StubTask { root_promise, preloaded });
    }

    pub async fn set_suspend_returns_redirect(&self, val: bool) {
        let mut net = self.network.lock().await;
        net.suspend_returns_redirect = val;
    }

    pub fn build_send_fn(&self) -> SendFn {
        let network = self.network.clone();
        let send_count = self.send_count.clone();
        let sent_requests = self.sent_requests.clone();

        Arc::new(move |req: Request| {
            let network = network.clone();
            let send_count = send_count.clone();
            let sent_requests = sent_requests.clone();
            Box::pin(async move {
                send_count.fetch_add(1, Ordering::SeqCst);
                sent_requests.lock().await.push(req.clone());

                let mut net = network.lock().await;

                match req {
                    Request::PromiseCreate(create_req) => {
                        // Idempotent: if exists, return existing
                        if let Some(existing) = net.promises.get(&create_req.id) {
                            return Ok(Response::Promise(existing.clone()));
                        }
                        let record = PromiseRecord {
                            id: create_req.id.clone(),
                            state: PromiseState::Pending,
                            timeout_at: create_req.timeout_at,
                            param: create_req.param,
                            value: Value::default(),
                            tags: create_req.tags,
                            created_at: 0,
                            settled_at: None,
                        };
                        net.promises.insert(create_req.id, record.clone());
                        Ok(Response::Promise(record))
                    }
                    Request::PromiseSettle(settle_req) => {
                        let promise = net.promises.get_mut(&settle_req.id);
                        match promise {
                            Some(p) => {
                                if p.state != PromiseState::Pending {
                                    return Ok(Response::Promise(p.clone()));
                                }
                                p.state = match settle_req.state {
                                    SettleState::Resolved => PromiseState::Resolved,
                                    SettleState::Rejected | SettleState::RejectedCanceled => {
                                        PromiseState::Rejected
                                    }
                                };
                                p.value = settle_req.value;
                                p.settled_at = Some(1);
                                Ok(Response::Promise(p.clone()))
                            }
                            None => {
                                let state = match settle_req.state {
                                    SettleState::Resolved => PromiseState::Resolved,
                                    SettleState::Rejected | SettleState::RejectedCanceled => {
                                        PromiseState::Rejected
                                    }
                                };
                                let record = PromiseRecord {
                                    id: settle_req.id.clone(),
                                    state,
                                    timeout_at: i64::MAX,
                                    param: Value::default(),
                                    value: settle_req.value,
                                    tags: HashMap::new(),
                                    created_at: 0,
                                    settled_at: Some(1),
                                };
                                net.promises.insert(settle_req.id, record.clone());
                                Ok(Response::Promise(record))
                            }
                        }
                    }
                    Request::TaskAcquire { task_id } => {
                        if let Some(task) = net.tasks.get(&task_id) {
                            Ok(Response::TaskAcquireResult {
                                root_promise: task.root_promise.clone(),
                                preloaded: task.preloaded.clone(),
                            })
                        } else {
                            Err(error::Error::ServerError {
                                code: 404,
                                message: format!("task {} not found", task_id),
                            })
                        }
                    }
                    Request::TaskFulfill { .. } => {
                        Ok(Response::Suspended) // Reuse Suspended as ack
                    }
                    Request::TaskSuspend { .. } => {
                        if net.suspend_returns_redirect && net.redirect_count < net.max_redirects {
                            net.redirect_count += 1;
                            Ok(Response::Redirect {
                                preloaded: Vec::new(),
                            })
                        } else {
                            Ok(Response::Suspended)
                        }
                    }
                    Request::TaskRelease { .. } => {
                        Ok(Response::Suspended) // Reuse Suspended as ack
                    }
                }
            })
        })
    }

    pub fn build_effects(&self, preload: Vec<PromiseRecord>) -> Effects {
        Effects::new(self.build_send_fn(), Codec, preload)
    }

    pub async fn sent_requests(&self) -> Vec<Request> {
        self.sent_requests.lock().await.clone()
    }

    /// Settle a promise in the stub directly (simulating remote completion).
    pub async fn settle_promise_in_stub(&self, id: &str, value: serde_json::Value) {
        let codec = Codec;
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
    let codec = Codec;
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
    let codec = Codec;
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
    let codec = Codec;
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
    let codec = Codec;
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

/// A no-op match function for tests (returns target unchanged).
pub fn test_match_fn() -> crate::context::MatchFn {
    std::sync::Arc::new(|target: &str| target.to_string())
}

/// Build a root Context for testing with mock effects.
pub fn test_context(id: &str, effects: Effects) -> Context {
    Context::root(id.to_string(), i64::MAX, "test".to_string(), effects, test_match_fn())
}

/// Build a root Context for testing with a specific function name.
#[allow(dead_code)]
pub fn test_context_with_func(id: &str, func_name: &str, effects: Effects) -> Context {
    Context::root(id.to_string(), i64::MAX, func_name.to_string(), effects, test_match_fn())
}

/// Build a root Context for testing with a custom match function.
#[allow(dead_code)]
pub fn test_context_with_match(id: &str, effects: Effects, match_fn: crate::context::MatchFn) -> Context {
    Context::root(id.to_string(), i64::MAX, "test".to_string(), effects, match_fn)
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
