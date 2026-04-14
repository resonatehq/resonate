use futures::FutureExt;
use parking_lot::RwLock;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use serde::Deserialize;

use crate::codec::Codec;
use crate::context::{Context, TargetResolver};
use crate::durable::ExecutionEnv;
use crate::effects::Effects;
use crate::error::{Error, Result};
use crate::heartbeat::Heartbeat;
use crate::registry::Registry;
use crate::send::{Sender, SuspendResult};
use crate::types::{DurableKind, PromiseRecord, PromiseState, SettleState, Status, TaskData};

/// Core is the top-level component that manages the full lifecycle of a task.
/// It takes a `send` function and uses it for all server communication.
///
/// Core exposes two entry points:
///
/// 1. **`execute_until_blocked(task_id, promise, preload)`**
///    Called from `Resonate::run` / `Resonate::begin_run` when the task is *already acquired*.
///    Skips the acquire step. Runs the registered function until it completes
///    (Done → fulfills the task) or suspends (Suspended → suspends the task).
///    On error → releases the task.
///
/// 2. **`on_message(task_id)`**
///    Called when an `execute` message arrives from the network.
///    Acquires the task first, then delegates to `execute_until_blocked`.
pub struct Core {
    sender: Sender,
    codec: Codec,
    registry: Arc<RwLock<Registry>>,
    target_resolver: TargetResolver,
    heartbeat: Arc<dyn Heartbeat>,
    pid: String,
    ttl: i64,
    deps: Arc<crate::DependencyMap>,
}

impl Core {
    pub(crate) fn new(
        sender: Sender,
        codec: Codec,
        registry: Arc<RwLock<Registry>>,
        target_resolver: TargetResolver,
        heartbeat: Arc<dyn Heartbeat>,
        pid: String,
        ttl: i64,
        deps: Arc<crate::DependencyMap>,
    ) -> Self {
        Self {
            sender,
            codec,
            registry,
            target_resolver,
            heartbeat,
            pid,
            ttl,
            deps,
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Path 1: on_message — acquires the task, then executes
    // ═══════════════════════════════════════════════════════════════

    /// Called when an `execute` message arrives from the network.
    /// Acquires the task, decodes the promise, then runs
    /// `execute_until_blocked`.
    pub async fn on_message(&self, task_id: &str, version: i64) -> Result<Status> {
        // 1. ACQUIRE the task
        let result = self
            .sender
            .task_acquire(task_id, version, &self.pid, self.ttl)
            .await?;

        tracing::debug!(task_id = task_id, "task acquired");

        // 2. Decode promise
        let task_version = result.task.version;
        let promise = self.codec.decode_promise(result.promise)?;

        // 3. Delegate to execute_until_blocked
        self.execute_until_blocked(task_id, task_version, promise, Some(result.preload))
            .await
    }

    // ═══════════════════════════════════════════════════════════════
    //  Path 2: execute_until_blocked — task already acquired
    // ═══════════════════════════════════════════════════════════════

    /// Runs the registered function for the given task + promise until it
    /// completes (Done) or suspends waiting on remote promises (Suspended).
    ///
    /// - On Done → fulfills the task (settles the promise and completes
    ///   the task).
    /// - On Suspended → suspends the task (registers callbacks for unresolved
    ///   remote dependencies).
    /// - On error → releases the task so another worker can pick it up.
    ///
    /// This method does NOT acquire the task — the caller must ensure the task
    /// is already in `acquired` state.
    pub async fn execute_until_blocked(
        &self,
        task_id: &str,
        task_version: i64,
        promise: PromiseRecord,
        preload: Option<Vec<PromiseRecord>>,
    ) -> Result<Status> {
        // Start heartbeat before execution to keep the task lease alive
        self.heartbeat.start(task_id, task_version);

        tracing::debug!(task_id = task_id, promise_id = %promise.id, "starting execution");

        let result = self
            .execute_until_blocked_inner(task_id, task_version, &promise, preload)
            .await;

        // Stop heartbeat after execution completes (both success and error)
        self.heartbeat.stop(task_id);

        match result {
            Ok(status) => Ok(status),
            Err(e) => {
                // On error → release the task so another worker can retry
                tracing::error!(
                    error = %e,
                    task_id = task_id,
                    promise_id = %promise.id,
                    "execution failed, releasing task"
                );
                if let Err(release_err) = self.release_task(task_id, task_version).await {
                    tracing::error!(
                        error = %release_err,
                        task_id = task_id,
                        "failed to release task after error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn execute_until_blocked_inner(
        &self,
        task_id: &str,
        task_version: i64,
        promise: &PromiseRecord,
        preload: Option<Vec<PromiseRecord>>,
    ) -> Result<Status> {
        // 1. Extract function name and args from promise
        let task_data: TaskData = TaskData::deserialize(promise.param.data_as_ref())
            .map_err(|e| Error::DecodingError(format!("invalid task data: {}", e)))?;

        // 2. Look up the function in the registry (hold lock briefly, clone func out)
        let (func, kind) = {
            let reg = self.registry.read();
            let entry = reg
                .get(&task_data.func)
                .ok_or_else(|| Error::FunctionNotFound(task_data.func.clone()))?;
            (entry.func.clone(), entry.kind)
        };

        // 3. SHORT-CIRCUIT: if promise is already settled, fulfill the task
        //    without executing the function.
        if promise.state != PromiseState::Pending {
            tracing::info!(
                task_id = task_id,
                promise_id = %promise.id,
                state = ?promise.state,
                "promise already settled, fulfilling task without execution"
            );
            // Value is already decoded (decode_promise was called before this point)
            let settled_value = promise.value.data_as_ref().clone();
            let state = match promise.state {
                PromiseState::Resolved => SettleState::Resolved,
                PromiseState::Rejected
                | PromiseState::RejectedCanceled
                | PromiseState::RejectedTimedout => SettleState::Rejected,
                PromiseState::Pending => unreachable!(),
            };
            self.fulfill_task(task_id, task_version, &promise.id, state, settled_value)
                .await?;
            return Ok(Status::Done);
        }

        // 4. EXECUTE in a loop, on redirect, re-execute with new preloaded promises
        //    without re-acquiring the task.
        let mut current_preload = preload;
        loop {
            let effects = Effects::new(
                self.sender.clone(),
                self.codec.clone(),
                current_preload.unwrap_or_default(),
            );

            let ctx = Context::root(
                promise.id.clone(),
                promise.timeout_at,
                task_data.func.clone(),
                effects,
                self.target_resolver.clone(),
                self.deps.clone(),
            );

            let info = crate::info::Info::new(
                promise.id.clone(),
                String::new(),
                promise.id.clone(),
                promise.id.clone(),
                promise.timeout_at,
                task_data.func.clone(),
                promise.tags.clone(),
                self.deps.clone(),
            );

            // Execute via the func
            let env = match kind {
                DurableKind::Function => ExecutionEnv::Function(&info),
                DurableKind::Workflow => ExecutionEnv::Workflow(&ctx),
            };
            // Catch panics from user-defined functions. This is necessary because
            // Error::Suspended is a control-flow mechanism (not a real error) and
            // users may accidentally call .unwrap() on it, causing a panic that
            // would silently kill the spawned task and leave it in a zombie state.
            //
            // AssertUnwindSafe is sound here because on panic we skip all
            // post-execution logic (flush, suspend, fulfill) and return an error
            // immediately, so we never observe partially-mutated Context state.
            let result =
                match AssertUnwindSafe((func)(env, task_data.args.clone()))
                    .catch_unwind()
                    .await
                {
                    Ok(result) => result,
                    Err(panic_payload) => {
                        let msg = panic_payload
                            .downcast_ref::<String>()
                            .map(|s| s.as_str())
                            .or_else(|| panic_payload.downcast_ref::<&str>().copied())
                            .unwrap_or("unknown panic");

                        if msg.contains("execution suspended") {
                            tracing::error!(
                                task_id = task_id,
                                "user function panicked by unwrapping Error::Suspended — \
                                 use `?` to propagate suspension errors instead of `.unwrap()`"
                            );
                        } else {
                            tracing::error!(
                                task_id = task_id,
                                panic = msg,
                                "user function panicked"
                            );
                        }

                        return Err(Error::Application {
                            message: format!("user function panicked: {}", msg),
                        });
                    }
                };

            // Flush remaining local work
            let flush_remote = ctx.flush_local_work().await;
            let mut remote_todos = ctx.take_remote_todos().await;
            remote_todos.extend(flush_remote);

            // 5. FINALIZE: determine outcome
            if remote_todos.is_empty() {
                let (state, value) = match &result {
                    Ok(val) => (SettleState::Resolved, val.clone()),
                    Err(err) => (SettleState::Rejected, crate::codec::encode_error(err)),
                };
                self.fulfill_task(task_id, task_version, &promise.id, state, value)
                    .await?;
                tracing::debug!(task_id = task_id, promise_id = %promise.id, "task fulfilled");
                return Ok(Status::Done);
            }

            // 6. SUSPEND: if redirect, loop with new preload; otherwise return Suspended
            tracing::debug!(
                task_id = task_id,
                remote_deps = remote_todos.len(),
                "attempting to suspend task"
            );
            match self
                .suspend_task(task_id, task_version, remote_todos)
                .await?
            {
                SuspendResult::Suspended => {
                    tracing::debug!(task_id = task_id, "task suspended");
                    return Ok(Status::Suspended);
                }
                SuspendResult::Redirect { preload } => {
                    tracing::debug!(
                        task_id = task_id,
                        preload = preload.len(),
                        "suspend returned redirect, re-executing task"
                    );
                    current_preload = Some(preload);
                    continue;
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Task lifecycle helpers
    // ═══════════════════════════════════════════════════════════════

    /// Fulfill a task by settling its promise and completing the task.
    async fn fulfill_task(
        &self,
        task_id: &str,
        task_version: i64,
        promise_id: &str,
        state: SettleState,
        value: serde_json::Value,
    ) -> Result<PromiseRecord> {
        let encoded_value = self.codec.encode(&value)?;

        self.sender
            .task_fulfill(
                task_id,
                task_version,
                crate::types::PromiseSettleReq {
                    id: promise_id.to_string(),
                    state,
                    value: encoded_value,
                },
            )
            .await
    }

    /// Suspend a task by registering callbacks for the unresolved remote
    /// dependencies. Returns `SuspendResult::Redirect` with preloaded promises
    /// if the server indicates some deps are already resolved, allowing the
    /// caller to re-execute without re-acquiring the task.
    async fn suspend_task(
        &self,
        task_id: &str,
        task_version: i64,
        remote_todos: Vec<String>,
    ) -> Result<SuspendResult> {
        let actions = remote_todos
            .into_iter()
            .map(|awaited| crate::types::PromiseRegisterCallbackData {
                awaited,
                awaiter: task_id.to_string(),
            })
            .collect();
        self.sender
            .task_suspend(task_id, task_version, actions)
            .await
    }

    /// Release a task so another worker can pick it up.
    /// Called when execution fails with an error.
    async fn release_task(&self, task_id: &str, version: i64) -> Result<()> {
        self.sender.task_release(task_id, version).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Codec, NoopEncryptor};
    use crate::error::Error;
    use crate::heartbeat::NoopHeartbeat;
    use crate::registry::Registry;
    use crate::test_utils::*;
    use crate::types::{PromiseRecord, PromiseState, Value};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn noop_codec() -> Codec {
        Codec::new(Arc::new(NoopEncryptor))
    }

    /// Build a Core for testing with a no-op match function and no-op heartbeat.
    fn test_core(sender: Sender, codec: Codec, registry: Arc<RwLock<Registry>>) -> Core {
        test_core_with_heartbeat(sender, codec, registry, Arc::new(NoopHeartbeat))
    }

    // ── Test functions ─────────────────────────────────────────────

    #[resonate_macros::function]
    async fn noop() -> Result<()> {
        Ok(())
    }

    #[resonate_macros::function]
    async fn simple() -> Result<i64> {
        Ok(1)
    }

    #[resonate_macros::function]
    async fn fail() -> Result<i64> {
        Err(Error::Application {
            message: "err".to_string(),
        })
    }

    #[resonate_macros::function]
    async fn obj() -> Result<serde_json::Value> {
        Ok(serde_json::json!({"x": 1}))
    }

    #[resonate_macros::function]
    async fn add(x: i64, y: i64) -> Result<i64> {
        Ok(x + y)
    }

    #[resonate_macros::function]
    async fn double(x: i64) -> Result<i64> {
        Ok(x * 2)
    }

    /// Workflow that suspends on a single remote dependency.
    #[resonate_macros::function]
    async fn suspending_once(ctx: &Context) -> Result<i64> {
        let _ = ctx.rpc::<i32>("dep", &()).spawn().await?;
        Ok(0)
    }

    /// Workflow that suspends on two remote dependencies.
    #[resonate_macros::function]
    async fn suspending_multi(ctx: &Context) -> Result<i64> {
        let _ = ctx.rpc::<i32>("dep-a", &()).spawn().await?;
        let _ = ctx.rpc::<i32>("dep-b", &()).spawn().await?;
        Ok(0)
    }

    static COMP_COUNT: AtomicUsize = AtomicUsize::new(0);

    /// Workflow that suspends on first call and completes on second (redirect test).
    #[resonate_macros::function]
    async fn suspending_then_done(ctx: &Context) -> Result<i64> {
        let count = COMP_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        if count == 0 {
            let _ = ctx.rpc::<i32>("dep", &()).spawn().await?;
            Ok(0)
        } else {
            Ok(42)
        }
    }

    #[resonate_macros::function]
    async fn use_preload(ctx: &Context) -> Result<i32> {
        let v: i32 = ctx.rpc("remote", &()).await?;
        Ok(v)
    }

    #[resonate_macros::function]
    async fn remote_dep(ctx: &Context) -> Result<i32> {
        let _ = ctx.rpc::<i32>("dep-a", &()).spawn().await?;
        Ok(0)
    }

    /// Helper: create an encoded promise for a task.
    fn make_root_promise(id: &str, func: &str, args: serde_json::Value) -> PromiseRecord {
        let codec = noop_codec();
        let param_data = serde_json::json!({"func": func, "args": args});
        PromiseRecord {
            id: id.to_string(),
            state: PromiseState::Pending,
            timeout_at: i64::MAX,
            param: codec.encode(&param_data).unwrap(),
            value: Value::default(),
            tags: HashMap::new(),
            created_at: 0,
            settled_at: None,
        }
    }

    // ── Fulfill ────────────────────────────────────────────────────

    #[tokio::test]
    async fn computation_resolves_sends_task_fulfill_with_resolved_value() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "add", serde_json::json!([3, 4]));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(add).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let fulfill = requests.iter().find(|r| r["kind"] == "task.fulfill");
        assert!(fulfill.is_some(), "should have sent task.fulfill");

        let fulfill = fulfill.unwrap();
        {
            assert_eq!(fulfill["action"]["state"], "resolved");
        }
    }

    #[tokio::test]
    async fn computation_rejects_sends_task_fulfill_with_rejected_value() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "fail", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(fail).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let fulfill = requests.iter().find(|r| r["kind"] == "task.fulfill");
        assert!(fulfill.is_some(), "should have sent task.fulfill");

        let fulfill = fulfill.unwrap();
        {
            assert_eq!(fulfill["action"]["state"], "rejected");
        }
    }

    #[tokio::test]
    async fn fulfill_encodes_value_correctly_via_codec() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "obj", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(obj).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let fulfill = requests.iter().find(|r| r["kind"] == "task.fulfill");
        assert!(fulfill.is_some());

        let fulfill = fulfill.unwrap();
        {
            let data_str = fulfill["action"]["value"]["data"].as_str().unwrap();
            assert!(
                Codec::is_valid_base64(data_str),
                "value.data should be valid base64: {}",
                data_str
            );
        }
    }

    // ── Error handling ─────────────────────────────────────────────

    #[tokio::test]
    async fn acquire_failure_returns_error() {
        // Task not found in stub → 404 error
        let harness = TestHarness::new();
        let registry = Registry::new();
        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );

        let result = core.on_message("nonexistent", 0).await;
        assert!(result.is_err(), "should fail when task doesn't exist");
    }

    // ── Suspend ────────────────────────────────────────────────────

    #[tokio::test]
    async fn computation_suspends_sends_task_suspend_with_awaited_ids() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "suspending_multi", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(suspending_multi).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let suspend = requests.iter().find(|r| r["kind"] == "task.suspend");
        assert!(suspend.is_some(), "should have sent task.suspend");

        let suspend = suspend.unwrap();
        let actions = suspend["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 2, "should have 2 awaited IDs");
    }

    #[tokio::test]
    async fn suspend_with_redirect_re_executes_immediately() {
        let harness = TestHarness::new();
        harness.set_suspend_returns_redirect(true).await;

        let root = make_root_promise("p1", "suspending_then_done", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        COMP_COUNT.store(0, AtomicOrdering::SeqCst);

        let mut registry = Registry::new();
        registry.register(suspending_then_done).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        assert_eq!(
            COMP_COUNT.load(AtomicOrdering::SeqCst),
            2,
            "computation should have been called twice (suspend + redirect)"
        );
    }

    static REDIR_NO_ACQUIRE_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[resonate_macros::function]
    async fn redir_no_acquire(ctx: &Context) -> Result<i64> {
        let count = REDIR_NO_ACQUIRE_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        if count == 0 {
            let _ = ctx.rpc::<i32>("dep", &()).spawn().await?;
            Ok(0)
        } else {
            Ok(42)
        }
    }

    #[tokio::test]
    async fn redirect_does_not_send_second_task_acquire() {
        let harness = TestHarness::new();
        harness.set_suspend_returns_redirect(true).await;

        REDIR_NO_ACQUIRE_COUNT.store(0, AtomicOrdering::SeqCst);

        let root = make_root_promise("p1", "redir_no_acquire", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(redir_no_acquire).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let acquire_count = requests
            .iter()
            .filter(|r| r["kind"] == "task.acquire")
            .count();
        assert_eq!(
            acquire_count, 1,
            "redirect should re-execute without sending a second TaskAcquire"
        );
    }

    static REDIR_PRELOAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[resonate_macros::function]
    async fn redir_preload(ctx: &Context) -> Result<i64> {
        let count = REDIR_PRELOAD_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        if count == 0 {
            let _ = ctx.rpc::<i32>("dep", &()).spawn().await?;
            Ok(0)
        } else {
            Ok(42)
        }
    }

    #[tokio::test]
    async fn redirect_preloaded_promises_passed_to_next_execution() {
        let harness = TestHarness::new();
        harness.set_suspend_returns_redirect(true).await;

        REDIR_PRELOAD_COUNT.store(0, AtomicOrdering::SeqCst);

        let root = make_root_promise("p1", "redir_preload", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(redir_preload).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        // After redirect + re-execution, task should complete with fulfill
        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.fulfill"),
            "task should be fulfilled after redirect re-execution"
        );
    }

    static MULTI_REDIR_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[resonate_macros::function]
    async fn multi_redirect(ctx: &Context) -> Result<i64> {
        let count = MULTI_REDIR_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        if count < 2 {
            let _ = ctx.rpc::<i32>("dep", &()).spawn().await?;
        }
        Ok(count as i64)
    }

    #[tokio::test]
    async fn multiple_consecutive_redirects_handled_correctly() {
        let harness = TestHarness::new();
        harness.set_suspend_returns_redirect(true).await;
        // Allow up to 2 redirects
        harness.set_max_redirects(2).await;

        MULTI_REDIR_COUNT.store(0, AtomicOrdering::SeqCst);

        let root = make_root_promise("p1", "multi_redirect", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(multi_redirect).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        core.on_message("task1", 0).await.unwrap();

        assert_eq!(
            MULTI_REDIR_COUNT.load(AtomicOrdering::SeqCst),
            3,
            "should have been called 3 times (initial + 2 redirects)"
        );

        let requests = harness.sent_requests_json().await;
        let acquire_count = requests
            .iter()
            .filter(|r| r["kind"] == "task.acquire")
            .count();
        assert_eq!(
            acquire_count, 1,
            "only one TaskAcquire even with multiple redirects"
        );
    }

    // ── on_message (Path 1): acquires then executes ──────────────

    #[tokio::test]
    async fn on_message_acquires_task_then_executes() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "simple", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(simple).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let status = core.on_message("task1", 0).await.unwrap();
        assert_eq!(status, Status::Done);

        let requests = harness.sent_requests_json().await;

        assert!(
            requests[0]["kind"] == "task.acquire",
            "first request should be TaskAcquire"
        );

        let has_fulfill = requests.iter().any(|r| r["kind"] == "task.fulfill");
        assert!(has_fulfill, "should have sent TaskFulfill");
    }

    #[tokio::test]
    async fn on_message_acquire_failure_returns_error() {
        let harness = TestHarness::new();
        let registry = Registry::new();
        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );

        let result = core.on_message("nonexistent", 0).await;
        assert!(result.is_err(), "should fail when task doesn't exist");
    }

    #[tokio::test]
    async fn on_message_returns_suspended_status() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "suspending_once", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(suspending_once).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let status = core.on_message("task1", 0).await.unwrap();
        assert_eq!(status, Status::Suspended);
    }

    // ── execute_until_blocked (Path 2): already acquired ───────────

    #[tokio::test]
    async fn execute_until_blocked_skips_acquire() {
        let harness = TestHarness::new();
        // NOTE: task is NOT added to the harness — proves Path 2 skips acquire.
        let root = make_root_promise("p1", "add", serde_json::json!([10, 20]));

        let mut registry = Registry::new();
        registry.register(add).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let status = core
            .execute_until_blocked("task-already-acquired", 0, decoded, None)
            .await
            .unwrap();
        assert_eq!(status, Status::Done);

        let requests = harness.sent_requests_json().await;
        assert!(
            !requests.iter().any(|r| r["kind"] == "task.acquire"),
            "execute_until_blocked should NOT send TaskAcquire"
        );
        assert!(
            requests.iter().any(|r| r["kind"] == "task.fulfill"),
            "should have sent TaskFulfill"
        );
    }

    #[tokio::test]
    async fn execute_until_blocked_with_preloaded_promises() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "use_preload", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(use_preload).unwrap();

        // Child promise "p1.0" is preloaded as resolved — rpc won't suspend
        let preloaded = vec![resolved_promise("p1.0", serde_json::json!(99))];

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let status = core
            .execute_until_blocked("task-preloaded", 0, decoded, Some(preloaded))
            .await
            .unwrap();
        assert_eq!(status, Status::Done);
    }

    #[tokio::test]
    async fn execute_until_blocked_suspends_on_remote() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "remote_dep", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(remote_dep).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let status = core
            .execute_until_blocked("task-suspend", 0, decoded, None)
            .await
            .unwrap();
        assert_eq!(status, Status::Suspended);

        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.suspend"),
            "should have sent TaskSuspend"
        );
    }

    #[tokio::test]
    async fn execute_until_blocked_short_circuits_on_settled_promise() {
        let harness = TestHarness::new();
        let codec = noop_codec();
        // Promise is already resolved — func must never be called but task must be fulfilled
        let param_data = serde_json::json!({"func": "noop", "args": null});
        let root = PromiseRecord {
            id: "settled-p".to_string(),
            state: PromiseState::Resolved,
            timeout_at: i64::MAX,
            param: codec.encode(&param_data).unwrap(),
            value: codec.encode(&serde_json::json!(42)).unwrap(),
            tags: HashMap::new(),
            created_at: 0,
            settled_at: Some(1),
        };

        let mut registry = Registry::new();
        registry.register(noop).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = codec.decode_promise(root).unwrap();
        let status = core
            .execute_until_blocked("task-settled", 0, decoded, None)
            .await
            .unwrap();
        assert_eq!(status, Status::Done);

        // Short-circuits before calling the func but still sends TaskFulfill
        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.fulfill"),
            "settled promise should still send TaskFulfill"
        );
    }

    #[tokio::test]
    async fn short_circuit_resolved_promise_sends_fulfill_with_resolved_state() {
        let harness = TestHarness::new();
        let codec = noop_codec();
        let param_data = serde_json::json!({"func": "noop", "args": null});
        let root = PromiseRecord {
            id: "resolved-p".to_string(),
            state: PromiseState::Resolved,
            timeout_at: i64::MAX,
            param: codec.encode(&param_data).unwrap(),
            value: codec.encode(&serde_json::json!(42)).unwrap(),
            tags: HashMap::new(),
            created_at: 0,
            settled_at: Some(1),
        };

        let mut registry = Registry::new();
        registry.register(noop).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = codec.decode_promise(root).unwrap();
        core.execute_until_blocked("task-resolved", 0, decoded, None)
            .await
            .unwrap();

        let requests = harness.sent_requests_json().await;
        let fulfill = requests.iter().find(|r| r["kind"] == "task.fulfill");
        assert!(fulfill.is_some(), "should have sent TaskFulfill");
        let fulfill = fulfill.unwrap();
        {
            assert_eq!(fulfill["action"]["state"], "resolved");
        }
    }

    #[tokio::test]
    async fn short_circuit_rejected_promise_sends_fulfill_with_rejected_state() {
        let harness = TestHarness::new();
        let codec = noop_codec();
        let param_data = serde_json::json!({"func": "noop", "args": null});
        let err_val = serde_json::json!({"__type": "error", "message": "something failed"});
        let root = PromiseRecord {
            id: "rejected-p".to_string(),
            state: PromiseState::Rejected,
            timeout_at: i64::MAX,
            param: codec.encode(&param_data).unwrap(),
            value: codec.encode(&err_val).unwrap(),
            tags: HashMap::new(),
            created_at: 0,
            settled_at: Some(1),
        };

        let mut registry = Registry::new();
        registry.register(noop).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = codec.decode_promise(root).unwrap();
        core.execute_until_blocked("task-rejected", 0, decoded, None)
            .await
            .unwrap();

        let requests = harness.sent_requests_json().await;
        let fulfill = requests.iter().find(|r| r["kind"] == "task.fulfill");
        assert!(fulfill.is_some(), "should have sent TaskFulfill");
        let fulfill = fulfill.unwrap();
        {
            assert_eq!(fulfill["action"]["state"], "rejected");
        }
    }

    // ── Error handling: release on error ───────────────────────────

    #[tokio::test]
    async fn execute_until_blocked_releases_task_on_function_not_found() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "missing_func", serde_json::json!(null));

        let registry = Registry::new(); // empty — function not registered
        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let result = core
            .execute_until_blocked("task-error", 0, decoded, None)
            .await;

        assert!(result.is_err(), "should fail when function not found");

        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.release"),
            "should send TaskRelease when execution errors"
        );
    }

    // ── Both paths produce same fulfill result ─────────────────────

    #[tokio::test]
    async fn both_paths_produce_same_result_for_same_function() {
        // Path 1: on_message (acquires then executes)
        let harness1 = TestHarness::new();
        let root1 = make_root_promise("p1", "double", serde_json::json!(5));
        harness1.add_task("task1", root1.clone(), vec![]).await;

        let mut registry1 = Registry::new();
        registry1.register(double).unwrap();

        let core1 = test_core(
            harness1.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry1)),
        );
        let status1 = core1.on_message("task1", 0).await.unwrap();

        // Path 2: execute_until_blocked (already acquired)
        let harness2 = TestHarness::new();
        let root2 = make_root_promise("p1", "double", serde_json::json!(5));

        let mut registry2 = Registry::new();
        registry2.register(double).unwrap();

        let core2 = test_core(
            harness2.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry2)),
        );
        let decoded = noop_codec().decode_promise(root2).unwrap();
        let status2 = core2
            .execute_until_blocked("task-direct", 0, decoded, None)
            .await
            .unwrap();

        assert_eq!(status1, Status::Done);
        assert_eq!(status2, Status::Done);

        // Path 1: Acquire + Fulfill
        let reqs1 = harness1.sent_requests_json().await;
        assert!(reqs1.iter().any(|r| r["kind"] == "task.acquire"));
        assert!(reqs1.iter().any(|r| r["kind"] == "task.fulfill"));

        // Path 2: Fulfill only (no Acquire)
        let reqs2 = harness2.sent_requests_json().await;
        assert!(!reqs2.iter().any(|r| r["kind"] == "task.acquire"));
        assert!(reqs2.iter().any(|r| r["kind"] == "task.fulfill"));
    }

    // ── Heartbeat tests ───────────────────────────────────────────

    /// A tracking heartbeat that records start/stop calls.
    struct TrackingHeartbeat {
        started: AtomicUsize,
        stopped: AtomicUsize,
    }

    impl TrackingHeartbeat {
        fn new() -> Self {
            Self {
                started: AtomicUsize::new(0),
                stopped: AtomicUsize::new(0),
            }
        }

        fn start_count(&self) -> usize {
            self.started.load(AtomicOrdering::SeqCst)
        }

        fn stop_count(&self) -> usize {
            self.stopped.load(AtomicOrdering::SeqCst)
        }
    }

    impl Heartbeat for TrackingHeartbeat {
        fn start(&self, _task_id: &str, _task_version: i64) {
            self.started.fetch_add(1, AtomicOrdering::SeqCst);
        }
        fn stop(&self, _task_id: &str) {
            self.stopped.fetch_add(1, AtomicOrdering::SeqCst);
        }
        fn shutdown(&self) {}
    }

    fn test_core_with_heartbeat(
        sender: Sender,
        codec: Codec,
        registry: Arc<RwLock<Registry>>,
        heartbeat: Arc<dyn Heartbeat>,
    ) -> Core {
        let target_resolver: TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| target.unwrap_or("default").to_string());
        Core::new(
            sender,
            codec,
            registry,
            target_resolver,
            heartbeat,
            "test-pid".to_string(),
            60_000,
            crate::test_utils::empty_deps(),
        )
    }

    #[tokio::test]
    async fn heartbeat_started_and_stopped_on_successful_execution() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "simple", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(simple).unwrap();

        let hb = Arc::new(TrackingHeartbeat::new());
        let core = test_core_with_heartbeat(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
            hb.clone(),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let status = core
            .execute_until_blocked("task-hb", 0, decoded, None)
            .await
            .unwrap();

        assert_eq!(status, Status::Done);
        assert_eq!(hb.start_count(), 1, "heartbeat should be started once");
        assert_eq!(hb.stop_count(), 1, "heartbeat should be stopped once");
    }

    #[tokio::test]
    async fn heartbeat_stopped_on_error() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "missing_func", serde_json::json!(null));

        let registry = Registry::new(); // empty — function not registered
        let hb = Arc::new(TrackingHeartbeat::new());
        let core = test_core_with_heartbeat(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
            hb.clone(),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let result = core
            .execute_until_blocked("task-hb-err", 0, decoded, None)
            .await;

        assert!(result.is_err(), "should fail when function not found");
        assert_eq!(hb.start_count(), 1, "heartbeat should be started once");
        assert_eq!(
            hb.stop_count(),
            1,
            "heartbeat should be stopped even on error"
        );
    }

    // ── Panic handling (catch_unwind) ─────────────────────────────

    #[resonate_macros::function]
    async fn unwrap_suspend(ctx: &Context) -> Result<i32> {
        // Simulates a user accidentally calling .unwrap() on a suspended rpc
        let handle = ctx.rpc::<i32>("dep", &()).spawn().await?;
        let val: i32 = handle.await.unwrap();
        Ok(val)
    }

    #[tokio::test]
    async fn panic_from_unwrap_suspend_is_caught_and_task_released() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "unwrap_suspend", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(unwrap_suspend).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let result = core
            .execute_until_blocked("task-panic-suspend", 0, decoded, None)
            .await;

        assert!(result.is_err(), "should return an error, not panic");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("panicked"),
            "error should mention panic: {err_msg}"
        );

        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.release"),
            "task should be released after panic"
        );
    }

    #[resonate_macros::function]
    async fn plain_panic(_ctx: &Context) -> Result<i32> {
        panic!("something went wrong");
    }

    #[tokio::test]
    async fn panic_from_user_function_is_caught_and_task_released() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "plain_panic", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(plain_panic).unwrap();

        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let result = core
            .execute_until_blocked("task-panic-plain", 0, decoded, None)
            .await;

        assert!(result.is_err(), "should return an error, not panic");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("something went wrong"),
            "error should contain the panic message: {err_msg}"
        );

        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.release"),
            "task should be released after panic"
        );
    }

    #[tokio::test]
    async fn panic_from_unwrap_suspend_stops_heartbeat() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "unwrap_suspend", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(unwrap_suspend).unwrap();

        let hb = Arc::new(TrackingHeartbeat::new());
        let core = test_core_with_heartbeat(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
            hb.clone(),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let _ = core
            .execute_until_blocked("task-panic-hb", 0, decoded, None)
            .await;

        assert_eq!(hb.start_count(), 1, "heartbeat should be started once");
        assert_eq!(
            hb.stop_count(),
            1,
            "heartbeat should be stopped even after panic"
        );
    }

    #[tokio::test]
    async fn noop_heartbeat_does_not_interfere_in_local_mode() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "add", serde_json::json!([5, 10]));

        let mut registry = Registry::new();
        registry.register(add).unwrap();

        // Use NoopHeartbeat (same as local mode)
        let core = test_core(
            harness.build_sender(),
            noop_codec(),
            Arc::new(RwLock::new(registry)),
        );
        let decoded = noop_codec().decode_promise(root).unwrap();
        let status = core
            .execute_until_blocked("task-noop-hb", 0, decoded, None)
            .await
            .unwrap();

        assert_eq!(status, Status::Done);

        let requests = harness.sent_requests_json().await;
        assert!(
            requests.iter().any(|r| r["kind"] == "task.fulfill"),
            "should complete normally with NoopHeartbeat"
        );
    }
}
