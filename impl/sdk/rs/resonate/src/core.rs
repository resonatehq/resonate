use std::sync::{Arc, RwLock};

use serde::Deserialize;

use crate::codec::Codec;
use crate::context::{Context, MatchFn};
use crate::effects::Effects;
use crate::error::{Error, Result};
use crate::heartbeat::Heartbeat;
use crate::registry::Registry;
use crate::send::{Request, Response, SendFn};
use crate::types::{PromiseRecord, PromiseState, SettleState, Status, TaskData};

/// Internal result of a suspend attempt.
enum SuspendResult {
    Suspended,
    Redirect { preloaded: Vec<PromiseRecord> },
}

/// Core is the top-level component that manages the full lifecycle of a task.
/// It takes a `send` function and uses it for all server communication.
///
/// Core exposes two entry points:
///
/// 1. **`execute_until_blocked(task_id, root_promise, preload)`**
///    Called from `Resonate::begin_run` when the task is *already acquired*.
///    Skips the acquire step. Runs the registered function until it completes
///    (Done → fulfills the task) or suspends (Suspended → suspends the task).
///    On error → releases the task.
///
/// 2. **`on_message(task_id)`**
///    Called when an `execute` message arrives from the network.
///    Acquires the task first, then delegates to `execute_until_blocked`.
pub struct Core {
    send: SendFn,
    codec: Codec,
    registry: Arc<RwLock<Registry>>,
    match_fn: MatchFn,
    heartbeat: Arc<dyn Heartbeat>,
}

impl Core {
    pub fn new(
        send: SendFn,
        codec: Codec,
        registry: Arc<RwLock<Registry>>,
        match_fn: MatchFn,
        heartbeat: Arc<dyn Heartbeat>,
    ) -> Self {
        Self {
            send,
            codec,
            registry,
            match_fn,
            heartbeat,
        }
    }

    /// Clone a reference to this Core for spawning into background tasks.
    pub fn clone_core_ref(&self) -> Arc<Core> {
        Arc::new(Core {
            send: self.send.clone(),
            codec: self.codec.clone(),
            registry: self.registry.clone(),
            match_fn: self.match_fn.clone(),
            heartbeat: self.heartbeat.clone(),
        })
    }

    // ═══════════════════════════════════════════════════════════════
    //  Path 1: on_message — acquires the task, then executes
    // ═══════════════════════════════════════════════════════════════

    /// Called when an `execute` message arrives from the network.
    /// Acquires the task, decodes the root promise, then runs
    /// `execute_until_blocked`.
    pub fn on_message<'a>(
        &'a self,
        task_id: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Status>> {
        Box::pin(self.on_message_inner(task_id))
    }

    async fn on_message_inner(&self, task_id: &str) -> Result<Status> {
        // 1. ACQUIRE the task
        let acquire_response = (self.send)(Request::TaskAcquire {
            task_id: task_id.to_string(),
        })
        .await?;

        let (root_promise_raw, preloaded_raw) = match acquire_response {
            Response::TaskAcquireResult {
                root_promise,
                preloaded,
            } => (root_promise, preloaded),
            _ => {
                return Err(Error::ServerError {
                    code: 500,
                    message: "unexpected response for task.acquire".into(),
                })
            }
        };

        // 2. Decode root promise
        let root_promise = self.codec.decode_promise(&root_promise_raw)?;

        // 3. Delegate to execute_until_blocked
        self.execute_until_blocked(task_id, root_promise, Some(preloaded_raw))
            .await
    }

    // ═══════════════════════════════════════════════════════════════
    //  Path 2: execute_until_blocked — task already acquired
    // ═══════════════════════════════════════════════════════════════

    /// Runs the registered function for the given task + promise until it
    /// completes (Done) or suspends waiting on remote promises (Suspended).
    ///
    /// - On Done → fulfills the task (settles the root promise and completes
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
        root_promise: PromiseRecord,
        preload: Option<Vec<PromiseRecord>>,
    ) -> Result<Status> {
        // Start heartbeat before execution to keep the task lease alive
        if let Err(e) = self.heartbeat.start().await {
            tracing::warn!(error = %e, task_id = task_id, "failed to start heartbeat");
        }

        let result = self
            .execute_until_blocked_inner(task_id, &root_promise, preload)
            .await;

        // Stop heartbeat after execution completes (both success and error)
        if let Err(e) = self.heartbeat.stop().await {
            tracing::warn!(error = %e, task_id = task_id, "failed to stop heartbeat");
        }

        match result {
            Ok(status) => Ok(status),
            Err(e) => {
                // On error → release the task so another worker can retry
                tracing::error!(
                    error = %e,
                    task_id = task_id,
                    promise_id = %root_promise.id,
                    "execution failed, releasing task"
                );
                if let Err(release_err) = self.release_task(task_id).await {
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
        root_promise: &PromiseRecord,
        preload: Option<Vec<PromiseRecord>>,
    ) -> Result<Status> {
        // 1. Extract function name and args from root promise
        let task_data: TaskData = TaskData::deserialize(root_promise.param.data_as_ref())
            .map_err(|e| Error::DecodingError(format!("invalid task data: {}", e)))?;

        // 2. Look up the function in the registry (hold lock briefly, clone factory out)
        let factory = {
            let reg = self.registry.read().unwrap();
            let entry = reg
                .get(&task_data.func)
                .ok_or_else(|| Error::FunctionNotFound(task_data.func.clone()))?;
            entry.factory.clone()
        };

        // 3. SHORT-CIRCUIT: if root promise is already settled, fulfill the task
        //    without executing the function (matching TS SDK behavior).
        if root_promise.state != PromiseState::Pending {
            tracing::info!(
                task_id = task_id,
                promise_id = %root_promise.id,
                state = ?root_promise.state,
                "root promise already settled, fulfilling task without execution"
            );
            // Value is already decoded (decode_promise was called before this point)
            let settled_value = root_promise.value.data_as_ref().clone();
            let result: Result<serde_json::Value> = match root_promise.state {
                PromiseState::Resolved => Ok(settled_value),
                PromiseState::Rejected
                | PromiseState::RejectedCanceled
                | PromiseState::RejectedTimedout => Err(Error::Application {
                    message: settled_value
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("rejected")
                        .to_string(),
                }),
                PromiseState::Pending => unreachable!(),
            };
            self.fulfill_task(task_id, &root_promise.id, &result).await?;
            return Ok(Status::Done);
        }

        // 4. EXECUTE in a loop — on redirect, re-execute with new preloaded promises
        //    without re-acquiring the task (matching TS SDK behavior).
        let mut current_preload = preload;
        loop {
            let effects = Effects::new(
                self.send.clone(),
                self.codec.clone(),
                current_preload.unwrap_or_default(),
            );

            let ctx = Context::root(
                root_promise.id.clone(),
                root_promise.timeout_at,
                task_data.func.clone(),
                effects.clone(),
                self.match_fn.clone(),
            );

            let info = crate::info::Info::new(
                root_promise.id.clone(),
                String::new(),
                root_promise.id.clone(),
                root_promise.id.clone(),
                root_promise.timeout_at,
                task_data.func.clone(),
                root_promise.tags.clone(),
            );

            // Execute via the factory
            let result = (factory)(Some(&ctx), Some(&info), task_data.args.clone()).await;

            // Flush remaining local work
            let flush_remote = ctx.flush_local_work().await;
            let mut remote_todos = ctx.take_remote_todos().await;
            remote_todos.extend(flush_remote);

            // 5. FINALIZE: determine outcome
            if remote_todos.is_empty() {
                self.fulfill_task(task_id, &root_promise.id, &result)
                    .await?;
                return Ok(Status::Done);
            }

            // 6. SUSPEND: if redirect, loop with new preload; otherwise return Suspended
            match self.suspend_task(task_id, remote_todos).await? {
                SuspendResult::Suspended => return Ok(Status::Suspended),
                SuspendResult::Redirect { preloaded } => {
                    tracing::info!(task_id = task_id, "redirect received, re-executing task");
                    current_preload = Some(preloaded);
                    continue;
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Legacy compat: on_execute (acquires then executes)
    //  Used by existing tests. Delegates to on_message.
    // ═══════════════════════════════════════════════════════════════

    /// Legacy entry point that acquires then executes. Kept for backward
    /// compatibility with existing tests.
    pub fn on_execute<'a>(
        &'a self,
        task_id: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.on_message(task_id).await?;
            Ok(())
        })
    }

    // ═══════════════════════════════════════════════════════════════
    //  Task lifecycle helpers
    // ═══════════════════════════════════════════════════════════════

    /// Fulfill a task by settling its root promise and completing the task.
    async fn fulfill_task(
        &self,
        task_id: &str,
        promise_id: &str,
        result: &Result<serde_json::Value>,
    ) -> Result<()> {
        let (state, value_data) = match result {
            Ok(val) => (SettleState::Resolved, val.clone()),
            Err(err) => (SettleState::Rejected, crate::codec::encode_error(err)),
        };

        let encoded_value = self.codec.encode(&value_data)?;

        let req = Request::TaskFulfill {
            task_id: task_id.to_string(),
            settle: crate::types::PromiseSettleReq {
                id: promise_id.to_string(),
                state,
                value: encoded_value,
            },
        };

        (self.send)(req).await?;
        Ok(())
    }

    /// Suspend a task by registering callbacks for the unresolved remote
    /// dependencies. Returns `SuspendResult::Redirect` with preloaded promises
    /// if the server indicates some deps are already resolved, allowing the
    /// caller to re-execute without re-acquiring the task.
    async fn suspend_task(
        &self,
        task_id: &str,
        remote_todos: Vec<String>,
    ) -> Result<SuspendResult> {
        let req = Request::TaskSuspend {
            task_id: task_id.to_string(),
            callbacks: remote_todos,
        };

        let response = (self.send)(req).await?;

        match response {
            Response::Suspended => Ok(SuspendResult::Suspended),
            Response::Redirect { preloaded } => Ok(SuspendResult::Redirect { preloaded }),
            _ => Err(Error::ServerError {
                code: 500,
                message: "unexpected response for task.suspend".into(),
            }),
        }
    }

    /// Release a task so another worker can pick it up.
    /// Called when execution fails with an error.
    async fn release_task(&self, task_id: &str) -> Result<()> {
        let req = Request::TaskRelease {
            task_id: task_id.to_string(),
        };
        (self.send)(req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Codec;
    use crate::error::{Error, Result};
    use crate::heartbeat::NoopHeartbeat;
    use crate::registry::Registry;
    use crate::test_utils::*;
    use crate::types::{PromiseRecord, PromiseState, Value};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::RwLock;

    /// Build a Core for testing with a no-op match function and no-op heartbeat.
    fn test_core(send: SendFn, codec: Codec, registry: Arc<RwLock<Registry>>) -> Core {
        let match_fn: MatchFn = std::sync::Arc::new(|target: &str| target.to_string());
        let heartbeat: Arc<dyn Heartbeat> = Arc::new(NoopHeartbeat);
        Core::new(send, codec, registry, match_fn, heartbeat)
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
        let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep", &()).await;
        Ok(0)
    }

    /// Workflow that suspends on two remote dependencies.
    #[resonate_macros::function]
    async fn suspending_multi(ctx: &Context) -> Result<i64> {
        let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep-a", &()).await;
        let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep-b", &()).await;
        Ok(0)
    }

    static COMP_COUNT: AtomicUsize = AtomicUsize::new(0);

    /// Workflow that suspends on first call and completes on second (redirect test).
    #[resonate_macros::function]
    async fn suspending_then_done(ctx: &Context) -> Result<i64> {
        let count = COMP_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        if count == 0 {
            let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep", &()).await;
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
        let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep-a", &()).await;
        Ok(0)
    }

    /// Helper: create an encoded root promise for a task.
    fn make_root_promise(id: &str, func: &str, args: serde_json::Value) -> PromiseRecord {
        let codec = Codec;
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
        registry.register(Add);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        let requests = harness.sent_requests().await;
        let fulfill = requests.iter().find(|r| matches!(r, Request::TaskFulfill { .. }));
        assert!(fulfill.is_some(), "should have sent task.fulfill");

        if let Request::TaskFulfill { settle, .. } = fulfill.unwrap() {
            assert_eq!(settle.state, crate::types::SettleState::Resolved);
        }
    }

    #[tokio::test]
    async fn computation_rejects_sends_task_fulfill_with_rejected_value() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "fail", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(Fail);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        let requests = harness.sent_requests().await;
        let fulfill = requests.iter().find(|r| matches!(r, Request::TaskFulfill { .. }));
        assert!(fulfill.is_some(), "should have sent task.fulfill");

        if let Request::TaskFulfill { settle, .. } = fulfill.unwrap() {
            assert_eq!(settle.state, crate::types::SettleState::Rejected);
        }
    }

    #[tokio::test]
    async fn fulfill_encodes_value_correctly_via_codec() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "obj", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(Obj);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        let requests = harness.sent_requests().await;
        let fulfill = requests.iter().find(|r| matches!(r, Request::TaskFulfill { .. }));
        assert!(fulfill.is_some());

        if let Request::TaskFulfill { settle, .. } = fulfill.unwrap() {
            let data_val = settle.value.data_or_null();
            let data_str = data_val.as_str().unwrap();
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
        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));

        let result = core.on_execute("nonexistent").await;
        assert!(result.is_err(), "should fail when task doesn't exist");
    }

    // ── Suspend ────────────────────────────────────────────────────

    #[tokio::test]
    async fn computation_suspends_sends_task_suspend_with_awaited_ids() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "suspending_multi", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(SuspendingMulti);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        let requests = harness.sent_requests().await;
        let suspend = requests.iter().find(|r| matches!(r, Request::TaskSuspend { .. }));
        assert!(suspend.is_some(), "should have sent task.suspend");

        if let Request::TaskSuspend { callbacks, .. } = suspend.unwrap() {
            assert_eq!(callbacks.len(), 2, "should have 2 awaited IDs");
        }
    }

    #[tokio::test]
    async fn suspend_with_redirect_re_executes_immediately() {
        let harness = TestHarness::new();
        harness.set_suspend_returns_redirect(true).await;

        let root = make_root_promise("p1", "suspending_then_done", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        COMP_COUNT.store(0, AtomicOrdering::SeqCst);

        let mut registry = Registry::new();
        registry.register(SuspendingThenDone);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

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
            let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep", &()).await;
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
        registry.register(RedirNoAcquire);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        let requests = harness.sent_requests().await;
        let acquire_count = requests
            .iter()
            .filter(|r| matches!(r, Request::TaskAcquire { .. }))
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
            let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep", &()).await;
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
        registry.register(RedirPreload);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        // After redirect + re-execution, task should complete with fulfill
        let requests = harness.sent_requests().await;
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskFulfill { .. })),
            "task should be fulfilled after redirect re-execution"
        );
    }

    static MULTI_REDIR_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[resonate_macros::function]
    async fn multi_redirect(ctx: &Context) -> Result<i64> {
        let count = MULTI_REDIR_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
        if count < 2 {
            let _: crate::futures::RemoteFuture<i32> = ctx.begin_rpc("dep", &()).await;
        }
        Ok(count as i64)
    }

    #[tokio::test]
    async fn multiple_consecutive_redirects_handled_correctly() {
        let harness = TestHarness::new();
        harness.set_suspend_returns_redirect(true).await;
        // Allow up to 2 redirects
        {
            let mut net = harness.network.lock().await;
            net.max_redirects = 2;
        }

        MULTI_REDIR_COUNT.store(0, AtomicOrdering::SeqCst);

        let root = make_root_promise("p1", "multi_redirect", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(MultiRedirect);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        assert_eq!(
            MULTI_REDIR_COUNT.load(AtomicOrdering::SeqCst),
            3,
            "should have been called 3 times (initial + 2 redirects)"
        );

        let requests = harness.sent_requests().await;
        let acquire_count = requests
            .iter()
            .filter(|r| matches!(r, Request::TaskAcquire { .. }))
            .count();
        assert_eq!(acquire_count, 1, "only one TaskAcquire even with multiple redirects");
    }

    // ── on_message (Path 1): acquires then executes ──────────────

    #[tokio::test]
    async fn on_message_acquires_task_then_executes() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "simple", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(Simple);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let status = core.on_message("task1").await.unwrap();
        assert_eq!(status, Status::Done);

        let requests = harness.sent_requests().await;

        assert!(
            matches!(&requests[0], Request::TaskAcquire { .. }),
            "first request should be TaskAcquire"
        );

        let has_fulfill = requests
            .iter()
            .any(|r| matches!(r, Request::TaskFulfill { .. }));
        assert!(has_fulfill, "should have sent TaskFulfill");
    }

    #[tokio::test]
    async fn on_message_acquire_failure_returns_error() {
        let harness = TestHarness::new();
        let registry = Registry::new();
        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));

        let result = core.on_message("nonexistent").await;
        assert!(result.is_err(), "should fail when task doesn't exist");
    }

    #[tokio::test]
    async fn on_message_returns_suspended_status() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "suspending_once", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(SuspendingOnce);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let status = core.on_message("task1").await.unwrap();
        assert_eq!(status, Status::Suspended);
    }

    // ── execute_until_blocked (Path 2): already acquired ───────────

    #[tokio::test]
    async fn execute_until_blocked_skips_acquire() {
        let harness = TestHarness::new();
        // NOTE: task is NOT added to the harness — proves Path 2 skips acquire.
        let root = make_root_promise("p1", "add", serde_json::json!([10, 20]));

        let mut registry = Registry::new();
        registry.register(Add);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = Codec.decode_promise(&root).unwrap();
        let status = core
            .execute_until_blocked("task-already-acquired", decoded, None)
            .await
            .unwrap();
        assert_eq!(status, Status::Done);

        let requests = harness.sent_requests().await;
        assert!(
            !requests.iter().any(|r| matches!(r, Request::TaskAcquire { .. })),
            "execute_until_blocked should NOT send TaskAcquire"
        );
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskFulfill { .. })),
            "should have sent TaskFulfill"
        );
    }

    #[tokio::test]
    async fn execute_until_blocked_with_preloaded_promises() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "use_preload", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(UsePreload);

        // Child promise "p1.0" is preloaded as resolved — rpc won't suspend
        let preloaded = vec![resolved_promise("p1.0", serde_json::json!(99))];

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = Codec.decode_promise(&root).unwrap();
        let status = core
            .execute_until_blocked("task-preloaded", decoded, Some(preloaded))
            .await
            .unwrap();
        assert_eq!(status, Status::Done);
    }

    #[tokio::test]
    async fn execute_until_blocked_suspends_on_remote() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "remote_dep", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(RemoteDep);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = Codec.decode_promise(&root).unwrap();
        let status = core
            .execute_until_blocked("task-suspend", decoded, None)
            .await
            .unwrap();
        assert_eq!(status, Status::Suspended);

        let requests = harness.sent_requests().await;
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskSuspend { .. })),
            "should have sent TaskSuspend"
        );
    }

    #[tokio::test]
    async fn execute_until_blocked_short_circuits_on_settled_promise() {
        let harness = TestHarness::new();
        let codec = Codec;
        // Promise is already resolved — factory must never be called but task must be fulfilled
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
        registry.register(Noop);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = codec.decode_promise(&root).unwrap();
        let status = core
            .execute_until_blocked("task-settled", decoded, None)
            .await
            .unwrap();
        assert_eq!(status, Status::Done);

        // Short-circuits before calling the factory but still sends TaskFulfill
        let requests = harness.sent_requests().await;
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskFulfill { .. })),
            "settled promise should still send TaskFulfill"
        );
    }

    #[tokio::test]
    async fn short_circuit_resolved_promise_sends_fulfill_with_resolved_state() {
        let harness = TestHarness::new();
        let codec = Codec;
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
        registry.register(Noop);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = codec.decode_promise(&root).unwrap();
        core.execute_until_blocked("task-resolved", decoded, None)
            .await
            .unwrap();

        let requests = harness.sent_requests().await;
        let fulfill = requests
            .iter()
            .find(|r| matches!(r, Request::TaskFulfill { .. }));
        assert!(fulfill.is_some(), "should have sent TaskFulfill");
        if let Request::TaskFulfill { settle, .. } = fulfill.unwrap() {
            assert_eq!(settle.state, SettleState::Resolved);
        }
    }

    #[tokio::test]
    async fn short_circuit_rejected_promise_sends_fulfill_with_rejected_state() {
        let harness = TestHarness::new();
        let codec = Codec;
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
        registry.register(Noop);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = codec.decode_promise(&root).unwrap();
        core.execute_until_blocked("task-rejected", decoded, None)
            .await
            .unwrap();

        let requests = harness.sent_requests().await;
        let fulfill = requests
            .iter()
            .find(|r| matches!(r, Request::TaskFulfill { .. }));
        assert!(fulfill.is_some(), "should have sent TaskFulfill");
        if let Request::TaskFulfill { settle, .. } = fulfill.unwrap() {
            assert_eq!(settle.state, SettleState::Rejected);
        }
    }

    // ── Error handling: release on error ───────────────────────────

    #[tokio::test]
    async fn execute_until_blocked_releases_task_on_function_not_found() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "missing_func", serde_json::json!(null));

        let registry = Registry::new(); // empty — function not registered
        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = Codec.decode_promise(&root).unwrap();
        let result = core
            .execute_until_blocked("task-error", decoded, None)
            .await;

        assert!(result.is_err(), "should fail when function not found");

        let requests = harness.sent_requests().await;
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskRelease { .. })),
            "should send TaskRelease when execution errors"
        );
    }

    // ── Legacy on_execute still works ──────────────────────────────

    #[tokio::test]
    async fn on_execute_acquires_task_then_delegates_to_execute() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "simple", serde_json::json!(null));
        harness.add_task("task1", root, vec![]).await;

        let mut registry = Registry::new();
        registry.register(Simple);

        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        core.on_execute("task1").await.unwrap();

        let requests = harness.sent_requests().await;

        assert!(
            matches!(&requests[0], Request::TaskAcquire { .. }),
            "first request should be TaskAcquire"
        );
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskFulfill { .. })),
            "should have sent TaskFulfill"
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
        registry1.register(Double);

        let core1 = test_core(harness1.build_send_fn(), Codec, Arc::new(RwLock::new(registry1)));
        let status1 = core1.on_message("task1").await.unwrap();

        // Path 2: execute_until_blocked (already acquired)
        let harness2 = TestHarness::new();
        let root2 = make_root_promise("p1", "double", serde_json::json!(5));

        let mut registry2 = Registry::new();
        registry2.register(Double);

        let core2 = test_core(harness2.build_send_fn(), Codec, Arc::new(RwLock::new(registry2)));
        let decoded = Codec.decode_promise(&root2).unwrap();
        let status2 = core2
            .execute_until_blocked("task-direct", decoded, None)
            .await
            .unwrap();

        assert_eq!(status1, Status::Done);
        assert_eq!(status2, Status::Done);

        // Path 1: Acquire + Fulfill
        let reqs1 = harness1.sent_requests().await;
        assert!(reqs1.iter().any(|r| matches!(r, Request::TaskAcquire { .. })));
        assert!(reqs1.iter().any(|r| matches!(r, Request::TaskFulfill { .. })));

        // Path 2: Fulfill only (no Acquire)
        let reqs2 = harness2.sent_requests().await;
        assert!(!reqs2.iter().any(|r| matches!(r, Request::TaskAcquire { .. })));
        assert!(reqs2.iter().any(|r| matches!(r, Request::TaskFulfill { .. })));
    }

    // ── Heartbeat tests ───────────────────────────────────────────

    /// A tracking heartbeat that records start/stop calls.
    struct TrackingHeartbeat {
        started: std::sync::atomic::AtomicUsize,
        stopped: std::sync::atomic::AtomicUsize,
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

    #[async_trait::async_trait]
    impl Heartbeat for TrackingHeartbeat {
        async fn start(&self) -> Result<()> {
            self.started.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
        async fn stop(&self) -> Result<()> {
            self.stopped.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    fn test_core_with_heartbeat(
        send: SendFn,
        codec: Codec,
        registry: Arc<RwLock<Registry>>,
        heartbeat: Arc<dyn Heartbeat>,
    ) -> Core {
        let match_fn: MatchFn = std::sync::Arc::new(|target: &str| target.to_string());
        Core::new(send, codec, registry, match_fn, heartbeat)
    }

    #[tokio::test]
    async fn heartbeat_started_and_stopped_on_successful_execution() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "simple", serde_json::json!(null));

        let mut registry = Registry::new();
        registry.register(Simple);

        let hb = Arc::new(TrackingHeartbeat::new());
        let core = test_core_with_heartbeat(
            harness.build_send_fn(),
            Codec,
            Arc::new(RwLock::new(registry)),
            hb.clone(),
        );
        let decoded = Codec.decode_promise(&root).unwrap();
        let status = core
            .execute_until_blocked("task-hb", decoded, None)
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
            harness.build_send_fn(),
            Codec,
            Arc::new(RwLock::new(registry)),
            hb.clone(),
        );
        let decoded = Codec.decode_promise(&root).unwrap();
        let result = core
            .execute_until_blocked("task-hb-err", decoded, None)
            .await;

        assert!(result.is_err(), "should fail when function not found");
        assert_eq!(hb.start_count(), 1, "heartbeat should be started once");
        assert_eq!(hb.stop_count(), 1, "heartbeat should be stopped even on error");
    }

    #[tokio::test]
    async fn noop_heartbeat_does_not_interfere_in_local_mode() {
        let harness = TestHarness::new();
        let root = make_root_promise("p1", "add", serde_json::json!([5, 10]));

        let mut registry = Registry::new();
        registry.register(Add);

        // Use NoopHeartbeat (same as local mode)
        let core = test_core(harness.build_send_fn(), Codec, Arc::new(RwLock::new(registry)));
        let decoded = Codec.decode_promise(&root).unwrap();
        let status = core
            .execute_until_blocked("task-noop-hb", decoded, None)
            .await
            .unwrap();

        assert_eq!(status, Status::Done);

        let requests = harness.sent_requests().await;
        assert!(
            requests.iter().any(|r| matches!(r, Request::TaskFulfill { .. })),
            "should complete normally with NoopHeartbeat"
        );
    }
}
