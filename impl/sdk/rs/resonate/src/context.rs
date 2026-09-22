use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::codec::deserialize_error;
use crate::durable::{Durable, ExecutionEnv};
use crate::effects::Effects;
use crate::error::{Error, Result};
use crate::futures::{DetachedHandle, DurableFuture, RemoteFuture};
use crate::info::Info;
use crate::sequencing::{CreationSequencer, CreationState};
use crate::types::{
    DurableKind, Outcome, PromiseCreateReq, PromiseRecord, PromiseState, TaskData, Value,
};

/// Resolves a logical target name (e.g. a function name like `"payments"`)
/// into a routable address (e.g. a URL or network identifier).
/// If the input is already a URL, it is returned unchanged.
/// Mirrors `network.match()` — passed down from Resonate → Core → Context.
pub type TargetResolver = Arc<dyn Fn(Option<&str>) -> String + Send + Sync>;

/// The boxed future returned by the lazy `*Task` builders' `IntoFuture` impls
/// (and by `block_on_remote`). Boxed because each `.await` body is a distinct
/// `async` block that can't be named on stable Rust.
type TaskFuture<'ctx, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'ctx>>;

/// Shared state between a spawned background task and the context's flush
/// mechanism. The DurableFuture also reads from this via a oneshot channel.
pub(crate) struct SpawnedHandle {
    pub id: String,
    pub handle: tokio::task::JoinHandle<Outcome<()>>,
}

/// Abort the background task if the handle is dropped without being flushed.
///
/// The normal path (`flush_local_work`) awaits every handle to completion, so
/// `abort()` then runs on an already-finished task and is a no-op. The case
/// that matters is an abnormal early return — e.g. a user function that
/// panics after spawning children — where the `Context` is dropped without
/// flushing: dropping the leftover handles cancels the in-flight promise
/// creations instead of detaching them, so no spawned task outlives its
/// execution.
impl Drop for SpawnedHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The primary interface for workflow functions.
/// Provides two core operations: `run` (local execution) and `rpc` (remote execution).
///
/// Both return builder structs (`RunTask`, `RpcTask`) that implement `IntoFuture`,
/// so `.await` works seamlessly for the sequential case. Use the synchronous
/// `.spawn()` to start the work eagerly on a background tokio task and return a
/// handle to await later or `tokio::join!` for cooperative
/// concurrency. The builders are lazy: nothing executes until `.spawn()` or
/// `.await`. Durable promises are created in terminal-op call order (each
/// background creation waits for its predecessor to succeed).
pub struct Context {
    id: String,
    origin_id: String,
    branch_id: String,
    parent_id: String,
    func_name: String,
    timeout_at: i64,
    seq: AtomicU32,
    effects: Arc<Effects>,
    target_resolver: TargetResolver,
    remote_todos: Arc<Mutex<Vec<String>>>,
    spawned_handles: Arc<Mutex<Vec<SpawnedHandle>>>,
    sequencer: CreationSequencer,
    deps: Arc<crate::DependencyMap>,
}

impl Context {
    /// Create a root context for a top-level task execution.
    ///
    /// `origin_id` is the lineage origin: everything before the first `:` of
    /// every id in this workflow, set at the top (`resonate.run`/`rpc`) and
    /// carried through the `resonate:origin` tag unchanged forever — including
    /// into `detached` children, which mint `{origin}:d{16hex}` off it. It is
    /// also the anchor every descendant id extends, so it doubles as the
    /// id-generation root. The caller resolves it from the dispatching
    /// promise's tags, falling back to [`crate::ids::origin_of`] for tag-less
    /// promises — the same derivation the server uses.
    pub(crate) fn root(
        id: String,
        origin_id: String,
        timeout_at: i64,
        func_name: String,
        effects: Effects,
        target_resolver: TargetResolver,
        deps: Arc<crate::DependencyMap>,
    ) -> Self {
        Self {
            origin_id,
            branch_id: id.clone(),
            parent_id: String::new(),
            id,
            func_name,
            timeout_at,
            seq: AtomicU32::new(0),
            effects: Arc::new(effects),
            target_resolver,
            remote_todos: Arc::new(Mutex::new(Vec::new())),
            spawned_handles: Arc::new(Mutex::new(Vec::new())),
            sequencer: CreationSequencer::new(),
            deps,
        }
    }

    /// Create a child context for a nested workflow.
    fn child(&self, id: &str, func_name: &str, timeout_at: i64) -> Context {
        self.child_seed().context(id, func_name, timeout_at)
    }

    /// Create an Info for a child leaf function.
    fn child_info(&self, id: &str, func_name: &str, timeout_at: i64) -> Info {
        self.child_seed().info(id, func_name, timeout_at)
    }

    /// Retrieve a dependency by type. Panics if not found.
    pub fn get_dependency<T: Send + Sync + 'static>(&self) -> Arc<T> {
        self.deps.get::<T>()
    }

    /// Generate the next deterministic child ID. A bare root joins its first
    /// lineage segment with `:`, deeper segments join with `.` — see
    /// [`crate::ids::join_id`].
    fn next_id(&self) -> String {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        crate::ids::join_id(&self.id, &seq.to_string())
    }

    /// Default timeout for child promises (24 hours).
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(86_400);

    /// Calculate timeout for child promises.
    /// Computes `min(now + requested_timeout, parent_timeout)`.
    /// If no explicit timeout is provided, defaults to 24 hours.
    fn child_timeout(&self, requested: Option<Duration>) -> i64 {
        let timeout = requested.unwrap_or(Self::DEFAULT_TIMEOUT);
        let now = now_ms();
        let child_deadline = now.saturating_add(timeout.as_millis() as i64);
        std::cmp::min(child_deadline, self.timeout_at)
    }

    /// Get read-only info for this context.
    pub fn info(&self) -> Info {
        Info::new(
            self.id.clone(),
            self.parent_id.clone(),
            self.origin_id.clone(),
            self.branch_id.clone(),
            self.timeout_at,
            self.func_name.clone(),
            HashMap::new(),
            self.deps.clone(),
        )
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn parent_id(&self) -> &str {
        &self.parent_id
    }

    pub fn origin_id(&self) -> &str {
        &self.origin_id
    }

    pub fn timeout_at(&self) -> i64 {
        self.timeout_at
    }

    pub fn func_name(&self) -> &str {
        &self.func_name
    }

    /// Build the lineage tags shared by every child promise create request.
    fn child_tags(&self, scope: &str, branch: &str) -> HashMap<String, String> {
        self.child_tags_with_parent(scope, branch, &self.id)
    }

    /// [`Context::child_tags`] with an explicit `resonate:parent`. Only
    /// `detached` overrides the parent: its id is minted off the origin rather
    /// than off the spawning context, and the server requires every id to
    /// extend its declared `resonate:parent`.
    fn child_tags_with_parent(
        &self,
        scope: &str,
        branch: &str,
        parent: &str,
    ) -> HashMap<String, String> {
        HashMap::from([
            ("resonate:scope".to_string(), scope.to_string()),
            ("resonate:branch".to_string(), branch.to_string()),
            ("resonate:parent".to_string(), parent.to_string()),
            ("resonate:origin".to_string(), self.origin_id.clone()),
        ])
    }

    /// Build a local create request.
    fn local_create_req(
        &self,
        id: &str,
        args: &impl Serialize,
        timeout: Option<Duration>,
    ) -> Result<PromiseCreateReq> {
        let tags = self.child_tags("local", &self.branch_id);

        Ok(PromiseCreateReq {
            id: id.to_string(),
            timeout_at: self.child_timeout(timeout),
            param: Value::from_serializable(args)?,
            tags,
        })
    }

    /// Build a remote create request.
    ///
    /// If `target_override` is provided, it is resolved through `target_resolver`;
    /// otherwise the group name is used as the default target input.
    fn remote_create_req(
        &self,
        id: &str,
        func_name: &str,
        parent: &str,
        args: &impl Serialize,
        timeout: Option<Duration>,
        target_override: Option<&str>,
    ) -> Result<PromiseCreateReq> {
        let target = (self.target_resolver)(target_override);
        let mut tags = self.child_tags_with_parent("global", id, parent);
        tags.insert("resonate:target".to_string(), target);

        Ok(PromiseCreateReq {
            id: id.to_string(),
            timeout_at: self.child_timeout(timeout),
            param: TaskData::into_value(func_name, args)?,
            tags,
        })
    }

    /// Local execution. Returns a `RunTask` builder that implements `IntoFuture`.
    ///
    /// # Usage patterns
    /// ```ignore
    /// // Sequential (common case)
    /// let result: i32 = ctx.run(my_func, args).await?;
    ///
    /// // With timeout
    /// let result: i32 = ctx.run(my_func, args).timeout(Duration::from_secs(30)).await?;
    ///
    /// // Concurrency via tokio::join!
    /// let (r1, r2) = tokio::join!(ctx.run(F, a), ctx.run(F, b));
    ///
    /// // Background execution via .spawn() — sync, work starts immediately
    /// // (parallel on a multi-thread runtime, concurrent on current_thread)
    /// let handle = ctx.run(MyFunc, args).spawn()?;
    /// let id = handle.id().await?; // promise ID, available once created
    /// let result = handle.await?;
    /// ```
    pub fn run<D, Args, T>(&self, func: D, args: Args) -> RunTask<'_, D, Args, T>
    where
        D: Durable<Args, T>,
        Args: Serialize,
    {
        let child_id = self.next_id();
        RunTask {
            child_id,
            ctx: self,
            func,
            args,
            timeout_override: None,
            _phantom: PhantomData,
        }
    }

    /// Build a latent promise create request.
    ///
    /// Similar to `sleep_create_req` but without `resonate:timer` — the promise
    /// is expected to be resolved externally (webhook, human, another process).
    fn promise_create_req(&self, id: &str, timeout: Option<Duration>) -> PromiseCreateReq {
        PromiseCreateReq {
            id: id.to_string(),
            timeout_at: self.child_timeout(timeout),
            param: Value::default(),
            tags: self.child_tags("global", id),
        }
    }

    /// Build a sleep (timer) create request.
    ///
    /// The timer is the promise's own deadline: `resonate:timer` makes the
    /// server settle it **resolved** (rather than timed out) when the deadline
    /// arrives, which fires the callbacks every sleeper is suspended on.
    ///
    /// The `resonate:target` is what makes that deadline *happen*: the server
    /// only schedules timeouts for promises carrying an address, so a
    /// target-less timer would simply never fire. The flip side is that a
    /// target also spawns a task, dispatched immediately rather than at the
    /// wake; a timer names no function to run, so the worker that receives it
    /// drops it and lets the deadline do the waking (see
    /// `Core::execute_until_blocked`).
    fn sleep_create_req(&self, id: &str, duration: Duration) -> PromiseCreateReq {
        let mut tags = self.child_tags("global", id);
        tags.insert("resonate:timer".to_string(), "true".to_string());
        tags.insert("resonate:target".to_string(), (self.target_resolver)(None));

        PromiseCreateReq {
            id: id.to_string(),
            timeout_at: self.child_timeout(Some(duration)),
            param: Value::default(),
            tags,
        }
    }

    /// Sleep (timer). Returns a `SleepTask` builder that implements `IntoFuture`.
    ///
    /// Creates a durable timer promise that resolves after the given duration.
    /// Behaves like an RPC: on `Pending`, the workflow suspends and the server
    /// resolves the promise when the timer elapses.
    ///
    /// # Usage patterns
    /// ```ignore
    /// // Sequential (common case)
    /// ctx.sleep(Duration::from_secs(60)).await?;
    ///
    /// // Fire-and-forget via .spawn()
    /// let handle = ctx.sleep(Duration::from_secs(60)).spawn()?;
    /// ```
    pub fn sleep(&self, duration: Duration) -> SleepTask<'_> {
        let child_id = self.next_id();
        let req = self.sleep_create_req(&child_id, duration);
        SleepTask {
            child_id,
            ctx: self,
            req,
        }
    }

    /// Latent durable promise. Returns a `PromiseTask` builder that implements `IntoFuture`.
    ///
    /// Creates a promise on the Resonate server with no function backing it.
    /// The promise is expected to be resolved externally (webhook, human approval,
    /// cross-process signal, etc.).
    ///
    /// # Usage patterns
    /// ```ignore
    /// // Sequential — wait for external resolution
    /// let result: String = ctx.promise().await?;
    ///
    /// // With timeout
    /// let result: String = ctx.promise().timeout(Duration::from_secs(300)).await?;
    ///
    /// // Create and get handle (to await later); the ID — available once the
    /// // promise exists on the server — can be handed to an external system
    /// let handle: RemoteFuture<String> = ctx.promise::<String>().create()?;
    /// let id = handle.id().await?;
    /// let result = handle.await?;
    /// ```
    pub fn promise<T>(&self) -> PromiseTask<'_, T> {
        let child_id = self.next_id();
        let req = self.promise_create_req(&child_id, None);
        PromiseTask {
            child_id,
            ctx: self,
            req,
            _phantom: PhantomData,
        }
    }

    /// Remote execution. Returns an `RpcTask` builder that implements `IntoFuture`.
    ///
    /// # Usage patterns
    /// ```ignore
    /// // Sequential (common case)
    /// let result: i32 = ctx.rpc("func", &args).await?;
    ///
    /// // With timeout and/or target override
    /// let result: i32 = ctx.rpc("func", &args)
    ///     .timeout(Duration::from_secs(30))
    ///     .target("custom-worker")
    ///     .await?;
    ///
    /// // Fire-and-start via .spawn()
    /// let handle = ctx.rpc::<i32>("func", &args).spawn()?;
    /// ```
    pub fn rpc<T>(&self, func: &str, args: impl Serialize) -> RpcTask<'_, T> {
        let child_id = self.next_id();
        let (req, serialization_error) =
            match self.remote_create_req(&child_id, func, &self.id, &args, None, None) {
                Ok(req) => (req, None),
                Err(e) => (
                    PromiseCreateReq::default_with_id(&child_id),
                    Some(e.to_string()),
                ),
            };
        RpcTask {
            child_id,
            ctx: self,
            req,
            serialization_error,
            _phantom: PhantomData,
        }
    }

    /// Detached fire-and-forget remote execution. Returns a `DetachedTask`
    /// builder whose terminal `.spawn()` returns a `DetachedHandle`.
    ///
    /// Detached calls are **not** part of structured concurrency:
    /// - They are never added to `remote_todos`, so the parent workflow does
    ///   not suspend or wait for them to complete.
    /// - The handle is not a future: it exposes only `id()`, which yields the
    ///   promise ID once the promise exists on the server. A detached call's
    ///   result is never delivered back to the parent.
    ///
    /// The promise ID is computed deterministically as `{origin_id}:d{16-hex
    /// hash of the next child id}` — minted off the lineage origin (fixed at
    /// the top and propagated unchanged forever) so recursion stays bounded at
    /// one segment past the origin instead of growing a segment per level. The
    /// `d` marks the segment as a detached child (vs a normal child's numeric
    /// `{seq}`). Hashing (stable `seahash` over deterministic inputs) keeps
    /// the id length bounded while preserving replay safety.
    ///
    /// The child keeps the *parent's* origin rather than re-rooting onto its
    /// own id: the server derives the origin as everything before the first
    /// `:` and requires every id in a lineage to start with `{origin}:`. Its
    /// declared `resonate:parent` is the origin too — the id hangs off the
    /// origin, not off this context — while `resonate:branch` stays the
    /// child's own id: a detached child roots its own branch.
    ///
    /// # Usage
    /// ```ignore
    /// // Common case: fire and forget — the promise is still created.
    /// ctx.detached("audit_log", &payload).spawn()?;
    ///
    /// // Or hold the handle to obtain the ID for an external system.
    /// let id = ctx.detached("audit_log", &payload).spawn()?.id().await?;
    /// ```
    pub fn detached(&self, func: &str, args: impl Serialize) -> DetachedTask<'_> {
        let raw = self.next_id();
        let child_id = crate::ids::join_id(&self.origin_id, &format!("d{}", hash_id(&raw)));
        let (req, serialization_error) =
            match self.remote_create_req(&child_id, func, &self.origin_id, &args, None, None) {
                Ok(req) => (req, None),
                Err(e) => (
                    PromiseCreateReq::default_with_id(&child_id),
                    Some(e.to_string()),
                ),
            };
        DetachedTask {
            child_id,
            ctx: self,
            req,
            serialization_error,
        }
    }

    /// Take all accumulated remote todos.
    pub(crate) fn take_remote_todos(&self) -> Vec<String> {
        let mut todos = self.remote_todos.lock();
        std::mem::take(&mut *todos)
    }

    // ── Shared helpers for task state-machine logic ─────────────────

    /// Check a deferred serialization error, returning `Err` if present.
    fn check_serialization_error(err: &Option<String>) -> Result<()> {
        if let Some(ref e) = err {
            return Err(Error::EncodingError(format!(
                "failed to serialize args: {}",
                e
            )));
        }
        Ok(())
    }

    /// Flush all eagerly spawned local tasks.
    /// Awaits every spawned task's JoinHandle (no early return, so no task is
    /// orphaned), collects remote_todos from any that suspended.
    ///
    /// Any handle not flushed through here (e.g. because the user function
    /// panicked before flush ran) is aborted when its `SpawnedHandle` drops —
    /// see the `Drop` impl on `SpawnedHandle`.
    ///
    /// Returns `Err` if any task failed with an infrastructure error (e.g. a
    /// promise creation that failed in the background) — this is what makes a
    /// fire-and-forget creation failure fail the whole task execution, so the
    /// task is released and retried instead of settling with lost work.
    pub(crate) async fn flush_local_work(&self) -> Result<Vec<String>> {
        let handles = {
            let mut handles = self.spawned_handles.lock();
            std::mem::take(&mut *handles)
        };

        let mut remote_todos = Vec::new();
        let mut first_err: Option<Error> = None;

        for mut task in handles {
            // Await by mutable borrow so the `SpawnedHandle`'s `Drop` impl can
            // still run — moving `handle` out would be E0509. `JoinHandle` is
            // `Unpin`, so `&mut` awaits directly without any `Pin` wrapper.
            match (&mut task.handle).await {
                Ok(Outcome::Done(Ok(_))) => {
                    // Already settled inside the spawned task
                }
                Ok(Outcome::Done(Err(e))) => {
                    tracing::error!(task_id = %task.id, error = %e, "spawned task failed");
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Ok(Outcome::Suspended {
                    remote_todos: child_remote,
                }) => {
                    remote_todos.extend(child_remote);
                }
                Err(e) => {
                    tracing::error!(task_id = %task.id, error = %e, "spawned task panicked");
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(remote_todos),
        }
    }

    /// Collect every remote todo the task is now waiting on: the context's own
    /// plus those surfaced by flushing spawned children. Flushing the children
    /// (awaiting their local work) is how their suspensions become visible here.
    /// The context's own todos are drained even when the flush fails (the error
    /// wins and the todos are dropped), so a retry starts clean.
    pub(crate) async fn collect_remote_todos(&self) -> Result<Vec<String>> {
        let spawned_todos = self.flush_local_work().await;
        let mut todos = self.take_remote_todos();
        todos.extend(spawned_todos?);
        Ok(todos)
    }

    /// Capture the owned pieces a background task needs to build child
    /// `Context`/`Info` values after `create_promise` returns the
    /// authoritative `timeout_at` (`Context::child` borrows `&self`, which
    /// isn't `'static`).
    fn child_seed(&self) -> ChildSeed {
        ChildSeed {
            parent_id: self.id.clone(),
            origin_id: self.origin_id.clone(),
            effects: Arc::clone(&self.effects),
            target_resolver: self.target_resolver.clone(),
            deps: Arc::clone(&self.deps),
        }
    }
}

/// Owned snapshot of a parent context, movable into a `tokio::spawn`'d task.
struct ChildSeed {
    parent_id: String,
    origin_id: String,
    effects: Arc<Effects>,
    target_resolver: TargetResolver,
    deps: Arc<crate::DependencyMap>,
}

impl ChildSeed {
    /// Build a child `Context` (the single child-context constructor —
    /// `Context::child` delegates here).
    fn context(&self, id: &str, func_name: &str, timeout_at: i64) -> Context {
        Context {
            id: id.to_string(),
            origin_id: self.origin_id.clone(),
            branch_id: id.to_string(),
            parent_id: self.parent_id.clone(),
            func_name: func_name.to_string(),
            timeout_at,
            seq: AtomicU32::new(0),
            effects: Arc::clone(&self.effects),
            target_resolver: self.target_resolver.clone(),
            remote_todos: Arc::new(Mutex::new(Vec::new())),
            spawned_handles: Arc::new(Mutex::new(Vec::new())),
            sequencer: CreationSequencer::new(),
            deps: Arc::clone(&self.deps),
        }
    }

    /// Build a child `Info` (the single child-info constructor —
    /// `Context::child_info` delegates here).
    fn info(&self, id: &str, func_name: &str, timeout_at: i64) -> Info {
        Info::new(
            id.to_string(),
            self.parent_id.clone(),
            self.origin_id.clone(),
            id.to_string(),
            timeout_at,
            func_name.to_string(),
            HashMap::new(),
            self.deps.clone(),
        )
    }
}

/// Duplicate an error for delivery to both a handle's oneshot and flush.
/// `Error` is not `Clone` (it wraps `reqwest::Error` etc.), so the copy
/// degrades to `Error::Application` with the original message — fine for the
/// handle side (the original goes to flush).
fn duplicate_error(e: &Error) -> Error {
    Error::Application {
        message: e.to_string(),
    }
}

impl PromiseRecord {
    /// Map an already-settled record into `Result<T>` (for `IntoFuture` paths).
    /// Returns `None` for `Pending` state — the caller must handle suspension.
    fn as_result<T: DeserializeOwned>(&self) -> Option<Result<T>> {
        match self.state {
            PromiseState::Resolved => Some(self.value.decode::<T>()),
            PromiseState::Rejected
            | PromiseState::RejectedCanceled
            | PromiseState::RejectedTimedout => {
                Some(Err(deserialize_error(self.value.data_or_null())))
            }
            PromiseState::Pending => None,
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  RunTask — builder returned by ctx.run()
// ═══════════════════════════════════════════════════════════════

/// A lazy local execution task. Created by `ctx.run()`.
///
/// Implements `IntoFuture` so `.await` works directly. Nothing happens until
/// a terminal op runs: `.spawn()` starts the work on a background tokio task
/// and returns a `DurableFuture` handle; `.await` creates the promise and
/// executes inline.
pub struct RunTask<'ctx, D, Args, T> {
    child_id: String,
    ctx: &'ctx Context,
    func: D,
    args: Args,
    /// Optional timeout override set via `.timeout()`. `None` uses the parent's default.
    timeout_override: Option<Duration>,
    _phantom: PhantomData<fn() -> T>,
}

impl<'ctx, D, Args, T> RunTask<'ctx, D, Args, T>
where
    Args: Serialize,
{
    /// Set an explicit timeout for the child promise (capped to parent's timeout).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout_override = Some(timeout);
        self
    }

    /// Spawn the task onto a new tokio task so it runs in the background.
    ///
    /// On a multi-thread runtime the spawned task can run in parallel on a
    /// separate worker; on a `current_thread` runtime it runs concurrently,
    /// interleaved with other tasks on the single thread.
    ///
    /// Synchronous: returns a `DurableFuture` handle immediately while promise
    /// creation and execution proceed on a background task. `Err` only for
    /// argument serialization failures; creation/execution errors surface when
    /// the handle is awaited (and at flush for fire-and-forget handles).
    /// Requires `D: Send + 'static` because the function is moved into the
    /// spawned task.
    pub fn spawn(self) -> Result<DurableFuture<T>>
    where
        D: Durable<Args, T> + Send + 'static,
        Args: Serialize + DeserializeOwned + Send + 'static,
        T: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let RunTask {
            child_id,
            ctx,
            func,
            args,
            timeout_override,
            ..
        } = self;

        let req = ctx.local_create_req(&child_id, &args, timeout_override)?;
        let effects = Arc::clone(&ctx.effects);
        let seed = ctx.child_seed();
        let task_id = child_id.clone();

        tracing::info!(
            target: "resonate::validation",
            promise_id = %child_id,
            "promise_execution_spawn"
        );
        let (rx, created_rx) = spawn_sequenced(ctx, child_id.clone(), req, move |record, tx| {
            async move {
                // Replay short-circuit: an already-settled promise skips execution.
                match record.state {
                    PromiseState::Resolved => {
                        return match record.value.into_decoded::<T>() {
                            Ok(val) => {
                                let _ = tx.send(Ok(val));
                                Outcome::Done(Ok(()))
                            }
                            Err(e) => {
                                let _ = tx.send(Err(duplicate_error(&e)));
                                Outcome::Done(Err(e))
                            }
                        };
                    }
                    PromiseState::Rejected
                    | PromiseState::RejectedCanceled
                    | PromiseState::RejectedTimedout => {
                        let _ = tx.send(Err(deserialize_error(record.value.into_data_or_null())));
                        return Outcome::Done(Ok(()));
                    }
                    PromiseState::Pending => {}
                }

                let info = seed.info(&task_id, D::NAME, record.timeout_at);
                let child_ctx = seed.context(&task_id, D::NAME, record.timeout_at);

                let env = match D::KIND {
                    DurableKind::Function => ExecutionEnv::Function(&info),
                    DurableKind::Workflow => ExecutionEnv::Workflow(&child_ctx),
                };
                let result = func.execute(env, args).await;

                // Collect remote todos (workflows only)
                let mut child_remote = Vec::new();
                if D::KIND == DurableKind::Workflow {
                    match child_ctx.collect_remote_todos().await {
                        Ok(todos) => child_remote = todos,
                        Err(e) => {
                            // A grandchild's background creation failed — do NOT
                            // settle this child's promise; fail the whole task.
                            let _ = tx.send(Err(duplicate_error(&e)));
                            return Outcome::Done(Err(e));
                        }
                    }
                }

                // Explicit suspension handling: if the workflow suspended
                // (e.g. a pending ctx.rpc().await), handle it directly
                // instead of letting it fall through as an application error.
                if matches!(&result, Err(Error::Suspended)) {
                    debug_assert!(
                        !child_remote.is_empty(),
                        "Suspended error but no remote todos — this is a bug"
                    );
                    let _ = tx.send(Err(Error::Suspended));
                    return Outcome::Suspended {
                        remote_todos: child_remote,
                    };
                }

                // Spawned sub-workflows may have remote todos even if the
                // main function completed successfully.
                if child_remote.is_empty() {
                    let _ = effects.settle_promise(&task_id, &result).await;
                    let _ = tx.send(result);
                    Outcome::Done(Ok(()))
                } else {
                    let _ = tx.send(Err(Error::Suspended));
                    Outcome::Suspended {
                        remote_todos: child_remote,
                    }
                }
            }
        });

        Ok(DurableFuture::pending(child_id, rx, created_rx))
    }
}

impl<'ctx, D, Args, T> IntoFuture for RunTask<'ctx, D, Args, T>
where
    D: Durable<Args, T>,
    Args: Serialize + DeserializeOwned + Send + 'static,
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Output = Result<T>;
    type IntoFuture = TaskFuture<'ctx, T>;

    fn into_future(self) -> Self::IntoFuture {
        let RunTask {
            child_id,
            ctx,
            func,
            args,
            timeout_override,
            ..
        } = self;

        let req = match ctx.local_create_req(&child_id, &args, timeout_override) {
            Ok(req) => req,
            Err(e) => return Box::pin(async move { Err(e) }),
        };
        let slot = ctx.sequencer.claim_slot();

        Box::pin(async move {
            let promise_record = slot.create(&ctx.effects, req).await?;

            if let Some(result) = promise_record.as_result::<T>() {
                return result;
            }

            // Pending, execute locally
            tracing::info!(
                target: "resonate::validation",
                promise_id = %child_id,
                "promise_execution_spawn"
            );
            let timeout_at = promise_record.timeout_at;

            let info;
            let child_ctx;
            let env = match D::KIND {
                DurableKind::Function => {
                    info = ctx.child_info(&child_id, D::NAME, timeout_at);
                    ExecutionEnv::Function(&info)
                }
                DurableKind::Workflow => {
                    child_ctx = ctx.child(&child_id, D::NAME, timeout_at);
                    ExecutionEnv::Workflow(&child_ctx)
                }
            };
            tracing::info!(
                target: "resonate::validation",
                promise_id = %child_id,
                "promise_execution_await"
            );
            let result = func.execute(env, args).await;
            let child_remotes = env.collect_remote_todos().await?;

            // FINALIZE. A child suspends iff it left unresolved remote
            // dependencies — independent of `result`: it can return `Ok` while
            // still having spawned remote work it never awaited, and that work
            // is what it now waits on. So `child_remote`, not `result`, decides.
            // Mirrors `Core::execute_until_blocked_inner`, except a child hoists
            // its todos to the parent and still hands `result` back to the
            // caller (the root executor returns a `Status` instead).
            if child_remotes.is_empty() {
                debug_assert!(!matches!(&result, Err(Error::Suspended)));
                ctx.effects.settle_promise(&child_id, &result).await?;
            } else {
                ctx.remote_todos.lock().extend(child_remotes);
            }
            result
        })
    }
}

// ═══════════════════════════════════════════════════════════════
//  RpcTask — builder returned by ctx.rpc()
// ═══════════════════════════════════════════════════════════════

/// A lazy remote execution task. Created by `ctx.rpc()`.
///
/// Implements `IntoFuture` so `.await` works directly. On `Pending` state,
/// awaiting pushes to `remote_todos` and returns `Err(Suspended)`. Nothing
/// happens until a terminal op runs: `.spawn()` starts promise creation on a
/// background task and returns a `RemoteFuture` handle.
pub struct RpcTask<'ctx, T> {
    child_id: String,
    ctx: &'ctx Context,
    req: PromiseCreateReq,
    /// Serialization error deferred from construction (if args failed to serialize).
    serialization_error: Option<String>,
    _phantom: PhantomData<T>,
}

impl<'ctx, T> RpcTask<'ctx, T> {
    /// Set an explicit timeout for the child promise (capped to parent's timeout).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.req.timeout_at = self.ctx.child_timeout(Some(timeout));
        self
    }

    /// Override the target for this RPC call (resolved through `target_resolver`).
    pub fn target(mut self, target: &str) -> Self {
        let resolved = (self.ctx.target_resolver)(Some(target));
        self.req
            .tags
            .insert("resonate:target".to_string(), resolved);
        self
    }

    /// Start the RPC and return a `RemoteFuture` handle.
    ///
    /// Synchronous: promise creation happens on a background task. `Err` only
    /// for argument serialization failures; creation errors surface when the
    /// handle is awaited (and at flush for fire-and-forget handles).
    pub fn spawn(self) -> Result<RemoteFuture<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        Context::check_serialization_error(&self.serialization_error)?;
        let RpcTask {
            child_id, ctx, req, ..
        } = self;
        Ok(spawn_remote(ctx, child_id, req))
    }
}

impl<'ctx, T> IntoFuture for RpcTask<'ctx, T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Output = Result<T>;
    type IntoFuture = TaskFuture<'ctx, T>;

    fn into_future(self) -> Self::IntoFuture {
        let RpcTask {
            child_id,
            ctx,
            req,
            serialization_error,
            ..
        } = self;
        if let Err(e) = Context::check_serialization_error(&serialization_error) {
            return Box::pin(async move { Err(e) });
        }
        block_on_remote(ctx, child_id, req)
    }
}

/// Shared scaffold for the spawn-like terminal ops: claim the creation slot
/// synchronously (so creation order matches terminal-op call order), then run
/// the sequenced creation on a background tokio task tracked in
/// `spawned_handles` (so flush always joins it). The promise record is handed
/// to `on_created`; a creation failure is delivered to both the handle's
/// channel and — via the task's `Outcome` — to flush.
fn spawn_sequenced<T, F, Fut>(
    ctx: &Context,
    child_id: String,
    req: PromiseCreateReq,
    on_created: F,
) -> (
    tokio::sync::oneshot::Receiver<Result<T>>,
    tokio::sync::watch::Receiver<CreationState>,
)
where
    T: Send + 'static,
    F: FnOnce(PromiseRecord, tokio::sync::oneshot::Sender<Result<T>>) -> Fut + Send + 'static,
    Fut: Future<Output = Outcome<()>> + Send,
{
    let slot = ctx.sequencer.claim_slot();
    let created_rx = slot.subscribe();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let effects = Arc::clone(&ctx.effects);

    let handle = tokio::spawn(async move {
        match slot.create(&effects, req).await {
            Ok(record) => on_created(record, tx).await,
            Err(e) => {
                let _ = tx.send(Err(duplicate_error(&e)));
                Outcome::Done(Err(e))
            }
        }
    });

    // Track for flush
    ctx.spawned_handles.lock().push(SpawnedHandle {
        id: child_id,
        handle,
    });

    (rx, created_rx)
}

/// Shared `spawn()` body for `rpc`, `sleep`, and `promise`: start the sequenced
/// promise creation on a background task and return a `RemoteFuture` handle.
///
/// The background task short-circuits an already-settled record (replay),
/// otherwise reports the pending child as a remote todo via its `Outcome` —
/// collected by `collect_remote_todos`, which every caller runs at flush, so a
/// fire-and-forget spawn is never lost.
fn spawn_remote<T>(ctx: &Context, child_id: String, req: PromiseCreateReq) -> RemoteFuture<T>
where
    T: DeserializeOwned + Send + 'static,
{
    let task_id = child_id.clone();
    let (rx, created_rx) = spawn_sequenced(ctx, child_id.clone(), req, move |record, tx| {
        async move {
            match record.as_result::<T>() {
                // Replay short-circuit: already settled, deliver the value/error.
                Some(result) => {
                    let _ = tx.send(result);
                    Outcome::Done(Ok(()))
                }
                // Pending: suspend the awaiter, report the remote todo via flush.
                None => {
                    tracing::info!(
                        target: "resonate::validation",
                        promise_id = %task_id,
                        "promise_execution_block"
                    );
                    let _ = tx.send(Err(Error::Suspended));
                    Outcome::Suspended {
                        remote_todos: vec![task_id],
                    }
                }
            }
        }
    });

    RemoteFuture::pending(child_id, rx, created_rx)
}

/// Shared `.await` body for `rpc`, `sleep`, and `promise`: sequenced creation,
/// replay short-circuit, otherwise register the remote todo and suspend. The
/// creation slot is claimed synchronously (before the future is returned), so
/// creation order matches the order in which ops were awaited/spawned.
fn block_on_remote<'ctx, T>(
    ctx: &'ctx Context,
    child_id: String,
    req: PromiseCreateReq,
) -> TaskFuture<'ctx, T>
where
    T: DeserializeOwned + Send + 'ctx,
{
    let slot = ctx.sequencer.claim_slot();
    Box::pin(async move {
        let record = slot.create(&ctx.effects, req).await?;

        if let Some(result) = record.as_result::<T>() {
            return result;
        }

        // Pending
        tracing::info!(
            target: "resonate::validation",
            promise_id = %child_id,
            "promise_execution_block"
        );
        ctx.remote_todos.lock().push(child_id);
        Err(Error::Suspended)
    })
}

// ═══════════════════════════════════════════════════════════════
//  PromiseTask — builder returned by ctx.promise()
// ═══════════════════════════════════════════════════════════════

/// A lazy latent promise task. Created by `ctx.promise()`.
///
/// Creates a durable promise with no function backing it — resolved externally
/// (webhook, human approval, cross-process signal, etc.).
///
/// Implements `IntoFuture` so `.await` works directly. On `Pending` state,
/// awaiting pushes to `remote_todos` and returns `Err(Suspended)`, just like
/// RPC. `.create()` starts promise creation on a background task and returns a
/// `RemoteFuture` handle; use `handle.id()` to obtain the promise ID once the
/// promise exists on the server.
pub struct PromiseTask<'ctx, T> {
    child_id: String,
    ctx: &'ctx Context,
    req: PromiseCreateReq,
    _phantom: PhantomData<T>,
}

impl<'ctx, T> PromiseTask<'ctx, T> {
    /// Set an explicit timeout for the promise (capped to parent's timeout).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.req.timeout_at = self.ctx.child_timeout(Some(timeout));
        self
    }

    /// Attach data to the promise param (visible to external resolvers).
    pub fn data(mut self, data: &impl Serialize) -> Result<Self> {
        self.req.param = Value::from_serializable(data)?;
        Ok(self)
    }

    /// Start promise creation and return a `RemoteFuture` handle.
    ///
    /// Synchronous: creation happens on a background task. Use `handle.id()`
    /// to get the promise ID once creation has succeeded (hand it to the
    /// external resolver), then await the handle for the resolution.
    pub fn create(self) -> Result<RemoteFuture<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let PromiseTask {
            child_id, ctx, req, ..
        } = self;
        Ok(spawn_remote(ctx, child_id, req))
    }
}

impl<'ctx, T> IntoFuture for PromiseTask<'ctx, T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Output = Result<T>;
    type IntoFuture = TaskFuture<'ctx, T>;

    fn into_future(self) -> Self::IntoFuture {
        let PromiseTask {
            child_id, ctx, req, ..
        } = self;
        block_on_remote(ctx, child_id, req)
    }
}

// ═══════════════════════════════════════════════════════════════
//  SleepTask — builder returned by ctx.sleep()
// ═══════════════════════════════════════════════════════════════

/// A lazy timer task. Created by `ctx.sleep()`.
///
/// Implements `IntoFuture` so `.await` works directly. On `Pending` state,
/// awaiting pushes to `remote_todos` and returns `Err(Suspended)`, just like
/// RPC. `.spawn()` starts the timer's promise creation on a background task
/// and returns a `RemoteFuture` handle.
pub struct SleepTask<'ctx> {
    child_id: String,
    ctx: &'ctx Context,
    req: PromiseCreateReq,
}

impl<'ctx> SleepTask<'ctx> {
    /// Start the timer and return a `RemoteFuture` handle.
    ///
    /// Synchronous: promise creation happens on a background task. `Result`
    /// is kept for API consistency with the other spawn-like methods; this
    /// method has no synchronous failure mode today.
    pub fn spawn(self) -> Result<RemoteFuture<()>> {
        let SleepTask {
            child_id, ctx, req, ..
        } = self;
        Ok(spawn_remote(ctx, child_id, req))
    }
}

impl<'ctx> IntoFuture for SleepTask<'ctx> {
    type Output = Result<()>;
    type IntoFuture = TaskFuture<'ctx, ()>;

    fn into_future(self) -> Self::IntoFuture {
        let SleepTask {
            child_id, ctx, req, ..
        } = self;
        block_on_remote(ctx, child_id, req)
    }
}

use crate::now_ms;

// ═══════════════════════════════════════════════════════════════
//  DetachedTask — builder returned by ctx.detached()
// ═══════════════════════════════════════════════════════════════

/// Compute a 64-bit `seahash` of `s` and return it as a 16-character
/// lowercase hex string. Stable across processes and Rust versions.
fn hash_id(s: &str) -> String {
    use std::hash::Hasher;
    let mut h = seahash::SeaHasher::new();
    h.write(s.as_bytes());
    format!("{:016x}", h.finish())
}

/// A fire-and-forget remote execution builder. Created by `ctx.detached()`.
///
/// Unlike `RpcTask`, this does **not** implement `IntoFuture` — there is no
/// awaitable result. `.spawn()` returns a `DetachedHandle` (also not a
/// future); its only operation is `id()`, which yields the promise ID once
/// the promise exists on the server. Detached promises are not registered in
/// `remote_todos`, so the parent workflow does not suspend on them.
pub struct DetachedTask<'ctx> {
    child_id: String,
    ctx: &'ctx Context,
    req: PromiseCreateReq,
    /// Serialization error deferred from construction (if args failed to serialize).
    serialization_error: Option<String>,
}

impl<'ctx> DetachedTask<'ctx> {
    /// Set an explicit timeout for the detached promise (capped to parent's timeout).
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.req.timeout_at = self.ctx.child_timeout(Some(timeout));
        self
    }

    /// Override the target for this detached call (resolved through `target_resolver`).
    pub fn target(mut self, target: &str) -> Self {
        let resolved = (self.ctx.target_resolver)(Some(target));
        self.req
            .tags
            .insert("resonate:target".to_string(), resolved);
        self
    }

    /// Start the detached call and return a `DetachedHandle`.
    ///
    /// Synchronous: promise creation happens on a background task. The handle
    /// is deliberately not awaitable — a detached call's result is never
    /// delivered back to the parent. Call `handle.id()` to obtain the promise
    /// ID once the promise exists on the server (never hand an ID to an
    /// external system before then). Detached promises are not registered in
    /// `remote_todos`, so the parent workflow does not suspend on them, but a
    /// creation failure still fails the task at flush — and the promise is
    /// created even if the handle is dropped.
    pub fn spawn(self) -> Result<DetachedHandle> {
        Context::check_serialization_error(&self.serialization_error)?;
        let DetachedTask {
            child_id, ctx, req, ..
        } = self;

        let task_id = child_id.clone();
        // The result channel is unused: a detached call never reports a result
        // back to the parent, so the closure simply drops `tx`. Reusing
        // `spawn_sequenced` keeps detached creation in the creation sequence
        // and tracked for flush.
        let (_rx, created_rx) = spawn_sequenced::<(), _, _>(
            ctx,
            child_id.clone(),
            req,
            move |_record, _tx| async move {
                tracing::info!(
                    target: "resonate::validation",
                    promise_id = %task_id,
                    "promise_detached_spawn"
                );
                Outcome::Done(Ok(()))
            },
        );

        Ok(DetachedHandle::pending(child_id, created_rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::{Durable, ExecutionEnv};
    #[allow(unused_imports)]
    use crate::futures::RemoteFuture;
    use crate::test_utils::*;
    use crate::types::{DurableKind, Outcome};

    // ═══════════════════════════════════════════════════════════════
    //  Test Durable function implementations
    // ═══════════════════════════════════════════════════════════════

    struct Bar;
    impl Durable<(), i32> for Bar {
        const NAME: &'static str = "bar";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            Ok(42)
        }
    }

    struct Baz;
    impl Durable<(), i32> for Baz {
        const NAME: &'static str = "baz";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            Ok(31416)
        }
    }

    struct Add;
    impl Durable<(i32, i32), i32> for Add {
        const NAME: &'static str = "add";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _env: ExecutionEnv<'_>,
            args: (i32, i32),
        ) -> crate::error::Result<i32> {
            Ok(args.0 + args.1)
        }
    }

    struct Double;
    impl Durable<i32, i32> for Double {
        const NAME: &'static str = "double";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, args: i32) -> crate::error::Result<i32> {
            Ok(args * 2)
        }
    }

    struct Square;
    impl Durable<i32, i32> for Square {
        const NAME: &'static str = "square";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, args: i32) -> crate::error::Result<i32> {
            Ok(args * args)
        }
    }

    struct Multiply;
    impl Durable<(i32, i32), i32> for Multiply {
        const NAME: &'static str = "multiply";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _env: ExecutionEnv<'_>,
            args: (i32, i32),
        ) -> crate::error::Result<i32> {
            Ok(args.0 * args.1)
        }
    }

    struct Failing;
    impl Durable<(), i32> for Failing {
        const NAME: &'static str = "failing";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            Err(Error::Application {
                message: "boom".to_string(),
            })
        }
    }

    struct Noop;
    impl Durable<(), ()> for Noop {
        const NAME: &'static str = "noop";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<()> {
            Ok(())
        }
    }

    struct Concat;
    impl Durable<(String, String, String), String> for Concat {
        const NAME: &'static str = "concat";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _env: ExecutionEnv<'_>,
            args: (String, String, String),
        ) -> crate::error::Result<String> {
            Ok(format!("{}-{}-{}", args.0, args.1, args.2))
        }
    }

    struct Slow;
    impl Durable<(), i32> for Slow {
        const NAME: &'static str = "slow";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            Ok(1)
        }
    }

    struct Fast;
    impl Durable<(), i32> for Fast {
        const NAME: &'static str = "fast";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            Ok(2)
        }
    }

    use std::sync::atomic::AtomicI32;
    static CALL_COUNT: AtomicI32 = AtomicI32::new(0);

    struct Counter;
    impl Durable<(), i32> for Counter {
        const NAME: &'static str = "counter";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(&self, _env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            let val = CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(val + 1)
        }
    }

    // Child workflow: calls rpc, returns v * 2
    struct ChildWorkflow;
    impl Durable<(), i32> for ChildWorkflow {
        const NAME: &'static str = "child";
        const KIND: DurableKind = DurableKind::Workflow;
        async fn execute(&self, env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            let ctx = env.into_context();
            let v: i32 = ctx.rpc("remoteFunc", ()).await?;
            Ok(v * 2)
        }
    }

    // Child workflow that calls a leaf twice
    struct ChildWithLeaves;
    impl Durable<(), i32> for ChildWithLeaves {
        const NAME: &'static str = "child_with_leaves";
        const KIND: DurableKind = DurableKind::Workflow;
        async fn execute(&self, env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            let ctx = env.into_context();
            let a: i32 = ctx.run(Bar, ()).await?;
            let b: i32 = ctx.run(Bar, ()).await?;
            Ok(a + b)
        }
    }

    // Inner workflow that calls a failing leaf
    struct InnerFailing;
    impl Durable<(), i32> for InnerFailing {
        const NAME: &'static str = "inner_failing";
        const KIND: DurableKind = DurableKind::Workflow;
        async fn execute(&self, env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            let ctx = env.into_context();
            ctx.run(Failing, ()).await
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. Execution Tests (Coroutine) — Basic completion
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn workflow_calling_leaf_completes_with_done() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let val: i32 = ctx.run(Bar, ()).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(val)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 42),
            other => panic!("expected Done(Ok(42)), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn workflow_calling_multiple_leaves_completes_with_final_value() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let a: i32 = ctx.run(Bar, ()).await.unwrap();
        let b: i32 = ctx.run(Baz, ()).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(a + b)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 31458),
            other => panic!("expected Done(Ok(31458)), got {:?}", other),
        }
    }

    // ── Suspension ─────────────────────────────────────────────────

    #[tokio::test]
    async fn workflow_with_remote_suspends_then_completes_after_settlement() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        // First execution: local bar runs, remote suspends
        let local_future = ctx.run(Bar, ()).spawn().unwrap();
        let _remote_future: RemoteFuture<i32> = ctx.rpc::<i32>("bar", &()).spawn().unwrap();
        let local_val: i32 = local_future.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(local_val)).await;

        let remote_id = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
                remote_todos[0].clone()
            }
            other => panic!("expected Suspended, got {:?}", other),
        };

        // Settle the remote promise in the stub
        harness.settle_promise_in_stub(&remote_id, 100_i32).await;

        // Second execution: remote promise is now resolved via preload
        let effects2 = harness.build_effects(vec![resolved_promise(&remote_id, 100_i32)]);
        let ctx2 = test_context("root", effects2);

        let local_future2 = ctx2.run(Bar, ()).spawn().unwrap();
        let remote_future2: RemoteFuture<i32> = ctx2.rpc::<i32>("bar", &()).spawn().unwrap();
        let local_val2: i32 = local_future2.await.unwrap();
        let remote_val2: i32 = remote_future2.await.unwrap();
        let outcome2 = finalize_context(&ctx2, Ok(local_val2 + remote_val2)).await;

        match outcome2 {
            Outcome::Done(Ok(v)) => assert_eq!(v, 142),
            other => panic!("expected Done after settlement, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn structured_concurrency_multiple_remotes_require_multiple_settle_cycles() {
        let harness = TestHarness::new();

        // First execution: two remotes, both pending
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);
        let _r1 = ctx.rpc::<i32>("bar", &()).spawn().unwrap();
        let _r2 = ctx.rpc::<i32>("bar", &()).spawn().unwrap();
        let outcome = finalize_context(&ctx, Ok(99)).await;

        let todos = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 2);
                remote_todos.clone()
            }
            other => panic!("expected Suspended with 2 todos, got {:?}", other),
        };

        // Settle first remote
        harness.settle_promise_in_stub(&todos[0], 10_i32).await;

        // Second execution: one resolved, one still pending
        let effects2 = harness.build_effects(vec![resolved_promise(&todos[0], 10_i32)]);
        let ctx2 = test_context("root", effects2);
        let _r1 = ctx2.rpc::<i32>("bar", &()).spawn().unwrap();
        let _r2 = ctx2.rpc::<i32>("bar", &()).spawn().unwrap();
        let outcome2 = finalize_context(&ctx2, Ok(99)).await;

        match &outcome2 {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended with 1 todo, got {:?}", other),
        }

        // Settle second remote
        harness.settle_promise_in_stub(&todos[1], 20_i32).await;

        // Third execution: both resolved
        let effects3 = harness.build_effects(vec![
            resolved_promise(&todos[0], 10_i32),
            resolved_promise(&todos[1], 20_i32),
        ]);
        let ctx3 = test_context("root", effects3);
        let _r1 = ctx3.rpc::<i32>("bar", &()).spawn().unwrap();
        let _r2 = ctx3.rpc::<i32>("bar", &()).spawn().unwrap();
        let outcome3 = finalize_context(&ctx3, Ok(99)).await;

        match outcome3 {
            Outcome::Done(Ok(v)) => assert_eq!(v, 99),
            other => panic!("expected Done(99), got {:?}", other),
        }
    }

    // ── Structured concurrency ─────────────────────────────────────

    #[tokio::test]
    async fn fire_and_forget_local_leaves_flushed_at_return() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _f1 = ctx.run(Bar, ()).spawn().unwrap();
        let _f2 = ctx.run(Baz, ()).spawn().unwrap();
        let outcome = finalize_context(&ctx, Ok(99)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 99),
            other => panic!("expected Done(99), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn mixed_local_fire_and_forget_plus_remote_suspends_on_remote() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _local = ctx.run(Bar, ()).spawn().unwrap();
        let _remote = ctx.rpc::<i32>("someRemote", &()).spawn().unwrap();
        let outcome = finalize_context(&ctx, Ok(77)).await;

        match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    // ── Error handling ─────────────────────────────────────────────

    #[tokio::test]
    async fn local_function_error_surfaces_at_await_time() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let result: crate::error::Result<i32> = ctx.run(Failing, ()).await;
        let msg = match result {
            Err(e) => format!("caught: {}", e),
            Ok(_) => "should not happen".to_string(),
        };
        let outcome = finalize_context(&ctx, Ok(msg)).await;

        match outcome {
            Outcome::Done(Ok(v)) => {
                assert!(v.contains("caught:"), "got: {}", v);
                assert!(v.contains("boom"), "got: {}", v);
            }
            other => panic!("expected Done(Ok(caught: boom)), got {:?}", other),
        }
    }

    // ── Concurrency ────────────────────────────────────────────────

    #[tokio::test]
    async fn multiple_local_functions_run_concurrently() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let f_slow = ctx.run(Slow, ()).spawn().unwrap();
        let f_fast = ctx.run(Fast, ()).spawn().unwrap();
        let a: i32 = f_slow.await.unwrap();
        let b: i32 = f_fast.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(a + b)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 3),
            other => panic!("expected Done(3), got {:?}", other),
        }
    }

    // ── Nested workflows ───────────────────────────────────────────

    #[tokio::test]
    async fn child_workflow_suspends_on_remote_parent_suspends_too() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("foo", effects);

        // First execution: child suspends on rpc("remoteFunc")
        let result: crate::error::Result<i32> = ctx.run(ChildWorkflow, ()).await;
        let workflow_result = match result {
            Ok(v) => Ok(v + 1),
            Err(Error::Suspended) => Err(Error::Suspended),
            Err(e) => Err(e),
        };
        let outcome = finalize_context(&ctx, workflow_result).await;

        let remote_id = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert!(!remote_todos.is_empty(), "expected at least 1 remote todo");
                remote_todos[0].clone()
            }
            other => panic!("expected Suspended, got {:?}", other),
        };

        // Settle the remote promise
        harness.settle_promise_in_stub(&remote_id, 21_i32).await;

        // Second execution: remote resolved, child completes, parent completes
        let effects2 = harness.build_effects(vec![resolved_promise(&remote_id, 21_i32)]);
        let ctx2 = test_context("foo", effects2);

        let result2: crate::error::Result<i32> = ctx2.run(ChildWorkflow, ()).await;
        let workflow_result2 = match result2 {
            Ok(v) => Ok(v + 1),
            Err(e) => Err(e),
        };
        let outcome2 = finalize_context(&ctx2, workflow_result2).await;

        match outcome2 {
            Outcome::Done(Ok(v)) => assert_eq!(v, 43),
            other => panic!("expected Done(43), got {:?}", other),
        }
    }

    // ── Double-await ───────────────────────────────────────────────
    // NOTE: In Rust, DurableFuture's IntoFuture impl consumes self (takes ownership).
    // Double-await is prevented at compile time by Rust's ownership model.
    // We test that a single await works correctly.

    #[tokio::test]
    async fn awaiting_a_durable_future_returns_correct_value() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let future = ctx.run(Bar, ()).spawn().unwrap();
        let v: i32 = future.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(v)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 42),
            other => panic!("expected Done(42), got {:?}", other),
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. Dispatch Tests (Computation)
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn single_run_with_leaf_resolves_correctly() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result: i32 = ctx.run(Add, (3, 4)).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(result)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 7),
            other => panic!("expected Done(7), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn multiple_run_calls_with_leaves_complete_with_final_value() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let a: i32 = ctx.run(Double, 5).await.unwrap();
        let b: i32 = ctx.run(Square, 3).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(a + b)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 19),
            other => panic!("expected Done(19), got {:?}", other),
        }
    }

    // ── Remote only ────────────────────────────────────────────────

    #[tokio::test]
    async fn single_rpc_suspends_with_awaited_id() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result: crate::error::Result<i32> = ctx.rpc("remoteFunc", ()).await;
        let outcome = finalize_context(&ctx, result).await;

        match outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended with 1 entry, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn multiple_rpc_spawn_suspends_with_multiple_awaited_ids() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let _a = ctx.rpc::<i32>("a", &()).spawn().unwrap();
        let _b = ctx.rpc::<i32>("b", &()).spawn().unwrap();
        let outcome = finalize_context(&ctx, Ok(0)).await;

        match outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 2);
            }
            other => panic!("expected Suspended with 2 entries, got {:?}", other),
        }
    }

    // ── Mixed local + remote ───────────────────────────────────────

    #[tokio::test]
    async fn local_todo_processed_first_then_suspends_on_remote() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let local_val: i32 = ctx.run(Add, (1, 2)).await.unwrap();
        let remote_result: crate::error::Result<i32> = ctx.rpc("remoteFunc", ()).await;
        let workflow_result = match remote_result {
            Ok(v) => Ok(local_val + v),
            Err(Error::Suspended) => Err(Error::Suspended),
            Err(e) => Err(e),
        };
        let outcome = finalize_context(&ctx, workflow_result).await;

        match outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn spawn_local_and_rpc_remote_in_parallel() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let local_future = ctx.run(Multiply, (3, 7)).spawn().unwrap();
        let _remote_future = ctx.rpc::<i32>("remote", &()).spawn().unwrap();
        let local_val: i32 = local_future.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(local_val)).await;

        match outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    // ── Regular function (non-workflow) ────────────────────────────

    #[tokio::test]
    async fn regular_function_resolves_with_returned_value() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let v: i32 = ctx.run(Add, (3, 4)).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(v)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 7),
            other => panic!("expected Done(7), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn regular_function_rejects_when_function_throws() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result: crate::error::Result<i32> = ctx.run(Failing, ()).await;
        let outcome = finalize_context(&ctx, result).await;

        match outcome {
            Outcome::Done(Err(e)) => {
                assert!(e.to_string().contains("boom"), "got: {}", e);
            }
            other => panic!("expected Done(Err(boom)), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn regular_function_resolves_with_unit_when_nothing_returned() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let _: () = ctx.run(Noop, ()).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(())).await;

        match outcome {
            Outcome::Done(Ok(())) => {}
            other => panic!("expected Done(()), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn regular_function_passes_arguments_correctly() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let v: String = ctx
            .run(Concat, ("x".to_string(), "y".to_string(), "z".to_string()))
            .await
            .unwrap();
        let outcome = finalize_context(&ctx, Ok(v)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, "x-y-z"),
            other => panic!("expected Done(x-y-z), got {:?}", other),
        }
    }

    // ── Error handling (dispatch) ──────────────────────────────────

    #[tokio::test]
    async fn local_function_that_throws_results_in_rejected() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result: crate::error::Result<i32> = ctx.run(Failing, ()).await;
        let outcome = finalize_context(&ctx, result).await;

        match outcome {
            Outcome::Done(Err(_)) => {} // Expected
            other => panic!("expected Done(Err), got {:?}", other),
        }
    }

    // ── No re-execution ────────────────────────────────────────────

    #[tokio::test]
    async fn local_function_executes_exactly_once() {
        CALL_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);

        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let v: i32 = ctx.run(Counter, ()).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(v)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 1),
            other => panic!("expected Done(1), got {:?}", other),
        }
        assert_eq!(
            CALL_COUNT.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "counter should have been called exactly once"
        );
    }

    // ═══════════════════════════════════════════════════════════════
    //  7. Integration Tests — Lineage / Tags
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn local_call_sets_correct_tags() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create_req = requests.iter().find(|r| r["kind"] == "promise.create");
        assert!(create_req.is_some(), "should have sent a PromiseCreate");
        let create = create_req.unwrap();
        assert_eq!(create["tags"]["resonate:scope"].as_str().unwrap(), "local");
        assert_eq!(create["tags"]["resonate:parent"].as_str().unwrap(), "root");
        assert_eq!(create["tags"]["resonate:origin"].as_str().unwrap(), "root");
        assert!(create["tags"].get("resonate:branch").is_some());
    }

    #[tokio::test]
    async fn remote_call_sets_correct_tags() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _: crate::error::Result<i32> = ctx.rpc("remote", ()).await;

        let requests = harness.sent_requests_json().await;
        let create_req = requests.iter().find(|r| r["kind"] == "promise.create");
        assert!(create_req.is_some());
        let create = create_req.unwrap();
        assert_eq!(create["tags"]["resonate:scope"].as_str().unwrap(), "global");
        // Default target uses the group name ("default"), not the function name
        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "default"
        );
        assert_eq!(create["tags"]["resonate:parent"].as_str().unwrap(), "root");
        assert_eq!(create["tags"]["resonate:origin"].as_str().unwrap(), "root");
        assert!(create["tags"].get("resonate:branch").is_some());
    }

    // ── Deterministic IDs & origin consistency ───────────────────

    #[tokio::test]
    async fn origin_matches_root_for_all_nested_calls() {
        // All promises created by nested Context calls should carry
        // resonate:origin == the root context ID.
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        // ChildWithLeaves creates two nested children inside root.0
        let _: i32 = ctx.run(ChildWithLeaves, ()).await.unwrap();
        // Another direct child at root.1
        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let creates: Vec<_> = requests
            .iter()
            .filter(|r| r["kind"] == "promise.create")
            .collect();

        // All created promises should have origin == "root"
        for create in &creates {
            assert_eq!(
                create["tags"]["resonate:origin"].as_str().unwrap(),
                "root",
                "promise {} should have origin 'root'",
                create["id"].as_str().unwrap(),
            );
        }
        // Verify we got the nested ones too (root.0, root.0.0, root.0.1, root.1)
        let ids: Vec<&str> = creates.iter().map(|c| c["id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"root:0"), "should have root:0");
        assert!(ids.contains(&"root:0.0"), "should have root:0.0");
        assert!(ids.contains(&"root:0.1"), "should have root:0.1");
        assert!(ids.contains(&"root:1"), "should have root:1");
    }

    // ── Match Function (target resolution) ─────────────────────────

    #[tokio::test]
    async fn rpc_target_is_resolved_through_target_resolver() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("local://any@{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        let _: crate::error::Result<i32> = ctx.rpc("hello", ()).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Default target uses the group name ("default"), not the function name
        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "local://any@default"
        );
    }

    #[tokio::test]
    async fn rpc_target_uses_custom_prefix_from_target_resolver() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("http://server:8001/workers/{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        let _: crate::error::Result<String> = ctx.rpc("my_func", 42i32).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Default target uses the group name ("default"), not the function name
        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "http://server:8001/workers/default"
        );
    }

    #[tokio::test]
    async fn rpc_spawn_target_is_resolved_through_target_resolver() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("remote://group/{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        // Await the handle so the background creation completes before
        // asserting on the sent requests (Err(Suspended) is expected).
        let _ = ctx.rpc::<i32>("greet", &"world").spawn().unwrap().await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Default target uses the group name ("default"), not the function name
        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "remote://group/default"
        );
    }

    #[tokio::test]
    async fn identity_target_resolver_passes_target_through_unchanged() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // Identity resolver — no transformation
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| target.unwrap_or("default").to_string());
        let ctx = test_context_with_match("root", effects, target_resolver);

        let _: crate::error::Result<i32> = ctx.rpc("bare_name", ()).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Default target uses the group name ("default"), not the function name
        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "default"
        );
    }

    #[tokio::test]
    async fn target_resolver_propagates_through_multiple_rpc_calls() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("custom://{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        // First rpc call
        let _: crate::error::Result<i32> = ctx.rpc("func_a", ()).await;
        // Second rpc call — same context, target_resolver should still work
        let _: crate::error::Result<i32> = ctx.rpc("func_b", ()).await;

        let requests = harness.sent_requests_json().await;
        let creates: Vec<_> = requests
            .iter()
            .filter(|r| r["kind"] == "promise.create")
            .collect();

        // Both use the group name ("default") as the default target, not func names
        assert_eq!(creates.len(), 2);
        assert_eq!(
            creates[0]["tags"]["resonate:target"].as_str().unwrap(),
            "custom://default"
        );
        assert_eq!(
            creates[1]["tags"]["resonate:target"].as_str().unwrap(),
            "custom://default"
        );
    }

    #[tokio::test]
    async fn rpc_target_override_with_url_passes_through_unchanged() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // This resolver prefixes "local://any@" for bare names.
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                let t = target.unwrap_or("default");
                if t.contains("://") {
                    t.to_string()
                } else {
                    format!("local://any@{}", t)
                }
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        // Explicit .target() with a URL — should be kept as-is
        let _: crate::error::Result<i32> = ctx
            .rpc("some_func", ())
            .target("http://other-host:8001/workers/hello")
            .await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "http://other-host:8001/workers/hello",
            "URL target should pass through unchanged"
        );
    }

    #[tokio::test]
    async fn rpc_target_override_bare_name_is_resolved_url_passes_through() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // Simulates real behavior: bare names get rewritten, URLs don't
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                let t = target.unwrap_or("default");
                if t.contains("://") {
                    t.to_string()
                } else {
                    format!("local://any@{}", t)
                }
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        // Bare name target override — should be rewritten
        let _: crate::error::Result<i32> = ctx.rpc("func_a", ()).target("workers").await;
        // URL target override — should NOT be rewritten
        let _: crate::error::Result<i32> = ctx
            .rpc("func_b", ())
            .target("https://remote.example.com/workers/greet")
            .await;

        let requests = harness.sent_requests_json().await;
        let creates: Vec<_> = requests
            .iter()
            .filter(|r| r["kind"] == "promise.create")
            .collect();

        assert_eq!(creates.len(), 2);
        assert_eq!(
            creates[0]["tags"]["resonate:target"].as_str().unwrap(),
            "local://any@workers",
            "bare name target override should be rewritten by resolver"
        );
        assert_eq!(
            creates[1]["tags"]["resonate:target"].as_str().unwrap(),
            "https://remote.example.com/workers/greet",
            "URL target override should pass through unchanged"
        );
    }

    #[tokio::test]
    async fn local_run_does_not_set_target_tag() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("SHOULD_NOT_APPEAR://{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        // ctx.run uses local_create_req, not remote_create_req
        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Local calls set scope=local and should NOT have resonate:target
        assert_eq!(create["tags"]["resonate:scope"].as_str().unwrap(), "local");
        assert!(
            create["tags"].get("resonate:target").is_none(),
            "local run should not set resonate:target"
        );
    }

    // ── Target override via rpc_with_opts / begin_rpc_with_opts ───

    #[tokio::test]
    async fn rpc_with_target_builder_overrides_func_name() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("local://any@{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        let _: crate::error::Result<i32> =
            ctx.rpc::<i32>("my_func", &()).target("custom-target").await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "local://any@custom-target",
            "custom target should override func_name in target_resolver"
        );
    }

    #[tokio::test]
    async fn rpc_default_target_uses_group_name() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("local://any@{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        let _: crate::error::Result<i32> = ctx.rpc::<i32>("my_func", &()).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Default target uses the group name ("default"), not the function name
        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "local://any@default",
            "default target should use group name, not func_name"
        );
    }

    #[tokio::test]
    async fn rpc_with_url_target_passes_through() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                let t = target.unwrap_or("default");
                if t.contains("://") {
                    t.to_string()
                } else {
                    format!("local://any@{}", t)
                }
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        let _: crate::error::Result<i32> = ctx
            .rpc::<i32>("my_func", &())
            .target("https://remote:9000/workers/foo")
            .await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "https://remote:9000/workers/foo",
            "URL target should pass through unchanged"
        );
    }

    #[tokio::test]
    async fn rpc_spawn_with_target_builder() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let target_resolver: crate::context::TargetResolver =
            std::sync::Arc::new(|target: Option<&str>| {
                format!("remote://{}", target.unwrap_or("default"))
            });
        let ctx = test_context_with_match("root", effects, target_resolver);

        // Await the handle so the background creation completes before
        // asserting on the sent requests (Err(Suspended) is expected).
        let _ = ctx
            .rpc::<i32>("my_func", &())
            .target("override-target")
            .spawn()
            .unwrap()
            .await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["tags"]["resonate:target"].as_str().unwrap(),
            "remote://override-target",
        );
    }

    // ── Deterministic IDs ──────────────────────────────────────────

    #[tokio::test]
    async fn sequential_calls_produce_deterministic_ids() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _: i32 = ctx.run(Bar, ()).await.unwrap();
        let _: i32 = ctx.run(Baz, ()).await.unwrap();
        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create_ids: Vec<String> = requests
            .iter()
            .filter(|r| r["kind"] == "promise.create")
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect();

        assert_eq!(create_ids[0], "root:0");
        assert_eq!(create_ids[1], "root:1");
        assert_eq!(create_ids[2], "root:2");
    }

    #[tokio::test]
    async fn nested_calls_produce_hierarchical_ids() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _: i32 = ctx.run(ChildWithLeaves, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create_ids: Vec<String> = requests
            .iter()
            .filter(|r| r["kind"] == "promise.create")
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect();

        assert!(create_ids.contains(&"root:0".to_string()));
        assert!(create_ids.contains(&"root:0.0".to_string()));
        assert!(create_ids.contains(&"root:0.1".to_string()));
    }

    // ── Concurrent vs Sequential execution ─────────────────────────

    #[tokio::test]
    async fn concurrent_execution_spawn_is_actually_concurrent() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);
        let start = tokio::time::Instant::now();

        let f1 = ctx.run(Slow, ()).spawn().unwrap();
        let f2 = ctx.run(Fast, ()).spawn().unwrap();
        let a: i32 = f1.await.unwrap();
        let b: i32 = f2.await.unwrap();
        let elapsed = start.elapsed();
        let outcome = finalize_context(&ctx, Ok(a + b)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 3),
            other => panic!("expected Done(3), got {:?}", other),
        }
        assert!(elapsed.as_millis() < 200, "took too long: {:?}", elapsed);
    }

    #[tokio::test]
    async fn sequential_execution_run_is_actually_sequential() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let a: i32 = ctx.run(Bar, ()).await.unwrap();
        let b: i32 = ctx.run(Baz, ()).await.unwrap();
        let outcome = finalize_context(&ctx, Ok(a + b)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, 31458),
            other => panic!("expected Done(31458), got {:?}", other),
        }
    }

    // ── Error propagation ──────────────────────────────────────────

    #[tokio::test]
    async fn leaf_throwing_error_propagates_to_workflow_result() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let result: crate::error::Result<i32> = ctx.run(Failing, ()).await;
        let outcome = finalize_context(&ctx, result).await;

        match outcome {
            Outcome::Done(Err(e)) => {
                assert!(e.to_string().contains("boom"), "got: {}", e);
            }
            other => panic!("expected Done(Err), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn nested_workflow_error_propagates_upward() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let result: crate::error::Result<i32> = ctx.run(InnerFailing, ()).await;
        let outcome = finalize_context(&ctx, result).await;

        match outcome {
            Outcome::Done(Err(e)) => {
                assert!(e.to_string().contains("boom"), "got: {}", e);
            }
            other => panic!("expected Done(Err), got {:?}", other),
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Timeout Capping Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn child_timeout_capped_to_parent_for_local_run() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // Parent timeout is 5 seconds from now
        let now = super::now_ms();
        let parent_timeout = now + 5_000;
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Default child timeout is 24h, which exceeds parent's 5s → clamped
        assert!(
            create["timeoutAt"].as_i64().unwrap() <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout
        );
    }

    #[tokio::test]
    async fn child_timeout_capped_to_parent_for_remote_rpc() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 5_000;
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        let _: crate::error::Result<i32> = ctx.rpc("remote_func", ()).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert!(
            create["timeoutAt"].as_i64().unwrap() <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout
        );
    }

    #[tokio::test]
    async fn child_timeout_capped_to_parent_for_run_spawn() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 5_000;
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        // Await the handle so the background creation completes before
        // asserting on the sent requests.
        let _: i32 = ctx.run(Bar, ()).spawn().unwrap().await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert!(
            create["timeoutAt"].as_i64().unwrap() <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout
        );
    }

    #[tokio::test]
    async fn child_timeout_capped_to_parent_for_rpc_spawn() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 5_000;
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        // Await the handle so the background creation completes before
        // asserting on the sent requests (Err(Suspended) is expected).
        let _ = ctx.rpc::<i32>("remote", &()).spawn().unwrap().await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert!(
            create["timeoutAt"].as_i64().unwrap() <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout
        );
    }

    #[tokio::test]
    async fn explicit_child_timeout_smaller_than_parent_is_respected() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 60_000; // 60 seconds
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        // Request 10 seconds — smaller than parent's 60s, should be respected
        let child_timeout = std::time::Duration::from_secs(10);
        let _: i32 = ctx.run(Bar, ()).timeout(child_timeout).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // Should be approximately now + 10s, not parent's 60s
        let expected_approx = now + 10_000;
        let tolerance = 1_000; // 1 second tolerance for test execution time
        assert!(
            create["timeoutAt"].as_i64().unwrap() >= expected_approx - tolerance
                && create["timeoutAt"].as_i64().unwrap() <= expected_approx + tolerance,
            "child timeout_at ({}) should be ~{} (now + 10s), not parent timeout_at ({})",
            create["timeoutAt"].as_i64().unwrap(),
            expected_approx,
            parent_timeout
        );
    }

    #[tokio::test]
    async fn explicit_child_timeout_exceeding_parent_is_clamped() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 5_000; // 5 seconds
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        // Request 60 seconds — exceeds parent's 5s, should be clamped
        let child_timeout = std::time::Duration::from_secs(60);
        let _: i32 = ctx.run(Bar, ()).timeout(child_timeout).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout,
            "child timeout_at should be clamped to parent timeout_at"
        );
    }

    #[tokio::test]
    async fn rpc_with_timeout_builder_smaller_than_parent_is_respected() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 60_000;
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        let child_timeout = std::time::Duration::from_secs(10);
        let _: crate::error::Result<i32> =
            ctx.rpc::<i32>("remote", &()).timeout(child_timeout).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let expected_approx = now + 10_000;
        let tolerance = 1_000;
        assert!(
            create["timeoutAt"].as_i64().unwrap() >= expected_approx - tolerance
                && create["timeoutAt"].as_i64().unwrap() <= expected_approx + tolerance,
            "child timeout_at ({}) should be ~{} (now + 10s)",
            create["timeoutAt"].as_i64().unwrap(),
            expected_approx
        );
    }

    #[tokio::test]
    async fn rpc_spawn_with_timeout_exceeding_parent_is_clamped() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 5_000;
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        let child_timeout = std::time::Duration::from_secs(60);
        // Await the handle so the background creation completes before
        // asserting on the sent requests (Err(Suspended) is expected).
        let _ = ctx
            .rpc::<i32>("remote", &())
            .timeout(child_timeout)
            .spawn()
            .unwrap()
            .await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout,
            "child timeout_at should be clamped to parent timeout_at"
        );
    }

    #[tokio::test]
    async fn default_child_timeout_with_large_parent_uses_24h() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // Parent timeout is very far in the future (i64::MAX)
        let ctx = test_context("root", effects);

        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let now = super::now_ms();
        let expected_24h = now + 86_400_000; // 24 hours in ms
        let tolerance = 2_000; // 2 second tolerance

        // With i64::MAX parent, child should get ~now + 24h
        assert!(
            create["timeoutAt"].as_i64().unwrap() >= expected_24h - tolerance
                && create["timeoutAt"].as_i64().unwrap() <= expected_24h + tolerance,
            "child timeout_at ({}) should be ~{} (now + 24h), got diff={}ms",
            create["timeoutAt"].as_i64().unwrap(),
            expected_24h,
            (create["timeoutAt"].as_i64().unwrap() - expected_24h).abs()
        );
    }

    // ═══════════════════════════════════════════════════════════════
    //  Sleep Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn sleep_suspends_on_pending() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let result = ctx.sleep(Duration::from_secs(60)).await;
        assert!(matches!(result, Err(Error::Suspended)));

        let outcome = finalize_context::<()>(&ctx, Err(Error::Suspended)).await;
        match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn sleep_creates_promise_with_timer_tags() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _ = ctx.sleep(Duration::from_secs(60)).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let tags = create["tags"]
            .as_object()
            .expect("tags should be an object");
        assert_eq!(tags["resonate:scope"].as_str().unwrap(), "global");
        assert_eq!(tags["resonate:timer"].as_str().unwrap(), "true");
        assert_eq!(tags["resonate:parent"].as_str().unwrap(), "root");
        assert_eq!(tags["resonate:origin"].as_str().unwrap(), "root");
        // branch should be the child id
        assert!(tags.contains_key("resonate:branch"));
        // The server only schedules a timeout for a promise carrying a
        // target, so a target-less timer would never fire — the timer must
        // carry one.
        assert!(tags.contains_key("resonate:target"));
    }

    #[tokio::test]
    async fn sleep_timeout_uses_duration() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 120_000; // 2 minutes
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        let _ = ctx.sleep(Duration::from_secs(60)).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let expected_approx = now + 60_000;
        let tolerance = 1_000;
        assert!(
            create["timeoutAt"].as_i64().unwrap() >= expected_approx - tolerance
                && create["timeoutAt"].as_i64().unwrap() <= expected_approx + tolerance,
            "sleep timeout_at ({}) should be ~{} (now + 60s)",
            create["timeoutAt"].as_i64().unwrap(),
            expected_approx
        );
    }

    #[tokio::test]
    async fn sleep_timeout_capped_to_parent() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 5_000; // 5 seconds
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        // Request 60 seconds sleep, but parent only has 5s left
        let _ = ctx.sleep(Duration::from_secs(60)).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        assert_eq!(
            create["timeoutAt"].as_i64().unwrap(),
            parent_timeout,
            "sleep timeout_at should be clamped to parent timeout_at"
        );
    }

    #[tokio::test]
    async fn sleep_returns_ok_when_already_resolved() {
        let harness = TestHarness::new();
        let sleep_id = "root:0";
        harness.settle_promise_in_stub(sleep_id, ()).await;

        let effects = harness.build_effects(vec![resolved_promise(sleep_id, ())]);
        let ctx = test_context("root", effects);

        let result = ctx.sleep(Duration::from_secs(60)).await;
        assert!(result.is_ok(), "sleep should return Ok(()) when resolved");
    }

    #[tokio::test]
    async fn sleep_spawn_returns_remote_future_pending() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _handle = ctx.sleep(Duration::from_secs(30)).spawn().unwrap();

        let outcome = finalize_context(&ctx, Ok("done")).await;
        match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn sleep_spawn_resolved_returns_ok() {
        let harness = TestHarness::new();
        let sleep_id = "root:0";
        harness.settle_promise_in_stub(sleep_id, ()).await;

        let effects = harness.build_effects(vec![resolved_promise(sleep_id, ())]);
        let ctx = test_context("root", effects);

        let handle = ctx.sleep(Duration::from_secs(30)).spawn().unwrap();
        let result = handle.await;
        assert!(result.is_ok(), "sleep spawn should resolve to Ok(())");
    }

    #[tokio::test]
    async fn sleep_has_empty_param() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _ = ctx.sleep(Duration::from_secs(10)).await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        // param.data should be null or empty (no meaningful payload)
        let param = &create["param"];
        let data = &param["data"];
        assert!(
            data.is_null() || data.as_str().is_some_and(|s| s.is_empty()),
            "sleep param data should be null or empty, got {:?}",
            data
        );
    }

    #[tokio::test]
    async fn workflow_with_sleep_suspends_then_completes_after_settlement() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        // First execution: sleep suspends
        let result = ctx.sleep(Duration::from_secs(30)).await;
        let outcome = finalize_context(&ctx, result).await;

        let sleep_id = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
                remote_todos[0].clone()
            }
            other => panic!("expected Suspended, got {:?}", other),
        };

        // Settle the timer promise (server resolves it after duration)
        harness.settle_promise_in_stub(&sleep_id, ()).await;

        // Second execution: timer resolved, sleep returns Ok
        let effects2 = harness.build_effects(vec![resolved_promise(&sleep_id, ())]);
        let ctx2 = test_context("root", effects2);

        let result2 = ctx2.sleep(Duration::from_secs(30)).await;
        assert!(result2.is_ok());
        let outcome2 = finalize_context(&ctx2, Ok(())).await;

        match outcome2 {
            Outcome::Done(Ok(())) => {}
            other => panic!("expected Done(\"awake\"), got {:?}", other),
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Promise Tests
    // ═══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn promise_suspends_on_pending() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let result = ctx.promise::<String>().await;
        assert!(matches!(result, Err(Error::Suspended)));

        let outcome = finalize_context::<()>(&ctx, Err(Error::Suspended)).await;
        match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn promise_creates_with_correct_tags() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _ = ctx.promise::<String>().await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let tags = create["tags"]
            .as_object()
            .expect("tags should be an object");
        assert_eq!(tags["resonate:scope"].as_str().unwrap(), "global");
        assert_eq!(tags["resonate:parent"].as_str().unwrap(), "root");
        assert_eq!(tags["resonate:origin"].as_str().unwrap(), "root");
        assert!(tags.contains_key("resonate:branch"));
        // should NOT have target or timer tags
        assert!(!tags.contains_key("resonate:target"));
        assert!(!tags.contains_key("resonate:timer"));
    }

    #[tokio::test]
    async fn promise_has_empty_param() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _ = ctx.promise::<String>().await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let param = &create["param"];
        let data = &param["data"];
        assert!(
            data.is_null() || data.as_str().is_some_and(|s| s.is_empty()),
            "promise param data should be null or empty, got {:?}",
            data
        );
    }

    #[tokio::test]
    async fn promise_with_timeout() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let now = super::now_ms();
        let parent_timeout = now + 300_000; // 5 minutes
        let ctx = test_context_with_timeout("root", parent_timeout, effects);

        let _ = ctx
            .promise::<String>()
            .timeout(Duration::from_secs(120))
            .await;

        let requests = harness.sent_requests_json().await;
        let create = requests
            .iter()
            .find(|r| r["kind"] == "promise.create")
            .expect("should have sent promise.create");

        let expected_approx = now + 120_000;
        let tolerance = 1_000;
        assert!(
            create["timeoutAt"].as_i64().unwrap() >= expected_approx - tolerance
                && create["timeoutAt"].as_i64().unwrap() <= expected_approx + tolerance,
            "promise timeout_at ({}) should be ~{} (now + 120s)",
            create["timeoutAt"].as_i64().unwrap(),
            expected_approx
        );
    }

    #[tokio::test]
    async fn promise_create_pending_returns_remote_future() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let handle = ctx.promise::<String>().create().unwrap();
        // Pending promise should suspend when awaited
        let err = handle.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }

    #[tokio::test]
    async fn promise_resolved_returns_value() {
        let harness = TestHarness::new();
        let promise_id = "root:0";
        harness
            .settle_promise_in_stub(promise_id, "hello".to_string())
            .await;

        let effects =
            harness.build_effects(vec![resolved_promise(promise_id, "hello".to_string())]);
        let ctx = test_context("root", effects);

        let result: String = ctx.promise().await.unwrap();
        assert_eq!(result, "hello");
    }

    // ═══════════════════════════════════════════════════════════════
    //  Creation sequencing Tests
    // ═══════════════════════════════════════════════════════════════

    /// Extract the promise.create request IDs in the order they reached the stub.
    async fn create_ids_in_order(harness: &TestHarness) -> Vec<String> {
        harness
            .sent_requests_json()
            .await
            .iter()
            .filter(|r| r["kind"] == "promise.create")
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn sequenced_spawns_create_promises_in_call_order() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        // Fire-and-forget: creations run on concurrent background tasks, but
        // the creation sequence forces them to reach the server in call order.
        for i in 0..8 {
            let _ = ctx.rpc::<i32>(&format!("f{}", i), &()).spawn().unwrap();
        }
        let _ = finalize_context(&ctx, Ok(0)).await;

        let ids = create_ids_in_order(&harness).await;
        let expected: Vec<String> = (0..8).map(|i| format!("root:{}", i)).collect();
        assert_eq!(
            ids, expected,
            "creations must reach the server in call order"
        );
    }

    #[tokio::test]
    async fn mixed_spawn_and_sequential_ops_create_in_call_order() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _h0 = ctx.run(Bar, ()).spawn().unwrap(); // root.0
        let _h1 = ctx.rpc::<i32>("remote", &()).spawn().unwrap(); // root.1
        let _: i32 = ctx.run(Baz, ()).await.unwrap(); // root.2 (sequential)
        let _h3 = ctx.sleep(Duration::from_secs(30)).spawn().unwrap(); // root.3
        let _ = finalize_context(&ctx, Ok(0)).await;

        let ids = create_ids_in_order(&harness).await;
        // Only the direct children of root, in terminal-op call order.
        let root_children: Vec<&String> = ids
            .iter()
            .filter(|id| id.matches(':').count() == 1 && !id.contains('.'))
            .collect();
        assert_eq!(root_children, ["root:0", "root:1", "root:2", "root:3"]);
    }

    #[tokio::test]
    async fn failed_creation_aborts_all_successors() {
        let harness = TestHarness::new();
        harness.set_fail_promise_create(true).await;
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let h0 = ctx.rpc::<i32>("a", &()).spawn().unwrap();
        let h1 = ctx.rpc::<i32>("b", &()).spawn().unwrap();
        let h2 = ctx.rpc::<i32>("c", &()).spawn().unwrap();

        // All handles resolve (no hang): the first with the creation error,
        // the successors with the sequence-aborted error.
        let e0 = h0.await.unwrap_err();
        assert!(
            !matches!(e0, Error::Suspended),
            "first handle should fail, got: {e0}"
        );
        let e1 = h1.await.unwrap_err();
        assert!(matches!(e1, Error::Application { .. }), "got: {e1}");
        let e2 = h2.await.unwrap_err();
        assert!(matches!(e2, Error::Application { .. }), "got: {e2}");

        // Flush propagates the failure.
        let result = try_finalize_context(&ctx, Ok(0)).await;
        assert!(result.is_err(), "flush should surface the creation failure");

        // Success-gating: only the first creation ever reached the server.
        let ids = create_ids_in_order(&harness).await;
        assert_eq!(ids, ["root:0"], "successors must not issue promise.create");
    }

    #[tokio::test]
    async fn fire_and_forget_failed_creation_fails_at_flush() {
        // One sub-test per spawn-like API: the handle is never awaited, but the
        // creation failure must still fail the task at flush.
        let harness = TestHarness::new();
        harness.set_fail_promise_create(true).await;

        {
            let ctx = test_context("r1", harness.build_effects(vec![]));
            let _ = ctx.rpc::<i32>("f", &()).spawn().unwrap();
            assert!(try_finalize_context(&ctx, Ok(0)).await.is_err(), "rpc");
        }
        {
            let ctx = test_context("r2", harness.build_effects(vec![]));
            let _ = ctx.run(Bar, ()).spawn().unwrap();
            assert!(try_finalize_context(&ctx, Ok(0)).await.is_err(), "run");
        }
        {
            let ctx = test_context("r3", harness.build_effects(vec![]));
            let _ = ctx.sleep(Duration::from_secs(5)).spawn().unwrap();
            assert!(try_finalize_context(&ctx, Ok(0)).await.is_err(), "sleep");
        }
        {
            let ctx = test_context("r4", harness.build_effects(vec![]));
            let _ = ctx.detached("f", ()).spawn().unwrap();
            assert!(try_finalize_context(&ctx, Ok(0)).await.is_err(), "detached");
        }
    }

    #[tokio::test]
    async fn rpc_spawn_handle_awaits_suspended_then_flush_collects_todo() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let handle = ctx.rpc::<i32>("remote", &()).spawn().unwrap();
        let err = handle.await.unwrap_err();
        assert!(matches!(err, Error::Suspended), "got: {err}");

        let outcome = finalize_context(&ctx, Err::<i32, _>(Error::Suspended)).await;
        match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos, &["root:0".to_string()]);
            }
            other => panic!("expected Suspended, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn rpc_spawn_preloaded_resolved_returns_value_via_handle() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![resolved_promise("root:0", 99_i32)]);
        let ctx = test_context("root", effects);

        // Replay short-circuit flows through the handle's oneshot.
        let handle = ctx.rpc::<i32>("remote", &()).spawn().unwrap();
        let v: i32 = handle.await.unwrap();
        assert_eq!(v, 99);
    }

    #[tokio::test]
    async fn detached_spawn_handle_yields_id_after_creation() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let handle = ctx.detached("audit", ()).spawn().unwrap();
        let id = handle.id().await.unwrap();
        assert!(id.starts_with("root:"), "id = {id}");

        // The promise existed on the server before the handle resolved.
        let ids = create_ids_in_order(&harness).await;
        assert_eq!(ids, [id]);

        // Detached never becomes a remote todo.
        let outcome = finalize_context(&ctx, Ok(0)).await;
        assert!(matches!(outcome, Outcome::Done(_)));
    }

    #[tokio::test]
    async fn handle_id_returns_only_after_creation_reached_server() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let handle = ctx.rpc::<i32>("remote", &()).spawn().unwrap();
        let id = handle.id().await.unwrap();
        assert_eq!(id, "root:0");

        // At the moment id() resolved, the create request had reached the stub.
        let ids = create_ids_in_order(&harness).await;
        assert_eq!(ids, ["root:0"]);

        let _ = finalize_context(&ctx, Ok(0)).await;
    }

    #[tokio::test]
    async fn handle_id_fails_when_creation_fails() {
        let harness = TestHarness::new();
        harness.set_fail_promise_create(true).await;
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let h0 = ctx.rpc::<i32>("a", &()).spawn().unwrap();
        let h1 = ctx.rpc::<i32>("b", &()).spawn().unwrap();

        let e0 = h0.id().await.unwrap_err();
        assert!(matches!(e0, Error::PromiseCreation(_)), "got: {e0}");
        // Sequence-aborted successor also reports failure via id().
        let e1 = h1.id().await.unwrap_err();
        assert!(matches!(e1, Error::PromiseCreation(_)), "got: {e1}");

        let _ = try_finalize_context(&ctx, Ok(0)).await;
    }

    #[tokio::test]
    async fn dropping_spawned_handle_aborts_inflight_task() {
        use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

        // Drops only when the task's future is dropped — either on normal
        // completion or on abort. Since the body parks forever and never
        // returns, the only way this runs is cancellation.
        struct Probe(Arc<AtomicBool>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.store(true, SeqCst);
            }
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let probe = Probe(cancelled.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            let _probe = probe;
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await; // park forever; only abort ends this
            Outcome::Done(Ok(()))
        });

        let sh = SpawnedHandle {
            id: "child".to_string(),
            handle,
        };

        // Ensure the task is actually running and parked before we drop.
        started_rx.await.unwrap();
        drop(sh); // abort-on-drop fires
        tokio::task::yield_now().await; // let the runtime process the abort

        assert!(
            cancelled.load(SeqCst),
            "parked task should have been aborted when its SpawnedHandle dropped"
        );
    }

    // ── Promise id format compliance (the server's rules, ported) ──

    /// Deep workflow used by the id-format tests: nested children, a durable
    /// sleep, and a detached child spawned from a *nested* context.
    struct IdFormatMid;
    impl Durable<(), i32> for IdFormatMid {
        const NAME: &'static str = "id_format_mid";
        const KIND: DurableKind = DurableKind::Workflow;
        async fn execute(&self, env: ExecutionEnv<'_>, _args: ()) -> crate::error::Result<i32> {
            let ctx = env.into_context();
            let a: i32 = ctx.run(ChildWithLeaves, ()).await?;
            // A global-scope timer promise: minted from the same seq as
            // everything else. Not awaited (it would suspend).
            let _ = ctx.sleep(Duration::from_secs(3600)).spawn()?;
            // Detached from a nested context: its id is minted off the
            // origin, not off this context, so its declared ancestors must be
            // the origin too.
            let _ = ctx.detached("tail", ()).spawn()?;
            Ok(a)
        }
    }

    async fn run_id_format_workflow(harness: &TestHarness, root: &str) -> Vec<serde_json::Value> {
        let effects = harness.build_effects(vec![]);
        let ctx = test_context(root, effects);
        let _: i32 = ctx.run(IdFormatMid, ()).await.unwrap();
        let _: i32 = ctx.run(IdFormatMid, ()).await.unwrap();
        let _ = finalize_context(&ctx, Ok(0)).await;

        harness
            .sent_requests_json()
            .await
            .into_iter()
            .filter(|r| r["kind"] == "promise.create")
            .collect()
    }

    #[tokio::test]
    async fn every_created_promise_passes_server_validation() {
        // A dotted root is a caller's prerogative: '.' is only read below the
        // origin, so every id minted under it still validates and still shares
        // that origin.
        for root in ["wf", "my.app.workflow"] {
            let harness = TestHarness::new();
            let creates = run_id_format_workflow(&harness, root).await;
            assert!(creates.len() >= 8, "expected a real tree: {:?}", creates);
            for create in &creates {
                let id = create["id"].as_str().unwrap();
                server_validate(id, &create["tags"]);
                assert_eq!(server_origin(id), root, "id {} origin", id);
            }
        }
    }

    #[tokio::test]
    async fn whole_workflow_shares_one_origin() {
        // The origin is the server's partition key and the unit both
        // promise.register_callback and task.suspend match on, so every
        // promise a workflow creates — detached children included — must
        // share it.
        let harness = TestHarness::new();
        let creates = run_id_format_workflow(&harness, "wf").await;
        for create in &creates {
            let id = create["id"].as_str().unwrap();
            assert_eq!(server_origin(id), "wf", "id {} origin", id);
            assert_eq!(
                create["tags"]["resonate:origin"].as_str().unwrap(),
                "wf",
                "id {} resonate:origin",
                id
            );
        }
    }

    #[tokio::test]
    async fn child_ids_are_colon_then_dot_separated() {
        let harness = TestHarness::new();
        let creates = run_id_format_workflow(&harness, "wf").await;
        let ids: Vec<&str> = creates.iter().map(|c| c["id"].as_str().unwrap()).collect();
        // First level below the root joins with ':', deeper levels with '.'.
        for want in ["wf:0", "wf:0.0", "wf:0.0.0"] {
            assert!(ids.contains(&want), "expected {} in {:?}", want, ids);
        }
        // No id keeps the old all-'.' shape.
        assert!(
            !ids.iter().any(|id| id.starts_with("wf.")),
            "old-shape ids: {:?}",
            ids
        );
    }

    #[tokio::test]
    async fn detached_ids_stay_bounded_below_the_origin() {
        // Detached ids are `{origin}:d{16 hex}` — one segment past the origin
        // no matter how deep the spawning context is.
        let harness = TestHarness::new();
        let creates = run_id_format_workflow(&harness, "wf").await;
        let detached: Vec<&serde_json::Value> = creates
            .iter()
            .filter(|c| c["id"].as_str().unwrap().starts_with("wf:d"))
            .collect();
        assert_eq!(detached.len(), 2, "one per IdFormatMid invocation");
        for create in detached {
            let id = create["id"].as_str().unwrap();
            let suffix = &id["wf:d".len()..];
            assert_eq!(suffix.len(), 16, "id {}", id);
            assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()), "id {}", id);
            // The id hangs off the origin, not off the spawning context, so
            // the origin is also the ancestor it declares; branch stays the
            // child's own id.
            assert_eq!(create["tags"]["resonate:parent"].as_str().unwrap(), "wf");
            assert_eq!(create["tags"]["resonate:branch"].as_str().unwrap(), id);
        }
    }

    #[tokio::test]
    async fn prefix_tag_is_not_emitted() {
        let harness = TestHarness::new();
        let creates = run_id_format_workflow(&harness, "wf").await;
        for create in &creates {
            assert!(
                create["tags"].get("resonate:prefix").is_none(),
                "promise {} emits resonate:prefix",
                create["id"]
            );
        }
    }
}
