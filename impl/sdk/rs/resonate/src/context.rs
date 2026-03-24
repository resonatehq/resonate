use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::codec::deserialize_error;
use crate::durable::Durable;
use crate::effects::Effects;
use crate::error::{Error, Result};
use crate::futures::{DurableFuture, RemoteFuture};
use crate::info::Info;
use crate::types::{DurableKind, Outcome, PromiseCreateReq, PromiseRecord, PromiseState, Value};

/// A function that resolves a logical target name to a routable address.
/// Mirrors `network.match()` — passed down from Resonate → Core → Context.
pub type MatchFn = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Shared state between a spawned task and the context's flush mechanism.
/// The DurableFuture also reads from this via a oneshot channel.
pub(crate) struct SpawnedTask {
    pub id: String,
    pub handle: tokio::task::JoinHandle<Outcome>,
}

/// The primary interface for workflow functions.
/// Provides two core operations: `run` (local execution) and `rpc` (remote execution).
///
/// Both return builder structs (`RunTask`, `RpcTask`) that implement `IntoFuture`,
/// so `.await` works seamlessly for the sequential case. Use `.spawn()` for true
/// parallelism via `tokio::spawn`, or `tokio::join!` for cooperative concurrency.
///
/// **Intentional divergence from TS SDK**: Context calls do not accept custom IDs.
/// IDs are always generated deterministically via `next_id()` (`parent.0`, `parent.1`, …).
/// This eliminates the need for `breaksLineage` logic — `resonate:origin` always matches
/// the root ID for the entire call tree. Custom IDs are only available at the top-level
/// `Resonate` API (`Resonate::run`, `Resonate::begin_run`, etc.).
pub struct Context {
    id: String,
    origin_id: String,
    branch_id: String,
    parent_id: String,
    func_name: String,
    timeout_at: i64,
    seq: AtomicU32,
    effects: Effects,
    match_fn: MatchFn,
    remote_todos: Arc<Mutex<Vec<String>>>,
    spawned_tasks: Arc<Mutex<Vec<SpawnedTask>>>,
}

// Context needs to be Sync for use in async functions, but AtomicU32 is already Sync.
// The Mutex-wrapped fields provide interior mutability safely.
unsafe impl Sync for Context {}

impl Context {
    /// Create a root context for a top-level task execution.
    pub(crate) fn root(
        id: String,
        timeout_at: i64,
        func_name: String,
        effects: Effects,
        match_fn: MatchFn,
    ) -> Self {
        Self {
            origin_id: id.clone(),
            branch_id: id.clone(),
            parent_id: String::new(),
            id,
            func_name,
            timeout_at,
            seq: AtomicU32::new(0),
            effects,
            match_fn,
            remote_todos: Arc::new(Mutex::new(Vec::new())),
            spawned_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create a child context for a nested workflow.
    fn child(&self, id: &str, func_name: &str, timeout_at: i64) -> Context {
        Context {
            id: id.to_string(),
            origin_id: self.origin_id.clone(),
            branch_id: id.to_string(),
            parent_id: self.id.clone(),
            func_name: func_name.to_string(),
            timeout_at,
            seq: AtomicU32::new(0),
            effects: self.effects.clone(),
            match_fn: self.match_fn.clone(),
            remote_todos: Arc::new(Mutex::new(Vec::new())),
            spawned_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Create an Info for a child leaf function.
    fn child_info(&self, id: &str, func_name: &str, timeout_at: i64) -> Info {
        Info::new(
            id.to_string(),
            self.id.clone(),
            self.origin_id.clone(),
            id.to_string(),
            timeout_at,
            func_name.to_string(),
            HashMap::new(),
        )
    }

    /// Generate the next deterministic child ID.
    fn next_id(&self) -> String {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        format!("{}.{}", self.id, seq)
    }

    /// Default timeout for child promises (24 hours), matching TS SDK.
    const DEFAULT_CHILD_TIMEOUT: Duration = Duration::from_secs(86_400);

    /// Calculate timeout for child promises.
    /// Computes `min(now + requested_timeout, parent_timeout)`, matching the
    /// TS SDK behavior: `Math.min(now + opts.timeout, parent.timeout)`.
    /// If no explicit timeout is provided, defaults to 24 hours.
    fn child_timeout(&self, requested: Option<Duration>) -> i64 {
        let timeout = requested.unwrap_or(Self::DEFAULT_CHILD_TIMEOUT);
        let now = now_ms();
        let child_deadline = now + timeout.as_millis() as i64;
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

    /// Build a local create request.
    fn local_create_req(
        &self,
        id: &str,
        args: &impl Serialize,
        timeout: Option<Duration>,
    ) -> PromiseCreateReq {
        let mut tags = HashMap::new();
        tags.insert("resonate:scope".to_string(), "local".to_string());
        tags.insert("resonate:branch".to_string(), self.branch_id.clone());
        tags.insert("resonate:parent".to_string(), self.id.clone());
        tags.insert("resonate:origin".to_string(), self.origin_id.clone());

        PromiseCreateReq {
            id: id.to_string(),
            timeout_at: self.child_timeout(timeout),
            param: Value {
                headers: None,
                data: Some(serde_json::to_value(args).unwrap_or(serde_json::Value::Null)),
            },
            tags,
        }
    }

    /// Build a remote create request.
    ///
    /// If `target_override` is provided, it is resolved through `match_fn`;
    /// otherwise `func_name` is used as the default target input.
    fn remote_create_req(
        &self,
        id: &str,
        func_name: &str,
        args: &impl Serialize,
        timeout: Option<Duration>,
        target_override: Option<&str>,
    ) -> PromiseCreateReq {
        let target_input = target_override.unwrap_or(func_name);
        let target = (self.match_fn)(target_input);
        let mut tags = HashMap::new();
        tags.insert("resonate:scope".to_string(), "global".to_string());
        tags.insert("resonate:target".to_string(), target);
        tags.insert("resonate:branch".to_string(), id.to_string());
        tags.insert("resonate:parent".to_string(), self.id.clone());
        tags.insert("resonate:origin".to_string(), self.origin_id.clone());

        PromiseCreateReq {
            id: id.to_string(),
            timeout_at: self.child_timeout(timeout),
            param: Value {
                headers: None,
                data: Some(serde_json::json!({
                    "func": func_name,
                    "args": serde_json::to_value(args).unwrap_or(serde_json::Value::Null),
                })),
            },
            tags,
        }
    }

    /// Convert a typed Result<T> into a Result<serde_json::Value>.
    fn to_json_result<T: Serialize>(result: &Result<T>) -> Result<serde_json::Value> {
        match result {
            Ok(val) => serde_json::to_value(val).map_err(|e| e.into()),
            Err(e) => Err(Error::Application {
                message: e.to_string(),
            }),
        }
    }

    /// Local execution. Returns a `RunTask` builder that implements `IntoFuture`.
    ///
    /// # Usage patterns
    /// ```ignore
    /// // Sequential (common case)
    /// let result: i32 = ctx.run(MyFunc, args).await?;
    ///
    /// // With timeout
    /// let result: i32 = ctx.run(MyFunc, args).timeout(Duration::from_secs(30)).await?;
    ///
    /// // Access promise ID before execution
    /// let task = ctx.run(MyFunc, args);
    /// let id = task.id().await?;
    /// let result = task.await?;
    ///
    /// // Concurrency via tokio::join!
    /// let (r1, r2) = tokio::join!(ctx.run(F, a), ctx.run(F, b));
    ///
    /// // True parallelism via .spawn()
    /// let handle = ctx.run(MyFunc, args).spawn().await?;
    /// let result = handle.await?;
    /// ```
    pub fn run<D, Args, T>(&self, func: D, args: Args) -> RunTask<'_, D, Args, T>
    where
        D: Durable<Args, T>,
        Args: Serialize,
    {
        let child_id = self.next_id();
        let req = self.local_create_req(&child_id, &args, None);
        RunTask {
            child_id,
            ctx: self,
            func,
            args,
            req,
            record: tokio::sync::OnceCell::new(),
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
    /// let handle = ctx.rpc::<i32>("func", &args).spawn().await?;
    /// ```
    pub fn rpc<T>(&self, func: &str, args: &impl Serialize) -> RpcTask<'_, T> {
        let child_id = self.next_id();
        let req = self.remote_create_req(&child_id, func, args, None, None);
        RpcTask {
            child_id,
            ctx: self,
            req,
            record: tokio::sync::OnceCell::new(),
            _phantom: PhantomData,
        }
    }

    /// Take all accumulated remote todos.
    pub(crate) async fn take_remote_todos(&self) -> Vec<String> {
        let mut todos = self.remote_todos.lock().await;
        std::mem::take(&mut *todos)
    }

    /// Flush all eagerly spawned local tasks.
    /// Awaits every spawned task's JoinHandle, collects remote_todos from
    /// any that suspended.
    pub(crate) async fn flush_local_work(&self) -> Vec<String> {
        let tasks = {
            let mut tasks = self.spawned_tasks.lock().await;
            std::mem::take(&mut *tasks)
        };

        let mut remote_todos = Vec::new();

        for task in tasks {
            match task.handle.await {
                Ok(Outcome::Done(_)) => {
                    // Already settled inside the spawned task
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

        remote_todos
    }

    #[allow(dead_code)]
    pub(crate) fn effects(&self) -> &Effects {
        &self.effects
    }
}

// ═══════════════════════════════════════════════════════════════
//  RunTask — builder returned by ctx.run()
// ═══════════════════════════════════════════════════════════════

/// A lazy local execution task. Created by `ctx.run()`.
///
/// Implements `IntoFuture` so `.await` works directly. The durable promise is
/// created lazily — at most once — triggered by whichever comes first: `.id()`,
/// `.spawn()`, or `.await`.
pub struct RunTask<'ctx, D, Args, T> {
    child_id: String,
    ctx: &'ctx Context,
    func: D,
    args: Args,
    req: PromiseCreateReq,
    record: tokio::sync::OnceCell<PromiseRecord>,
    _phantom: PhantomData<fn() -> T>,
}

impl<'ctx, D, Args, T> RunTask<'ctx, D, Args, T>
where
    Args: Serialize,
{
    /// Set an explicit timeout for the child promise (capped to parent's timeout).
    ///
    /// Must be called before `.id()`, `.await`, or `.spawn()`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        debug_assert!(self.record.get().is_none(), "cannot set timeout after promise creation");
        self.req.timeout_at = self.ctx.child_timeout(Some(timeout));
        self
    }

    /// Returns the promise ID, creating the durable promise if it hasn't been yet.
    pub async fn id(&self) -> Result<String> {
        self.ensure_created().await?;
        Ok(self.child_id.clone())
    }

    /// Ensure the durable promise exists (lazy creation via OnceCell).
    async fn ensure_created(&self) -> Result<&PromiseRecord> {
        if let Some(record) = self.record.get() {
            return Ok(record);
        }
        self.record
            .get_or_try_init(|| self.ctx.effects.create_promise(self.req.clone()))
            .await
    }

    /// Spawn the task onto a new tokio task for true parallelism.
    /// Replaces the old `ctx.begin_run()`.
    ///
    /// This is async because it eagerly creates the durable promise, then
    /// spawns execution via `tokio::spawn`. Requires `D: Send + 'static`
    /// because the function is moved into the spawned task.
    pub async fn spawn(self) -> Result<DurableFuture<T>>
    where
        D: Durable<Args, T> + Send + 'static,
        Args: Serialize + DeserializeOwned + Send + 'static,
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let RunTask {
            child_id,
            ctx,
            func,
            args,
            req,
            record: cell,
            _phantom,
        } = self;

        // Eagerly create the promise
        cell.get_or_try_init(|| ctx.effects.create_promise(req))
            .await?;
        let record = cell.into_inner().unwrap();

        match record.state {
            PromiseState::Resolved => Ok(DurableFuture::completed(record.value.data_or_null())),
            PromiseState::Rejected
            | PromiseState::RejectedCanceled
            | PromiseState::RejectedTimedout => {
                Ok(DurableFuture::failed(record.value.data_or_null()))
            }
            PromiseState::Pending => {
                let effects = ctx.effects.clone();
                let child_id_for_task = child_id.clone();
                let parent_remote_todos = ctx.remote_todos.clone();
                let (tx, rx) = tokio::sync::oneshot::channel();

                let handle = match D::KIND {
                    DurableKind::Function => {
                        let info = ctx.child_info(&child_id, D::NAME, record.timeout_at);
                        tokio::spawn(async move {
                            let result = func.execute(None, Some(&info), args).await;
                            let json_result = match &result {
                                Ok(val) => {
                                    serde_json::to_value(val).map_err(Error::SerializationError)
                                }
                                Err(e) => Err(Error::Application {
                                    message: e.to_string(),
                                }),
                            };
                            let _ = effects
                                .settle_promise(&child_id_for_task, &json_result)
                                .await;
                            let outcome = Outcome::Done(json_result);
                            let _ = tx.send(outcome_to_sendable(&outcome));
                            outcome
                        })
                    }
                    DurableKind::Workflow => {
                        let child_ctx = ctx.child(&child_id, D::NAME, record.timeout_at);
                        tokio::spawn(async move {
                            let result = func.execute(Some(&child_ctx), None, args).await;
                            let flush_remote = child_ctx.flush_local_work().await;
                            let mut child_remote = child_ctx.take_remote_todos().await;
                            child_remote.extend(flush_remote);

                            let json_result = match &result {
                                Ok(val) => {
                                    serde_json::to_value(val).map_err(Error::SerializationError)
                                }
                                Err(e) => Err(Error::Application {
                                    message: e.to_string(),
                                }),
                            };

                            let outcome = if child_remote.is_empty() {
                                let _ = effects
                                    .settle_promise(&child_id_for_task, &json_result)
                                    .await;
                                Outcome::Done(json_result)
                            } else {
                                parent_remote_todos
                                    .lock()
                                    .await
                                    .extend(child_remote.clone());
                                Outcome::Suspended {
                                    remote_todos: child_remote,
                                }
                            };
                            let _ = tx.send(outcome_to_sendable(&outcome));
                            outcome
                        })
                    }
                };

                // Track for flush
                {
                    let mut tasks = ctx.spawned_tasks.lock().await;
                    tasks.push(SpawnedTask {
                        id: child_id.clone(),
                        handle,
                    });
                }

                Ok(DurableFuture::pending(child_id, rx))
            }
        }
    }
}

impl<'ctx, D, Args, T> IntoFuture for RunTask<'ctx, D, Args, T>
where
    D: Durable<Args, T>,
    Args: Serialize + DeserializeOwned + Send + 'static,
    T: Serialize + DeserializeOwned + Send + 'static,
{
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<T>> + Send + 'ctx>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let RunTask {
                child_id,
                ctx,
                func,
                args,
                req,
                record: cell,
                _phantom,
            } = self;

            // Ensure promise is created, then take ownership
            cell.get_or_try_init(|| ctx.effects.create_promise(req))
                .await?;
            let record = cell.into_inner().unwrap();

            match record.state {
                PromiseState::Resolved => Ok(serde_json::from_value(record.value.data_or_null())?),
                PromiseState::Rejected
                | PromiseState::RejectedCanceled
                | PromiseState::RejectedTimedout => {
                    Err(deserialize_error(record.value.data_or_null()))
                }
                PromiseState::Pending => {
                    let timeout_at = record.timeout_at;
                    match D::KIND {
                        DurableKind::Function => {
                            let info = ctx.child_info(&child_id, D::NAME, timeout_at);
                            let result = func.execute(None, Some(&info), args).await;
                            let json_result = Context::to_json_result(&result);
                            ctx.effects.settle_promise(&child_id, &json_result).await?;
                            result
                        }
                        DurableKind::Workflow => {
                            let child_ctx = ctx.child(&child_id, D::NAME, timeout_at);
                            let result = func.execute(Some(&child_ctx), None, args).await;

                            let flush_remote = child_ctx.flush_local_work().await;
                            let mut child_remote = child_ctx.take_remote_todos().await;
                            child_remote.extend(flush_remote);

                            if child_remote.is_empty() {
                                let json_result = Context::to_json_result(&result);
                                ctx.effects.settle_promise(&child_id, &json_result).await?;
                            } else {
                                ctx.remote_todos.lock().await.extend(child_remote);
                            }
                            result
                        }
                    }
                }
            }
        })
    }
}

// ═══════════════════════════════════════════════════════════════
//  RpcTask — builder returned by ctx.rpc()
// ═══════════════════════════════════════════════════════════════

/// A lazy remote execution task. Created by `ctx.rpc()`.
///
/// Implements `IntoFuture` so `.await` works directly. On `Pending` state,
/// awaiting pushes to `remote_todos` and returns `Err(Suspended)`.
pub struct RpcTask<'ctx, T> {
    child_id: String,
    ctx: &'ctx Context,
    req: PromiseCreateReq,
    record: tokio::sync::OnceCell<PromiseRecord>,
    _phantom: PhantomData<T>,
}

impl<'ctx, T> RpcTask<'ctx, T> {
    /// Set an explicit timeout for the child promise (capped to parent's timeout).
    ///
    /// Must be called before `.id()`, `.await`, or `.spawn()`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        debug_assert!(self.record.get().is_none(), "cannot set timeout after promise creation");
        self.req.timeout_at = self.ctx.child_timeout(Some(timeout));
        self
    }

    /// Override the target for this RPC call (resolved through `match_fn`).
    ///
    /// Must be called before `.id()`, `.await`, or `.spawn()`.
    pub fn target(mut self, target: &str) -> Self {
        debug_assert!(self.record.get().is_none(), "cannot set target after promise creation");
        let resolved = (self.ctx.match_fn)(target);
        self.req
            .tags
            .insert("resonate:target".to_string(), resolved);
        self
    }

    /// Returns the promise ID, creating the durable promise if it hasn't been yet.
    pub async fn id(&self) -> Result<String> {
        self.ensure_created().await?;
        Ok(self.child_id.clone())
    }

    /// Ensure the durable promise exists (lazy creation via OnceCell).
    async fn ensure_created(&self) -> Result<&PromiseRecord> {
        if let Some(record) = self.record.get() {
            return Ok(record);
        }
        self.record
            .get_or_try_init(|| self.ctx.effects.create_promise(self.req.clone()))
            .await
    }

    /// Eagerly create the promise and return a `RemoteFuture` handle.
    /// Replaces the old `ctx.begin_rpc()`.
    pub async fn spawn(self) -> Result<RemoteFuture<T>>
    where
        T: DeserializeOwned,
    {
        let RpcTask {
            child_id,
            ctx,
            req,
            record: cell,
            _phantom,
        } = self;

        // Eagerly create the promise
        cell.get_or_try_init(|| ctx.effects.create_promise(req))
            .await?;
        let record = cell.into_inner().unwrap();

        match record.state {
            PromiseState::Resolved => Ok(RemoteFuture::completed(record.value.data_or_null())),
            PromiseState::Rejected
            | PromiseState::RejectedCanceled
            | PromiseState::RejectedTimedout => {
                Ok(RemoteFuture::failed(record.value.data_or_null()))
            }
            PromiseState::Pending => {
                ctx.remote_todos.lock().await.push(child_id.clone());
                Ok(RemoteFuture::pending(child_id, ctx.effects.clone()))
            }
        }
    }
}

impl<'ctx, T> IntoFuture for RpcTask<'ctx, T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<T>> + Send + 'ctx>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let RpcTask {
                child_id,
                ctx,
                req,
                record: cell,
                _phantom,
            } = self;

            // Ensure promise is created, then take ownership
            cell.get_or_try_init(|| ctx.effects.create_promise(req))
                .await?;
            let record = cell.into_inner().unwrap();

            match record.state {
                PromiseState::Resolved => Ok(serde_json::from_value(record.value.data_or_null())?),
                PromiseState::Rejected
                | PromiseState::RejectedCanceled
                | PromiseState::RejectedTimedout => {
                    Err(deserialize_error(record.value.data_or_null()))
                }
                PromiseState::Pending => {
                    ctx.remote_todos.lock().await.push(child_id);
                    Err(Error::Suspended)
                }
            }
        })
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Convert an Outcome reference to a sendable form for the oneshot channel.
fn outcome_to_sendable(outcome: &Outcome) -> Outcome {
    match outcome {
        Outcome::Done(result) => match result {
            Ok(v) => Outcome::Done(Ok(v.clone())),
            Err(e) => Outcome::Done(Err(Error::Application {
                message: e.to_string(),
            })),
        },
        Outcome::Suspended { remote_todos } => Outcome::Suspended {
            remote_todos: remote_todos.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::Durable;
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
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            Ok(42)
        }
    }

    struct Baz;
    impl Durable<(), i32> for Baz {
        const NAME: &'static str = "baz";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            Ok(31416)
        }
    }

    struct Add;
    impl Durable<(i32, i32), i32> for Add {
        const NAME: &'static str = "add";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            args: (i32, i32),
        ) -> crate::error::Result<i32> {
            Ok(args.0 + args.1)
        }
    }

    struct Double;
    impl Durable<i32, i32> for Double {
        const NAME: &'static str = "double";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            args: i32,
        ) -> crate::error::Result<i32> {
            Ok(args * 2)
        }
    }

    struct Square;
    impl Durable<i32, i32> for Square {
        const NAME: &'static str = "square";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            args: i32,
        ) -> crate::error::Result<i32> {
            Ok(args * args)
        }
    }

    struct Multiply;
    impl Durable<(i32, i32), i32> for Multiply {
        const NAME: &'static str = "multiply";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            args: (i32, i32),
        ) -> crate::error::Result<i32> {
            Ok(args.0 * args.1)
        }
    }

    struct Failing;
    impl Durable<(), i32> for Failing {
        const NAME: &'static str = "failing";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            Err(Error::Application {
                message: "boom".to_string(),
            })
        }
    }

    struct Noop;
    impl Durable<(), ()> for Noop {
        const NAME: &'static str = "noop";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }

    struct Concat;
    impl Durable<(String, String, String), String> for Concat {
        const NAME: &'static str = "concat";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            args: (String, String, String),
        ) -> crate::error::Result<String> {
            Ok(format!("{}-{}-{}", args.0, args.1, args.2))
        }
    }

    struct Slow;
    impl Durable<(), i32> for Slow {
        const NAME: &'static str = "slow";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            Ok(1)
        }
    }

    struct Fast;
    impl Durable<(), i32> for Fast {
        const NAME: &'static str = "fast";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            Ok(2)
        }
    }

    use std::sync::atomic::AtomicI32;
    static CALL_COUNT: AtomicI32 = AtomicI32::new(0);

    struct Counter;
    impl Durable<(), i32> for Counter {
        const NAME: &'static str = "counter";
        const KIND: DurableKind = DurableKind::Function;
        async fn execute(
            &self,
            _ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            let val = CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(val + 1)
        }
    }

    // Child workflow: calls rpc, returns v * 2
    struct ChildWorkflow;
    impl Durable<(), i32> for ChildWorkflow {
        const NAME: &'static str = "child";
        const KIND: DurableKind = DurableKind::Workflow;
        async fn execute(
            &self,
            ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            let ctx = ctx.unwrap();
            let v: i32 = ctx.rpc("remoteFunc", &()).await?;
            Ok(v * 2)
        }
    }

    // Child workflow that calls a leaf twice
    struct ChildWithLeaves;
    impl Durable<(), i32> for ChildWithLeaves {
        const NAME: &'static str = "child_with_leaves";
        const KIND: DurableKind = DurableKind::Workflow;
        async fn execute(
            &self,
            ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            let ctx = ctx.unwrap();
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
        async fn execute(
            &self,
            ctx: Option<&Context>,
            _info: Option<&Info>,
            _args: (),
        ) -> crate::error::Result<i32> {
            let ctx = ctx.unwrap();
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(val))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(42)),
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(a + b))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(31458)),
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
        let local_future = ctx.run(Bar, ()).spawn().await.unwrap();
        let _remote_future: RemoteFuture<i32> = ctx.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let local_val: i32 = local_future.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(local_val))).await;

        let remote_id = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
                remote_todos[0].clone()
            }
            other => panic!("expected Suspended, got {:?}", other),
        };

        // Settle the remote promise in the stub
        harness
            .settle_promise_in_stub(&remote_id, serde_json::json!(100))
            .await;

        // Second execution: remote promise is now resolved via preload
        let effects2 =
            harness.build_effects(vec![resolved_promise(&remote_id, serde_json::json!(100))]);
        let ctx2 = test_context("root", effects2);

        let local_future2 = ctx2.run(Bar, ()).spawn().await.unwrap();
        let remote_future2: RemoteFuture<i32> = ctx2.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let local_val2: i32 = local_future2.await.unwrap();
        let remote_val2: i32 = remote_future2.await.unwrap();
        let outcome2 =
            finalize_context(&ctx2, Ok(serde_json::json!(local_val2 + remote_val2))).await;

        match outcome2 {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(142)),
            other => panic!("expected Done after settlement, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn structured_concurrency_multiple_remotes_require_multiple_settle_cycles() {
        let harness = TestHarness::new();

        // First execution: two remotes, both pending
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);
        let _r1 = ctx.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let _r2 = ctx.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(99))).await;

        let todos = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 2);
                remote_todos.clone()
            }
            other => panic!("expected Suspended with 2 todos, got {:?}", other),
        };

        // Settle first remote
        harness
            .settle_promise_in_stub(&todos[0], serde_json::json!(10))
            .await;

        // Second execution: one resolved, one still pending
        let effects2 =
            harness.build_effects(vec![resolved_promise(&todos[0], serde_json::json!(10))]);
        let ctx2 = test_context("root", effects2);
        let _r1 = ctx2.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let _r2 = ctx2.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let outcome2 = finalize_context(&ctx2, Ok(serde_json::json!(99))).await;

        match &outcome2 {
            Outcome::Suspended { remote_todos } => {
                assert_eq!(remote_todos.len(), 1);
            }
            other => panic!("expected Suspended with 1 todo, got {:?}", other),
        }

        // Settle second remote
        harness
            .settle_promise_in_stub(&todos[1], serde_json::json!(20))
            .await;

        // Third execution: both resolved
        let effects3 = harness.build_effects(vec![
            resolved_promise(&todos[0], serde_json::json!(10)),
            resolved_promise(&todos[1], serde_json::json!(20)),
        ]);
        let ctx3 = test_context("root", effects3);
        let _r1 = ctx3.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let _r2 = ctx3.rpc::<i32>("bar", &()).spawn().await.unwrap();
        let outcome3 = finalize_context(&ctx3, Ok(serde_json::json!(99))).await;

        match outcome3 {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(99)),
            other => panic!("expected Done(99), got {:?}", other),
        }
    }

    // ── Structured concurrency ─────────────────────────────────────

    #[tokio::test]
    async fn fire_and_forget_local_leaves_flushed_at_return() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _f1 = ctx.run(Bar, ()).spawn().await.unwrap();
        let _f2 = ctx.run(Baz, ()).spawn().await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(99))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(99)),
            other => panic!("expected Done(99), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn mixed_local_fire_and_forget_plus_remote_suspends_on_remote() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _local = ctx.run(Bar, ()).spawn().await.unwrap();
        let _remote = ctx.rpc::<i32>("someRemote", &()).spawn().await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(77))).await;

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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(msg))).await;

        match outcome {
            Outcome::Done(Ok(v)) => {
                let s = v.as_str().unwrap();
                assert!(s.contains("caught:"), "got: {}", s);
                assert!(s.contains("boom"), "got: {}", s);
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

        let f_slow = ctx.run(Slow, ()).spawn().await.unwrap();
        let f_fast = ctx.run(Fast, ()).spawn().await.unwrap();
        let a: i32 = f_slow.await.unwrap();
        let b: i32 = f_fast.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(a + b))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(3)),
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
            Ok(v) => Ok(serde_json::json!(v + 1)),
            Err(Error::Suspended) => Err(Error::Suspended),
            Err(e) => Err(e),
        };
        let outcome = finalize_context(&ctx, workflow_result).await;

        let remote_id = match &outcome {
            Outcome::Suspended { remote_todos } => {
                assert!(remote_todos.len() >= 1, "expected at least 1 remote todo");
                remote_todos[0].clone()
            }
            other => panic!("expected Suspended, got {:?}", other),
        };

        // Settle the remote promise
        harness
            .settle_promise_in_stub(&remote_id, serde_json::json!(21))
            .await;

        // Second execution: remote resolved, child completes, parent completes
        let effects2 =
            harness.build_effects(vec![resolved_promise(&remote_id, serde_json::json!(21))]);
        let ctx2 = test_context("foo", effects2);

        let result2: crate::error::Result<i32> = ctx2.run(ChildWorkflow, ()).await;
        let workflow_result2 = match result2 {
            Ok(v) => Ok(serde_json::json!(v + 1)),
            Err(e) => Err(e),
        };
        let outcome2 = finalize_context(&ctx2, workflow_result2).await;

        match outcome2 {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(43)),
            other => panic!("expected Done(43), got {:?}", other),
        }
    }

    // ── Double-await ───────────────────────────────────────────────
    // NOTE: In Rust, DurableFuture::await_result() consumes self (takes ownership).
    // Double-await is prevented at compile time by Rust's ownership model.
    // We test that a single await works correctly.

    #[tokio::test]
    async fn awaiting_a_durable_future_returns_correct_value() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let future = ctx.run(Bar, ()).spawn().await.unwrap();
        let v: i32 = future.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(v))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(42)),
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(result))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(7)),
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(a + b))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(19)),
            other => panic!("expected Done(19), got {:?}", other),
        }
    }

    // ── Remote only ────────────────────────────────────────────────

    #[tokio::test]
    async fn single_rpc_suspends_with_awaited_id() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result: crate::error::Result<i32> = ctx.rpc("remoteFunc", &()).await;
        let workflow_result = match result {
            Ok(v) => Ok(serde_json::json!(v)),
            Err(Error::Suspended) => Err(Error::Suspended),
            Err(e) => Err(e),
        };
        let outcome = finalize_context(&ctx, workflow_result).await;

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

        let _a = ctx.rpc::<i32>("a", &()).spawn().await.unwrap();
        let _b = ctx.rpc::<i32>("b", &()).spawn().await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(0))).await;

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
        let remote_result: crate::error::Result<i32> = ctx.rpc("remoteFunc", &()).await;
        let workflow_result = match remote_result {
            Ok(v) => Ok(serde_json::json!(local_val + v)),
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

        let local_future = ctx.run(Multiply, (3, 7)).spawn().await.unwrap();
        let _remote_future = ctx.rpc::<i32>("remote", &()).spawn().await.unwrap();
        let local_val: i32 = local_future.await.unwrap();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(local_val))).await;

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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(v))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(7)),
            other => panic!("expected Done(7), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn regular_function_rejects_when_function_throws() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result: crate::error::Result<i32> = ctx.run(Failing, ()).await;
        let workflow_result = result.map(|v| serde_json::json!(v));
        let outcome = finalize_context(&ctx, workflow_result).await;

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
        let outcome = finalize_context(&ctx, Ok(serde_json::Value::Null)).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::Value::Null),
            other => panic!("expected Done(null), got {:?}", other),
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(v))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!("x-y-z")),
            other => panic!("expected Done(x-y-z), got {:?}", other),
        }
    }

    // ── Error handling (dispatch) ──────────────────────────────────

    #[tokio::test]
    async fn local_function_that_throws_results_in_rejected() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("main", effects);

        let result = ctx.run(Failing, ()).await.map(|v| serde_json::json!(v));
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(v))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(1)),
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

        let requests = harness.sent_requests().await;
        let create_req = requests.iter().find_map(|r| {
            if let crate::send::Request::PromiseCreate(c) = r {
                Some(c)
            } else {
                None
            }
        });
        assert!(create_req.is_some(), "should have sent a PromiseCreate");
        let create = create_req.unwrap();
        assert_eq!(create.tags.get("resonate:scope").unwrap(), "local");
        assert_eq!(create.tags.get("resonate:parent").unwrap(), "root");
        assert_eq!(create.tags.get("resonate:origin").unwrap(), "root");
        assert!(create.tags.contains_key("resonate:branch"));
    }

    #[tokio::test]
    async fn remote_call_sets_correct_tags() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _: crate::error::Result<i32> = ctx.rpc("remote", &()).await;

        let requests = harness.sent_requests().await;
        let create_req = requests.iter().find_map(|r| {
            if let crate::send::Request::PromiseCreate(c) = r {
                Some(c)
            } else {
                None
            }
        });
        assert!(create_req.is_some());
        let create = create_req.unwrap();
        assert_eq!(create.tags.get("resonate:scope").unwrap(), "global");
        assert_eq!(create.tags.get("resonate:target").unwrap(), "remote");
        assert_eq!(create.tags.get("resonate:parent").unwrap(), "root");
        assert_eq!(create.tags.get("resonate:origin").unwrap(), "root");
        assert!(create.tags.contains_key("resonate:branch"));
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

        let requests = harness.sent_requests().await;
        let creates: Vec<_> = requests
            .iter()
            .filter_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();

        // All created promises should have origin == "root"
        for create in &creates {
            assert_eq!(
                create.tags.get("resonate:origin").unwrap(),
                "root",
                "promise {} should have origin 'root'",
                create.id,
            );
        }
        // Verify we got the nested ones too (root.0, root.0.0, root.0.1, root.1)
        let ids: Vec<&str> = creates.iter().map(|c| c.id.as_str()).collect();
        assert!(ids.contains(&"root.0"), "should have root.0");
        assert!(ids.contains(&"root.0.0"), "should have root.0.0");
        assert!(ids.contains(&"root.0.1"), "should have root.0.1");
        assert!(ids.contains(&"root.1"), "should have root.1");
    }

    // ── Match Function (target resolution) ─────────────────────────

    #[tokio::test]
    async fn rpc_target_is_resolved_through_match_fn() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("local://any@{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        let _: crate::error::Result<i32> = ctx.rpc("hello", &()).await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "local://any@hello"
        );
    }

    #[tokio::test]
    async fn rpc_target_uses_custom_prefix_from_match_fn() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("http://server:8001/workers/{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        let _: crate::error::Result<String> = ctx.rpc("my_func", &42i32).await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "http://server:8001/workers/my_func"
        );
    }

    #[tokio::test]
    async fn rpc_spawn_target_is_resolved_through_match_fn() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("remote://group/{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        let _future = ctx.rpc::<i32>("greet", &"world").spawn().await.unwrap();

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "remote://group/greet"
        );
    }

    #[tokio::test]
    async fn identity_match_fn_passes_target_through_unchanged() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // Identity match — no transformation
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| target.to_string());
        let ctx = test_context_with_match("root", effects, match_fn);

        let _: crate::error::Result<i32> = ctx.rpc("bare_name", &()).await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(create.tags.get("resonate:target").unwrap(), "bare_name");
    }

    #[tokio::test]
    async fn match_fn_propagates_through_multiple_rpc_calls() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);

        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("custom://{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        // First rpc call
        let _: crate::error::Result<i32> = ctx.rpc("func_a", &()).await;
        // Second rpc call — same context, match_fn should still work
        let _: crate::error::Result<i32> = ctx.rpc("func_b", &()).await;

        let requests = harness.sent_requests().await;
        let creates: Vec<_> = requests
            .iter()
            .filter_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(creates.len(), 2);
        assert_eq!(
            creates[0].tags.get("resonate:target").unwrap(),
            "custom://func_a"
        );
        assert_eq!(
            creates[1].tags.get("resonate:target").unwrap(),
            "custom://func_b"
        );
    }

    #[tokio::test]
    async fn match_fn_skips_rewrite_when_target_is_already_a_url() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // This match_fn would prefix "local://any@" — but it should NOT
        // be called when the target already looks like a URL.
        let match_fn: crate::context::MatchFn = std::sync::Arc::new(|target: &str| {
            if target.contains("://") {
                target.to_string()
            } else {
                format!("local://any@{}", target)
            }
        });
        let ctx = test_context_with_match("root", effects, match_fn);

        // Pass a full URL as the function name — should be kept as-is
        let _: crate::error::Result<i32> =
            ctx.rpc("http://other-host:8001/workers/hello", &()).await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "http://other-host:8001/workers/hello",
            "URL target should pass through unchanged"
        );
    }

    #[tokio::test]
    async fn match_fn_rewrites_bare_name_but_not_url() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        // Simulates real behavior: bare names get rewritten, URLs don't
        let match_fn: crate::context::MatchFn = std::sync::Arc::new(|target: &str| {
            if target.contains("://") {
                target.to_string()
            } else {
                format!("local://any@{}", target)
            }
        });
        let ctx = test_context_with_match("root", effects, match_fn);

        // Bare name — should be rewritten
        let _: crate::error::Result<i32> = ctx.rpc("hello", &()).await;
        // URL — should NOT be rewritten
        let _: crate::error::Result<i32> = ctx
            .rpc("https://remote.example.com/workers/greet", &())
            .await;

        let requests = harness.sent_requests().await;
        let creates: Vec<_> = requests
            .iter()
            .filter_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(creates.len(), 2);
        assert_eq!(
            creates[0].tags.get("resonate:target").unwrap(),
            "local://any@hello",
            "bare name should be rewritten by match"
        );
        assert_eq!(
            creates[1].tags.get("resonate:target").unwrap(),
            "https://remote.example.com/workers/greet",
            "URL should pass through unchanged"
        );
    }

    #[tokio::test]
    async fn local_run_does_not_set_target_tag() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("SHOULD_NOT_APPEAR://{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        // ctx.run uses local_create_req, not remote_create_req
        let _: i32 = ctx.run(Bar, ()).await.unwrap();

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        // Local calls set scope=local and should NOT have resonate:target
        assert_eq!(create.tags.get("resonate:scope").unwrap(), "local");
        assert!(
            !create.tags.contains_key("resonate:target"),
            "local run should not set resonate:target"
        );
    }

    // ── Target override via rpc_with_opts / begin_rpc_with_opts ───

    #[tokio::test]
    async fn rpc_with_target_builder_overrides_func_name() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("local://any@{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        let _: crate::error::Result<i32> =
            ctx.rpc::<i32>("my_func", &()).target("custom-target").await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "local://any@custom-target",
            "custom target should override func_name in match_fn"
        );
    }

    #[tokio::test]
    async fn rpc_default_target_uses_func_name() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("local://any@{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        let _: crate::error::Result<i32> = ctx.rpc::<i32>("my_func", &()).await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "local://any@my_func",
            "None target should fall back to func_name"
        );
    }

    #[tokio::test]
    async fn rpc_with_url_target_passes_through() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn = std::sync::Arc::new(|target: &str| {
            if target.contains("://") {
                target.to_string()
            } else {
                format!("local://any@{}", target)
            }
        });
        let ctx = test_context_with_match("root", effects, match_fn);

        let _: crate::error::Result<i32> = ctx
            .rpc::<i32>("my_func", &())
            .target("https://remote:9000/workers/foo")
            .await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
            "https://remote:9000/workers/foo",
            "URL target should pass through unchanged"
        );
    }

    #[tokio::test]
    async fn rpc_spawn_with_target_builder() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let match_fn: crate::context::MatchFn =
            std::sync::Arc::new(|target: &str| format!("remote://{}", target));
        let ctx = test_context_with_match("root", effects, match_fn);

        let _ = ctx
            .rpc::<i32>("my_func", &())
            .target("override-target")
            .spawn()
            .await
            .unwrap();

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.tags.get("resonate:target").unwrap(),
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

        let requests = harness.sent_requests().await;
        let create_ids: Vec<String> = requests
            .iter()
            .filter_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c.id.clone())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(create_ids[0], "root.0");
        assert_eq!(create_ids[1], "root.1");
        assert_eq!(create_ids[2], "root.2");
    }

    #[tokio::test]
    async fn nested_calls_produce_hierarchical_ids() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let _: i32 = ctx.run(ChildWithLeaves, ()).await.unwrap();

        let requests = harness.sent_requests().await;
        let create_ids: Vec<String> = requests
            .iter()
            .filter_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c.id.clone())
                } else {
                    None
                }
            })
            .collect();

        assert!(create_ids.contains(&"root.0".to_string()));
        assert!(create_ids.contains(&"root.0.0".to_string()));
        assert!(create_ids.contains(&"root.0.1".to_string()));
    }

    // ── Concurrent vs Sequential execution ─────────────────────────

    #[tokio::test]
    async fn concurrent_execution_spawn_is_actually_concurrent() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);
        let start = tokio::time::Instant::now();

        let f1 = ctx.run(Slow, ()).spawn().await.unwrap();
        let f2 = ctx.run(Fast, ()).spawn().await.unwrap();
        let a: i32 = f1.await.unwrap();
        let b: i32 = f2.await.unwrap();
        let elapsed = start.elapsed();
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(a + b))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(3)),
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
        let outcome = finalize_context(&ctx, Ok(serde_json::json!(a + b))).await;

        match outcome {
            Outcome::Done(Ok(v)) => assert_eq!(v, serde_json::json!(31458)),
            other => panic!("expected Done(31458), got {:?}", other),
        }
    }

    // ── Error propagation ──────────────────────────────────────────

    #[tokio::test]
    async fn leaf_throwing_error_propagates_to_workflow_result() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let ctx = test_context("root", effects);

        let result = ctx
            .run(Failing, ())
            .await
            .map(|v: i32| serde_json::json!(v));
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

        let result = ctx
            .run(InnerFailing, ())
            .await
            .map(|v: i32| serde_json::json!(v));
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

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        // Default child timeout is 24h, which exceeds parent's 5s → clamped
        assert!(
            create.timeout_at <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create.timeout_at,
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

        let _: crate::error::Result<i32> = ctx.rpc("remote_func", &()).await;

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert!(
            create.timeout_at <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create.timeout_at,
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

        let _future = ctx.run(Bar, ()).spawn().await.unwrap();

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert!(
            create.timeout_at <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create.timeout_at,
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

        let _future = ctx.rpc::<i32>("remote", &()).spawn().await.unwrap();

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert!(
            create.timeout_at <= parent_timeout,
            "child timeout_at ({}) should be <= parent timeout_at ({})",
            create.timeout_at,
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

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        // Should be approximately now + 10s, not parent's 60s
        let expected_approx = now + 10_000;
        let tolerance = 1_000; // 1 second tolerance for test execution time
        assert!(
            create.timeout_at >= expected_approx - tolerance
                && create.timeout_at <= expected_approx + tolerance,
            "child timeout_at ({}) should be ~{} (now + 10s), not parent timeout_at ({})",
            create.timeout_at,
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

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.timeout_at, parent_timeout,
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

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        let expected_approx = now + 10_000;
        let tolerance = 1_000;
        assert!(
            create.timeout_at >= expected_approx - tolerance
                && create.timeout_at <= expected_approx + tolerance,
            "child timeout_at ({}) should be ~{} (now + 10s)",
            create.timeout_at,
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
        let _future = ctx
            .rpc::<i32>("remote", &())
            .timeout(child_timeout)
            .spawn()
            .await
            .unwrap();

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        assert_eq!(
            create.timeout_at, parent_timeout,
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

        let requests = harness.sent_requests().await;
        let create = requests
            .iter()
            .find_map(|r| {
                if let crate::send::Request::PromiseCreate(c) = r {
                    Some(c)
                } else {
                    None
                }
            })
            .expect("should have sent promise.create");

        let now = super::now_ms();
        let expected_24h = now + 86_400_000; // 24 hours in ms
        let tolerance = 2_000; // 2 second tolerance

        // With i64::MAX parent, child should get ~now + 24h
        assert!(
            create.timeout_at >= expected_24h - tolerance
                && create.timeout_at <= expected_24h + tolerance,
            "child timeout_at ({}) should be ~{} (now + 24h), got diff={}ms",
            create.timeout_at,
            expected_24h,
            (create.timeout_at - expected_24h).abs()
        );
    }
}
