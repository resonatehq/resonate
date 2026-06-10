use std::future::IntoFuture;
use std::pin::Pin;

use crate::error::{Error, Result};

/// State of the background promise creation backing a spawned handle.
///
/// Broadcast through a `tokio::sync::watch` channel that doubles as the
/// create-promise chain link: the background task moves the state out of
/// `InFlight` exactly once, right after `create_promise` completes. Handles
/// gate `id()` on this — the promise ID is only handed out once the durable
/// promise is known to exist on the server — and the next creation in the
/// chain gates on its predecessor reaching `Created`.
#[derive(Clone, Debug)]
pub(crate) enum CreationState {
    InFlight,
    Created,
    Failed(String),
}

/// Create a `watch` channel for a creation's state, starting `InFlight`.
pub(crate) fn creation_channel() -> (
    tokio::sync::watch::Sender<CreationState>,
    tokio::sync::watch::Receiver<CreationState>,
) {
    tokio::sync::watch::channel(CreationState::InFlight)
}

/// Wait until the creation state leaves `InFlight`, then map it to the ID.
/// The ID is only returned on confirmed server-side creation. Shared by every
/// handle type's `id()` so the gating logic lives in one place.
async fn await_created_id(
    id: &str,
    created: &tokio::sync::watch::Receiver<CreationState>,
) -> Result<String> {
    let mut rx = created.clone();
    let state = rx
        .wait_for(|s| !matches!(s, CreationState::InFlight))
        .await
        .map_err(|_| Error::JoinError(format!("task {} was dropped", id)))?;
    match &*state {
        CreationState::Created => Ok(id.to_string()),
        CreationState::Failed(msg) => Err(Error::PromiseCreation(msg.clone())),
        CreationState::InFlight => unreachable!("wait_for excludes InFlight"),
    }
}

/// Shared state of a spawned-task handle: the promise ID, the creation gate,
/// and the typed result channel. `DurableFuture` and `RemoteFuture` are thin
/// wrappers over this.
struct Handle<T> {
    id: String,
    created: tokio::sync::watch::Receiver<CreationState>,
    receiver: tokio::sync::oneshot::Receiver<Result<T>>,
}

impl<T> Handle<T> {
    /// Wait until the creation state leaves `InFlight`, then map it to the ID.
    /// The ID is only returned on confirmed server-side creation.
    async fn id(&self) -> Result<String> {
        await_created_id(&self.id, &self.created).await
    }

    /// Await the typed result delivered by the background task.
    async fn recv(self) -> Result<T> {
        self.receiver
            .await
            .map_err(|_| Error::JoinError(format!("task {} was dropped", self.id)))?
    }
}

/// A handle to an eagerly spawned local durable task.
///
/// Created by `ctx.run(F, args).spawn()`. Awaiting this future returns the
/// result once the spawned task completes. `id()` returns the durable promise
/// ID once the promise has been successfully created on the server.
pub struct DurableFuture<T>(Handle<T>);

impl<T> DurableFuture<T> {
    pub(crate) fn pending(
        id: String,
        receiver: tokio::sync::oneshot::Receiver<Result<T>>,
        created: tokio::sync::watch::Receiver<CreationState>,
    ) -> Self {
        Self(Handle {
            id,
            created,
            receiver,
        })
    }

    /// Returns the durable promise ID once the promise has been successfully
    /// created on the server. Fails if creation failed (or was aborted because
    /// an earlier promise creation in the same workflow failed).
    pub async fn id(&self) -> Result<String> {
        self.0.id().await
    }
}

impl<T: Send + 'static> IntoFuture for DurableFuture<T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            tracing::info!(
                target: "resonate::validation",
                promise_id = %self.0.id,
                "promise_execution_await"
            );
            self.0.recv().await
        })
    }
}

/// A handle to a remote durable task.
///
/// Created by `ctx.rpc("func", &args).spawn()`. Awaiting this future returns
/// the result once the remote promise is settled — or `Err(Error::Suspended)`
/// if the promise is still pending, which suspends the workflow until the
/// remote settles. `id()` returns the durable promise ID once the promise has
/// been successfully created on the server.
pub struct RemoteFuture<T>(Handle<T>);

impl<T> RemoteFuture<T> {
    pub(crate) fn pending(
        id: String,
        receiver: tokio::sync::oneshot::Receiver<Result<T>>,
        created: tokio::sync::watch::Receiver<CreationState>,
    ) -> Self {
        Self(Handle {
            id,
            created,
            receiver,
        })
    }

    /// Returns the durable promise ID once the promise has been successfully
    /// created on the server. Fails if creation failed (or was aborted because
    /// an earlier promise creation in the same workflow failed).
    pub async fn id(&self) -> Result<String> {
        self.0.id().await
    }
}

impl<T: Send + 'static> IntoFuture for RemoteFuture<T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.0.recv().await })
    }
}

/// A handle to a detached, fire-and-forget remote execution.
///
/// Created by `ctx.detached("func", &args).spawn()`. Unlike [`RemoteFuture`],
/// a `DetachedHandle` is **not** a future — it deliberately does not implement
/// `IntoFuture`, because a detached call's result is never delivered back to
/// the parent. The only thing the parent can observe is the promise ID, via
/// [`id()`](DetachedHandle::id), once the promise exists on the server.
///
/// Dropping the handle is fine and common: the detached promise is still
/// created on the server (and a creation failure still fails the task at
/// flush). Hold the handle only when you need the ID to hand to an external
/// system.
pub struct DetachedHandle {
    id: String,
    created: tokio::sync::watch::Receiver<CreationState>,
}

impl DetachedHandle {
    pub(crate) fn pending(
        id: String,
        created: tokio::sync::watch::Receiver<CreationState>,
    ) -> Self {
        Self { id, created }
    }

    /// Returns the durable promise ID once the promise has been successfully
    /// created on the server. Fails if creation failed (or was aborted because
    /// an earlier promise creation in the same workflow failed).
    pub async fn id(&self) -> Result<String> {
        await_created_id(&self.id, &self.created).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settled_creation() -> tokio::sync::watch::Receiver<CreationState> {
        let (tx, rx) = creation_channel();
        let _ = tx.send(CreationState::Created);
        rx
    }

    // ── DurableFuture ──────────────────────────────────────────────

    #[tokio::test]
    async fn durable_future_pending_resolves_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<String> =
            DurableFuture::pending("test-id".into(), rx, settled_creation());

        tx.send(Ok("hello".to_string())).unwrap();
        let result: String = future.await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn durable_future_pending_error_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<i32> =
            DurableFuture::pending("test-id".into(), rx, settled_creation());

        tx.send(Err(Error::Application {
            message: "task failed".into(),
        }))
        .unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Application { .. }));
    }

    #[tokio::test]
    async fn durable_future_pending_suspended_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<i32> =
            DurableFuture::pending("test-id".into(), rx, settled_creation());

        tx.send(Err(Error::Suspended)).unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }

    // ── RemoteFuture ───────────────────────────────────────────────

    #[tokio::test]
    async fn remote_future_completed_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: RemoteFuture<String> =
            RemoteFuture::pending("test-id".into(), rx, settled_creation());

        tx.send(Ok("remote-value".to_string())).unwrap();
        let result: String = future.await.unwrap();
        assert_eq!(result, "remote-value");
    }

    #[tokio::test]
    async fn remote_future_failed_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: RemoteFuture<i32> =
            RemoteFuture::pending("test-id".into(), rx, settled_creation());

        tx.send(Err(Error::Application {
            message: "remote error".into(),
        }))
        .unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Application { .. }));
    }

    #[tokio::test]
    async fn remote_future_pending_returns_suspended_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: RemoteFuture<i32> =
            RemoteFuture::pending("test-id".into(), rx, settled_creation());

        tx.send(Err(Error::Suspended)).unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }

    // ── id() gating ────────────────────────────────────────────────

    #[tokio::test]
    async fn id_returns_after_creation_succeeds() {
        let (created_tx, created_rx) = creation_channel();
        let (_result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<i32>>();
        let future: RemoteFuture<i32> = RemoteFuture::pending("p.1".into(), result_rx, created_rx);

        created_tx.send(CreationState::Created).unwrap();
        assert_eq!(future.id().await.unwrap(), "p.1");
        // id() can be called more than once
        assert_eq!(future.id().await.unwrap(), "p.1");
    }

    #[tokio::test]
    async fn id_fails_when_creation_failed() {
        let (created_tx, created_rx) = creation_channel();
        let (_result_tx, result_rx) = tokio::sync::oneshot::channel::<Result<i32>>();
        let future: RemoteFuture<i32> = RemoteFuture::pending("p.1".into(), result_rx, created_rx);

        created_tx
            .send(CreationState::Failed("boom".into()))
            .unwrap();
        let err = future.id().await.unwrap_err();
        assert!(matches!(err, Error::PromiseCreation(_)), "got: {err}");
    }
}
