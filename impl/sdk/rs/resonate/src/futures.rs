use std::future::IntoFuture;
use std::pin::Pin;

use crate::codec::deserialize_error;
use crate::error::{Error, Result};

/// A handle to an eagerly spawned local durable task.
///
/// Created by `ctx.run(F, args).spawn()`. Awaiting this future returns the result
/// once the spawned task completes.
pub struct DurableFuture<T> {
    inner: DurableFutureInner<T>,
}

enum DurableFutureInner<T> {
    /// The promise was already resolved — return the typed value directly.
    Resolved(T),
    /// The promise was already rejected — return the cached error.
    Rejected(serde_json::Value),
    /// The task is running — await the oneshot receiver for the typed result.
    Pending {
        id: String,
        receiver: tokio::sync::oneshot::Receiver<Result<T>>,
    },
}

impl<T> DurableFuture<T> {
    pub(crate) fn resolved(value: T) -> Self {
        Self {
            inner: DurableFutureInner::Resolved(value),
        }
    }

    pub(crate) fn rejected(value: serde_json::Value) -> Self {
        Self {
            inner: DurableFutureInner::Rejected(value),
        }
    }

    pub(crate) fn pending(id: String, receiver: tokio::sync::oneshot::Receiver<Result<T>>) -> Self {
        Self {
            inner: DurableFutureInner::Pending { id, receiver },
        }
    }
}

impl<T: 'static> IntoFuture for DurableFuture<T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Result<T>>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            match self.inner {
                DurableFutureInner::Resolved(value) => Ok(value),
                DurableFutureInner::Rejected(value) => Err(deserialize_error(value)),
                DurableFutureInner::Pending { id, receiver } => receiver
                    .await
                    .map_err(|_| Error::JoinError(format!("task {} was dropped", id)))?,
            }
        })
    }
}

/// A handle to a remote durable task.
///
/// Created by `ctx.rpc("func", &args).spawn()`. Awaiting this future returns the result
/// once the remote worker resolves the promise.
pub struct RemoteFuture<T> {
    inner: RemoteFutureInner<T>,
}

enum RemoteFutureInner<T> {
    /// The promise was already resolved — return the typed value directly.
    Resolved(T),
    /// The promise was already rejected — return the cached error.
    Rejected(serde_json::Value),
    /// The task is pending — only another worker can resolve it.
    Pending,
}

impl<T> RemoteFuture<T> {
    pub(crate) fn resolved(value: T) -> Self {
        Self {
            inner: RemoteFutureInner::Resolved(value),
        }
    }

    pub(crate) fn rejected(value: serde_json::Value) -> Self {
        Self {
            inner: RemoteFutureInner::Rejected(value),
        }
    }

    pub(crate) fn pending() -> Self {
        Self {
            inner: RemoteFutureInner::Pending,
        }
    }
}

impl<T: Send + 'static> IntoFuture for RemoteFuture<T> {
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            match self.inner {
                RemoteFutureInner::Resolved(value) => Ok(value),
                RemoteFutureInner::Rejected(value) => Err(deserialize_error(value)),
                RemoteFutureInner::Pending => Err(Error::Suspended),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // ── DurableFuture ──────────────────────────────────────────────

    #[tokio::test]
    async fn durable_future_completed_via_await() {
        let future: DurableFuture<i32> = DurableFuture::resolved(42);
        let result: i32 = future.await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn durable_future_failed_via_await() {
        let future: DurableFuture<i32> = DurableFuture::rejected(serde_json::json!({
            "type": "application",
            "message": "boom"
        }));
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Application { .. }));
    }

    #[tokio::test]
    async fn durable_future_pending_resolves_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<String> = DurableFuture::pending("test-id".into(), rx);

        tx.send(Ok("hello".to_string())).unwrap();
        let result: String = future.await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn durable_future_pending_error_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<i32> = DurableFuture::pending("test-id".into(), rx);

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
        let future: DurableFuture<i32> = DurableFuture::pending("test-id".into(), rx);

        tx.send(Err(Error::Suspended)).unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }

    // ── RemoteFuture ───────────────────────────────────────────────

    #[tokio::test]
    async fn remote_future_completed_via_await() {
        let future: RemoteFuture<String> = RemoteFuture::resolved("remote-value".to_string());
        let result: String = future.await.unwrap();
        assert_eq!(result, "remote-value");
    }

    #[tokio::test]
    async fn remote_future_failed_via_await() {
        let future: RemoteFuture<i32> = RemoteFuture::rejected(serde_json::json!({
            "type": "application",
            "message": "remote error"
        }));
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Application { .. }));
    }

    #[tokio::test]
    async fn remote_future_pending_returns_suspended_via_await() {
        let future: RemoteFuture<i32> = RemoteFuture::pending();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }
}
