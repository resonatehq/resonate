use serde::de::DeserializeOwned;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::codec::deserialize_error;
use crate::effects::Effects;
use crate::error::{Error, Result};
use crate::types::Outcome;

/// A handle to an eagerly spawned local durable task.
///
/// Created by `ctx.run(F, args).spawn()`. Awaiting this future returns the result
/// once the spawned task completes.
pub struct DurableFuture<T> {
    inner: DurableFutureInner,
    _phantom: PhantomData<T>,
}

enum DurableFutureInner {
    /// The promise was already resolved — return the cached value.
    Completed(serde_json::Value),
    /// The promise was already rejected — return the cached error.
    Failed(serde_json::Value),
    /// The task is running — await the join handle.
    Pending {
        id: String,
        receiver: tokio::sync::oneshot::Receiver<Outcome>,
    },
}

impl<T> DurableFuture<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn completed(value: serde_json::Value) -> Self {
        Self {
            inner: DurableFutureInner::Completed(value),
            _phantom: PhantomData,
        }
    }

    pub(crate) fn failed(value: serde_json::Value) -> Self {
        Self {
            inner: DurableFutureInner::Failed(value),
            _phantom: PhantomData,
        }
    }

    pub(crate) fn pending(id: String, receiver: tokio::sync::oneshot::Receiver<Outcome>) -> Self {
        Self {
            inner: DurableFutureInner::Pending { id, receiver },
            _phantom: PhantomData,
        }
    }

    /// Await the result of the durable task.
    pub async fn await_result(self) -> Result<T> {
        match self.inner {
            DurableFutureInner::Completed(value) => {
                let result: T = serde_json::from_value(value)?;
                Ok(result)
            }
            DurableFutureInner::Failed(value) => Err(deserialize_error(value)),
            DurableFutureInner::Pending { id, receiver } => {
                let outcome = receiver
                    .await
                    .map_err(|_| Error::JoinError(format!("task {} was dropped", id)))?;

                match outcome {
                    Outcome::Done(Ok(value)) => {
                        let result: T = serde_json::from_value(value)?;
                        Ok(result)
                    }
                    Outcome::Done(Err(err)) => Err(err),
                    Outcome::Suspended { .. } => Err(Error::Suspended),
                }
            }
        }
    }
}

impl<T> IntoFuture for DurableFuture<T>
where
    T: DeserializeOwned + 'static,
{
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Result<T>>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.await_result())
    }
}

/// A handle to a remote durable task.
///
/// Created by `ctx.rpc("func", &args).spawn()`. Awaiting this future returns the result
/// once the remote worker resolves the promise.
pub struct RemoteFuture<T> {
    inner: RemoteFutureInner,
    _phantom: PhantomData<T>,
}

enum RemoteFutureInner {
    /// The promise was already resolved.
    Completed(serde_json::Value),
    /// The promise was already rejected.
    Failed(serde_json::Value),
    /// The promise is pending on a remote worker.
    Pending { _id: String, _effects: Effects },
}

impl<T> RemoteFuture<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn completed(value: serde_json::Value) -> Self {
        Self {
            inner: RemoteFutureInner::Completed(value),
            _phantom: PhantomData,
        }
    }

    pub(crate) fn failed(value: serde_json::Value) -> Self {
        Self {
            inner: RemoteFutureInner::Failed(value),
            _phantom: PhantomData,
        }
    }

    pub(crate) fn pending(id: String, effects: Effects) -> Self {
        Self {
            inner: RemoteFutureInner::Pending {
                _id: id,
                _effects: effects,
            },
            _phantom: PhantomData,
        }
    }

    /// Await the result of the remote task.
    /// Note: For pending remote futures, this will return a Suspended error
    /// since remote tasks can only be resolved by another worker.
    pub async fn await_result(self) -> Result<T> {
        match self.inner {
            RemoteFutureInner::Completed(value) => {
                let result: T = serde_json::from_value(value)?;
                Ok(result)
            }
            RemoteFutureInner::Failed(value) => Err(deserialize_error(value)),
            RemoteFutureInner::Pending {
                _id: _,
                _effects: _,
            } => Err(Error::Suspended),
        }
    }
}

impl<T> IntoFuture for RemoteFuture<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Output = Result<T>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.await_result())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestHarness;
    use crate::types::Outcome;

    // ── DurableFuture ──────────────────────────────────────────────

    #[tokio::test]
    async fn durable_future_completed_via_await() {
        let future: DurableFuture<i32> = DurableFuture::completed(serde_json::json!(42));
        let result: i32 = future.await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn durable_future_failed_via_await() {
        let future: DurableFuture<i32> = DurableFuture::failed(serde_json::json!({
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

        tx.send(Outcome::Done(Ok(serde_json::json!("hello"))))
            .unwrap();
        let result: String = future.await.unwrap();
        assert_eq!(result, "hello");
    }

    #[tokio::test]
    async fn durable_future_pending_error_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<i32> = DurableFuture::pending("test-id".into(), rx);

        tx.send(Outcome::Done(Err(Error::Application {
            message: "task failed".into(),
        })))
        .unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Application { .. }));
    }

    #[tokio::test]
    async fn durable_future_pending_suspended_via_await() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future: DurableFuture<i32> = DurableFuture::pending("test-id".into(), rx);

        tx.send(Outcome::Suspended {
            remote_todos: vec![],
        })
        .unwrap();
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }

    // ── RemoteFuture ───────────────────────────────────────────────

    #[tokio::test]
    async fn remote_future_completed_via_await() {
        let future: RemoteFuture<String> =
            RemoteFuture::completed(serde_json::json!("remote-value"));
        let result: String = future.await.unwrap();
        assert_eq!(result, "remote-value");
    }

    #[tokio::test]
    async fn remote_future_failed_via_await() {
        let future: RemoteFuture<i32> = RemoteFuture::failed(serde_json::json!({
            "type": "application",
            "message": "remote error"
        }));
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Application { .. }));
    }

    #[tokio::test]
    async fn remote_future_pending_returns_suspended_via_await() {
        let harness = TestHarness::new();
        let effects = harness.build_effects(vec![]);
        let future: RemoteFuture<i32> = RemoteFuture::pending("remote-id".into(), effects);
        let err = future.await.unwrap_err();
        assert!(matches!(err, Error::Suspended));
    }
}
