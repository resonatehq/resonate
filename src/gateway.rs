//! The HTTP gateway: axum in front of a [`ResonateServer`].
//!
//! The only inbound edge today. It owns the socket and the axum task, and
//! nothing else — the routes, their state and their layers are assembled by
//! whoever builds it, so this file knows nothing about promises, tasks, or the
//! poll transport that shares its listener.

use std::sync::Arc;

use async_trait::async_trait;
use resonate_core::{ResonateGateway, ResonateServer, Unavailable};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

/// Where to listen, and what to call ourselves.
#[derive(Debug, Clone)]
pub struct HttpGatewayConfig {
    pub bind: String,
    pub port: u16,
    /// The URL clients reach this server on, for logging only.
    pub url: String,
}

/// axum, hosting the Resonate protocol over HTTP.
pub struct HttpGateway {
    config: HttpGatewayConfig,
    /// Held so the server outlives every request it can still accept. Strong,
    /// unlike a worker's handle: nothing points back at a gateway, so there is
    /// no cycle to break. Unused directly — the routes carry their own handle —
    /// but the ownership is the point.
    #[allow(dead_code)]
    server: Arc<dyn ResonateServer>,
    /// The fully-built application: routes, state and layers already applied.
    /// Taken by `init`, which is what makes `init` the only place a socket is
    /// bound.
    app: Mutex<Option<axum::Router>>,
    /// Set by `stop` to release the graceful-shutdown future.
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl HttpGateway {
    /// Build the gateway. Nothing is bound and nothing runs until `init`.
    pub fn new(
        server: Arc<dyn ResonateServer>,
        config: HttpGatewayConfig,
        app: axum::Router,
    ) -> Self {
        Self {
            config,
            server,
            app: Mutex::new(Some(app)),
            shutdown: Mutex::new(None),
            task: Mutex::new(None),
        }
    }
}

#[async_trait]
impl ResonateGateway for HttpGateway {
    async fn init(&self) -> Result<(), Unavailable> {
        let Some(app) = self.app.lock().await.take() else {
            return Ok(()); // already started
        };

        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| Unavailable::new(format!("failed to bind {addr}: {e}")))?;

        let (tx, rx) = oneshot::channel();
        *self.shutdown.lock().await = Some(tx);

        tracing::info!(
            bind = %self.config.bind,
            port = self.config.port,
            server_url = %self.config.url,
            "Server listening"
        );

        let handle = tokio::spawn(async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    // An error means `stop` dropped the sender rather than
                    // sending, which is still a request to stop.
                    let _ = rx.await;
                })
                .await;
            if let Err(e) = served {
                tracing::error!(error = %e, "HTTP gateway stopped with an error");
            }
        });
        *self.task.lock().await = Some(handle);
        Ok(())
    }

    /// Close the listener and wait for what is in flight.
    ///
    /// Called last, after every worker has stopped — which is what makes the
    /// wait finite. A poll transport's SSE streams are long-lived responses
    /// axum's graceful shutdown would otherwise wait on forever; by now that
    /// transport has dropped its senders and every one of those streams has
    /// ended on its own.
    async fn stop(&self) -> Result<(), Unavailable> {
        if let Some(tx) = self.shutdown.lock().await.take() {
            let _ = tx.send(());
        }
        let handle = self.task.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }
}
