use tokio::sync::Mutex;

use crate::error::Result;
use crate::transport::Transport;

/// Heartbeat trait for keeping task leases alive.
#[async_trait::async_trait]
pub trait Heartbeat: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
}

/// No-op heartbeat for local mode.
pub struct NoopHeartbeat;

#[async_trait::async_trait]
impl Heartbeat for NoopHeartbeat {
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

/// Async heartbeat that sends task.heartbeat requests at regular intervals.
pub struct AsyncHeartbeat {
    pid: String,
    interval_ms: u64,
    transport: Transport,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl AsyncHeartbeat {
    pub fn new(pid: String, interval_ms: u64, transport: Transport) -> Self {
        Self {
            pid,
            interval_ms,
            transport,
            handle: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl Heartbeat for AsyncHeartbeat {
    async fn start(&self) -> Result<()> {
        let pid = self.pid.clone();
        let interval = std::time::Duration::from_millis(self.interval_ms);
        let transport = self.transport.clone();

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let req = serde_json::json!({
                    "kind": "task.heartbeat",
                    "corrId": format!("hb-{}", chrono_now_ms()),
                    "pid": pid,
                });
                if let Err(e) = transport.send(req).await {
                    tracing::warn!(error = %e, "heartbeat failed");
                }
            }
        });

        *self.handle.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(handle) = self.handle.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
