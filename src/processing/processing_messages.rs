//! Message processing — background loop.
//!
//! Periodically claims a batch of outgoing messages (DELETE ... RETURNING)
//! and delivers them fire-and-forget via the transport dispatcher.

use std::sync::Arc;
use std::time::Duration;

use crate::metrics;
use crate::persistence::Storage;
use crate::transport::TransportDispatcher;
use crate::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};

/// Background message processing loop.
pub async fn message_processing_loop(
    state: Arc<crate::server::Server>,
    dispatcher: Arc<TransportDispatcher>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_millis(state.config.messages.poll_interval);
    let batch_size = state.config.messages.batch_size;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                tracing::info!("Message processing loop shutting down");
                return;
            }
        }

        if state.debug_mode.load(std::sync::atomic::Ordering::SeqCst) {
            continue;
        }

        let server_url = state.config.server.url.clone().unwrap_or_default();
        process_batch(&state.storage, &dispatcher, batch_size, &server_url).await;
    }
}

/// Process one batch of outgoing messages.
///
/// Called by the background loop and `debug.tick`.
pub async fn process_batch(
    storage: &Storage,
    dispatcher: &TransportDispatcher,
    batch_size: i64,
    server_url: &str,
) {
    let (execute_msgs, unblock_msgs) = match storage
        .transact(move |db| db.take_outgoing(batch_size))
        .await
    {
        Ok(msgs) => msgs,
        Err(e) => {
            tracing::error!(error = %e, "Failed to take outgoing messages: storage error");
            return;
        }
    };

    let execute_count = execute_msgs.len();
    let unblock_count = unblock_msgs.len();

    if execute_count > 0 || unblock_count > 0 {
        tracing::debug!(
            execute_count = execute_count,
            unblock_count = unblock_count,
            "Claimed outgoing messages for delivery"
        );
    }

    metrics::MESSAGES_TOTAL
        .with_label_values(&["execute"])
        .inc_by(execute_count as f64);
    metrics::MESSAGES_TOTAL
        .with_label_values(&["unblock"])
        .inc_by(unblock_count as f64);

    for msg in execute_msgs {
        tracing::info!(
            kind = "execute",
            task_id = %msg.id,
            version = msg.version,
            address = %msg.address,
            "Dispatching execute message"
        );
        let payload = ExecuteMsg {
            kind: "execute".to_string(),
            head: MessageHead {
                server_url: server_url.to_string(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: msg.id,
                    version: msg.version,
                },
            },
        };
        dispatcher
            .send(&msg.address, &serde_json::to_value(&payload).unwrap())
            .await;
    }

    for msg in unblock_msgs {
        tracing::info!(
            kind = "unblock",
            promise_id = %msg.promise.id,
            promise_state = %msg.promise.state,
            address = %msg.address,
            "Dispatching unblock message"
        );
        let payload = serde_json::json!({
            "kind": "unblock",
            "head": {},
            "data": {
                "promise": msg.promise
            }
        });
        dispatcher.send(&msg.address, &payload).await;
    }
}
