//! Message processing — background loop.
//!
//! Periodically claims a batch of outgoing messages (DELETE ... RETURNING)
//! and delivers them fire-and-forget via the transport dispatcher.

use std::sync::Arc;
use std::time::Duration;

use crate::metrics;
use crate::persistence::Storage;
use crate::transport::TransportDispatcher;

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

        process_batch(&state.storage, &dispatcher, batch_size).await;
    }
}

/// Process one batch of outgoing messages.
///
/// Called by the background loop and `debug.tick`.
pub async fn process_batch(storage: &Storage, dispatcher: &TransportDispatcher, batch_size: i64) {
    let (execute_msgs, unblock_msgs) = match storage
        .transact(move |db| db.take_outgoing(batch_size))
        .await
    {
        Ok(msgs) => msgs,
        Err(e) => {
            tracing::error!("Failed to take outgoing messages: {}", e);
            return;
        }
    };

    metrics::MESSAGES_TOTAL
        .with_label_values(&["execute"])
        .inc_by(execute_msgs.len() as f64);
    metrics::MESSAGES_TOTAL
        .with_label_values(&["unblock"])
        .inc_by(unblock_msgs.len() as f64);

    for msg in execute_msgs {
        let payload = serde_json::json!({
            "kind": "execute",
            "head": {},
            "data": {
                "task": {
                    "id": msg.id,
                    "version": msg.version
                }
            }
        });
        dispatcher.send(&msg.address, &payload).await;
    }

    for msg in unblock_msgs {
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
