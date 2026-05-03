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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use serde_json::json;

    use crate::config::Config;
    use crate::persistence::{persistence_sqlite::SqliteStorage, Storage};
    use crate::server::{dispatch as server_dispatch, Server};
    use crate::transport::stubs::{RecordingHttpTransport, RecordingPollTransport};
    use crate::transport::{HttpTransport, PollTransport, TransportDispatcher};
    use crate::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};

    // ---- helpers ----

    fn make_server() -> Arc<Server> {
        let sqlite = SqliteStorage::open(":memory:", 30_000).unwrap();
        let storage = Storage::Sqlite(sqlite);
        let mut config = Config::default();
        config.server.url = Some("http://localhost:8001".to_string());
        Arc::new(Server::new(config, None, storage))
    }

    fn req(kind: &str, data: serde_json::Value) -> RequestEnvelope {
        RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: "test".to_string(),
                version: SUPPORTED_VERSIONS[0].to_string(),
                auth: None,
                debug_time: None,
            },
            data,
        }
    }

    async fn dispatch(server: &Arc<Server>, kind: &str, data: serde_json::Value) {
        let resp = server_dispatch(server, &req(kind, data), 1_000_000).await;
        assert_eq!(resp.head.status, 200, "{kind} failed: {:?}", resp.data);
    }

    /// Create a task whose promise carries `resonate:target = address`, then
    /// immediately release it so an outgoing_execute row is queued for delivery.
    ///
    /// task.create returns the task in "acquired" state (version 1).
    /// task.release transitions it back to "pending" and inserts the
    /// outgoing_execute row that process_batch will pick up.
    async fn create_task_with_target(server: &Arc<Server>, task_id: &str, address: &str) {
        dispatch(
            server,
            "task.create",
            json!({
                "pid": "test-worker",
                "ttl": 60_000,
                "action": {
                    "kind": "promise.create",
                    "head": {},
                    "data": {
                        "id": task_id,
                        "timeoutAt": 2_000_000,
                        "param": {},
                        "tags": { "resonate:target": address }
                    }
                }
            }),
        )
        .await;

        // Release puts the task back to pending and queues the outgoing_execute row.
        dispatch(
            server,
            "task.release",
            json!({ "id": task_id, "version": 1 }),
        )
        .await;
    }

    /// Register a poll listener on `promise_id` so settling it produces an
    /// outgoing_unblock row addressed to `poll_address`.
    async fn register_listener(server: &Arc<Server>, promise_id: &str, poll_address: &str) {
        dispatch(
            server,
            "promise.register_listener",
            json!({ "awaited": promise_id, "address": poll_address }),
        )
        .await;
    }

    async fn settle_promise(server: &Arc<Server>, promise_id: &str) {
        dispatch(
            server,
            "promise.settle",
            json!({ "id": promise_id, "state": "resolved", "value": {} }),
        )
        .await;
    }

    // ---- execute-message tests ----

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dispatched_when_http_push_enabled() {
        let server = make_server();
        create_task_with_target(&server, "task-1", "http://stub-server/webhook").await;

        let stub = Arc::new(RecordingHttpTransport::new());
        let dispatcher = TransportDispatcher::new(
            Some(stub.clone() as Arc<dyn HttpTransport>),
            None,
            None,
            None,
        );

        process_batch(&server.storage, &dispatcher, 100, "http://localhost:8001").await;

        let calls = stub.calls();
        assert_eq!(calls.len(), 1, "expected exactly one HTTP dispatch");
        assert_eq!(calls[0].0, "http://stub-server/webhook");
        assert_eq!(calls[0].1["kind"], "execute");
        assert_eq!(calls[0].1["data"]["task"]["id"], "task-1");
        assert_eq!(calls[0].1["head"]["serverUrl"], "http://localhost:8001");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dropped_but_dequeued_when_http_push_disabled() {
        let server = make_server();
        create_task_with_target(&server, "task-2", "http://stub-server/webhook").await;

        // First pass: http_push disabled — message is consumed from queue and dropped.
        let disabled = TransportDispatcher::new(None, None, None, None);
        process_batch(&server.storage, &disabled, 100, "http://localhost:8001").await;

        // Second pass: http_push now enabled — queue should already be empty.
        let stub = Arc::new(RecordingHttpTransport::new());
        let enabled = TransportDispatcher::new(
            Some(stub.clone() as Arc<dyn HttpTransport>),
            None,
            None,
            None,
        );
        process_batch(&server.storage, &enabled, 100, "http://localhost:8001").await;

        assert_eq!(
            stub.calls().len(),
            0,
            "message was already drained on the first pass; should not be re-delivered"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dispatched_when_poll_enabled() {
        let server = make_server();
        create_task_with_target(&server, "task-3", "poll://any@default").await;

        let stub = Arc::new(RecordingPollTransport::new());
        let dispatcher = TransportDispatcher::new(
            None,
            Some(stub.clone() as Arc<dyn PollTransport>),
            None,
            None,
        );

        process_batch(&server.storage, &dispatcher, 100, "http://localhost:8001").await;

        let calls = stub.calls();
        assert_eq!(calls.len(), 1, "expected exactly one poll dispatch");
        assert_eq!(calls[0].0, "default");
        let body: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(body["kind"], "execute");
        assert_eq!(body["data"]["task"]["id"], "task-3");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dropped_but_dequeued_when_poll_disabled() {
        let server = make_server();
        create_task_with_target(&server, "task-4", "poll://any@default").await;

        let disabled = TransportDispatcher::new(None, None, None, None);
        process_batch(&server.storage, &disabled, 100, "http://localhost:8001").await;

        let stub = Arc::new(RecordingPollTransport::new());
        let enabled = TransportDispatcher::new(
            None,
            Some(stub.clone() as Arc<dyn PollTransport>),
            None,
            None,
        );
        process_batch(&server.storage, &enabled, 100, "http://localhost:8001").await;

        assert_eq!(stub.calls().len(), 0);
    }

    // ---- unblock-message tests ----

    #[tokio::test(flavor = "multi_thread")]
    async fn unblock_message_dispatched_when_http_poll_enabled() {
        let server = make_server();

        // Create a bare promise (no task) and register a poll listener on it.
        dispatch(
            &server,
            "promise.create",
            json!({
                "id": "p-unblock-1",
                "timeoutAt": 2_000_000,
                "param": {},
                "tags": {}
            }),
        )
        .await;
        register_listener(&server, "p-unblock-1", "poll://uni@worker-group/worker-1").await;
        settle_promise(&server, "p-unblock-1").await;

        let stub = Arc::new(RecordingPollTransport::new());
        let dispatcher = TransportDispatcher::new(
            None,
            Some(stub.clone() as Arc<dyn PollTransport>),
            None,
            None,
        );

        process_batch(&server.storage, &dispatcher, 100, "http://localhost:8001").await;

        let calls = stub.calls();
        assert_eq!(calls.len(), 1, "expected exactly one unblock dispatch");
        assert_eq!(calls[0].0, "worker-group");
        let body: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(body["kind"], "unblock");
        assert_eq!(body["data"]["promise"]["id"], "p-unblock-1");
        assert_eq!(body["data"]["promise"]["state"], "resolved");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unblock_message_dropped_but_dequeued_when_http_poll_disabled() {
        let server = make_server();

        dispatch(
            &server,
            "promise.create",
            json!({
                "id": "p-unblock-2",
                "timeoutAt": 2_000_000,
                "param": {},
                "tags": {}
            }),
        )
        .await;
        register_listener(&server, "p-unblock-2", "poll://uni@worker-group/worker-2").await;
        settle_promise(&server, "p-unblock-2").await;

        // First pass: poll disabled — message consumed and dropped.
        let disabled = TransportDispatcher::new(None, None, None, None);
        process_batch(&server.storage, &disabled, 100, "http://localhost:8001").await;

        // Second pass: poll enabled — queue already drained.
        let stub = Arc::new(RecordingPollTransport::new());
        let enabled = TransportDispatcher::new(
            None,
            Some(stub.clone() as Arc<dyn PollTransport>),
            None,
            None,
        );
        process_batch(&server.storage, &enabled, 100, "http://localhost:8001").await;

        assert_eq!(stub.calls().len(), 0);
    }
}
