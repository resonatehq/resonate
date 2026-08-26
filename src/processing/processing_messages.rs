//! Message processing — background loop.
//!
//! Periodically claims a batch of outgoing messages (DELETE ... RETURNING)
//! and delivers them fire-and-forget via the transport dispatcher.

use std::sync::Arc;
use std::time::Duration;

use crate::core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, UnblockMsg, UnblockMsgData,
    UnblockMsgHead,
};
use crate::core::ResonateRouter;
use crate::metrics::Metrics;
use crate::persistence::Storage;

/// Background message processing loop.
pub async fn message_processing_loop(
    state: Arc<crate::server::Server>,
    router: Arc<dyn ResonateRouter>,
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
        process_batch(
            &state.storage,
            router.as_ref(),
            batch_size,
            &server_url,
            &state.metrics,
        )
        .await;
    }
}

/// Process one batch of outgoing messages.
///
/// Called by the background loop and `debug.tick`.
pub async fn process_batch(
    storage: &Storage,
    router: &dyn ResonateRouter,
    batch_size: i64,
    server_url: &str,
    metrics: &Metrics,
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

    metrics
        .messages_total
        .with_label_values(&["execute"])
        .inc_by(execute_count as f64);
    metrics
        .messages_total
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
        let payload = Message::Execute(ExecuteMsg {
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
        });
        if let Err(e) = router.route(&msg.address, &payload).await {
            tracing::warn!(address = %msg.address, error = %e, "Execute message not delivered");
        }
    }

    for msg in unblock_msgs {
        tracing::info!(
            kind = "unblock",
            promise_id = %msg.promise.id,
            promise_state = %msg.promise.state,
            address = %msg.address,
            "Dispatching unblock message"
        );
        let payload = Message::Unblock(UnblockMsg {
            kind: "unblock".to_string(),
            head: UnblockMsgHead {},
            data: UnblockMsgData {
                promise: msg.promise,
            },
        });
        if let Err(e) = router.route(&msg.address, &payload).await {
            tracing::warn!(address = %msg.address, error = %e, "Unblock message not delivered");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::metrics::Metrics;
    use crate::testing::{self, empty_router, ok, router_with, RecordingWorker, T0};

    const SERVER_URL: &str = "http://localhost:8001";

    /// Run one delivery batch against `router`, reporting into `metrics`.
    async fn drain(
        server: &Arc<crate::server::Server>,
        router: &dyn ResonateRouter,
        metrics: &Metrics,
    ) {
        process_batch(&server.storage, router, 100, SERVER_URL, metrics).await;
    }

    /// Create a task whose promise carries `resonate:target = address`, then
    /// immediately release it so an outgoing_execute row is queued for delivery.
    ///
    /// task.create returns the task in "acquired" state (version 1).
    /// task.release transitions it back to "pending" and inserts the
    /// outgoing_execute row that process_batch will pick up.
    async fn create_task_with_target(
        server: &Arc<crate::server::Server>,
        task_id: &str,
        address: &str,
    ) {
        ok(
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
                        "timeoutAt": T0 + 1_000_000,
                        "param": {},
                        "tags": { "resonate:target": address }
                    }
                }
            }),
            T0,
        )
        .await;

        ok(
            server,
            "task.release",
            json!({ "id": task_id, "version": 1 }),
            T0,
        )
        .await;
    }

    /// Register a poll listener on `promise_id` so settling it produces an
    /// outgoing_unblock row addressed to `poll_address`.
    async fn settled_promise_with_listener(
        server: &Arc<crate::server::Server>,
        promise_id: &str,
        poll_address: &str,
    ) {
        ok(
            server,
            "promise.create",
            json!({ "id": promise_id, "timeoutAt": T0 + 1_000_000, "param": {}, "tags": {} }),
            T0,
        )
        .await;
        ok(
            server,
            "promise.register_listener",
            json!({ "awaited": promise_id, "address": poll_address }),
            T0,
        )
        .await;
        ok(
            server,
            "promise.settle",
            json!({ "id": promise_id, "state": "resolved", "value": {} }),
            T0,
        )
        .await;
    }

    // ---- execute-message tests ----

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dispatched_when_http_push_enabled() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        create_task_with_target(&server, "task-1", "http://stub-server/webhook").await;

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("http", stub.clone()), &metrics).await;

        let (address, body) = stub.only_call();
        assert_eq!(address, "http://stub-server/webhook");
        assert_eq!(body["kind"], "execute");
        assert_eq!(body["data"]["task"]["id"], "task-1");
        assert_eq!(body["head"]["serverUrl"], SERVER_URL);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dropped_but_dequeued_when_http_push_disabled() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        create_task_with_target(&server, "task-2", "http://stub-server/webhook").await;

        // First pass: http_push disabled — message is consumed from queue and dropped.
        drain(&server, &empty_router(), &metrics).await;

        // Second pass: http_push now enabled — queue should already be empty.
        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("http", stub.clone()), &metrics).await;

        assert_eq!(
            stub.calls().len(),
            0,
            "message was already drained on the first pass; should not be re-delivered"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dispatched_when_poll_enabled() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        create_task_with_target(&server, "task-3", "poll://any@default").await;

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("poll", stub.clone()), &metrics).await;

        let (address, body) = stub.only_call();
        // The worker receives the address verbatim, not a decomposed group.
        assert_eq!(address, "poll://any@default");
        assert_eq!(body["kind"], "execute");
        assert_eq!(body["data"]["task"]["id"], "task-3");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_message_dropped_but_dequeued_when_poll_disabled() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        create_task_with_target(&server, "task-4", "poll://any@default").await;

        drain(&server, &empty_router(), &metrics).await;

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("poll", stub.clone()), &metrics).await;

        assert_eq!(stub.calls().len(), 0);
    }

    // ---- unblock-message tests ----

    #[tokio::test(flavor = "multi_thread")]
    async fn unblock_message_dispatched_when_http_poll_enabled() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        settled_promise_with_listener(&server, "p-unblock-1", "poll://uni@worker-group/worker-1")
            .await;

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("poll", stub.clone()), &metrics).await;

        let (address, body) = stub.only_call();
        assert_eq!(address, "poll://uni@worker-group/worker-1");
        assert_eq!(body["kind"], "unblock");
        assert_eq!(body["data"]["promise"]["id"], "p-unblock-1");
        assert_eq!(body["data"]["promise"]["state"], "resolved");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unblock_message_dropped_but_dequeued_when_http_poll_disabled() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        settled_promise_with_listener(&server, "p-unblock-2", "poll://uni@worker-group/worker-2")
            .await;

        drain(&server, &empty_router(), &metrics).await;

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("poll", stub.clone()), &metrics).await;

        assert_eq!(stub.calls().len(), 0);
    }

    // ---- requeue semantics ----

    /// Is an undeliverable execute message *permanently* lost, or re-queued?
    ///
    /// `take_outgoing` deletes before delivery (at-most-once), so a failed
    /// route loses that attempt. But a task left `pending` keeps its type-0
    /// retry timeout, and `process_timeouts` re-inserts `outgoing_execute` when
    /// it fires — so the message comes back.
    #[tokio::test(flavor = "multi_thread")]
    async fn undeliverable_execute_message_is_requeued_not_lost() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        create_task_with_target(&server, "task-requeue", "http://stub-server/webhook").await;

        // First pass with nothing registered: the message is dequeued and dropped.
        drain(&server, &empty_router(), &metrics).await;

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("http", stub.clone()), &metrics).await;
        assert_eq!(stub.calls().len(), 0, "that attempt really was consumed");

        // Advance past the task retry timeout and let timeout processing run.
        let retry_deadline = T0 + 60_000;
        server
            .storage
            .transact(move |db| db.process_timeouts(retry_deadline))
            .await
            .expect("timeout processing");

        // The message should be back.
        drain(&server, &router_with("http", stub.clone()), &metrics).await;
        let (address, _) = stub.only_call();
        assert_eq!(
            address, "http://stub-server/webhook",
            "a pending task re-queues its execute message on retry timeout — \
             the message is lost for one attempt, not permanently"
        );
    }

    /// A worker that accepts the handoff and one that reports it undeliverable
    /// must both leave the queue drained: `take_outgoing` is at-most-once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_worker_reporting_unavailable_still_consumes_the_message() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        create_task_with_target(&server, "task-unavail", "http://stub-server/webhook").await;

        let failing = Arc::new(RecordingWorker::failing("connection refused"));
        drain(&server, &router_with("http", failing.clone()), &metrics).await;
        assert_eq!(failing.calls().len(), 0, "it never accepted the message");

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("http", stub.clone()), &metrics).await;
        assert_eq!(
            stub.calls().len(),
            0,
            "the row was deleted before delivery was attempted"
        );
    }

    // ---- metrics ----
    //
    // Only possible now that a server can be given its own registry: against
    // the global one these counts would include every other test's messages.

    #[tokio::test(flavor = "multi_thread")]
    async fn claimed_messages_are_counted_by_kind() {
        let server = testing::server();
        let metrics = Metrics::isolated();

        create_task_with_target(&server, "m-exec", "poll://any@g").await;
        settled_promise_with_listener(&server, "m-unblock", "poll://uni@g/w").await;

        assert_eq!(metrics.message_count("execute"), 0.0);
        assert_eq!(metrics.message_count("unblock"), 0.0);

        let stub = Arc::new(RecordingWorker::new());
        drain(&server, &router_with("poll", stub.clone()), &metrics).await;

        assert_eq!(metrics.message_count("execute"), 1.0);
        assert_eq!(metrics.message_count("unblock"), 1.0);

        // A second, empty batch must not move the counters.
        drain(&server, &router_with("poll", stub.clone()), &metrics).await;
        assert_eq!(metrics.message_count("execute"), 1.0);
        assert_eq!(metrics.message_count("unblock"), 1.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_empty_queue_is_a_no_op() {
        let server = testing::server();
        let metrics = Metrics::isolated();
        let stub = Arc::new(RecordingWorker::new());

        drain(&server, &router_with("poll", stub.clone()), &metrics).await;

        assert_eq!(stub.calls().len(), 0);
        assert_eq!(metrics.message_count("execute"), 0.0);
    }
}
