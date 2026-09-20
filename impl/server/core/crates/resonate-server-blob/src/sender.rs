//! Outgoing messages: straight to the router, post-commit.
//!
//! # Contract
//!
//! Sends are post-commit effects. The kernel decides them — as fully-formed
//! `core::types::Message`s, the router's own vocabulary, so nothing here
//! translates anything — the applier hands them here *after* the document
//! lands, and delivery is at-most-once: a message is routed exactly when it
//! is dispatched, and a delivery failure is logged and dropped rather than
//! retried. A lost `Execute` is recovered by the task's retry deadline; a
//! lost `Unblock` is lost, exactly as on the SQL backends.
//!
//! There is no queue in production — the router is the queue's replacement,
//! and it exists from construction. Under the debug startup flag messages are
//! **held** instead of routed, forever: `debug.snap` reads them, `debug.reset`
//! clears them, and nothing else touches them. The held set collapses the way
//! the SQL backends' outgoing tables collapse — `outgoing_execute`'s primary
//! key is the task id, so a newer dispatch supersedes an older one;
//! `outgoing_unblock`'s is `(promise_id, address)` and the first write wins —
//! or `debug.snap` would diverge from the oracle's queue.
//!
//! # Dependencies
//!
//! The message types in `core::types` and the [`ResonateRouter`] port for
//! actual delivery.
//!
//! # Dependants
//!
//! The applier hands sends here after every commit; the scan service reads
//! `snapshot()` into `debug.snap`; `debug.reset` calls `clear()`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::metrics;
use resonate_core::types::{Message, SnapshotMessage};
use resonate_core::{ResonateRouter, Unavailable};

/// A router with nowhere to route to: every message is accepted and dropped.
///
/// For tests and for wiring that registers no transports — the same "message
/// with no router is dropped" behaviour the SQL path has, stated as a router
/// instead of an `Option`.
pub struct NullRouter;

#[async_trait]
impl ResonateRouter for NullRouter {
    async fn route(&self, address: &str, _msg: &Message) -> Result<(), Unavailable> {
        tracing::debug!(address = %address, "No transports registered; message dropped");
        Ok(())
    }
}

/// Messages held under the debug flag, keyed the way the SQL backends key
/// their outgoing tables.
#[derive(Default)]
struct Held {
    /// Task id to (address, message). One row per task: a newer dispatch
    /// replaces an older one.
    executes: BTreeMap<String, (String, Message)>,
    /// (promise id, address) to the unblock. First write wins.
    unblocks: BTreeMap<(String, String), Message>,
}

/// Where a decided message goes.
pub struct Sender {
    router: Arc<dyn ResonateRouter>,
    /// `Some` exactly when the debug startup flag is set — messages are held
    /// here for `debug.snap` instead of leaving the process.
    held: Option<Mutex<Held>>,
}

impl Sender {
    pub fn new(router: Arc<dyn ResonateRouter>, debug: bool) -> Self {
        Self {
            router,
            held: debug.then(|| Mutex::new(Held::default())),
        }
    }

    /// Route one message, or hold it under the debug flag.
    pub async fn dispatch(&self, address: &str, msg: Message) {
        if let Some(held) = &self.held {
            let mut held = held.lock().unwrap_or_else(|e| e.into_inner());
            match &msg {
                Message::Execute(e) => {
                    held.executes
                        .insert(e.data.task.id.clone(), (address.to_string(), msg));
                }
                Message::Unblock(u) => {
                    held.unblocks
                        .entry((u.data.promise.id.clone(), address.to_string()))
                        .or_insert(msg);
                }
            }
            return;
        }

        match &msg {
            Message::Execute(e) => {
                metrics::MESSAGES_TOTAL
                    .with_label_values(&["execute"])
                    .inc();
                tracing::info!(kind = "execute", task_id = %e.data.task.id, version = e.data.task.version, address = %address, "Dispatching execute message");
            }
            Message::Unblock(u) => {
                metrics::MESSAGES_TOTAL
                    .with_label_values(&["unblock"])
                    .inc();
                tracing::info!(kind = "unblock", promise_id = %u.data.promise.id, address = %address, "Dispatching unblock message");
            }
        }
        if let Err(e) = self.router.route(address, &msg).await {
            tracing::warn!(address = %address, error = %e, "Message not delivered");
        }
    }

    /// The held messages as `debug.snap` reports them. Empty outside debug.
    ///
    /// Shape and order match the SQL backends' `snap`: executes by task id,
    /// then unblocks by `(promise id, address)`, each with an empty head —
    /// the snapshot never carried a `serverUrl`.
    pub fn snapshot(&self) -> Vec<SnapshotMessage> {
        let Some(held) = &self.held else {
            return Vec::new();
        };
        let held = held.lock().unwrap_or_else(|e| e.into_inner());
        let blank_head = |msg: &Message, address: &str| {
            let mut message = serde_json::to_value(msg).expect("a message serializes");
            message["head"] = serde_json::json!({});
            SnapshotMessage {
                address: address.to_string(),
                message,
            }
        };
        let mut out = Vec::with_capacity(held.executes.len() + held.unblocks.len());
        for (address, msg) in held.executes.values() {
            out.push(blank_head(msg, address));
        }
        for ((_, address), msg) in &held.unblocks {
            out.push(blank_head(msg, address));
        }
        out
    }

    /// Forget everything held — `debug.reset`.
    pub fn clear(&self) {
        if let Some(held) = &self.held {
            *held.lock().unwrap_or_else(|e| e.into_inner()) = Held::default();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::{
        ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead, PromiseRecord, PromiseState,
        PromiseValue, UnblockMsg, UnblockMsgData, UnblockMsgHead,
    };

    /// A router that records what it was asked to deliver.
    struct Recorder {
        sent: Mutex<Vec<(String, String)>>,
        fail: bool,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                fail: false,
            })
        }
        fn failing() -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                fail: true,
            })
        }
        fn sent(&self) -> Vec<(String, String)> {
            self.sent.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ResonateRouter for Recorder {
        async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
            let body = serde_json::to_string(&serde_json::to_value(msg).unwrap()).unwrap();
            self.sent.lock().unwrap().push((address.to_string(), body));
            if self.fail {
                return Err(Unavailable::new("no worker"));
            }
            Ok(())
        }
    }

    fn execute(task_id: &str, version: i64) -> Message {
        Message::Execute(ExecuteMsg {
            kind: "execute".to_string(),
            head: MessageHead {
                server_url: "http://server:8001".to_string(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: task_id.to_string(),
                    version,
                },
            },
        })
    }

    fn unblock(promise_id: &str) -> Message {
        Message::Unblock(UnblockMsg {
            kind: "unblock".to_string(),
            head: UnblockMsgHead {},
            data: UnblockMsgData {
                promise: PromiseRecord {
                    id: promise_id.to_string(),
                    state: PromiseState::Resolved,
                    param: PromiseValue::default(),
                    value: PromiseValue::default(),
                    tags: Default::default(),
                    timeout_at: 100,
                    created_at: 1,
                    settled_at: Some(50),
                },
            },
        })
    }

    fn live(router: Arc<Recorder>) -> Sender {
        Sender::new(router, false)
    }

    fn debug(router: Arc<Recorder>) -> Sender {
        Sender::new(router, true)
    }

    #[tokio::test]
    async fn an_execute_is_routed_as_decided_with_its_server_url() {
        let rec = Recorder::new();
        let s = live(Arc::clone(&rec));
        s.dispatch("http://w", execute("o:t", 3)).await;
        let sent = rec.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "http://w");
        assert_eq!(
            sent[0].1,
            r#"{"data":{"task":{"id":"o:t","version":3}},"head":{"serverUrl":"http://server:8001"},"kind":"execute"}"#
        );
        assert!(s.snapshot().is_empty(), "nothing is held outside debug");
    }

    #[tokio::test]
    async fn an_unblock_is_routed_with_an_empty_head() {
        let rec = Recorder::new();
        let s = live(Arc::clone(&rec));
        s.dispatch("poll://any@g", unblock("o:p")).await;
        let sent = rec.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "poll://any@g");
        // Unblock has never carried a serverUrl.
        assert!(sent[0].1.contains(r#""head":{}"#));
        assert!(sent[0].1.contains(r#""id":"o:p""#));
    }

    #[tokio::test]
    async fn a_failed_delivery_is_dropped_not_retried() {
        let rec = Recorder::failing();
        let s = live(Arc::clone(&rec));
        s.dispatch("http://w", execute("o:t", 0)).await;
        // Routed once, held nowhere: the message is gone whether or not it
        // landed.
        assert_eq!(rec.sent().len(), 1);
        assert!(s.snapshot().is_empty());
    }

    #[tokio::test]
    async fn a_null_router_accepts_and_drops() {
        let s = Sender::new(Arc::new(NullRouter), false);
        s.dispatch("http://w", execute("o:t", 0)).await;
        assert!(s.snapshot().is_empty());
    }

    #[tokio::test]
    async fn under_debug_messages_are_held_not_routed() {
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        s.dispatch("http://w", execute("o:t", 0)).await;
        assert!(rec.sent().is_empty());
        assert_eq!(s.snapshot().len(), 1);
    }

    #[tokio::test]
    async fn a_newer_execute_supersedes_an_older_one_for_the_same_task() {
        // outgoing_execute's primary key is the task id.
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        for (address, version) in [("http://a", 0), ("http://b", 1)] {
            s.dispatch(address, execute("o:t", version)).await;
        }
        let snap = s.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].address, "http://b");
        assert_eq!(snap[0].message["data"]["task"]["version"], 1);
    }

    #[tokio::test]
    async fn a_repeated_unblock_to_one_address_is_held_once() {
        // outgoing_unblock inserts ON CONFLICT DO NOTHING: the first write wins.
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        for _ in 0..3 {
            s.dispatch("http://w", unblock("o:p")).await;
        }
        assert_eq!(s.snapshot().len(), 1);
    }

    #[tokio::test]
    async fn the_same_promise_reaches_two_addresses() {
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        for address in ["http://a", "http://b"] {
            s.dispatch(address, unblock("o:p")).await;
        }
        assert_eq!(s.snapshot().len(), 2);
    }

    #[tokio::test]
    async fn a_snapshot_lists_executes_before_unblocks_each_in_key_order() {
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        for task in ["o:z", "o:a"] {
            s.dispatch("http://w", execute(task, 0)).await;
        }
        for p in ["o:q", "o:b"] {
            s.dispatch("http://w", unblock(p)).await;
        }
        let kinds: Vec<String> = s
            .snapshot()
            .iter()
            .map(|m| {
                format!(
                    "{}:{}",
                    m.message["kind"].as_str().unwrap(),
                    m.message
                        .pointer("/data/task/id")
                        .or_else(|| m.message.pointer("/data/promise/id"))
                        .unwrap()
                        .as_str()
                        .unwrap()
                )
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["execute:o:a", "execute:o:z", "unblock:o:b", "unblock:o:q"]
        );
    }

    #[tokio::test]
    async fn the_snapshot_blanks_the_execute_head() {
        // The SQL backends' snap reports messages with empty heads; the held
        // execute carries a serverUrl, so the snapshot must blank it.
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        s.dispatch("http://w", execute("o:t", 0)).await;
        assert_eq!(s.snapshot()[0].message["head"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn clearing_forgets_everything_held() {
        let rec = Recorder::new();
        let s = debug(Arc::clone(&rec));
        s.dispatch("http://w", execute("o:t", 0)).await;
        s.clear();
        assert!(s.snapshot().is_empty());
        assert!(rec.sent().is_empty());
    }
}
