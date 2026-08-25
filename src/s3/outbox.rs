//! Outgoing messages: the `outgoing_execute` and `outgoing_unblock` tables,
//! held in memory instead of in the document.
//!
//! Sends are post-commit effects. The kernel decides them, the applier hands
//! them here *after* the document lands, and delivery is at-most-once — the
//! same contract `take_outgoing` has today, because a claimed row is deleted
//! before it is routed. A lost `Execute` is recovered by the task's retry
//! deadline; a lost `Unblock` is lost, exactly as it is today.
//!
//! Two things this holds that the plan's sketch did not, both because they are
//! observable behaviour rather than convenience:
//!
//! - **A pending set with the tables' keys.** `outgoing_execute`'s primary key
//!   is the task id, so a newer dispatch supersedes an older one; the pending
//!   set must collapse the same way or `debug.snap` diverges.
//!   `outgoing_unblock`'s is `(promise_id, address)` and its insert is
//!   `ON CONFLICT DO NOTHING`, so the first write wins.
//! - **A pause.** `debug.start` stops the message-processing loop, leaving rows
//!   queued for `debug.snap` to see. Pausing delivery here is that, and it is
//!   what makes the differential suite's `messages` comparison meaningful.
//!
//! The pending set is *not* durable, which is the one place this is weaker than
//! a table: a crash loses queued messages rather than delivering them late.
//! At-most-once already permits that, and the retry deadline covers the
//! dispatches that matter.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, PromiseRecord,
    SnapshotMessage, UnblockMsg, UnblockMsgData, UnblockMsgHead,
};
use crate::core::ResonateRouter;
use crate::kernel::state::OutEntry;
use crate::metrics;

#[derive(Default)]
struct Pending {
    /// Task id to (address, version). One row per task: a newer dispatch
    /// replaces an older one.
    executes: BTreeMap<String, (String, i64)>,
    /// (promise id, address) to the settled promise. First write wins.
    unblocks: BTreeMap<(String, String), PromiseRecord>,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.executes.is_empty() && self.unblocks.is_empty()
    }
}

/// Where a decided message goes.
pub struct Outbox {
    /// Absent in tests and in any deployment with no transports registered; a
    /// message with nowhere to go is dropped, as it is today.
    router: Option<Arc<dyn ResonateRouter>>,
    server_url: String,
    paused: AtomicBool,
    pending: Mutex<Pending>,
}

impl Outbox {
    pub fn new(router: Option<Arc<dyn ResonateRouter>>, server_url: impl Into<String>) -> Self {
        Self {
            router,
            server_url: server_url.into(),
            paused: AtomicBool::new(false),
            pending: Mutex::new(Pending::default()),
        }
    }

    /// Hold messages instead of delivering them — what `debug.start` does to
    /// the message-processing loop.
    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queue one message, then deliver unless paused.
    pub async fn dispatch(&self, address: &str, out: OutEntry) {
        {
            let mut pending = self.lock();
            match out {
                OutEntry::Execute { task_id, version } => {
                    pending
                        .executes
                        .insert(task_id, (address.to_string(), version));
                }
                OutEntry::Unblock {
                    promise_id,
                    promise,
                } => {
                    pending
                        .unblocks
                        .entry((promise_id, address.to_string()))
                        .or_insert(promise);
                }
            }
        }
        if !self.is_paused() {
            self.deliver().await;
        }
    }

    /// Deliver everything queued, executes first — `take_outgoing`'s order.
    ///
    /// Claim-then-send, like the DELETE ... RETURNING it replaces: the messages
    /// leave the queue before they are routed, so a delivery failure is logged
    /// and dropped rather than retried forever.
    pub async fn deliver(&self) {
        let taken = {
            let mut pending = self.lock();
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };

        metrics::MESSAGES_TOTAL
            .with_label_values(&["execute"])
            .inc_by(taken.executes.len() as f64);
        metrics::MESSAGES_TOTAL
            .with_label_values(&["unblock"])
            .inc_by(taken.unblocks.len() as f64);

        for (task_id, (address, version)) in taken.executes {
            let msg = Message::Execute(ExecuteMsg {
                kind: "execute".to_string(),
                head: MessageHead {
                    server_url: self.server_url.clone(),
                },
                data: ExecuteMsgData {
                    task: ExecuteMsgTask {
                        id: task_id.clone(),
                        version,
                    },
                },
            });
            tracing::info!(kind = "execute", task_id = %task_id, version, address = %address, "Dispatching execute message");
            self.route(&address, &msg).await;
        }
        for ((promise_id, address), promise) in taken.unblocks {
            let msg = Message::Unblock(UnblockMsg {
                kind: "unblock".to_string(),
                head: UnblockMsgHead {},
                data: UnblockMsgData { promise },
            });
            tracing::info!(kind = "unblock", promise_id = %promise_id, address = %address, "Dispatching unblock message");
            self.route(&address, &msg).await;
        }
    }

    async fn route(&self, address: &str, msg: &Message) {
        match &self.router {
            Some(router) => {
                if let Err(e) = router.route(address, msg).await {
                    tracing::warn!(address = %address, error = %e, "Message not delivered");
                }
            }
            None => {
                tracing::debug!(address = %address, "No router configured; message dropped");
            }
        }
    }

    /// The queued messages as `debug.snap` reports them.
    ///
    /// Shape and order match the SQL backends' `snap`: executes by task id,
    /// then unblocks by `(promise id, address)`, each with an empty head.
    pub fn snapshot(&self) -> Vec<SnapshotMessage> {
        let pending = self.lock();
        let mut out = Vec::with_capacity(pending.executes.len() + pending.unblocks.len());
        for (task_id, (address, version)) in &pending.executes {
            out.push(SnapshotMessage {
                address: address.clone(),
                message: serde_json::json!({
                    "kind": "execute",
                    "head": {},
                    "data": { "task": { "id": task_id, "version": version } }
                }),
            });
        }
        for ((_, address), promise) in &pending.unblocks {
            out.push(SnapshotMessage {
                address: address.clone(),
                message: serde_json::json!({
                    "kind": "unblock",
                    "head": {},
                    "data": { "promise": promise }
                }),
            });
        }
        out
    }

    /// Forget everything queued — `debug.reset`.
    pub fn clear(&self) {
        *self.lock() = Pending::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PromiseState, PromiseValue};
    use crate::core::Unavailable;
    use async_trait::async_trait;

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

    fn promise(id: &str) -> PromiseRecord {
        PromiseRecord {
            id: id.to_string(),
            state: PromiseState::Resolved,
            param: PromiseValue::default(),
            value: PromiseValue::default(),
            tags: Default::default(),
            timeout_at: 100,
            created_at: 1,
            settled_at: Some(50),
        }
    }

    fn outbox(router: Arc<Recorder>) -> Outbox {
        Outbox::new(Some(router), "http://server:8001")
    }

    #[tokio::test]
    async fn an_execute_is_routed_with_the_server_url() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.dispatch(
            "http://w",
            OutEntry::Execute {
                task_id: "o:t".into(),
                version: 3,
            },
        )
        .await;
        let sent = rec.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "http://w");
        assert_eq!(
            sent[0].1,
            r#"{"data":{"task":{"id":"o:t","version":3}},"head":{"serverUrl":"http://server:8001"},"kind":"execute"}"#
        );
        assert!(ob.snapshot().is_empty(), "delivered messages leave the queue");
    }

    #[tokio::test]
    async fn an_unblock_is_routed_with_an_empty_head() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.dispatch(
            "poll://any@g",
            OutEntry::Unblock {
                promise_id: "o:p".into(),
                promise: promise("o:p"),
            },
        )
        .await;
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
        let ob = outbox(Arc::clone(&rec));
        ob.dispatch(
            "http://w",
            OutEntry::Execute {
                task_id: "o:t".into(),
                version: 0,
            },
        )
        .await;
        assert_eq!(rec.sent().len(), 1);
        // Claim-then-send: the message is gone whether or not it landed.
        ob.deliver().await;
        assert_eq!(rec.sent().len(), 1);
    }

    #[tokio::test]
    async fn a_message_with_no_router_is_dropped() {
        let ob = Outbox::new(None, "http://server:8001");
        ob.dispatch(
            "http://w",
            OutEntry::Execute {
                task_id: "o:t".into(),
                version: 0,
            },
        )
        .await;
        assert!(ob.snapshot().is_empty());
    }

    #[tokio::test]
    async fn paused_messages_queue_up() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.set_paused(true);
        ob.dispatch(
            "http://w",
            OutEntry::Execute {
                task_id: "o:t".into(),
                version: 0,
            },
        )
        .await;
        assert!(rec.sent().is_empty());
        assert_eq!(ob.snapshot().len(), 1);

        ob.set_paused(false);
        ob.deliver().await;
        assert_eq!(rec.sent().len(), 1);
        assert!(ob.snapshot().is_empty());
    }

    #[tokio::test]
    async fn a_newer_execute_supersedes_an_older_one_for_the_same_task() {
        // outgoing_execute's primary key is the task id.
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.set_paused(true);
        for (address, version) in [("http://a", 0), ("http://b", 1)] {
            ob.dispatch(
                address,
                OutEntry::Execute {
                    task_id: "o:t".into(),
                    version,
                },
            )
            .await;
        }
        let snap = ob.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].address, "http://b");
        assert_eq!(snap[0].message["data"]["task"]["version"], 1);
    }

    #[tokio::test]
    async fn a_repeated_unblock_to_one_address_is_queued_once() {
        // outgoing_unblock inserts ON CONFLICT DO NOTHING: the first write wins.
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.set_paused(true);
        for _ in 0..3 {
            ob.dispatch(
                "http://w",
                OutEntry::Unblock {
                    promise_id: "o:p".into(),
                    promise: promise("o:p"),
                },
            )
            .await;
        }
        assert_eq!(ob.snapshot().len(), 1);
    }

    #[tokio::test]
    async fn the_same_promise_reaches_two_addresses() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.set_paused(true);
        for address in ["http://a", "http://b"] {
            ob.dispatch(
                address,
                OutEntry::Unblock {
                    promise_id: "o:p".into(),
                    promise: promise("o:p"),
                },
            )
            .await;
        }
        assert_eq!(ob.snapshot().len(), 2);
    }

    #[tokio::test]
    async fn a_snapshot_lists_executes_before_unblocks_each_in_key_order() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.set_paused(true);
        for task in ["o:z", "o:a"] {
            ob.dispatch(
                "http://w",
                OutEntry::Execute {
                    task_id: task.into(),
                    version: 0,
                },
            )
            .await;
        }
        for p in ["o:q", "o:b"] {
            ob.dispatch(
                "http://w",
                OutEntry::Unblock {
                    promise_id: p.into(),
                    promise: promise(p),
                },
            )
            .await;
        }
        let kinds: Vec<String> = ob
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
    async fn clearing_forgets_everything_queued() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.set_paused(true);
        ob.dispatch(
            "http://w",
            OutEntry::Execute {
                task_id: "o:t".into(),
                version: 0,
            },
        )
        .await;
        ob.clear();
        assert!(ob.snapshot().is_empty());
        ob.set_paused(false);
        ob.deliver().await;
        assert!(rec.sent().is_empty());
    }
}
