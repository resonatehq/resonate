//! Outgoing messages: the `outgoing_execute` and `outgoing_unblock` tables,
//! held in memory instead of in the document.
//!
//! # Contract
//!
//! Sends are post-commit effects. The kernel decides them, the applier hands
//! them here *after* the document lands, and delivery is at-most-once — the
//! same contract `take_outgoing` has today, because a claimed row is deleted
//! before it is routed. A lost `Execute` is recovered by the task's retry
//! deadline; a lost `Unblock` is lost, exactly as it is today.
//!
//! Two properties are observable behaviour, not convenience:
//!
//! - **The pending set collapses with the tables' keys.** `outgoing_execute`'s
//!   primary key is the task id, so a newer dispatch supersedes an older one;
//!   the pending set must collapse the same way or `debug.snap` diverges.
//!   `outgoing_unblock`'s is `(promise_id, address)` and its insert is
//!   `ON CONFLICT DO NOTHING`, so the first write wins.
//! - **Delivery pauses.** the debug startup flag stops the message-processing loop,
//!   leaving rows queued for `debug.snap` to see. Pausing delivery here is
//!   that, and it is what makes the differential suite's `messages` comparison
//!   meaningful.
//!
//! The pending set is *not* durable, which is the one place this is weaker than
//! a table: a crash loses queued messages rather than delivering them late.
//! At-most-once already permits that, and the retry deadline covers the
//! dispatches that matter.
//!
//! # Dependencies
//!
//! The message types in `core::types`, the [`ResonateRouter`] port for actual
//! delivery (via [`LateRouter`] in production, because the router is built
//! after the server), and `kernel::state::OutEntry`, the shape the kernel
//! decides sends in.
//!
//! # Dependants
//!
//! The applier hands sends here after every commit; `S3Server` pauses, resumes
//! and clears it for the `debug.*` operations; the scan service reads
//! `snapshot()` into `debug.snap`; `main` binds the real router into the
//! [`LateRouter`] once the transports exist.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;

use crate::kernel::state::OutEntry;
use crate::metrics;
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, PromiseRecord,
    SnapshotMessage, UnblockMsg, UnblockMsgData, UnblockMsgHead,
};
use resonate_core::{ResonateRouter, Unavailable};

/// A router registered after the thing that needs it was built.
///
/// The dependency graph has one knot: the outbox needs a router, the router
/// needs its workers, and a worker needs a handle to the server the outbox
/// lives in. Something has to be filled in late, and a router is the safest
/// candidate — nothing is delivered until the server is listening.
#[derive(Default)]
pub struct LateRouter {
    inner: OnceLock<Arc<dyn ResonateRouter>>,
}

impl LateRouter {
    pub fn new() -> Self {
        Self {
            inner: OnceLock::new(),
        }
    }

    /// Register the router. Returns false if one was already registered.
    pub fn bind(&self, router: Arc<dyn ResonateRouter>) -> bool {
        self.inner.set(router).is_ok()
    }
}

#[async_trait]
impl ResonateRouter for LateRouter {
    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        match self.inner.get() {
            Some(router) => router.route(address, msg).await,
            None => Err(Unavailable::new(
                "no router registered yet; message dropped",
            )),
        }
    }
}

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

    /// Hold messages instead of delivering them — what the debug startup flag does to
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
                        .or_insert(*promise);
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
    use resonate_core::types::{PromiseState, PromiseValue};

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
        assert!(
            ob.snapshot().is_empty(),
            "delivered messages leave the queue"
        );
    }

    #[tokio::test]
    async fn an_unblock_is_routed_with_an_empty_head() {
        let rec = Recorder::new();
        let ob = outbox(Arc::clone(&rec));
        ob.dispatch(
            "poll://any@g",
            OutEntry::Unblock {
                promise_id: "o:p".into(),
                promise: Box::new(promise("o:p")),
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
                    promise: Box::new(promise("o:p")),
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
                    promise: Box::new(promise("o:p")),
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
                    promise: Box::new(promise(p)),
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

#[cfg(test)]
mod late_router_tests {
    use super::*;
    use resonate_core::types::{PromiseRecord, PromiseState, PromiseValue};

    struct Counting(Mutex<usize>);

    #[async_trait]
    impl ResonateRouter for Counting {
        async fn route(&self, _address: &str, _msg: &Message) -> Result<(), Unavailable> {
            *self.0.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn unblock() -> Message {
        Message::Unblock(UnblockMsg {
            kind: "unblock".into(),
            head: UnblockMsgHead {},
            data: UnblockMsgData {
                promise: PromiseRecord {
                    id: "o:p".into(),
                    state: PromiseState::Resolved,
                    param: PromiseValue::default(),
                    value: PromiseValue::default(),
                    tags: Default::default(),
                    timeout_at: 1,
                    created_at: 0,
                    settled_at: Some(1),
                },
            },
        })
    }

    #[tokio::test]
    async fn routing_before_binding_reports_the_message_undelivered() {
        let late = LateRouter::new();
        let err = late
            .route("http://w", &unblock())
            .await
            .expect_err("no router");
        assert!(err.to_string().contains("no router registered"));
    }

    #[tokio::test]
    async fn once_bound_every_message_reaches_the_router() {
        let late = LateRouter::new();
        let counting = Arc::new(Counting(Mutex::new(0)));
        assert!(late.bind(Arc::clone(&counting) as Arc<dyn ResonateRouter>));
        late.route("http://w", &unblock()).await.unwrap();
        late.route("http://w", &unblock()).await.unwrap();
        assert_eq!(*counting.0.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn a_second_binding_is_refused_rather_than_silently_replacing_the_first() {
        let late = LateRouter::new();
        let first = Arc::new(Counting(Mutex::new(0)));
        assert!(late.bind(Arc::clone(&first) as Arc<dyn ResonateRouter>));
        assert!(!late.bind(Arc::new(Counting(Mutex::new(0))) as Arc<dyn ResonateRouter>));
        late.route("http://w", &unblock()).await.unwrap();
        assert_eq!(*first.0.lock().unwrap(), 1);
    }
}
