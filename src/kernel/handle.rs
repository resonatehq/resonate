//! The decider: one request in, effects and a reply out.
//!
//! `handle` never mutates the caller's document. It works on a copy, and the
//! copy comes back as a [`Effect::SetDocument`] — so the transition *is*
//! `apply_effects(handle(..).0)` and there is no second updater to drift.
//!
//! Two things happen before every operation, in this order:
//!
//! 1. **Ghost timeouts.** Any promise the request *names* whose deadline has
//!    passed is settled first, with its settlement chain run in full. This
//!    mirrors `Db::try_timeout` (`persistence_sqlite.rs:363-419`), which every
//!    operation in `server.rs` calls before touching state. Only the named ids
//!    are swept — a full sweep happens on [`drain`](super::drain) — because
//!    the SQL backends behave that way and `debug.snap` must agree byte for
//!    byte.
//! 2. **The operation itself**, against the post-ghost state.
//!
//! Statuses and messages are `Server::dispatch`'s, verbatim: a rejection is a
//! [`Reply`] with a status, never an `Err`.

use std::collections::{BTreeMap, BTreeSet};

use crate::core::is_valid_address;
use crate::core::types::{
    PromiseCreateData, PromiseRecord, PromiseResponseData, PromiseState, PromiseValue, SettleState,
    TaskState,
};

use super::state::{
    min_deadline, Effect, KernelCfg, OriginDoc, OutEntry, PromiseDoc, Reply, Req, TaskDoc,
    TAG_BRANCH, TAG_DELAY, TAG_TARGET,
};

/// A decision in progress: the working document plus the messages it owes.
pub(crate) struct Tx {
    pub(crate) doc: OriginDoc,
    pub(crate) sends: Vec<(String, OutEntry)>,
}

impl Tx {
    pub(crate) fn new(doc: &OriginDoc) -> Self {
        Self {
            doc: doc.clone(),
            sends: Vec::new(),
        }
    }

    /// Turn the working state into effects, in the order the shell performs
    /// them: arm the new timer, commit the document, clear the old timer, send.
    pub(crate) fn finish(mut self, before: &OriginDoc) -> Vec<Effect> {
        let old = before.timer_at;
        let new = min_deadline(&self.doc);
        self.doc.timer_at = new;

        let mut fx = Vec::new();
        if old != new {
            if let Some(at) = new {
                fx.push(Effect::SetTimeout { at });
            }
        }
        fx.push(Effect::SetDocument(self.doc));
        if old != new {
            if let Some(at) = old {
                fx.push(Effect::DelTimeout { at });
            }
        }
        for (address, out) in self.sends {
            fx.push(Effect::Send { address, out });
        }
        fx
    }

    /// Queue a dispatch for `task_id`. A promise with no `resonate:target` has
    /// nowhere to send, exactly as `outgoing_execute`'s insert is guarded on a
    /// non-null target.
    pub(crate) fn send_execute(&mut self, task_id: &str, version: i64) {
        let address = match self.doc.promises.get(task_id).and_then(|p| p.target()) {
            Some(a) => a.to_string(),
            None => return,
        };
        self.sends.push((
            address,
            OutEntry::Execute {
                task_id: task_id.to_string(),
                version,
            },
        ));
    }
}

/// Decide one request.
pub fn handle(doc: &OriginDoc, req: &Req, now: i64, cfg: &KernelCfg) -> (Vec<Effect>, Reply) {
    let mut tx = Tx::new(doc);
    let reply = match req {
        Req::PromiseGet(r) => op_promise_get(&mut tx, r, now, cfg),
        Req::PromiseCreate(r) => op_promise_create(&mut tx, r, now, cfg),
        Req::PromiseSettle(r) => op_promise_settle(&mut tx, r, now, cfg),
        Req::PromiseRegisterCallback(r) => op_promise_register_callback(&mut tx, r, now, cfg),
        Req::PromiseRegisterListener(r) => op_promise_register_listener(&mut tx, r, now, cfg),
        // Task operations land in the next commit; nothing routes here yet.
        _ => Reply::err(501, "Operation not implemented"),
    };
    (tx.finish(doc), reply)
}

// ============================================================================
// Promise operations
// ============================================================================

fn op_promise_get(
    tx: &mut Tx,
    r: &crate::core::types::PromiseGetData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    match tx.doc.promises.get(&r.id) {
        Some(p) => Reply::ok(&PromiseResponseData {
            promise: p.to_record(&r.id),
        }),
        None => Reply::err(404, "Promise not found"),
    }
}

fn op_promise_create(tx: &mut Tx, r: &PromiseCreateData, now: i64, cfg: &KernelCfg) -> Reply {
    if let Some(addr) = r.tags.get(TAG_TARGET) {
        if !is_valid_address(addr) {
            return Reply::err(400, "Invalid resonate:target address");
        }
    }
    try_timeout(tx, &[&r.id], now, cfg);
    if let Some(p) = tx.doc.promises.get(&r.id) {
        // Create is idempotent on id alone: the stored promise wins.
        return Reply::ok(&PromiseResponseData {
            promise: p.to_record(&r.id),
        });
    }
    let record = create_promise(tx, &r.id, r, now, cfg);
    Reply::ok(&PromiseResponseData { promise: record })
}

fn op_promise_settle(
    tx: &mut Tx,
    r: &crate::core::types::PromiseSettleData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let pending = match tx.doc.promises.get(&r.id) {
        Some(p) => p.state == PromiseState::Pending,
        None => return Reply::err(404, "Promise not found"),
    };
    if !pending {
        // Settlement is terminal: a second settle reports the first one.
        let p = &tx.doc.promises[&r.id];
        return Reply::ok(&PromiseResponseData {
            promise: p.to_record(&r.id),
        });
    }
    let record = settle(tx, &r.id, r.state, &r.value, now, cfg);
    Reply::ok(&PromiseResponseData { promise: record })
}

fn op_promise_register_callback(
    tx: &mut Tx,
    r: &crate::core::types::PromiseRegisterCallbackData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.awaited, &r.awaiter], now, cfg);

    let awaited_record = match tx.doc.promises.get(&r.awaited) {
        Some(p) => p.to_record(&r.awaited),
        None => return Reply::err(404, "Awaited promise not found"),
    };
    let (awaiter_state, awaiter_has_target) = match tx.doc.promises.get(&r.awaiter) {
        Some(p) => (p.state, p.target().is_some()),
        None => return Reply::err(422, "Awaiter promise not found"),
    };
    if !awaiter_has_target {
        return Reply::err(422, "Awaiter promise has no resonate:target tag");
    }

    let awaited_pending = awaited_record.state == PromiseState::Pending;
    let awaiter_pending = awaiter_state == PromiseState::Pending;

    if awaited_pending && awaiter_pending {
        register_callback(tx, &r.awaited, &r.awaiter);
    } else if !awaited_pending && awaiter_pending {
        // The awaited promise is already settled, so there is nothing to wait
        // for: wake the awaiter now instead of registering.
        resume_awaiter(tx, &r.awaiter, &r.awaited, now, cfg);
    }

    Reply::ok(&PromiseResponseData {
        promise: awaited_record,
    })
}

fn op_promise_register_listener(
    tx: &mut Tx,
    r: &crate::core::types::PromiseRegisterListenerData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    if !is_valid_address(&r.address) {
        return Reply::err(400, "Invalid listener address");
    }
    try_timeout(tx, &[&r.awaited], now, cfg);
    let pending = match tx.doc.promises.get(&r.awaited) {
        Some(p) => p.state == PromiseState::Pending,
        None => return Reply::err(404, "Awaited promise not found"),
    };
    if pending {
        let p = tx.doc.promises.get_mut(&r.awaited).expect("checked above");
        if !p.listeners.iter().any(|a| a == &r.address) {
            p.listeners.push(r.address.clone());
        }
    }
    let p = &tx.doc.promises[&r.awaited];
    Reply::ok(&PromiseResponseData {
        promise: p.to_record(&r.awaited),
    })
}

// ============================================================================
// Shared state transitions
// ============================================================================

/// Create a promise and, when it carries a `resonate:target`, its task.
///
/// Mirrors `Server::op_promise_create` + `Db::promise_create`
/// (`persistence_sqlite.rs:434-500`): a promise created past its own deadline
/// is born settled — resolved if it is a timer, timed out otherwise — with
/// `created_at` and `settled_at` both stamped at the deadline, and its task
/// born already fulfilled.
pub(crate) fn create_promise(
    tx: &mut Tx,
    id: &str,
    r: &PromiseCreateData,
    now: i64,
    cfg: &KernelCfg,
) -> PromiseRecord {
    let tags: BTreeMap<String, String> = r
        .tags
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let already_timedout = now >= r.timeout_at;
    let doc = PromiseDoc {
        state: PromiseState::Pending,
        param: r.param.clone(),
        value: PromiseValue::default(),
        tags,
        timeout_at: r.timeout_at,
        created_at: if already_timedout { r.timeout_at } else { now },
        settled_at: None,
        callbacks: Vec::new(),
        listeners: Vec::new(),
    };
    let mut doc = doc;
    if already_timedout {
        doc.state = doc.timeout_state();
        doc.settled_at = Some(r.timeout_at);
    }
    let has_target = doc.target().is_some();
    let created_at = doc.created_at;
    let delay_at = doc.tags.get(TAG_DELAY).and_then(|v| v.parse::<i64>().ok());
    let record = doc.to_record(id);
    tx.doc.promises.insert(id.to_string(), doc);

    if !has_target {
        // No target means no task and no armed deadline: such a promise only
        // ever expires lazily, when someone reads it.
        return record;
    }

    if already_timedout {
        tx.doc.tasks.insert(
            id.to_string(),
            TaskDoc {
                state: TaskState::Fulfilled,
                version: 0,
                pid: None,
                ttl: None,
                resumes: BTreeSet::new(),
                retry_at: None,
                lease_at: None,
            },
        );
        return record;
    }

    let mut task = TaskDoc {
        state: TaskState::Pending,
        version: 0,
        pid: None,
        ttl: None,
        resumes: BTreeSet::new(),
        retry_at: None,
        lease_at: None,
    };
    // `resonate:delay` is an absolute instant before which the task must not be
    // dispatched: arm the retry timer there and send nothing. This follows
    // `src/oracle.rs:281-299`; the SQL backends do not implement it.
    match delay_at {
        Some(at) if now < at => {
            task.arm_retry(at);
            tx.doc.tasks.insert(id.to_string(), task);
        }
        _ => {
            task.arm_retry(created_at + cfg.retry_timeout);
            tx.doc.tasks.insert(id.to_string(), task);
            tx.send_execute(id, 0);
        }
    }
    record
}

/// Settle a pending promise and run its settlement chain.
///
/// Returns the settled record as the caller must report it — captured before
/// the chain runs, which only touches registrations, never the promise's own
/// fields.
pub(crate) fn settle(
    tx: &mut Tx,
    id: &str,
    state: SettleState,
    value: &PromiseValue,
    now: i64,
    cfg: &KernelCfg,
) -> PromiseRecord {
    {
        let p = tx.doc.promises.get_mut(id).expect("caller checked presence");
        debug_assert_eq!(p.state, PromiseState::Pending);
        p.state = match state {
            SettleState::Resolved => PromiseState::Resolved,
            SettleState::Rejected => PromiseState::Rejected,
            SettleState::RejectedCanceled => PromiseState::RejectedCanceled,
        };
        p.value = value.clone();
        p.settled_at = Some(now);
    }
    let record = tx.doc.promises[id].to_record(id);
    trigger_settlement(tx, id, now, cfg);
    record
}

/// Settle every named promise whose deadline has passed.
///
/// `Db::try_timeout`: a timer resolves, everything else becomes
/// `rejected_timedout`, and `settled_at` is the *deadline*, not `now` — so the
/// record is identical whenever the expiry is noticed.
pub(crate) fn try_timeout(tx: &mut Tx, ids: &[&str], now: i64, cfg: &KernelCfg) {
    let expired: Vec<(String, PromiseState, i64)> = ids
        .iter()
        .filter_map(|id| {
            tx.doc
                .promises
                .get(*id)
                .filter(|p| p.state == PromiseState::Pending && now >= p.timeout_at)
                .map(|p| (id.to_string(), p.timeout_state(), p.timeout_at))
        })
        .collect();
    for (id, state, timeout_at) in expired {
        if let Some(p) = tx.doc.promises.get_mut(&id) {
            p.state = state;
            p.settled_at = Some(timeout_at);
        }
        trigger_settlement(tx, &id, now, cfg);
    }
}

/// The settlement chain: fulfil the promise's own task, wake its awaiters,
/// notify its listeners.
///
/// One pass, in this order, matching `settle_promise`
/// (`persistence_sqlite.rs:314-340`): `settlement_enqueued`, then
/// `resumption_enqueued`, then `listener_unblocked`.
pub(crate) fn trigger_settlement(tx: &mut Tx, id: &str, now: i64, cfg: &KernelCfg) {
    trigger_fulfilled(tx, id);
    trigger_callbacks(tx, id, now, cfg);
    trigger_listeners(tx, id);
}

/// `settlement_enqueued`: the settled promise's own task is done, and its
/// registrations against other promises are dropped.
pub(crate) fn trigger_fulfilled(tx: &mut Tx, id: &str) {
    let live = tx
        .doc
        .tasks
        .get(id)
        .is_some_and(|t| t.state != TaskState::Fulfilled);
    if !live {
        return;
    }
    let t = tx.doc.tasks.get_mut(id).expect("checked above");
    t.state = TaskState::Fulfilled;
    t.pid = None;
    t.ttl = None;
    t.resumes.clear();
    t.disarm();
    // DELETE FROM callbacks WHERE awaiter_id = id — a finished task waits on
    // nothing.
    for p in tx.doc.promises.values_mut() {
        p.callbacks.retain(|awaiter| awaiter != id);
    }
}

/// `resumption_enqueued`: every awaiter registered against `awaited` observes
/// the settlement, in registration order.
pub(crate) fn trigger_callbacks(tx: &mut Tx, awaited: &str, now: i64, cfg: &KernelCfg) {
    let awaiters = match tx.doc.promises.get_mut(awaited) {
        Some(p) => std::mem::take(&mut p.callbacks),
        None => return,
    };
    for awaiter in awaiters {
        let live = tx
            .doc
            .promises
            .get(&awaiter)
            .is_some_and(|p| p.state == PromiseState::Pending && now < p.timeout_at);
        if !live {
            // The awaiter is itself settled or already past its deadline; this
            // sweep will fulfil it rather than resume it.
            continue;
        }
        resume_awaiter(tx, &awaiter, awaited, now, cfg);
    }
}

/// Wake one awaiter because `awaited` settled.
///
/// A suspended task goes back to pending and is re-dispatched; a task that is
/// already running only records the resume, which is what `resumes` counts.
pub(crate) fn resume_awaiter(
    tx: &mut Tx,
    awaiter: &str,
    awaited: &str,
    now: i64,
    cfg: &KernelCfg,
) {
    let state = match tx.doc.tasks.get(awaiter) {
        Some(t) => t.state,
        None => return,
    };
    match state {
        TaskState::Suspended => {
            let version = {
                let t = tx.doc.tasks.get_mut(awaiter).expect("checked above");
                t.state = TaskState::Pending;
                t.resumes.clear();
                t.resumes.insert(awaited.to_string());
                t.arm_retry(now + cfg.retry_timeout);
                t.version
            };
            tx.send_execute(awaiter, version);
        }
        TaskState::Pending | TaskState::Acquired => {
            let t = tx.doc.tasks.get_mut(awaiter).expect("checked above");
            t.resumes.insert(awaited.to_string());
        }
        // A halted task is not listening, and a fulfilled one is done.
        TaskState::Halted | TaskState::Fulfilled => {}
    }
}

/// Register `awaiter` against `awaited`, keeping registration order and
/// rejecting duplicates — `callbacks`' primary key is `(awaited, awaiter)`.
pub(crate) fn register_callback(tx: &mut Tx, awaited: &str, awaiter: &str) {
    let has_target = tx
        .doc
        .promises
        .get(awaiter)
        .is_some_and(|p| p.target().is_some());
    if !has_target {
        return;
    }
    let p = match tx.doc.promises.get_mut(awaited) {
        Some(p) => p,
        None => return,
    };
    if !p.callbacks.iter().any(|a| a == awaiter) {
        p.callbacks.push(awaiter.to_string());
    }
}

/// `listener_unblocked`: hand the settled promise to everyone listening, then
/// forget them.
pub(crate) fn trigger_listeners(tx: &mut Tx, id: &str) {
    let listeners = match tx.doc.promises.get_mut(id) {
        Some(p) => std::mem::take(&mut p.listeners),
        None => return,
    };
    if listeners.is_empty() {
        return;
    }
    let promise = tx.doc.promises[id].to_record(id);
    for address in listeners {
        tx.sends.push((
            address,
            OutEntry::Unblock {
                promise_id: id.to_string(),
                promise: promise.clone(),
            },
        ));
    }
}

/// The promises a worker is handed alongside a task: everything sharing the
/// task promise's `resonate:branch`, itself excluded.
///
/// `Db::compute_preload` (`persistence_sqlite.rs:1176-1200`). A branch is
/// always a prefix of the ids under it, so a branch never spans origins and
/// this stays a single-document read.
pub(crate) fn preload(doc: &OriginDoc, id: &str) -> Vec<PromiseRecord> {
    let branch = match doc.promises.get(id).and_then(|p| p.tags.get(TAG_BRANCH)) {
        Some(b) if !b.is_empty() => b.clone(),
        _ => return Vec::new(),
    };
    doc.promises
        .iter()
        .filter(|(other, p)| {
            other.as_str() != id && p.tags.get(TAG_BRANCH).is_some_and(|b| *b == branch)
        })
        .map(|(other, p)| p.to_record(other))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::state::{apply_effects, TAG_TIMER};
    use serde_json::json;

    const W: &str = "http://worker:9999";

    fn cfg() -> KernelCfg {
        KernelCfg {
            retry_timeout: 30_000,
        }
    }

    fn parse<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).expect("test fixture deserializes")
    }

    /// Apply a request and return the new document, the sends, and the reply.
    fn step(doc: &OriginDoc, req: Req, now: i64) -> (OriginDoc, Vec<(String, OutEntry)>, Reply) {
        let (fx, reply) = handle(doc, &req, now, &cfg());
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        let sends = fx
            .into_iter()
            .filter_map(|e| match e {
                Effect::Send { address, out } => Some((address, out)),
                _ => None,
            })
            .collect();
        (next, sends, reply)
    }

    fn create(id: &str, timeout_at: i64, tags: serde_json::Value) -> Req {
        Req::PromiseCreate(parse(
            json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags }),
        ))
    }

    fn get(id: &str) -> Req {
        Req::PromiseGet(parse(json!({ "id": id })))
    }

    fn settle_req(id: &str, state: &str) -> Req {
        Req::PromiseSettle(parse(json!({ "id": id, "state": state, "value": {} })))
    }

    fn callback(awaited: &str, awaiter: &str) -> Req {
        Req::PromiseRegisterCallback(parse(json!({ "awaited": awaited, "awaiter": awaiter })))
    }

    fn listener(awaited: &str, address: &str) -> Req {
        Req::PromiseRegisterListener(parse(json!({ "awaited": awaited, "address": address })))
    }

    /// Park a task in `suspended` — the state `task.suspend` produces, which
    /// this commit cannot reach yet.
    fn suspend(doc: &mut OriginDoc, id: &str) {
        let t = doc.tasks.get_mut(id).expect("task exists");
        t.state = TaskState::Suspended;
        t.disarm();
    }

    /// A document holding one pending, targeted promise `o:a` created at 0 with
    /// deadline 100_000 — so it has a task, a retry timer, and one dispatch.
    fn with_targeted(id: &str, timeout_at: i64) -> OriginDoc {
        let (doc, _, reply) = step(
            &OriginDoc::default(),
            create(id, timeout_at, json!({ "resonate:target": W })),
            0,
        );
        assert_eq!(reply.status, 200);
        doc
    }

    // --- create ------------------------------------------------------------

    #[test]
    fn creating_an_untargeted_promise_makes_no_task_and_arms_no_timer() {
        let (doc, sends, reply) = step(&OriginDoc::default(), create("o:a", 100, json!({})), 0);
        assert_eq!(reply.status, 200);
        assert_eq!(doc.promises["o:a"].state, PromiseState::Pending);
        assert!(doc.tasks.is_empty());
        assert_eq!(doc.timer_at, None);
        assert!(sends.is_empty());
    }

    #[test]
    fn creating_a_targeted_promise_arms_a_retry_timer_and_dispatches() {
        let (doc, sends, _) = step(
            &OriginDoc::default(),
            create("o:a", 100_000, json!({ "resonate:target": W })),
            1_000,
        );
        let task = &doc.tasks["o:a"];
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.version, 0);
        assert_eq!(task.retry_at, Some(31_000));
        assert_eq!(task.lease_at, None);
        // The armed set is the retry timer and the promise deadline; the timer
        // object sits at the nearer of the two.
        assert_eq!(doc.timer_at, Some(31_000));
        assert_eq!(
            sends,
            vec![(
                W.to_string(),
                OutEntry::Execute {
                    task_id: "o:a".into(),
                    version: 0
                }
            )]
        );
    }

    #[test]
    fn a_promise_created_past_its_deadline_is_born_timed_out() {
        let (doc, sends, reply) = step(
            &OriginDoc::default(),
            create("o:a", 500, json!({ "resonate:target": W })),
            900,
        );
        let p = &doc.promises["o:a"];
        assert_eq!(p.state, PromiseState::RejectedTimedout);
        // Both stamps are the deadline, not `now`: the record reads the same
        // whenever the expiry is noticed.
        assert_eq!((p.created_at, p.settled_at), (500, Some(500)));
        assert_eq!(doc.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(doc.timer_at, None);
        assert!(sends.is_empty());
        assert_eq!(reply.data["promise"]["state"], "rejected_timedout");
    }

    #[test]
    fn a_timer_promise_created_past_its_deadline_resolves_instead() {
        let (doc, _, _) = step(
            &OriginDoc::default(),
            create("o:a", 500, json!({ TAG_TIMER: "true" })),
            900,
        );
        assert_eq!(doc.promises["o:a"].state, PromiseState::Resolved);
    }

    #[test]
    fn create_is_idempotent_on_the_id_alone() {
        let doc = with_targeted("o:a", 100_000);
        let (next, sends, reply) = step(
            &doc,
            create("o:a", 999_999, json!({ "resonate:target": W })),
            1,
        );
        assert_eq!(reply.status, 200);
        // The stored promise wins: the second create's timeout is ignored.
        assert_eq!(reply.data["promise"]["timeoutAt"], 100_000);
        assert_eq!(next, doc);
        assert!(sends.is_empty());
    }

    #[test]
    fn create_rejects_a_target_that_is_not_an_address() {
        let (doc, _, reply) = step(
            &OriginDoc::default(),
            create("o:a", 100, json!({ "resonate:target": "not a url" })),
            0,
        );
        assert_eq!(reply.status, 400);
        assert_eq!(reply.data, json!("Invalid resonate:target address"));
        assert!(doc.promises.is_empty());
    }

    #[test]
    fn a_delay_tag_defers_the_first_dispatch_to_its_instant() {
        // resonate:delay is an absolute instant, per src/oracle.rs:281-299.
        let (doc, sends, _) = step(
            &OriginDoc::default(),
            create(
                "o:a",
                100_000,
                json!({ "resonate:target": W, "resonate:delay": "5000" }),
            ),
            1_000,
        );
        assert_eq!(doc.tasks["o:a"].retry_at, Some(5_000));
        assert!(sends.is_empty(), "a delayed task is not dispatched yet");
        assert_eq!(doc.timer_at, Some(5_000));
    }

    #[test]
    fn a_delay_already_past_dispatches_immediately() {
        let (doc, sends, _) = step(
            &OriginDoc::default(),
            create(
                "o:a",
                100_000,
                json!({ "resonate:target": W, "resonate:delay": "500" }),
            ),
            1_000,
        );
        assert_eq!(doc.tasks["o:a"].retry_at, Some(31_000));
        assert_eq!(sends.len(), 1);
    }

    // --- get ---------------------------------------------------------------

    #[test]
    fn getting_an_unknown_promise_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), get("o:a"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Promise not found"));
    }

    #[test]
    fn getting_an_expired_promise_settles_it_first() {
        let doc = with_targeted("o:a", 1_000);
        let (next, _, reply) = step(&doc, get("o:a"), 1_500);
        assert_eq!(reply.data["promise"]["state"], "rejected_timedout");
        assert_eq!(reply.data["promise"]["settledAt"], 1_000);
        // The read is a real transition: the task is fulfilled and the origin's
        // timer disarmed.
        assert_eq!(next.promises["o:a"].state, PromiseState::RejectedTimedout);
        assert_eq!(next.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(next.timer_at, None);
    }

    #[test]
    fn getting_a_live_promise_changes_nothing() {
        let doc = with_targeted("o:a", 100_000);
        let (next, sends, reply) = step(&doc, get("o:a"), 500);
        assert_eq!(reply.status, 200);
        assert_eq!(next, doc);
        assert!(sends.is_empty());
    }

    // --- settle ------------------------------------------------------------

    #[test]
    fn settling_an_unknown_promise_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), settle_req("o:a", "resolved"), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn settling_stamps_now_and_fulfils_the_promises_own_task() {
        let doc = with_targeted("o:a", 100_000);
        let (next, _, reply) = step(&doc, settle_req("o:a", "resolved"), 700);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert_eq!(reply.data["promise"]["settledAt"], 700);
        assert_eq!(next.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(next.timer_at, None);
    }

    #[test]
    fn settling_twice_reports_the_first_settlement() {
        let doc = with_targeted("o:a", 100_000);
        let (once, _, _) = step(&doc, settle_req("o:a", "resolved"), 700);
        let (twice, sends, reply) = step(&once, settle_req("o:a", "rejected"), 800);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert_eq!(reply.data["promise"]["settledAt"], 700);
        assert_eq!(twice, once);
        assert!(sends.is_empty());
    }

    #[test]
    fn settling_unblocks_listeners_and_forgets_them() {
        let doc = with_targeted("o:a", 100_000);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 1);
        let (doc, _, _) = step(&doc, listener("o:a", "http://two"), 2);
        assert_eq!(doc.promises["o:a"].listeners, vec!["http://one", "http://two"]);

        let (next, sends, _) = step(&doc, settle_req("o:a", "resolved"), 700);
        assert_eq!(sends.len(), 2);
        // Registration order is preserved on the way out.
        assert_eq!(sends[0].0, "http://one");
        assert_eq!(sends[1].0, "http://two");
        match &sends[0].1 {
            OutEntry::Unblock { promise_id, promise } => {
                assert_eq!(promise_id, "o:a");
                assert_eq!(promise.state, PromiseState::Resolved);
            }
            other => panic!("expected unblock, got {other:?}"),
        }
        assert!(next.promises["o:a"].listeners.is_empty());
    }

    #[test]
    fn settling_fans_out_to_awaiters_in_registration_order() {
        // Two suspended awaiters, registered in a known order, both waiting on
        // the same promise: the dispatches must come back in that order.
        let mut doc = with_targeted("o:awaited", 100_000);
        for awaiter in ["o:z", "o:a"] {
            let (next, _, _) = step(
                &doc,
                create(awaiter, 100_000, json!({ "resonate:target": W })),
                0,
            );
            doc = next;
            suspend(&mut doc, awaiter);
            let (next, _, reply) = step(&doc, callback("o:awaited", awaiter), 1);
            assert_eq!(reply.status, 200);
            doc = next;
        }
        assert_eq!(doc.promises["o:awaited"].callbacks, vec!["o:z", "o:a"]);

        let (next, sends, _) = step(&doc, settle_req("o:awaited", "resolved"), 700);
        let executed: Vec<&str> = sends
            .iter()
            .filter_map(|(_, o)| match o {
                OutEntry::Execute { task_id, .. } => Some(task_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(executed, vec!["o:z", "o:a"], "fanout follows registration");
        for awaiter in ["o:z", "o:a"] {
            let t = &next.tasks[awaiter];
            assert_eq!(t.state, TaskState::Pending);
            assert_eq!(t.retry_at, Some(30_700));
            assert_eq!(t.resumes.iter().collect::<Vec<_>>(), vec!["o:awaited"]);
        }
        assert!(next.promises["o:awaited"].callbacks.is_empty());
    }

    #[test]
    fn a_settled_awaiter_is_not_resumed() {
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        suspend(&mut doc, "o:awaiter");
        let (next, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        // Settle the awaiter itself, then the awaited promise.
        let (next, _, _) = step(&next, settle_req("o:awaiter", "resolved"), 2);
        let (next, sends, _) = step(&next, settle_req("o:awaited", "resolved"), 3);
        assert!(sends.is_empty());
        assert_eq!(next.tasks["o:awaiter"].state, TaskState::Fulfilled);
    }

    #[test]
    fn fulfilling_a_task_drops_its_registrations_against_others() {
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        suspend(&mut doc, "o:awaiter");
        let (doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        assert_eq!(doc.promises["o:awaited"].callbacks, vec!["o:awaiter"]);

        // Settling the awaiter fulfils its task, which drops the registration.
        let (next, _, _) = step(&doc, settle_req("o:awaiter", "resolved"), 2);
        assert!(next.promises["o:awaited"].callbacks.is_empty());
    }

    // --- register_callback -------------------------------------------------

    #[test]
    fn registering_against_an_unknown_awaited_is_a_404() {
        let doc = with_targeted("o:awaiter", 100_000);
        let (_, _, reply) = step(&doc, callback("o:missing", "o:awaiter"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Awaited promise not found"));
    }

    #[test]
    fn registering_an_unknown_awaiter_is_a_422() {
        let doc = with_targeted("o:awaited", 100_000);
        let (_, _, reply) = step(&doc, callback("o:awaited", "o:missing"), 0);
        assert_eq!(reply.status, 422);
        assert_eq!(reply.data, json!("Awaiter promise not found"));
    }

    #[test]
    fn an_awaiter_without_a_target_cannot_register() {
        let doc = with_targeted("o:awaited", 100_000);
        let (doc, _, _) = step(&doc, create("o:awaiter", 100_000, json!({})), 0);
        let (next, _, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 0);
        assert_eq!(reply.status, 422);
        assert_eq!(reply.data, json!("Awaiter promise has no resonate:target tag"));
        assert!(next.promises["o:awaited"].callbacks.is_empty());
    }

    #[test]
    fn registering_twice_registers_once() {
        let doc = with_targeted("o:awaited", 100_000);
        let (doc, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        let (doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        let (doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(doc.promises["o:awaited"].callbacks, vec!["o:awaiter"]);
    }

    #[test]
    fn registering_against_a_settled_promise_resumes_instead() {
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        let (mut doc, _, _) = step(&doc, settle_req("o:awaited", "resolved"), 1);
        suspend(&mut doc, "o:awaiter");

        let (next, sends, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert!(next.promises["o:awaited"].callbacks.is_empty());
        let t = &next.tasks["o:awaiter"];
        assert_eq!(t.state, TaskState::Pending);
        assert_eq!(t.retry_at, Some(30_002));
        assert_eq!(sends.len(), 1);
    }

    #[test]
    fn registering_against_a_settled_promise_records_a_resume_for_a_running_task() {
        let doc = with_targeted("o:awaited", 100_000);
        let (doc, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        let (doc, _, _) = step(&doc, settle_req("o:awaited", "resolved"), 1);
        // The awaiter's task is still pending — already running, so it is not
        // re-dispatched; the resume is only recorded.
        let (next, sends, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(reply.status, 200);
        assert!(sends.is_empty());
        assert_eq!(
            next.tasks["o:awaiter"].resumes.iter().collect::<Vec<_>>(),
            vec!["o:awaited"]
        );
    }

    // --- register_listener -------------------------------------------------

    #[test]
    fn a_listener_address_must_be_an_address() {
        let doc = with_targeted("o:a", 100_000);
        let (next, _, reply) = step(&doc, listener("o:a", "not a url"), 0);
        assert_eq!(reply.status, 400);
        assert_eq!(reply.data, json!("Invalid listener address"));
        assert_eq!(next, doc);
    }

    #[test]
    fn listening_to_an_unknown_promise_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), listener("o:a", "http://one"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Awaited promise not found"));
    }

    #[test]
    fn listening_to_a_settled_promise_registers_nothing() {
        let doc = with_targeted("o:a", 100_000);
        let (doc, _, _) = step(&doc, settle_req("o:a", "resolved"), 1);
        let (next, sends, reply) = step(&doc, listener("o:a", "http://one"), 2);
        assert_eq!(reply.status, 200);
        assert!(next.promises["o:a"].listeners.is_empty());
        assert!(sends.is_empty());
    }

    #[test]
    fn listening_twice_from_one_address_registers_once() {
        let doc = with_targeted("o:a", 100_000);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 1);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 2);
        assert_eq!(doc.promises["o:a"].listeners, vec!["http://one"]);
    }

    // --- preload -----------------------------------------------------------

    #[test]
    fn preload_is_the_rest_of_the_branch() {
        let branch = json!({ "resonate:target": W, "resonate:branch": "o" });
        let (doc, _, _) = step(&OriginDoc::default(), create("o:a", 100_000, branch.clone()), 0);
        let (doc, _, _) = step(&doc, create("o:b", 100_000, branch), 0);
        let (doc, _, _) = step(&doc, create("o:c", 100_000, json!({})), 0);
        let ids: Vec<String> = preload(&doc, "o:a").into_iter().map(|p| p.id).collect();
        assert_eq!(ids, vec!["o:b"]);
    }

    #[test]
    fn a_promise_without_a_branch_preloads_nothing() {
        let doc = with_targeted("o:a", 100_000);
        assert!(preload(&doc, "o:a").is_empty());
    }
}
