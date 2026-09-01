//! The sweep: everything whose deadline has passed, in one pass.
//!
//! `drain` is `Db::process_timeouts` (`persistence_sqlite.rs:1422-1556`) as a
//! pure function, restricted to one origin. Its three phases run in that
//! backend's order — armed promise deadlines, then task retry deadlines, then
//! task lease deadlines — and each phase reads the state the previous one left,
//! which is what makes a task fulfilled by an expiring promise drop out of the
//! retry sweep instead of being re-dispatched.
//!
//! Where [`handle`](super::handle) settles only the promises a request *names*,
//! `drain` settles every armed one. That is the whole difference between the
//! two, and it is why the shell needs a timer at all.
//!
//! # Dependencies
//!
//! [`handle`](super::handle)'s settlement machinery (`Tx`,
//! `trigger_settlement`) and the state types. Pure, like `handle`.
//!
//! # Dependants
//!
//! The s3 applier's `tick`, which the timer poller's sweep and `debug.tick`
//! drive.

use resonate_core::types::{PromiseState, TaskState};

use super::handle::{trigger_settlement, Tx};
use super::state::{Effect, KernelCfg, OriginDoc};

/// Sweep every deadline at or before `now`.
pub fn drain(doc: &OriginDoc, now: i64, cfg: &KernelCfg) -> Vec<Effect> {
    let mut tx = Tx::new(doc, cfg);

    // Phase 1 — settle every armed promise deadline that has passed. All of
    // them first, then their chains, so an awaiter that is itself expiring is
    // already settled when its awaited promise fans out and is skipped rather
    // than resumed.
    let expired: Vec<String> = tx
        .doc
        .promises
        .iter()
        .filter(|(_, p)| p.timeout_armed() && now >= p.timeout_at)
        .map(|(id, _)| id.clone())
        .collect();
    for id in &expired {
        let p = tx.doc.promises.get_mut(id).expect("just listed");
        p.state = p.timeout_state();
        p.settled_at = Some(p.timeout_at);
    }
    // Phase 2 — the settlement chain for each, in id order.
    for id in &expired {
        trigger_settlement(&mut tx, id, now, cfg);
    }

    // Phase 3 — re-dispatch pending tasks whose retry deadline has passed.
    // Read after phase 2, as the SQL backend's second statement is: a task the
    // settlement just fulfilled has no timer left to fire.
    let retries: Vec<String> = tx
        .doc
        .tasks
        .iter()
        .filter(|(_, t)| t.state == TaskState::Pending && t.retry_at.is_some_and(|at| at <= now))
        .map(|(id, _)| id.clone())
        .collect();
    for id in &retries {
        let version = {
            let t = tx.doc.tasks.get_mut(id).expect("just listed");
            t.arm_retry(now + cfg.retry_timeout);
            t.version
        };
        tx.send_execute(id, version);
    }

    // Phase 4 — expire leases. The holder is presumed gone, so the task goes
    // back to pending at the *same* version and is re-dispatched; whoever
    // picks it up bumps the version and fences the old holder out.
    let leases: Vec<String> = tx
        .doc
        .tasks
        .iter()
        .filter(|(_, t)| t.state == TaskState::Acquired && t.lease_at.is_some_and(|at| at <= now))
        .map(|(id, _)| id.clone())
        .collect();
    for id in &leases {
        let version = {
            let t = tx.doc.tasks.get_mut(id).expect("just listed");
            t.state = TaskState::Pending;
            t.pid = None;
            t.ttl = None;
            t.arm_retry(now + cfg.retry_timeout);
            t.version
        };
        tx.send_execute(id, version);
    }

    debug_assert!(
        !expired.is_empty()
            || !retries.is_empty()
            || !leases.is_empty()
            || tx.doc.promises == doc.promises && tx.doc.tasks == doc.tasks,
        "a drain that fired nothing must change nothing"
    );
    debug_assert!(
        tx.doc
            .promises
            .values()
            .all(|p| p.state != PromiseState::Pending || now < p.timeout_at || !p.timeout_armed()),
        "drain left an armed deadline in the past"
    );
    tx.finish(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::handle::handle;
    use crate::kernel::state::{apply_effects, Req};
    use resonate_core::types::Message;
    use serde_json::json;

    const W: &str = "http://worker:9999";
    const PID: &str = "pid-1";

    fn cfg() -> KernelCfg {
        KernelCfg {
            retry_timeout: 30_000,
            ..Default::default()
        }
    }

    fn req<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).expect("test fixture deserializes")
    }

    fn apply(doc: &OriginDoc, r: Req, now: i64) -> OriginDoc {
        let (fx, reply) = handle(doc, &r, now, &cfg());
        assert!(reply.status < 400, "setup request failed: {reply:?}");
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        next
    }

    fn sweep(doc: &OriginDoc, now: i64) -> (OriginDoc, Vec<(String, Message)>) {
        let fx = drain(doc, now, &cfg());
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        let sends = fx
            .into_iter()
            .filter_map(|e| match e {
                Effect::Send { address, msg } => Some((address, *msg)),
                _ => None,
            })
            .collect();
        (next, sends)
    }

    fn targeted(id: &str, timeout_at: i64) -> Req {
        Req::PromiseCreate(req(json!({
            "id": id, "timeoutAt": timeout_at, "param": {},
            "tags": { "resonate:target": W }
        })))
    }

    fn plain(id: &str, timeout_at: i64) -> Req {
        Req::PromiseCreate(req(
            json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": {} }),
        ))
    }

    fn executed(sends: &[(String, Message)]) -> Vec<(String, i64)> {
        sends
            .iter()
            .filter_map(|(_, o)| match o {
                Message::Execute(e) => Some((e.data.task.id.clone(), e.data.task.version)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn an_empty_document_drains_to_nothing() {
        let (next, sends) = sweep(&OriginDoc::default(), 1_000_000);
        assert_eq!(next, OriginDoc::default());
        assert!(sends.is_empty());
    }

    #[test]
    fn a_document_with_nothing_due_is_unchanged() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 100_000), 0);
        let (next, sends) = sweep(&doc, 1_000);
        assert_eq!(next, doc);
        assert!(sends.is_empty());
    }

    #[test]
    fn an_expired_promise_settles_at_its_own_deadline() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 1_000), 0);
        let (next, _) = sweep(&doc, 5_000);
        let p = &next.promises["o:a"];
        assert_eq!(p.state, PromiseState::RejectedTimedout);
        // The stamp is the deadline, not the sweep time.
        assert_eq!(p.settled_at, Some(1_000));
        assert_eq!(next.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(next.timer_at, None);
    }

    #[test]
    fn an_expired_timer_promise_resolves() {
        let r = Req::PromiseCreate(req(json!({
            "id": "o:a", "timeoutAt": 1_000, "param": {},
            "tags": { "resonate:target": W, "resonate:timer": "true" }
        })));
        let doc = apply(&OriginDoc::default(), r, 0);
        let (next, _) = sweep(&doc, 5_000);
        assert_eq!(next.promises["o:a"].state, PromiseState::Resolved);
    }

    #[test]
    fn an_untargeted_promise_is_not_swept() {
        // No resonate:target means no promise_timeouts row: it expires lazily,
        // when a request names it.
        let doc = apply(&OriginDoc::default(), plain("o:a", 1_000), 0);
        let (next, sends) = sweep(&doc, 5_000);
        assert_eq!(next.promises["o:a"].state, PromiseState::Pending);
        assert!(sends.is_empty());
    }

    #[test]
    fn expired_promises_settle_in_id_order() {
        let mut doc = OriginDoc::default();
        for id in ["o:c", "o:a", "o:b"] {
            doc = apply(&doc, targeted(id, 1_000), 0);
        }
        // Every task is fulfilled by its own promise expiring, so no dispatches
        // remain — the visible order is the settle order in the document.
        let (next, sends) = sweep(&doc, 5_000);
        assert!(sends.is_empty());
        let states: Vec<&str> = next.promises.values().map(|p| p.state.as_str()).collect();
        assert_eq!(states, vec!["rejected_timedout"; 3]);
        assert_eq!(
            next.promises.keys().cloned().collect::<Vec<_>>(),
            vec!["o:a", "o:b", "o:c"]
        );
    }

    #[test]
    fn an_expiring_promise_wakes_its_suspended_awaiters_in_registration_order() {
        let mut doc = apply(&OriginDoc::default(), targeted("o:awaited", 1_000), 0);
        for awaiter in ["o:z", "o:a"] {
            doc = apply(&doc, targeted(awaiter, 100_000), 0);
            let t = doc.tasks.get_mut(awaiter).unwrap();
            t.state = TaskState::Suspended;
            t.disarm();
            doc = apply(
                &doc,
                Req::PromiseRegisterCallback(req(
                    json!({ "awaited": "o:awaited", "awaiter": awaiter }),
                )),
                1,
            );
        }
        let (next, sends) = sweep(&doc, 5_000);
        assert_eq!(
            executed(&sends),
            vec![("o:z".to_string(), 0), ("o:a".to_string(), 0)]
        );
        for awaiter in ["o:z", "o:a"] {
            let t = &next.tasks[awaiter];
            assert_eq!(t.state, TaskState::Pending);
            assert_eq!(t.retry_at, Some(35_000));
        }
    }

    #[test]
    fn a_task_fulfilled_by_the_same_sweep_is_not_re_dispatched() {
        // The promise deadline and the task's retry deadline both fall due.
        // Phase 1 fulfils the task, so phase 3 has nothing to re-send.
        let doc = apply(&OriginDoc::default(), targeted("o:a", 1_000), 0);
        assert_eq!(doc.tasks["o:a"].retry_at, Some(30_000));
        let (next, sends) = sweep(&doc, 40_000);
        assert_eq!(next.tasks["o:a"].state, TaskState::Fulfilled);
        assert!(sends.is_empty(), "a finished task is not dispatched");
    }

    #[test]
    fn a_pending_tasks_retry_deadline_re_dispatches_it() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 1_000_000), 0);
        let (next, sends) = sweep(&doc, 30_000);
        assert_eq!(executed(&sends), vec![("o:a".to_string(), 0)]);
        let t = &next.tasks["o:a"];
        assert_eq!(t.state, TaskState::Pending);
        // Re-armed, so a worker that never answers is retried again.
        assert_eq!(t.retry_at, Some(60_000));
        assert_eq!(t.version, 0);
    }

    #[test]
    fn a_retry_is_re_armed_relative_to_the_sweep_not_the_old_deadline() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 1_000_000), 0);
        let (next, _) = sweep(&doc, 100_000);
        assert_eq!(next.tasks["o:a"].retry_at, Some(130_000));
    }

    #[test]
    fn an_expired_lease_returns_the_task_at_the_same_version() {
        let create = Req::TaskCreate(req(json!({
            "pid": PID, "ttl": 10_000,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": "o:t", "timeoutAt": 1_000_000, "param": {},
                "tags": { "resonate:target": W } } }
        })));
        let doc = apply(&OriginDoc::default(), create, 0);
        assert_eq!(doc.tasks["o:t"].lease_at, Some(10_000));

        let (next, sends) = sweep(&doc, 10_000);
        let t = &next.tasks["o:t"];
        assert_eq!(t.state, TaskState::Pending);
        assert_eq!((t.pid.as_deref(), t.ttl), (None, None));
        assert_eq!(t.retry_at, Some(40_000));
        assert_eq!(t.lease_at, None);
        // Same version: the next claim bumps it and fences the lost holder out.
        assert_eq!(executed(&sends), vec![("o:t".to_string(), 1)]);
        assert_eq!(t.version, 1);
    }

    #[test]
    fn a_lease_that_is_still_live_is_left_alone() {
        let create = Req::TaskCreate(req(json!({
            "pid": PID, "ttl": 10_000,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": "o:t", "timeoutAt": 1_000_000, "param": {},
                "tags": { "resonate:target": W } } }
        })));
        let doc = apply(&OriginDoc::default(), create, 0);
        let (next, sends) = sweep(&doc, 9_999);
        assert_eq!(next, doc);
        assert!(sends.is_empty());
    }

    #[test]
    fn sweeping_twice_at_the_same_instant_is_idempotent_but_for_the_retry_clock() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 1_000), 0);
        let (once, _) = sweep(&doc, 5_000);
        let (twice, sends) = sweep(&once, 5_000);
        assert_eq!(twice, once);
        assert!(sends.is_empty());
    }

    #[test]
    fn a_sweep_moves_the_origins_timer_to_the_next_deadline() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 45_000), 0);
        assert_eq!(doc.timer_at, Some(30_000));
        let fx = drain(&doc, 30_000, &cfg());
        assert!(fx.contains(&Effect::SetTimeout { at: 45_000 }));
        assert!(fx.contains(&Effect::DelTimeout { at: 30_000 }));
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        assert_eq!(next.timer_at, Some(45_000));
    }

    #[test]
    fn a_sweep_that_fires_nothing_emits_no_timer_movement() {
        let doc = apply(&OriginDoc::default(), targeted("o:a", 45_000), 0);
        let fx = drain(&doc, 100, &cfg());
        assert!(!fx
            .iter()
            .any(|e| matches!(e, Effect::SetTimeout { .. } | Effect::DelTimeout { .. })));
    }
}
