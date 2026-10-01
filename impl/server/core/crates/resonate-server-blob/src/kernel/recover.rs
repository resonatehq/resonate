//! Finishing what a settlement started — for a shell whose writes are not all
//! or nothing.
//!
//! # Contract
//!
//! The specification does not make a settlement and its consequences one
//! step. Settling writes the promise; delivering each callback (resume the
//! awaiter, then drop the callback) and each listener (send the unblock, then
//! drop the listener) are steps of their own, taken later. The kernel takes
//! them all at once, and a shell that commits atomically never shows anything
//! in between. A shell that cannot — one writing its records one by one, in
//! the order that makes every prefix a state the specification allows — can
//! stop part-way, and then holds one of these:
//!
//! - a **settled promise that still has callbacks or listeners**: the
//!   deliveries are owed. [`recover`] makes them, exactly as the settlement's
//!   fan-out would have ([`trigger_callbacks`], [`trigger_listeners`]):
//!   resuming is idempotent (a task already resumed by `awaited` only records
//!   the resume again), so a delivery that did land before the stop is not
//!   made twice — except for the unblock, which goes out again (at least once,
//!   as every message is).
//! - a **callback whose awaiter's task is fulfilled**: the cleanup of a
//!   finished task's registrations did not land. [`recover`] drops it, as
//!   [`super::handle::trigger_fulfilled`] would have. No state the kernel
//!   reaches holds one, because nothing registers a callback for a settled
//!   awaiter.
//!
//! Everything else a prefix can hold is a state the kernel reaches by itself
//! (a callback registered before its task suspends is what an explicit
//! registration makes, too). So after [`recover`], the document is one the
//! kernel could have produced, and it is the one it *did* produce when
//! recovery runs at the instant the interrupted decision was made.
//!
//! On a document that needs none of it, [`recover`] returns no effects, and
//! [`needs_recovery`] says so without cloning anything.

use std::collections::BTreeSet;

use resonate_core::types::{PromiseState, TaskState};

use super::handle::{trigger_callbacks, trigger_listeners, Tx};
use super::state::{Effect, KernelCfg, OriginDoc};

/// The fulfilled tasks some callback still names.
fn finished_awaiters(doc: &OriginDoc) -> BTreeSet<&str> {
    doc.promises
        .values()
        .flat_map(|p| p.callbacks.iter())
        .filter(|a| {
            doc.tasks
                .get(a.as_str())
                .is_some_and(|t| t.state == TaskState::Fulfilled)
        })
        .map(|a| a.as_str())
        .collect()
}

/// Whether `doc` holds anything [`recover`] would change.
pub fn needs_recovery(doc: &OriginDoc) -> bool {
    doc.promises.values().any(|p| {
        p.state != PromiseState::Pending && (!p.callbacks.is_empty() || !p.listeners.is_empty())
    }) || !finished_awaiters(doc).is_empty()
}

/// Make the deliveries a stopped settlement still owes, and drop the
/// registrations a finished task left behind. See the module docs.
pub fn recover(doc: &OriginDoc, now: i64, cfg: &KernelCfg) -> Vec<Effect> {
    if !needs_recovery(doc) {
        return Vec::new();
    }
    let finished: BTreeSet<String> = finished_awaiters(doc)
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut tx = Tx::new(doc, cfg);
    for p in tx.doc.promises.values_mut() {
        p.callbacks.retain(|a| !finished.contains(a));
    }
    let owed: Vec<String> = tx
        .doc
        .promises
        .iter()
        .filter(|(_, p)| {
            p.state != PromiseState::Pending && (!p.callbacks.is_empty() || !p.listeners.is_empty())
        })
        .map(|(id, _)| id.clone())
        .collect();
    for id in &owed {
        trigger_callbacks(&mut tx, id, now, cfg);
        trigger_listeners(&mut tx, id);
    }
    tx.finish(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::state::{apply_effects, Req};
    use crate::kernel::{drain, handle};
    use resonate_core::types::Message;
    use serde_json::json;

    const W: &str = "http://worker:9999";

    fn cfg() -> KernelCfg {
        KernelCfg {
            retry_timeout: 30_000,
            ..Default::default()
        }
    }

    fn parse<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).expect("fixture")
    }

    fn settle(id: &str) -> Req {
        Req::PromiseSettle(parse(json!({ "id": id, "state": "resolved", "value": {} })))
    }

    fn step(doc: &OriginDoc, r: Req, now: i64) -> (OriginDoc, Vec<Effect>) {
        let (fx, reply) = handle(doc, &r, now, &cfg());
        assert!(reply.status < 400, "{:?}", reply);
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        (next, fx)
    }

    fn create(id: &str, timeout_at: i64) -> Req {
        Req::PromiseCreate(parse(json!({
            "id": id, "timeoutAt": timeout_at, "param": {}, "tags": { "resonate:target": W }
        })))
    }

    fn sends(fx: &[Effect]) -> Vec<(String, &'static str)> {
        fx.iter()
            .filter_map(|e| match e {
                Effect::Send { address, msg } => Some((
                    address.clone(),
                    match **msg {
                        Message::Execute(_) => "execute",
                        Message::Unblock(_) => "unblock",
                    },
                )),
                _ => None,
            })
            .collect()
    }

    /// `o:awaited` with two suspended awaiters and a listener; `o:a` also
    /// awaits `o:other`.
    fn fixture() -> OriginDoc {
        let mut doc = OriginDoc::default();
        for id in ["o:awaited", "o:other", "o:a", "o:b"] {
            doc = step(&doc, create(id, 100_000), 0).0;
        }
        for awaiter in ["o:a", "o:b"] {
            let t = doc.tasks.get_mut(awaiter).unwrap();
            t.state = TaskState::Suspended;
            t.disarm();
            doc.promises
                .get_mut("o:awaited")
                .unwrap()
                .callbacks
                .push(awaiter.into());
        }
        doc.promises
            .get_mut("o:other")
            .unwrap()
            .callbacks
            .push("o:a".into());
        doc.promises
            .get_mut("o:awaited")
            .unwrap()
            .listeners
            .push("http://listener:1".into());
        doc
    }

    #[test]
    fn a_settlement_cut_after_the_promise_is_finished_as_the_kernel_would() {
        let before = fixture();
        let (after, fx) = step(&before, settle("o:awaited"), 700);

        // Only the promise landed, with what it owed still on it.
        let mut cut = before.clone();
        let mut p = after.promises["o:awaited"].clone();
        p.callbacks = before.promises["o:awaited"].callbacks.clone();
        p.listeners = before.promises["o:awaited"].listeners.clone();
        cut.promises.insert("o:awaited".into(), p);
        cut.tasks
            .insert("o:awaited".into(), after.tasks["o:awaited"].clone());
        assert!(needs_recovery(&cut));

        let rfx = recover(&cut, 700, &cfg());
        let mut recovered = cut.clone();
        apply_effects(&mut recovered, &rfx);
        assert_eq!(recovered, after);
        assert_eq!(sends(&rfx), sends(&fx));
        assert!(!needs_recovery(&recovered));
        assert!(recover(&recovered, 700, &cfg()).is_empty());
    }

    #[test]
    fn an_awaiter_already_resumed_is_not_resumed_twice() {
        let before = fixture();
        let (after, _) = step(&before, settle("o:awaited"), 700);
        // Everything but the promise's final value landed.
        let mut cut = after.clone();
        let p = cut.promises.get_mut("o:awaited").unwrap();
        p.callbacks = before.promises["o:awaited"].callbacks.clone();
        p.listeners = before.promises["o:awaited"].listeners.clone();

        let rfx = recover(&cut, 700, &cfg());
        let mut recovered = cut.clone();
        apply_effects(&mut recovered, &rfx);
        assert_eq!(recovered, after);
        // Resumed already: no dispatch again. The unblock goes out again.
        assert_eq!(
            sends(&rfx),
            vec![("http://listener:1".to_string(), "unblock")]
        );
    }

    #[test]
    fn a_finished_tasks_registrations_are_dropped() {
        let before = fixture();
        // o:a's own promise settles: its task is done, and it no longer
        // waits on o:awaited or o:other.
        let (after, _) = step(&before, settle("o:a"), 700);
        assert!(!after.promises["o:other"]
            .callbacks
            .contains(&"o:a".to_string()));

        // Only o:a's record landed.
        let mut cut = before.clone();
        cut.promises
            .insert("o:a".into(), after.promises["o:a"].clone());
        cut.tasks.insert("o:a".into(), after.tasks["o:a"].clone());
        let mut recovered = cut.clone();
        apply_effects(&mut recovered, &recover(&cut, 700, &cfg()));
        assert_eq!(recovered, after);
    }

    #[test]
    fn a_kernel_state_needs_nothing() {
        let mut doc = fixture();
        assert!(!needs_recovery(&doc));
        let fx = drain(&doc, 200_000, &cfg());
        apply_effects(&mut doc, &fx);
        assert!(!needs_recovery(&doc));
    }
}
