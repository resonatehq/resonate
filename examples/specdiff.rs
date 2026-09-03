//! Probe the oracle at the places `resonate-specification` makes a decision.
//!
//! The Lean specification is the reference; the oracle is the reference model
//! the differential compares every engine against. Where they disagree, every
//! engine inherits the oracle's answer and the disagreement is invisible — the
//! differential compares implementations with each other, and the specification
//! is not one of them.
//!
//! So this runs one scenario per decision the specification takes, prints what
//! the oracle answers, and states what `spec/02-abstract/external.lean` says.
//! Reading is how the spec side is established; running is how the oracle side
//! is. Nothing here asserts — a difference may be deliberate.
//!
//! Run: cargo run --release --example specdiff

use resonate_core::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_oracle::Oracle;
use serde_json::{json, Value};

const T0: i64 = 1_000_000_000;
const TARGET: &str = "poll://any@g";

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: "p".to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: Some(T0),
        },
        data,
    }
}

/// Drive a fresh model through `setup`, then report the last step's status.
fn probe(o: &mut Oracle, kind: &str, data: Value) -> i32 {
    let r = o.apply(&req(kind, data));
    if std::env::var("SPECDIFF_VERBOSE").is_ok() {
        eprintln!("  [{kind}] {} {}", r.head.status, r.data);
    }
    r.head.status
}

struct Case {
    what: &'static str,
    spec: i32,
    got: i32,
}

fn main() {
    let mut cases: Vec<Case> = Vec::new();
    // Differences that are not a status code — an effect, or a message that
    // was or was not emitted. Printed after the table.
    let mut notes: Vec<String> = Vec::new();
    let mut case = |what: &'static str, spec: i32, got: i32| {
        cases.push(Case { what, spec, got });
    };

    // ── promise.create ──────────────────────────────────────────────────────
    {
        let mut o = Oracle::new();
        let got = probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {},
                    "tags": { "resonate:timer": "true", "resonate:target": TARGET } }),
        );
        case(
            "promise.create with BOTH resonate:timer and resonate:target",
            400,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        let got = probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {},
                    "tags": { "resonate:target": "not a url" } }),
        );
        case(
            "promise.create with an unparseable resonate:target",
            200,
            got,
        );
    }
    {
        // The specification returns the EXISTING promise and compares nothing.
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {}, "tags": {} }),
        );
        let got = probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 999_000, "param": {}, "tags": {} }),
        );
        case("promise.create again, with a DIFFERENT timeoutAt", 200, got);
    }

    // ── promise.register_listener ───────────────────────────────────────────
    for (addr, what) in [
        (
            "poll://nogroup",
            "register_listener on poll:// with no @group",
        ),
        ("gcps://topic", "register_listener on a gcps:// address"),
        ("mailto:x", "register_listener on a non-http, non-poll URI"),
    ] {
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o,
            "promise.register_listener",
            json!({ "awaited": "a:1", "address": addr }),
        );
        case(what, 400, got);
    }
    {
        // `external` = the external tag, OR a target, OR a timer.
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {}, "tags": {} }),
        );
        let got = probe(
            &mut o,
            "promise.register_listener",
            json!({ "awaited": "a:1", "address": "http://w" }),
        );
        case(
            "register_listener on an UNTARGETED promise (not external)",
            422,
            got,
        );
    }

    // ── promise.register_callback ───────────────────────────────────────────
    {
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o,
            "promise.register_callback",
            json!({ "awaited": "a:1", "awaiter": "a:1" }),
        );
        case("register_callback where awaited == awaiter", 400, got);
    }
    {
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o,
            "promise.register_callback",
            json!({ "awaited": "a:1", "awaiter": "a:2" }),
        );
        case(
            "register_callback where the AWAITER does not exist",
            422,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        for id in ["a:1", "a:2"] {
            probe(
                &mut o,
                "promise.create",
                json!({ "id": id, "timeoutAt": T0 + 60_000, "param": {},
                          "tags": { "resonate:target": TARGET } }),
            );
        }
        // a:2 exists and is targeted; make a:1 untargeted so it is not external.
        let mut o2 = Oracle::new();
        probe(
            &mut o2,
            "promise.create",
            json!({ "id": "a:1", "timeoutAt": T0 + 60_000, "param": {}, "tags": {} }),
        );
        probe(
            &mut o2,
            "promise.create",
            json!({ "id": "a:2", "timeoutAt": T0 + 60_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o2,
            "promise.register_callback",
            json!({ "awaited": "a:1", "awaiter": "a:2" }),
        );
        case(
            "register_callback where the AWAITED is not external",
            422,
            got,
        );

        let got = probe(
            &mut o,
            "promise.register_callback",
            json!({ "awaited": "a:1", "awaiter": "a:2" }),
        );
        case(
            "register_callback, both targeted and pending (the happy path)",
            200,
            got,
        );
    }

    // ── task.create ─────────────────────────────────────────────────────────
    {
        let mut o = Oracle::new();
        let got = probe(
            &mut o,
            "task.create",
            json!({ "pid": "w", "ttl": 60_000, "action": { "kind": "promise.create",
                      "head": {}, "data": { "id": "t:1", "timeoutAt": T0 + 60_000,
                      "param": {}, "tags": {} } } }),
        );
        case(
            "task.create whose action carries NO resonate:target",
            400,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        let got = probe(
            &mut o,
            "task.create",
            json!({ "pid": "w", "ttl": 60_000, "action": { "kind": "promise.create",
                      "head": {}, "data": { "id": "t:1", "timeoutAt": T0 + 60_000,
                      "param": {}, "tags": { "resonate:target": TARGET,
                                             "resonate:timer": "true" } } } }),
        );
        case("task.create with BOTH timer and target", 400, got);
    }
    {
        // An existing promise with no task at all.
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "t:1", "timeoutAt": T0 + 60_000, "param": {}, "tags": {} }),
        );
        let got = probe(
            &mut o,
            "task.create",
            json!({ "pid": "w", "ttl": 60_000, "action": { "kind": "promise.create",
                      "head": {}, "data": { "id": "t:1", "timeoutAt": T0 + 60_000,
                      "param": {}, "tags": { "resonate:target": TARGET } } } }),
        );
        case("task.create onto an existing UNTARGETED promise", 422, got);
    }

    // ── task lifecycle ──────────────────────────────────────────────────────
    let acquired = |o: &mut Oracle| {
        probe(
            o,
            "task.create",
            json!({ "pid": "w", "ttl": 60_000, "action": { "kind": "promise.create",
                      "head": {}, "data": { "id": "t:1", "timeoutAt": T0 + 600_000,
                      "param": {}, "tags": { "resonate:target": TARGET } } } }),
        );
    };
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        let got = probe(
            &mut o,
            "task.fence",
            json!({ "id": "t:1", "version": 1, "action": { "kind": "promise.create",
                      "head": {}, "data": { "id": "t:1", "timeoutAt": T0 + 60_000,
                      "param": {}, "tags": {} } } }),
        );
        case("task.fence whose action names the task's OWN id", 400, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(
            &mut o,
            "task.fulfill",
            json!({ "id": "t:1", "version": 1, "action": { "kind": "promise.settle",
                      "head": {}, "data": { "id": "t:1", "state": "resolved", "value": {} } } }),
        );
        let got = probe(&mut o, "task.halt", json!({ "id": "t:1" }));
        case("task.halt on a FULFILLED task", 409, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(&mut o, "task.halt", json!({ "id": "t:1" }));
        let got = probe(&mut o, "task.halt", json!({ "id": "t:1" }));
        case("task.halt on an already-HALTED task", 200, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(&mut o, "task.halt", json!({ "id": "t:1" }));
        probe(
            &mut o,
            "promise.settle",
            json!({ "id": "t:1", "state": "resolved", "value": {} }),
        );
        let got = probe(&mut o, "task.continue", json!({ "id": "t:1" }));
        case(
            "task.continue when the PROMISE is already settled",
            409,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(
            &mut o,
            "promise.settle",
            json!({ "id": "t:1", "state": "resolved", "value": {} }),
        );
        let got = probe(
            &mut o,
            "task.acquire",
            json!({ "id": "t:1", "version": 1, "pid": "w", "ttl": 60_000 }),
        );
        case("task.acquire when the promise is settled", 409, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "t:9", "timeoutAt": T0 + 600_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o,
            "task.suspend",
            json!({ "id": "t:1", "version": 1, "actions": [
                  { "kind": "promise.register_callback", "head": {},
                    "data": { "awaited": "t:9", "awaiter": "t:1" } },
                  { "kind": "promise.register_callback", "head": {},
                    "data": { "awaited": "t:9", "awaiter": "t:1" } } ] }),
        );
        case("task.suspend naming the SAME awaited twice", 400, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        let got = probe(
            &mut o,
            "task.suspend",
            json!({ "id": "t:1", "version": 1, "actions": [] }),
        );
        case("task.suspend with an EMPTY action list", 400, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "t:9", "timeoutAt": T0 + 600_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        probe(
            &mut o,
            "promise.settle",
            json!({ "id": "t:9", "state": "resolved", "value": {} }),
        );
        let got = probe(
            &mut o,
            "task.suspend",
            json!({ "id": "t:1", "version": 1, "actions": [
                  { "kind": "promise.register_callback", "head": {},
                    "data": { "awaited": "t:9", "awaiter": "t:1" } } ] }),
        );
        case(
            "task.suspend where the awaited is ALREADY SETTLED",
            300,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        let got = probe(
            &mut o,
            "task.heartbeat",
            json!({ "pid": "nobody", "tasks": [ { "id": "t:1", "version": 1 } ] }),
        );
        case("task.heartbeat from the WRONG pid", 200, got);
    }

    // ── Part C of the specification's own conformance table: does an
    // operation gate on the PROJECTION? A promise past its timeoutAt is
    // logically settled whether or not a sweep has run, so an operation on its
    // task is an operation on a dead object. C4, C5 and C7. ────────────────
    let dead = |o: &mut Oracle| {
        // A targeted task promise whose deadline has passed, with no sweep.
        probe(
            o,
            "task.create",
            json!({ "pid": "w", "ttl": 60_000, "action": { "kind": "promise.create",
                      "head": {}, "data": { "id": "t:1", "timeoutAt": T0 + 1_000,
                      "param": {}, "tags": { "resonate:target": TARGET } } } }),
        );
    };
    const LATER: i64 = T0 + 5_000;
    let at = |kind: &str, data: Value| RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: "p".to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: Some(LATER),
        },
        data,
    };
    {
        let mut o = Oracle::new();
        dead(&mut o);
        let got = o
            .apply(&at("task.halt", json!({ "id": "t:1" })))
            .head
            .status;
        case(
            "C4 — task.halt on a task whose promise has EXPIRED",
            409,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        dead(&mut o);
        probe(&mut o, "task.halt", json!({ "id": "t:1" }));
        let got = o
            .apply(&at("task.continue", json!({ "id": "t:1" })))
            .head
            .status;
        case(
            "C4 — task.continue on a task whose promise has EXPIRED",
            409,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        dead(&mut o);
        let got = o
            .apply(&at(
                "task.heartbeat",
                json!({ "pid": "w", "tasks": [ { "id": "t:1", "version": 1 } ] }),
            ))
            .head
            .status;
        // Always 200; what C7 asks is whether the LEASE was extended.
        let _ = got;
        let ext = o.apply(&at("task.get", json!({ "id": "t:1" })));
        let st = ext
            .data
            .pointer("/task/state")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        notes.push("\nC7 — heartbeat on a task whose promise has EXPIRED:".to_string());
        notes.push(
            "    specification: refused (the promise projects settled, task fulfilled)".to_string(),
        );
        notes.push(format!(
            "    oracle:        task.get afterwards reports state = {st}"
        ));
    }
    {
        let mut o = Oracle::new();
        dead(&mut o);
        probe(&mut o, "task.release", json!({ "id": "t:1", "version": 1 }));
        let got = o
            .apply(&at(
                "task.acquire",
                json!({ "id": "t:1", "version": 2, "pid": "w", "ttl": 60_000 }),
            ))
            .head
            .status;
        case(
            "C4 — task.acquire on a task whose promise has EXPIRED",
            409,
            got,
        );
    }

    // ── the origin rules, which live in `resonate-core::types` and which the
    // specification has no counterpart for ─────────────────────────────────
    {
        let mut o = Oracle::new();
        for id in ["a:1", "b:2"] {
            probe(
                &mut o,
                "promise.create",
                json!({ "id": id, "timeoutAt": T0 + 60_000, "param": {},
                          "tags": { "resonate:target": TARGET } }),
            );
        }
        let got = probe(
            &mut o,
            "promise.register_callback",
            json!({ "awaited": "a:1", "awaiter": "b:2" }),
        );
        case("register_callback across DIFFERENT origins", 200, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "z:9", "timeoutAt": T0 + 600_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o,
            "task.suspend",
            json!({ "id": "t:1", "version": 1, "actions": [
                  { "kind": "promise.register_callback", "head": {},
                    "data": { "awaited": "z:9", "awaiter": "t:1" } } ] }),
        );
        case("task.suspend awaiting a DIFFERENT origin", 200, got);
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "t:9", "timeoutAt": T0 + 600_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        let got = probe(
            &mut o,
            "task.suspend",
            json!({ "id": "t:1", "version": 1, "actions": [
                  { "kind": "promise.register_callback", "head": {},
                    "data": { "awaited": "t:9", "awaiter": "t:5" } } ] }),
        );
        case(
            "task.suspend whose action awaiter is not the task id",
            200,
            got,
        );
    }
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        let got = probe(
            &mut o,
            "task.heartbeat",
            json!({ "pid": "w", "tasks": [ { "id": "t:1", "version": 1 },
                                             { "id": "q:2", "version": 1 } ] }),
        );
        case("task.heartbeat naming tasks of DIFFERENT origins", 200, got);
    }

    // ── searches ────────────────────────────────────────────────────────────
    for (kind, what) in [
        ("promise.search", "promise.search"),
        ("task.search", "task.search"),
        ("schedule.search", "schedule.search"),
    ] {
        let mut o = Oracle::new();
        let got = probe(&mut o, kind, json!({ "limit": 10 }));
        case(what, 501, got);
    }

    // ── schedule.create ─────────────────────────────────────────────────────
    {
        let mut o = Oracle::new();
        let got = probe(
            &mut o,
            "schedule.create",
            json!({ "id": "s1", "cron": "* * * * *", "promiseId": "p-{{.id}}",
                      "promiseTimeout": 60_000, "promiseParam": {},
                      "promiseTags": { "resonate:timer": "true",
                                       "resonate:target": TARGET } }),
        );
        case(
            "schedule.create with BOTH timer and target in promiseTags",
            400,
            got,
        );
    }

    // ── an effect difference, not a status one ─────────────────────────────
    //
    // The specification registers a callback only while the awaited promise is
    // pending; on a settled one it answers 200 and writes nothing, leaving the
    // awaiter where it was. Waking it is the internal `processCallback` step's,
    // and it only runs for a callback that was registered.
    {
        let mut o = Oracle::new();
        acquired(&mut o);
        // t:1 is acquired. Suspend it on a pending awaited, so it is suspended.
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "t:7", "timeoutAt": T0 + 600_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        probe(
            &mut o,
            "task.suspend",
            json!({ "id": "t:1", "version": 1, "actions": [
                  { "kind": "promise.register_callback", "head": {},
                    "data": { "awaited": "t:7", "awaiter": "t:1" } } ] }),
        );
        // A second promise, already settled.
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "t:8", "timeoutAt": T0 + 600_000, "param": {},
                      "tags": { "resonate:target": TARGET } }),
        );
        probe(
            &mut o,
            "promise.settle",
            json!({ "id": "t:8", "state": "resolved", "value": {} }),
        );
        // Register a callback on the SETTLED promise for the suspended task.
        probe(
            &mut o,
            "promise.register_callback",
            json!({ "awaited": "t:8", "awaiter": "t:1" }),
        );
        let st = o.apply(&req("task.get", json!({ "id": "t:1" })));
        let state = st
            .data
            .pointer("/task/state")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        notes.push(
            "\nregister_callback on an ALREADY-SETTLED awaited, for a SUSPENDED awaiter:"
                .to_string(),
        );
        notes.push("    specification: the awaiter stays  suspended  (200, no writes)".to_string());
        notes.push(format!("    oracle:        the awaiter is now {state}"));
    }

    // ── C1: which promises carry a durable (armed) timeout? ───────────────
    //
    // The specification says EXTERNAL ones, and external is three things:
    // tagged `resonate:external`, targeted, or a timer. A promise that is
    // armed is swept at its deadline, and the sweep is what discharges the
    // obligations recorded against it — a listener waiting on it is told.
    //
    // A promise that is NOT armed still settles when read, because the read
    // projects. But nothing reads it on its own, so nothing tells the listener.
    for (tag, what) in [
        (
            "resonate:external",
            "C1 — a listener on an `external`-tagged, UNTARGETED promise",
        ),
        (
            "resonate:timer",
            "C1 — a listener on a `timer`-tagged, UNTARGETED promise",
        ),
    ] {
        let mut o = Oracle::new();
        probe(
            &mut o,
            "promise.create",
            json!({ "id": "e:1", "timeoutAt": T0 + 1_000, "param": {},
                      "tags": { tag: "true" } }),
        );
        probe(
            &mut o,
            "promise.register_listener",
            json!({ "awaited": "e:1", "address": "http://w" }),
        );
        let _ = o.take_emitted();
        // Move time past the deadline and sweep, as a background loop would.
        o.sweep(T0 + 5_000);
        let n = o.take_emitted().len();
        notes.push(format!("\n{what}:"));
        notes.push(
            "    specification: the sweep settles it and the listener is told (1 unblock)"
                .to_string(),
        );
        notes.push(format!(
            "    oracle:        the sweep emitted {n} message(s)"
        ));
    }

    // ── report ──────────────────────────────────────────────────────────────
    let (mut same, mut diff) = (0, 0);
    println!("{:<62} {:>6} {:>7}", "scenario", "spec", "oracle");
    println!("{}", "─".repeat(78));
    for c in &cases {
        let mark = if c.spec == c.got {
            same += 1;
            "  "
        } else {
            diff += 1;
            "≠ "
        };
        println!("{mark}{:<60} {:>6} {:>7}", c.what, c.spec, c.got);
    }
    println!("{}", "─".repeat(78));
    println!("{same} agree, {diff} differ, of {} probed\n", cases.len());
    for n in &notes {
        println!("{n}");
    }
}
