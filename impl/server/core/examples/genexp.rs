//! Does coverage guidance replace domain knowledge in the generator?
//!
//! Our differential's generator reads live model state to build a request that
//! can land: `task.acquire` names a task that is actually pending, at the
//! version the model actually holds. That is domain knowledge in the test, and
//! it has to be maintained alongside the protocol.
//!
//! The alternative a coverage-guided fuzzer offers is to know nothing and
//! search: name an id from a fixed pool, guess a version, and let feedback keep
//! whatever tape happened to reach somewhere new. This measures whether that
//! actually works, on the same model, at the same step budget.
//!
//! Three arms, one metric — how much distinct behaviour each reaches:
//!
//!   blind      a request built from tape bytes alone. Ids come from a fixed
//!              pool, versions are guessed; nothing is read about what exists.
//!   informed   what the differential does today — eligibility and operands
//!              read out of the model.
//!
//! Each is run twice: unguided, where every tape is fresh random bytes, and
//! guided, where a tape that reached a behavioural signature no tape has
//! reached before is kept in a corpus and mutated.
//!
//! Run: cargo run --release --example genexp

use std::collections::{HashMap, HashSet};

use resonate_core::types::{RequestEnvelope, RequestHead, TaskState, SUPPORTED_VERSIONS};
use resonate_oracle::Oracle;
use serde_json::{json, Value};

const T0: i64 = 1_000_000_000;
const WORKER_URL: &str = "http://diff-test-worker:9999";
const PID: &str = "diff-test-pid";
const TTL: i64 = 60_000;

/// Steps each arm is allowed. The same for all three, so the comparison is of
/// reach per unit of work rather than of patience.
const BUDGET: usize = 3_000_000;
/// Steps in one program, after which the model is reset. The corpus needs a
/// bounded, replayable unit; the other arms use the same so the reset rate is
/// not a confound.
const PROGRAM: usize = 128;

const OPS: &[&str] = &[
    "promise.create",
    "promise.get",
    "promise.settle",
    "promise.register_callback",
    "promise.register_listener",
    "promise.search",
    "task.create",
    "task.get",
    "task.acquire",
    "task.release",
    "task.fulfill",
    "task.suspend",
    "task.fence",
    "task.heartbeat",
    "task.halt",
    "task.continue",
    "task.search",
    "schedule.create",
    "schedule.get",
    "schedule.delete",
    "schedule.search",
    "debug.tick",
];

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: "x".to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

// ─── the tape ────────────────────────────────────────────────────────────────

struct Tape<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Tape<'a> {
    fn new(b: &'a [u8]) -> Self {
        Tape { b, i: 0 }
    }
    fn done(&self) -> bool {
        self.i >= self.b.len()
    }
    fn byte(&mut self) -> u8 {
        let v = self.b.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }
    fn upto(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            self.byte() as usize % n
        }
    }
}

// ─── the blind generator ─────────────────────────────────────────────────────
//
// Knows the request shapes and nothing else. Ids come from a fixed pool, so a
// request can name a row that exists — but which rows exist, what state they
// are in, and what version they carry are all guesses.

fn promise_id(n: usize) -> String {
    format!("diff:p{n}")
}
fn task_id(n: usize) -> String {
    format!("diff:t{n}")
}
fn schedule_id(n: usize) -> String {
    format!("s{n}")
}

/// One id from the shared pool — promise or task, since either may be named.
fn any_id(t: &mut Tape) -> String {
    let n = t.upto(16);
    if n < 8 {
        promise_id(n)
    } else {
        task_id(n - 8)
    }
}

fn blind(t: &mut Tape, now: i64) -> (RequestEnvelope, i64) {
    let op = OPS[t.upto(OPS.len())];
    let ver = t.upto(4) as i64;
    let settle = ["resolved", "rejected", "rejected_canceled"][t.upto(3)];
    let mut next_now = now;
    let data = match op {
        "promise.create" => json!({
            "id": any_id(t), "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
            "param": {}, "tags": {}
        }),
        "promise.get" => json!({ "id": any_id(t) }),
        "promise.settle" => {
            json!({ "id": any_id(t), "state": settle, "value": {} })
        }
        "promise.register_callback" => {
            json!({ "awaited": any_id(t), "awaiter": any_id(t) })
        }
        "promise.register_listener" => {
            json!({ "awaited": any_id(t), "address": WORKER_URL })
        }
        "promise.search" => match t.upto(3) {
            0 => json!({ "state": "pending", "limit": 10 }),
            1 => json!({ "state": "resolved", "limit": 10 }),
            _ => json!({ "limit": 10 }),
        },
        "task.create" => json!({
            "pid": PID, "ttl": TTL,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": task_id(t.upto(8)),
                "timeoutAt": now + (t.upto(60) as i64 + 1) * 10_000,
                "param": {}, "tags": { "resonate:target": WORKER_URL }
            }}
        }),
        "task.get" => json!({ "id": any_id(t) }),
        "task.acquire" => json!({
            "id": any_id(t), "version": ver, "pid": PID, "ttl": TTL
        }),
        "task.release" => json!({ "id": any_id(t), "version": ver }),
        "task.fulfill" => {
            let id = any_id(t);
            json!({ "id": id, "version": ver, "action": {
                "kind": "promise.settle", "head": {},
                "data": { "id": id, "state": settle, "value": {} } }})
        }
        "task.suspend" => {
            let id = any_id(t);
            json!({ "id": id, "version": ver, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": any_id(t), "awaiter": id } }]})
        }
        "task.fence" => {
            let id = any_id(t);
            if t.byte().is_multiple_of(2) {
                json!({ "id": id, "version": ver, "action": {
                    "kind": "promise.create", "head": {},
                    "data": { "id": any_id(t),
                              "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
                              "param": {}, "tags": {} } }})
            } else {
                json!({ "id": id, "version": ver, "action": {
                    "kind": "promise.settle", "head": {},
                    "data": { "id": any_id(t), "state": settle, "value": {} } }})
            }
        }
        "task.heartbeat" => {
            let n = 1 + t.upto(3);
            let tasks: Vec<Value> = (0..n)
                .map(|_| json!({ "id": any_id(t), "version": t.upto(4) as i64 }))
                .collect();
            let pid = if t.byte().is_multiple_of(7) {
                "wrong-pid"
            } else {
                PID
            };
            json!({ "pid": pid, "tasks": tasks })
        }
        "task.halt" => json!({ "id": any_id(t) }),
        "task.continue" => json!({ "id": any_id(t) }),
        "task.search" => match t.upto(5) {
            0 => json!({ "state": "acquired", "limit": 10 }),
            1 => json!({ "state": "pending", "limit": 10 }),
            2 => json!({ "state": "suspended", "limit": 10 }),
            3 => json!({ "state": "halted", "limit": 10 }),
            _ => json!({ "limit": 10 }),
        },
        "schedule.create" => json!({
            "id": schedule_id(t.upto(4)), "cron": "* * * * *",
            "promiseId": "sched-promise-{{.id}}-{{.timestamp}}",
            "promiseTimeout": now + (t.upto(60) as i64 + 1) * 10_000,
            "promiseParam": {},
            "promiseTags": { "resonate:target": WORKER_URL }
        }),
        "schedule.get" => json!({ "id": schedule_id(t.upto(4)) }),
        "schedule.delete" => json!({ "id": schedule_id(t.upto(4)) }),
        "schedule.search" => json!({ "limit": 10 }),
        _ => {
            next_now = now + (t.upto(10) as i64) * 5_000;
            json!({ "time": next_now })
        }
    };
    (req(op, data), next_now)
}

// ─── the informed generator ──────────────────────────────────────────────────
//
// What the differential does: read the model, pick an operation that can land,
// and fill its operands from rows that exist. Abbreviated — the shapes are the
// same, only the operand choice differs — but the domain knowledge is all here.

fn pick<T: Clone>(t: &mut Tape, v: &[T]) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v[t.upto(v.len())].clone())
    }
}

fn informed(t: &mut Tape, o: &Oracle, now: i64) -> (RequestEnvelope, i64) {
    let acquired = o.tasks_by_state(TaskState::Acquired);
    let pending_t = o.tasks_by_state(TaskState::Pending);
    let halted = o.tasks_by_state(TaskState::Halted);
    let suspended = o.tasks_by_state(TaskState::Suspended);
    let pending_p = o.pending_promise_ids();
    let all_p = o.all_promise_ids();
    let scheds = o.schedule_ids();

    // Eligibility: only operations whose operands exist.
    let mut elig: Vec<&str> = vec![
        "promise.create",
        "task.create",
        "schedule.create",
        "promise.search",
        "task.search",
        "schedule.search",
        "debug.tick",
    ];
    if !all_p.is_empty() {
        elig.push("promise.get");
        elig.push("task.get");
    }
    if !pending_p.is_empty() {
        elig.push("promise.settle");
        elig.push("promise.register_listener");
        elig.push("promise.register_callback");
    }
    if !pending_t.is_empty() {
        elig.push("task.acquire");
    }
    if !acquired.is_empty() {
        elig.push("task.release");
        elig.push("task.fulfill");
        elig.push("task.fence");
        elig.push("task.heartbeat");
        if !pending_p.is_empty() {
            elig.push("task.suspend");
        }
    }
    if !halted.is_empty() {
        elig.push("task.continue");
    }
    if !acquired.is_empty() || !pending_t.is_empty() || !suspended.is_empty() {
        elig.push("task.halt");
    }
    if !scheds.is_empty() {
        elig.push("schedule.get");
        elig.push("schedule.delete");
    }

    let op = elig[t.upto(elig.len())];
    let settle = ["resolved", "rejected", "rejected_canceled"][t.upto(3)];
    let mut next_now = now;

    let task_of = |t: &mut Tape, v: &[(String, i64)]| -> (String, i64) {
        pick(t, v).unwrap_or_else(|| (task_id(0), 1))
    };

    let data = match op {
        "promise.create" => {
            let id = pick(t, &all_p).unwrap_or_else(|| promise_id(t.upto(8)));
            json!({ "id": id, "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
                    "param": {}, "tags": {} })
        }
        "promise.get" => json!({ "id": pick(t, &all_p).unwrap_or_else(|| promise_id(0)) }),
        "promise.settle" => {
            let id = pick(t, &pending_p).unwrap_or_else(|| promise_id(0));
            json!({ "id": id, "state": settle, "value": {} })
        }
        "promise.register_callback" => {
            let awaiter = pick(t, &acquired)
                .or_else(|| pick(t, &pending_t))
                .map(|(id, _)| id)
                .unwrap_or_else(|| task_id(0));
            let awaited = pending_p
                .iter()
                .find(|p| **p != awaiter)
                .cloned()
                .unwrap_or_else(|| promise_id(0));
            json!({ "awaited": awaited, "awaiter": awaiter })
        }
        "promise.register_listener" => {
            let id = pick(t, &pending_p).unwrap_or_else(|| promise_id(0));
            json!({ "awaited": id, "address": WORKER_URL })
        }
        "promise.search" => match t.upto(3) {
            0 => json!({ "state": "pending", "limit": 10 }),
            1 => json!({ "state": "resolved", "limit": 10 }),
            _ => json!({ "limit": 10 }),
        },
        "task.create" => json!({
            "pid": PID, "ttl": TTL,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": task_id(t.upto(8)),
                "timeoutAt": now + (t.upto(60) as i64 + 1) * 10_000,
                "param": {}, "tags": { "resonate:target": WORKER_URL }
            }}
        }),
        "task.get" => {
            let mut all = acquired.clone();
            all.extend(pending_t.clone());
            all.extend(suspended.clone());
            all.extend(halted.clone());
            json!({ "id": task_of(t, &all).0 })
        }
        "task.acquire" => {
            let (id, v) = task_of(t, &pending_t);
            json!({ "id": id, "version": v, "pid": PID, "ttl": TTL })
        }
        "task.release" => {
            let (id, v) = task_of(t, &acquired);
            json!({ "id": id, "version": v })
        }
        "task.fulfill" => {
            let (id, v) = task_of(t, &acquired);
            json!({ "id": id, "version": v, "action": {
                "kind": "promise.settle", "head": {},
                "data": { "id": id, "state": settle, "value": {} } }})
        }
        "task.suspend" => {
            let (id, v) = task_of(t, &acquired);
            let awaited = pending_p
                .iter()
                .find(|p| **p != id)
                .cloned()
                .unwrap_or_else(|| promise_id(0));
            json!({ "id": id, "version": v, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": awaited, "awaiter": id } }]})
        }
        "task.fence" => {
            let (id, v) = task_of(t, &acquired);
            if pending_p.is_empty() || t.byte().is_multiple_of(4) {
                json!({ "id": id, "version": v, "action": {
                    "kind": "promise.create", "head": {},
                    "data": { "id": promise_id(t.upto(8)),
                              "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
                              "param": {}, "tags": {} } }})
            } else {
                let p = pick(t, &pending_p).unwrap_or_else(|| promise_id(0));
                json!({ "id": id, "version": v, "action": {
                    "kind": "promise.settle", "head": {},
                    "data": { "id": p, "state": settle, "value": {} } }})
            }
        }
        "task.heartbeat" => {
            let tasks: Vec<Value> = if acquired.is_empty() {
                vec![json!({ "id": task_id(0), "version": 1 })]
            } else {
                acquired
                    .iter()
                    .take(3)
                    .map(|(id, v)| json!({ "id": id, "version": v }))
                    .collect()
            };
            let pid = if t.byte().is_multiple_of(7) {
                "wrong-pid"
            } else {
                PID
            };
            json!({ "pid": pid, "tasks": tasks })
        }
        "task.halt" => {
            let mut all = acquired.clone();
            all.extend(suspended.clone());
            all.extend(pending_t.clone());
            json!({ "id": task_of(t, &all).0 })
        }
        "task.continue" => json!({ "id": task_of(t, &halted).0 }),
        "task.search" => match t.upto(5) {
            0 => json!({ "state": "acquired", "limit": 10 }),
            1 => json!({ "state": "pending", "limit": 10 }),
            2 => json!({ "state": "suspended", "limit": 10 }),
            3 => json!({ "state": "halted", "limit": 10 }),
            _ => json!({ "limit": 10 }),
        },
        "schedule.create" => json!({
            "id": schedule_id(t.upto(4)), "cron": "* * * * *",
            "promiseId": "sched-promise-{{.id}}-{{.timestamp}}",
            "promiseTimeout": now + (t.upto(60) as i64 + 1) * 10_000,
            "promiseParam": {},
            "promiseTags": { "resonate:target": WORKER_URL }
        }),
        "schedule.get" => {
            json!({ "id": pick(t, &scheds).unwrap_or_else(|| schedule_id(0)) })
        }
        "schedule.delete" => {
            json!({ "id": pick(t, &scheds).unwrap_or_else(|| schedule_id(0)) })
        }
        "schedule.search" => json!({ "limit": 10 }),
        _ => {
            next_now = now + (t.upto(10) as i64) * 5_000;
            json!({ "time": next_now })
        }
    };
    (req(op, data), next_now)
}

// ─── the feedback signal ─────────────────────────────────────────────────────
//
// A behavioural signature, in the shape resonate-pg's fuzzer settled on:
// the operation and its status, the shape of the store both in aggregate and
// per object, and the whole thing mixed with the signature of the previous
// step so a new ORDER of the same steps counts as new.
//
// Reading state here is not the thing under test. Feedback may look at the
// world; the question is whether the GENERATOR has to.

fn mix(mut h: u64, v: u64) -> u64 {
    h ^= v
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(h << 6)
        .wrapping_add(h >> 2);
    h
}

fn bucket(n: usize) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4..=7 => 4,
        8..=15 => 5,
        _ => 6,
    }
}

fn shape(o: &Oracle) -> u64 {
    let mut h = 0u64;
    h = mix(h, bucket(o.all_promise_ids().len()));
    h = mix(h, bucket(o.pending_promise_ids().len()));
    for st in [
        TaskState::Pending,
        TaskState::Acquired,
        TaskState::Suspended,
        TaskState::Halted,
        TaskState::Fulfilled,
    ] {
        h = mix(h, bucket(o.tasks_by_state(st).len()));
    }
    h = mix(h, bucket(o.schedule_ids().len()));
    h = mix(h, bucket(o.upcoming(64).len()));
    h
}

// ─── running one program ─────────────────────────────────────────────────────

#[derive(Default)]
struct Reach {
    sigs: HashSet<u64>,
    /// State configurations visited, ignoring which operation got there.
    shapes: HashSet<u64>,
    /// Steps that actually changed or read something — a 2xx. The rest is
    /// budget spent being refused.
    useful: usize,
    /// Operations that have reached a 2xx at least once, and how often.
    ok: HashMap<String, usize>,
    tried: HashMap<String, usize>,
    steps: usize,
}

impl Reach {
    fn report(&self, name: &str, elapsed: f64) {
        println!("\n=== {name} — {} steps in {elapsed:.1}s ===", self.steps);
        println!("  distinct behavioural signatures: {}", self.sigs.len());
        println!("  distinct state configurations:   {}", self.shapes.len());
        println!(
            "  steps reaching 2xx:              {} ({:.1}%)",
            self.useful,
            self.useful as f64 / self.steps as f64 * 100.0
        );
        let covered = OPS.iter().filter(|o| self.ok.contains_key(**o)).count();
        println!("  operations reaching 2xx: {covered}/{}", OPS.len());
        for op in OPS {
            let tried = self.tried.get(*op).copied().unwrap_or(0);
            let ok = self.ok.get(*op).copied().unwrap_or(0);
            let mark = if ok == 0 { "MISS" } else { "ok  " };
            println!(
                "    [{mark}] {op:<28} {:>6.2}% of {tried}",
                if tried == 0 {
                    0.0
                } else {
                    ok as f64 / tried as f64 * 100.0
                }
            );
        }
    }
}

enum Arm {
    Blind,
    Informed,
}

/// Run one tape against a fresh model. Returns the signatures it reached.
fn run_program(tape: &[u8], arm: &Arm, r: &mut Reach) -> HashSet<u64> {
    let mut o = Oracle::new();
    let mut t = Tape::new(tape);
    let mut now = T0;
    let prev = 0u64;
    let mut reached = HashSet::new();

    while !t.done() && reached.len() < PROGRAM {
        let (mut env, next_now) = match arm {
            Arm::Blind => blind(&mut t, now),
            Arm::Informed => informed(&mut t, &o, now),
        };
        env.head.debug_time = Some(now);
        let kind = env.kind.clone();
        let resp = o.apply(&env);
        now = next_now.max(now);

        let status = resp.head.status;
        *r.tried.entry(kind.clone()).or_insert(0) += 1;
        if (200..300).contains(&status) {
            *r.ok.entry(kind.clone()).or_insert(0) += 1;
        }
        r.steps += 1;

        let op_i = OPS.iter().position(|o| *o == kind).unwrap_or(0) as u64;
        let sh = shape(&o);
        // Status CLASS, not status: 404 and 409 on the same operation in the
        // same shape are the same kind of event for coverage purposes.
        let sig = mix(mix(mix(0, op_i), (status / 100) as u64), sh);
        let _ = prev;
        reached.insert(sig);
        r.sigs.insert(sig);
        r.shapes.insert(sh);
        if (200..300).contains(&status) {
            r.useful += 1;
        }
    }
    reached
}

// ─── the three arms ──────────────────────────────────────────────────────────

fn random_tape(rng: &mut fastrand::Rng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.u8(..)).collect()
}

/// Havoc-lite: the mutations that matter for a decision tape — change a
/// decision, move a decision, lengthen the program.
fn mutate(rng: &mut fastrand::Rng, base: &[u8]) -> Vec<u8> {
    let mut v = base.to_vec();
    if v.is_empty() {
        return random_tape(rng, 256);
    }
    for _ in 0..1 + rng.usize(0..4) {
        match rng.u32(0..5) {
            0 => {
                let i = rng.usize(0..v.len());
                v[i] = rng.u8(..);
            }
            1 => {
                let i = rng.usize(0..v.len());
                v[i] = v[i].wrapping_add(1);
            }
            2 => {
                let i = rng.usize(0..v.len());
                v.insert(i, rng.u8(..));
            }
            3 => {
                if v.len() > 8 {
                    let i = rng.usize(0..v.len());
                    v.remove(i);
                }
            }
            _ => {
                let tail: Vec<u8> = (0..rng.usize(1..64)).map(|_| rng.u8(..)).collect();
                v.extend(tail);
            }
        }
    }
    v.truncate(4096);
    v
}

fn arm_unguided(name: &str, arm: Arm, seed: u64, budget: usize) -> Reach {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut r = Reach::default();
    let t0 = std::time::Instant::now();
    while r.steps < budget {
        let tape = random_tape(&mut rng, 512);
        run_program(&tape, &arm, &mut r);
    }
    r.report(name, t0.elapsed().as_secs_f64());
    r
}

fn arm_guided(name: &str, arm: Arm, seed: u64, budget: usize) -> Reach {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut r = Reach::default();
    let mut corpus: Vec<Vec<u8>> = Vec::new();
    let mut global: HashSet<u64> = HashSet::new();
    let t0 = std::time::Instant::now();

    while r.steps < budget {
        let tape = if corpus.is_empty() || rng.u32(0..16) == 0 {
            random_tape(&mut rng, 512)
        } else {
            let base = &corpus[rng.usize(0..corpus.len())];
            mutate(&mut rng, base)
        };
        let reached = run_program(&tape, &arm, &mut r);
        // Keep the tape if it reached anywhere no tape has reached before.
        if reached.iter().any(|s| !global.contains(s)) {
            global.extend(reached);
            corpus.push(tape);
            // Bound the corpus so the walk keeps moving rather than re-running
            // an ever-growing set of near-duplicates.
            if corpus.len() > 512 {
                corpus.remove(rng.usize(0..corpus.len() / 2));
            }
        }
    }
    r.report(name, t0.elapsed().as_secs_f64());
    println!("  corpus: {} tapes", corpus.len());
    r
}

fn main() {
    let budget: usize = std::env::var("GENEXP_BUDGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(BUDGET);
    let only = std::env::var("GENEXP_ONLY").unwrap_or_default();
    let seed: u64 = std::env::var("GENEXP_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0xfeed_1234);
    println!("budget: {budget} steps per arm, {PROGRAM} steps per program\n");
    if !only.is_empty() {
        let r = match only.as_str() {
            "blind" => arm_guided("blind, GUIDED", Arm::Blind, seed, budget),
            _ => arm_guided("informed, GUIDED", Arm::Informed, seed, budget),
        };
        let covered = OPS.iter().filter(|o| r.ok.contains_key(**o)).count();
        println!(
            "\nSUMMARY {only} sigs={} shapes={} useful={} ops={covered}/{}",
            r.sigs.len(),
            r.shapes.len(),
            r.useful,
            OPS.len()
        );
        return;
    }
    let blind_random = arm_unguided("blind, unguided", Arm::Blind, 0xfeed_1234, budget);
    let blind_guided = arm_guided("blind, GUIDED", Arm::Blind, 0xfeed_1234, budget);
    let informed_random = arm_unguided("informed, unguided", Arm::Informed, 0xfeed_1234, budget);
    let informed_guided = arm_guided("informed, GUIDED", Arm::Informed, 0xfeed_1234, budget);

    println!("\n=== summary ===");
    println!(
        "{:<22} {:>11} {:>8} {:>9} {:>8}",
        "arm", "signatures", "shapes", "2xx steps", "ops 2xx"
    );
    for (name, r) in [
        ("blind, unguided", &blind_random),
        ("blind, guided", &blind_guided),
        ("informed, unguided", &informed_random),
        ("informed, guided", &informed_guided),
    ] {
        let covered = OPS.iter().filter(|o| r.ok.contains_key(**o)).count();
        println!(
            "{name:<22} {:>11} {:>8} {:>9} {:>5}/{}",
            r.sigs.len(),
            r.shapes.len(),
            r.useful,
            covered,
            OPS.len()
        );
    }
}
