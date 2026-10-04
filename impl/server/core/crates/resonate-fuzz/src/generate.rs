//! The two generators, ported from `examples/genexp.rs`.
//!
//! A program is a byte tape decoded one decision at a time, so the guided loop
//! can keep a tape that reached somewhere new and mutate it.
//!
//!   blind      a request built from tape bytes alone. Ids come from a fixed
//!              pool, versions are guessed; nothing is read about what exists.
//!   informed   eligibility and operands read out of the oracle: an operation
//!              is only chosen if its operands exist, and an acquire names a
//!              pending task at the version the oracle holds — or, when the
//!              server has pushed one, the task an `execute` message offered.
//!
//! Two things differ from genexp because a real server is on the other end:
//! every id lives in the program's own namespace (`<ns>:p3`), so a program
//! never meets rows another one left behind, and the worker address is the
//! fuzzer's own callback listener, so the server's messages come back here.

use resonate_core::types::TaskState;
use resonate_oracle::Oracle;
use serde_json::{json, Value};

pub const PID: &str = "fuzz-pid";
pub const TTL: i64 = 60_000;

pub const OPS: &[&str] = &[
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

/// The operations both spec checkers decode (spec/work/go/record.go
/// `recordable`), plus the clock. A trace drawn from these alone is one the
/// checkers can explain; anything else — schedules, halt, continue, searches —
/// changes state no checker can see, and a later read of it would be refuted.
pub fn in_spec_alphabet(kind: &str) -> bool {
    matches!(
        kind,
        "promise.create"
            | "promise.get"
            | "promise.settle"
            | "promise.register_callback"
            | "promise.register_listener"
            | "task.get"
            | "task.acquire"
            | "task.suspend"
            | "task.fulfill"
            | "task.create"
            | "task.fence"
            | "task.release"
            | "task.heartbeat"
            | "debug.tick"
    )
}

pub fn op_index(kind: &str) -> u64 {
    OPS.iter().position(|o| *o == kind).unwrap_or(OPS.len()) as u64
}

// ─── the tape ────────────────────────────────────────────────────────────────

pub struct Tape<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Tape<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Tape { b, i: 0 }
    }
    pub fn done(&self) -> bool {
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

// ─── what a request is built from ────────────────────────────────────────────

/// Where a program's ids live and where its workers listen.
pub struct Names {
    /// The program's namespace: the origin of every id it makes.
    pub ns: String,
    /// The fuzzer's callback listener, as the server must reach it.
    pub worker: String,
    /// Searches read across programs; off when a stale row could answer.
    pub searches: bool,
    /// Only operations the spec checkers decode.
    pub spec_only: bool,
}

impl Names {
    /// Whether an id belongs to this program: one it named, or one a schedule
    /// it created named (`sched-promise-<schedule id>-<time>`).
    pub fn owns(&self, id: &str) -> bool {
        id.strip_prefix(&self.ns)
            .is_some_and(|rest| rest.starts_with(':'))
            || id.contains(&format!("-{}-s", self.ns))
    }
    fn promise(&self, n: usize) -> String {
        format!("{}:p{n}", self.ns)
    }
    fn task(&self, n: usize) -> String {
        format!("{}:t{n}", self.ns)
    }
    fn schedule(&self, n: usize) -> String {
        format!("{}-s{n}", self.ns)
    }
    /// One id from the shared pool — promise or task, since either may be named.
    fn any(&self, t: &mut Tape) -> String {
        let n = t.upto(16);
        if n < 8 {
            self.promise(n)
        } else {
            self.task(n - 8)
        }
    }
}

/// One request, and the instant after it (a `debug.tick` moves the clock).
pub struct Request {
    pub kind: String,
    pub data: Value,
    pub next_now: i64,
    /// An acquire of an offer the server pushed, rather than of a task the
    /// oracle says is pending.
    pub from_inbox: bool,
}

fn search(kind: &str, t: &mut Tape) -> Value {
    match kind {
        "promise.search" => match t.upto(3) {
            0 => json!({ "state": "pending", "limit": 10 }),
            1 => json!({ "state": "resolved", "limit": 10 }),
            _ => json!({ "limit": 10 }),
        },
        "task.search" => match t.upto(5) {
            0 => json!({ "state": "acquired", "limit": 10 }),
            1 => json!({ "state": "pending", "limit": 10 }),
            2 => json!({ "state": "suspended", "limit": 10 }),
            3 => json!({ "state": "halted", "limit": 10 }),
            _ => json!({ "limit": 10 }),
        },
        _ => json!({ "limit": 10 }),
    }
}

fn schedule_create(names: &Names, t: &mut Tape, now: i64) -> Value {
    json!({
        "id": names.schedule(t.upto(4)), "cron": "* * * * *",
        "promiseId": "sched-promise-{{.id}}-{{.timestamp}}",
        "promiseTimeout": now + (t.upto(60) as i64 + 1) * 10_000,
        "promiseParam": {},
        "promiseTags": { "resonate:target": names.worker }
    })
}

fn tick(t: &mut Tape, now: i64) -> (Value, i64) {
    let next = now + (t.upto(10) as i64) * 5_000;
    (json!({ "time": next }), next)
}

// ─── the blind generator ─────────────────────────────────────────────────────

pub fn blind(t: &mut Tape, names: &Names, now: i64) -> Request {
    let mut op = OPS[t.upto(OPS.len())];
    if (!names.searches && op.ends_with(".search")) || (names.spec_only && !in_spec_alphabet(op)) {
        op = "promise.get";
    }
    let ver = t.upto(4) as i64;
    let settle = ["resolved", "rejected", "rejected_canceled"][t.upto(3)];
    let mut next_now = now;
    let data = match op {
        "promise.create" => json!({
            "id": names.any(t), "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
            "param": {}, "tags": {}
        }),
        "promise.get" => json!({ "id": names.any(t) }),
        "promise.settle" => json!({ "id": names.any(t), "state": settle, "value": {} }),
        "promise.register_callback" => {
            json!({ "awaited": names.any(t), "awaiter": names.any(t) })
        }
        "promise.register_listener" => {
            json!({ "awaited": names.any(t), "address": names.worker })
        }
        "task.create" => json!({
            "pid": PID, "ttl": TTL,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": names.task(t.upto(8)),
                "timeoutAt": now + (t.upto(60) as i64 + 1) * 10_000,
                "param": {}, "tags": { "resonate:target": names.worker }
            }}
        }),
        "task.get" => json!({ "id": names.any(t) }),
        "task.acquire" => json!({ "id": names.any(t), "version": ver, "pid": PID, "ttl": TTL }),
        "task.release" => json!({ "id": names.any(t), "version": ver }),
        "task.fulfill" => {
            let id = names.any(t);
            json!({ "id": id, "version": ver, "action": {
                "kind": "promise.settle", "head": {},
                "data": { "id": id, "state": settle, "value": {} } }})
        }
        "task.suspend" => {
            let id = names.any(t);
            json!({ "id": id, "version": ver, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": names.any(t), "awaiter": id } }]})
        }
        "task.fence" => {
            let id = names.any(t);
            if t.byte().is_multiple_of(2) {
                json!({ "id": id, "version": ver, "action": {
                    "kind": "promise.create", "head": {},
                    "data": { "id": names.any(t),
                              "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
                              "param": {}, "tags": {} } }})
            } else {
                json!({ "id": id, "version": ver, "action": {
                    "kind": "promise.settle", "head": {},
                    "data": { "id": names.any(t), "state": settle, "value": {} } }})
            }
        }
        "task.heartbeat" => {
            let n = 1 + t.upto(3);
            let tasks: Vec<Value> = (0..n)
                .map(|_| json!({ "id": names.any(t), "version": t.upto(4) as i64 }))
                .collect();
            let pid = if t.byte().is_multiple_of(7) {
                "wrong-pid"
            } else {
                PID
            };
            json!({ "pid": pid, "tasks": tasks })
        }
        "task.halt" => json!({ "id": names.any(t) }),
        "task.continue" => json!({ "id": names.any(t) }),
        "schedule.create" => schedule_create(names, t, now),
        "schedule.get" | "schedule.delete" => json!({ "id": names.schedule(t.upto(4)) }),
        "promise.search" | "task.search" | "schedule.search" => search(op, t),
        _ => {
            let (d, n) = tick(t, now);
            next_now = n;
            d
        }
    };
    Request {
        kind: op.to_string(),
        data,
        next_now,
        from_inbox: false,
    }
}

// ─── the informed generator ──────────────────────────────────────────────────

fn pick<T: Clone>(t: &mut Tape, v: &[T]) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v[t.upto(v.len())].clone())
    }
}

/// `inbox` holds the `execute` offers the server pushed for this program —
/// a task and the version it was offered at — oldest first.
pub fn informed(
    t: &mut Tape,
    o: &Oracle,
    names: &Names,
    inbox: &mut Vec<(String, i64)>,
    now: i64,
) -> Request {
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
        "debug.tick",
    ];
    if names.searches {
        elig.extend(["promise.search", "task.search", "schedule.search"]);
    }
    if !all_p.is_empty() {
        elig.push("promise.get");
        elig.push("task.get");
    }
    if !pending_p.is_empty() {
        elig.push("promise.settle");
        elig.push("promise.register_listener");
        elig.push("promise.register_callback");
    }
    if !pending_t.is_empty() || !inbox.is_empty() {
        elig.push("task.acquire");
    }
    if !acquired.is_empty() {
        elig.push("task.release");
        elig.push("task.fulfill");
        elig.push("task.fence");
        elig.push("task.heartbeat");
        // Something to await other than a task's own promise.
        if pending_p
            .iter()
            .any(|p| !acquired.iter().any(|(a, _)| a == p))
        {
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

    if names.spec_only {
        elig.retain(|op| in_spec_alphabet(op));
    }
    let op = elig[t.upto(elig.len())];
    let settle = ["resolved", "rejected", "rejected_canceled"][t.upto(3)];
    let mut next_now = now;
    let mut from_inbox = false;

    let task_of = |t: &mut Tape, v: &[(String, i64)]| -> (String, i64) {
        pick(t, v).unwrap_or_else(|| (names.task(0), 1))
    };

    let data = match op {
        "promise.create" => {
            let id = pick(t, &all_p).unwrap_or_else(|| names.promise(t.upto(8)));
            // External a quarter of the time, so deadlines are armed and fire.
            let tags = if t.upto(4) == 0 {
                json!({ "resonate:external": "true" })
            } else {
                json!({})
            };
            json!({ "id": id, "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
                    "param": {}, "tags": tags })
        }
        "promise.get" => json!({ "id": pick(t, &all_p).unwrap_or_else(|| names.promise(0)) }),
        "promise.settle" => {
            let id = pick(t, &pending_p).unwrap_or_else(|| names.promise(0));
            json!({ "id": id, "state": settle, "value": {} })
        }
        "promise.register_callback" => {
            let awaiter = pick(t, &acquired)
                .or_else(|| pick(t, &pending_t))
                .map(|(id, _)| id)
                .unwrap_or_else(|| names.task(0));
            let awaited = pending_p
                .iter()
                .find(|p| **p != awaiter)
                .cloned()
                .unwrap_or_else(|| names.promise(0));
            json!({ "awaited": awaited, "awaiter": awaiter })
        }
        "promise.register_listener" => {
            let id = pick(t, &pending_p).unwrap_or_else(|| names.promise(0));
            json!({ "awaited": id, "address": names.worker })
        }
        "task.create" => json!({
            "pid": PID, "ttl": TTL,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": names.task(t.upto(8)),
                "timeoutAt": now + (t.upto(60) as i64 + 1) * 10_000,
                "param": {}, "tags": { "resonate:target": names.worker }
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
            // What a worker does: take the offer the server pushed, at the
            // version it was offered — which may since have gone stale.
            // An offer that still names a pending task at its current version
            // is a worker doing its job; any other is a stale offer, which the
            // server must refuse.
            let fresh = inbox.iter().position(|offer| pending_t.contains(offer));
            let (id, v) = if let (Some(i), true) = (fresh, t.upto(3) != 0) {
                from_inbox = true;
                inbox.remove(i)
            } else if !inbox.is_empty() && (pending_t.is_empty() || t.upto(2) == 0) {
                from_inbox = true;
                inbox.remove(t.upto(inbox.len()))
            } else {
                task_of(t, &pending_t)
            };
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
            let others: Vec<String> = pending_p.iter().filter(|p| **p != id).cloned().collect();
            let awaited = pick(t, &others).unwrap_or_else(|| names.promise(0));
            json!({ "id": id, "version": v, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": awaited, "awaiter": id } }]})
        }
        "task.fence" => {
            let (id, v) = task_of(t, &acquired);
            if pending_p.is_empty() || t.byte().is_multiple_of(4) {
                json!({ "id": id, "version": v, "action": {
                    "kind": "promise.create", "head": {},
                    "data": { "id": names.promise(t.upto(8)),
                              "timeoutAt": now + (t.upto(30) as i64 + 1) * 10_000,
                              "param": {}, "tags": {} } }})
            } else {
                let p = pick(t, &pending_p).unwrap_or_else(|| names.promise(0));
                json!({ "id": id, "version": v, "action": {
                    "kind": "promise.settle", "head": {},
                    "data": { "id": p, "state": settle, "value": {} } }})
            }
        }
        "task.heartbeat" => {
            let tasks: Vec<Value> = if acquired.is_empty() {
                vec![json!({ "id": names.task(0), "version": 1 })]
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
        "schedule.create" => schedule_create(names, t, now),
        "schedule.get" | "schedule.delete" => {
            json!({ "id": pick(t, &scheds).unwrap_or_else(|| names.schedule(0)) })
        }
        "promise.search" | "task.search" | "schedule.search" => search(op, t),
        _ => {
            let (d, n) = tick(t, now);
            next_now = n;
            d
        }
    };
    Request {
        kind: op.to_string(),
        data,
        next_now,
        from_inbox,
    }
}
