// Shared machinery for the three generative tests that drive the engines.
//
//   differential.rs — one weighted random walk, run to a coverage plateau
//   fuzz.rs         — a coverage-guided loop over mutated byte tapes
//   prop.rs         — proptest state machine, with shrinking
//
// All three ask the same question: do the backends agree? They differ only in
// where the next operation comes from. So the backend set, the step, and every
// comparison live here, and each target supplies its own source of choices.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use resonate_core::types::TaskState;

// Serializes tests that share the same Postgres/MySQL database so that
// concurrent debug.reset calls from one test cannot truncate another's data.
static DB_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
pub fn db_lock() -> &'static Mutex<()> {
    DB_LOCK.get_or_init(|| Mutex::new(()))
}

use resonate_core::types::{RequestEnvelope, RequestHead, ResponseEnvelope, SUPPORTED_VERSIONS};

use resonate_server_dbms::{
    engine_mysql::MysqlEngine,
    engine_port::{Input, Outgoing, ResonateEngine, Scheduled, Timeout},
    engine_postgres::PostgresEngine,
    engine_sqlite::SqliteEngine,
    oracle::{Oracle, SharedOracle},
};
use serde_json::{json, Value};

pub const TASK_RETRY_TIMEOUT_MS: i64 = 30_000;
/// Every backend gets the same limit, or `preload` would differ by
/// construction rather than by behaviour. The storage configs' default.
pub const PRELOAD_LIMIT: u32 = 10;
// Fixed epoch anchor; all test times are offsets from here (ms).
pub const T0: i64 = 1_000_000_000;
// Fake worker URL — passes is_valid_address but no actual delivery attempted.
pub const WORKER_URL: &str = "http://diff-test-worker:9999";
pub const PID: &str = "diff-test-pid";
pub const TTL: i64 = 60_000;

// All operation kinds that must produce at least one 2xx before the test ends.
pub const ALL_OPS: &[&str] = &[
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

// ---------------------------------------------------------------------------
// Entropy
// ---------------------------------------------------------------------------

/// Where a generator's choices come from.
///
/// The generators below are the only description of a well-formed request that
/// exists, so all three targets have to use them. They differ in what answers
/// the questions: a seeded PRNG, a mutable byte tape, or a recorded script.
pub trait Entropy {
    fn u32(&mut self, r: Range<u32>) -> u32;
    fn i64(&mut self, r: Range<i64>) -> i64;
    fn usize(&mut self, r: Range<usize>) -> usize;
    fn bool(&mut self) -> bool;
}

impl Entropy for fastrand::Rng {
    fn u32(&mut self, r: Range<u32>) -> u32 {
        fastrand::Rng::u32(self, r)
    }
    fn i64(&mut self, r: Range<i64>) -> i64 {
        fastrand::Rng::i64(self, r)
    }
    fn usize(&mut self, r: Range<usize>) -> usize {
        fastrand::Rng::usize(self, r)
    }
    fn bool(&mut self) -> bool {
        fastrand::Rng::bool(self)
    }
}

/// A fuzzer input, read as a stream of decisions.
///
/// The bytes *are* the test case: the same tape always produces the same run,
/// so a crash is reproducible from the bytes alone, and shrinking the bytes
/// shrinks the run. Each decision consumes the fewest bytes its range needs,
/// which keeps a one-byte mutation local to one decision instead of shifting
/// every choice after it.
///
/// Running off the end is not an error, it is the end of the test: reads
/// return zero and `exhausted` goes true, so the driver stops.
pub struct Tape<'a> {
    bytes: &'a [u8],
    pos: usize,
    exhausted: bool,
}

impl<'a> Tape<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Tape {
            bytes,
            pos: 0,
            exhausted: false,
        }
    }

    pub fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn consumed(&self) -> usize {
        self.pos
    }

    /// Read `span` worth of bytes and fold them into `0..span`.
    fn take(&mut self, span: u128) -> u128 {
        let width = match span {
            0..=0x100 => 1,
            0x101..=0x1_0000 => 2,
            0x1_0001..=0x1_0000_0000 => 4,
            _ => 8,
        };
        let mut v: u128 = 0;
        for i in 0..width {
            let b = match self.bytes.get(self.pos) {
                Some(b) => *b,
                None => {
                    self.exhausted = true;
                    0
                }
            };
            self.pos += 1;
            v |= (b as u128) << (8 * i);
        }
        if span == 0 {
            0
        } else {
            v % span
        }
    }
}

impl Entropy for Tape<'_> {
    fn u32(&mut self, r: Range<u32>) -> u32 {
        let span = (r.end - r.start) as u128;
        r.start + self.take(span) as u32
    }
    fn i64(&mut self, r: Range<i64>) -> i64 {
        let span = (r.end as i128 - r.start as i128) as u128;
        r.start + self.take(span) as i64
    }
    fn usize(&mut self, r: Range<usize>) -> usize {
        let span = (r.end - r.start) as u128;
        r.start + self.take(span) as usize
    }
    fn bool(&mut self) -> bool {
        self.take(2) == 1
    }
}

/// A fixed list of answers, replayed in order.
///
/// proptest generates values, not entropy, so its transitions carry the
/// numbers the generators would have drawn. Replaying them through the same
/// generators means proptest shrinks the numbers and the request follows.
pub struct Script {
    values: Vec<u64>,
    pos: usize,
}

impl Script {
    pub fn new(values: Vec<u64>) -> Self {
        Script { values, pos: 0 }
    }

    fn next(&mut self, span: u128) -> u128 {
        let v = self.values.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        if span == 0 {
            0
        } else {
            v as u128 % span
        }
    }
}

impl Entropy for Script {
    fn u32(&mut self, r: Range<u32>) -> u32 {
        let span = (r.end - r.start) as u128;
        r.start + self.next(span) as u32
    }
    fn i64(&mut self, r: Range<i64>) -> i64 {
        let span = (r.end as i128 - r.start as i128) as u128;
        r.start + self.next(span) as i64
    }
    fn usize(&mut self, r: Range<usize>) -> usize {
        let span = (r.end - r.start) as u128;
        r.start + self.next(span) as usize
    }
    fn bool(&mut self) -> bool {
        self.next(2) == 1
    }
}

thread_local! {
    /// Correlation ids come from a counter, not the thread RNG, so the same
    /// tape produces the same requests in every process.
    static CORR: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub fn req(kind: &str, data: Value) -> RequestEnvelope {
    let corr = CORR.with(|c| {
        c.set(c.get() + 1);
        c.get()
    });
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: corr.to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

// ---------------------------------------------------------------------------
// Backend abstraction
// ---------------------------------------------------------------------------

// A backend is any implementation of durable state. Comparing engines rather
// than servers keeps the server layer — envelope validation, the clock gate,
// the HTTP edge — out of the comparison, so a divergence is a divergence in
// the thing being tested.
type Backend = Arc<dyn ResonateEngine>;

/// Send one request to a backend at time `now`.
///
/// `now` rides in the envelope rather than alongside it: `process` resolves the
/// effective time from `head.debug_time`, identically for every backend. The
/// server gates that on `config.debug`, which `debug_config()` enables.
pub async fn send(backend: &Backend, envelope: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    send_full(backend, envelope, now).await.0
}

/// The response *and* what the transition emitted.
///
/// An engine that still writes an outbox row returns no messages; its
/// emissions are compared through `debug.snap` instead. See
/// `assert_emissions_agree`.
pub async fn send_full(
    backend: &Backend,
    envelope: &RequestEnvelope,
    now: i64,
) -> (ResponseEnvelope, Vec<Outgoing>, Vec<Scheduled>) {
    let out = backend.process(Input::External(envelope), now).await;
    let resp = out.response.unwrap_or_else(|| {
        panic!(
            "engine returned no response for External input ({})",
            envelope.kind
        )
    });
    (resp, out.messages, out.timeouts)
}

/// A deadline as (kind, id, when) — the form both a returned `Scheduled` and a
/// snapshot row can be reduced to.
///
/// `pid` is dropped: `SnapshotTaskTimeout` does not carry it, and a lease is
/// identified by the task it is on.
pub fn armed_key(s: &Scheduled) -> (&'static str, String, i64) {
    match &s.timeout {
        Timeout::PromiseTimeout { promise_id } => ("promise", promise_id.clone(), s.at),
        Timeout::TaskRetryTimeout { task_id } => ("retry", task_id.clone(), s.at),
        Timeout::TaskLeaseTimeout { task_id, .. } => ("lease", task_id.clone(), s.at),
        Timeout::ScheduleDue { schedule_id } => ("schedule", schedule_id.clone(), s.at),
    }
}

/// Every deadline the snapshot says this step armed or moved.
///
/// A deadline that is in the after-snapshot and was not in the before-snapshot
/// at the same instant was either newly armed or pushed to a new time. Either
/// way the engine wrote it, so it owes an announcement. Disarming is not
/// included — the contract reports arming only.
pub fn snapshot_arms(before: &Value, after: &Value) -> HashSet<(&'static str, String, i64)> {
    fn collect(snap: &Value) -> HashSet<(&'static str, String, i64)> {
        let mut out = HashSet::new();
        if let Some(rows) = snap.get("promiseTimeouts").and_then(|v| v.as_array()) {
            for r in rows {
                if let (Some(id), Some(t)) = (
                    r.get("id").and_then(|v| v.as_str()),
                    r.get("timeout").and_then(|v| v.as_i64()),
                ) {
                    out.insert(("promise", id.to_string(), t));
                }
            }
        }
        if let Some(rows) = snap.get("taskTimeouts").and_then(|v| v.as_array()) {
            for r in rows {
                if let (Some(id), Some(ty), Some(t)) = (
                    r.get("id").and_then(|v| v.as_str()),
                    r.get("type").and_then(|v| v.as_i64()),
                    r.get("timeout").and_then(|v| v.as_i64()),
                ) {
                    out.insert((if ty == 1 { "lease" } else { "retry" }, id.to_string(), t));
                }
            }
        }
        out
    }
    let before = collect(before);
    collect(after).difference(&before).cloned().collect()
}

/// Every deadline the durable state gained must have been announced.
///
/// This is the check the messages could never have: the deadline stays in the
/// table, so the table itself is the oracle. Cross-engine agreement alone would
/// not catch it — an engine that forgets to announce a deadline behaves
/// identically to one that does, because the sweep covers it, so the omission
/// is invisible except against the row it wrote.
///
/// A superset is allowed. Announcing a deadline that did not move costs one
/// wheel entry that fires into a no-op; failing to announce one costs the
/// latency the wheel exists to remove.
pub fn assert_arms_announced(
    name: &str,
    before: &Value,
    after: &Value,
    armed: &[Scheduled],
    ctx: &str,
) {
    let expected = snapshot_arms(before, after);
    if expected.is_empty() {
        return;
    }
    let announced: HashSet<_> = armed.iter().map(armed_key).collect();
    let missing: Vec<_> = expected.difference(&announced).collect();
    if !missing.is_empty() {
        panic!(
            "{ctx}: {name} armed a deadline it did not announce\n  \
             missing: {missing:?}\n  announced: {:?}",
            announced.iter().collect::<Vec<_>>()
        );
    }
}

// Pick a random element from a slice.
pub fn pick<T: Clone>(rng: &mut dyn Entropy, v: &[T]) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v[rng.usize(0..v.len())].clone())
    }
}

pub fn print_timing_summary(
    timings: &mut HashMap<(String, String), Vec<u64>>,
    backends: &[(String, Backend)],
) {
    let backend_names: Vec<&str> = backends.iter().map(|(n, _)| n.as_str()).collect();
    let op_w = ALL_OPS.iter().map(|s| s.len()).max().unwrap_or(20);
    let cell_w = 16usize;

    eprintln!("\n[diff] timing summary (mean / p99 µs):");
    let header = format!(
        "  {:<op_w$}  {}",
        "operation",
        backend_names
            .iter()
            .map(|n| format!("{:>cell_w$}", n))
            .collect::<Vec<_>>()
            .join("  ")
    );
    eprintln!("[diff] {header}");
    eprintln!(
        "[diff]   {}",
        "─".repeat(op_w + 2 + backend_names.len() * (cell_w + 2))
    );

    for op in ALL_OPS {
        let cells: Vec<String> = backend_names
            .iter()
            .map(|name| {
                let key = (name.to_string(), op.to_string());
                if let Some(samples) = timings.get_mut(&key) {
                    samples.sort_unstable();
                    let mean_us = samples.iter().sum::<u64>() / samples.len() as u64 / 1000;
                    let p99_us = percentile(samples, 99.0) / 1000;
                    format!("{:>cell_w$}", format!("{mean_us}/{p99_us}µs"))
                } else {
                    format!("{:>cell_w$}", "—")
                }
            })
            .collect();
        eprintln!("[diff]   {:<op_w$}  {}", op, cells.join("  "));
    }
    eprintln!();
}

pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

/// Bring every backend to an empty store.
///
/// There is nothing to set up beyond that any more. This used to send
/// `debug.start` first, to put each engine into the paused mode a request could
/// enter; engines are now built with the debug flag and are simply in it.
pub async fn reset_all(backends: &[(String, Backend)], now: i64) {
    let envelope = req("debug.reset", json!({}));
    for (name, b) in backends {
        let resp = send(b, &envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.reset failed on {name}");
    }
}

pub async fn send_all(
    backends: &[(String, Backend)],
    envelope: &RequestEnvelope,
    now: i64,
    timings: &mut HashMap<(String, String), Vec<u64>>,
    emissions: &mut Vec<(String, Value)>,
    armed: &mut Vec<(String, Vec<Scheduled>)>,
) -> Vec<(String, i32, Value)> {
    let mut out = Vec::new();
    let kind = envelope.kind.clone();
    emissions.clear();
    armed.clear();
    for (name, b) in backends {
        let t0 = Instant::now();
        let (resp, messages, timeouts) = send_full(b, envelope, now).await;
        let ns = t0.elapsed().as_nanos() as u64;
        timings
            .entry((name.clone(), kind.clone()))
            .or_default()
            .push(ns);
        if b.returns_messages() {
            let mut msgs: Vec<Value> = messages
                .iter()
                .map(|m| json!({ "address": m.address(), "message": m.to_json() }))
                .collect();
            sort_messages(&mut msgs);
            emissions.push((name.clone(), Value::Array(msgs)));
        }
        armed.push((name.clone(), timeouts));
        out.push((name.clone(), resp.head.status, resp.data));
    }
    out
}

/// Fire one timeout on every backend and compare what came back.
///
/// This is the check the narrow path needs. `Internal` is where an engine can
/// most easily diverge without anyone noticing: the sweep would have caught the
/// row anyway, so a per-timeout path that fires the wrong row, or nothing, or
/// everything, still converges to the same state a moment later. Comparing the
/// messages and deadlines of the firing itself is what distinguishes "fired
/// exactly this" from "fired the whole queue".
pub async fn fire_all(backends: &[(String, Backend)], timeout: &Timeout, now: i64, ctx: &str) {
    let mut emissions: Vec<(String, Value)> = Vec::new();
    let mut armed: Vec<(String, Value)> = Vec::new();

    for (name, b) in backends {
        let out = b.process(Input::Internal(timeout.clone()), now).await;
        assert!(
            out.response.is_none(),
            "{ctx}: {name} returned a response for Internal input"
        );
        if b.returns_messages() {
            let mut msgs: Vec<Value> = out
                .messages
                .iter()
                .map(|m| json!({ "address": m.address(), "message": m.to_json() }))
                .collect();
            sort_messages(&mut msgs);
            emissions.push((name.clone(), Value::Array(msgs)));
        }
        let mut keys: Vec<Value> = out
            .timeouts
            .iter()
            .map(|t| {
                let (kind, id, at) = armed_key(t);
                json!([kind, id, at])
            })
            .collect();
        sort_by_json(&mut keys);
        armed.push((name.clone(), Value::Array(keys)));
    }

    assert_agree(&emissions, "emitted messages", ctx);
    assert_agree(&armed, "armed timeouts", ctx);
}

/// Every engine must hold the same near future.
///
/// `upcoming` is a hint, so the contract permits returning fewer than asked
/// for — but not a *different* set. Comparing it is what catches a backfill
/// query whose predicates have drifted from the sweep's, which would otherwise
/// show up only as a timer that sleeps through a deadline.
pub async fn assert_upcoming_agrees(backends: &[(String, Backend)], limit: usize, ctx: &str) {
    let mut out: Vec<(String, Value)> = Vec::new();
    for (name, b) in backends {
        let ups = b.upcoming(limit).await.expect("upcoming");
        let keys: Vec<Value> = ups
            .iter()
            .map(|t| {
                let (kind, id, at) = armed_key(t);
                json!([kind, id, at])
            })
            .collect();
        out.push((name.clone(), Value::Array(keys)));
    }
    assert_agree(&out, "upcoming deadlines", ctx);
}

/// Engines that return their emissions must return the same ones.
///
/// This is the check that replaces comparing `snap().messages` once a backend
/// stops queueing, and it is strictly stronger: an accumulated queue only says
/// a message was emitted at some point, this says which operation emitted it.
pub fn assert_emissions_agree(emissions: &[(String, Value)], ctx: &str) {
    assert_agree(emissions, "emitted messages", ctx);
}

/// State, and separately the queue.
///
/// `messages` is split out because it is the one section that is *meant* to
/// differ: an engine that returns what it emitted has nothing queued, so its
/// `messages` is empty by construction. The rest of the snapshot must still
/// agree everywhere, and the queue must still agree among the backends that
/// have one. Emissions are compared where they are produced, in `send_all`.
pub async fn snap_all(
    backends: &[(String, Backend)],
    now: i64,
) -> (Vec<(String, Value)>, Vec<(String, Value)>) {
    let envelope = req("debug.snap", json!({}));
    let mut state = Vec::new();
    let mut queued = Vec::new();
    for (name, b) in backends {
        let resp = send(b, &envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.snap failed on {name}");
        let mut data = resp.data;
        normalize_snap(&mut data);
        let messages = data
            .as_object_mut()
            .and_then(|o| o.remove("messages"))
            .unwrap_or(Value::Null);
        if !b.returns_messages() {
            queued.push((name.clone(), messages));
        }
        state.push((name.clone(), data));
    }
    (state, queued)
}

/// Every entry must be the same value.
pub fn assert_agree(vals: &[(String, Value)], what: &str, ctx: &str) {
    let all: Vec<(&str, &Value)> = vals.iter().map(|(n, v)| (n.as_str(), v)).collect();
    if !all.windows(2).all(|w| w[0].1 == w[1].1) {
        let detail: String = all
            .iter()
            .map(|(n, v)| format!("  {n}:\n{v:#}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("{ctx}: {what} diverged\n{detail}");
    }
}

pub fn assert_resps_agree(results: &[(String, i32, Value)], ctx: &str) {
    let all_statuses: Vec<(&str, i32)> = results.iter().map(|(n, s, _)| (n.as_str(), *s)).collect();
    let statuses_agree = all_statuses.windows(2).all(|w| w[0].1 == w[1].1);
    if !statuses_agree {
        // The body too, not just the code: an error response carries the reason
        // in `data`, and on a 500 that reason is the only thing that says which
        // constraint or driver error produced it.
        let detail: String = results
            .iter()
            .map(|(n, s, d)| format!("  {n}={s} {d}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("{ctx}: status mismatch\n{detail}");
    }

    let all_data: Vec<(&str, &Value)> = results.iter().map(|(n, _, d)| (n.as_str(), d)).collect();
    let data_agree = all_data.windows(2).all(|w| w[0].1 == w[1].1);
    if !data_agree {
        let detail: String = all_data
            .iter()
            .map(|(n, d)| format!("  {n}:\n{d:#}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("{ctx}: data mismatch\n{detail}");
    }
}

pub fn assert_snaps_agree(snaps: &[(String, Value)], ctx: &str) {
    let all: Vec<(&str, &Value)> = snaps.iter().map(|(n, v)| (n.as_str(), v)).collect();
    let agree = all.windows(2).all(|w| w[0].1 == w[1].1);
    if !agree {
        let detail: String = all
            .iter()
            .map(|(n, v)| format!("  {n}:\n{v:#}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("{ctx}: snapshot mismatch\n{detail}");
    }
}

pub fn assert_no_divergence(snaps: &[(String, Value)], keys: &[&str], ctx: &str) {
    for &key in keys {
        let vals: Vec<(&str, Value)> = snaps
            .iter()
            .map(|(n, s)| (n.as_str(), s.get(key).cloned().unwrap_or(Value::Null)))
            .collect();
        let agree = vals.windows(2).all(|w| w[0].1 == w[1].1);
        if !agree {
            let detail: String = vals
                .iter()
                .map(|(n, v)| format!("  {n}:\n{v:#}"))
                .collect::<Vec<_>>()
                .join("\n");
            panic!("{ctx}: `{key}` diverged\n{detail}");
        }
    }
}

pub fn normalize_snap(snap: &mut Value) {
    if let Some(obj) = snap.as_object_mut() {
        for (key, v) in obj.iter_mut() {
            if let Some(arr) = v.as_array_mut() {
                if key == "messages" {
                    sort_messages(arr);
                } else {
                    sort_by_id(arr);
                }
            }
        }
    }
}

/// Order a list of small JSON arrays deterministically, so two engines that
/// announce the same deadlines in a different order still compare equal.
pub fn sort_by_json(arr: &mut [Value]) {
    arr.sort_by_key(|v| v.to_string());
}

pub fn sort_messages(arr: &mut [Value]) {
    arr.sort_by_key(msg_sort_key);
}

pub fn msg_sort_key(msg: &Value) -> String {
    let kind = msg
        .pointer("/message/kind")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    match kind {
        "execute" => {
            let id = msg
                .pointer("/message/data/task/id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            format!("0_execute_{id}")
        }
        "unblock" => {
            let id = msg
                .pointer("/message/data/promise/id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let addr = msg.get("address").and_then(|v| v.as_str()).unwrap_or("");
            format!("1_unblock_{id}_{addr}")
        }
        _ => format!("2_{kind}"),
    }
}

pub fn normalize_resp(data: &mut Value) {
    for key in &["promises", "tasks", "schedules"] {
        if let Some(arr) = data.get_mut(*key).and_then(|v| v.as_array_mut()) {
            sort_by_id(arr);
        }
    }
}

pub fn sort_by_id(arr: &mut [Value]) {
    arr.sort_by(|a, b| {
        let key = |v: &Value| {
            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                id.to_string()
            } else {
                // No "id" field (e.g. callbacks have awaited/awaiter, listeners have
                // awaited/address) — fall back to full serialization for a stable sort.
                serde_json::to_string(v).unwrap_or_default()
            }
        };
        key(a).cmp(&key(b))
    });
}

pub fn state_class(snap: &Value) -> u8 {
    let mut c = 0u8;
    let non_empty = |key: &str| {
        snap.get(key)
            .and_then(|v| v.as_array())
            .is_some_and(|a| !a.is_empty())
    };
    if non_empty("promises") {
        c |= 1 << 0;
    }
    if non_empty("tasks") {
        c |= 1 << 1;
    }
    if non_empty("callbacks") {
        c |= 1 << 2;
    }
    if non_empty("listeners") {
        c |= 1 << 3;
    }
    if non_empty("messages") {
        c |= 1 << 4;
    }
    if non_empty("promiseTimeouts") {
        c |= 1 << 5;
    }
    if non_empty("schedules") {
        c |= 1 << 6;
    }
    if non_empty("taskTimeouts") {
        c |= 1 << 7;
    }
    c
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    PromiseCreate,
    PromiseGet,
    PromiseSettle,
    PromiseRegisterCallback,
    PromiseRegisterListener,
    PromiseSearch,
    TaskCreate,
    TaskGet,
    TaskAcquire,
    TaskRelease,
    TaskFulfill,
    TaskSuspend,
    TaskFence,
    TaskHeartbeat,
    TaskHalt,
    TaskContinue,
    TaskSearch,
    ScheduleCreate,
    ScheduleGet,
    ScheduleDelete,
    ScheduleSearch,
    DebugTick,
}

impl Op {
    /// Same order as `ALL_OPS`, so an index means the same thing to every
    /// target and to the coverage tables they keep.
    pub const ALL: [Op; 22] = [
        Op::PromiseCreate,
        Op::PromiseGet,
        Op::PromiseSettle,
        Op::PromiseRegisterCallback,
        Op::PromiseRegisterListener,
        Op::PromiseSearch,
        Op::TaskCreate,
        Op::TaskGet,
        Op::TaskAcquire,
        Op::TaskRelease,
        Op::TaskFulfill,
        Op::TaskSuspend,
        Op::TaskFence,
        Op::TaskHeartbeat,
        Op::TaskHalt,
        Op::TaskContinue,
        Op::TaskSearch,
        Op::ScheduleCreate,
        Op::ScheduleGet,
        Op::ScheduleDelete,
        Op::ScheduleSearch,
        Op::DebugTick,
    ];

    pub fn index(self) -> usize {
        Op::ALL.iter().position(|o| *o == self).expect("Op in ALL")
    }

    pub fn kind(self) -> &'static str {
        ALL_OPS[self.index()]
    }
}
pub fn build_envelope(
    op: Op,
    rng: &mut dyn Entropy,
    oracle: &Oracle,
    now: i64,
) -> (RequestEnvelope, i64) {
    match op {
        Op::PromiseCreate => (gen_promise_create(rng, oracle, now), now),
        Op::PromiseGet => (gen_promise_get(rng, oracle), now),
        Op::PromiseSettle => (gen_promise_settle(rng, oracle), now),
        Op::PromiseRegisterCallback => (gen_promise_register_callback(rng, oracle), now),
        Op::PromiseRegisterListener => (gen_promise_register_listener(rng, oracle), now),
        Op::PromiseSearch => (gen_promise_search(rng), now),
        Op::TaskCreate => (gen_task_create(rng, now), now),
        Op::TaskGet => (gen_task_get(rng, oracle), now),
        Op::TaskAcquire => (gen_task_acquire(rng, oracle), now),
        Op::TaskRelease => (gen_task_release(rng, oracle), now),
        Op::TaskFulfill => (gen_task_fulfill(rng, oracle), now),
        Op::TaskSuspend => (gen_task_suspend(rng, oracle), now),
        Op::TaskFence => (gen_task_fence(rng, oracle, now), now),
        Op::TaskHeartbeat => (gen_task_heartbeat(rng, oracle), now),
        Op::TaskHalt => (gen_task_halt(rng, oracle), now),
        Op::TaskContinue => (gen_task_continue(rng, oracle), now),
        Op::TaskSearch => (gen_task_search(rng), now),
        Op::ScheduleCreate => (gen_schedule_create(rng, now), now),
        Op::ScheduleGet => (gen_schedule_get(rng, oracle), now),
        Op::ScheduleDelete => (gen_schedule_delete(rng, oracle), now),
        Op::ScheduleSearch => (gen_schedule_search(rng), now),
        Op::DebugTick => gen_debug_tick(rng, now),
    }
}

pub fn gen_promise_create(rng: &mut dyn Entropy, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let all = oracle.all_promise_ids();
    let id = pick(rng, &all).unwrap_or_else(|| random_promise_id(rng));
    let timeout_at = now + rng.i64(30_000..300_000);
    // Roughly half the pool is awaitable. Create is idempotent by id, so an
    // id's tags are fixed by its first success and the pool stays split —
    // which is what keeps both sides of the awaitability rule reachable: a
    // callback on an external promise is a 200, on a plain one a 422.
    let tags = if rng.bool() {
        json!({ "resonate:external": "true" })
    } else {
        json!({})
    };
    req(
        "promise.create",
        json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags }),
    )
}

/// An awaited promise for a callback or a suspend.
///
/// Mostly awaitable, so the registration paths are actually walked; one time
/// in four whatever is pending, so the 422 stays covered too.
pub fn pick_awaited(rng: &mut dyn Entropy, oracle: &Oracle, awaiter: &str) -> String {
    let prefer_external = rng.u32(0..4) != 0;
    if prefer_external {
        let external = oracle.external_pending_promise_ids();
        if let Some(id) = external.iter().find(|p| p.as_str() != awaiter) {
            return id.clone();
        }
    }
    oracle
        .pending_promise_ids()
        .iter()
        .find(|p| p.as_str() != awaiter)
        .cloned()
        .unwrap_or_else(|| promise_id_different_from(rng, awaiter))
}

pub fn gen_promise_get(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let all = oracle.all_promise_ids();
    let id = pick(rng, &all).unwrap_or_else(|| random_promise_id(rng));
    req("promise.get", json!({ "id": id }))
}

pub fn gen_promise_settle(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let pending = oracle.pending_promise_ids();
    let id = pick(rng, &pending).unwrap_or_else(|| random_promise_id(rng));
    let state = if rng.bool() { "resolved" } else { "rejected" };
    req(
        "promise.settle",
        json!({ "id": id, "state": state, "value": {} }),
    )
}

pub fn gen_promise_register_callback(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let pending_t = oracle.tasks_by_state(TaskState::Pending);
    let awaiter = pick(rng, &acquired)
        .or_else(|| pick(rng, &pending_t))
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    let awaited = pick_awaited(rng, oracle, &awaiter);
    req(
        "promise.register_callback",
        json!({ "awaited": awaited, "awaiter": awaiter }),
    )
}

pub fn gen_promise_register_listener(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let pending = oracle.pending_promise_ids();
    let id = pick(rng, &pending).unwrap_or_else(|| random_promise_id(rng));
    req(
        "promise.register_listener",
        json!({ "awaited": id, "address": WORKER_URL }),
    )
}

pub fn gen_promise_search(rng: &mut dyn Entropy) -> RequestEnvelope {
    let data = match rng.u32(0..4) {
        0 => json!({ "state": "pending",  "limit": 10 }),
        1 => json!({ "state": "resolved", "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("promise.search", data)
}

pub fn gen_task_create(rng: &mut dyn Entropy, now: i64) -> RequestEnvelope {
    let id = task_id(rng.u32(0..8));
    let timeout_at = now + rng.i64(60_000..600_000);
    req(
        "task.create",
        json!({
            "pid": PID,
            "ttl": TTL,
            "action": {
                "kind": "promise.create",
                "head": {},
                "data": {
                    "id": id,
                    "timeoutAt": timeout_at,
                    "param": {},
                    "tags": { "resonate:target": WORKER_URL }
                }
            }
        }),
    )
}

pub fn gen_task_get(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let mut all = oracle.tasks_by_state(TaskState::Acquired);
    all.extend(oracle.tasks_by_state(TaskState::Pending));
    all.extend(oracle.tasks_by_state(TaskState::Suspended));
    all.extend(oracle.tasks_by_state(TaskState::Halted));
    let id = pick(rng, &all)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.get", json!({ "id": id }))
}

pub fn gen_task_acquire(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let pending = oracle.tasks_by_state(TaskState::Pending);
    let (id, version) = pick(rng, &pending).unwrap_or_else(|| (random_task_id(rng), 0));
    req(
        "task.acquire",
        json!({ "id": id, "version": version, "pid": PID, "ttl": TTL }),
    )
}

pub fn gen_task_release(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    req("task.release", json!({ "id": id, "version": version }))
}

pub fn gen_task_fulfill(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    let state = if rng.bool() { "resolved" } else { "rejected" };
    req(
        "task.fulfill",
        json!({
            "id": id,
            "version": version,
            "action": {
                "kind": "promise.settle",
                "head": {},
                "data": { "id": id, "state": state, "value": {} }
            }
        }),
    )
}

pub fn gen_task_suspend(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (task_id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    let awaited = pick_awaited(rng, oracle, &task_id);
    req(
        "task.suspend",
        json!({
            "id": task_id,
            "version": version,
            "actions": [{
                "kind": "promise.register_callback",
                "head": {},
                "data": { "awaited": awaited, "awaiter": task_id }
            }]
        }),
    )
}

pub fn gen_task_fence(rng: &mut dyn Entropy, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (task_id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    let pending_p = oracle.pending_promise_ids();
    let do_settle = !pending_p.is_empty() && rng.u32(0..4) != 0;
    if !do_settle {
        let new_promise_id = promise_id(rng.u32(0..8));
        let timeout_at = now + rng.i64(30_000..300_000);
        req(
            "task.fence",
            json!({
                "id": task_id,
                "version": version,
                "action": {
                    "kind": "promise.create",
                    "head": {},
                    "data": { "id": new_promise_id, "timeoutAt": timeout_at, "param": {}, "tags": {} }
                }
            }),
        )
    } else {
        let promise_id = pick(rng, &pending_p).unwrap_or_else(|| random_promise_id(rng));
        let state = if rng.bool() { "resolved" } else { "rejected" };
        req(
            "task.fence",
            json!({
                "id": task_id,
                "version": version,
                "action": {
                    "kind": "promise.settle",
                    "head": {},
                    "data": { "id": promise_id, "state": state, "value": {} }
                }
            }),
        )
    }
}

pub fn gen_task_heartbeat(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let mut acquired = oracle.tasks_by_state(TaskState::Acquired);
    let take = acquired.len().min(3);
    for i in 0..take {
        let j = rng.usize(i..acquired.len());
        acquired.swap(i, j);
    }
    let tasks: Vec<Value> = acquired
        .into_iter()
        .take(3)
        .map(|(id, version)| {
            let v = if rng.u32(0..4) == 0 {
                version - 1
            } else {
                version
            };
            json!({ "id": id, "version": v })
        })
        .collect();
    let tasks = if tasks.is_empty() {
        vec![json!({ "id": random_task_id(rng), "version": rng.i64(0..3) })]
    } else {
        tasks
    };
    let pid = if rng.u32(0..7) == 0 { "wrong-pid" } else { PID };
    req("task.heartbeat", json!({ "pid": pid, "tasks": tasks }))
}

pub fn gen_task_halt(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let mut all = oracle.tasks_by_state(TaskState::Acquired);
    all.extend(oracle.tasks_by_state(TaskState::Suspended));
    all.extend(oracle.tasks_by_state(TaskState::Pending));
    let id = pick(rng, &all)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.halt", json!({ "id": id }))
}

pub fn gen_task_continue(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let halted = oracle.tasks_by_state(TaskState::Halted);
    let id = pick(rng, &halted)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.continue", json!({ "id": id }))
}

pub fn gen_task_search(rng: &mut dyn Entropy) -> RequestEnvelope {
    let data = match rng.u32(0..5) {
        0 => json!({ "state": "acquired",  "limit": 10 }),
        1 => json!({ "state": "pending",   "limit": 10 }),
        2 => json!({ "state": "suspended", "limit": 10 }),
        3 => json!({ "state": "halted",    "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("task.search", data)
}

pub fn gen_schedule_create(rng: &mut dyn Entropy, now: i64) -> RequestEnvelope {
    let id = schedule_id(rng.u32(0..4));
    let promise_timeout = now + rng.i64(60_000..600_000);
    req(
        "schedule.create",
        json!({
            "id": id,
            "cron": "* * * * *",
            "promiseId": format!("sched-promise-{{{{.id}}}}-{{{{.timestamp}}}}"),
            "promiseTimeout": promise_timeout,
            "promiseParam": {},
            "promiseTags": { "resonate:target": WORKER_URL }
        }),
    )
}

pub fn gen_schedule_get(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let schedules = oracle.schedule_ids();
    let id = pick(rng, &schedules).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.get", json!({ "id": id }))
}

pub fn gen_schedule_delete(rng: &mut dyn Entropy, oracle: &Oracle) -> RequestEnvelope {
    let schedules = oracle.schedule_ids();
    let id = pick(rng, &schedules).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.delete", json!({ "id": id }))
}

pub fn gen_schedule_search(rng: &mut dyn Entropy) -> RequestEnvelope {
    let limit = if rng.bool() { 10 } else { 5 };
    req("schedule.search", json!({ "limit": limit }))
}

pub fn gen_debug_tick(rng: &mut dyn Entropy, now: i64) -> (RequestEnvelope, i64) {
    let new_now = now + rng.i64(0..50_000);
    (req("debug.tick", json!({ "time": new_now })), new_now)
}

// IDs share the origin "diff" (text before the first ':'), so a task promise
// (`diff:tN`) and a plain promise (`diff:pM`) pass the origin-match validation
// used by promise.register_callback and task.suspend.
pub fn promise_id(n: u32) -> String {
    format!("diff:p{n}")
}
pub fn task_id(n: u32) -> String {
    format!("diff:t{n}")
}
pub fn schedule_id(n: u32) -> String {
    format!("s{n}")
}

pub fn random_promise_id(rng: &mut dyn Entropy) -> String {
    promise_id(rng.u32(0..8))
}
pub fn random_task_id(rng: &mut dyn Entropy) -> String {
    task_id(rng.u32(0..8))
}
pub fn random_schedule_id(rng: &mut dyn Entropy) -> String {
    schedule_id(rng.u32(0..4))
}

pub fn promise_id_different_from(rng: &mut dyn Entropy, other: &str) -> String {
    let n = rng.u32(0..8);
    let candidate = promise_id(n);
    if candidate == other {
        promise_id((n + 1) % 8)
    } else {
        candidate
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Everything one step needs: the backends, the model, and the clock.
pub struct Harness {
    pub backends: Vec<(String, Backend)>,
    pub oracle: Arc<SharedOracle>,
    /// The model, when it is not one of the compared backends.
    generator: Option<Backend>,
    pub now: i64,
    /// Steps taken. `reset` leaves it alone, so the differential keeps one
    /// count across its batches; a target that wants per-run numbering in the
    /// step labels sets it back to zero itself.
    pub steps: usize,
    pub fired: usize,
    /// A step per line is what you want when reading one run and the last
    /// thing you want when running a million.
    pub verbose: bool,
    pub timings: HashMap<(String, String), Vec<u64>>,
    emissions: Vec<(String, Value)>,
    armed: Vec<(String, Vec<Scheduled>)>,
}

/// What a step did, in the terms steering is expressed in.
pub struct StepOutcome {
    pub kind: String,
    pub status: u16,
    pub pre_class: u8,
    pub post_class: u8,
    pub pre_census: u32,
    pub post_census: u32,
    /// The `Timeout::rank` of the deadline this step fired, if it fired one.
    pub fired: Option<u8>,
}

/// The size of every collection, in log2 buckets, packed into one word.
///
/// `state_class` says only whether a table is empty. That saturates almost
/// immediately, so it cannot tell a run that reached three tasks from one that
/// reached thirty — and depth is where the interesting states are. Buckets
/// keep the signal finite while still rewarding going deeper.
pub fn census(snap: &Value) -> u32 {
    const KEYS: [&str; 8] = [
        "promises",
        "tasks",
        "callbacks",
        "listeners",
        "messages",
        "promiseTimeouts",
        "schedules",
        "taskTimeouts",
    ];
    let mut out = 0u32;
    for (i, key) in KEYS.iter().enumerate() {
        let n = snap
            .get(key)
            .and_then(|v| v.as_array())
            .map_or(0, |a| a.len());
        let bucket = (usize::BITS - n.leading_zeros()).min(15);
        out |= bucket << (i * 4);
    }
    out
}

impl Harness {
    /// Open every backend the environment offers.
    ///
    /// SQLite and the oracle are always in. Postgres and MySQL join when their
    /// URL is set, and `TEST_BACKENDS` narrows the comparison to a named
    /// subset — see the note on `generator`.
    pub async fn open() -> Harness {
        let sqlite = Arc::new(
            SqliteEngine::open(":memory:", TASK_RETRY_TIMEOUT_MS, PRELOAD_LIMIT, true, true)
                .expect("sqlite open"),
        ) as Backend;
        let oracle = Arc::new(SharedOracle::with_preload_limit(PRELOAD_LIMIT));

        let pg_url = std::env::var("TEST_POSTGRES_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let my_url = std::env::var("TEST_MYSQL_URL")
            .ok()
            .filter(|s| !s.is_empty());

        // Take the lock and never give it back: a harness owns its database
        // for as long as the process lives, and keeping a guard in the struct
        // would make the harness `!Send`, which proptest needs it not to be.
        if pg_url.is_some() || my_url.is_some() {
            std::mem::forget(db_lock().lock().unwrap_or_else(|e| e.into_inner()));
        }

        let pg_backend: Option<Backend> = match pg_url {
            Some(url) => {
                let pg =
                    PostgresEngine::connect(&url, 5, TASK_RETRY_TIMEOUT_MS, PRELOAD_LIMIT, true)
                        .await
                        .expect("postgres connect");
                pg.init(true).await.expect("postgres schema init");
                Some(Arc::new(pg) as Backend)
            }
            None => {
                eprintln!("[diff] TEST_POSTGRES_URL not set — PostgreSQL skipped");
                None
            }
        };

        let my_backend: Option<Backend> = match my_url {
            Some(url) => {
                let my = MysqlEngine::connect(&url, 5, TASK_RETRY_TIMEOUT_MS, PRELOAD_LIMIT, true)
                    .await
                    .expect("mysql connect");
                my.init(true).await.expect("mysql schema init");
                Some(Arc::new(my) as Backend)
            }
            None => {
                eprintln!("[diff] TEST_MYSQL_URL not set — MySQL skipped");
                None
            }
        };

        let mut backends: Vec<(String, Backend)> = vec![
            ("sqlite".into(), sqlite),
            ("oracle".into(), Arc::clone(&oracle) as Backend),
        ];
        if let Some(pg) = pg_backend {
            backends.push(("postgres".into(), pg));
        }
        if let Some(my) = my_backend {
            backends.push(("mysql".into(), my));
        }

        // Iterating on one backend does not need all of them, and a full run is
        // slow enough to discourage running it. TEST_BACKENDS=sqlite,oracle
        // narrows the comparison; unset runs everything available.
        if let Ok(want) = std::env::var("TEST_BACKENDS") {
            let want: Vec<&str> = want
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            for name in &want {
                assert!(
                    backends.iter().any(|(n, _)| n == name),
                    "TEST_BACKENDS names '{name}', which is not available; have: {}",
                    backends
                        .iter()
                        .map(|(n, _)| n.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            backends.retain(|(n, _)| want.contains(&n.as_str()));
            assert!(
                backends.len() >= 2,
                "a differential needs at least two backends, got {}",
                backends.len()
            );
        }

        let names: Vec<&str> = backends.iter().map(|(n, _)| n.as_str()).collect();
        eprintln!("[diff] backends: {}", names.join(", "));

        // The oracle is not only a backend to compare against, it is what
        // `build_envelope` plans from: a task.acquire needs a pending task's
        // current version, and only a model of the state knows it. So when
        // TEST_BACKENDS drops the oracle from the comparison, keep driving it
        // anyway — otherwise the generator plans against an empty world and
        // whole operations never reach a 2xx.
        let generator: Option<Backend> = if backends.iter().any(|(n, _)| n == "oracle") {
            None
        } else {
            Some(Arc::clone(&oracle) as Backend)
        };

        Harness {
            backends,
            oracle,
            generator,
            now: T0,
            steps: 0,
            fired: 0,
            verbose: false,
            timings: HashMap::new(),
            emissions: Vec::new(),
            armed: Vec::new(),
        }
    }

    /// Empty every store and put the clock back to the epoch.
    pub async fn reset(&mut self) {
        reset_all(&self.backends, self.now).await;
        if let Some(g) = &self.generator {
            reset_all(
                std::slice::from_ref(&("oracle".to_string(), Arc::clone(g))),
                self.now,
            )
            .await;
        }
        self.now = T0;
    }

    /// Turn a chosen operation into a request, using the model's current state.
    pub fn plan(&self, rng: &mut dyn Entropy, op: Op) -> (RequestEnvelope, i64) {
        let o = self.oracle.lock();
        build_envelope(op, rng, &o, self.now)
    }

    /// Send one request everywhere, compare everything, then fire one deadline.
    ///
    /// Every assertion in the file is reached from here, so a target that calls
    /// `step` is running the whole comparison whatever it chose to send.
    pub async fn step(&mut self, rng: &mut dyn Entropy, op: Op) -> StepOutcome {
        let (envelope, now_after) = self.plan(rng, op);
        self.now = now_after;
        self.steps += 1;
        let now = self.now;

        let kind = envelope.kind.clone();
        let ctx = format!("step={} op={kind}", self.steps);
        if self.verbose {
            eprintln!("[diff] {ctx} now={now}");
        }

        // Verify backends agree before this step.
        let (pre_snaps, pre_queued) = snap_all(&self.backends, now).await;
        assert_no_divergence(
            &pre_snaps,
            &[
                "promises",
                "tasks",
                "callbacks",
                "taskTimeouts",
                "promiseTimeouts",
            ],
            &format!("BEFORE {ctx}"),
        );
        assert_agree(&pre_queued, "queued messages", &format!("BEFORE {ctx}"));

        let mut results = send_all(
            &self.backends,
            &envelope,
            now,
            &mut self.timings,
            &mut self.emissions,
            &mut self.armed,
        )
        .await;
        assert_emissions_agree(&self.emissions, &format!("EMIT {ctx}"));
        if let Some(g) = &self.generator {
            // Keep the model in step with the backends; its response is
            // not compared, only its state is read by the generator.
            let _ = send(g, &envelope, now).await;
        }
        for (_, _, data) in &mut results {
            normalize_resp(data);
        }

        let status = results[0].1;
        assert_resps_agree(&results, &ctx);

        let (post_snaps, mid_queued) = snap_all(&self.backends, now).await;
        assert_agree(&mid_queued, "queued messages", &format!("AFTER {ctx}"));

        // Announced deadlines: compared across engines, and — for the two
        // kinds the snapshot carries — against what the row actually says.
        let armed_json: Vec<(String, Value)> = self
            .armed
            .iter()
            .map(|(n, ts)| {
                let mut keys: Vec<Value> = ts
                    .iter()
                    .map(|t| {
                        let (kind, id, at) = armed_key(t);
                        json!([kind, id, at])
                    })
                    .collect();
                sort_by_json(&mut keys);
                (n.clone(), Value::Array(keys))
            })
            .collect();
        assert_agree(&armed_json, "armed timeouts", &format!("ARM {ctx}"));
        for ((name, ts), (pre, post)) in self.armed.iter().zip(pre_snaps.iter().zip(&post_snaps)) {
            assert_arms_announced(name, &pre.1, &post.1, ts, &format!("ARM {ctx}"));
        }

        // The near future every engine holds must be the same near future.
        assert_upcoming_agrees(&self.backends, 16, &format!("UPCOMING {ctx}")).await;

        // Then fire one of those deadlines the narrow way, as a timer
        // would. Firing one that is already due exercises the path that
        // does work; firing one that is not exercises the no-op that
        // idempotency promises, which is just as easy to get wrong.
        let candidates = {
            let o = self.oracle.lock();
            o.upcoming(8)
        };
        let mut fired = None;
        if let Some(chosen) = pick(rng, &candidates) {
            let fire_ctx = format!("FIRE {ctx} timeout={:?}", chosen.timeout);
            if self.verbose {
                eprintln!("[diff] {fire_ctx}");
            }
            fire_all(&self.backends, &chosen.timeout, now, &fire_ctx).await;
            if let Some(g) = &self.generator {
                let _ = g
                    .process(Input::Internal(chosen.timeout.clone()), now)
                    .await;
            }
            self.fired += 1;
            fired = Some(chosen.timeout.rank() as u8);

            let (fire_snaps, fire_queued) = snap_all(&self.backends, now).await;
            assert_snaps_agree(&fire_snaps, &fire_ctx);
            assert_agree(&fire_queued, "queued messages", &fire_ctx);
        }

        StepOutcome {
            kind,
            status: status as u16,
            pre_class: state_class(&pre_snaps[0].1),
            post_class: state_class(&post_snaps[0].1),
            pre_census: census(&pre_snaps[0].1),
            post_census: census(&post_snaps[0].1),
            fired,
        }
    }

    /// Assert the backends still agree in full.
    pub async fn assert_settled(&self, ctx: &str) {
        let (snaps, queued) = snap_all(&self.backends, self.now).await;
        assert_snaps_agree(&snaps, ctx);
        assert_agree(&queued, "queued messages", ctx);
    }
}
