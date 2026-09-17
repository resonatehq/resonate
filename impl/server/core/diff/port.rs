// Differential random testing at the ports.
//
// The same seeded request sequence goes to every backend through
// `ResonateServer`, and whatever each backend emits comes back out through a
// `ResonateRouter` the harness owns. Responses, routed messages and
// `debug.snap` must agree at every step.
//
// This is `diff/differential.rs` moved from the SQL family's internal
// `Engine` contract to the two ports every server has, so a backend joins by
// being a server and nothing else: the in-memory oracle, the SQL shell over
// SQLite (Postgres and MySQL are the same shell), and the blob server over an
// in-process object store all sit in one comparison.
//
// What is *not* compared here, by design: announced deadlines, `upcoming`,
// and narrow `Internal` firing. Those are properties of the SQL engines'
// timer contract, not of the protocol, and belong in `resonate-sql` as
// per-engine checks. Time exists in one place — `head.debug_time` — and moves
// only through `debug.tick`.
//
// Run:
//   cargo test --release --test port -- --nocapture
//   TEST_BACKENDS=oracle,blob cargo test --release --test port -- --nocapture
//
// Knobs, all environment variables:
//   TEST_MAX_STEPS=N          stop after N steps (default 200000)
//   TEST_SOFT=1               log a response divergence and carry on while the
//                             state and routed messages still agree; report
//                             them all at the end
//   TEST_SAME_ORIGIN_FENCE=1  keep fence actions in the task's origin — the
//                             blob server refuses cross-origin fences, and this
//                             is what gets past that known deviation to the rest
//   TEST_TRACE_FROM=N         print tasks, callbacks and promises per backend
//                             after every step from step N on
//
// Findings so far:
//   - The shared fence validator (resonate-core) now states the origin rule:
//     a settle shares the task's origin; a create shares it or names a root.
//     Strict, oracle + sqlite: 34000 steps to the coverage plateau, 424
//     behavioural signatures, no divergence.
//   - Blob still refuses every cross-origin fence before reading state, so a
//     fenced root create diverges there (200 elsewhere). Tracked as the
//     check-then-create issue; TEST_SAME_ORIGIN_FENCE=1 is the way past it
//     until then: 48200 steps, 427 signatures, no divergence with blob in.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, RequestEnvelope, RequestHead,
    ResponseEnvelope, TaskState, UnblockMsg, UnblockMsgData, UnblockMsgHead, SUPPORTED_VERSIONS,
};
use resonate_core::{ResonateRouter, ResonateServer, Unavailable};
use resonate_oracle::{Oracle, SharedOracle};
use resonate_server_blob::server::{Server as BlobServer, ServerCfg as BlobCfg};
use resonate_server_blob::store::ObjectStoreAdapter;
use resonate_server_sqlite::SqliteEngine;
use resonate_sql::engine::{Engine, Outgoing};
use serde_json::{json, Value};

const TASK_RETRY_TIMEOUT_MS: i64 = 30_000;
/// Every backend gets the same limit, or `preload` would differ by
/// construction rather than by behaviour. The storage configs' default.
const PRELOAD_LIMIT: u32 = 10;
// Fixed epoch anchor; all test times are offsets from here (ms).
const T0: i64 = 1_000_000_000;
// Fake worker URL — passes is_valid_address but no actual delivery attempted.
const WORKER_URL: &str = "http://diff-test-worker:9999";
/// Stamped into every execute message's head by every backend, so the routed
/// messages compare byte for byte.
const SERVER_URL: &str = "http://diff-test-server:8001";
const PID: &str = "diff-test-pid";
const TTL: i64 = 60_000;

// All operation kinds that must produce at least one 2xx before the test ends.
const ALL_OPS: &[&str] = &[
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

/// One request at one instant. Time rides in the head and nowhere else.
fn req(kind: &str, data: Value, now: i64) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: fastrand::u64(..).to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: Some(now),
        },
        data,
    }
}

// ---------------------------------------------------------------------------
// The outbound port, recorded
// ---------------------------------------------------------------------------

/// A router that keeps what it was asked to deliver.
///
/// Every backend delivers inside `process`, after its commit and before its
/// response, so by the time the harness holds the response the recorder holds
/// everything that transition emitted. No quiescence to wait for.
#[derive(Default)]
struct Recorder {
    sent: Mutex<Vec<Value>>,
}

impl Recorder {
    /// Everything routed since the last take, in the snapshot's message shape
    /// and order, so a routed message and a queued one compare as the same
    /// thing.
    fn take(&self) -> Vec<Value> {
        let mut out: Vec<Value> = self
            .sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        sort_messages(&mut out);
        out
    }
}

#[async_trait]
impl ResonateRouter for Recorder {
    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let message = serde_json::to_value(msg).expect("a message serializes");
        self.sent
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(json!({ "address": address, "message": message }));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The oracle behind the ports
// ---------------------------------------------------------------------------

/// The reference model as a server that delivers through a router.
///
/// `SharedOracle` already answers `ResonateServer`, but it keeps what it
/// emitted in a queue for `debug.snap` rather than routing it. This adapter
/// routes, shaping each emission the way the SQL shell's `deliver` does, so
/// the model is observed through exactly the two ports every other backend is.
struct OracleServer {
    oracle: Arc<SharedOracle>,
    router: Arc<dyn ResonateRouter>,
}

#[async_trait]
impl ResonateServer for OracleServer {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        // No await while the guard is held.
        let (resp, emitted) = {
            let mut o = self.oracle.lock();
            let resp = o.apply(req);
            let emitted = o.take_emitted();
            let _ = o.take_armed();
            (resp, emitted)
        };
        for msg in emitted {
            let (address, payload) = match msg {
                Outgoing::Execute {
                    address,
                    task_id,
                    version,
                } => (
                    address,
                    Message::Execute(ExecuteMsg {
                        kind: "execute".to_string(),
                        head: MessageHead {
                            server_url: SERVER_URL.to_string(),
                        },
                        data: ExecuteMsgData {
                            task: ExecuteMsgTask {
                                id: task_id,
                                version,
                            },
                        },
                    }),
                ),
                Outgoing::Unblock { address, promise } => (
                    address,
                    Message::Unblock(UnblockMsg {
                        kind: "unblock".to_string(),
                        head: UnblockMsgHead {},
                        data: UnblockMsgData { promise },
                    }),
                ),
            };
            let _ = self.router.route(&address, &payload).await;
        }
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

/// A backend is a server and the router the harness gave it.
struct Backend {
    name: String,
    server: Arc<dyn ResonateServer>,
    router: Arc<Recorder>,
}

async fn oracle_backend(oracle: &Arc<SharedOracle>) -> Backend {
    let router = Arc::new(Recorder::default());
    let server = Arc::new(OracleServer {
        oracle: Arc::clone(oracle),
        router: router.clone(),
    });
    Backend {
        name: "oracle".into(),
        server,
        router,
    }
}

async fn sqlite_backend() -> Backend {
    let router = Arc::new(Recorder::default());
    let server = resonate_sql::server::Server::new(
        Box::new(|debug| {
            Box::pin(async move {
                let engine = SqliteEngine::open(
                    ":memory:",
                    TASK_RETRY_TIMEOUT_MS,
                    PRELOAD_LIMIT,
                    true,
                    debug,
                )
                .map_err(|e| Unavailable::new(format!("cannot open sqlite: {e}")))?;
                Ok(Arc::new(engine) as Arc<dyn Engine>)
            })
        }),
        router.clone(),
        resonate_sql::server::Options {
            server_url: SERVER_URL.to_string(),
            wheel_capacity: 8192,
            wheel_refresh: 30_000,
            sweep_interval: 60_000,
        },
    );
    server.init(true).await.expect("sqlite init");
    Backend {
        name: "sqlite".into(),
        server,
        router,
    }
}

async fn blob_backend() -> Backend {
    let router = Arc::new(Recorder::default());
    let server = BlobServer::build(
        Arc::new(ObjectStoreAdapter::in_memory()),
        router.clone(),
        BlobCfg {
            debug: true,
            search: true,
            server_url: SERVER_URL.to_string(),
            ..Default::default()
        },
    );
    server.init(true).await.expect("blob init");
    Backend {
        name: "blob".into(),
        server,
        router,
    }
}

async fn send(b: &Backend, envelope: &RequestEnvelope) -> ResponseEnvelope {
    b.server
        .process(envelope)
        .await
        .unwrap_or_else(|e| panic!("{}: no answer for {}: {e:?}", b.name, envelope.kind))
}

// Pick a random element from a slice.
fn pick<T: Clone>(rng: &mut fastrand::Rng, v: &[T]) -> Option<T> {
    if v.is_empty() {
        None
    } else {
        Some(v[rng.usize(0..v.len())].clone())
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn port_differential_random() {
    debug_assert_eq!(22, ALL_OPS.len(), "Op has 22 variants; ALL_OPS must match");

    // The oracle is also what the planner reads: a task.acquire needs a
    // pending task's current version, and only a model of the state knows it.
    let oracle = Arc::new(SharedOracle::with_preload_limit(PRELOAD_LIMIT));

    let mut backends: Vec<Backend> = vec![
        oracle_backend(&oracle).await,
        sqlite_backend().await,
        blob_backend().await,
    ];

    if let Ok(want) = std::env::var("TEST_BACKENDS") {
        let want: Vec<&str> = want
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        for name in &want {
            assert!(
                backends.iter().any(|b| b.name == *name),
                "TEST_BACKENDS names '{name}', which is not available; have: {}",
                backends
                    .iter()
                    .map(|b| b.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        backends.retain(|b| want.contains(&b.name.as_str()));
        assert!(
            backends.len() >= 2,
            "a differential needs at least two backends, got {}",
            backends.len()
        );
    }
    // The planner needs the model in step even when it is not compared.
    let planner_only = !backends.iter().any(|b| b.name == "oracle");
    let planner = if planner_only {
        Some(oracle_backend(&oracle).await)
    } else {
        None
    };

    let names: Vec<&str> = backends.iter().map(|b| b.name.as_str()).collect();
    eprintln!("[port] backends: {}", names.join(", "));

    let max_steps: usize = std::env::var("TEST_MAX_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    const BATCH_SIZE: usize = 200;
    const PLATEAU_BATCHES: usize = 20;

    let mut rng = fastrand::Rng::with_seed(0x00c0_ffee_dead_beef);
    let mut now = T0;
    let mut covered: HashMap<String, usize> = HashMap::new();
    let mut total_steps = 0usize;
    let mut seen_sigs: HashSet<(String, u16, u8)> = HashSet::new();
    let mut plateau_count = 0usize;
    let mut timings: HashMap<(String, String), Vec<u64>> = HashMap::new();
    let soft = std::env::var("TEST_SOFT").is_ok();
    let mut divergences: Vec<String> = Vec::new();

    'outer: loop {
        reset_all(&backends, now).await;
        if let Some(p) = &planner {
            reset_all(std::slice::from_ref(p), now).await;
        }
        now = T0;

        let sigs_before = seen_sigs.len();

        for _ in 0..BATCH_SIZE {
            if total_steps >= max_steps {
                break 'outer;
            }

            let (envelope, now_after) = {
                let o = oracle.lock();
                let op = pick_op(&mut rng, &o, &covered);
                build_envelope(op, &mut rng, &o, now)
            };
            now = now_after;
            total_steps += 1;

            let kind = envelope.kind.clone();
            let ctx = format!("step={total_steps} op={kind}");
            eprintln!("[port] {ctx} now={now} data={}", envelope.data);

            let pre_snaps = snap_all(&backends, now).await;
            assert_agree(&pre_snaps, "snapshot", &format!("BEFORE {ctx}"));

            let (mut results, routed) = send_all(&backends, &envelope, &mut timings).await;
            if let Some(p) = &planner {
                let _ = send(p, &envelope).await;
                let _ = p.router.take();
            }
            for (_, _, data) in &mut results {
                normalize_resp(data);
            }

            let status = results[0].1;
            if status < 300 {
                covered.entry(kind.clone()).or_insert(total_steps);
            }
            let sc = state_class(&pre_snaps[0].1);
            seen_sigs.insert((kind.clone(), status as u16, sc));

            // TEST_SOFT=1 records a response divergence and carries on, as long
            // as the state and the routed messages still agree — a validation
            // order that differs leaves the store the same, and stopping at
            // the first one hides the second. State divergence always stops.
            if soft {
                if let Some(detail) = resps_disagree(&results) {
                    eprintln!("[diverge] {ctx} data={}\n{detail}", envelope.data);
                    divergences.push(format!("{kind}: {detail}"));
                }
            } else {
                assert_resps_agree(&results, &ctx);
            }
            assert_agree(&routed, "routed messages", &format!("ROUTE {ctx}"));

            let post_snaps = snap_all(&backends, now).await;
            if let Some(from) = std::env::var("TEST_TRACE_FROM")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                if total_steps >= from {
                    for (name, snap) in &post_snaps {
                        eprintln!(
                            "[trace] {ctx} {name} status={status} tasks={} callbacks={} promises={}",
                            snap["tasks"], snap["callbacks"],
                            snap["promises"].as_array().map(|a| a.iter().map(|p| json!({"id": p["id"], "state": p["state"], "timeoutAt": p["timeoutAt"]})).collect::<Vec<_>>()).map(Value::Array).unwrap_or(Value::Null)
                        );
                    }
                }
            }
            assert_agree(&post_snaps, "snapshot", &format!("AFTER {ctx}"));
        }

        let new_sigs = seen_sigs.len().saturating_sub(sigs_before);
        if covered.len() == ALL_OPS.len() {
            if new_sigs == 0 {
                plateau_count += 1;
                eprintln!(
                    "[port] plateau {plateau_count}/{PLATEAU_BATCHES} — {} total signatures, no new in this batch",
                    seen_sigs.len()
                );
            } else {
                plateau_count = 0;
            }
            if plateau_count >= PLATEAU_BATCHES {
                eprintln!(
                    "[port] coverage plateau reached after {total_steps} steps ({} signatures)",
                    seen_sigs.len()
                );
                break 'outer;
            }
        }
    }

    let snaps = snap_all(&backends, now).await;
    assert_agree(&snaps, "snapshot", "final");

    eprintln!("[port] coverage after {total_steps} steps:");
    let mut missing = Vec::new();
    for op in ALL_OPS {
        if let Some(step) = covered.get(*op) {
            eprintln!("  [OK ] {op} (first 2xx at step {step})");
        } else {
            eprintln!("  [MISS] {op}");
            missing.push(*op);
        }
    }
    if !missing.is_empty() {
        panic!(
            "Coverage incomplete after {total_steps} steps — these ops never produced a 2xx: {:?}",
            missing
        );
    }

    if !divergences.is_empty() {
        let mut distinct: Vec<&String> = Vec::new();
        for d in &divergences {
            if !distinct.contains(&d) {
                distinct.push(d);
            }
        }
        eprintln!(
            "[port] {} response divergences ({} distinct) over {total_steps} steps:",
            divergences.len(),
            distinct.len()
        );
        for d in &distinct {
            eprintln!("[port]   {d}");
        }
        panic!("response divergences under TEST_SOFT — see [diverge] lines");
    }

    eprintln!(
        "[port] PASSED — {total_steps} steps, {} backends, all {} ops covered, {} behavioral signatures",
        backends.len(),
        ALL_OPS.len(),
        seen_sigs.len(),
    );
    print_timing_summary(&mut timings, &backends);
}

fn print_timing_summary(timings: &mut HashMap<(String, String), Vec<u64>>, backends: &[Backend]) {
    let backend_names: Vec<&str> = backends.iter().map(|b| b.name.as_str()).collect();
    let op_w = ALL_OPS.iter().map(|s| s.len()).max().unwrap_or(20);
    let cell_w = 16usize;
    eprintln!("\n[port] timing summary (mean / p99 µs):");
    eprintln!(
        "[port]   {:<op_w$}  {}",
        "operation",
        backend_names
            .iter()
            .map(|n| format!("{:>cell_w$}", n))
            .collect::<Vec<_>>()
            .join("  ")
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
        eprintln!("[port]   {:<op_w$}  {}", op, cells.join("  "));
    }
    eprintln!();
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Infrastructure
// ---------------------------------------------------------------------------

/// Bring every backend to an empty store, and forget anything routed.
async fn reset_all(backends: &[Backend], now: i64) {
    let envelope = req("debug.reset", json!({}), now);
    for b in backends {
        let resp = send(b, &envelope).await;
        assert_eq!(resp.head.status, 200, "debug.reset failed on {}", b.name);
        let _ = b.router.take();
    }
}

/// One request to every backend: its response, and what it routed.
async fn send_all(
    backends: &[Backend],
    envelope: &RequestEnvelope,
    timings: &mut HashMap<(String, String), Vec<u64>>,
) -> (Vec<(String, i32, Value)>, Vec<(String, Value)>) {
    let mut out = Vec::new();
    let mut routed = Vec::new();
    for b in backends {
        let t0 = Instant::now();
        let resp = send(b, envelope).await;
        let ns = t0.elapsed().as_nanos() as u64;
        timings
            .entry((b.name.clone(), envelope.kind.clone()))
            .or_default()
            .push(ns);
        routed.push((b.name.clone(), Value::Array(b.router.take())));
        out.push((b.name.clone(), resp.head.status, resp.data));
    }
    (out, routed)
}

/// The durable state, without the message queue.
///
/// `messages` is dropped: a backend that routes has nothing queued, one that
/// also holds under debug reports what it held, and the model keeps a queue.
/// None of that is behaviour. What was emitted is compared where it is
/// observable — at the router — in `send_all`.
async fn snap_all(backends: &[Backend], now: i64) -> Vec<(String, Value)> {
    let envelope = req("debug.snap", json!({}), now);
    let mut state = Vec::new();
    for b in backends {
        let resp = send(b, &envelope).await;
        assert_eq!(resp.head.status, 200, "debug.snap failed on {}", b.name);
        let mut data = resp.data;
        normalize_snap(&mut data);
        if let Some(o) = data.as_object_mut() {
            o.remove("messages");
        }
        state.push((b.name.clone(), data));
    }
    state
}

/// Every entry must be the same value.
fn assert_agree(vals: &[(String, Value)], what: &str, ctx: &str) {
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

/// How the responses differ, if they do — status and body per backend on
/// one line, for the soft mode's log.
fn resps_disagree(results: &[(String, i32, Value)]) -> Option<String> {
    let agree = results
        .windows(2)
        .all(|w| w[0].1 == w[1].1 && w[0].2 == w[1].2);
    if agree {
        return None;
    }
    Some(
        results
            .iter()
            .map(|(n, s, d)| format!("{n}={s} {d}"))
            .collect::<Vec<_>>()
            .join(" | "),
    )
}

fn assert_resps_agree(results: &[(String, i32, Value)], ctx: &str) {
    let statuses_agree = results.windows(2).all(|w| w[0].1 == w[1].1);
    if !statuses_agree {
        let detail: String = results
            .iter()
            .map(|(n, s, d)| format!("  {n}={s} {d}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("{ctx}: status mismatch\n{detail}");
    }
    let data_agree = results.windows(2).all(|w| w[0].2 == w[1].2);
    if !data_agree {
        let detail: String = results
            .iter()
            .map(|(n, _, d)| format!("  {n}:\n{d:#}"))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("{ctx}: data mismatch\n{detail}");
    }
}

fn normalize_snap(snap: &mut Value) {
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
fn sort_messages(arr: &mut [Value]) {
    arr.sort_by_key(msg_sort_key);
}

fn msg_sort_key(msg: &Value) -> String {
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

fn normalize_resp(data: &mut Value) {
    for key in &["promises", "tasks", "schedules"] {
        if let Some(arr) = data.get_mut(*key).and_then(|v| v.as_array_mut()) {
            sort_by_id(arr);
        }
    }
}

fn sort_by_id(arr: &mut [Value]) {
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

fn state_class(snap: &Value) -> u8 {
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

enum Op {
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

fn pick_op(rng: &mut fastrand::Rng, oracle: &Oracle, covered: &HashMap<String, usize>) -> Op {
    let uncovered = |kind: &str| !covered.contains_key(kind);

    let has_acquired = oracle.has_tasks_in_state(TaskState::Acquired);
    let has_pending_t = oracle.has_tasks_in_state(TaskState::Pending);
    let has_suspended = oracle.has_tasks_in_state(TaskState::Suspended);
    let has_halted = oracle.has_tasks_in_state(TaskState::Halted);
    let has_pending_p = oracle.has_pending_promises();
    let has_pending_p_with_target = oracle.has_pending_promises_with_target();
    let has_schedules = oracle.has_schedules();

    if uncovered("task.suspend") && has_acquired && has_pending_p_with_target {
        return Op::TaskSuspend;
    }
    if uncovered("task.continue") && has_halted {
        return Op::TaskContinue;
    }
    if uncovered("task.release") && has_acquired {
        return Op::TaskRelease;
    }
    if uncovered("task.fulfill") && has_acquired {
        return Op::TaskFulfill;
    }
    if uncovered("task.halt") && (has_acquired || has_pending_t || has_suspended) {
        return Op::TaskHalt;
    }
    if uncovered("task.acquire") && has_pending_t {
        return Op::TaskAcquire;
    }
    if uncovered("promise.register_callback")
        && has_pending_p_with_target
        && (has_acquired || has_pending_t)
    {
        return Op::PromiseRegisterCallback;
    }
    if uncovered("promise.register_listener") && has_pending_p {
        return Op::PromiseRegisterListener;
    }
    if uncovered("schedule.delete") && has_schedules {
        return Op::ScheduleDelete;
    }
    if uncovered("task.fence") && has_acquired {
        return Op::TaskFence;
    }

    match rng.u32(0..100) {
        0..=14 => Op::PromiseCreate,
        15..=19 => Op::PromiseGet,
        20..=24 => Op::PromiseSettle,
        25..=27 => Op::PromiseRegisterCallback,
        28..=29 => Op::PromiseRegisterListener,
        30..=31 => Op::PromiseSearch,
        32..=39 => Op::TaskCreate,
        40..=41 => Op::TaskGet,
        42..=44 => Op::TaskAcquire,
        45..=47 => Op::TaskRelease,
        48..=52 => Op::TaskFulfill,
        53..=57 => Op::TaskSuspend,
        58..=60 => Op::TaskFence,
        61..=63 => Op::TaskHeartbeat,
        64..=66 => Op::TaskHalt,
        67..=69 => Op::TaskContinue,
        70..=71 => Op::TaskSearch,
        72..=77 => Op::ScheduleCreate,
        78..=80 => Op::ScheduleGet,
        81..=83 => Op::ScheduleDelete,
        84..=85 => Op::ScheduleSearch,
        _ => Op::DebugTick,
    }
}

fn build_envelope(
    op: Op,
    rng: &mut fastrand::Rng,
    oracle: &Oracle,
    now: i64,
) -> (RequestEnvelope, i64) {
    match op {
        Op::PromiseCreate => (gen_promise_create(rng, oracle, now), now),
        Op::PromiseGet => (gen_promise_get(rng, oracle, now), now),
        Op::PromiseSettle => (gen_promise_settle(rng, oracle, now), now),
        Op::PromiseRegisterCallback => (gen_promise_register_callback(rng, oracle, now), now),
        Op::PromiseRegisterListener => (gen_promise_register_listener(rng, oracle, now), now),
        Op::PromiseSearch => (gen_promise_search(rng, now), now),
        Op::TaskCreate => (gen_task_create(rng, now), now),
        Op::TaskGet => (gen_task_get(rng, oracle, now), now),
        Op::TaskAcquire => (gen_task_acquire(rng, oracle, now), now),
        Op::TaskRelease => (gen_task_release(rng, oracle, now), now),
        Op::TaskFulfill => (gen_task_fulfill(rng, oracle, now), now),
        Op::TaskSuspend => (gen_task_suspend(rng, oracle, now), now),
        Op::TaskFence => (gen_task_fence(rng, oracle, now), now),
        Op::TaskHeartbeat => (gen_task_heartbeat(rng, oracle, now), now),
        Op::TaskHalt => (gen_task_halt(rng, oracle, now), now),
        Op::TaskContinue => (gen_task_continue(rng, oracle, now), now),
        Op::TaskSearch => (gen_task_search(rng, now), now),
        Op::ScheduleCreate => (gen_schedule_create(rng, now), now),
        Op::ScheduleGet => (gen_schedule_get(rng, oracle, now), now),
        Op::ScheduleDelete => (gen_schedule_delete(rng, oracle, now), now),
        Op::ScheduleSearch => (gen_schedule_search(rng, now), now),
        Op::DebugTick => gen_debug_tick(rng, now),
    }
}

fn gen_promise_create(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
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
        now,
    )
}

/// An awaited promise for a callback or a suspend.
///
/// Mostly awaitable, so the registration paths are actually walked; one time
/// in four whatever is pending, so the 422 stays covered too.
fn pick_awaited(rng: &mut fastrand::Rng, oracle: &Oracle, awaiter: &str) -> String {
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

fn gen_promise_get(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let all = oracle.all_promise_ids();
    let id = pick(rng, &all).unwrap_or_else(|| random_promise_id(rng));
    req("promise.get", json!({ "id": id }), now)
}

fn gen_promise_settle(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let pending = oracle.pending_promise_ids();
    let id = pick(rng, &pending).unwrap_or_else(|| random_promise_id(rng));
    let state = if rng.bool() { "resolved" } else { "rejected" };
    req(
        "promise.settle",
        json!({ "id": id, "state": state, "value": {} }),
        now,
    )
}

fn gen_promise_register_callback(
    rng: &mut fastrand::Rng,
    oracle: &Oracle,
    now: i64,
) -> RequestEnvelope {
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
        now,
    )
}

fn gen_promise_register_listener(
    rng: &mut fastrand::Rng,
    oracle: &Oracle,
    now: i64,
) -> RequestEnvelope {
    let pending = oracle.pending_promise_ids();
    let id = pick(rng, &pending).unwrap_or_else(|| random_promise_id(rng));
    req(
        "promise.register_listener",
        json!({ "awaited": id, "address": WORKER_URL }),
        now,
    )
}

fn gen_promise_search(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
    let data = match rng.u32(0..4) {
        0 => json!({ "state": "pending",  "limit": 10 }),
        1 => json!({ "state": "resolved", "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("promise.search", data, now)
}

fn gen_task_create(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
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
        now,
    )
}

fn gen_task_get(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let mut all = oracle.tasks_by_state(TaskState::Acquired);
    all.extend(oracle.tasks_by_state(TaskState::Pending));
    all.extend(oracle.tasks_by_state(TaskState::Suspended));
    all.extend(oracle.tasks_by_state(TaskState::Halted));
    let id = pick(rng, &all)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.get", json!({ "id": id }), now)
}

fn gen_task_acquire(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let pending = oracle.tasks_by_state(TaskState::Pending);
    let (id, version) = pick(rng, &pending).unwrap_or_else(|| (random_task_id(rng), 0));
    req(
        "task.acquire",
        json!({ "id": id, "version": version, "pid": PID, "ttl": TTL }),
        now,
    )
}

fn gen_task_release(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    req("task.release", json!({ "id": id, "version": version }), now)
}

fn gen_task_fulfill(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
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
        now,
    )
}

fn gen_task_suspend(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
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
        now,
    )
}

fn gen_task_fence(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (task_id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    let task_origin = task_id.split(':').next().unwrap_or("").to_string();
    // TEST_SAME_ORIGIN_FENCE=1 keeps every fence action inside the task's
    // origin. The rule is: a settle must share the origin; a create may share
    // it or name a root (an id with no ':'), which is a detached computation.
    // The blob server still refuses every cross-origin fence before reading
    // state, so with that backend in the comparison this is what gets past
    // the first cross-origin root create to whatever else may differ. Off,
    // the divergence is reported like any other.
    let same_origin = std::env::var("TEST_SAME_ORIGIN_FENCE").is_ok();
    let pending_p: Vec<String> = oracle
        .pending_promise_ids()
        .into_iter()
        .filter(|p| !same_origin || p.split(':').next().unwrap_or("") == task_origin)
        .collect();
    let do_settle = !pending_p.is_empty() && rng.u32(0..4) != 0;
    if !do_settle {
        // Four flavours of create: a child in the task's origin (the common
        // case), a root in another origin (detached: allowed), a child in
        // another origin (refused), and a root that is the task's own origin
        // (refused as self-reference when it is, allowed otherwise).
        let new_promise_id = match rng.u32(0..8) {
            0 | 1 | 2 | 3 | 4 => format!("{task_origin}:p{}", rng.u32(0..8)),
            5 if !same_origin => format!("root{}", rng.u32(0..4)),
            6 if !same_origin => format!("other:p{}", rng.u32(0..4)),
            _ => promise_id(rng.u32(0..8)),
        };
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
            now,
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
            now,
        )
    }
}

fn gen_task_heartbeat(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
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
    req("task.heartbeat", json!({ "pid": pid, "tasks": tasks }), now)
}

fn gen_task_halt(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let mut all = oracle.tasks_by_state(TaskState::Acquired);
    all.extend(oracle.tasks_by_state(TaskState::Suspended));
    all.extend(oracle.tasks_by_state(TaskState::Pending));
    let id = pick(rng, &all)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.halt", json!({ "id": id }), now)
}

fn gen_task_continue(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let halted = oracle.tasks_by_state(TaskState::Halted);
    let id = pick(rng, &halted)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.continue", json!({ "id": id }), now)
}

fn gen_task_search(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
    let data = match rng.u32(0..5) {
        0 => json!({ "state": "acquired",  "limit": 10 }),
        1 => json!({ "state": "pending",   "limit": 10 }),
        2 => json!({ "state": "suspended", "limit": 10 }),
        3 => json!({ "state": "halted",    "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("task.search", data, now)
}

fn gen_schedule_create(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
    let id = schedule_id(rng.u32(0..4));
    let promise_timeout = now + rng.i64(60_000..600_000);
    req(
        "schedule.create",
        json!({
            "id": id,
            "cron": "* * * * *",
            "promiseId": "sched-promise-{{.id}}-{{.timestamp}}".to_string(),
            "promiseTimeout": promise_timeout,
            "promiseParam": {},
            "promiseTags": { "resonate:target": WORKER_URL }
        }),
        now,
    )
}

fn gen_schedule_get(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let schedules = oracle.schedule_ids();
    let id = pick(rng, &schedules).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.get", json!({ "id": id }), now)
}

fn gen_schedule_delete(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let schedules = oracle.schedule_ids();
    let id = pick(rng, &schedules).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.delete", json!({ "id": id }), now)
}

fn gen_schedule_search(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
    let limit = if rng.bool() { 10 } else { 5 };
    req("schedule.search", json!({ "limit": limit }), now)
}

fn gen_debug_tick(rng: &mut fastrand::Rng, now: i64) -> (RequestEnvelope, i64) {
    let new_now = now + rng.i64(0..50_000);
    // The tick is the one request whose head time is the time it moves to:
    // the shell resolves `now` from the head, and the sweep runs at that.
    (
        req("debug.tick", json!({ "time": new_now }), new_now),
        new_now,
    )
}

// IDs share the origin "diff" (text before the first ':'), so a task promise
// (`diff:tN`) and a plain promise (`diff:pM`) pass the origin-match validation
// used by promise.register_callback and task.suspend.
fn promise_id(n: u32) -> String {
    format!("diff:p{n}")
}
fn task_id(n: u32) -> String {
    format!("diff:t{n}")
}
fn schedule_id(n: u32) -> String {
    format!("s{n}")
}

fn random_promise_id(rng: &mut fastrand::Rng) -> String {
    promise_id(rng.u32(0..8))
}
fn random_task_id(rng: &mut fastrand::Rng) -> String {
    task_id(rng.u32(0..8))
}
fn random_schedule_id(rng: &mut fastrand::Rng) -> String {
    schedule_id(rng.u32(0..4))
}

fn promise_id_different_from(rng: &mut fastrand::Rng, other: &str) -> String {
    let n = rng.u32(0..8);
    let candidate = promise_id(n);
    if candidate == other {
        promise_id((n + 1) % 8)
    } else {
        candidate
    }
}
