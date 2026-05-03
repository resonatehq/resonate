// Differential random testing: drive the same random operation sequence through
// multiple backends simultaneously and assert identical responses and state
// snapshots at every step.
//
// Backends:
//   SQLite   — always active (in-memory, :memory:)
//   Oracle   — always active (in-memory reference model)
//   Postgres — active when TEST_POSTGRES_URL env var is set
//   MySQL    — active when TEST_MYSQL_URL env var is set
//
// Coverage requirement: the test runs until every operation kind has produced
// at least one 2xx response, guaranteeing that we are not trivially passing by
// only exercising failure paths.
//
// Run (SQLite + Oracle only):
//   cargo test --test differential -- --nocapture
//
// Run (all backends):
//   TEST_POSTGRES_URL=postgres://resonate:resonate@localhost:5432/resonate \
//   TEST_MYSQL_URL=mysql://resonate:resonate@localhost:3306/resonate \
//     cargo test --test differential -- --nocapture

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use resonate::types::TaskState;

// Serializes tests that share the same Postgres/MySQL database so that
// concurrent debug.reset calls from one test cannot truncate another's data.
static DB_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
fn db_lock() -> &'static Mutex<()> {
    DB_LOCK.get_or_init(|| Mutex::new(()))
}

use resonate::{
    config::Config,
    oracle::Oracle,
    persistence::{
        persistence_mysql::MysqlStorage, persistence_postgres::PostgresStorage,
        persistence_sqlite::SqliteStorage, Storage,
    },
    server::{dispatch, Server},
    types::{RequestEnvelope, RequestHead, ResponseEnvelope, SUPPORTED_VERSIONS},
};
use serde_json::{json, Value};

const TASK_RETRY_TIMEOUT_MS: i64 = 30_000;
// Fixed epoch anchor; all test times are offsets from here (ms).
const T0: i64 = 1_000_000_000;
// Fake worker URL — passes is_valid_address but no actual delivery attempted.
const WORKER_URL: &str = "http://diff-test-worker:9999";
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

fn debug_config() -> Config {
    serde_json::from_value(json!({ "debug": true })).expect("valid default config")
}

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: fastrand::u64(..).to_string(),
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

enum Backend {
    Server(Arc<Server>),
    Oracle(Arc<Mutex<Oracle>>),
}

impl Backend {
    async fn dispatch(&self, envelope: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        match self {
            Backend::Server(srv) => dispatch(srv, envelope, now).await,
            Backend::Oracle(o) => {
                let mut req = envelope.clone();
                req.head.debug_time = Some(now);
                o.lock().unwrap().apply(&req)
            }
        }
    }
}

fn server_backend(storage: Storage) -> Backend {
    Backend::Server(Arc::new(Server::new(debug_config(), None, storage)))
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
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn differential_random() {
    debug_assert_eq!(22, ALL_OPS.len(), "Op has 22 variants; ALL_OPS must match");

    let sqlite = SqliteStorage::open(":memory:", TASK_RETRY_TIMEOUT_MS).expect("sqlite open");
    let oracle = Arc::new(Mutex::new(Oracle::new()));

    // Postgres and MySQL are opt-in via env vars.
    let pg_url = std::env::var("TEST_POSTGRES_URL").ok();
    let my_url = std::env::var("TEST_MYSQL_URL").ok();

    // Hold the lock while connecting + initializing schemas so concurrent test
    // runs on the same DB don't race on debug.reset / schema creation.
    let _db_guard = if pg_url.is_some() || my_url.is_some() {
        Some(db_lock().lock().unwrap_or_else(|e| e.into_inner()))
    } else {
        None
    };

    let pg_backend: Option<Backend> = match pg_url {
        Some(url) => {
            let pg = PostgresStorage::connect(&url, 5, TASK_RETRY_TIMEOUT_MS)
                .await
                .expect("postgres connect");
            pg.init().await.expect("postgres schema init");
            Some(server_backend(Storage::Postgres(pg)))
        }
        None => {
            eprintln!("[diff] TEST_POSTGRES_URL not set — PostgreSQL skipped");
            None
        }
    };

    let my_backend: Option<Backend> = match my_url {
        Some(url) => {
            let my = MysqlStorage::connect(&url, 5, TASK_RETRY_TIMEOUT_MS)
                .await
                .expect("mysql connect");
            my.init().await.expect("mysql schema init");
            Some(server_backend(Storage::Mysql(my)))
        }
        None => {
            eprintln!("[diff] TEST_MYSQL_URL not set — MySQL skipped");
            None
        }
    };

    let mut backends: Vec<(String, Backend)> = vec![
        ("sqlite".into(), server_backend(Storage::Sqlite(sqlite))),
        ("oracle".into(), Backend::Oracle(Arc::clone(&oracle))),
    ];
    if let Some(pg) = pg_backend {
        backends.push(("postgres".into(), pg));
    }
    if let Some(my) = my_backend {
        backends.push(("mysql".into(), my));
    }

    let names: Vec<&str> = backends.iter().map(|(n, _)| n.as_str()).collect();
    eprintln!("[diff] backends: {}", names.join(", "));

    setup_all(&backends, T0).await;

    const MAX_STEPS: usize = 200_000;
    const BATCH_SIZE: usize = 200;
    const PLATEAU_BATCHES: usize = 20;

    let mut rng = fastrand::Rng::with_seed(0xc0ffee_dead_beef);
    let mut now = T0;
    let mut covered: HashMap<String, usize> = HashMap::new();
    let mut total_steps = 0usize;
    let mut seen_sigs: HashSet<(String, u16, u8)> = HashSet::new();
    let mut plateau_count = 0usize;
    let mut timings: HashMap<(String, String), Vec<u64>> = HashMap::new();

    'outer: loop {
        reset_all(&backends, now).await;
        now = T0;

        let sigs_before = seen_sigs.len();

        for _ in 0..BATCH_SIZE {
            if total_steps >= MAX_STEPS {
                break 'outer;
            }

            // Query oracle state to generate the next request, then release
            // the lock before dispatching so the oracle backend can reacquire it.
            let (envelope, now_after) = {
                let o = oracle.lock().unwrap();
                let op = pick_op(&mut rng, &o, &covered);
                build_envelope(op, &mut rng, &o, now)
            };
            now = now_after;
            total_steps += 1;

            let kind = envelope.kind.clone();
            let ctx = format!("step={total_steps} op={kind}");
            eprintln!("[diff] {ctx} now={now}");

            // Verify backends agree before this step.
            let pre_snaps = snap_all(&backends, now).await;
            assert_no_divergence(
                &pre_snaps,
                &[
                    "promises",
                    "messages",
                    "tasks",
                    "callbacks",
                    "taskTimeouts",
                    "promiseTimeouts",
                ],
                &format!("BEFORE {ctx}"),
            );

            let mut results = send_all(&backends, &envelope, now, &mut timings).await;
            for (_, _, data) in &mut results {
                normalize_resp(data);
            }

            let (_, status, _) = &results[0];
            let status = *status;

            if status < 300 {
                covered.entry(kind.clone()).or_insert(total_steps);
            }

            let sc = state_class(&pre_snaps[0].1);
            seen_sigs.insert((kind.clone(), status as u16, sc));

            assert_resps_agree(&results, &ctx);

            let mid_snaps = snap_all(&backends, now).await;
            assert_no_divergence(&mid_snaps, &["messages"], &format!("AFTER {ctx}"));
        }

        let snaps = snap_all(&backends, now).await;
        assert_snaps_agree(&snaps, &format!("step={total_steps}"));

        let new_sigs = seen_sigs.len().saturating_sub(sigs_before);
        if covered.len() == ALL_OPS.len() {
            if new_sigs == 0 {
                plateau_count += 1;
                eprintln!(
                    "[diff] plateau {plateau_count}/{PLATEAU_BATCHES} — {} total signatures, no new in this batch",
                    seen_sigs.len()
                );
            } else {
                plateau_count = 0;
            }
            if plateau_count >= PLATEAU_BATCHES {
                eprintln!(
                    "[diff] coverage plateau reached after {total_steps} steps ({} signatures)",
                    seen_sigs.len()
                );
                break 'outer;
            }
        }
    }

    let snaps = snap_all(&backends, now).await;
    assert_snaps_agree(&snaps, "final");

    eprintln!("[diff] coverage after {total_steps} steps:");
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

    eprintln!(
        "[diff] PASSED — {total_steps} steps, {} backends, all {} ops covered, {} behavioral signatures",
        backends.len(),
        ALL_OPS.len(),
        seen_sigs.len(),
    );

    print_timing_summary(&mut timings, &backends);
}

fn print_timing_summary(
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

async fn setup_all(backends: &[(String, Backend)], now: i64) {
    for envelope in &[req("debug.start", json!({})), req("debug.reset", json!({}))] {
        for (name, b) in backends {
            let resp = b.dispatch(envelope, now).await;
            assert_eq!(resp.head.status, 200, "{} failed on {name}", envelope.kind);
        }
    }
}

async fn reset_all(backends: &[(String, Backend)], now: i64) {
    let envelope = req("debug.reset", json!({}));
    for (name, b) in backends {
        let resp = b.dispatch(&envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.reset failed on {name}");
    }
}

async fn send_all(
    backends: &[(String, Backend)],
    envelope: &RequestEnvelope,
    now: i64,
    timings: &mut HashMap<(String, String), Vec<u64>>,
) -> Vec<(String, i32, Value)> {
    let mut out = Vec::new();
    let kind = envelope.kind.clone();
    for (name, b) in backends {
        let t0 = Instant::now();
        let resp = b.dispatch(envelope, now).await;
        let ns = t0.elapsed().as_nanos() as u64;
        timings
            .entry((name.clone(), kind.clone()))
            .or_default()
            .push(ns);
        out.push((name.clone(), resp.head.status, resp.data));
    }
    out
}

async fn snap_all(backends: &[(String, Backend)], now: i64) -> Vec<(String, Value)> {
    let envelope = req("debug.snap", json!({}));
    let mut out = Vec::new();
    for (name, b) in backends {
        let resp = b.dispatch(&envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.snap failed on {name}");
        let mut data = resp.data;
        normalize_snap(&mut data);
        out.push((name.clone(), data));
    }
    out
}

fn assert_resps_agree(results: &[(String, i32, Value)], ctx: &str) {
    let all_statuses: Vec<(&str, i32)> = results.iter().map(|(n, s, _)| (n.as_str(), *s)).collect();
    let statuses_agree = all_statuses.windows(2).all(|w| w[0].1 == w[1].1);
    if !statuses_agree {
        let detail: String = all_statuses
            .iter()
            .map(|(n, s)| format!("  {n}={s}"))
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

fn assert_snaps_agree(snaps: &[(String, Value)], ctx: &str) {
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

fn assert_no_divergence(snaps: &[(String, Value)], keys: &[&str], ctx: &str) {
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

fn sort_messages(arr: &mut Vec<Value>) {
    arr.sort_by(|a, b| msg_sort_key(a).cmp(&msg_sort_key(b)));
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

fn sort_by_id(arr: &mut Vec<Value>) {
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
    let has_schedules = oracle.has_schedules();

    if uncovered("task.suspend") && has_acquired && has_pending_p {
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
    if uncovered("promise.register_callback") && has_pending_p && (has_acquired || has_pending_t) {
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

fn gen_promise_create(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let all = oracle.all_promise_ids();
    let id = pick(rng, &all).unwrap_or_else(|| random_promise_id(rng));
    let timeout_at = now + rng.i64(30_000..300_000);
    req(
        "promise.create",
        json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": {} }),
    )
}

fn gen_promise_get(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let all = oracle.all_promise_ids();
    let id = pick(rng, &all).unwrap_or_else(|| random_promise_id(rng));
    req("promise.get", json!({ "id": id }))
}

fn gen_promise_settle(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let pending = oracle.pending_promise_ids();
    let id = pick(rng, &pending).unwrap_or_else(|| random_promise_id(rng));
    let state = if rng.bool() { "resolved" } else { "rejected" };
    req(
        "promise.settle",
        json!({ "id": id, "state": state, "value": {} }),
    )
}

fn gen_promise_register_callback(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let pending_t = oracle.tasks_by_state(TaskState::Pending);
    let awaiter = pick(rng, &acquired)
        .or_else(|| pick(rng, &pending_t))
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    let pending_p = oracle.pending_promise_ids();
    let awaited = pending_p
        .iter()
        .find(|p| p.as_str() != awaiter)
        .cloned()
        .unwrap_or_else(|| promise_id_different_from(rng, &awaiter));
    req(
        "promise.register_callback",
        json!({ "awaited": awaited, "awaiter": awaiter }),
    )
}

fn gen_promise_register_listener(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let pending = oracle.pending_promise_ids();
    let id = pick(rng, &pending).unwrap_or_else(|| random_promise_id(rng));
    req(
        "promise.register_listener",
        json!({ "awaited": id, "address": WORKER_URL }),
    )
}

fn gen_promise_search(rng: &mut fastrand::Rng) -> RequestEnvelope {
    let data = match rng.u32(0..4) {
        0 => json!({ "state": "pending",  "limit": 10 }),
        1 => json!({ "state": "resolved", "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("promise.search", data)
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
    )
}

fn gen_task_get(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let mut all = oracle.tasks_by_state(TaskState::Acquired);
    all.extend(oracle.tasks_by_state(TaskState::Pending));
    all.extend(oracle.tasks_by_state(TaskState::Suspended));
    all.extend(oracle.tasks_by_state(TaskState::Halted));
    let id = pick(rng, &all)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.get", json!({ "id": id }))
}

fn gen_task_acquire(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let pending = oracle.tasks_by_state(TaskState::Pending);
    let (id, version) = pick(rng, &pending).unwrap_or_else(|| (random_task_id(rng), 0));
    req(
        "task.acquire",
        json!({ "id": id, "version": version, "pid": PID, "ttl": TTL }),
    )
}

fn gen_task_release(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    req("task.release", json!({ "id": id, "version": version }))
}

fn gen_task_fulfill(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
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

fn gen_task_suspend(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (task_id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    let pending_p = oracle.pending_promise_ids();
    let awaited = pending_p
        .iter()
        .find(|p| p.as_str() != task_id)
        .cloned()
        .unwrap_or_else(|| promise_id_different_from(rng, &task_id));
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

fn gen_task_fence(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
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

fn gen_task_heartbeat(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
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

fn gen_task_halt(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let mut all = oracle.tasks_by_state(TaskState::Acquired);
    all.extend(oracle.tasks_by_state(TaskState::Suspended));
    all.extend(oracle.tasks_by_state(TaskState::Pending));
    let id = pick(rng, &all)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.halt", json!({ "id": id }))
}

fn gen_task_continue(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let halted = oracle.tasks_by_state(TaskState::Halted);
    let id = pick(rng, &halted)
        .map(|(id, _)| id)
        .unwrap_or_else(|| random_task_id(rng));
    req("task.continue", json!({ "id": id }))
}

fn gen_task_search(rng: &mut fastrand::Rng) -> RequestEnvelope {
    let data = match rng.u32(0..5) {
        0 => json!({ "state": "acquired",  "limit": 10 }),
        1 => json!({ "state": "pending",   "limit": 10 }),
        2 => json!({ "state": "suspended", "limit": 10 }),
        3 => json!({ "state": "halted",    "limit": 10 }),
        _ => json!({ "limit": 10 }),
    };
    req("task.search", data)
}

fn gen_schedule_create(rng: &mut fastrand::Rng, now: i64) -> RequestEnvelope {
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
            "promiseTags": {}
        }),
    )
}

fn gen_schedule_get(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let schedules = oracle.schedule_ids();
    let id = pick(rng, &schedules).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.get", json!({ "id": id }))
}

fn gen_schedule_delete(rng: &mut fastrand::Rng, oracle: &Oracle) -> RequestEnvelope {
    let schedules = oracle.schedule_ids();
    let id = pick(rng, &schedules).unwrap_or_else(|| random_schedule_id(rng));
    req("schedule.delete", json!({ "id": id }))
}

fn gen_schedule_search(rng: &mut fastrand::Rng) -> RequestEnvelope {
    let limit = if rng.bool() { 10 } else { 5 };
    req("schedule.search", json!({ "limit": limit }))
}

fn gen_debug_tick(rng: &mut fastrand::Rng, now: i64) -> (RequestEnvelope, i64) {
    let new_now = now + rng.i64(0..50_000);
    (req("debug.tick", json!({ "time": new_now })), new_now)
}

fn promise_id(n: u32) -> String {
    format!("p{n}")
}
fn task_id(n: u32) -> String {
    format!("t{n}")
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
