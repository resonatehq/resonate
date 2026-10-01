// In-crate differential: the Kafka node against the oracle, both behind
// `ResonateServer`, driven through one seeded random trajectory and compared
// at every step — response first, then `debug.snap`.
//
// A copy of the blob backend's own differential with only the backend under
// test swapped: one node owning every partition of an in-process log
// (`MemLog`), eight partitions, so every request is routed, grouped into
// rounds, diffed into per-promise records, committed, and read back from the
// local store before the next one. Time moves only through `debug.tick`.
//
// Run:
//   cargo test -p resonate-server-kafka --test differential -- --nocapture

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use resonate_core::types::{
    RequestEnvelope, RequestHead, ResponseEnvelope, TaskState, SUPPORTED_VERSIONS,
};
use resonate_core::ResonateServer;
use resonate_server_blob::oracle::{Oracle, SharedOracle};
use resonate_server_blob::sender::NullRouter;
use resonate_server_kafka::log::mem::{Fault, MemLog};
use resonate_server_kafka::node::{Node, NodeCfg};
use serde_json::{json, Value};

/// Every backend gets the same limit, or `preload` would differ by
/// construction rather than by behaviour. The storage configs' default.
const PRELOAD_LIMIT: u32 = 10;
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

type Backend = Arc<dyn ResonateServer>;

/// Send one request to a backend at time `now`.
///
/// `now` rides in `head.debug_time`: the server resolves the effective time
/// from it under its debug flag, and the oracle honours it unconditionally.
async fn send(backend: &Backend, envelope: &RequestEnvelope, now: i64) -> ResponseEnvelope {
    let mut envelope = envelope.clone();
    envelope.head.debug_time = Some(now);
    backend
        .process(&envelope)
        .await
        .unwrap_or_else(|e| panic!("backend unavailable for {}: {}", envelope.kind, e.message))
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

fn node_cfg() -> NodeCfg {
    NodeCfg {
        debug: true,
        search: true,
        partition: resonate_server_kafka::partition::PartitionCfg {
            kernel: resonate_server_blob::kernel::state::KernelCfg {
                preload_limit: PRELOAD_LIMIT,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn differential_random() {
    let server = Node::in_memory(8, Arc::new(NullRouter), node_cfg());
    server.start().await.expect("every partition taken over");
    run(server, usize::MAX, None).await;
}

/// The same trajectory over a log without transactions: every decision's
/// records written one by one, in the order that makes each prefix valid.
#[tokio::test(flavor = "multi_thread")]
async fn differential_random_plain() {
    let log = MemLog::plain(8);
    let server = Node::in_memory_on(log, Arc::new(NullRouter), node_cfg());
    server.start().await.expect("every partition taken over");
    run(server, usize::MAX, None).await;
}

/// Every prefix is a state the protocol allows: the same trajectory over a
/// log without transactions, with each step's commit cut after a random
/// number of its records — none, some, or all of them landing — and then
/// reported lost, as a broker that fails mid-commit would.
///
/// The partition stops and is taken over again; recovery finishes the
/// deliveries the cut left owing. Then both sides tick at the same instant
/// and either the node holds what the oracle holds — the step happened — or
/// the step is retried on the node, as a client told "unavailable" would,
/// and response and state must agree with the oracle's. `TEST_CUT_EVERY`
/// (default 1) cuts every n-th step.
#[tokio::test(flavor = "multi_thread")]
async fn prefix_random() {
    let log = MemLog::plain(8);
    let server = Node::in_memory_on(Arc::clone(&log), Arc::new(NullRouter), node_cfg());
    server.start().await.expect("every partition taken over");
    let steps: usize = std::env::var("TEST_PREFIX_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(usize::MAX);
    run(server, steps, Some(log)).await;
}

/// The same trajectory over a real Kafka: every round a transaction, every
/// `debug.reset` a batch of tombstones. Opt-in, and shorter by default.
/// `TEST_KAFKA_TRANSACTIONS=0` runs it without transactions.
///
///   TEST_KAFKA_BROKERS=localhost:9092 TEST_KAFKA_STEPS=5000 \
///     cargo test -p resonate-server-kafka --test differential -- --nocapture
#[tokio::test(flavor = "multi_thread")]
async fn differential_random_on_kafka() {
    let Ok(brokers) = std::env::var("TEST_KAFKA_BROKERS") else {
        eprintln!("[diff] TEST_KAFKA_BROKERS not set — the Kafka leg is skipped");
        return;
    };
    let steps: usize = std::env::var("TEST_KAFKA_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000);
    let prefix = format!("t-diff-{:08x}", fastrand::u32(..));
    let log = resonate_server_kafka::log::kafka::KafkaLog::connect(
        resonate_server_kafka::log::kafka::KafkaCfg {
            brokers,
            topic_prefix: prefix.clone(),
            txn_prefix: prefix,
            partitions: 8,
            replication_factor: 1,
            transactional: std::env::var("TEST_KAFKA_TRANSACTIONS").as_deref() != Ok("0"),
            ..Default::default()
        },
    )
    .await
    .expect("kafka");
    let cfg = node_cfg();
    let server = Node::new(
        cfg.clone(),
        log,
        resonate_server_kafka::local::mem::MemLocal::new(),
        Arc::new(resonate_server_blob::sender::Sender::new(
            Arc::new(NullRouter),
            cfg.debug,
        )),
        resonate_server_kafka::membership::StaticMembership::new(8),
        Arc::new(resonate_server_kafka::directory::NoDirectory),
        resonate_server_kafka::peer::LocalPeers::new(),
    );
    server.start().await.expect("every partition taken over");
    run(server, steps, None).await;
}

/// What the cuts did, for the summary: steps cut, by records landed against
/// records in the commit.
#[derive(Default)]
struct Cuts {
    /// (landed, of) → count.
    shapes: std::collections::BTreeMap<(usize, usize), usize>,
    /// The step happened, by recovery alone.
    applied: usize,
    /// The step was retried.
    retried: usize,
    /// Messages the cut lost, as a crash after a commit loses them.
    lost: usize,
}

async fn run(server: Arc<Node>, max_steps: usize, cut: Option<Arc<MemLog>>) {
    let node = Arc::clone(&server);
    let mut cuts = Cuts::default();
    let cut_every: usize = std::env::var("TEST_CUT_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    debug_assert_eq!(22, ALL_OPS.len(), "Op has 22 variants; ALL_OPS must match");
    let oracle = Arc::new(SharedOracle::with_preload_limit(PRELOAD_LIMIT));

    let backends: Vec<(String, Backend)> = vec![
        ("kafka".into(), server as Backend),
        ("oracle".into(), Arc::clone(&oracle) as Backend),
    ];

    let max_steps_cap: usize = 200_000;
    let max_steps = max_steps.min(max_steps_cap);
    const BATCH_SIZE: usize = 200;
    const PLATEAU_BATCHES: usize = 20;

    // TEST_SEED=<u64> picks the trajectory; the default is the one CI walks.
    let seed: u64 = std::env::var("TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x00c0_ffee_dead_beef);
    eprintln!("[diff] seed: {seed:#x}");
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut now = T0;
    let mut covered: HashMap<String, usize> = HashMap::new();
    let mut total_steps = 0usize;
    let mut seen_sigs: HashSet<(String, u16, u8)> = HashSet::new();
    let mut plateau_count = 0usize;

    'outer: loop {
        reset_all(&backends, now).await;
        now = T0;

        let sigs_before = seen_sigs.len();

        for _ in 0..BATCH_SIZE {
            if total_steps >= max_steps {
                break 'outer;
            }

            // Query oracle state to generate the next request, then release
            // the lock before dispatching so the oracle backend can reacquire
            // it.
            let (envelope, now_after) = {
                let o = oracle.lock();
                let op = pick_op(&mut rng, &o, &covered);
                build_envelope(op, &mut rng, &o, now)
            };
            now = now_after;
            total_steps += 1;

            let kind = envelope.kind.clone();
            let ctx = format!("step={total_steps} op={kind}");

            let pre_snaps = snap_all(&backends, now).await;
            assert_snaps_agree(&pre_snaps, &format!("BEFORE {ctx}"));

            let mut results = match &cut {
                Some(log) if total_steps.is_multiple_of(cut_every) && cuttable(&kind) => {
                    cut_step(
                        &node, log, &backends, &envelope, now, &mut rng, &mut cuts, &ctx,
                    )
                    .await
                }
                _ => send_all(&backends, &envelope, now).await,
            };
            for (_, _, data) in &mut results {
                normalize_resp(data);
            }
            if results.len() == 1 {
                // Cut, recovered, and found to have happened: only the state
                // is left to compare, and only the oracle's answer to count.
                if results[0].1 < 300 {
                    covered.entry(kind.clone()).or_insert(total_steps);
                }
                let post_snaps = snap_all(&backends, now).await;
                assert_snaps_agree(&post_snaps, &format!("AFTER {ctx} (cut, recovered)"));
                continue;
            }

            let (_, status, _) = &results[0];
            let status = *status;

            if status < 300 {
                covered.entry(kind.clone()).or_insert(total_steps);
            }

            let sc = state_class(&pre_snaps[0].1);
            seen_sigs.insert((kind.clone(), status as u16, sc));

            assert_resps_agree(&results, &ctx);

            let post_snaps = snap_all(&backends, now).await;
            assert_snaps_agree(&post_snaps, &format!("AFTER {ctx}"));
        }

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

    if cut.is_some() {
        let total: usize = cuts.shapes.values().sum();
        let partial: usize = cuts
            .shapes
            .iter()
            .filter(|((k, n), _)| *k > 0 && k < n)
            .map(|(_, c)| c)
            .sum();
        eprintln!(
            "[prefix] {total} commits cut, {partial} part-way; {} happened by recovery, {} retried; {} messages lost",
            cuts.applied, cuts.retried, cuts.lost
        );
        eprintln!("[prefix] (landed, of): count — {:?}", cuts.shapes);
        assert!(
            partial > 0,
            "no commit was cut part-way: the test proved nothing"
        );
    }

    eprintln!(
        "[diff] PASSED — {total_steps} steps, all {} ops covered, {} behavioral signatures",
        ALL_OPS.len(),
        seen_sigs.len(),
    );
}

// ---------------------------------------------------------------------------
// Drivers and comparisons
// ---------------------------------------------------------------------------

/// Steps that decide something and may commit. Reads commit nothing, so a
/// cut would stay armed; `debug.*` steps are the harness's own.
fn cuttable(kind: &str) -> bool {
    !kind.starts_with("debug.") && !kind.ends_with(".get") && !kind.ends_with(".search")
}

/// One step with its commit cut: see [`prefix_random`]. Returns the results
/// to compare, as [`send_all`] does.
#[allow(clippy::too_many_arguments)]
async fn cut_step(
    node: &Arc<Node>,
    log: &Arc<MemLog>,
    backends: &[(String, Backend)],
    envelope: &RequestEnvelope,
    now: i64,
    rng: &mut fastrand::Rng,
    cuts: &mut Cuts,
    ctx: &str,
) -> Vec<(String, i32, Value)> {
    let (_, oracle) = &backends[1];
    let fences = log.fences();
    // Half the cuts fall strictly inside the commit, whatever its length;
    // the rest land none, some or all of it, including "all, then lost".
    log.fail_next(if rng.bool() {
        Fault::CutWithin(rng.u64(..))
    } else {
        Fault::Cut(rng.usize(0..=4))
    });
    let mut env = envelope.clone();
    env.head.debug_time = Some(now);
    let first = node.process(&env).await;
    let oracle_resp = send(oracle, envelope, now).await;
    let oracle_result = (
        "oracle".to_string(),
        oracle_resp.head.status,
        oracle_resp.data,
    );
    if std::env::var("TEST_PREFIX_TRACE").is_ok() {
        eprintln!(
            "[prefix] {ctx}: first={:?}",
            first
                .as_ref()
                .map(|r| r.head.status)
                .map_err(|e| e.message.clone())
        );
    }
    let Err(_) = first else {
        // Nothing was committed, so nothing was cut.
        assert!(log.disarm(), "{ctx}: answered, yet the cut fired");
        let resp = first.unwrap();
        return vec![("kafka".into(), resp.head.status, resp.data), oracle_result];
    };
    let shape = log
        .take_cut()
        .unwrap_or_else(|| panic!("{ctx}: failed without a cut"));
    *cuts.shapes.entry(shape).or_default() += 1;
    if std::env::var("TEST_PREFIX_TRACE").is_ok() {
        eprintln!("[prefix] {ctx}: cut {shape:?}");
    }

    // Taken over again: fenced anew, the landed prefix replayed.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while log.fences() == fences || node.serving().len() < 8 {
        assert!(
            std::time::Instant::now() < deadline,
            "{ctx}: not taken over again"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    // Recovery at the same instant, on both sides.
    let tick = req("debug.tick", json!({ "time": now }));
    for (name, b) in backends {
        let r = send(b, &tick, now).await;
        assert_eq!(r.head.status, 200, "{ctx}: tick on {name}");
    }
    // The state: the snapshot without the messages (see
    // `reconcile_messages`), and the schedules, which it does not hold.
    let snaps = snap_all(backends, now).await;
    let mut states = Vec::new();
    for (i, (_, b)) in backends.iter().enumerate() {
        let mut v = snaps[i].1.clone();
        let o = v.as_object_mut().unwrap();
        o.remove("messages");
        o.insert("schedules".into(), schedules(b, now).await);
        states.push(v);
    }
    let state = |v: &Value| v.clone();
    let snaps: Vec<(String, Value)> = states.into_iter().map(|v| (String::new(), v)).collect();
    let out = if snaps[0].1 == snaps[1].1 {
        cuts.applied += 1;
        // The step happened; what it answered is lost with the commit.
        vec![oracle_result]
    } else {
        cuts.retried += 1;
        if std::env::var("TEST_PREFIX_TRACE").is_ok() {
            eprintln!(
                "[prefix] {ctx}: retry; kafka={} oracle={}",
                state(&snaps[0].1),
                state(&snaps[1].1)
            );
        }
        let again = send(&(Arc::clone(node) as Backend), envelope, now).await;
        vec![
            ("kafka".into(), again.head.status, again.data),
            oracle_result,
        ]
    };
    cuts.lost += reconcile_messages(node, backends, now, ctx).await;
    out
}

/// Every schedule a backend holds, by id.
async fn schedules(b: &Backend, now: i64) -> Value {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut data = json!({ "limit": 100 });
        if let Some(c) = &cursor {
            data["cursor"] = json!(c);
        }
        let r = send(b, &req("schedule.search", data), now).await;
        assert_eq!(r.head.status, 200, "schedule.search: {}", r.data);
        out.extend(r.data["schedules"].as_array().cloned().unwrap_or_default());
        match r.data["cursor"].as_str() {
            Some(c) if !c.is_empty() => cursor = Some(c.to_string()),
            _ => break,
        }
    }
    sort_by_id(&mut out);
    Value::Array(out)
}

/// What a held message is keyed by, as the sender and the oracle key it: an
/// execute by its task, an unblock by its promise and address.
fn message_key(m: &Value) -> (String, String, String) {
    let kind = m["message"]["kind"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let address = m["address"].as_str().unwrap_or_default().to_string();
    match kind.as_str() {
        "execute" => (
            kind,
            m["message"]["data"]["task"]["id"].to_string(),
            String::new(),
        ),
        _ => (
            kind,
            m["message"]["data"]["promise"]["id"].to_string(),
            address,
        ),
    }
}

/// Sends are made after a commit, at most once, on every backend: a commit
/// cut short loses its round's. Executes are made again by the retry timer;
/// unblocks are not. So after a cut the node may hold fewer messages than the
/// oracle — never one the oracle does not. Hand the node what it lost, so the
/// trajectory goes on comparing, and count it.
async fn reconcile_messages(
    node: &Arc<Node>,
    backends: &[(String, Backend)],
    now: i64,
    ctx: &str,
) -> usize {
    use resonate_core::types::{
        ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, PromiseRecord,
        UnblockMsg, UnblockMsgData, UnblockMsgHead,
    };
    let snaps = snap_all(backends, now).await;
    let msgs = |v: &Value| v["messages"].as_array().cloned().unwrap_or_default();
    let (ours, theirs) = (msgs(&snaps[0].1), msgs(&snaps[1].1));
    let keys: HashSet<_> = theirs.iter().map(message_key).collect();
    for m in &ours {
        assert!(
            keys.contains(&message_key(m)),
            "{ctx}: recovery sent what the oracle never did: {m}"
        );
    }
    let mut lost = 0;
    for m in theirs.iter().filter(|m| !ours.contains(m)) {
        lost += 1;
        let address = m["address"].as_str().unwrap();
        let body = &m["message"];
        let msg = match body["kind"].as_str() {
            Some("execute") => Message::Execute(ExecuteMsg {
                kind: "execute".into(),
                head: MessageHead {
                    server_url: body["head"]["serverUrl"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                },
                data: ExecuteMsgData {
                    task: ExecuteMsgTask {
                        id: body["data"]["task"]["id"].as_str().unwrap().into(),
                        version: body["data"]["task"]["version"].as_i64().unwrap(),
                    },
                },
            }),
            _ => Message::Unblock(UnblockMsg {
                kind: "unblock".into(),
                head: UnblockMsgHead {},
                data: UnblockMsgData {
                    promise: serde_json::from_value::<PromiseRecord>(
                        body["data"]["promise"].clone(),
                    )
                    .unwrap(),
                },
            }),
        };
        node.sender().dispatch(address, msg).await;
    }
    lost
}

async fn reset_all(backends: &[(String, Backend)], now: i64) {
    let envelope = req("debug.reset", json!({}));
    for (name, b) in backends {
        let resp = send(b, &envelope, now).await;
        assert_eq!(resp.head.status, 200, "debug.reset failed on {name}");
    }
}

async fn send_all(
    backends: &[(String, Backend)],
    envelope: &RequestEnvelope,
    now: i64,
) -> Vec<(String, i32, Value)> {
    let mut out = Vec::new();
    for (name, b) in backends {
        let resp = send(b, envelope, now).await;
        out.push((name.clone(), resp.head.status, resp.data));
    }
    out
}

/// The whole snapshot, messages included: both backends queue — the blob
/// sender holds messages under the debug flag and the oracle keeps
/// `outgoing` — so unlike the engine-level harness there is no
/// returned-vs-queued split.
async fn snap_all(backends: &[(String, Backend)], now: i64) -> Vec<(String, Value)> {
    let envelope = req("debug.snap", json!({}));
    let mut out = Vec::new();
    for (name, b) in backends {
        let resp = send(b, &envelope, now).await;
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
        // The body too, not just the code: an error response carries the reason
        // in `data`, and on a 500 that reason is the only thing that says which
        // constraint produced it.
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
                // No "id" field (e.g. callbacks have awaited/awaiter, listeners
                // have awaited/address) — fall back to full serialization for a
                // stable sort.
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
// Generators — identical in shape to diff/differential.rs, planning from this
// crate's own oracle copy.
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
    let awaited = pick_awaited(rng, oracle, &awaiter);
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

fn gen_task_fence(rng: &mut fastrand::Rng, oracle: &Oracle, now: i64) -> RequestEnvelope {
    let acquired = oracle.tasks_by_state(TaskState::Acquired);
    let (task_id, version) = pick(rng, &acquired).unwrap_or_else(|| (random_task_id(rng), 1));
    // Same-origin actions only: the blob server refuses a cross-origin fence
    // by design (one CAS, one document), where the oracle — carrying main's
    // semantics — accepts it. That deviation is deliberate and unit-tested in
    // server.rs; the differential stays inside the envelope both agree on.
    // Schedule-created promises (`sched-promise-*`) live in another origin,
    // so they are filtered out here.
    let pending_p: Vec<String> = oracle
        .pending_promise_ids()
        .into_iter()
        .filter(|p| p.split(':').next() == task_id.split(':').next())
        .collect();
    let do_settle = !pending_p.is_empty() && rng.u32(0..4) != 0;
    if !do_settle {
        let task_origin = task_id.split(':').next().unwrap().to_string();
        let new_promise_id = format!("{task_origin}:p{}", rng.u32(0..8));
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
            "promiseId": "sched-promise-{{.id}}-{{.timestamp}}".to_string(),
            "promiseTimeout": promise_timeout,
            "promiseParam": {},
            "promiseTags": { "resonate:target": WORKER_URL }
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
