//! Record a CONCURRENT history of a live server for the specification's
//! porcupine checker.
//!
//!   RESONATE_DEBUG=true resonate serve &
//!   cargo run --release --example conctrace -- --url http://127.0.0.1:8001/ --out run
//!   go run ./cmd/conccheck -partition=false < run.history
//!
//! ## Which checker, and why it matters
//!
//! `valid/porc` has two. `lincheck` reads the `.ndjson` and asks whether ONE
//! order — the order the harness recorded — satisfies the model; on a
//! concurrent run it refutes almost anything, because return order is only one
//! of many legal linearizations. `conccheck` reads this `.history`, with real
//! call/return instants per operation, and asks whether ANY order consistent
//! with them works. Only the second is a linearizability check of a concurrent
//! run, and only its refutation is a claim about the server.
//!
//! So this writes the history: one line per operation, carrying the nanosecond
//! interval it actually occupied.
//!
//! ## Why not the specification's own `loadgen`
//!
//! Two things there do not fit this server, neither of them the checker:
//! `loadgen` opens with `debug.start`, which is a startup flag here and
//! answers 400; and it builds ids like `c0.a0` because `originOf` splits on
//! '.', where this server's origin is everything before the first ':'
//! (`resonate_core::types::origin`, and Postgres's `split_part(id, ':', 1)`).
//! Ids here are `c0:a0`, which is also why the checker runs with
//! `-partition=false`: upstream `originOf` sees no '.' and reads every id as
//! its own partition. Unpartitioned is the stronger check regardless — it
//! replays against whole state rather than per-origin slices.
//!
//! ## The id space
//!
//! Two origins, three suffixes: twelve promises, shared by every client. An
//! unbounded space names mostly-absent promises and linearizes on 404s, which
//! proves nothing. Bounded, the same run produces version conflicts and
//! resume-now responses — the states worth searching an order for.

use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const VERSION: &str = "2026-04-01";

struct Args {
    url: String,
    out: String,
    clients: usize,
    ops: u64,
    seed: u64,
    batch: u64,
}

fn parse_args() -> Args {
    let mut a = Args {
        url: "http://127.0.0.1:8001/".into(),
        out: "run".into(),
        clients: 8,
        ops: 600,
        seed: 1,
        batch: 4,
    };
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i + 1 < argv.len() {
        let v = argv[i + 1].clone();
        match argv[i].as_str() {
            "--url" => a.url = v,
            "--out" => a.out = v,
            "--clients" => a.clients = v.parse().expect("clients"),
            "--ops" => a.ops = v.parse().expect("ops"),
            "--seed" => a.seed = v.parse().expect("seed"),
            "--batch" => a.batch = v.parse().expect("batch"),
            other => panic!("unknown flag {other}"),
        }
        i += 2;
    }
    a
}

/// Hands out debug instants. Operations in a batch share one, so concurrency
/// is expressible under the model's non-decreasing clock.
struct Clock {
    now: Mutex<u64>,
    n: AtomicU64,
    batch: u64,
}

impl Clock {
    fn next(&self, rng: &mut fastrand::Rng) -> u64 {
        let n = self.n.fetch_add(1, Ordering::SeqCst) + 1;
        let mut now = self.now.lock().unwrap();
        if n.is_multiple_of(self.batch) {
            *now += 10 + rng.u64(0..40);
        }
        *now
    }
}

struct Row {
    kind: String,
    now: u64,
    req: Value,
    res: Value,
    call: i64,
    ret: i64,
    client: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();
    let client = reqwest::Client::new();
    // Debug mode is a startup flag, so there is no mode to enter — only state
    // to clear, in case this is not the first trace of a session.
    let _ = post(&client, &args.url, "debug.reset", json!({}), None).await;

    let clock = Arc::new(Clock {
        now: Mutex::new(1000),
        n: AtomicU64::new(0),
        batch: args.batch,
    });
    let rows: Arc<Mutex<Vec<Row>>> = Arc::new(Mutex::new(Vec::new()));
    let seq = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let mut handles = Vec::new();
    for cid in 0..args.clients {
        let (client, url, clock, rows, seq) = (
            client.clone(),
            args.url.clone(),
            Arc::clone(&clock),
            Arc::clone(&rows),
            Arc::clone(&seq),
        );
        let (ops, seed) = (args.ops, args.seed);
        handles.push(tokio::spawn(async move {
            let mut rng = fastrand::Rng::with_seed(seed * 1000 + cid as u64);
            loop {
                let i = seq.fetch_add(1, Ordering::SeqCst) + 1;
                if i > ops {
                    return;
                }
                let (kind, data) = next_op(&mut rng, cid, i);
                let now = clock.next(&mut rng);
                let call = t0.elapsed().as_nanos() as i64;
                let res = post(&client, &url, &kind, data.clone(), Some(now)).await;
                let ret = t0.elapsed().as_nanos() as i64;
                if let Some(res) = res {
                    rows.lock().unwrap().push(Row {
                        kind,
                        now,
                        req: data,
                        res,
                        call,
                        ret,
                        client: cid,
                    });
                }
            }
        }));
    }
    for h in handles {
        h.await.expect("client task");
    }

    let mut rows = Arc::try_unwrap(rows).ok().unwrap().into_inner().unwrap();
    rows.sort_by_key(|r| (r.now, r.ret));

    let mut history = String::new();
    let mut ndjson = String::new();
    for r in &rows {
        history.push_str(
            &json!({ "kind": r.kind, "now": r.now, "req": r.req, "res": r.res,
                     "call": r.call, "return": r.ret, "client": r.client })
            .to_string(),
        );
        history.push('\n');
        ndjson.push_str(
            &json!({ "kind": r.kind, "now": r.now, "req": r.req, "res": r.res }).to_string(),
        );
        ndjson.push('\n');
    }
    std::fs::write(format!("{}.history", args.out), history).expect("write history");
    std::fs::write(format!("{}.ndjson", args.out), ndjson).expect("write ndjson");

    // A run that intended to overlap and did not proves nothing, so it is
    // measured rather than assumed.
    let (mut pairs, mut max) = (0usize, 0usize);
    for (i, a) in rows.iter().enumerate() {
        let mut live = 1;
        for b in rows.iter().skip(i + 1) {
            if b.call < a.ret {
                pairs += 1;
                live += 1;
            }
        }
        max = max.max(live);
    }
    let ok = rows
        .iter()
        .filter(|r| r.res["head"]["status"].as_i64() == Some(200))
        .count();
    println!(
        "{} events ({ok} of them 200) from {} clients",
        rows.len(),
        args.clients
    );
    println!("  overlapping pairs: {pairs}   max concurrency: {max}");
    println!("  wrote {}.history and {}.ndjson", args.out, args.out);

    if ok == 0 {
        eprintln!("  FAIL: nothing succeeded — the history says nothing about linearizability");
        std::process::exit(1);
    }
    if max < 2 {
        eprintln!("  FAIL: nothing overlapped — this is a sequential run");
        std::process::exit(1);
    }
}

async fn post(
    client: &reqwest::Client,
    url: &str,
    kind: &str,
    data: Value,
    now: Option<u64>,
) -> Option<Value> {
    let mut head = json!({ "corrId": "conctrace", "version": VERSION });
    if let Some(now) = now {
        head["resonate:debug_time"] = json!(now);
    }
    let body = json!({ "kind": kind, "head": head, "data": data });
    let resp = client.post(url).json(&body).send().await.ok()?;
    resp.json::<Value>().await.ok()
}

/// One request, over a bounded id space: two origins, three suffixes. Every id
/// a client builds is `cN:something`, prefixed by the origin the server
/// derives from it.
fn next_op(rng: &mut fastrand::Rng, cid: usize, i: u64) -> (String, Value) {
    let origin = format!("c{}", cid % 2);
    let wf = (i / 6) % 3;
    let a = format!("{origin}:a{wf}");
    let x = format!("{origin}:x{wf}");
    let ext = json!({ "resonate:external": "true", "resonate:origin": origin });
    let tgt = json!({ "resonate:target": "poll://any@w1", "resonate:origin": origin });

    match rng.u32(0..12) {
        0 => (
            "promise.create".into(),
            json!({ "id": a, "timeoutAt": 900000, "param": {}, "tags": ext }),
        ),
        1 => (
            "promise.create".into(),
            json!({ "id": x, "timeoutAt": 900000, "param": {}, "tags": tgt }),
        ),
        2 | 3 => ("task.get".into(), json!({ "id": x })),
        4 => (
            "task.acquire".into(),
            json!({ "id": x, "version": rng.u32(0..3), "pid": format!("p{cid}"), "ttl": 60000 }),
        ),
        5 => (
            "task.suspend".into(),
            json!({ "id": x, "version": rng.u32(0..3), "actions": [
                { "kind": "promise.register_callback", "head": {},
                  "data": { "awaited": a, "awaiter": x } }]}),
        ),
        6 => (
            "promise.settle".into(),
            json!({ "id": a, "state": "resolved", "value": {} }),
        ),
        7 => (
            "task.fulfill".into(),
            json!({ "id": x, "version": rng.u32(0..4), "action": {
                "kind": "promise.settle", "head": {},
                "data": { "id": x, "state": "resolved", "value": {} } } }),
        ),
        8 | 9 => {
            let id = if rng.bool() { a } else { x };
            ("promise.get".into(), json!({ "id": id }))
        }
        10 => (
            "task.release".into(),
            json!({ "id": x, "version": rng.u32(0..4) }),
        ),
        _ => (
            "promise.register_listener".into(),
            json!({ "awaited": a, "address": "poll://any@w1" }),
        ),
    }
}
