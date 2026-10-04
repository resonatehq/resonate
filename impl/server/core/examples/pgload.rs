//! Load against the Postgres engine, with no HTTP in front of it.
//!
//! Each worker runs whole executions the way the TypeScript SDK drives them:
//! a root task is created and acquired, does two local steps (a fenced
//! `promise.create` and a fenced `promise.settle` each), spawns one remote
//! child, suspends on it; the child is acquired and fulfilled, which resumes
//! the root, and the root is acquired again and fulfilled. Eleven requests an
//! execution, every one a transition the server must make durable.
//!
//!   PG_URL=postgres://resonate:resonate@localhost:5432/resonate \
//!     cargo run --release --example pgload -- <workers> <seconds> [pool]
//!
//! It reports requests per second and per-operation latency percentiles. The
//! database is not reset: run it against a table that already holds a few
//! million rows to see what a full scan would cost.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use resonate_core::types::{RequestEnvelope, RequestHead, ResponseEnvelope, SUPPORTED_VERSIONS};
use resonate_server_postgres::PostgresEngine;
use resonate_sql::engine::{Engine, Input};
use serde_json::{json, Value};

const TARGET: &str = "poll://any@bench";
const PID: &str = "bench-pid";
const TTL: i64 = 60_000;

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: String::new(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

type Lat = Arc<Mutex<BTreeMap<&'static str, Vec<u32>>>>;

struct Ctx {
    engine: Arc<PostgresEngine>,
    lat: Vec<(&'static str, u32)>,
    errors: u64,
}

impl Ctx {
    async fn call(&mut self, kind: &'static str, data: Value, want: &[i32]) -> ResponseEnvelope {
        let r = req(kind, data);
        let t = Instant::now();
        let out = self.engine.process(Input::External(&r), now()).await;
        self.lat.push((kind, t.elapsed().as_micros() as u32));
        let resp = out.response.unwrap();
        if !want.contains(&resp.head.status) {
            self.errors += 1;
            if self.errors < 5 {
                eprintln!("{kind}: unexpected {} {}", resp.head.status, resp.data);
            }
        }
        resp
    }
}

async fn execution(c: &mut Ctx, root: &str) {
    let t0 = now();
    let timeout = t0 + 3_600_000;
    let r = c
        .call(
            "task.create",
            json!({ "pid": PID, "ttl": TTL, "action": { "kind": "promise.create", "head": {}, "data": {
                "id": root, "timeoutAt": timeout, "param": { "data": "aGVsbG8=" },
                "tags": { "resonate:target": TARGET, "resonate:scope": "global",
                          "resonate:branch": root, "resonate:parent": root, "resonate:origin": root }
            }}}),
            &[200],
        )
        .await;
    if r.head.status != 200 {
        return;
    }
    let mut v = 1;
    for i in 0..2 {
        let id = format!("{root}:{i}");
        c.call("task.fence", json!({ "id": root, "version": v, "action": { "kind": "promise.create", "head": {}, "data": {
            "id": id, "timeoutAt": timeout, "param": { "data": "aGVsbG8=" },
            "tags": { "resonate:scope": "local", "resonate:branch": root, "resonate:parent": root, "resonate:origin": root }
        }}}), &[200]).await;
        c.call("task.fence", json!({ "id": root, "version": v, "action": { "kind": "promise.settle", "head": {}, "data": {
            "id": id, "state": "resolved", "value": { "data": "d29ybGQ=" }
        }}}), &[200]).await;
    }
    let child = format!("{root}:2");
    c.call("task.fence", json!({ "id": root, "version": v, "action": { "kind": "promise.create", "head": {}, "data": {
        "id": child, "timeoutAt": timeout, "param": { "data": "aGVsbG8=" },
        "tags": { "resonate:scope": "global", "resonate:target": TARGET, "resonate:branch": child,
                  "resonate:parent": root, "resonate:origin": root }
    }}}), &[200]).await;
    c.call("task.suspend", json!({ "id": root, "version": v, "actions": [{
        "kind": "promise.register_callback", "head": {}, "data": { "awaited": child, "awaiter": root }
    }]}), &[200, 300]).await;
    c.call("task.acquire", json!({ "id": child, "version": 0, "pid": PID, "ttl": TTL }), &[200])
        .await;
    c.call("task.fulfill", json!({ "id": child, "version": 1, "action": { "kind": "promise.settle", "head": {}, "data": {
        "id": child, "state": "resolved", "value": { "data": "d29ybGQ=" }
    }}}), &[200]).await;
    c.call("task.acquire", json!({ "id": root, "version": v, "pid": PID, "ttl": TTL }), &[200])
        .await;
    v += 1;
    c.call("task.fulfill", json!({ "id": root, "version": v, "action": { "kind": "promise.settle", "head": {}, "data": {
        "id": root, "state": "resolved", "value": { "data": "d29ybGQ=" }
    }}}), &[200]).await;
    c.call("promise.get", json!({ "id": root }), &[200]).await;
}

fn pct(v: &mut [u32], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i] as f64 / 1000.0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let workers: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(32);
    let secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20);
    let pool: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(32);
    let url = std::env::var("PG_URL")
        .unwrap_or_else(|_| "postgres://resonate:resonate@localhost:5432/resonate".into());

    let engine = PostgresEngine::connect(&url, pool, 30_000, 10, false)
        .await
        .expect("connect");
    engine.init(true).await.expect("init");
    let engine = Arc::new(engine);

    let run = format!("b{}", now());
    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(AtomicU64::new(0));
    let lat: Lat = Arc::new(Mutex::new(BTreeMap::new()));
    let start = Instant::now();
    let mut handles = Vec::new();
    for w in 0..workers {
        let (engine, stop, done, errs, lat, run) = (
            engine.clone(),
            stop.clone(),
            done.clone(),
            errs.clone(),
            lat.clone(),
            run.clone(),
        );
        handles.push(tokio::spawn(async move {
            let mut c = Ctx {
                engine,
                lat: Vec::with_capacity(100_000),
                errors: 0,
            };
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let root = format!("{run}-{w}-{n}");
                execution(&mut c, &root).await;
                n += 1;
                done.fetch_add(1, Ordering::Relaxed);
            }
            errs.fetch_add(c.errors, Ordering::Relaxed);
            let mut l = lat.lock().unwrap();
            for (k, us) in c.lat {
                l.entry(k).or_default().push(us);
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64();
    let mut l = lat.lock().unwrap();
    let total: usize = l.values().map(|v| v.len()).sum();
    println!(
        "workers={workers} pool={pool} executions={} requests={total} errors={} \
         exec/s={:.0} req/s={:.0}",
        done.load(Ordering::Relaxed),
        errs.load(Ordering::Relaxed),
        done.load(Ordering::Relaxed) as f64 / elapsed,
        total as f64 / elapsed
    );
    println!(
        "{:<14} {:>8} {:>8} {:>8} {:>8}",
        "op", "n", "p50ms", "p99ms", "max"
    );
    let mut all = Vec::new();
    for (k, v) in l.iter_mut() {
        v.sort_unstable();
        all.extend_from_slice(v);
        println!(
            "{:<14} {:>8} {:>8.2} {:>8.2} {:>8.2}",
            k,
            v.len(),
            pct(v, 0.5),
            pct(v, 0.99),
            pct(v, 1.0)
        );
    }
    all.sort_unstable();
    println!(
        "{:<14} {:>8} {:>8.2} {:>8.2} {:>8.2}",
        "ALL",
        all.len(),
        pct(&mut all, 0.5),
        pct(&mut all, 0.99),
        pct(&mut all, 1.0)
    );
}
