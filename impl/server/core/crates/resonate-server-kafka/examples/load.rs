//! End-to-end load over HTTP, where the time went, and whether anything
//! acknowledged was lost.
//!
//! Two workloads, each on a fresh origin per operation, against the HTTP
//! gateways it is given (round-robin, as a load balancer would):
//!
//!   lifecycle  `promise.create`, then `promise.settle`.
//!   workflow   a root task that runs `--steps` local steps (a fenced create
//!              and settle each), dispatches a remote step to a push worker,
//!              suspends on it, and is resumed and completed by the worker —
//!              push delivery, callbacks and task acquisition, end to end.
//!              The push worker is this process: `--worker-bind` is where it
//!              listens, `--worker-url` where the server reaches it.
//!
//! Two ways to drive them:
//!
//!   closed  `--clients N`, one operation in flight per client. A slow server
//!           receives fewer requests, so its tail looks better than it is.
//!   open    `--rate R` operations per second on a fixed schedule, each
//!           measured from when it was due, not when it was sent.
//!
//! Every acknowledged write goes into a journal. After the run every
//! journalled promise is read back: a promise that is missing, or less
//! settled than acknowledged, is a lost write (`--journal` keeps it on disk,
//! `--verify-only` checks a kept one again later).
//!
//! It scrapes every node's `/metrics` before and after the measured window
//! and breaks the time down:
//!
//!   edge     resonate_request_duration_seconds          (HTTP gateway)
//!   server   resonate_server_request_duration_seconds   (server port)
//!   queue    resonate_kafka_queue_wait_seconds          (waiting for a round)
//!   round    resonate_kafka_round_seconds               (load, decide, commit, apply)
//!   commit   resonate_kafka_commit_seconds              (producing a round)
//!
//! `--timeline` writes one CSV row per second of the whole run (warm-up
//! included), and the last line printed is `RESULT {json}` for scripts.
//!
//!   cargo run --release -p resonate-server-kafka --example load -- \
//!     --url http://127.0.0.1:8101 --metrics http://127.0.0.1:9101/metrics \
//!     --clients 64 --seconds 20
//!
//!   cargo run --release -p resonate-server-kafka --example load -- \
//!     --url http://127.0.0.1:8101 --mode workflow --rate 500 \
//!     --worker-bind 127.0.0.1:7001 --worker-url http://127.0.0.1:7001/

use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use resonate_plugin::axum;
use serde_json::{json, Value};
use tokio::sync::{oneshot, Semaphore};

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Lifecycle,
    Workflow,
}

struct Args {
    urls: Vec<String>,
    metrics: Vec<String>,
    mode: Mode,
    clients: usize,
    rate: f64,
    seconds: u64,
    warmup: u64,
    payload: usize,
    steps: usize,
    worker_bind: String,
    worker_url: Option<String>,
    max_inflight: usize,
    retry_ms: u64,
    timeout_ms: u64,
    journal: Option<String>,
    verify: bool,
    verify_only: Option<String>,
    timeline: Option<String>,
}

fn args() -> Args {
    let mut a = Args {
        urls: vec!["http://127.0.0.1:8101".into()],
        metrics: vec![],
        mode: Mode::Lifecycle,
        clients: 64,
        rate: 0.0,
        seconds: 20,
        warmup: 3,
        payload: 256,
        steps: 3,
        worker_bind: "0.0.0.0:7001".into(),
        worker_url: None,
        max_inflight: 10_000,
        retry_ms: 0,
        timeout_ms: 10_000,
        journal: None,
        verify: true,
        verify_only: None,
        timeline: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_default();
        match flag.as_str() {
            "--url" => a.urls = v.split(',').map(String::from).collect(),
            "--metrics" => a.metrics = v.split(',').map(String::from).collect(),
            "--mode" => {
                a.mode = match v.as_str() {
                    "lifecycle" => Mode::Lifecycle,
                    "workflow" => Mode::Workflow,
                    other => panic!("unknown mode {other}"),
                }
            }
            "--clients" => a.clients = v.parse().expect("--clients"),
            "--rate" => a.rate = v.parse().expect("--rate"),
            "--seconds" => a.seconds = v.parse().expect("--seconds"),
            "--warmup" => a.warmup = v.parse().expect("--warmup"),
            "--payload" => a.payload = v.parse().expect("--payload"),
            "--steps" => a.steps = v.parse().expect("--steps"),
            "--worker-bind" => a.worker_bind = v,
            "--worker-url" => a.worker_url = Some(v),
            "--max-inflight" => a.max_inflight = v.parse().expect("--max-inflight"),
            "--retry-ms" => a.retry_ms = v.parse().expect("--retry-ms"),
            "--timeout-ms" => a.timeout_ms = v.parse().expect("--timeout-ms"),
            "--journal" => a.journal = Some(v),
            "--verify" => a.verify = v.parse().expect("--verify true|false"),
            "--verify-only" => a.verify_only = Some(v),
            "--timeline" => a.timeline = Some(v),
            other => panic!("unknown flag {other}"),
        }
    }
    a
}

// --- what gets recorded ---------------------------------------------------------

/// Request kinds, and `op` for a whole operation measured from when it was due.
const KINDS: &[&str] = &[
    "op",
    "promise.create",
    "promise.settle",
    "task.create",
    "task.fence",
    "task.suspend",
    "task.fulfill",
];

fn kind_ix(kind: &str) -> u8 {
    KINDS.iter().position(|k| *k == kind).expect("known kind") as u8
}

#[derive(Clone, Copy)]
struct Rec {
    /// When it was due, in ms since the run started.
    at_ms: u32,
    lat_us: u32,
    kind: u8,
    ok: bool,
}

/// The strongest state a promise was acknowledged in.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum Acked {
    Created,
    Resolved,
}

struct Shared {
    http: reqwest::Client,
    urls: Vec<String>,
    rr: AtomicUsize,
    run: String,
    data: String,
    steps: usize,
    worker_url: String,
    retry: Duration,
    t0: Instant,
    recs: Mutex<Vec<Rec>>,
    errors: Mutex<HashMap<String, u64>>,
    worker_errors: Mutex<HashMap<String, u64>>,
    journal: Mutex<Vec<(String, Acked)>>,
    /// Roots waiting for the worker to complete them.
    waiters: Mutex<HashMap<String, oneshot::Sender<()>>>,
}

impl Shared {
    fn record(&self, at: Instant, lat: Duration, kind: &str, ok: bool) {
        let rec = Rec {
            at_ms: at.saturating_duration_since(self.t0).as_millis() as u32,
            lat_us: lat.as_micros().min(u32::MAX as u128) as u32,
            kind: kind_ix(kind),
            ok,
        };
        self.recs.lock().unwrap().push(rec);
    }

    fn error(&self, e: String) {
        *self.errors.lock().unwrap().entry(e).or_default() += 1;
    }

    fn worker_error(&self, e: String) {
        *self.worker_errors.lock().unwrap().entry(e).or_default() += 1;
    }

    fn ack(&self, id: &str, state: Acked) {
        self.journal.lock().unwrap().push((id.to_string(), state));
    }

    fn url(&self) -> &str {
        &self.urls[self.rr.fetch_add(1, Ordering::Relaxed) % self.urls.len()]
    }

    /// One request. `Ok((status, data))` for any answer, `Err` for transport
    /// trouble. Retries 503s and transport errors for `--retry-ms`, each time
    /// at the next gateway.
    async fn send(&self, kind: &'static str, data: &Value) -> Result<(i64, Value), String> {
        let started = Instant::now();
        loop {
            let out = send(&self.http, self.url(), kind, data).await;
            let retryable = match &out {
                Ok((503, _)) => true,
                Ok(_) => false,
                Err(_) => true,
            };
            if retryable && started.elapsed() < self.retry {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            return out;
        }
    }

    /// [`Self::send`], recorded.
    async fn call(&self, kind: &'static str, data: Value) -> Result<(i64, Value), String> {
        let started = Instant::now();
        let out = self.send(kind, &data).await;
        let ok = matches!(out, Ok((s, _)) if s < 400);
        self.record(started, started.elapsed(), kind, ok);
        out
    }

    /// A call that must answer 200 (or one of `also`), else an error string.
    async fn expect(&self, kind: &'static str, data: Value, also: &[i64]) -> Result<(i64, Value), String> {
        match self.call(kind, data).await {
            Ok((s, v)) if s == 200 || also.contains(&s) => Ok((s, v)),
            Ok((s, _)) => Err(format!("{kind} {s}")),
            Err(e) => Err(format!("{kind} {e}")),
        }
    }
}

async fn send(http: &reqwest::Client, url: &str, kind: &str, data: &Value) -> Result<(i64, Value), String> {
    let body = json!({
        "kind": kind,
        "head": { "corrId": "load", "version": resonate_core::types::SUPPORTED_VERSIONS[0] },
        "data": data,
    });
    let resp = http.post(url).json(&body).send().await.map_err(|e| {
        if e.is_timeout() {
            "timeout".to_string()
        } else if e.is_connect() {
            "connect".to_string()
        } else {
            "transport".to_string()
        }
    })?;
    let v: Value = resp.json().await.map_err(|_| "decode".to_string())?;
    match v["head"]["status"].as_i64() {
        Some(s) => Ok((s, v["data"].clone())),
        None => Err("no status".into()),
    }
}

/// A fenced action's own status, inside the fence's answer.
fn inner(v: &Value) -> i64 {
    v["action"]["head"]["status"].as_i64().unwrap_or(0)
}

const FAR: i64 = 4_000_000_000_000;

// --- the workloads ------------------------------------------------------------

async fn lifecycle(s: &Shared, i: u64) -> Result<(), String> {
    let id = format!("load-{}-{i}", s.run);
    s.expect(
        "promise.create",
        json!({ "id": id, "timeoutAt": FAR, "param": { "data": s.data }, "tags": {} }),
        &[],
    )
    .await?;
    s.ack(&id, Acked::Created);
    s.expect(
        "promise.settle",
        json!({ "id": id, "state": "resolved", "value": { "data": s.data } }),
        &[],
    )
    .await?;
    s.ack(&id, Acked::Resolved);
    Ok(())
}

async fn fence(s: &Shared, root: &str, version: i64, action: Value) -> Result<(), String> {
    let kind = action["kind"].as_str().unwrap_or("?").to_string();
    let (_, v) = s
        .expect("task.fence", json!({ "id": root, "version": version, "action": action }), &[])
        .await?;
    match inner(&v) {
        200 => Ok(()),
        st => Err(format!("task.fence/{kind} {st}")),
    }
}

async fn workflow(s: &Shared, i: u64, timeout: Duration) -> Result<(), String> {
    let root = format!("wf-{}-{i}", s.run);
    let pid = format!("load-{}", s.run);
    let (_, v) = s
        .expect(
            "task.create",
            json!({ "pid": pid, "ttl": 60_000, "action": { "kind": "promise.create", "head": {},
                    "data": { "id": root, "timeoutAt": FAR, "param": { "data": s.data },
                              "tags": { "resonate:target": s.worker_url } } } }),
            &[],
        )
        .await?;
    s.ack(&root, Acked::Created);
    let version = v["task"]["version"].as_i64().ok_or("task.create: no version")?;

    for step in 1..=s.steps {
        let id = format!("{root}:{step}");
        fence(s, &root, version, json!({ "kind": "promise.create", "head": {},
            "data": { "id": id, "timeoutAt": FAR, "param": { "data": s.data }, "tags": {} } }))
        .await?;
        s.ack(&id, Acked::Created);
        fence(s, &root, version, json!({ "kind": "promise.settle", "head": {},
            "data": { "id": id, "state": "resolved", "value": { "data": s.data } } }))
        .await?;
        s.ack(&id, Acked::Resolved);
    }

    let remote = format!("{root}:r");
    let (tx, rx) = oneshot::channel();
    s.waiters.lock().unwrap().insert(root.clone(), tx);
    let out = async {
        fence(s, &root, version, json!({ "kind": "promise.create", "head": {},
            "data": { "id": remote, "timeoutAt": FAR, "param": { "data": s.data },
                      "tags": { "resonate:target": s.worker_url } } }))
        .await?;
        s.ack(&remote, Acked::Created);
        let (status, _) = s
            .expect(
                "task.suspend",
                json!({ "id": root, "version": version, "actions": [
                    { "kind": "promise.register_callback", "head": {},
                      "data": { "awaited": remote, "awaiter": root } } ] }),
                &[300],
            )
            .await?;
        if status == 300 {
            // The remote step finished first: carry on and complete the root here.
            s.waiters.lock().unwrap().remove(&root);
            s.expect("task.fulfill", fulfill(&root, version, &s.data), &[]).await?;
        } else {
            tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| "workflow: not completed in time".to_string())?
                .map_err(|_| "workflow: waiter dropped".to_string())?;
        }
        Ok::<(), String>(())
    }
    .await;
    s.waiters.lock().unwrap().remove(&root);
    out?;
    s.ack(&remote, Acked::Resolved);
    s.ack(&root, Acked::Resolved);
    Ok(())
}

fn fulfill(id: &str, version: i64, data: &str) -> Value {
    json!({ "id": id, "version": version, "action": { "kind": "promise.settle", "head": {},
            "data": { "id": id, "state": "resolved", "value": { "data": data } } } })
}

// --- the push worker ----------------------------------------------------------

/// An `execute`: acquire the task and fulfill it at once. A root it completes
/// wakes the operation waiting for it.
async fn execute(s: Arc<Shared>, msg: Value) {
    let (Some(id), Some(version)) = (
        msg["data"]["task"]["id"].as_str().map(String::from),
        msg["data"]["task"]["version"].as_i64(),
    ) else {
        s.worker_error(format!("malformed {}", msg["kind"]));
        return;
    };
    let pid = format!("worker-{}", s.run);
    let acquired = s
        .send(
            "task.acquire",
            &json!({ "id": id, "version": version, "pid": pid, "ttl": 60_000 }),
        )
        .await;
    let version = match acquired {
        Ok((200, v)) => v["task"]["version"].as_i64().unwrap_or(version),
        Ok((st, _)) => return s.worker_error(format!("task.acquire {st}")),
        Err(e) => return s.worker_error(format!("task.acquire {e}")),
    };
    match s.send("task.fulfill", &fulfill(&id, version, &s.data)).await {
        Ok((200, _)) => {
            if let Some(tx) = s.waiters.lock().unwrap().remove(&id) {
                let _ = tx.send(());
            }
        }
        Ok((st, _)) => s.worker_error(format!("task.fulfill {st}")),
        Err(e) => s.worker_error(format!("task.fulfill {e}")),
    }
}

async fn serve_worker(s: Arc<Shared>, bind: &str) {
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::post(
                |axum::extract::State(s): axum::extract::State<Arc<Shared>>,
                 axum::Json(msg): axum::Json<Value>| async move {
                    if msg["kind"] == "execute" {
                        tokio::spawn(execute(s, msg));
                    }
                    axum::http::StatusCode::OK
                },
            ),
        )
        .with_state(s);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .unwrap_or_else(|e| panic!("cannot bind worker on {bind}: {e}"));
    tokio::spawn(async move { axum::serve(listener, app).await });
}

// --- verification ---------------------------------------------------------------

#[derive(Default)]
struct Verdict {
    checked: u64,
    missing: u64,
    regressed: u64,
    unreadable: u64,
    examples: Vec<String>,
}

/// Read every journalled promise back. Missing, or less settled than
/// acknowledged, is a lost write.
async fn verify(http: &reqwest::Client, urls: &[String], journal: Vec<(String, Acked)>) -> Verdict {
    let mut want: HashMap<String, Acked> = HashMap::new();
    for (id, st) in journal {
        let e = want.entry(id).or_insert(st);
        if st > *e {
            *e = st;
        }
    }
    let verdict = Arc::new(Mutex::new(Verdict::default()));
    let sem = Arc::new(Semaphore::new(128));
    let mut handles = Vec::with_capacity(want.len());
    for (n, (id, st)) in want.into_iter().enumerate() {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let (http, url, verdict) = (http.clone(), urls[n % urls.len()].clone(), verdict.clone());
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let mut answer = Err(String::new());
            for _ in 0..20 {
                answer = send(&http, &url, "promise.get", &json!({ "id": id })).await;
                match answer {
                    Ok((s, _)) if s != 503 => break,
                    _ => tokio::time::sleep(Duration::from_millis(250)).await,
                }
            }
            let mut v = verdict.lock().unwrap();
            v.checked += 1;
            let bad = match answer {
                Ok((200, p)) => {
                    let state = p["promise"]["state"].as_str().unwrap_or("");
                    if st == Acked::Resolved && state != "resolved" {
                        v.regressed += 1;
                        Some(format!("{id}: acknowledged resolved, reads {state}"))
                    } else {
                        None
                    }
                }
                Ok((404, _)) => {
                    v.missing += 1;
                    Some(format!("{id}: acknowledged, reads 404"))
                }
                other => {
                    v.unreadable += 1;
                    Some(format!("{id}: unreadable {other:?}"))
                }
            };
            if let Some(b) = bad {
                if v.examples.len() < 10 {
                    v.examples.push(b);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    Arc::try_unwrap(verdict).ok().unwrap().into_inner().unwrap()
}

fn write_journal(path: &str, journal: &[(String, Acked)]) {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).expect("journal"));
    for (id, st) in journal {
        let st = match st {
            Acked::Created => "created",
            Acked::Resolved => "resolved",
        };
        writeln!(f, "{id}\t{st}").unwrap();
    }
}

fn read_journal(path: &str) -> Vec<(String, Acked)> {
    std::fs::read_to_string(path)
        .expect("journal")
        .lines()
        .filter_map(|l| {
            let (id, st) = l.split_once('\t')?;
            Some((id.to_string(), if st == "resolved" { Acked::Resolved } else { Acked::Created }))
        })
        .collect()
}

fn print_verdict(v: &Verdict) {
    println!(
        "\nverify: {} promises read back, {} missing, {} less settled than acknowledged, {} unreadable",
        v.checked, v.missing, v.regressed, v.unreadable
    );
    for e in &v.examples {
        println!("  {e}");
    }
}

// --- Prometheus text, just enough of it ---------------------------------------

type Sample = (String, HashMap<String, String>, f64);

fn parse(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (head, value) = match line.rsplit_once(' ') {
            Some(p) => p,
            None => continue,
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        let (name, labels) = match head.split_once('{') {
            Some((n, rest)) => {
                let mut labels = HashMap::new();
                for pair in rest.trim_end_matches('}').split("\",") {
                    if let Some((k, v)) = pair.split_once("=\"") {
                        labels.insert(k.to_string(), v.trim_end_matches('"').to_string());
                    }
                }
                (n.to_string(), labels)
            }
            None => (head.to_string(), HashMap::new()),
        };
        out.push((name, labels, value));
    }
    out
}

async fn scrape(http: &reqwest::Client, urls: &[String]) -> Vec<Sample> {
    let mut all = Vec::new();
    for url in urls {
        match http.get(url).send().await {
            Ok(r) => all.extend(parse(&r.text().await.unwrap_or_default())),
            Err(e) => eprintln!("scrape {url}: {e}"),
        }
    }
    all
}

/// Sum of a series over every label set matching `filter`.
fn total(samples: &[Sample], name: &str, filter: &dyn Fn(&HashMap<String, String>) -> bool) -> f64 {
    samples
        .iter()
        .filter(|(n, l, _)| n == name && filter(l))
        .map(|(_, _, v)| v)
        .sum()
}

/// A histogram's buckets (upper bound → cumulative count), summed over label
/// sets matching `filter`.
fn buckets(
    samples: &[Sample],
    name: &str,
    filter: &dyn Fn(&HashMap<String, String>) -> bool,
) -> Vec<(f64, f64)> {
    let mut by_le: HashMap<String, f64> = HashMap::new();
    for (n, l, v) in samples {
        if *n == format!("{name}_bucket") && filter(l) {
            *by_le.entry(l["le"].clone()).or_default() += v;
        }
    }
    let mut out: Vec<(f64, f64)> = by_le
        .into_iter()
        .map(|(le, c)| {
            (
                if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().unwrap()
                },
                c,
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    out
}

/// Mean, p50 and p99 of a histogram over the window between two scrapes.
fn window(
    before: &[Sample],
    after: &[Sample],
    name: &str,
    filter: &dyn Fn(&HashMap<String, String>) -> bool,
) -> Option<(f64, f64, f64, f64)> {
    let count = total(after, &format!("{name}_count"), filter)
        - total(before, &format!("{name}_count"), filter);
    if count <= 0.0 {
        return None;
    }
    let sum = total(after, &format!("{name}_sum"), filter)
        - total(before, &format!("{name}_sum"), filter);
    let b0: HashMap<String, f64> = buckets(before, name, filter)
        .into_iter()
        .map(|(le, c)| (le.to_string(), c))
        .collect();
    let delta: Vec<(f64, f64)> = buckets(after, name, filter)
        .into_iter()
        .map(|(le, c)| (le, c - b0.get(&le.to_string()).copied().unwrap_or(0.0)))
        .collect();
    let q = |q: f64| {
        let target = q * count;
        let mut prev = (0.0, 0.0);
        for &(le, c) in &delta {
            if c >= target {
                if le.is_infinite() {
                    return prev.0;
                }
                let span = (c - prev.1).max(1e-9);
                return prev.0 + (le - prev.0) * ((target - prev.1) / span);
            }
            prev = (le, c);
        }
        prev.0
    };
    Some((count, sum / count, q(0.5), q(0.99)))
}

fn ms(s: f64) -> String {
    format!("{:.2} ms", s * 1e3)
}

fn pct(sorted: &[u32], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i] as f64 / 1e3
}

fn write_timeline(path: &str, recs: &[Rec]) {
    let op = kind_ix("op");
    let mut by_sec: std::collections::BTreeMap<u32, (u64, u64, Vec<u32>)> = Default::default();
    for r in recs.iter().filter(|r| r.kind == op) {
        let e = by_sec.entry(r.at_ms / 1000).or_default();
        if r.ok {
            e.0 += 1;
            e.2.push(r.lat_us);
        } else {
            e.1 += 1;
        }
    }
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).expect("timeline"));
    writeln!(f, "second,ok,failed,p50_ms,p99_ms,max_ms").unwrap();
    for (sec, (ok, failed, mut lat)) in by_sec {
        lat.sort_unstable();
        writeln!(
            f,
            "{sec},{ok},{failed},{:.2},{:.2},{:.2}",
            pct(&lat, 0.5),
            pct(&lat, 0.99),
            pct(&lat, 1.0)
        )
        .unwrap();
    }
}

#[tokio::main]
async fn main() {
    let a = args();
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(1024)
        .timeout(Duration::from_millis(a.timeout_ms))
        .build()
        .unwrap();
    let urls: Vec<String> = a
        .urls
        .iter()
        .map(|u| format!("{}/", u.trim_end_matches('/')))
        .collect();

    if let Some(path) = &a.verify_only {
        let v = verify(&http, &urls, read_journal(path)).await;
        print_verdict(&v);
        println!(
            "RESULT {}",
            json!({ "checked": v.checked, "missing": v.missing, "regressed": v.regressed, "unreadable": v.unreadable })
        );
        return;
    }

    let run = format!("{:08x}", fastrand::u32(..));
    let s = Arc::new(Shared {
        http: http.clone(),
        urls: urls.clone(),
        rr: AtomicUsize::new(fastrand::usize(..)),
        run: run.clone(),
        data: "A".repeat(a.payload),
        steps: a.steps,
        worker_url: a
            .worker_url
            .clone()
            .unwrap_or_else(|| format!("http://{}/", a.worker_bind)),
        retry: Duration::from_millis(a.retry_ms),
        t0: Instant::now(),
        recs: Mutex::new(Vec::new()),
        errors: Mutex::new(HashMap::new()),
        worker_errors: Mutex::new(HashMap::new()),
        journal: Mutex::new(Vec::new()),
        waiters: Mutex::new(HashMap::new()),
    });
    if a.mode == Mode::Workflow {
        serve_worker(s.clone(), &a.worker_bind).await;
    }

    let op_timeout = Duration::from_millis(a.timeout_ms.max(30_000));
    let run_op = {
        let s = s.clone();
        let mode = a.mode;
        move |i: u64, due: Instant| {
            let s = s.clone();
            async move {
                let out = match mode {
                    Mode::Lifecycle => lifecycle(&s, i).await,
                    Mode::Workflow => workflow(&s, i, op_timeout).await,
                };
                let ok = out.is_ok();
                if let Err(e) = out {
                    s.error(e);
                }
                s.record(due, due.elapsed(), "op", ok);
            }
        }
    };

    let start = s.t0;
    let warm_end = start + Duration::from_secs(a.warmup);
    let end = warm_end + Duration::from_secs(a.seconds);
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    if a.rate > 0.0 {
        // Open loop: operation i is due at start + i/rate, whatever came before.
        let sem = Arc::new(Semaphore::new(a.max_inflight));
        let run_op = run_op.clone();
        handles.push(tokio::spawn(async move {
            let mut ops = Vec::new();
            let mut i = 0u64;
            loop {
                let due = start + Duration::from_secs_f64(i as f64 / a.rate);
                if due >= end {
                    break;
                }
                tokio::time::sleep_until(due.into()).await;
                let permit = sem.clone().acquire_owned().await.unwrap();
                let f = run_op(i, due);
                ops.push(tokio::spawn(async move {
                    f.await;
                    drop(permit);
                }));
                i += 1;
            }
            for o in ops {
                let _ = o.await;
            }
        }));
    } else {
        let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
        for _ in 0..a.clients {
            let (stop, next, run_op) = (stop.clone(), next.clone(), run_op.clone());
            handles.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    run_op(i, Instant::now()).await;
                }
            }));
        }
    }

    tokio::time::sleep_until(warm_end.into()).await;
    let before = scrape(&http, &a.metrics).await;
    tokio::time::sleep_until(end.into()).await;
    let after = scrape(&http, &a.metrics).await;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }
    let elapsed = a.seconds as f64;

    // Measured: operations due inside the window.
    let (w0, w1) = (a.warmup as u32 * 1000, (a.warmup + a.seconds) as u32 * 1000);
    let recs = std::mem::take(&mut *s.recs.lock().unwrap());
    let mut lat: HashMap<u8, Vec<u32>> = HashMap::new();
    let mut failed: HashMap<u8, u64> = HashMap::new();
    for r in recs.iter().filter(|r| r.at_ms >= w0 && r.at_ms < w1) {
        if r.ok {
            lat.entry(r.kind).or_default().push(r.lat_us);
        } else {
            *failed.entry(r.kind).or_default() += 1;
        }
    }
    for v in lat.values_mut() {
        v.sort_unstable();
    }
    let op = kind_ix("op");
    let ops_ok = lat.get(&op).map_or(0, |v| v.len());
    let ops_failed = failed.get(&op).copied().unwrap_or(0);
    let requests: usize = lat.iter().filter(|(k, _)| **k != op).map(|(_, v)| v.len()).sum();

    let driver = if a.rate > 0.0 {
        format!("open loop at {:.0} ops/s", a.rate)
    } else {
        format!("closed loop, {} clients", a.clients)
    };
    let mode = match a.mode {
        Mode::Lifecycle => "lifecycle".to_string(),
        Mode::Workflow => format!("workflow ({} local steps + 1 remote)", a.steps),
    };
    println!(
        "\n{mode}, {driver}, {} gateway(s), {:.0}s measured, {}-byte payloads",
        a.urls.len(),
        elapsed,
        a.payload
    );
    println!(
        "throughput: {:.0} ops/s completed, {:.0} requests/s from the client; {} ops failed",
        ops_ok as f64 / elapsed,
        requests as f64 / elapsed,
        ops_failed
    );
    println!("\nclient-side latency (op: from when it was due):");
    for (ix, kind) in KINDS.iter().enumerate() {
        let Some(v) = lat.get(&(ix as u8)) else { continue };
        println!(
            "  {kind:<15} n={:<8} p50 {:>8.2} ms  p99 {:>8.2} ms  p99.9 {:>8.2} ms  max {:>8.2} ms",
            v.len(),
            pct(v, 0.5),
            pct(v, 0.99),
            pct(v, 0.999),
            pct(v, 1.0)
        );
    }
    let errors = s.errors.lock().unwrap().clone();
    if !errors.is_empty() {
        println!("errors: {errors:?}");
    }
    let worker_errors = s.worker_errors.lock().unwrap().clone();
    if !worker_errors.is_empty() {
        println!("worker errors: {worker_errors:?}");
    }

    let mut breakdown = serde_json::Map::new();
    if !a.metrics.is_empty() {
        let any = |_: &HashMap<String, String>| true;
        println!("\nserver-side, all nodes (mean / p50 / p99):");
        for (label, name) in [
            ("edge", "resonate_request_duration_seconds"),
            ("server", "resonate_server_request_duration_seconds"),
            ("queue", "resonate_kafka_queue_wait_seconds"),
            ("round", "resonate_kafka_round_seconds"),
            ("commit", "resonate_kafka_commit_seconds"),
        ] {
            if let Some((n, mean, p50, p99)) = window(&before, &after, name, &any) {
                println!(
                    "  {label:<7} n={n:<9.0} {:>9} {:>9} {:>9}",
                    ms(mean),
                    ms(p50),
                    ms(p99)
                );
                breakdown.insert(
                    label.into(),
                    json!({ "mean_ms": mean * 1e3, "p50_ms": p50 * 1e3, "p99_ms": p99 * 1e3 }),
                );
            }
        }
        if let Some((n, mean, p50, p99)) =
            window(&before, &after, "resonate_kafka_round_requests", &any)
        {
            println!("  rounds  n={n:<9.0} {mean:>6.1} requests/round (p50 {p50:.0}, p99 {p99:.0})");
            breakdown.insert("requests_per_round".into(), json!(mean));
        }
        let delta = |name: &str, f: &dyn Fn(&HashMap<String, String>) -> bool| {
            total(&after, name, f) - total(&before, name, f)
        };
        let forwarded = delta("resonate_kafka_forwarded_total", &|l| {
            l.get("call").map(String::as_str) == Some("process")
        });
        let served = delta("resonate_server_requests_total", &any);
        let hits = delta("resonate_kafka_doc_cache_total", &|l| {
            l.get("result").map(String::as_str) == Some("hit")
        });
        let misses = delta("resonate_kafka_doc_cache_total", &|l| {
            l.get("result").map(String::as_str) == Some("miss")
        });
        println!(
            "  forwarded {:.0} of {:.0} requests reaching a server; doc cache {:.0}% hits",
            forwarded,
            served,
            100.0 * hits / (hits + misses).max(1.0)
        );
        breakdown.insert("forwarded".into(), json!(forwarded));
        breakdown.insert("served".into(), json!(served));
    }

    if let Some(path) = &a.timeline {
        write_timeline(path, &recs);
    }
    let journal = std::mem::take(&mut *s.journal.lock().unwrap());
    if let Some(path) = &a.journal {
        write_journal(path, &journal);
    }
    let verdict = if a.verify {
        let v = verify(&http, &urls, journal).await;
        print_verdict(&v);
        Some(v)
    } else {
        None
    };

    let op_lat = lat.get(&op).cloned().unwrap_or_default();
    println!(
        "RESULT {}",
        json!({
            "run": run,
            "mode": match a.mode { Mode::Lifecycle => "lifecycle", Mode::Workflow => "workflow" },
            "rate": a.rate, "clients": a.clients, "seconds": a.seconds, "payload": a.payload,
            "steps": a.steps, "gateways": a.urls.len(),
            "ops_per_s": ops_ok as f64 / elapsed,
            "requests_per_s": requests as f64 / elapsed,
            "ops_failed": ops_failed,
            "p50_ms": pct(&op_lat, 0.5), "p99_ms": pct(&op_lat, 0.99),
            "p999_ms": pct(&op_lat, 0.999), "max_ms": pct(&op_lat, 1.0),
            "errors": errors, "worker_errors": worker_errors,
            "server": breakdown,
            "verify": verdict.as_ref().map(|v| json!({ "checked": v.checked, "missing": v.missing,
                "regressed": v.regressed, "unreadable": v.unreadable })),
        })
    );
}
