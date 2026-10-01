//! End-to-end load over HTTP, and where the time went.
//!
//! Each client runs a promise's life — `promise.create`, then
//! `promise.settle` — on a fresh origin each time, against the HTTP gateways
//! it is given (round-robin, as a load balancer would). It measures what a
//! client sees, then scrapes every node's `/metrics` before and after the
//! measured window and breaks the time down:
//!
//!   edge     resonate_request_duration_seconds          (HTTP gateway)
//!   server   resonate_server_request_duration_seconds   (server port)
//!   queue    resonate_kafka_queue_wait_seconds          (waiting for a round)
//!   round    resonate_kafka_round_seconds               (load, decide, commit, apply)
//!   commit   resonate_kafka_commit_seconds              (producing a round)
//!
//!   cargo run --release -p resonate-server-kafka --example load -- \
//!     --url http://127.0.0.1:8101 --metrics http://127.0.0.1:9101/metrics \
//!     --clients 64 --seconds 20

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Args {
    urls: Vec<String>,
    metrics: Vec<String>,
    clients: usize,
    seconds: u64,
    warmup: u64,
    payload: usize,
}

fn args() -> Args {
    let mut a = Args {
        urls: vec!["http://127.0.0.1:8101".into()],
        metrics: vec![],
        clients: 64,
        seconds: 20,
        warmup: 3,
        payload: 256,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_default();
        match flag.as_str() {
            "--url" => a.urls = v.split(',').map(String::from).collect(),
            "--metrics" => a.metrics = v.split(',').map(String::from).collect(),
            "--clients" => a.clients = v.parse().expect("--clients"),
            "--seconds" => a.seconds = v.parse().expect("--seconds"),
            "--warmup" => a.warmup = v.parse().expect("--warmup"),
            "--payload" => a.payload = v.parse().expect("--payload"),
            other => panic!("unknown flag {other}"),
        }
    }
    a
}

#[derive(Default)]
struct Tally {
    /// Microseconds, per kind.
    latencies: HashMap<&'static str, Vec<u32>>,
    errors: HashMap<String, u64>,
}

async fn call(
    http: &reqwest::Client,
    url: &str,
    kind: &'static str,
    data: Value,
) -> Result<(), String> {
    let body = json!({
        "kind": kind,
        "head": { "corrId": "load", "version": resonate_core::types::SUPPORTED_VERSIONS[0] },
        "data": data,
    });
    let resp = http
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("transport: {e}"))?;
    let v: Value = resp.json().await.map_err(|e| format!("decode: {e}"))?;
    match v["head"]["status"].as_i64() {
        Some(200) => Ok(()),
        Some(s) => Err(format!("{kind} {s}")),
        None => Err(format!("{kind}: no status")),
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

#[tokio::main]
async fn main() {
    let a = args();
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(a.clients)
        .build()
        .unwrap();
    let run = format!("{:08x}", fastrand::u32(..));
    let data = "A".repeat(a.payload);

    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for c in 0..a.clients {
        let (http, stop, measuring) = (http.clone(), stop.clone(), measuring.clone());
        let url = format!("{}/", a.urls[c % a.urls.len()].trim_end_matches('/'));
        let (run, data) = (run.clone(), data.clone());
        handles.push(tokio::spawn(async move {
            let mut tally = Tally::default();
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let id = format!("load-{run}-{c}-{n}");
                let steps: [(&'static str, Value); 2] = [
                    (
                        "promise.create",
                        json!({ "id": id, "timeoutAt": 4_000_000_000_000i64,
                                "param": { "data": data }, "tags": {} }),
                    ),
                    (
                        "promise.settle",
                        json!({ "id": id, "state": "resolved", "value": { "data": data } }),
                    ),
                ];
                for (kind, body) in steps {
                    let t = Instant::now();
                    let out = call(&http, &url, kind, body).await;
                    if measuring.load(Ordering::Relaxed) {
                        match out {
                            Ok(()) => tally
                                .latencies
                                .entry(kind)
                                .or_default()
                                .push(t.elapsed().as_micros() as u32),
                            Err(e) => *tally.errors.entry(e).or_default() += 1,
                        }
                    }
                }
            }
            tally
        }));
    }

    tokio::time::sleep(Duration::from_secs(a.warmup)).await;
    let before = scrape(&http, &a.metrics).await;
    measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(a.seconds)).await;
    measuring.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    let after = scrape(&http, &a.metrics).await;
    stop.store(true, Ordering::Relaxed);

    let mut all: HashMap<&'static str, Vec<u32>> = HashMap::new();
    let mut errors: HashMap<String, u64> = HashMap::new();
    for h in handles {
        let t = h.await.unwrap();
        for (k, v) in t.latencies {
            all.entry(k).or_default().extend(v);
        }
        for (k, v) in t.errors {
            *errors.entry(k).or_default() += v;
        }
    }

    let total_ok: usize = all.values().map(|v| v.len()).sum();
    println!(
        "\n{} clients, {} node(s), {:.0}s measured, {}-byte payloads",
        a.clients,
        a.urls.len(),
        elapsed,
        a.payload
    );
    println!(
        "throughput: {:.0} requests/s ({:.0} promise lifecycles/s)",
        total_ok as f64 / elapsed,
        total_ok as f64 / elapsed / 2.0
    );
    println!("\nclient-side latency:");
    for kind in ["promise.create", "promise.settle"] {
        let mut v = all.remove(kind).unwrap_or_default();
        v.sort_unstable();
        println!(
            "  {kind:<16} n={:<8} p50 {:>7.2} ms  p90 {:>7.2} ms  p99 {:>7.2} ms  max {:>7.2} ms",
            v.len(),
            pct(&v, 0.5),
            pct(&v, 0.9),
            pct(&v, 0.99),
            pct(&v, 1.0)
        );
    }
    if !errors.is_empty() {
        println!("errors: {errors:?}");
    }

    if a.metrics.is_empty() {
        return;
    }
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
        }
    }
    if let Some((n, mean, p50, p99)) =
        window(&before, &after, "resonate_kafka_round_requests", &any)
    {
        println!("  rounds  n={n:<9.0} {mean:>6.1} requests/round (p50 {p50:.0}, p99 {p99:.0})");
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
}
