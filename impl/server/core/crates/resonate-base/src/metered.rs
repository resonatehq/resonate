//! Metering at the ports: every request a server answers, every message a
//! worker takes, and every plugin's lifecycle — measured once, here, for every
//! plugin in the binary.
//!
//! # Why at the ports
//!
//! Every request reaches the server through `ResonateServer::process`,
//! whatever brought it — the HTTP gateway, the web console, a worker calling
//! back in process — and every message leaves through
//! `ResonateWorker::process`. Both carry exactly what is worth labelling by.
//! So the composition root wraps the chosen server in a [`MeteredServer`] and
//! each worker in a [`MeteredWorker`] as it builds them, and no plugin has to
//! measure itself to be measured. What stays plugin-specific is only what the
//! ports cannot see: work a plugin starts on its own (timers, sweeps) and its
//! internals (commits, caches).
//!
//! The HTTP gateway keeps its own `resonate_request_*`: those are the edge —
//! HTTP, auth and serialization included — and the gap between the two is
//! what the edge costs.
//!
//! # Metrics
//!
//! | metric | labels | |
//! |---|---|---|
//! | `resonate_server_requests_total` | `kind`, `status` | `status` is the response code, or `unavailable` when there is no answer |
//! | `resonate_server_request_duration_seconds` | `kind`, `status` | |
//! | `resonate_worker_messages_total` | `worker`, `kind`, `outcome` | `outcome`: `delivered`, `unroutable`, `failed` |
//! | `resonate_worker_message_duration_seconds` | `worker`, `kind` | |
//! | `resonate_plugin_init_duration_seconds` | `plugin`, `role` | |
//! | `resonate_plugin_init_failures_total` | `plugin`, `role` | |
//! | `resonate_plugin_up` | `plugin`, `role` | 1 from a successful `init` to `stop` |
//! | `resonate_server_ready` | | the last answer to `ready()` |
//!
//! `kind` is one of the protocol's operations; anything else is counted as
//! `other`, so a client cannot mint label values.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lazy_static::lazy_static;
use resonate_plugin::prometheus::{
    exponential_buckets, register_counter_vec, register_gauge, register_histogram_vec,
    register_int_gauge_vec, CounterVec, Gauge, HistogramVec, IntGaugeVec,
};

use resonate_core::types::{Message, RequestEnvelope, ResponseEnvelope};
use resonate_core::{Cause, ResonateGateway, ResonateServer, ResonateWorker, Unavailable};

/// Latency buckets from 100µs to about 13s, doubling: fine enough for an
/// in-memory request and wide enough for a slow commit.
fn latency_buckets() -> Vec<f64> {
    exponential_buckets(0.000_1, 2.0, 18).expect("valid buckets")
}

lazy_static! {
    static ref SERVER_REQUESTS: CounterVec = register_counter_vec!(
        "resonate_server_requests_total",
        "Requests answered by the server, by kind and status",
        &["kind", "status"]
    )
    .unwrap();
    static ref SERVER_DURATION: HistogramVec = register_histogram_vec!(
        "resonate_server_request_duration_seconds",
        "Time the server took to answer, by kind and status",
        &["kind", "status"],
        latency_buckets()
    )
    .unwrap();
    static ref SERVER_READY: Gauge = register_gauge!(
        "resonate_server_ready",
        "1 if the server last reported ready, else 0"
    )
    .unwrap();
    static ref WORKER_MESSAGES: CounterVec = register_counter_vec!(
        "resonate_worker_messages_total",
        "Messages handed to a worker, by worker, kind and outcome",
        &["worker", "kind", "outcome"]
    )
    .unwrap();
    static ref WORKER_DURATION: HistogramVec = register_histogram_vec!(
        "resonate_worker_message_duration_seconds",
        "Time a worker took to take a message, by worker and kind",
        &["worker", "kind"],
        latency_buckets()
    )
    .unwrap();
    static ref PLUGIN_INIT: HistogramVec = register_histogram_vec!(
        "resonate_plugin_init_duration_seconds",
        "Time a plugin's init took",
        &["plugin", "role"],
        latency_buckets()
    )
    .unwrap();
    static ref PLUGIN_INIT_FAILURES: CounterVec = register_counter_vec!(
        "resonate_plugin_init_failures_total",
        "Plugin inits that failed",
        &["plugin", "role"]
    )
    .unwrap();
    static ref PLUGIN_UP: IntGaugeVec = register_int_gauge_vec!(
        "resonate_plugin_up",
        "1 from a plugin's successful init until its stop",
        &["plugin", "role"]
    )
    .unwrap();
}

/// The protocol's operations, the only values `kind` takes.
const KINDS: &[&str] = &[
    "promise.get",
    "promise.create",
    "promise.settle",
    "promise.register_callback",
    "promise.register_listener",
    "promise.search",
    "task.get",
    "task.create",
    "task.acquire",
    "task.release",
    "task.fulfill",
    "task.suspend",
    "task.fence",
    "task.heartbeat",
    "task.halt",
    "task.continue",
    "task.search",
    "schedule.get",
    "schedule.create",
    "schedule.delete",
    "schedule.search",
    "debug.reset",
    "debug.snap",
    "debug.tick",
];

/// The label a request kind is counted under.
pub fn kind_label(kind: &str) -> &str {
    KINDS
        .iter()
        .chain(resonate_core::ui::KINDS)
        .find(|k| **k == kind)
        .copied()
        .unwrap_or("other")
}

fn message_kind(msg: &Message) -> &'static str {
    match msg {
        Message::Execute(_) => "execute",
        Message::Unblock(_) => "unblock",
    }
}

/// Run a plugin's `init`, measured.
async fn metered_init(
    plugin: &str,
    role: &str,
    init: impl std::future::Future<Output = Result<(), Unavailable>>,
) -> Result<(), Unavailable> {
    let start = Instant::now();
    let out = init.await;
    PLUGIN_INIT
        .with_label_values(&[plugin, role])
        .observe(start.elapsed().as_secs_f64());
    match &out {
        Ok(()) => PLUGIN_UP.with_label_values(&[plugin, role]).set(1),
        Err(_) => PLUGIN_INIT_FAILURES
            .with_label_values(&[plugin, role])
            .inc(),
    }
    out
}

fn mark_down(plugin: &str, role: &str) {
    PLUGIN_UP.with_label_values(&[plugin, role]).set(0);
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// A server, measured.
pub struct MeteredServer {
    plugin: String,
    inner: Arc<dyn ResonateServer>,
}

impl MeteredServer {
    pub fn new(plugin: impl Into<String>, inner: Arc<dyn ResonateServer>) -> Self {
        Self {
            plugin: plugin.into(),
            inner,
        }
    }
}

#[async_trait]
impl ResonateServer for MeteredServer {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        metered_init(&self.plugin, "server", self.inner.init(debug)).await
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        let out = self.inner.stop().await;
        mark_down(&self.plugin, "server");
        out
    }

    async fn ready(&self) -> bool {
        let ready = self.inner.ready().await;
        SERVER_READY.set(if ready { 1.0 } else { 0.0 });
        ready
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        let start = Instant::now();
        let out = self.inner.process(req).await;
        let elapsed = start.elapsed();
        let kind = kind_label(&req.kind);
        let status = match &out {
            Ok(resp) => resp.head.status.to_string(),
            Err(_) => "unavailable".to_string(),
        };
        SERVER_REQUESTS.with_label_values(&[kind, &status]).inc();
        SERVER_DURATION
            .with_label_values(&[kind, &status])
            .observe(elapsed.as_secs_f64());
        tracing::debug!(
            kind = %req.kind,
            corr_id = %req.head.corr_id,
            status = %status,
            elapsed_us = elapsed.as_micros() as u64,
            "Request processed"
        );
        out
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// A worker, measured.
pub struct MeteredWorker {
    plugin: String,
    inner: Arc<dyn ResonateWorker>,
}

impl MeteredWorker {
    pub fn new(plugin: impl Into<String>, inner: Arc<dyn ResonateWorker>) -> Self {
        Self {
            plugin: plugin.into(),
            inner,
        }
    }
}

#[async_trait]
impl ResonateWorker for MeteredWorker {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        metered_init(&self.plugin, "worker", self.inner.init(debug)).await
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        let out = self.inner.stop().await;
        mark_down(&self.plugin, "worker");
        out
    }

    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let start = Instant::now();
        let out = self.inner.process(address, msg).await;
        let kind = message_kind(msg);
        let outcome = match &out {
            Ok(()) => "delivered",
            Err(e) if e.cause == Cause::Unroutable => "unroutable",
            Err(_) => "failed",
        };
        WORKER_MESSAGES
            .with_label_values(&[&self.plugin, kind, outcome])
            .inc();
        WORKER_DURATION
            .with_label_values(&[&self.plugin, kind])
            .observe(start.elapsed().as_secs_f64());
        out
    }
}

// ---------------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------------

/// A gateway, measured. Lifecycle only: a gateway's traffic reaches the
/// server, where it is measured already.
pub struct MeteredGateway {
    plugin: String,
    inner: Arc<dyn ResonateGateway>,
}

impl MeteredGateway {
    pub fn new(plugin: impl Into<String>, inner: Arc<dyn ResonateGateway>) -> Self {
        Self {
            plugin: plugin.into(),
            inner,
        }
    }
}

#[async_trait]
impl ResonateGateway for MeteredGateway {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        metered_init(&self.plugin, "gateway", self.inner.init(debug)).await
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        let out = self.inner.stop().await;
        mark_down(&self.plugin, "gateway");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::RequestHead;
    use resonate_plugin::prometheus;

    struct Echo;

    #[async_trait]
    impl ResonateServer for Echo {
        async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
            if req.kind == "promise.settle" {
                return Err(Unavailable::new("down"));
            }
            Ok(ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                404,
                serde_json::json!("nope"),
            ))
        }
    }

    fn req(kind: &str) -> RequestEnvelope {
        RequestEnvelope {
            kind: kind.into(),
            head: RequestHead {
                corr_id: "1".into(),
                version: "v".into(),
                auth: None,
                debug_time: None,
            },
            data: serde_json::json!({}),
        }
    }

    fn count(kind: &str, status: &str) -> f64 {
        SERVER_REQUESTS.with_label_values(&[kind, status]).get()
    }

    #[tokio::test]
    async fn every_answer_and_every_missing_answer_is_counted() {
        let s = MeteredServer::new("echo", Arc::new(Echo));
        let (get, settle, other) = (
            count("promise.get", "404"),
            count("promise.settle", "unavailable"),
            count("other", "404"),
        );
        s.process(&req("promise.get")).await.unwrap();
        assert!(s.process(&req("promise.settle")).await.is_err());
        s.process(&req("made.up.kind")).await.unwrap();
        assert_eq!(count("promise.get", "404"), get + 1.0);
        assert_eq!(count("promise.settle", "unavailable"), settle + 1.0);
        assert_eq!(
            count("other", "404"),
            other + 1.0,
            "unknown kinds fold into one label"
        );

        let families = prometheus::gather();
        assert!(families
            .iter()
            .any(|f| f.get_name() == "resonate_server_request_duration_seconds"));
    }

    #[test]
    fn a_kind_label_is_a_known_kind_or_other() {
        assert_eq!(kind_label("task.fence"), "task.fence");
        assert_eq!(kind_label("ui.executions.search"), "ui.executions.search");
        assert_eq!(kind_label("x".repeat(1000).as_str()), "other");
    }
}
