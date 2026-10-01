//! What only this backend can see.
//!
//! Requests, their kinds, statuses and latencies are measured at the server
//! port for every backend (`resonate_server_*`, in `resonate-base`). These are
//! the internals behind them — where a request's time goes inside a node:
//!
//! | metric | |
//! |---|---|
//! | `resonate_kafka_queue_wait_seconds` | from a request reaching its partition to its round starting |
//! | `resonate_kafka_round_seconds` | a whole round: load, decide, commit, apply |
//! | `resonate_kafka_round_requests` | requests decided per round — the group commit at work |
//! | `resonate_kafka_commit_seconds` | the Kafka transaction alone |
//! | `resonate_kafka_commit_errors_total{kind}` | `unavailable`, `fenced`, `uncertain` |
//! | `resonate_kafka_records_committed_total{topic}` | |
//! | `resonate_kafka_doc_cache_total{result}` | `hit`, `miss` |
//! | `resonate_kafka_takeover_seconds`, `resonate_kafka_takeovers_total{outcome}` | |
//! | `resonate_kafka_replayed_records_total` | |
//! | `resonate_kafka_partitions_served` | |
//! | `resonate_kafka_forwarded_total{call,outcome}` | requests, fires and searches sent to another node |
//! | `resonate_kafka_timers_armed` | |
//!
//! Declared into the process-wide registry through `resonate-plugin`'s
//! prometheus, so the metrics gateway serves them with everything else.

use std::sync::LazyLock;

use resonate_plugin::prometheus::{
    exponential_buckets, register_counter_vec, register_histogram, register_int_counter,
    register_int_gauge, CounterVec, Histogram, IntCounter, IntGauge,
};

fn latency() -> Vec<f64> {
    exponential_buckets(0.000_05, 2.0, 20).expect("valid buckets")
}

pub static QUEUE_WAIT: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "resonate_kafka_queue_wait_seconds",
        "Time from a request reaching its partition to its round starting",
        latency()
    )
    .unwrap()
});

pub static ROUND_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "resonate_kafka_round_seconds",
        "Time a whole round took: load, decide, commit, apply",
        latency()
    )
    .unwrap()
});

pub static ROUND_REQUESTS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "resonate_kafka_round_requests",
        "Requests decided per round",
        exponential_buckets(1.0, 2.0, 11).unwrap()
    )
    .unwrap()
});

pub static COMMIT_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "resonate_kafka_commit_seconds",
        "Time a round's Kafka transaction took",
        latency()
    )
    .unwrap()
});

pub static COMMIT_ERRORS: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        "resonate_kafka_commit_errors_total",
        "Commits that failed, by kind",
        &["kind"]
    )
    .unwrap()
});

pub static RECORDS_COMMITTED: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        "resonate_kafka_records_committed_total",
        "Records committed, by topic",
        &["topic"]
    )
    .unwrap()
});

pub static DOC_CACHE: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        "resonate_kafka_doc_cache_total",
        "Origin loads, by whether the decoded document was cached",
        &["result"]
    )
    .unwrap()
});

pub static TAKEOVER_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "resonate_kafka_takeover_seconds",
        "Time a partition takeover took: fence, replay, index",
        latency()
    )
    .unwrap()
});

pub static TAKEOVERS: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        "resonate_kafka_takeovers_total",
        "Partition takeovers, by outcome",
        &["outcome"]
    )
    .unwrap()
});

pub static REPLAYED: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "resonate_kafka_replayed_records_total",
        "Records replayed from the log into local copies"
    )
    .unwrap()
});

pub static PARTITIONS_SERVED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "resonate_kafka_partitions_served",
        "Partitions this node serves"
    )
    .unwrap()
});

pub static FORWARDED: LazyLock<CounterVec> = LazyLock::new(|| {
    register_counter_vec!(
        "resonate_kafka_forwarded_total",
        "Calls forwarded to another node, by call and outcome",
        &["call", "outcome"]
    )
    .unwrap()
});

pub static TIMERS_ARMED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "resonate_kafka_timers_armed",
        "Deadlines armed across the partitions this node serves"
    )
    .unwrap()
});

/// Count a forward's outcome.
pub fn forwarded<T, E>(call: &str, out: &Result<T, E>) {
    FORWARDED
        .with_label_values(&[call, if out.is_ok() { "ok" } else { "failed" }])
        .inc();
}
