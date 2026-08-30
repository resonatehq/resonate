//! This backend's own metrics, in prometheus' global default registry.
//!
//! The names carry a `blob` segment because the binary's `metrics.rs` already
//! registers `resonate_messages_total` and `resonate_schedule_promises_total`
//! there; two registrations of one name in one process is an `Err` that a
//! `lazy_static` `.unwrap()` turns into a panic on first use. Distinct names
//! keep the two paths distinguishable on a dashboard anyway — the blob outbox
//! and `Server::deliver` are different code delivering different queues.

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_int_gauge, Counter, CounterVec, IntGauge,
};

lazy_static! {
    pub static ref MESSAGES_TOTAL: CounterVec = register_counter_vec!(
        "resonate_blob_messages_total",
        "Total number of messages delivered by the blob outbox, by kind",
        &["kind"]
    )
    .unwrap();
    pub static ref SCHEDULE_PROMISES_TOTAL: Counter = register_counter!(
        "resonate_blob_schedule_promises_total",
        "Total number of promises created by schedules on the blob backend"
    )
    .unwrap();
    pub static ref TIMER_QUEUE_LEN: IntGauge = register_int_gauge!(
        "resonate_blob_timer_queue_len",
        "Number of armed deadlines in the blob backend's in-memory timer queue"
    )
    .unwrap();
    pub static ref DOC_CACHE_MISSES_TOTAL: Counter = register_counter!(
        "resonate_blob_doc_cache_misses_total",
        "Document cache misses on the blob backend's read path"
    )
    .unwrap();
    pub static ref DOC_CACHE_HITS_TOTAL: Counter = register_counter!(
        "resonate_blob_doc_cache_hits_total",
        "Document cache hits on the blob backend's read path"
    )
    .unwrap();
}
