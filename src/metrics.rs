//! What the router counts, and the handler that serves everything.
//!
//! Only one counter lives here now: the rest belong to the code that
//! increments them and moved with it. `gather` reads prometheus' process-wide
//! default registry, so a plugin declaring its own counters appears here with
//! nothing central listing it.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use lazy_static::lazy_static;
use prometheus::{register_counter_vec, CounterVec};

/// Serve Prometheus metrics in text exposition format.
pub async fn metrics_handler() -> Response {
    use axum::http::header;
    use prometheus::Encoder;

    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buf = Vec::new();
    if encoder.encode(&metric_families, &mut buf).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode metrics",
        )
            .into_response();
    }
    ([(header::CONTENT_TYPE, encoder.format_type())], buf).into_response()
}

lazy_static! {
    /// Hand-offs to a worker, by outcome. Recorded in the router because it is
    /// the one place that sees every message — and "never reached a worker" is
    /// an outcome only visible from there.
    pub static ref DELIVERIES_TOTAL: CounterVec = register_counter_vec!(
        "resonate_deliveries_total",
        "Total number of message deliveries by status",
        &["status"]
    )
    .unwrap();
}
