use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use lazy_static::lazy_static;
use prometheus::{register_counter, register_counter_vec, Counter, CounterVec};

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
    // `resonate_request_total` and `resonate_request_duration_seconds` are
    // declared by `resonate-gateway-http`, which is the only thing that records
    // them. They still appear below: `register_*!` writes into prometheus'
    // global default registry, and `gather` reads the same one.
    pub static ref MESSAGES_TOTAL: CounterVec = register_counter_vec!(
        "resonate_messages_total",
        "Total number of messages delivered by kind",
        &["kind"]
    )
    .unwrap();
    pub static ref DELIVERIES_TOTAL: CounterVec = register_counter_vec!(
        "resonate_deliveries_total",
        "Total number of message deliveries by status",
        &["status"]
    )
    .unwrap();
    pub static ref SCHEDULE_PROMISES_TOTAL: Counter = register_counter!(
        "resonate_schedule_promises_total",
        "Total number of promises created by schedules"
    )
    .unwrap();
}
