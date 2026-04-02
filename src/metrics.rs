use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_histogram_vec, Counter, CounterVec,
    HistogramVec,
};

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
    pub static ref REQUEST_TOTAL: CounterVec = register_counter_vec!(
        "resonate_request_total",
        "Total number of requests by kind and status",
        &["kind", "status"]
    )
    .unwrap();
    pub static ref REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "resonate_request_duration_seconds",
        "Request duration in seconds by kind",
        &["kind"]
    )
    .unwrap();
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
