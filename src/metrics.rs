//! Metrics — Prometheus counters and histograms.
//!
//! These used to be `lazy_static` handles registered into the prometheus
//! *default global registry*. That made them a process-global: counters
//! accumulated across every test in the binary, so no test could assert that
//! an operation had incremented a particular series, and nothing could be
//! reset between cases.
//!
//! [`Metrics`] is a value instead. Each [`Server`](crate::server::Server) owns
//! one; [`Metrics::global`] returns the shared process-wide instance that the
//! `/metrics` endpoint scrapes, and is what production wires up.
//! [`Metrics::isolated`] builds a fresh set over a private registry for tests.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use prometheus::{Counter, CounterVec, HistogramVec, Opts, Registry};

/// One server's metric handles, over one registry.
#[derive(Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    pub request_total: CounterVec,
    pub request_duration: HistogramVec,
    pub messages_total: CounterVec,
    pub deliveries_total: CounterVec,
    pub schedule_promises_total: Counter,
}

impl Metrics {
    /// Build a metric set registered into `registry`.
    pub fn new(registry: Arc<Registry>) -> Self {
        let request_total = CounterVec::new(
            Opts::new(
                "resonate_request_total",
                "Total number of requests by kind and status",
            ),
            &["kind", "status"],
        )
        .expect("valid metric definition");

        let request_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "resonate_request_duration_seconds",
                "Request duration in seconds by kind",
            ),
            &["kind"],
        )
        .expect("valid metric definition");

        let messages_total = CounterVec::new(
            Opts::new(
                "resonate_messages_total",
                "Total number of messages delivered by kind",
            ),
            &["kind"],
        )
        .expect("valid metric definition");

        let deliveries_total = CounterVec::new(
            Opts::new(
                "resonate_deliveries_total",
                "Total number of message deliveries by status",
            ),
            &["status"],
        )
        .expect("valid metric definition");

        let schedule_promises_total = Counter::with_opts(Opts::new(
            "resonate_schedule_promises_total",
            "Total number of promises created by schedules",
        ))
        .expect("valid metric definition");

        // Registration fails only on a duplicate name in the same registry.
        // The global registry is built once, so ignore an AlreadyReg error
        // rather than panicking if a second Metrics is built over it.
        let _ = registry.register(Box::new(request_total.clone()));
        let _ = registry.register(Box::new(request_duration.clone()));
        let _ = registry.register(Box::new(messages_total.clone()));
        let _ = registry.register(Box::new(deliveries_total.clone()));
        let _ = registry.register(Box::new(schedule_promises_total.clone()));

        Self {
            registry,
            request_total,
            request_duration,
            messages_total,
            deliveries_total,
            schedule_promises_total,
        }
    }

    /// The process-wide metric set that `/metrics` serves.
    ///
    /// Built once, on first use. This is what production wires into the server.
    pub fn global() -> Self {
        use std::sync::OnceLock;
        static GLOBAL: OnceLock<Metrics> = OnceLock::new();
        GLOBAL
            .get_or_init(|| Metrics::new(Arc::new(prometheus::default_registry().clone())))
            .clone()
    }

    /// A metric set over a private registry.
    ///
    /// Nothing here is shared with any other `Metrics`, so a test can assert on
    /// exact counter values without interference from tests running in
    /// parallel.
    pub fn isolated() -> Self {
        Metrics::new(Arc::new(Registry::new()))
    }

    /// The registry these metrics are registered into.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Current value of `resonate_request_total{kind,status}`.
    ///
    /// A read-back helper so tests can assert on what was recorded without
    /// scraping and parsing the text exposition format.
    pub fn request_count(&self, kind: &str, status: &str) -> f64 {
        self.request_total
            .with_label_values(&[kind, status])
            .get()
    }

    /// Current value of `resonate_messages_total{kind}`.
    pub fn message_count(&self, kind: &str) -> f64 {
        self.messages_total.with_label_values(&[kind]).get()
    }

    /// Current value of `resonate_deliveries_total{status}`.
    pub fn delivery_count(&self, status: &str) -> f64 {
        self.deliveries_total.with_label_values(&[status]).get()
    }

    /// Render this registry in Prometheus text exposition format.
    pub fn encode(&self) -> Result<(String, Vec<u8>), String> {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let mut buf = Vec::new();
        encoder
            .encode(&self.registry.gather(), &mut buf)
            .map_err(|e| e.to_string())?;
        Ok((encoder.format_type().to_string(), buf))
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::global()
    }
}

/// Serve Prometheus metrics in text exposition format.
pub async fn metrics_handler(
    axum::extract::State(metrics): axum::extract::State<Metrics>,
) -> Response {
    use axum::http::header;
    match metrics.encode() {
        Ok((content_type, buf)) => ([(header::CONTENT_TYPE, content_type)], buf).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "Failed to encode metrics");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to encode metrics",
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolated_metric_sets_do_not_share_counters() {
        let a = Metrics::isolated();
        let b = Metrics::isolated();

        a.request_total
            .with_label_values(&["promise.get", "200"])
            .inc();

        assert_eq!(a.request_count("promise.get", "200"), 1.0);
        assert_eq!(
            b.request_count("promise.get", "200"),
            0.0,
            "a second isolated set must start clean — that is the whole point"
        );
    }

    #[test]
    fn counters_are_readable_back_by_label() {
        let m = Metrics::isolated();
        m.request_total.with_label_values(&["task.create", "409"]).inc();
        m.request_total.with_label_values(&["task.create", "409"]).inc();
        m.request_total.with_label_values(&["task.create", "200"]).inc();

        assert_eq!(m.request_count("task.create", "409"), 2.0);
        assert_eq!(m.request_count("task.create", "200"), 1.0);
        assert_eq!(m.request_count("task.create", "500"), 0.0);
    }

    #[test]
    fn encoding_produces_text_exposition_format() {
        let m = Metrics::isolated();
        m.messages_total.with_label_values(&["execute"]).inc();

        let (content_type, buf) = m.encode().expect("encodes");
        let body = String::from_utf8(buf).expect("utf-8");

        assert!(content_type.contains("text/plain"), "{content_type}");
        assert!(body.contains("resonate_messages_total"), "{body}");
        assert!(body.contains("execute"), "{body}");
    }

    #[test]
    fn the_global_set_is_the_same_handles_every_time() {
        let before = Metrics::global().message_count("unblock");
        Metrics::global()
            .messages_total
            .with_label_values(&["unblock"])
            .inc();
        assert_eq!(
            Metrics::global().message_count("unblock"),
            before + 1.0,
            "global() must return handles onto one shared registry"
        );
    }
}
