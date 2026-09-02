//! The Prometheus endpoint.
//!
//! A gateway like any other — its own socket, its own port — and the one that
//! reads nothing from the server. What it serves is prometheus' process-wide
//! default registry, which every plugin in the binary declares into through the
//! `prometheus` re-exported from `resonate-plugin`. That is the whole reason
//! this crate needs no list of counters and no wiring to the plugins whose
//! counters it publishes: there is one registry, and the plugins that have
//! something to say have already said it.
//!
//! ```text
//!   GET /metrics   → the text exposition format
//! ```

use std::sync::Arc;

// axum comes from `resonate-plugin`, so a build has one of it — see that
// crate's re-export for why.
use resonate_plugin::axum;

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use resonate_plugin::prometheus;
use serde::{Deserialize, Serialize};

/// This endpoint, as a plugin.
pub static PLUGIN: resonate_plugin::GatewayPlugin =
    resonate_plugin::GatewayPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[gateways.gateway_metrics]`, and build it unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    _deps: resonate_plugin::GatewayDependencies,
) -> Result<Option<Arc<dyn resonate_plugin::ResonateGateway>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    Ok(Some(Arc::new(Metrics {
        config,
        serving: std::sync::Mutex::new(None),
    })))
}

/// Where to serve metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Serve at all. On by default: an endpoint nobody scrapes costs a socket,
    /// and one that is off when an operator expected it costs an incident.
    #[serde(default = "yes")]
    pub enabled: bool,

    /// Where to listen [default: 0.0.0.0:9090].
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn yes() -> bool {
    true
}

fn default_bind() -> String {
    "0.0.0.0:9090".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_bind(),
        }
    }
}

/// The one route, for a caller that wants to serve it somewhere else.
pub fn routes<S>() -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    axum::Router::new().route("/metrics", get(handle))
}

/// Encode the default registry, as Prometheus reads it.
async fn handle() -> Response {
    use prometheus::Encoder;

    let encoder = prometheus::TextEncoder::new();
    let families = prometheus::gather();
    let mut buf = Vec::new();
    if encoder.encode(&families, &mut buf).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode metrics\n",
        )
            .into_response();
    }
    ([(header::CONTENT_TYPE, encoder.format_type())], buf).into_response()
}

struct Serving {
    task: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

/// The endpoint, serving itself.
struct Metrics {
    config: Config,
    serving: std::sync::Mutex<Option<Serving>>,
}

#[async_trait::async_trait]
impl resonate_plugin::ResonateGateway for Metrics {
    async fn init(&self, _debug: bool) -> Result<(), resonate_plugin::Unavailable> {
        let listener = tokio::net::TcpListener::bind(&self.config.bind)
            .await
            .map_err(|e| {
                resonate_plugin::Unavailable::new(format!(
                    "metrics cannot bind {}: {e}",
                    self.config.bind
                ))
            })?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let app: axum::Router<()> = routes();
        let task = tokio::spawn(async move {
            let served = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = rx.await;
            });
            if let Err(e) = served.await {
                tracing::error!(error = %e, "Metrics listener stopped");
            }
        });
        *self.serving.lock().expect("metrics serving mutex") = Some(Serving { task, shutdown: tx });
        tracing::info!(bind = %self.config.bind, "Metrics listening");
        Ok(())
    }

    async fn stop(&self) -> Result<(), resonate_plugin::Unavailable> {
        // Out of the guard before the await: a std MutexGuard is not Send.
        let serving = self.serving.lock().expect("metrics serving mutex").take();
        if let Some(serving) = serving {
            let _ = serving.shutdown.send(());
            let _ = serving.task.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn the_endpoint_serves_what_a_plugin_declared() {
        // A counter declared the way any plugin declares one — into the default
        // registry, which is the only thing this crate knows about.
        let counter = prometheus::register_counter!(
            "resonate_gateway_metrics_probe_total",
            "a counter declared by a test"
        )
        .expect("declared once");
        counter.inc();

        let app: axum::Router<()> = routes();
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("infallible");
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("resonate_gateway_metrics_probe_total 1"),
            "{text}"
        );
    }
}
