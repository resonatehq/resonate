//! HTTP transport — send messages via HTTP POST to webhook URLs.

use std::time::Duration;

use reqwest::Client;

use super::HttpAddress;
use crate::metrics;

/// HTTP push transport — delivers messages via HTTP POST.
pub struct HttpPushTransport {
    client: Client,
}

impl HttpPushTransport {
    pub fn new(connect_timeout: Duration, request_timeout: Duration) -> Self {
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .expect("failed to build HTTP client");
        Self { client }
    }

    pub async fn send(&self, address: &HttpAddress, payload: &serde_json::Value) {
        let request = self
            .client
            .post(&address.url)
            .header("Content-Type", "application/json")
            .json(payload);

        match request.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if resp.status().is_success() {
                    tracing::debug!(
                        address = %address.url,
                        status = status,
                        "HTTP push delivery succeeded"
                    );
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["success"])
                        .inc();
                } else {
                    tracing::warn!(
                        address = %address.url,
                        status = status,
                        "HTTP push delivery rejected by target"
                    );
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                }
            }
            Err(e) => {
                tracing::warn!(
                    address = %address.url,
                    error = %e,
                    error_kind = if e.is_connect() { "connect" } else if e.is_timeout() { "timeout" } else { "other" },
                    "HTTP push delivery failed"
                );
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["error"])
                    .inc();
            }
        }
    }
}
