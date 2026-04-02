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
            Ok(_) => {
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["success"])
                    .inc();
            }
            Err(e) => {
                tracing::warn!(
                    url = %address.url,
                    error = %e,
                    kind = if e.is_connect() { "connect" } else if e.is_timeout() { "timeout" } else { "other" },
                    "HTTP delivery failed"
                );
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["error"])
                    .inc();
            }
        }
    }
}
