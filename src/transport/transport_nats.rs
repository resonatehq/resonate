//! NATS transport — publish messages to NATS subjects.
//!
//! Address format: nats://subject

use async_nats::Client;

use super::NatsAddress;
use crate::metrics;

/// NATS transport — publishes messages to subjects.
pub struct NatsTransport {
    client: Client,
}

impl NatsTransport {
    pub async fn connect(url: &str) -> Result<Self, String> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| format!("Failed to connect to NATS at {}: {}", url, e))?;
        Ok(Self { client })
    }

    pub async fn send(&self, address: &NatsAddress, payload: &serde_json::Value) {
        let data = serde_json::to_vec(payload).unwrap_or_default();
        match self
            .client
            .publish(address.subject.clone(), data.into())
            .await
        {
            Ok(()) => {
                tracing::debug!(subject = %address.subject, "NATS delivery succeeded");
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["success"])
                    .inc();
            }
            Err(e) => {
                tracing::warn!(subject = %address.subject, error = %e, "NATS delivery failed");
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["error"])
                    .inc();
            }
        }
    }
}
