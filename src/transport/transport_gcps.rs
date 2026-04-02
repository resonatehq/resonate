//! Google Cloud Pub/Sub transport — publish messages to GCP topics.
//!
//! Address format: gcps://project/topic

use std::collections::HashMap;

use google_cloud_pubsub::client::Publisher;
use tokio::sync::Mutex;

use super::GcpsAddress;
use crate::metrics;

/// Google Cloud Pub/Sub transport — publishes messages to topics.
///
/// Uses Application Default Credentials (ADC) for authentication.
/// Publishers are cached per topic.
pub struct GcpsPubSubTransport {
    publishers: Mutex<HashMap<String, Publisher>>,
}

impl GcpsPubSubTransport {
    pub fn new() -> Self {
        Self {
            publishers: Mutex::new(HashMap::new()),
        }
    }

    async fn get_publisher(&self, topic_fqn: &str) -> Result<Publisher, String> {
        let mut cache = self.publishers.lock().await;
        if let Some(p) = cache.get(topic_fqn) {
            return Ok(Publisher::clone(p));
        }
        let p = Publisher::builder(topic_fqn)
            .build()
            .await
            .map_err(|e| format!("Failed to create publisher for {}: {}", topic_fqn, e))?;
        cache.insert(topic_fqn.to_string(), Publisher::clone(&p));
        Ok(p)
    }

    pub async fn send(&self, address: &GcpsAddress, payload: &serde_json::Value) {
        let topic_fqn = format!("projects/{}/topics/{}", address.project, address.topic);

        let publisher = match self.get_publisher(&topic_fqn).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get GCP Pub/Sub publisher");
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["error"])
                    .inc();
                return;
            }
        };

        let data = serde_json::to_vec(payload).unwrap_or_default();
        let mut msg = <google_cloud_pubsub::model::Message as Default>::default();
        msg.data = data.into();

        let fut = publisher.publish(msg);
        let project = address.project.clone();
        let topic = address.topic.clone();

        tokio::spawn(async move {
            match tokio::time::timeout(std::time::Duration::from_secs(10), fut).await {
                Ok(Ok(_message_id)) => {
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["success"])
                        .inc();
                }
                Ok(Err(e)) => {
                    tracing::warn!(project = %project, topic = %topic, error = %e, "GCP Pub/Sub delivery failed");
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                }
                Err(_) => {
                    tracing::warn!(project = %project, topic = %topic, "GCP Pub/Sub publish timed out");
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                }
            }
        });
    }
}
