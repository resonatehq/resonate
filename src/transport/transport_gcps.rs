//! Google Cloud Pub/Sub transport — publish messages to GCP topics.
//!
//! Address format: gcps://project/topic

use std::collections::HashMap;
use std::sync::Arc;

use google_cloud_pubsub::client::Publisher;
use tokio::sync::{Mutex, Semaphore};

use super::GcpsAddress;
use crate::metrics;

/// Google Cloud Pub/Sub transport — publishes messages to topics.
///
/// Uses Application Default Credentials (ADC) for authentication.
/// Publishers are cached per topic. Deliveries are bounded by a semaphore
/// and dispatched on background tasks so the message-processing loop is
/// never blocked by publisher construction or slow publish RPCs.
pub struct GcpsPubSubTransport {
    publishers: Arc<Mutex<HashMap<String, Publisher>>>,
    semaphore: Arc<Semaphore>,
}

impl GcpsPubSubTransport {
    pub fn new(concurrency: usize) -> Self {
        Self {
            publishers: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(concurrency)),
        }
    }

    /// Get-or-build a Publisher for the topic.
    ///
    /// Double-checked locking: the mutex is released across `build().await`
    /// so a cold miss for one topic does not stall cache hits (or builds)
    /// for unrelated topics. Two concurrent misses for the *same* topic
    /// may both build a Publisher; the second one is discarded on insert.
    /// `Publisher::build()` is cheap (no network on construction in this
    /// SDK) so that's fine.
    async fn get_publisher(
        publishers: &Mutex<HashMap<String, Publisher>>,
        topic_fqn: &str,
    ) -> Result<Publisher, String> {
        if let Some(p) = publishers.lock().await.get(topic_fqn) {
            return Ok(Publisher::clone(p));
        }
        let p = Publisher::builder(topic_fqn)
            .build()
            .await
            .map_err(|e| format!("Failed to create publisher for {}: {}", topic_fqn, e))?;
        let mut cache = publishers.lock().await;
        let entry = cache
            .entry(topic_fqn.to_string())
            .or_insert_with(|| Publisher::clone(&p));
        Ok(Publisher::clone(entry))
    }

    /// Non-blocking dispatch: acquires a concurrency permit and spawns the
    /// publish (including publisher acquire) on a background task. Returns
    /// immediately so the message-processing loop is never head-of-line
    /// blocked. If all permits are in use the message is dropped.
    pub async fn send(&self, address: &GcpsAddress, payload: &serde_json::Value) {
        let address_str = format!("gcps://{}/{}", address.project, address.topic);

        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!(
                    address = %address_str,
                    "GCP Pub/Sub concurrency limit reached, message dropped"
                );
                metrics::DELIVERIES_TOTAL
                    .with_label_values(&["dropped"])
                    .inc();
                return;
            }
        };

        let publishers = self.publishers.clone();
        let project = address.project.clone();
        let topic = address.topic.clone();
        let topic_fqn = format!("projects/{}/topics/{}", project, topic);
        let data = serde_json::to_vec(payload).unwrap_or_default();

        tokio::spawn(async move {
            // Hold the permit for the task's lifetime — its Drop is what
            // releases the semaphore slot. Referencing `permit` here is also
            // what forces `async move` to capture it; without this line the
            // permit would drop when `send()` returns and the cap would be a
            // no-op.
            let _permit = permit;

            let publisher = match Self::get_publisher(&publishers, &topic_fqn).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(address = %address_str, error = %e, "Failed to get GCP Pub/Sub publisher");
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["error"])
                        .inc();
                    return;
                }
            };

            tracing::debug!(address = %address_str, project = %project, topic = %topic, "Publishing to GCP Pub/Sub");
            let mut msg = <google_cloud_pubsub::model::Message as Default>::default();
            msg.data = data.into();
            let fut = publisher.publish(msg);

            match tokio::time::timeout(std::time::Duration::from_secs(10), fut).await {
                Ok(Ok(_message_id)) => {
                    tracing::debug!(project = %project, topic = %topic, "GCP Pub/Sub delivery succeeded");
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
