//! Google Cloud Pub/Sub transport — publish messages to GCP topics.
//!
//! Address format: gcps://project/topic

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use google_cloud_pubsub::client::Publisher;
use tokio::sync::{mpsc, Mutex, Semaphore};

use super::GcpsAddress;
use crate::metrics;

/// Google Cloud Pub/Sub transport — publishes messages to topics.
///
/// Uses Application Default Credentials (ADC) for authentication.
/// Publishers are cached per topic. Deliveries are queued and processed by
/// a dispatcher task that gates spawns on a concurrency semaphore; `send()`
/// only blocks if the queue is full, never on the publish RPC.
pub struct GcpsPubSubTransport {
    tx: mpsc::Sender<PublishJob>,
}

struct PublishJob {
    address: GcpsAddress,
    data: Vec<u8>,
}

impl GcpsPubSubTransport {
    pub fn new(concurrency: usize, timeout: Duration) -> Self {
        let publishers = Arc::new(Mutex::new(HashMap::<String, Publisher>::new()));
        let semaphore = Arc::new(Semaphore::new(concurrency));
        // Queue capacity larger than concurrency so short bursts smooth out;
        // full queue + full in-flight pushes back on `send()`, which in turn
        // pushes back on the claim loop in processing_messages — the DB is
        // the durable buffer.
        let (tx, rx) = mpsc::channel::<PublishJob>(concurrency);

        tokio::spawn(dispatcher(publishers, semaphore, timeout, rx));

        Self { tx }
    }

    /// Enqueue a publish. Returns once the job is on the in-memory queue.
    /// Blocks only when the queue is full (never on the publish RPC).
    pub async fn send(&self, address: &GcpsAddress, payload: &serde_json::Value) {
        let job = PublishJob {
            address: address.clone(),
            data: serde_json::to_vec(payload).unwrap_or_default(),
        };
        if let Err(mpsc::error::SendError(job)) = self.tx.send(job).await {
            tracing::warn!(
                address = %format!("gcps://{}/{}", job.address.project, job.address.topic),
                "GCP Pub/Sub dispatcher gone, message dropped"
            );
            metrics::DELIVERIES_TOTAL
                .with_label_values(&["dropped"])
                .inc();
        }
    }
}

async fn dispatcher(
    publishers: Arc<Mutex<HashMap<String, Publisher>>>,
    semaphore: Arc<Semaphore>,
    timeout: Duration,
    mut rx: mpsc::Receiver<PublishJob>,
) {
    while let Some(job) = rx.recv().await {
        let permit = match Arc::clone(&semaphore).acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let publishers = Arc::clone(&publishers);
        tokio::spawn(async move {
            let _permit = permit;
            deliver(publishers, job, timeout).await;
        });
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

async fn deliver(
    publishers: Arc<Mutex<HashMap<String, Publisher>>>,
    job: PublishJob,
    timeout: Duration,
) {
    let PublishJob { address, data } = job;
    let address_str = format!("gcps://{}/{}", address.project, address.topic);
    let topic_fqn = format!("projects/{}/topics/{}", address.project, address.topic);

    let publisher = match get_publisher(&publishers, &topic_fqn).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(address = %address_str, error = %e, "Failed to get GCP Pub/Sub publisher");
            metrics::DELIVERIES_TOTAL
                .with_label_values(&["error"])
                .inc();
            return;
        }
    };

    tracing::debug!(address = %address_str, project = %address.project, topic = %address.topic, "Publishing to GCP Pub/Sub");
    let mut msg = <google_cloud_pubsub::model::Message as Default>::default();
    msg.data = data.into();
    let fut = publisher.publish(msg);

    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(_message_id)) => {
            tracing::debug!(project = %address.project, topic = %address.topic, "GCP Pub/Sub delivery succeeded");
            metrics::DELIVERIES_TOTAL
                .with_label_values(&["success"])
                .inc();
        }
        Ok(Err(e)) => {
            tracing::warn!(project = %address.project, topic = %address.topic, error = %e, "GCP Pub/Sub delivery failed");
            metrics::DELIVERIES_TOTAL
                .with_label_values(&["error"])
                .inc();
        }
        Err(_) => {
            tracing::warn!(project = %address.project, topic = %address.topic, "GCP Pub/Sub publish timed out");
            metrics::DELIVERIES_TOTAL
                .with_label_values(&["error"])
                .inc();
        }
    }
}
