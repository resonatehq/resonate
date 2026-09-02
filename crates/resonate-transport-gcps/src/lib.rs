//! Resonate transport: Google Cloud Pub/Sub.
//!
//! Delivers a message by publishing it to a topic. A transport rather than a
//! plugin: it knows how to put a message on the wire and nothing about what
//! the message means.
//!
//! Authentication uses Application Default Credentials. Address format:
//! `gcps://project/topic`.

/// The address scheme this transport serves.
pub const SCHEME: &str = "gcps";

/// This transport, as a plugin. The one thing a binary names to get `gcps://`
/// addresses delivered.
pub static PLUGIN: resonate_plugin::WorkerPlugin =
    resonate_plugin::WorkerPlugin::new(env!("CARGO_PKG_NAME"), &[SCHEME], configure);

/// Read `[workers.transport_gcps]`, and build the transport unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::WorkerDependencies,
) -> Result<Option<std::sync::Arc<dyn ResonateWorker>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    // Zero permits sizes the publish semaphore to nothing, so nothing could
    // ever acquire a slot and every message would queue forever.
    if config.concurrency == 0 {
        return Err(settings.reject("concurrency", "must be at least 1 (got 0)"));
    }
    Ok(Some(std::sync::Arc::new(GcpsPubSubTransport::new(
        deps.server,
        config,
    ))))
}

/// Everything under `[transports.gcps]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Enable the gcps:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// GCP project, when the address does not carry one.
    #[serde(default)]
    pub project: Option<String>,

    /// Concurrent publishes in flight [default: 100]
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Publish timeout in milliseconds [default: 30000]
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_concurrency() -> usize {
    100
}
fn default_timeout() -> u64 {
    30_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            project: None,
            concurrency: default_concurrency(),
            timeout: default_timeout(),
        }
    }
}

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use google_cloud_pubsub::client::Publisher;
use tokio::sync::{mpsc, Mutex, Semaphore};

use resonate_plugin::types::Message;
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};

/// A `gcps://` destination: `gcps://<project>/<topic>`.
#[derive(Debug, Clone)]
pub struct GcpsAddress {
    pub project: String,
    pub topic: String,
}

impl GcpsAddress {
    /// Parse a `gcps://` address. This worker owns the syntax; the router has
    /// only checked the scheme.
    pub fn parse(address: &str) -> Result<Self, Unavailable> {
        let bad = || Unavailable::new(format!("malformed gcps address: {address}"));
        let parsed = url::Url::parse(address).map_err(|_| bad())?;
        let project = parsed.host_str().ok_or_else(bad)?.to_string();
        let path = parsed.path();
        if path.len() <= 1 {
            return Err(bad());
        }
        let topic = path[1..].to_string();
        if topic.is_empty() {
            return Err(bad());
        }
        Ok(GcpsAddress { project, topic })
    }
}

/// Google Cloud Pub/Sub transport — publishes messages to topics.
///
/// Uses Application Default Credentials (ADC) for authentication.
/// Publishers are cached per topic. Deliveries are queued and processed by
/// a dispatcher task that gates spawns on a concurrency semaphore; `send()`
/// only blocks if the queue is full, never on the publish RPC.
pub struct GcpsPubSubTransport {
    config: Config,
    /// Set by `init`, cleared by `stop`.
    tx: std::sync::Mutex<Option<mpsc::Sender<PublishJob>>>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Held so a delivery failure can be reported back to the server (e.g.
    /// releasing the task instead of dropping it). Not used yet.
    ///
    /// Weak: the server holds the router and the router holds this worker, so
    /// a strong handle back would close a reference cycle.
    #[allow(dead_code)]
    server: Weak<dyn ResonateServer>,
}

struct PublishJob {
    address: GcpsAddress,
    data: Vec<u8>,
}

impl GcpsPubSubTransport {
    /// Builds the value. Nothing is started and nothing can fail — see
    /// [`ResonateWorker::init`].
    pub fn new(server: Weak<dyn ResonateServer>, config: Config) -> Self {
        Self {
            config,
            tx: std::sync::Mutex::new(None),
            task: std::sync::Mutex::new(None),
            server,
        }
    }

    /// Enqueue a publish. Returns once the job is on the in-memory queue.
    /// Blocks only when the queue is full (never on the publish RPC).
    pub async fn send(
        &self,
        address: &GcpsAddress,
        payload: &serde_json::Value,
    ) -> Result<(), Unavailable> {
        let job = PublishJob {
            address: address.clone(),
            data: serde_json::to_vec(payload).unwrap_or_default(),
        };
        // Clone the sender out before awaiting — the guard must not be held
        // across the await point.
        let tx = match self.tx.lock().expect("gcps tx mutex").clone() {
            Some(tx) => tx,
            None => return Err(Unavailable::new("GCP Pub/Sub transport not initialised")),
        };
        if let Err(mpsc::error::SendError(job)) = tx.send(job).await {
            return Err(Unavailable::new(format!(
                "GCP Pub/Sub dispatcher gone, gcps://{}/{} not enqueued",
                job.address.project, job.address.topic
            )));
        }
        Ok(())
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
        }
        Ok(Err(e)) => {
            tracing::warn!(project = %address.project, topic = %address.topic, error = %e, "GCP Pub/Sub delivery failed");
        }
        Err(_) => {
            tracing::warn!(project = %address.project, topic = %address.topic, "GCP Pub/Sub publish timed out");
        }
    }
}

#[async_trait::async_trait]
impl ResonateWorker for GcpsPubSubTransport {
    /// Build the publisher cache and start the delivery queue.
    /// Nothing here runs on wall time — the publisher is driven by the queue —
    /// so the debug flag changes nothing for this transport.
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        let publishers = Arc::new(Mutex::new(HashMap::<String, Publisher>::new()));
        let semaphore = Arc::new(Semaphore::new(self.config.concurrency));
        // Queue capacity larger than concurrency so short bursts smooth out;
        // full queue + full in-flight pushes back on `send()`, which in turn
        // pushes back on the claim loop that feeds the router — the DB is the
        // durable buffer.
        let (tx, rx) = mpsc::channel::<PublishJob>(self.config.concurrency);
        let timeout = Duration::from_millis(self.config.timeout);

        let handle = tokio::spawn(dispatcher(publishers, semaphore, timeout, rx));
        *self.tx.lock().expect("gcps tx mutex") = Some(tx);
        *self.task.lock().expect("gcps task mutex") = Some(handle);
        Ok(())
    }

    /// Close the queue and wait for the dispatcher to drain.
    async fn stop(&self) -> Result<(), Unavailable> {
        // Dropping the sender closes the channel; the dispatcher finishes what
        // it has and returns.
        self.tx.lock().expect("gcps tx mutex").take();
        let handle = self.task.lock().expect("gcps task mutex").take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let addr = GcpsAddress::parse(address)?;
        let payload = serde_json::to_value(msg)
            .map_err(|e| Unavailable::new(format!("cannot serialize message: {e}")))?;
        GcpsPubSubTransport::send(self, &addr, &payload).await
    }
}
