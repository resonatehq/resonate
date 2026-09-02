//! Resonate transport: HTTP(S) push.
//!
//! Delivers a message by POSTing it to the worker's URL. A transport rather
//! than a plugin: it knows nothing about what the message means, only how to
//! put it on the wire.

/// The address schemes this transport serves.
pub const SCHEMES: &[&str] = &["http", "https"];

/// This transport, as a plugin. The one thing a binary names to get `http://`
/// and `https://` addresses delivered.
pub static PLUGIN: resonate_plugin::WorkerPlugin =
    resonate_plugin::WorkerPlugin::new(env!("CARGO_PKG_NAME"), SCHEMES, configure);

/// Read `[workers.transport_http_push]`, and build the transport unless it is turned off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::WorkerDependencies,
) -> Result<Option<std::sync::Arc<dyn ResonateWorker>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    // Zero permits sizes the delivery semaphore to nothing, so the dispatcher
    // could never acquire a slot: every message would queue and then block the
    // loop that feeds it, forever. Refused here rather than hung on later.
    if config.concurrency == 0 {
        return Err(settings.reject("concurrency", "must be at least 1 (got 0)"));
    }
    Ok(Some(std::sync::Arc::new(HttpPushTransport::new(
        deps.server,
        config,
    ))))
}

/// Everything under `[transports.http_push]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Enable the http:// and https:// address schemes [default: true]
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Deliveries in flight at once [default: 100]
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Connect timeout in milliseconds [default: 5000]
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,

    /// Request timeout in milliseconds [default: 30000]
    #[serde(default = "default_request_timeout")]
    pub request_timeout: u64,

    /// Outbound auth. Absent means no Authorization header.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

fn default_enabled() -> bool {
    true
}
fn default_concurrency() -> usize {
    100
}
fn default_connect_timeout() -> u64 {
    5_000
}
fn default_request_timeout() -> u64 {
    30_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            concurrency: default_concurrency(),
            connect_timeout: default_connect_timeout(),
            request_timeout: default_request_timeout(),
            auth: None,
        }
    }
}

use serde::{Deserialize, Serialize};

/// Outbound auth mode for HTTP push deliveries.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// No auth header. Default.
    #[default]
    None,
    /// Static `Authorization: Bearer <token>`.
    Bearer,
    /// GCP OIDC ID token via the GCP metadata server.
    Gcp,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::default(),
            token: None,
            audience: None,
            header: default_auth_header(),
        }
    }
}

/// Outbound authentication for HTTP push deliveries.
///
/// Example config:
/// ```toml
/// [transports.http_push.auth]
/// mode = "gcp"
/// # audience = "https://my-function.example.com"  # optional; defaults to delivery URL
/// ```
///
/// Equivalent env vars (double-underscore nesting):
///   RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__MODE=gcp
///   RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__AUDIENCE=https://...
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Auth mode. Default: `none`.
    #[serde(default)]
    pub mode: AuthMode,

    /// Static bearer token. Used only when `mode = "bearer"`.
    /// Falls back to the `RESONATE_TRANSPORTS__HTTP_PUSH__AUTH__TOKEN` env var.
    #[serde(default)]
    pub token: Option<String>,

    /// GCP audience override. Used only when `mode = "gcp"`.
    /// When absent, each delivery target URL is used as its own audience.
    #[serde(default)]
    pub audience: Option<String>,

    /// Header name to set. Default: `"Authorization"`.
    #[serde(default = "default_auth_header")]
    pub header: String,
}

fn default_auth_header() -> String {
    "Authorization".to_string()
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use reqwest::Client;
use tokio::sync::{mpsc, Semaphore};

use async_trait::async_trait;

use resonate_plugin::types::Message;
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};

/// An `http://` or `https://` destination. The whole address is the URL, so
/// parsing is the identity — the type exists to keep the delivery queue typed.
#[derive(Debug, Clone)]
pub struct HttpAddress {
    pub url: String,
}

// ---------------------------------------------------------------------------
// Token provider abstraction
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait TokenProvider: Send + Sync {
    async fn get_token(&self, audience: &str) -> Result<String, String>;
}

// ---------------------------------------------------------------------------
// GCP ID token provider (backed by google-cloud-auth)
// ---------------------------------------------------------------------------

use google_cloud_auth::credentials::idtoken::{Builder as IdTokenBuilder, IDTokenCredentials};

struct GcpIdTokenProvider {
    cache: Mutex<HashMap<String, IDTokenCredentials>>,
}

#[async_trait::async_trait]
impl TokenProvider for GcpIdTokenProvider {
    async fn get_token(&self, audience: &str) -> Result<String, String> {
        let cached = self.cache.lock().unwrap().get(audience).cloned();
        let creds = if let Some(c) = cached {
            c
        } else {
            let c = IdTokenBuilder::new(audience)
                .build()
                .map_err(|e| e.to_string())?;
            self.cache
                .lock()
                .unwrap()
                .entry(audience.to_string())
                .or_insert(c)
                .clone()
        };
        creds.id_token().await.map_err(|e| e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Outbound auth
// ---------------------------------------------------------------------------

pub enum Auth {
    None,
    StaticBearer {
        header: String,
        value: String,
    },
    GcpIdToken {
        header: String,
        fixed_audience: Option<String>,
        provider: Box<dyn TokenProvider>,
    },
}

impl Auth {
    pub fn from_config(config: &AuthConfig) -> Self {
        match config.mode {
            AuthMode::None => Auth::None,
            AuthMode::Bearer => {
                let token = config.token.clone().unwrap_or_default();
                Auth::StaticBearer {
                    header: config.header.clone(),
                    value: format!("Bearer {token}"),
                }
            }
            AuthMode::Gcp => Auth::GcpIdToken {
                header: config.header.clone(),
                fixed_audience: config.audience.clone(),
                provider: Box::new(GcpIdTokenProvider {
                    cache: Mutex::new(HashMap::new()),
                }),
            },
        }
    }

    async fn resolve(&self, target_url: &str) -> Option<(String, String)> {
        match self {
            Auth::None => None,
            Auth::StaticBearer { header, value } => Some((header.clone(), value.clone())),
            Auth::GcpIdToken {
                header,
                fixed_audience,
                provider,
            } => {
                let audience = fixed_audience.as_deref().unwrap_or(target_url);
                match provider.get_token(audience).await {
                    Ok(token) => Some((header.clone(), format!("Bearer {token}"))),
                    Err(err) => {
                        tracing::warn!(
                            target_url = %target_url,
                            error = %err,
                            "OIDC ID token mint failed; sending request unauthenticated"
                        );
                        None
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

struct DeliveryJob {
    address: HttpAddress,
    payload: serde_json::Value,
}

pub struct HttpPushTransport {
    config: Config,
    /// Set by `init`, cleared by `stop`.
    tx: std::sync::Mutex<Option<mpsc::Sender<DeliveryJob>>>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Held so a delivery failure can be reported back to the server (e.g.
    /// releasing the task instead of dropping it). Not used yet.
    ///
    /// Weak: the server holds the router and the router holds this worker, so
    /// a strong handle back would close a reference cycle.
    #[allow(dead_code)]
    server: Weak<dyn ResonateServer>,
}

impl HttpPushTransport {
    /// Start the delivery queue with an already-built [`Auth`].
    ///
    /// `init` derives the `Auth` from config; tests supply one directly,
    /// because a mock token provider cannot be expressed as configuration.
    async fn start(&self, auth: Auth) -> Result<(), Unavailable> {
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(self.config.connect_timeout))
            .timeout(Duration::from_millis(self.config.request_timeout))
            .build()
            .map_err(|e| Unavailable::new(format!("cannot build HTTP client: {e}")))?;
        let semaphore = Arc::new(Semaphore::new(self.config.concurrency));
        // Queue capacity is intentionally larger than `concurrency` so short
        // bursts smooth out; full queue + full in-flight pushes back on
        // `send()`, which in turn pushes back on the claim loop that feeds the
        // router — the DB is the durable buffer.
        let (tx, rx) = mpsc::channel::<DeliveryJob>(self.config.concurrency);

        let handle = tokio::spawn(dispatcher(client, Arc::new(auth), semaphore, rx));
        *self.tx.lock().expect("http push tx mutex") = Some(tx);
        *self.task.lock().expect("http push task mutex") = Some(handle);
        Ok(())
    }

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

    /// Enqueue a delivery. Returns once the job is on the in-memory queue.
    /// Blocks only when the queue is full (never on network I/O), which
    /// applies natural backpressure to the message-processing loop instead
    /// of dropping messages.
    pub async fn send(
        &self,
        address: &HttpAddress,
        payload: &serde_json::Value,
    ) -> Result<(), Unavailable> {
        let job = DeliveryJob {
            address: address.clone(),
            payload: payload.clone(),
        };
        // Clone the sender out before awaiting — the guard must not be held
        // across the await point.
        let tx = match self.tx.lock().expect("http push tx mutex").clone() {
            Some(tx) => tx,
            None => return Err(Unavailable::new("HTTP push transport not initialised")),
        };
        if let Err(mpsc::error::SendError(job)) = tx.send(job).await {
            // Dispatcher task is gone (transport shutting down). Should not
            // happen during normal operation; surface it to the caller, which
            // is what records the outcome.
            return Err(Unavailable::new(format!(
                "HTTP push dispatcher gone, {} not enqueued",
                job.address.url
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl ResonateWorker for HttpPushTransport {
    /// Build the HTTP client and start the delivery queue.
    /// Nothing here runs on wall time — the dispatcher is driven by the queue,
    /// and a request timeout is scoped to a request — so the debug flag changes
    /// nothing for this transport.
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        let auth = match &self.config.auth {
            Some(cfg) => Auth::from_config(cfg),
            None => Auth::None,
        };
        self.start(auth).await
    }

    /// Close the queue and wait for the dispatcher to drain.
    async fn stop(&self) -> Result<(), Unavailable> {
        self.tx.lock().expect("http push tx mutex").take();
        let handle = self.task.lock().expect("http push task mutex").take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    /// The address is the URL verbatim; the router has already guaranteed the
    /// scheme is `http` or `https`.
    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let payload = serde_json::to_value(msg)
            .map_err(|e| Unavailable::new(format!("cannot serialize message: {e}")))?;
        HttpPushTransport::send(
            self,
            &HttpAddress {
                url: address.to_string(),
            },
            &payload,
        )
        .await
    }
}

async fn dispatcher(
    client: Client,
    auth: Arc<Auth>,
    semaphore: Arc<Semaphore>,
    mut rx: mpsc::Receiver<DeliveryJob>,
) {
    while let Some(job) = rx.recv().await {
        // Wait for an in-flight slot rather than dropping. Backpressure
        // propagates: full in-flight → dispatcher parked here → queue fills
        // → send() parks the claim loop.
        let permit = match Arc::clone(&semaphore).acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let client = client.clone();
        let auth = Arc::clone(&auth);
        tokio::spawn(async move {
            let _permit = permit;
            deliver(client, auth, job).await;
        });
    }
}

async fn deliver(client: Client, auth: Arc<Auth>, job: DeliveryJob) {
    let DeliveryJob { address, payload } = job;
    let auth_header = auth.resolve(&address.url).await;

    let mut request = client
        .post(&address.url)
        .header("Content-Type", "application/json")
        .json(&payload);

    if let Some((name, value)) = auth_header {
        request = request.header(name, value);
    }

    match request.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            if resp.status().is_success() {
                tracing::debug!(address = %address.url, status, "HTTP push delivery succeeded");
            } else {
                tracing::warn!(address = %address.url, status, "HTTP push delivery rejected by target");
            }
        }
        Err(e) => {
            tracing::warn!(
                address = %address.url,
                error = %e,
                error_kind = if e.is_connect() { "connect" } else if e.is_timeout() { "timeout" } else { "other" },
                "HTTP push delivery failed"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Router};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    struct MockTokenProvider {
        result: Result<String, String>,
        recorded_audience: Mutex<Option<String>>,
    }

    impl MockTokenProvider {
        fn ok(token: impl Into<String>) -> Self {
            Self {
                result: Ok(token.into()),
                recorded_audience: Mutex::new(None),
            }
        }

        fn err(msg: impl Into<String>) -> Self {
            Self {
                result: Err(msg.into()),
                recorded_audience: Mutex::new(None),
            }
        }
    }

    #[async_trait::async_trait]
    impl TokenProvider for MockTokenProvider {
        async fn get_token(&self, audience: &str) -> Result<String, String> {
            *self.recorded_audience.lock().unwrap() = Some(audience.to_string());
            self.result.clone()
        }
    }

    // Allow Arc<MockTokenProvider> to be boxed as dyn TokenProvider so tests can
    // retain a handle to read recorded_audience after the send.
    #[async_trait::async_trait]
    impl TokenProvider for Arc<MockTokenProvider> {
        async fn get_token(&self, audience: &str) -> Result<String, String> {
            self.as_ref().get_token(audience).await
        }
    }

    async fn spawn_capture_server() -> (String, mpsc::Receiver<axum::http::HeaderMap>) {
        let (tx, rx) = mpsc::channel::<axum::http::HeaderMap>(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tx = Arc::new(tx);

        let app = Router::new()
            .route("/", post(capture_handler))
            .with_state(tx);

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://127.0.0.1:{}", addr.port()), rx)
    }

    async fn capture_handler(
        State(tx): State<Arc<mpsc::Sender<axum::http::HeaderMap>>>,
        req: axum::extract::Request,
    ) -> axum::http::StatusCode {
        let _ = tx.send(req.headers().clone()).await;
        axum::http::StatusCode::OK
    }

    /// The transport holds a `ResonateServer` for the failure path it does not
    /// use yet; these tests never exercise it.
    struct NoopServer;

    #[async_trait]
    impl resonate_plugin::ResonateServer for NoopServer {
        async fn process(
            &self,
            _req: &resonate_plugin::types::RequestEnvelope,
        ) -> Result<resonate_plugin::types::ResponseEnvelope, Unavailable> {
            Err(Unavailable::new("NoopServer answers nothing"))
        }
    }

    async fn make_transport(auth: Auth) -> HttpPushTransport {
        let server: Arc<dyn resonate_plugin::ResonateServer> = Arc::new(NoopServer);
        let t = HttpPushTransport::new(
            Arc::downgrade(&server),
            Config {
                connect_timeout: 5_000,
                request_timeout: 5_000,
                concurrency: 16,
                ..Config::default()
            },
        );
        t.start(auth).await.expect("started");
        // The tests only exercise the transport, so nothing else holds the
        // server; keep it alive for the duration.
        std::mem::forget(server);
        t
    }

    #[tokio::test]
    async fn no_auth_omits_authorization_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::None)
            .await
            .send(&HttpAddress { url }, &serde_json::json!({}))
            .await
            .expect("enqueued");
        let headers = rx.recv().await.expect("server received no request");
        assert!(
            !headers.contains_key("authorization"),
            "expected no Authorization header but found one"
        );
    }

    #[tokio::test]
    async fn bearer_auth_sends_token_in_authorization_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::StaticBearer {
            header: "Authorization".to_string(),
            value: "Bearer secret-token".to_string(),
        })
        .await
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        let headers = rx.recv().await.expect("server received no request");
        assert_eq!(
            headers
                .get("authorization")
                .expect("expected Authorization header")
                .to_str()
                .unwrap(),
            "Bearer secret-token"
        );
    }

    #[tokio::test]
    async fn bearer_auth_with_custom_header_uses_that_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::StaticBearer {
            header: "X-Custom-Auth".to_string(),
            value: "Bearer custom-token".to_string(),
        })
        .await
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        let headers = rx.recv().await.expect("server received no request");
        assert_eq!(
            headers
                .get("x-custom-auth")
                .expect("expected X-Custom-Auth header")
                .to_str()
                .unwrap(),
            "Bearer custom-token",
        );
        assert!(
            !headers.contains_key("authorization"),
            "expected no standard Authorization header"
        );
    }

    #[tokio::test]
    async fn gcp_auth_fetches_token_and_sends_it() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            fixed_audience: None,
            provider: Box::new(MockTokenProvider::ok("mock-token")),
        })
        .await
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        assert_eq!(
            rx.recv()
                .await
                .expect("delivery target received no request")
                .get("authorization")
                .expect("expected Authorization header")
                .to_str()
                .unwrap(),
            "Bearer mock-token",
        );
    }

    #[tokio::test]
    async fn gcp_auth_fixed_audience_is_passed_to_provider() {
        let (url, mut rx) = spawn_capture_server().await;
        let mock = Arc::new(MockTokenProvider::ok("mock-token"));
        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            fixed_audience: Some("https://my-audience.example.com".to_string()),
            provider: Box::new(Arc::clone(&mock)),
        })
        .await
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        // send() spawns the request; wait for the server to receive it so the
        // mock's recorded_audience is populated before we assert on it.
        rx.recv()
            .await
            .expect("delivery target received no request");
        assert_eq!(
            mock.recorded_audience.lock().unwrap().as_deref(),
            Some("https://my-audience.example.com"),
        );
    }

    #[tokio::test]
    async fn gcp_auth_target_url_used_as_audience_when_none_configured() {
        let (url, mut rx) = spawn_capture_server().await;
        let mock = Arc::new(MockTokenProvider::ok("mock-token"));
        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            fixed_audience: None,
            provider: Box::new(Arc::clone(&mock)),
        })
        .await
        .send(&HttpAddress { url: url.clone() }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        rx.recv()
            .await
            .expect("delivery target received no request");
        assert_eq!(
            mock.recorded_audience.lock().unwrap().as_deref(),
            Some(url.as_str()),
        );
    }

    #[tokio::test]
    async fn gcp_auth_with_custom_header_sends_token_in_that_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::GcpIdToken {
            header: "X-Goog-Token".to_string(),
            fixed_audience: None,
            provider: Box::new(MockTokenProvider::ok("mock-token")),
        })
        .await
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        let headers = rx
            .recv()
            .await
            .expect("delivery target received no request");
        assert_eq!(
            headers
                .get("x-goog-token")
                .expect("expected X-Goog-Token header")
                .to_str()
                .unwrap(),
            "Bearer mock-token",
        );
        assert!(
            !headers.contains_key("authorization"),
            "expected no standard Authorization header"
        );
    }

    #[tokio::test]
    async fn gcp_auth_token_failure_sends_request_without_auth_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            fixed_audience: None,
            provider: Box::new(MockTokenProvider::err("simulated failure")),
        })
        .await
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await
        .expect("enqueued");
        let headers = rx
            .recv()
            .await
            .expect("delivery target received no request");
        assert!(
            !headers.contains_key("authorization"),
            "expected no Authorization header on token failure"
        );
    }

    // ─── The plugin ──────────────────────────────────────────────────────────

    fn no_server() -> resonate_plugin::WorkerDependencies {
        resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>
        )
    }

    fn settings(pairs: &[(&str, &str)]) -> resonate_plugin::Configuration {
        let mut loader = resonate_plugin::Loader::new();
        for (k, v) in pairs {
            loader = loader.set(k, v).unwrap();
        }
        loader.load()
    }

    #[test]
    fn a_section_nobody_wrote_gets_this_crate_s_defaults() {
        let config = settings(&[]);
        let worker = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()).unwrap();
        assert!(worker.is_some(), "push is on unless turned off");
        assert_eq!(PLUGIN.schemes, &["http", "https"]);
        assert_eq!(
            config.worker(&PLUGIN.id()).key(),
            "workers.transport_http_push"
        );
    }

    #[test]
    fn turning_it_off_is_its_own_setting() {
        let config = settings(&[("workers.transport_http_push.enabled", "false")]);
        assert!(
            (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn zero_concurrency_is_refused_at_startup() {
        // It used to be checked in the server's own config::validate, which
        // meant the rule lived nowhere near the semaphore it is about.
        let config = settings(&[("workers.transport_http_push.concurrency", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()) else {
            panic!("every message would queue and never leave");
        };
        assert_eq!(err.key, "workers.transport_http_push.concurrency");
        assert!(err.source.is_some(), "and says where the value came from");
    }
}
