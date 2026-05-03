//! HTTP transport — send messages via HTTP POST to webhook URLs.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use reqwest::Client;

use super::HttpAddress;
use crate::config::{HttpPushAuthConfig, HttpPushAuthMode};
use crate::metrics;

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
    pub fn from_config(config: &HttpPushAuthConfig) -> Self {
        match config.mode {
            HttpPushAuthMode::None => Auth::None,
            HttpPushAuthMode::Bearer => {
                let token = config.token.clone().unwrap_or_default();
                Auth::StaticBearer {
                    header: config.header.clone(),
                    value: format!("Bearer {token}"),
                }
            }
            HttpPushAuthMode::Gcp => Auth::GcpIdToken {
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

pub struct HttpPushTransport {
    client: Client,
    auth: Auth,
}

impl HttpPushTransport {
    pub fn new(connect_timeout: Duration, request_timeout: Duration, auth: Auth) -> Self {
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .expect("failed to build HTTP client");
        Self { client, auth }
    }

    pub async fn send(&self, address: &HttpAddress, payload: &serde_json::Value) {
        let auth_header = self.auth.resolve(&address.url).await;

        let mut request = self
            .client
            .post(&address.url)
            .header("Content-Type", "application/json")
            .json(payload);

        if let Some((name, value)) = auth_header {
            request = request.header(name, value);
        }

        match request.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if resp.status().is_success() {
                    tracing::debug!(address = %address.url, status, "HTTP push delivery succeeded");
                    metrics::DELIVERIES_TOTAL
                        .with_label_values(&["success"])
                        .inc();
                } else {
                    tracing::warn!(address = %address.url, status, "HTTP push delivery rejected by target");
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::HttpAddress;
    use axum::{extract::State, routing::post, Router};
    use std::sync::Arc;
    use std::time::Duration;
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

    fn make_transport(auth: Auth) -> HttpPushTransport {
        HttpPushTransport::new(Duration::from_secs(5), Duration::from_secs(5), auth)
    }

    #[tokio::test]
    async fn no_auth_omits_authorization_header() {
        let (url, mut rx) = spawn_capture_server().await;
        make_transport(Auth::None)
            .send(&HttpAddress { url }, &serde_json::json!({}))
            .await;
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
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
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
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
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
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
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
        let (url, _rx) = spawn_capture_server().await;
        let mock = Arc::new(MockTokenProvider::ok("mock-token"));
        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            fixed_audience: Some("https://my-audience.example.com".to_string()),
            provider: Box::new(Arc::clone(&mock)),
        })
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
        assert_eq!(
            mock.recorded_audience.lock().unwrap().as_deref(),
            Some("https://my-audience.example.com"),
        );
    }

    #[tokio::test]
    async fn gcp_auth_target_url_used_as_audience_when_none_configured() {
        let (url, _rx) = spawn_capture_server().await;
        let mock = Arc::new(MockTokenProvider::ok("mock-token"));
        make_transport(Auth::GcpIdToken {
            header: "Authorization".to_string(),
            fixed_audience: None,
            provider: Box::new(Arc::clone(&mock)),
        })
        .send(&HttpAddress { url: url.clone() }, &serde_json::json!({}))
        .await;
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
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
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
        .send(&HttpAddress { url }, &serde_json::json!({}))
        .await;
        let headers = rx
            .recv()
            .await
            .expect("delivery target received no request");
        assert!(
            !headers.contains_key("authorization"),
            "expected no Authorization header on token failure"
        );
    }
}
