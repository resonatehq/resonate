use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::error::Result;
use crate::network::Network;

type Subscribers = Arc<RwLock<Vec<Box<dyn Fn(String) + Send + Sync>>>>;

/// Authentication for HTTP requests.
#[derive(Clone)]
pub enum HttpAuth {
    Bearer(String),
    Basic { username: String, password: String },
}

/// Network implementation that communicates with a Resonate server over HTTP.
///
/// - Requests are sent via `POST /api` (JSON envelope format).
/// - Incoming messages (execute/unblock) are received via SSE on `GET /poll/{group}/{id}`.
/// - Addresses use the `poll://` scheme: `poll://uni@group/id` and `poll://any@group/id`.
pub struct HttpNetwork {
    url: String,
    pid: String,
    group: String,
    unicast: String,
    anycast: String,
    auth: Option<HttpAuth>,
    client: reqwest::Client,
    subscribers: Subscribers,
    sse_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl HttpNetwork {
    /// Create a new HttpNetwork.
    ///
    /// - `url`: Base URL of the Resonate server (e.g. `http://localhost:8001`).
    /// - `pid`: Process ID for this worker (or generated).
    /// - `group`: Group name for routing (default: `"default"`).
    /// - `auth`: Optional authentication.
    pub fn new(
        url: String,
        pid: Option<String>,
        group: Option<String>,
        auth: Option<HttpAuth>,
    ) -> Self {
        let pid = pid.unwrap_or_else(uuid_no_dashes);
        let group = group.unwrap_or_else(|| "default".to_string());
        let unicast = format!("poll://uni@{}/{}", group, pid);
        let anycast = format!("poll://any@{}/{}", group, pid);

        // Strip trailing slash from url
        let url = url.trim_end_matches('/').to_string();

        Self {
            url,
            pid,
            group,
            unicast,
            anycast,
            auth,
            client: reqwest::Client::new(),
            subscribers: Arc::new(RwLock::new(Vec::new())),
            sse_handle: Mutex::new(None),
        }
    }

    /// Build an authenticated request.
    fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(HttpAuth::Bearer(token)) => builder.bearer_auth(token),
            Some(HttpAuth::Basic { username, password }) => {
                builder.basic_auth(username, Some(password))
            }
            None => builder,
        }
    }
}

#[async_trait::async_trait]
impl Network for HttpNetwork {
    fn pid(&self) -> &str {
        &self.pid
    }

    fn group(&self) -> &str {
        &self.group
    }

    fn unicast(&self) -> &str {
        &self.unicast
    }

    fn anycast(&self) -> &str {
        &self.anycast
    }

    /// Start the SSE listener for incoming messages from the server.
    async fn start(&self) -> Result<()> {
        let url = format!("{}/poll/{}/{}", self.url, self.group, self.pid);
        let subscribers = self.subscribers.clone();
        let client = self.client.clone();
        let auth = self.auth.clone();

        let handle = tokio::spawn(async move {
            loop {
                tracing::debug!(url = %url, "connecting to SSE endpoint");

                let mut request = client.get(&url);
                request = match &auth {
                    Some(HttpAuth::Bearer(token)) => request.bearer_auth(token),
                    Some(HttpAuth::Basic { username, password }) => {
                        request.basic_auth(username, Some(password))
                    }
                    None => request,
                };

                let response = match request.send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        tracing::warn!(error = %e, "SSE connection failed, retrying in 5s");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                if !response.status().is_success() {
                    tracing::warn!(status = %response.status(), "SSE endpoint returned error, retrying in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }

                tracing::info!(url = %url, "SSE connection established");

                // Read SSE stream
                let mut stream = response.bytes_stream();
                let mut buffer = String::new();

                use futures::StreamExt;
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(bytes) => {
                            let text = match std::str::from_utf8(&bytes) {
                                Ok(t) => t,
                                Err(_) => continue,
                            };
                            buffer.push_str(text);

                            // Parse SSE events from buffer
                            // SSE format: "data: <json>\n\n"
                            while let Some(pos) = buffer.find("\n\n") {
                                let event_block = buffer[..pos].to_string();
                                buffer = buffer[pos + 2..].to_string();

                                // Extract data lines
                                for line in event_block.lines() {
                                    if let Some(data) = line.strip_prefix("data:") {
                                        let data = data.trim();
                                        if data.is_empty() {
                                            continue;
                                        }
                                        tracing::debug!(direction = "sse_recv", body = %data, "http_network");
                                        let subs = subscribers.read().await;
                                        for cb in subs.iter() {
                                            cb(data.to_string());
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "SSE stream error, reconnecting");
                            break;
                        }
                    }
                }

                tracing::info!("SSE connection closed, reconnecting in 1s");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        *self.sse_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(handle) = self.sse_handle.lock().await.take() {
            handle.abort();
        }
        self.subscribers.write().await.clear();
        Ok(())
    }

    /// Send a request to the Resonate server via POST /api.
    async fn send(&self, req: String) -> Result<String> {
        tracing::debug!(direction = "http_send", body = %req, "http_network");

        let request = self
            .client
            .post(&format!("{}/api", self.url))
            .header("Content-Type", "application/json")
            .body(req);

        let request = self.apply_auth(request);

        let response = request.send().await?;

        let resp_str = response.text().await?;

        tracing::debug!(direction = "http_recv", body = %resp_str, "http_network");

        Ok(resp_str)
    }

    /// Register a callback for incoming SSE messages.
    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        let subscribers = self.subscribers.clone();
        tokio::spawn(async move {
            subscribers.write().await.push(callback);
        });
    }

    /// Resolve a target name to a poll:// anycast address.
    fn r#match(&self, target: &str) -> String {
        format!("poll://any@{}", target)
    }
}

fn uuid_no_dashes() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", now ^ 0xdeadbeef_cafebabe_u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_network_identity() {
        let net = HttpNetwork::new(
            "http://localhost:8001".to_string(),
            Some("mypid".to_string()),
            Some("mygroup".to_string()),
            None,
        );
        assert_eq!(net.pid(), "mypid");
        assert_eq!(net.group(), "mygroup");
        assert_eq!(net.unicast(), "poll://uni@mygroup/mypid");
        assert_eq!(net.anycast(), "poll://any@mygroup/mypid");
    }

    #[test]
    fn http_network_match_returns_poll_anycast() {
        let net = HttpNetwork::new(
            "http://localhost:8001".to_string(),
            None,
            None,
            None,
        );
        assert_eq!(net.r#match("my-target"), "poll://any@my-target");
    }

    #[test]
    fn http_network_strips_trailing_slash() {
        let net = HttpNetwork::new(
            "http://localhost:8001/".to_string(),
            Some("pid".to_string()),
            None,
            None,
        );
        assert_eq!(net.url, "http://localhost:8001");
    }

    #[test]
    fn http_network_default_group() {
        let net = HttpNetwork::new(
            "http://localhost:8001".to_string(),
            Some("pid1".to_string()),
            None,
            None,
        );
        assert_eq!(net.group(), "default");
        assert_eq!(net.unicast(), "poll://uni@default/pid1");
    }
}
