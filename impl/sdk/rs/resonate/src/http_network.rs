use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::network::Network;

type Subscribers = Arc<RwLock<Vec<Box<dyn Fn(String) + Send + Sync>>>>;

/// Network implementation that communicates with a Resonate server over HTTP.
///
/// - Requests are sent via `POST /` (JSON envelope format).
/// - Incoming messages (execute/unblock) are received via SSE on `GET /poll/{group}/{id}`.
/// - Addresses use the `poll://` scheme: `poll://uni@group/id` and `poll://any@group/id`.
pub struct HttpNetwork {
    url: String,
    pid: String,
    group: String,
    unicast: String,
    anycast: String,
    auth: Option<String>,
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
        auth: Option<String>,
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
            Some(token) => builder.bearer_auth(token),
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
            let mut backoff_secs = 1u64;
            const MAX_BACKOFF_SECS: u64 = 60;

            loop {
                tracing::debug!(url = %url, "connecting to SSE endpoint");

                let mut request = client.get(&url);
                request = match &auth {
                    Some(token) => request.bearer_auth(token),
                    None => request,
                };

                let response = match request.send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        tracing::warn!(error = %e, backoff = backoff_secs, "SSE connection failed, retrying");
                        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        continue;
                    }
                };

                if !response.status().is_success() {
                    tracing::warn!(status = %response.status(), backoff = backoff_secs, "SSE endpoint returned error, retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }

                // Connection succeeded, reset backoff.
                backoff_secs = 1;
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
                                buffer.drain(..pos + 2);

                                // Extract data lines
                                for line in event_block.lines() {
                                    if let Some(data) = line.strip_prefix("data:") {
                                        let data = data.trim();
                                        if data.is_empty() {
                                            continue;
                                        }
                                        tracing::debug!(direction = "sse_recv", body = %data, "http_network");
                                        let subs = subscribers.read();
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

                tracing::info!(
                    backoff = backoff_secs,
                    "SSE connection closed, reconnecting"
                );
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
        });

        *self.sse_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(handle) = self.sse_handle.lock().await.take() {
            handle.abort();
        }
        self.subscribers.write().clear();
        Ok(())
    }

    /// Send a request to the Resonate server via POST /api.
    async fn send(&self, req: String) -> Result<String> {
        tracing::debug!(direction = "http_req", body = %req, "http_network");

        let request = self
            .client
            .post(format!("{}/", self.url))
            .header("Content-Type", "application/json")
            .body(req);

        let request = self.apply_auth(request);

        let response = request.send().await?;

        let resp_str = response.text().await?;

        tracing::debug!(direction = "http_res", body = %resp_str, "http_network");

        Ok(resp_str)
    }

    /// Register a callback for incoming SSE messages.
    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        let subscribers = self.subscribers.clone();
        subscribers.write().push(callback);
    }

    /// Resolve a target name to a poll:// anycast address.
    fn target_resolver(&self, target: &str) -> String {
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
        let net = HttpNetwork::new("http://localhost:8001".to_string(), None, None, None);
        assert_eq!(net.target_resolver("my-target"), "poll://any@my-target");
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
