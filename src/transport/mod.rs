pub mod transport_exec_bash;
pub mod transport_gcps;
pub mod transport_http_poll;
pub mod transport_http_push;

use std::sync::Arc;

use async_trait::async_trait;

use crate::core::address::{parse_address, Address, GcpsAddress, HttpAddress, PollAddress};

// ---- Per-transport traits ----

#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn send(&self, addr: &HttpAddress, payload: &serde_json::Value);
}

#[async_trait]
pub trait PollTransport: Send + Sync {
    async fn send(&self, addr: &PollAddress, payload: &str) -> bool;
}

#[async_trait]
pub trait GcpsTransport: Send + Sync {
    async fn send(&self, addr: &GcpsAddress, payload: &serde_json::Value);
}

#[async_trait]
pub trait BashTransport: Send + Sync {
    async fn send(&self, address: &str, payload: &serde_json::Value);
}

// ---- Trait impls for real transport types ----

#[async_trait]
impl HttpTransport for transport_http_push::HttpPushTransport {
    async fn send(&self, addr: &HttpAddress, payload: &serde_json::Value) {
        transport_http_push::HttpPushTransport::send(self, addr, payload).await
    }
}

#[async_trait]
impl PollTransport for transport_http_poll::PollRegistry {
    async fn send(&self, addr: &PollAddress, payload: &str) -> bool {
        transport_http_poll::PollRegistry::send_poll(self, addr, payload).await
    }
}

#[async_trait]
impl GcpsTransport for transport_gcps::GcpsPubSubTransport {
    async fn send(&self, addr: &GcpsAddress, payload: &serde_json::Value) {
        transport_gcps::GcpsPubSubTransport::send(self, addr, payload).await
    }
}

#[async_trait]
impl BashTransport for transport_exec_bash::BashExecTransport {
    async fn send(&self, address: &str, payload: &serde_json::Value) {
        transport_exec_bash::BashExecTransport::send(self, address, payload).await
    }
}

// ---- Dispatcher ----

/// Dispatches messages to the appropriate transport by parsing the address once.
/// Routes by URL scheme: http/https → push, poll → SSE, gcps → GCP Pub/Sub, bash → bash exec.
pub struct TransportDispatcher {
    http: Option<Arc<dyn HttpTransport>>,
    poll: Option<Arc<dyn PollTransport>>,
    gcps: Option<Arc<dyn GcpsTransport>>,
    bash: Option<Arc<dyn BashTransport>>,
}

impl TransportDispatcher {
    pub fn new(
        http: Option<Arc<dyn HttpTransport>>,
        poll: Option<Arc<dyn PollTransport>>,
        gcps: Option<Arc<dyn GcpsTransport>>,
        bash: Option<Arc<dyn BashTransport>>,
    ) -> Self {
        Self {
            http,
            poll,
            gcps,
            bash,
        }
    }

    /// Parse the address, route to the correct transport, deliver.
    pub async fn send(&self, address: &str, payload: &serde_json::Value) {
        let kind = payload
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        match parse_address(address) {
            Some(Address::Http(addr)) => match &self.http {
                Some(http) => {
                    tracing::debug!(transport = "http", address = %addr.url, kind = kind, "Dispatching message via HTTP push");
                    http.send(&addr, payload).await;
                }
                None => {
                    tracing::warn!(address = %address, "HTTP push transport disabled, message dropped")
                }
            },
            Some(Address::Poll(addr)) => match &self.poll {
                Some(poll) => {
                    tracing::debug!(transport = "poll", group = %addr.group, kind = kind, "Dispatching message via poll/SSE");
                    let sse_data = serde_json::to_string(payload).unwrap_or_default();
                    poll.send(&addr, &sse_data).await;
                }
                None => {
                    tracing::warn!(address = %address, "HTTP poll transport disabled, message dropped")
                }
            },
            Some(Address::Gcps(addr)) => match &self.gcps {
                Some(gcps) => {
                    tracing::debug!(transport = "gcps", project = %addr.project, topic = %addr.topic, kind = kind, "Dispatching message via GCP Pub/Sub");
                    gcps.send(&addr, payload).await;
                }
                None => {
                    tracing::warn!(address = %address, "GCP Pub/Sub transport not configured, message dropped")
                }
            },
            Some(Address::Bash(_)) => match &self.bash {
                Some(bash) => {
                    tracing::debug!(
                        transport = "bash",
                        address,
                        kind,
                        "Dispatching message via bash exec"
                    );
                    bash.send(address, payload).await;
                }
                None => {
                    tracing::warn!(address = %address, "Bash exec transport not configured, message dropped")
                }
            },
            None => {
                tracing::warn!(address = %address, "Invalid address, message cannot be routed");
            }
        }
    }
}

// ---- Test stubs ----

#[cfg(test)]
pub mod stubs {
    use super::*;
    use std::sync::Mutex;

    /// Records every (url, payload) pair delivered via HTTP push.
    pub struct RecordingHttpTransport {
        pub calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl RecordingHttpTransport {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(vec![]),
            }
        }

        pub fn calls(&self) -> Vec<(String, serde_json::Value)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HttpTransport for RecordingHttpTransport {
        async fn send(&self, addr: &HttpAddress, payload: &serde_json::Value) {
            self.calls
                .lock()
                .unwrap()
                .push((addr.url.clone(), payload.clone()));
        }
    }

    /// Records every (group, payload_str) pair delivered via poll/SSE.
    pub struct RecordingPollTransport {
        pub calls: Mutex<Vec<(String, String)>>,
    }

    impl RecordingPollTransport {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(vec![]),
            }
        }

        pub fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PollTransport for RecordingPollTransport {
        async fn send(&self, addr: &PollAddress, payload: &str) -> bool {
            self.calls
                .lock()
                .unwrap()
                .push((addr.group.clone(), payload.to_string()));
            true
        }
    }

    /// Records every (project/topic, payload) pair delivered via GCP Pub/Sub.
    pub struct RecordingGcpsTransport {
        pub calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl RecordingGcpsTransport {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(vec![]),
            }
        }

        pub fn calls(&self) -> Vec<(String, serde_json::Value)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl GcpsTransport for RecordingGcpsTransport {
        async fn send(&self, addr: &GcpsAddress, payload: &serde_json::Value) {
            let key = format!("{}/{}", addr.project, addr.topic);
            self.calls.lock().unwrap().push((key, payload.clone()));
        }
    }

    /// Records every (address, payload) pair delivered via bash exec.
    pub struct RecordingBashTransport {
        pub calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl RecordingBashTransport {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(vec![]),
            }
        }

        pub fn calls(&self) -> Vec<(String, serde_json::Value)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl BashTransport for RecordingBashTransport {
        async fn send(&self, address: &str, payload: &serde_json::Value) {
            self.calls
                .lock()
                .unwrap()
                .push((address.to_string(), payload.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::stubs::*;
    use super::*;
    use serde_json::json;

    fn payload(kind: &str) -> serde_json::Value {
        json!({ "kind": kind, "head": {}, "data": {} })
    }

    fn dispatcher_with_http(stub: Arc<RecordingHttpTransport>) -> TransportDispatcher {
        TransportDispatcher::new(Some(stub as Arc<dyn HttpTransport>), None, None, None)
    }

    fn dispatcher_with_poll(stub: Arc<RecordingPollTransport>) -> TransportDispatcher {
        TransportDispatcher::new(None, Some(stub as Arc<dyn PollTransport>), None, None)
    }

    fn dispatcher_with_gcps(stub: Arc<RecordingGcpsTransport>) -> TransportDispatcher {
        TransportDispatcher::new(None, None, Some(stub as Arc<dyn GcpsTransport>), None)
    }

    fn dispatcher_with_bash(stub: Arc<RecordingBashTransport>) -> TransportDispatcher {
        TransportDispatcher::new(None, None, None, Some(stub as Arc<dyn BashTransport>))
    }

    // ---- http_push ----

    #[tokio::test]
    async fn http_push_enabled_routes_to_stub() {
        let stub = Arc::new(RecordingHttpTransport::new());
        let dispatcher = dispatcher_with_http(stub.clone());
        dispatcher
            .send("http://example.com/callback", &payload("execute"))
            .await;
        let calls = stub.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "http://example.com/callback");
        assert_eq!(calls[0].1["kind"], "execute");
    }

    #[tokio::test]
    async fn http_push_disabled_drops_without_error() {
        let dispatcher = TransportDispatcher::new(None, None, None, None);
        // Should return without panicking — message silently dropped
        dispatcher
            .send("http://example.com/callback", &payload("execute"))
            .await;
    }

    #[tokio::test]
    async fn https_push_enabled_routes_to_stub() {
        let stub = Arc::new(RecordingHttpTransport::new());
        let dispatcher = dispatcher_with_http(stub.clone());
        dispatcher
            .send("https://example.com/secure", &payload("execute"))
            .await;
        let calls = stub.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "https://example.com/secure");
    }

    // ---- http_poll ----

    #[tokio::test]
    async fn http_poll_enabled_routes_to_stub() {
        let stub = Arc::new(RecordingPollTransport::new());
        let dispatcher = dispatcher_with_poll(stub.clone());
        dispatcher
            .send("poll://any@default", &payload("execute"))
            .await;
        let calls = stub.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "default");
        let body: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(body["kind"], "execute");
    }

    #[tokio::test]
    async fn http_poll_disabled_drops_without_error() {
        let dispatcher = TransportDispatcher::new(None, None, None, None);
        dispatcher
            .send("poll://any@default", &payload("execute"))
            .await;
    }

    // ---- gcps ----

    #[tokio::test]
    async fn gcps_enabled_routes_to_stub() {
        let stub = Arc::new(RecordingGcpsTransport::new());
        let dispatcher = dispatcher_with_gcps(stub.clone());
        dispatcher
            .send("gcps://my-project/my-topic", &payload("execute"))
            .await;
        let calls = stub.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "my-project/my-topic");
        assert_eq!(calls[0].1["kind"], "execute");
    }

    #[tokio::test]
    async fn gcps_disabled_drops_without_error() {
        let dispatcher = TransportDispatcher::new(None, None, None, None);
        dispatcher
            .send("gcps://my-project/my-topic", &payload("execute"))
            .await;
    }

    // ---- bash ----

    #[tokio::test]
    async fn bash_enabled_routes_to_stub() {
        let stub = Arc::new(RecordingBashTransport::new());
        let dispatcher = dispatcher_with_bash(stub.clone());
        // bash:// is the inline-script address scheme
        dispatcher.send("bash://", &payload("execute")).await;
        let calls = stub.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "bash://");
        assert_eq!(calls[0].1["kind"], "execute");
    }

    #[tokio::test]
    async fn bash_disabled_drops_without_error() {
        let dispatcher = TransportDispatcher::new(None, None, None, None);
        dispatcher.send("bash://", &payload("execute")).await;
    }
}
