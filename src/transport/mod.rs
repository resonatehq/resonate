pub mod transport_airflow;
pub mod transport_exec_bash;
pub mod transport_gcps;
pub mod transport_http_poll;
pub mod transport_http_push;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::core::types::Message;
use crate::core::{scheme_of, ResonateRouter, ResonateWorker, Unavailable};

// ---- Router ----

/// Routes a message to the worker registered for its address scheme.
///
/// The router's knowledge of an address stops at the scheme: it reads the
/// scheme, looks up a worker, and hands over the untouched address string.
/// Registering a new scheme is therefore the whole cost of adding a worker —
/// nothing here, and nothing in `core`, has to change.
pub struct TransportDispatcher {
    workers: HashMap<String, Arc<dyn ResonateWorker>>,
}

impl TransportDispatcher {
    pub fn new(workers: HashMap<String, Arc<dyn ResonateWorker>>) -> Self {
        Self { workers }
    }
}

#[async_trait]
impl ResonateRouter for TransportDispatcher {
    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let scheme = scheme_of(address)
            .ok_or_else(|| Unavailable::new(format!("address is not a URI: {address}")))?;
        let worker = self.workers.get(&scheme).ok_or_else(|| {
            Unavailable::new(format!("no worker registered for scheme '{scheme}'"))
        })?;
        worker.send(address, msg).await
    }
}

// ---- Test stubs ----

#[cfg(test)]
pub mod stubs {
    use super::*;
    use crate::core::types::{RequestEnvelope, ResponseEnvelope};
    use crate::core::ResonateServer;
    use std::sync::Mutex;

    /// A server that answers nothing. Workers hold a `ResonateServer` handle
    /// for the failure path; tests that never exercise it can use this.
    pub struct NoopServer;

    #[async_trait]
    impl ResonateServer for NoopServer {
        async fn process(&self, _req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
            Err(Unavailable::new("NoopServer answers nothing"))
        }
    }

    /// Records every `(address, message)` pair it is asked to deliver.
    ///
    /// One stub serves every scheme now that workers share a trait — register
    /// it under whichever scheme the test is exercising.
    pub struct RecordingWorker {
        pub calls: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl RecordingWorker {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(vec![]),
            }
        }

        /// Delivered messages as `(address, serialized message)`.
        pub fn calls(&self) -> Vec<(String, serde_json::Value)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Default for RecordingWorker {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ResonateWorker for RecordingWorker {
        async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
            let value = serde_json::to_value(msg).expect("message serializes");
            self.calls
                .lock()
                .unwrap()
                .push((address.to_string(), value));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::stubs::*;
    use super::*;
    use crate::core::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};

    fn execute_msg() -> Message {
        Message::Execute(ExecuteMsg {
            kind: "execute".to_string(),
            head: MessageHead {
                server_url: "http://localhost:8001".to_string(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: "t1".to_string(),
                    version: 1,
                },
            },
        })
    }

    fn router_with(scheme: &str, stub: Arc<RecordingWorker>) -> TransportDispatcher {
        let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
        workers.insert(scheme.to_string(), stub);
        TransportDispatcher::new(workers)
    }

    fn empty_router() -> TransportDispatcher {
        TransportDispatcher::new(HashMap::new())
    }

    #[tokio::test]
    async fn routes_by_scheme_to_the_registered_worker() {
        for (scheme, address) in [
            ("http", "http://example.com/callback"),
            ("https", "https://example.com/secure"),
            ("poll", "poll://any@default"),
            ("gcps", "gcps://my-project/my-topic"),
            ("bash", "bash://docker/alpine"),
        ] {
            let stub = Arc::new(RecordingWorker::new());
            let router = router_with(scheme, stub.clone());
            router.route(address, &execute_msg()).await.unwrap();

            let calls = stub.calls();
            assert_eq!(calls.len(), 1, "for {address}");
            // The worker receives the address verbatim — the router does not
            // decompose it.
            assert_eq!(calls[0].0, address);
            assert_eq!(calls[0].1["kind"], "execute");
        }
    }

    #[tokio::test]
    async fn unregistered_scheme_is_reported_not_dropped() {
        for address in [
            "http://example.com/callback",
            "poll://any@default",
            "gcps://my-project/my-topic",
            "bash://docker/alpine",
        ] {
            let err = empty_router()
                .route(address, &execute_msg())
                .await
                .expect_err("no worker is registered");
            assert!(
                err.to_string().contains("no worker registered"),
                "for {address}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn non_uri_address_is_reported() {
        let stub = Arc::new(RecordingWorker::new());
        let router = router_with("http", stub.clone());
        let err = router
            .route("not a url", &execute_msg())
            .await
            .expect_err("not a URI");
        assert!(err.to_string().contains("not a URI"), "{err}");
        assert!(stub.calls().is_empty());
    }

    #[tokio::test]
    async fn a_worker_serves_every_address_of_its_scheme() {
        let stub = Arc::new(RecordingWorker::new());
        let router = router_with("poll", stub.clone());
        for address in ["poll://any@a", "poll://uni@b/id", "poll://malformed"] {
            router.route(address, &execute_msg()).await.unwrap();
        }
        let calls = stub.calls();
        assert_eq!(calls.len(), 3);
        // Including the malformed one: rejecting it is the worker's job, not
        // the router's.
        assert_eq!(calls[2].0, "poll://malformed");
    }
}
