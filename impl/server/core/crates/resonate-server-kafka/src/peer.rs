//! Node to node: the channel the roster forwards over.
//!
//! # Contract
//!
//! Any node takes any request. The routing layer in front of the node asks
//! the roster where the request's partition is served and, if that is another
//! node, the roster carries it there — once. A request that arrives here is
//! served locally or refused with a 503; it is never forwarded again, so two
//! nodes with briefly different views of the directory cannot bounce a
//! request between them. The client's retry covers the moment in between, as
//! it does a rebalance.
//!
//! Three calls cross between nodes: a protocol request ([`Peers::process`]),
//! a schedule firing into another partition's origin ([`Peers::fire`]) — not a
//! protocol operation, so it has its own call — and one owner's part of a
//! search ([`Peers::search`]).
//!
//! The listener is internal: it is bound separately from the gateway, is meant
//! for the cluster's own network, and trusts what arrives because the gateway
//! that first took the request already authenticated it. A shared token, when
//! configured, keeps anything else from speaking to it.
//!
//! # Dependencies
//!
//! axum (through `resonate-plugin`) for the listener, reqwest for the client.
//!
//! # Dependants
//!
//! The node routes through [`Peers`]; the plugin binds [`serve`].

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use resonate_core::types::{
    PromiseValue, RequestEnvelope, RequestHead, ResponseEnvelope, ResponseHead,
};
use resonate_core::Unavailable;
use resonate_plugin::axum;

use crate::directory::Owner;
use crate::node::Node;

/// A schedule's occurrence, created in the partition that owns its origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fire {
    pub id: String,
    pub timeout_at: i64,
    pub param: PromiseValue,
    pub tags: BTreeMap<String, String>,
    pub fired_at: i64,
    pub now: i64,
}

/// One owner's part of a search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Search {
    pub kind: String,
    pub data: Value,
    pub now: i64,
}

/// Reaching another node.
#[async_trait]
pub trait Peers: Send + Sync {
    async fn process(
        &self,
        to: &Owner,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable>;
    async fn fire(&self, to: &Owner, fire: &Fire) -> Result<(), Unavailable>;
    async fn search(
        &self,
        to: &Owner,
        search: &Search,
    ) -> Result<Vec<(String, Value)>, Unavailable>;
}

/// The request envelope as it goes over the wire. The core type only
/// deserializes, since nothing else ever sends one.
fn envelope_json(req: &RequestEnvelope) -> Value {
    let mut head = json!({
        "corrId": req.head.corr_id,
        "version": req.head.version,
    });
    if let Some(auth) = &req.head.auth {
        head["auth"] = json!(auth);
    }
    if let Some(t) = req.head.debug_time {
        head["resonate:debug_time"] = json!(t);
    }
    json!({ "kind": req.kind, "head": head, "data": req.data })
}

#[derive(Deserialize)]
struct WireResponse {
    kind: String,
    head: WireResponseHead,
    data: Value,
}

#[derive(Deserialize)]
struct WireResponseHead {
    #[serde(rename = "corrId")]
    corr_id: String,
    status: i32,
    version: String,
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

const TOKEN_HEADER: &str = "x-resonate-peer-token";

/// Peers over HTTP.
pub struct HttpPeers {
    client: reqwest::Client,
    token: Option<String>,
}

impl HttpPeers {
    pub fn new(timeout: Duration, token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            // A peer that is gone should fail fast, not after `timeout`: the
            // directory is refreshed on failure and the client retries.
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("an HTTP client builds");
        Self { client, token }
    }

    async fn post<T: Serialize + ?Sized>(
        &self,
        to: &Owner,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response, Unavailable> {
        let url = format!("{}{path}", to.peer_url.trim_end_matches('/'));
        let mut req = self.client.post(&url).json(body);
        if let Some(token) = &self.token {
            req = req.header(TOKEN_HEADER, token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| Unavailable::new(format!("peer {} unreachable: {e}", to.node)))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(Unavailable::new(format!(
                "peer {} answered {status}: {text}",
                to.node
            )));
        }
        Ok(resp)
    }
}

#[async_trait]
impl Peers for HttpPeers {
    async fn process(
        &self,
        to: &Owner,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable> {
        let resp: WireResponse = self
            .post(to, "/peer/process", &envelope_json(req))
            .await?
            .json()
            .await
            .map_err(|e| Unavailable::new(format!("peer {} answered garbage: {e}", to.node)))?;
        Ok(ResponseEnvelope {
            kind: resp.kind,
            head: ResponseHead {
                corr_id: resp.head.corr_id,
                status: resp.head.status,
                version: resp.head.version,
            },
            data: resp.data,
        })
    }

    async fn fire(&self, to: &Owner, fire: &Fire) -> Result<(), Unavailable> {
        self.post(to, "/peer/fire", fire).await.map(|_| ())
    }

    async fn search(
        &self,
        to: &Owner,
        search: &Search,
    ) -> Result<Vec<(String, Value)>, Unavailable> {
        self.post(to, "/peer/search", search)
            .await?
            .json()
            .await
            .map_err(|e| Unavailable::new(format!("peer {} answered garbage: {e}", to.node)))
    }
}

#[derive(Clone)]
struct State {
    node: Weak<Node>,
    token: Option<String>,
}

type HttpError = (axum::http::StatusCode, String);

impl State {
    fn check(&self, headers: &axum::http::HeaderMap) -> Result<Arc<Node>, HttpError> {
        if let Some(want) = &self.token {
            let got = headers.get(TOKEN_HEADER).and_then(|v| v.to_str().ok());
            if got != Some(want.as_str()) {
                return Err((
                    axum::http::StatusCode::UNAUTHORIZED,
                    "bad peer token".into(),
                ));
            }
        }
        self.node.upgrade().ok_or((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "node stopped".into(),
        ))
    }
}

fn unavailable(e: Unavailable) -> HttpError {
    (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.message)
}

/// The peer listener's routes.
pub fn router(node: Weak<Node>, token: Option<String>) -> axum::Router {
    use axum::extract::State as S;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::Json;

    async fn process(
        S(state): S<State>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Result<Json<ResponseEnvelope>, HttpError> {
        let node = state.check(&headers)?;
        let req: RequestEnvelope = serde_json::from_value(body)
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
        node.serve(&req).await.map(Json).map_err(unavailable)
    }

    async fn fire(
        S(state): S<State>,
        headers: HeaderMap,
        Json(fire): Json<Fire>,
    ) -> Result<Json<Value>, HttpError> {
        let node = state.check(&headers)?;
        node.fire_forwarded(fire).await.map_err(unavailable)?;
        Ok(Json(json!({})))
    }

    async fn search(
        S(state): S<State>,
        headers: HeaderMap,
        Json(search): Json<Search>,
    ) -> Result<Json<Vec<(String, Value)>>, HttpError> {
        let node = state.check(&headers)?;
        node.search_forwarded(&search)
            .map(Json)
            .map_err(unavailable)
    }

    axum::Router::new()
        .route("/peer/process", post(process))
        .route("/peer/fire", post(fire))
        .route("/peer/search", post(search))
        .with_state(State { node, token })
}

/// Bind the peer listener and serve it until `shutdown` fires.
pub async fn serve(
    bind: &str,
    node: Weak<Node>,
    token: Option<String>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<tokio::task::JoinHandle<()>, Unavailable> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| Unavailable::new(format!("cannot bind the peer listener on {bind}: {e}")))?;
    let app = router(node, token);
    Ok(tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await;
    }))
}

// ---------------------------------------------------------------------------
// In process
// ---------------------------------------------------------------------------

/// Peers in the same process, by node id — for tests that run a cluster
/// without sockets.
#[derive(Default)]
pub struct LocalPeers {
    nodes: RwLock<HashMap<String, Weak<Node>>>,
}

impl LocalPeers {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, node: &Arc<Node>) {
        self.nodes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node.id().to_string(), Arc::downgrade(node));
    }

    fn get(&self, to: &Owner) -> Result<Arc<Node>, Unavailable> {
        self.nodes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&to.node)
            .and_then(Weak::upgrade)
            .ok_or_else(|| Unavailable::new(format!("peer {} is not running", to.node)))
    }
}

#[async_trait]
impl Peers for LocalPeers {
    async fn process(
        &self,
        to: &Owner,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable> {
        // Through the wire format, so the in-process path proves the HTTP one.
        let req: RequestEnvelope = serde_json::from_value(envelope_json(req))
            .map_err(|e| Unavailable::new(e.to_string()))?;
        self.get(to)?.serve(&req).await
    }

    async fn fire(&self, to: &Owner, fire: &Fire) -> Result<(), Unavailable> {
        self.get(to)?.fire_forwarded(fire.clone()).await
    }

    async fn search(
        &self,
        to: &Owner,
        search: &Search,
    ) -> Result<Vec<(String, Value)>, Unavailable> {
        self.get(to)?.search_forwarded(search)
    }
}

/// A request head, for callers that build envelopes in code.
pub fn head(corr_id: &str, debug_time: Option<i64>) -> RequestHead {
    RequestHead {
        corr_id: corr_id.to_string(),
        version: resonate_core::types::SUPPORTED_VERSIONS[0].to_string(),
        auth: None,
        debug_time,
    }
}
