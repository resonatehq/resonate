//! The routing layer: each request goes where its roster says, once.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use resonate_plugin::types::{RequestEnvelope, ResponseEnvelope};
use resonate_plugin::{Peer, ResonateRoster, ResonateServer, Route, Routed, Unavailable};
use serde_json::{json, Value};

/// A server that answers 200 and says it was the one asked.
struct Here;

#[async_trait]
impl ResonateServer for Here {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            json!({ "by": "here" }),
        ))
    }
}

/// A roster that routes every id one way, and records what it forwarded.
struct Scripted {
    route: Route,
    forwarded: Mutex<Vec<(String, String)>>,
}

impl Scripted {
    fn new(route: Route) -> Arc<Self> {
        Arc::new(Self {
            route,
            forwarded: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl ResonateRoster for Scripted {
    fn me(&self) -> Peer {
        peer("me")
    }

    fn peers(&self) -> Vec<Peer> {
        Vec::new()
    }

    fn route(&self, _id: &str) -> Route {
        self.route.clone()
    }

    async fn forward(
        &self,
        to: &Peer,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable> {
        self.forwarded
            .lock()
            .unwrap()
            .push((to.name.clone(), req.kind.clone()));
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            json!({ "by": to.name }),
        ))
    }
}

fn peer(name: &str) -> Peer {
    Peer {
        name: name.to_string(),
        addr: format!("http://{name}"),
    }
}

fn req(kind: &str, data: Value) -> RequestEnvelope {
    serde_json::from_value(json!({
        "kind": kind,
        "head": { "corrId": "c", "version": resonate_plugin::types::SUPPORTED_VERSIONS[0] },
        "data": data,
    }))
    .unwrap()
}

async fn answered_by(roster: Arc<Scripted>, r: RequestEnvelope) -> Result<String, Unavailable> {
    let routed = Routed::new(Arc::new(Here), roster);
    let resp = routed.process(&r).await?;
    Ok(resp.data["by"].as_str().unwrap().to_string())
}

#[tokio::test]
async fn me_and_any_are_processed_here() {
    for route in [Route::Me, Route::Any] {
        let roster = Scripted::new(route);
        let by = answered_by(
            Arc::clone(&roster),
            req("promise.get", json!({ "id": "a:1" })),
        )
        .await
        .unwrap();
        assert_eq!(by, "here");
        assert!(roster.forwarded.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_peer_route_is_forwarded_once() {
    let roster = Scripted::new(Route::Peer(peer("b")));
    let by = answered_by(
        Arc::clone(&roster),
        req("task.acquire", json!({ "id": "a:1" })),
    )
    .await
    .unwrap();
    assert_eq!(by, "b");
    assert_eq!(
        *roster.forwarded.lock().unwrap(),
        vec![("b".to_string(), "task.acquire".to_string())]
    );
}

#[tokio::test]
async fn an_unknown_owner_is_no_answer() {
    let roster = Scripted::new(Route::Unknown);
    let err = answered_by(roster, req("schedule.get", json!({ "id": "nightly" })))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("nightly"), "{err}");
}

#[tokio::test]
async fn a_request_with_no_id_is_answered_here_whatever_the_roster_says() {
    for r in [
        req("promise.search", json!({ "limit": 10 })),
        req("debug.snap", json!({})),
        req("promise.get", json!({})),
    ] {
        let roster = Scripted::new(Route::Peer(peer("b")));
        let by = answered_by(Arc::clone(&roster), r).await.unwrap();
        assert_eq!(by, "here");
        assert!(roster.forwarded.lock().unwrap().is_empty());
    }
}
