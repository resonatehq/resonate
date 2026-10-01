//! `build` puts the server behind its roster: what a gateway is handed routes
//! each request where the roster says.

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use resonate_base::{build, Options};
use resonate_plugin::types::{RequestEnvelope, ResponseEnvelope};
use resonate_plugin::{
    Configured, GatewayPlugin, Loader, Peer, Registry, ResonateGateway, ResonateRoster,
    ResonateServer, Route, ServerPlugin, Unavailable,
};
use serde_json::{json, Value};

fn reply(req: &RequestEnvelope, by: &str) -> ResponseEnvelope {
    ResponseEnvelope::new(
        req.kind.clone(),
        req.head.corr_id.clone(),
        200,
        json!({ "by": by }),
    )
}

struct Server;

#[async_trait]
impl ResonateServer for Server {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        Ok(reply(req, "here"))
    }
}

/// Routes by the id's origin: `here` is this node, `there` is node `b`, and
/// anything else has no owner yet.
struct Roster;

#[async_trait]
impl ResonateRoster for Roster {
    fn me(&self) -> Peer {
        peer("a")
    }

    fn peers(&self) -> Vec<Peer> {
        vec![peer("b")]
    }

    fn route(&self, id: &str) -> Route {
        match id.split_once(':').map(|(o, _)| o).unwrap_or(id) {
            "here" => Route::Me,
            "there" => Route::Peer(peer("b")),
            _ => Route::Unknown,
        }
    }

    async fn forward(
        &self,
        to: &Peer,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable> {
        forwarded().lock().unwrap().push(req.kind.clone());
        Ok(reply(req, &to.name))
    }
}

fn peer(name: &str) -> Peer {
    Peer {
        name: name.to_string(),
        addr: format!("http://{name}"),
    }
}

fn forwarded() -> &'static Mutex<Vec<String>> {
    static FORWARDED: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    FORWARDED.get_or_init(Default::default)
}

/// The server a gateway was handed, kept so the test can call it as the
/// gateway would.
fn handed() -> &'static Mutex<Option<Arc<dyn ResonateServer>>> {
    static HANDED: OnceLock<Mutex<Option<Arc<dyn ResonateServer>>>> = OnceLock::new();
    HANDED.get_or_init(Default::default)
}

static SERVER: ServerPlugin = ServerPlugin::new("resonate-server-routed", |_settings, _deps| {
    Ok(Configured::new(Arc::new(Roster), Arc::new(Server)))
});

struct Gateway;

impl ResonateGateway for Gateway {}

static GATEWAY: GatewayPlugin = GatewayPlugin::new("resonate-gateway-keeps", |_settings, deps| {
    *handed().lock().unwrap() = Some(Arc::clone(&deps.server));
    Ok(Some(Arc::new(Gateway) as Arc<dyn ResonateGateway>))
});

fn req(kind: &str, data: Value) -> RequestEnvelope {
    serde_json::from_value(json!({
        "kind": kind,
        "head": { "corrId": "c", "version": resonate_plugin::types::SUPPORTED_VERSIONS[0] },
        "data": data,
    }))
    .unwrap()
}

#[tokio::test]
async fn the_server_a_gateway_is_handed_routes_through_the_roster() {
    let registry = Registry::new().server(&SERVER).gateway(&GATEWAY);
    let running = build(
        &registry,
        &Loader::new().load(),
        &Options::default().default_server("server_routed"),
    )
    .expect("builds");
    let server = handed()
        .lock()
        .unwrap()
        .clone()
        .expect("the gateway was built");

    let by = |r: Result<ResponseEnvelope, Unavailable>| r.unwrap().data["by"].clone();

    // Owned here: processed here.
    assert_eq!(
        by(server
            .process(&req("promise.get", json!({ "id": "here:1" })))
            .await),
        "here"
    );
    // Owned by b: forwarded to b, once.
    assert_eq!(
        by(server
            .process(&req("promise.create", json!({ "id": "there:1" })))
            .await),
        "b"
    );
    assert_eq!(
        *forwarded().lock().unwrap(),
        vec!["promise.create".to_string()]
    );
    // No owner known: no answer, which the gateway turns into a 503.
    assert!(server
        .process(&req("task.get", json!({ "id": "elsewhere:1" })))
        .await
        .is_err());
    // Naming no id: processed here.
    assert_eq!(
        by(server.process(&req("promise.search", json!({}))).await),
        "here"
    );

    // And `Running::server` is the same routed server.
    assert_eq!(
        by(running
            .server()
            .process(&req("task.get", json!({ "id": "there:2" })))
            .await),
        "b"
    );
}
