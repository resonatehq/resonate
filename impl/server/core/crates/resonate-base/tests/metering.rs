use std::sync::{Arc, Weak};

use resonate_base::{build, Options};
use resonate_plugin::types::{Message, RequestEnvelope, RequestHead, ResponseEnvelope};
use resonate_plugin::{
    Configuration, GatewayPlugin, Loader, Registry, ResonateGateway, ResonateServer, ResonateWorker,
    ServerPlugin, Unavailable, WorkerPlugin,
};

fn request() -> RequestEnvelope {
    RequestEnvelope {
        kind: "promise.get".into(),
        head: RequestHead {
            corr_id: "metering".into(),
            version: "v".into(),
            auth: None,
            debug_time: None,
        },
        data: serde_json::json!({}),
    }
}

struct Server;

#[async_trait::async_trait]
impl ResonateServer for Server {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            serde_json::json!({}),
        ))
    }
}

struct Gateway(Arc<dyn ResonateServer>);

#[async_trait::async_trait]
impl ResonateGateway for Gateway {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        self.0.process(&request()).await?;
        Ok(())
    }
}

struct Worker(Weak<dyn ResonateServer>);

#[async_trait::async_trait]
impl ResonateWorker for Worker {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        self.0.upgrade().unwrap().process(&request()).await?;
        Ok(())
    }

    async fn process(&self, _address: &str, _msg: &Message) -> Result<(), Unavailable> {
        Ok(())
    }
}

static SERVER: ServerPlugin = ServerPlugin::new("resonate-server-test", |_, _| Ok(Arc::new(Server)));
static WORKER: WorkerPlugin = WorkerPlugin::new("resonate-worker-test", &["test"], |_, deps| {
    Ok(Some(Arc::new(Worker(deps.server))))
});
static HTTP: GatewayPlugin = GatewayPlugin::new("resonate-gateway-http-test", |_, deps| {
    Ok(Some(Arc::new(Gateway(deps.server))))
});
static CONSOLE: GatewayPlugin = GatewayPlugin::new("resonate-gateway-console-test", |_, deps| {
    Ok(Some(Arc::new(Gateway(deps.server))))
});

#[tokio::test]
async fn composition_meters_gateway_worker_and_direct_requests_once() {
    let registry = Registry::new()
        .server(&SERVER)
        .worker(&WORKER)
        .gateway(&HTTP)
        .gateway(&CONSOLE);
    let config: Configuration = Loader::new().load();
    let running = build(&registry, &config, &Options::default()).unwrap();
    running.start(false).await.unwrap();
    running.server().process(&request()).await.unwrap();
    let families = resonate_plugin::prometheus::gather();
    let requests = families
        .iter()
        .find(|f| f.get_name() == "resonate_server_requests_total")
        .unwrap();
    assert_eq!(requests.get_metric().len(), 1);
    assert_eq!(requests.get_metric()[0].get_counter().get_value(), 4.0);
    running.stop(std::time::Duration::from_secs(1)).await;
}
