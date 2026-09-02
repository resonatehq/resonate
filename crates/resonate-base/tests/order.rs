//! What `resonate-base` is for: building and starting three kinds of plugin in
//! the right order, exactly once each.
//!
//! These are the only things pinning that order. Everything else about it is
//! prose, and prose drifts — the double-`init` these tests now catch survived a
//! comment asserting it was impossible.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use resonate_base::{build, Options};
use resonate_plugin::types::{Message, RequestEnvelope, ResponseEnvelope};
use resonate_plugin::{
    Configuration, GatewayPlugin, Loader, Registry, ResonateGateway, ResonateRouter,
    ResonateServer, ResonateWorker, ServerPlugin, Unavailable, WorkerPlugin,
};

// --- what happened, in the order it happened ---------------------------------

/// Every `init` and `stop` any stub performs, appended as it happens.
///
/// Process-wide because a plugin is a `static` and its `configure` is a plain
/// `fn` — there is nowhere to hand a per-test handle. The tests take `lock()`
/// so they cannot interleave.
fn log() -> &'static Mutex<Vec<String>> {
    static LOG: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    LOG.get_or_init(Default::default)
}

/// Held for the whole of each test, so two of them cannot interleave into the
/// one shared log. Async-aware because a test holds it across `start`/`stop`.
fn lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Default::default)
}

fn record(what: &str) {
    log().lock().unwrap().push(what.to_string());
}

fn events() -> Vec<String> {
    log().lock().unwrap().clone()
}

fn reset() {
    log().lock().unwrap().clear();
    DISABLED.store(0, Ordering::SeqCst);
}

/// How many plugins turned themselves off, counted from the stubs' side.
static DISABLED: AtomicUsize = AtomicUsize::new(0);

// --- the stubs ---------------------------------------------------------------

struct Server;

#[async_trait::async_trait]
impl ResonateServer for Server {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        record("server.init");
        Ok(())
    }
    async fn stop(&self) -> Result<(), Unavailable> {
        record("server.stop");
        Ok(())
    }
    async fn process(&self, _req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        unreachable!("no request is made")
    }
}

struct Worker(&'static str);

#[async_trait::async_trait]
impl ResonateWorker for Worker {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        record(&format!("worker.{}.init", self.0));
        Ok(())
    }
    async fn stop(&self) -> Result<(), Unavailable> {
        record(&format!("worker.{}.stop", self.0));
        Ok(())
    }
    async fn process(&self, _address: &str, _msg: &Message) -> Result<(), Unavailable> {
        Ok(())
    }
}

struct Gateway(&'static str, bool);

#[async_trait::async_trait]
impl ResonateGateway for Gateway {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        record(&format!("gateway.{}.init", self.0));
        if self.1 {
            return Err(Unavailable::new("this one cannot bind"));
        }
        Ok(())
    }
    async fn stop(&self) -> Result<(), Unavailable> {
        record(&format!("gateway.{}.stop", self.0));
        Ok(())
    }
}

static SERVER: ServerPlugin = ServerPlugin::new("resonate-server-stub", |_settings, deps| {
    // The router exists and is still empty — that is what step 1 is for.
    let _: &Arc<dyn ResonateRouter> = &deps.router;
    Ok(Arc::new(Server) as Arc<dyn ResonateServer>)
});

/// Two schemes, one worker. The shape that used to start twice.
static TWO_SCHEMES: WorkerPlugin =
    WorkerPlugin::new("resonate-worker-two", &["one", "two"], |_settings, deps| {
        // The server exists by now, and the handle back is weak.
        assert!(deps.server.upgrade().is_some(), "the server exists");
        Ok(Some(Arc::new(Worker("two")) as Arc<dyn ResonateWorker>))
    });

static ONE_SCHEME: WorkerPlugin =
    WorkerPlugin::new("resonate-worker-one", &["solo"], |_settings, _deps| {
        Ok(Some(Arc::new(Worker("one")) as Arc<dyn ResonateWorker>))
    });

static OFF_WORKER: WorkerPlugin =
    WorkerPlugin::new("resonate-worker-off", &["off"], |_settings, _deps| {
        DISABLED.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    });

static GATEWAY: GatewayPlugin = GatewayPlugin::new("resonate-gateway-stub", |_settings, _deps| {
    Ok(Some(
        Arc::new(Gateway("stub", false)) as Arc<dyn ResonateGateway>
    ))
});

static OFF_GATEWAY: GatewayPlugin =
    GatewayPlugin::new("resonate-gateway-off", |_settings, _deps| {
        DISABLED.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    });

static BAD_GATEWAY: GatewayPlugin =
    GatewayPlugin::new("resonate-gateway-bad", |_settings, _deps| {
        Ok(Some(
            Arc::new(Gateway("bad", true)) as Arc<dyn ResonateGateway>
        ))
    });

fn config() -> Configuration {
    Loader::new().load()
}

fn options() -> Options {
    Options::default().default_server("server_stub")
}

// --- the tests ---------------------------------------------------------------

/// The regression. `resonate-transport-http-push` claims `http` and `https`, so
/// it is in the routing table twice; driving lifecycle from that table built
/// two reqwest clients and two dispatcher tasks and orphaned the first.
#[tokio::test]
async fn a_worker_with_two_schemes_starts_once() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new().server(&SERVER).worker(&TWO_SCHEMES);
    let running = build(&registry, &config(), &options()).expect("builds");
    running.start(false).await.expect("starts");

    let inits = events()
        .iter()
        .filter(|e| e.as_str() == "worker.two.init")
        .count();
    assert_eq!(inits, 1, "one plugin, one init: {:?}", events());

    running.stop(std::time::Duration::from_secs(5)).await;
    let stops = events()
        .iter()
        .filter(|e| e.as_str() == "worker.two.stop")
        .count();
    assert_eq!(stops, 1, "and one stop: {:?}", events());
}

/// Workers, then the server, then the gateways — because the server's `init`
/// arms a timer that routes, and a gateway must not accept what nothing behind
/// it can serve yet.
#[tokio::test]
async fn start_runs_workers_then_the_server_then_the_gateways() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new()
        .server(&SERVER)
        .worker(&ONE_SCHEME)
        .gateway(&GATEWAY);
    let running = build(&registry, &config(), &options()).expect("builds");
    running.start(false).await.expect("starts");

    assert_eq!(
        events(),
        vec!["worker.one.init", "server.init", "gateway.stub.init"]
    );
}

/// The server first — its timer is the only thing that can still hand it work —
/// then the workers, then the gateways. A gateway drains last because its
/// graceful shutdown can be waiting on a response only a worker's `stop`
/// releases.
#[tokio::test]
async fn stop_runs_the_server_then_the_workers_then_the_gateways() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new()
        .server(&SERVER)
        .worker(&ONE_SCHEME)
        .gateway(&GATEWAY);
    let running = build(&registry, &config(), &options()).expect("builds");
    running.start(false).await.expect("starts");
    log().lock().unwrap().clear();
    running.stop(std::time::Duration::from_secs(5)).await;

    assert_eq!(
        events(),
        vec!["server.stop", "worker.one.stop", "gateway.stub.stop"]
    );
}

/// A plugin that fails to start does not leave the ones before it running.
#[tokio::test]
async fn a_failed_start_stops_what_it_already_started() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new()
        .server(&SERVER)
        .worker(&ONE_SCHEME)
        .gateway(&BAD_GATEWAY);
    let running = build(&registry, &config(), &options()).expect("builds");
    let err = running.start(false).await.expect_err("the gateway refuses");

    // The failure names which gateway, because three of them bind three ports
    // and "a gateway failed to bind" does not say which to go and look at.
    assert!(err.contains("gateway_bad"), "{err}");

    let seen = events();
    assert!(seen.contains(&"server.stop".to_string()), "{seen:?}");
    assert!(seen.contains(&"worker.one.stop".to_string()), "{seen:?}");
}

/// A worker and a gateway can turn themselves off, and the server cannot —
/// there is one per binary, chosen by name.
#[tokio::test]
async fn a_plugin_that_turns_itself_off_is_neither_built_nor_started() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new()
        .server(&SERVER)
        .worker(&ONE_SCHEME)
        .worker(&OFF_WORKER)
        .gateway(&GATEWAY)
        .gateway(&OFF_GATEWAY);
    let running = build(&registry, &config(), &options()).expect("builds");
    running.start(false).await.expect("starts");

    assert_eq!(DISABLED.load(Ordering::SeqCst), 2, "both were asked");
    assert_eq!(
        events(),
        vec!["worker.one.init", "server.init", "gateway.stub.init"],
        "and neither was started"
    );
}

/// The selection error names what the binary actually carries. A backend that
/// is not compiled in is a different problem from one that is misspelled, and
/// neither may quietly fall through to a different backend — which is the bug
/// this whole registry exists to make impossible.
#[tokio::test]
async fn a_server_that_is_not_compiled_in_is_named_as_such() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new().server(&SERVER);
    // Asked for by name, so there is nothing to fall back to.
    let config = Loader::new()
        .set("servers.active", "server_cassandra")
        .expect("a key and a value")
        .load();
    let Err(err) = build(&registry, &config, &options()) else {
        panic!("a server this binary does not carry must not build");
    };
    assert!(err.contains("server_cassandra"), "{err}");
    assert!(err.contains("server_stub"), "{err}");
}

/// A binary carrying one server does not need to be told which it is — and
/// must not be defeated by a default naming a server it was built without.
#[tokio::test]
async fn one_server_is_its_own_default() {
    let _guard = lock().lock().await;
    reset();
    let registry = Registry::new().server(&SERVER);
    // The default names something else entirely, as it would in a build with
    // `--no-default-features --features postgres` against a sqlite default.
    let running = build(
        &registry,
        &config(),
        &Options::default().default_server("server_sqlite"),
    )
    .expect("the one server it carries is the one it uses");
    running.start(false).await.expect("starts");
    assert!(events().contains(&"server.init".to_string()));
}
