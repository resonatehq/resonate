//! Test support — helpers and doubles shared by unit tests, integration tests
//! and the differential harness.
//!
//! Two rules govern everything in here.
//!
//! **Helpers never return errors.** They panic with a message that names what
//! went wrong. A caller writes `let server = server();`, not
//! `let server = server().expect("...")`, so a test reads as a sequence of
//! steps rather than a chain of `?` and `unwrap`.
//!
//! **It is compiled into the library, not gated behind `cfg(test)`.** A
//! `cfg(test)` module is invisible to integration tests (`diff/`, `tests/`),
//! which is why the differential harness used to carry its own copy of the
//! same three helpers. Everything here is available to every test target.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use crate::config::Config;
use crate::core::types::{
    Message, RequestEnvelope, RequestHead, ResponseEnvelope, PROTOCOL_VERSION,
};
use crate::core::{ResonateServer, ResonateWorker, Unavailable};
use crate::persistence::{
    persistence_sqlite::SqliteStorage, FailingStorage, Storage, StorageError,
};
use crate::server::Server;
use crate::util::Clock;

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

/// A [`Config`] suitable for tests: in-memory SQLite, debug operations enabled,
/// a fixed server URL, and a clock the test drives.
///
/// `debug` is on because the debug operations (`debug.snap`, `debug.tick`,
/// `debug.reset`) are how a test inspects and advances the server.
pub fn config() -> Config {
    let mut config = Config::default();
    config.debug = true;
    config.storage.sqlite.path = ":memory:".to_string();
    config.server.url = Some("http://localhost:8001".to_string());
    config
}

/// A server over in-memory SQLite, with no auth, an isolated metric set, and a
/// clock stopped at [`T0`].
///
/// The fixed clock matters: an in-process worker reaching the server through
/// [`ResonateServer::process`] carries no `debug_time`, so without it the
/// server would resolve `now` from the wall clock while the test expressed
/// every deadline as an offset from `T0` — and every promise would arrive
/// already timed out.
///
/// Panics rather than returning a `Result`: a storage backend that will not
/// open is a broken test environment, not a case to handle.
pub fn server() -> Arc<Server> {
    server_at(T0).0
}

/// A server over in-memory SQLite using the supplied config, on a clock fixed
/// at [`T0`].
pub fn server_with(config: Config) -> Arc<Server> {
    build(config, Clock::fixed(T0))
}

/// A server whose clock the test drives, plus a handle onto that clock.
///
/// Use when a test needs to move time forward deliberately:
///
/// ```ignore
/// let (server, clock) = testing::server_at(T0);
/// clock.advance(60_000);
/// ```
pub fn server_at(now: i64) -> (Arc<Server>, Clock) {
    let clock = Clock::fixed(now);
    let server = build(config(), clock.clone());
    (server, clock)
}

/// A server whose storage fails every operation with `error`.
///
/// The error arms in `server.rs` are only reachable this way: a real backend
/// produces `StorageError::Serialization` only under a genuine write conflict,
/// and `StorageError::InvalidInput` only from a backend-specific column
/// constraint. Both are now assertable in a unit test, on a fixed clock, with
/// no database at all.
pub fn failing_server(error: StorageError) -> Arc<Server> {
    Arc::new(
        Server::builder(config(), None, Storage::Failing(FailingStorage::new(error)))
            .clock(Clock::fixed(T0))
            .metrics(crate::metrics::Metrics::isolated())
            .build(),
    )
}

fn build(config: Config, clock: Clock) -> Arc<Server> {
    let path = config.storage.sqlite.path.clone();
    let storage = SqliteStorage::open(&path, config.tasks.retry_timeout)
        .unwrap_or_else(|e| panic!("open sqlite at {path:?}: {e}"));
    Arc::new(
        Server::builder(config, None, Storage::Sqlite(storage))
            .clock(clock)
            .metrics(crate::metrics::Metrics::isolated())
            .build(),
    )
}

/// Fixed epoch anchor. Tests express times as offsets from here so that no test
/// depends on the wall clock.
pub const T0: i64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// Envelopes
// ---------------------------------------------------------------------------

/// A request envelope with a valid head and a random correlation id.
pub fn request(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: fastrand::u64(..).to_string(),
            version: PROTOCOL_VERSION.to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

/// A request envelope carrying `now` as its debug time.
pub fn request_at(kind: &str, data: Value, now: i64) -> RequestEnvelope {
    let mut req = request(kind, data);
    req.head.debug_time = Some(now);
    req
}

/// Send one request through a [`ResonateServer`] port at time `now`.
///
/// Panics if the backend reports itself unavailable — an in-process backend
/// always answers, so that is a bug rather than an outcome under test.
pub async fn send(
    backend: &Arc<dyn ResonateServer>,
    req: &RequestEnvelope,
    now: i64,
) -> ResponseEnvelope {
    let mut req = req.clone();
    req.head.debug_time = Some(now);
    backend
        .process(&req)
        .await
        .unwrap_or_else(|e| panic!("in-process backend reported unavailable: {e}"))
}

/// Dispatch `kind`/`data` at `now` and assert a 2xx, returning the response data.
///
/// This is the workhorse for arranging state: a test that needs a settled
/// promise says so in one line and gets a hard failure, at the right place, if
/// the arrangement itself broke.
pub async fn ok(server: &Arc<Server>, kind: &str, data: Value, now: i64) -> Value {
    let resp = server.dispatch(&request(kind, data), now).await;
    assert!(
        (200..300).contains(&resp.head.status),
        "{kind} expected 2xx, got {}: {}",
        resp.head.status,
        resp.data
    );
    resp.data
}

/// Dispatch `kind`/`data` at `now` and assert the exact status, returning the
/// response data.
pub async fn status(
    server: &Arc<Server>,
    kind: &str,
    data: Value,
    now: i64,
    expected: i32,
) -> Value {
    let resp = server.dispatch(&request(kind, data), now).await;
    assert_eq!(
        resp.head.status, expected,
        "{kind} expected {expected}, got {}: {}",
        resp.head.status, resp.data
    );
    resp.data
}

// ---------------------------------------------------------------------------
// Doubles
// ---------------------------------------------------------------------------

/// A server that answers nothing.
///
/// Workers hold a [`ResonateServer`] handle for the delivery-failure path;
/// tests that never exercise it can use this.
pub struct NoopServer;

#[async_trait]
impl ResonateServer for NoopServer {
    async fn process(&self, _req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        Err(Unavailable::new("NoopServer answers nothing"))
    }
}

/// Records every `(address, message)` pair it is asked to deliver.
///
/// One double serves every scheme, since all workers share a trait — register
/// it under whichever scheme the test is exercising.
#[derive(Default)]
pub struct RecordingWorker {
    calls: Mutex<Vec<(String, Value)>>,
    /// When set, `send` fails with this message instead of recording.
    fail_with: Mutex<Option<String>>,
}

impl RecordingWorker {
    pub fn new() -> Self {
        Self::default()
    }

    /// A worker that reports every delivery as undeliverable.
    pub fn failing(message: &str) -> Self {
        Self {
            calls: Mutex::new(vec![]),
            fail_with: Mutex::new(Some(message.to_string())),
        }
    }

    /// Delivered messages as `(address, serialized message)`.
    pub fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().expect("not poisoned").clone()
    }

    /// The single delivery this worker received. Panics on any other count.
    pub fn only_call(&self) -> (String, Value) {
        let calls = self.calls();
        assert_eq!(
            calls.len(),
            1,
            "expected exactly one delivery, got {}",
            calls.len()
        );
        calls.into_iter().next().expect("length checked")
    }
}

#[async_trait]
impl ResonateWorker for RecordingWorker {
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        if let Some(err) = self.fail_with.lock().expect("not poisoned").as_ref() {
            return Err(Unavailable::new(err.clone()));
        }
        let value = serde_json::to_value(msg).expect("message serializes");
        self.calls
            .lock()
            .expect("not poisoned")
            .push((address.to_string(), value));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Routers
// ---------------------------------------------------------------------------

/// A dispatcher with `worker` registered for `scheme` and nothing else, so any
/// other scheme is undeliverable.
pub fn router_with(
    scheme: &str,
    worker: Arc<RecordingWorker>,
) -> crate::transport::TransportDispatcher {
    let mut workers: HashMap<String, Arc<dyn ResonateWorker>> = HashMap::new();
    workers.insert(scheme.to_string(), worker);
    crate::transport::TransportDispatcher::new(workers)
}

/// A dispatcher with no workers at all: every address is undeliverable.
pub fn empty_router() -> crate::transport::TransportDispatcher {
    crate::transport::TransportDispatcher::new(HashMap::new())
}
