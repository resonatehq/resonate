//! Resonate transport: HTTP poll (SSE).
//!
//! For workers that cannot be dialled: the worker opens an SSE connection to
//! the server and messages are pushed down it. A transport rather than a
//! router — it knows how to reach a worker, not what the message means.
//!
//! Unlike the dialled transports this one has an inbound half: the endpoint
//! workers connect to. So it binds its own socket and serves it — one plugin,
//! one listener, and no gateway hosting a route on another plugin's behalf.
//!
//! Workers connect via `GET /poll/{group}/{id}` and receive messages as
//! Server-Sent Events; [`PollRegistry`] holds those connections open and
//! pushes to them based on `poll://` address routing.

// axum comes from `resonate-plugin`, so a build has one of it — see that
// crate's re-export for why.
use resonate_plugin::axum;

/// The address scheme this transport serves.
pub const SCHEME: &str = "poll";

/// This transport, as a plugin. The one thing a binary names to get `poll://`
/// addresses delivered.
pub static PLUGIN: resonate_plugin::WorkerPlugin =
    resonate_plugin::WorkerPlugin::new(env!("CARGO_PKG_NAME"), &[SCHEME], configure);

/// Read `[workers.transport_http_poll]`, and build the registry unless it is turned off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::WorkerDependencies,
) -> Result<Option<std::sync::Arc<dyn ResonateWorker>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    // `tokio::sync::mpsc::channel` panics on a zero capacity, and a connection
    // limit of zero would accept nothing while still holding the scheme.
    if config.buffer_size == 0 {
        return Err(settings.reject("buffer_size", "must be at least 1 (got 0)"));
    }
    if config.max_connections == 0 {
        return Err(settings.reject("max_connections", "must be at least 1 (got 0)"));
    }
    let registry = PollRegistry::new(deps.server, config);

    // The inbound half. A worker that cannot be dialled dials in, so this
    // transport needs an HTTP endpoint — but it does not need a *listener*.
    // The gateway has one, and one port is what makes the endpoint reachable
    // wherever the protocol is.
    let handler = Arc::clone(&registry);
    deps.routes.add(PLUGIN.id(), move |auth| {
        Router::new()
            .route(POLL_PATH, get(handle))
            .with_state((handler, auth))
    });

    Ok(Some(registry))
}

/// Where workers connect. On the gateway's listener, alongside the protocol.
pub const POLL_PATH: &str = "/poll/:group/:id";

/// Everything under `[workers.transport_http_poll]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Enable the poll:// address scheme [default: true]
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Maximum concurrent SSE connections held open [default: 1000]
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Per-connection message buffer [default: 100]
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    // No `bind` and no `auth`. This transport serves a route, not a socket:
    // the gateway that owns the listener owns the address it is reachable at
    // and the policy that admits a connection to it. One door, one lock.
}

fn default_enabled() -> bool {
    true
}
fn default_max_connections() -> usize {
    1000
}
fn default_buffer_size() -> usize {
    100
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_connections: default_max_connections(),
            buffer_size: default_buffer_size(),
        }
    }
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Mutex};

use resonate_plugin::types::Message;
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};

/// A `poll://` destination: `poll://<cast>@<group>[/<id>]`.
#[derive(Debug, Clone)]
pub struct PollAddress {
    pub cast: PollCast,
    pub group: String,
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PollCast {
    Uni,
    Any,
}

impl PollAddress {
    /// Parse a `poll://` address. This worker owns the syntax; the router has
    /// only checked the scheme.
    pub fn parse(address: &str) -> Result<Self, Unavailable> {
        let bad = || Unavailable::new(format!("malformed poll address: {address}"));
        let parsed = url::Url::parse(address).map_err(|_| bad())?;
        let cast = match parsed.username() {
            "uni" => PollCast::Uni,
            "any" => PollCast::Any,
            _ => return Err(bad()),
        };
        let group = parsed.host_str().ok_or_else(bad)?.to_string();
        let path = parsed.path();
        let id = if path.len() > 1 {
            Some(path[1..].to_string())
        } else {
            None
        };
        Ok(PollAddress { cast, group, id })
    }
}

/// A single SSE connection to a worker.
pub struct PollConnection {
    /// Unique identifier for this specific connection instance.
    pub conn_id: u64,
    pub id: String,
    pub tx: mpsc::Sender<String>,
}

/// Manages all active poll connections, grouped by group name.
pub struct PollRegistry {
    /// group -> [connection]
    connections: Mutex<HashMap<String, Vec<Arc<PollConnection>>>>,
    /// Monotonically increasing counter for unique connection IDs.
    next_conn_id: AtomicU64,
    pub max_connections: usize,
    pub buffer_size: usize,
    /// Held so a delivery failure can be reported back to the server (e.g.
    /// releasing the task instead of dropping it). Not used yet.
    ///
    /// Weak: the server holds the router and the router holds this worker, so
    /// a strong handle back would close a reference cycle.
    #[allow(dead_code)]
    server: Weak<dyn ResonateServer>,
}

impl PollRegistry {
    /// `Arc`, because the route registered for it holds one.
    pub fn new(server: Weak<dyn ResonateServer>, config: Config) -> Arc<Self> {
        Arc::new(Self {
            connections: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            max_connections: config.max_connections,
            buffer_size: config.buffer_size,
            server,
        })
    }

    /// How many connections are open, across every group.
    pub async fn connection_count(&self) -> usize {
        self.connections
            .lock()
            .await
            .values()
            .map(|v| v.len())
            .sum()
    }

    /// Register a new connection. Returns its id and the receiving end of the
    /// message channel. Returns None if max connections exceeded.
    ///
    /// The id rather than the connection itself, deliberately: the registry is
    /// the sole owner of every [`PollConnection`], and therefore of every
    /// sender. That is what makes [`stop`](ResonateWorker::stop) able to end
    /// the streams by clearing the map — a caller holding an `Arc` of its own
    /// would keep its sender alive and the stream would never see the channel
    /// close. The id is all a caller needs, to deregister on disconnect.
    pub async fn register(&self, group: &str, id: &str) -> Option<(u64, mpsc::Receiver<String>)> {
        let mut conns = self.connections.lock().await;

        // Check total connection count
        let total: usize = conns.values().map(|v| v.len()).sum();
        if total >= self.max_connections {
            return None;
        }

        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(self.buffer_size);
        let conn = Arc::new(PollConnection {
            conn_id,
            id: id.to_string(),
            tx,
        });

        conns.entry(group.to_string()).or_default().push(conn);

        tracing::info!(
            group = %group,
            id = %id,
            conn_id = conn_id,
            total_connections = total + 1,
            "Poll connection registered"
        );

        Some((conn_id, rx))
    }

    /// Deregister a specific connection by its unique connection ID.
    pub async fn deregister(&self, group: &str, conn_id: u64) {
        let mut conns = self.connections.lock().await;
        if let Some(group_conns) = conns.get_mut(group) {
            group_conns.retain(|c| c.conn_id != conn_id);
            if group_conns.is_empty() {
                conns.remove(group);
            }
        }
        tracing::info!(group = %group, conn_id = conn_id, "Poll connection deregistered");
    }

    /// Send a message to the appropriate connection(s) based on the poll address.
    /// Returns true if the message was delivered, false otherwise.
    pub async fn send_poll(&self, address: &PollAddress, payload: &str) -> bool {
        let conns = self.connections.lock().await;

        let group_conns = match conns.get(&address.group) {
            Some(c) if !c.is_empty() => c,
            _ => {
                tracing::warn!(
                    group = %address.group,
                    "Poll send failed: no active connections for group"
                );
                return false;
            }
        };

        let delivered = match address.cast {
            PollCast::Uni => {
                // Must have an id, must match exactly
                if let Some(target_id) = &address.id {
                    if let Some(conn) = group_conns.iter().find(|c| &c.id == target_id) {
                        conn.tx.try_send(payload.to_string()).is_ok()
                    } else {
                        tracing::warn!(
                            group = %address.group,
                            target_id = %target_id,
                            "Poll send failed: target connection not found in group"
                        );
                        false
                    }
                } else {
                    false
                }
            }
            PollCast::Any => {
                // Prefer specific id, fall back to random
                if let Some(target_id) = &address.id {
                    if let Some(conn) = group_conns.iter().find(|c| &c.id == target_id) {
                        let ok = conn.tx.try_send(payload.to_string()).is_ok();
                        if ok {
                            tracing::debug!(
                                group = %address.group,
                                target_id = %target_id,
                                "Poll message delivered to preferred connection"
                            );
                        }
                        return ok;
                    }
                }
                // Fall back to random selection to distribute work
                let idx = fastrand::usize(..group_conns.len());
                let ok = group_conns[idx].tx.try_send(payload.to_string()).is_ok();
                if ok {
                    tracing::debug!(
                        group = %address.group,
                        selected_id = %group_conns[idx].id,
                        "Poll message delivered to random connection"
                    );
                }
                ok
            }
        };
        if !delivered {
            tracing::warn!(
                group = %address.group,
                "Poll message delivery failed: channel full or closed"
            );
        }
        delivered
    }
}

#[async_trait::async_trait]
impl ResonateWorker for PollRegistry {
    // No `init`. There is nothing to open: the route was registered during
    // `configure` and the gateway binds the socket it lives on.

    /// Drop every connection, which is what ends the SSE streams.
    ///
    /// Each registered connection owns the sending half of its stream's
    /// channel. Clearing the map drops them all, the handler's `recv` returns
    /// `None`, and the stream finishes on its own — so this transport tears
    /// down its own connections and nothing outside it needs a say.
    ///
    /// That is also what lets the gateway stop *after* the workers: axum's
    /// graceful shutdown waits for every in-flight response, and an SSE
    /// response is in flight until its stream ends. By the time the gateway
    /// drains, there are none left for it to wait on.
    /// `a_drained_registry_ends_its_streams` says so.
    async fn stop(&self) -> Result<(), Unavailable> {
        let mut conns = self.connections.lock().await;
        let n: usize = conns.values().map(|v| v.len()).sum();
        conns.clear();
        if n > 0 {
            tracing::info!(connections = n, "Poll connections closed");
        }
        Ok(())
    }

    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let addr = PollAddress::parse(address)?;
        // Serialize via `Value` rather than straight from the struct. serde_json
        // has no `preserve_order`, so a `Value` map is a BTreeMap and emits keys
        // alphabetically — which is what SSE consumers have always received.
        // Going direct would emit declaration order instead and change the bytes.
        let body = serde_json::to_value(msg)
            .and_then(|v| serde_json::to_string(&v))
            .map_err(|e| Unavailable::new(format!("cannot serialize message: {e}")))?;
        if PollRegistry::send_poll(self, &addr, &body).await {
            Ok(())
        } else {
            Err(Unavailable::new(format!(
                "no poll connection accepted delivery for {address}"
            )))
        }
    }
}

// ─── The endpoint workers dial ───────────────────────────────────────────────
//
// It used to be a route the HTTP gateway hosted, which meant the gateway named
// `PollRegistry` — one plugin reaching into another's concrete type, and the
// reason the gateway could not be a plugin itself.

use axum::extract::{Path, State};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

/// Serve `GET /poll/{group}/{id}` until `stop`.
/// What the handler needs: the registry to register into, and the gateway's
/// auth policy — handed over when it built these routes, so this endpoint
/// admits exactly whom the protocol endpoint admits.
type PollState = (Arc<PollRegistry>, Option<Arc<resonate_auth::AuthConfig>>);

async fn handle(
    State((registry, auth)): State<PollState>,
    headers: axum::http::HeaderMap,
    Path((group, id)): Path<(String, String)>,
) -> Response {
    if let Some(auth) = &auth {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if resonate_auth::auth_check_token(auth, token).is_err() {
            tracing::warn!(group = %group, id = %id, "Poll connection rejected: unauthorized");
            return (axum::http::StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

    tracing::info!(group = %group, id = %id, "Poll SSE connection requested");
    match registry.register(&group, &id).await {
        Some((conn_id, mut rx)) => {
            tracing::info!(group = %group, id = %id, conn_id, "Poll SSE connection established");
            // The stream ends when the channel closes, and nothing else. A
            // client disconnecting drops this response; the transport stopping
            // clears its registry, which drops the only sender. There is no
            // shutdown signal to keep in step with, because the thing that owns
            // the connection is the thing that ends it.
            let stream = async_stream::stream! {
                let _guard = PollGuard { registry: registry.clone(), group: group.clone(), conn_id };
                while let Some(msg) = rx.recv().await {
                    yield Ok::<_, std::convert::Infallible>(Event::default().data(msg));
                }
            };
            Sse::new(stream).into_response()
        }
        None => {
            tracing::warn!(group = %group, id = %id, "Poll connection rejected: at capacity");
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Poll registration at capacity",
            )
                .into_response()
        }
    }
}

struct PollGuard {
    registry: Arc<PollRegistry>,
    group: String,
    conn_id: u64,
}

impl Drop for PollGuard {
    fn drop(&mut self) {
        let registry = self.registry.clone();
        let group = self.group.clone();
        let conn_id = self.conn_id;
        tokio::spawn(async move {
            registry.deregister(&group, conn_id).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopServer;

    #[async_trait::async_trait]
    impl ResonateServer for NoopServer {
        async fn process(
            &self,
            _req: &resonate_plugin::types::RequestEnvelope,
        ) -> Result<resonate_plugin::types::ResponseEnvelope, Unavailable> {
            unreachable!("never called")
        }
    }

    fn no_server() -> resonate_plugin::WorkerDependencies {
        resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>,
            resonate_plugin::Routes::new(),
        )
    }

    fn settings(pairs: &[(&str, &str)]) -> resonate_plugin::Configuration {
        let mut loader = resonate_plugin::Loader::new();
        for (k, v) in pairs {
            loader = loader.set(k, v).unwrap();
        }
        loader.load()
    }

    #[test]
    fn a_section_nobody_wrote_gets_this_crate_s_defaults() {
        let config = settings(&[]);
        let worker = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()).unwrap();
        assert!(worker.is_some(), "poll is on unless turned off");
        assert_eq!(PLUGIN.schemes, &["poll"]);
        assert_eq!(
            config.worker(&PLUGIN.id()).key(),
            "workers.transport_http_poll"
        );
    }

    #[test]
    fn turning_it_off_is_its_own_setting() {
        let config = settings(&[("workers.transport_http_poll.enabled", "false")]);
        assert!(
            (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_zero_sized_buffer_is_refused_at_startup() {
        // tokio::sync::mpsc::channel panics on a zero capacity, so this used to
        // be a check in the server's main, three crates away from the channel.
        let config = settings(&[("workers.transport_http_poll.buffer_size", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()) else {
            panic!("channel construction would panic");
        };
        assert_eq!(err.key, "workers.transport_http_poll.buffer_size");
    }

    /// Registering a route is what this transport does instead of binding.
    ///
    /// The gateway serves it, so `configure` has to have put it there before
    /// any gateway's `init` runs — which is why registration is here and not in
    /// an `init` of its own.
    #[test]
    fn it_registers_a_route_rather_than_taking_a_port() {
        let config = settings(&[]);
        let routes = resonate_plugin::Routes::new();
        let deps = resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>,
            Arc::clone(&routes),
        );
        (PLUGIN.configure)(&config.worker(&PLUGIN.id()), deps).unwrap();

        let registered = routes.take();
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].0, "transport_http_poll");
    }

    /// A transport that is off registers nothing — no dangling endpoint on the
    /// gateway for a scheme nothing routes to.
    #[test]
    fn turning_it_off_takes_the_route_with_it() {
        let config = settings(&[("workers.transport_http_poll.enabled", "false")]);
        let routes = resonate_plugin::Routes::new();
        let deps = resonate_plugin::WorkerDependencies::new(
            std::sync::Weak::<NoopServer>::new() as std::sync::Weak<dyn ResonateServer>,
            Arc::clone(&routes),
        );
        assert!((PLUGIN.configure)(&config.worker(&PLUGIN.id()), deps)
            .unwrap()
            .is_none());
        assert!(routes.is_empty());
    }

    /// `stop` ends every stream, which is what lets the gateway drain after it.
    ///
    /// axum's graceful shutdown waits for in-flight responses, and an SSE
    /// response is in flight until its stream ends — and these streams end when
    /// this registry drops its senders. If `stop` left one alive, the gateway
    /// would wait on it until the shutdown timeout.
    #[tokio::test]
    async fn a_drained_registry_ends_its_streams() {
        let poll = PollRegistry::new(
            std::sync::Weak::<NoopServer>::new() as Weak<dyn ResonateServer>,
            Config::default(),
        );
        let (_conn_id, mut rx) = poll.register("default", "w1").await.expect("capacity");
        assert_eq!(poll.connection_count().await, 1);

        poll.stop().await.expect("stops");

        assert_eq!(poll.connection_count().await, 0);
        assert!(
            rx.recv().await.is_none(),
            "the stream must see its channel close, or the gateway waits on it"
        );
    }

    #[test]
    fn a_zero_connection_limit_is_refused_at_startup() {
        let config = settings(&[("workers.transport_http_poll.max_connections", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.worker(&PLUGIN.id()), no_server()) else {
            panic!("it would hold the scheme and accept nothing");
        };
        assert_eq!(err.key, "workers.transport_http_poll.max_connections");
    }
}
