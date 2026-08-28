//! Resonate transport: HTTP poll (SSE).
//!
//! For workers that cannot be dialled: the worker opens an SSE connection to
//! the server and messages are pushed down it. A transport rather than a
//! plugin — it knows how to reach a worker, not what the message means.
//!
//! Unlike the dialled transports this one has an inbound half: the server must
//! host the endpoint workers connect to. [`PollRegistry`] is that connection
//! pool, and something has to mount the route that fills it.
//!
//! Nothing does, at present. The gateway serves the RPC endpoint and nothing
//! else, so a worker has no `GET /poll/{group}/{id}` to open an SSE connection
//! against: the scheme is still registered and still routes, but the registry
//! it routes to holds no connections until an endpoint is mounted again.

/// The address scheme this transport serves.
pub const SCHEME: &str = "poll";

/// Everything under `[transports.http_poll]`.
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

use resonate_core::types::Message;
use resonate_core::{ResonateServer, ResonateWorker, Unavailable};

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
    pub fn new(server: Weak<dyn ResonateServer>, config: Config) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            max_connections: config.max_connections,
            buffer_size: config.buffer_size,
            server,
        }
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
    /// Drop every connection, which is what ends the SSE streams.
    ///
    /// Each registered connection owns the sending half of its stream's
    /// channel. Clearing the map drops them all, the handler's `recv` returns
    /// `None`, and the stream finishes on its own — so the transport tears down
    /// its own connections and nothing outside it needs a say. That is also
    /// what lets the HTTP gateway stop *after* this: by the time it drains,
    /// there are no long-lived responses left for it to wait on.
    async fn stop(&self) -> Result<(), Unavailable> {
        let mut conns = self.connections.lock().await;
        let n: usize = conns.values().map(|v| v.len()).sum();
        conns.clear();
        if n > 0 {
            tracing::info!(connections = n, "Poll connections closed");
        }
        Ok(())
    }

    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
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
