//! Poll transport — SSE-based message delivery.
//!
//! Workers connect via `GET /poll/{group}/{id}` and receive messages
//! as Server-Sent Events. The server holds connections open and pushes
//! messages to them based on poll:// address routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::core::types::Message;
use crate::core::{ResonateServer, ResonateWorker, Unavailable};

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
    #[allow(dead_code)]
    server: Arc<dyn ResonateServer>,
}

impl PollRegistry {
    pub fn new(
        server: Arc<dyn ResonateServer>,
        max_connections: usize,
        buffer_size: usize,
    ) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            max_connections,
            buffer_size,
            server,
        }
    }

    /// Register a new connection. Returns the receiver end of the message channel.
    /// Returns None if max connections exceeded.
    pub async fn register(
        &self,
        group: &str,
        id: &str,
    ) -> Option<(Arc<PollConnection>, mpsc::Receiver<String>)> {
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

        conns
            .entry(group.to_string())
            .or_default()
            .push(conn.clone());

        tracing::info!(
            group = %group,
            id = %id,
            conn_id = conn_id,
            total_connections = total + 1,
            "Poll connection registered"
        );

        Some((conn, rx))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};
    use crate::testing::NoopServer;

    fn registry(max_connections: usize, buffer_size: usize) -> PollRegistry {
        PollRegistry::new(Arc::new(NoopServer), max_connections, buffer_size)
    }

    fn execute_msg(task_id: &str) -> Message {
        Message::Execute(ExecuteMsg {
            kind: "execute".to_string(),
            head: MessageHead {
                server_url: "http://localhost:8001".to_string(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: task_id.to_string(),
                    version: 1,
                },
            },
        })
    }

    /// Register a connection, failing the test if the registry refused.
    async fn register(
        reg: &PollRegistry,
        group: &str,
        id: &str,
    ) -> (Arc<PollConnection>, mpsc::Receiver<String>) {
        reg.register(group, id)
            .await
            .unwrap_or_else(|| panic!("registry refused {group}/{id}"))
    }

    // ---- address parsing ----

    #[test]
    fn parses_a_unicast_address_with_an_id() {
        let addr = PollAddress::parse("poll://uni@workers/worker-1").expect("valid");
        assert_eq!(addr.cast, PollCast::Uni);
        assert_eq!(addr.group, "workers");
        assert_eq!(addr.id.as_deref(), Some("worker-1"));
    }

    #[test]
    fn parses_an_anycast_address_without_an_id() {
        let addr = PollAddress::parse("poll://any@workers").expect("valid");
        assert_eq!(addr.cast, PollCast::Any);
        assert_eq!(addr.group, "workers");
        assert_eq!(addr.id, None);
    }

    #[test]
    fn parses_an_anycast_address_with_a_preferred_id() {
        let addr = PollAddress::parse("poll://any@workers/preferred").expect("valid");
        assert_eq!(addr.cast, PollCast::Any);
        assert_eq!(addr.id.as_deref(), Some("preferred"));
    }

    #[test]
    fn rejects_addresses_that_are_not_uni_or_any() {
        for bad in [
            "poll://workers",       // no cast
            "poll://multi@workers", // unknown cast
            "poll://@workers",      // empty cast
            "not a url",
        ] {
            assert!(PollAddress::parse(bad).is_err(), "should reject {bad:?}");
        }
    }

    // ---- capacity ----

    #[tokio::test]
    async fn registration_is_refused_at_capacity() {
        let reg = registry(2, 4);
        let _a = register(&reg, "g", "a").await;
        let _b = register(&reg, "g", "b").await;

        assert!(
            reg.register("g", "c").await.is_none(),
            "the third connection exceeds max_connections"
        );
    }

    #[tokio::test]
    async fn capacity_counts_across_groups_not_within_them() {
        let reg = registry(2, 4);
        let _a = register(&reg, "group-one", "a").await;
        let _b = register(&reg, "group-two", "b").await;

        assert!(
            reg.register("group-three", "c").await.is_none(),
            "max_connections is a total, not a per-group limit"
        );
    }

    #[tokio::test]
    async fn deregistering_frees_capacity() {
        let reg = registry(1, 4);
        let (conn, _rx) = register(&reg, "g", "a").await;
        assert!(reg.register("g", "b").await.is_none(), "at capacity");

        reg.deregister("g", conn.conn_id).await;

        assert!(reg.register("g", "b").await.is_some(), "the slot came back");
    }

    #[tokio::test]
    async fn deregistering_an_unknown_connection_is_harmless() {
        let reg = registry(4, 4);
        let (conn, mut rx) = register(&reg, "g", "a").await;

        reg.deregister("g", conn.conn_id + 999).await;
        reg.deregister("no-such-group", conn.conn_id).await;

        // The real connection still works.
        reg.send(&"poll://uni@g/a".to_string(), &execute_msg("t1"))
            .await
            .expect("still registered");
        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn connection_ids_are_unique_even_within_a_group() {
        let reg = registry(4, 4);
        let (a, _ra) = register(&reg, "g", "same-id").await;
        let (b, _rb) = register(&reg, "g", "same-id").await;
        assert_ne!(
            a.conn_id, b.conn_id,
            "two connections may share a worker id; the conn_id disambiguates"
        );
    }

    // ---- unicast ----

    #[tokio::test]
    async fn unicast_reaches_exactly_the_named_connection() {
        let reg = registry(4, 4);
        let (_a, mut rx_a) = register(&reg, "g", "a").await;
        let (_b, mut rx_b) = register(&reg, "g", "b").await;

        reg.send("poll://uni@g/b", &execute_msg("t1"))
            .await
            .expect("b is registered");

        let msg = rx_b.recv().await.expect("b receives");
        assert!(msg.contains("\"t1\""), "{msg}");
        assert!(
            rx_a.try_recv().is_err(),
            "a must not receive a message addressed to b"
        );
    }

    #[tokio::test]
    async fn unicast_to_an_unknown_id_is_reported_not_rerouted() {
        let reg = registry(4, 4);
        let (_a, mut rx_a) = register(&reg, "g", "a").await;

        let err = reg
            .send("poll://uni@g/nobody", &execute_msg("t1"))
            .await
            .expect_err("no such connection");
        assert!(err.to_string().contains("no poll connection"), "{err}");
        assert!(
            rx_a.try_recv().is_err(),
            "unicast must never fall back to another connection"
        );
    }

    #[tokio::test]
    async fn unicast_without_an_id_is_undeliverable() {
        let reg = registry(4, 4);
        let (_a, _rx) = register(&reg, "g", "a").await;
        assert!(
            reg.send("poll://uni@g", &execute_msg("t1")).await.is_err(),
            "unicast requires a target id"
        );
    }

    #[tokio::test]
    async fn a_message_to_a_group_with_no_connections_is_reported() {
        let reg = registry(4, 4);
        for address in ["poll://uni@empty/a", "poll://any@empty"] {
            assert!(
                reg.send(address, &execute_msg("t1")).await.is_err(),
                "nothing is listening on {address}"
            );
        }
    }

    // ---- anycast ----

    #[tokio::test]
    async fn anycast_prefers_the_named_connection_when_present() {
        // Stay within the per-connection buffer so a refusal means "not
        // preferred", not "queue full".
        let reg = registry(4, 8);
        let (_a, mut rx_a) = register(&reg, "g", "a").await;
        let (_b, mut rx_b) = register(&reg, "g", "b").await;

        for _ in 0..8 {
            reg.send("poll://any@g/b", &execute_msg("t"))
                .await
                .expect("b is present");
        }

        assert!(
            rx_a.try_recv().is_err(),
            "the preference is honoured every time"
        );
        assert!(rx_b.try_recv().is_ok());
    }

    #[tokio::test]
    async fn anycast_falls_back_when_the_preferred_connection_is_gone() {
        let reg = registry(4, 4);
        let (_a, mut rx_a) = register(&reg, "g", "a").await;

        reg.send("poll://any@g/not-here", &execute_msg("t1"))
            .await
            .expect("falls back to any connection in the group");

        assert!(
            rx_a.recv().await.is_some(),
            "anycast may be served by any member of the group"
        );
    }

    #[tokio::test]
    async fn anycast_without_an_id_reaches_some_connection_in_the_group() {
        let reg = registry(4, 8);
        let (_a, mut rx_a) = register(&reg, "g", "a").await;
        let (_b, mut rx_b) = register(&reg, "g", "b").await;

        for _ in 0..8 {
            reg.send("poll://any@g", &execute_msg("t"))
                .await
                .expect("someone is listening");
        }

        let total = {
            let mut n = 0;
            while rx_a.try_recv().is_ok() {
                n += 1;
            }
            while rx_b.try_recv().is_ok() {
                n += 1;
            }
            n
        };
        assert_eq!(total, 8, "every message landed somewhere, exactly once");
    }

    // ---- backpressure ----

    #[tokio::test]
    async fn a_full_connection_buffer_is_reported_not_silently_dropped() {
        let reg = registry(4, 1);
        let (_a, _rx) = register(&reg, "g", "a").await;

        reg.send("poll://uni@g/a", &execute_msg("t1"))
            .await
            .expect("first fits the 1-slot buffer");

        let err = reg
            .send("poll://uni@g/a", &execute_msg("t2"))
            .await
            .expect_err("buffer is full");
        assert!(err.to_string().contains("no poll connection"), "{err}");
    }

    #[tokio::test]
    async fn a_closed_receiver_is_reported() {
        let reg = registry(4, 4);
        let (_a, rx) = register(&reg, "g", "a").await;
        drop(rx);

        assert!(
            reg.send("poll://uni@g/a", &execute_msg("t1"))
                .await
                .is_err(),
            "the worker went away"
        );
    }

    // ---- serialization ----

    #[tokio::test]
    async fn the_payload_is_json_with_alphabetically_ordered_keys() {
        // SSE consumers have always received keys in this order, because the
        // message is serialized via `Value` (a BTreeMap) rather than straight
        // from the struct. Going direct would emit declaration order and change
        // the bytes on the wire.
        let reg = registry(4, 4);
        let (_a, mut rx) = register(&reg, "g", "a").await;

        reg.send("poll://uni@g/a", &execute_msg("task-42"))
            .await
            .expect("delivered");

        let body = rx.recv().await.expect("received");
        let data_at = body.find("\"data\"").expect("has data");
        let head_at = body.find("\"head\"").expect("has head");
        let kind_at = body.find("\"kind\"").expect("has kind");
        assert!(
            data_at < head_at && head_at < kind_at,
            "expected data < head < kind, got {body}"
        );

        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        assert_eq!(parsed["data"]["task"]["id"], "task-42");
    }
}
