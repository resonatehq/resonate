//! Postgres-backed [`Network`] for the Resonate SDK.
//!
//! The server is Postgres: the Resonate protocol runs as stored procedures
//! in a `resonate` schema (github.com/resonatehq/resonate-pg). Requests go
//! through `resonate.resonate_rpc(jsonb)`; incoming messages arrive via
//! LISTEN/NOTIFY on this node's outbox channels, with a fallback tick. Each
//! wake fires due timers (`resonate.process_timeouts()`) and dequeues this
//! node's execute/unblock rows.
//!
//! Apply `resonate.sql` to the database before use. pg_cron is optional:
//! the pump advances timers whenever a node is running.
//!
//! ```no_run
//! use std::sync::Arc;
//! use resonate_sdk::prelude::*;
//! use resonate_sdk_postgres::PostgresNetwork;
//!
//! let network = PostgresNetwork::builder("postgres://user:pass@localhost:5432/db")
//!     .group("workers")
//!     .build();
//! let resonate = Resonate::new(ResonateConfig {
//!     network: Some(Arc::new(network)),
//!     ..Default::default()
//! });
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use parking_lot::RwLock;
use resonate_sdk::error::{Error, Result};
use resonate_sdk::network::Network;
use tokio::sync::{Mutex, Notify};
use tokio_postgres::{AsyncMessage, Client};

const DEQUEUE_LIMIT: i32 = 100;
const MAX_BACKOFF_SECS: u64 = 60;

type Subscribers = RwLock<Vec<Box<dyn Fn(String) + Send + Sync>>>;
type PgConnection = tokio_postgres::Connection<
    tokio_postgres::Socket,
    postgres_native_tls::TlsStream<tokio_postgres::Socket>,
>;

/// State shared between the [`PostgresNetwork`] handle and its pump and
/// listen tasks.
struct NetworkState {
    conn_str: String,
    unicast: String,
    anycast: String,
    tick: Duration,
    subscribers: Subscribers,
    rpc_client: Mutex<Option<Arc<Client>>>,
    notify: Notify,
    stopped: AtomicBool,
}

/// Network implementation backed by a resonate-pg database.
///
/// - Requests are sent via `SELECT resonate.resonate_rpc($1::jsonb)`.
/// - Incoming messages are pumped by draining this node's outbox rows,
///   woken by NOTIFY or a fallback tick.
/// - Addresses use the `poll://` scheme: `poll://uni@group/pid` and
///   `poll://any@group`.
pub struct PostgresNetwork {
    pid: String,
    group: String,
    state: Arc<NetworkState>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// Builder for [`PostgresNetwork`].
pub struct PostgresNetworkBuilder {
    conn_str: String,
    pid: Option<String>,
    group: Option<String>,
    tick: Option<Duration>,
}

impl PostgresNetworkBuilder {
    /// Process id for this worker. Defaults to a generated id.
    pub fn pid(mut self, pid: impl Into<String>) -> Self {
        self.pid = Some(pid.into());
        self
    }

    /// Group name for routing. Defaults to `"default"`.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Fallback poll interval. Defaults to 250ms.
    pub fn tick(mut self, tick: Duration) -> Self {
        self.tick = Some(tick);
        self
    }

    pub fn build(self) -> PostgresNetwork {
        let pid = self.pid.unwrap_or_else(uuid_no_dashes);
        let group = self.group.unwrap_or_else(|| "default".to_string());
        let unicast = format!("poll://uni@{}/{}", group, pid);
        // A task targets its group: dequeue matches the address byte-for-byte,
        // so anycast must equal target_resolver(group) exactly — no pid.
        let anycast = format!("poll://any@{}", group);

        PostgresNetwork {
            pid,
            group,
            state: Arc::new(NetworkState {
                conn_str: self.conn_str,
                unicast,
                anycast,
                tick: self.tick.unwrap_or(Duration::from_millis(250)),
                subscribers: RwLock::new(Vec::new()),
                rpc_client: Mutex::new(None),
                notify: Notify::new(),
                stopped: AtomicBool::new(false),
            }),
            handles: Mutex::new(Vec::new()),
        }
    }
}

impl PostgresNetwork {
    /// Create a network with default pid, group, and tick. Use
    /// [`builder`](Self::builder) to override them.
    ///
    /// `conn_str` is a Postgres connection string
    /// (e.g. `postgres://user:pass@host:5432/db`).
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self::builder(conn_str).build()
    }

    pub fn builder(conn_str: impl Into<String>) -> PostgresNetworkBuilder {
        PostgresNetworkBuilder {
            conn_str: conn_str.into(),
            pid: None,
            group: None,
            tick: None,
        }
    }
}

#[async_trait::async_trait]
impl Network for PostgresNetwork {
    fn pid(&self) -> &str {
        &self.pid
    }

    fn group(&self) -> &str {
        &self.group
    }

    fn unicast(&self) -> &str {
        &self.state.unicast
    }

    fn anycast(&self) -> &str {
        &self.state.anycast
    }

    /// Start the message pump and the LISTEN connection. Infallible by
    /// design: all connection setup lives in the retry loops, so a database
    /// that is down at boot self-heals.
    async fn start(&self) -> Result<()> {
        let mut handles = self.handles.lock().await;
        if handles.is_empty() {
            handles.push(tokio::spawn(pump_task(self.state.clone())));
            handles.push(tokio::spawn(listen_task(self.state.clone())));
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.state.stopped.store(true, Ordering::Relaxed);
        for handle in self.handles.lock().await.drain(..) {
            handle.abort();
        }
        *self.state.rpc_client.lock().await = None;
        self.state.subscribers.write().clear();
        Ok(())
    }

    /// Send a request to the server via `resonate.resonate_rpc`.
    async fn send(&self, req: String) -> Result<String> {
        if self.state.stopped.load(Ordering::Relaxed) {
            return Err(Error::NetworkError("network stopped".to_string()));
        }
        tracing::debug!(direction = "pg_req", body = %req, "postgres_network");

        let client = self.state.client().await?;
        let row = client
            .query_one(
                "SELECT resonate.resonate_rpc($1::text::jsonb)::text",
                &[&req],
            )
            .await
            .map_err(net_err)?;
        let resp: String = row.try_get(0).map_err(net_err)?;

        tracing::debug!(direction = "pg_res", body = %resp, "postgres_network");
        Ok(resp)
    }

    /// Register a callback for incoming messages.
    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        self.state.subscribers.write().push(callback);
    }

    /// Resolve a target name to a poll:// anycast address.
    fn target_resolver(&self, target: &str) -> String {
        format!("poll://any@{}", target)
    }
}

impl NetworkState {
    /// RPC client, connected lazily and replaced when its connection dies.
    async fn client(&self) -> Result<Arc<Client>> {
        let mut guard = self.rpc_client.lock().await;
        if let Some(client) = guard.as_ref() {
            if !client.is_closed() {
                return Ok(client.clone());
            }
        }
        let (client, connection) = pg_connect(&self.conn_str).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres rpc connection closed");
            }
        });
        let client = Arc::new(client);
        *guard = Some(client.clone());
        Ok(client)
    }
}

async fn pg_connect(conn_str: &str) -> Result<(Client, PgConnection)> {
    let connector = native_tls::TlsConnector::builder()
        .build()
        .map_err(net_err)?;
    let tls = postgres_native_tls::MakeTlsConnector::new(connector);
    tokio_postgres::connect(conn_str, tls)
        .await
        .map_err(net_err)
}

fn net_err(e: impl std::fmt::Display) -> Error {
    Error::NetworkError(e.to_string())
}

/// Drain in a loop, woken by NOTIFY or the fallback tick. Errors are logged
/// and retried on the next wake; the pump never dies.
async fn pump_task(state: Arc<NetworkState>) {
    loop {
        if state.stopped.load(Ordering::Relaxed) {
            return;
        }
        if let Err(e) = drain(&state).await {
            if !state.stopped.load(Ordering::Relaxed) {
                tracing::warn!(error = %e, "postgres drain error");
            }
        }
        // The tick backstops NOTIFY: it fires due timers while the node is
        // otherwise idle and picks up rows enqueued while the LISTEN
        // connection was down.
        tokio::select! {
            _ = state.notify.notified() => {}
            _ = tokio::time::sleep(state.tick) => {}
        }
    }
}

/// Fire due timers, then dequeue this node's outbox rows, delivering each.
/// Delivery is at-least-once: a crash between dequeue and completion is
/// recovered by the task retry timeout.
async fn drain(state: &NetworkState) -> Result<()> {
    let client = state.client().await?;

    // Timers fire on database time: one clock shared by every node.
    client
        .query("SELECT resonate.process_timeouts()", &[])
        .await
        .map_err(net_err)?;

    // The queues drain concurrently (tokio-postgres pipelines the queries)
    // so a steady stream of executes cannot starve unblocks.
    tokio::try_join!(
        drain_executes(state, &client, &state.anycast),
        drain_executes(state, &client, &state.unicast),
        drain_unblocks(state, &client),
    )?;
    Ok(())
}

async fn drain_executes(state: &NetworkState, client: &Client, address: &str) -> Result<()> {
    loop {
        let rows = client
            .query(
                "SELECT task_id, version FROM resonate.dequeue_execute($1, $2)",
                &[&address, &DEQUEUE_LIMIT],
            )
            .await
            .map_err(net_err)?;
        let n = rows.len();
        for row in rows {
            let task_id: String = row.try_get(0).map_err(net_err)?;
            let version: i32 = row.try_get(1).map_err(net_err)?;
            let msg = serde_json::json!({
                "kind": "execute",
                "head": {},
                "data": { "task": { "id": task_id, "version": version as i64 } }
            });
            deliver(state, msg.to_string());
        }
        if n < DEQUEUE_LIMIT as usize {
            return Ok(());
        }
    }
}

async fn drain_unblocks(state: &NetworkState, client: &Client) -> Result<()> {
    loop {
        let rows = client
            .query(
                "SELECT promise FROM resonate.dequeue_unblock($1, $2)",
                &[&state.unicast, &DEQUEUE_LIMIT],
            )
            .await
            .map_err(net_err)?;
        let n = rows.len();
        for row in rows {
            let promise: serde_json::Value = row.try_get(0).map_err(net_err)?;
            let msg = serde_json::json!({
                "kind": "unblock",
                "head": {},
                "data": { "promise": promise }
            });
            deliver(state, msg.to_string());
        }
        if n < DEQUEUE_LIMIT as usize {
            return Ok(());
        }
    }
}

fn deliver(state: &NetworkState, msg: String) {
    if state.stopped.load(Ordering::Relaxed) {
        return;
    }
    let subscribers = state.subscribers.read();
    for callback in subscribers.iter() {
        callback(msg.clone());
    }
}

/// Hold a dedicated LISTEN connection, reconnecting with backoff.
async fn listen_task(state: Arc<NetworkState>) {
    let mut backoff_secs = 1u64;
    loop {
        if state.stopped.load(Ordering::Relaxed) {
            return;
        }
        match listen_once(&state).await {
            Ok(()) => backoff_secs = 1,
            Err(e) => tracing::warn!(error = %e, "postgres listen error"),
        }
        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
    }
}

/// Connect, LISTEN on this node's outbox channels, and forward notifications
/// to the pump until the connection dies. `Ok` means a session was
/// established (the caller resets its backoff).
async fn listen_once(state: &Arc<NetworkState>) -> Result<()> {
    let (client, mut connection) = pg_connect(&state.conn_str).await?;

    let notify_state = state.clone();
    let driver = tokio::spawn(async move {
        let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        while let Some(message) = messages.next().await {
            match message {
                Ok(AsyncMessage::Notification(_)) => notify_state.notify.notify_one(),
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "postgres listen connection error");
                    break;
                }
            }
        }
    });

    let setup = async {
        for address in [state.unicast.as_str(), state.anycast.as_str()] {
            let row = client
                .query_one("SELECT resonate.outbox_channel($1)", &[&address])
                .await
                .map_err(net_err)?;
            let channel: String = row.try_get(0).map_err(net_err)?;
            client
                .batch_execute(&format!("LISTEN \"{}\"", channel))
                .await
                .map_err(net_err)?;
        }
        Ok(())
    };

    match setup.await {
        Ok(()) => {
            tracing::info!("postgres listen established");
            // Wake the pump: rows may have been enqueued while disconnected.
            state.notify.notify_one();
            let _ = driver.await;
            Ok(())
        }
        Err(e) => {
            driver.abort();
            Err(e)
        }
    }
}

fn uuid_no_dashes() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", now ^ 0xdeadbeef_cafebabe_u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn network(pid: Option<&str>, group: Option<&str>) -> PostgresNetwork {
        let mut builder = PostgresNetwork::builder("postgres://localhost:5432/db");
        if let Some(pid) = pid {
            builder = builder.pid(pid);
        }
        if let Some(group) = group {
            builder = builder.group(group);
        }
        builder.build()
    }

    #[test]
    fn postgres_network_identity() {
        let net = network(Some("mypid"), Some("mygroup"));
        assert_eq!(net.pid(), "mypid");
        assert_eq!(net.group(), "mygroup");
        assert_eq!(net.unicast(), "poll://uni@mygroup/mypid");
        assert_eq!(net.anycast(), "poll://any@mygroup");
    }

    #[test]
    fn postgres_network_target_resolver() {
        let net = network(None, None);
        assert_eq!(net.target_resolver("my-target"), "poll://any@my-target");
    }

    #[test]
    fn postgres_network_defaults() {
        let net = PostgresNetwork::new("postgres://localhost:5432/db");
        assert_eq!(net.group(), "default");
        assert_eq!(net.pid().len(), 32);
        assert!(net.pid().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn postgres_network_anycast_matches_resolved_group() {
        let net = network(None, Some("workers"));
        assert_eq!(net.anycast(), net.target_resolver(net.group()));
    }

    #[tokio::test]
    async fn stop_before_start_is_ok() {
        let net = network(None, None);
        net.stop().await.unwrap();
    }
}
