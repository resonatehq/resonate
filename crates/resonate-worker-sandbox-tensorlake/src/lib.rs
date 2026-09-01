//! Resonate worker: Tensorlake sandboxes.
//!
//! A worker rather than a transport, like `bash://` — the other end of a
//! `tensorlake://` address is this process. What it does differently is that
//! it does not do the work. It provisions a sandbox, starts the SDK inside it,
//! and then gets out of the way: the code in the sandbox acquires the task,
//! heartbeats its lease and settles the promise. This worker never calls
//! `task.acquire` and never settles anything.
//!
//! # The sandbox is the promise
//!
//! A task id *is* a promise id — `task_state` and `task_version` are columns on
//! `promises`, there is no separate table — so the sandbox is named for the
//! task it was made for and that name is stable for the life of the promise.
//! Every later message for the same promise finds the same sandbox: a retry
//! after a crash, a redispatch after a lease expired, a resumption after the
//! function suspended awaiting a child. Tensorlake suspends a *named* sandbox
//! when it goes idle with its filesystem, memory and running processes intact,
//! so the normal path is to resume one, not to build one.
//!
//! # The network problem
//!
//! The SDK has to reach a Resonate server, and a sandbox has no route to one
//! that is not on the public internet. So it does not get a route: the
//! protocol is tunnelled over the sandbox process's own stdio — the sandbox
//! writes requests on stdout, this worker applies them in-process and writes
//! the responses back on stdin. See [`tunnel`]. The consequence worth knowing
//! is that the SDK inside needs no credentials, no egress and no reachable
//! server address, and the tunnel works identically against a server running
//! on a laptop.
//!
//! # Addresses
//!
//! `tensorlake://[account[/image[/process]]]` — see [`TensorlakeAddress`].

/// The address scheme this worker serves.
pub const SCHEME: &str = "tensorlake";

mod address;
mod api;
mod tunnel;

pub use address::TensorlakeAddress;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Mutex;

use resonate_core::types::{
    Message, RequestEnvelope, RequestHead, ResponseEnvelope, PROTOCOL_VERSION,
};
use resonate_core::{ResonateServer, ResonateWorker, Unavailable};

use api::{Api, ProcessSpec};

// ─── Configuration ────────────────────────────────────────────────────────────

/// Everything under `[transports.tensorlake]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Enable the tensorlake:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// Credentials and endpoints, by the name an address puts in its host
    /// position. An address with no account uses `default`, which needs no
    /// entry here: absent, it means the public endpoints and the API key from
    /// `TENSORLAKE_API_KEY`.
    #[serde(default)]
    pub accounts: HashMap<String, Account>,

    /// Sandbox image for addresses that do not name one. Unset means
    /// Tensorlake's default environment.
    #[serde(default)]
    pub image: Option<String>,

    /// Executable to start for addresses that do not name one.
    #[serde(default)]
    pub process: Option<String>,

    /// Arguments for it. The address names an executable, never a command
    /// line — there is no quoting convention in a URL path that would survive
    /// a filename with a space in it.
    #[serde(default)]
    pub args: Vec<String>,

    /// Working directory for the process.
    #[serde(default)]
    pub working_dir: Option<String>,

    /// Idle timeout (seconds) requested when a sandbox is created. A *named*
    /// sandbox suspends when it expires rather than terminating, so this is
    /// how long an idle sandbox stays warm, not how long a task may run.
    #[serde(default = "default_sandbox_timeout")]
    pub sandbox_timeout: i64,

    /// Keep the sandbox when the process exits with the promise still pending
    /// [default: true].
    ///
    /// A function that suspends to await a child exits its process; its
    /// sandbox holds the state it will want when the child settles. Turning
    /// this off deletes the sandbox on every exit, which trades that state for
    /// a smaller footprint.
    #[serde(default = "default_true")]
    pub keep_pending: bool,
}

fn default_sandbox_timeout() -> i64 {
    600
}
fn default_true() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            accounts: HashMap::new(),
            image: None,
            process: None,
            args: Vec::new(),
            working_dir: None,
            sandbox_timeout: default_sandbox_timeout(),
            keep_pending: default_true(),
        }
    }
}

/// One set of Tensorlake credentials and endpoints.
///
/// Not a Tensorlake concept — the API authenticates with a key and the key
/// implies the project. It exists so that an address can choose between
/// several, and so that a self-hosted deployment is a config entry rather than
/// a code change.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Account {
    /// The API key. Prefer `api_key_env`: a key in a config file is a key in
    /// version control eventually.
    #[serde(default)]
    pub api_key: Option<String>,

    /// Environment variable holding the API key [default: TENSORLAKE_API_KEY]
    #[serde(default)]
    pub api_key_env: Option<String>,

    /// Management API base, `/sandboxes` included.
    #[serde(default = "default_api_url")]
    pub api_url: String,

    /// Host the per-sandbox proxy subdomain hangs off.
    #[serde(default = "default_proxy_host")]
    pub proxy_host: String,
}

fn default_api_url() -> String {
    "https://api.tensorlake.ai/sandboxes".to_string()
}
fn default_proxy_host() -> String {
    "sandbox.tensorlake.ai".to_string()
}

impl Default for Account {
    fn default() -> Self {
        Self {
            api_key: None,
            api_key_env: None,
            api_url: default_api_url(),
            proxy_host: default_proxy_host(),
        }
    }
}

impl Account {
    /// The API key, from the config or from the environment.
    ///
    /// Read per task rather than at startup so that a key rotated in the
    /// environment takes effect without a restart, and so that a worker
    /// enabled but unused does not fail to start for want of a key.
    fn api_key(&self) -> Result<String, String> {
        if let Some(k) = self.api_key.as_ref().filter(|k| !k.is_empty()) {
            return Ok(k.clone());
        }
        let var = self.api_key_env.as_deref().unwrap_or("TENSORLAKE_API_KEY");
        std::env::var(var).map_err(|_| format!("{var} is not set"))
    }
}

impl Config {
    /// The account an address selects, or why it selects nothing.
    fn account(&self, name: Option<&str>) -> Result<Account, String> {
        let name = name.unwrap_or("default");
        match self.accounts.get(name) {
            Some(a) => Ok(a.clone()),
            // `default` is implied so that no configuration at all still
            // works; any other name is a typo worth reporting.
            None if name == "default" => Ok(Account::default()),
            None => Err(format!("no account '{name}' is configured")),
        }
    }
}

// ─── Sandbox naming ───────────────────────────────────────────────────────────

/// The sandbox name for a promise.
///
/// Tensorlake generates the id, so the *name* carries the identity: it is what
/// `find_sandbox` looks up and what makes a sandbox reusable. Promise ids are
/// arbitrary strings and sandbox names are not, so this is the same shape the
/// plugins use to put a promise id into a provider's namespace — a readable
/// prefix for whoever is reading a sandbox list, and a hash of the whole id to
/// keep two promises that reduce alike apart.
///
/// The safe set is narrower than the plugins' (`[a-z0-9-]`, where they keep
/// `._`) and the readable part shorter, because Tensorlake does not document
/// what a name may contain or how long it may be. A name the API rejects fails
/// the task, and being conservative costs nothing but legibility.
pub fn sandbox_name(promise_id: &str) -> String {
    let safe: String = promise_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(32)
        .collect();
    let safe = safe.trim_matches('-');
    format!("resonate-{safe}-{}", fnv1a64_hex(promise_id.as_bytes()))
}

/// FNV-1a. Not a cryptographic hash and does not need to be — it only has to
/// be stable across processes and platforms, which `DefaultHasher` explicitly
/// is not.
fn fnv1a64_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

// ─── Worker ───────────────────────────────────────────────────────────────────

/// How long a sandbox may take to come up, and how often to look.
const READY_TIMEOUT_MS: u64 = 120_000;
const READY_POLL_MS: u64 = 1_000;

pub struct TensorlakeSandboxTransport {
    /// This worker runs in the server's own process, so the tunnel reaches the
    /// port directly instead of over the wire. Weak: the server holds the
    /// router, the router holds this worker, and a strong handle back would
    /// close that ring.
    server: Weak<dyn ResonateServer>,
    config: Config,
    http: reqwest::Client,
    /// Sandboxes with a live pump, by name.
    ///
    /// A task can be dispatched again while its process is still running — a
    /// lease that expired under a slow step is the usual way — and starting a
    /// second process in the same sandbox would put two clients on one tunnel.
    /// The one already running holds the lease and will settle the promise.
    ///
    /// Shared with the spawned task so that it can take its own name back out
    /// on the way past; a name left behind would mean the promise never runs
    /// again in this process.
    running: Arc<Mutex<HashSet<String>>>,
}

impl TensorlakeSandboxTransport {
    pub fn new(server: Weak<dyn ResonateServer>, config: Config) -> Self {
        Self {
            server,
            config,
            http: reqwest::Client::new(),
            running: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        // Only `execute` names a task to run. An `unblock` says a promise this
        // sandbox awaits has settled — but a suspended function's process has
        // already exited, so there is no tunnel to tell. The server follows an
        // unblock with a task for the awaiting promise, and the `execute` for
        // that task reattaches to this same sandbox, which is the path that
        // survives a restart anyway.
        let task = match msg {
            Message::Execute(e) => &e.data.task,
            Message::Unblock(_) => return Ok(()),
        };

        let addr = TensorlakeAddress::parse(address).map_err(|e| {
            Unavailable::new(format!("tensorlake: cannot parse address {address}: {e}"))
        })?;
        let account = self
            .config
            .account(addr.account.as_deref())
            .map_err(|e| Unavailable::new(format!("tensorlake: {e}")))?;
        let api_key = account
            .api_key()
            .map_err(|e| Unavailable::new(format!("tensorlake: {e}")))?;

        let image = addr.image.or_else(|| self.config.image.clone());
        let process = addr
            .process
            .or_else(|| self.config.process.clone())
            .ok_or_else(|| {
                Unavailable::new(
                    "tensorlake: no process to run — name one in the address \
                     (tensorlake://account/image/process) or in transports.tensorlake.process"
                        .to_string(),
                )
            })?;

        let Some(server) = self.server.upgrade() else {
            return Err(Unavailable::new("tensorlake: server is gone"));
        };

        let name = sandbox_name(&task.id);
        if !self.running.lock().await.insert(name.clone()) {
            tracing::debug!(
                task_id = %task.id,
                sandbox = %name,
                "tensorlake: already running for this promise"
            );
            return Ok(());
        }

        let api = Arc::new(Api::new(
            self.http.clone(),
            api_key,
            account.api_url,
            account.proxy_host,
        ));
        let spec = ProcessSpec {
            command: process,
            args: self.config.args.clone(),
            env: task_env(&task.id, task.version),
            working_dir: self.config.working_dir.clone(),
        };
        let config = self.config.clone();
        let task_id = task.id.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let outcome = run_task(
                Arc::clone(&server),
                Arc::clone(&api),
                &config,
                &name,
                &task_id,
                image.as_deref(),
                &spec,
            )
            .await;
            if let Err(e) = outcome {
                // Nothing is settled here, deliberately. This worker does not
                // own the promise — the SDK does — so a failure to get the
                // sandbox running is a delivery failure, and the task is left
                // for the server to dispatch again.
                tracing::warn!(task_id, sandbox = %name, error = %e, "tensorlake: task not run");
            }
            running.lock().await.remove(&name);
        });
        Ok(())
    }
}

#[async_trait]
impl ResonateWorker for TensorlakeSandboxTransport {
    /// Nothing to start.
    ///
    /// Unlike the bash worker this one keeps no clock: the lease belongs to
    /// the SDK in the sandbox, so the heartbeats are its to send and they
    /// arrive here as ordinary tunnel requests. The debug flag therefore has
    /// nothing to suppress.
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        Ok(())
    }

    /// Running sandboxes are left alone.
    ///
    /// They are named for their promises and hold their state, so the next
    /// process to come up — in this server or another — reattaches. Tearing
    /// them down on the way out would discard exactly what the naming is for.
    async fn stop(&self) -> Result<(), Unavailable> {
        Ok(())
    }

    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        TensorlakeSandboxTransport::send(self, address, msg).await
    }
}

/// What the SDK is told about the task it is there to run.
///
/// The task id and version are what `task.acquire` needs, and they are all it
/// needs: everything else — the promise, its parameters, its deadline — comes
/// back in the acquire response over the tunnel.
fn task_env(task_id: &str, version: i64) -> Vec<(String, String)> {
    vec![
        ("RESONATE_TASK_ID".into(), task_id.to_string()),
        ("RESONATE_TASK_VERSION".into(), version.to_string()),
        // The task id is the promise id; named separately because that is the
        // name the SDK's own API uses.
        ("RESONATE_PROMISE_ID".into(), task_id.to_string()),
        ("RESONATE_TRANSPORT".into(), "stdio".into()),
        (
            "RESONATE_PROTOCOL_VERSION".into(),
            PROTOCOL_VERSION.to_string(),
        ),
    ]
}

// ─── Orchestration ────────────────────────────────────────────────────────────

async fn run_task(
    server: Arc<dyn ResonateServer>,
    api: Arc<Api>,
    config: &Config,
    name: &str,
    task_id: &str,
    image: Option<&str>,
    spec: &ProcessSpec,
) -> Result<(), String> {
    let sandbox_id = ensure_sandbox(&api, name, image, config.sandbox_timeout).await?;
    tracing::debug!(
        task_id,
        sandbox = name,
        sandbox_id,
        "tensorlake: sandbox ready"
    );

    let pid = api.start_process(&sandbox_id, spec).await?;
    tracing::info!(
        task_id,
        sandbox_id,
        pid,
        command = spec.command,
        "tensorlake: process started"
    );

    let outcome = tunnel::pump(
        Arc::clone(&server),
        Arc::clone(&api),
        &sandbox_id,
        pid,
        task_id,
    )
    .await;
    release(
        &server,
        &api,
        &sandbox_id,
        pid,
        task_id,
        &outcome,
        config.keep_pending,
    )
    .await;
    Ok(())
}

/// Find this promise's sandbox and get it running, or make one.
async fn ensure_sandbox(
    api: &Api,
    name: &str,
    image: Option<&str>,
    timeout_secs: i64,
) -> Result<String, String> {
    let sandbox = match api.find_sandbox(name).await? {
        // A terminated sandbox is a name and nothing else — the state it held
        // is gone, so there is nothing to reattach to.
        Some(sb) if sb.status != "terminated" => sb,
        _ => api.create_sandbox(name, image, timeout_secs).await?,
    };

    if sandbox.status == "running" {
        return Ok(sandbox.id);
    }
    if sandbox.status == "suspended" {
        api.resume_sandbox(&sandbox.id).await?;
    }
    wait_running(api, &sandbox.id).await?;
    Ok(sandbox.id)
}

async fn wait_running(api: &Api, sandbox_id: &str) -> Result<(), String> {
    let mut waited = 0;
    loop {
        match api.sandbox_status(sandbox_id).await?.as_str() {
            "running" => return Ok(()),
            "terminated" => {
                return Err(format!("sandbox {sandbox_id} is terminated"));
            }
            // The idle timer can win a race against a resume. Ask again.
            "suspended" => api.resume_sandbox(sandbox_id).await?,
            _ => {}
        }
        if waited >= READY_TIMEOUT_MS {
            return Err(format!("sandbox {sandbox_id} did not start in time"));
        }
        tokio::time::sleep(Duration::from_millis(READY_POLL_MS)).await;
        waited += READY_POLL_MS;
    }
}

/// Delete the sandbox, or leave it holding the promise's state.
///
/// Whether the promise settled is asked of the server rather than inferred
/// from what went through the tunnel: the promise's state is the fact that
/// matters and one request settles it, where watching for a settle would have
/// to guess which of several operations counted and would still be wrong after
/// a reconnect.
async fn release(
    server: &Arc<dyn ResonateServer>,
    api: &Api,
    sandbox_id: &str,
    pid: i64,
    task_id: &str,
    outcome: &tunnel::Outcome,
    keep_pending: bool,
) {
    if let tunnel::Outcome::Lost(e) = outcome {
        // The tunnel went, not necessarily the process. Deleting now could
        // kill something still holding the lease and about to settle, so the
        // sandbox stays: the lease expires, the task comes back, and the next
        // execute reattaches. Closing stdin is the one thing worth doing —
        // a process blocked on a reply that will never arrive reads EOF
        // instead of waiting out its lease.
        tracing::warn!(task_id, sandbox_id, error = %e, "tensorlake: tunnel lost, sandbox kept");
        if let Err(e) = api.close_stdin(sandbox_id, pid).await {
            tracing::debug!(task_id, sandbox_id, error = %e, "tensorlake: stdin not closed");
        }
        return;
    }

    // How it ended is worth a line: a process that exited non-zero without
    // settling its promise is the failure this worker cannot report any other
    // way, having no promise of its own to reject.
    match api.process_status(sandbox_id, pid).await {
        Ok(status) if status.status == "signaled" => {
            tracing::warn!(
                task_id,
                sandbox_id,
                signal = status.signal,
                "tensorlake: process killed"
            )
        }
        Ok(status) if status.exit_code.unwrap_or(0) != 0 => {
            tracing::warn!(
                task_id,
                sandbox_id,
                exit_code = status.exit_code,
                "tensorlake: process failed"
            )
        }
        _ => tracing::debug!(task_id, sandbox_id, "tensorlake: process exited"),
    }

    if keep_pending && promise_pending(server, task_id).await {
        // Left alone on purpose: a named sandbox suspends itself when it goes
        // idle, keeping its filesystem and memory for the next execute.
        tracing::debug!(
            task_id,
            sandbox_id,
            "tensorlake: sandbox kept for a pending promise"
        );
        return;
    }
    if let Err(e) = api.delete_sandbox(sandbox_id).await {
        tracing::warn!(task_id, sandbox_id, error = %e, "tensorlake: sandbox not deleted");
    }
}

/// Is the promise still pending?
///
/// Anything other than a plain "yes" answers no: a promise that is gone has no
/// state worth keeping, and an unanswerable server is not a reason to hold a
/// sandbox open indefinitely.
async fn promise_pending(server: &Arc<dyn ResonateServer>, promise_id: &str) -> bool {
    let resp = request(server, "promise.get", json!({ "id": promise_id })).await;
    match resp {
        Ok(r) if r.head.status == 200 => {
            // `promise.get` answers with the record under `promise`, not
            // inline: reading `data.state` finds nothing and reports every
            // promise settled, which would delete every sandbox.
            r.data
                .get("promise")
                .and_then(|p| p.get("state"))
                .and_then(|s| s.as_str())
                == Some("pending")
        }
        Ok(_) => false,
        Err(e) => {
            tracing::warn!(promise_id, error = %e, "tensorlake: promise state unknown");
            false
        }
    }
}

/// Issue one protocol request at the server this worker is attached to.
async fn request(
    server: &Arc<dyn ResonateServer>,
    kind: &str,
    data: serde_json::Value,
) -> Result<ResponseEnvelope, Unavailable> {
    server
        .process(&RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: format!("tensorlake-{}", fastrand::u64(..)),
                version: PROTOCOL_VERSION.to_string(),
                auth: None,
                debug_time: None,
            },
            data,
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sandbox_name_is_stable_for_a_promise() {
        assert_eq!(sandbox_name("foo"), sandbox_name("foo"));
    }

    #[test]
    fn a_sandbox_name_is_safe_and_readable() {
        let name = sandbox_name("Order:12345/step-2");
        assert_eq!(name, "resonate-order-12345-step-2-3cc935ac3e8521ca");
        assert!(name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    }

    #[test]
    fn promises_that_slug_alike_get_different_names() {
        // Everything unsafe becomes '-', so the slug alone would collide.
        assert_ne!(sandbox_name("a/b"), sandbox_name("a:b"));
    }

    #[test]
    fn a_long_promise_id_stays_within_the_length_a_name_may_be() {
        let name = sandbox_name(&"x".repeat(500));
        assert!(name.len() <= 63, "{} chars: {name}", name.len());
    }

    #[test]
    fn a_promise_id_with_nothing_safe_in_it_still_names_a_sandbox() {
        let name = sandbox_name("///");
        assert_eq!(name, "resonate--eed41417e7153e64");
    }

    #[test]
    fn the_default_account_needs_no_configuration() {
        let config = Config::default();
        let account = config.account(None).unwrap();
        assert_eq!(account.api_url, default_api_url());
    }

    #[test]
    fn an_account_that_is_not_configured_is_an_error() {
        assert!(Config::default().account(Some("prod")).is_err());
    }

    #[test]
    fn a_configured_account_wins() {
        let mut config = Config::default();
        config.accounts.insert(
            "prod".to_string(),
            Account {
                api_url: "https://self.hosted/sandboxes".to_string(),
                ..Account::default()
            },
        );
        assert_eq!(
            config.account(Some("prod")).unwrap().api_url,
            "https://self.hosted/sandboxes"
        );
    }

    #[test]
    fn an_inline_key_is_preferred_over_the_environment() {
        let account = Account {
            api_key: Some("inline".to_string()),
            ..Account::default()
        };
        assert_eq!(account.api_key().unwrap(), "inline");
    }

    #[test]
    fn a_missing_key_names_the_variable_it_wanted() {
        let account = Account {
            api_key_env: Some("NO_SUCH_VARIABLE_FOR_THIS_TEST".to_string()),
            ..Account::default()
        };
        let err = account.api_key().unwrap_err();
        assert!(err.contains("NO_SUCH_VARIABLE_FOR_THIS_TEST"), "{err}");
    }

    #[test]
    fn the_sdk_is_told_the_task_and_how_to_talk() {
        let env: HashMap<String, String> = task_env("promise-1", 7).into_iter().collect();
        assert_eq!(env["RESONATE_TASK_ID"], "promise-1");
        assert_eq!(env["RESONATE_TASK_VERSION"], "7");
        assert_eq!(env["RESONATE_PROMISE_ID"], "promise-1");
        assert_eq!(env["RESONATE_TRANSPORT"], "stdio");
    }
}
