//! Resonate worker: sandboxed execution.
//!
//! Runs each task dispatched to `sandbox://<image>` inside an isolated sandbox
//! booted from that image. The plugin is the only component that talks to the
//! server; the guest has no network by default, and reaches the server only
//! through its own stdio — newline-delimited JSON frames, relayed by rn8, the
//! entrypoint baked into the image.
//!
//! ```text
//!  server ──execute──▶ plugin ──create, exec──▶ sandbox
//!                        │  ◀──── frames on stdio ────▶ rn8 ◀─HTTP─▶ SDK
//!                        └── each req, scoped and authenticated ──▶ server
//! ```
//!
//! 1. The server dispatches a task to a `sandbox://` target.
//! 2. The plugin calls `create`, then `exec` on the image's entrypoint.
//! 3. The plugin writes the task message as the first frame.
//! 4. rn8 starts the worker and pushes the task to it over loopback HTTP.
//! 5. The SDK sends its requests to rn8, which relays them as frames. The
//!    plugin forwards each one to the server with auth attached and returns the
//!    response — once [`scope`] has said the request is this task's to make.
//! 6. The process exits when the step ends, and the plugin calls `destroy`.
//!
//! The plugin destroys the sandbox when the task's lease expires.
//!
//! # Addressing
//!
//! `sandbox://<image>`, an OCI reference pinned by digest
//! (`sandbox://ghcr.io/acme/worker@sha256:…`). A mutable tag would let a
//! resume run different code than the invoke, so a tag is refused unless
//! `require_digest = false`. Which function runs, with which arguments, is the
//! promise's business: the plugin is function-agnostic. CPU, memory and egress
//! are this plugin's configuration, so they are in neither the address nor
//! the backend trait.

mod local;
pub mod scope;
mod session;

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use resonate_plugin::types::Message;
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};
use resonate_sandbox::{Backend, Egress, Limits};
use resonate_sandbox_microsandbox::Microsandbox;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

pub use local::Local;

/// The address scheme this worker serves.
pub const SCHEME: &str = "sandbox";

/// This worker, as a plugin. The one thing a binary names to get `sandbox://`
/// addresses executed.
pub static PLUGIN: resonate_plugin::WorkerPlugin =
    resonate_plugin::WorkerPlugin::new(env!("CARGO_PKG_NAME"), &[SCHEME], configure);

/// Read `[workers.worker_sandbox]`, and build the worker unless it is off.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::WorkerDependencies,
) -> Result<Option<Arc<dyn ResonateWorker>>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if !config.enabled {
        return Ok(None);
    }
    // Zero permits would park every dispatch forever.
    if config.concurrency == 0 {
        return Err(settings.reject("concurrency", "must be at least 1 (got 0)"));
    }
    if config.cpus == Some(0) {
        return Err(settings.reject("cpus", "must be at least 1"));
    }
    if config.memory_mib == Some(0) {
        return Err(settings.reject("memory_mib", "must be at least 1"));
    }
    if config.backend == BackendKind::Local {
        if config.command.is_empty() {
            return Err(settings.reject(
                "command",
                "the local backend has no image to take an entrypoint from; name the command \
                 to run (rn8 and the worker)",
            ));
        }
        if config.egress == Egress::None {
            return Err(settings.reject(
                "egress",
                "the local backend runs on the host and cannot take the network away; \
                 set egress = \"all\" to say you know",
            ));
        }
    }
    let worker: Arc<dyn ResonateWorker> = match config.backend {
        BackendKind::Microsandbox => Arc::new(SandboxWorker::new(
            deps.server,
            Microsandbox::new(config.msb.clone(), config.limits()),
            config,
        )),
        BackendKind::Local => Arc::new(SandboxWorker::new(deps.server, Local::new(), config)),
    };
    Ok(Some(worker))
}

/// Which backend creates the sandboxes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    /// A microsandbox microVM per task, through the `msb` CLI.
    #[default]
    Microsandbox,
    /// No sandbox at all: `command` runs as a host process, and the image is
    /// ignored. For developing an image's worker, and for tests — never for
    /// code you do not trust.
    Local,
}

/// Everything under `[workers.worker_sandbox]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Enable the sandbox:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// The backend [default: microsandbox]
    #[serde(default)]
    pub backend: BackendKind,

    /// Virtual CPUs per sandbox [default: the backend's]
    #[serde(default)]
    pub cpus: Option<u8>,

    /// Memory per sandbox, in MiB [default: the backend's]
    #[serde(default)]
    pub memory_mib: Option<u32>,

    /// Outbound network for the guest: "none" or "all" [default: none]
    #[serde(default)]
    pub egress: Egress,

    /// Refuse an image that is not pinned by digest [default: true]
    #[serde(default = "default_true")]
    pub require_digest: bool,

    /// The command to run in the sandbox. Empty runs the image's own
    /// entrypoint, which is rn8 [default: empty]
    #[serde(default)]
    pub command: Vec<String>,

    /// Extra environment for that command. Not for secrets: the guest can
    /// read its own environment, and holding credentials is the plugin's job.
    #[serde(default)]
    pub env: BTreeMap<String, String>,

    /// Sandboxes running at once [default: 16]
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Bearer token attached to every request the plugin forwards. Whatever
    /// the guest put in `head.auth` is discarded [default: none]
    #[serde(default)]
    pub token: Option<String>,

    /// How long the guest has, from dispatch, to acquire its task, in ms.
    /// Creating the sandbox counts against it [default: 120000]
    #[serde(default = "default_start_timeout")]
    pub start_timeout: u64,

    /// How long the guest has to exit once its step has ended — the task is
    /// fulfilled or suspended — before it is destroyed anyway, in ms
    /// [default: 5000]
    #[serde(default = "default_exit_grace")]
    pub exit_grace: u64,

    /// The microsandbox CLI [default: "msb", from PATH]
    #[serde(default = "default_msb")]
    pub msb: String,
}

fn default_true() -> bool {
    true
}
fn default_concurrency() -> usize {
    16
}
fn default_start_timeout() -> u64 {
    120_000
}
fn default_exit_grace() -> u64 {
    5_000
}
fn default_msb() -> String {
    "msb".into()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: BackendKind::default(),
            cpus: None,
            memory_mib: None,
            egress: Egress::default(),
            require_digest: true,
            command: Vec::new(),
            env: BTreeMap::new(),
            concurrency: default_concurrency(),
            token: None,
            start_timeout: default_start_timeout(),
            exit_grace: default_exit_grace(),
            msb: default_msb(),
        }
    }
}

impl Config {
    fn limits(&self) -> Limits {
        Limits {
            cpus: self.cpus,
            memory_mib: self.memory_mib,
            egress: self.egress,
        }
    }
}

// ─── Addressing ──────────────────────────────────────────────────────────────

/// The image in `sandbox://<image>`.
///
/// Not parsed as a URL: `registry/repo@sha256:…` would read as userinfo and a
/// host. The image is everything after the scheme, verbatim.
pub fn parse_address(address: &str, require_digest: bool) -> Result<&str, String> {
    let image = address
        .strip_prefix("sandbox://")
        .ok_or_else(|| format!("expected sandbox://<image>, got {address}"))?;
    if image.is_empty() {
        return Err("sandbox:// needs an image".into());
    }
    if image.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(format!("not an image reference: {image:?}"));
    }
    if require_digest && !is_pinned(image) {
        return Err(format!(
            "{image} is not pinned by digest (…@sha256:<64 hex>); a tag could resolve to \
             different code on resume"
        ));
    }
    Ok(image)
}

/// `…@sha256:<64 hex>` or `…@sha512:<128 hex>`.
fn is_pinned(image: &str) -> bool {
    let Some((name, digest)) = image.rsplit_once('@') else {
        return false;
    };
    let Some((alg, hex)) = digest.split_once(':') else {
        return false;
    };
    let len = match alg {
        "sha256" => 64,
        "sha512" => 128,
        _ => return false,
    };
    !name.is_empty() && hex.len() == len && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

// ─── The worker ──────────────────────────────────────────────────────────────

/// What every session shares.
pub(crate) struct Shared<B> {
    /// Weak: the server holds the router, the router holds this worker.
    pub server: Weak<dyn ResonateServer>,
    pub backend: B,
    pub command: resonate_sandbox::Command,
    pub token: Option<String>,
    pub start_timeout: Duration,
    pub exit_grace: Duration,
}

pub struct SandboxWorker<B> {
    shared: Arc<Shared<B>>,
    require_digest: bool,
    permits: Arc<Semaphore>,
    concurrency: usize,
}

/// How long `stop` waits for running sandboxes.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

impl<B: Backend> SandboxWorker<B> {
    pub fn new(server: Weak<dyn ResonateServer>, backend: B, config: Config) -> Self {
        Self {
            shared: Arc::new(Shared {
                server,
                backend,
                command: resonate_sandbox::Command {
                    argv: config.command,
                    env: config.env.into_iter().collect(),
                },
                token: config.token,
                start_timeout: Duration::from_millis(config.start_timeout),
                exit_grace: Duration::from_millis(config.exit_grace),
            }),
            require_digest: config.require_digest,
            permits: Arc::new(Semaphore::new(config.concurrency)),
            concurrency: config.concurrency,
        }
    }
}

#[async_trait]
impl<B: Backend> ResonateWorker for SandboxWorker<B> {
    /// Start a sandbox for an `execute`.
    ///
    /// Returns once the session is running in the background. Waits for a
    /// permit first, so a full house pushes back on the loop that feeds the
    /// router rather than queueing sandboxes without bound — the store is the
    /// durable buffer.
    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        // An `unblock` is for a waiting worker. A sandboxed one does not wait:
        // it suspends, and is dispatched again when it can continue.
        let Message::Execute(execute) = msg else {
            return Ok(());
        };
        let image = parse_address(address, self.require_digest)
            .map_err(|e| Unavailable::unroutable(format!("sandbox: {e}")))?
            .to_string();
        let task = execute.data.task.clone();
        let message = serde_json::to_value(msg)
            .map_err(|e| Unavailable::new(format!("sandbox: cannot serialize message: {e}")))?;

        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Unavailable::new("sandbox: worker is stopping"))?;
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            session::run(
                shared,
                image,
                message,
                scope::Claim::new(task.id, task.version),
            )
            .await;
            drop(permit);
        });
        Ok(())
    }

    /// Wait, briefly, for running sandboxes to finish and be destroyed, then
    /// take no more.
    ///
    /// Every permit back means every session has destroyed its sandbox. One
    /// still running when the wait runs out is left to its lease; cleaning up
    /// after a plugin that is gone is out of scope for this version.
    async fn stop(&self) -> Result<(), Unavailable> {
        let all = u32::try_from(self.concurrency).unwrap_or(u32::MAX);
        let _ = tokio::time::timeout(STOP_TIMEOUT, self.permits.acquire_many(all)).await;
        self.permits.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn the_image_is_everything_after_the_scheme() {
        let addr = format!("sandbox://ghcr.io/acme/worker@{DIGEST}");
        assert_eq!(
            parse_address(&addr, true).unwrap(),
            format!("ghcr.io/acme/worker@{DIGEST}")
        );
        let addr = format!("sandbox://localhost:5000/w@{DIGEST}");
        assert!(parse_address(&addr, true).is_ok());
    }

    #[test]
    fn a_tag_is_refused_unless_allowed() {
        assert!(parse_address("sandbox://ghcr.io/acme/worker:latest", true).is_err());
        assert!(parse_address("sandbox://ghcr.io/acme/worker", true).is_err());
        assert!(parse_address("sandbox://w@sha256:abc", true).is_err());
        assert!(parse_address("sandbox://w@md5:0123456789abcdef0123456789abcdef", true).is_err());
        assert_eq!(
            parse_address("sandbox://ghcr.io/acme/worker:latest", false).unwrap(),
            "ghcr.io/acme/worker:latest"
        );
    }

    #[test]
    fn not_an_image_is_refused() {
        assert!(parse_address("sandbox://", false).is_err());
        assert!(parse_address("http://x", false).is_err());
        assert!(parse_address("sandbox://a b", false).is_err());
    }

    #[test]
    fn the_defaults_are_isolated_and_pinned() {
        let c: Config = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(!c.enabled);
        assert_eq!(c.backend, BackendKind::Microsandbox);
        assert_eq!(c.egress, Egress::None);
        assert!(c.require_digest);
        assert!(c.command.is_empty());
        assert!(c.token.is_none());
    }
}
