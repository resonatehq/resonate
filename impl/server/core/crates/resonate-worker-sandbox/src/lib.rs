//! Resonate worker: sandboxed execution.
//!
//! Runs each task dispatched to `sandbox://` inside an isolated sandbox
//! booted from the address's image. The plugin is the only component that
//! talks to the server; the guest has no network by default, and reaches the
//! server only through its own stdio — newline-delimited JSON frames, relayed
//! by rn8, the entrypoint baked into the image.
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
//! ```text
//! sandbox://<image>              the default backend
//! sandbox://<provider>/<image>   that provider: microsandbox, tensorlake, local
//! ```
//!
//! `<image>` is an OCI reference pinned by digest
//! (`ghcr.io/acme/worker@sha256:…`). A mutable tag would let a resume run
//! different code than the invoke, so a tag is refused unless
//! `require_digest = false`.
//!
//! A provider is recognised by name in the first segment, and a provider name
//! has no `.` or `:`, so it never reads as a registry host. It does read as a
//! Docker Hub namespace — `sandbox://tensorlake/foo` is the Tensorlake
//! provider, not the image `tensorlake/foo` — so a Docker Hub image is named in
//! full: `sandbox://docker.io/tensorlake/foo@sha256:…`.
//!
//! Which function runs, with which arguments, is the promise's business: the
//! plugin is function-agnostic. CPU, memory and egress are this plugin's
//! configuration, so they are in neither the address nor the backend trait.

mod dynamic;
mod local;
pub mod scope;
mod session;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use resonate_plugin::types::Message;
use resonate_plugin::{ResonateServer, ResonateWorker, Unavailable};
use resonate_sandbox::{Egress, Limits};
use resonate_sandbox_microsandbox::Microsandbox;
use resonate_sandbox_tensorlake::Tensorlake;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

pub use dynamic::{AnyBackend, AnyProcess};
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

    let mut backends: HashMap<Provider, Arc<dyn AnyBackend>> = HashMap::new();
    for provider in config.providers() {
        let backend: Arc<dyn AnyBackend> = match provider {
            Provider::Microsandbox => Arc::new(Microsandbox::new(
                config.microsandbox.msb.clone(),
                config.limits(),
            )),
            Provider::Tensorlake => {
                let tl = &config.tensorlake;
                // The key is the operator's, in this section or by the name
                // Tensorlake's own tools read it from.
                let api_key = tl
                    .api_key
                    .clone()
                    .or_else(|| std::env::var("TENSORLAKE_API_KEY").ok())
                    .filter(|k| !k.is_empty());
                if api_key.is_none() {
                    return Err(settings.reject(
                        "tensorlake.api_key",
                        "the tensorlake provider needs an API key: set it here, or \
                         TENSORLAKE_API_KEY",
                    ));
                }
                if tl.timeout_secs == 0 {
                    return Err(settings.reject("tensorlake.timeout_secs", "must be at least 1"));
                }
                Arc::new(Tensorlake::new(
                    resonate_sandbox_tensorlake::Options {
                        api_url: tl.api_url.clone(),
                        proxy_url: tl.proxy_url.clone(),
                        api_key,
                        timeout_secs: tl.timeout_secs,
                        ready_timeout: Duration::from_millis(tl.ready_timeout),
                        ..resonate_sandbox_tensorlake::Options::default()
                    },
                    config.limits(),
                ))
            }
            Provider::Local => {
                if config.command.is_empty() {
                    return Err(settings.reject(
                        "command",
                        "the local provider has no image to take an entrypoint from; name the \
                         command to run (rn8 and the worker)",
                    ));
                }
                if config.egress == Egress::None {
                    return Err(settings.reject(
                        "egress",
                        "the local provider runs on the host and cannot take the network away; \
                         set egress = \"all\" to say you know",
                    ));
                }
                Arc::new(Local::new())
            }
        };
        backends.insert(provider, backend);
    }
    Ok(Some(Arc::new(SandboxWorker::new(
        deps.server,
        backends,
        config,
    ))))
}

/// Who creates the sandboxes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// A microsandbox microVM per task, through the `msb` CLI.
    #[default]
    Microsandbox,
    /// A Tensorlake sandbox per task, through Tensorlake's API.
    Tensorlake,
    /// No sandbox at all: `command` runs as a host process, and the image is
    /// ignored. For developing an image's worker, and for tests — never for
    /// code you do not trust.
    Local,
}

impl Provider {
    pub const ALL: [Provider; 3] = [
        Provider::Microsandbox,
        Provider::Tensorlake,
        Provider::Local,
    ];

    /// The name in `sandbox://<provider>/<image>` and in the configuration.
    pub fn name(self) -> &'static str {
        match self {
            Provider::Microsandbox => "microsandbox",
            Provider::Tensorlake => "tensorlake",
            Provider::Local => "local",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.name() == name)
    }
}

/// Everything under `[workers.worker_sandbox]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Enable the sandbox:// address scheme [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// The provider `sandbox://<image>` uses, always enabled [default: microsandbox]
    #[serde(default)]
    pub backend: Provider,

    /// Virtual CPUs per sandbox [default: the provider's]
    #[serde(default)]
    pub cpus: Option<u8>,

    /// Memory per sandbox, in MiB [default: the provider's]
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

    /// Sandboxes running at once, across providers [default: 16]
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

    /// `[workers.worker_sandbox.microsandbox]`
    #[serde(default)]
    pub microsandbox: MicrosandboxConfig,

    /// `[workers.worker_sandbox.tensorlake]`
    #[serde(default)]
    pub tensorlake: TensorlakeConfig,

    /// `[workers.worker_sandbox.local]`
    #[serde(default)]
    pub local: LocalConfig,
}

/// The microsandbox provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicrosandboxConfig {
    /// Serve `sandbox://microsandbox/<image>` even when it is not the default
    /// [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// The microsandbox CLI [default: "msb", from PATH]
    #[serde(default = "default_msb")]
    pub msb: String,
}

impl Default for MicrosandboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            msb: default_msb(),
        }
    }
}

/// The Tensorlake provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TensorlakeConfig {
    /// Serve `sandbox://tensorlake/<image>` even when it is not the default
    /// [default: false]
    #[serde(default)]
    pub enabled: bool,

    /// The API key. Absent reads TENSORLAKE_API_KEY [default: none]
    #[serde(default)]
    pub api_key: Option<String>,

    /// The control plane [default: https://api.tensorlake.ai]
    #[serde(default = "default_tensorlake_api")]
    pub api_url: String,

    /// The sandbox proxy, used when a sandbox does not name its own URL
    /// [default: https://sandbox.tensorlake.ai]
    #[serde(default = "default_tensorlake_proxy")]
    pub proxy_url: String,

    /// Tensorlake terminates a sandbox after this long, whatever happens to
    /// the plugin — the backstop for a sandbox whose plugin died before
    /// destroying it, in seconds [default: 900]
    #[serde(default = "default_tensorlake_timeout")]
    pub timeout_secs: u64,

    /// How long a new sandbox may take to start running, in ms [default: 120000]
    #[serde(default = "default_start_timeout")]
    pub ready_timeout: u64,
}

impl Default for TensorlakeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key: None,
            api_url: default_tensorlake_api(),
            proxy_url: default_tensorlake_proxy(),
            timeout_secs: default_tensorlake_timeout(),
            ready_timeout: default_start_timeout(),
        }
    }
}

/// The local provider.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// Serve `sandbox://local/<image>` even when it is not the default
    /// [default: false]
    #[serde(default)]
    pub enabled: bool,
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
fn default_tensorlake_api() -> String {
    "https://api.tensorlake.ai".into()
}
fn default_tensorlake_proxy() -> String {
    "https://sandbox.tensorlake.ai".into()
}
fn default_tensorlake_timeout() -> u64 {
    900
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: Provider::default(),
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
            microsandbox: MicrosandboxConfig::default(),
            tensorlake: TensorlakeConfig::default(),
            local: LocalConfig::default(),
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

    /// The providers to build: the default, and every one switched on.
    pub fn providers(&self) -> Vec<Provider> {
        Provider::ALL
            .into_iter()
            .filter(|p| {
                *p == self.backend
                    || match p {
                        Provider::Microsandbox => self.microsandbox.enabled,
                        Provider::Tensorlake => self.tensorlake.enabled,
                        Provider::Local => self.local.enabled,
                    }
            })
            .collect()
    }
}

// ─── Addressing ──────────────────────────────────────────────────────────────

/// What a `sandbox://` address names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target<'a> {
    /// `None` for `sandbox://<image>`: the default.
    pub provider: Option<Provider>,
    pub image: &'a str,
}

/// `sandbox://<image>` or `sandbox://<provider>/<image>`.
///
/// Not parsed as a URL: `registry/repo@sha256:…` would read as userinfo and a
/// host. The address is split by hand: a first segment that is a provider's
/// name is the provider, and everything after it is the image, verbatim.
pub fn parse_address(address: &str, require_digest: bool) -> Result<Target<'_>, String> {
    let rest = address
        .strip_prefix("sandbox://")
        .ok_or_else(|| format!("expected sandbox://[<provider>/]<image>, got {address}"))?;
    let (provider, image) = match rest.split_once('/') {
        Some((first, image)) => match Provider::from_name(first) {
            Some(p) => (Some(p), image),
            None => (None, rest),
        },
        None => match Provider::from_name(rest) {
            Some(p) => (Some(p), ""),
            None => (None, rest),
        },
    };
    if image.is_empty() {
        return Err(match provider {
            Some(p) => format!("sandbox://{}/ needs an image", p.name()),
            None => "sandbox:// needs an image".into(),
        });
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
    Ok(Target { provider, image })
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
pub(crate) struct Shared {
    /// Weak: the server holds the router, the router holds this worker.
    pub server: Weak<dyn ResonateServer>,
    pub command: resonate_sandbox::Command,
    pub token: Option<String>,
    pub start_timeout: Duration,
    pub exit_grace: Duration,
}

pub struct SandboxWorker {
    shared: Arc<Shared>,
    backends: HashMap<Provider, Arc<dyn AnyBackend>>,
    default: Provider,
    require_digest: bool,
    permits: Arc<Semaphore>,
    concurrency: usize,
}

/// How long `stop` waits for running sandboxes.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

impl SandboxWorker {
    /// `backends` must hold `config.backend`, the default.
    pub fn new(
        server: Weak<dyn ResonateServer>,
        backends: HashMap<Provider, Arc<dyn AnyBackend>>,
        config: Config,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                server,
                command: resonate_sandbox::Command {
                    argv: config.command,
                    env: config.env.into_iter().collect(),
                },
                token: config.token,
                start_timeout: Duration::from_millis(config.start_timeout),
                exit_grace: Duration::from_millis(config.exit_grace),
            }),
            backends,
            default: config.backend,
            require_digest: config.require_digest,
            permits: Arc::new(Semaphore::new(config.concurrency)),
            concurrency: config.concurrency,
        }
    }
}

#[async_trait]
impl ResonateWorker for SandboxWorker {
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
        let target = parse_address(address, self.require_digest)
            .map_err(|e| Unavailable::unroutable(format!("sandbox: {e}")))?;
        let provider = target.provider.unwrap_or(self.default);
        let backend = self.backends.get(&provider).cloned().ok_or_else(|| {
            Unavailable::unroutable(format!(
                "sandbox: the {} provider is not enabled ({address})",
                provider.name()
            ))
        })?;
        let image = target.image.to_string();
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
                backend,
                provider,
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

    fn target(address: &str) -> Target<'_> {
        parse_address(address, true).unwrap()
    }

    #[test]
    fn the_image_is_everything_after_the_scheme() {
        let addr = format!("sandbox://ghcr.io/acme/worker@{DIGEST}");
        assert_eq!(
            target(&addr),
            Target {
                provider: None,
                image: &format!("ghcr.io/acme/worker@{DIGEST}"),
            }
        );
        let addr = format!("sandbox://localhost:5000/w@{DIGEST}");
        assert_eq!(target(&addr).provider, None);
        let addr = format!("sandbox://w@{DIGEST}");
        assert_eq!(target(&addr).image, format!("w@{DIGEST}"));
    }

    #[test]
    fn a_provider_is_the_first_segment_by_name() {
        let addr = format!("sandbox://tensorlake/ghcr.io/acme/worker@{DIGEST}");
        assert_eq!(
            target(&addr),
            Target {
                provider: Some(Provider::Tensorlake),
                image: &format!("ghcr.io/acme/worker@{DIGEST}"),
            }
        );
        for p in Provider::ALL {
            let addr = format!("sandbox://{}/w@{DIGEST}", p.name());
            assert_eq!(target(&addr).provider, Some(p));
            assert_eq!(target(&addr).image, format!("w@{DIGEST}"));
        }
        // A namespace that is not a provider stays part of the image.
        let addr = format!("sandbox://acme/worker@{DIGEST}");
        assert_eq!(target(&addr).provider, None);
        assert_eq!(target(&addr).image, format!("acme/worker@{DIGEST}"));
        // Spelled in full, a Docker Hub namespace named like a provider is an image.
        let addr = format!("sandbox://docker.io/tensorlake/worker@{DIGEST}");
        assert_eq!(target(&addr).provider, None);
    }

    #[test]
    fn a_provider_needs_an_image() {
        assert!(parse_address("sandbox://tensorlake", false).is_err());
        assert!(parse_address("sandbox://tensorlake/", false).is_err());
    }

    #[test]
    fn a_tag_is_refused_unless_allowed() {
        assert!(parse_address("sandbox://ghcr.io/acme/worker:latest", true).is_err());
        assert!(parse_address("sandbox://tensorlake/acme/worker:latest", true).is_err());
        assert!(parse_address("sandbox://ghcr.io/acme/worker", true).is_err());
        assert!(parse_address("sandbox://w@sha256:abc", true).is_err());
        assert!(parse_address("sandbox://w@md5:0123456789abcdef0123456789abcdef", true).is_err());
        assert_eq!(
            parse_address("sandbox://tensorlake/python:3.12", false).unwrap(),
            Target {
                provider: Some(Provider::Tensorlake),
                image: "python:3.12"
            }
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
        assert_eq!(c.backend, Provider::Microsandbox);
        assert_eq!(c.providers(), [Provider::Microsandbox]);
        assert_eq!(c.egress, Egress::None);
        assert!(c.require_digest);
        assert!(c.command.is_empty());
        assert!(c.token.is_none());
    }

    struct NoServer;

    #[async_trait]
    impl ResonateServer for NoServer {
        async fn process(
            &self,
            _: &resonate_plugin::types::RequestEnvelope,
        ) -> Result<resonate_plugin::types::ResponseEnvelope, Unavailable> {
            Err(Unavailable::new("no server"))
        }
    }

    fn execute() -> Message {
        use resonate_plugin::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};
        Message::Execute(ExecuteMsg {
            kind: "execute".into(),
            head: MessageHead {
                server_url: String::new(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: "t".into(),
                    version: 0,
                },
            },
        })
    }

    /// A provider named in the address but not switched on is refused, not
    /// quietly sent to the default: the image was meant for somewhere else.
    #[tokio::test]
    async fn a_provider_that_is_not_enabled_is_unroutable() {
        let mut backends: HashMap<Provider, Arc<dyn AnyBackend>> = HashMap::new();
        backends.insert(Provider::Local, Arc::new(Local::new()));
        let worker = SandboxWorker::new(
            Weak::<NoServer>::new(),
            backends,
            Config {
                backend: Provider::Local,
                ..Config::default()
            },
        );
        let addr = format!("sandbox://tensorlake/w@{DIGEST}");
        let e = worker.process(&addr, &execute()).await.unwrap_err();
        assert!(
            e.message.contains("tensorlake provider is not enabled"),
            "{e}"
        );
    }

    #[test]
    fn the_default_provider_is_always_on_and_others_by_section() {
        let c: Config = serde_json::from_value(serde_json::json!({
            "backend": "tensorlake",
            "local": { "enabled": true },
        }))
        .unwrap();
        assert_eq!(c.providers(), [Provider::Tensorlake, Provider::Local]);
    }
}
