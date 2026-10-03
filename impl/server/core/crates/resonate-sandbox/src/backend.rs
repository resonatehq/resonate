//! What the plugin asks of a sandbox runtime.
//!
//! A backend creates a sandbox from an image, runs one command in it with live
//! stdio, and destroys it. Nothing else: CPU, memory and egress are the
//! backend's own configuration — it is constructed from the plugin's — so they
//! are neither in the address nor in this trait. Warm start, if a backend has
//! one, is an optimisation inside `create`.
//!
//! microsandbox is the first implementation. Firecracker and the Kubernetes
//! agent-sandbox API fit the same three calls.

use std::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};

/// One command to run in a sandbox.
///
/// An empty `argv` means the image's own entrypoint and command, which is what
/// the plugin runs by default: rn8 is the entrypoint, followed by the worker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Command {
    pub argv: Vec<String>,
    pub env: Vec<(String, String)>,
}

/// What every sandbox a backend creates is given, from the plugin's
/// configuration.
///
/// Not part of the trait: a backend is *constructed* with these, so every
/// sandbox it makes gets the same, and a task cannot ask for more.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Virtual CPUs. Absent means the backend's default.
    #[serde(default)]
    pub cpus: Option<u8>,
    /// Memory in MiB. Absent means the backend's default.
    #[serde(default)]
    pub memory_mib: Option<u32>,
    /// Outbound network for the guest [default: none].
    #[serde(default)]
    pub egress: Egress,
}

/// Outbound network for the guest.
///
/// `none` is the point of the design: the plugin is the only thing that talks
/// to the server, and it does so over the guest's stdio, which needs no
/// network at all. A guest whose work *is* the network — a browser — gets an
/// allow-list rather than everything, so that what it can reach is the sites
/// it was sent to and not the host's neighbours.
///
/// In configuration: `"none"`, `"all"`, or `{ allow = ["example.com", …] }`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Egress {
    #[default]
    None,
    All,
    /// Hostnames, IPs or CIDRs; everything else is refused.
    Allow(Vec<String>),
}

impl serde::Serialize for Egress {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Egress::None => s.serialize_str("none"),
            Egress::All => s.serialize_str("all"),
            Egress::Allow(hosts) => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("allow", hosts)?;
                m.end()
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for Egress {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Mode(String),
            Allow { allow: Vec<String> },
        }
        match Wire::deserialize(d)? {
            Wire::Mode(m) if m == "none" => Ok(Egress::None),
            Wire::Mode(m) if m == "all" => Ok(Egress::All),
            Wire::Mode(m) => Err(serde::de::Error::custom(format!(
                "egress is \"none\", \"all\" or {{ allow = [...] }}, not {m:?}"
            ))),
            Wire::Allow { allow } if allow.is_empty() => Err(serde::de::Error::custom(
                "an empty allow-list is egress = \"none\"; say that instead",
            )),
            Wire::Allow { allow } => Ok(Egress::Allow(allow)),
        }
    }
}

pub trait Backend: Send + Sync + 'static {
    type Handle: Send + Sync;
    type Process: Process;
    type Error: std::error::Error + Send + Sync + 'static;

    /// `image` is the digest-pinned OCI reference from `sandbox://<image>`.
    fn create(&self, image: &str)
        -> impl Future<Output = Result<Self::Handle, Self::Error>> + Send;

    fn exec(
        &self,
        handle: &Self::Handle,
        command: Command,
    ) -> impl Future<Output = Result<Self::Process, Self::Error>> + Send;

    /// Idempotent: succeeds if the sandbox is already gone.
    fn destroy(&self, handle: Self::Handle)
        -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Frames, plugin to guest.
pub type Stdin = Box<dyn AsyncWrite + Send + Unpin>;
/// Frames, guest to plugin.
pub type Stdout = Box<dyn AsyncRead + Send + Unpin>;
/// Diagnostics from rn8.
pub type Stderr = Box<dyn AsyncRead + Send + Unpin>;

/// A command running in a sandbox.
///
/// The three streams are *taken*, not borrowed. The plugin writes frames,
/// reads frames, drains diagnostics and waits for the exit status all at once,
/// and three `&mut self` accessors cannot be held at the same time — so each
/// stream is handed over once, the way `tokio::process::Child` does it, and
/// the process keeps only what `wait` needs.
pub trait Process: Send + 'static {
    /// Frames, plugin to guest. `None` once taken.
    fn stdin(&mut self) -> Option<Stdin>;
    /// Frames, guest to plugin. `None` once taken.
    fn stdout(&mut self) -> Option<Stdout>;
    /// Diagnostics from rn8. `None` once taken.
    fn stderr(&mut self) -> Option<Stderr>;
    /// Exit status.
    fn wait(&mut self) -> impl Future<Output = std::io::Result<i32>> + Send;
}

/// A [`Process`] that is a host child process.
///
/// What a backend driven through a CLI hands back: `msb exec`, `docker run -i`
/// and a plain local spawn all run the guest command's stdio through a host
/// process's pipes, so they share this.
pub struct ChildProcess {
    child: tokio::process::Child,
}

impl ChildProcess {
    /// `child` must have been spawned with all three streams piped.
    pub fn new(child: tokio::process::Child) -> Self {
        Self { child }
    }

    /// The host pid, while the process has not been reaped.
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }
}

impl Process for ChildProcess {
    fn stdin(&mut self) -> Option<Stdin> {
        self.child.stdin.take().map(|s| Box::new(s) as Stdin)
    }

    fn stdout(&mut self) -> Option<Stdout> {
        self.child.stdout.take().map(|s| Box::new(s) as Stdout)
    }

    fn stderr(&mut self) -> Option<Stderr> {
        self.child.stderr.take().map(|s| Box::new(s) as Stderr)
    }

    async fn wait(&mut self) -> std::io::Result<i32> {
        let status = self.child.wait().await?;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(sig) = status.signal() {
                return Ok(128 + sig);
            }
        }
        Ok(status.code().unwrap_or(-1))
    }
}
