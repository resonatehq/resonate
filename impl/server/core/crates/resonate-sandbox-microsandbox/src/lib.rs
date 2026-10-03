//! Resonate sandbox backend: microsandbox.
//!
//! Each task gets its own microVM, booted from the task's image:
//!
//! | trait     | msb                                                          |
//! |-----------|--------------------------------------------------------------|
//! | `create`  | `msb create <image> --name <n> [--no-net] [--cpus] [--memory]` |
//! | `exec`    | `msb exec <n> --stream -- <argv>`                            |
//! | `destroy` | `msb rm --force <n>`                                         |
//!
//! `--stream` is msb's byte-faithful mode: stdin and stdout are piped both ways
//! with no PTY, so no echo and no CRLF translation — which is what frames,
//! newline-delimited JSON, need.
//!
//! An empty `argv` runs the image's own entrypoint and command. `msb exec`
//! needs an explicit command, so `exec` resolves them from the image's config
//! (`msb image inspect --format json`) — the image `create` already pulled.

use std::process::Stdio;
use std::sync::Arc;

use resonate_sandbox::{Backend, ChildProcess, Command, Egress, Limits};
use tokio::process::Command as HostCommand;

/// How to reach microsandbox.
#[derive(Debug, Clone)]
pub struct Microsandbox {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// The `msb` executable.
    msb: String,
    limits: Limits,
}

/// A sandbox this backend created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handle {
    pub name: String,
    pub image: String,
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "microsandbox: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl Microsandbox {
    /// `msb` is the CLI to run, `"msb"` to find it on `PATH`.
    pub fn new(msb: impl Into<String>, limits: Limits) -> Self {
        Self {
            inner: Arc::new(Inner {
                msb: msb.into(),
                limits,
            }),
        }
    }

    fn msb(&self) -> HostCommand {
        let mut cmd = HostCommand::new(&self.inner.msb);
        // A sandbox outlives no plugin that is still waiting on it, but the
        // CLI must not outlive a plugin that stopped waiting.
        cmd.kill_on_drop(true);
        cmd
    }

    /// The `msb create` arguments for one sandbox.
    fn create_args(&self, name: &str, image: &str) -> Vec<String> {
        let mut args = vec![
            "create".to_string(),
            image.to_string(),
            "--name".to_string(),
            name.to_string(),
            "--quiet".to_string(),
            "--label".to_string(),
            "resonate=sandbox".to_string(),
            // The image is pinned by digest, so the cached copy is the copy.
            "--pull".to_string(),
            "if-missing".to_string(),
        ];
        let limits = &self.inner.limits;
        if let Some(cpus) = limits.cpus {
            args.extend(["--cpus".to_string(), cpus.to_string()]);
        }
        if let Some(mib) = limits.memory_mib {
            args.extend(["--memory".to_string(), format!("{mib}M")]);
        }
        match &limits.egress {
            Egress::None => args.push("--no-net".to_string()),
            Egress::All => {}
            Egress::Allow(hosts) => {
                args.extend(["--net-default-egress".to_string(), "deny".to_string()]);
                for host in hosts {
                    args.extend(["--net-rule".to_string(), format!("allow@{host}")]);
                }
            }
        }
        args
    }

    /// The `msb exec` arguments for one command.
    fn exec_args(name: &str, argv: &[String], env: &[(String, String)]) -> Vec<String> {
        let mut args = vec![
            "exec".to_string(),
            name.to_string(),
            "--stream".to_string(),
            "--quiet".to_string(),
        ];
        for (k, v) in env {
            args.extend(["--env".to_string(), format!("{k}={v}")]);
        }
        args.push("--".to_string());
        args.extend(argv.iter().cloned());
        args
    }

    /// The image's entrypoint followed by its command, as OCI resolves them.
    async fn default_argv(&self, image: &str) -> Result<Vec<String>, Error> {
        let out = self
            .msb()
            .args(["image", "inspect", image, "--format", "json"])
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| Error(format!("cannot run {}: {e}", self.inner.msb)))?;
        if !out.status.success() {
            return Err(Error(format!(
                "image inspect {image}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let detail: serde_json::Value = serde_json::from_slice(&out.stdout)
            .map_err(|e| Error(format!("image inspect {image}: not JSON: {e}")))?;
        default_argv_from(&detail).ok_or_else(|| {
            Error(format!(
                "image {image} has neither an entrypoint nor a command"
            ))
        })
    }
}

/// `entrypoint ++ cmd` from `msb image inspect --format json`.
fn default_argv_from(detail: &serde_json::Value) -> Option<Vec<String>> {
    let config = detail.get("config")?;
    let list = |key: &str| -> Vec<String> {
        config
            .get(key)
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let mut argv = list("entrypoint");
    argv.extend(list("cmd"));
    (!argv.is_empty()).then_some(argv)
}

impl Backend for Microsandbox {
    type Handle = Handle;
    type Process = ChildProcess;
    type Error = Error;

    async fn create(&self, image: &str) -> Result<Handle, Error> {
        let name = format!("resonate-{:016x}", fastrand::u64(..));
        let out = self
            .msb()
            .args(self.create_args(&name, image))
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| Error(format!("cannot run {}: {e}", self.inner.msb)))?;
        if !out.status.success() {
            return Err(Error(format!(
                "create {image}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(Handle {
            name,
            image: image.to_string(),
        })
    }

    async fn exec(&self, handle: &Handle, command: Command) -> Result<ChildProcess, Error> {
        let argv = if command.argv.is_empty() {
            self.default_argv(&handle.image).await?
        } else {
            command.argv
        };
        let child = self
            .msb()
            .args(Self::exec_args(&handle.name, &argv, &command.env))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error(format!("cannot run {}: {e}", self.inner.msb)))?;
        Ok(ChildProcess::new(child))
    }

    async fn destroy(&self, handle: Handle) -> Result<(), Error> {
        let out = self
            .msb()
            .args(["rm", "--force", "--quiet", &handle.name])
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| Error(format!("cannot run {}: {e}", self.inner.msb)))?;
        if out.status.success() {
            return Ok(());
        }
        // Idempotent: a sandbox that is already gone is destroyed. Asked
        // rather than read out of the error text, which is msb's to reword.
        let gone = !self
            .msb()
            .args(["inspect", &handle.name, "--format", "json"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false);
        if gone {
            return Ok(());
        }
        Err(Error(format!(
            "rm {}: {}",
            handle.name,
            String::from_utf8_lossy(&out.stderr).trim()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn create_asks_for_the_limits_and_no_network() {
        let b = Microsandbox::new(
            "msb",
            Limits {
                cpus: Some(2),
                memory_mib: Some(512),
                egress: Egress::None,
            },
        );
        let args = b.create_args("n", "ghcr.io/a/b@sha256:00");
        assert_eq!(
            &args[..4],
            ["create", "ghcr.io/a/b@sha256:00", "--name", "n"]
        );
        let joined = args.join(" ");
        assert!(joined.contains("--cpus 2"), "{joined}");
        assert!(joined.contains("--memory 512M"), "{joined}");
        assert!(joined.contains("--no-net"), "{joined}");
    }

    #[test]
    fn egress_all_leaves_the_network_alone() {
        let b = Microsandbox::new(
            "msb",
            Limits {
                egress: Egress::All,
                ..Limits::default()
            },
        );
        let args = b.create_args("n", "img");
        assert!(!args
            .iter()
            .any(|a| a == "--no-net" || a == "--cpus" || a == "--memory"));
    }

    #[test]
    fn an_allow_list_denies_by_default_and_allows_each_host() {
        let b = Microsandbox::new(
            "msb",
            Limits {
                egress: Egress::Allow(vec!["a.example".into(), "10.0.0.0/8".into()]),
                ..Limits::default()
            },
        );
        let joined = b.create_args("n", "img").join(" ");
        assert!(
            joined.ends_with(
                "--net-default-egress deny --net-rule allow@a.example --net-rule allow@10.0.0.0/8"
            ),
            "{joined}"
        );
        assert!(!joined.contains("--no-net"));
    }

    #[test]
    fn exec_streams_and_puts_the_command_last() {
        let args = Microsandbox::exec_args(
            "n",
            &["rn8".into(), "--".into(), "node".into()],
            &[("K".into(), "v".into())],
        );
        assert_eq!(
            args,
            ["exec", "n", "--stream", "--quiet", "--env", "K=v", "--", "rn8", "--", "node"]
        );
    }

    #[test]
    fn the_default_command_is_entrypoint_then_cmd() {
        let detail = json!({"config": {"entrypoint": ["rn8", "--"], "cmd": ["node", "w.js"]}});
        assert_eq!(
            default_argv_from(&detail).unwrap(),
            ["rn8", "--", "node", "w.js"]
        );
        let detail = json!({"config": {"entrypoint": null, "cmd": ["sh"]}});
        assert_eq!(default_argv_from(&detail).unwrap(), ["sh"]);
        assert!(default_argv_from(&json!({"config": {}})).is_none());
        assert!(default_argv_from(&json!({})).is_none());
    }
}
