//! rn8's command line.
//!
//! Hand-parsed: four options and a command. rn8 is in every image, so it
//! carries nothing it does not need.

use std::time::Duration;

pub const USAGE: &str = "\
usage: rn8 [options] -- <worker command> [args...]

options:
  --worker-port <port>     the port the worker listens on for the push
                           [env RN8_WORKER_PORT, default 8080; passed to the worker as PORT]
                           0 picks a free one: for several guests sharing a host
  --push-path <path>       the path the task is POSTed to [env RN8_PUSH_PATH, default /]
  --ready-timeout <ms>     how long to wait for the worker to accept connections
                           [env RN8_READY_TIMEOUT, default 60000]
  --env <key>=<value>      an extra variable for the worker; repeatable
  --listen <port>          take frames over TCP instead of stdin and stdout
                           [env RN8_LISTEN]: for a provider with no stdin.
                           Needs RN8_TOKEN; the first connection to send it,
                           as its first line, is the plugin's, and the only one

The worker is started with RESONATE_URL set to rn8's loopback relay.";

#[derive(Debug, Clone, PartialEq)]
pub struct Args {
    pub worker_port: u16,
    pub push_path: String,
    pub ready_timeout: Duration,
    pub env: Vec<(String, String)>,
    pub command: Vec<String>,
    /// Frames over TCP on this port, behind this token.
    pub listen: Option<(u16, String)>,
}

impl Args {
    pub fn parse(argv: impl IntoIterator<Item = String>) -> Result<Self, String> {
        Self::parse_with(argv, |k| std::env::var(k).ok())
    }

    fn parse_with(
        argv: impl IntoIterator<Item = String>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let mut worker_port = env("RN8_WORKER_PORT");
        let mut push_path = env("RN8_PUSH_PATH");
        let mut ready_timeout = env("RN8_READY_TIMEOUT");
        let mut listen = env("RN8_LISTEN");
        let mut extra = Vec::new();
        let mut command = None;

        let mut argv = argv.into_iter();
        while let Some(arg) = argv.next() {
            let mut value = |name: &str| argv.next().ok_or_else(|| format!("{name} needs a value"));
            match arg.as_str() {
                "--" => {
                    command = Some(argv.by_ref().collect::<Vec<_>>());
                    break;
                }
                "--worker-port" => worker_port = Some(value("--worker-port")?),
                "--push-path" => push_path = Some(value("--push-path")?),
                "--ready-timeout" => ready_timeout = Some(value("--ready-timeout")?),
                "--listen" => listen = Some(value("--listen")?),
                "--env" => {
                    let kv = value("--env")?;
                    let (k, v) = kv
                        .split_once('=')
                        .ok_or_else(|| format!("--env takes key=value, got {kv:?}"))?;
                    extra.push((k.to_string(), v.to_string()));
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }

        let command = command
            .filter(|c| !c.is_empty())
            .ok_or("no worker command; it follows --")?;
        let worker_port = match worker_port {
            None => 8080,
            Some(p) => p
                .parse::<u16>()
                .map_err(|_| format!("worker port must be 0-65535, got {p:?}"))?,
        };
        let push_path = push_path.unwrap_or_else(|| "/".into());
        if !push_path.starts_with('/') {
            return Err(format!("push path must start with '/', got {push_path:?}"));
        }
        let ready_timeout = match ready_timeout {
            None => Duration::from_secs(60),
            Some(ms) => Duration::from_millis(
                ms.parse()
                    .map_err(|_| format!("ready timeout must be milliseconds, got {ms:?}"))?,
            ),
        };
        let listen = match listen {
            None => None,
            Some(p) => {
                let port = p
                    .parse::<u16>()
                    .ok()
                    .filter(|p| *p != 0)
                    .ok_or_else(|| format!("listen port must be 1-65535, got {p:?}"))?;
                // Without it, whoever connects first would be the plugin.
                let token = env("RN8_TOKEN")
                    .filter(|t| t.len() >= 16)
                    .ok_or("--listen needs RN8_TOKEN, at least 16 characters")?;
                // Both on this guest: the push would reach rn8, not the worker.
                if port == worker_port {
                    return Err(format!(
                        "listen port {port} is the worker's port; give one of them another"
                    ));
                }
                Some((port, token))
            }
        };
        Ok(Self {
            worker_port,
            push_path,
            ready_timeout,
            env: extra,
            command,
            listen,
        })
    }

    /// A worker port of 0 is a free one, chosen now.
    ///
    /// Inside a sandbox the guest has its own network and 8080 is always
    /// free. Several guests sharing one host — the local provider — do not, so
    /// each asks for a port of its own. The port is released before the worker
    /// binds it, which leaves a window; on loopback, for a port the kernel just
    /// handed out, that is a window nobody else is aiming at.
    pub fn resolve_worker_port(&mut self) -> std::io::Result<()> {
        if self.worker_port == 0 {
            let probe = std::net::TcpListener::bind(("127.0.0.1", 0))?;
            self.worker_port = probe.local_addr()?.port();
        }
        Ok(())
    }

    /// Where the task is pushed: always loopback.
    pub fn push_target(&self) -> crate::relay::PushTarget {
        crate::relay::PushTarget {
            port: self.worker_port,
            path: self.push_path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(argv: &[&str]) -> Result<Args, String> {
        Args::parse_with(argv.iter().map(|s| s.to_string()), |_| None)
    }

    #[test]
    fn the_command_follows_the_separator() {
        let a = parse(&["--", "node", "worker.js", "--port", "1"]).unwrap();
        assert_eq!(a.command, ["node", "worker.js", "--port", "1"]);
        assert_eq!(a.worker_port, 8080);
        assert_eq!(a.push_path, "/");
        assert_eq!(a.ready_timeout, Duration::from_secs(60));
    }

    #[test]
    fn options_come_before_the_separator() {
        let a = parse(&[
            "--worker-port",
            "9000",
            "--push-path",
            "/push",
            "--ready-timeout",
            "500",
            "--env",
            "A=b=c",
            "--",
            "python3",
        ])
        .unwrap();
        assert_eq!(a.worker_port, 9000);
        assert_eq!(a.push_path, "/push");
        assert_eq!(a.ready_timeout, Duration::from_millis(500));
        assert_eq!(a.env, [("A".to_string(), "b=c".to_string())]);
    }

    #[test]
    fn the_environment_is_a_default_the_flags_override() {
        let env = |k: &str| (k == "RN8_WORKER_PORT").then(|| "7000".to_string());
        let a = Args::parse_with(["--", "x"].map(String::from), env).unwrap();
        assert_eq!(a.worker_port, 7000);
        let a =
            Args::parse_with(["--worker-port", "7001", "--", "x"].map(String::from), env).unwrap();
        assert_eq!(a.worker_port, 7001);
    }

    #[test]
    fn refuses_what_it_cannot_run() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--"]).is_err());
        assert!(parse(&["node"]).is_err());
        assert!(parse(&["--worker-port", "70000", "--", "x"]).is_err());
        assert!(parse(&["--worker-port", "x", "--", "x"]).is_err());
        assert!(parse(&["--push-path", "push", "--", "x"]).is_err());
        assert!(parse(&["--env", "novalue", "--", "x"]).is_err());
        assert!(parse(&["--listen", "8443", "--", "x"]).is_err(), "no token");
        assert!(parse(&["--listen", "0", "--", "x"]).is_err());
    }

    #[test]
    fn listening_takes_its_token_from_the_environment() {
        let token = "0123456789abcdef0123";
        let env = |k: &str| (k == "RN8_TOKEN").then(|| token.to_string());
        let a = Args::parse_with(["--listen", "8443", "--", "x"].map(String::from), env).unwrap();
        assert_eq!(a.listen, Some((8443, token.to_string())));
        let short = |k: &str| (k == "RN8_TOKEN").then(|| "short".to_string());
        assert!(
            Args::parse_with(["--listen", "8443", "--", "x"].map(String::from), short).is_err()
        );
        assert_eq!(parse(&["--", "x"]).unwrap().listen, None);
        // 8080 is the worker's port by default.
        assert!(Args::parse_with(["--listen", "8080", "--", "x"].map(String::from), env).is_err());
    }
}
