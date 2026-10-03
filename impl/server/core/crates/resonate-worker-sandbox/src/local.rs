//! The local backend: no sandbox at all.
//!
//! `exec` runs the command as a host process and `create` ignores the image.
//! It exists so an image's worker can be developed — rn8 and the SDK, against
//! a real server — without a VM, and so the plugin can be tested end to end on
//! any machine. It isolates nothing; `configure` makes the operator say
//! `egress = "all"` to use it, so that is never a surprise.

use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use resonate_sandbox::{Backend, ChildProcess, Command};

#[derive(Debug, Default)]
pub struct Local;

impl Local {
    pub fn new() -> Self {
        Self
    }
}

/// The process group `exec` started, for `destroy` to kill.
#[derive(Debug, Default)]
pub struct Handle {
    group: Mutex<Option<i32>>,
}

#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "local: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl Backend for Local {
    type Handle = Handle;
    type Process = ChildProcess;
    type Error = Error;

    async fn create(&self, _image: &str) -> Result<Handle, Error> {
        Ok(Handle::default())
    }

    async fn exec(&self, handle: &Handle, command: Command) -> Result<ChildProcess, Error> {
        let (program, args) = command
            .argv
            .split_first()
            .ok_or_else(|| Error("no command to run".into()))?;
        let child = tokio::process::Command::new(program)
            .args(args)
            .envs(command.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own group, so `destroy` takes the worker rn8 started too.
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Error(format!("cannot run {program}: {e}")))?;
        *handle.group.lock().expect("group") = child.id().map(|p| p as i32);
        Ok(ChildProcess::new(child))
    }

    /// SIGTERM to the group, which rn8 forwards to the worker; then, if
    /// anything is left after a grace period, SIGKILL.
    async fn destroy(&self, handle: Handle) -> Result<(), Error> {
        let Some(group) = handle.group.lock().expect("group").take() else {
            return Ok(());
        };
        // ESRCH — the group is already gone — is a destroyed sandbox.
        if !signal_group(group, libc::SIGTERM) {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + DESTROY_GRACE;
        while tokio::time::Instant::now() < deadline {
            if !signal_group(group, 0) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        signal_group(group, libc::SIGKILL);
        Ok(())
    }
}

/// How long `destroy` waits after SIGTERM.
const DESTROY_GRACE: Duration = Duration::from_secs(2);

/// Signal a process group; false if it no longer exists.
fn signal_group(group: i32, sig: i32) -> bool {
    // SAFETY: kill(2) with a negative pid signals a group; signal 0 only
    // checks that it exists. Neither touches our memory.
    unsafe { libc::kill(-group, sig) == 0 }
}
