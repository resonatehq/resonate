//! The worker process: started in its own group, reaped by hand, killed whole.
//!
//! rn8 is usually PID 1 in the guest, which makes it the reaper of every
//! orphan in there. So it reaps with `waitpid(-1)` on every SIGCHLD rather
//! than waiting on its one child: anything the worker double-forks is reaped
//! too, and the worker's own status is picked out of the same loop. Nothing
//! else in rn8 waits on a process, so nothing else can steal a status.
//!
//! When rn8 is not PID 1 it asks to be a child subreaper (Linux), so the same
//! holds: orphans of the worker come to rn8, not to an init that is not there.

use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use resonate_sandbox::frame::{FromGuest, LogStream};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::diag;
use crate::relay::Out;

pub struct Worker {
    /// Also its process group id: it leads a group of its own.
    pid: libc::pid_t,
    exited: Mutex<Option<oneshot::Receiver<i32>>>,
    logs: Mutex<Vec<JoinHandle<()>>>,
}

impl Worker {
    pub fn spawn(argv: &[String], env: &[(String, String)], out: Out) -> std::io::Result<Self> {
        // Registered before the spawn, so a worker that exits at once still
        // raises a SIGCHLD someone is listening for.
        let mut sigchld = signal(SignalKind::child())?;

        let mut cmd = std::process::Command::new(&argv[0]);
        cmd.args(&argv[1..])
            .envs(env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own group, so the whole tree can be signalled and killed at
            // once, and a signal meant for rn8 is not also delivered to it.
            .process_group(0);
        // And if rn8 is SIGKILLed — which it cannot clean up after — the
        // worker goes with it rather than outliving the relay it talks through.
        #[cfg(target_os = "linux")]
        // SAFETY: runs in the child between fork and exec, and only calls
        // prctl(2), which is async-signal-safe.
        unsafe {
            cmd.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn()?;
        let pid = child.id() as libc::pid_t;

        let stdout = tokio::process::ChildStdout::from_std(child.stdout.take().expect("piped"))?;
        let stderr = tokio::process::ChildStderr::from_std(child.stderr.take().expect("piped"))?;
        let logs = vec![
            tokio::spawn(forward(stdout, LogStream::Stdout, out.clone())),
            tokio::spawn(forward(stderr, LogStream::Stderr, out)),
        ];
        // Never waited on through `child`: the reaper below owns every status.
        drop(child);

        let (exit_tx, exit_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut exit_tx = Some(exit_tx);
            loop {
                for (reaped, code) in reap_all() {
                    if reaped == pid {
                        if let Some(tx) = exit_tx.take() {
                            let _ = tx.send(code);
                        }
                    }
                }
                if sigchld.recv().await.is_none() {
                    return;
                }
            }
        });

        Ok(Self {
            pid,
            exited: Mutex::new(Some(exit_rx)),
            logs: Mutex::new(logs),
        })
    }

    /// Resolves with the worker's exit code: its status, or 128 + the signal
    /// that killed it. Once; later calls get a receiver that never resolves.
    pub fn exited(&self) -> oneshot::Receiver<i32> {
        self.exited
            .lock()
            .expect("exited")
            .take()
            .unwrap_or_else(|| oneshot::channel().1)
    }

    /// Forward a signal to the worker's group.
    pub fn signal(&self, sig: i32) {
        // SAFETY: kill(2) with a negative pid signals a process group; it
        // touches no memory of ours.
        unsafe {
            libc::kill(-self.pid, sig);
        }
    }

    /// Kill the worker and everything in its group.
    pub fn kill_group(&self) {
        self.signal(libc::SIGKILL);
    }

    /// Wait, at most `limit`, for the worker's last lines to become frames.
    ///
    /// After `kill_group` the pipes close as the group dies, so this is
    /// normally immediate. A grandchild that left the group and kept a pipe
    /// open is what the limit is for.
    pub async fn drain_logs(&self, limit: Duration) {
        let logs = std::mem::take(&mut *self.logs.lock().expect("logs"));
        let _ = tokio::time::timeout(limit, async {
            for h in logs {
                let _ = h.await;
            }
        })
        .await;
    }
}

/// Every child that has exited, reaped.
fn reap_all() -> Vec<(libc::pid_t, i32)> {
    let mut reaped = Vec::new();
    loop {
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid writes the status into a local we own.
        let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if pid <= 0 {
            return reaped;
        }
        let code = if libc::WIFEXITED(status) {
            libc::WEXITSTATUS(status)
        } else if libc::WIFSIGNALED(status) {
            128 + libc::WTERMSIG(status)
        } else {
            continue;
        };
        reaped.push((pid, code));
    }
}

/// One of the worker's streams, line by line, as `log` frames.
async fn forward(stream: impl AsyncRead + Unpin, which: LogStream, out: Out) {
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                if line.last() == Some(&b'\n') {
                    line.pop();
                }
                out.send(FromGuest::Log {
                    stream: which,
                    data: String::from_utf8_lossy(&line).into_owned(),
                });
            }
        }
    }
}

/// SIGTERM and SIGINT, as they arrive, for forwarding to the worker.
pub fn signals() -> mpsc::UnboundedReceiver<i32> {
    let (tx, rx) = mpsc::unbounded_channel();
    for (kind, num) in [
        (SignalKind::terminate(), libc::SIGTERM),
        (SignalKind::interrupt(), libc::SIGINT),
    ] {
        let tx = tx.clone();
        match signal(kind) {
            Ok(mut s) => {
                tokio::spawn(async move {
                    while s.recv().await.is_some() {
                        if tx.send(num).is_err() {
                            return;
                        }
                    }
                });
            }
            Err(e) => diag(&format!("cannot listen for signal {num}: {e}")),
        }
    }
    rx
}

/// Adopt the worker's orphans when rn8 is not PID 1.
pub fn become_subreaper() {
    #[cfg(target_os = "linux")]
    // SAFETY: prctl(PR_SET_CHILD_SUBREAPER) takes integers and sets a flag on
    // this process.
    unsafe {
        libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0);
    }
}
