//! rn8: the relay inside a Resonate sandbox.
//!
//! The image's entrypoint, followed by the worker command:
//!
//! ```dockerfile
//! ENTRYPOINT ["rn8", "--", "node", "worker.js"]
//! ```
//!
//! The guest has no network. Everything the worker says to Resonate goes
//! through rn8, which turns it into frames on its own stdout, and everything
//! Resonate says back arrives as frames on its stdin. The SDK cannot tell: it
//! is pointed at a loopback port, it speaks HTTP, and it gets HTTP back.
//!
//! - **Start**: read the task frame, listen on a loopback port, start the
//!   worker with `RESONATE_URL` set to that port and `RESONATE_PUSH=1`.
//! - **Push**: wait until the worker accepts connections, then POST it the task.
//! - **Relay**: each SDK request becomes a `req` frame, answered by the `res`
//!   with the same id.
//! - **Logs**: the worker's stdout and stderr become `log` frames.
//! - **End**: exit when the push returns. On stdin EOF, kill the worker and exit.
//! - **PID 1**: reap zombies, forward SIGTERM, kill the worker's process group
//!   on the way out.
//!
//! Exit status: 0 the step ended normally, 1 the worker crashed or the push
//! failed, 2 a framing or version error.

mod args;
mod listen;
mod relay;
mod worker;

use std::process::ExitCode;
use std::time::Duration;

use resonate_sandbox::exit;
use resonate_sandbox::frame::{FrameReader, ToGuest, VERSION};
use tokio::io::BufReader;
use tokio::sync::mpsc;

/// How long the worker's last lines get to reach stdout after it is killed.
const LOG_DRAIN: Duration = Duration::from_secs(1);

fn main() -> ExitCode {
    let mut args = match args::Args::parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            diag(&format!("{e}\n\n{}", args::USAGE));
            return ExitCode::from(exit::FRAMING as u8);
        }
    };

    if let Err(e) = args.resolve_worker_port() {
        diag(&format!("cannot pick a worker port: {e}"));
        return ExitCode::from(exit::WORKER as u8);
    }

    // One thread. rn8 relays bytes; it is never the bottleneck, and a single
    // thread keeps the signal handling and the reaping simple to reason about.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            diag(&format!("cannot start the runtime: {e}"));
            return ExitCode::from(exit::WORKER as u8);
        }
    };
    let code = rt.block_on(run(args));
    // Exit here rather than return: tokio reads stdin on a blocking thread,
    // and dropping the runtime would wait for that read — that is, for the
    // plugin to close stdin, which it does only once rn8 has exited. Every
    // frame is already flushed; the worker's group is already killed.
    std::process::exit(code)
}

/// What ended the step.
enum End {
    /// The push returned this HTTP status.
    Pushed(u16),
    /// The push could not be made.
    PushFailed(String),
    /// The worker exited before the push returned.
    WorkerExited(i32),
    /// The plugin closed stdin.
    StdinClosed,
    /// The plugin sent something that is not a frame, or not this one.
    Framing(String),
}

async fn run(args: args::Args) -> i32 {
    // Before anything is spawned, so no orphan of the worker's escapes to a
    // reaper that is not us when rn8 is not PID 1.
    worker::become_subreaper();

    // Frames on stdio, or — for a provider with no stdin — over HTTP.
    let listen::Ends {
        input,
        output,
        delivered,
    } = match args.listen.clone() {
        None => listen::Ends::stdio(),
        Some((port, token)) => match listen::start(port, token).await {
            Ok(ends) => ends,
            Err(e) => {
                diag(&format!("cannot listen on port {port}: {e}"));
                return exit::WORKER;
            }
        },
    };

    // ── Start: the task frame, first and exactly once. ──────────────────────
    let mut frames = FrameReader::new(BufReader::new(input));
    let mut task = match frames.next::<ToGuest>().await {
        Ok(Some(ToGuest::Task { v, task })) if v == VERSION => task,
        Ok(Some(ToGuest::Task { v, .. })) => {
            diag(&format!(
                "frame version {v} is not supported; this rn8 speaks {VERSION}"
            ));
            return exit::FRAMING;
        }
        Ok(Some(other)) => {
            diag(&format!("the first frame must be the task, got {other:?}"));
            return exit::FRAMING;
        }
        Ok(None) => {
            diag("stdin closed before the task frame");
            return exit::FRAMING;
        }
        Err(e) => {
            diag(&e.to_string());
            return exit::FRAMING;
        }
    };

    // Frames out: one writer owns stdout, so frames never interleave.
    let (out, writer) = relay::Out::start(output);

    // ── The relay port. ─────────────────────────────────────────────────────
    let relay = match relay::Relay::bind(out.clone()).await {
        Ok(r) => r,
        Err(e) => {
            diag(&format!("cannot listen on loopback: {e}"));
            return exit::WORKER;
        }
    };
    let relay_url = format!("http://{}", relay.addr());

    // An SDK that follows the message's own `serverUrl` rather than
    // `RESONATE_URL` must still reach the relay: the server's URL is not
    // reachable from here, by design.
    if let Some(head) = task.get_mut("head").and_then(|h| h.as_object_mut()) {
        if head.contains_key("serverUrl") {
            head.insert("serverUrl".into(), relay_url.clone().into());
        }
    }

    // ── Frames in: responses, and the end of stdin. ─────────────────────────
    let (end_tx, mut end_rx) = mpsc::unbounded_channel::<End>();
    {
        let pending = relay.pending();
        let end_tx = end_tx.clone();
        tokio::spawn(async move {
            loop {
                match frames.next::<ToGuest>().await {
                    Ok(Some(ToGuest::Res { id, status, body })) => {
                        if !pending.answer(id, status, body) {
                            diag(&format!("res {id} answers no outstanding req; ignored"));
                        }
                    }
                    Ok(Some(ToGuest::Task { .. })) => {
                        let _ = end_tx.send(End::Framing("a second task frame".into()));
                        return;
                    }
                    Ok(None) => {
                        let _ = end_tx.send(End::StdinClosed);
                        return;
                    }
                    Err(e) => {
                        let _ = end_tx.send(End::Framing(e.to_string()));
                        return;
                    }
                }
            }
        });
    }
    let relay_task = tokio::spawn(relay.serve());

    // ── The worker. ─────────────────────────────────────────────────────────
    let mut env = vec![
        ("RESONATE_URL".to_string(), relay_url.clone()),
        ("PORT".to_string(), args.worker_port.to_string()),
        // The SDK's cue that work is pushed, not polled for: no poller, no
        // connection held open, `handle()` takes the one task.
        ("RESONATE_PUSH".to_string(), "1".to_string()),
    ];
    env.extend(args.env.iter().cloned());
    let child = match worker::Worker::spawn(&args.command, &env, out.clone()) {
        Ok(c) => c,
        Err(e) => {
            diag(&format!("cannot start the worker {:?}: {e}", args.command));
            relay_task.abort();
            out.flush(LOG_DRAIN).await;
            drop(writer);
            return exit::WORKER;
        }
    };
    {
        let end_tx = end_tx.clone();
        let exited = child.exited();
        tokio::spawn(async move {
            if let Ok(code) = exited.await {
                let _ = end_tx.send(End::WorkerExited(code));
            }
        });
    }

    // ── Push. ───────────────────────────────────────────────────────────────
    {
        let end_tx = end_tx.clone();
        let target = args.push_target();
        let ready_timeout = args.ready_timeout;
        let body = serde_json::to_vec(&task).expect("a JSON value always serializes");
        tokio::spawn(async move {
            let end = match relay::push(&target, body, ready_timeout).await {
                Ok(status) => End::Pushed(status),
                Err(e) => End::PushFailed(e),
            };
            let _ = end_tx.send(end);
        });
    }
    drop(end_tx);

    // ── Wait for the end, forwarding SIGTERM meanwhile. ─────────────────────
    let mut term = worker::signals();
    let end = loop {
        tokio::select! {
            end = end_rx.recv() => break end.unwrap_or(End::StdinClosed),
            Some(sig) = term.recv() => child.signal(sig),
        }
    };

    let code = match end {
        End::Pushed(status) if (200..300).contains(&status) => exit::OK,
        End::Pushed(status) => {
            diag(&format!("the worker answered the push with {status}"));
            exit::WORKER
        }
        End::PushFailed(e) => {
            diag(&format!("push failed: {e}"));
            exit::WORKER
        }
        End::WorkerExited(code) => {
            diag(&format!(
                "the worker exited with {code} before the step ended"
            ));
            exit::WORKER
        }
        End::StdinClosed => {
            diag("stdin closed; stopping the worker");
            exit::WORKER
        }
        End::Framing(e) => {
            diag(&e);
            exit::FRAMING
        }
    };

    // ── End: the worker's whole group goes, then its last lines, then us. ───
    child.kill_group();
    relay_task.abort();
    child.drain_logs(LOG_DRAIN).await;
    out.close(LOG_DRAIN).await;
    drop(writer);
    // Over HTTP, the last frames are the plugin's once the response has
    // ended on the wire, not when they were handed to it.
    if let Some(delivered) = delivered {
        let _ = tokio::time::timeout(LOG_DRAIN, delivered).await;
    }
    code
}

/// A diagnostic, on stderr. Never on stdout: stdout is frames.
pub(crate) fn diag(msg: &str) {
    eprintln!("rn8: {msg}");
}

/// Hand a frame to the writer. Used by the relay and the log readers.
pub(crate) type FrameTx = mpsc::UnboundedSender<relay::OutMsg>;
