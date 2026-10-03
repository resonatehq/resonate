//! One task, one sandbox: create, exec, relay, destroy.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use resonate_plugin::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_sandbox::frame::{self, FrameReader, FromGuest, LogStream, ToGuest};
use resonate_sandbox::{exit, Backend, Process};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::{sleep_until, timeout_at, Instant};

use crate::scope::Claim;
use crate::Shared;

/// Run the task in a fresh sandbox, and destroy it whatever happens.
pub(crate) async fn run<B: Backend>(
    shared: Arc<Shared<B>>,
    image: String,
    message: Value,
    claim: Claim,
) {
    let task_id = claim.id.clone();
    // Creating the sandbox and getting the guest to acquire its task share
    // one clock: until the guest holds a lease, this is the only deadline.
    let start_deadline = Instant::now() + shared.start_timeout;

    let handle = match timeout_at(start_deadline, shared.backend.create(&image)).await {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => {
            tracing::warn!(task_id, image, error = %e, "sandbox: create failed");
            return;
        }
        Err(_) => {
            // Abandoned mid-create; whatever the backend half-made is an
            // orphan, which is out of scope for this version.
            tracing::warn!(
                task_id,
                image,
                "sandbox: create did not finish before the start timeout"
            );
            return;
        }
    };
    tracing::debug!(task_id, image, "sandbox: created");

    match drive(&shared, &handle, message, claim, start_deadline).await {
        Ok(End::Exited(code)) => match code {
            exit::OK => tracing::debug!(task_id, "sandbox: step ended"),
            exit::WORKER => {
                tracing::warn!(task_id, "sandbox: the worker crashed or the push failed")
            }
            exit::FRAMING => {
                tracing::error!(task_id, "sandbox: rn8 reported a framing or version error")
            }
            other => tracing::warn!(
                task_id,
                code = other,
                "sandbox: exited with an unexpected status"
            ),
        },
        Ok(End::LeaseExpired) => tracing::warn!(task_id, "sandbox: lease expired, destroying"),
        Ok(End::NoExit) => tracing::warn!(task_id, "sandbox: closed its output but did not exit"),
        Err(e) => tracing::warn!(task_id, error = %e, "sandbox: session failed"),
    }

    if let Err(e) = shared.backend.destroy(handle).await {
        tracing::warn!(task_id, error = %e, "sandbox: destroy failed");
    }
}

enum End {
    /// rn8 exited with this status.
    Exited(i32),
    /// The deadline passed: no acquire in time, the lease lapsed, or the
    /// guest outstayed its step.
    LeaseExpired,
    /// Output closed, and no exit status within the grace period.
    NoExit,
}

/// What a forwarded response means for the deadline.
enum Lease {
    /// Acquired or renewed for this long.
    Held(Duration),
    /// The step is over: fulfilled, or suspended.
    Ended,
}

async fn drive<B: Backend>(
    shared: &Arc<Shared<B>>,
    handle: &B::Handle,
    message: Value,
    claim: Claim,
    start_deadline: Instant,
) -> Result<End, String> {
    let task_id = claim.id.clone();
    let mut process = timeout_at(
        start_deadline,
        shared.backend.exec(handle, shared.command.clone()),
    )
    .await
    .map_err(|_| "exec did not start before the start timeout".to_string())?
    .map_err(|e| format!("exec failed: {e}"))?;
    let mut stdin = process.stdin().ok_or("the process has no stdin")?;
    let stdout = process.stdout().ok_or("the process has no stdout")?;
    let stderr = process.stderr().ok_or("the process has no stderr")?;

    // rn8's own diagnostics.
    {
        let task_id = task_id.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(task_id, "{line}");
            }
        });
    }

    // The task frame first, then responses as they come. One writer owns
    // stdin, so frames never interleave.
    frame::write(
        &mut stdin,
        &ToGuest::Task {
            v: frame::VERSION,
            task: message,
        },
    )
    .await
    .map_err(|e| format!("cannot write the task frame: {e}"))?;
    let (res_tx, mut res_rx) = mpsc::unbounded_channel::<ToGuest>();
    tokio::spawn(async move {
        while let Some(f) = res_rx.recv().await {
            if frame::write(&mut stdin, &f).await.is_err() {
                return;
            }
        }
        // Every sender is gone: the session is over, and rn8 sees EOF.
    });

    let claim = Arc::new(Mutex::new(claim));
    let (lease_tx, mut lease_rx) = mpsc::unbounded_channel::<Lease>();
    let mut deadline = start_deadline;
    let mut frames = FrameReader::new(BufReader::new(stdout));

    let ended = loop {
        tokio::select! {
            f = frames.next::<FromGuest>() => match f {
                Ok(Some(FromGuest::Req { id, body })) => {
                    let shared = Arc::clone(shared);
                    let claim = Arc::clone(&claim);
                    let res_tx = res_tx.clone();
                    let lease_tx = lease_tx.clone();
                    // Concurrently: the SDK may have several requests in
                    // flight, and answers go back in whatever order they finish.
                    tokio::spawn(async move {
                        let (status, body, lease) = forward(&shared, &claim, body).await;
                        if let Some(l) = lease {
                            let _ = lease_tx.send(l);
                        }
                        let _ = res_tx.send(ToGuest::Res { id, status, body });
                    });
                }
                Ok(Some(FromGuest::Log { stream, data })) => match stream {
                    LogStream::Stdout => tracing::info!(task_id, stream = "stdout", "{data}"),
                    LogStream::Stderr => tracing::info!(task_id, stream = "stderr", "{data}"),
                },
                Ok(None) => break None,
                Err(e) => {
                    tracing::warn!(task_id, error = %e, "sandbox: unreadable output");
                    break None;
                }
            },
            Some(l) = lease_rx.recv() => {
                deadline = match l {
                    Lease::Held(ttl) => Instant::now() + ttl,
                    Lease::Ended => Instant::now() + shared.exit_grace,
                };
            }
            _ = sleep_until(deadline) => break Some(End::LeaseExpired),
        }
    };
    drop(res_tx);
    if let Some(end) = ended {
        return Ok(end);
    }

    // Output is closed; the exit status follows, or the grace runs out.
    match tokio::time::timeout(shared.exit_grace, process.wait()).await {
        Ok(Ok(code)) => Ok(End::Exited(code)),
        Ok(Err(e)) => Err(format!("wait failed: {e}")),
        Err(_) => Ok(End::NoExit),
    }
}

/// Forward one guest request: scope it, authenticate it, process it.
///
/// Returns the HTTP status and body rn8 answers the SDK with — the same pair
/// the HTTP gateway would have produced — and what the answer means for the
/// lease.
async fn forward<B>(
    shared: &Shared<B>,
    claim: &Mutex<Claim>,
    body: Value,
) -> (u16, Value, Option<Lease>) {
    let respond = |r: ResponseEnvelope| {
        let status = u16::try_from(r.head.status).unwrap_or(500);
        let body = serde_json::to_value(&r).expect("a response always serializes");
        (status, body)
    };

    // Validated exactly as the gateway validates a remote worker's request:
    // the same parse, the same rejection.
    let bytes = serde_json::to_vec(&body).expect("a JSON value always serializes");
    let mut req: RequestEnvelope = match types::parse_and_validate(&bytes) {
        Ok(r) => r,
        Err(invalid) => {
            let (kind, corr_id) = types::salvage_context(&bytes);
            let (s, b) = respond(invalid.to_response(kind, corr_id));
            return (s, b, None);
        }
    };

    let refusal = claim
        .lock()
        .expect("claim")
        .check(&req.kind, &req.data)
        .err();
    if let Some(why) = refusal {
        tracing::warn!(kind = %req.kind, reason = %why, "sandbox: request outside the task's scope refused");
        let (s, b) = respond(ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            403,
            &format!("outside this sandbox's task: {why}"),
        ));
        return (s, b, None);
    }

    // The guest holds no credentials. Whatever it sent is dropped, and the
    // plugin's own goes on.
    req.head.auth = shared.token.clone();

    let Some(server) = shared.server.upgrade() else {
        let (s, b) = respond(ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            503,
            "the server is stopping",
        ));
        return (s, b, None);
    };
    let resp = match server.process(&req).await {
        Ok(r) => r,
        Err(e) => ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            503,
            &e.to_string(),
        ),
    };

    let ok = resp.head.status == 200;
    if ok {
        claim.lock().expect("claim").created(&req.kind, &req.data);
    }
    let lease = match req.kind.as_str() {
        "task.acquire" | "task.heartbeat" if ok => lease_ttl(&req.kind, &req.data, claim)
            .map(|ttl| Lease::Held(Duration::from_millis(ttl))),
        // 200 is the end of the step. A suspend answered 300 is not: an
        // awaited promise had already settled and the guest carries on.
        "task.fulfill" | "task.suspend" if ok => Some(Lease::Ended),
        _ => None,
    };
    let (s, b) = respond(resp);
    (s, b, lease)
}

/// The lease a successful acquire or heartbeat holds.
///
/// An acquire names its ttl; a heartbeat renews for the same, so the ttl from
/// the acquire is remembered on the claim.
fn lease_ttl(kind: &str, data: &Value, claim: &Mutex<Claim>) -> Option<u64> {
    let mut claim = claim.lock().expect("claim");
    if kind == "task.acquire" {
        let ttl = data.get("ttl").and_then(Value::as_u64)?;
        claim.ttl = Some(ttl);
    }
    claim.ttl
}
