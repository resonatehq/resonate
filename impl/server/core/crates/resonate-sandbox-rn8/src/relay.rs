//! The relay: loopback HTTP on one side, frames on the other.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use resonate_sandbox::frame::{self, FromGuest, MAX_FRAME};
use serde_json::{json, Value};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::{diag, FrameTx};

// ─── Frames out ──────────────────────────────────────────────────────────────

pub enum OutMsg {
    Frame(FromGuest),
    /// Everything queued before this has been written.
    Flush(oneshot::Sender<()>),
    /// Write what is queued, then end the stream.
    Close(oneshot::Sender<()>),
}

/// The one writer of frames: stdout, or the plugin's connection.
#[derive(Clone)]
pub struct Out {
    tx: FrameTx,
}

impl Out {
    pub fn start(
        mut stdout: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::unbounded_channel::<OutMsg>();
        let writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    OutMsg::Frame(f) => {
                        if stdout.write_all(&frame::encode(&f)).await.is_err() {
                            // The plugin is gone; nothing written now is read.
                            return;
                        }
                        // Flush only when nothing else is waiting: a burst of
                        // log lines becomes one write, a lone `req` is never
                        // left in a buffer.
                        if rx.is_empty() && stdout.flush().await.is_err() {
                            return;
                        }
                    }
                    OutMsg::Flush(done) => {
                        let _ = stdout.flush().await;
                        let _ = done.send(());
                    }
                    OutMsg::Close(done) => {
                        let _ = stdout.shutdown().await;
                        let _ = done.send(());
                        return;
                    }
                }
            }
        });
        (Self { tx }, writer)
    }

    pub fn send(&self, f: FromGuest) {
        let _ = self.tx.send(OutMsg::Frame(f));
    }

    /// Write everything sent so far and end the stream, waiting at most
    /// `limit`. Nothing sent after this is written.
    pub async fn close(&self, limit: Duration) {
        let (done, wait) = oneshot::channel();
        if self.tx.send(OutMsg::Close(done)).is_ok() {
            let _ = tokio::time::timeout(limit, wait).await;
        }
    }

    /// Wait, at most `limit`, for everything sent so far to be written.
    pub async fn flush(&self, limit: Duration) {
        let (done, wait) = oneshot::channel();
        if self.tx.send(OutMsg::Flush(done)).is_ok() {
            let _ = tokio::time::timeout(limit, wait).await;
        }
    }
}

// ─── Requests in flight ──────────────────────────────────────────────────────

type Answer = (u16, Value);

/// `req` frames waiting for their `res`, by id.
#[derive(Clone, Default)]
pub struct Pending {
    next: Arc<AtomicU64>,
    waiting: Arc<Mutex<HashMap<u64, oneshot::Sender<Answer>>>>,
}

impl Pending {
    fn open(&self) -> (u64, oneshot::Receiver<Answer>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();
        self.waiting.lock().expect("pending").insert(id, tx);
        (id, rx)
    }

    fn forget(&self, id: u64) {
        self.waiting.lock().expect("pending").remove(&id);
    }

    /// Deliver a `res`. False when no `req` with that id is waiting.
    pub fn answer(&self, id: u64, status: u16, body: Value) -> bool {
        match self.waiting.lock().expect("pending").remove(&id) {
            Some(tx) => {
                // The SDK may have hung up; its answer has nowhere to go.
                let _ = tx.send((status, body));
                true
            }
            None => false,
        }
    }
}

// ─── The loopback server ─────────────────────────────────────────────────────

pub struct Relay {
    listener: TcpListener,
    out: Out,
    pending: Pending,
}

impl Relay {
    pub async fn bind(out: Out) -> std::io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        Ok(Self {
            listener,
            out,
            pending: Pending::default(),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("a bound listener has an address")
    }

    pub fn pending(&self) -> Pending {
        self.pending.clone()
    }

    pub async fn serve(self) {
        loop {
            let stream = match self.listener.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    // Out of descriptors, most likely: back off rather than spin.
                    diag(&format!("accept: {e}"));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let out = self.out.clone();
            let pending = self.pending.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    let out = out.clone();
                    let pending = pending.clone();
                    async move { Ok::<_, std::convert::Infallible>(handle(req, out, pending).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    }
}

/// One SDK request: a `req` out, wait for its `res`, answer with it.
///
/// Any method, any path. The protocol is one endpoint taking a JSON envelope,
/// and which path an SDK posts it to is not the relay's business.
async fn handle(req: Request<Incoming>, out: Out, pending: Pending) -> Response<Full<Bytes>> {
    let body = match Limited::new(req.into_body(), MAX_FRAME).collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => return local_error(StatusCode::PAYLOAD_TOO_LARGE, &format!("body: {e}")),
    };
    let body: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return local_error(StatusCode::BAD_REQUEST, &format!("not JSON: {e}")),
    };

    let (id, answer) = pending.open();
    out.send(FromGuest::Req { id, body });
    // Dropped only if rn8 is on its way out; the plugin will not answer now.
    let Ok((status, body)) = answer.await else {
        pending.forget(id);
        return local_error(StatusCode::BAD_GATEWAY, "the relay closed");
    };
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    json_response(status, &body)
}

/// An answer rn8 gives itself, because the request never became a frame.
fn local_error(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    diag(msg);
    json_response(status, &json!({ "error": msg }))
}

fn json_response(status: StatusCode, body: &Value) -> Response<Full<Bytes>> {
    let bytes = serde_json::to_vec(body).expect("a JSON value always serializes");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(bytes)))
        .expect("a static status and header always build")
}

// ─── Push ────────────────────────────────────────────────────────────────────

pub struct PushTarget {
    pub port: u16,
    pub path: String,
}

/// Wait for the worker to accept connections, then POST it the task.
///
/// Returns the push's HTTP status once the worker answers — which is when the
/// step has ended: the worker answers a push when the function completes or
/// suspends.
pub async fn push(
    target: &PushTarget,
    body: Vec<u8>,
    ready_timeout: Duration,
) -> Result<u16, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], target.port));
    let stream = tokio::time::timeout(ready_timeout, async {
        loop {
            match TcpStream::connect(addr).await {
                Ok(s) => return s,
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .map_err(|_| {
        format!("the worker did not accept connections on {addr} within {ready_timeout:?}")
    })?;

    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| format!("handshake with {addr}: {e}"))?;
    tokio::spawn(conn);

    let req = Request::post(&target.path)
        .header("host", format!("127.0.0.1:{}", target.port))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| format!("push request: {e}"))?;
    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| format!("push to {addr}{}: {e}", target.path))?;
    let status = resp.status().as_u16();
    // Read the body to the end so the worker sees its answer delivered.
    let _ = resp.into_body().collect().await;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_are_matched_by_id_in_any_order() {
        let p = Pending::default();
        let (a, mut ra) = p.open();
        let (b, mut rb) = p.open();
        assert_ne!(a, b);
        assert!(p.answer(b, 404, json!("b")));
        assert!(p.answer(a, 200, json!("a")));
        assert_eq!(ra.try_recv().unwrap(), (200, json!("a")));
        assert_eq!(rb.try_recv().unwrap(), (404, json!("b")));
        assert!(!p.answer(a, 200, json!(null)), "answered twice");
    }
}
