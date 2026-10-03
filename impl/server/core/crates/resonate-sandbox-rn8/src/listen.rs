//! Frames over HTTP, for a provider that gives a guest no stdin.
//!
//! Some providers start an instance with arguments and an environment and
//! nothing else: no exec, no stdin, only a port reachable through their HTTP
//! edge. There the plugin reaches rn8 instead, on `--listen <port>`:
//!
//! - `GET /frames` — rn8's frames, as one streamed NDJSON response. One
//!   reader, ever: the first to claim it.
//! - `POST /frames` — frames to rn8: any bytes of the NDJSON stream, in order.
//! - `POST /frames/close` — the end of that stream, as stdin's EOF would be.
//!
//! Every request carries `Authorization: Bearer <RN8_TOKEN>`. The port is
//! public; the token, set by the plugin when it created the instance, is what
//! makes the one connection the plugin's.
//!
//! Everything past this point is the same as on stdio: the two ends become a
//! reader and a writer, and `run` does not know the difference.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use hyper::body::{Frame, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::diag;

/// How much of either stream may wait for the other end.
const PIPE: usize = 1 << 20;

/// A blank line — which a frame reader skips — when nothing else has been
/// sent for this long, so that no proxy between here and the plugin decides
/// the stream is idle and closes it.
const KEEPALIVE: Duration = Duration::from_secs(15);

type Body = BoxBody<Bytes, Infallible>;

struct State {
    token: String,
    /// The plugin's frames, in. `None` once closed.
    input: tokio::sync::Mutex<Option<DuplexStream>>,
    /// rn8's frames, out. Taken by the one `GET /frames`.
    output: Mutex<Option<DuplexStream>>,
}

/// The two ends `run` reads frames from and writes them to, and what tells
/// it the plugin has had the last of them.
pub struct Ends {
    pub input: Box<dyn AsyncRead + Send + Unpin>,
    pub output: Box<dyn AsyncWrite + Send + Unpin>,
    /// Over HTTP, fires when the connection that carried `GET /frames` is
    /// closed — after the end of the response is on the wire, once `output`
    /// is shut down. On stdio, written is delivered.
    pub delivered: Option<oneshot::Receiver<()>>,
}

impl Ends {
    pub fn stdio() -> Self {
        Self {
            input: Box::new(tokio::io::stdin()),
            output: Box::new(tokio::io::stdout()),
            delivered: None,
        }
    }
}

/// Listen on `port`.
pub async fn start(port: u16, token: String) -> std::io::Result<Ends> {
    let listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
    let (input_w, input_r) = tokio::io::duplex(PIPE);
    let (output_w, output_r) = tokio::io::duplex(PIPE);
    let state = Arc::new(State {
        token,
        input: tokio::sync::Mutex::new(Some(input_w)),
        output: Mutex::new(Some(output_r)),
    });
    let (delivered_tx, delivered) = oneshot::channel();
    let delivered_tx = Arc::new(Mutex::new(Some(delivered_tx)));
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let state = Arc::clone(&state);
            let delivered_tx = Arc::clone(&delivered_tx);
            tokio::spawn(async move {
                // Whether this connection is the one carrying the frames out.
                let carried = Arc::new(AtomicBool::new(false));
                let flag = Arc::clone(&carried);
                let service = hyper::service::service_fn(move |req| {
                    let state = Arc::clone(&state);
                    let flag = Arc::clone(&flag);
                    async move { Ok::<_, Infallible>(handle(&state, &flag, req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
                if carried.load(Ordering::SeqCst) {
                    if let Some(tx) = delivered_tx.lock().expect("delivered").take() {
                        let _ = tx.send(());
                    }
                }
            });
        }
    });
    Ok(Ends {
        input: Box::new(input_r),
        output: Box::new(output_w),
        delivered: Some(delivered),
    })
}

async fn handle(state: &State, carried: &AtomicBool, req: Request<Incoming>) -> Response<Body> {
    if !authorized(&state.token, &req) {
        return status(StatusCode::UNAUTHORIZED);
    }
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/frames") => {
            let Some(output) = state.output.lock().expect("output").take() else {
                return status(StatusCode::CONFLICT);
            };
            carried.store(true, Ordering::SeqCst);
            Response::builder()
                // Closed when the frames end, which is how rn8 knows they
                // were delivered.
                .header("connection", "close")
                .header("content-type", "application/x-ndjson")
                // Proxies that buffer by default leave a stream alone with this.
                .header("cache-control", "no-cache, no-transform")
                .header("x-accel-buffering", "no")
                .body(stream(output))
                .expect("a valid response")
        }
        (&Method::POST, "/frames") => {
            let mut input = state.input.lock().await;
            let Some(w) = input.as_mut() else {
                return status(StatusCode::GONE);
            };
            let mut body = req.into_body();
            while let Some(frame) = body.frame().await {
                let Ok(frame) = frame else {
                    return status(StatusCode::BAD_REQUEST);
                };
                if let Ok(data) = frame.into_data() {
                    if w.write_all(&data).await.is_err() {
                        return status(StatusCode::GONE);
                    }
                }
            }
            status(StatusCode::NO_CONTENT)
        }
        (&Method::POST, "/frames/close") => {
            if let Some(mut w) = state.input.lock().await.take() {
                let _ = w.shutdown().await;
            }
            status(StatusCode::NO_CONTENT)
        }
        _ => status(StatusCode::NOT_FOUND),
    }
}

/// rn8's frames as a response body, with a keepalive when they pause.
fn stream(mut output: DuplexStream) -> Body {
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let chunk = match tokio::time::timeout(KEEPALIVE, output.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) => return,
                Ok(Ok(n)) => Bytes::copy_from_slice(&buf[..n]),
                Err(_) => Bytes::from_static(b"\n"),
            };
            if tx.send(chunk).await.is_err() {
                // The plugin hung up: what is written from here is not read.
                diag("the plugin's frame stream closed");
                return;
            }
        }
    });
    Channel(rx).boxed()
}

/// A response body fed from a channel; it ends when the sender does.
struct Channel(mpsc::Receiver<Bytes>);

impl hyper::body::Body for Channel {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.poll_recv(cx).map(|b| b.map(|b| Ok(Frame::data(b))))
    }
}

fn authorized(token: &str, req: &Request<Incoming>) -> bool {
    let Some(given) = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    // Constant time in the token's length: the port is public.
    given.len() == token.len()
        && given
            .bytes()
            .zip(token.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn status(code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(code)
        .body(Empty::new().boxed())
        .expect("a valid response")
}
