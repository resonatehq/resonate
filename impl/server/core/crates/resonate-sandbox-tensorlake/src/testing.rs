//! A fake Tensorlake: enough of the control plane and the sandbox proxy for
//! the backend to run against, in process.
//!
//! A "sandbox" is a record; a "process" in it is a host process. Output is
//! kept line by line and served as Tensorlake serves it — replayed, then
//! followed, as server-sent events. It holds the backend to the wire as this
//! crate understands it, not to Tensorlake itself.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::watch;

pub const KEY: &str = "test-key";

/// What the fake has seen, for a test to assert on.
#[derive(Default)]
pub struct Seen {
    /// Every create request body.
    pub creates: Vec<Value>,
    /// Ids of sandboxes deleted, in order.
    pub deletes: Vec<String>,
    /// Sandboxes that exist.
    pub live: Vec<String>,
}

pub struct FakeTensorlake {
    pub url: String,
    state: Arc<Fake>,
}

impl FakeTensorlake {
    /// Serve on a loopback port. Every create answers with this fake's own
    /// URL as the sandbox's, so one listener is both API and proxy.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let state = Arc::new(Fake {
            url: url.clone(),
            seen: Mutex::new(Seen::default()),
            procs: Mutex::new(HashMap::new()),
            next: Mutex::new(100),
        });
        let app = Router::new()
            .route("/sandboxes", post(create))
            .route("/sandboxes/:id", get(info).delete(delete))
            .route("/api/v1/processes", post(start))
            .route("/api/v1/processes/:pid", get(process))
            .route("/api/v1/processes/:pid/stdin", post(stdin))
            .route("/api/v1/processes/:pid/stdin/close", post(stdin_close))
            .route("/api/v1/processes/:pid/:stream/follow", get(follow))
            .with_state(Arc::clone(&state));
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self { url, state }
    }

    pub fn seen<T>(&self, f: impl FnOnce(&Seen) -> T) -> T {
        f(&self.state.seen.lock().unwrap())
    }
}

struct Fake {
    url: String,
    seen: Mutex<Seen>,
    procs: Mutex<HashMap<i64, Arc<Proc>>>,
    next: Mutex<i64>,
}

struct Proc {
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    /// stdout and stderr, line by line, and whether each has ended.
    out: Mutex<[(Vec<String>, bool); 2]>,
    exit: Mutex<Option<i32>>,
    /// Bumped on every change, for followers to wake on.
    changed: watch::Sender<u64>,
}

impl Proc {
    fn bump(&self) {
        self.changed.send_modify(|v| *v += 1);
    }
}

fn authorized(headers: &HeaderMap) -> bool {
    headers.get("authorization").and_then(|v| v.to_str().ok()) == Some(&format!("Bearer {KEY}"))
}

macro_rules! auth {
    ($headers:expr) => {
        if !authorized(&$headers) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };
}

async fn create(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    auth!(headers);
    let id = format!("sb-{:08x}", fastrand::u32(..));
    let mut seen = f.seen.lock().unwrap();
    seen.creates.push(body);
    seen.live.push(id.clone());
    // "pending" first, so the backend has to poll for "running".
    Json(json!({ "sandbox_id": id, "status": "pending" })).into_response()
}

async fn info(State(f): State<Arc<Fake>>, headers: HeaderMap, Path(id): Path<String>) -> Response {
    auth!(headers);
    if !f.seen.lock().unwrap().live.contains(&id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    Json(json!({ "sandbox_id": id, "status": "running", "sandbox_url": f.url })).into_response()
}

async fn delete(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    auth!(headers);
    let mut seen = f.seen.lock().unwrap();
    seen.deletes.push(id.clone());
    let Some(i) = seen.live.iter().position(|s| *s == id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    seen.live.remove(i);
    StatusCode::OK.into_response()
}

async fn start(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    auth!(headers);
    let command = body["command"].as_str().unwrap_or_default().to_string();
    let args: Vec<String> = body["args"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let env: Vec<(String, String)> = body["env"]
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .unwrap_or_default();
    let piped = body["stdin_mode"] == "pipe";
    let mut child = match tokio::process::Command::new(&command)
        .args(&args)
        .envs(env)
        .stdin(if piped { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let pid = {
        let mut next = f.next.lock().unwrap();
        *next += 1;
        *next
    };
    let proc = Arc::new(Proc {
        stdin: tokio::sync::Mutex::new(child.stdin.take()),
        out: Mutex::new([(Vec::new(), false), (Vec::new(), false)]),
        exit: Mutex::new(None),
        changed: watch::channel(0).0,
    });
    let readers = [
        tokio::spawn(collect(child.stdout.take().unwrap(), Arc::clone(&proc), 0)),
        tokio::spawn(collect(child.stderr.take().unwrap(), Arc::clone(&proc), 1)),
    ];
    {
        let proc = Arc::clone(&proc);
        tokio::spawn(async move {
            let status = child.wait().await;
            for r in readers {
                let _ = r.await;
            }
            *proc.exit.lock().unwrap() = Some(status.ok().and_then(|s| s.code()).unwrap_or(-1));
            proc.bump();
        });
    }
    f.procs.lock().unwrap().insert(pid, proc);
    Json(json!({ "pid": pid, "status": "running", "command": command, "args": args, "started_at": 0 }))
        .into_response()
}

async fn collect(stream: impl AsyncRead + Unpin, proc: Arc<Proc>, which: usize) {
    let mut lines = BufReader::new(stream).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        proc.out.lock().unwrap()[which].0.push(line);
        proc.bump();
    }
    proc.out.lock().unwrap()[which].1 = true;
    proc.bump();
}

fn find(f: &Fake, pid: i64) -> Option<Arc<Proc>> {
    f.procs.lock().unwrap().get(&pid).cloned()
}

async fn process(State(f): State<Arc<Fake>>, headers: HeaderMap, Path(pid): Path<i64>) -> Response {
    auth!(headers);
    let Some(p) = find(&f, pid) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let exit = *p.exit.lock().unwrap();
    Json(json!({
        "pid": pid,
        "status": if exit.is_some() { "exited" } else { "running" },
        "exit_code": exit,
        "command": "",
        "started_at": 0,
    }))
    .into_response()
}

async fn stdin(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Path(pid): Path<i64>,
    body: Bytes,
) -> Response {
    auth!(headers);
    let Some(p) = find(&f, pid) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut stdin = p.stdin.lock().await;
    let Some(s) = stdin.as_mut() else {
        return StatusCode::CONFLICT.into_response();
    };
    if s.write_all(&body).await.is_ok() && s.flush().await.is_ok() {
        StatusCode::OK.into_response()
    } else {
        StatusCode::CONFLICT.into_response()
    }
}

async fn stdin_close(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Path(pid): Path<i64>,
) -> Response {
    auth!(headers);
    let Some(p) = find(&f, pid) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    p.stdin.lock().await.take();
    StatusCode::OK.into_response()
}

async fn follow(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    Path((pid, stream)): Path<(i64, String)>,
) -> Response {
    auth!(headers);
    let which = match stream.as_str() {
        "stdout" => 0,
        "stderr" => 1,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let Some(p) = find(&f, pid) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let rx = p.changed.subscribe();
    let events =
        futures_util::stream::unfold((p, rx, 0usize, false), move |(p, mut rx, i, hello)| {
            let stream = stream.clone();
            async move {
                // A heartbeat first, which the backend must read past.
                if !hello {
                    let ping = Bytes::from_static(b": ping\n\ndata: {\"heartbeat\":true}\n\n");
                    return Some((Ok::<_, Infallible>(ping), (p, rx, i, true)));
                }
                loop {
                    let (line, ended) = {
                        let out = p.out.lock().unwrap();
                        (out[which].0.get(i).cloned(), out[which].1)
                    };
                    if let Some(line) = line {
                        let event = json!({ "line": line, "timestamp": 0, "stream": stream });
                        let bytes = Bytes::from(format!("data: {event}\n\n"));
                        return Some((Ok(bytes), (p, rx, i + 1, true)));
                    }
                    if ended {
                        return None;
                    }
                    if rx.changed().await.is_err() {
                        return None;
                    }
                }
            }
        });
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(events))
        .unwrap()
}
