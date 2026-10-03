//! The plugin end to end: a real server, a `sandbox://` target, rn8, a worker.
//!
//! The backend is `local` — no VM — so this runs anywhere with `python3`; what
//! it exercises is everything the backend does not: dispatch, the task frame,
//! the push, every request relayed, scoped and answered, and the promise
//! settled by the worker from inside. The worker is rn8's own fixture, a
//! stand-in for an SDK in push mode.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use resonate_base::{build, Options, Registry};
use resonate_plugin::types::{RequestEnvelope, RequestHead, PROTOCOL_VERSION};
use resonate_plugin::{Loader, ResonateServer};
use serde_json::{json, Value};

const IMAGE: &str =
    "example.com/worker@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// rn8, built from this workspace.
fn rn8() -> PathBuf {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = std::process::Command::new(cargo)
        .args(["build", "-q", "-p", "resonate-sandbox-rn8", "--bin", "rn8"])
        .status()
        .expect("cargo runs");
    assert!(status.success(), "rn8 builds");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| root.join("target"));
    target.join("debug/rn8")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn call(server: &Arc<dyn ResonateServer>, kind: &str, data: Value) -> (i32, Value) {
    let resp = server
        .process(&RequestEnvelope {
            kind: kind.into(),
            head: RequestHead {
                corr_id: "e2e".into(),
                version: PROTOCOL_VERSION.into(),
                auth: None,
                debug_time: None,
            },
            data,
        })
        .await
        .expect("the server answers");
    (resp.head.status, resp.data)
}

/// A server carrying the sandbox plugin on the local backend, its command rn8
/// in front of the fixture worker.
async fn start(env: Value) -> resonate_base::Running {
    // RUST_LOG=debug to watch the session.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let worker = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../resonate-sandbox-rn8/tests/fixtures/worker.py");
    let command = json!([
        rn8(),
        "--worker-port",
        free_port().to_string(),
        "--",
        "python3",
        worker
    ]);
    let env = env
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| format!("{k} = {v}"))
        .collect::<Vec<_>>()
        .join(", ");

    let registry = Registry::new()
        .server(&resonate_server_sqlite::PLUGIN)
        .worker(&resonate_worker_sandbox::PLUGIN);
    let config = Loader::new()
        .set("servers.server_sqlite.path", "\":memory:\"")
        .unwrap()
        .set("servers.server_sqlite.migrate", "true")
        .unwrap()
        .set("workers.worker_sandbox.enabled", "true")
        .unwrap()
        .set("workers.worker_sandbox.backend", "\"local\"")
        .unwrap()
        .set("workers.worker_sandbox.egress", "\"all\"")
        .unwrap()
        .set("workers.worker_sandbox.command", &command.to_string())
        .unwrap()
        .set("workers.worker_sandbox.env", &format!("{{ {env} }}"))
        .unwrap()
        .load();
    let running = build(&registry, &config, &Options::default()).expect("builds");
    running.start(false).await.expect("starts");
    running
}

async fn dispatch(server: &Arc<dyn ResonateServer>, id: &str) {
    let (status, _) = call(
        server,
        "promise.create",
        json!({
            "id": id,
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": { "resonate:target": format!("sandbox://{IMAGE}") }
        }),
    )
    .await;
    assert!(status == 200 || status == 201, "promise.create: {status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_runs_in_a_sandbox_and_settles_its_own_promise() {
    if !python() {
        eprintln!("skipped: no python3");
        return;
    }
    let running = start(json!({})).await;
    let server = Arc::clone(running.server());
    dispatch(&server, "e2e").await;

    let deadline = Instant::now() + Duration::from_secs(30);
    let promise = loop {
        let (_, data) = call(&server, "promise.get", json!({ "id": "e2e" })).await;
        if data["promise"]["state"] != "pending" {
            break data["promise"].clone();
        }
        assert!(
            Instant::now() < deadline,
            "the sandbox did not settle the promise: {data}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(promise["state"], "resolved", "{promise}");

    let report: Value = serde_json::from_slice(
        &base64::engine::general_purpose::STANDARD
            .decode(promise["value"]["data"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    // Its own task, acquired through the relay.
    assert_eq!(report["acquire"], 200, "{report}");
    // Someone else's task, and a schedule: refused by the plugin.
    assert_eq!(report["other_task"], 403, "{report}");
    assert_eq!(report["schedule"], 403, "{report}");
    // The message's serverUrl pointed back at the relay, not the server.
    assert_eq!(report["server_url_rewritten"], true, "{report}");

    running.stop(Duration::from_secs(10)).await;
}

/// A guest that acquires and then goes quiet is destroyed when its lease runs
/// out — worker and all — rather than holding a sandbox forever.
#[tokio::test(flavor = "multi_thread")]
async fn a_sandbox_is_destroyed_when_its_lease_expires() {
    if !python() {
        eprintln!("skipped: no python3");
        return;
    }
    let pidfile = std::env::temp_dir().join(format!("rn8-hang-{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);
    let running = start(json!({
        "WORKER_MODE": "hang",
        "WORKER_PIDFILE": pidfile.to_string_lossy(),
    }))
    .await;
    let server = Arc::clone(running.server());
    dispatch(&server, "hang").await;

    let deadline = Instant::now() + Duration::from_secs(20);
    let pid = loop {
        if let Ok(pid) = std::fs::read_to_string(&pidfile) {
            if !pid.is_empty() {
                break pid;
            }
        }
        assert!(Instant::now() < deadline, "the worker never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // The lease is 500ms. Well after that, the worker must be gone.
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::path::Path::new(&format!("/proc/{pid}")).exists() && !zombie(&pid) {
        assert!(Instant::now() < deadline, "worker {pid} outlived its lease");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (_, data) = call(&server, "promise.get", json!({ "id": "hang" })).await;
    assert_eq!(data["promise"]["state"], "pending", "nothing settled it");

    let _ = std::fs::remove_file(&pidfile);
    running.stop(Duration::from_secs(10)).await;
}

fn zombie(pid: &str) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map(|s| s.split_whitespace().nth(2) == Some("Z"))
        .unwrap_or(false)
}
