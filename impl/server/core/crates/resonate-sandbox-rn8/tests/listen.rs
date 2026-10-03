//! rn8 with `--listen`: the same relay, with frames over HTTP instead of stdio.
//!
//! The test plays the plugin over HTTP. Skipped where there is no `python3`.

use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};

const TOKEN: &str = "a-token-of-sufficient-length";

fn python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn line(frame: Value) -> String {
    format!("{frame}\n")
}

#[tokio::test(flavor = "multi_thread")]
async fn relays_a_step_over_http_and_exits_zero() {
    if !python() {
        eprintln!("skipped: no python3");
        return;
    }
    let port = free_port();
    let worker = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/worker.py");
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_rn8"))
        .args(["--listen", &port.to_string()])
        .args(["--worker-port", &free_port().to_string()])
        .args(["--env", "WORKER_MODE=ok"])
        .args(["--", "python3", worker])
        .env("RN8_TOKEN", TOKEN)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("rn8 runs");
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();

    // Up when it answers; a wrong token is refused.
    let mut up = false;
    for _ in 0..100 {
        if let Ok(r) = http
            .get(format!("{base}/frames"))
            .bearer_auth("wrong-token-wrong-token")
            .send()
            .await
        {
            assert_eq!(r.status(), 401);
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(up, "rn8 never listened");

    let mut frames = http
        .get(format!("{base}/frames"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(frames.status(), 200);
    // One reader, ever.
    let second = http
        .get(format!("{base}/frames"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 409);

    let post = |body: String| {
        let http = http.clone();
        let base = base.clone();
        async move {
            let r = http
                .post(format!("{base}/frames"))
                .bearer_auth(TOKEN)
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(r.status(), 204);
        }
    };
    post(line(json!({
        "type": "task", "v": 1,
        "task": { "kind": "execute", "head": { "serverUrl": "http://unreachable.invalid" },
                  "data": { "task": { "id": "t1", "version": 1 } } }
    })))
    .await;

    let mut kinds = Vec::new();
    let mut logs = Vec::new();
    let mut buf = Vec::new();
    while let Some(chunk) = frames.chunk().await.unwrap() {
        buf.extend_from_slice(&chunk);
        while let Some(i) = buf.iter().position(|b| *b == b'\n') {
            let l: Vec<u8> = buf.drain(..=i).collect();
            if l.trim_ascii().is_empty() {
                continue;
            }
            let frame: Value = serde_json::from_slice(&l).expect("frames only");
            match frame["type"].as_str().unwrap() {
                "req" => {
                    let body = &frame["body"];
                    let kind = body["kind"].as_str().unwrap().to_string();
                    let status = if body["data"]["id"] == "t1" { 200 } else { 403 };
                    post(line(json!({
                        "type": "res", "id": frame["id"], "status": status,
                        "body": { "kind": kind, "head": { "corrId": body["head"]["corrId"], "status": status, "version": "2026-04-01" }, "data": {} }
                    })))
                    .await;
                    kinds.push(kind);
                }
                "log" => logs.push(frame["data"].as_str().unwrap().to_string()),
                other => panic!("unexpected frame type {other}"),
            }
        }
    }
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("rn8 exits")
        .unwrap();
    assert_eq!(status.code(), Some(0));
    assert_eq!(
        kinds,
        [
            "task.acquire",
            "task.acquire",
            "schedule.create",
            "task.fulfill"
        ]
    );
    assert!(
        logs.contains(&"worker: executing t1".to_string()),
        "{logs:?}"
    );
}

#[tokio::test]
async fn listening_without_a_token_is_refused() {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_rn8"))
        .args(["--listen", &free_port().to_string(), "--", "true"])
        .env_remove("RN8_TOKEN")
        .output()
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("RN8_TOKEN"));
}
