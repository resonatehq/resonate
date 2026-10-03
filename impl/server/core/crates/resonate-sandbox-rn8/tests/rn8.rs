//! rn8 as a process: frames in on stdin, frames out on stdout, an exit status.
//!
//! The test plays the plugin. The worker is `fixtures/worker.py`, a stand-in
//! for an SDK in push mode. Skipped where there is no `python3`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

fn python() -> bool {
    Command::new("python3")
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

fn rn8(mode: &str) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    let worker = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/worker.py");
    let mut child = Command::new(env!("CARGO_BIN_EXE_rn8"))
        .args([
            "--worker-port",
            &free_port().to_string(),
            "--ready-timeout",
            "20000",
        ])
        .args(["--env", &format!("WORKER_MODE={mode}")])
        .args(["--", "python3", worker])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("rn8 runs");
    let stdin = child.stdin.take().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    (child, stdin, stdout)
}

fn send(stdin: &mut ChildStdin, frame: Value) {
    let mut line = serde_json::to_vec(&frame).unwrap();
    line.push(b'\n');
    stdin.write_all(&line).unwrap();
    stdin.flush().unwrap();
}

fn task_frame(v: u32) -> Value {
    json!({
        "type": "task",
        "v": v,
        "task": {
            "kind": "execute",
            "head": { "serverUrl": "http://unreachable.invalid" },
            "data": { "task": { "id": "t1", "version": 1 } }
        }
    })
}

fn wait(child: &mut Child, limit: Duration) -> i32 {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code().unwrap_or(-1);
        }
        if start.elapsed() > limit {
            let _ = child.kill();
            panic!("rn8 did not exit within {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The whole relay: task in, requests out and answered, logs out, exit 0.
#[test]
fn relays_a_step_and_exits_zero() {
    if !python() {
        eprintln!("skipped: no python3");
        return;
    }
    let (mut child, mut stdin, mut stdout) = rn8("ok");
    send(&mut stdin, task_frame(1));

    let mut kinds = Vec::new();
    let mut logs = Vec::new();
    let mut report = None;
    let mut line = String::new();
    while stdout.read_line(&mut line).unwrap() > 0 {
        let frame: Value = serde_json::from_str(&line).expect("stdout is frames only");
        line.clear();
        match frame["type"].as_str().unwrap() {
            "req" => {
                let body = &frame["body"];
                let kind = body["kind"].as_str().unwrap().to_string();
                if kind == "task.fulfill" {
                    let data = body["data"]["action"]["data"]["value"]["data"]
                        .as_str()
                        .unwrap();
                    report = Some(decode(data));
                }
                // Played by the test: the claimed task is fine, anything
                // else is refused, as the plugin would.
                let status = if body["data"]["id"] == "t1" { 200 } else { 403 };
                send(
                    &mut stdin,
                    json!({
                        "type": "res",
                        "id": frame["id"],
                        "status": status,
                        "body": { "kind": kind, "head": { "corrId": body["head"]["corrId"], "status": status, "version": "2026-04-01" }, "data": {} }
                    }),
                );
                kinds.push(kind);
            }
            "log" => logs.push((
                frame["stream"].as_str().unwrap().to_string(),
                frame["data"].as_str().unwrap().to_string(),
            )),
            other => panic!("unexpected frame type {other}"),
        }
    }
    assert_eq!(wait(&mut child, Duration::from_secs(10)), 0);

    assert_eq!(
        kinds,
        [
            "task.acquire",
            "task.acquire",
            "schedule.create",
            "task.fulfill"
        ]
    );
    let report = report.expect("the worker fulfilled");
    assert_eq!(report["acquire"], 200);
    assert_eq!(report["other_task"], 403);
    assert_eq!(report["server_url_rewritten"], true, "{report}");
    assert!(
        logs.contains(&("stdout".into(), "worker: executing t1".into())),
        "{logs:?}"
    );
    assert!(
        logs.contains(&("stderr".into(), "worker: a line on stderr".into())),
        "{logs:?}"
    );
}

#[test]
fn a_version_it_does_not_speak_exits_two() {
    let (mut child, mut stdin, _stdout) = rn8("ok");
    send(&mut stdin, task_frame(2));
    assert_eq!(wait(&mut child, Duration::from_secs(10)), 2);
}

#[test]
fn a_first_frame_that_is_not_the_task_exits_two() {
    let (mut child, mut stdin, _stdout) = rn8("ok");
    send(
        &mut stdin,
        json!({"type": "res", "id": 1, "status": 200, "body": {}}),
    );
    assert_eq!(wait(&mut child, Duration::from_secs(10)), 2);
}

#[test]
fn garbage_exits_two() {
    let (mut child, mut stdin, _stdout) = rn8("ok");
    stdin.write_all(b"this is not a frame\n").unwrap();
    assert_eq!(wait(&mut child, Duration::from_secs(10)), 2);
}

#[test]
fn a_worker_that_dies_on_the_push_exits_one() {
    if !python() {
        eprintln!("skipped: no python3");
        return;
    }
    let (mut child, mut stdin, _stdout) = rn8("crash");
    send(&mut stdin, task_frame(1));
    assert_eq!(wait(&mut child, Duration::from_secs(20)), 1);
}

#[test]
fn stdin_closing_stops_the_worker_and_rn8() {
    if !python() {
        eprintln!("skipped: no python3");
        return;
    }
    let (mut child, mut stdin, mut stdout) = rn8("ok");
    send(&mut stdin, task_frame(1));
    // Wait for the first request, so the worker is up, then hang up.
    let mut line = String::new();
    loop {
        line.clear();
        assert!(stdout.read_line(&mut line).unwrap() > 0, "rn8 ended early");
        if line.contains("\"type\":\"req\"") {
            break;
        }
    }
    drop(stdin);
    assert_eq!(wait(&mut child, Duration::from_secs(10)), 1);
}

fn decode(b64: &str) -> Value {
    // Minimal base64, to keep the test's dependencies at serde_json.
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut bits = 0u32;
    let mut n = 0;
    let mut out = Vec::new();
    for c in b64.bytes().filter(|c| *c != b'=') {
        bits = (bits << 6) | ALPHABET.iter().position(|a| *a == c).unwrap() as u32;
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8);
            bits &= (1 << n) - 1;
        }
    }
    serde_json::from_slice(&out).unwrap()
}
