//! Against Unikraft Cloud. Skipped unless `UKC_TOKEN` and `UKC_LIVE_IMAGE`
//! are set; the image is rn8 as the entrypoint in front of rn8's own fixture
//! worker (`resonate-sandbox-rn8/tests/fixtures/worker.py`):
//!
//! ```sh
//! UKC_TOKEN=… UKC_LIVE_IMAGE=<user>/rn8-smoke:latest \
//!   cargo test -p resonate-sandbox-unikraft --test live -- --nocapture
//! ```
//!
//! The test plays the plugin: the task frame in, each `req` answered, rn8's
//! exit status from the instance, and the instance gone afterwards.

use std::time::{Duration, Instant};

use resonate_sandbox::{Backend, Command, Egress, Limits, Process};
use resonate_sandbox_unikraft::{Options, Unikraft};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn live() -> Option<(String, String)> {
    let token = std::env::var("UKC_TOKEN").ok().filter(|s| !s.is_empty())?;
    let image = std::env::var("UKC_LIVE_IMAGE")
        .ok()
        .filter(|s| !s.is_empty())?;
    Some((token, image))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_step_relays_through_the_edge_and_the_instance_goes() {
    let Some((token, image)) = live() else {
        eprintln!("skipped: UKC_TOKEN and UKC_LIVE_IMAGE not set");
        return;
    };
    let b = Unikraft::new(
        Options {
            token: Some(token),
            api_url: std::env::var("UKC_API_URL")
                .unwrap_or_else(|_| "https://api.fra.unikraft.cloud".into()),
            ..Options::default()
        },
        Limits {
            cpus: Some(1),
            memory_mib: Some(256),
            egress: Egress::All,
        },
    );
    let started = Instant::now();
    let h = b.create(&image).await.expect("create");
    let mut p = b
        .exec(
            &h,
            Command {
                argv: vec![],
                env: vec![("WORKER_MODE".into(), "ok".into())],
            },
        )
        .await
        .expect("exec");
    let mut stdin = p.stdin().unwrap();
    let mut out = BufReader::new(p.stdout().unwrap()).lines();
    let mut err = BufReader::new(p.stderr().unwrap()).lines();
    let console = tokio::spawn(async move {
        let mut lines = Vec::new();
        while let Ok(Some(l)) = err.next_line().await {
            eprintln!("console: {l}");
            lines.push(l);
        }
        lines
    });

    let task = json!({
        "type": "task", "v": 1,
        "task": { "kind": "execute", "head": { "serverUrl": "http://unreachable.invalid" },
                  "data": { "task": { "id": "t1", "version": 1 } } }
    });
    stdin
        .write_all(format!("{task}\n").as_bytes())
        .await
        .unwrap();
    stdin.flush().await.unwrap();

    let mut kinds = Vec::new();
    let mut logs = Vec::new();
    let mut first_req = None;
    while let Some(line) = out.next_line().await.expect("frames") {
        if line.trim().is_empty() {
            continue;
        }
        let frame: Value = serde_json::from_str(&line).expect("a frame");
        match frame["type"].as_str().unwrap() {
            "req" => {
                first_req.get_or_insert_with(|| started.elapsed());
                let body = &frame["body"];
                let kind = body["kind"].as_str().unwrap().to_string();
                let status = if body["data"]["id"] == "t1" { 200 } else { 403 };
                let res = json!({
                    "type": "res", "id": frame["id"], "status": status,
                    "body": { "kind": kind, "head": { "corrId": body["head"]["corrId"], "status": status, "version": "2026-04-01" }, "data": {} }
                });
                stdin
                    .write_all(format!("{res}\n").as_bytes())
                    .await
                    .unwrap();
                stdin.flush().await.unwrap();
                kinds.push(kind);
            }
            "log" => logs.push(frame["data"].as_str().unwrap().to_string()),
            other => panic!("unexpected frame {other}"),
        }
    }
    let code = tokio::time::timeout(Duration::from_secs(60), p.wait())
        .await
        .expect("the instance stops")
        .unwrap();
    eprintln!(
        "first request after {:?}, step over after {:?}",
        first_req.unwrap_or_default(),
        started.elapsed()
    );
    drop(stdin);
    drop(p);
    let console = tokio::time::timeout(Duration::from_secs(30), console)
        .await
        .expect("the console log ends once the instance has stopped")
        .unwrap_or_default();
    b.destroy(h).await.expect("destroy");
    assert!(
        console.iter().any(|l| l.contains("exit code: 0")),
        "{console:?}"
    );

    assert_eq!(code, 0, "console: {console:?}");
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
