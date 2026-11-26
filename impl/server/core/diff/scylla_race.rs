// Concurrent task transitions on ScyllaDB.
//
// The differentials send one request at a time, so a conditional write that
// loses to a concurrent one never happens there. Here the same request goes
// to two ScyllaDB engines (two servers on one keyspace) at the same instant,
// several times over. Identical requests have one serial outcome up to order:
// the first answer, then the answer to a repeat. The oracle gives both, and
// the concurrent responses must be exactly that multiset — in particular, a
// loser gets the 409 a serial order gives, not a retryable 500.
//
// Run: TEST_SCYLLA_HOSTS=127.0.0.1 cargo test --release --test scylla_race

use std::sync::Arc;

use resonate_core::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_oracle::SharedOracle;
use resonate_server_scylladb::{Config, ScyllaEngine};
use resonate_sql::engine::{Engine, Input};
use serde_json::{json, Value};

const T0: i64 = 1_000_000_000;
const RETRY: i64 = 30_000;
const COPIES: usize = 6;
const ROUNDS: usize = 20;
const WORKER_URL: &str = "http://race-test-worker:9999";

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: fastrand::u64(..).to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

fn create(id: &str) -> RequestEnvelope {
    req(
        "task.create",
        json!({
            "pid": "race-pid", "ttl": 60_000,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": id, "timeoutAt": T0 + 600_000, "param": {},
                "tags": { "resonate:target": WORKER_URL }
            }}
        }),
    )
}

fn create_promise(id: &str) -> RequestEnvelope {
    req(
        "promise.create",
        json!({ "id": id, "timeoutAt": T0 + 600_000, "param": {}, "tags": {} }),
    )
}

/// The status of every response, sorted: what is compared is the multiset.
fn statuses(rs: Vec<i32>) -> Vec<i32> {
    let mut rs = rs;
    rs.sort();
    rs
}

async fn scylla() -> Option<[Arc<ScyllaEngine>; 2]> {
    let hosts = std::env::var("TEST_SCYLLA_HOSTS").ok()?;
    let cfg = Config {
        hosts: hosts.split(',').map(String::from).collect(),
        keyspace: "resonate_race".into(),
        migrate: true,
        ..Default::default()
    };
    let a = ScyllaEngine::connect(&cfg, RETRY, true)
        .await
        .expect("connect");
    let b = ScyllaEngine::connect(&cfg, RETRY, true)
        .await
        .expect("connect");
    Some([Arc::new(a), Arc::new(b)])
}

/// `setup` on both models, then `op` COPIES times: serially on the oracle,
/// concurrently across the two engines.
async fn race(s: &[Arc<ScyllaEngine>; 2], setup: &[RequestEnvelope], op: &RequestEnvelope) {
    let reset = req("debug.reset", json!({}));
    let oracle = SharedOracle::new();
    oracle.process(Input::External(&reset), T0).await;
    assert_eq!(
        s[0].dispatch(&reset, T0)
            .await
            .response
            .unwrap()
            .head
            .status,
        200
    );
    for r in setup {
        let want = oracle
            .process(Input::External(r), T0)
            .await
            .response
            .unwrap();
        let got = s[0].dispatch(r, T0).await.response.unwrap();
        assert_eq!(
            got.head.status, want.head.status,
            "setup {}: {}",
            r.kind, got.data
        );
    }
    let mut serial = Vec::new();
    for _ in 0..COPIES {
        serial.push(
            oracle
                .process(Input::External(op), T0)
                .await
                .response
                .unwrap()
                .head
                .status,
        );
    }
    let handles: Vec<_> = (0..COPIES)
        .map(|i| {
            let e = Arc::clone(&s[i % 2]);
            let op = op.clone();
            tokio::spawn(async move { e.dispatch(&op, T0).await.response.unwrap() })
        })
        .collect();
    let mut concurrent = Vec::new();
    let mut bodies = Vec::new();
    for h in handles {
        let r = h.await.unwrap();
        concurrent.push(r.head.status);
        bodies.push(r.data);
    }
    assert_eq!(
        statuses(concurrent),
        statuses(serial),
        "{} x{COPIES} concurrently gave other answers than any serial order: {bodies:?}",
        op.kind
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_task_transitions_answer_as_serial() {
    let Some(s) = scylla().await else {
        eprintln!("[race] TEST_SCYLLA_HOSTS not set — skipped");
        return;
    };
    for round in 0..ROUNDS {
        let t = format!("race:t{round}");
        let p = format!("race:p{round}");
        // The creator holds the task at version 1, acquired.
        race(&s, &[], &create(&t)).await;
        let suspend = req(
            "task.suspend",
            json!({ "id": t, "version": 1, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": p, "awaiter": t }
            }]}),
        );
        race(&s, &[create(&t), create_promise(&p)], &suspend).await;
        race(&s, &[create(&t)], &req("task.halt", json!({ "id": t }))).await;
        race(
            &s,
            &[create(&t)],
            &req("task.release", json!({ "id": t, "version": 1 })),
        )
        .await;
    }
}
