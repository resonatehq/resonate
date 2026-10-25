//! Ids and addresses longer than their `VARCHAR(255)` column are refused
//! with a 400, never stored cut short. Needs `TEST_MYSQL_URL` (MySQL or
//! TiDB), and empties the database it names.
//!
//! The engine writes with `INSERT IGNORE`, which turns strict mode's
//! "Data too long" into a warning and keeps the first 255 characters; before
//! the length checks a 300-character listener address got its `unblock` at
//! the first 255 characters of it.

use resonate_core::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_server_mysql::MysqlEngine;
use resonate_sql::engine::{Engine, Input, Output};
use serde_json::{json, Value};

const T0: i64 = 1_700_000_000_000;
const HOUR: i64 = 3_600_000;

/// Both tests reset one database.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: "t".to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

async fn send(e: &MysqlEngine, kind: &str, data: Value, now: i64) -> (i32, Value, Output) {
    let mut out = e.process(Input::External(&req(kind, data)), now).await;
    let resp = out.response.take().expect("a response");
    (resp.head.status, resp.data, out)
}

/// The addresses the output's messages went to.
fn addresses(out: &Output) -> Vec<String> {
    out.messages
        .iter()
        .map(|m| m.address().to_string())
        .collect()
}

/// A `poll://` address of exactly `n` characters.
fn address(n: usize) -> String {
    let head = "poll://any@g";
    format!("{head}{}", "a".repeat(n - head.len()))
}

async fn engine() -> Option<MysqlEngine> {
    let Ok(url) = std::env::var("TEST_MYSQL_URL") else {
        eprintln!("TEST_MYSQL_URL not set: skipped");
        return None;
    };
    let e = MysqlEngine::connect(&url, 2, 30_000, 10, true)
        .await
        .expect("connect");
    e.init(true).await.expect("schema");
    e.process(Input::External(&req("debug.reset", json!({}))), T0)
        .await;
    Some(e)
}

#[tokio::test(flavor = "multi_thread")]
async fn values_longer_than_their_column_are_refused_not_truncated() {
    let _turn = TURN.lock().await;
    let Some(e) = engine().await else { return };
    let far = T0 + HOUR;

    // A target of 255 characters is kept whole, at create and at redispatch.
    let t255 = address(255);
    let (status, _, out) = send(
        &e,
        "promise.create",
        json!({ "id": "o:t255", "timeoutAt": far, "tags": { "resonate:target": t255 } }),
        T0,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(addresses(&out), vec![t255.clone()]);
    let (_, messages, _) = e.tick(T0 + 30_000).await.expect("tick");
    let redispatched: Vec<&str> = messages.iter().map(|m| m.address()).collect();
    assert_eq!(
        redispatched,
        vec![t255.as_str()],
        "redispatch uses the whole target"
    );

    // One more character is refused, and nothing is created.
    let (status, data, out) = send(
        &e,
        "promise.create",
        json!({ "id": "o:t256", "timeoutAt": far, "tags": { "resonate:target": address(256) } }),
        T0,
    )
    .await;
    assert_eq!(status, 400, "{data}");
    assert!(out.messages.is_empty());
    let (status, _, _) = send(&e, "promise.get", json!({ "id": "o:t256" }), T0).await;
    assert_eq!(status, 404, "nothing was created");

    // A listener address: 255 is delivered whole, 256 is refused.
    let (status, _, _) = send(
        &e,
        "promise.create",
        json!({ "id": "o:g", "timeoutAt": far, "tags": { "resonate:scope": "global" } }),
        T0,
    )
    .await;
    assert_eq!(status, 200);
    let (status, data, _) = send(
        &e,
        "promise.register_listener",
        json!({ "awaited": "o:g", "address": address(256) }),
        T0,
    )
    .await;
    assert_eq!(status, 400, "{data}");
    let (status, _, _) = send(
        &e,
        "promise.register_listener",
        json!({ "awaited": "o:g", "address": t255 }),
        T0,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _, out) = send(
        &e,
        "promise.settle",
        json!({ "id": "o:g", "state": "resolved", "value": {} }),
        T0,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        addresses(&out),
        vec![t255.clone()],
        "unblock goes to the whole address"
    );

    // task.create and the create in task.fence check the target too.
    let task_create = |id: &str, target: String| {
        json!({ "pid": "p", "ttl": 60_000, "action": { "kind": "promise.create", "head": {},
                "data": { "id": id, "timeoutAt": far, "tags": { "resonate:target": target } } } })
    };
    let (status, data, _) = send(&e, "task.create", task_create("o:tc", address(256)), T0).await;
    assert_eq!(status, 400, "{data}");
    let (status, _, _) = send(&e, "task.create", task_create("o:tc", address(20)), T0).await;
    assert_eq!(status, 200);
    let fence = json!({ "id": "o:tc", "version": 1, "action": { "kind": "promise.create", "head": {},
        "data": { "id": "o:child", "timeoutAt": far, "tags": { "resonate:target": address(256) } } } });
    let (status, data, _) = send(&e, "task.fence", fence, T0).await;
    assert_eq!(status, 400, "{data}");

    // Characters, not bytes: 255 two-byte characters fit, 256 do not.
    let wide = "é".repeat(255);
    let (status, data, _) = send(
        &e,
        "promise.create",
        json!({ "id": wide, "timeoutAt": far }),
        T0,
    )
    .await;
    assert_eq!(status, 200, "{data}");
    assert_eq!(data["promise"]["id"], json!(wide), "kept whole");
    let (status, _, _) = send(
        &e,
        "promise.create",
        json!({ "id": "é".repeat(256), "timeoutAt": far }),
        T0,
    )
    .await;
    assert_eq!(status, 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn schedule_ids_templates_and_targets_must_fit() {
    let _turn = TURN.lock().await;
    let Some(e) = engine().await else { return };
    let schedule = |id: &str, template: &str, target: String| {
        json!({ "id": id, "cron": "* * * * *", "promiseId": template, "promiseTimeout": 60_000,
                "promiseTags": { "resonate:target": target } })
    };
    for (what, data) in [
        (
            "id",
            schedule(&"s".repeat(256), "p.{{.timestamp}}", address(20)),
        ),
        ("template", schedule("s", &"p".repeat(256), address(20))),
        // 240 + 19 digits of timestamp > 255, though the template is short.
        (
            "expansion",
            schedule(
                "s",
                &format!("{}.{{{{.timestamp}}}}", "p".repeat(240)),
                address(20),
            ),
        ),
        ("target", schedule("s", "p.{{.timestamp}}", address(256))),
    ] {
        let (status, body, _) = send(&e, "schedule.create", data, T0).await;
        assert_eq!(status, 400, "{what}: {body}");
    }
    let (status, body, _) = send(
        &e,
        "schedule.create",
        schedule(
            "s",
            &format!("{}.{{{{.timestamp}}}}", "p".repeat(200)),
            address(255),
        ),
        T0,
    )
    .await;
    assert_eq!(status, 200, "{body}");
}
