//! `resonate:delay` on the Postgres engine. Needs `TEST_POSTGRES_URL`, and
//! empties the database it names.

use resonate_core::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_server_postgres::PostgresEngine;
use resonate_sql::engine::{Engine, Input, Outgoing};
use serde_json::{json, Value};

const T0: i64 = 1_700_000_000_000;
const W: &str = "poll://any@w";

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

async fn create(e: &dyn Engine, id: &str, tags: Value, now: i64) -> (i32, Vec<Outgoing>) {
    let r = req(
        "promise.create",
        json!({ "id": id, "timeoutAt": now + 60_000, "param": {}, "tags": tags }),
    );
    let out = e.process(Input::External(&r), now).await;
    (out.response.expect("a response").head.status, out.messages)
}

fn executes(messages: &[Outgoing]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|m| match m {
            Outgoing::Execute { task_id, .. } => Some(task_id.clone()),
            Outgoing::Unblock { .. } => None,
        })
        .collect()
}

/// `resonate:delay` holds a new task's first dispatch back to the delay
/// (the specification's `createPromise`: the retry deadline is
/// `max(delay, now)`), and a delay already past dispatches at once.
async fn delay_holds_back_the_first_dispatch(e: &dyn Engine) {
    let due = T0 + 5_000;
    let (status, messages) = create(
        e,
        "delay:a",
        json!({ "resonate:target": W, "resonate:delay": due.to_string() }),
        T0,
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        executes(&messages).is_empty(),
        "dispatched at create: {messages:?}"
    );

    let (_, messages, _) = e.tick(due - 1).await.expect("tick");
    assert!(
        executes(&messages).is_empty(),
        "dispatched before the delay"
    );
    let (_, messages, _) = e.tick(due).await.expect("tick");
    assert_eq!(executes(&messages), vec!["delay:a".to_string()]);

    let (status, messages) = create(
        e,
        "delay:b",
        json!({ "resonate:target": W, "resonate:delay": (due - 1).to_string() }),
        due,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        executes(&messages),
        vec!["delay:b".to_string()],
        "a past delay dispatches at once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_honours_resonate_delay() {
    let Ok(url) = std::env::var("TEST_POSTGRES_URL") else {
        eprintln!("TEST_POSTGRES_URL not set: skipped");
        return;
    };
    let e = PostgresEngine::connect(&url, 2, 30_000, 10, true)
        .await
        .expect("connect");
    e.init(true).await.expect("schema");
    let reset = req("debug.reset", json!({}));
    e.process(Input::External(&reset), T0).await;
    delay_holds_back_the_first_dispatch(&e).await;
}
