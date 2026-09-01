//! The console's read model, exercised against every engine that is compiled
//! in.
//!
//! One body of assertions, run once per backend. That is the point: the `ui.*`
//! requests are answered by four independent implementations, and what makes
//! them one protocol rather than four is that the same sequence produces the
//! same JSON from each. Where a dialect has an opinion — how NULLs sort, what
//! a JSON containment operator is called — the answer must still be the same.
//!
//! SQLite and the oracle always run. Postgres and MySQL run when
//! `TEST_POSTGRES_URL` / `TEST_MYSQL_URL` name a database, exactly as the
//! differential does.

use std::collections::HashMap;

use resonate_core::types::{
    PromiseState, RequestEnvelope, RequestHead, ResponseEnvelope, SUPPORTED_VERSIONS,
};
use resonate_server_dbms::engine_port::{Input, ResonateEngine};
use resonate_server_dbms::oracle::SharedOracle;
use serde_json::{json, Value};

const T0: i64 = 1_700_000_000_000;
const WORKER: &str = "http://worker:9999";

fn req(kind: &str, data: Value) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: "c1".to_string(),
            version: SUPPORTED_VERSIONS[0].to_string(),
            auth: None,
            debug_time: None,
        },
        data,
    }
}

async fn call(engine: &dyn ResonateEngine, kind: &str, data: Value, now: i64) -> ResponseEnvelope {
    let r = req(kind, data);
    engine
        .process(Input::External(&r), now)
        .await
        .response
        .expect("an external request always has a response")
}

async fn ok(engine: &dyn ResonateEngine, kind: &str, data: Value, now: i64) -> Value {
    let resp = call(engine, kind, data, now).await;
    assert_eq!(resp.head.status, 200, "{kind} failed: {:?}", resp.data);
    resp.data
}

/// `{"func": …}`, base64 — the shape `resonate invoke` and every SDK writes,
/// and the only place a function name exists.
fn param(func: &str) -> Value {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    json!({ "data": STANDARD.encode(json!({ "func": func, "args": [] }).to_string()) })
}

/// Two executions, one of them a two-level tree with a task on a child.
///
/// Written through the ordinary worker protocol, never straight into the
/// tables: what the console reads has to be what a worker actually leaves
/// behind.
async fn seed(engine: &dyn ResonateEngine) {
    // A settled root, no children.
    ok(
        engine,
        "promise.create",
        json!({
            "id": "billing.invoice-1",
            "timeoutAt": T0 + 60_000,
            "param": param("sendInvoice"),
            "tags": { "resonate:scope": "global", "env": "prod" }
        }),
        T0 + 1_000,
    )
    .await;
    ok(
        engine,
        "promise.settle",
        json!({ "id": "billing.invoice-1", "state": "resolved", "value": { "data": "ok" } }),
        T0 + 5_000,
    )
    .await;

    // A pending root that dispatches: a target makes it a task.
    ok(
        engine,
        "promise.create",
        json!({
            "id": "checkout.order-8842",
            "timeoutAt": T0 + 3_600_000,
            "param": param("processCheckout"),
            "tags": { "resonate:target": WORKER, "env": "prod" }
        }),
        T0 + 2_000,
    )
    .await;
    // A child that runs inside its parent's task — no target, so no task.
    ok(
        engine,
        "promise.create",
        json!({
            "id": "checkout.order-8842:1",
            "timeoutAt": T0 + 3_600_000,
            "param": param("reserveStock"),
            "tags": { "resonate:parent": "checkout.order-8842" }
        }),
        T0 + 3_000,
    )
    .await;
    // A child that opens a task of its own.
    ok(
        engine,
        "promise.create",
        json!({
            "id": "checkout.order-8842:2",
            "timeoutAt": T0 + 3_600_000,
            "param": param("chargeCard"),
            "tags": { "resonate:parent": "checkout.order-8842", "resonate:target": WORKER }
        }),
        T0 + 4_000,
    )
    .await;

    ok(
        engine,
        "schedule.create",
        json!({
            "id": "sync-stripe",
            "cron": "0 0 * * * *",
            "promiseId": "sync-stripe.{{.timestamp}}",
            "promiseTimeout": 60_000,
            "promiseTags": { "resonate:target": WORKER }
        }),
        T0 + 6_000,
    )
    .await;
}

fn ids(items: &Value) -> Vec<String> {
    items
        .as_array()
        .expect("items is an array")
        .iter()
        .map(|i| i["id"].as_str().expect("an id").to_string())
        .collect()
}

/// Everything the console asks for, against one engine.
async fn console_reads(engine: &dyn ResonateEngine, backend: &str) {
    seed(engine).await;
    let now = T0 + 10_000;

    // --- the list ---------------------------------------------------------

    let page = ok(
        engine,
        "ui.executions.search",
        json!({ "countTotal": true }),
        now,
    )
    .await;
    assert_eq!(
        ids(&page["items"]),
        vec!["checkout.order-8842", "billing.invoice-1"],
        "{backend}: roots only, newest first"
    );
    assert_eq!(page["total"], 2, "{backend}");
    assert!(page["cursor"].is_null(), "{backend}: one page holds both");
    assert_eq!(
        page["items"][0]["func"], "processCheckout",
        "{backend}: the function comes out of the param"
    );
    assert_eq!(page["items"][0]["state"], "pending", "{backend}");
    assert_eq!(page["items"][1]["settledAt"], T0 + 5_000, "{backend}");
    assert_eq!(page["items"][0]["tags"]["env"], "prod", "{backend}");

    // A child is not an execution.
    assert!(
        !ids(&page["items"]).contains(&"checkout.order-8842:1".to_string()),
        "{backend}: children are not roots"
    );

    // --- pagination -------------------------------------------------------

    let first = ok(engine, "ui.executions.search", json!({ "limit": 1 }), now).await;
    assert_eq!(
        ids(&first["items"]),
        vec!["checkout.order-8842"],
        "{backend}"
    );
    let cursor = first["cursor"].as_str().expect("more to come").to_string();
    let second = ok(
        engine,
        "ui.executions.search",
        json!({ "limit": 1, "cursor": cursor.clone() }),
        now,
    )
    .await;
    assert_eq!(
        ids(&second["items"]),
        vec!["billing.invoice-1"],
        "{backend}"
    );
    assert!(
        second["cursor"].is_null(),
        "{backend}: the last page has no cursor"
    );

    // A cursor is stamped with its sort, so changing sort mid-scroll is caught
    // rather than served a page from two orderings.
    let mismatch = call(
        engine,
        "ui.executions.search",
        json!({ "limit": 1, "cursor": cursor, "sort": "createdAt:asc" }),
        now,
    )
    .await;
    assert_eq!(mismatch.head.status, 400, "{backend}");
    assert_eq!(mismatch.data["error"], "cursor_sort_mismatch", "{backend}");

    // --- filters ----------------------------------------------------------

    let pending = ok(
        engine,
        "ui.executions.search",
        json!({ "state": ["pending"], "countTotal": true }),
        now,
    )
    .await;
    assert_eq!(
        ids(&pending["items"]),
        vec!["checkout.order-8842"],
        "{backend}"
    );
    assert_eq!(
        pending["total"], 1,
        "{backend}: the total follows the filter"
    );

    let by_tag = ok(
        engine,
        "ui.executions.search",
        json!({ "tags": { "env": "prod" } }),
        now,
    )
    .await;
    assert_eq!(by_tag["items"].as_array().unwrap().len(), 2, "{backend}");
    let no_tag = ok(
        engine,
        "ui.executions.search",
        json!({ "tags": { "env": "staging" } }),
        now,
    )
    .await;
    assert!(no_tag["items"].as_array().unwrap().is_empty(), "{backend}");

    let by_prefix = ok(
        engine,
        "ui.executions.search",
        json!({ "idPrefix": "checkout." }),
        now,
    )
    .await;
    assert_eq!(
        ids(&by_prefix["items"]),
        vec!["checkout.order-8842"],
        "{backend}"
    );

    let by_func = ok(
        engine,
        "ui.executions.search",
        json!({ "func": "sendInvoice" }),
        now,
    )
    .await;
    assert_eq!(
        ids(&by_func["items"]),
        vec!["billing.invoice-1"],
        "{backend}"
    );

    let window = ok(
        engine,
        "ui.executions.search",
        json!({ "createdFrom": T0 + 2_000, "createdTo": T0 + 2_000 }),
        now,
    )
    .await;
    assert_eq!(
        ids(&window["items"]),
        vec!["checkout.order-8842"],
        "{backend}"
    );

    // --- sorting ----------------------------------------------------------

    let asc = ok(
        engine,
        "ui.executions.search",
        json!({ "sort": "createdAt:asc" }),
        now,
    )
    .await;
    assert_eq!(
        ids(&asc["items"]),
        vec!["billing.invoice-1", "checkout.order-8842"],
        "{backend}"
    );

    // Unsettled sorts at the end of time, in every dialect — the sentinel is
    // what makes NULL ordering not a per-backend opinion.
    let by_settled = ok(
        engine,
        "ui.executions.search",
        json!({ "sort": "settledAt:asc" }),
        now,
    )
    .await;
    assert_eq!(
        ids(&by_settled["items"]),
        vec!["billing.invoice-1", "checkout.order-8842"],
        "{backend}: settled first ascending"
    );
    let by_settled_desc = ok(
        engine,
        "ui.executions.search",
        json!({ "sort": "settledAt:desc" }),
        now,
    )
    .await;
    assert_eq!(
        ids(&by_settled_desc["items"]),
        vec!["checkout.order-8842", "billing.invoice-1"],
        "{backend}: and first descending"
    );

    // --- the detail view --------------------------------------------------

    let view = ok(
        engine,
        "ui.execution.get",
        json!({ "id": "checkout.order-8842" }),
        now,
    )
    .await;
    assert_eq!(view["root"]["id"], "checkout.order-8842", "{backend}");
    assert_eq!(
        ids(&view["nodes"]),
        vec![
            "checkout.order-8842",
            "checkout.order-8842:1",
            "checkout.order-8842:2"
        ],
        "{backend}: the whole tree, root included, in createdAt order"
    );
    assert_eq!(view["truncated"], false, "{backend}");
    assert_eq!(
        view["nodes"][1]["parentId"], "checkout.order-8842",
        "{backend}"
    );
    assert!(
        view["nodes"][1]["taskId"].is_null(),
        "{backend}: a run child executes inside its parent's task"
    );
    assert_eq!(
        view["nodes"][2]["taskId"], "checkout.order-8842:2",
        "{backend}: a targeted child opens its own task"
    );
    assert_eq!(view["nodes"][2]["func"], "chargeCard", "{backend}");
    assert!(
        view["nodes"][1]["param"]["data"].is_string(),
        "{backend}: the inspector reads param off the node"
    );
    assert_eq!(
        ids(&view["tasks"]),
        vec!["checkout.order-8842", "checkout.order-8842:2"],
        "{backend}: one task per targeted promise"
    );
    assert_eq!(view["tasks"][0]["state"], "pending", "{backend}");
    assert!(
        view["tasks"][0]["expiresAt"].is_i64(),
        "{backend}: a pending task's deadline is its retry"
    );

    // Any id inside the execution opens it — that is what makes
    // /executions/:id?step=:promiseId a deep link.
    let from_step = ok(
        engine,
        "ui.execution.get",
        json!({ "id": "checkout.order-8842:2" }),
        now,
    )
    .await;
    assert_eq!(from_step["root"]["id"], "checkout.order-8842", "{backend}");

    // Truncation is reported, never silent.
    let cut = ok(
        engine,
        "ui.execution.get",
        json!({ "id": "checkout.order-8842", "maxNodes": 2 }),
        now,
    )
    .await;
    assert_eq!(cut["truncated"], true, "{backend}");
    assert_eq!(cut["nodes"].as_array().unwrap().len(), 2, "{backend}");

    let missing = call(engine, "ui.execution.get", json!({ "id": "nope" }), now).await;
    assert_eq!(missing.head.status, 404, "{backend}");
    assert_eq!(missing.data["error"], "not_found", "{backend}");

    // --- schedules --------------------------------------------------------

    let schedules = ok(
        engine,
        "ui.schedules.search",
        json!({ "countTotal": true }),
        now,
    )
    .await;
    assert_eq!(ids(&schedules["items"]), vec!["sync-stripe"], "{backend}");
    assert_eq!(schedules["total"], 1, "{backend}");
    assert_eq!(schedules["items"][0]["cron"], "0 0 * * * *", "{backend}");
    assert_eq!(
        schedules["items"][0]["promiseId"], "sync-stripe.{{.timestamp}}",
        "{backend}"
    );
    assert!(
        schedules["items"][0]["lastRunAt"].is_null(),
        "{backend}: never run"
    );

    // --- refusals ---------------------------------------------------------

    for bad in [
        json!({ "limit": 0 }),
        json!({ "limit": 201 }),
        json!({ "sort": "createdAt" }),
        json!({ "sort": "name:asc" }),
        json!({ "cursor": "not-a-cursor" }),
        json!({ "createdFrom": 5, "createdTo": 1 }),
    ] {
        let resp = call(engine, "ui.executions.search", bad.clone(), now).await;
        assert_eq!(resp.head.status, 400, "{backend}: {bad} should be refused");
        assert_eq!(resp.data["error"], "invalid_request", "{backend}: {bad}");
    }

    // The console is read-only: nothing above changed anything.
    let after = ok(
        engine,
        "promise.get",
        json!({ "id": "checkout.order-8842" }),
        now,
    )
    .await;
    assert_eq!(after["promise"]["state"], "pending", "{backend}");
}

/// The four answers, compared field for field.
///
/// The differential does this for the worker protocol; this does it for the
/// console, which is where a dialect's opinion about NULL ordering or JSON
/// containment would otherwise show up as a UI that looks different depending
/// on which database is behind it.
async fn same_answers(engines: &[(&str, &dyn ResonateEngine)]) {
    if engines.len() < 2 {
        return;
    }
    for (_, e) in engines {
        seed(*e).await;
    }
    let now = T0 + 10_000;
    let probes: Vec<(&str, Value)> = vec![
        ("ui.executions.search", json!({ "countTotal": true })),
        ("ui.executions.search", json!({ "sort": "settledAt:desc" })),
        ("ui.executions.search", json!({ "state": ["pending"] })),
        ("ui.executions.search", json!({ "limit": 1 })),
        ("ui.execution.get", json!({ "id": "checkout.order-8842" })),
        ("ui.schedules.search", json!({ "countTotal": true })),
    ];
    let (first_name, first) = engines[0];
    for (kind, data) in probes {
        let expected = ok(first, kind, data.clone(), now).await;
        for (name, other) in &engines[1..] {
            let got = ok(*other, kind, data.clone(), now).await;
            assert_eq!(
                got, expected,
                "{name} and {first_name} disagree on {kind} {data}"
            );
        }
    }
}

fn sqlite() -> resonate_server_dbms::engine_sqlite::SqliteEngine {
    resonate_server_dbms::engine_sqlite::SqliteEngine::open(":memory:", 30_000, 10, true, false)
        .expect("in-memory sqlite")
}

fn oracle() -> SharedOracle {
    SharedOracle::with_preload_limit(10)
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_answers_the_console() {
    console_reads(&sqlite(), "sqlite").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_oracle_answers_the_console() {
    console_reads(&oracle(), "oracle").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_backend_gives_the_same_answer() {
    let sqlite = sqlite();
    let oracle = oracle();
    #[allow(unused_mut)] // only postgres and mysql push onto it
    let mut engines: Vec<(&str, &dyn ResonateEngine)> =
        vec![("sqlite", &sqlite), ("oracle", &oracle)];

    // Postgres and MySQL join when a database is named, as in the
    // differential. `debug.reset` first: these are shared databases.
    #[cfg(feature = "postgres")]
    let pg;
    #[cfg(feature = "postgres")]
    if let Ok(url) = std::env::var("TEST_POSTGRES_URL") {
        pg = resonate_server_dbms::engine_postgres::PostgresEngine::connect(
            &url, 4, 30_000, 10, true,
        )
        .await
        .expect("postgres");
        pg.init(true).await.expect("postgres schema");
        reset(&pg).await;
        engines.push(("postgres", &pg));
    }

    #[cfg(feature = "mysql")]
    let my;
    #[cfg(feature = "mysql")]
    if let Ok(url) = std::env::var("TEST_MYSQL_URL") {
        my = resonate_server_dbms::engine_mysql::MysqlEngine::connect(&url, 4, 30_000, 10, true)
            .await
            .expect("mysql");
        my.init(true).await.expect("mysql schema");
        reset(&my).await;
        engines.push(("mysql", &my));
    }

    same_answers(&engines).await;
}

#[allow(dead_code)] // only the optional backends need clearing
async fn reset(engine: &dyn ResonateEngine) {
    let _ = call(engine, "debug.reset", json!({}), T0).await;
}

/// A promise whose param is not the SDK's shape is not an error — it is a
/// promise nobody labelled, and it still belongs in the list.
#[tokio::test(flavor = "multi_thread")]
async fn an_unlabelled_execution_still_lists() {
    let engine = sqlite();
    ok(
        &engine,
        "promise.create",
        json!({
            "id": "bare",
            "timeoutAt": T0 + 60_000,
            "param": { "data": "not json at all" },
            "tags": { "resonate:scope": "global" }
        }),
        T0,
    )
    .await;
    let page = ok(&engine, "ui.executions.search", json!({}), T0 + 1).await;
    assert_eq!(ids(&page["items"]), vec!["bare"]);
    assert!(page["items"][0]["func"].is_null());
}

/// States are OR-ed, and the enum is the whole vocabulary.
#[tokio::test(flavor = "multi_thread")]
async fn several_states_are_or_ed() {
    let engine = sqlite();
    seed(&engine).await;
    let page = ok(
        &engine,
        "ui.executions.search",
        json!({ "state": ["pending", "resolved"] }),
        T0 + 10_000,
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 2);

    let resp = call(
        &engine,
        "ui.executions.search",
        json!({ "state": ["nonsense"] }),
        T0 + 10_000,
    )
    .await;
    assert_eq!(resp.head.status, 400);

    // Every state the enum has is accepted.
    for state in [
        PromiseState::Pending,
        PromiseState::Resolved,
        PromiseState::Rejected,
        PromiseState::RejectedCanceled,
        PromiseState::RejectedTimedout,
    ] {
        let resp = call(
            &engine,
            "ui.executions.search",
            json!({ "state": [state.as_str()] }),
            T0 + 10_000,
        )
        .await;
        assert_eq!(resp.head.status, 200, "{state}");
    }
}

/// The one write the console has is the ordinary one.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_is_promise_settle_and_the_list_sees_it() {
    let engine = sqlite();
    seed(&engine).await;
    ok(
        &engine,
        "promise.settle",
        json!({ "id": "checkout.order-8842", "state": "rejected_canceled" }),
        T0 + 9_000,
    )
    .await;
    let page = ok(&engine, "ui.executions.search", json!({}), T0 + 10_000).await;
    let by_id: HashMap<_, _> = page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| (i["id"].as_str().unwrap(), i.clone()))
        .collect();
    assert_eq!(by_id["checkout.order-8842"]["state"], "rejected_canceled");
    assert_eq!(by_id["checkout.order-8842"]["settledAt"], T0 + 9_000);
}
