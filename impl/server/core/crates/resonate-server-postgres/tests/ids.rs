//! Promise ids with more than one ':' on the Postgres engine. The origin is
//! the text before the first ':'; the rest is an id like any other. Needs
//! `TEST_POSTGRES_URL`, and empties the database it names (both tests drop
//! and recreate the `resonate` schema, so they take turns).

use resonate_core::types::{RequestEnvelope, RequestHead, SUPPORTED_VERSIONS};
use resonate_server_postgres::PostgresEngine;
use resonate_sql::engine::{Engine, Input};
use serde_json::{json, Value};

const T0: i64 = 1_700_000_000_000;

/// Both tests rebuild the schema of one database.
static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// An empty database: no `resonate` schema and no migration history.
async fn empty(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("connect");
    sqlx::raw_sql(
        "DROP SCHEMA IF EXISTS resonate CASCADE; DROP TABLE IF EXISTS public._sqlx_migrations",
    )
    .execute(&pool)
    .await
    .expect("drop schema");
    pool.close().await;
}

async fn create_status(e: &PostgresEngine, id: &str) -> i32 {
    let create = req(
        "promise.create",
        json!({ "id": id, "timeoutAt": T0 + 60_000, "param": {}, "tags": {} }),
    );
    let out = e.process(Input::External(&create), T0).await;
    out.response.expect("a response").head.status
}

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

#[tokio::test(flavor = "multi_thread")]
async fn postgres_accepts_ids_with_several_colons() {
    let Ok(url) = std::env::var("TEST_POSTGRES_URL") else {
        eprintln!("TEST_POSTGRES_URL not set: skipped");
        return;
    };
    let _turn = TURN.lock().await;
    empty(&url).await;
    let e = PostgresEngine::connect(&url, 2, 30_000, 10, true)
        .await
        .expect("connect");
    e.init(true).await.expect("schema");

    for (id, tags) in [
        ("a:b:c", json!({})),
        ("x::y", json!({})),
        (
            "wft:n1:order-42:r1:3",
            json!({ "resonate:target": "poll://any@w" }),
        ),
    ] {
        let create = req(
            "promise.create",
            json!({ "id": id, "timeoutAt": T0 + 60_000, "param": {}, "tags": tags }),
        );
        let out = e.process(Input::External(&create), T0).await;
        let resp = out.response.expect("a response");
        assert_eq!(resp.head.status, 200, "{id}: {:?}", resp.data);
        assert_eq!(resp.data["promise"]["id"], json!(id));

        let get = req("promise.get", json!({ "id": id }));
        let out = e.process(Input::External(&get), T0).await;
        assert_eq!(out.response.expect("a response").head.status, 200, "{id}");
    }

    // The task on such an id goes through a claim like any other.
    let acquire = req(
        "task.acquire",
        json!({ "id": "wft:n1:order-42:r1:3", "version": 0, "pid": "p", "ttl": 10_000 }),
    );
    let out = e.process(Input::External(&acquire), T0).await;
    assert_eq!(out.response.expect("a response").head.status, 200);
}

/// A database 0001 created still carries the old check, and 0002 removes it:
/// before the migration an id with two ':' is refused, the server will not
/// start on the stale schema without `migrate`, and after it the id is
/// accepted.
#[tokio::test(flavor = "multi_thread")]
async fn a_database_from_the_first_migration_migrates_and_then_accepts_them() {
    let Ok(url) = std::env::var("TEST_POSTGRES_URL") else {
        eprintln!("TEST_POSTGRES_URL not set: skipped");
        return;
    };
    let _turn = TURN.lock().await;
    empty(&url).await;

    // The database as 0001 alone left it.
    let dir = std::env::temp_dir().join(format!("resonate-pg-0001-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/migrations/0001_initial.sql"),
        dir.join("0001_initial.sql"),
    )
    .expect("copy 0001");
    let first = sqlx::migrate::Migrator::new(dir.as_path())
        .await
        .expect("0001");
    let e = PostgresEngine::connect(&url, 2, 30_000, 10, true)
        .await
        .expect("connect");
    let pool = e_pool(&url).await;
    sqlx::raw_sql("CREATE SCHEMA IF NOT EXISTS resonate")
        .execute(&pool)
        .await
        .expect("schema");
    first.run(&pool).await.expect("run 0001");
    pool.close().await;
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        create_status(&e, "a:b:c").await,
        500,
        "0001 refuses two ':'"
    );
    assert_eq!(create_status(&e, "a:b").await, 200);

    // Behind the binary: refused without `migrate`, applied with it.
    assert!(
        e.init(false).await.is_err(),
        "a pending migration needs `migrate`"
    );
    e.init(true).await.expect("0002");

    assert_eq!(create_status(&e, "a:b:c").await, 200, "accepted after 0002");
    assert_eq!(create_status(&e, "wft:n1:order-42:r1:3").await, 200);
}

/// A pool with the engine's search path, for running a migrator by hand.
async fn e_pool(url: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET search_path TO resonate, public")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .expect("connect")
}
