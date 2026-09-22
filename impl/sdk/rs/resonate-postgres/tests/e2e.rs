//! End-to-end tests that run against a real Postgres database with the
//! resonate-pg schema applied (github.com/resonatehq/resonate-pg):
//!
//! ```bash
//! psql "$RESONATE_PG_URL" -f resonate.sql
//! ```
//!
//! Tests are ignored when the `RESONATE_PG_URL` environment variable is
//! not set (via [`test_with::env`]). pg_cron is not required: the network
//! pump drives `resonate.process_timeouts()`.
//!
//! ```bash
//! RESONATE_PG_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test -p resonate-sdk-postgres --test e2e
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use resonate_sdk::prelude::*;
use resonate_sdk::types::Value;
use resonate_sdk_postgres::PostgresNetwork;

// ═══════════════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════════════

fn pg_url() -> String {
    std::env::var("RESONATE_PG_URL").expect("RESONATE_PG_URL must be set")
}

/// Generate a unique ID for a test run to avoid collisions in the database.
fn unique_id(test_name: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("e2e-{}-{}-{}", test_name, ts, n)
}

/// Default timeout applied to every server-facing await.
const E2E_TIMEOUT: Duration = Duration::from_secs(30);

async fn with_timeout<F: std::future::IntoFuture>(f: F) -> F::Output {
    tokio::time::timeout(E2E_TIMEOUT, f.into_future())
        .await
        .expect("e2e test timed out")
}

/// Build a `Resonate` instance on a PostgresNetwork. A unique group per
/// test isolates each test's anycast queue.
fn make_resonate() -> Resonate {
    let network = PostgresNetwork::builder(pg_url())
        .pid(unique_id("worker"))
        .group(unique_id("group"))
        .build();
    Resonate::new(ResonateConfig {
        network: Some(Arc::new(network)),
        ..Default::default()
    })
}

// ═══════════════════════════════════════════════════════════════════
//  Test functions
// ═══════════════════════════════════════════════════════════════════

#[resonate_sdk::function]
async fn add(x: i64, y: i64) -> Result<i64> {
    Ok(x + y)
}

#[resonate_sdk::function]
async fn fail_always(msg: String) -> Result<String> {
    Err(Error::Application { message: msg })
}

#[resonate_sdk::function]
async fn sequential_workflow(ctx: &Context) -> Result<i64> {
    let a: i64 = ctx.rpc::<i64>("add", (1_i64, 2_i64)).await?;
    let b: i64 = ctx.rpc::<i64>("add", (a, 3_i64)).await?;
    Ok(b)
}

#[resonate_sdk::function]
async fn parallel_workflow(ctx: &Context) -> Result<i64> {
    let h1 = ctx.rpc::<i64>("add", (10_i64, 20_i64)).spawn()?;
    let h2 = ctx.rpc::<i64>("add", (30_i64, 40_i64)).spawn()?;
    let r1: i64 = h1.await?;
    let r2: i64 = h2.await?;
    Ok(r1 + r2)
}

// ═══════════════════════════════════════════════════════════════════
//  Tests
// ═══════════════════════════════════════════════════════════════════

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn connectivity() {
    let r = make_resonate();
    let id = unique_id("connectivity");

    let created = with_timeout(
        r.promises
            .create(&id, i64::MAX, Value::default(), HashMap::new()),
    )
    .await;
    assert!(
        created.is_ok(),
        "should create promise: {:?}",
        created.err()
    );

    let fetched = with_timeout(r.promises.get(&id)).await;
    assert!(fetched.is_ok(), "should get promise: {:?}", fetched.err());
    assert_eq!(fetched.unwrap().id, id);

    r.stop().await.unwrap();
}

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn simple_add() {
    let r = make_resonate();
    r.register(add).unwrap();

    let id = unique_id("simple-add");
    let result: i64 = with_timeout(r.run(&id, add, (3_i64, 4_i64))).await.unwrap();
    assert_eq!(result, 7);

    r.stop().await.unwrap();
}

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn rpc_to_registered_function() {
    let r = make_resonate();
    r.register(add).unwrap();

    let id = unique_id("rpc-add");
    let result: i64 = with_timeout(r.rpc(&id, "add", (10_i64, 20_i64)))
        .await
        .unwrap();
    assert_eq!(result, 30);

    r.stop().await.unwrap();
}

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn idempotent_run() {
    let r = make_resonate();
    r.register(add).unwrap();

    let id = unique_id("idempotent-run");
    let r1: i64 = with_timeout(r.run(&id, add, (5_i64, 5_i64))).await.unwrap();
    let r2: i64 = with_timeout(r.run(&id, add, (5_i64, 5_i64))).await.unwrap();
    assert_eq!(r1, 10);
    assert_eq!(r2, 10);

    r.stop().await.unwrap();
}

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn workflow_sequential_rpcs() {
    let r = make_resonate();
    r.register(add).unwrap();
    r.register(sequential_workflow).unwrap();

    let id = unique_id("seq-workflow");
    let result: i64 = with_timeout(r.run(&id, sequential_workflow, ()))
        .await
        .unwrap();
    // 1+2=3, 3+3=6
    assert_eq!(result, 6);

    r.stop().await.unwrap();
}

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn workflow_parallel_rpcs() {
    let r = make_resonate();
    r.register(add).unwrap();
    r.register(parallel_workflow).unwrap();

    let id = unique_id("par-workflow");
    let result: i64 = with_timeout(r.run(&id, parallel_workflow, ()))
        .await
        .unwrap();
    // (10+20) + (30+40) = 100
    assert_eq!(result, 100);

    r.stop().await.unwrap();
}

#[test_with::env(RESONATE_PG_URL)]
#[tokio::test]
async fn error_propagation() {
    let r = make_resonate();
    r.register(fail_always).unwrap();

    let id = unique_id("error-prop");
    let result: Result<String> = with_timeout(r.run(&id, fail_always, "boom".to_string())).await;
    assert!(result.is_err(), "should propagate error");

    r.stop().await.unwrap();
}
