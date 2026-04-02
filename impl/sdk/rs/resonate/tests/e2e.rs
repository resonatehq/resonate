//! End-to-end integration tests that run against a real Resonate server.
//!
//! These tests are **skipped** when the `RESONATE_URL` environment variable
//! is not set.  When the variable is present its value is used as the server
//! URL (e.g. `http://localhost:8001`).
//!
//! ```bash
//! # Skip e2e (default):
//! cargo test
//!
//! # Run e2e against a local server:
//! RESONATE_URL=http://localhost:8001 cargo test --test e2e
//! ```

use std::time::Duration;

use resonate::prelude::*;

// ═══════════════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════════════

/// Read the server URL from the environment. Returns `None` when unset.
fn resonate_url() -> Option<String> {
    std::env::var("RESONATE_URL").ok()
}

/// Early-return from a test when no server URL is configured.
/// The test counts as passed but prints a notice on stderr.
macro_rules! require_server {
    () => {
        match resonate_url() {
            Some(url) => url,
            None => {
                eprintln!("RESONATE_URL not set — skipping e2e test");
                return;
            }
        }
    };
}

/// Generate a unique ID for a test run to avoid collisions on the server.
fn unique_id(test_name: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("e2e-{}-{}", test_name, ts)
}

/// Default timeout applied to every server-facing await.
const E2E_TIMEOUT: Duration = Duration::from_secs(30);

/// Convenience wrapper: await a future with the default e2e timeout.
/// Accepts anything that implements `IntoFuture` (including the Resonate builders).
async fn with_timeout<F: std::future::IntoFuture>(f: F) -> F::Output {
    tokio::time::timeout(E2E_TIMEOUT, f.into_future())
        .await
        .expect("e2e test timed out")
}

/// Build a `Resonate` instance pointed at the server.
fn make_resonate(url: &str) -> Resonate {
    Resonate::new(ResonateConfig {
        url: Some(url.to_string()),
        ..Default::default()
    })
}

// ═══════════════════════════════════════════════════════════════════
//  Test functions (leaf)
// ═══════════════════════════════════════════════════════════════════

#[resonate::function]
async fn add(x: i64, y: i64) -> Result<i64> {
    Ok(x + y)
}

#[resonate::function]
async fn greet(name: String) -> Result<String> {
    Ok(format!("hello, {}!", name))
}

#[resonate::function]
async fn noop() -> Result<()> {
    Ok(())
}

#[resonate::function]
async fn fail_always(msg: String) -> Result<String> {
    Err(Error::Application { message: msg })
}

// ═══════════════════════════════════════════════════════════════════
//  Test functions (workflows)
// ═══════════════════════════════════════════════════════════════════

#[resonate::function]
async fn sequential_workflow(ctx: &Context) -> Result<i64> {
    let a: i64 = ctx.rpc::<i64>("add", (1_i64, 2_i64)).await?;
    let b: i64 = ctx.rpc::<i64>("add", (a, 3_i64)).await?;
    Ok(b)
}

#[resonate::function]
async fn parallel_workflow(ctx: &Context) -> Result<i64> {
    let h1 = ctx.rpc::<i64>("add", (10_i64, 20_i64)).spawn().await?;
    let h2 = ctx.rpc::<i64>("add", (30_i64, 40_i64)).spawn().await?;
    let r1: i64 = h1.await?;
    let r2: i64 = h2.await?;
    Ok(r1 + r2)
}

#[resonate::function]
async fn run_sub_workflow(ctx: &Context) -> Result<i64> {
    let a: i64 = ctx.run(add, (5_i64, 5_i64)).await?;
    let b: i64 = ctx.run(add, (a, 10_i64)).await?;
    Ok(b)
}

// ═══════════════════════════════════════════════════════════════════
//  Tests — Basic connectivity & simple functions
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn connectivity() {
    let url = require_server!();
    let r = make_resonate(&url);
    let id = unique_id("connectivity");

    // Create and fetch a promise via the sub-client
    let created = with_timeout(
        r.promises
            .create(&id, i64::MAX, serde_json::json!(null), serde_json::json!({})),
    )
    .await;
    assert!(created.is_ok(), "should create promise: {:?}", created.err());

    let fetched = with_timeout(r.promises.get(&id)).await;
    assert!(fetched.is_ok(), "should get promise: {:?}", fetched.err());
    assert_eq!(fetched.unwrap()["id"], id);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn simple_add() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();

    let id = unique_id("simple-add");
    let result: i64 = with_timeout(r.run(&id, add, (3_i64, 4_i64))).await.unwrap();
    assert_eq!(result, 7);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn simple_greet() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(greet).unwrap();

    let id = unique_id("simple-greet");
    let result: String = with_timeout(r.run(&id, greet, "world".to_string()))
        .await
        .unwrap();
    assert_eq!(result, "hello, world!");

    r.stop().await.unwrap();
}

#[tokio::test]
async fn simple_noop() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(noop).unwrap();

    let id = unique_id("simple-noop");
    let result: () = with_timeout(r.run(&id, noop, ())).await.unwrap();
    assert_eq!(result, ());

    r.stop().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════
//  Tests — RPC & idempotency
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn rpc_to_registered_function() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();

    let id = unique_id("rpc-add");
    let result: i64 = with_timeout(r.rpc(&id, "add", (10_i64, 20_i64))).await.unwrap();
    assert_eq!(result, 30);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn idempotent_run() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();

    let id = unique_id("idempotent-run");
    let r1: i64 = with_timeout(r.run(&id, add, (5_i64, 5_i64))).await.unwrap();
    let r2: i64 = with_timeout(r.run(&id, add, (5_i64, 5_i64))).await.unwrap();
    assert_eq!(r1, 10);
    assert_eq!(r2, 10);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn idempotent_rpc() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();

    let id = unique_id("idempotent-rpc");
    let r1: i64 = with_timeout(r.rpc(&id, "add", (7_i64, 8_i64))).await.unwrap();
    let r2: i64 = with_timeout(r.rpc(&id, "add", (7_i64, 8_i64))).await.unwrap();
    assert_eq!(r1, 15);
    assert_eq!(r2, 15);

    r.stop().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════
//  Tests — Workflows with sub-calls and parallelism
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn workflow_sequential_rpcs() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();
    r.register(sequential_workflow).unwrap();

    let id = unique_id("seq-workflow");
    let result: i64 = with_timeout(r.run(&id, sequential_workflow, ())).await.unwrap();
    // 1+2=3, 3+3=6
    assert_eq!(result, 6);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn workflow_parallel_rpcs() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();
    r.register(parallel_workflow).unwrap();

    let id = unique_id("par-workflow");
    let result: i64 = with_timeout(r.run(&id, parallel_workflow, ())).await.unwrap();
    // (10+20) + (30+40) = 100
    assert_eq!(result, 100);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn workflow_with_ctx_run() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();
    r.register(run_sub_workflow).unwrap();

    let id = unique_id("run-sub-workflow");
    let result: i64 = with_timeout(r.run(&id, run_sub_workflow, ())).await.unwrap();
    // 5+5=10, 10+10=20
    assert_eq!(result, 20);

    r.stop().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════
//  Tests — Error propagation, handles, schedules
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn error_propagation() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(fail_always).unwrap();

    let id = unique_id("error-prop");
    let result: Result<String> =
        with_timeout(r.run(&id, fail_always, "boom".to_string())).await;
    assert!(result.is_err(), "should propagate error");

    r.stop().await.unwrap();
}

#[tokio::test]
async fn handle_spawn_and_result() {
    let url = require_server!();
    let r = make_resonate(&url);
    r.register(add).unwrap();

    let id = unique_id("handle-spawn");
    let mut handle = with_timeout(r.run(&id, add, (100_i64, 200_i64)).spawn())
        .await
        .unwrap();

    let result: i64 = with_timeout(handle.result()).await.unwrap();
    assert_eq!(result, 300);

    r.stop().await.unwrap();
}

#[tokio::test]
async fn schedule_create_and_delete() {
    let url = require_server!();
    let r = make_resonate(&url);

    let name = unique_id("schedule");
    let schedule = with_timeout(r.schedule(&name, "*/5 * * * *", "add", (1_i64, 2_i64)))
        .await
        .unwrap();

    let delete_result = with_timeout(schedule.delete()).await;
    assert!(
        delete_result.is_ok(),
        "should delete schedule: {:?}",
        delete_result.err()
    );

    r.stop().await.unwrap();
}
