//! Smoke tests against a real object store.
//!
//! Everything else about the S3 backend is exercised over `object_store`'s
//! in-process store, which is faithful but is not the thing that ships. These
//! two tests answer the questions only a real bucket can:
//!
//! 1. Does this store's conditional write actually behave like a
//!    compare-and-swap? The whole design rests on it. A store that accepts
//!    `If-Match` and ignores it — MinIO, B2, Spaces — would pass every other
//!    test in the repository and lose writes in production.
//! 2. Does listing come back in key order, and is it read-after-write
//!    consistent? The timer poller reads nearest-deadline-first purely from key
//!    ordering.
//!
//! Skipped unless `TEST_S3_BUCKET` is set, so `cargo test` stays offline:
//!
//! ```text
//! TEST_S3_BUCKET=my-bucket TEST_S3_REGION=us-east-1 \
//!   cargo test --test s3_live -- --nocapture
//! ```
//!
//! For an S3-compatible service, add `TEST_S3_ENDPOINT` and, for a local one
//! over plain HTTP, `TEST_S3_ALLOW_HTTP=1`. Credentials come from the
//! environment the way any AWS client reads them.
//!
//! Every run works under a prefix of its own and deletes it afterwards, so two
//! runs against one bucket cannot collide.

use std::sync::Arc;

use resonate_core::types::{RequestEnvelope, RequestHead, ResponseEnvelope, SUPPORTED_VERSIONS};
use resonate_core::ResonateServer;
use resonate_server_blob::{
    applier::KeySpace,
    sender::NullRouter,
    server::{Server, ServerCfg},
    store::{Etag, ObjectStoreAdapter, Store, StoreError},
};
use serde_json::{json, Value};

const T0: i64 = 1_000_000_000;
const WORKER_URL: &str = "http://s3-live-test-worker:9999";
const PID: &str = "s3-live-pid";
const TTL: i64 = 60_000;

/// A live store and the prefix this run owns, or `None` when unconfigured.
fn live_store() -> Option<(Arc<dyn Store>, String)> {
    let bucket = match std::env::var("TEST_S3_BUCKET") {
        Ok(b) if !b.is_empty() => b,
        _ => {
            eprintln!("[s3-live] TEST_S3_BUCKET not set — skipped");
            return None;
        }
    };
    let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&bucket);
    if let Ok(region) = std::env::var("TEST_S3_REGION") {
        builder = builder.with_region(region);
    }
    if let Ok(endpoint) = std::env::var("TEST_S3_ENDPOINT") {
        builder = builder.with_endpoint(endpoint);
    }
    if std::env::var("TEST_S3_ALLOW_HTTP").is_ok() {
        builder = builder.with_allow_http(true);
    }
    let store = builder.build().expect("S3 store configuration");
    // Unique per run: two runs against one bucket must not see each other, and
    // the teardown deletes the whole prefix.
    let prefix = format!("resonate-live-test/{:016x}", fastrand::u64(..));
    eprintln!("[s3-live] bucket={bucket} prefix={prefix}");
    Some((Arc::new(ObjectStoreAdapter::new(store)), prefix))
}

async fn teardown(store: &Arc<dyn Store>, prefix: &str) {
    if let Err(e) = store.delete_prefix(prefix).await {
        eprintln!("[s3-live] teardown of {prefix} failed: {e}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_store_honours_the_conditional_write_contract() {
    let (store, prefix) = match live_store() {
        Some(pair) => pair,
        None => return,
    };
    let key = format!("{prefix}/doc");

    // Absent is not an error.
    assert_eq!(store.get(&key).await.unwrap(), None);

    // Create wins once.
    let first = store
        .put_if_none_match(&key, b"one".to_vec())
        .await
        .expect("create");
    let (body, read) = store.get(&key).await.unwrap().expect("present");
    assert_eq!(body, b"one", "read-after-write");
    assert_eq!(
        read, first,
        "the ETag a read reports is the one put returned"
    );

    assert_eq!(
        store.put_if_none_match(&key, b"two".to_vec()).await,
        Err(StoreError::PreconditionFailed),
        "a second create must lose"
    );
    assert_eq!(store.get(&key).await.unwrap().unwrap().0, b"one");

    // Replace on the current version wins; on a stale one, loses.
    let second = store
        .put_if_match(&key, b"two".to_vec(), &first)
        .await
        .expect("conditional replace");
    assert_ne!(first, second, "a write moves the version");
    assert_eq!(
        store.put_if_match(&key, b"three".to_vec(), &first).await,
        Err(StoreError::PreconditionFailed),
        "THIS IS THE LOAD-BEARING ASSERTION: a stale ETag must be refused. \
         A store that accepts it silently loses writes under this design."
    );
    assert_eq!(store.get(&key).await.unwrap().unwrap().0, b"two");

    assert_eq!(
        store
            .put_if_match(
                &format!("{prefix}/absent"),
                b"x".to_vec(),
                &Etag("\"1\"".into())
            )
            .await,
        Err(StoreError::PreconditionFailed),
        "a conditional replace of nothing must be refused"
    );

    // Deleting is idempotent — the timer poller relies on it.
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), None);

    teardown(&store, &prefix).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_store_lists_in_key_order() {
    let (store, prefix) = match live_store() {
        Some(pair) => pair,
        None => return,
    };
    // Written out of order, on purpose: the poller reads
    // nearest-deadline-first from key ordering alone.
    for deadline in [500, 100, 400, 200, 300] {
        store
            .put(&format!("{prefix}/t/00/{deadline:020}_origin"), Vec::new())
            .await
            .unwrap();
    }
    store
        .put(&format!("{prefix}/t/01/{:020}_other", 50), Vec::new())
        .await
        .unwrap();

    let listed = store.list(&format!("{prefix}/t/00"), 10).await.unwrap();
    let deadlines: Vec<i64> = listed
        .iter()
        .map(|k| {
            let name = k.rsplit('/').next().unwrap();
            name[..20].parse().unwrap()
        })
        .collect();
    assert_eq!(deadlines, vec![100, 200, 300, 400, 500]);

    // A capped listing returns the *nearest* deadlines, not the first ones the
    // service happened to hand over.
    let capped = store.list(&format!("{prefix}/t/00"), 2).await.unwrap();
    assert_eq!(capped.len(), 2);
    assert!(capped[0].contains(&format!("{:020}", 100)));
    assert!(capped[1].contains(&format!("{:020}", 200)));

    // And the shard prefixes are genuinely separate.
    assert_eq!(
        store
            .list(&format!("{prefix}/t/01"), 10)
            .await
            .unwrap()
            .len(),
        1
    );

    teardown(&store, &prefix).await;
}

fn envelope(kind: &str, data: Value, now: i64) -> RequestEnvelope {
    RequestEnvelope {
        kind: kind.to_string(),
        head: RequestHead {
            corr_id: "s3-live".into(),
            version: SUPPORTED_VERSIONS[0].into(),
            auth: None,
            debug_time: Some(now),
        },
        data,
    }
}

async fn send(server: &Arc<Server>, kind: &str, data: Value, now: i64) -> ResponseEnvelope {
    server
        .process(&envelope(kind, data, now))
        .await
        .expect("the live backend answers")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workflow_runs_end_to_end_against_a_live_store() {
    let (store, prefix) = match live_store() {
        Some(pair) => pair,
        None => return,
    };
    let server = Server::build(
        Arc::clone(&store),
        Arc::new(NullRouter),
        ServerCfg {
            keys: KeySpace::new(prefix.clone(), 4),
            debug: true,
            search: true,
            ..Default::default()
        },
    );
    // Debug is a startup flag: the server above was built with `debug: true`,
    // so messages are held rather than routed and stay visible in the
    // snapshot.

    // Claim work by describing it.
    let created = send(
        &server,
        "task.create",
        json!({ "pid": PID, "ttl": TTL, "action": {
            "kind": "promise.create", "head": {}, "data": {
                "id": "live:t", "timeoutAt": T0 + 600_000, "param": { "data": "aGk=" },
                "tags": { "resonate:target": WORKER_URL } } } }),
        T0,
    )
    .await;
    assert_eq!(created.head.status, 200);
    assert_eq!(created.data["task"]["state"], "acquired");
    assert_eq!(created.data["task"]["version"], 1);

    // A promise to wait on, and a listener to be told about it.
    assert_eq!(
        send(
            &server,
            "promise.create",
            json!({ "id": "live:a", "timeoutAt": T0 + 600_000, "param": {}, "tags": {} }),
            T0 + 1_000,
        )
        .await
        .head
        .status,
        200
    );
    assert_eq!(
        send(
            &server,
            "promise.register_listener",
            json!({ "awaited": "live:a", "address": WORKER_URL }),
            T0 + 1_500,
        )
        .await
        .head
        .status,
        200
    );

    // Park the task on it, then settle it and watch the task come back.
    assert_eq!(
        send(
            &server,
            "task.suspend",
            json!({ "id": "live:t", "version": 1, "actions": [{
                "kind": "promise.register_callback", "head": {},
                "data": { "awaited": "live:a", "awaiter": "live:t" } }] }),
            T0 + 2_000,
        )
        .await
        .head
        .status,
        200
    );
    assert_eq!(
        send(&server, "task.get", json!({ "id": "live:t" }), T0 + 2_100)
            .await
            .data["task"]["state"],
        "suspended"
    );

    let settled = send(
        &server,
        "promise.settle",
        json!({ "id": "live:a", "state": "resolved", "value": { "data": "b2s=" } }),
        T0 + 3_000,
    )
    .await;
    assert_eq!(settled.head.status, 200);
    assert_eq!(settled.data["promise"]["settledAt"], T0 + 3_000);

    let resumed = send(&server, "task.get", json!({ "id": "live:t" }), T0 + 3_100).await;
    assert_eq!(resumed.data["task"]["state"], "pending");
    assert_eq!(resumed.data["task"]["resumes"], 1);

    // The settlement queued a dispatch for the resumed task and an unblock for
    // the listener.
    let snap = send(&server, "debug.snap", json!({}), T0 + 3_100).await;
    let kinds: Vec<&str> = snap.data["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["message"]["kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"execute"), "got {kinds:?}");
    assert!(kinds.contains(&"unblock"), "got {kinds:?}");

    // Sweep past every deadline: the workflow promise times out and its task
    // finishes.
    assert_eq!(
        send(
            &server,
            "debug.tick",
            json!({ "time": T0 + 1_000_000 }),
            T0 + 1_000_000,
        )
        .await
        .head
        .status,
        200
    );
    let final_snap = send(&server, "debug.snap", json!({}), T0 + 1_000_000).await;
    for promise in final_snap.data["promises"].as_array().unwrap() {
        assert_ne!(promise["state"], "pending", "{promise}");
    }
    for task in final_snap.data["tasks"].as_array().unwrap() {
        assert_eq!(task["state"], "fulfilled", "{task}");
    }
    assert!(final_snap.data["promiseTimeouts"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(final_snap.data["taskTimeouts"]
        .as_array()
        .unwrap()
        .is_empty());

    // Search reaches every origin document.
    let found = send(
        &server,
        "promise.search",
        json!({ "limit": 100 }),
        T0 + 1_000_000,
    )
    .await;
    let ids: Vec<&str> = found.data["promises"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["live:a", "live:t"]);

    // debug.reset is the teardown: it deletes every prefix this run wrote.
    assert_eq!(
        send(&server, "debug.reset", json!({}), T0 + 1_000_000)
            .await
            .head
            .status,
        200
    );
    assert!(store.list(&prefix, 10).await.unwrap().is_empty());
    teardown(&store, &prefix).await;
}
