// Against a real Kafka: fencing, restore, and a two-node cluster with a real
// consumer group, RocksDB copies and HTTP forwarding.
//
// Opt-in, like the blob backend's live tests. Every test uses fresh topics
// under a random prefix, so a shared cluster is fine; a single broker needs
// its transaction log configured for one replica (the stock KRaft
// `server.properties` is).
//
// Run:
//   TEST_KAFKA_BROKERS=localhost:9092 \
//     cargo test -p resonate-server-kafka --test live -- --nocapture --test-threads=1

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use resonate_core::types::{RequestEnvelope, SUPPORTED_VERSIONS};
use resonate_core::ResonateServer;
use resonate_server_blob::sender::{NullRouter, Sender};
use resonate_server_kafka::local::rocks::{RocksCfg, RocksLocal};
use resonate_server_kafka::log::kafka::{KafkaCfg, KafkaLog};
use resonate_server_kafka::log::{Checkpoint, Log, LogError, Record, Topic};
use resonate_server_kafka::membership::kafka::{GroupCfg, KafkaMembership};
use resonate_server_kafka::node::{Node, NodeCfg};
use resonate_server_kafka::peer::{self, HttpPeers};
use serde_json::{json, Value};

fn brokers() -> Option<String> {
    match std::env::var("TEST_KAFKA_BROKERS") {
        Ok(b) => Some(b),
        Err(_) => {
            eprintln!("[kafka-live] TEST_KAFKA_BROKERS not set — skipped");
            None
        }
    }
}

fn cfg(brokers: &str, prefix: &str, partitions: u32, node: &str) -> KafkaCfg {
    KafkaCfg {
        brokers: brokers.to_string(),
        topic_prefix: prefix.to_string(),
        txn_prefix: prefix.to_string(),
        partitions,
        replication_factor: 1,
        create_topics: true,
        node_id: node.to_string(),
        ..Default::default()
    }
}

fn prefix(test: &str) -> String {
    format!("t-{test}-{:08x}", fastrand::u32(..))
}

fn envelope(kind: &str, data: Value) -> RequestEnvelope {
    serde_json::from_value(json!({
        "kind": kind,
        "head": { "corrId": "c", "version": SUPPORTED_VERSIONS[0] },
        "data": data,
    }))
    .unwrap()
}

/// Send until answered: a rebalance answers 503s for a moment, as it would to
/// any client, and a client retries.
async fn ok(node: &Arc<Node>, kind: &str, data: Value) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match node.process(&envelope(kind, data.clone())).await {
            Ok(resp) => {
                assert_eq!(resp.head.status, 200, "{kind}: {}", resp.data);
                return resp.data;
            }
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "{kind} never answered: {}",
                    e.message
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

const FAR: i64 = 4_000_000_000_000;

fn create(id: &str) -> Value {
    json!({ "id": id, "timeoutAt": FAR, "param": { "data": "aGk=" }, "tags": {} })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_fence_refuses_the_first_writer() {
    let Some(brokers) = brokers() else { return };
    let log = KafkaLog::connect(cfg(&brokers, &prefix("fence"), 2, "n"))
        .await
        .expect("connects and creates topics");

    let first = log.fence(1).await.expect("fenced");
    let rec = |k: &str| Record {
        topic: Topic::Promises,
        key: k.to_string(),
        value: Some(b"v".to_vec()),
    };
    let after = first
        .commit(vec![rec("a"), rec("b")], None, Checkpoint::default())
        .await
        .expect("the only writer commits");
    assert_eq!(after.promises, 2, "two records, offsets 0 and 1");

    let second = log.fence(1).await.expect("fenced again");
    match first.commit(vec![rec("c")], None, after).await {
        Err(LogError::Fenced(_)) => {}
        other => panic!("the first writer must be fenced, got {other:?}"),
    }
    let after = second
        .commit(
            vec![Record {
                topic: Topic::Schedules,
                key: "s".into(),
                value: Some(b"x".to_vec()),
            }],
            None,
            after,
        )
        .await
        .expect("the new writer commits");

    // Read back: committed only, the fenced write nowhere, and a checkpoint
    // past every marker.
    let mut reader = log.reader(1, Checkpoint::default()).await.unwrap();
    let mut keys = Vec::new();
    let mut end = Checkpoint::default();
    while let Some((batch, cp)) = reader.next().await.unwrap() {
        keys.extend(batch.into_iter().map(|c| (c.topic, c.key)));
        end = cp;
    }
    assert_eq!(
        keys,
        vec![
            (Topic::Promises, "a".to_string()),
            (Topic::Promises, "b".to_string()),
            (Topic::Schedules, "s".to_string())
        ]
    );
    assert!(end.covers(&after), "{end:?} must cover {after:?}");
    // Resuming at the end reads nothing.
    let mut reader = log.reader(1, end).await.unwrap();
    while let Some((batch, _)) = reader.next().await.unwrap() {
        assert!(batch.is_empty());
    }
}

struct Started {
    node: Arc<Node>,
    stop_peer: tokio::sync::watch::Sender<bool>,
}

async fn start_node(
    brokers: &str,
    prefix: &str,
    partitions: u32,
    id: &str,
    dir: &std::path::Path,
) -> Started {
    let kafka = cfg(brokers, prefix, partitions, id);
    let log = KafkaLog::connect(kafka.clone()).await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let node = Node::new(
        NodeCfg {
            node_id: id.to_string(),
            peer_url: format!("http://127.0.0.1:{port}"),
            search: true,
            ..Default::default()
        },
        log,
        RocksLocal::open(dir, &RocksCfg::default()).unwrap(),
        Arc::new(Sender::new(Arc::new(NullRouter), false)),
        KafkaMembership::new(
            kafka,
            GroupCfg {
                group_id: prefix.to_string(),
                session_timeout: Duration::from_secs(6),
                instance_id: None,
            },
        ),
        Arc::new(HttpPeers::new(
            Duration::from_secs(10),
            Some("secret".into()),
        )),
    );
    let (stop_peer, stop_rx) = tokio::sync::watch::channel(false);
    peer::serve(
        &format!("127.0.0.1:{port}"),
        Arc::downgrade(&node),
        Some("secret".into()),
        stop_rx,
    )
    .await
    .unwrap();
    node.start().await.unwrap();
    Started { node, stop_peer }
}

async fn settle(nodes: &[&Arc<Node>], partitions: u32) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut all = BTreeSet::new();
        let mut total = 0;
        let mut everyone = true;
        for n in nodes {
            let s = n.serving();
            everyone &= !s.is_empty();
            total += s.len();
            all.extend(s);
        }
        if everyone && all.len() == partitions as usize && total == partitions as usize {
            return;
        }
        assert!(Instant::now() < deadline, "the group never settled");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_restarts_from_its_rocksdb_copy_and_the_log() {
    let Some(brokers) = brokers() else { return };
    let prefix = prefix("restart");
    let dir = tempfile::tempdir().unwrap();

    let a = start_node(&brokers, &prefix, 4, "a", dir.path()).await;
    settle(&[&a.node], 4).await;
    for i in 0..20 {
        ok(&a.node, "promise.create", create(&format!("r{i}"))).await;
    }
    ok(
        &a.node,
        "promise.settle",
        json!({ "id": "r3", "state": "resolved", "value": {} }),
    )
    .await;
    a.node.stop().await;
    let _ = a.stop_peer.send(true);
    drop(a);

    // A fresh process over the same directory.
    let b = start_node(&brokers, &prefix, 4, "a", dir.path()).await;
    settle(&[&b.node], 4).await;
    let got = ok(&b.node, "promise.get", json!({ "id": "r3" })).await;
    assert_eq!(got["promise"]["state"], "resolved");
    let page = ok(&b.node, "promise.search", json!({ "limit": 100 })).await;
    assert_eq!(page["promises"].as_array().unwrap().len(), 20);
    b.node.stop().await;
    let _ = b.stop_peer.send(true);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_share_the_partitions_and_forward_to_each_other() {
    let Some(brokers) = brokers() else { return };
    let prefix = prefix("pair");
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    let a = start_node(&brokers, &prefix, 6, "a", da.path()).await;
    settle(&[&a.node], 6).await;
    let b = start_node(&brokers, &prefix, 6, "b", db.path()).await;
    settle(&[&a.node, &b.node], 6).await;
    assert!(!a.node.serving().is_empty() && !b.node.serving().is_empty());

    // Written through one, read through the other: half of it forwarded.
    for i in 0..30 {
        ok(&a.node, "promise.create", create(&format!("p{i}:x"))).await;
    }
    for i in 0..30 {
        let got = ok(&b.node, "promise.get", json!({ "id": format!("p{i}:x") })).await;
        assert_eq!(got["promise"]["param"]["data"], "aGk=");
    }
    let page = ok(&b.node, "promise.search", json!({ "limit": 1000 })).await;
    assert_eq!(page["promises"].as_array().unwrap().len(), 30);

    // B leaves; A takes everything, state and all.
    b.node.stop().await;
    let _ = b.stop_peer.send(true);
    settle(&[&a.node], 6).await;
    for i in 0..30 {
        ok(
            &a.node,
            "promise.settle",
            json!({ "id": format!("p{i}:x"), "state": "resolved", "value": {} }),
        )
        .await;
    }
    let page = ok(
        &a.node,
        "promise.search",
        json!({ "state": "resolved", "limit": 1000 }),
    )
    .await;
    assert_eq!(page["promises"].as_array().unwrap().len(), 30);
    a.node.stop().await;
    let _ = a.stop_peer.send(true);
}
