// Several nodes, one log: the scaling and failover story, in process.
//
// Every node shares one `MemLog` (fencing and transactions as Kafka has them),
// one `MemGroup` (a consumer group: round-robin, cooperative rebalances) and
// one `LocalPeers` (forwarding without sockets, through the wire format).
// Each node has its own in-memory local store, as each process would have its
// own disk.
//
// Run:
//   cargo test -p resonate-server-kafka --test cluster

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use resonate_core::types::{RequestEnvelope, ResponseEnvelope, SUPPORTED_VERSIONS};
use resonate_core::{ResonateServer, Unavailable};
use resonate_server_blob::sender::{NullRouter, Sender};
use resonate_server_kafka::keys::partition_of;
use resonate_server_kafka::local::mem::MemLocal;
use resonate_server_kafka::log::mem::MemLog;
use resonate_server_kafka::membership::MemGroup;
use resonate_server_kafka::node::{Node, NodeCfg};
use resonate_server_kafka::peer::LocalPeers;
use serde_json::{json, Value};

const PARTITIONS: u32 = 8;
const FAR: i64 = 4_000_000_000_000;

struct Cluster {
    log: Arc<MemLog>,
    group: Arc<MemGroup>,
    peers: Arc<LocalPeers>,
    nodes: Vec<Arc<Node>>,
}

impl Cluster {
    async fn new(n: usize) -> Self {
        let log = MemLog::new(PARTITIONS);
        let group = MemGroup::new(PARTITIONS);
        let peers = LocalPeers::new();
        let mut nodes = Vec::new();
        for i in 0..n {
            let id = format!("node-{i}");
            let node = Node::new(
                NodeCfg {
                    node_id: id.clone(),
                    search: true,
                    ..Default::default()
                },
                Arc::clone(&log) as _,
                MemLocal::new(),
                Arc::new(Sender::new(Arc::new(NullRouter), false)),
                group.member(&id),
                Arc::clone(&group) as _,
                Arc::clone(&peers) as _,
            );
            peers.register(&node);
            node.start().await.unwrap();
            nodes.push(node);
        }
        let c = Self {
            log,
            group,
            peers,
            nodes,
        };
        c.settle(&(0..n).collect::<Vec<_>>()).await;
        c
    }

    /// Wait until the named nodes serve every partition between them, each
    /// exactly once.
    async fn settle(&self, live: &[usize]) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut all = BTreeSet::new();
            let mut total = 0;
            for &i in live {
                let s = self.nodes[i].serving();
                total += s.len();
                all.extend(s);
            }
            if all.len() == PARTITIONS as usize && total == PARTITIONS as usize {
                return;
            }
            assert!(Instant::now() < deadline, "the cluster never settled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn envelope(kind: &str, data: Value) -> RequestEnvelope {
    serde_json::from_value(json!({
        "kind": kind,
        "head": { "corrId": "c", "version": SUPPORTED_VERSIONS[0] },
        "data": data,
    }))
    .unwrap()
}

async fn send(node: &Arc<Node>, kind: &str, data: Value) -> Result<ResponseEnvelope, Unavailable> {
    node.process(&envelope(kind, data)).await
}

async fn ok(node: &Arc<Node>, kind: &str, data: Value) -> Value {
    let resp = send(node, kind, data).await.expect("an answer");
    assert_eq!(resp.head.status, 200, "{kind}: {}", resp.data);
    resp.data
}

fn create(id: &str) -> Value {
    json!({ "id": id, "timeoutAt": FAR, "param": { "data": "aGk=" }, "tags": {} })
}

/// An origin whose partition is `p`.
fn origin_in(p: u32, salt: &str) -> String {
    (0..)
        .map(|i| format!("{salt}-{i}"))
        .find(|o| partition_of(o, PARTITIONS) == p)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_partitions_are_spread_and_any_node_answers_for_any_origin() {
    let c = Cluster::new(3).await;
    for node in &c.nodes {
        assert!(!node.serving().is_empty(), "every node owns something");
    }

    // Written through every node, read back through every other.
    for i in 0..40 {
        let id = format!("wf-{i}:root");
        ok(&c.nodes[i % 3], "promise.create", create(&id)).await;
    }
    for i in 0..40 {
        let id = format!("wf-{i}:root");
        let got = ok(&c.nodes[(i + 1) % 3], "promise.get", json!({ "id": id })).await;
        assert_eq!(got["promise"]["id"], id);
        assert_eq!(got["promise"]["state"], "pending");
    }

    // A search gathers every owner's part.
    let page = ok(&c.nodes[2], "promise.search", json!({ "limit": 1000 })).await;
    assert_eq!(page["promises"].as_array().unwrap().len(), 40);
    let page = ok(&c.nodes[0], "promise.search", json!({ "limit": 15 })).await;
    assert_eq!(page["promises"].as_array().unwrap().len(), 15);
    let rest = ok(
        &c.nodes[1],
        "promise.search",
        json!({ "limit": 1000, "cursor": page["cursor"] }),
    )
    .await;
    assert_eq!(rest["promises"].as_array().unwrap().len(), 25);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_leaves_hands_its_partitions_over_with_their_state() {
    let c = Cluster::new(3).await;
    for i in 0..30 {
        ok(&c.nodes[0], "promise.create", create(&format!("o{i}"))).await;
    }
    let before = c.log.commits();

    c.nodes[2].stop().await;
    c.settle(&[0, 1]).await;
    assert!(c.nodes[2].serving().is_empty());

    for i in 0..30 {
        let got = ok(&c.nodes[1], "promise.get", json!({ "id": format!("o{i}") })).await;
        assert_eq!(got["promise"]["param"]["data"], "aGk=");
    }
    // Still writable, wherever it moved to.
    ok(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": "o7", "state": "resolved", "value": {} }),
    )
    .await;
    let got = ok(&c.nodes[1], "promise.get", json!({ "id": "o7" })).await;
    assert_eq!(got["promise"]["state"], "resolved");
    assert!(c.log.commits() > before);

    // And it can come back.
    c.nodes[2].start().await.unwrap();
    c.settle(&[0, 1, 2]).await;
    let got = ok(&c.nodes[2], "promise.get", json!({ "id": "o7" })).await;
    assert_eq!(got["promise"]["state"], "resolved");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zombie_cannot_commit_after_its_partitions_moved() {
    let c = Cluster::new(3).await;
    let zombie = Arc::clone(&c.nodes[2]);
    let held: Vec<u32> = zombie.serving().into_iter().collect();
    let p = held[0];
    let origin = origin_in(p, "z");
    ok(&c.nodes[0], "promise.create", create(&origin)).await;

    // The group gives the zombie's partitions away without telling it.
    c.group.expel("node-2").await;
    c.settle(&[0, 1]).await;
    assert!(
        zombie.serving().contains(&p),
        "the zombie still believes it owns {p}"
    );

    // It still takes a write for them — and cannot land it.
    let attempt = send(
        &zombie,
        "promise.settle",
        json!({ "id": origin, "state": "rejected", "value": {} }),
    )
    .await;
    assert!(
        attempt.is_err(),
        "a fenced writer answered as if it had committed: {attempt:?}"
    );

    // The rightful owner never saw it.
    let got = ok(&c.nodes[1], "promise.get", json!({ "id": origin })).await;
    assert_eq!(got["promise"]["state"], "pending");
    // And writes to it where it now lives.
    ok(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": origin, "state": "resolved", "value": {} }),
    )
    .await;

    // The zombie's next poll tells it; it lets go.
    c.group.tell_lost("node-2").await;
    assert!(zombie.serving().is_empty());
    let got = ok(&c.nodes[0], "promise.get", json!({ "id": origin })).await;
    assert_eq!(got["promise"]["state"], "resolved");
    let _ = &c.peers;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_restores_from_a_compacted_log() {
    let c = Cluster::new(2).await;
    let p = 3;
    let origin = origin_in(p, "compact");
    ok(&c.nodes[0], "promise.create", create(&origin)).await;
    for i in 0..10 {
        ok(
            &c.nodes[1],
            "promise.create",
            create(&format!("{origin}:child{i}")),
        )
        .await;
    }
    ok(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": format!("{origin}:child3"), "state": "resolved", "value": {} }),
    )
    .await;
    c.log.compact(p);

    // Everyone leaves but one, which must rebuild partition p from scratch
    // (its own local copy of p, if it ever had one, is in memory elsewhere).
    let owner = if c.nodes[0].serving().contains(&p) {
        0
    } else {
        1
    };
    let other = 1 - owner;
    c.nodes[owner].stop().await;
    c.settle(&[other]).await;

    let got = ok(
        &c.nodes[other],
        "promise.get",
        json!({ "id": format!("{origin}:child3") }),
    )
    .await;
    assert_eq!(got["promise"]["state"], "resolved");
    let page = ok(&c.nodes[other], "promise.search", json!({ "limit": 1000 })).await;
    assert_eq!(page["promises"].as_array().unwrap().len(), 11);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_commit_is_a_retry_and_changes_nothing() {
    use resonate_server_kafka::log::mem::Fault;
    use resonate_server_kafka::log::LogError;
    let c = Cluster::new(1).await;
    ok(&c.nodes[0], "promise.create", create("r")).await;

    c.log
        .fail_next(Fault::Refuse(LogError::Unavailable("injected".into())));
    let attempt = send(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": "r", "state": "resolved", "value": {} }),
    )
    .await;
    assert!(
        attempt.is_err(),
        "nothing landed, so nothing may be claimed"
    );
    let got = ok(&c.nodes[0], "promise.get", json!({ "id": "r" })).await;
    assert_eq!(got["promise"]["state"], "pending");

    // The partition carried on: the retry lands.
    ok(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": "r", "state": "resolved", "value": {} }),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_uncertain_commit_makes_the_partition_re_read_the_log() {
    use resonate_server_kafka::log::mem::Fault;
    let c = Cluster::new(1).await;
    ok(&c.nodes[0], "promise.create", create("u")).await;

    // It lands, and the node is told it may not have.
    c.log.fail_next(Fault::LandThenUncertain);
    let attempt = send(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": "u", "state": "resolved", "value": {} }),
    )
    .await;
    assert!(attempt.is_err());

    // The partition fences itself anew and replays; what landed is what it
    // then serves.
    c.settle(&[0]).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(resp) = send(&c.nodes[0], "promise.get", json!({ "id": "u" })).await {
            assert_eq!(resp.data["promise"]["state"], "resolved");
            break;
        }
        assert!(Instant::now() < deadline, "the partition never came back");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_local_copy_in_another_format_is_rebuilt_from_the_log() {
    use resonate_server_kafka::directory::NoDirectory;
    use resonate_server_kafka::keys::promise_key;
    use resonate_server_kafka::local::{LocalStore, LOCAL_FORMAT};
    use resonate_server_kafka::membership::StaticMembership;

    let log = MemLog::new(PARTITIONS);
    let local = MemLocal::new();
    let node = |id: &str| {
        Node::new(
            NodeCfg {
                node_id: id.into(),
                ..Default::default()
            },
            Arc::clone(&log) as _,
            Arc::clone(&local) as _,
            Arc::new(Sender::new(Arc::new(NullRouter), false)),
            StaticMembership::new(PARTITIONS),
            Arc::new(NoDirectory),
            LocalPeers::new(),
        )
    };

    let first = node("n");
    first.start().await.unwrap();
    ok(&first, "promise.create", create("fmt")).await;
    first.stop().await;

    // An older build's copy: another stamp, and bytes this build cannot read.
    let p = partition_of("fmt", PARTITIONS);
    let store = local.open(p).unwrap();
    assert_eq!(store.format().unwrap(), Some(LOCAL_FORMAT));
    let cp = store.checkpoint().unwrap().expect("applied");
    store
        .apply(
            vec![(promise_key("fmt"), Some(b"not postcard".to_vec()))],
            cp,
        )
        .unwrap();
    store.set_format(LOCAL_FORMAT + 1).unwrap();

    // The next takeover drops it and replays the log instead of reading it.
    let second = node("n");
    second.start().await.unwrap();
    let got = ok(&second, "promise.get", json!({ "id": "fmt" })).await;
    assert_eq!(got["promise"]["state"], "pending");
    assert_eq!(local.open(p).unwrap().format().unwrap(), Some(LOCAL_FORMAT));
    second.stop().await;
}
