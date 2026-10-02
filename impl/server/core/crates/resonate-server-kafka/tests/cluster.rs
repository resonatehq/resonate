// Several nodes, one log: the scaling and failover story, in process.
//
// Every node shares one `MemLog` (claims, epochs and zombie writes as on Kafka),
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
use resonate_core::{ResonateRoster, ResonateServer, Route, Unavailable};
use resonate_server_blob::sender::{NullRouter, Sender};
use resonate_server_kafka::keys::partition_of;
use resonate_server_kafka::local::mem::MemLocal;
use resonate_server_kafka::log::mem::MemLog;
use resonate_server_kafka::membership::MemGroup;
use resonate_server_kafka::node::{Node, NodeCfg};
use resonate_server_kafka::partition::PartitionCfg;
use resonate_server_kafka::peer::LocalPeers;
use serde_json::{json, Value};

const PARTITIONS: u32 = 8;
const FAR: i64 = 4_000_000_000_000;

struct Cluster {
    log: Arc<MemLog>,
    group: Arc<MemGroup>,
    nodes: Vec<Arc<Node>>,
}

impl Cluster {
    async fn new(n: usize) -> Self {
        Self::on(MemLog::new(PARTITIONS), n).await
    }

    async fn on(log: Arc<MemLog>, n: usize) -> Self {
        Self::build(log, n, false).await
    }

    /// With `prune_shadowed` on.
    async fn pruning(n: usize) -> Self {
        Self::build(MemLog::new(PARTITIONS), n, true).await
    }

    async fn build(log: Arc<MemLog>, n: usize, prune: bool) -> Self {
        let group = MemGroup::new(PARTITIONS);
        let peers = LocalPeers::new();
        let mut nodes = Vec::new();
        for i in 0..n {
            let id = format!("node-{i}");
            let node = Node::new(
                NodeCfg {
                    node_id: id.clone(),
                    // What the in-memory group advertises for it.
                    peer_url: format!("local://{id}"),
                    search: true,
                    partition: PartitionCfg {
                        idle_check: Duration::from_millis(50),
                        prune,
                        ..Default::default()
                    },
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
        let c = Self { log, group, nodes };
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

/// A request to `node` as a client's reaches it in a binary: through the
/// routing layer, with the node as its own roster.
async fn send(node: &Arc<Node>, kind: &str, data: Value) -> Result<ResponseEnvelope, Unavailable> {
    resonate_plugin::Routed::new(Arc::clone(node) as _, Arc::clone(node) as _)
        .process(&envelope(kind, data))
        .await
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

/// Nothing stops a fenced writer's records landing. They
/// land after the new owner's claim, carrying an older epoch, so every reader
/// reads past them; the owner notices the log moved under it (its idle check),
/// takes the partition over again, and writes the value they hide once more,
/// so compaction keeps that and not them.
#[tokio::test(flavor = "multi_thread")]
async fn a_zombies_records_land_and_are_read_past() {
    let c = Cluster::new(3).await;
    let zombie = Arc::clone(&c.nodes[2]);
    let p = *zombie.serving().iter().next().unwrap();
    let origin = origin_in(p, "z");
    ok(&c.nodes[0], "promise.create", create(&origin)).await;

    c.group.expel("node-2").await;
    c.settle(&[0, 1]).await;
    let before = c.log.end(p);
    let attempt = send(
        &zombie,
        "promise.settle",
        json!({ "id": origin, "state": "rejected", "value": {} }),
    )
    .await;
    assert!(attempt.is_err(), "a fenced writer answered: {attempt:?}");
    assert!(
        c.log.end(p).promises > before.promises,
        "the zombie's record landed, as it would on a broker"
    );
    c.group.tell_lost("node-2").await;

    // The owner hears of it on its own and writes the hidden value again.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let records = c.log.promise_records(p);
        let last = records.iter().rev().find(|(k, _)| *k == origin).unwrap();
        let state =
            resonate_server_kafka::record::decode_promise(&origin, last.1.as_ref().unwrap())
                .unwrap()
                .0
                .state;
        if state == resonate_core::types::PromiseState::Pending {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the zombie's record is still the newest"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    c.settle(&[0, 1]).await;
    let got = ok(&c.nodes[1], "promise.get", json!({ "id": origin })).await;
    assert_eq!(got["promise"]["state"], "pending");

    // Compacted, and rebuilt from scratch elsewhere: still pending.
    c.log.compact(p);
    let owner = if c.nodes[0].serving().contains(&p) {
        0
    } else {
        1
    };
    c.nodes[owner].stop().await;
    c.settle(&[1 - owner]).await;
    let got = ok(&c.nodes[1 - owner], "promise.get", json!({ "id": origin })).await;
    assert_eq!(got["promise"]["state"], "pending");
    ok(
        &c.nodes[1 - owner],
        "promise.settle",
        json!({ "id": origin, "state": "resolved", "value": {} }),
    )
    .await;
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

#[tokio::test(flavor = "multi_thread")]
async fn the_roster_names_the_owner_and_forwards_to_it() {
    let c = Cluster::new(3).await;

    for p in 0..PARTITIONS {
        let origin = origin_in(p, "roster");
        let owner = c
            .nodes
            .iter()
            .find(|n| n.serving().contains(&p))
            .expect("every partition is served");

        // A promise of the origin, and the origin itself as a schedule id,
        // both route to the partition's owner.
        for id in [format!("{origin}:1.1"), origin.clone()] {
            for node in &c.nodes {
                let want = if Arc::ptr_eq(node, owner) {
                    Route::Me
                } else {
                    Route::Peer(owner.me())
                };
                assert_eq!(ResonateRoster::route(node.as_ref(), &id), want, "{id}");
            }
        }

        // Forwarding through the roster reaches the owner.
        let other = c.nodes.iter().find(|n| !Arc::ptr_eq(n, owner)).unwrap();
        let id = format!("{origin}:1");
        let resp = ResonateRoster::forward(
            other.as_ref(),
            &owner.me(),
            &envelope("promise.create", create(&id)),
        )
        .await
        .expect("an answer");
        assert_eq!(resp.head.status, 200, "{}", resp.data);
        let got = ok(owner, "promise.get", json!({ "id": id })).await;
        assert_eq!(got["promise"]["id"], id);
    }

    // Every node lists the other two, not itself.
    for node in &c.nodes {
        let mut peers: Vec<String> = node.peers().into_iter().map(|p| p.name).collect();
        peers.sort();
        let mut want: Vec<String> = c
            .nodes
            .iter()
            .filter(|n| !Arc::ptr_eq(n, node))
            .map(|n| n.id().to_string())
            .collect();
        want.sort();
        assert_eq!(peers, want);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_serves_what_it_owns_and_forwards_nothing_itself() {
    let c = Cluster::new(3).await;
    let p = 0;
    let id = format!("{}:1", origin_in(p, "hop"));
    let owner = c.nodes.iter().find(|n| n.serving().contains(&p)).unwrap();
    let other = c.nodes.iter().find(|n| !Arc::ptr_eq(n, owner)).unwrap();
    let commits = c.log.commits();

    // Handed straight to a node that does not serve the partition — as a
    // peer's forward would hand it — the request is refused, not passed on.
    let err = other
        .process(&envelope("promise.create", create(&id)))
        .await
        .expect_err("not served here");
    assert!(
        err.message.contains(&format!("partition {p}")),
        "{}",
        err.message
    );
    assert_eq!(c.log.commits(), commits, "nothing was written anywhere");

    // The owner serves it directly, and through the routing layer any node
    // reaches it.
    let resp = owner
        .process(&envelope("promise.create", create(&id)))
        .await
        .expect("served by its owner");
    assert_eq!(resp.head.status, 200, "{}", resp.data);
    let got = ok(other, "promise.get", json!({ "id": id })).await;
    assert_eq!(got["promise"]["state"], "pending");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_settled_promise_prunes_what_it_shadows_and_the_log_agrees() {
    let c = Cluster::pruning(2).await;
    let p = 5;
    let o = origin_in(p, "prune");
    let id = |lineage: &str| format!("{o}:{lineage}");
    let status = |node: usize, id: String| {
        let node = Arc::clone(&c.nodes[node]);
        async move {
            send(&node, "promise.get", json!({ "id": id }))
                .await
                .expect("an answer")
                .head
                .status
        }
    };
    let settle = |node: usize, id: String| {
        let node = Arc::clone(&c.nodes[node]);
        async move {
            ok(
                &node,
                "promise.settle",
                json!({ "id": id, "state": "resolved", "value": {} }),
            )
            .await
        }
    };

    ok(&c.nodes[0], "promise.create", create(&o)).await;
    for lineage in ["1", "1.1", "1.1.1", "1.1.2", "1.2", "1.2.1"] {
        ok(&c.nodes[1], "promise.create", create(&id(lineage))).await;
    }

    // Children settle first: nothing above them is settled yet, so they stay.
    settle(0, id("1.1.1")).await;
    settle(1, id("1.1.2")).await;
    assert_eq!(status(0, id("1.1.1")).await, 200);
    assert_eq!(status(1, id("1.1.2")).await, 200);

    // 1.1 settles: a replay of 1 reads it and never asks for its children.
    settle(0, id("1.1")).await;
    assert_eq!(status(1, id("1.1.1")).await, 404);
    assert_eq!(status(0, id("1.1.2")).await, 404);
    let kept = ok(&c.nodes[1], "promise.get", json!({ "id": id("1.1") })).await;
    assert_eq!(kept["promise"]["state"], "resolved");

    // A child still pending when its parent settles keeps working ...
    settle(1, id("1.2")).await;
    let pending = ok(&c.nodes[0], "promise.get", json!({ "id": id("1.2.1") })).await;
    assert_eq!(pending["promise"]["state"], "pending");
    // ... is answered when it settles, and is gone after.
    let done = settle(0, id("1.2.1")).await;
    assert_eq!(done["promise"]["state"], "resolved");
    assert_eq!(status(1, id("1.2.1")).await, 404);

    // A parent that timed out does not shadow a grandchild whose own parent
    // is still running: a replay of 2.1 still reads 2.1.1.
    for lineage in ["2", "2.1", "2.1.1"] {
        ok(&c.nodes[0], "promise.create", create(&id(lineage))).await;
    }
    settle(1, id("2.1.1")).await;
    ok(
        &c.nodes[0],
        "promise.settle",
        json!({ "id": id("2"), "state": "rejected", "value": {} }),
    )
    .await;
    assert_eq!(status(1, id("2.1.1")).await, 200, "2.1 may still replay");
    settle(0, id("2.1")).await;
    assert_eq!(status(1, id("2.1.1")).await, 404);
    assert_eq!(
        status(0, id("2.1")).await,
        404,
        "under a settled 2, nothing pending"
    );

    // The deletions are tombstones in the log: compacted, and rebuilt from
    // scratch on the one node left, the same promises remain.
    c.log.compact(p);
    let owner = usize::from(!c.nodes[0].serving().contains(&p));
    let other = 1 - owner;
    c.nodes[owner].stop().await;
    c.settle(&[other]).await;
    for (lineage, want) in [
        ("1", 200),
        ("1.1", 200),
        ("1.1.1", 404),
        ("1.1.2", 404),
        ("1.2", 200),
        ("1.2.1", 404),
        ("2", 200),
        ("2.1", 404),
        ("2.1.1", 404),
    ] {
        assert_eq!(status(other, id(lineage)).await, want, "{lineage}");
    }
    let page = ok(&c.nodes[other], "promise.search", json!({ "limit": 1000 })).await;
    let mut left: Vec<String> = page["promises"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    left.sort();
    assert_eq!(
        left,
        vec![o.clone(), id("1"), id("1.1"), id("1.2"), id("2")]
    );

    // Root settles: 1 settles first, then the root takes everything under it.
    settle(other, id("1")).await;
    assert_eq!(status(other, id("1.1")).await, 404);
    settle(other, o.clone()).await;
    let page = ok(&c.nodes[other], "promise.search", json!({ "limit": 1000 })).await;
    let left: Vec<&str> = page["promises"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(left, vec![o.as_str()], "a finished workflow is its root");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cut_prune_is_finished_by_the_next_decision() {
    use resonate_server_kafka::log::mem::Fault;

    // The settle of o:1.1 commits its settlement, then five tombstones (its
    // four children, and itself: its parent is settled). Cut it after each
    // record in turn; whatever landed, the client's retry ends in one place.
    for k in 1..=5 {
        let c = Cluster::pruning(1).await;
        let o = origin_in(2, &format!("cut{k}"));
        let id = |lineage: &str| format!("{o}:{lineage}");
        let settle = |pid: String| json!({ "id": pid, "state": "resolved", "value": {} });
        ok(&c.nodes[0], "promise.create", create(&o)).await;
        for lineage in ["1", "1.1", "1.1.1", "1.1.2", "1.1.3", "1.1.4"] {
            ok(&c.nodes[0], "promise.create", create(&id(lineage))).await;
        }
        for lineage in ["1.1.1", "1.1.2", "1.1.3", "1.1.4", "1"] {
            ok(&c.nodes[0], "promise.settle", settle(id(lineage))).await;
        }
        // With 1.1 pending under a settled 1, nothing is pruned yet.
        for lineage in ["1.1.1", "1.1.4"] {
            let got = ok(&c.nodes[0], "promise.get", json!({ "id": id(lineage) })).await;
            assert_eq!(got["promise"]["state"], "resolved");
        }

        c.log.fail_next(Fault::Cut(k));
        assert!(
            send(&c.nodes[0], "promise.settle", settle(id("1.1")))
                .await
                .is_err(),
            "cut at {k}: an uncertain commit is no answer"
        );
        // The client retries until answered; the partition re-reads the log.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match send(&c.nodes[0], "promise.settle", settle(id("1.1"))).await {
                Ok(resp) if resp.head.status == 200 || resp.head.status == 404 => break,
                other => {
                    assert!(Instant::now() < deadline, "cut at {k}: {other:?}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }

        for lineage in ["1.1", "1.1.1", "1.1.2", "1.1.3", "1.1.4"] {
            let resp = send(&c.nodes[0], "promise.get", json!({ "id": id(lineage) }))
                .await
                .expect("an answer");
            assert_eq!(resp.head.status, 404, "cut at {k}: {lineage} is pruned");
        }
        let kept = ok(&c.nodes[0], "promise.get", json!({ "id": id("1") })).await;
        assert_eq!(kept["promise"]["state"], "resolved", "cut at {k}");
        c.nodes[0].stop().await;
    }
}
