//! The roster port: who the nodes are, where a request goes, and how it gets
//! there.

use async_trait::async_trait;

use super::types::{RequestEnvelope, ResponseEnvelope};
use super::Unavailable;

/// A node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Peer {
    /// Stable node name, from configuration. It survives restarts.
    pub name: String,
    /// Base URL where peers reach this node.
    pub addr: String,
}

/// Where a request for an id should be processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// This node owns the id: process it here.
    Me,
    /// Storage is shared: any node may process it, so process it here.
    Any,
    /// Another node owns the id: forward it there, one hop at most.
    Peer(Peer),
    /// The id is owned, but no owner is known yet: answer 503 so the
    /// client retries.
    Unknown,
}

/// A Resonate roster: this node, the others, where a request goes, and the
/// channel between them.
///
/// Built by the server plugin, next to the server it routes for: the two
/// share a configuration and often a connection, and pairing one backend's
/// server with another's roster means nothing. Each backend fills the view
/// from the storage it already has — a consumer group, a members table,
/// lease rows — so a roster adds no coordination service of its own. A
/// single node has a roster too: no peers, and every route is
/// [`Route::Any`].
///
/// **The roster decides who does the work, never what is correct.** The
/// view is eventually consistent: two nodes may both believe they own an
/// id, and a route may name a node that no longer does. That must cost at
/// most duplicated work or a 503, never wrong state. Correctness stays in
/// storage, where every backend already enforces it.
///
/// `me`, `peers` and `route` read a view the implementation keeps up to date
/// in the background, so they are synchronous: routing a request never waits
/// on the network. Starting and stopping that background work is the
/// server's [`init`](super::ResonateServer::init) and
/// [`stop`](super::ResonateServer::stop), since the two are one
/// implementation.
///
/// A request forwarded by a peer is processed only if its route here is
/// [`Route::Me`] or [`Route::Any`]; otherwise it is answered with a 503 and
/// never forwarded again.
#[async_trait]
pub trait ResonateRoster: Send + Sync {
    /// This node.
    fn me(&self) -> Peer;

    /// The other live nodes, not including this one.
    fn peers(&self) -> Vec<Peer>;

    /// Where a request for `id` — a promise id or a schedule id, as the
    /// request carries it — should be processed.
    fn route(&self, id: &str) -> Route;

    /// Hand `req` to `to` and return its answer.
    ///
    /// `Err(Unavailable)` when `to` could not be reached or did not answer;
    /// as with every port, the request may already have been applied.
    async fn forward(
        &self,
        to: &Peer,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable>;
}

/// The id a request is routed by: the promise or schedule it acts on.
///
/// What a roster's [`route`](ResonateRoster::route) is asked about. A
/// backend that partitions places each request by this same id, so the two
/// must agree; it is defined here, once, rather than by each caller.
///
/// - a promise or task operation: the promise it names. `task.create` names
///   its action's promise, and `task.heartbeat` its first task (a batch
///   shares one origin).
/// - `promise.register_callback`: the awaiter, whose callback it records.
///   `promise.register_listener`: the awaited.
/// - a schedule operation: the schedule.
///
/// `None` for a request that names no single id — a search, a debug
/// operation, an unknown kind — and for one too malformed to name one. The
/// server it is handed to answers those itself, with the error if there is
/// one.
pub fn routing_id(req: &RequestEnvelope) -> Option<&str> {
    let d = &req.data;
    let id = match req.kind.as_str() {
        "promise.get" | "promise.create" | "promise.settle" => &d["id"],
        "promise.register_callback" => &d["awaiter"],
        "promise.register_listener" => &d["awaited"],
        "task.get" | "task.acquire" | "task.release" | "task.fulfill" | "task.suspend"
        | "task.fence" | "task.halt" | "task.continue" => &d["id"],
        "task.create" => &d["action"]["data"]["id"],
        "task.heartbeat" => &d["tasks"][0]["id"],
        "schedule.get" | "schedule.create" | "schedule.delete" => &d["id"],
        _ => return None,
    };
    id.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn req(kind: &str, data: Value) -> RequestEnvelope {
        serde_json::from_value(json!({
            "kind": kind,
            "head": { "corrId": "c", "version": crate::types::SUPPORTED_VERSIONS[0] },
            "data": data,
        }))
        .unwrap()
    }

    #[test]
    fn each_kind_routes_by_the_id_it_acts_on() {
        let cases = [
            ("promise.get", json!({ "id": "a:1" }), Some("a:1")),
            ("promise.create", json!({ "id": "a:1" }), Some("a:1")),
            ("promise.settle", json!({ "id": "a:1" }), Some("a:1")),
            (
                "promise.register_callback",
                json!({ "awaited": "b:1", "awaiter": "a:1" }),
                Some("a:1"),
            ),
            (
                "promise.register_listener",
                json!({ "awaited": "b:1", "address": "poll://x" }),
                Some("b:1"),
            ),
            ("task.get", json!({ "id": "a:1" }), Some("a:1")),
            ("task.acquire", json!({ "id": "a:1" }), Some("a:1")),
            ("task.fence", json!({ "id": "a:1" }), Some("a:1")),
            (
                "task.create",
                json!({ "pid": "p", "action": { "data": { "id": "a:2" } } }),
                Some("a:2"),
            ),
            (
                "task.heartbeat",
                json!({ "pid": "p", "tasks": [{ "id": "a:3" }, { "id": "a:4" }] }),
                Some("a:3"),
            ),
            (
                "schedule.create",
                json!({ "id": "nightly" }),
                Some("nightly"),
            ),
            (
                "schedule.delete",
                json!({ "id": "nightly" }),
                Some("nightly"),
            ),
            ("promise.search", json!({ "limit": 10 }), None),
            ("debug.snap", json!({}), None),
            ("no.such.kind", json!({ "id": "a:1" }), None),
            ("promise.get", json!({}), None),
            ("promise.get", json!({ "id": 7 }), None),
            ("task.heartbeat", json!({ "tasks": [] }), None),
        ];
        for (kind, data, want) in cases {
            assert_eq!(routing_id(&req(kind, data.clone())), want, "{kind} {data}");
        }
    }
}
