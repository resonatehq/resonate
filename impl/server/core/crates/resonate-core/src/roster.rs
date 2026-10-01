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
