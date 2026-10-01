//! The owner directory: which node a partition's requests should go to.
//!
//! # Contract
//!
//! The directory is for **routing only**, never for correctness. Writes are
//! kept safe by the log's fence, so a directory that is briefly wrong costs a
//! request a 503 — the owner it names refuses what it does not serve, and a
//! forwarded request is never forwarded again — and the client's retry covers
//! it, as it covers a rebalance.
//!
//! The source of truth is the group itself: whoever the consumer group
//! assigns a partition to is its owner. Nodes find each other through the
//! group, never through a list of their own:
//!
//! - [`kafka`] asks the group coordinator (`DescribeConsumerGroups`) every few
//!   seconds, and at once after a forward fails. Each member's `client.id`
//!   carries its node id and peer URL ([`encode_client_id`]).
//! - [`crate::membership::MemGroup`] answers from its own assignment, for
//!   tests that run several nodes in one process.
//! - [`NoDirectory`] knows nobody: a single node, which owns everything.
//!
//! # Dependants
//!
//! The node, which forwards to whoever [`Directory::owner`] names and calls
//! [`Directory::stale`] when that fails.

pub mod kafka;

use serde::{Deserialize, Serialize};

/// Who serves a partition, and where to reach them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Owner {
    /// The owning node's id.
    pub node: String,
    /// Where the owning node takes forwarded requests.
    pub peer_url: String,
}

/// Where a partition's requests should go.
pub trait Directory: Send + Sync {
    /// The node the group assigns `partition` to, as last heard.
    fn owner(&self, partition: u32) -> Option<Owner>;

    /// A forward to what [`Directory::owner`] named just failed: look again
    /// soon rather than at the next scheduled refresh.
    fn stale(&self) {}
}

/// A directory for a node with no peers.
pub struct NoDirectory;

impl Directory for NoDirectory {
    fn owner(&self, _partition: u32) -> Option<Owner> {
        None
    }
}

const CLIENT_ID_PREFIX: &str = "resonate";

/// The group consumer's `client.id`: `resonate/<node>/<peer url>`. The one
/// field of a group member the coordinator reports back verbatim, so it is
/// how a node tells the others where to find it.
pub fn encode_client_id(node: &str, peer_url: &str) -> String {
    format!("{CLIENT_ID_PREFIX}/{node}/{peer_url}")
}

/// Read a member's `client.id` back into an owner. `None` for a client that
/// is not a node of this backend.
pub fn decode_client_id(client_id: &str) -> Option<Owner> {
    let mut parts = client_id.splitn(3, '/');
    if parts.next()? != CLIENT_ID_PREFIX {
        return None;
    }
    let node = parts.next()?;
    let peer_url = parts.next()?;
    if node.is_empty() {
        return None;
    }
    Some(Owner {
        node: node.to_string(),
        peer_url: peer_url.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_id_carries_the_node_and_its_url() {
        let id = encode_client_id("node-a", "http://10.0.0.5:8002/x");
        assert_eq!(
            decode_client_id(&id),
            Some(Owner {
                node: "node-a".into(),
                peer_url: "http://10.0.0.5:8002/x".into()
            })
        );
        // No peer URL configured still names the node.
        assert_eq!(
            decode_client_id(&encode_client_id("b", "")).map(|o| o.node),
            Some("b".to_string())
        );
    }

    #[test]
    fn a_foreign_client_id_is_nobody() {
        assert_eq!(decode_client_id("rdkafka"), None);
        assert_eq!(decode_client_id("other/a/b"), None);
        assert_eq!(decode_client_id("resonate//http://x"), None);
    }
}
