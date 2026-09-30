//! Membership: which partitions this node should own, as a stream of assign
//! and revoke events.
//!
//! # Contract
//!
//! An implementation tells the node what the group decided, and nothing more:
//!
//! - [`Event::Assigned`] — take these partitions over.
//! - [`Event::Revoked`] — stop serving these, then signal `done`. The group
//!   waits for `done` before it hands them to anyone else, which is what makes
//!   an ordinary handoff clean. `lost: true` means the group already gave them
//!   away (this node's session expired): someone else may be serving them now,
//!   so there is nothing to finish, only to stop.
//!
//! Events are what make handoffs *clean*; they are not what makes them *safe*.
//! A node that crashed, froze or was partitioned never hears its revoke. Safety
//! is the log's fence: whoever takes a partition over fences it first, and a
//! writer that was fenced out can never commit again.
//!
//! # Dependencies
//!
//! None here. [`kafka`] is a consumer group; [`StaticMembership`] owns every
//! partition, for a single node; [`MemGroup`] is an in-process group, for
//! tests that run several nodes against one [`crate::log::mem::MemLog`].
//!
//! # Dependants
//!
//! The node, which turns events into takeovers and stops.

pub mod kafka;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use resonate_core::Unavailable;

/// What the group decided for this node.
#[derive(Debug)]
pub enum Event {
    Assigned(Vec<u32>),
    Revoked {
        partitions: Vec<u32>,
        lost: bool,
        done: oneshot::Sender<()>,
    },
}

/// A source of assign and revoke events for one node.
#[async_trait]
pub trait Membership: Send + Sync {
    /// Join, and deliver every decision to `events` from now on.
    async fn start(&self, events: mpsc::UnboundedSender<Event>) -> Result<(), Unavailable>;

    /// Leave. Everything assigned is revoked (and waited for) first.
    async fn stop(&self);

    /// Whether this node is meant to own every partition — a single node,
    /// which may then wait for all of them before it reports ready.
    fn owns_everything(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Static
// ---------------------------------------------------------------------------

/// Every partition, always: a single node.
pub struct StaticMembership {
    partitions: u32,
    events: Mutex<Option<mpsc::UnboundedSender<Event>>>,
}

impl StaticMembership {
    pub fn new(partitions: u32) -> Arc<Self> {
        Arc::new(Self {
            partitions,
            events: Mutex::new(None),
        })
    }
}

#[async_trait]
impl Membership for StaticMembership {
    async fn start(&self, events: mpsc::UnboundedSender<Event>) -> Result<(), Unavailable> {
        events
            .send(Event::Assigned((0..self.partitions).collect()))
            .map_err(|_| Unavailable::new("the node is not listening"))?;
        *self.events.lock().unwrap_or_else(|e| e.into_inner()) = Some(events);
        Ok(())
    }

    async fn stop(&self) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(events) = events {
            let (done, wait) = oneshot::channel();
            if events
                .send(Event::Revoked {
                    partitions: (0..self.partitions).collect(),
                    lost: false,
                    done,
                })
                .is_ok()
            {
                let _ = wait.await;
            }
        }
    }

    fn owns_everything(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// In-process group
// ---------------------------------------------------------------------------

struct Member {
    events: mpsc::UnboundedSender<Event>,
    owned: BTreeSet<u32>,
}

/// A consumer group in process: partitions spread round-robin over the
/// members, rebalanced cooperatively (every revoke finishes before any assign)
/// whenever a member joins or leaves.
pub struct MemGroup {
    partitions: u32,
    members: Mutex<BTreeMap<String, Member>>,
    /// Members expelled without being told, until they are.
    expelled: Mutex<BTreeMap<String, Member>>,
    /// One rebalance at a time.
    rebalancing: tokio::sync::Mutex<()>,
}

impl MemGroup {
    pub fn new(partitions: u32) -> Arc<Self> {
        Arc::new(Self {
            partitions,
            members: Mutex::new(BTreeMap::new()),
            expelled: Mutex::new(BTreeMap::new()),
            rebalancing: tokio::sync::Mutex::new(()),
        })
    }

    /// The membership of node `node` in this group.
    pub fn member(self: &Arc<Self>, node: &str) -> Arc<dyn Membership> {
        Arc::new(MemMember {
            group: Arc::clone(self),
            node: node.to_string(),
        })
    }

    /// Which partitions each member owns.
    pub fn assignment(&self) -> BTreeMap<String, BTreeSet<u32>> {
        self.lock()
            .iter()
            .map(|(n, m)| (n.clone(), m.owned.clone()))
            .collect()
    }

    /// Drop `node` from the group without telling it — a crash, or a session
    /// that expired while the node was frozen. Its partitions go to the rest,
    /// and it goes on believing it owns them until [`MemGroup::tell_lost`].
    pub async fn expel(&self, node: &str) {
        let member = self.lock().remove(node);
        if let Some(member) = member {
            self.expelled
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(node.to_string(), member);
        }
        self.rebalance().await;
    }

    /// Tell an expelled node what the group did — what its next poll would
    /// hear from Kafka: every partition it held, revoked as lost.
    pub async fn tell_lost(&self, node: &str) {
        let member = self
            .expelled
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(node);
        if let Some(member) = member {
            let (done, wait) = oneshot::channel();
            if member
                .events
                .send(Event::Revoked {
                    partitions: member.owned.into_iter().collect(),
                    lost: true,
                    done,
                })
                .is_ok()
            {
                let _ = wait.await;
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Member>> {
        self.members.lock().unwrap_or_else(|e| e.into_inner())
    }

    async fn rebalance(&self) {
        let _serial = self.rebalancing.lock().await;
        let target: BTreeMap<String, BTreeSet<u32>> = {
            let members = self.lock();
            let names: Vec<&String> = members.keys().collect();
            let mut target: BTreeMap<String, BTreeSet<u32>> =
                names.iter().map(|n| ((*n).clone(), BTreeSet::new())).collect();
            if !names.is_empty() {
                for p in 0..self.partitions {
                    let owner = names[p as usize % names.len()];
                    target.get_mut(owner).expect("a member").insert(p);
                }
            }
            target
        };

        // Revoke first, and wait: nothing is assigned while its old owner may
        // still be serving it.
        let revokes: Vec<(mpsc::UnboundedSender<Event>, Vec<u32>)> = {
            let members = self.lock();
            members
                .iter()
                .filter_map(|(name, m)| {
                    let gone: Vec<u32> = m.owned.difference(&target[name]).copied().collect();
                    (!gone.is_empty()).then(|| (m.events.clone(), gone))
                })
                .collect()
        };
        for (events, partitions) in revokes {
            let (done, wait) = oneshot::channel();
            if events
                .send(Event::Revoked {
                    partitions,
                    lost: false,
                    done,
                })
                .is_ok()
            {
                let _ = wait.await;
            }
        }

        let mut members = self.lock();
        for (name, m) in members.iter_mut() {
            let want = &target[name];
            let new: Vec<u32> = want.difference(&m.owned).copied().collect();
            m.owned = want.clone();
            if !new.is_empty() {
                let _ = m.events.send(Event::Assigned(new));
            }
        }
    }
}

struct MemMember {
    group: Arc<MemGroup>,
    node: String,
}

#[async_trait]
impl Membership for MemMember {
    async fn start(&self, events: mpsc::UnboundedSender<Event>) -> Result<(), Unavailable> {
        self.group.lock().insert(
            self.node.clone(),
            Member {
                events,
                owned: BTreeSet::new(),
            },
        );
        self.group.rebalance().await;
        Ok(())
    }

    async fn stop(&self) {
        let member = self.group.lock().remove(&self.node);
        if let Some(member) = member {
            if !member.owned.is_empty() {
                let (done, wait) = oneshot::channel();
                if member
                    .events
                    .send(Event::Revoked {
                        partitions: member.owned.into_iter().collect(),
                        lost: false,
                        done,
                    })
                    .is_ok()
                {
                    let _ = wait.await;
                }
            }
        }
        self.group.rebalance().await;
    }
}
