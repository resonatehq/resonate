//! A Resonate server over Kafka, scaling across nodes by partition.
//!
//! Every promise and task operation is single-origin, so the origin picks a
//! partition, and one partition owner can answer any single operation. Nodes
//! share the partitions through a Kafka consumer group; a partition is taken
//! over by fencing its transactional id, replaying its log into a local
//! RocksDB copy, and serving; every round of decisions is one Kafka
//! transaction of per-promise records. The protocol itself is the blob
//! backend's kernel, reused as it is. See the README for the whole design.
//!
//! The internal graph, bottom up: [`keys`] routes and lays out keys;
//! [`record`] is the bytes of a record; [`log`] is the fenced, transactional
//! log (in process, or Kafka); [`local`] is the node's copy of its partitions
//! (in memory, or RocksDB); [`timers`] is a partition's deadline index;
//! [`partition`] takes a partition over and runs its rounds; [`membership`]
//! says which partitions to own; [`peer`] forwards to other nodes; [`search`]
//! gathers searches; [`node`] is the `ResonateServer` that ties them together;
//! and [`plugin`] reads the configuration.

pub mod keys;
pub mod local;
pub mod log;
pub mod membership;
pub mod node;
pub mod partition;
pub mod peer;
pub mod plugin;
pub mod record;
pub mod search;
pub mod timers;

pub use plugin::{Config, PLUGIN};
