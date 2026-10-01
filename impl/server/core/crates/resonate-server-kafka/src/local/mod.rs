//! The local store port: a disposable, rebuildable copy of the partitions a
//! node owns.
//!
//! # Contract
//!
//! Kafka is the durable copy; this is a cache of it that survives a restart.
//! Everything written here was committed to the log first, so the store can be
//! behind the log but never ahead of it, and it can be thrown away at any time
//! at the cost of a longer restore.
//!
//! - One [`PartitionStore`] per partition, holding that partition's promise
//!   and schedule records under the key layout in [`crate::keys`], plus the
//!   [`Checkpoint`] it has applied up to.
//! - [`PartitionStore::apply`] writes a batch and its checkpoint atomically.
//!   After a crash the store holds some prefix of the batches applied — the
//!   checkpoint is never ahead of the data it names. Data ahead of the
//!   checkpoint is harmless: every record is a whole object version, so
//!   replaying records the store already has converges to the same state.
//!   That is what lets the RocksDB store run without its write-ahead log.
//! - [`LocalStore::drop_partition`] discards a partition's copy entirely.
//!
//! # Dependencies
//!
//! None: [`mem`] keeps it in a map, [`rocks`] in a RocksDB column family per
//! partition.
//!
//! # Dependants
//!
//! The partition shell: restore applies replayed batches, every commit round
//! applies its records, and every load, search and snapshot reads here.

pub mod mem;
pub mod rocks;

use std::sync::Arc;

use crate::log::Checkpoint;

/// The encoding this build writes into the local store ([`crate::record::local`]).
///
/// Stamped into every partition's copy. A copy stamped otherwise — or not at
/// all, from before stamps — was written by another build, and is dropped
/// and rebuilt from the log rather than read. That is what lets the local
/// encoding change between any two releases with no migration: bump this.
///
/// 1: postcard promises. 2: checkpoints carry the logs' epochs.
pub const LOCAL_FORMAT: u32 = 2;

/// One write to apply: a key and its new value, or `None` to delete it.
pub type Op = (Vec<u8>, Option<Vec<u8>>);

/// One key and its value, read back.
pub type Row = (Vec<u8>, Vec<u8>);

/// Every partition's local copy on this node.
pub trait LocalStore: Send + Sync {
    /// The partition's store, created empty if it does not exist.
    fn open(&self, partition: u32) -> Result<Arc<dyn PartitionStore>, String>;

    /// Discard the partition's copy. Dropping what does not exist succeeds.
    fn drop_partition(&self, partition: u32) -> Result<(), String>;

    /// The partitions a copy exists for.
    fn partitions(&self) -> Result<Vec<u32>, String>;
}

/// One partition's local copy.
pub trait PartitionStore: Send + Sync {
    /// The checkpoint applied so far, or `None` for a copy never written.
    fn checkpoint(&self) -> Result<Option<Checkpoint>, String>;

    /// The local format the copy is stamped with, or `None` if it never was.
    fn format(&self) -> Result<Option<u32>, String>;

    /// Stamp the copy. Written before anything it describes, so a copy whose
    /// data survived a crash has its stamp too.
    fn set_format(&self, format: u32) -> Result<(), String>;

    /// Apply `ops` and record `checkpoint`, atomically.
    fn apply(&self, ops: Vec<Op>, checkpoint: Checkpoint) -> Result<(), String>;

    /// One key's value.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;

    /// Every key under `prefix`, ascending, with its value.
    fn scan(&self, prefix: &[u8]) -> Result<Vec<Row>, String>;

    /// Make everything applied so far durable locally. Called on revoke and
    /// stop, so a clean shutdown restarts from where it left off.
    fn flush(&self) -> Result<(), String>;
}
