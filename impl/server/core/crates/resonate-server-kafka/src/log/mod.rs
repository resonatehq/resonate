//! The log port: fenced writes and filtered reads, per partition.
//!
//! # Contract
//!
//! A partition is two logs of the same index — promise records and schedule
//! records — and whoever owns one owns both. Ownership is not decided here;
//! what is decided here is whose writes *count*. There are no transactions:
//! the broker takes every record it is sent, so fencing lives in the log
//! itself ([`epoch`]):
//!
//! - [`Log::fence`] appends a claim to each of the partition's logs. From then
//!   on the caller's records are the only ones any reader admits; a writer
//!   handed out earlier still lands records, and every reader reads past
//!   them.
//! - [`Writer::commit`] lands its records in order, and can stop after any
//!   prefix of them. It fails if they did not land exactly where this writer
//!   last left the log. The partition shell orders its records so that every
//!   prefix is a state the protocol allows, and repairs what a cut leaves
//!   ([`crate::partition`]).
//! - [`Writer::check`] asks, without writing, whether the log still ends where
//!   this writer left it — how an idle owner hears of a fenced writer's
//!   records.
//! - [`Log::reader`] reads the admitted records from a checkpoint up to the end
//!   of the log *as it is when the reader is created*. Called after `fence`,
//!   that end is everything any earlier owner ever wrote. Once read to the
//!   end, it names the keys whose newest record it refused
//!   ([`Reader::stale`]).
//!
//! The error taxonomy is the load-bearing part, as it is for the blob store:
//!
//! - [`LogError::Unavailable`] — the commit certainly did not land. Fail the
//!   callers; the partition carries on.
//! - [`LogError::Fenced`] — someone else owns the partition now. Stop serving
//!   it; nothing this writer does will land again.
//! - [`LogError::Uncertain`] — nobody knows whether the commit landed. The
//!   partition's local state may now disagree with the log, so it must not
//!   serve again until it has fenced anew and re-read the log.
//!
//! # Dependencies
//!
//! None: this module is the seam. [`mem`] implements it in process, [`kafka`]
//! over librdkafka.
//!
//! # Dependants
//!
//! The partition shell, which fences on takeover, replays through a reader,
//! and commits every round through its writer. Who owns a partition is not
//! the log's business: see [`crate::membership`] and [`crate::directory`].

pub mod epoch;
pub mod kafka;
pub mod mem;

use std::sync::Arc;

use async_trait::async_trait;

/// Which of a partition's two logs a record belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Topic {
    Promises,
    Schedules,
}

/// Where to resume reading a partition: the next offset of each of its logs,
/// and the epoch in force at that offset on each ([`epoch`]). `-1` is "no
/// claim seen yet".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub promises: i64,
    pub schedules: i64,
    pub epochs: [i64; 2],
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self::at(0, 0)
    }
}

impl Checkpoint {
    /// Offsets, with no epoch yet.
    pub fn at(promises: i64, schedules: i64) -> Self {
        Self {
            promises,
            schedules,
            epochs: [-1, -1],
        }
    }

    pub fn get(&self, topic: Topic) -> i64 {
        match topic {
            Topic::Promises => self.promises,
            Topic::Schedules => self.schedules,
        }
    }

    pub fn set(&mut self, topic: Topic, offset: i64) {
        match topic {
            Topic::Promises => self.promises = offset,
            Topic::Schedules => self.schedules = offset,
        }
    }

    pub fn epoch(&self, topic: Topic) -> i64 {
        self.epochs[topic as usize]
    }

    /// Whether this checkpoint is at or past `other` on both logs.
    pub fn covers(&self, other: &Checkpoint) -> bool {
        self.promises >= other.promises && self.schedules >= other.schedules
    }

    pub fn to_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&self.promises.to_be_bytes());
        out[8..16].copy_from_slice(&self.schedules.to_be_bytes());
        out[16..24].copy_from_slice(&self.epochs[0].to_be_bytes());
        out[24..].copy_from_slice(&self.epochs[1].to_be_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 32 {
            return None;
        }
        let at = |i: usize| -> Option<i64> {
            Some(i64::from_be_bytes(bytes[i..i + 8].try_into().ok()?))
        };
        Some(Self {
            promises: at(0)?,
            schedules: at(8)?,
            epochs: [at(16)?, at(24)?],
        })
    }
}

/// One record to write. `value: None` is a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub topic: Topic,
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// One committed record, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consumed {
    pub topic: Topic,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub offset: i64,
    /// The writer's epoch ([`epoch`]).
    pub epoch: Option<i64>,
    /// A claim, not data ([`epoch`]). Never handed past the epoch filter.
    pub claim: bool,
}

/// Why the log did not do what was asked. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogError {
    Unavailable(String),
    Fenced(String),
    Uncertain(String),
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::Unavailable(m) => write!(f, "log unavailable: {m}"),
            LogError::Fenced(m) => write!(f, "fenced: {m}"),
            LogError::Uncertain(m) => write!(f, "commit outcome unknown: {m}"),
        }
    }
}

impl std::error::Error for LogError {}

/// The partitioned log.
#[async_trait]
pub trait Log: Send + Sync {
    /// How many partitions there are. Fixed for the life of a deployment: the
    /// partition of an origin is a function of it.
    fn partitions(&self) -> u32;

    /// Become `partition`'s only writer: claim it ([`epoch`]).
    async fn fence(&self, partition: u32) -> Result<Arc<dyn Writer>, LogError>;

    /// The earliest offsets still readable on the partition's two logs.
    async fn start(&self, partition: u32) -> Result<Checkpoint, LogError>;

    /// Read admitted records from `from` to the end of the log as it is now.
    async fn reader(&self, partition: u32, from: Checkpoint) -> Result<Box<dyn Reader>, LogError>;

    /// Whether the log answers at all — what `/ready` reports.
    async fn ready(&self) -> bool;
}

/// The one writer of one partition.
#[async_trait]
pub trait Writer: Send + Sync {
    /// Land `records`, in order. `base` is the partition's checkpoint before
    /// this commit; the result is its checkpoint after it. An error may leave
    /// any prefix of them landed — except `Unavailable`, which landed none.
    async fn commit(&self, records: Vec<Record>, base: Checkpoint) -> Result<Checkpoint, LogError>;

    /// Whether the log still ends at `at`, where this writer left it. A
    /// fenced writer's records still land, after a newer owner's claim; an
    /// idle owner hears of them only by asking.
    async fn check(&self, at: Checkpoint) -> Result<(), LogError>;
}

/// A read of one partition, from a checkpoint to a fixed end.
#[async_trait]
pub trait Reader: Send {
    /// The next batch and the checkpoint after it, or `None` at the end.
    async fn next(&mut self) -> Result<Option<(Vec<Consumed>, Checkpoint)>, LogError>;

    /// Once read to the end: the keys whose newest record was refused as a
    /// fenced writer's ([`epoch`]). Compaction would keep that record and
    /// drop the value it hides, so the owner writes the value again. A raw
    /// reader, below the epoch filter, refuses nothing.
    fn stale(&self) -> Vec<(Topic, String)> {
        Vec::new()
    }
}
