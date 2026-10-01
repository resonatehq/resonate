//! The log port: fenced, transactional writes and committed reads, per
//! partition.
//!
//! # Contract
//!
//! A partition is two logs of the same index — promise records and schedule
//! records — and whoever owns one owns both. Ownership is not decided here;
//! what is decided here is who may *write*:
//!
//! - [`Log::fence`] makes the caller the partition's only writer. Every writer
//!   handed out earlier for that partition is refused from then on
//!   ([`LogError::Fenced`]), whatever it believes about itself. On Kafka this is
//!   `init_transactions` on the partition's transactional id, which also
//!   finishes whatever transaction the previous writer left open.
//! - [`Writer::commit`] is all or nothing: every record lands, or none does, and
//!   a reader never sees part of one (`read_committed`).
//! - [`Log::reader`] reads committed records from a checkpoint up to the end of
//!   the log *as it is when the reader is created*. Called after `fence`, that
//!   end is everything any earlier writer ever committed.
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

/// Where to resume reading a partition: the next offset of each of its logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Checkpoint {
    pub promises: i64,
    pub schedules: i64,
}

impl Checkpoint {
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

    /// Whether this checkpoint is at or past `other` on both logs.
    pub fn covers(&self, other: &Checkpoint) -> bool {
        self.promises >= other.promises && self.schedules >= other.schedules
    }

    pub fn to_bytes(self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.promises.to_be_bytes());
        out[8..].copy_from_slice(&self.schedules.to_be_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 {
            return None;
        }
        Some(Self {
            promises: i64::from_be_bytes(bytes[..8].try_into().ok()?),
            schedules: i64::from_be_bytes(bytes[8..].try_into().ok()?),
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

/// The partitioned, transactional log.
#[async_trait]
pub trait Log: Send + Sync {
    /// How many partitions there are. Fixed for the life of a deployment: the
    /// partition of an origin is a function of it.
    fn partitions(&self) -> u32;

    /// Become `partition`'s only writer, refusing every earlier one.
    async fn fence(&self, partition: u32) -> Result<Arc<dyn Writer>, LogError>;

    /// The earliest offsets still readable on the partition's two logs.
    async fn start(&self, partition: u32) -> Result<Checkpoint, LogError>;

    /// Read committed records from `from` to the end of the log as it is now.
    async fn reader(&self, partition: u32, from: Checkpoint) -> Result<Box<dyn Reader>, LogError>;

    /// Whether the log answers at all — what `/ready` reports.
    async fn ready(&self) -> bool;
}

/// The one writer of one partition.
#[async_trait]
pub trait Writer: Send + Sync {
    /// Commit `records` as one transaction. `base` is the partition's
    /// checkpoint before this commit; the result is its checkpoint after it.
    async fn commit(&self, records: Vec<Record>, base: Checkpoint) -> Result<Checkpoint, LogError>;
}

/// A committed read of one partition, from a checkpoint to a fixed end.
#[async_trait]
pub trait Reader: Send {
    /// The next batch and the checkpoint after it, or `None` at the end.
    async fn next(&mut self) -> Result<Option<(Vec<Consumed>, Checkpoint)>, LogError>;
}
