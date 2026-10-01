//! Fencing without transactions: claims, epochs, and the filter that applies
//! them.
//!
//! # Contract
//!
//! Without transactions the broker refuses nobody: a writer that lost its
//! partition still lands records. So ownership is decided in the log itself,
//! and every reader decides it the same way:
//!
//! - **A claim** is a record a new owner appends to each of the partition's
//!   two logs, carrying in its epoch header the offset it expects to land at —
//!   the log's end as the claimer last saw it. It is **valid iff it landed
//!   exactly there**: two claimers racing for one end cannot both win, and the
//!   loser learns it from the offset its claim got. A valid claim's offset is
//!   the new **epoch** of that log.
//! - **Data** carries its writer's epoch. A reader admits a record iff its
//!   epoch is the one in force where it lies, so a fenced writer's records —
//!   landing after a newer claim — are read past. A record with no epoch at
//!   all predates the first claim (a log first written with transactions) and
//!   is admitted until one is seen.
//! - **The writer checks every offset.** Its records must land exactly where
//!   it last left the log. If anything else landed in between, a claim or a
//!   zombie's record, the writer stops: its records may lie past a claim, or
//!   be interleaved with ones nobody admits. Re-reading is the one way back.
//!
//! Claims are keyed by their epoch, so compaction never removes one, and a
//! reader from the start of a compacted log sees every epoch boundary.
//!
//! # Stale records
//!
//! A refused record is still the newest record of its key, and compaction
//! keeps the newest: left alone, it would erase the admitted value it hides.
//! So the filter reports every key whose newest record it refused
//! ([`Reader::stale`]), and the owner writes the admitted value (or a
//! tombstone) again before serving. The topics' `min.compaction.lag.ms` must
//! outlast the time a fenced writer's records can take to land, plus the
//! owner's idle check ([`crate::log::Writer::check`]).
//!
//! # Dependants
//!
//! [`super::mem`] in its plain mode and [`super::kafka`] without
//! transactions wrap their raw readers in [`Filtered`].

use std::collections::BTreeSet;

use async_trait::async_trait;

use super::{Checkpoint, Consumed, LogError, Reader, Topic};

/// The record header that carries a writer's epoch, as 8 bytes big-endian.
pub const EPOCH_HEADER: &str = "resonate-epoch";

/// The record header that marks a claim.
pub const CLAIM_HEADER: &str = "resonate-claim";

/// A claim's key: unique per attempt, so compaction keeps every one.
pub fn claim_key(epoch: i64) -> String {
    format!("\u{0}claim/{epoch:020}")
}

pub fn encode_epoch(epoch: i64) -> [u8; 8] {
    epoch.to_be_bytes()
}

pub fn decode_epoch(bytes: &[u8]) -> Option<i64> {
    Some(i64::from_be_bytes(bytes.try_into().ok()?))
}

/// The epochs in force, and what was refused.
#[derive(Debug, Clone)]
pub struct Filter {
    epochs: [i64; 2],
    stale: BTreeSet<(Topic, String)>,
}

impl Filter {
    /// Start where `from` left off.
    pub fn new(from: &Checkpoint) -> Self {
        Self {
            epochs: from.epochs,
            stale: BTreeSet::new(),
        }
    }

    /// Whether `c` is data to apply. Claims are never data; a valid one moves
    /// the epoch.
    pub fn admit(&mut self, c: &Consumed) -> bool {
        let slot = &mut self.epochs[c.topic as usize];
        if c.claim {
            if c.epoch == Some(c.offset) && c.offset > *slot {
                *slot = c.offset;
            }
            return false;
        }
        let admitted = match c.epoch {
            Some(e) => e == *slot,
            None => *slot < 0,
        };
        let key = (c.topic, c.key.clone());
        if admitted {
            self.stale.remove(&key);
        } else {
            self.stale.insert(key);
        }
        admitted
    }

    pub fn epochs(&self) -> [i64; 2] {
        self.epochs
    }

    pub fn stale(&self) -> Vec<(Topic, String)> {
        self.stale.iter().cloned().collect()
    }
}

/// A raw reader, filtered: hands on only admitted data, and checkpoints the
/// epochs with the offsets.
pub struct Filtered {
    inner: Box<dyn Reader>,
    filter: Filter,
}

impl Filtered {
    pub fn new(inner: Box<dyn Reader>, from: &Checkpoint) -> Self {
        Self {
            inner,
            filter: Filter::new(from),
        }
    }
}

#[async_trait]
impl Reader for Filtered {
    async fn next(&mut self) -> Result<Option<(Vec<Consumed>, Checkpoint)>, LogError> {
        let Some((batch, mut after)) = self.inner.next().await? else {
            return Ok(None);
        };
        let batch = batch.into_iter().filter(|c| self.filter.admit(c)).collect();
        after.epochs = self.filter.epochs();
        Ok(Some((batch, after)))
    }

    fn stale(&self) -> Vec<(Topic, String)> {
        self.filter.stale()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(key: &str, offset: i64, epoch: Option<i64>) -> Consumed {
        Consumed {
            topic: Topic::Promises,
            key: key.into(),
            value: Some(vec![1]),
            offset,
            epoch,
            claim: false,
        }
    }

    fn claim(offset: i64, epoch: i64) -> Consumed {
        Consumed {
            topic: Topic::Promises,
            key: claim_key(epoch),
            value: None,
            offset,
            epoch: Some(epoch),
            claim: true,
        }
    }

    #[test]
    fn a_claim_is_valid_only_where_it_meant_to_land() {
        let mut f = Filter::new(&Checkpoint::default());
        assert!(!f.admit(&claim(0, 0)));
        assert_eq!(f.epochs()[0], 0);
        // Meant for 1, landed at 2: someone got there first.
        f.admit(&claim(2, 1));
        assert_eq!(f.epochs()[0], 0);
        f.admit(&claim(3, 3));
        assert_eq!(f.epochs()[0], 3);
    }

    #[test]
    fn a_fenced_writers_records_are_refused_and_reported() {
        let mut f = Filter::new(&Checkpoint::default());
        f.admit(&claim(0, 0));
        assert!(f.admit(&data("a", 1, Some(0))));
        f.admit(&claim(2, 2));
        // The old owner, still writing.
        assert!(!f.admit(&data("a", 3, Some(0))));
        assert!(!f.admit(&data("b", 4, Some(0))));
        assert!(f.admit(&data("b", 5, Some(2))));
        // a's newest record was refused; b's was written again since.
        assert_eq!(f.stale(), vec![(Topic::Promises, "a".to_string())]);
    }

    #[test]
    fn records_without_an_epoch_count_only_before_the_first_claim() {
        let mut f = Filter::new(&Checkpoint::default());
        assert!(f.admit(&data("a", 0, None)));
        f.admit(&claim(1, 1));
        assert!(!f.admit(&data("a", 2, None)));
    }

    #[test]
    fn the_epoch_resumes_from_the_checkpoint() {
        let mut cp = Checkpoint::at(10, 0);
        cp.epochs = [7, -1];
        let mut f = Filter::new(&cp);
        assert!(f.admit(&data("a", 10, Some(7))));
        assert!(!f.admit(&data("a", 11, Some(5))));
    }
}
