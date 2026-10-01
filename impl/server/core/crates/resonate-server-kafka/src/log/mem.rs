//! The log in process: Kafka without a broker, written as [`super::kafka`]
//! writes it — claims, epochs and offset checks ([`super::epoch`]).
//!
//! What it models, because the shell depends on it:
//!
//! - **Claims.** [`Log::fence`] appends a claim to each of a partition's logs,
//!   where it ends. Several in-process nodes sharing one `MemLog` fence each
//!   other exactly as processes sharing a cluster do.
//! - **Zombie writes.** A writer lands its records wherever the log ends and
//!   only then finds out whether that was where it left it. A fenced writer's
//!   records land too — after the claim that fenced it — so a test sees
//!   exactly the records a real broker would take from a zombie.
//! - **Compaction.** [`MemLog::compact`] keeps the newest record per key, which
//!   is what lets a test restore from a compacted log.
//! - **Faults.** [`MemLog::fail_next`] makes the next commit fail with the error
//!   of your choice, land and still report `Uncertain` — the case that forces
//!   a partition to re-read — or land only a prefix of its records
//!   ([`Fault::Cut`], [`Fault::CutWithin`]).
//!
//! # Dependants
//!
//! The differential suite and every test that runs nodes in process.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{epoch, Checkpoint, Consumed, Log, LogError, Reader, Record, Topic, Writer};

#[derive(Debug, Clone)]
struct Entry {
    offset: i64,
    key: String,
    value: Option<Vec<u8>>,
    epoch: Option<i64>,
    claim: bool,
}

#[derive(Default)]
struct PartitionLog {
    promises: Vec<Entry>,
    schedules: Vec<Entry>,
    next_promises: i64,
    next_schedules: i64,
    /// The newest claim on each log.
    claims: [i64; 2],
}

impl PartitionLog {
    fn entries(&self, topic: Topic) -> &Vec<Entry> {
        match topic {
            Topic::Promises => &self.promises,
            Topic::Schedules => &self.schedules,
        }
    }

    fn push(
        &mut self,
        topic: Topic,
        key: String,
        value: Option<Vec<u8>>,
        epoch: Option<i64>,
        claim: bool,
    ) -> i64 {
        let (log, next) = match topic {
            Topic::Promises => (&mut self.promises, &mut self.next_promises),
            Topic::Schedules => (&mut self.schedules, &mut self.next_schedules),
        };
        let offset = *next;
        log.push(Entry {
            offset,
            key,
            value,
            epoch,
            claim,
        });
        *next += 1;
        offset
    }

    /// The newest claim on `topic`.
    fn claimed(&self, topic: Topic) -> i64 {
        self.claims[topic as usize]
    }

    fn end(&self) -> Checkpoint {
        Checkpoint::at(self.next_promises, self.next_schedules)
    }
}

/// A fault to inject into the next commit.
#[derive(Debug, Clone)]
pub enum Fault {
    /// Refuse the commit with this error; nothing lands.
    Refuse(LogError),
    /// Land the commit, then report `Uncertain`.
    LandThenUncertain,
    /// Land the commit's first *k* records, then report `Uncertain`.
    Cut(usize),
    /// Land a prefix of the commit chosen from its own length — `seed`
    /// modulo it, so never the whole of it — then report `Uncertain`. Hits
    /// the middle of a commit of any size, which a fixed [`Fault::Cut`] only
    /// does by luck.
    CutWithin(u64),
}

#[derive(Default)]
struct Inner {
    partitions: Vec<PartitionLog>,
    fault: Option<Fault>,
    commits: u64,
    fences: u64,
    /// The last [`Fault::Cut`] that fired: records landed, records asked for.
    cut: Option<(usize, usize)>,
}

/// An in-process log. Cheap to clone a handle to: share one between nodes.
pub struct MemLog {
    n: u32,
    inner: Arc<Mutex<Inner>>,
}

impl MemLog {
    pub fn new(partitions: u32) -> Arc<Self> {
        let partitions = partitions.max(1);
        Arc::new(Self {
            n: partitions,
            inner: Arc::new(Mutex::new(Inner {
                partitions: (0..partitions).map(|_| PartitionLog::default()).collect(),
                ..Default::default()
            })),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        lock(&self.inner)
    }

    /// Make the next commit, on any partition, fail this way.
    pub fn fail_next(&self, fault: Fault) {
        self.lock().fault = Some(fault);
    }

    /// Disarm a fault that has not fired. Whether one was armed.
    pub fn disarm(&self) -> bool {
        self.lock().fault.take().is_some()
    }

    /// The last cut that fired, as (records landed, records in the commit),
    /// forgetting it.
    pub fn take_cut(&self) -> Option<(usize, usize)> {
        self.lock().cut.take()
    }

    /// Fences so far, on any partition: one per takeover.
    pub fn fences(&self) -> u64 {
        self.lock().fences
    }

    /// Commits that landed so far, on any partition.
    pub fn commits(&self) -> u64 {
        self.lock().commits
    }

    /// Keep only the newest record of each key on `partition`, as log
    /// compaction does. Tombstones are kept (their retention has not passed).
    pub fn compact(&self, partition: u32) {
        let mut inner = self.lock();
        let log = &mut inner.partitions[partition as usize];
        for entries in [&mut log.promises, &mut log.schedules] {
            let mut newest: BTreeMap<String, Entry> = BTreeMap::new();
            for e in entries.drain(..) {
                newest.insert(e.key.clone(), e);
            }
            let mut kept: Vec<Entry> = newest.into_values().collect();
            kept.sort_by_key(|e| e.offset);
            *entries = kept;
        }
    }

    /// Every committed data record of `partition`'s promise log, oldest
    /// first — fenced writers' included, claims not.
    pub fn promise_records(&self, partition: u32) -> Vec<(String, Option<Vec<u8>>)> {
        self.lock().partitions[partition as usize]
            .promises
            .iter()
            .filter(|e| !e.claim)
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect()
    }

    /// The partition's logs end here.
    pub fn end(&self, partition: u32) -> Checkpoint {
        self.lock().partitions[partition as usize].end()
    }
}

#[async_trait]
impl Log for MemLog {
    fn partitions(&self) -> u32 {
        self.n
    }

    async fn fence(&self, partition: u32) -> Result<Arc<dyn Writer>, LogError> {
        let mut inner = self.lock();
        inner.fences += 1;
        let log = inner
            .partitions
            .get_mut(partition as usize)
            .ok_or_else(|| LogError::Unavailable(format!("no partition {partition}")))?;
        // A claim on each log, where it ends: in process nobody races us to
        // it, so it lands where it meant to.
        let mut epochs = [-1; 2];
        for topic in [Topic::Promises, Topic::Schedules] {
            let at = log.end().get(topic);
            let landed = log.push(
                topic,
                epoch::claim_key(at),
                Some(Vec::new()),
                Some(at),
                true,
            );
            debug_assert_eq!(landed, at);
            epochs[topic as usize] = at;
            log.claims[topic as usize] = at;
        }
        Ok(Arc::new(MemWriter {
            inner: Arc::clone(&self.inner),
            partition,
            epochs,
        }))
    }

    async fn start(&self, partition: u32) -> Result<Checkpoint, LogError> {
        let inner = self.lock();
        let log = &inner.partitions[partition as usize];
        let first = |entries: &Vec<Entry>, next: i64| entries.first().map_or(next, |e| e.offset);
        Ok(Checkpoint::at(
            first(&log.promises, log.next_promises),
            first(&log.schedules, log.next_schedules),
        ))
    }

    async fn reader(&self, partition: u32, from: Checkpoint) -> Result<Box<dyn Reader>, LogError> {
        let inner = self.lock();
        let log = &inner.partitions[partition as usize];
        let end = log.end();
        let mut records = Vec::new();
        for topic in [Topic::Promises, Topic::Schedules] {
            for e in log.entries(topic) {
                if e.offset >= from.get(topic) {
                    records.push(Consumed {
                        topic,
                        key: e.key.clone(),
                        value: e.value.clone(),
                        offset: e.offset,
                        epoch: e.epoch,
                        claim: e.claim,
                    });
                }
            }
        }
        let raw = Box::new(MemReader {
            records,
            position: from,
            end,
            done: false,
        });
        Ok(Box::new(epoch::Filtered::new(raw, &from)))
    }

    async fn ready(&self) -> bool {
        true
    }
}

/// A partition's writer: its records land wherever the log ends, and it
/// checks afterwards that that was where it left the log.
struct MemWriter {
    inner: Arc<Mutex<Inner>>,
    partition: u32,
    epochs: [i64; 2],
}

impl MemWriter {
    /// Why the log does not end at `at`: a newer claim fenced this writer, or
    /// something else landed and nobody knows what of ours is admitted.
    fn moved(&self, log: &PartitionLog, at: &Checkpoint) -> Option<LogError> {
        for topic in [Topic::Promises, Topic::Schedules] {
            if log.claimed(topic) > self.epochs[topic as usize] {
                return Some(LogError::Fenced(format!(
                    "partition {} was claimed past epoch {}",
                    self.partition, self.epochs[topic as usize]
                )));
            }
        }
        let end = log.end();
        (end.promises != at.promises || end.schedules != at.schedules).then(|| {
            LogError::Uncertain(format!(
                "partition {} ends at {:?}, not where this writer left it ({:?})",
                self.partition,
                (end.promises, end.schedules),
                (at.promises, at.schedules)
            ))
        })
    }
}

#[async_trait]
impl Writer for MemWriter {
    async fn commit(&self, records: Vec<Record>, base: Checkpoint) -> Result<Checkpoint, LogError> {
        let mut inner = lock(&self.inner);
        let fault = inner.fault.take();
        if let Some(Fault::Refuse(e)) = &fault {
            return Err(e.clone());
        }
        let land = match fault {
            Some(Fault::Cut(k)) => {
                inner.cut = Some((k.min(records.len()), records.len()));
                k.min(records.len())
            }
            Some(Fault::CutWithin(seed)) => {
                let k = (seed % records.len().max(1) as u64) as usize;
                inner.cut = Some((k, records.len()));
                k
            }
            _ => records.len(),
        };
        let partition = &mut inner.partitions[self.partition as usize];
        // Checked before and after, as a producer learns it: from the offsets
        // its records got. A fenced writer's records land all the same.
        let moved = self.moved(partition, &base);
        let mut after = base;
        for record in records.into_iter().take(land) {
            let epoch = self.epochs[record.topic as usize];
            let offset = partition.push(record.topic, record.key, record.value, Some(epoch), false);
            after.set(record.topic, offset + 1);
        }
        after.epochs = self.epochs;
        inner.commits += 1;
        if let Some(e) = moved {
            return Err(e);
        }
        match fault {
            Some(Fault::LandThenUncertain) => {
                Err(LogError::Uncertain("injected: landed, then lost".into()))
            }
            Some(Fault::Cut(k)) => Err(LogError::Uncertain(format!(
                "injected: cut after {k} records, then lost"
            ))),
            Some(Fault::CutWithin(_)) => Err(LogError::Uncertain(format!(
                "injected: cut after {land} records, then lost"
            ))),
            _ => Ok(after),
        }
    }

    async fn check(&self, at: Checkpoint) -> Result<(), LogError> {
        let inner = lock(&self.inner);
        match self.moved(&inner.partitions[self.partition as usize], &at) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

struct MemReader {
    records: Vec<Consumed>,
    position: Checkpoint,
    end: Checkpoint,
    done: bool,
}

#[async_trait]
impl Reader for MemReader {
    async fn next(&mut self) -> Result<Option<(Vec<Consumed>, Checkpoint)>, LogError> {
        if self.done {
            return Ok(None);
        }
        const BATCH: usize = 1_000;
        if self.records.is_empty() {
            self.done = true;
            // The end, so a resume starts there.
            if (self.position.promises, self.position.schedules)
                != (self.end.promises, self.end.schedules)
            {
                self.position = self.end;
                return Ok(Some((Vec::new(), self.end)));
            }
            return Ok(None);
        }
        let take = self.records.len().min(BATCH);
        let batch: Vec<Consumed> = self.records.drain(..take).collect();
        for r in &batch {
            self.position.set(r.topic, r.offset + 1);
        }
        Ok(Some((batch, self.position)))
    }
}

fn lock(inner: &Mutex<Inner>) -> std::sync::MutexGuard<'_, Inner> {
    // A panic under the lock leaves nothing half-written that matters to a
    // test: every mutation is a single append or insert.
    inner.lock().unwrap_or_else(|e| e.into_inner())
}
