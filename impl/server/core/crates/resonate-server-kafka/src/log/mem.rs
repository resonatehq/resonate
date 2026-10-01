//! The log in process: Kafka's fencing and transaction semantics, without
//! Kafka.
//!
//! What it models, because the shell depends on it:
//!
//! - **Fencing by epoch.** Each partition has an epoch, as a transactional id
//!   has a producer epoch; [`Log::fence`] bumps it, and a writer holding an
//!   older one is refused. Several in-process nodes sharing one `MemLog` fence
//!   each other exactly as processes sharing a cluster do.
//! - **Atomic commits with markers.** A commit appends every record and then a
//!   control marker, which takes an offset and is never returned to a reader —
//!   so offsets have gaps, as they do on Kafka.
//! - **Compaction.** [`MemLog::compact`] keeps the newest record per key, which
//!   is what lets a test restore from a compacted log.
//! - **Faults.** [`MemLog::fail_next`] makes the next commit fail with the error
//!   of your choice, and [`MemLog::land_next_then_fail`] makes it *land* and
//!   still report `Uncertain` — the case that forces a partition to re-read.
//!
//! # Dependants
//!
//! The differential suite and every test that runs nodes in process.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{Checkpoint, Consumed, Log, LogError, Reader, Record, Topic, Writer};

#[derive(Debug, Clone)]
struct Entry {
    offset: i64,
    key: String,
    value: Option<Vec<u8>>,
}

#[derive(Default)]
struct PartitionLog {
    epoch: u64,
    promises: Vec<Entry>,
    schedules: Vec<Entry>,
    next_promises: i64,
    next_schedules: i64,
}

impl PartitionLog {
    fn entries(&self, topic: Topic) -> &Vec<Entry> {
        match topic {
            Topic::Promises => &self.promises,
            Topic::Schedules => &self.schedules,
        }
    }

    fn append(&mut self, topic: Topic, key: String, value: Option<Vec<u8>>) -> i64 {
        let (log, next) = match topic {
            Topic::Promises => (&mut self.promises, &mut self.next_promises),
            Topic::Schedules => (&mut self.schedules, &mut self.next_schedules),
        };
        let offset = *next;
        log.push(Entry { offset, key, value });
        *next += 1;
        offset
    }

    /// A transaction marker: an offset nobody reads.
    fn mark(&mut self, topic: Topic) {
        match topic {
            Topic::Promises => self.next_promises += 1,
            Topic::Schedules => self.next_schedules += 1,
        }
    }

    fn end(&self) -> Checkpoint {
        Checkpoint {
            promises: self.next_promises,
            schedules: self.next_schedules,
        }
    }
}

/// A fault to inject into the next commit.
#[derive(Debug, Clone)]
pub enum Fault {
    /// Refuse the commit with this error; nothing lands.
    Refuse(LogError),
    /// Land the commit, then report `Uncertain`.
    LandThenUncertain,
}

#[derive(Default)]
struct Inner {
    partitions: Vec<PartitionLog>,
    fault: Option<Fault>,
    commits: u64,
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

    /// Every committed record of `partition`'s promise log, oldest first.
    pub fn promise_records(&self, partition: u32) -> Vec<(String, Option<Vec<u8>>)> {
        self.lock().partitions[partition as usize]
            .promises
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect()
    }
}

#[async_trait]
impl Log for MemLog {
    fn partitions(&self) -> u32 {
        self.n
    }

    async fn fence(&self, partition: u32) -> Result<Arc<dyn Writer>, LogError> {
        let mut inner = self.lock();
        let log = inner
            .partitions
            .get_mut(partition as usize)
            .ok_or_else(|| LogError::Unavailable(format!("no partition {partition}")))?;
        log.epoch += 1;
        Ok(Arc::new(MemWriter {
            inner: Arc::clone(&self.inner),
            partition,
            epoch: log.epoch,
        }))
    }

    async fn start(&self, partition: u32) -> Result<Checkpoint, LogError> {
        let inner = self.lock();
        let log = &inner.partitions[partition as usize];
        let first = |entries: &Vec<Entry>, next: i64| entries.first().map_or(next, |e| e.offset);
        Ok(Checkpoint {
            promises: first(&log.promises, log.next_promises),
            schedules: first(&log.schedules, log.next_schedules),
        })
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
                    });
                }
            }
        }
        Ok(Box::new(MemReader {
            records,
            position: from,
            end,
            done: false,
        }))
    }

    async fn ready(&self) -> bool {
        true
    }
}

struct MemWriter {
    inner: Arc<Mutex<Inner>>,
    partition: u32,
    epoch: u64,
}

#[async_trait]
impl Writer for MemWriter {
    async fn commit(&self, records: Vec<Record>, base: Checkpoint) -> Result<Checkpoint, LogError> {
        let mut inner = lock(&self.inner);
        let fault = inner.fault.take();
        if let Some(Fault::Refuse(e)) = &fault {
            return Err(e.clone());
        }
        let current = inner.partitions[self.partition as usize].epoch;
        if current != self.epoch {
            return Err(LogError::Fenced(format!(
                "partition {} is at epoch {current}, this writer holds {}",
                self.partition, self.epoch
            )));
        }
        let mut after = base;
        let partition = &mut inner.partitions[self.partition as usize];
        let mut touched = [false, false];
        for record in records {
            let offset = partition.append(record.topic, record.key, record.value);
            after.set(record.topic, offset + 1);
            touched[record.topic as usize] = true;
        }
        for (i, topic) in [Topic::Promises, Topic::Schedules].into_iter().enumerate() {
            if touched[i] {
                partition.mark(topic);
            }
        }
        inner.commits += 1;
        if let Some(Fault::LandThenUncertain) = fault {
            return Err(LogError::Uncertain("injected: landed, then lost".into()));
        }
        Ok(after)
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
            // The end, past any trailing marker, so a resume starts after it.
            if self.position != self.end {
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
