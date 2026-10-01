//! One partition, owned: take it over, then decide and commit every request
//! for it, one round at a time.
//!
//! # Takeover
//!
//! In this order, and the order is the safety argument:
//!
//! 1. **Fence** ([`Log::fence`]). From here no earlier owner can commit —
//!    including one that is paused and does not know it lost the partition.
//! 2. **Check the local copy.** A checkpoint below the log's start offset
//!    means the log was truncated past it, so the copy is dropped and rebuilt.
//! 3. **Replay** from the checkpoint to the end of the log. Because step 1
//!    came first, that end is everything anyone will ever have committed
//!    before us.
//! 4. **Rebuild the timer index** from the records.
//! 5. **Serve.**
//!
//! Nothing is announced: the group's assignment is what tells other nodes
//! where the partition lives ([`crate::directory`]).
//!
//! # A round
//!
//! One actor per partition serializes every decision in it. It drains its
//! mailbox, then:
//!
//! 1. loads each origin (and schedule) the batch names from the local store,
//!    once, and folds the batch through the kernel in arrival order — request
//!    *k* sees request *k-1*'s document;
//! 2. diffs every document it touched into records ([`record::diff`]), each
//!    in the log's encoding and the local store's;
//! 3. commits all of them in **one transaction** — the group commit — and
//!    nothing at all if nothing changed;
//! 4. applies the same records to the local store with the checkpoint the
//!    commit returned;
//! 5. re-arms the timer index for every target it touched;
//! 6. hands the sends to the sender — post-commit, at most once, as on every
//!    other backend;
//! 7. answers the callers.
//!
//! The outcome of step 3 decides the rest. `Unavailable`: nothing landed, so
//! the round's decisions are dropped and its callers told to retry; the
//! partition carries on. `Fenced`: another node owns the partition; stop.
//! `Uncertain` (or a local write that fails after a commit that landed): the
//! local copy may now disagree with the log, so stop, and let the node take
//! the partition over again — fence, replay, serve — which is the one path
//! that is known to bring the two back together.
//!
//! # Dependencies
//!
//! The kernel for every decision, [`record`] for the bytes, the log port for
//! the commit, the local store for loads, and the blob backend's `Sender` for
//! delivery.
//!
//! # Dependants
//!
//! The node, which takes partitions over and routes every request, timer and
//! schedule firing to them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};

use resonate_core::types::{ScheduleCreateData, ScheduleResponseData};
use resonate_core::{util, Unavailable};
use resonate_server_blob::kernel::state::{
    apply_effects, min_deadline, Effect, KernelCfg, OriginDoc, Reply, Req, TAG_TARGET,
};
use resonate_server_blob::kernel::{drain, handle};
use resonate_server_blob::schedules::{ScheduleDoc, SCHEDULE_FORMAT_VERSION};
use resonate_server_blob::sender::Sender;

use crate::cache::DocCache;
use crate::keys;
use crate::local::{LocalStore, Op, PartitionStore, LOCAL_FORMAT};
use crate::log::{Checkpoint, Consumed, Log, LogError, Record, Topic, Writer};
use crate::metrics;
use crate::record;
use crate::timers::{Target, Timers};

/// Tuning a partition reads.
#[derive(Debug, Clone)]
pub struct PartitionCfg {
    pub kernel: KernelCfg,
    /// The most requests one round decides — the group commit's ceiling.
    pub max_batch: usize,
    /// Mailbox depth.
    pub mailbox: usize,
    /// The hot-document cache's budget, in promises ([`DocCache`]). Zero
    /// turns it off.
    pub cache_promises: usize,
}

impl Default for PartitionCfg {
    fn default() -> Self {
        Self {
            kernel: KernelCfg::default(),
            max_batch: 512,
            mailbox: 4_096,
            cache_promises: 2_000,
        }
    }
}

/// Why a partition stopped serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    /// Asked to: the partition was revoked or the node is stopping.
    Stopped,
    /// Another node fenced this one out.
    Fenced(String),
    /// A commit's outcome is unknown; the local copy must be re-read.
    Uncertain(String),
}

/// What happens to an origin.
pub enum OriginOp {
    Req(Box<Req>),
    /// Sweep the origin's deadlines — a timer firing.
    Tick,
}

/// What happens to a schedule.
pub enum ScheduleOp {
    Get,
    Create(ScheduleCreateData),
    Delete,
    /// Move past the occurrence at `deadline`, which has been fired.
    Advance {
        deadline: i64,
    },
}

type Answer = oneshot::Sender<Result<Reply, Unavailable>>;

enum Work {
    Origin {
        origin: String,
        op: OriginOp,
        now: i64,
        reply: Answer,
        /// When it reached the partition, for the queue-wait metric.
        at: Instant,
    },
    Schedule {
        id: String,
        op: ScheduleOp,
        now: i64,
        reply: Answer,
        at: Instant,
    },
    /// `debug.reset`: tombstone everything the partition holds.
    Reset { reply: Answer },
}

impl Work {
    fn at(&self) -> Option<Instant> {
        match self {
            Work::Origin { at, .. } | Work::Schedule { at, .. } => Some(*at),
            Work::Reset { .. } => None,
        }
    }

    fn fail(self, e: &Unavailable) {
        let reply = match self {
            Work::Origin { reply, .. } | Work::Schedule { reply, .. } | Work::Reset { reply } => {
                reply
            }
        };
        let _ = reply.send(Err(e.clone()));
    }
}

/// A partition this node owns and serves.
pub struct Partition {
    id: u32,
    tx: mpsc::Sender<Work>,
    store: Arc<dyn PartitionStore>,
    timers: Arc<Timers>,
    stop: watch::Sender<bool>,
    actor: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Why a takeover did not complete.
#[derive(Debug)]
pub struct TakeoverError(pub String);

impl std::fmt::Display for TakeoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<LogError> for TakeoverError {
    fn from(e: LogError) -> Self {
        TakeoverError(e.to_string())
    }
}

impl Partition {
    /// Take partition `id` over and start serving it. See the module docs for
    /// the order of the steps and why it is that order.
    ///
    /// `on_exit` hears why the partition stopped, if it stops on its own.
    #[allow(clippy::too_many_arguments)]
    pub async fn take_over(
        id: u32,
        log: &Arc<dyn Log>,
        local: &Arc<dyn LocalStore>,
        sender: Arc<Sender>,
        cfg: PartitionCfg,
        on_exit: Box<dyn FnOnce(Exit) + Send>,
    ) -> Result<Arc<Partition>, TakeoverError> {
        let started = Instant::now();
        let out = Self::take_over_inner(id, log, local, sender, cfg, on_exit).await;
        metrics::TAKEOVER_SECONDS.observe(started.elapsed().as_secs_f64());
        metrics::TAKEOVERS
            .with_label_values(&[if out.is_ok() { "ok" } else { "failed" }])
            .inc();
        out
    }

    async fn take_over_inner(
        id: u32,
        log: &Arc<dyn Log>,
        local: &Arc<dyn LocalStore>,
        sender: Arc<Sender>,
        cfg: PartitionCfg,
        on_exit: Box<dyn FnOnce(Exit) + Send>,
    ) -> Result<Arc<Partition>, TakeoverError> {
        // (1) Fence first: whatever is read after this is final.
        let writer = log.fence(id).await?;

        // (2) The local copy, if it is still usable.
        let start = log.start(id).await?;
        let mut store = local.open(id).map_err(TakeoverError)?;
        let mut checkpoint = store.checkpoint().map_err(TakeoverError)?;
        if let Some(cp) = checkpoint {
            let format = store.format().map_err(TakeoverError)?;
            let why = if format != Some(LOCAL_FORMAT) {
                Some("written in another local format")
            } else if !cp.covers(&start) {
                Some("behind the log's start offset")
            } else {
                None
            };
            if let Some(why) = why {
                tracing::warn!(
                    partition = id,
                    ?format,
                    "Local copy is {why}; rebuilding it from the log"
                );
                local.drop_partition(id).map_err(TakeoverError)?;
                store = local.open(id).map_err(TakeoverError)?;
                checkpoint = None;
            }
        }
        if checkpoint.is_none() {
            // Stamped before the first batch it describes.
            store.set_format(LOCAL_FORMAT).map_err(TakeoverError)?;
        }
        let from = checkpoint.unwrap_or(start);

        // (3) Replay to the end.
        let mut position = from;
        let mut replayed = 0usize;
        let mut reader = log.reader(id, from).await?;
        while let Some((batch, after)) = reader.next().await? {
            replayed += batch.len();
            metrics::REPLAYED.inc_by(batch.len() as u64);
            let ops = batch
                .into_iter()
                .map(op_of)
                .collect::<Result<Vec<Op>, String>>()
                .map_err(TakeoverError)?;
            store.apply(ops, after).map_err(TakeoverError)?;
            position = after;
        }

        // (4) The timer index, from the records themselves.
        let timers = Arc::new(Timers::new());
        seed_timers(store.as_ref(), &timers).map_err(TakeoverError)?;

        tracing::info!(
            partition = id,
            replayed,
            armed = timers.len(),
            "Partition taken over"
        );

        // (5) Serve.
        let (tx, rx) = mpsc::channel(cfg.mailbox.max(1));
        let (stop, stop_rx) = watch::channel(false);
        let actor = Actor {
            id,
            writer,
            store: Arc::clone(&store),
            timers: Arc::clone(&timers),
            sender,
            cache: DocCache::new(cfg.cache_promises),
            cfg,
            checkpoint: position,
        };
        let handle = tokio::spawn(async move {
            let exit = actor.run(rx, stop_rx).await;
            if exit != Exit::Stopped {
                on_exit(exit);
            }
        });
        Ok(Arc::new(Partition {
            id,
            tx,
            store,
            timers,
            stop,
            actor: Mutex::new(Some(handle)),
        }))
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn store(&self) -> &Arc<dyn PartitionStore> {
        &self.store
    }

    pub fn timers(&self) -> &Arc<Timers> {
        &self.timers
    }

    async fn submit(&self, work: impl FnOnce(Answer) -> Work) -> Result<Reply, Unavailable> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(work(tx))
            .await
            .map_err(|_| Unavailable::new(format!("partition {} is not serving", self.id)))?;
        rx.await.map_err(|_| {
            Unavailable::new(format!("partition {} stopped before answering", self.id))
        })?
    }

    /// Decide one origin operation.
    pub async fn origin(&self, origin: &str, op: OriginOp, now: i64) -> Result<Reply, Unavailable> {
        let origin = origin.to_string();
        self.submit(|reply| Work::Origin {
            origin,
            op,
            now,
            reply,
            at: Instant::now(),
        })
        .await
    }

    /// Decide one schedule operation.
    pub async fn schedule(&self, id: &str, op: ScheduleOp, now: i64) -> Result<Reply, Unavailable> {
        let id = id.to_string();
        self.submit(|reply| Work::Schedule {
            id,
            op,
            now,
            reply,
            at: Instant::now(),
        })
        .await
    }

    /// Tombstone everything this partition holds. `debug.reset` only.
    pub async fn reset(&self) -> Result<(), Unavailable> {
        self.submit(|reply| Work::Reset { reply }).await.map(|_| ())
    }

    /// A schedule as last committed.
    pub fn committed_schedule(&self, id: &str) -> Result<Option<ScheduleDoc>, Unavailable> {
        match self
            .store
            .get(&keys::schedule_key(id))
            .map_err(Unavailable::new)?
        {
            Some(bytes) => record::decode_schedule(&bytes)
                .map(Some)
                .map_err(|e| Unavailable::new(format!("schedule {id} unreadable: {e}"))),
            None => Ok(None),
        }
    }

    /// Stop serving: finish the round in flight, refuse what is queued, and
    /// make the local copy durable so a return replays only the tail.
    pub async fn stop(&self) {
        let _ = self.stop.send(true);
        let handle = self.actor.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        if let Err(e) = self.store.flush() {
            tracing::warn!(partition = self.id, error = %e, "Local copy not flushed on stop");
        }
    }
}

/// The local write a consumed record becomes: a promise transcoded into the
/// local format, a schedule as it is.
fn op_of(c: Consumed) -> Result<Op, String> {
    Ok(match c.topic {
        Topic::Promises => {
            let value = match &c.value {
                Some(v) => Some(record::local::from_log(&c.key, v)?),
                None => None,
            };
            (keys::promise_key(&c.key), value)
        }
        Topic::Schedules => (keys::schedule_key(&c.key), c.value),
    })
}

/// Arm one entry per origin and per schedule from what the store holds.
fn seed_timers(store: &dyn PartitionStore, timers: &Timers) -> Result<(), String> {
    let mut earliest: BTreeMap<String, i64> = BTreeMap::new();
    for (key, value) in store.scan(&keys::all_promises_prefix())? {
        let id = keys::id_of_promise_key(&key).ok_or("unreadable promise key")?;
        let (promise, task) = record::local::decode(&value)?;
        let mut doc = OriginDoc::default();
        doc.promises.insert(id.clone(), promise);
        if let Some(task) = task {
            doc.tasks.insert(id.clone(), task);
        }
        if let Some(at) = min_deadline(&doc) {
            let slot = earliest
                .entry(keys::origin_of(&id).to_string())
                .or_insert(at);
            *slot = (*slot).min(at);
        }
    }
    for (origin, at) in earliest {
        timers.set(Target::Origin(origin), Some(at));
    }
    for (key, value) in store.scan(&keys::all_schedules_prefix())? {
        let id = keys::id_of_schedule_key(&key).ok_or("unreadable schedule key")?;
        let doc = record::decode_schedule(&value)?;
        timers.set(Target::Schedule(id), Some(doc.next_run_at));
    }
    Ok(())
}

/// Read an origin's document from the local store.
fn load_origin(store: &dyn PartitionStore, origin: &str) -> Result<OriginDoc, Unavailable> {
    let rows = store
        .scan(&keys::origin_prefix(origin))
        .map_err(Unavailable::new)?;
    let mut records = Vec::with_capacity(rows.len());
    for (key, value) in &rows {
        let id = keys::id_of_promise_key(key)
            .ok_or_else(|| Unavailable::new(format!("unreadable key under origin {origin}")))?;
        records.push((id, &value[..]));
    }
    // A document that cannot be read is not something to paper over: refusing
    // beats deciding against a guess.
    record::local::assemble(records)
        .map_err(|e| Unavailable::new(format!("origin {origin} unreadable: {e}")))
}

fn load_schedule(store: &dyn PartitionStore, id: &str) -> Result<Option<ScheduleDoc>, Unavailable> {
    match store
        .get(&keys::schedule_key(id))
        .map_err(Unavailable::new)?
    {
        Some(bytes) => record::decode_schedule(&bytes)
            .map(Some)
            .map_err(|e| Unavailable::new(format!("schedule {id} unreadable: {e}"))),
        None => Ok(None),
    }
}

/// The partition's serialized decision loop.
struct Actor {
    id: u32,
    writer: Arc<dyn Writer>,
    store: Arc<dyn PartitionStore>,
    timers: Arc<Timers>,
    sender: Arc<Sender>,
    cfg: PartitionCfg,
    checkpoint: Checkpoint,
    /// Hot documents, decoded. The actor's own: see [`DocCache`].
    cache: DocCache,
}

/// A round's working state: every document and schedule it touched, as they
/// were before the round and as they are now.
#[derive(Default)]
struct Overlay {
    origins: BTreeMap<String, (OriginDoc, OriginDoc)>,
    schedules: BTreeMap<String, (Option<ScheduleDoc>, Option<ScheduleDoc>)>,
}

impl Actor {
    async fn run(mut self, mut rx: mpsc::Receiver<Work>, mut stop: watch::Receiver<bool>) -> Exit {
        let exit = loop {
            let first = tokio::select! {
                biased;
                _ = stop.changed() => break Exit::Stopped,
                work = rx.recv() => match work {
                    Some(work) => work,
                    None => break Exit::Stopped,
                },
            };
            // Group commit: take everything already queued, up to the ceiling.
            let mut batch = vec![first];
            while batch.len() < self.cfg.max_batch {
                match rx.try_recv() {
                    Ok(work) => batch.push(work),
                    Err(_) => break,
                }
            }
            // A reset is its own round, so it sees what came before it.
            let mut segment = Vec::new();
            let mut outcome = Ok(());
            for work in batch {
                if outcome.is_err() {
                    work.fail(&Unavailable::new(format!(
                        "partition {} stopped serving",
                        self.id
                    )));
                    continue;
                }
                if matches!(work, Work::Reset { .. }) {
                    outcome = self.round(std::mem::take(&mut segment)).await;
                    if outcome.is_ok() {
                        outcome = self.reset(work).await;
                    } else {
                        work.fail(&Unavailable::new("partition stopped serving"));
                    }
                } else {
                    segment.push(work);
                }
            }
            if outcome.is_ok() && !segment.is_empty() {
                outcome = self.round(segment).await;
            } else {
                for work in segment {
                    work.fail(&Unavailable::new("partition stopped serving"));
                }
            }
            if let Err(exit) = outcome {
                break exit;
            }
        };
        // Whatever is still queued will not be decided here.
        rx.close();
        let reason = match &exit {
            Exit::Stopped => format!("partition {} moved", self.id),
            other => format!("partition {} stopped serving: {other:?}", self.id),
        };
        while let Ok(work) = rx.try_recv() {
            work.fail(&Unavailable::new(reason.clone()));
        }
        match &exit {
            Exit::Stopped => {}
            Exit::Fenced(m) => {
                tracing::warn!(partition = self.id, reason = %m, "Partition fenced out")
            }
            Exit::Uncertain(m) => {
                tracing::warn!(partition = self.id, reason = %m, "Partition must be re-read")
            }
        }
        exit
    }

    /// Decide, commit, apply, arm, send, answer.
    async fn round(&mut self, batch: Vec<Work>) -> Result<(), Exit> {
        if batch.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        metrics::ROUND_REQUESTS.observe(batch.len() as f64);
        for work in &batch {
            if let Some(at) = work.at() {
                metrics::QUEUE_WAIT.observe(started.duration_since(at).as_secs_f64());
            }
        }
        let out = self.decide_and_commit(batch).await;
        metrics::ROUND_SECONDS.observe(started.elapsed().as_secs_f64());
        out
    }

    async fn decide_and_commit(&mut self, batch: Vec<Work>) -> Result<(), Exit> {
        let mut overlay = Overlay::default();
        let mut answers: Vec<(Answer, Result<Reply, Unavailable>)> =
            Vec::with_capacity(batch.len());
        let mut sends: Vec<Effect> = Vec::new();

        for work in batch {
            match work {
                Work::Origin {
                    origin,
                    op,
                    now,
                    reply,
                    ..
                } => {
                    if !overlay.origins.contains_key(&origin) {
                        // The cache first; a document taken out of it is
                        // put back only if this round commits.
                        let loaded = match self.cache.take(&origin) {
                            Some(doc) => Ok(doc),
                            None => load_origin(self.store.as_ref(), &origin),
                        };
                        match loaded {
                            Ok(doc) => {
                                overlay.origins.insert(origin.clone(), (doc.clone(), doc));
                            }
                            Err(e) => {
                                answers.push((reply, Err(e)));
                                continue;
                            }
                        }
                    }
                    let doc = &mut overlay.origins.get_mut(&origin).expect("loaded").1;
                    let (answer, fx) = decide(doc, &op, now, &self.cfg.kernel);
                    sends.extend(fx);
                    answers.push((reply, Ok(answer)));
                }
                Work::Schedule {
                    id, op, now, reply, ..
                } => {
                    if !overlay.schedules.contains_key(&id) {
                        match load_schedule(self.store.as_ref(), &id) {
                            Ok(doc) => {
                                overlay.schedules.insert(id.clone(), (doc.clone(), doc));
                            }
                            Err(e) => {
                                answers.push((reply, Err(e)));
                                continue;
                            }
                        }
                    }
                    let doc = &mut overlay.schedules.get_mut(&id).expect("loaded").1;
                    answers.push((reply, Ok(decide_schedule(&id, doc, op, now))));
                }
                Work::Reset { .. } => unreachable!("a reset is a round of its own"),
            }
        }

        let mut records = Vec::new();
        let mut ops: Vec<Op> = Vec::new();
        for (before, after) in overlay.origins.values() {
            for change in record::diff(before, after) {
                ops.push((keys::promise_key(&change.id), change.local));
                records.push(Record {
                    topic: Topic::Promises,
                    key: change.id,
                    value: change.log,
                });
            }
        }
        for (id, (before, after)) in &overlay.schedules {
            if before != after {
                let value = after.as_ref().map(record::encode_schedule);
                ops.push((keys::schedule_key(id), value.clone()));
                records.push(Record {
                    topic: Topic::Schedules,
                    key: id.clone(),
                    value,
                });
            }
        }

        let result = self.commit(records, ops).await;
        match result {
            Ok(()) => {
                // Arm every target the round touched, changed or not: a timer
                // that fired early is re-armed rather than forgotten.
                for (origin, (_, after)) in &overlay.origins {
                    self.timers
                        .set(Target::Origin(origin.clone()), min_deadline(after));
                }
                for (id, (_, after)) in &overlay.schedules {
                    self.timers.set(
                        Target::Schedule(id.clone()),
                        after.as_ref().map(|s| s.next_run_at),
                    );
                }
                // Committed, so these are what the store now holds.
                for (origin, (_, after)) in overlay.origins {
                    self.cache.put(origin, after);
                }
                for effect in sends {
                    if let Effect::Send { address, msg } = effect {
                        self.sender.dispatch(&address, *msg).await;
                    }
                }
                for (reply, answer) in answers {
                    let _ = reply.send(answer);
                }
                Ok(())
            }
            Err(e) => {
                let (unavailable, exit) = match e {
                    LogError::Unavailable(m) => (Unavailable::new(m), None),
                    LogError::Fenced(m) => (Unavailable::new(m.clone()), Some(Exit::Fenced(m))),
                    LogError::Uncertain(m) => {
                        (Unavailable::new(m.clone()), Some(Exit::Uncertain(m)))
                    }
                };
                for (reply, _) in answers {
                    let _ = reply.send(Err(unavailable.clone()));
                }
                match exit {
                    Some(exit) => Err(exit),
                    None => Ok(()),
                }
            }
        }
    }

    /// Commit `records` and apply them locally. Nothing is written when
    /// nothing changed.
    /// Commit `records` to the log, then apply `ops` — the same changes in
    /// the local encoding — to the local store.
    async fn commit(&mut self, records: Vec<Record>, ops: Vec<Op>) -> Result<(), LogError> {
        if records.is_empty() {
            return Ok(());
        }
        let (promises, schedules) = records
            .iter()
            .fold((0u64, 0u64), |(p, s), r| match r.topic {
                Topic::Promises => (p + 1, s),
                Topic::Schedules => (p, s + 1),
            });
        let started = Instant::now();
        let committed = self.writer.commit(records, self.checkpoint).await;
        metrics::COMMIT_SECONDS.observe(started.elapsed().as_secs_f64());
        let after = match committed {
            Ok(after) => after,
            Err(e) => {
                let kind = match &e {
                    LogError::Unavailable(_) => "unavailable",
                    LogError::Fenced(_) => "fenced",
                    LogError::Uncertain(_) => "uncertain",
                };
                metrics::COMMIT_ERRORS.with_label_values(&[kind]).inc();
                return Err(e);
            }
        };
        metrics::RECORDS_COMMITTED
            .with_label_values(&["promises"])
            .inc_by(promises as f64);
        metrics::RECORDS_COMMITTED
            .with_label_values(&["schedules"])
            .inc_by(schedules as f64);
        // The log has it. A local copy that cannot take it now disagrees with
        // the log, which is exactly the uncertain case.
        self.store
            .apply(ops, after)
            .map_err(|e| LogError::Uncertain(format!("local apply failed after commit: {e}")))?;
        self.checkpoint = after;
        Ok(())
    }

    async fn reset(&mut self, work: Work) -> Result<(), Exit> {
        let Work::Reset { reply } = work else {
            unreachable!("only a reset is handed here");
        };
        let mut records = Vec::new();
        let scanned = self
            .store
            .scan(&keys::all_promises_prefix())
            .and_then(|promises| {
                self.store
                    .scan(&keys::all_schedules_prefix())
                    .map(|schedules| (promises, schedules))
            });
        let (promises, schedules) = match scanned {
            Ok(pair) => pair,
            Err(e) => {
                let _ = reply.send(Err(Unavailable::new(e)));
                return Ok(());
            }
        };
        for (key, _) in promises {
            if let Some(id) = keys::id_of_promise_key(&key) {
                records.push(Record {
                    topic: Topic::Promises,
                    key: id,
                    value: None,
                });
            }
        }
        for (key, _) in schedules {
            if let Some(id) = keys::id_of_schedule_key(&key) {
                records.push(Record {
                    topic: Topic::Schedules,
                    key: id,
                    value: None,
                });
            }
        }
        // Tombstones: the same in both encodings.
        let ops: Vec<Op> = records
            .iter()
            .map(|r| match r.topic {
                Topic::Promises => (keys::promise_key(&r.key), None),
                Topic::Schedules => (keys::schedule_key(&r.key), None),
            })
            .collect();
        match self.commit(records, ops).await {
            Ok(()) => {
                self.timers.clear();
                self.cache.clear();
                let _ = reply.send(Ok(Reply::status(200, serde_json::json!({}))));
                Ok(())
            }
            Err(LogError::Unavailable(m)) => {
                let _ = reply.send(Err(Unavailable::new(m)));
                Ok(())
            }
            Err(LogError::Fenced(m)) => {
                let _ = reply.send(Err(Unavailable::new(m.clone())));
                Err(Exit::Fenced(m))
            }
            Err(LogError::Uncertain(m)) => {
                let _ = reply.send(Err(Unavailable::new(m.clone())));
                Err(Exit::Uncertain(m))
            }
        }
    }
}

/// Fold one origin operation through the kernel. Pure: no I/O, no clock.
///
/// The same fold the blob applier makes: every request first sweeps its
/// origin, so the handler sees a document with nothing due; the sweep's
/// changes ride the same commit.
fn decide(doc: &mut OriginDoc, op: &OriginOp, now: i64, cfg: &KernelCfg) -> (Reply, Vec<Effect>) {
    let (reply, fx) = match op {
        OriginOp::Req(req) => {
            let mut fx = drain(doc, now, cfg);
            apply_effects(doc, &fx);
            let (mut hfx, reply) = handle(doc, req, now, cfg);
            fx.append(&mut hfx);
            (reply, fx)
        }
        OriginOp::Tick => (
            Reply::status(200, serde_json::Value::Array(vec![])),
            drain(doc, now, cfg),
        ),
    };
    apply_effects(doc, &fx);
    let sends = fx
        .into_iter()
        .filter(|e| matches!(e, Effect::Send { .. }))
        .collect();
    (reply, sends)
}

/// Apply one schedule operation to `doc`, the schedule's current version.
///
/// The semantics are the blob schedule service's, which are the SQL
/// backends': create is idempotent on the id and validates first; an advance
/// fires only while the schedule still names the occurrence that came due.
fn decide_schedule(id: &str, doc: &mut Option<ScheduleDoc>, op: ScheduleOp, now: i64) -> Reply {
    match op {
        ScheduleOp::Get => match doc {
            Some(d) => Reply::ok(&ScheduleResponseData {
                schedule: d.to_record(id),
            }),
            None => Reply::err(404, "Schedule not found"),
        },
        ScheduleOp::Create(r) => {
            if let Some(addr) = r.promise_tags.get(TAG_TARGET) {
                if !resonate_core::is_valid_address(addr) {
                    return Reply::err(400, "Invalid resonate:target address");
                }
            }
            if !util::is_valid_cron(&r.cron) {
                return Reply::err(400, "Invalid cron expression");
            }
            if let Some(existing) = doc {
                return Reply::ok(&ScheduleResponseData {
                    schedule: existing.to_record(id),
                });
            }
            let created = ScheduleDoc {
                v: SCHEDULE_FORMAT_VERSION,
                cron: r.cron.clone(),
                promise_id: r.promise_id.clone(),
                promise_timeout: r.promise_timeout,
                promise_param_headers: r
                    .promise_param
                    .headers
                    .as_ref()
                    .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                promise_param_data: r.promise_param.data.clone(),
                promise_tags: r
                    .promise_tags
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                created_at: now,
                next_run_at: util::compute_next_cron(&r.cron, now),
                last_run_at: None,
            };
            let reply = Reply::ok(&ScheduleResponseData {
                schedule: created.to_record(id),
            });
            *doc = Some(created);
            reply
        }
        ScheduleOp::Delete => {
            if doc.is_none() {
                return Reply::err(404, "Schedule not found");
            }
            *doc = None;
            Reply::status(200, serde_json::json!({}))
        }
        ScheduleOp::Advance { deadline } => {
            if let Some(d) = doc {
                if d.next_run_at == deadline {
                    // One occurrence at a time, as the SQL path does: a
                    // schedule far behind catches up one sweep at a time.
                    d.last_run_at = Some(deadline);
                    d.next_run_at = util::compute_next_cron(&d.cron, deadline);
                }
            }
            Reply::status(200, serde_json::json!({}))
        }
    }
}
