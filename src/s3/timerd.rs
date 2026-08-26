//! The timer service: fire what is due from memory, list only to recover.
//!
//! Deadlines live twice. Durably, as empty objects under a timer prefix keyed
//! by zero-padded deadline then target — written *before* the state that arms
//! them, so no crash window leaves a deadline uncovered. And in memory, in the
//! timer queue every writer arms right after its PUT lands. Normal operation
//! fires from the queue alone: the loop sleeps until the nearest armed
//! deadline, wakes, sweeps, collects — finding what is due costs zero store
//! operations.
//!
//! The listing path survives for exactly two callers. [`Timerd::seed`]
//! rebuilds the queue from one full listing at startup — the deployment is a
//! single node, so the queue and the store can only disagree across a crash,
//! and this is what repairs it. And [`Timerd::round`] is the sweep behind
//! `debug.tick`, driven by a synthetic clock with the loop paused, so the
//! differential suite exercises the same sweep the firing loop performs.
//!
//! **The deadline in the key is the fence.** A timer write is an unconditional
//! PUT, because the key carries everything the write means: arming the same
//! deadline twice is the same object, and arming a different one is a different
//! object. Nothing can be lost by a racing writer. Deletes are free, so
//! collecting an orphan costs nothing.
//!
//! # Why the delete comes after the sweep
//!
//! A key is deleted only once its target has been swept, so a process that
//! dies mid-fire leaves the key behind and the next seed retries it. That is
//! at-least-once firing over an idempotent sweep, which is the safe direction.
//! A sweep that fails outright re-arms its keys in memory a beat later, so a
//! struggling store is retried rather than spun on — and the keys it could
//! not collect are still durable if the retrying stops the hard way.
//!
//! It relies on one precondition: a deadline re-armed by a sweep is strictly
//! later than the `now` it swept at, so the key just written is never one of
//! the keys about to be deleted. Every re-armed deadline is `now` plus the
//! retry timeout or plus a task's ttl, and both are required to be positive —
//! `config::validate` holds the retry timeout above zero and `TaskAcquireData`
//! validates `ttl >= 1`.
//!
//! # Dependencies
//!
//! The timer queue, for what is armed; the store, to seed, to list for
//! `debug.tick`, and to collect fired keys; the applier, to sweep an origin
//! (`tick`) and to parse timer keys (`KeySpace`); and the [`ScheduleFirer`]
//! port, so a due schedule can be handed over without knowing what a schedule
//! is.
//!
//! # Dependants
//!
//! `main` spawns the wall-clock loop; `S3Server` calls [`Timerd::round`]
//! directly from `debug.tick` and pauses the loop from `debug.start`, so a
//! test on a synthetic clock and a server on a real one take the same path.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::core::Unavailable;

use super::applier::{ApplierPool, KeySpace, TimerEntry};
use super::store::Store;
use super::timer_queue::TimerQueue;

/// Something that can fire a due schedule.
///
/// A port rather than a direct dependency so the poller does not need to know
/// what a schedule is: it reads a key, sees a schedule id, and hands it over.
#[async_trait]
pub trait ScheduleFirer: Send + Sync {
    async fn fire(&self, id: &str, deadline: i64, now: i64) -> Result<(), Unavailable>;
}

#[derive(Debug, Clone)]
pub struct TimerdCfg {
    /// Keys read per shard per `debug.tick` round. Small: the nearest
    /// deadlines sort first, so a round that fills its batch simply leaves the
    /// rest for the next one.
    pub batch: usize,
}

impl Default for TimerdCfg {
    fn default() -> Self {
        Self { batch: 256 }
    }
}

/// How long a failed sweep's keys wait in memory before the loop looks again.
const RETRY_DELAY_MS: i64 = 1_000;

/// How long the loop naps between pause checks while `debug.start` holds it.
const PAUSED_NAP: Duration = Duration::from_millis(100);

pub struct Timerd {
    store: Arc<dyn Store>,
    applier: Arc<ApplierPool>,
    schedules: Option<Arc<dyn ScheduleFirer>>,
    queue: Arc<TimerQueue>,
    keys: KeySpace,
    cfg: TimerdCfg,
}

impl Timerd {
    pub fn new(
        store: Arc<dyn Store>,
        applier: Arc<ApplierPool>,
        schedules: Option<Arc<dyn ScheduleFirer>>,
        queue: Arc<TimerQueue>,
        keys: KeySpace,
        cfg: TimerdCfg,
    ) -> Self {
        Self {
            store,
            applier,
            schedules,
            queue,
            keys,
            cfg,
        }
    }

    /// Rebuild the queue from one full listing — the recovery read.
    ///
    /// The last process's queue died with it; the keys did not. Everything
    /// parseable is re-armed, so what was about to fire fires now and what was
    /// far off waits in memory as it did before. Returns how many keys were
    /// armed.
    pub async fn seed(&self) -> Result<usize, Unavailable> {
        let keys = self
            .store
            .list(&self.keys.timer_prefix(), usize::MAX)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        let mut armed = 0;
        for key in keys {
            match self.keys.parse_timer_key(&key) {
                Some(entry) => {
                    self.queue.arm(entry.deadline(), key);
                    armed += 1;
                }
                None => tracing::warn!(key = %key, "Unparseable timer key ignored"),
            }
        }
        Ok(armed)
    }

    /// Fire everything the queue says is due at `now`. No listing: this is
    /// the whole normal-operation read path, and it reads nothing.
    pub async fn fire_due(&self, now: i64) -> u64 {
        let due = self.queue.take_due(now);
        if due.is_empty() {
            return 0;
        }
        let grouped = self.group(due);
        self.sweep(grouped, now).await
    }

    /// One listing-driven sweep at `now`. Returns how many targets were swept.
    ///
    /// This is what `debug.tick` calls, with a synthetic clock and the firing
    /// loop paused, so the differential suite exercises the same sweep the
    /// loop performs.
    pub async fn round(&self, now: i64) -> Result<u64, Unavailable> {
        let due = self.due(now).await?;
        if due.is_empty() {
            return Ok(0);
        }
        Ok(self.sweep(due, now).await)
    }

    /// Sweep every target, then collect its keys. In that order, always.
    async fn sweep(&self, due: Vec<(TimerEntry, Vec<String>)>, now: i64) -> u64 {
        let mut swept = 0u64;
        for (entry, keys) in due {
            let outcome = match &entry {
                TimerEntry::Origin { origin, .. } => self.applier.tick(origin, now).await,
                TimerEntry::Schedule { id, deadline } => match &self.schedules {
                    Some(firer) => firer.fire(id, *deadline, now).await,
                    None => {
                        tracing::warn!(schedule_id = %id, "Schedule timer fired with no schedule service configured");
                        Ok(())
                    }
                },
            };
            match outcome {
                Ok(()) => {
                    swept += 1;
                    // Only now: a process that dies before this leaves the key
                    // for the next seed, which is the safe direction.
                    for key in keys {
                        if let Some(entry) = self.keys.parse_timer_key(&key) {
                            self.queue.disarm(entry.deadline(), &key);
                        }
                        if let Err(e) = self.store.delete(&key).await {
                            // Not collected, so re-armed: the entry becomes
                            // the collector, a beat later.
                            self.queue.arm(now + RETRY_DELAY_MS, key.clone());
                            tracing::debug!(key = %key, error = %e, "Fired timer key not collected; its armed entry will retry");
                        }
                    }
                }
                Err(e) => {
                    // Still due, so still armed — a beat later, so a store
                    // that is down is retried rather than spun on. The keys
                    // themselves were never deleted.
                    tracing::warn!(error = %e, "Timer sweep failed; keys re-armed to retry");
                    for key in keys {
                        self.queue.arm(now + RETRY_DELAY_MS, key);
                    }
                }
            }
        }
        swept
    }

    /// Every due target, with all of its due keys — the listing path.
    async fn due(&self, now: i64) -> Result<Vec<(TimerEntry, Vec<String>)>, Unavailable> {
        let prefixes: Vec<String> = (0..self.keys.timer_shards)
            .map(|shard| self.keys.timer_shard_prefix(shard))
            .collect();
        let listings = futures::future::join_all(
            prefixes
                .iter()
                .map(|prefix| self.store.list(prefix, self.cfg.batch)),
        )
        .await;
        let mut due = Vec::new();
        for listing in listings {
            for key in listing.map_err(|e| Unavailable::new(e.to_string()))? {
                match self.keys.parse_timer_key(&key) {
                    // The keys sort by deadline, so everything after a future
                    // one in its shard is also in the future — but shards
                    // interleave, so keep reading rather than breaking.
                    Some(entry) if entry.deadline() <= now => due.push(key),
                    Some(_) => {}
                    None => tracing::warn!(key = %key, "Unparseable timer key ignored"),
                }
            }
        }
        Ok(self.group(due))
    }

    /// Group keys by target, earliest deadline first per target.
    ///
    /// Grouping matters: an origin can own several due keys — one current, the
    /// rest orphaned by a crash between a commit and its cleanup — and it must
    /// be swept once and have all of them collected.
    fn group(&self, keys: Vec<String>) -> Vec<(TimerEntry, Vec<String>)> {
        // Keyed by target, holding the earliest due deadline for it and every
        // due key that names it. BTreeMap so a sweep is deterministic.
        let mut grouped: BTreeMap<String, (TimerEntry, Vec<String>)> = BTreeMap::new();
        for key in keys {
            let entry = match self.keys.parse_timer_key(&key) {
                Some(entry) => entry,
                None => {
                    tracing::warn!(key = %key, "Unparseable timer key ignored");
                    continue;
                }
            };
            let target = match &entry {
                TimerEntry::Origin { origin, .. } => format!("o:{origin}"),
                TimerEntry::Schedule { id, .. } => format!("s:{id}"),
            };
            let slot = grouped
                .entry(target)
                .or_insert_with(|| (entry.clone(), Vec::new()));
            // Fire at the earliest due deadline: a schedule's occurrence is
            // identified by it.
            if entry.deadline() < slot.0.deadline() {
                slot.0 = entry;
            }
            slot.1.push(key);
        }
        grouped.into_values().collect()
    }

    /// The wall-clock loop: seed once, then sleep until the nearest armed
    /// deadline and fire it. Paused while `paused` is set, which is what
    /// `debug.start` does: with the loop stopped, `debug.tick` is the only
    /// thing that moves time.
    pub fn spawn(
        self: Arc<Self>,
        paused: Arc<AtomicBool>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Recovery first: the queue died with the last process, the keys
            // did not. Nothing may fire until what survived is re-armed, so
            // this retries rather than proceeds.
            loop {
                match self.seed().await {
                    Ok(0) => break,
                    Ok(n) => {
                        tracing::info!(armed = n, "Timer queue seeded from the store");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Timer queue could not be seeded; retrying");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS as u64)) => {}
                            _ = shutdown.changed() => return,
                        }
                    }
                }
            }
            loop {
                let sleep_for = if paused.load(Ordering::SeqCst) {
                    PAUSED_NAP
                } else {
                    match self.queue.next_deadline() {
                        Some(at) => {
                            Duration::from_millis(
                                at.saturating_sub(crate::util::system_time_ms()).max(0) as u64,
                            )
                        }
                        // Nothing armed: sleep until an arm wakes us.
                        None => Duration::from_secs(3_600),
                    }
                };
                tokio::select! {
                    _ = tokio::time::sleep(sleep_for) => {}
                    _ = self.queue.armed_nearer() => {}
                    _ = shutdown.changed() => {
                        tracing::info!("Timer loop shutting down");
                        return;
                    }
                }
                if paused.load(Ordering::SeqCst) {
                    continue;
                }
                let now = crate::util::system_time_ms();
                match self.fire_due(now).await {
                    0 => {}
                    n => tracing::debug!(swept = n, "Timers fired"),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::PromiseState;
    use crate::kernel::state::Req;
    use crate::s3::applier::ApplierCfg;
    use crate::s3::cache::{DocCache, MemDocCache};
    use crate::s3::codec;
    use crate::s3::outbox::Outbox;
    use crate::s3::store::ObjectStoreAdapter;
    use serde_json::json;
    use std::sync::Mutex;

    const W: &str = "http://worker:9999";

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    struct Rig {
        store: Arc<dyn Store>,
        applier: Arc<ApplierPool>,
        outbox: Arc<Outbox>,
        timers: Arc<TimerQueue>,
    }

    fn rig() -> Rig {
        let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(16));
        let outbox = Arc::new(Outbox::new(None, "http://server"));
        outbox.set_paused(true);
        // One queue, as `S3Server::build` wires it.
        let timers = Arc::new(TimerQueue::new());
        let applier = Arc::new(ApplierPool::new(
            Arc::clone(&store),
            cache,
            Arc::clone(&outbox),
            Arc::clone(&timers),
            keys(),
            ApplierCfg::default(),
        ));
        Rig {
            store,
            applier,
            outbox,
            timers,
        }
    }

    fn timerd(rig: &Rig, firer: Option<Arc<dyn ScheduleFirer>>) -> Timerd {
        Timerd::new(
            Arc::clone(&rig.store),
            Arc::clone(&rig.applier),
            firer,
            Arc::clone(&rig.timers),
            keys(),
            TimerdCfg::default(),
        )
    }

    async fn create(rig: &Rig, origin: &str, id: &str, timeout_at: i64, now: i64) {
        let req = Req::PromiseCreate(
            serde_json::from_value(json!({
                "id": id, "timeoutAt": timeout_at, "param": {},
                "tags": { "resonate:target": W }
            }))
            .unwrap(),
        );
        assert_eq!(rig.applier.submit(origin, req, now).await.unwrap().status, 200);
    }

    async fn stored(rig: &Rig, origin: &str) -> Option<crate::kernel::state::OriginDoc> {
        let bytes = rig.store.get(&keys().doc_key(origin)).await.unwrap()?;
        Some(codec::decode(&bytes.0, origin).unwrap())
    }

    async fn timer_keys(rig: &Rig) -> Vec<String> {
        rig.store.list(&keys().timer_prefix(), 100).await.unwrap()
    }

    #[tokio::test]
    async fn a_round_with_nothing_armed_does_nothing() {
        let r = rig();
        assert_eq!(timerd(&r, None).round(1_000_000).await.unwrap(), 0);
    }

    // --- firing from memory -------------------------------------------------

    /// A store that refuses to be listed, so a test can prove the normal
    /// firing path never does.
    struct NoListing(Arc<dyn Store>);

    #[async_trait]
    impl Store for NoListing {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, super::super::store::Etag)>, super::super::store::StoreError> {
            self.0.get(key).await
        }
        async fn put_if_match(
            &self,
            key: &str,
            body: Vec<u8>,
            etag: &super::super::store::Etag,
        ) -> Result<super::super::store::Etag, super::super::store::StoreError> {
            self.0.put_if_match(key, body, etag).await
        }
        async fn put_if_none_match(
            &self,
            key: &str,
            body: Vec<u8>,
        ) -> Result<super::super::store::Etag, super::super::store::StoreError> {
            self.0.put_if_none_match(key, body).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<super::super::store::Etag, super::super::store::StoreError> {
            self.0.put(key, body).await
        }
        async fn delete(&self, key: &str) -> Result<(), super::super::store::StoreError> {
            self.0.delete(key).await
        }
        async fn list(&self, prefix: &str, _max_keys: usize) -> Result<Vec<String>, super::super::store::StoreError> {
            panic!("normal operation must not list, but listed {prefix}");
        }
    }

    #[tokio::test]
    async fn firing_what_is_due_reads_nothing_from_the_store() {
        let inner: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
        let r = {
            let store: Arc<dyn Store> = Arc::new(NoListing(Arc::clone(&inner)));
            let outbox = Arc::new(Outbox::new(None, "http://server"));
            outbox.set_paused(true);
            let timers = Arc::new(TimerQueue::new());
            let applier = Arc::new(ApplierPool::new(
                Arc::clone(&store),
                Arc::new(MemDocCache::new(16)),
                Arc::clone(&outbox),
                Arc::clone(&timers),
                keys(),
                ApplierCfg::default(),
            ));
            Rig {
                store,
                applier,
                outbox,
                timers,
            }
        };
        create(&r, "o", "o:a", 5_000, 0).await;
        assert_eq!(r.timers.next_deadline(), Some(5_000));

        // The whole fire — find, sweep, collect — without a single listing.
        assert_eq!(timerd(&r, None).fire_due(9_000).await, 1);
        let doc = {
            let bytes = inner.get(&keys().doc_key("o")).await.unwrap().unwrap();
            codec::decode(&bytes.0, "o").unwrap()
        };
        assert_eq!(doc.promises["o:a"].state, PromiseState::RejectedTimedout);
        assert!(r.timers.is_empty(), "nothing left armed");
        assert!(
            inner.list(&keys().timer_prefix(), 10).await.unwrap().is_empty(),
            "the fired key was collected"
        );
    }

    #[tokio::test]
    async fn firing_with_nothing_due_leaves_the_future_armed() {
        let r = rig();
        create(&r, "o", "o:a", 500_000, 0).await;
        assert_eq!(timerd(&r, None).fire_due(1_000).await, 0);
        assert_eq!(r.timers.len(), 1, "the future deadline stays armed");
    }

    #[tokio::test]
    async fn a_fire_that_re_arms_covers_the_new_deadline_in_memory() {
        let r = rig();
        create(&r, "o", "o:a", 500_000, 0).await;
        // The retry deadline (30_000) fires and re-dispatches; the applier
        // arms the next retry (60_000) as part of the sweep's own commit.
        assert_eq!(timerd(&r, None).fire_due(30_000).await, 1);
        assert_eq!(r.timers.next_deadline(), Some(60_000));
        assert_eq!(r.timers.len(), 1);
    }

    #[tokio::test]
    async fn seeding_rebuilds_the_queue_after_a_restart() {
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        r.store
            .put(&keys().sched_timer_key("s0", 8_000), Vec::new())
            .await
            .unwrap();

        // A new process: same store, empty queue.
        let restarted = Rig {
            store: Arc::clone(&r.store),
            applier: Arc::clone(&r.applier),
            outbox: Arc::clone(&r.outbox),
            timers: Arc::new(TimerQueue::new()),
        };
        let td = timerd(&restarted, None);
        assert_eq!(td.seed().await.unwrap(), 2);
        assert_eq!(restarted.timers.next_deadline(), Some(5_000));
        assert_eq!(restarted.timers.len(), 2);
        // And what was seeded fires.
        assert_eq!(td.fire_due(9_000).await, 2);
        assert!(timer_keys(&restarted).await.is_empty());
    }

    #[tokio::test]
    async fn a_failed_sweep_re_arms_its_keys_a_beat_later() {
        struct Failing;
        #[async_trait]
        impl ScheduleFirer for Failing {
            async fn fire(&self, _: &str, _: i64, _: i64) -> Result<(), Unavailable> {
                Err(Unavailable::new("store down"))
            }
        }
        let r = rig();
        let key = keys().sched_timer_key("s0", 4_000);
        r.store.put(&key, Vec::new()).await.unwrap();
        r.timers.arm(4_000, key.clone());

        let td = timerd(&r, Some(Arc::new(Failing)));
        assert_eq!(td.fire_due(9_000).await, 0);
        // Still armed — but a beat later, so a dead store is not spun on.
        assert_eq!(r.timers.next_deadline(), Some(9_000 + 1_000));
        assert_eq!(timer_keys(&r).await, vec![key], "the key was never deleted");
    }

    #[tokio::test]
    async fn a_deadline_in_the_future_is_left_alone() {
        let r = rig();
        create(&r, "o", "o:a", 500_000, 0).await;
        let armed = timer_keys(&r).await;
        assert_eq!(armed.len(), 1);
        assert_eq!(timerd(&r, None).round(1_000).await.unwrap(), 0);
        assert_eq!(timer_keys(&r).await, armed);
    }

    #[tokio::test]
    async fn a_due_deadline_sweeps_its_origin_and_collects_the_key() {
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        // The promise deadline is nearer than the retry deadline, so the armed
        // key is at 5_000.
        assert_eq!(timerd(&r, None).round(9_000).await.unwrap(), 1);
        let doc = stored(&r, "o").await.expect("document");
        assert_eq!(doc.promises["o:a"].state, PromiseState::RejectedTimedout);
        assert!(timer_keys(&r).await.is_empty(), "the key was collected");
    }

    #[tokio::test]
    async fn a_retry_deadline_re_dispatches_and_re_arms() {
        let r = rig();
        create(&r, "o", "o:a", 500_000, 0).await;
        assert_eq!(timerd(&r, None).round(30_000).await.unwrap(), 1);
        let doc = stored(&r, "o").await.expect("document");
        assert_eq!(doc.tasks["o:a"].retry_at, Some(60_000));
        // One key again, now at the new deadline — the old one was collected.
        let armed = timer_keys(&r).await;
        assert_eq!(armed.len(), 1);
        assert!(armed[0].contains(&format!("{:020}", 60_000)));
        assert_eq!(r.outbox.snapshot().len(), 1, "the task was re-dispatched");
    }

    #[tokio::test]
    async fn sweeping_twice_at_the_same_instant_is_idempotent() {
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        let td = timerd(&r, None);
        assert_eq!(td.round(9_000).await.unwrap(), 1);
        let after_first = stored(&r, "o").await.unwrap();
        // Nothing is armed any more, so the second round finds nothing.
        assert_eq!(td.round(9_000).await.unwrap(), 0);
        assert_eq!(stored(&r, "o").await.unwrap(), after_first);
    }

    #[tokio::test]
    async fn two_pollers_over_one_store_both_sweep_safely() {
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        let a = timerd(&r, None);
        let b = timerd(&r, None);
        let (ra, rb) = tokio::join!(a.round(9_000), b.round(9_000));
        // Both may fire; the sweep is idempotent and one conditional write
        // wins, so the outcome is the same either way.
        assert!(ra.unwrap() + rb.unwrap() >= 1);
        let doc = stored(&r, "o").await.unwrap();
        assert_eq!(doc.promises["o:a"].state, PromiseState::RejectedTimedout);
    }

    #[tokio::test]
    async fn an_orphaned_key_is_collected_along_with_the_current_one() {
        // A crash between a commit and its cleanup leaves a stale key. The
        // origin must be swept once and both keys collected.
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        for stale in [1_000, 2_000] {
            r.store
                .put(&keys().timer_key("o", stale), Vec::new())
                .await
                .unwrap();
        }
        assert_eq!(timer_keys(&r).await.len(), 3);
        assert_eq!(
            timerd(&r, None).round(9_000).await.unwrap(),
            1,
            "one origin, one sweep"
        );
        assert!(timer_keys(&r).await.is_empty());
    }

    #[tokio::test]
    async fn a_key_for_an_origin_with_no_document_is_collected() {
        let r = rig();
        r.store
            .put(&keys().timer_key("ghost", 100), Vec::new())
            .await
            .unwrap();
        assert_eq!(timerd(&r, None).round(9_000).await.unwrap(), 1);
        assert!(timer_keys(&r).await.is_empty());
        // Sweeping an absent document writes nothing, by the write law.
        assert!(stored(&r, "ghost").await.is_none());
    }

    #[tokio::test]
    async fn every_shard_is_read() {
        // Origins land on different shards by construction; a round that only
        // read one prefix would miss most of them.
        let r = rig();
        let mut origins = Vec::new();
        for n in 0..12 {
            let origin = format!("origin-{n}");
            create(&r, &origin, &format!("{origin}:a"), 5_000, 0).await;
            origins.push(origin);
        }
        let shards: std::collections::HashSet<String> = timer_keys(&r)
            .await
            .iter()
            .map(|k| k[..k.rfind('/').unwrap()].to_string())
            .collect();
        assert!(shards.len() > 1, "test is only meaningful across shards");
        assert_eq!(timerd(&r, None).round(9_000).await.unwrap(), 12);
        for origin in &origins {
            assert_eq!(
                stored(&r, origin).await.unwrap().promises[&format!("{origin}:a")].state,
                PromiseState::RejectedTimedout
            );
        }
    }

    #[tokio::test]
    async fn a_schedule_key_is_handed_to_the_schedule_service() {
        struct Recording {
            fired: Mutex<Vec<(String, i64, i64)>>,
        }
        #[async_trait]
        impl ScheduleFirer for Recording {
            async fn fire(&self, id: &str, deadline: i64, now: i64) -> Result<(), Unavailable> {
                self.fired
                    .lock()
                    .unwrap()
                    .push((id.to_string(), deadline, now));
                Ok(())
            }
        }
        let r = rig();
        let firer = Arc::new(Recording {
            fired: Mutex::new(Vec::new()),
        });
        r.store
            .put(&keys().sched_timer_key("s0", 4_000), Vec::new())
            .await
            .unwrap();
        let td = timerd(&r, Some(Arc::clone(&firer) as Arc<dyn ScheduleFirer>));
        assert_eq!(td.round(9_000).await.unwrap(), 1);
        assert_eq!(
            *firer.fired.lock().unwrap(),
            vec![("s0".to_string(), 4_000, 9_000)]
        );
        assert!(timer_keys(&r).await.is_empty());
    }

    #[tokio::test]
    async fn a_schedule_that_fails_to_fire_keeps_its_key() {
        struct Failing;
        #[async_trait]
        impl ScheduleFirer for Failing {
            async fn fire(&self, _: &str, _: i64, _: i64) -> Result<(), Unavailable> {
                Err(Unavailable::new("store down"))
            }
        }
        let r = rig();
        r.store
            .put(&keys().sched_timer_key("s0", 4_000), Vec::new())
            .await
            .unwrap();
        let td = timerd(&r, Some(Arc::new(Failing)));
        assert_eq!(td.round(9_000).await.unwrap(), 0);
        assert_eq!(timer_keys(&r).await.len(), 1, "still due, so still there");
    }

    #[tokio::test]
    async fn a_schedule_key_with_no_service_is_reported_and_collected() {
        let r = rig();
        r.store
            .put(&keys().sched_timer_key("s0", 4_000), Vec::new())
            .await
            .unwrap();
        assert_eq!(timerd(&r, None).round(9_000).await.unwrap(), 1);
        assert!(timer_keys(&r).await.is_empty());
    }

    #[tokio::test]
    async fn an_unparseable_key_is_ignored_not_fatal() {
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        r.store
            .put(&format!("{}/00/rubbish", keys().timer_prefix()), Vec::new())
            .await
            .unwrap();
        assert_eq!(timerd(&r, None).round(9_000).await.unwrap(), 1);
        // The good key was collected; the rubbish is left rather than guessed
        // at.
        assert_eq!(timer_keys(&r).await.len(), 1);
    }

    /// Wait until `condition` holds, or fail after a couple of seconds.
    async fn eventually<F, Fut>(mut condition: F, what: &str)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..200 {
            if condition().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{what}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_loop_does_nothing_while_paused_and_fires_once_resumed() {
        let r = rig();
        // A deadline already in wall-clock terms overdue.
        create(&r, "o", "o:a", 5_000, 0).await;
        let paused = Arc::new(AtomicBool::new(true));
        let (tx, rx) = tokio::sync::watch::channel(false);
        let td = Arc::new(timerd(&r, None));
        let handle = Arc::clone(&td).spawn(Arc::clone(&paused), rx);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(timer_keys(&r).await.len(), 1, "paused, so nothing swept");

        paused.store(false, Ordering::SeqCst);
        eventually(
            || async { timer_keys(&r).await.is_empty() },
            "resumed, so the overdue key must be swept",
        )
        .await;

        let _ = tx.send(true);
        handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_loop_seeds_from_the_store_before_firing() {
        // Keys written by a previous process: nothing has armed this rig's
        // queue, so only the seed can know about them.
        let r = rig();
        r.store
            .put(&keys().timer_key("ghost", 100), Vec::new())
            .await
            .unwrap();
        assert!(r.timers.is_empty());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let handle = Arc::new(timerd(&r, None)).spawn(Arc::new(AtomicBool::new(false)), rx);
        eventually(
            || async { timer_keys(&r).await.is_empty() },
            "the seeded key must fire and be collected",
        )
        .await;
        let _ = tx.send(true);
        handle.await.unwrap();
    }
}
