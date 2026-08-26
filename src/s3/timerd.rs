//! The timer poller: find what is due, sweep it, collect the keys.
//!
//! Deadlines live as empty objects under a timer prefix, keyed by
//! zero-padded deadline then target. Lexicographic order is therefore time
//! order, so a capped `ListObjectsV2` returns the nearest deadlines first and
//! finding what is due costs one small, strongly-consistent listing per shard —
//! no index, no scan, no second store.
//!
//! **The deadline in the key is the fence.** A timer write is an unconditional
//! PUT, because the key carries everything the write means: arming the same
//! deadline twice is the same object, and arming a different one is a different
//! object. Nothing can be lost by a racing writer. Deletes are free, so
//! collecting an orphan costs nothing.
//!
//! Multi-node safe with no coordination at all: two pollers may both see the
//! same due key and both sweep it. One conditional write lands, the other is
//! refused and re-decides into a no-op, and the sweep is idempotent to begin
//! with.
//!
//! # Why the delete comes after the sweep
//!
//! A key is deleted only once its target has been swept, so a poller that dies
//! mid-round leaves the key behind and the next round retries it. That is
//! at-least-once firing over an idempotent sweep, which is the safe direction.
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
//! The store, to list the timer shards and collect fired keys; the applier, to
//! sweep an origin (`tick`) and to parse timer keys (`KeySpace`); and the
//! [`ScheduleFirer`] port, so the poller can hand a due schedule over without
//! knowing what a schedule is. The poller holds no state of its own — every
//! round starts from a listing.
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
    /// How often the wall-clock loop sweeps.
    pub poll_interval: Duration,
    /// Keys read per shard per round. Small: the nearest deadlines sort first,
    /// so a round that fills its batch simply leaves the rest for the next one.
    pub batch: usize,
}

impl Default for TimerdCfg {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(1_000),
            batch: 256,
        }
    }
}

pub struct Timerd {
    store: Arc<dyn Store>,
    applier: Arc<ApplierPool>,
    schedules: Option<Arc<dyn ScheduleFirer>>,
    keys: KeySpace,
    cfg: TimerdCfg,
}

impl Timerd {
    pub fn new(
        store: Arc<dyn Store>,
        applier: Arc<ApplierPool>,
        schedules: Option<Arc<dyn ScheduleFirer>>,
        keys: KeySpace,
        cfg: TimerdCfg,
    ) -> Self {
        Self {
            store,
            applier,
            schedules,
            keys,
            cfg,
        }
    }

    /// One sweep at `now`. Returns how many targets were swept.
    ///
    /// This is the whole body of the background loop, and it is what
    /// `debug.tick` calls — so a test driving a synthetic clock and a server
    /// driving a real one take exactly the same path.
    pub async fn round(&self, now: i64) -> Result<u64, Unavailable> {
        let due = self.due(now).await?;
        if due.is_empty() {
            return Ok(0);
        }
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
                    // Only now: a poller that dies before this leaves the key
                    // for the next round, which is the safe direction.
                    for key in keys {
                        if let Err(e) = self.store.delete(&key).await {
                            tracing::debug!(key = %key, error = %e, "Fired timer key not collected; a later round will retry");
                        }
                    }
                }
                Err(e) => {
                    // Leave the keys: whatever is wrong, the deadline is still
                    // due and must fire eventually.
                    tracing::warn!(error = %e, "Timer sweep failed; keys left for the next round");
                }
            }
        }
        Ok(swept)
    }

    /// Every due target, with all of its due keys.
    ///
    /// Grouping matters: an origin can own several due keys — one current, the
    /// rest orphaned by a crash between a commit and its cleanup — and it must
    /// be swept once and have all of them collected.
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

        // Keyed by target, holding the earliest due deadline for it and every
        // due key that names it. BTreeMap so a round is deterministic.
        let mut grouped: BTreeMap<String, (TimerEntry, Vec<String>)> = BTreeMap::new();
        for listing in listings {
            for key in listing.map_err(|e| Unavailable::new(e.to_string()))? {
                let entry = match self.keys.parse_timer_key(&key) {
                    Some(entry) => entry,
                    None => {
                        tracing::warn!(key = %key, "Unparseable timer key ignored");
                        continue;
                    }
                };
                if entry.deadline() > now {
                    // The keys sort by deadline, so everything after this one in
                    // this shard is also in the future — but shards interleave,
                    // so keep reading rather than breaking.
                    continue;
                }
                let target = match &entry {
                    TimerEntry::Origin { origin, .. } => format!("o:{origin}"),
                    TimerEntry::Schedule { id, .. } => format!("s:{id}"),
                };
                let slot = grouped.entry(target).or_insert_with(|| (entry.clone(), Vec::new()));
                // Fire at the earliest due deadline: a schedule's occurrence is
                // identified by it.
                if entry.deadline() < slot.0.deadline() {
                    slot.0 = entry;
                }
                slot.1.push(key);
            }
        }
        Ok(grouped.into_values().collect())
    }

    /// The wall-clock loop. Paused while `paused` is set, which is what
    /// `debug.start` does: with the loop stopped, `debug.tick` is the only
    /// thing that moves time.
    pub fn spawn(
        self: Arc<Self>,
        paused: Arc<AtomicBool>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(self.cfg.poll_interval) => {}
                    _ = shutdown.changed() => {
                        tracing::info!("Timer poller shutting down");
                        return;
                    }
                }
                if paused.load(Ordering::SeqCst) {
                    continue;
                }
                let now = crate::util::system_time_ms();
                match self.round(now).await {
                    Ok(0) => {}
                    Ok(n) => tracing::debug!(swept = n, "Timer round"),
                    Err(e) => tracing::error!(error = %e, "Timer round failed"),
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
    }

    fn rig() -> Rig {
        let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(16));
        let outbox = Arc::new(Outbox::new(None, "http://server"));
        outbox.set_paused(true);
        let applier = Arc::new(ApplierPool::new(
            Arc::clone(&store),
            cache,
            Arc::clone(&outbox),
            keys(),
            ApplierCfg::default(),
        ));
        Rig {
            store,
            applier,
            outbox,
        }
    }

    fn timerd(rig: &Rig, firer: Option<Arc<dyn ScheduleFirer>>) -> Timerd {
        Timerd::new(
            Arc::clone(&rig.store),
            Arc::clone(&rig.applier),
            firer,
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

    #[tokio::test]
    async fn the_loop_does_nothing_while_paused() {
        let r = rig();
        create(&r, "o", "o:a", 5_000, 0).await;
        let paused = Arc::new(AtomicBool::new(true));
        let (tx, rx) = tokio::sync::watch::channel(false);
        let td = Arc::new(Timerd::new(
            Arc::clone(&r.store),
            Arc::clone(&r.applier),
            None,
            keys(),
            TimerdCfg {
                poll_interval: Duration::from_millis(5),
                batch: 16,
            },
        ));
        let handle = Arc::clone(&td).spawn(Arc::clone(&paused), rx);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(timer_keys(&r).await.len(), 1, "paused, so nothing swept");

        paused.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(timer_keys(&r).await.is_empty(), "resumed and swept");

        let _ = tx.send(true);
        handle.await.unwrap();
    }
}
