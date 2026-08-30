//! The in-memory mirror of the timer prefix.
//!
//! # Contract
//!
//! Every timer key written to the store is armed here by its writer,
//! *after* the PUT succeeds — durability first, always. In normal operation
//! finding what is due therefore costs no store operation at all: the firing
//! loop consults the queue, not the bucket. The queue is never authoritative —
//! the store is. An entry with no key behind it fires a sweep that finds
//! nothing due and deletes a key that is already gone, both idempotent; a key
//! with no entry (the process died between writing and firing) is found by
//! seeding the queue from one full listing at startup. Losing the process
//! loses the queue and nothing else.
//!
//! The ordering key is a wall-clock deadline in ms, the same value the store
//! key carries — except when a sweep fails and the key is re-armed a beat
//! later: the memory deadline then only says when to *look* again, while the
//! key still says what is due. [`TimerQueue::arm`] wakes the firing loop
//! whenever the nearest deadline moves closer, so a fresh deadline never waits
//! out an unrelated sleep.
//!
//! # Dependencies
//!
//! `tokio::sync::Notify`, and the `resonate_timer_queue_len` gauge — the queue
//! is the process's one workload-proportional structure, so its size is the
//! number to watch. No store handle, no kernel, no keys' meanings.
//!
//! # Dependants
//!
//! The applier and the schedule service arm and disarm around their timer-key
//! writes; the timer service takes what is due, seeds after a restart, and
//! re-arms what it could not sweep. `S3Server::build` creates the one shared
//! instance.

use std::collections::BTreeSet;
use std::sync::Mutex;

use tokio::sync::Notify;

/// Armed timer keys, nearest deadline first.
pub struct TimerQueue {
    armed: Mutex<BTreeSet<(i64, String)>>,
    /// Signalled when [`TimerQueue::arm`] moves the nearest deadline closer.
    nearer: Notify,
}

impl TimerQueue {
    pub fn new() -> Self {
        Self {
            armed: Mutex::new(BTreeSet::new()),
            nearer: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeSet<(i64, String)>> {
        // A panic while holding the lock cannot corrupt anything durable: the
        // store is the authority, and a wrong entry costs one idempotent
        // no-op sweep.
        self.armed.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Report the size while still holding the lock, so the gauge never
    /// interleaves with another mutation.
    fn observe(armed: &BTreeSet<(i64, String)>) {
        crate::metrics::TIMER_QUEUE_LEN.set(armed.len() as i64);
    }

    /// Record an armed timer key. Call only after the key is durable in the
    /// store, never before, so the queue can only under-promise.
    pub fn arm(&self, deadline: i64, key: impl Into<String>) {
        let key = key.into();
        let mut armed = self.lock();
        let nearer = armed.first().is_none_or(|(head, _)| deadline < *head);
        armed.insert((deadline, key));
        Self::observe(&armed);
        drop(armed);
        if nearer {
            self.nearer.notify_one();
        }
    }

    /// Forget a key whose object was deleted. Forgetting what is not armed is
    /// fine — the store is the authority, not the queue.
    pub fn disarm(&self, deadline: i64, key: &str) {
        let mut armed = self.lock();
        // Borrowed lookup keys for a BTreeSet of pairs need an owned tuple.
        armed.remove(&(deadline, key.to_string()));
        Self::observe(&armed);
    }

    /// Remove and return every key due at `now`, nearest deadline first.
    pub fn take_due(&self, now: i64) -> Vec<String> {
        let mut armed = self.lock();
        // Everything at `now` sorts before (now + 1, ""), whatever its key.
        let future = armed.split_off(&(now.saturating_add(1), String::new()));
        let due = std::mem::replace(&mut *armed, future);
        Self::observe(&armed);
        due.into_iter().map(|(_, key)| key).collect()
    }

    /// The nearest armed deadline — how long the firing loop may sleep.
    pub fn next_deadline(&self) -> Option<i64> {
        self.lock().first().map(|(at, _)| *at)
    }

    /// Resolves when [`TimerQueue::arm`] brings the nearest deadline closer —
    /// the firing loop's wake-up call.
    pub async fn armed_nearer(&self) {
        self.nearer.notified().await;
    }

    /// Forget everything. `debug.reset` deletes the keys out from under the
    /// queue, so it has to say so.
    pub fn clear(&self) {
        let mut armed = self.lock();
        armed.clear();
        Self::observe(&armed);
    }

    /// Entries armed. Diagnostics and tests.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for TimerQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn taking_what_is_due_leaves_the_future_armed() {
        let q = TimerQueue::new();
        q.arm(300, "t/00/300_c");
        q.arm(100, "t/00/100_a");
        q.arm(200, "t/01/200_b");
        assert_eq!(q.take_due(200), vec!["t/00/100_a", "t/01/200_b"]);
        assert_eq!(q.len(), 1);
        assert_eq!(q.next_deadline(), Some(300));
        assert!(q.take_due(200).is_empty(), "taken means gone");
    }

    #[test]
    fn the_same_key_armed_twice_is_one_entry() {
        let q = TimerQueue::new();
        q.arm(100, "t/00/100_a");
        q.arm(100, "t/00/100_a");
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn disarming_forgets_the_entry_and_tolerates_the_unknown() {
        let q = TimerQueue::new();
        q.arm(100, "t/00/100_a");
        q.disarm(100, "t/00/100_a");
        assert!(q.is_empty());
        assert_eq!(q.next_deadline(), None);
        q.disarm(999, "never-armed");
    }

    #[test]
    fn clearing_forgets_everything() {
        let q = TimerQueue::new();
        q.arm(100, "a");
        q.arm(200, "b");
        q.clear();
        assert!(q.is_empty());
    }

    #[tokio::test]
    async fn arming_a_nearer_deadline_wakes_the_sleeper() {
        let q = Arc::new(TimerQueue::new());
        // The first arm of an empty queue moves the head, so it stores a wake.
        q.arm(5_000, "t/00/5000_a");
        tokio::time::timeout(Duration::from_secs(1), q.armed_nearer())
            .await
            .expect("the first arm wakes");

        let sleeper = {
            let q = Arc::clone(&q);
            tokio::spawn(async move { q.armed_nearer().await })
        };
        // A later deadline changes nothing the sleeper cares about.
        q.arm(9_000, "t/00/9000_b");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!sleeper.is_finished(), "a farther deadline does not wake");

        q.arm(1_000, "t/00/1000_c");
        tokio::time::timeout(Duration::from_secs(1), sleeper)
            .await
            .expect("a nearer deadline wakes")
            .unwrap();
    }
}
