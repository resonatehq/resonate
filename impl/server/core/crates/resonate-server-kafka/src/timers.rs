//! A partition's timer index: one deadline per origin and per schedule, in
//! memory only.
//!
//! # Contract
//!
//! Nothing here is durable, and nothing needs to be. Every deadline is a field
//! of a record — a promise's timeout, a task's retry or lease, a schedule's
//! next run — and records are committed atomically with the state that arms
//! them. So the index is rebuilt from the records on takeover and kept current
//! after every round; losing it loses nothing.
//!
//! One entry per target, at the target's earliest deadline. Setting a target
//! replaces its entry; [`Timers::take_due`] removes what it returns, and the
//! round that sweeps a target sets it again from the state it leaves behind —
//! including a sweep that changed nothing, so an entry that fired early is
//! re-armed rather than forgotten.
//!
//! # Dependants
//!
//! The partition shell sets entries after every round and seeds them on
//! takeover; its timer loop and `debug.tick` take what is due.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use tokio::sync::Notify;

/// What a deadline belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Target {
    Origin(String),
    Schedule(String),
}

#[derive(Default)]
struct Inner {
    by_time: BTreeSet<(i64, Target)>,
    by_target: HashMap<Target, i64>,
}

#[derive(Default)]
pub struct Timers {
    inner: Mutex<Inner>,
    nearer: Notify,
}

impl Timers {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Arm `target` at `at`, or disarm it with `None`.
    pub fn set(&self, target: Target, at: Option<i64>) {
        let mut inner = self.lock();
        if let Some(old) = inner.by_target.remove(&target) {
            inner.by_time.remove(&(old, target.clone()));
        }
        let Some(at) = at else {
            return;
        };
        let nearer = inner.by_time.first().is_none_or(|(head, _)| at < *head);
        inner.by_time.insert((at, target.clone()));
        inner.by_target.insert(target, at);
        drop(inner);
        if nearer {
            self.nearer.notify_one();
        }
    }

    /// Remove and return everything due at `now`, earliest first.
    pub fn take_due(&self, now: i64) -> Vec<(i64, Target)> {
        let mut inner = self.lock();
        let mut due = Vec::new();
        while let Some((at, _)) = inner.by_time.first() {
            if *at > now {
                break;
            }
            let (at, target) = inner.by_time.pop_first().expect("non-empty");
            inner.by_target.remove(&target);
            due.push((at, target));
        }
        due
    }

    /// The earliest armed deadline.
    pub fn next_deadline(&self) -> Option<i64> {
        self.lock().by_time.first().map(|(at, _)| *at)
    }

    /// Resolves when [`Timers::set`] brings the earliest deadline closer.
    pub async fn armed_nearer(&self) {
        self.nearer.notified().await;
    }

    pub fn clear(&self) {
        *self.lock() = Inner::default();
    }

    pub fn len(&self) -> usize {
        self.lock().by_target.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(s: &str) -> Target {
        Target::Origin(s.into())
    }

    #[test]
    fn one_entry_per_target_at_its_latest_setting() {
        let t = Timers::new();
        t.set(o("a"), Some(300));
        t.set(o("a"), Some(100));
        t.set(o("b"), Some(200));
        assert_eq!(t.len(), 2);
        assert_eq!(t.next_deadline(), Some(100));
        t.set(o("a"), None);
        assert_eq!(t.next_deadline(), Some(200));
    }

    #[test]
    fn taking_what_is_due_removes_only_that() {
        let t = Timers::new();
        t.set(o("a"), Some(100));
        t.set(Target::Schedule("s".into()), Some(100));
        t.set(o("b"), Some(500));
        let due = t.take_due(100);
        assert_eq!(
            due,
            vec![(100, o("a")), (100, Target::Schedule("s".into()))]
        );
        assert_eq!(t.len(), 1);
        assert!(t.take_due(499).is_empty());
        assert_eq!(t.take_due(500), vec![(500, o("b"))]);
        assert!(t.is_empty());
    }
}
