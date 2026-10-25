//! The wheel the timer runs: [`spec::spec_merge`](crate::spec) over an ordered
//! map and a hash index, so arming a deadline costs `O(log n)`.
//!
//! [`TimerWheel`](crate::TimerWheel) is the proved implementation. It keeps
//! the wheel in a flat `Vec` and rebuilds it on every merge, which is
//! quadratic in the wheel's size: the untouched entries are moved out one
//! `Vec::remove(0)` at a time, and the union is insertion-sorted back in. At
//! the near-horizon sizes it was written for that did not matter. Under load
//! it does. A server arms one or two deadlines per request, a wheel of 8192
//! entries costs tens of milliseconds per merge, and the timer task falls so
//! far behind its queue that deadlines fire many seconds late.
//!
//! This module computes the same function differently, which is the freedom
//! the specification was written to leave open (see the crate README, "Cost"):
//!
//! ```text
//! drop      each identity the batch names: one hash lookup, one tree removal
//! add       the batch's last update per identity: one tree insertion each
//! sort      the tree is the order
//! cut       pop the tree's last entry while over capacity
//! ```
//!
//! # Why the order is the same
//!
//! Entries are keyed in the tree by `(deadline, seq)`, where `seq` grows with
//! every insertion. The proved wheel orders by deadline and breaks ties by
//! arrival: `spec_insert` places a newcomer after everything due no later
//! than it, and untouched entries come before the batch in the union. So in
//! both, among equal deadlines, the entry that arrived (or was last moved)
//! earlier comes first. Within one batch the surviving updates arrive in batch
//! order, and they get increasing `seq` in that order. The two are therefore
//! the same sequence after every merge, every cut and every `pop_expired`.
//!
//! # What is and is not proved
//!
//! Verus does not see this module (it is gated on `verus_keep_ghost`, like the
//! timer), so nothing here is proved. It is tied to the proved wheel the other
//! way: `tests/indexed.rs` drives both with the same operations and asserts
//! that they hold the same sequence after every step, exhaustively over small
//! wheels and pseudorandomly over large ones. A divergence anywhere in the
//! spec's four steps (drop, last-update-wins, stable order, cut) fails it.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use crate::comparator::Comparator;
use crate::timeout::Timeout;

/// A [`Comparator`] that can also name an identity as a hashable key.
///
/// The obligation is the comparator's, restated for keys: `key(a) == key(b)`
/// exactly when `eq(a, b)`. An implementation that breaks it makes the wheel
/// treat two equivalent timeouts as different ones, or the reverse.
pub trait KeyedComparator<T>: Comparator<T> {
    /// The identity of a payload.
    type Key: Hash + Eq + Clone;

    /// The identity of `value`. Must agree with [`Comparator::eq`].
    fn key(&self, value: &T) -> Self::Key;
}

impl<T: crate::comparator::HasId> KeyedComparator<T> for crate::comparator::IdComparator {
    type Key = u64;

    fn key(&self, value: &T) -> u64 {
        value.id()
    }
}

/// A bounded, deduplicating timer wheel with `O(log n)` arming.
///
/// The same behaviour as [`TimerWheel`](crate::TimerWheel), which is the
/// specification it is tested against: ordered by deadline with ties in
/// arrival order, one entry per identity with the last update winning, and at
/// most `capacity` entries, the farthest future cut first.
///
/// | operation | cost, wheel of `n`, batch of `m`, `k` returned or cut |
/// | --- | --- |
/// | `merge` | `O(m log n + k log n)` |
/// | `pop_expired` | `O(k log n)` |
/// | `next` | `O(log n)` |
pub struct IndexedWheel<T, C: KeyedComparator<T>> {
    cmp: C,
    cap: usize,
    /// The next tie-breaker. Strictly increasing, so a later arrival sorts
    /// behind an earlier one with the same deadline.
    seq: u64,
    /// The wheel, in order: `(deadline, seq)` to the entry.
    order: BTreeMap<(u64, u64), (C::Key, T)>,
    /// Where each identity sits in `order`.
    index: HashMap<C::Key, (u64, u64)>,
}

impl<T, C: KeyedComparator<T>> IndexedWheel<T, C> {
    /// An empty wheel holding at most `capacity` entries.
    pub fn new(capacity: usize, cmp: C) -> Self {
        Self {
            cmp,
            cap: capacity,
            seq: 0,
            order: BTreeMap::new(),
            index: HashMap::new(),
        }
    }

    /// How many entries the wheel holds.
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Whether the wheel holds nothing.
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// The most entries the wheel will hold.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Whether the wheel holds `capacity` entries.
    pub fn is_full(&self) -> bool {
        self.order.len() == self.cap
    }

    /// The nearest deadline, without removing anything. Nothing in the wheel
    /// is due sooner.
    pub fn next(&self) -> Option<u64> {
        self.order
            .first_key_value()
            .map(|(&(deadline, _), _)| deadline)
    }

    /// The entries in order, nearest first.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &T)> {
        self.order
            .iter()
            .map(|(&(deadline, _), (_, value))| (deadline, value))
    }

    /// Merge a batch of arrivals: [`spec_merge`](crate::spec), as
    /// [`TimerWheel::merge`](crate::TimerWheel::merge) does it.
    ///
    /// Every entry whose identity the batch names is dropped, the batch's last
    /// update per identity is added, and the wheel is cut back to capacity
    /// from the far end.
    pub fn merge(&mut self, incoming: Vec<Timeout<T>>) {
        if incoming.is_empty() {
            return;
        }
        let keyed: Vec<(C::Key, Timeout<T>)> = incoming
            .into_iter()
            .map(|t| (self.cmp.key(&t.value), t))
            .collect();

        // The position of each identity's last arrival. A one-entry batch,
        // the request path's usual shape, needs no map.
        let last: Option<HashMap<C::Key, usize>> = (keyed.len() > 1).then(|| {
            let mut last = HashMap::with_capacity(keyed.len());
            for (i, (k, _)) in keyed.iter().enumerate() {
                last.insert(k.clone(), i);
            }
            last
        });

        // drop: every entry the batch mentions. All of them before anything
        // is added, so an arrival never displaces a sibling from its batch.
        for (k, _) in &keyed {
            if let Some(at) = self.index.remove(k) {
                self.order.remove(&at);
            }
        }

        // add: the last update per identity, in batch order.
        for (i, (k, t)) in keyed.into_iter().enumerate() {
            if last.as_ref().is_some_and(|last| last[&k] != i) {
                continue;
            }
            let at = (t.deadline, self.seq);
            self.seq += 1;
            self.index.insert(k.clone(), at);
            self.order.insert(at, (k, t.value));
        }

        // cut: keep the first `capacity`.
        while self.order.len() > self.cap {
            if let Some((_, (k, _))) = self.order.pop_last() {
                self.index.remove(&k);
            }
        }
    }

    /// A one-entry merge.
    pub fn insert(&mut self, t: Timeout<T>) {
        self.merge(vec![t]);
    }

    /// Remove and return everything due at or before `now`, nearest first.
    ///
    /// What comes back plus what stays is what was there; everything returned
    /// is due and nothing retained is.
    pub fn pop_expired(&mut self, now: u64) -> Vec<Timeout<T>> {
        if self.next().is_none_or(|d| now < d) {
            return Vec::new();
        }
        let expired = match now.checked_add(1) {
            Some(bound) => {
                let rest = self.order.split_off(&(bound, 0));
                std::mem::replace(&mut self.order, rest)
            }
            None => std::mem::take(&mut self.order),
        };
        let mut out = Vec::with_capacity(expired.len());
        for ((deadline, _), (k, value)) in expired {
            self.index.remove(&k);
            out.push(Timeout::new(deadline, value));
        }
        out
    }
}
