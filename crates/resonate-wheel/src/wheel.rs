//! The wheel itself: a bounded, deduplicating, deadline-ordered set of timeouts.

use vstd::prelude::*;

use crate::comparator::*;
use crate::proof::*;
use crate::spec::*;
use crate::timeout::Timeout;

verus! {

/// A bounded set of pending [`Timeout`]s, ordered by deadline and deduplicated
/// by a [`Comparator`].
///
/// The wheel holds at most `capacity` timeouts, kept sorted with the nearest
/// deadline first, and never holds two timeouts the comparator calls the same.
/// [`merge`](TimerWheel::merge) folds a batch of arrivals in under those rules:
/// same identity replaces rather than duplicates, and when the union overflows
/// capacity it is the farthest-future timeouts that fall off.
///
/// The three properties above are not conventions — they are
/// [`TimerWheel::wf`], and every method that returns a wheel proves it.
///
/// # Cost
///
/// `merge` is `O((n + m)^2)` for a wheel of `n` holding a batch of `m`: each
/// arrival scans for its identity and again for its slot. The wheel is sized
/// for the near horizon — thousands of entries, not millions — and a flat
/// `Vec` beats a heap or a hash index at that size on the operation that
/// actually runs hot, which is walking the front in deadline order.
///
/// # Example
///
/// ```ignore
/// let mut w = TimerWheel::new(2, IdComparator);
/// let mut batch = Vec::new();
/// batch.push(Timeout::new(30, 1u64));
/// batch.push(Timeout::new(10, 2u64));
/// batch.push(Timeout::new(20, 3u64));
/// w.merge(batch);            // capacity 2 keeps deadlines 10 and 20
///
/// let mut moved = Vec::new();
/// moved.push(Timeout::new(5, 3u64));
/// w.merge(moved);            // id 3 moves 20 -> 5; it is not stored twice
/// ```
pub struct TimerWheel<T, C: Comparator<T>> {
    cmp: C,
    cap: usize,
    items: Vec<Timeout<T>>,
}

impl<T, C: Comparator<T>> View for TimerWheel<T, C> {
    type V = Seq<Timeout<T>>;

    closed spec fn view(&self) -> Seq<Timeout<T>> {
        self.items@
    }
}

impl<T, C: Comparator<T>> TimerWheel<T, C> {
    /// The comparator this wheel was built with. Ghost accessor.
    pub closed spec fn comparator(&self) -> C {
        self.cmp
    }

    /// The wheel's capacity. Ghost accessor; [`TimerWheel::capacity`] is the
    /// executable one.
    pub closed spec fn capacity_spec(&self) -> nat {
        self.cap as nat
    }

    /// **The wheel invariant**, in three conjuncts:
    ///
    /// - deadlines are non-decreasing, so index 0 is the next timeout to fire;
    /// - no two entries are the same logical timeout;
    /// - the wheel is within capacity.
    ///
    /// Every public method requires it and re-establishes it, so a wheel
    /// obtained from [`TimerWheel::new`] satisfies it forever.
    pub open spec fn wf(&self) -> bool {
        &&& sorted(self@)
        &&& distinct(self.comparator(), self@)
        &&& self@.len() <= self.capacity_spec()
    }

    /// An empty wheel holding at most `capacity` timeouts, identifying them
    /// with `cmp`.
    pub fn new(capacity: usize, cmp: C) -> (r: Self)
        ensures
            r.wf(),
            r@ == Seq::<Timeout<T>>::empty(),
            r.capacity_spec() == capacity,
            r.comparator() == cmp,
    {
        TimerWheel { cmp, cap: capacity, items: Vec::new() }
    }

    /// How many timeouts the wheel is holding.
    pub fn len(&self) -> (r: usize)
        ensures
            r == self@.len(),
    {
        self.items.len()
    }

    /// The most timeouts this wheel will ever hold.
    pub fn capacity(&self) -> (r: usize)
        ensures
            r == self.capacity_spec(),
    {
        self.cap
    }

    /// Whether the wheel is holding nothing.
    pub fn is_empty(&self) -> (r: bool)
        ensures
            r == (self@.len() == 0),
    {
        self.items.is_empty()
    }

    /// Whether the wheel is at capacity — the state in which a merge starts
    /// dropping far-future arrivals.
    pub fn is_full(&self) -> (r: bool)
        requires
            self.wf(),
        ensures
            r == (self@.len() == self.capacity_spec()),
    {
        self.items.len() == self.cap
    }

    /// The timeout due soonest, without removing it.
    pub fn peek(&self) -> (r: Option<&Timeout<T>>)
        ensures
            self@.len() == 0 ==> r is None,
            self@.len() > 0 ==> r is Some && r->Some_0 == self@[0],
    {
        if self.items.len() == 0 {
            None
        } else {
            Some(&self.items[0])
        }
    }

    // -----------------------------------------------------------------------
    // Scans. Both are linear and both establish exactly the side condition its
    // bridge lemma in `proof.rs` needs.
    // -----------------------------------------------------------------------

    /// The index of the entry that is the same logical timeout as `v`, or
    /// `len()` when the wheel holds no such entry.
    fn find_same(&self, v: &T) -> (r: usize)
        ensures
            r <= self@.len(),
            r < self@.len() ==> self.comparator().same(self@[r as int].value, *v),
            forall|j: int| 0 <= j < r ==> !self.comparator().same(#[trigger] self@[j].value, *v),
    {
        let mut i: usize = 0;
        while i < self.items.len()
            invariant
                i <= self.items@.len(),
                forall|j: int|
                    0 <= j < i ==> !self.cmp.same(#[trigger] self.items@[j].value, *v),
            decreases self.items@.len() - i,
        {
            if self.cmp.eq(&self.items[i].value, v) {
                return i;
            }
            i = i + 1;
        }
        i
    }

    /// The index at which a timeout due at `deadline` belongs: after every
    /// entry due no later than it.
    ///
    /// Landing *after* the ties, rather than before, is what makes a full
    /// wheel stable — an arrival that ties with the last surviving deadline
    /// sorts behind it and is the one dropped.
    fn find_slot(&self, deadline: u64) -> (r: usize)
        requires
            sorted(self@),
        ensures
            r <= self@.len(),
            forall|j: int| 0 <= j < r ==> #[trigger] self@[j].deadline <= deadline,
            r < self@.len() ==> deadline < self@[r as int].deadline,
    {
        let mut i: usize = 0;
        while i < self.items.len() && self.items[i].deadline <= deadline
            invariant
                i <= self.items@.len(),
                forall|j: int| 0 <= j < i ==> #[trigger] self.items@[j].deadline <= deadline,
            decreases self.items@.len() - i,
        {
            i = i + 1;
        }
        i
    }

    // -----------------------------------------------------------------------
    // One merge step
    // -----------------------------------------------------------------------

    /// Replace-then-place, ignoring capacity.
    ///
    /// Capacity is deliberately *not* enforced here: `merge` applies it once,
    /// after the whole batch, so the outcome does not depend on the order the
    /// batch arrived in. See [`spec_merge`] for why that matters. This is the
    /// only method that may leave the wheel over capacity, which is why it is
    /// private and why its postcondition bounds the overshoot at one.
    fn upsert_uncapped(&mut self, t: Timeout<T>)
        requires
            sorted(old(self)@),
            distinct(old(self).comparator(), old(self)@),
        ensures
            self.comparator() == old(self).comparator(),
            self.capacity_spec() == old(self).capacity_spec(),
            self@ == spec_upsert(old(self).comparator(), old(self)@, t),
            sorted(self@),
            distinct(self.comparator(), self@),
            self@.len() <= old(self)@.len() + 1,
    {
        let ghost c = self.cmp;
        let ghost s0 = self.items@;

        // 1. Replace. If the wheel already holds this logical timeout, it goes
        //    -- whatever its old deadline was.
        let i = self.find_same(&t.value);
        if i < self.items.len() {
            proof {
                lemma_remove_index_is_spec_remove(c, s0, t.value, i as int);
            }
            self.items.remove(i);
        } else {
            proof {
                assert(fresh(c, s0, t.value));
                lemma_fresh_spec_remove_id(c, s0, t.value);
            }
        }
        proof {
            lemma_spec_remove_sorted(c, s0, t.value);
            lemma_spec_remove_distinct(c, s0, t.value);
            lemma_spec_remove_fresh(c, s0, t.value);
            lemma_spec_remove_len(c, s0, t.value);
        }

        // 2. Place, by deadline.
        let ghost s1 = self.items@;
        let j = self.find_slot(t.deadline);
        proof {
            lemma_insert_at_is_spec_insert(s1, t, j as int);
        }
        self.items.insert(j, t);
        proof {
            lemma_spec_insert_sorted(s1, t);
            lemma_spec_insert_distinct(c, s1, t);
            lemma_spec_insert_len(s1, t);
        }
    }

    // -----------------------------------------------------------------------
    // Merge
    // -----------------------------------------------------------------------

    /// Fold a batch of timeouts into the wheel.
    ///
    /// Each arrival is upserted in order — an arrival whose identity is
    /// already present replaces it, moving the deadline rather than storing
    /// the timeout twice — and only then is capacity applied, keeping the
    /// `capacity` nearest deadlines of the union.
    ///
    /// The postconditions say all of that, and the last two say the part worth
    /// saying out loud: what gets dropped is *only* what is farthest in the
    /// future. `dropped` names an arrival that did not survive; the wheel is
    /// then necessarily full, and every timeout it kept is due no later than
    /// the one it dropped. Merging a batch of new timeouts whose deadlines all
    /// sit beyond the last surviving entry therefore changes nothing.
    pub fn merge(&mut self, incoming: Vec<Timeout<T>>)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            self.comparator() == old(self).comparator(),
            self.capacity_spec() == old(self).capacity_spec(),
            // The functional specification: this *is* what merge means.
            self@ == spec_merge(
                old(self).comparator(),
                old(self)@,
                incoming@,
                old(self).capacity_spec(),
            ),
            // Nothing kept is due later than anything dropped.
            ({
                let full = spec_upsert_all(
                    old(self).comparator(),
                    old(self)@,
                    incoming@,
                    incoming@.len(),
                );
                &&& self@.len() <= full.len()
                &&& self@.len() < full.len() ==> self@.len() == self.capacity_spec()
                &&& forall|i: int, j: int|
                    0 <= i < self@.len() <= j < full.len() ==> #[trigger] self@[i].deadline
                        <= #[trigger] full[j].deadline
            }),
    {
        let ghost c = self.cmp;
        let ghost cap0 = self.cap;
        let ghost s0 = self.items@;
        let ghost inc0 = incoming@;

        let mut rest = incoming;
        let n = rest.len();
        let mut k: usize = 0;

        while k < n
            invariant
                self.cmp == c,
                self.cap == cap0,
                k <= n,
                n == inc0.len(),
                rest@ == inc0.skip(k as int),
                sorted(self.items@),
                distinct(c, self.items@),
                self.items@ == spec_upsert_all(c, s0, inc0, k as nat),
            decreases n - k,
        {
            assert(rest@[0] == inc0[k as int]);
            let t = rest.remove(0);
            self.upsert_uncapped(t);
            proof {
                assert(rest@ =~= inc0.skip(k as int + 1));
            }
            k = k + 1;
        }

        // Capacity, applied once, to the whole union.
        let ghost full = self.items@;
        self.items.truncate(self.cap);

        proof {
            lemma_take_sorted(full, cap0 as nat);
            lemma_take_distinct(c, full, cap0 as nat);
            lemma_take_len(full, cap0 as nat);
            lemma_merge_horizon(c, s0, inc0, cap0 as nat);
            assert(self.items@ =~= take_at_most(full, cap0 as nat));
        }
    }

    /// Merge a single timeout. Same rules as [`TimerWheel::merge`]: it
    /// replaces an entry with the same identity, and it is dropped if the
    /// wheel is full and nothing is due later.
    pub fn insert(&mut self, t: Timeout<T>)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            self.comparator() == old(self).comparator(),
            self.capacity_spec() == old(self).capacity_spec(),
            self@ == spec_merge(
                old(self).comparator(),
                old(self)@,
                seq![t],
                old(self).capacity_spec(),
            ),
    {
        let mut batch = Vec::new();
        batch.push(t);
        proof {
            assert(batch@ =~= seq![t]);
        }
        self.merge(batch);
    }

    // -----------------------------------------------------------------------
    // Firing
    // -----------------------------------------------------------------------

    /// Remove and return every timeout due at or before `now`, nearest first.
    ///
    /// The postcondition is an exact split: `r@ + self@` reconstructs the wheel
    /// as it was, everything returned is due, and nothing left behind is. There
    /// is no third bucket, so no timeout can be silently lost here.
    pub fn pop_expired(&mut self, now: u64) -> (r: Vec<Timeout<T>>)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            self.comparator() == old(self).comparator(),
            self.capacity_spec() == old(self).capacity_spec(),
            r@ + self@ == old(self)@,
            forall|i: int| 0 <= i < r@.len() ==> #[trigger] r@[i].deadline <= now,
            forall|i: int| 0 <= i < self@.len() ==> now < #[trigger] self@[i].deadline,
    {
        let ghost c = self.cmp;
        let ghost s0 = self.items@;

        let k = self.find_slot(now);

        // `split_off` twice, rather than `mem::swap`: the tail comes off, then
        // the head, leaving `items` empty for the tail to move back into.
        let remaining = self.items.split_off(k);
        let expired = self.items.split_off(0);
        self.items = remaining;

        proof {
            lemma_suffix_sorted(s0, k as int);
            lemma_suffix_distinct(c, s0, k as int);
            assert(expired@ + self.items@ =~= s0);
            assert forall|i: int| 0 <= i < self.items@.len() implies now
                < self.items@[i].deadline by {
                assert(self.items@[i] == s0[k + i]);
                assert(s0[k as int].deadline <= s0[k + i].deadline);
            }
        }
        expired
    }
}

} // verus!
