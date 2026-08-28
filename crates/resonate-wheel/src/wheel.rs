//! The wheel itself: a bounded, deduplicating, deadline-ordered set of timeouts.

use vstd::prelude::*;

use crate::comparator::*;
use crate::proof::*;
use crate::spec::*;
use crate::timeout::Timeout;

verus! {

// ---------------------------------------------------------------------------
// Sorted-vector helpers, shared by the wheel and by the batch sort.
// ---------------------------------------------------------------------------

/// The index at which a timeout due at `deadline` belongs in a sorted vector:
/// after every entry due no later than it.
///
/// Landing *after* the ties, rather than before, is what makes both the wheel
/// and the batch sort stable -- an arrival that ties with an entry already
/// present sorts behind it, so on a full wheel the newcomer is the one dropped,
/// and a sorted batch keeps the caller's order among equal deadlines.
fn slot_for<T>(items: &Vec<Timeout<T>>, deadline: u64) -> (r: usize)
    requires
        sorted(items@),
    ensures
        r <= items@.len(),
        forall|j: int| 0 <= j < r ==> #[trigger] items@[j].deadline <= deadline,
        r < items@.len() ==> deadline < items@[r as int].deadline,
{
    let mut i: usize = 0;
    while i < items.len() && items[i].deadline <= deadline
        invariant
            i <= items@.len(),
            forall|j: int| 0 <= j < i ==> #[trigger] items@[j].deadline <= deadline,
        decreases items@.len() - i,
    {
        i = i + 1;
    }
    i
}

/// Place `t` in a sorted vector, keeping it sorted.
fn place<T>(items: &mut Vec<Timeout<T>>, t: Timeout<T>)
    requires
        sorted(old(items)@),
    ensures
        final(items)@ == spec_insert(old(items)@, t),
        sorted(final(items)@),
        final(items)@.len() == old(items)@.len() + 1,
{
    let ghost s0 = items@;
    let j = slot_for(items, t.deadline);
    proof {
        lemma_insert_at_is_spec_insert(s0, t, j as int);
    }
    items.insert(j, t);
    proof {
        lemma_spec_insert_sorted(s0, t);
        lemma_spec_insert_len(s0, t);
    }
}

/// Sort a batch by deadline, nearest first.
///
/// Insertion sort through `place`, which means the ordering rule is written
/// once and the sort is stable by construction. `O(m^2)` on the batch, matched
/// to the batch sizes a wheel actually sees; the specification says nothing
/// about the algorithm, so a merge sort could be dropped in underneath without
/// the statement of correctness changing.
fn sort_by_deadline<T>(batch: Vec<Timeout<T>>) -> (r: Vec<Timeout<T>>)
    ensures
        r@ == spec_sort(batch@),
        sorted(r@),
        r@.len() == batch@.len(),
{
    let ghost inc0 = batch@;
    let mut src = batch;
    let mut out: Vec<Timeout<T>> = Vec::new();
    let n = src.len();
    let mut k: usize = 0;

    while k < n
        invariant
            k <= n,
            n == inc0.len(),
            src@ == inc0.skip(k as int),
            out@ == spec_sort_prefix(inc0, k as nat),
            sorted(out@),
        decreases n - k,
    {
        assert(src@[0] == inc0[k as int]);
        let t = src.remove(0);
        place(&mut out, t);
        proof {
            assert(src@ =~= inc0.skip(k as int + 1));
        }
        k = k + 1;
    }
    proof {
        lemma_spec_sort_wf(inc0, inc0.len());
    }
    out
}

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
/// ```
/// use resonate_wheel::{IdComparator, Timeout, TimerWheel};
///
/// let mut w = TimerWheel::new(2, IdComparator);
/// w.merge(vec![
///     Timeout::new(30, 1u64),
///     Timeout::new(10, 2u64),
///     Timeout::new(20, 3u64),
/// ]);
/// // Capacity 2 keeps the two nearest deadlines; id 1 at 30 is dropped.
/// assert_eq!(w.len(), 2);
/// assert_eq!(w.peek().unwrap().deadline, 10);
///
/// // id 3's deadline moves 20 -> 5. It is replaced, not stored twice.
/// w.merge(vec![Timeout::new(5, 3u64)]);
/// assert_eq!(w.len(), 2);
/// assert_eq!(w.peek().unwrap().value, 3);
///
/// // `next` answers "how long may I sleep?" without disturbing anything.
/// assert_eq!(w.next(), Some(5));
/// assert_eq!(w.next(), Some(5));
///
/// let due = w.pop_expired(5);
/// assert_eq!(due.len(), 1);
/// assert_eq!(w.len(), 1);
/// assert_eq!(w.next(), Some(10));
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
    ///
    /// [`TimerWheel::next`] returns just its deadline, which is usually what a
    /// scheduler wants and does not borrow the wheel.
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

    /// The deadline of the timeout due soonest, without removing anything.
    ///
    /// This is the question a scheduler asks between ticks: *how long may I
    /// sleep?* The answer is only meaningful because the third postcondition
    /// holds — nothing in the wheel is due before what comes back — so a caller
    /// that waits until this deadline cannot have missed a timer. `None` means
    /// the wheel is empty and there is nothing to wait for.
    ///
    /// Returning the deadline alone rather than the timeout is deliberate: the
    /// caller does not borrow the wheel and is free to mutate it while the
    /// answer is in hand. Use [`TimerWheel::peek`] when the payload is wanted
    /// too, and [`TimerWheel::pop_expired`] to take the due timeouts out.
    ///
    /// Despite the name this does **not** advance anything, and `TimerWheel` is
    /// not an `Iterator`. Repeated calls return the same deadline until the
    /// wheel is changed.
    pub fn next(&self) -> (r: Option<u64>)
        requires
            self.wf(),
        ensures
            r is Some <==> self@.len() > 0,
            r matches Some(d) ==> d == self@[0].deadline,
            // Nothing is due sooner. This is what makes it safe to sleep on.
            r matches Some(d) ==> forall|i: int|
                0 <= i < self@.len() ==> d <= #[trigger] self@[i].deadline,
    {
        if self.items.len() == 0 {
            None
        } else {
            Some(self.items[0].deadline)
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

    // -----------------------------------------------------------------------
    // One arrival
    // -----------------------------------------------------------------------

    /// Replace, then add: drop whatever the wheel held under this identity, and
    /// put the arrival on the end.
    ///
    /// The removal is unconditional, so a replacement and a fresh addition take
    /// exactly the same path. Note what this does *not* do: it does not place
    /// the arrival in deadline order and it does not look at capacity. A merge
    /// applies its whole batch this way and only then sorts and cuts, which is
    /// why the wheel is deliberately out of order in between — `wf` does not
    /// hold across this call, which is why it is private.
    fn apply(&mut self, t: Timeout<T>)
        requires
            distinct(old(self).comparator(), old(self)@),
        ensures
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            final(self)@ == spec_apply(old(self).comparator(), old(self)@, t),
            distinct(final(self).comparator(), final(self)@),
    {
        let ghost c = self.cmp;
        let ghost s0 = self.items@;

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
        self.items.push(t);
        proof {
            lemma_spec_apply_distinct(c, s0, t);
        }
    }

    // -----------------------------------------------------------------------
    // Merge
    // -----------------------------------------------------------------------

    /// Fold a batch of timeouts into the wheel.
    ///
    /// Three steps, and they are the three lines of [`spec_merge`]:
    ///
    /// 1. **replace, then add** — every arrival drops whatever the wheel held
    ///    under its identity and takes its place; an arrival for a timeout the
    ///    wheel did not have is simply added;
    /// 2. **sort** by deadline, nearest first;
    /// 3. **cut** to capacity.
    ///
    /// The wheel is deliberately out of order between steps 1 and 2. Capacity
    /// is applied once, to the finished union, which is what makes the result a
    /// function of the batch's contents rather than of the order it arrived in.
    ///
    /// The postcondition names `spec_merge` directly, so this method is *proved
    /// equal* to those three lines. Nothing below the equality is promised: the
    /// scan, the insertion sort and the truncation are an implementation, and
    /// can be replaced by anything faster without the guarantee moving.
    pub fn merge(&mut self, incoming: Vec<Timeout<T>>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            // The functional specification: this *is* what merge means.
            final(self)@ == spec_merge(
                old(self).comparator(),
                old(self)@,
                incoming@,
                old(self).capacity_spec(),
            ),
            // A merge never costs the wheel a slot it was already using.
            old(self)@.len() <= final(self)@.len(),
            // No duplicates in, no duplicates out -- whatever the batch does.
            no_duplicates(old(self).comparator(), old(self)@)
                ==> no_duplicates(final(self).comparator(), final(self)@),
            // An arrival REPLACES: whatever the wheel held under that identity,
            // afterwards nothing under it survives with any other deadline. So
            // a timeout already in the wheel either comes back with the new
            // deadline or is pushed out -- never both, never stale. See
            // `lemma_merge_replaces_or_evicts` for that spelled out.
            forall|j: int|
                #![trigger incoming@[j].value]
                0 <= j < incoming@.len() && (forall|l: int|
                    j < l < incoming@.len() ==> !old(self).comparator().same(
                        #[trigger] incoming@[l].value,
                        incoming@[j].value,
                    )) ==> identity_carries(
                    final(self).comparator(),
                    final(self)@,
                    incoming@[j].value,
                    incoming@[j].deadline,
                ),
    {
        let ghost c = self.cmp;
        let ghost cap0 = self.cap;
        let ghost s0 = self.items@;
        let ghost inc0 = incoming@;

        // 1. Replace, then add.
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
                distinct(c, self.items@),
                self.items@ == spec_apply_all(c, s0, inc0, k as nat),
            decreases n - k,
        {
            assert(rest@[0] == inc0[k as int]);
            let t = rest.remove(0);
            self.apply(t);
            proof {
                assert(rest@ =~= inc0.skip(k as int + 1));
            }
            k = k + 1;
        }

        // 2. Order by deadline.
        let ghost union = self.items@;
        let unsorted = self.items.split_off(0);
        proof {
            // `split_off(0)` hands back `subrange(0, len)`, which is the whole
            // sequence but not syntactically the same term.
            assert(unsorted@ =~= union);
        }
        self.items = sort_by_deadline(unsorted);

        // 3. Cut to capacity.
        let ghost ordered = self.items@;
        self.items.truncate(self.cap);

        proof {
            assert(union == spec_apply_all(c, s0, inc0, inc0.len()));
            assert(ordered == spec_sort(union));
            assert(self.items@ =~= take_at_most(ordered, cap0 as nat));
            lemma_merge_wf(c, s0, inc0, cap0 as nat);
            lemma_no_duplicates_iff_distinct(c, s0);
            lemma_merge_preserves_no_duplicates(c, s0, inc0, cap0 as nat);
            assert forall|j: int|
                0 <= j < inc0.len() && (forall|l: int|
                    j < l < inc0.len() ==> !c.same(#[trigger] inc0[l].value, inc0[j].value))
                    implies identity_carries(c, self.items@, inc0[j].value, inc0[j].deadline) by {
                lemma_merge_sets_identity(c, s0, inc0, j, cap0 as nat);
            }
        }
    }

    /// Merge a single timeout. Same rules as [`TimerWheel::merge`]: it
    /// replaces an entry with the same identity, and it is dropped if the
    /// wheel is full and nothing is due later.
    pub fn insert(&mut self, t: Timeout<T>)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            final(self)@ == spec_merge(
                old(self).comparator(),
                old(self)@,
                seq![t],
                old(self).capacity_spec(),
            ),
            no_duplicates(old(self).comparator(), old(self)@)
                ==> no_duplicates(final(self).comparator(), final(self)@),
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
            final(self).wf(),
            final(self).comparator() == old(self).comparator(),
            final(self).capacity_spec() == old(self).capacity_spec(),
            r@ + final(self)@ == old(self)@,
            forall|i: int| 0 <= i < r@.len() ==> #[trigger] r@[i].deadline <= now,
            forall|i: int| 0 <= i < final(self)@.len() ==> now < #[trigger] final(self)@[i].deadline,
            // Nothing fires early. This is the other end of `next`: a caller
            // that slept until the deadline `next` handed back comes here and
            // gets nothing, so waiting on that answer cannot miss a timeout.
            (old(self)@.len() > 0 && now < old(self)@[0].deadline) ==> r@.len() == 0,
    {
        let ghost c = self.cmp;
        let ghost s0 = self.items@;

        let k = slot_for(&self.items, now);

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
            // Anything returned would have to start at the wheel's own front,
            // which by hypothesis is not due yet.
            if expired@.len() > 0 {
                assert(expired@[0] == s0[0]);
            }
        }
        expired
    }
}

} // verus!
