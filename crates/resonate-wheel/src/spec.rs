//! The specification layer: what the wheel *is*, independent of how it is built.
//!
//! Everything here is ghost. It defines the wheel's two structural invariants
//! ([`sorted`], [`distinct`]) and a pure, executable-free model of merging
//! ([`spec_merge`]). The exec code in [`crate::wheel`] is then proved equal to
//! that model, so `spec_merge` — not the loops — is the definition of the
//! wheel's behaviour, and the place to look when asking what merge does.

use vstd::prelude::*;

use crate::comparator::*;
use crate::timeout::Timeout;

verus! {

// ---------------------------------------------------------------------------
// Structural invariants
// ---------------------------------------------------------------------------

/// Deadlines are non-decreasing: the nearest timeout is at index 0.
pub open spec fn sorted<T>(s: Seq<Timeout<T>>) -> bool {
    forall|i: int, j: int|
        #![trigger s[i].deadline, s[j].deadline]
        0 <= i <= j < s.len() ==> s[i].deadline <= s[j].deadline
}

/// No two entries denote the same logical timeout.
pub open spec fn distinct<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>) -> bool {
    forall|i: int, j: int|
        #![trigger cmp.same(s[i].value, s[j].value)]
        0 <= i < j < s.len() ==> !cmp.same(s[i].value, s[j].value)
}

/// **No two entries are the same logical timeout.** The reader's form of
/// [`distinct`].
///
/// The two say the same thing and [`lemma_no_duplicates_iff_distinct`] proves
/// it. They are kept apart because they are convenient for different jobs:
/// `distinct` quantifies over ordered pairs `i < j`, which halves the work in
/// an induction, while `no_duplicates` quantifies over every pair of distinct
/// positions, which is what someone asking "can the wheel hold a duplicate?"
/// actually means. Bridging them is exactly one use of the comparator's
/// symmetry law.
///
/// Note that this is a statement about distinct *positions*, not distinct
/// values. It has to be: `same` is reflexive, so every entry is trivially the
/// same logical timeout as itself, and a version of this predicate that let
/// `i` equal `j` would be unsatisfiable for any non-empty wheel.
pub open spec fn no_duplicates<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>) -> bool {
    forall|i: int, j: int|
        #![trigger cmp.same(s[i].value, s[j].value)]
        0 <= i < s.len() && 0 <= j < s.len() && i != j ==> !cmp.same(s[i].value, s[j].value)
}

/// `s` holds nothing equivalent to `v` — so `v` may be inserted without
/// breaking [`distinct`].
pub open spec fn fresh<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T) -> bool {
    forall|i: int|
        #![trigger cmp.same(s[i].value, v)]
        0 <= i < s.len() ==> !cmp.same(s[i].value, v)
}

// ---------------------------------------------------------------------------
// The model of a merge
// ---------------------------------------------------------------------------

/// Drops *every* entry of `s` equivalent to `v`.
///
/// On a [`distinct`] sequence at most one entry can match, so this is "remove
/// the entry with this identity, if present". It is defined as a filter rather
/// than as a single removal because a filter needs no side condition, which
/// makes it much easier to reason about; [`crate::proof::lemma_remove_index_is_spec_remove`]
/// bridges the two views.
pub open spec fn spec_remove<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T) -> Seq<
    Timeout<T>,
>
    decreases s.len(),
{
    if s.len() == 0 {
        Seq::empty()
    } else if cmp.same(s[0].value, v) {
        spec_remove(cmp, s.skip(1), v)
    } else {
        seq![s[0]] + spec_remove(cmp, s.skip(1), v)
    }
}

/// Inserts `t` into a deadline-sorted `s`, keeping it sorted.
///
/// `t` lands *after* every entry whose deadline is less than or equal to its
/// own. Among equal deadlines the newcomer therefore sorts last, which is what
/// makes ties deterministic: on a full wheel the entry that has been waiting
/// longest at a given deadline is the one that survives.
pub open spec fn spec_insert<T>(s: Seq<Timeout<T>>, t: Timeout<T>) -> Seq<Timeout<T>>
    decreases s.len(),
{
    if s.len() == 0 {
        seq![t]
    } else if t.deadline < s[0].deadline {
        seq![t] + s
    } else {
        seq![s[0]] + spec_insert(s.skip(1), t)
    }
}

/// One arrival: replace any entry with `t`'s identity, then place `t` by deadline.
///
/// This is the whole of "same timeout, moved deadline — replace, don't add
/// both": the removal is unconditional, so an update and a fresh arrival take
/// exactly the same path.
pub open spec fn spec_upsert<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
) -> Seq<Timeout<T>> {
    spec_insert(spec_remove(cmp, s, t.value), t)
}

/// `s.take(n)`, but tolerating `n` beyond the end.
pub open spec fn take_at_most<T>(s: Seq<T>, n: nat) -> Seq<T> {
    if s.len() <= n {
        s
    } else {
        s.take(n as int)
    }
}

// ---------------------------------------------------------------------------
// Sorting the batch
// ---------------------------------------------------------------------------

/// Insertion sort, as a fold of [`spec_insert`] over the first `k` arrivals.
///
/// Reusing `spec_insert` rather than writing a second ordering rule is what
/// makes the sort *stable*: `spec_insert` places an entry after every entry due
/// no later than it, so equal deadlines keep the order the caller supplied.
pub open spec fn spec_sort_prefix<T>(inc: Seq<Timeout<T>>, k: nat) -> Seq<Timeout<T>>
    decreases k,
{
    if k == 0 {
        Seq::empty()
    } else {
        spec_insert(spec_sort_prefix(inc, (k - 1) as nat), inc[k - 1])
    }
}

/// The batch, in deadline order, nearest first.
///
/// Sorting is load-bearing rather than cosmetic. Capacity is enforced on every
/// arrival, so without it the outcome would depend on the order the caller
/// happened to hand the batch over: a far-future arrival seen first could take
/// the last free slot and evict a nearer one seen later in the same batch.
/// Sorting first makes the result a function of the batch's *contents*.
pub open spec fn spec_sort<T>(inc: Seq<Timeout<T>>) -> Seq<Timeout<T>> {
    spec_sort_prefix(inc, inc.len())
}

// ---------------------------------------------------------------------------
// The model of a merge
// ---------------------------------------------------------------------------

/// One step of a merge: upsert the arrival, then cut back to capacity.
///
/// Because the result of [`spec_upsert`] is sorted, the entry that the cut
/// drops is always the one due farthest in the future — either the arrival
/// itself, when nothing is due later than it, or the entry that was last.
pub open spec fn spec_step<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    capacity: nat,
) -> Seq<Timeout<T>> {
    take_at_most(spec_upsert(cmp, s, t), capacity)
}

/// The first `k` arrivals of an already-sorted batch, stepped in one at a time.
pub open spec fn spec_merge_prefix<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    k: nat,
    capacity: nat,
) -> Seq<Timeout<T>>
    decreases k,
{
    if k == 0 {
        s
    } else {
        spec_step(cmp, spec_merge_prefix(cmp, s, inc, (k - 1) as nat, capacity), inc[k - 1], capacity)
    }
}

/// **The definition of merging.** Sort the batch nearest-deadline first, then
/// take the arrivals one at a time, honouring capacity at every step.
///
/// The drop rule falls out of the two pieces. Each step leaves the wheel
/// sorted, and each cut removes from the end, so what a merge can cost you is
/// only ever the timeouts due farthest in the future — never a near one. With
/// capacity 1000, a batch of *new* timeouts whose deadlines all sit beyond the
/// 1000th entry's is dropped whole; that is
/// [`lemma_merge_ignores_far_future_newcomers`](crate::proof::lemma_merge_ignores_far_future_newcomers),
/// proved rather than asserted.
///
/// # Two consequences worth knowing
///
/// **An update is not an addition.** An arrival that shares an identity with an
/// entry already in the wheel replaces it, so it inherits that entry's slot
/// however far in the future it is — a full wheel does not reject it. That is
/// why the far-future theorem is stated about *new* timeouts.
///
/// **Within one batch, the farthest deadline wins.** Because the batch is
/// sorted before it is stepped, two arrivals sharing an identity are applied
/// nearest-first, so the *later* deadline is the one left standing — not the
/// one the caller listed last. If a batch is meant to carry a sequence of
/// updates to the same timeout, deduplicate it before handing it over.
pub open spec fn spec_merge<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    capacity: nat,
) -> Seq<Timeout<T>> {
    spec_merge_prefix(cmp, s, spec_sort(inc), inc.len(), capacity)
}

} // verus!
