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

/// Whatever `s` holds under identity `v` is due at `d`.
///
/// Vacuously true when `s` holds nothing equivalent to `v` — which is what lets
/// one predicate carry both halves of "replaced or evicted". After a merge that
/// carried an arrival for `v`, this holds with `d` set to the arrival's
/// deadline, so a surviving entry necessarily has the *new* timestamp and the
/// old one cannot still be there.
pub open spec fn identity_carries<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    d: u64,
) -> bool {
    forall|k: int|
        #![trigger cmp.same(s[k].value, v)]
        0 <= k < s.len() && cmp.same(s[k].value, v) ==> s[k].deadline == d
}

/// `s` holds an entry under identity `v`, due at `d`.
pub open spec fn holds_at<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    d: u64,
) -> bool {
    exists|k: int|
        #![trigger cmp.same(s[k].value, v)]
        0 <= k < s.len() && cmp.same(s[k].value, v) && s[k].deadline == d
}

/// `s` holds nothing equivalent to `v` — so `v` may be inserted without
/// breaking [`distinct`].
pub open spec fn fresh<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T) -> bool {
    forall|i: int|
        #![trigger cmp.same(s[i].value, v)]
        0 <= i < s.len() ==> !cmp.same(s[i].value, v)
}

// ---------------------------------------------------------------------------
// Primitives the definition is built from
// ---------------------------------------------------------------------------

/// Drops *every* entry of `s` equivalent to `v`.
///
/// On a [`distinct`] sequence at most one entry can match, so this is "remove
/// the entry with this identity, if present". It is defined as a filter rather
/// than as a single removal because a filter needs no side condition, which
/// makes it much easier to reason about; [`crate::proof::lemma_remove_index_is_spec_remove`]
/// bridges the two views. It is what the *drop* step of a merge is built on.
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

/// `s.take(n)`, but tolerating `n` beyond the end.
pub open spec fn take_at_most<T>(s: Seq<T>, n: nat) -> Seq<T> {
    if s.len() <= n {
        s
    } else {
        s.take(n as int)
    }
}

// ---------------------------------------------------------------------------
// Ordering by deadline
// ---------------------------------------------------------------------------

/// Insertion sort, as a fold of [`spec_insert`] over the first `k` entries.
///
/// Reusing `spec_insert` rather than writing a second ordering rule is what
/// makes the sort *stable*: `spec_insert` places an entry after every entry due
/// no later than it, so equal deadlines keep the order they came in. On the
/// sequence a merge sorts — surviving entries first, then arrivals — that means
/// an entry the wheel was already holding keeps its place over an arrival that
/// merely ties with it.
pub open spec fn spec_sort_prefix<T>(u: Seq<Timeout<T>>, k: nat) -> Seq<Timeout<T>>
    decreases k,
{
    if k == 0 {
        Seq::empty()
    } else {
        spec_insert(spec_sort_prefix(u, (k - 1) as nat), u[k - 1])
    }
}

/// `u`, in deadline order, nearest first.
pub open spec fn spec_sort<T>(u: Seq<Timeout<T>>) -> Seq<Timeout<T>> {
    spec_sort_prefix(u, u.len())
}

// ---------------------------------------------------------------------------
// The model of a merge
// ---------------------------------------------------------------------------

/// Does `batch` name a timeout with this identity?
pub open spec fn mentions<T, C: Comparator<T>>(cmp: C, batch: Seq<Timeout<T>>, v: T) -> bool {
    !fresh(cmp, batch, v)
}

/// The wheel's entries the batch says nothing about. They stay exactly as they
/// are; everything else the wheel held is dropped, to be re-added by the batch.
pub open spec fn spec_untouched<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
) -> Seq<Timeout<T>>
    decreases s.len(),
{
    if s.len() == 0 {
        Seq::empty()
    } else if mentions(cmp, batch, s[0].value) {
        spec_untouched(cmp, s.skip(1), batch)
    } else {
        seq![s[0]] + spec_untouched(cmp, s.skip(1), batch)
    }
}

/// The batch's updates: one per timeout, the last one the caller gave.
///
/// An arrival is dropped only when a *later* arrival names the same timeout, so
/// a batch reads as a sequence of updates and the last one stands.
pub open spec fn spec_updates<T, C: Comparator<T>>(
    cmp: C,
    batch: Seq<Timeout<T>>,
) -> Seq<Timeout<T>>
    decreases batch.len(),
{
    if batch.len() == 0 {
        Seq::empty()
    } else if mentions(cmp, batch.skip(1), batch[0].value) {
        spec_updates(cmp, batch.skip(1))
    } else {
        seq![batch[0]] + spec_updates(cmp, batch.skip(1))
    }
}

/// **The definition of merging, in one line.**
///
/// ```text
///   drop      every entry the batch mentions
///   add       the batch's updates
///   sort      by deadline, nearest first
///   cut       keep the first `capacity`
/// ```
///
/// That is the whole specification, and it is meant to be read rather than
/// traced. Everything the wheel guarantees is a consequence of those four
/// words, and a one-sentence summary falls out of them:
///
/// > after a merge, the wheel holds the `capacity` nearest deadlines of
/// > everything it was already keeping track of that the batch did not touch,
/// > together with the batch itself.
///
/// Note what the definition does *not* say: nothing about scanning, indices,
/// insertion points or loops. [`TimerWheel::merge`](crate::TimerWheel::merge)
/// is proved equal to this, so the implementation underneath is free to change
/// — a heap, an index, a different scan order — without the statement of
/// correctness moving at all.
///
/// # Two consequences worth knowing
///
/// **An update is not a lease on a slot.** An arrival naming a timeout the
/// wheel already holds does not inherit its place: the old entry is dropped in
/// the first step and the arrival competes on its new deadline like anything
/// else. Moving a deadline outward can cost you the timeout.
///
/// **Cutting happens once, at the end.** Capacity is applied to the finished
/// union, not to each arrival as it lands, which is what makes the result a
/// function of the batch's *contents* rather than of the order it arrived in.
pub open spec fn spec_merge<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    capacity: nat,
) -> Seq<Timeout<T>> {
    take_at_most(
        spec_sort(spec_untouched(cmp, s, batch) + spec_updates(cmp, batch)),
        capacity,
    )
}

} // verus!
