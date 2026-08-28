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

/// One arrival: drop whatever the wheel held under that identity, then add it.
///
/// The removal is unconditional, which is the whole of "same timeout, moved
/// deadline — replace, don't add both". When the wheel held nothing under that
/// identity the removal does nothing and this is simply an addition, so a
/// *replace* and an *add* are the same operation and need no case split.
pub open spec fn spec_apply<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
) -> Seq<Timeout<T>> {
    spec_remove(cmp, s, t.value).push(t)
}

/// The first `k` arrivals applied, in the order the caller supplied them.
///
/// Order matters only when the batch names one identity twice: the later
/// arrival replaces the earlier, so a batch reads as a sequence of updates and
/// the last one wins.
pub open spec fn spec_apply_all<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    k: nat,
) -> Seq<Timeout<T>>
    decreases k,
{
    if k == 0 {
        s
    } else {
        spec_apply(cmp, spec_apply_all(cmp, s, inc, (k - 1) as nat), inc[k - 1])
    }
}

/// **The definition of merging, in three steps.**
///
/// ```text
///   replace, then add     every arrival drops whatever the wheel held under
///                         its identity and takes its place; an arrival for a
///                         timeout the wheel did not have is simply added
///   sort                  by deadline, nearest first
///   cut                   keep the first `capacity`
/// ```
///
/// That is the whole specification, and it is meant to be read rather than
/// traced. Everything the wheel guarantees is a consequence of these three
/// lines, and a one-sentence summary falls out of them:
///
/// > after a merge, the wheel holds the `capacity` nearest deadlines of
/// > everything it had — with moved deadlines moved — together with everything
/// > the batch added.
///
/// Note what the definition does *not* say: nothing about scanning, indices,
/// insertion points or loops. [`TimerWheel::merge`](crate::TimerWheel::merge)
/// is proved equal to this, so the implementation underneath is free to change
/// — a heap, an index, a different scan order — without the statement of
/// correctness moving at all.
///
/// # Two consequences worth knowing
///
/// **An update is not an addition.** An arrival that names a timeout the wheel
/// already holds replaces it, so it inherits nothing and competes for a slot on
/// its new deadline alone. If that new deadline is beyond the horizon, the
/// timeout is dropped — moving a deadline out can cost you the timeout.
///
/// **Cutting happens once, at the end.** Capacity is applied to the finished
/// union, not to each arrival as it lands, which is what makes the result a
/// function of the batch's *contents* rather than of the order it arrived in.
pub open spec fn spec_merge<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    capacity: nat,
) -> Seq<Timeout<T>> {
    take_at_most(spec_sort(spec_apply_all(cmp, s, inc, inc.len())), capacity)
}

} // verus!
