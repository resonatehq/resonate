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

/// One merge step: replace any entry with `t`'s identity, then place `t` by deadline.
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

/// The first `k` entries of `inc` upserted into `s`, in order.
///
/// Order matters when `inc` itself carries two entries with the same identity:
/// the later one wins, matching the intuition that a batch is a sequence of
/// updates rather than a set.
pub open spec fn spec_upsert_all<T, C: Comparator<T>>(
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
        spec_upsert(cmp, spec_upsert_all(cmp, s, inc, (k - 1) as nat), inc[k - 1])
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

/// **The definition of merging.** Upsert every incoming timeout, then keep the
/// `capacity` nearest deadlines.
///
/// Capacity is applied *after* the whole batch, not during it, and that is a
/// deliberate choice rather than an implementation accident. Enforcing capacity
/// per-item would make the outcome depend on the order the batch happens to
/// arrive in — an early far-future arrival could claim the last slot and lock
/// out a nearer timeout later in the same batch. Applying it once at the end
/// means the surviving set is always *the `capacity` nearest deadlines of the
/// union*, whatever order the batch came in.
///
/// This is also where the drop rule comes from: since the sequence is sorted
/// before it is cut, the entries that fall off the end are exactly the ones
/// furthest in the future. Merging a batch whose deadlines all sit beyond the
/// last surviving entry drops the batch entirely — see
/// [`TimerWheel::merge`](crate::TimerWheel::merge), whose postconditions state
/// that consequence directly.
pub open spec fn spec_merge<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    capacity: nat,
) -> Seq<Timeout<T>> {
    take_at_most(spec_upsert_all(cmp, s, inc, inc.len()), capacity)
}

} // verus!
