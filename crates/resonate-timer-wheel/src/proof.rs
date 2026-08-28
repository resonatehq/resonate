//! The proof library: everything the exec code in [`crate::wheel`] leans on.
//!
//! Three layers, bottom up:
//!
//! 1. **Sequence shape** — `cons`/`skip` plumbing. [`sorted`], [`distinct`] and
//!    [`fresh`] are quantified predicates, so every induction step has to
//!    re-establish them across a `seq![x] + rest` boundary. These lemmas do
//!    that once each.
//! 2. **The spec preserves the invariants** — [`spec_remove`], [`spec_insert`],
//!    [`spec_upsert`] and [`spec_upsert_all`] each carry `sorted` and
//!    `distinct` through, by induction over the same structure the definitions
//!    recurse on.
//! 3. **The exec code implements the spec** — the bridge. The code finds an
//!    index and calls `Vec::remove` / `Vec::insert` once;
//!    [`lemma_remove_index_is_spec_remove`] and [`lemma_insert_at_is_spec_insert`]
//!    say that a single indexed operation coincides with the filter/fold the
//!    spec is written as.
//!
//! At the end, three corollaries state the wheel's user-visible guarantees:
//! [`lemma_merge_wf`], [`lemma_merge_horizon`] and
//! [`lemma_merge_ignores_far_future_newcomers`].

use vstd::prelude::*;

use crate::comparator::*;
use crate::spec::*;
use crate::timeout::Timeout;

verus! {

// ===========================================================================
// 1. Sequence shape
// ===========================================================================

/// Splitting off the head and putting it back is the identity.
pub proof fn lemma_cons_head_skip<T>(s: Seq<Timeout<T>>)
    requires
        s.len() > 0,
    ensures
        seq![s[0]] + s.skip(1) == s,
{
    assert(seq![s[0]] + s.skip(1) =~= s);
}

/// Indexing past the head.
pub proof fn lemma_skip_index<T>(s: Seq<Timeout<T>>, i: int)
    requires
        s.len() > 0,
        0 <= i < s.len() - 1,
    ensures
        s.skip(1)[i] == s[i + 1],
{
}

/// `sorted` survives dropping the head.
pub proof fn lemma_sorted_skip<T>(s: Seq<Timeout<T>>)
    requires
        sorted(s),
        s.len() > 0,
    ensures
        sorted(s.skip(1)),
{
    assert forall|i: int, j: int| 0 <= i <= j < s.skip(1).len() implies s.skip(1)[i].deadline
        <= s.skip(1)[j].deadline by {
        assert(s.skip(1)[i] == s[i + 1]);
        assert(s.skip(1)[j] == s[j + 1]);
    }
}

/// `distinct` survives dropping the head.
pub proof fn lemma_distinct_skip<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>)
    requires
        distinct(cmp, s),
        s.len() > 0,
    ensures
        distinct(cmp, s.skip(1)),
{
    assert forall|i: int, j: int| 0 <= i < j < s.skip(1).len() implies !cmp.same(
        s.skip(1)[i].value,
        s.skip(1)[j].value,
    ) by {
        assert(s.skip(1)[i] == s[i + 1]);
        assert(s.skip(1)[j] == s[j + 1]);
    }
}

/// The reader's form of "no duplicates" and the proof-friendly form agree.
///
/// [`distinct`] quantifies over ordered pairs `i < j`; [`no_duplicates`]
/// quantifies over every pair of distinct positions. Getting from the first to
/// the second means turning `!same(s[j], s[i])` into `!same(s[i], s[j])`, which
/// is precisely -- and only -- the comparator's symmetry law. An implementor
/// who could not discharge that obligation would leave the wheel able to hold
/// two entries each of which is a duplicate of the other in one direction only.
pub proof fn lemma_no_duplicates_iff_distinct<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>)
    ensures
        no_duplicates(cmp, s) <==> distinct(cmp, s),
{
    lemma_symmetric_all::<T, C>(cmp);
    if distinct(cmp, s) {
        assert forall|i: int, j: int| 0 <= i < s.len() && 0 <= j < s.len() && i != j implies
            !cmp.same(s[i].value, s[j].value) by {
            if j < i {
                assert(!cmp.same(s[j].value, s[i].value));
            }
        }
    }
}

/// `fresh` survives dropping the head.
pub proof fn lemma_fresh_skip<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    requires
        fresh(cmp, s, v),
        s.len() > 0,
    ensures
        fresh(cmp, s.skip(1), v),
{
    assert forall|i: int| 0 <= i < s.skip(1).len() implies !cmp.same(s.skip(1)[i].value, v) by {
        assert(s.skip(1)[i] == s[i + 1]);
    }
}

/// Prepending a small-enough head keeps a sorted tail sorted.
pub proof fn lemma_sorted_cons<T>(x: Timeout<T>, rest: Seq<Timeout<T>>)
    requires
        sorted(rest),
        forall|i: int| 0 <= i < rest.len() ==> x.deadline <= #[trigger] rest[i].deadline,
    ensures
        sorted(seq![x] + rest),
{
    let s = seq![x] + rest;
    assert forall|i: int, j: int| 0 <= i <= j < s.len() implies s[i].deadline <= s[j].deadline by {
        if i == 0 {
            if j == 0 {
            } else {
                assert(s[j] == rest[j - 1]);
            }
        } else {
            assert(s[i] == rest[i - 1]);
            assert(s[j] == rest[j - 1]);
        }
    }
}

/// Prepending a head nothing in the tail matches keeps the tail distinct.
pub proof fn lemma_distinct_cons<T, C: Comparator<T>>(
    cmp: C,
    x: Timeout<T>,
    rest: Seq<Timeout<T>>,
)
    requires
        distinct(cmp, rest),
        fresh(cmp, rest, x.value),
    ensures
        distinct(cmp, seq![x] + rest),
{
    lemma_equivalence::<T, C>(cmp);
    let s = seq![x] + rest;
    assert forall|i: int, j: int| 0 <= i < j < s.len() implies !cmp.same(s[i].value, s[j].value) by {
        assert(s[j] == rest[j - 1]);
        if i == 0 {
            assert(!cmp.same(rest[j - 1].value, x.value));
        } else {
            assert(s[i] == rest[i - 1]);
        }
    }
}

/// `fresh` distributes over `cons`.
pub proof fn lemma_fresh_cons<T, C: Comparator<T>>(
    cmp: C,
    x: Timeout<T>,
    rest: Seq<Timeout<T>>,
    v: T,
)
    requires
        fresh(cmp, rest, v),
        !cmp.same(x.value, v),
    ensures
        fresh(cmp, seq![x] + rest, v),
{
    let s = seq![x] + rest;
    assert forall|i: int| 0 <= i < s.len() implies !cmp.same(s[i].value, v) by {
        if i > 0 {
            assert(s[i] == rest[i - 1]);
        }
    }
}

/// Every deadline in `s` is at least `d`. The bound `sorted` reasoning needs
/// when a recursive call returns a rearranged tail.
pub open spec fn bounded_below<T>(s: Seq<Timeout<T>>, d: u64) -> bool {
    forall|i: int| 0 <= i < s.len() ==> d <= #[trigger] s[i].deadline
}

/// A sorted sequence is bounded below by its head.
pub proof fn lemma_sorted_bounded_below<T>(s: Seq<Timeout<T>>)
    requires
        sorted(s),
        s.len() > 0,
    ensures
        bounded_below(s, s[0].deadline),
{
}

// ===========================================================================
// 2. The spec preserves the invariants
// ===========================================================================

// --- spec_remove ---

/// Removal only ever drops entries, so a lower bound on deadlines survives it.
pub proof fn lemma_spec_remove_bounded_below<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    d: u64,
)
    requires
        bounded_below(s, d),
    ensures
        bounded_below(spec_remove(cmp, s, v), d),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        assert(bounded_below(s.skip(1), d)) by {
            assert forall|i: int| 0 <= i < s.skip(1).len() implies d <= s.skip(1)[i].deadline by {
                assert(s.skip(1)[i] == s[i + 1]);
            }
        }
        lemma_spec_remove_bounded_below(cmp, s.skip(1), v, d);
        if !cmp.same(s[0].value, v) {
            let rest = spec_remove(cmp, s.skip(1), v);
            assert forall|i: int| 0 <= i < (seq![s[0]] + rest).len() implies d <= (seq![s[0]]
                + rest)[i].deadline by {
                if i > 0 {
                    assert((seq![s[0]] + rest)[i] == rest[i - 1]);
                }
            }
        }
    }
}

/// Removal only ever drops entries, so freshness w.r.t. any `w` survives it.
pub proof fn lemma_spec_remove_preserves_fresh<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    w: T,
)
    requires
        fresh(cmp, s, w),
    ensures
        fresh(cmp, spec_remove(cmp, s, v), w),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        lemma_fresh_skip(cmp, s, w);
        lemma_spec_remove_preserves_fresh(cmp, s.skip(1), v, w);
        if !cmp.same(s[0].value, v) {
            lemma_fresh_cons(cmp, s[0], spec_remove(cmp, s.skip(1), v), w);
        }
    }
}

/// Removal keeps the sequence sorted.
pub proof fn lemma_spec_remove_sorted<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    requires
        sorted(s),
    ensures
        sorted(spec_remove(cmp, s, v)),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        lemma_sorted_skip(s);
        lemma_spec_remove_sorted(cmp, s.skip(1), v);
        if !cmp.same(s[0].value, v) {
            assert(bounded_below(s.skip(1), s[0].deadline)) by {
                assert forall|i: int| 0 <= i < s.skip(1).len() implies s[0].deadline
                    <= s.skip(1)[i].deadline by {
                    assert(s.skip(1)[i] == s[i + 1]);
                }
            }
            lemma_spec_remove_bounded_below(cmp, s.skip(1), v, s[0].deadline);
            lemma_sorted_cons(s[0], spec_remove(cmp, s.skip(1), v));
        }
    }
}

/// Removal keeps the sequence distinct.
pub proof fn lemma_spec_remove_distinct<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_remove(cmp, s, v)),
    decreases s.len(),
{
    lemma_equivalence::<T, C>(cmp);
    if s.len() == 0 {
    } else {
        lemma_distinct_skip(cmp, s);
        lemma_spec_remove_distinct(cmp, s.skip(1), v);
        if !cmp.same(s[0].value, v) {
            assert(fresh(cmp, s.skip(1), s[0].value)) by {
                assert forall|i: int| 0 <= i < s.skip(1).len() implies !cmp.same(
                    s.skip(1)[i].value,
                    s[0].value,
                ) by {
                    assert(s.skip(1)[i] == s[i + 1]);
                    assert(!cmp.same(s[0].value, s[i + 1].value));
                }
            }
            lemma_spec_remove_preserves_fresh(cmp, s.skip(1), v, s[0].value);
            lemma_distinct_cons(cmp, s[0], spec_remove(cmp, s.skip(1), v));
        }
    }
}

/// After removing `v`, nothing equivalent to `v` is left. This is what makes
/// the following insert safe to do without re-checking.
pub proof fn lemma_spec_remove_fresh<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    ensures
        fresh(cmp, spec_remove(cmp, s, v), v),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        lemma_spec_remove_fresh(cmp, s.skip(1), v);
        if !cmp.same(s[0].value, v) {
            lemma_fresh_cons(cmp, s[0], spec_remove(cmp, s.skip(1), v), v);
        }
    }
}

/// Removal never grows the sequence.
pub proof fn lemma_spec_remove_len<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    ensures
        spec_remove(cmp, s, v).len() <= s.len(),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        lemma_spec_remove_len(cmp, s.skip(1), v);
    }
}

// --- spec_insert ---

/// Insertion adds exactly one entry.
pub proof fn lemma_spec_insert_len<T>(s: Seq<Timeout<T>>, t: Timeout<T>)
    ensures
        spec_insert(s, t).len() == s.len() + 1,
    decreases s.len(),
{
    if s.len() == 0 {
    } else if t.deadline < s[0].deadline {
    } else {
        lemma_spec_insert_len(s.skip(1), t);
    }
}

/// A lower bound survives insertion, provided the newcomer respects it too.
pub proof fn lemma_spec_insert_bounded_below<T>(s: Seq<Timeout<T>>, t: Timeout<T>, d: u64)
    requires
        bounded_below(s, d),
        d <= t.deadline,
    ensures
        bounded_below(spec_insert(s, t), d),
    decreases s.len(),
{
    if s.len() == 0 {
    } else if t.deadline < s[0].deadline {
        assert forall|i: int| 0 <= i < (seq![t] + s).len() implies d <= (seq![t] + s)[i].deadline
            by {
            if i > 0 {
                assert((seq![t] + s)[i] == s[i - 1]);
            }
        }
    } else {
        assert(bounded_below(s.skip(1), d)) by {
            assert forall|i: int| 0 <= i < s.skip(1).len() implies d <= s.skip(1)[i].deadline by {
                assert(s.skip(1)[i] == s[i + 1]);
            }
        }
        lemma_spec_insert_bounded_below(s.skip(1), t, d);
        let rest = spec_insert(s.skip(1), t);
        assert forall|i: int| 0 <= i < (seq![s[0]] + rest).len() implies d <= (seq![s[0]]
            + rest)[i].deadline by {
            if i > 0 {
                assert((seq![s[0]] + rest)[i] == rest[i - 1]);
            }
        }
    }
}

/// Freshness survives insertion, provided the newcomer is itself fresh.
pub proof fn lemma_spec_insert_preserves_fresh<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    w: T,
)
    requires
        fresh(cmp, s, w),
        !cmp.same(t.value, w),
    ensures
        fresh(cmp, spec_insert(s, t), w),
    decreases s.len(),
{
    if s.len() == 0 {
        assert forall|i: int| 0 <= i < seq![t].len() implies !cmp.same(seq![t][i].value, w) by {
        }
    } else if t.deadline < s[0].deadline {
        lemma_fresh_cons(cmp, t, s, w);
    } else {
        lemma_fresh_skip(cmp, s, w);
        lemma_spec_insert_preserves_fresh(cmp, s.skip(1), t, w);
        lemma_fresh_cons(cmp, s[0], spec_insert(s.skip(1), t), w);
    }
}

/// **Insertion keeps the sequence sorted.** This is the property that makes
/// truncation mean "drop the farthest-future entries".
pub proof fn lemma_spec_insert_sorted<T>(s: Seq<Timeout<T>>, t: Timeout<T>)
    requires
        sorted(s),
    ensures
        sorted(spec_insert(s, t)),
    decreases s.len(),
{
    if s.len() == 0 {
    } else if t.deadline < s[0].deadline {
        lemma_sorted_bounded_below(s);
        lemma_sorted_cons(t, s);
    } else {
        lemma_sorted_skip(s);
        lemma_spec_insert_sorted(s.skip(1), t);
        assert(bounded_below(s.skip(1), s[0].deadline)) by {
            assert forall|i: int| 0 <= i < s.skip(1).len() implies s[0].deadline
                <= s.skip(1)[i].deadline by {
                assert(s.skip(1)[i] == s[i + 1]);
            }
        }
        lemma_spec_insert_bounded_below(s.skip(1), t, s[0].deadline);
        lemma_sorted_cons(s[0], spec_insert(s.skip(1), t));
    }
}

/// Insertion keeps the sequence distinct, given the newcomer is fresh.
pub proof fn lemma_spec_insert_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
)
    requires
        distinct(cmp, s),
        fresh(cmp, s, t.value),
    ensures
        distinct(cmp, spec_insert(s, t)),
    decreases s.len(),
{
    lemma_equivalence::<T, C>(cmp);
    if s.len() == 0 {
    } else if t.deadline < s[0].deadline {
        lemma_distinct_cons(cmp, t, s);
    } else {
        lemma_distinct_skip(cmp, s);
        lemma_fresh_skip(cmp, s, t.value);
        lemma_spec_insert_distinct(cmp, s.skip(1), t);
        assert(fresh(cmp, spec_insert(s.skip(1), t), s[0].value)) by {
            assert(fresh(cmp, s.skip(1), s[0].value)) by {
                assert forall|i: int| 0 <= i < s.skip(1).len() implies !cmp.same(
                    s.skip(1)[i].value,
                    s[0].value,
                ) by {
                    assert(s.skip(1)[i] == s[i + 1]);
                    assert(!cmp.same(s[0].value, s[i + 1].value));
                }
            }
            assert(!cmp.same(t.value, s[0].value));
            lemma_spec_insert_preserves_fresh(cmp, s.skip(1), t, s[0].value);
        }
        lemma_distinct_cons(cmp, s[0], spec_insert(s.skip(1), t));
    }
}

// --- how far removal can shorten a sequence ---

/// Removal drops at most one entry, when there is at most one to drop.
pub proof fn lemma_spec_remove_len_lower<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    requires
        distinct(cmp, s),
    ensures
        s.len() <= spec_remove(cmp, s, v).len() + 1,
    decreases s.len(),
{
    if s.len() == 0 {
    } else if cmp.same(s[0].value, v) {
        // The head matched, so by distinctness nothing else can: the rest is
        // untouched and exactly one entry went.
        lemma_at_most_one_match(cmp, s, v, 0);
        assert(fresh(cmp, s.skip(1), v)) by {
            assert forall|j: int| 0 <= j < s.skip(1).len() implies !cmp.same(
                s.skip(1)[j].value,
                v,
            ) by {
                assert(s.skip(1)[j] == s[j + 1]);
            }
        }
        lemma_fresh_spec_remove_id(cmp, s.skip(1), v);
    } else {
        lemma_distinct_skip(cmp, s);
        lemma_spec_remove_len_lower(cmp, s.skip(1), v);
    }
}

// ===========================================================================
// 3. The exec code implements the spec
// ===========================================================================

/// On a distinct sequence, at most one entry can match `v`.
///
/// This is where the comparator's equivalence laws earn their keep: without
/// symmetry and transitivity, two entries could each match `v` while not
/// matching each other, and "remove the one we found" would not be the same as
/// "remove everything equivalent".
pub proof fn lemma_at_most_one_match<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    i: int,
)
    requires
        distinct(cmp, s),
        0 <= i < s.len(),
        cmp.same(s[i].value, v),
    ensures
        forall|j: int|
            #![trigger cmp.same(s[j].value, v)]
            0 <= j < s.len() && j != i ==> !cmp.same(s[j].value, v),
{
    lemma_equivalence::<T, C>(cmp);
    assert forall|j: int| 0 <= j < s.len() && j != i implies !cmp.same(s[j].value, v) by {
        if cmp.same(s[j].value, v) {
            // same(s[j], v) and same(v, s[i]) give same(s[j], s[i]), which
            // contradicts distinctness of the pair (min(i,j), max(i,j)).
            assert(cmp.same(v, s[i].value));
            assert(cmp.same(s[j].value, s[i].value));
            assert(cmp.same(s[i].value, s[j].value));
            if i < j {
                assert(!cmp.same(s[i].value, s[j].value));
            } else {
                assert(!cmp.same(s[j].value, s[i].value));
            }
            assert(false);
        }
    }
}

/// If nothing matches, removal is a no-op.
pub proof fn lemma_fresh_spec_remove_id<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    requires
        fresh(cmp, s, v),
    ensures
        spec_remove(cmp, s, v) == s,
    decreases s.len(),
{
    if s.len() == 0 {
        assert(spec_remove(cmp, s, v) =~= s);
    } else {
        assert(!cmp.same(s[0].value, v));
        lemma_fresh_skip(cmp, s, v);
        lemma_fresh_spec_remove_id(cmp, s.skip(1), v);
        lemma_cons_head_skip(s);
    }
}

/// **The bridge for removal.** `Vec::remove(i)` on the one matching index does
/// exactly what the spec's filter does.
pub proof fn lemma_remove_index_is_spec_remove<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    i: int,
)
    requires
        distinct(cmp, s),
        0 <= i < s.len(),
        cmp.same(s[i].value, v),
    ensures
        spec_remove(cmp, s, v) == s.remove(i),
    decreases s.len(),
{
    lemma_at_most_one_match(cmp, s, v, i);
    if i == 0 {
        assert(fresh(cmp, s.skip(1), v)) by {
            assert forall|j: int| 0 <= j < s.skip(1).len() implies !cmp.same(
                s.skip(1)[j].value,
                v,
            ) by {
                assert(s.skip(1)[j] == s[j + 1]);
            }
        }
        lemma_fresh_spec_remove_id(cmp, s.skip(1), v);
        assert(s.remove(0) =~= s.skip(1));
    } else {
        assert(!cmp.same(s[0].value, v));
        lemma_distinct_skip(cmp, s);
        assert(s.skip(1)[i - 1] == s[i]);
        lemma_remove_index_is_spec_remove(cmp, s.skip(1), v, i - 1);
        assert(s.remove(i) =~= seq![s[0]] + s.skip(1).remove(i - 1));
    }
}

/// **The bridge for insertion.** `Vec::insert(i, t)` at the index the scan
/// stops on does exactly what the spec's recursion does.
pub proof fn lemma_insert_at_is_spec_insert<T>(s: Seq<Timeout<T>>, t: Timeout<T>, i: int)
    requires
        0 <= i <= s.len(),
        forall|j: int| 0 <= j < i ==> #[trigger] s[j].deadline <= t.deadline,
        i < s.len() ==> t.deadline < s[i].deadline,
    ensures
        spec_insert(s, t) == s.insert(i, t),
    decreases s.len(),
{
    if s.len() == 0 {
        assert(s.insert(0, t) =~= seq![t]);
    } else if i == 0 {
        assert(t.deadline < s[0].deadline);
        assert(s.insert(0, t) =~= seq![t] + s);
    } else {
        assert(s[0].deadline <= t.deadline);
        assert forall|j: int| 0 <= j < i - 1 implies s.skip(1)[j].deadline <= t.deadline by {
            assert(s.skip(1)[j] == s[j + 1]);
        }
        if i - 1 < s.skip(1).len() {
            assert(s.skip(1)[i - 1] == s[i]);
        }
        lemma_insert_at_is_spec_insert(s.skip(1), t, i - 1);
        assert(s.insert(i, t) =~= seq![s[0]] + s.skip(1).insert(i - 1, t));
    }
}

// ===========================================================================
// Capacity: truncation
// ===========================================================================

/// A prefix of a sorted sequence is sorted.
pub proof fn lemma_take_sorted<T>(s: Seq<Timeout<T>>, n: nat)
    requires
        sorted(s),
    ensures
        sorted(take_at_most(s, n)),
{
    if s.len() > n {
        assert forall|i: int, j: int| 0 <= i <= j < s.take(n as int).len() implies s.take(
            n as int,
        )[i].deadline <= s.take(n as int)[j].deadline by {
            assert(s.take(n as int)[i] == s[i]);
            assert(s.take(n as int)[j] == s[j]);
        }
    }
}

/// A prefix of a distinct sequence is distinct.
pub proof fn lemma_take_distinct<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, n: nat)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, take_at_most(s, n)),
{
    if s.len() > n {
        assert forall|i: int, j: int| 0 <= i < j < s.take(n as int).len() implies !cmp.same(
            s.take(n as int)[i].value,
            s.take(n as int)[j].value,
        ) by {
            assert(s.take(n as int)[i] == s[i]);
            assert(s.take(n as int)[j] == s[j]);
        }
    }
}

/// Truncation respects capacity.
pub proof fn lemma_take_len<T>(s: Seq<Timeout<T>>, n: nat)
    ensures
        take_at_most(s, n).len() <= n,
        take_at_most(s, n).len() <= s.len(),
{
}

// --- suffixes (what `pop_expired` leaves behind) ---

/// A suffix of a sorted sequence is sorted.
pub proof fn lemma_suffix_sorted<T>(s: Seq<Timeout<T>>, k: int)
    requires
        sorted(s),
        0 <= k <= s.len(),
    ensures
        sorted(s.subrange(k, s.len() as int)),
{
    let u = s.subrange(k, s.len() as int);
    assert forall|i: int, j: int| 0 <= i <= j < u.len() implies u[i].deadline <= u[j].deadline by {
        assert(u[i] == s[k + i]);
        assert(u[j] == s[k + j]);
    }
}

/// A suffix of a distinct sequence is distinct.
pub proof fn lemma_suffix_distinct<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, k: int)
    requires
        distinct(cmp, s),
        0 <= k <= s.len(),
    ensures
        distinct(cmp, s.subrange(k, s.len() as int)),
{
    let u = s.subrange(k, s.len() as int);
    assert forall|i: int, j: int| 0 <= i < j < u.len() implies !cmp.same(
        u[i].value,
        u[j].value,
    ) by {
        assert(u[i] == s[k + i]);
        assert(u[j] == s[k + j]);
    }
}


// ===========================================================================
// 4. Sorting
// ===========================================================================
//
// A merge sorts once, at the end, over the union it has built. So the sort has
// to carry everything the wheel cares about: order, no duplicates, and the
// deadline an identity ended up with. The first is what a sort is for; the
// other two are carried by the observation that sorting is a *rearrangement*,
// so any property holding of every entry still holds of every entry afterwards.
// `all_sat` states that once, over an arbitrary predicate, instead of proving
// it separately for each.

/// Every entry of `s` satisfies `p`.
pub open spec fn all_sat<T>(s: Seq<Timeout<T>>, p: spec_fn(Timeout<T>) -> bool) -> bool {
    forall|i: int| 0 <= i < s.len() ==> #[trigger] p(s[i])
}

/// Placing an entry carries a universal property through.
pub proof fn lemma_spec_insert_all_sat<T>(
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    p: spec_fn(Timeout<T>) -> bool,
)
    requires
        all_sat(s, p),
        p(t),
    ensures
        all_sat(spec_insert(s, t), p),
    decreases s.len(),
{
    if s.len() == 0 {
    } else if t.deadline < s[0].deadline {
        assert forall|i: int| 0 <= i < (seq![t] + s).len() implies p((seq![t] + s)[i]) by {
            if i > 0 {
                assert((seq![t] + s)[i] == s[i - 1]);
            }
        }
    } else {
        assert(all_sat(s.skip(1), p)) by {
            assert forall|i: int| 0 <= i < s.skip(1).len() implies p(s.skip(1)[i]) by {
                assert(s.skip(1)[i] == s[i + 1]);
            }
        }
        lemma_spec_insert_all_sat(s.skip(1), t, p);
        let rest = spec_insert(s.skip(1), t);
        assert forall|i: int| 0 <= i < (seq![s[0]] + rest).len() implies p(
            (seq![s[0]] + rest)[i],
        ) by {
            if i > 0 {
                assert((seq![s[0]] + rest)[i] == rest[i - 1]);
            }
        }
    }
}

/// **Sorting carries a universal property through.** Only the first `k` entries
/// need satisfy it, since only those are sorted.
pub proof fn lemma_spec_sort_all_sat<T>(
    u: Seq<Timeout<T>>,
    p: spec_fn(Timeout<T>) -> bool,
    k: nat,
)
    requires
        k <= u.len(),
        forall|l: int| 0 <= l < k ==> #[trigger] p(u[l]),
    ensures
        all_sat(spec_sort_prefix(u, k), p),
    decreases k,
{
    if k == 0 {
        assert(all_sat(Seq::<Timeout<T>>::empty(), p));
    } else {
        lemma_spec_sort_all_sat(u, p, (k - 1) as nat);
        lemma_spec_insert_all_sat(spec_sort_prefix(u, (k - 1) as nat), u[k - 1], p);
    }
}

/// Sorting produces a sorted sequence of the same length.
pub proof fn lemma_spec_sort_wf<T>(u: Seq<Timeout<T>>, k: nat)
    requires
        k <= u.len(),
    ensures
        sorted(spec_sort_prefix(u, k)),
        spec_sort_prefix(u, k).len() == k,
    decreases k,
{
    if k == 0 {
    } else {
        lemma_spec_sort_wf(u, (k - 1) as nat);
        lemma_spec_insert_sorted(spec_sort_prefix(u, (k - 1) as nat), u[k - 1]);
        lemma_spec_insert_len(spec_sort_prefix(u, (k - 1) as nat), u[k - 1]);
    }
}

/// **Sorting cannot create a duplicate.**
///
/// The entry about to be placed is not equivalent to any of the entries already
/// placed, because those are exactly the entries before it in `u` and `u` has
/// no duplicates. `all_sat` is what turns "the entries already placed are the
/// earlier ones" into something usable without reasoning about permutations.
pub proof fn lemma_spec_sort_distinct<T, C: Comparator<T>>(cmp: C, u: Seq<Timeout<T>>, k: nat)
    requires
        distinct(cmp, u),
        k <= u.len(),
    ensures
        distinct(cmp, spec_sort_prefix(u, k)),
    decreases k,
{
    if k == 0 {
    } else {
        lemma_spec_sort_distinct(cmp, u, (k - 1) as nat);
        let w = spec_sort_prefix(u, (k - 1) as nat);
        let unequal = |x: Timeout<T>| !cmp.same(x.value, u[k - 1].value);
        assert forall|l: int| 0 <= l < k - 1 implies unequal(u[l]) by {
        }
        lemma_spec_sort_all_sat(u, unequal, (k - 1) as nat);
        assert(fresh(cmp, w, u[k - 1].value)) by {
            assert forall|i: int| 0 <= i < w.len() implies !cmp.same(w[i].value, u[k - 1].value)
                by {
                assert(unequal(w[i]));
            }
        }
        lemma_spec_insert_distinct(cmp, w, u[k - 1]);
    }
}

/// Sorting keeps the deadline an identity is carrying.
pub proof fn lemma_spec_sort_keeps_identity<T, C: Comparator<T>>(
    cmp: C,
    u: Seq<Timeout<T>>,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, u, v, d),
    ensures
        identity_carries(cmp, spec_sort(u), v, d),
{
    let carries = |x: Timeout<T>| cmp.same(x.value, v) ==> x.deadline == d;
    assert forall|l: int| 0 <= l < u.len() implies carries(u[l]) by {
    }
    lemma_spec_sort_all_sat(u, carries, u.len());
    assert forall|i: int|
        0 <= i < spec_sort(u).len() && cmp.same(spec_sort(u)[i].value, v) implies spec_sort(
        u,
    )[i].deadline == d by {
        assert(carries(spec_sort(u)[i]));
    }
}

/// Cutting to capacity only ever drops entries, so it cannot break an
/// identity's deadline.
pub proof fn lemma_take_keeps_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    n: nat,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, s, v, d),
    ensures
        identity_carries(cmp, take_at_most(s, n), v, d),
{
    if s.len() > n {
        assert forall|k: int|
            0 <= k < s.take(n as int).len() && cmp.same(s.take(n as int)[k].value, v) implies s.take(
            n as int,
        )[k].deadline == d by {
            assert(s.take(n as int)[k] == s[k]);
        }
    }
}

// ===========================================================================
// 5. Building the union
// ===========================================================================
//
// A merge builds `spec_untouched + spec_updates` and hands it to the sort. Both
// halves are filters written in the same shape as `spec_remove`, so the proofs
// below are the same three shapes over and over: a filter drops entries, so it
// preserves anything of the form "every entry ...", and the two halves cannot
// collide because one keeps only entries the batch does not mention and the
// other keeps only entries it does.

/// Mentioning splits at the head, the same way `fresh` does.
pub proof fn lemma_mentions_cons<T, C: Comparator<T>>(cmp: C, batch: Seq<Timeout<T>>, v: T)
    requires
        batch.len() > 0,
    ensures
        mentions(cmp, batch, v) <==> (cmp.same(batch[0].value, v) || mentions(
            cmp,
            batch.skip(1),
            v,
        )),
{
    if fresh(cmp, batch, v) {
        lemma_fresh_skip(cmp, batch, v);
    }
    if !cmp.same(batch[0].value, v) && fresh(cmp, batch.skip(1), v) {
        lemma_fresh_cons(cmp, batch[0], batch.skip(1), v);
        lemma_cons_head_skip(batch);
    }
}

/// A longer batch mentions everything its tail mentions.
pub proof fn lemma_mentions_skip<T, C: Comparator<T>>(cmp: C, batch: Seq<Timeout<T>>, v: T)
    requires
        batch.len() > 0,
        mentions(cmp, batch.skip(1), v),
    ensures
        mentions(cmp, batch, v),
{
    lemma_mentions_cons(cmp, batch, v);
}

// --- spec_untouched ---

/// Everything left untouched is, by construction, unmentioned by the batch.
pub proof fn lemma_untouched_unmentioned<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
)
    ensures
        forall|i: int|
            0 <= i < spec_untouched(cmp, s, batch).len() ==> !mentions(
                cmp,
                batch,
                #[trigger] spec_untouched(cmp, s, batch)[i].value,
            ),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        lemma_untouched_unmentioned(cmp, s.skip(1), batch);
        if !mentions(cmp, batch, s[0].value) {
            let rest = spec_untouched(cmp, s.skip(1), batch);
            assert forall|i: int| 0 <= i < (seq![s[0]] + rest).len() implies !mentions(
                cmp,
                batch,
                (seq![s[0]] + rest)[i].value,
            ) by {
                if i > 0 {
                    assert((seq![s[0]] + rest)[i] == rest[i - 1]);
                }
            }
        }
    }
}

/// Dropping entries cannot introduce a duplicate.
pub proof fn lemma_untouched_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_untouched(cmp, s, batch)),
    decreases s.len(),
{
    lemma_equivalence::<T, C>(cmp);
    if s.len() == 0 {
    } else {
        lemma_distinct_skip(cmp, s);
        lemma_untouched_distinct(cmp, s.skip(1), batch);
        if !mentions(cmp, batch, s[0].value) {
            assert(fresh(cmp, s.skip(1), s[0].value)) by {
                assert forall|i: int| 0 <= i < s.skip(1).len() implies !cmp.same(
                    s.skip(1)[i].value,
                    s[0].value,
                ) by {
                    assert(s.skip(1)[i] == s[i + 1]);
                    assert(!cmp.same(s[0].value, s[i + 1].value));
                }
            }
            lemma_untouched_preserves_fresh(cmp, s.skip(1), batch, s[0].value);
            lemma_distinct_cons(cmp, s[0], spec_untouched(cmp, s.skip(1), batch));
        }
    }
}

/// Dropping entries cannot introduce a match either.
pub proof fn lemma_untouched_preserves_fresh<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    w: T,
)
    requires
        fresh(cmp, s, w),
    ensures
        fresh(cmp, spec_untouched(cmp, s, batch), w),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        lemma_fresh_skip(cmp, s, w);
        lemma_untouched_preserves_fresh(cmp, s.skip(1), batch, w);
        if !mentions(cmp, batch, s[0].value) {
            lemma_fresh_cons(cmp, s[0], spec_untouched(cmp, s.skip(1), batch), w);
        }
    }
}

// --- spec_updates ---

/// Every update kept is one the batch mentions — it came from the batch.
pub proof fn lemma_updates_mentioned<T, C: Comparator<T>>(cmp: C, batch: Seq<Timeout<T>>)
    ensures
        forall|i: int|
            0 <= i < spec_updates(cmp, batch).len() ==> mentions(
                cmp,
                batch,
                #[trigger] spec_updates(cmp, batch)[i].value,
            ),
    decreases batch.len(),
{
    lemma_equivalence::<T, C>(cmp);
    if batch.len() == 0 {
    } else {
        lemma_updates_mentioned(cmp, batch.skip(1));
        let rest = spec_updates(cmp, batch.skip(1));
        assert forall|i: int| 0 <= i < rest.len() implies mentions(cmp, batch, rest[i].value) by {
            lemma_mentions_skip(cmp, batch, rest[i].value);
        }
        if !mentions(cmp, batch.skip(1), batch[0].value) {
            assert forall|i: int| 0 <= i < (seq![batch[0]] + rest).len() implies mentions(
                cmp,
                batch,
                (seq![batch[0]] + rest)[i].value,
            ) by {
                if i > 0 {
                    assert((seq![batch[0]] + rest)[i] == rest[i - 1]);
                } else {
                    lemma_mentions_cons(cmp, batch, batch[0].value);
                }
            }
        }
    }
}

/// Freshness survives, since updates only ever drops arrivals.
pub proof fn lemma_updates_preserves_fresh<T, C: Comparator<T>>(
    cmp: C,
    batch: Seq<Timeout<T>>,
    w: T,
)
    requires
        fresh(cmp, batch, w),
    ensures
        fresh(cmp, spec_updates(cmp, batch), w),
    decreases batch.len(),
{
    if batch.len() == 0 {
    } else {
        lemma_fresh_skip(cmp, batch, w);
        lemma_updates_preserves_fresh(cmp, batch.skip(1), w);
        if !mentions(cmp, batch.skip(1), batch[0].value) {
            lemma_fresh_cons(cmp, batch[0], spec_updates(cmp, batch.skip(1)), w);
        }
    }
}

/// **The updates hold each timeout at most once**, whatever the batch does.
///
/// No hypothesis on the batch: an arrival is kept only when no later arrival
/// names the same timeout, so two kept arrivals cannot name the same one.
pub proof fn lemma_updates_distinct<T, C: Comparator<T>>(cmp: C, batch: Seq<Timeout<T>>)
    ensures
        distinct(cmp, spec_updates(cmp, batch)),
    decreases batch.len(),
{
    if batch.len() == 0 {
    } else {
        lemma_updates_distinct(cmp, batch.skip(1));
        if !mentions(cmp, batch.skip(1), batch[0].value) {
            // The head was kept, so nothing later matches it -- and the kept
            // tail is drawn from what comes later.
            lemma_updates_preserves_fresh(cmp, batch.skip(1), batch[0].value);
            lemma_distinct_cons(cmp, batch[0], spec_updates(cmp, batch.skip(1)));
        }
    }
}

/// **An arrival no later arrival overrides sets its identity's deadline.**
pub proof fn lemma_updates_set_identity<T, C: Comparator<T>>(
    cmp: C,
    batch: Seq<Timeout<T>>,
    j: int,
)
    requires
        0 <= j < batch.len(),
        forall|l: int| j < l < batch.len() ==> !cmp.same(#[trigger] batch[l].value, batch[j].value),
    ensures
        identity_carries(cmp, spec_updates(cmp, batch), batch[j].value, batch[j].deadline),
    decreases batch.len(),
{
    lemma_equivalence::<T, C>(cmp);
    let v = batch[j].value;
    let d = batch[j].deadline;

    if j == 0 {
        // Nothing later matches the head, so the head is kept and the tail
        // holds nothing equivalent to it.
        assert(fresh(cmp, batch.skip(1), v)) by {
            assert forall|i: int| 0 <= i < batch.skip(1).len() implies !cmp.same(
                batch.skip(1)[i].value,
                v,
            ) by {
                assert(batch.skip(1)[i] == batch[i + 1]);
            }
        }
        lemma_updates_preserves_fresh(cmp, batch.skip(1), v);
        let rest = spec_updates(cmp, batch.skip(1));
        assert forall|i: int|
            0 <= i < (seq![batch[0]] + rest).len() && cmp.same(
                (seq![batch[0]] + rest)[i].value,
                v,
            ) implies (seq![batch[0]] + rest)[i].deadline == d by {
            if i > 0 {
                assert((seq![batch[0]] + rest)[i] == rest[i - 1]);
            }
        }
    } else {
        assert(batch.skip(1)[j - 1] == batch[j]);
        assert forall|l: int|
            #![trigger cmp.same(batch.skip(1)[l].value, batch.skip(1)[j - 1].value)]
            j - 1 < l < batch.skip(1).len() implies !cmp.same(
            batch.skip(1)[l].value,
            batch.skip(1)[j - 1].value,
        ) by {
            assert(batch.skip(1)[l] == batch[l + 1]);
        }
        lemma_updates_set_identity(cmp, batch.skip(1), j - 1);
        let rest = spec_updates(cmp, batch.skip(1));
        if !mentions(cmp, batch.skip(1), batch[0].value) {
            // The head is kept. It cannot match `batch[j]`: if it did, the tail
            // would mention it (at `j - 1`) and it would have been dropped.
            assert(!cmp.same(batch[0].value, v)) by {
                if cmp.same(batch[0].value, v) {
                    assert(cmp.same(batch.skip(1)[j - 1].value, batch[0].value));
                    assert(!fresh(cmp, batch.skip(1), batch[0].value));
                    assert(false);
                }
            }
            assert forall|i: int|
                0 <= i < (seq![batch[0]] + rest).len() && cmp.same(
                    (seq![batch[0]] + rest)[i].value,
                    v,
                ) implies (seq![batch[0]] + rest)[i].deadline == d by {
                if i > 0 {
                    assert((seq![batch[0]] + rest)[i] == rest[i - 1]);
                }
            }
        }
    }
}

// --- putting the two halves together ---

/// Two sequences that cannot collide concatenate without a duplicate.
pub proof fn lemma_concat_distinct<T, C: Comparator<T>>(
    cmp: C,
    a: Seq<Timeout<T>>,
    b: Seq<Timeout<T>>,
)
    requires
        distinct(cmp, a),
        distinct(cmp, b),
        forall|i: int, j: int|
            0 <= i < a.len() && 0 <= j < b.len() ==> !cmp.same(
                #[trigger] a[i].value,
                #[trigger] b[j].value,
            ),
    ensures
        distinct(cmp, a + b),
{
    assert forall|i: int, j: int| 0 <= i < j < (a + b).len() implies !cmp.same(
        (a + b)[i].value,
        (a + b)[j].value,
    ) by {
        if j < a.len() {
            assert((a + b)[i] == a[i]);
            assert((a + b)[j] == a[j]);
        } else if i < a.len() {
            assert((a + b)[i] == a[i]);
            assert((a + b)[j] == b[j - a.len()]);
        } else {
            assert((a + b)[i] == b[i - a.len()]);
            assert((a + b)[j] == b[j - a.len()]);
        }
    }
}

/// An identity's deadline survives concatenation when both halves agree on it.
pub proof fn lemma_concat_identity<T, C: Comparator<T>>(
    cmp: C,
    a: Seq<Timeout<T>>,
    b: Seq<Timeout<T>>,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, a, v, d),
        identity_carries(cmp, b, v, d),
    ensures
        identity_carries(cmp, a + b, v, d),
{
    assert forall|i: int|
        0 <= i < (a + b).len() && cmp.same((a + b)[i].value, v) implies (a + b)[i].deadline == d
        by {
        if i < a.len() {
            assert((a + b)[i] == a[i]);
        } else {
            assert((a + b)[i] == b[i - a.len()]);
        }
    }
}

/// **The union holds each timeout at most once.**
///
/// The two halves cannot collide by construction: one keeps only entries the
/// batch does *not* mention, the other only entries it does.
pub proof fn lemma_union_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_untouched(cmp, s, batch) + spec_updates(cmp, batch)),
{
    lemma_equivalence::<T, C>(cmp);
    let a = spec_untouched(cmp, s, batch);
    let b = spec_updates(cmp, batch);
    lemma_untouched_distinct(cmp, s, batch);
    lemma_updates_distinct(cmp, batch);
    lemma_untouched_unmentioned(cmp, s, batch);
    lemma_updates_mentioned(cmp, batch);
    assert forall|i: int, j: int|
        0 <= i < a.len() && 0 <= j < b.len() implies !cmp.same(a[i].value, b[j].value) by {
        if cmp.same(a[i].value, b[j].value) {
            // `b[j]` is mentioned by the batch and `a[i]` matches it, so the
            // batch mentions `a[i]` too -- but `a` holds only unmentioned
            // entries.
            let k = choose|k: int| 0 <= k < batch.len() && cmp.same(batch[k].value, b[j].value);
            assert(cmp.same(batch[k].value, a[i].value));
            assert(!fresh(cmp, batch, a[i].value));
            assert(false);
        }
    }
    lemma_concat_distinct(cmp, a, b);
}

// --- how many entries the union has ---
//
// "A merge never loses a slot" needs a counting argument that the previous
// shape got for free: an entry the wheel loses is one the batch mentions, and
// every mentioned identity has an update waiting for it, so the losses are paid
// for one-for-one. `lemma_dropped_are_paid_for` is that argument, done by
// spending the updates as it walks the wheel rather than by building an
// injection.

/// Removal shortens a sequence that actually holds a match.
pub proof fn lemma_spec_remove_len_strict<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, v: T)
    requires
        mentions(cmp, s, v),
    ensures
        spec_remove(cmp, s, v).len() < s.len(),
    decreases s.len(),
{
    // An empty sequence mentions nothing, so there is a head to look at.
    assert(s.len() > 0);
    if cmp.same(s[0].value, v) {
        // The head goes; whatever the tail loses on top of that only helps.
        lemma_spec_remove_len(cmp, s.skip(1), v);
    } else {
        lemma_mentions_cons(cmp, s, v);
        lemma_spec_remove_len_strict(cmp, s.skip(1), v);
    }
}

/// Removing one identity does not remove a different one.
pub proof fn lemma_remove_preserves_mentions<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    v: T,
    w: T,
)
    requires
        mentions(cmp, s, w),
        !cmp.same(v, w),
    ensures
        mentions(cmp, spec_remove(cmp, s, v), w),
    decreases s.len(),
{
    lemma_equivalence::<T, C>(cmp);
    lemma_mentions_cons(cmp, s, w);
    if cmp.same(s[0].value, v) {
        // The head goes, but it was not the match for `w`: if it were, `v` and
        // `w` would be the same.
        assert(!cmp.same(s[0].value, w));
        lemma_remove_preserves_mentions(cmp, s.skip(1), v, w);
    } else {
        let tail = spec_remove(cmp, s.skip(1), v);
        if cmp.same(s[0].value, w) {
            assert(!fresh(cmp, seq![s[0]] + tail, w)) by {
                assert((seq![s[0]] + tail)[0] == s[0]);
            }
        } else {
            lemma_remove_preserves_mentions(cmp, s.skip(1), v, w);
            assert(!fresh(cmp, seq![s[0]] + tail, w)) by {
                let k = choose|k: int| 0 <= k < tail.len() && cmp.same(tail[k].value, w);
                assert((seq![s[0]] + tail)[k + 1] == tail[k]);
            }
        }
    }
}

/// **Every identity the batch mentions has an update waiting for it.**
///
/// The updates drop an arrival only when a later one names the same timeout, so
/// an identity can never be dropped altogether — the last arrival naming it
/// always survives.
pub proof fn lemma_updates_keep_mentions<T, C: Comparator<T>>(
    cmp: C,
    batch: Seq<Timeout<T>>,
    v: T,
)
    requires
        mentions(cmp, batch, v),
    ensures
        mentions(cmp, spec_updates(cmp, batch), v),
    decreases batch.len(),
{
    lemma_equivalence::<T, C>(cmp);
    lemma_mentions_cons(cmp, batch, v);
    let tail = spec_updates(cmp, batch.skip(1));

    if mentions(cmp, batch.skip(1), v) {
        lemma_updates_keep_mentions(cmp, batch.skip(1), v);
        if !mentions(cmp, batch.skip(1), batch[0].value) {
            assert(!fresh(cmp, seq![batch[0]] + tail, v)) by {
                let k = choose|k: int| 0 <= k < tail.len() && cmp.same(tail[k].value, v);
                assert((seq![batch[0]] + tail)[k + 1] == tail[k]);
            }
        }
    } else {
        // Only the head names `v`, so nothing later can override it and the
        // head is kept.
        assert(cmp.same(batch[0].value, v));
        assert(!mentions(cmp, batch.skip(1), batch[0].value)) by {
            if mentions(cmp, batch.skip(1), batch[0].value) {
                let k = choose|k: int|
                    0 <= k < batch.skip(1).len() && cmp.same(
                        batch.skip(1)[k].value,
                        batch[0].value,
                    );
                assert(cmp.same(batch.skip(1)[k].value, v));
                assert(!fresh(cmp, batch.skip(1), v));
                assert(false);
            }
        }
        assert(!fresh(cmp, seq![batch[0]] + tail, v)) by {
            assert((seq![batch[0]] + tail)[0] == batch[0]);
        }
    }
}

/// **The entries the wheel loses are paid for.**
///
/// `u` stands for the updates still unspent. Walking the wheel, an entry the
/// batch mentions is dropped and one update is spent on it; the wheel's
/// distinctness is what guarantees the *same* update is never spent twice,
/// since no two entries of the wheel share an identity.
pub proof fn lemma_dropped_are_paid_for<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    u: Seq<Timeout<T>>,
)
    requires
        distinct(cmp, s),
        // ... and `u` too, so that spending one update removes exactly one.
        distinct(cmp, u),
        forall|i: int|
            0 <= i < s.len() && mentions(cmp, batch, #[trigger] s[i].value) ==> mentions(
                cmp,
                u,
                s[i].value,
            ),
    ensures
        s.len() <= spec_untouched(cmp, s, batch).len() + u.len(),
    decreases s.len(),
{
    lemma_equivalence::<T, C>(cmp);
    if s.len() == 0 {
    } else {
        lemma_distinct_skip(cmp, s);
        if mentions(cmp, batch, s[0].value) {
            // Spend one update on the entry we are about to lose.
            let spent = spec_remove(cmp, u, s[0].value);
            lemma_spec_remove_len_strict(cmp, u, s[0].value);
            lemma_spec_remove_len_lower(cmp, u, s[0].value);
            lemma_spec_remove_distinct(cmp, u, s[0].value);
            assert forall|i: int|
                0 <= i < s.skip(1).len() && mentions(
                    cmp,
                    batch,
                    s.skip(1)[i].value,
                ) implies mentions(cmp, spent, s.skip(1)[i].value) by {
                assert(s.skip(1)[i] == s[i + 1]);
                assert(!cmp.same(s[0].value, s[i + 1].value));
                lemma_remove_preserves_mentions(cmp, u, s[0].value, s[i + 1].value);
            }
            lemma_dropped_are_paid_for(cmp, s.skip(1), batch, spent);
        } else {
            lemma_dropped_are_paid_for(cmp, s.skip(1), batch, u);
        }
    }
}

/// **The union is at least as long as the wheel was.**
pub proof fn lemma_union_len_lower<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
)
    requires
        distinct(cmp, s),
    ensures
        s.len() <= (spec_untouched(cmp, s, batch) + spec_updates(cmp, batch)).len(),
{
    let u = spec_updates(cmp, batch);
    lemma_updates_distinct(cmp, batch);
    assert forall|i: int|
        0 <= i < s.len() && mentions(cmp, batch, s[i].value) implies mentions(
            cmp,
            u,
            s[i].value,
        ) by {
        lemma_updates_keep_mentions(cmp, batch, s[i].value);
    }
    lemma_dropped_are_paid_for(cmp, s, batch, u);
}

// ===========================================================================
// 6. What the wheel guarantees
// ===========================================================================
//
// Each of these is a consequence of the four words of `spec_merge` — drop, add,
// sort, cut — and each is stated with the weakest hypothesis it actually needs
// rather than with the whole wheel invariant, so it is clear what it rests on.

/// **A merge lands well-formed, and never loses a slot.**
///
/// The last conjunct is the counting one: an entry the wheel loses is one the
/// batch mentions, and every mentioned identity has an update waiting for it,
/// so the losses are paid for one-for-one. See
/// [`lemma_dropped_are_paid_for`].
pub proof fn lemma_merge_wf<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    capacity: nat,
)
    requires
        distinct(cmp, s),
        s.len() <= capacity,
    ensures
        sorted(spec_merge(cmp, s, batch, capacity)),
        distinct(cmp, spec_merge(cmp, s, batch, capacity)),
        spec_merge(cmp, s, batch, capacity).len() <= capacity,
        s.len() <= spec_merge(cmp, s, batch, capacity).len(),
{
    let union = spec_untouched(cmp, s, batch) + spec_updates(cmp, batch);
    lemma_union_distinct(cmp, s, batch);
    lemma_spec_sort_wf(union, union.len());
    lemma_spec_sort_distinct(cmp, union, union.len());
    lemma_take_sorted(spec_sort(union), capacity);
    lemma_take_distinct(cmp, spec_sort(union), capacity);
    lemma_take_len(spec_sort(union), capacity);
    // Sorting is a rearrangement, so the union's length survives it, and the
    // cut cannot take the result below `capacity` -- which the wheel was
    // already within.
    lemma_union_len_lower(cmp, s, batch);
}

/// **The drop rule.** Everything the cut dropped is due at or after everything
/// it kept.
///
/// This is the whole of "sort, then cut": the survivors are a prefix of a
/// deadline-ordered sequence, so a merge can cost you a far timeout and never a
/// near one.
pub proof fn lemma_merge_horizon<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    capacity: nat,
)
    ensures
        ({
            let union = spec_untouched(cmp, s, batch) + spec_updates(cmp, batch);
            let ordered = spec_sort(union);
            let kept = spec_merge(cmp, s, batch, capacity);
            &&& sorted(ordered)
            &&& kept.len() <= ordered.len()
            &&& kept.len() < ordered.len() ==> kept.len() == capacity
            &&& forall|i: int, j: int|
                #![trigger kept[i].deadline, ordered[j].deadline]
                0 <= i < kept.len() <= j < ordered.len() ==> kept[i].deadline
                    <= ordered[j].deadline
        }),
{
    let union = spec_untouched(cmp, s, batch) + spec_updates(cmp, batch);
    let ordered = spec_sort(union);
    let kept = spec_merge(cmp, s, batch, capacity);
    lemma_spec_sort_wf(union, union.len());
    lemma_take_len(ordered, capacity);
    assert forall|i: int, j: int| 0 <= i < kept.len() <= j < ordered.len() implies kept[i].deadline
        <= ordered[j].deadline by {
        assert(kept[i] == ordered[i]);
    }
}

/// **If the wheel held no duplicate before a merge, it holds none after.**
///
/// The hypothesis is the whole hypothesis: no sortedness, no capacity bound, no
/// condition at all on the batch. The batch may repeat an identity as often as
/// it likes and collide with anything already held; what comes out still
/// carries each logical timeout at most once. Merging is the only way to put a
/// timeout into a wheel, so this makes "the wheel never holds a duplicate" true
/// of every wheel a caller can build.
pub proof fn lemma_merge_preserves_no_duplicates<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    capacity: nat,
)
    requires
        no_duplicates(cmp, s),
    ensures
        no_duplicates(cmp, spec_merge(cmp, s, batch, capacity)),
{
    let union = spec_untouched(cmp, s, batch) + spec_updates(cmp, batch);
    lemma_no_duplicates_iff_distinct(cmp, s);
    lemma_union_distinct(cmp, s, batch);
    lemma_spec_sort_distinct(cmp, union, union.len());
    lemma_take_distinct(cmp, spec_sort(union), capacity);
    lemma_no_duplicates_iff_distinct(cmp, spec_merge(cmp, s, batch, capacity));
}

/// **A merge that carries an arrival for an identity sets that identity's
/// deadline.** Nothing under it survives with any other.
///
/// The condition on the batch is that no *later* arrival names the same
/// identity — the last update is the one that stands. This assumes nothing
/// whatever about the wheel: not sortedness, not capacity, not even freedom
/// from duplicates.
pub proof fn lemma_merge_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    j: int,
    capacity: nat,
)
    requires
        0 <= j < batch.len(),
        forall|l: int| j < l < batch.len() ==> !cmp.same(#[trigger] batch[l].value, batch[j].value),
    ensures
        identity_carries(
            cmp,
            spec_merge(cmp, s, batch, capacity),
            batch[j].value,
            batch[j].deadline,
        ),
{
    lemma_equivalence::<T, C>(cmp);
    let v = batch[j].value;
    let d = batch[j].deadline;
    let a = spec_untouched(cmp, s, batch);
    let b = spec_updates(cmp, batch);

    // The untouched half cannot hold this identity at all: the batch mentions
    // it, at `j`, and the untouched half keeps only what the batch does not
    // mention.
    lemma_untouched_unmentioned(cmp, s, batch);
    assert(identity_carries(cmp, a, v, d)) by {
        assert forall|i: int| 0 <= i < a.len() && cmp.same(a[i].value, v) implies a[i].deadline
            == d by {
            assert(cmp.same(batch[j].value, a[i].value));
            assert(!fresh(cmp, batch, a[i].value));
            assert(false);
        }
    }

    lemma_updates_set_identity(cmp, batch, j);
    lemma_concat_identity(cmp, a, b, v, d);
    lemma_spec_sort_keeps_identity(cmp, a + b, v, d);
    lemma_take_keeps_identity(cmp, spec_sort(a + b), capacity, v, d);
}

/// **If a timeout is already in the wheel and the batch carries an update for
/// it, the old entry does not survive: it is replaced with the new deadline, or
/// pushed out altogether.**
///
/// The two conclusions are the whole of it. The first says the entry is not
/// still sitting there at its old deadline — anywhere, at any position. The
/// second says what happened instead: either it is back under the new deadline,
/// or the wheel no longer holds that identity at all, which is what happens
/// when the new deadline lands beyond the capacity horizon.
///
/// There is no third outcome, and the wheel never ends up holding the timeout
/// twice — that is [`lemma_merge_preserves_no_duplicates`], and this theorem is
/// what makes it load-bearing rather than trivial. On its own, "never holds a
/// duplicate" would also be satisfied by a merge that quietly ignored updates.
pub proof fn lemma_merge_replaces_or_evicts<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    i: int,
    j: int,
    capacity: nat,
)
    requires
        0 <= i < s.len(),
        0 <= j < batch.len(),
        // the timeout is already in the wheel ...
        cmp.same(s[i].value, batch[j].value),
        // ... and the batch moves its deadline
        s[i].deadline != batch[j].deadline,
        // ... and no later arrival overrides this one
        forall|l: int| j < l < batch.len() ==> !cmp.same(#[trigger] batch[l].value, batch[j].value),
    ensures
        ({
            let r = spec_merge(cmp, s, batch, capacity);
            // The old entry is gone -- not at its old deadline, nowhere.
            &&& !holds_at(cmp, r, batch[j].value, s[i].deadline)
            // Either it is back with the new deadline, or it is out entirely.
            &&& holds_at(cmp, r, batch[j].value, batch[j].deadline) || fresh(
                cmp,
                r,
                batch[j].value,
            )
        }),
{
    let r = spec_merge(cmp, s, batch, capacity);
    let v = batch[j].value;
    lemma_merge_sets_identity(cmp, s, batch, j, capacity);

    assert(!holds_at(cmp, r, v, s[i].deadline)) by {
        if holds_at(cmp, r, v, s[i].deadline) {
            let k = choose|k: int|
                0 <= k < r.len() && cmp.same(r[k].value, v) && r[k].deadline == s[i].deadline;
            assert(r[k].deadline == batch[j].deadline);
        }
    }

    if !fresh(cmp, r, v) {
        let k = choose|k: int| 0 <= k < r.len() && cmp.same(r[k].value, v);
        assert(r[k].deadline == batch[j].deadline);
        assert(holds_at(cmp, r, v, batch[j].deadline));
    }
}

} // verus!
