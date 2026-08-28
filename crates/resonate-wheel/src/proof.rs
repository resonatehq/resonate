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

// ===========================================================================
// 5. Applying the batch
// ===========================================================================

/// Applying an arrival keeps the wheel free of duplicates.
///
/// Note the hypotheses: `distinct` and nothing else. Not sortedness — the wheel
/// is deliberately unsorted while a merge is applying its batch, and only put
/// back in order at the end — and not the capacity bound either.
pub proof fn lemma_spec_apply_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_apply(cmp, s, t)),
        s.len() <= spec_apply(cmp, s, t).len(),
        spec_apply(cmp, s, t).len() <= s.len() + 1,
{
    let removed = spec_remove(cmp, s, t.value);
    lemma_spec_remove_distinct(cmp, s, t.value);
    lemma_spec_remove_fresh(cmp, s, t.value);
    lemma_spec_remove_len(cmp, s, t.value);
    lemma_spec_remove_len_lower(cmp, s, t.value);
    lemma_equivalence::<T, C>(cmp);
    assert forall|i: int, j: int| 0 <= i < j < removed.push(t).len() implies !cmp.same(
        removed.push(t)[i].value,
        removed.push(t)[j].value,
    ) by {
        if j < removed.len() {
            assert(removed.push(t)[i] == removed[i]);
            assert(removed.push(t)[j] == removed[j]);
        } else {
            assert(removed.push(t)[i] == removed[i]);
            assert(!cmp.same(removed[i].value, t.value));
        }
    }
}

/// Applying a whole batch keeps the wheel free of duplicates, and never
/// shortens it.
pub proof fn lemma_spec_apply_all_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    k: nat,
)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_apply_all(cmp, s, inc, k)),
        s.len() <= spec_apply_all(cmp, s, inc, k).len(),
    decreases k,
{
    if k == 0 {
    } else {
        lemma_spec_apply_all_distinct(cmp, s, inc, (k - 1) as nat);
        lemma_spec_apply_distinct(cmp, spec_apply_all(cmp, s, inc, (k - 1) as nat), inc[k - 1]);
    }
}

/// **An arrival sets its identity's deadline.** Whatever the wheel held under
/// that identity, afterwards there is one entry for it and it is the arrival.
///
/// No hypothesis at all: the removal strips *every* equivalent entry, so this
/// holds even on a wheel that is unsorted, over capacity, or already carrying
/// duplicates.
pub proof fn lemma_spec_apply_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
)
    ensures
        identity_carries(cmp, spec_apply(cmp, s, t), t.value, t.deadline),
{
    let removed = spec_remove(cmp, s, t.value);
    lemma_spec_remove_fresh(cmp, s, t.value);
    assert forall|i: int|
        0 <= i < removed.push(t).len() && cmp.same(removed.push(t)[i].value, t.value) implies
        removed.push(t)[i].deadline == t.deadline by {
        if i < removed.len() {
            assert(removed.push(t)[i] == removed[i]);
        }
    }
}

/// An arrival for a *different* identity leaves this one's deadline alone.
pub proof fn lemma_spec_apply_keeps_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, s, v, d),
        !cmp.same(t.value, v),
    ensures
        identity_carries(cmp, spec_apply(cmp, s, t), v, d),
{
    let removed = spec_remove(cmp, s, t.value);
    lemma_spec_remove_keeps_identity(cmp, s, t.value, v, d);
    assert forall|i: int|
        0 <= i < removed.push(t).len() && cmp.same(removed.push(t)[i].value, v) implies
        removed.push(t)[i].deadline == d by {
        if i < removed.len() {
            assert(removed.push(t)[i] == removed[i]);
        }
    }
}

/// Removal only ever drops entries, so it cannot break an identity's deadline.
pub proof fn lemma_spec_remove_keeps_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    w: T,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, s, v, d),
    ensures
        identity_carries(cmp, spec_remove(cmp, s, w), v, d),
    decreases s.len(),
{
    if s.len() == 0 {
    } else {
        assert(identity_carries(cmp, s.skip(1), v, d)) by {
            assert forall|k: int|
                0 <= k < s.skip(1).len() && cmp.same(s.skip(1)[k].value, v) implies s.skip(
                1,
            )[k].deadline == d by {
                assert(s.skip(1)[k] == s[k + 1]);
            }
        }
        lemma_spec_remove_keeps_identity(cmp, s.skip(1), w, v, d);
        if !cmp.same(s[0].value, w) {
            let rest = spec_remove(cmp, s.skip(1), w);
            assert forall|k: int|
                0 <= k < (seq![s[0]] + rest).len() && cmp.same(
                    (seq![s[0]] + rest)[k].value,
                    v,
                ) implies (seq![s[0]] + rest)[k].deadline == d by {
                if k > 0 {
                    assert((seq![s[0]] + rest)[k] == rest[k - 1]);
                }
            }
        }
    }
}

/// Cutting to capacity only ever drops entries.
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

/// Once the batch has applied the arrival at `j`, every later arrival preserves
/// the deadline it set.
pub proof fn lemma_spec_apply_all_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    j: int,
    k: nat,
)
    requires
        0 <= j < inc.len(),
        j < k <= inc.len(),
        forall|l: int|
            j < l < inc.len() ==> !cmp.same(#[trigger] inc[l].value, inc[j].value),
    ensures
        identity_carries(cmp, spec_apply_all(cmp, s, inc, k), inc[j].value, inc[j].deadline),
    decreases k,
{
    let u = spec_apply_all(cmp, s, inc, (k - 1) as nat);
    if k == j + 1 {
        lemma_spec_apply_sets_identity(cmp, u, inc[k - 1]);
    } else {
        lemma_spec_apply_all_sets_identity(cmp, s, inc, j, (k - 1) as nat);
        lemma_spec_apply_keeps_identity(cmp, u, inc[k - 1], inc[j].value, inc[j].deadline);
    }
}

// ===========================================================================
// 6. What the wheel guarantees
// ===========================================================================
//
// Each of these is a consequence of the three lines of `spec_merge`, and each
// is stated with the weakest hypothesis it actually needs rather than with the
// whole wheel invariant, so it is clear what each one rests on.

/// **A merge lands well-formed, and never loses a slot.**
pub proof fn lemma_merge_wf<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    capacity: nat,
)
    requires
        distinct(cmp, s),
        s.len() <= capacity,
    ensures
        sorted(spec_merge(cmp, s, inc, capacity)),
        distinct(cmp, spec_merge(cmp, s, inc, capacity)),
        spec_merge(cmp, s, inc, capacity).len() <= capacity,
        s.len() <= spec_merge(cmp, s, inc, capacity).len(),
{
    let union = spec_apply_all(cmp, s, inc, inc.len());
    let ordered = spec_sort(union);
    lemma_spec_apply_all_distinct(cmp, s, inc, inc.len());
    lemma_spec_sort_wf(union, union.len());
    lemma_spec_sort_distinct(cmp, union, union.len());
    lemma_take_sorted(ordered, capacity);
    lemma_take_distinct(cmp, ordered, capacity);
    lemma_take_len(ordered, capacity);
}

/// **The drop rule.** Everything the cut dropped is due at or after everything
/// it kept.
///
/// This is the whole of "sort, then cut": the survivors are a prefix of a
/// deadline-ordered sequence, so a merge can cost you a far timeout and never a
/// near one. With capacity 1000, whatever falls off is by construction among
/// the farthest-future entries of the union.
pub proof fn lemma_merge_horizon<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    capacity: nat,
)
    ensures
        ({
            let union = spec_apply_all(cmp, s, inc, inc.len());
            let ordered = spec_sort(union);
            let kept = spec_merge(cmp, s, inc, capacity);
            &&& sorted(ordered)
            &&& kept.len() <= ordered.len()
            &&& kept.len() < ordered.len() ==> kept.len() == capacity
            &&& forall|i: int, j: int|
                #![trigger kept[i].deadline, ordered[j].deadline]
                0 <= i < kept.len() <= j < ordered.len() ==> kept[i].deadline
                    <= ordered[j].deadline
        }),
{
    let union = spec_apply_all(cmp, s, inc, inc.len());
    let ordered = spec_sort(union);
    let kept = spec_merge(cmp, s, inc, capacity);
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
/// condition at all on the incoming batch. The batch may repeat an identity as
/// often as it likes and collide with anything already held; what comes out
/// still carries each logical timeout at most once. Merging is the only way to
/// put a timeout into a wheel, so this makes "the wheel never holds a
/// duplicate" true of every wheel a caller can build.
pub proof fn lemma_merge_preserves_no_duplicates<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    capacity: nat,
)
    requires
        no_duplicates(cmp, s),
    ensures
        no_duplicates(cmp, spec_merge(cmp, s, inc, capacity)),
{
    let union = spec_apply_all(cmp, s, inc, inc.len());
    lemma_no_duplicates_iff_distinct(cmp, s);
    lemma_spec_apply_all_distinct(cmp, s, inc, inc.len());
    lemma_spec_sort_distinct(cmp, union, union.len());
    lemma_take_distinct(cmp, spec_sort(union), capacity);
    lemma_no_duplicates_iff_distinct(cmp, spec_merge(cmp, s, inc, capacity));
}

/// **A merge that carries an arrival for an identity sets that identity's
/// deadline.** Nothing under it survives with any other.
///
/// The condition on the batch is that no *later* arrival names the same
/// identity — the last update is the one that stands. Like
/// [`lemma_spec_apply_sets_identity`], this assumes nothing whatever about the
/// wheel: not sortedness, not capacity, not even freedom from duplicates.
pub proof fn lemma_merge_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    j: int,
    capacity: nat,
)
    requires
        0 <= j < inc.len(),
        forall|l: int| j < l < inc.len() ==> !cmp.same(#[trigger] inc[l].value, inc[j].value),
    ensures
        identity_carries(
            cmp,
            spec_merge(cmp, s, inc, capacity),
            inc[j].value,
            inc[j].deadline,
        ),
{
    let union = spec_apply_all(cmp, s, inc, inc.len());
    lemma_spec_apply_all_sets_identity(cmp, s, inc, j, inc.len());
    lemma_spec_sort_keeps_identity(cmp, union, inc[j].value, inc[j].deadline);
    lemma_take_keeps_identity(cmp, spec_sort(union), capacity, inc[j].value, inc[j].deadline);
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
    inc: Seq<Timeout<T>>,
    i: int,
    j: int,
    capacity: nat,
)
    requires
        0 <= i < s.len(),
        0 <= j < inc.len(),
        // the timeout is already in the wheel ...
        cmp.same(s[i].value, inc[j].value),
        // ... and the batch moves its deadline
        s[i].deadline != inc[j].deadline,
        // ... and no later arrival overrides this one
        forall|l: int| j < l < inc.len() ==> !cmp.same(#[trigger] inc[l].value, inc[j].value),
    ensures
        ({
            let r = spec_merge(cmp, s, inc, capacity);
            // The old entry is gone -- not at its old deadline, nowhere.
            &&& !holds_at(cmp, r, inc[j].value, s[i].deadline)
            // Either it is back with the new deadline, or it is out entirely.
            &&& holds_at(cmp, r, inc[j].value, inc[j].deadline) || fresh(cmp, r, inc[j].value)
        }),
{
    let r = spec_merge(cmp, s, inc, capacity);
    let v = inc[j].value;
    lemma_merge_sets_identity(cmp, s, inc, j, capacity);

    assert(!holds_at(cmp, r, v, s[i].deadline)) by {
        if holds_at(cmp, r, v, s[i].deadline) {
            let k = choose|k: int|
                0 <= k < r.len() && cmp.same(r[k].value, v) && r[k].deadline == s[i].deadline;
            assert(r[k].deadline == inc[j].deadline);
        }
    }

    if !fresh(cmp, r, v) {
        let k = choose|k: int| 0 <= k < r.len() && cmp.same(r[k].value, v);
        assert(r[k].deadline == inc[j].deadline);
        assert(holds_at(cmp, r, v, inc[j].deadline));
    }
}

} // verus!
