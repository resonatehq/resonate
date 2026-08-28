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

// --- spec_upsert ---

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

/// One arrival keeps both invariants, adds at most one entry, and loses none.
pub proof fn lemma_spec_upsert_wf<T, C: Comparator<T>>(cmp: C, s: Seq<Timeout<T>>, t: Timeout<T>)
    requires
        sorted(s),
        distinct(cmp, s),
    ensures
        sorted(spec_upsert(cmp, s, t)),
        distinct(cmp, spec_upsert(cmp, s, t)),
        spec_upsert(cmp, s, t).len() <= s.len() + 1,
        s.len() <= spec_upsert(cmp, s, t).len(),
{
    lemma_spec_remove_len_lower(cmp, s, t.value);
    let removed = spec_remove(cmp, s, t.value);
    lemma_spec_remove_sorted(cmp, s, t.value);
    lemma_spec_remove_distinct(cmp, s, t.value);
    lemma_spec_remove_fresh(cmp, s, t.value);
    lemma_spec_remove_len(cmp, s, t.value);
    lemma_spec_insert_sorted(removed, t);
    lemma_spec_insert_distinct(cmp, removed, t);
    lemma_spec_insert_len(removed, t);
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
// Sorting the batch
// ===========================================================================

/// Every entry of `s` satisfies `p`.
///
/// Sorting is a rearrangement, so any property that holds of every arrival in
/// a batch still holds of every arrival in the sorted batch. Stating that once,
/// over an arbitrary predicate, saves proving it separately for the two
/// hypotheses the far-future theorem needs -- freshness and a deadline bound.
pub open spec fn all_sat<T>(s: Seq<Timeout<T>>, p: spec_fn(Timeout<T>) -> bool) -> bool {
    forall|i: int| 0 <= i < s.len() ==> #[trigger] p(s[i])
}

/// Insertion carries a universal property through.
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

/// Sorting carries a universal property through.
pub proof fn lemma_spec_sort_all_sat<T>(inc: Seq<Timeout<T>>, p: spec_fn(Timeout<T>) -> bool, k: nat)
    requires
        all_sat(inc, p),
        k <= inc.len(),
    ensures
        all_sat(spec_sort_prefix(inc, k), p),
    decreases k,
{
    if k == 0 {
        assert(all_sat(Seq::<Timeout<T>>::empty(), p));
    } else {
        lemma_spec_sort_all_sat(inc, p, (k - 1) as nat);
        lemma_spec_insert_all_sat(spec_sort_prefix(inc, (k - 1) as nat), inc[k - 1], p);
    }
}

/// Sorting produces a sorted sequence of the same length.
pub proof fn lemma_spec_sort_wf<T>(inc: Seq<Timeout<T>>, k: nat)
    requires
        k <= inc.len(),
    ensures
        sorted(spec_sort_prefix(inc, k)),
        spec_sort_prefix(inc, k).len() == k,
    decreases k,
{
    if k == 0 {
    } else {
        lemma_spec_sort_wf(inc, (k - 1) as nat);
        lemma_spec_insert_sorted(spec_sort_prefix(inc, (k - 1) as nat), inc[k - 1]);
        lemma_spec_insert_len(spec_sort_prefix(inc, (k - 1) as nat), inc[k - 1]);
    }
}

// ===========================================================================
// Corollaries: the wheel's user-visible guarantees
// ===========================================================================

// ---------------------------------------------------------------------------
// No duplicates in, no duplicates out
// ---------------------------------------------------------------------------

/// One arrival cannot introduce a duplicate.
///
/// Worth noticing what this does *not* assume: neither `sorted` nor the
/// capacity bound appears. Deduplication is independent of the wheel's other
/// two invariants -- it rests only on the comparator's laws -- so it survives
/// even a step taken on a wheel that is unsorted or over capacity.
///
/// The reason is structural. A step is `spec_remove` then `spec_insert` then a
/// cut. The removal strips *every* entry equivalent to the arrival, so the
/// arrival is fresh against what remains no matter what order that sequence was
/// in; the insertion is then the only entry with that identity; and the cut
/// only drops entries.
pub proof fn lemma_step_preserves_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    capacity: nat,
)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_step(cmp, s, t, capacity)),
{
    let removed = spec_remove(cmp, s, t.value);
    lemma_spec_remove_distinct(cmp, s, t.value);
    lemma_spec_remove_fresh(cmp, s, t.value);
    lemma_spec_insert_distinct(cmp, removed, t);
    lemma_take_distinct(cmp, spec_upsert(cmp, s, t), capacity);
}

/// A batch of arrivals cannot introduce a duplicate.
pub proof fn lemma_merge_prefix_preserves_distinct<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    k: nat,
    capacity: nat,
)
    requires
        distinct(cmp, s),
    ensures
        distinct(cmp, spec_merge_prefix(cmp, s, inc, k, capacity)),
    decreases k,
{
    if k == 0 {
    } else {
        lemma_merge_prefix_preserves_distinct(cmp, s, inc, (k - 1) as nat, capacity);
        lemma_step_preserves_distinct(
            cmp,
            spec_merge_prefix(cmp, s, inc, (k - 1) as nat, capacity),
            inc[k - 1],
            capacity,
        );
    }
}

/// **If the wheel held no duplicate before a merge, it holds none after.**
///
/// The hypothesis is the whole hypothesis: no sortedness, no capacity bound, no
/// condition at all on the incoming batch. The batch may repeat an identity as
/// many times as it likes, and may collide with anything already in the wheel;
/// what comes out still holds each logical timeout at most once. Merging is the
/// only way to put a timeout into a wheel, so this is what makes "the wheel
/// never holds a duplicate" true of every wheel a caller can construct, and not
/// merely of the ones this crate happens to build.
///
/// [`TimerWheel::merge`](crate::TimerWheel::merge) states the conclusion in its
/// own postcondition, so a caller reading the signature sees it without
/// unfolding [`TimerWheel::wf`](crate::TimerWheel::wf).
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
    lemma_no_duplicates_iff_distinct(cmp, s);
    lemma_merge_prefix_preserves_distinct(cmp, s, spec_sort(inc), inc.len(), capacity);
    lemma_no_duplicates_iff_distinct(cmp, spec_merge(cmp, s, inc, capacity));
}

// ---------------------------------------------------------------------------
// Replaced, or pushed out
// ---------------------------------------------------------------------------
//
// The claim: once a merge has carried an arrival for some identity, nothing
// under that identity survives with any *other* deadline. Everything below
// builds to that. The three preservation lemmas say the later steps of a merge
// cannot undo it; the three index lemmas say the arrival really does get
// applied, which is the half that needs the sort to be a rearrangement rather
// than an arbitrary sequence.

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

/// Placing an entry keeps an identity's deadline, provided the newcomer either
/// is not that identity or already carries that deadline.
pub proof fn lemma_spec_insert_keeps_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, s, v, d),
        cmp.same(t.value, v) ==> t.deadline == d,
    ensures
        identity_carries(cmp, spec_insert(s, t), v, d),
    decreases s.len(),
{
    if s.len() == 0 {
    } else if t.deadline < s[0].deadline {
        assert forall|k: int|
            0 <= k < (seq![t] + s).len() && cmp.same((seq![t] + s)[k].value, v) implies (seq![t]
            + s)[k].deadline == d by {
            if k > 0 {
                assert((seq![t] + s)[k] == s[k - 1]);
            }
        }
    } else {
        assert(identity_carries(cmp, s.skip(1), v, d)) by {
            assert forall|k: int|
                0 <= k < s.skip(1).len() && cmp.same(s.skip(1)[k].value, v) implies s.skip(
                1,
            )[k].deadline == d by {
                assert(s.skip(1)[k] == s[k + 1]);
            }
        }
        lemma_spec_insert_keeps_identity(cmp, s.skip(1), t, v, d);
        let rest = spec_insert(s.skip(1), t);
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

/// **An arrival sets its identity's deadline.** Whatever the wheel held under
/// that identity before, afterwards there is exactly one entry for it and it is
/// the arrival.
///
/// No hypothesis at all: the removal inside an upsert strips *every* equivalent
/// entry, so this holds even on a wheel that is unsorted, over capacity, or
/// already carrying duplicates.
pub proof fn lemma_upsert_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    u: Seq<Timeout<T>>,
    t: Timeout<T>,
)
    ensures
        identity_carries(cmp, spec_upsert(cmp, u, t), t.value, t.deadline),
{
    let removed = spec_remove(cmp, u, t.value);
    lemma_spec_remove_fresh(cmp, u, t.value);
    assert(identity_carries(cmp, removed, t.value, t.deadline));
    lemma_spec_insert_keeps_identity(cmp, removed, t, t.value, t.deadline);
}

/// An arrival for a *different* identity leaves this one's deadline alone.
pub proof fn lemma_upsert_keeps_identity<T, C: Comparator<T>>(
    cmp: C,
    u: Seq<Timeout<T>>,
    t: Timeout<T>,
    v: T,
    d: u64,
)
    requires
        identity_carries(cmp, u, v, d),
        !cmp.same(t.value, v),
    ensures
        identity_carries(cmp, spec_upsert(cmp, u, t), v, d),
{
    lemma_spec_remove_keeps_identity(cmp, u, t.value, v, d);
    lemma_spec_insert_keeps_identity(cmp, spec_remove(cmp, u, t.value), t, v, d);
}

// --- the arrival really is applied: index witnesses through the sort ---

/// Placing `t` puts it somewhere. Returns where.
pub proof fn lemma_spec_insert_index_of<T>(s: Seq<Timeout<T>>, t: Timeout<T>) -> (m: int)
    ensures
        0 <= m < spec_insert(s, t).len(),
        spec_insert(s, t)[m] == t,
    decreases s.len(),
{
    if s.len() == 0 {
        assert(spec_insert(s, t)[0] == t);
        0
    } else if t.deadline < s[0].deadline {
        assert((seq![t] + s)[0] == t);
        0
    } else {
        let inner = spec_insert(s.skip(1), t);
        let m1 = lemma_spec_insert_index_of(s.skip(1), t);
        assert((seq![s[0]] + inner)[m1 + 1] == inner[m1]);
        m1 + 1
    }
}

/// Placing an entry does not displace the ones already there. Returns where
/// `s[i]` ended up.
pub proof fn lemma_spec_insert_keeps_index<T>(s: Seq<Timeout<T>>, t: Timeout<T>, i: int) -> (m: int)
    requires
        0 <= i < s.len(),
    ensures
        0 <= m < spec_insert(s, t).len(),
        spec_insert(s, t)[m] == s[i],
    decreases s.len(),
{
    if t.deadline < s[0].deadline {
        assert((seq![t] + s)[i + 1] == s[i]);
        i + 1
    } else if i == 0 {
        let inner = spec_insert(s.skip(1), t);
        assert((seq![s[0]] + inner)[0] == s[0]);
        0
    } else {
        let inner = spec_insert(s.skip(1), t);
        assert(s.skip(1)[i - 1] == s[i]);
        let m1 = lemma_spec_insert_keeps_index(s.skip(1), t, i - 1);
        assert((seq![s[0]] + inner)[m1 + 1] == inner[m1]);
        m1 + 1
    }
}

/// **Sorting keeps every arrival.** Returns where `inc[j]` ended up in the
/// sorted batch.
///
/// This is what makes the sort a rearrangement rather than just some sorted
/// sequence, and it is the hinge of the replacement theorem: without it, the
/// merge might never apply the arrival at all, and the old entry could sit
/// there untouched.
pub proof fn lemma_spec_sort_index_of<T>(inc: Seq<Timeout<T>>, k: nat, j: int) -> (m: int)
    requires
        0 <= j < k <= inc.len(),
    ensures
        0 <= m < spec_sort_prefix(inc, k).len(),
        spec_sort_prefix(inc, k)[m] == inc[j],
    decreases k,
{
    let w = spec_sort_prefix(inc, (k - 1) as nat);
    if j == k - 1 {
        lemma_spec_insert_index_of(w, inc[j])
    } else {
        let m0 = lemma_spec_sort_index_of(inc, (k - 1) as nat, j);
        lemma_spec_insert_keeps_index(w, inc[k - 1], m0)
    }
}

// --- the induction ---

/// Once the batch has reached index `m`, every later step preserves what the
/// arrival at `m` set.
pub proof fn lemma_merge_prefix_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    batch: Seq<Timeout<T>>,
    m: int,
    k: nat,
    capacity: nat,
)
    requires
        0 <= m < batch.len(),
        m < k <= batch.len(),
        forall|l: int|
            0 <= l < batch.len() && cmp.same(#[trigger] batch[l].value, batch[m].value)
                ==> batch[l] == batch[m],
    ensures
        identity_carries(
            cmp,
            spec_merge_prefix(cmp, s, batch, k, capacity),
            batch[m].value,
            batch[m].deadline,
        ),
    decreases k,
{
    let u = spec_merge_prefix(cmp, s, batch, (k - 1) as nat, capacity);
    let t = batch[k - 1];

    if cmp.same(t.value, batch[m].value) {
        // Either this *is* the arrival at `m`, or the batch repeats it exactly;
        // uniqueness makes those the same case, and both re-set the deadline.
        assert(t == batch[m]);
        lemma_upsert_sets_identity(cmp, u, t);
    } else {
        // A different identity. Reflexivity makes `batch[m]` equivalent to
        // itself, so this step cannot be the one at `m`: `m` is strictly behind
        // us, and the induction hypothesis applies.
        cmp.lemma_reflexive(batch[m].value);
        assert(k - 1 != m);
        lemma_merge_prefix_sets_identity(cmp, s, batch, m, (k - 1) as nat, capacity);
        lemma_upsert_keeps_identity(cmp, u, t, batch[m].value, batch[m].deadline);
    }
    lemma_take_keeps_identity(
        cmp,
        spec_upsert(cmp, u, t),
        capacity,
        batch[m].value,
        batch[m].deadline,
    );
}

/// **A merge that carries an arrival for an identity sets that identity's
/// deadline.** Nothing under it survives with any other.
///
/// The hypothesis on the batch is that it names this identity at most once —
/// or, if more than once, always with the same deadline. A batch that carries
/// two genuinely different updates for one timeout resolves to the farther of
/// them (the batch is applied nearest-first), which is a different statement;
/// deduplicate such a batch before handing it over.
///
/// Like [`lemma_upsert_sets_identity`], this assumes nothing whatever about the
/// wheel — not sortedness, not capacity, not even that it was free of
/// duplicates.
pub proof fn lemma_merge_sets_identity<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    j: int,
    capacity: nat,
)
    requires
        0 <= j < inc.len(),
        forall|l: int|
            0 <= l < inc.len() && cmp.same(#[trigger] inc[l].value, inc[j].value) ==> inc[l]
                == inc[j],
    ensures
        identity_carries(
            cmp,
            spec_merge(cmp, s, inc, capacity),
            inc[j].value,
            inc[j].deadline,
        ),
{
    let batch = spec_sort(inc);
    lemma_spec_sort_wf(inc, inc.len());

    // Where the arrival ended up after sorting ...
    let m = lemma_spec_sort_index_of(inc, inc.len(), j);
    assert(batch[m] == inc[j]);

    // ... and the fact that sorting is a rearrangement, so "named at most once"
    // still holds of the sorted batch.
    let names_it_once = |x: Timeout<T>| cmp.same(x.value, inc[j].value) ==> x == inc[j];
    assert(all_sat(inc, names_it_once));
    lemma_spec_sort_all_sat(inc, names_it_once, inc.len());
    assert forall|l: int|
        0 <= l < batch.len() && cmp.same(batch[l].value, batch[m].value) implies batch[l]
        == batch[m] by {
        assert(names_it_once(batch[l]));
    }

    lemma_merge_prefix_sets_identity(cmp, s, batch, m, inc.len(), capacity);
}

/// **If a timeout is already in the wheel and the batch carries an update for
/// it, the old entry does not survive: it is replaced with the new deadline, or
/// pushed out altogether.**
///
/// The two conclusions are the whole of it. The first says the entry is not
/// still sitting there at its old deadline — anywhere, at any position. The
/// second says what happened to it instead: either it is back under the new
/// deadline, or the wheel no longer holds that identity at all, which is what
/// happens when the new deadline lands beyond the capacity horizon.
///
/// There is no third outcome, and in particular the wheel never ends up holding
/// the timeout twice — that is
/// [`lemma_merge_preserves_no_duplicates`], and this theorem is what makes it
/// load-bearing rather than trivial: a merge really does move deadlines, and it
/// moves them by replacement.
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
        forall|l: int|
            0 <= l < inc.len() && cmp.same(#[trigger] inc[l].value, inc[j].value) ==> inc[l]
                == inc[j],
    ensures
        ({
            let r = spec_merge(cmp, s, inc, capacity);
            // The old entry is gone -- not at its old deadline, nowhere.
            &&& !holds_at(cmp, r, inc[j].value, s[i].deadline)
            // Either it is back with the new deadline, or it is out entirely.
            &&& holds_at(cmp, r, inc[j].value, inc[j].deadline) || fresh(
                cmp,
                r,
                inc[j].value,
            )
        }),
{
    let r = spec_merge(cmp, s, inc, capacity);
    let v = inc[j].value;
    lemma_merge_sets_identity(cmp, s, inc, j, capacity);

    // Anything left under this identity carries the new deadline, so nothing
    // left under it carries the old one.
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

/// **Every step lands well-formed, and never loses a slot.**
pub proof fn lemma_step_wf<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    capacity: nat,
)
    requires
        sorted(s),
        distinct(cmp, s),
        s.len() <= capacity,
    ensures
        sorted(spec_step(cmp, s, t, capacity)),
        distinct(cmp, spec_step(cmp, s, t, capacity)),
        spec_step(cmp, s, t, capacity).len() <= capacity,
        s.len() <= spec_step(cmp, s, t, capacity).len(),
{
    let full = spec_upsert(cmp, s, t);
    lemma_spec_upsert_wf(cmp, s, t);
    lemma_take_sorted(full, capacity);
    lemma_take_distinct(cmp, full, capacity);
    // `spec_upsert` removes at most one entry and adds exactly one, so the
    // sequence being cut is at least as long as `s` was, and cutting it to
    // `capacity` cannot take it below `s.len()`.
    lemma_take_len(full, capacity);
}

/// **The drop rule, per step.** The entry a step drops is the one due farthest
/// in the future — a merge can cost you a far timeout, never a near one.
pub proof fn lemma_step_drops_the_farthest<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    t: Timeout<T>,
    capacity: nat,
)
    requires
        sorted(s),
        distinct(cmp, s),
    ensures
        ({
            let full = spec_upsert(cmp, s, t);
            let kept = spec_step(cmp, s, t, capacity);
            &&& sorted(full)
            &&& kept.len() <= full.len()
            &&& kept.len() < full.len() ==> kept.len() == capacity
            &&& forall|i: int, j: int|
                #![trigger kept[i].deadline, full[j].deadline]
                0 <= i < kept.len() <= j < full.len() ==> kept[i].deadline <= full[j].deadline
        }),
{
    let full = spec_upsert(cmp, s, t);
    let kept = spec_step(cmp, s, t, capacity);
    lemma_spec_upsert_wf(cmp, s, t);
    lemma_take_len(full, capacity);
    assert forall|i: int, j: int| 0 <= i < kept.len() <= j < full.len() implies kept[i].deadline
        <= full[j].deadline by {
        assert(kept[i] == full[i]);
    }
}

/// A merge lands on a well-formed wheel and never loses a slot.
pub proof fn lemma_merge_wf<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    k: nat,
    capacity: nat,
)
    requires
        sorted(s),
        distinct(cmp, s),
        s.len() <= capacity,
    ensures
        sorted(spec_merge_prefix(cmp, s, inc, k, capacity)),
        distinct(cmp, spec_merge_prefix(cmp, s, inc, k, capacity)),
        spec_merge_prefix(cmp, s, inc, k, capacity).len() <= capacity,
        s.len() <= spec_merge_prefix(cmp, s, inc, k, capacity).len(),
    decreases k,
{
    if k == 0 {
    } else {
        lemma_merge_wf(cmp, s, inc, (k - 1) as nat, capacity);
        lemma_step_wf(cmp, spec_merge_prefix(cmp, s, inc, (k - 1) as nat, capacity), inc[k - 1], capacity);
    }
}

/// Placing an entry nothing is due after appends it.
pub proof fn lemma_spec_insert_appends<T>(s: Seq<Timeout<T>>, t: Timeout<T>)
    requires
        forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].deadline <= t.deadline,
    ensures
        spec_insert(s, t) == s + seq![t],
    decreases s.len(),
{
    if s.len() == 0 {
        assert(spec_insert(s, t) =~= s + seq![t]);
    } else {
        assert(s[0].deadline <= t.deadline);
        assert forall|i: int| 0 <= i < s.skip(1).len() implies s.skip(1)[i].deadline
            <= t.deadline by {
            assert(s.skip(1)[i] == s[i + 1]);
        }
        lemma_spec_insert_appends(s.skip(1), t);
        assert(seq![s[0]] + (s.skip(1) + seq![t]) =~= s + seq![t]);
    }
}

/// **A batch of new, far-future timeouts merged into a full wheel changes
/// nothing.**
///
/// This is the user-facing shape of the drop rule: with capacity 1000, merging
/// a batch whose deadlines all sit at or beyond the 1000th entry's drops the
/// batch entirely.
///
/// The `fresh` hypothesis is load-bearing and easy to forget. The arrivals must
/// be *new*. An arrival that shares an identity with a timeout already in the
/// wheel is not an addition, it is a move: it replaces that entry and inherits
/// its slot, so the wheel does change, however far in the future the arrival
/// is. That is the intended behaviour -- a deadline that moved out is still a
/// deadline that moved -- but it is why "far-future arrivals are ignored" is
/// only true of genuinely new ones.
pub proof fn lemma_merge_ignores_far_future_newcomers<T, C: Comparator<T>>(
    cmp: C,
    s: Seq<Timeout<T>>,
    inc: Seq<Timeout<T>>,
    k: nat,
    capacity: nat,
)
    requires
        sorted(s),
        distinct(cmp, s),
        s.len() == capacity,
        k <= inc.len(),
        forall|j: int| 0 <= j < inc.len() ==> fresh(cmp, s, #[trigger] inc[j].value),
        forall|i: int, j: int|
            #![trigger s[i].deadline, inc[j].deadline]
            0 <= i < s.len() && 0 <= j < inc.len() ==> s[i].deadline <= inc[j].deadline,
    ensures
        spec_merge_prefix(cmp, s, spec_sort(inc), k, capacity) == s,
    decreases k,
{
    // Both hypotheses are properties of every arrival, so both survive sorting.
    let is_fresh = |x: Timeout<T>| fresh(cmp, s, x.value);
    let is_far = |x: Timeout<T>| forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].deadline <= x.deadline;
    assert(all_sat(inc, is_fresh));
    assert(all_sat(inc, is_far));
    lemma_spec_sort_all_sat(inc, is_fresh, inc.len());
    lemma_spec_sort_all_sat(inc, is_far, inc.len());
    lemma_spec_sort_wf(inc, inc.len());

    if k == 0 {
    } else {
        lemma_merge_ignores_far_future_newcomers(cmp, s, inc, (k - 1) as nat, capacity);
        let t = spec_sort(inc)[k - 1];
        assert(is_fresh(t));
        assert(is_far(t));

        // Nothing to replace, so the upsert is a plain append ...
        lemma_fresh_spec_remove_id(cmp, s, t.value);
        lemma_spec_insert_appends(s, t);
        // ... which overflows a full wheel by exactly one, and the cut takes
        // back the entry that is now last.
        assert((s + seq![t]).len() == capacity + 1);
        assert((s + seq![t]).take(capacity as int) =~= s);
    }
}

} // verus!
