//! Timeout identity: how the wheel decides that two timeouts are "the same one".

use vstd::prelude::*;

verus! {

/// Decides whether two payloads denote the *same logical timeout*.
///
/// A [`TimerWheel`](crate::TimerWheel) is instantiated with a comparator, and
/// that comparator is what gives merging its replace-don't-duplicate behaviour:
/// when an incoming timeout compares as the same as one already in the wheel,
/// the incoming one wins and the old one is dropped, so the deadline moves
/// instead of the entry being duplicated.
///
/// # The obligation on implementors
///
/// `same` must be an **equivalence relation** — reflexive, symmetric and
/// transitive. This is not a documentation-only convention: the three
/// `lemma_*` members below are proof obligations, and an implementation that
/// cannot discharge them does not compile under Verus. They are what makes
/// "the wheel holds no two equivalent timeouts" a stable invariant; without
/// transitivity, replacing an entry could leave behind a third entry
/// equivalent to the newcomer.
///
/// The executable `eq` must agree with the specification `same` exactly, which
/// is what lets the proofs about `same` say anything about what the code does.
pub trait Comparator<T>: Sized {
    /// Specification-level identity. Ghost only; it has no runtime cost.
    spec fn same(&self, a: T, b: T) -> bool;

    /// Obligation: `same` is reflexive.
    proof fn lemma_reflexive(&self, a: T)
        ensures
            self.same(a, a),
    ;

    /// Obligation: `same` is symmetric.
    proof fn lemma_symmetric(&self, a: T, b: T)
        ensures
            self.same(a, b) == self.same(b, a),
    ;

    /// Obligation: `same` is transitive.
    proof fn lemma_transitive(&self, a: T, b: T, c: T)
        requires
            self.same(a, b),
            self.same(b, c),
        ensures
            self.same(a, c),
    ;

    /// Executable identity test. Must decide exactly [`Comparator::same`].
    fn eq(&self, a: &T, b: &T) -> (r: bool)
        ensures
            r == self.same(*a, *b),
    ;
}

// ---------------------------------------------------------------------------
// Quantified restatements of the laws.
//
// The trait states each law for one tuple of arguments, which is the right
// shape for an implementor to discharge but an awkward one to use inside a
// `forall`. These lemmas lift each law once, so the proofs downstream can
// simply assume the quantified form.
// ---------------------------------------------------------------------------

/// `same` is reflexive, for all arguments.
pub proof fn lemma_reflexive_all<T, C: Comparator<T>>(cmp: C)
    ensures
        forall|a: T| cmp.same(a, a),
{
    assert forall|a: T| cmp.same(a, a) by {
        cmp.lemma_reflexive(a);
    }
}

/// `same` is symmetric, for all arguments.
pub proof fn lemma_symmetric_all<T, C: Comparator<T>>(cmp: C)
    ensures
        forall|a: T, b: T| cmp.same(a, b) == cmp.same(b, a),
{
    assert forall|a: T, b: T| cmp.same(a, b) == cmp.same(b, a) by {
        cmp.lemma_symmetric(a, b);
    }
}

/// `same` is transitive, for all arguments.
pub proof fn lemma_transitive_all<T, C: Comparator<T>>(cmp: C)
    ensures
        forall|a: T, b: T, c: T| cmp.same(a, b) && cmp.same(b, c) ==> cmp.same(a, c),
{
    assert forall|a: T, b: T, c: T| cmp.same(a, b) && cmp.same(b, c) implies cmp.same(a, c) by {
        cmp.lemma_transitive(a, b, c);
    }
}

/// All three laws at once. Most proofs in this crate open with this.
pub proof fn lemma_equivalence<T, C: Comparator<T>>(cmp: C)
    ensures
        forall|a: T| cmp.same(a, a),
        forall|a: T, b: T| cmp.same(a, b) == cmp.same(b, a),
        forall|a: T, b: T, c: T| cmp.same(a, b) && cmp.same(b, c) ==> cmp.same(a, c),
{
    lemma_reflexive_all::<T, C>(cmp);
    lemma_symmetric_all::<T, C>(cmp);
    lemma_transitive_all::<T, C>(cmp);
}

// ---------------------------------------------------------------------------
// A ready-made comparator: identity by `u64` key.
// ---------------------------------------------------------------------------

/// A payload that carries a `u64` identity.
///
/// This is the common case — a timeout belongs to a promise, a task, a
/// connection — so the crate ships it rather than making every caller write a
/// comparator by hand.
pub trait HasId: Sized {
    /// Specification-level identity of this payload.
    spec fn spec_id(&self) -> u64;

    /// Executable identity. Must agree with [`HasId::spec_id`].
    fn id(&self) -> (r: u64)
        ensures
            r == self.spec_id(),
    ;
}

/// The comparator that calls two payloads the same when their [`HasId`] ids match.
///
/// Its equivalence laws follow from those of `u64` equality, so implementors
/// get them for free.
pub struct IdComparator;

impl<T: HasId> Comparator<T> for IdComparator {
    open spec fn same(&self, a: T, b: T) -> bool {
        a.spec_id() == b.spec_id()
    }

    proof fn lemma_reflexive(&self, a: T) {
    }

    proof fn lemma_symmetric(&self, a: T, b: T) {
    }

    proof fn lemma_transitive(&self, a: T, b: T, c: T) {
    }

    fn eq(&self, a: &T, b: &T) -> (r: bool) {
        a.id() == b.id()
    }
}

impl HasId for u64 {
    open spec fn spec_id(&self) -> u64 {
        *self
    }

    fn id(&self) -> (r: u64) {
        *self
    }
}

} // verus!
