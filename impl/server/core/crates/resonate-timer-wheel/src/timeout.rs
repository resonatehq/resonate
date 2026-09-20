//! The element type stored by a [`TimerWheel`](crate::TimerWheel).

use vstd::prelude::*;

verus! {

/// A pending timeout: a `deadline` plus an arbitrary payload.
///
/// The wheel orders timeouts by `deadline` (nearest first) and identifies them
/// by `value`, through a [`Comparator`](crate::Comparator). Two timeouts whose
/// values compare as *the same* are the same logical timeout, even when their
/// deadlines differ — merging one over the other moves the deadline rather than
/// storing both.
///
/// `deadline` is an opaque `u64`. The wheel never does arithmetic on it, only
/// comparisons, so any monotone encoding works: milliseconds since the epoch,
/// nanoseconds since boot, a logical tick counter.
pub struct Timeout<T> {
    /// When the timeout comes due. Smaller means sooner.
    pub deadline: u64,
    /// The payload. Its identity is decided by the wheel's comparator.
    pub value: T,
}

impl<T> Timeout<T> {
    /// Builds a timeout due at `deadline` carrying `value`.
    pub fn new(deadline: u64, value: T) -> (r: Timeout<T>)
        ensures
            r.deadline == deadline,
            r.value == value,
    {
        Timeout { deadline, value }
    }
}

} // verus!
