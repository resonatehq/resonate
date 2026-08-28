//! Runtime tests for the wheel.
//!
//! Verus proves these properties for *all* inputs; these tests are not a
//! substitute for that. What they check is the other half of the claim —
//! that the code Verus verified is the code that runs. The crate compiles a
//! second time here under ghost erasure, with every `requires`/`ensures`
//! stripped, so a mismatch between the verified branches and the shipped ones
//! would show up as a failing assertion rather than as nothing at all.
//!
//! `model` re-derives `spec_merge` in ordinary Rust, deliberately by a
//! different route: it concatenates and stably sorts where the implementation
//! scans and splices. `merge_matches_the_model` then drives both over
//! pseudorandom batches. Agreement across ~880k merged timeouts is evidence
//! that the specification says what it was meant to say — the one thing a
//! proof assistant cannot check.

use resonate_wheel::{IdComparator, Timeout, TimerWheel};

/// An independent model of `spec_merge`, written the obvious way.
mod model {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Entry {
        pub deadline: u64,
        pub id: u64,
    }

    /// Sort the batch nearest-first, then take one arrival at a time,
    /// honouring capacity at every step.
    pub fn merge(wheel: &[Entry], incoming: &[Entry], capacity: usize) -> Vec<Entry> {
        let mut batch: Vec<Entry> = incoming.to_vec();
        // A *stable* sort keeps the caller's order among equal deadlines,
        // which is what `spec_insert` placing after equals means.
        batch.sort_by_key(|e| e.deadline);

        let mut all: Vec<Entry> = wheel.to_vec();
        for t in batch {
            all.retain(|e| e.id != t.id);
            let pos = all
                .iter()
                .position(|e| e.deadline > t.deadline)
                .unwrap_or(all.len());
            all.insert(pos, t);
            all.truncate(capacity);
        }
        all
    }
}

/// Drain a wheel into a plain vector. Consumes it.
fn drain<C: resonate_wheel::Comparator<u64>>(mut w: TimerWheel<u64, C>) -> Vec<model::Entry> {
    w.pop_expired(u64::MAX)
        .into_iter()
        .map(|t| model::Entry { deadline: t.deadline, id: t.value })
        .collect()
}

fn to_timeouts(es: &[model::Entry]) -> Vec<Timeout<u64>> {
    es.iter().map(|e| Timeout::new(e.deadline, e.id)).collect()
}

// ---------------------------------------------------------------------------
// Worked examples
// ---------------------------------------------------------------------------

#[test]
fn capacity_keeps_the_nearest_deadlines() {
    let mut w = TimerWheel::new(2, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry { deadline: 30, id: 1 },
        model::Entry { deadline: 10, id: 2 },
        model::Entry { deadline: 20, id: 3 },
    ]));
    assert_eq!(w.len(), 2);
    assert_eq!(
        drain(w),
        vec![
            model::Entry { deadline: 10, id: 2 },
            model::Entry { deadline: 20, id: 3 },
        ]
    );
}

#[test]
fn same_timeout_moved_replaces_rather_than_duplicates() {
    let mut w = TimerWheel::new(8, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry { deadline: 30, id: 1 },
        model::Entry { deadline: 20, id: 3 },
    ]));
    // id 3's deadline moves 20 -> 5. It must not be stored twice.
    w.merge(to_timeouts(&[model::Entry { deadline: 5, id: 3 }]));
    assert_eq!(w.len(), 2);
    assert_eq!(
        drain(w),
        vec![
            model::Entry { deadline: 5, id: 3 },
            model::Entry { deadline: 30, id: 1 },
        ]
    );
}

#[test]
fn a_far_future_batch_of_newcomers_is_dropped_whole() {
    // The property `lemma_merge_ignores_far_future_newcomers` states.
    let mut w = TimerWheel::new(3, IdComparator);
    let base = [
        model::Entry { deadline: 1, id: 1 },
        model::Entry { deadline: 2, id: 2 },
        model::Entry { deadline: 3, id: 3 },
    ];
    w.merge(to_timeouts(&base));
    assert!(w.is_full());

    w.merge(to_timeouts(&[
        model::Entry { deadline: 3, id: 9 },   // ties with the last kept entry
        model::Entry { deadline: 400, id: 10 },
        model::Entry { deadline: 500, id: 11 },
    ]));
    assert_eq!(drain(w), base.to_vec());
}

#[test]
fn a_far_future_arrival_that_is_not_new_still_moves_its_deadline() {
    // The hypothesis the lemma needs and the reason it is stated: an arrival
    // sharing an identity is a move, not an addition, so it is *not* ignored.
    let mut w = TimerWheel::new(2, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry { deadline: 1, id: 1 },
        model::Entry { deadline: 2, id: 2 },
    ]));
    w.merge(to_timeouts(&[model::Entry { deadline: 900, id: 1 }]));
    assert_eq!(
        drain(w),
        vec![
            model::Entry { deadline: 2, id: 2 },
            model::Entry { deadline: 900, id: 1 },
        ]
    );
}

#[test]
fn within_one_batch_the_farthest_deadline_wins() {
    // Pinning a consequence of sorting the batch, because it is the one that
    // can surprise: the arrivals are applied NEAREST FIRST, so of two entries
    // sharing an id the later deadline is the one left standing -- not the one
    // the caller listed last. A batch meant to carry a sequence of updates to
    // one timeout should be deduplicated before it is handed over.
    let mut w = TimerWheel::new(4, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry { deadline: 50, id: 7 },
        model::Entry { deadline: 10, id: 7 },
    ]));
    assert_eq!(drain(w), vec![model::Entry { deadline: 50, id: 7 }]);
}

#[test]
fn a_replacement_frees_a_slot_mid_batch() {
    // THE CASE THAT MAKES CAPACITY PER-ARRIVAL DIFFERENT from cutting once at
    // the end, and the reason the truncation lives inside the merge loop.
    //
    // Capacity 1, wheel holds A at 1. The batch is [B@2, A@3], already sorted.
    // B is considered first, against a wheel whose only entry is nearer than
    // it, and loses the slot fairly. A@3 is then a replacement, not an
    // addition: it frees A's slot and takes it back.
    //
    // Cutting once at the end would instead keep B@2, the nearest of the
    // union. Both are defensible; this is the one where an entry the wheel is
    // already tracking is not evicted by a newcomer that arrived while it was
    // full.
    let mut w = TimerWheel::new(1, IdComparator);
    w.merge(to_timeouts(&[model::Entry { deadline: 1, id: 1 }]));
    w.merge(to_timeouts(&[
        model::Entry { deadline: 2, id: 2 },
        model::Entry { deadline: 3, id: 1 },
    ]));
    assert_eq!(drain(w), vec![model::Entry { deadline: 3, id: 1 }]);
}

#[test]
fn the_result_does_not_depend_on_batch_order() {
    // What sorting the batch buys: capacity is spent on the nearest deadlines
    // whatever order the caller supplies. Without the sort, the first of these
    // would keep 9 and the second would not.
    let batch = [
        model::Entry { deadline: 9, id: 1 },
        model::Entry { deadline: 1, id: 2 },
        model::Entry { deadline: 2, id: 3 },
    ];
    let mut forward = TimerWheel::new(2, IdComparator);
    forward.merge(to_timeouts(&batch));

    let mut reversed: Vec<model::Entry> = batch.to_vec();
    reversed.reverse();
    let mut backward = TimerWheel::new(2, IdComparator);
    backward.merge(to_timeouts(&reversed));

    assert_eq!(drain(forward), drain(backward));
}

#[test]
fn pop_expired_splits_exactly() {
    let mut w = TimerWheel::new(16, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry { deadline: 5, id: 1 },
        model::Entry { deadline: 10, id: 2 },
        model::Entry { deadline: 15, id: 3 },
    ]));
    let due = w.pop_expired(10);
    assert_eq!(due.len(), 2);
    assert!(due.iter().all(|t| t.deadline <= 10));
    assert_eq!(w.len(), 1);
    assert_eq!(w.peek().unwrap().deadline, 15);
}

#[test]
fn zero_capacity_holds_nothing() {
    let mut w = TimerWheel::new(0, IdComparator);
    w.merge(to_timeouts(&[model::Entry { deadline: 1, id: 1 }]));
    assert!(w.is_empty());
    assert!(w.is_full());
    assert!(w.peek().is_none());
}

#[test]
fn insert_is_a_one_element_merge() {
    let mut a = TimerWheel::new(2, IdComparator);
    let mut b = TimerWheel::new(2, IdComparator);
    for e in [
        model::Entry { deadline: 9, id: 1 },
        model::Entry { deadline: 4, id: 2 },
        model::Entry { deadline: 7, id: 1 },
    ] {
        a.insert(Timeout::new(e.deadline, e.id));
        b.merge(to_timeouts(&[e]));
    }
    assert_eq!(drain(a), drain(b));
}

// ---------------------------------------------------------------------------
// Differential test against the model
// ---------------------------------------------------------------------------

/// xorshift64*, so the run is deterministic and needs no dev-dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn merge_matches_the_model() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut merged: u64 = 0;

    for case in 0..30_000u64 {
        // Small capacities and a small id space, so replacement and overflow
        // both happen constantly rather than by luck.
        let capacity = (rng.below(9)) as usize;
        let id_space = 1 + rng.below(12);
        let deadline_space = 1 + rng.below(20);

        let mut wheel = TimerWheel::new(capacity, IdComparator);
        let mut expected: Vec<model::Entry> = Vec::new();

        for _ in 0..1 + rng.below(12) {
            let batch: Vec<model::Entry> = (0..rng.below(10))
                .map(|_| model::Entry {
                    deadline: rng.below(deadline_space),
                    id: rng.below(id_space),
                })
                .collect();
            merged += batch.len() as u64;

            expected = model::merge(&expected, &batch, capacity);
            wheel.merge(to_timeouts(&batch));

            assert!(wheel.len() <= capacity, "case {case}: over capacity");
            assert_eq!(wheel.len(), expected.len(), "case {case}: length");
        }

        // Also exercise the split, at a cut point inside the range.
        let now = rng.below(deadline_space);
        let due: Vec<model::Entry> = wheel
            .pop_expired(now)
            .into_iter()
            .map(|t| model::Entry { deadline: t.deadline, id: t.value })
            .collect();
        let (want_due, want_left): (Vec<_>, Vec<_>) =
            expected.iter().partition(|e| e.deadline <= now);

        assert_eq!(due, want_due, "case {case}: pop_expired returned");
        assert_eq!(drain(wheel), want_left, "case {case}: pop_expired retained");
    }

    assert!(merged > 800_000, "expected a meaningful amount of work, got {merged}");
    println!("differential: {merged} timeouts merged across 30,000 wheels");
}

#[test]
fn a_merge_never_introduces_a_duplicate() {
    // The runtime face of `lemma_merge_preserves_no_duplicates`. The batches
    // here are built to collide as hard as possible: four ids across up to
    // sixteen arrivals, so nearly every batch repeats an identity several
    // times over AND collides with what the wheel already holds. The theorem
    // puts no condition at all on the batch, so neither does this.
    let mut rng = Rng(0x0BAD_F00D_1234_5678);

    for _ in 0..4_000 {
        let capacity = (1 + rng.below(6)) as usize;
        let mut wheel = TimerWheel::new(capacity, IdComparator);

        for _ in 0..1 + rng.below(8) {
            let batch: Vec<Timeout<u64>> = (0..rng.below(16))
                .map(|_| Timeout::new(rng.below(8), rng.below(4)))
                .collect();
            wheel.merge(batch);

            // Read the wheel out and put it straight back, so the check runs
            // against every intermediate state rather than only the last.
            let held: Vec<Timeout<u64>> = wheel.pop_expired(u64::MAX);
            for i in 0..held.len() {
                for j in 0..held.len() {
                    if i != j {
                        assert_ne!(
                            held[i].value, held[j].value,
                            "positions {i} and {j} hold the same logical timeout"
                        );
                    }
                }
            }
            wheel.merge(held);
        }
    }
}

#[test]
fn invariants_hold_under_random_merges() {
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);

    for _ in 0..2_000 {
        let capacity = (1 + rng.below(10)) as usize;
        let mut wheel = TimerWheel::new(capacity, IdComparator);

        for _ in 0..1 + rng.below(10) {
            let batch: Vec<Timeout<u64>> = (0..rng.below(14))
                .map(|_| Timeout::new(rng.below(30), rng.below(10)))
                .collect();
            wheel.merge(batch);

            assert!(wheel.len() <= capacity);
            assert_eq!(wheel.is_full(), wheel.len() == capacity);
            assert_eq!(wheel.is_empty(), wheel.len() == 0);
        }

        let contents = drain(wheel);
        // sorted
        assert!(contents.windows(2).all(|p| p[0].deadline <= p[1].deadline));
        // distinct
        for i in 0..contents.len() {
            for j in i + 1..contents.len() {
                assert_ne!(contents[i].id, contents[j].id);
            }
        }
    }
}
