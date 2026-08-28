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

// `len() == 0` is the deliberate counterpart of `len() == capacity` on the line
// above it, and says what it means more plainly than `is_empty()` would in an
// assertion *about* `is_empty`.
#![allow(clippy::len_zero, clippy::unnecessary_cast)]

use resonate_wheel::{IdComparator, Timeout, TimerWheel};

/// An independent model of `spec_merge`, written the obvious way.
mod model {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Entry {
        pub deadline: u64,
        pub id: u64,
    }

    /// An independent model, written the *other* way round.
    ///
    /// The specification says: drop everything the batch mentions, add the
    /// batch's last updates, sort, cut. This applies the batch one arrival at a
    /// time instead -- each removing whatever matches it and appending itself.
    /// The two are the same function, which is not obvious by inspection, so
    /// driving both over pseudorandom batches is a real check rather than a
    /// restatement.
    pub fn merge(wheel: &[Entry], incoming: &[Entry], capacity: usize) -> Vec<Entry> {
        // 1. Replace, then add.
        let mut u: Vec<Entry> = wheel.to_vec();
        for t in incoming {
            u.retain(|e| e.id != t.id);
            u.push(*t);
        }
        // 2. Sort by deadline, nearest first. Stable, so ties keep their order.
        u.sort_by_key(|e| e.deadline);
        // 3. Cut to capacity.
        u.truncate(capacity);
        u
    }
}

/// Drain a wheel into a plain vector. Consumes it.
fn drain<C: resonate_wheel::Comparator<u64>>(mut w: TimerWheel<u64, C>) -> Vec<model::Entry> {
    w.pop_expired(u64::MAX)
        .into_iter()
        .map(|t| model::Entry {
            deadline: t.deadline,
            id: t.value,
        })
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
        model::Entry {
            deadline: 30,
            id: 1,
        },
        model::Entry {
            deadline: 10,
            id: 2,
        },
        model::Entry {
            deadline: 20,
            id: 3,
        },
    ]));
    assert_eq!(w.len(), 2);
    assert_eq!(
        drain(w),
        vec![
            model::Entry {
                deadline: 10,
                id: 2
            },
            model::Entry {
                deadline: 20,
                id: 3
            },
        ]
    );
}

#[test]
fn same_timeout_moved_replaces_rather_than_duplicates() {
    let mut w = TimerWheel::new(8, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry {
            deadline: 30,
            id: 1,
        },
        model::Entry {
            deadline: 20,
            id: 3,
        },
    ]));
    // id 3's deadline moves 20 -> 5. It must not be stored twice.
    w.merge(to_timeouts(&[model::Entry { deadline: 5, id: 3 }]));
    assert_eq!(w.len(), 2);
    assert_eq!(
        drain(w),
        vec![
            model::Entry { deadline: 5, id: 3 },
            model::Entry {
                deadline: 30,
                id: 1
            },
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
        model::Entry { deadline: 3, id: 9 }, // ties with the last kept entry
        model::Entry {
            deadline: 400,
            id: 10,
        },
        model::Entry {
            deadline: 500,
            id: 11,
        },
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
    w.merge(to_timeouts(&[model::Entry {
        deadline: 900,
        id: 1,
    }]));
    assert_eq!(
        drain(w),
        vec![
            model::Entry { deadline: 2, id: 2 },
            model::Entry {
                deadline: 900,
                id: 1
            },
        ]
    );
}

#[test]
fn within_one_batch_the_last_update_wins() {
    // A batch reads as a sequence of updates: arrivals are applied in the order
    // given, each replacing the one before it, so the last one stands.
    let mut w = TimerWheel::new(4, IdComparator);
    w.merge(to_timeouts(&[
        model::Entry {
            deadline: 50,
            id: 7,
        },
        model::Entry {
            deadline: 10,
            id: 7,
        },
    ]));
    assert_eq!(
        drain(w),
        vec![model::Entry {
            deadline: 10,
            id: 7
        }]
    );
}

#[test]
fn an_update_competes_for_its_slot_on_the_new_deadline() {
    // Capacity 1, wheel holds A at 1, batch moves A out to 3 and adds B at 2.
    // The union is {A@3, B@2}; its nearest is B, so B is what survives.
    //
    // The point: an update is not a lease on a slot. A@3 inherits nothing from
    // A@1 -- it competes on its new deadline like anything else, and loses to a
    // nearer newcomer. That is what "sort the union, then cut" means, and it is
    // why moving a deadline outward can cost you the timeout.
    let mut w = TimerWheel::new(1, IdComparator);
    w.merge(to_timeouts(&[model::Entry { deadline: 1, id: 1 }]));
    w.merge(to_timeouts(&[
        model::Entry { deadline: 2, id: 2 },
        model::Entry { deadline: 3, id: 1 },
    ]));
    assert_eq!(drain(w), vec![model::Entry { deadline: 2, id: 2 }]);
}

#[test]
fn the_wheel_holds_the_capacity_nearest_of_the_union() {
    // The one-sentence reading of the spec, checked directly: whatever the
    // wheel had (with moved deadlines moved) plus whatever the batch added,
    // ordered by deadline, first `capacity`.
    let mut rng = Rng(0xC0FF_EE00_1234_ABCD);

    for _ in 0..5_000 {
        let capacity = (1 + rng.below(6)) as usize;
        let wheel_entries: Vec<model::Entry> = (0..rng.below(6))
            .map(|_| model::Entry {
                deadline: rng.below(30),
                id: rng.below(9),
            })
            .collect();
        let batch: Vec<model::Entry> = (0..rng.below(8))
            .map(|_| model::Entry {
                deadline: rng.below(30),
                id: rng.below(9),
            })
            .collect();

        let mut w = TimerWheel::new(capacity, IdComparator);
        w.merge(to_timeouts(&wheel_entries));
        let seeded = drain(w);

        let mut w = TimerWheel::new(capacity, IdComparator);
        w.merge(to_timeouts(&seeded));
        w.merge(to_timeouts(&batch));

        // Build the union by hand and take its nearest `capacity`.
        let mut union = seeded.clone();
        for t in &batch {
            union.retain(|e| e.id != t.id);
            union.push(*t);
        }
        union.sort_by_key(|e| e.deadline);
        union.truncate(capacity);

        assert_eq!(drain(w), union);
    }
}

#[test]
fn the_result_does_not_depend_on_batch_order() {
    // Cutting once, at the end, is what buys this: capacity is spent on the
    // nearest deadlines of the finished union, whatever order the caller
    // supplied them in. (Batches naming one identity twice are the exception,
    // and deliberately so -- there the order is the update sequence.)
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
        model::Entry {
            deadline: 10,
            id: 2,
        },
        model::Entry {
            deadline: 15,
            id: 3,
        },
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
            .map(|t| model::Entry {
                deadline: t.deadline,
                id: t.value,
            })
            .collect();
        let (want_due, want_left): (Vec<_>, Vec<_>) =
            expected.iter().partition(|e| e.deadline <= now);

        assert_eq!(due, want_due, "case {case}: pop_expired returned");
        assert_eq!(drain(wheel), want_left, "case {case}: pop_expired retained");
    }

    assert!(
        merged > 800_000,
        "expected a meaningful amount of work, got {merged}"
    );
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
fn an_update_replaces_or_evicts_but_never_leaves_a_stale_entry() {
    // The runtime face of `lemma_merge_replaces_or_evicts`. For every wheel and
    // every batch that names one identity exactly once, the old entry must be
    // gone: either back under the new deadline, or absent entirely. What must
    // never happen is the old deadline still sitting there.
    let mut rng = Rng(0xFEED_FACE_5EED_0001);
    let mut replaced = 0u32;
    let mut evicted = 0u32;

    for _ in 0..6_000 {
        let capacity = (1 + rng.below(5)) as usize;
        let mut wheel = TimerWheel::new(capacity, IdComparator);

        // Seed the wheel.
        let seed: Vec<Timeout<u64>> = (0..1 + rng.below(8))
            .map(|_| Timeout::new(rng.below(40), rng.below(6)))
            .collect();
        wheel.merge(seed);

        let before: Vec<model::Entry> = wheel
            .pop_expired(u64::MAX)
            .into_iter()
            .map(|t| model::Entry {
                deadline: t.deadline,
                id: t.value,
            })
            .collect();
        if before.is_empty() {
            continue;
        }
        // Pick a timeout the wheel is holding, and move its deadline.
        let target = before[(rng.below(before.len() as u64)) as usize];
        let moved = model::Entry {
            deadline: rng.below(40),
            id: target.id,
        };
        if moved.deadline == target.deadline {
            continue;
        }

        let mut wheel = TimerWheel::new(capacity, IdComparator);
        wheel.merge(to_timeouts(&before));

        // A batch naming the target's id exactly once, plus unrelated noise.
        let mut batch = vec![moved];
        for _ in 0..rng.below(5) {
            let id = rng.below(6);
            if id != target.id {
                batch.push(model::Entry {
                    deadline: rng.below(40),
                    id,
                });
            }
        }
        wheel.merge(to_timeouts(&batch));

        let after = drain(wheel);
        let found: Vec<&model::Entry> = after.iter().filter(|e| e.id == target.id).collect();

        // Never two entries for one timeout, and never the stale deadline.
        assert!(
            found.len() <= 1,
            "the wheel holds {} entries for one id",
            found.len()
        );
        assert!(
            !after.contains(&target),
            "the old entry {target:?} survived the update to {moved:?}"
        );
        match found.first() {
            Some(e) => {
                assert_eq!(
                    e.deadline, moved.deadline,
                    "surviving entry has a stale deadline"
                );
                replaced += 1;
            }
            None => evicted += 1,
        }
    }

    // Both outcomes must actually occur, or the test is only exercising one arm.
    assert!(replaced > 100, "expected replacements, saw {replaced}");
    assert!(evicted > 100, "expected evictions, saw {evicted}");
    println!("replaced: {replaced}, pushed out: {evicted}");
}

#[test]
fn next_reports_the_nearest_deadline_without_removing_it() {
    let mut w = TimerWheel::new(8, IdComparator);
    assert_eq!(w.next(), None);

    w.merge(to_timeouts(&[
        model::Entry {
            deadline: 30,
            id: 1,
        },
        model::Entry {
            deadline: 10,
            id: 2,
        },
        model::Entry {
            deadline: 20,
            id: 3,
        },
    ]));

    // Reading it does not disturb the wheel.
    assert_eq!(w.next(), Some(10));
    assert_eq!(w.next(), Some(10));
    assert_eq!(w.len(), 3);

    // It tracks a deadline that moves nearer, and one that moves away.
    w.merge(to_timeouts(&[model::Entry { deadline: 5, id: 1 }]));
    assert_eq!(w.next(), Some(5));
    w.merge(to_timeouts(&[model::Entry {
        deadline: 99,
        id: 1,
    }]));
    assert_eq!(w.next(), Some(10));

    assert_eq!(w.pop_expired(10).len(), 1);
    assert_eq!(w.next(), Some(20));
}

#[test]
fn nothing_fires_before_next() {
    // The guarantee that makes `next` safe to sleep on: at any instant strictly
    // before the reported deadline, `pop_expired` returns nothing at all.
    let mut rng = Rng(0x51EE_9000_0BEE_F001);

    for _ in 0..4_000 {
        let capacity = (1 + rng.below(8)) as usize;
        let mut wheel = TimerWheel::new(capacity, IdComparator);
        let batch: Vec<Timeout<u64>> = (0..rng.below(12))
            .map(|_| Timeout::new(1 + rng.below(50), rng.below(8)))
            .collect();
        wheel.merge(batch);

        match wheel.next() {
            None => assert!(wheel.is_empty()),
            Some(d) => {
                // Nothing in the wheel is due sooner.
                let held = drain(wheel);
                assert!(held.iter().all(|e| d <= e.deadline));
                assert_eq!(held[0].deadline, d);

                // And at every instant before it, nothing is due.
                let mut again = TimerWheel::new(capacity, IdComparator);
                again.merge(to_timeouts(&held));
                let now = rng.below(d as u64);
                assert!(now < d);
                assert!(again.pop_expired(now).is_empty());
                assert_eq!(again.next(), Some(d));
            }
        }
    }
}

#[test]
fn a_merge_never_loses_a_slot() {
    // `lemma_merge_wf`'s counting conjunct at runtime: an entry the wheel loses
    // is one the batch mentions, and every mentioned identity has an update
    // waiting for it, so a merge can only ever leave the wheel as full or
    // fuller than it found it.
    let mut rng = Rng(0x5107_5107_1234_9999);
    let mut grew = 0u32;
    let mut held = 0u32;

    for _ in 0..5_000 {
        let capacity = (1 + rng.below(7)) as usize;
        let mut wheel = TimerWheel::new(capacity, IdComparator);
        wheel.merge(
            (0..rng.below(8))
                .map(|_| Timeout::new(rng.below(40), rng.below(9)))
                .collect(),
        );

        for _ in 0..1 + rng.below(5) {
            let before = wheel.len();
            wheel.merge(
                (0..rng.below(10))
                    .map(|_| Timeout::new(rng.below(40), rng.below(9)))
                    .collect(),
            );
            assert!(
                wheel.len() >= before,
                "merge shrank the wheel from {before} to {}",
                wheel.len()
            );
            if wheel.len() > before {
                grew += 1;
            } else {
                held += 1;
            }
        }
    }
    assert!(
        grew > 100 && held > 100,
        "expected both outcomes, saw {grew}/{held}"
    );
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
