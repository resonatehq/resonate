//! `IndexedWheel` against the proved `TimerWheel`, step for step.
//!
//! Verus proves `TimerWheel::merge` equal to `spec_merge`. `IndexedWheel` is
//! not proved; it claims to compute the same function in `O(log n)`. These
//! tests are that claim's evidence. Both wheels get the same operations, and
//! after every one the tests compare what each returned, what each reports as
//! next and how many entries each holds. At the end of a run both are drained
//! and the full sequences, order included, must match.
//!
//! Three sizes of evidence:
//!
//! - `every_short_history_agrees` runs **every** sequence of up to three
//!   operations over a small alphabet (capacities 0 to 3, three ids, two
//!   deadlines so ties are common, batches of up to two that may name one id
//!   twice, and pops at every instant): 424,316 histories. Everything the specification
//!   distinguishes appears within it: drop, last update wins, stable ties,
//!   the cut, an update evicted by its own new deadline.
//! - `long_random_histories_agree` drives both through long pseudorandom
//!   histories on larger wheels.
//! - `arming_does_not_grow_with_the_wheel` is the regression for the cost:
//!   twenty thousand single-entry merges into a full wheel of 8192.

use std::time::{Duration, Instant};

use resonate_timer_wheel::{IdComparator, IndexedWheel, Timeout, TimerWheel};

#[derive(Clone, Debug)]
enum Op {
    Merge(Vec<(u64, u64)>),
    Pop(u64),
}

struct Pair {
    proved: TimerWheel<u64, IdComparator>,
    fast: IndexedWheel<u64, IdComparator>,
}

impl Pair {
    fn new(capacity: usize) -> Self {
        Self {
            proved: TimerWheel::new(capacity, IdComparator),
            fast: IndexedWheel::new(capacity, IdComparator),
        }
    }

    fn apply(&mut self, op: &Op, history: &[Op]) {
        match op {
            Op::Merge(batch) => {
                let mk = || batch.iter().map(|&(d, id)| Timeout::new(d, id)).collect();
                self.proved.merge(mk());
                self.fast.merge(mk());
            }
            Op::Pop(now) => {
                let a = flat(self.proved.pop_expired(*now));
                let b = flat(self.fast.pop_expired(*now));
                assert_eq!(a, b, "pop_expired({now}) differs after {history:?}");
            }
        }
        assert_eq!(
            self.proved.len(),
            self.fast.len(),
            "len differs after {history:?}"
        );
        assert_eq!(
            self.proved.next(),
            self.fast.next(),
            "next differs after {history:?}"
        );
        assert_eq!(
            self.proved.is_full(),
            self.fast.is_full(),
            "is_full differs after {history:?}"
        );
    }

    /// Drain both and compare the whole sequence, ties' order included.
    fn finish(mut self, history: &[Op]) {
        let fast_view: Vec<(u64, u64)> = self.fast.iter().map(|(d, &v)| (d, v)).collect();
        let a = flat(self.proved.pop_expired(u64::MAX));
        let b = flat(self.fast.pop_expired(u64::MAX));
        assert_eq!(a, b, "contents differ after {history:?}");
        assert_eq!(fast_view, b, "iter disagrees with pop_expired");
        assert!(self.fast.is_empty() && self.proved.is_empty());
    }
}

fn flat(ts: Vec<Timeout<u64>>) -> Vec<(u64, u64)> {
    ts.into_iter().map(|t| (t.deadline, t.value)).collect()
}

fn alphabet() -> Vec<Op> {
    const IDS: u64 = 3;
    const DEADLINES: [u64; 2] = [1, 2];
    let arrivals: Vec<(u64, u64)> = DEADLINES
        .iter()
        .flat_map(|&d| (0..IDS).map(move |id| (d, id)))
        .collect();
    let mut ops = vec![Op::Merge(vec![])];
    for &a in &arrivals {
        ops.push(Op::Merge(vec![a]));
        for &b in &arrivals {
            ops.push(Op::Merge(vec![a, b]));
        }
    }
    for now in 0..=3 {
        ops.push(Op::Pop(now));
    }
    ops
}

#[test]
fn every_short_history_agrees() {
    let ops = alphabet();
    let mut histories = 0usize;
    for capacity in 0..=3 {
        for len in 1..=3u32 {
            let total = ops.len().pow(len);
            for mut code in 0..total {
                let mut history = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    history.push(ops[code % ops.len()].clone());
                    code /= ops.len();
                }
                let mut pair = Pair::new(capacity);
                for i in 0..history.len() {
                    pair.apply(&history[i], &history[..=i]);
                }
                pair.finish(&history);
                histories += 1;
            }
        }
    }
    // 43 merges (empty, six single arrivals, 36 pairs) and 4 pops.
    let n = ops.len();
    assert_eq!(n, 47);
    // 4 capacities, every history of 1, 2 and 3 operations.
    assert_eq!(histories, 4 * (n + n * n + n * n * n));
}

/// xorshift64: deterministic, so a failure reproduces.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn long_random_histories_agree() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let mut steps = 0usize;
    for &(capacity, ids, spread, ops) in &[
        (1usize, 4u64, 4u64, 4_000usize),
        (8, 16, 8, 4_000),
        (64, 200, 50, 4_000),
        (256, 1_000, 400, 3_000),
        (512, 600, 2_000, 1_000),
    ] {
        let mut pair = Pair::new(capacity);
        let mut clock = 0u64;
        let mut history = Vec::new();
        for _ in 0..ops {
            let op = if rng.below(8) == 0 {
                clock += rng.below(spread / 4 + 1);
                Op::Pop(clock)
            } else {
                // Mostly one or two arrivals, the request path's shape, and
                // now and then a backfill-sized batch with repeats.
                let m = match rng.below(10) {
                    0 => rng.below(64) as usize,
                    1..=3 => 2,
                    _ => 1,
                };
                Op::Merge(
                    (0..m)
                        .map(|_| (clock + rng.below(spread), rng.below(ids)))
                        .collect(),
                )
            };
            history.push(op.clone());
            // Keep failure messages short: the last few operations.
            let tail = &history[history.len().saturating_sub(4)..];
            pair.apply(&op, tail);
            steps += 1;
        }
        pair.finish(&history[history.len().saturating_sub(4)..]);
    }
    assert_eq!(steps, 16_000);
}

/// The cost regression: arming a deadline must not cost the wheel's size.
///
/// The proved wheel rebuilds itself on every merge, so twenty thousand
/// single-entry merges into a full wheel of 8192 cost about 20,000 × 8192²
/// element moves — hours in a debug build. The indexed wheel does them in
/// milliseconds. The bound is generous on purpose: it separates `O(log n)`
/// from `O(n²)` on any machine, loaded or not, without timing anything
/// finely.
#[test]
fn arming_does_not_grow_with_the_wheel() {
    const N: u64 = 8192;
    let mut w = IndexedWheel::new(N as usize, IdComparator);
    w.merge((0..N).map(|i| Timeout::new(1_000 + i % 977, i)).collect());
    assert!(w.is_full());

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let start = Instant::now();
    for k in 0..20_000u64 {
        // Half move an entry the wheel holds, half bring a new one.
        let id = if k % 2 == 0 { rng.below(N) } else { N + k };
        w.merge(vec![Timeout::new(1_000 + rng.below(977), id)]);
    }
    let elapsed = start.elapsed();
    assert!(w.is_full());
    assert!(
        elapsed < Duration::from_secs(5),
        "20,000 merges into a full wheel of {N} took {elapsed:?}"
    );
}

#[test]
fn ties_keep_arrival_order_across_moves() {
    // A moved entry goes behind everything already due at its new deadline,
    // as `spec_insert` places it.
    let mut w = IndexedWheel::new(8, IdComparator);
    w.merge(vec![
        Timeout::new(5, 1),
        Timeout::new(5, 2),
        Timeout::new(5, 3),
    ]);
    w.merge(vec![Timeout::new(5, 1)]);
    let order: Vec<u64> = w.iter().map(|(_, &v)| v).collect();
    assert_eq!(order, vec![2, 3, 1]);
}

#[test]
fn zero_capacity_holds_nothing() {
    let mut w = IndexedWheel::new(0, IdComparator);
    w.merge(vec![Timeout::new(1, 1)]);
    assert!(w.is_empty());
    assert_eq!(w.next(), None);
    assert!(w.pop_expired(u64::MAX).is_empty());
}
