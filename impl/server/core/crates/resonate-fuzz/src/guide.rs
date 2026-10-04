//! The feedback loop, ported from `examples/genexp.rs`.
//!
//! A program's signature is (operation, status class, shape of the store).
//! The status is the SERVER's — what actually happened — and the shape is read
//! from the oracle, which after a step that agreed is the state the server
//! holds too. A tape that reached a signature no tape reached before is kept in
//! the corpus and mutated; genexp measured that this nearly doubles the state
//! configurations the informed generator reaches at the same budget.

use std::collections::HashSet;

use resonate_core::types::TaskState;
use resonate_oracle::Oracle;

pub fn mix(mut h: u64, v: u64) -> u64 {
    h ^= v
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(h << 6)
        .wrapping_add(h >> 2);
    h
}

fn bucket(n: usize) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4..=7 => 4,
        8..=15 => 5,
        _ => 6,
    }
}

/// The store's shape: counts, bucketed, of what exists and what is due.
pub fn shape(o: &Oracle) -> u64 {
    let mut h = 0u64;
    h = mix(h, bucket(o.all_promise_ids().len()));
    h = mix(h, bucket(o.pending_promise_ids().len()));
    for st in [
        TaskState::Pending,
        TaskState::Acquired,
        TaskState::Suspended,
        TaskState::Halted,
        TaskState::Fulfilled,
    ] {
        h = mix(h, bucket(o.tasks_by_state(st).len()));
    }
    h = mix(h, bucket(o.schedule_ids().len()));
    h = mix(h, bucket(o.upcoming(64).len()));
    h
}

/// Status CLASS, not status: 404 and 409 on the same operation in the same
/// shape are the same kind of event for coverage purposes.
pub fn signature(op: u64, status: i32, shape: u64) -> u64 {
    mix(mix(mix(0, op), (status / 100) as u64), shape)
}

pub fn random_tape(rng: &mut fastrand::Rng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.u8(..)).collect()
}

/// Havoc-lite: the mutations that matter for a decision tape — change a
/// decision, move a decision, lengthen the program.
pub fn mutate(rng: &mut fastrand::Rng, base: &[u8]) -> Vec<u8> {
    let mut v = base.to_vec();
    if v.is_empty() {
        return random_tape(rng, 256);
    }
    for _ in 0..1 + rng.usize(0..4) {
        match rng.u32(0..5) {
            0 => {
                let i = rng.usize(0..v.len());
                v[i] = rng.u8(..);
            }
            1 => {
                let i = rng.usize(0..v.len());
                v[i] = v[i].wrapping_add(1);
            }
            2 => {
                let i = rng.usize(0..v.len());
                v.insert(i, rng.u8(..));
            }
            3 => {
                if v.len() > 8 {
                    let i = rng.usize(0..v.len());
                    v.remove(i);
                }
            }
            _ => {
                let tail: Vec<u8> = (0..rng.usize(1..64)).map(|_| rng.u8(..)).collect();
                v.extend(tail);
            }
        }
    }
    v.truncate(4096);
    v
}

/// Tapes that reached somewhere new, and everywhere they reached.
pub struct Corpus {
    pub tapes: Vec<Vec<u8>>,
    pub seen: HashSet<u64>,
    guided: bool,
}

impl Corpus {
    pub fn new(guided: bool) -> Self {
        Corpus {
            tapes: Vec::new(),
            seen: HashSet::new(),
            guided,
        }
    }

    /// The next program: a mutated corpus tape, or (unguided, an empty corpus,
    /// or one time in sixteen) fresh random bytes.
    pub fn next(&self, rng: &mut fastrand::Rng) -> Vec<u8> {
        if !self.guided || self.tapes.is_empty() || rng.u32(0..16) == 0 {
            random_tape(rng, 512)
        } else {
            let i = rng.usize(0..self.tapes.len());
            mutate(rng, &self.tapes[i])
        }
    }

    /// Keep the tape if it reached anywhere no tape reached before. Returns how
    /// many signatures were new.
    pub fn observe(
        &mut self,
        rng: &mut fastrand::Rng,
        tape: Vec<u8>,
        reached: &HashSet<u64>,
    ) -> usize {
        let new = reached.iter().filter(|s| !self.seen.contains(s)).count();
        self.seen.extend(reached.iter().copied());
        if new > 0 && self.guided {
            self.tapes.push(tape);
            // Bound the corpus so the walk keeps moving rather than re-running
            // an ever-growing set of near-duplicates.
            if self.tapes.len() > 512 {
                self.tapes.remove(rng.usize(0..self.tapes.len() / 2));
            }
        }
        new
    }
}
