//! What arming one deadline costs as the wheel grows.
//!
//! ```sh
//! cargo run --release -p resonate-timer-wheel --example merge_cost
//! ```
//!
//! Fills a wheel to `n` entries, then times single-entry merges into it, the
//! shape of the request path, where every transition arms one or two
//! deadlines. Half the arrivals move an entry the wheel holds and half bring a
//! new id, so both the replace and the add-then-cut paths run. Both wheels get
//! the same arrivals: `TimerWheel`, the proved one, and `IndexedWheel`, the one
//! the timer runs.

use std::time::Instant;

use resonate_timer_wheel::{IdComparator, IndexedWheel, Timeout, TimerWheel};

/// The arrivals for a wheel of `n`, from a fixed seed.
fn arrivals(n: usize, merges: usize) -> Vec<Timeout<u64>> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..merges as u64)
        .map(|k| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let id = if k % 2 == 0 {
                x % n as u64
            } else {
                n as u64 + k
            };
            Timeout::new(1_000_000 + x % 1000, id)
        })
        .collect()
}

fn seed(n: usize) -> Vec<Timeout<u64>> {
    (0..n as u64)
        .map(|i| Timeout::new(1_000_000 + i * 7 % 1000, i))
        .collect()
}

fn main() {
    println!(
        "{:>6} {:>8} {:>18} {:>18}",
        "n", "merges", "TimerWheel us/op", "IndexedWheel us/op"
    );
    for &n in &[256usize, 1024, 2048, 4096, 8192] {
        // Enough merges to measure, few enough that the proved wheel finishes.
        let merges = (2_000_000 / n).clamp(50, 5_000);

        let mut proved = TimerWheel::new(n, IdComparator);
        proved.merge(seed(n));
        let start = Instant::now();
        for t in arrivals(n, merges) {
            proved.merge(vec![t]);
        }
        let slow = start.elapsed().as_secs_f64() * 1e6 / merges as f64;

        let fast_merges = 200_000;
        let mut fast = IndexedWheel::new(n, IdComparator);
        fast.merge(seed(n));
        let start = Instant::now();
        for t in arrivals(n, fast_merges) {
            fast.merge(vec![t]);
        }
        let quick = start.elapsed().as_secs_f64() * 1e6 / fast_merges as f64;

        assert!(proved.len() <= n && fast.len() <= n);
        println!("{n:>6} {merges:>8} {slow:>18.2} {quick:>18.3}");
    }
}
