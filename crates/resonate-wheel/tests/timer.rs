//! Runtime tests for the asynchronous front.
//!
//! The wheel's behaviour is proved; none of that is retested here. What these
//! check is the part a proof cannot reach: that the task sleeps for the right
//! length of time, wakes for the right reasons, and asks for more work when it
//! runs low.
//!
//! Time is real rather than paused. `tokio::time::pause` advances virtual time
//! whenever every task is idle, which for a timer whose whole job is to be idle
//! means it races ahead of the injected clock and the two disagree about what
//! is due. Real milliseconds, generously spaced, keep the two in step.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use resonate_wheel::timer::{Clock, OnBackfill, OnFire, TimerConfig};
use resonate_wheel::{IdComparator, Timeout, Timer};

/// A clock reading milliseconds since the test started, so a deadline of
/// `now() + 40` is reached by sleeping 40ms.
fn clock() -> (Clock, Instant) {
    let start = Instant::now();
    let c: Clock = Arc::new(move || start.elapsed().as_millis() as u64);
    (c, start)
}

/// Records every batch it is handed.
#[derive(Clone, Default)]
struct Fired(Arc<Mutex<Vec<u64>>>);

impl Fired {
    fn on_fire(&self) -> OnFire<u64> {
        let seen = self.0.clone();
        Arc::new(move |batch: Vec<Timeout<u64>>| {
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock()
                    .unwrap()
                    .extend(batch.into_iter().map(|t| t.value));
            })
        })
    }

    fn values(&self) -> Vec<u64> {
        self.0.lock().unwrap().clone()
    }
}

/// A backfill that hands back one fixed batch the first time and nothing after,
/// counting how often it was asked.
fn backfill_once(batch: Vec<Timeout<u64>>) -> (OnBackfill<u64>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    let batch = Arc::new(Mutex::new(Some(batch)));
    let cb: OnBackfill<u64> = Arc::new(move |_now, _room| {
        n.fetch_add(1, Ordering::SeqCst);
        let batch = batch.lock().unwrap().take().unwrap_or_default();
        Box::pin(async move { batch })
    });
    (cb, calls)
}

fn no_backfill() -> OnBackfill<u64> {
    Arc::new(|_now, _room| Box::pin(async { Vec::new() }))
}

fn config() -> TimerConfig {
    TimerConfig {
        capacity: 8,
        low_watermark: 0,
        backfill_interval: Duration::from_millis(10),
        idle: Duration::from_millis(20),
    }
}

#[tokio::test]
async fn init_seeds_the_wheel_from_backfill() {
    let (now, _) = clock();
    let fired = Fired::default();
    let (backfill, calls) = backfill_once(vec![Timeout::new(now() + 30, 7u64)]);

    let timer = Timer::new(config(), IdComparator, now, fired.on_fire(), backfill);
    timer.init().await;

    // `init` returned, so the seeding call has already happened.
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(fired.values(), vec![7]);
    timer.stop().await;
}

#[tokio::test]
async fn a_deadline_fires_when_it_passes() {
    let (now, _) = clock();
    let fired = Fired::default();
    let timer = Timer::new(
        config(),
        IdComparator,
        now.clone(),
        fired.on_fire(),
        no_backfill(),
    );
    timer.init().await;

    timer.merge(vec![Timeout::new(now() + 40, 1u64)]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(fired.values().is_empty(), "fired before its deadline");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fired.values(), vec![1]);
    timer.stop().await;
}

#[tokio::test]
async fn a_nearer_deadline_wakes_it_early() {
    let (now, _) = clock();
    let fired = Fired::default();
    // An idle far longer than the test, so only the merge itself can wake the
    // task — if it slept on the far deadline, nothing would fire.
    let cfg = TimerConfig {
        idle: Duration::from_secs(60),
        ..config()
    };
    let timer = Timer::new(
        cfg,
        IdComparator,
        now.clone(),
        fired.on_fire(),
        no_backfill(),
    );
    timer.init().await;

    timer.merge(vec![Timeout::new(now() + 50_000, 1u64)]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    timer.merge(vec![Timeout::new(now() + 20, 2u64)]);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        fired.values(),
        vec![2],
        "the near deadline should have fired"
    );
    timer.stop().await;
}

#[tokio::test]
async fn the_same_identity_moves_rather_than_duplicates() {
    let (now, _) = clock();
    let fired = Fired::default();
    let cfg = TimerConfig {
        idle: Duration::from_secs(60),
        ..config()
    };
    let timer = Timer::new(
        cfg,
        IdComparator,
        now.clone(),
        fired.on_fire(),
        no_backfill(),
    );
    timer.init().await;

    timer.merge(vec![Timeout::new(now() + 50_000, 9u64)]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    timer.merge(vec![Timeout::new(now() + 20, 9u64)]);

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        fired.values(),
        vec![9],
        "one entry, at the moved deadline, not two"
    );
    timer.stop().await;
}

#[tokio::test]
async fn it_refills_when_it_runs_low() {
    let (now, _) = clock();
    let fired = Fired::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    // Always empty, so the wheel stays below the watermark and the interval is
    // the only thing holding the refills back.
    let backfill: OnBackfill<u64> = Arc::new(move |_now, _room| {
        n.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Vec::new() })
    });
    let cfg = TimerConfig {
        low_watermark: 4,
        backfill_interval: Duration::from_millis(30),
        idle: Duration::from_millis(10),
        ..config()
    };

    let timer = Timer::new(cfg, IdComparator, now, fired.on_fire(), backfill);
    timer.init().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    timer.stop().await;

    // ~200ms at one refill per 30ms is around seven, and the 10ms idle means
    // the task woke about twenty times — so this also says the interval, not
    // the wake-up rate, is what governs.
    let n = calls.load(Ordering::SeqCst);
    assert!(
        (2..=12).contains(&n),
        "refilled {n} times, expected a handful"
    );
}

#[tokio::test]
async fn stop_ends_the_task() {
    let (now, _) = clock();
    let fired = Fired::default();
    let timer = Timer::new(
        config(),
        IdComparator,
        now.clone(),
        fired.on_fire(),
        no_backfill(),
    );
    timer.init().await;
    timer.merge(vec![Timeout::new(now() + 30, 1u64)]);
    timer.stop().await;

    // Stopped before the deadline: nothing fires afterwards.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fired.values().is_empty());
}
