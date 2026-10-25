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

use resonate_timer_wheel::timer::{Clock, OnBackfill, OnFire, TimerConfig};
use resonate_timer_wheel::{IdComparator, Timeout, Timer};

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

/// Nearest-rank percentile of `xs`, which must be sorted.
fn pct(xs: &[u64], p: f64) -> u64 {
    let rank = ((p / 100.0) * xs.len() as f64).ceil() as usize;
    xs[rank.clamp(1, xs.len()) - 1]
}

/// The regression for timer lateness under load: deadlines fire on time while
/// the request path keeps arming.
///
/// Sixteen producers each arm two far-future deadlines a millisecond, the
/// shape of a server under task load (a task arms a retry and a lease). That
/// fills a wheel of 8192 within a few hundred milliseconds and keeps the merge
/// queue busy for the rest of the test. Then 500 near deadlines are armed and
/// their lateness measured.
///
/// On the proved `TimerWheel`, each merge rebuilt the full wheel (about 20 ms
/// at 8192 entries), so the driver took about 50 batches a second against
/// thousands arriving. The queue never emptied and no deadline fired within
/// the test. The bound is generous: it fails on a timer that cannot keep up,
/// not on a slow machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlines_fire_on_time_while_the_wheel_is_full_and_busy() {
    const PRODUCERS: u64 = 16;
    const TIMERS: u64 = 500;

    let (now, _) = clock();
    let fired: Arc<Mutex<Vec<(u64, u64)>>> = Arc::default();
    let on_fire: OnFire<u64> = {
        let (fired, now) = (fired.clone(), now.clone());
        Arc::new(move |batch: Vec<Timeout<u64>>| {
            let at = now();
            fired
                .lock()
                .unwrap()
                .extend(batch.into_iter().map(|t| (t.value, at)));
            Box::pin(async {})
        })
    };
    let config = TimerConfig {
        capacity: 8192,
        low_watermark: 0,
        backfill_interval: Duration::from_secs(30),
        idle: Duration::from_secs(30),
    };
    let timer = Arc::new(Timer::new(
        config,
        IdComparator,
        now.clone(),
        on_fire,
        no_backfill(),
    ));
    timer.init().await;

    // The load: ids from 1_000_000 up, so they never collide with a timer.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut producers = Vec::new();
    for p in 0..PRODUCERS {
        let (timer, now, stop) = (timer.clone(), now.clone(), stop.clone());
        producers.push(tokio::spawn(async move {
            let mut k = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let id = 1_000_000 + (p << 32) + k;
                k += 2;
                let far = now() + 30_000 + (k % 997);
                timer.merge(vec![Timeout::new(far, id), Timeout::new(far + 1, id + 1)]);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            k
        }));
    }
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // The timers: 500 deadlines between 300 and 800 ms ahead.
    let mut due = std::collections::HashMap::new();
    for i in 0..TIMERS {
        let at = now() + 300 + (i % 100) * 5;
        due.insert(i, at);
        timer.merge(vec![Timeout::new(at, i)]);
    }
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    stop.store(true, Ordering::Relaxed);
    let mut armed = 0;
    for p in producers {
        armed += p.await.unwrap();
    }
    timer.stop().await;

    let mut late: Vec<u64> = fired
        .lock()
        .unwrap()
        .iter()
        .filter_map(|&(v, at)| due.get(&v).map(|&d| at.saturating_sub(d)))
        .collect();
    late.sort_unstable();
    eprintln!(
        "{armed} deadlines armed by the load; timers fired {}/{TIMERS}",
        late.len()
    );
    if let (Some(_), Some(&max)) = (late.first(), late.last()) {
        eprintln!(
            "lateness ms: p50 {}, p95 {}, p99 {}, max {max}",
            pct(&late, 50.0),
            pct(&late, 95.0),
            pct(&late, 99.0),
        );
    }
    assert_eq!(late.len() as u64, TIMERS, "timers did not all fire");
    let p99 = pct(&late, 99.0);
    assert!(p99 < 500, "p99 lateness {p99} ms");
}

/// A server under load arms deadlines on nearly every request, so merges
/// arrive without a gap. Firing must still happen: a due deadline waits for
/// at most one bounded drain of the queue, not for the stream to stop.
///
/// The producer never sleeps, so the queue is never empty when the driver
/// looks. A driver that always takes the next merge before it fires (the
/// biased `select!` this replaced) never gets round to firing, however cheap
/// each merge is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_of_merges_does_not_starve_firing() {
    let (now, _) = clock();
    let fired = Fired::default();
    let cfg = TimerConfig {
        capacity: 4_096,
        low_watermark: 0,
        backfill_interval: Duration::from_secs(60),
        idle: Duration::from_secs(60),
    };
    let far: Vec<Timeout<u64>> = (0..4_000)
        .map(|i| Timeout::new(now() + 600_000 + i, 1_000_000 + i))
        .collect();
    let (backfill, _) = backfill_once(far);
    let timer = Arc::new(Timer::new(
        cfg,
        IdComparator,
        now.clone(),
        fired.on_fire(),
        backfill,
    ));
    timer.init().await;

    let flooding = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flood = {
        let (timer, now, flooding) = (timer.clone(), now.clone(), flooding.clone());
        tokio::spawn(async move {
            let mut i = 0u64;
            while flooding.load(Ordering::SeqCst) {
                // Far deadlines that replace one another, so the wheel stays
                // the same size.
                timer.merge(vec![Timeout::new(now() + 600_000, 2_000_000 + i % 64)]);
                i += 1;
                if i.is_multiple_of(16) {
                    tokio::task::yield_now().await;
                }
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    let armed = Instant::now();
    timer.merge(vec![Timeout::new(now() + 20, 1u64)]);
    let mut starved = false;
    while !fired.values().contains(&1) {
        if armed.elapsed() > Duration::from_secs(2) {
            starved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    flooding.store(false, Ordering::SeqCst);
    flood.await.unwrap();
    timer.stop().await;
    assert!(
        !starved,
        "a due deadline did not fire within 2 s while merges kept arriving"
    );
}
