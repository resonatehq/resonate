//! The asynchronous front for [`TimerWheel`].
//!
//! [`TimerWheel`] is pure: a sorted, deduplicated, bounded set of deadlines and
//! four total functions over it. It knows nothing about clocks, tasks or
//! storage, which is what let it be proved. Everything a running system needs
//! around that — a clock, a task that sleeps, somewhere to report a deadline
//! that came due, somewhere to get more deadlines from — lives here.
//!
//! The split is deliberate: this module is not verified, and Verus never sees
//! it. `lib.rs` gates it on `verus_keep_ghost`, so `verify.sh` compiles the
//! proved core alone and the async machinery cannot weaken it.
//!
//! # How it runs
//!
//! One task owns the wheel. It sleeps until the nearest deadline, and wakes
//! early for anything that changes what "nearest" means:
//!
//! ```text
//!   sleep until wheel.next()   ── deadline reached ──▶ pop_expired, fire
//!         ▲                    ── batch arrived ─────▶ merge
//!         │                    ── shutdown ──────────▶ stop
//!         └── recompute, because any of those moved the front
//! ```
//!
//! The wheel is never locked. Callers hand batches over a channel, so a merge
//! from a request path is a send that cannot block on the timer, and the send
//! is itself the wake-up — there is no separate notification to keep in step
//! with the queue.
//!
//! # Why it can be fed everything
//!
//! A caller merges every deadline it writes, without deciding what is worth
//! keeping. That is safe because of the wheel's own guarantee: a merge into a
//! full wheel drops the farthest-future entries, never the nearest, and
//! `lemma_merge_ignores_far_future_newcomers` says a batch beyond the horizon
//! changes nothing at all. So the horizon manages itself, and the wheel holds
//! the near future exactly.
//!
//! # Why it may lose things
//!
//! Nothing here is a system of record. Every deadline the wheel holds is also
//! a committed row somewhere, and this is a cache of the near ones. A lost
//! entry, a process restart, a deadline another instance wrote — all of them
//! come back through `backfill`, and in the worst case through whatever
//! periodic sweep the durable side still runs. Which is why `idle` exists: the
//! task wakes on that interval even with a far-off next deadline, so a wheel
//! that has gone stale re-reads the world rather than sleeping through it.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::comparator::Comparator;
use crate::timeout::Timeout;
use crate::wheel::TimerWheel;

/// A future returned by one of the timer's callbacks.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Called with every batch of deadlines that has come due.
///
/// A batch rather than one at a time: they are due at the same instant, and the
/// caller can usually act on them together more cheaply than one by one.
///
/// Firing is a hint that a deadline *may* have passed. The callback is expected
/// to be idempotent and to re-check against durable state — a deadline that
/// moved after the wheel took a copy fires here into a no-op.
pub type OnFire<T> = Arc<dyn Fn(Vec<Timeout<T>>) -> BoxFuture<()> + Send + Sync>;

/// Called when the wheel runs low, with `(now, how many more it can hold)`.
///
/// This is where the near future comes from after a restart, and where
/// deadlines written by another instance are picked up. Returning fewer than
/// asked for — or none — is fine and means the world is smaller than the wheel.
pub type OnBackfill<T> = Arc<dyn Fn(u64, usize) -> BoxFuture<Vec<Timeout<T>>> + Send + Sync>;

/// The clock, injected so a test can drive it.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// How the timer behaves. See [`TimerConfig::new`] for the defaults.
#[derive(Debug, Clone)]
pub struct TimerConfig {
    /// The most deadlines the wheel will hold. The horizon is whatever this
    /// many nearest deadlines reach.
    pub capacity: usize,
    /// Refill when fewer than this many entries remain. Zero means refill only
    /// when the wheel is empty.
    pub low_watermark: usize,
    /// The floor on how often `backfill` may be called. Without it a busy
    /// system just below the watermark would refill on every merge.
    pub backfill_interval: Duration,
    /// The longest the task will sleep, whatever the next deadline says.
    ///
    /// This is the backstop, and the reason the timer can be wrong without
    /// being unsafe: a deadline another instance armed is invisible here until
    /// the next backfill, and this bounds how long that can last.
    pub idle: Duration,
}

impl TimerConfig {
    /// Capacity `capacity`, refilling below half of it, at most once a second,
    /// and never sleeping longer than 30 seconds.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            low_watermark: capacity / 2,
            backfill_interval: Duration::from_secs(1),
            idle: Duration::from_secs(30),
        }
    }
}

/// What the driver task is given when it starts.
struct Driver<T, C> {
    config: TimerConfig,
    cmp: C,
    now: Clock,
    on_fire: OnFire<T>,
    on_backfill: OnBackfill<T>,
    merges: mpsc::UnboundedReceiver<Vec<Timeout<T>>>,
    shutdown: watch::Receiver<bool>,
    /// Signalled once the first backfill has completed, so `init` can promise
    /// that the wheel is seeded by the time it returns.
    seeded: oneshot::Sender<()>,
}

/// An asynchronous timer over a [`TimerWheel`].
///
/// Construct it with [`Timer::new`], start it with [`Timer::init`], feed it
/// with [`Timer::merge`], and stop it with [`Timer::stop`].
pub struct Timer<T, C> {
    merges: mpsc::UnboundedSender<Vec<Timeout<T>>>,
    shutdown: watch::Sender<bool>,
    /// Taken by `init`. Holding the un-started driver here rather than
    /// spawning in `new` is what makes `init` the moment the first backfill
    /// happens, instead of something racing construction.
    pending: Mutex<Option<(Driver<T, C>, oneshot::Receiver<()>)>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl<T, C> Timer<T, C>
where
    T: Send + 'static,
    C: Comparator<T> + Send + 'static,
{
    /// Build a timer. Nothing runs until [`Timer::init`].
    pub fn new(
        config: TimerConfig,
        cmp: C,
        now: Clock,
        on_fire: OnFire<T>,
        on_backfill: OnBackfill<T>,
    ) -> Self {
        let (merge_tx, merge_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (seeded_tx, seeded_rx) = oneshot::channel();
        Self {
            merges: merge_tx,
            shutdown: shutdown_tx,
            pending: Mutex::new(Some((
                Driver {
                    config,
                    cmp,
                    now,
                    on_fire,
                    on_backfill,
                    merges: merge_rx,
                    shutdown: shutdown_rx,
                    seeded: seeded_tx,
                },
                seeded_rx,
            ))),
            task: Mutex::new(None),
        }
    }

    /// Start the driver task and wait for the first backfill.
    ///
    /// Returns once the wheel holds whatever `backfill` had for it, so a caller
    /// that starts a timer and immediately expects near deadlines to fire is
    /// not racing the seeding.
    pub async fn init(&self) {
        let (driver, seeded) = match self.pending.lock().expect("timer mutex poisoned").take() {
            Some(d) => d,
            None => return, // already started
        };
        let handle = tokio::spawn(run(driver));
        *self.task.lock().expect("timer mutex poisoned") = Some(handle);
        // The driver signals after its first backfill. An error means it
        // stopped before getting there, which is not this call's problem.
        let _ = seeded.await;
    }

    /// Hand the timer deadlines that were just armed or moved.
    ///
    /// Cheap and non-blocking: the batch goes on a queue, and that is also what
    /// wakes the task to reconsider how long it may sleep. An entry whose
    /// identity the wheel already holds replaces it rather than joining it.
    pub fn merge(&self, batch: Vec<Timeout<T>>) {
        if batch.is_empty() {
            return;
        }
        // The only error is a closed channel, which means the timer is stopped.
        // A dropped batch then is correct: there is nothing left to fire it.
        let _ = self.merges.send(batch);
    }

    /// Stop the driver task and wait for it to finish.
    pub async fn stop(&self) {
        let _ = self.shutdown.send(true);
        let handle = self.task.lock().expect("timer mutex poisoned").take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

/// The driver. Owns the wheel for its whole life, so nothing else can lock it.
async fn run<T, C>(mut d: Driver<T, C>)
where
    T: Send + 'static,
    C: Comparator<T> + Send + 'static,
{
    let mut wheel = TimerWheel::new(d.config.capacity, d.cmp);
    let mut last_backfill: Option<Instant> = None;

    // Seed first, then tell `init` it may return.
    backfill(&mut wheel, &d.config, &d.now, &d.on_backfill, &mut last_backfill).await;
    let _ = d.seeded.send(());

    loop {
        if *d.shutdown.borrow() {
            return;
        }

        // Refill before deciding how long to sleep: a backfill can pull the
        // front of the wheel nearer, and sleeping on a stale `next` would miss
        // it until the idle interval expired.
        if wheel.len() < d.config.low_watermark
            && last_backfill.is_none_or(|t| t.elapsed() >= d.config.backfill_interval)
        {
            backfill(&mut wheel, &d.config, &d.now, &d.on_backfill, &mut last_backfill).await;
        }

        let now = (d.now)();
        let wait = match wheel.next() {
            Some(deadline) => Duration::from_millis(deadline.saturating_sub(now)).min(d.config.idle),
            None => d.config.idle,
        };

        tokio::select! {
            // Bias the queue: a batch that arrives as a deadline expires should
            // land before the fire, so what fires is the newest view.
            biased;
            _ = d.shutdown.changed() => return,
            batch = d.merges.recv() => match batch {
                Some(batch) => wheel.merge(batch),
                // Every sender is gone, so nothing can feed this wheel again.
                None => return,
            },
            _ = tokio::time::sleep(wait) => {
                let due = wheel.pop_expired((d.now)());
                if !due.is_empty() {
                    (d.on_fire)(due).await;
                }
            }
        }
    }
}

async fn backfill<T, C>(
    wheel: &mut TimerWheel<T, C>,
    config: &TimerConfig,
    now: &Clock,
    on_backfill: &OnBackfill<T>,
    last: &mut Option<Instant>,
) where
    C: Comparator<T>,
{
    *last = Some(Instant::now());
    let room = config.capacity.saturating_sub(wheel.len());
    if room == 0 {
        return;
    }
    let batch = on_backfill(now(), room).await;
    if !batch.is_empty() {
        wheel.merge(batch);
    }
}
