//! The in-memory timer, and what it takes to point it at the engine.
//!
//! [`Engine`] reports every deadline a transition arms, and the durable
//! row it wrote is the truth. This is the cache in front of that: a wheel of
//! the near future, so a deadline fires when it comes due rather than whenever
//! the sweep next runs.
//!
//! Nothing here is load-bearing for correctness. The wheel holds a bounded
//! prefix of the deadlines one process has heard about, which is not the same
//! set as the deadlines that exist — another instance's arming is invisible
//! until the next backfill, a restart empties it, and a full wheel drops its
//! farthest entries. Every one of those gaps is closed by the sweep the server
//! still runs. What the timer buys is latency, and it buys it by being wrong in
//! only one direction: it may fire late or not at all, never early on something
//! that is not due.

use std::sync::{Arc, Weak};

use crate::engine::{Scheduled, Timeout};
use resonate_core::util;
use resonate_timer_wheel::timer::{BoxFuture, Clock, OnBackfill, OnFire, TimerConfig};
use resonate_timer_wheel::{Comparator, Timer};

use crate::server::Server;

/// The timer as this server instantiates it.
pub type DeadlineTimer = Timer<Timeout, TimeoutComparator>;

/// Identity for the wheel: two deadlines are the same one when they are the
/// same kind on the same row.
///
/// The id alone will not do. One promise row carries a promise deadline and a
/// task deadline at the same time, and a task's retry and lease deadlines are
/// two states of one id — merging either pair would silently drop a deadline
/// the engine armed.
///
/// A lease's `pid` is deliberately not part of this. The same lease renewed by
/// the same holder is the same deadline moving, which is exactly the case the
/// wheel's replace-don't-duplicate behaviour exists for.
pub struct TimeoutComparator;

impl Comparator<Timeout> for TimeoutComparator {
    fn eq(&self, a: &Timeout, b: &Timeout) -> bool {
        a.kind() == b.kind() && a.id() == b.id()
    }
}

/// Milliseconds since the epoch, the same clock the engine is driven with.
pub fn clock() -> Clock {
    Arc::new(|| util::system_time_ms().max(0) as u64)
}

/// Build the timer for `server`.
///
/// The handle is weak because the server owns the timer: the fire callback
/// pointing back at it would otherwise be a cycle that never drops. A failed
/// upgrade means the server is gone, and a deadline with no server to fire it
/// against is nothing to do.
pub fn build(capacity: usize, wheel_refresh: u64, server: Weak<Server>) -> DeadlineTimer {
    let refresh = std::time::Duration::from_millis(wheel_refresh);
    let cfg = TimerConfig {
        capacity,
        // Half full is when to look for more. Paired with `backfill_interval`
        // it is a floor on how often the world is re-read, not a promise to
        // fill the wheel: a deployment with fewer deadlines than capacity sits
        // permanently below the mark and refills on the interval alone.
        low_watermark: capacity / 2,
        backfill_interval: refresh,
        // The same interval as the backstop. This bounds how long a deadline
        // another instance armed can stay invisible here.
        idle: refresh,
    };

    let fire_handle = server.clone();
    let on_fire: OnFire<Timeout> = Arc::new(move |batch| {
        let server = fire_handle.clone();
        Box::pin(async move {
            let Some(server) = server.upgrade() else {
                return;
            };
            server
                .fire(batch.into_iter().map(|t| t.value).collect())
                .await;
        }) as BoxFuture<()>
    });

    let fill_handle = server;
    let on_backfill: OnBackfill<Timeout> = Arc::new(move |_now, room| {
        let server = fill_handle.clone();
        Box::pin(async move {
            let Some(server) = server.upgrade() else {
                return Vec::new();
            };
            match server.engine.upcoming(room).await {
                Ok(rows) => rows.into_iter().map(scheduled_to_entry).collect(),
                Err(e) => {
                    tracing::warn!(error = %e, "Timer backfill failed; the sweep still covers it");
                    Vec::new()
                }
            }
        }) as BoxFuture<Vec<resonate_timer_wheel::Timeout<Timeout>>>
    });

    Timer::new(cfg, TimeoutComparator, clock(), on_fire, on_backfill)
}

/// A deadline the engine reported, in the form the wheel stores.
///
/// A negative instant cannot happen — every deadline is an epoch millisecond —
/// but the wheel's deadline is unsigned, so the clamp says what to do rather
/// than leaving it to a wrapping cast. Zero is "already due", which is the
/// right reading of a deadline before the epoch.
pub fn scheduled_to_entry(s: Scheduled) -> resonate_timer_wheel::Timeout<Timeout> {
    resonate_timer_wheel::Timeout::new(s.at.max(0) as u64, s.timeout)
}
