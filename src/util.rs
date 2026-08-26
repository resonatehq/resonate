//! Utility functions — time and cron helpers.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// Time
// =============================================================================

/// A source of "now", in Unix milliseconds.
///
/// Time used to be a process-global: a free `system_time_ms()` reading a
/// `static AtomicI64` that no test could reset or override. The only way to
/// control it was `head.debug_time`, which is gated on `config.debug` — so
/// tests had to run the server in a mode production never uses.
///
/// A `Clock` is a value instead. Each [`Server`](crate::server::Server) owns
/// one, defaulting to [`Clock::system`], and a test constructs a
/// [`Clock::fixed`] it can advance deliberately. Nothing is shared between
/// tests, so they can run in parallel and in any order.
#[derive(Clone)]
pub struct Clock(ClockSource);

#[derive(Clone)]
enum ClockSource {
    /// Wall clock, clamped so it never runs backwards. The clamp state is
    /// per-clock, not per-process.
    System(Arc<AtomicI64>),
    /// A time the holder sets explicitly.
    Fixed(Arc<AtomicI64>),
}

impl Clock {
    /// A monotonic wall clock.
    ///
    /// If the system clock regresses (e.g. an NTP adjustment) this returns the
    /// last value it handed out rather than going backwards.
    pub fn system() -> Self {
        Clock(ClockSource::System(Arc::new(AtomicI64::new(0))))
    }

    /// A clock stopped at `now`, advanced only by [`Clock::set`] or
    /// [`Clock::advance`].
    ///
    /// Clones share the same underlying instant, so a test can hold one handle
    /// while the server holds another.
    pub fn fixed(now: i64) -> Self {
        Clock(ClockSource::Fixed(Arc::new(AtomicI64::new(now))))
    }

    /// The current time in Unix milliseconds.
    pub fn now_ms(&self) -> i64 {
        match &self.0 {
            ClockSource::System(last) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                last.fetch_max(now, Ordering::Relaxed).max(now)
            }
            ClockSource::Fixed(at) => at.load(Ordering::SeqCst),
        }
    }

    /// Move a fixed clock to `now`. No effect on a system clock.
    pub fn set(&self, now: i64) {
        if let ClockSource::Fixed(at) = &self.0 {
            at.store(now, Ordering::SeqCst);
        }
    }

    /// Move a fixed clock forward by `delta` ms, returning the new time.
    pub fn advance(&self, delta: i64) -> i64 {
        match &self.0 {
            ClockSource::Fixed(at) => at.fetch_add(delta, Ordering::SeqCst) + delta,
            ClockSource::System(_) => self.now_ms(),
        }
    }

    /// Resolve the effective `now` for an operation.
    ///
    /// `debug_time` (from the request head, already gated on `config.debug` by
    /// the caller) wins when present; otherwise the clock decides.
    pub fn resolve(&self, debug_time: Option<i64>) -> i64 {
        debug_time.unwrap_or_else(|| self.now_ms())
    }
}

impl Default for Clock {
    fn default() -> Self {
        Clock::system()
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.0 {
            ClockSource::System(_) => "system",
            ClockSource::Fixed(_) => "fixed",
        };
        f.debug_struct("Clock")
            .field("kind", &kind)
            .field("now_ms", &self.now_ms())
            .finish()
    }
}

// =============================================================================
// Cron
// =============================================================================

/// Normalize a cron expression for the `cron` crate.
///
/// The `cron` crate expects 6–7 fields (sec min hour dom month dow [year]).
/// Standard 5-field expressions (min hour dom month dow) are promoted by
/// prepending a `0` seconds field. 6- and 7-field expressions are used as-is.
fn normalize_cron(cron_expr: &str) -> String {
    if cron_expr.split_whitespace().count() == 5 {
        format!("0 {}", cron_expr.trim())
    } else {
        cron_expr.trim().to_string()
    }
}

/// Validate a cron expression. Returns `true` if the expression is parseable.
///
/// Accepts standard 5-field expressions (`min hour dom month dow`) or
/// 6–7-field expressions (`sec min hour dom month dow [year]`).
pub fn is_valid_cron(cron_expr: &str) -> bool {
    use cron::Schedule;
    use std::str::FromStr;

    Schedule::from_str(&normalize_cron(cron_expr)).is_ok()
}

/// Compute next cron occurrence after a given time (in ms).
pub fn compute_next_cron(cron_expr: &str, after_ms: i64) -> i64 {
    use chrono::Datelike;
    use chrono::Timelike;
    use cron::Schedule;
    use std::str::FromStr;

    let full_expr = normalize_cron(cron_expr);

    if let Ok(schedule) = Schedule::from_str(&full_expr) {
        let after_secs = after_ms / 1000;
        let after_dt = chrono::DateTime::from_timestamp(after_secs, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());

        // The cron crate only generates times up to ~year 2100.  Fuzz tests use
        // synthetic clock values that may be far in the future.  Find an
        // equivalent time in [1970, 2099] — same (month, day, weekday, h:m:s) —
        // compute the next cron tick from there, then shift the result back by
        // the same offset.
        const MAX_CRON_YEAR: i32 = 2099;
        let (ref_dt, delta_secs) = if after_dt.year() <= MAX_CRON_YEAR {
            (after_dt, 0i64)
        } else {
            let naive = after_dt.naive_utc();
            let (month, day, hour, minute, second) = (
                naive.month(),
                naive.day(),
                naive.hour(),
                naive.minute(),
                naive.second(),
            );
            let target_weekday = naive.weekday();
            let is_leap_day = month == 2 && day == 29;

            // Search for the latest year in [1970, 2099] with the same
            // (month, day, weekday) so the cron library sees a valid schedule.
            let equiv_secs = (1970i32..=MAX_CRON_YEAR).rev().find_map(|y| {
                if is_leap_day && chrono::NaiveDate::from_ymd_opt(y, 2, 29).is_none() {
                    return None;
                }
                chrono::NaiveDate::from_ymd_opt(y, month, day)
                    .filter(|d| d.weekday() == target_weekday)
                    .and_then(|d| d.and_hms_opt(hour, minute, second))
                    .map(|dt| dt.and_utc().timestamp())
            });

            if let Some(ref_secs) = equiv_secs {
                let ref_dt = chrono::DateTime::from_timestamp(ref_secs, 0).unwrap();
                (ref_dt, after_secs - ref_secs)
            } else {
                (after_dt, 0i64)
            }
        };

        if let Some(next) = schedule.after(&ref_dt).next() {
            return (next.timestamp() + delta_secs) * 1000;
        }
    }

    tracing::error!(
        cron_expr = cron_expr,
        "Failed to compute next cron occurrence, falling back to 60s retry"
    );
    after_ms + 60_000
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- clock ----

    #[test]
    fn a_fixed_clock_stays_where_it_was_put() {
        let clock = Clock::fixed(1_000);
        assert_eq!(clock.now_ms(), 1_000);
        assert_eq!(clock.now_ms(), 1_000, "reading it does not advance it");
    }

    #[test]
    fn a_fixed_clock_moves_only_when_told() {
        let clock = Clock::fixed(1_000);
        assert_eq!(clock.advance(500), 1_500);
        assert_eq!(clock.now_ms(), 1_500);
        clock.set(9_000);
        assert_eq!(clock.now_ms(), 9_000);
    }

    #[test]
    fn clones_of_a_fixed_clock_share_one_instant() {
        // The server holds one handle and the test holds another; advancing
        // either must be visible to both.
        let held_by_test = Clock::fixed(1_000);
        let held_by_server = held_by_test.clone();

        held_by_test.advance(250);
        assert_eq!(held_by_server.now_ms(), 1_250);

        held_by_server.set(7);
        assert_eq!(held_by_test.now_ms(), 7);
    }

    #[test]
    fn separate_fixed_clocks_are_independent() {
        // The property the old process-global `LAST_TIME_MS` could not offer:
        // two tests running in parallel cannot perturb each other.
        let a = Clock::fixed(1_000);
        let b = Clock::fixed(1_000);
        a.advance(5_000);
        assert_eq!(b.now_ms(), 1_000);
    }

    #[test]
    fn a_system_clock_is_near_the_wall_clock() {
        let clock = Clock::system();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("after 1970")
            .as_millis() as i64;
        assert!(
            (clock.now_ms() - wall).abs() < 5_000,
            "system clock should track wall time"
        );
    }

    #[test]
    fn a_system_clock_never_goes_backwards() {
        let clock = Clock::system();
        let mut previous = clock.now_ms();
        for _ in 0..1_000 {
            let now = clock.now_ms();
            assert!(now >= previous, "{now} < {previous}");
            previous = now;
        }
    }

    #[test]
    fn setting_a_system_clock_is_a_no_op() {
        let clock = Clock::system();
        let before = clock.now_ms();
        clock.set(0);
        assert!(
            clock.now_ms() >= before,
            "a system clock cannot be moved backwards by a caller"
        );
    }

    #[test]
    fn debug_time_wins_over_the_clock_when_present() {
        let clock = Clock::fixed(1_000);
        assert_eq!(clock.resolve(Some(42)), 42);
        assert_eq!(clock.resolve(None), 1_000);
    }

    #[test]
    fn the_default_clock_is_the_system_clock() {
        let clock = Clock::default();
        assert!(clock.now_ms() > 1_600_000_000_000, "a real epoch millis value");
    }

    // ---- cron validation ----

    #[test]
    fn five_and_six_field_cron_expressions_are_valid() {
        for expr in [
            "* * * * *",
            "0 * * * *",
            "0 0 * * *",
            "*/5 * * * *",
            "0 0 * * * *",
            "0 30 9 * * Mon-Fri",
        ] {
            assert!(is_valid_cron(expr), "{expr} should be valid");
        }
    }

    #[test]
    fn nonsense_cron_expressions_are_invalid() {
        for expr in ["", "not a cron", "* * *", "99 * * * *", "* * * * * * * *"] {
            assert!(!is_valid_cron(expr), "{expr} should be invalid");
        }
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        assert!(is_valid_cron("  * * * * *  "));
    }

    // ---- cron computation ----

    #[test]
    fn the_next_occurrence_is_strictly_in_the_future() {
        // Exactly on a minute boundary: the answer must be the *next* one, not
        // the current instant, or a schedule would fire in a tight loop.
        let on_the_minute = 1_700_000_040_000i64;
        let next = compute_next_cron("0 * * * * *", on_the_minute);
        assert!(next > on_the_minute, "{next} <= {on_the_minute}");
        assert_eq!(next, on_the_minute + 60_000);
    }

    #[test]
    fn a_five_field_expression_computes_the_same_as_its_six_field_form() {
        let t = 1_700_000_000_000i64;
        assert_eq!(
            compute_next_cron("* * * * *", t),
            compute_next_cron("0 * * * * *", t)
        );
    }

    #[test]
    fn an_hourly_schedule_lands_on_the_hour() {
        let next = compute_next_cron("0 0 * * * *", 1_700_000_000_000);
        assert_eq!(next % 3_600_000, 0, "{next} is not on an hour boundary");
    }

    #[test]
    fn an_unparseable_expression_falls_back_to_a_sixty_second_retry() {
        assert_eq!(compute_next_cron("nonsense", 1_000), 61_000);
    }

    /// Fuzz and differential tests use synthetic clock values far beyond the
    /// cron crate's ~year-2100 ceiling. The year-shifting workaround must keep
    /// returning a time after the input rather than falling back.
    #[test]
    fn times_beyond_the_cron_crates_year_ceiling_still_advance() {
        for t in [
            4_102_444_800_000i64,   // 2100
            32_503_680_000_000i64,  // 3000
            253_402_300_799_000i64, // 9999
        ] {
            let next = compute_next_cron("0 * * * * *", t);
            assert!(next > t, "at {t}: got {next}");
            assert!(
                next - t <= 61_000,
                "a minutely schedule should advance by about a minute, not {}",
                next - t
            );
        }
    }

    #[test]
    fn a_daily_schedule_far_in_the_future_advances_by_about_a_day() {
        let t = 32_503_680_000_000i64; // 3000
        let next = compute_next_cron("0 0 0 * * *", t);
        assert!(next > t);
        assert!(
            next - t <= 24 * 3_600_000 + 1_000,
            "expected at most a day, got {}",
            next - t
        );
    }
}
