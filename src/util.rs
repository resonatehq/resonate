//! Utility functions — time and cron helpers.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// Time
// =============================================================================

/// Tracks the latest time value returned, ensuring monotonicity.
static LAST_TIME_MS: AtomicI64 = AtomicI64::new(0);

/// Returns current wall-clock time in Unix milliseconds.
///
/// Monotonic: if the system clock regresses (e.g. NTP adjustment),
/// returns the last seen value instead of going backwards.
pub fn system_time_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    LAST_TIME_MS.fetch_max(now, Ordering::Relaxed).max(now)
}

/// Resolve the effective `now` for an operation.
///
/// If debug_time is provided (from `resonate:debug_time` header), use it.
/// Otherwise, use the wall-clock time.
pub fn resolve_time(debug_time: Option<i64>) -> i64 {
    debug_time.unwrap_or_else(system_time_ms)
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
    use cron::Schedule;
    use std::str::FromStr;

    let full_expr = normalize_cron(cron_expr);

    if let Ok(schedule) = Schedule::from_str(&full_expr) {
        let after_secs = after_ms / 1000;
        let after_dt = chrono::DateTime::from_timestamp(after_secs, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());

        if let Some(next) = schedule.after(&after_dt).next() {
            return next.timestamp() * 1000;
        }
    }

    tracing::error!(
        cron_expr = cron_expr,
        "Failed to compute next cron occurrence, falling back to 60s retry"
    );
    after_ms + 60_000
}
