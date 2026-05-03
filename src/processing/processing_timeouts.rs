//! Timeout processing — background loop.
//!
//! Periodically processes expired timeouts (promise, task retry, task lease)
//! and expired schedules.

use std::sync::Arc;
use std::time::Duration;

use crate::metrics;
use crate::persistence::{Db, StorageResult};
use crate::server::Server;
use crate::util;

/// Background timeout processing loop.
pub async fn timeout_processing_loop(
    state: Arc<Server>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_millis(state.config.timeouts.poll_interval);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                tracing::info!("Timeout processing loop shutting down");
                return;
            }
        }

        if state.debug_mode.load(std::sync::atomic::Ordering::SeqCst) {
            continue;
        }

        let now = util::system_time_ms();
        if let Err(e) = state
            .storage
            .transact(move |db| process_all_timeouts(db, now))
            .await
        {
            tracing::error!(error = %e, "Background timeout processing failed: storage error");
        }
    }
}

/// Process all expired timeouts at the given time.
///
/// Called by the background loop and `debug.tick`.
pub fn process_all_timeouts(db: &dyn Db, time: i64) -> StorageResult<()> {
    // Run the three tick CTE statements (promise timeouts, task retry, task lease)
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time)?;

    // Process expired schedules (application-level cron computation)
    process_schedule_timeouts(db, time)?;

    Ok(())
}

/// Process expired schedule timeouts.
fn process_schedule_timeouts(db: &dyn Db, time: i64) -> StorageResult<()> {
    let expired = db.get_expired_schedule_timeouts(time)?;

    for (schedule_id, fired_at) in &expired {
        let schedule = match db.schedule_get(schedule_id)? {
            Some(s) => s,
            None => continue,
        };

        let next_run_at = util::compute_next_cron(&schedule.cron, *fired_at);

        let mut promise_tags = schedule.promise_tags.clone();
        promise_tags.insert("resonate:schedule".to_string(), schedule_id.clone());

        match db.process_schedule_timeout(
            schedule_id,
            *fired_at,
            next_run_at,
            time,
            &promise_tags,
        )? {
            Some(_) => {
                tracing::info!(
                    schedule_id = %schedule_id,
                    fired_at = fired_at,
                    next_run_at = next_run_at,
                    "Schedule fired"
                );
                metrics::SCHEDULE_PROMISES_TOTAL.inc();
            }
            None => {
                // Idempotency guard fired or schedule was deleted — skip.
            }
        }
    }

    Ok(())
}
