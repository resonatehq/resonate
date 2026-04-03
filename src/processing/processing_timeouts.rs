//! Timeout processing — background loop.
//!
//! Periodically processes expired timeouts (promise, task retry, task lease)
//! and expired schedules.

use std::sync::Arc;
use std::time::Duration;

use crate::metrics;
use crate::persistence::{Db, ScheduleRun, StorageResult};
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
    let expired = db.get_expired_schedules(time)?;
    for schedule in expired {
        let mut cron_time = schedule.next_run_at;
        let mut runs = Vec::new();

        while cron_time <= time {
            let promise_id = schedule
                .promise_id
                .replace("{{.id}}", &schedule.id)
                .replace("{{.timestamp}}", &cron_time.to_string());

            let timeout_at = cron_time + schedule.promise_timeout;

            runs.push(ScheduleRun {
                id: promise_id,
                timeout_at,
                created_at: cron_time,
            });

            let next = util::compute_next_cron(&schedule.cron, cron_time);
            cron_time = next;
        }

        if !runs.is_empty() {
            tracing::info!(
                schedule_id = %schedule.id,
                cron = %schedule.cron,
                runs = runs.len(),
                "Schedule triggered, creating promises"
            );
            metrics::SCHEDULE_PROMISES_TOTAL.inc_by(runs.len() as f64);
            let last_run_at = runs.last().map(|r| r.created_at).unwrap();
            db.schedule_run(&schedule.id, last_run_at, cron_time, &runs)?;
        }
    }
    Ok(())
}
