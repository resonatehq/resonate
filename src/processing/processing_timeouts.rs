//! Timeout processing — background loop.
//!
//! Periodically processes expired timeouts (promise, task retry, task lease)
//! and expired schedules.

use std::sync::Arc;
use std::time::Duration;

use crate::metrics;
use crate::server::Server;
use resonate_core::util;

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

        if state.engine.is_paused() {
            continue;
        }

        let now = util::system_time_ms();
        match state.engine.tick(now).await {
            // The engine reports what happened; recording it is the caller's
            // job, the same way the router counts deliveries and the workers
            // do not.
            Ok(fired) => metrics::SCHEDULE_PROMISES_TOTAL.inc_by(fired as f64),
            Err(e) => {
                tracing::error!(error = %e, "Background timeout processing failed: storage error");
            }
        }
    }
}
