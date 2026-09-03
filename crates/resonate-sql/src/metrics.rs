//! What a server built here counts.
//!
//! Declared where they are incremented, not in a central list. `register_*!`
//! writes into prometheus' process-wide default registry and the `/metrics`
//! gateway reads the same one, so nothing has to introduce them to each other.
//! Through `resonate_plugin::prometheus` so there is one version of it in the
//! graph, and one registry.

use lazy_static::lazy_static;
use resonate_plugin::prometheus::{register_counter, register_counter_vec, Counter, CounterVec};

lazy_static! {
    /// Messages a transition emitted, by kind. Counted here rather than in the
    /// router because the router sees deliveries, and a message that never
    /// reached a worker still happened.
    pub static ref MESSAGES_TOTAL: CounterVec = register_counter_vec!(
        "resonate_messages_total",
        "Total number of messages delivered by kind",
        &["kind"]
    )
    .unwrap();

    /// Promises the sweep created by firing a schedule.
    pub static ref SCHEDULE_PROMISES_TOTAL: Counter = register_counter!(
        "resonate_schedule_promises_total",
        "Total number of promises created by schedules"
    )
    .unwrap();
}

/// Touch every counter so it is present at zero rather than appearing on its
/// first increment. A missing series and a zero series mean different things to
/// whoever is reading the dashboard.
pub fn declare() {
    MESSAGES_TOTAL.with_label_values(&["execute"]);
    MESSAGES_TOTAL.with_label_values(&["unblock"]);
    SCHEDULE_PROMISES_TOTAL.get();
}
