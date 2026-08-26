//! Timeout processing — background loop.
//!
//! Periodically processes expired timeouts (promise, task retry, task lease)
//! and expired schedules.

use std::sync::Arc;
use std::time::Duration;

use crate::core::types::ScheduleRecord;
use crate::metrics::Metrics;
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

        let now = state.clock.now_ms();
        let metrics = state.metrics.clone();
        if let Err(e) = state
            .storage
            .transact(move |db| process_all_timeouts(db, now, &metrics))
            .await
        {
            tracing::error!(error = %e, "Background timeout processing failed: storage error");
        }
    }
}

/// Process all expired timeouts at the given time.
///
/// Called by the background loop and `debug.tick`.
pub fn process_all_timeouts(db: &dyn Db, time: i64, metrics: &Metrics) -> StorageResult<()> {
    // Run the three tick CTE statements (promise timeouts, task retry, task lease)
    tracing::debug!(time = time, "Processing expired timeouts");
    db.process_timeouts(time)?;

    // Process expired schedules (application-level cron computation)
    process_schedule_timeouts(db, time, metrics)?;

    Ok(())
}

/// What firing a schedule at `fired_at` produces: the promise id to create, the
/// tags to stamp on it, and when the schedule should next run.
///
/// Pure. Extracted from the loop below so the id templating and the five
/// injected `resonate:*` tags — the part with actual rules in it — can be
/// tested without a database, a clock, or a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledPromise {
    pub promise_id: String,
    pub tags: std::collections::HashMap<String, String>,
    pub next_run_at: i64,
}

/// Expand a schedule into the promise it fires at `fired_at`.
pub fn schedule_promise(schedule: &ScheduleRecord, fired_at: i64) -> ScheduledPromise {
    let next_run_at = util::compute_next_cron(&schedule.cron, fired_at);

    // `{{.id}}` and `{{.timestamp}}` are the only supported placeholders.
    let promise_id = schedule
        .promise_id
        .replace("{{.id}}", &schedule.id)
        .replace("{{.timestamp}}", &fired_at.to_string());

    let mut tags = schedule.promise_tags.clone();
    tags.insert("resonate:schedule".to_string(), schedule.id.clone());
    // origin/branch/parent/prefix all name the promise itself: a scheduled
    // promise is the root of its own call graph.
    for key in [
        "resonate:origin",
        "resonate:branch",
        "resonate:parent",
        "resonate:prefix",
    ] {
        tags.insert(key.to_string(), promise_id.clone());
    }

    ScheduledPromise {
        promise_id,
        tags,
        next_run_at,
    }
}

/// Process expired schedule timeouts.
fn process_schedule_timeouts(db: &dyn Db, time: i64, metrics: &Metrics) -> StorageResult<()> {
    let expired = db.get_expired_schedule_timeouts(time)?;

    for (schedule_id, fired_at) in &expired {
        let schedule = match db.schedule_get(schedule_id)? {
            Some(s) => s,
            None => continue,
        };

        let fired = schedule_promise(&schedule, *fired_at);

        match db.process_schedule_timeout(
            schedule_id,
            *fired_at,
            fired.next_run_at,
            time,
            &fired.tags,
        )? {
            Some(_) => {
                tracing::info!(
                    schedule_id = %schedule_id,
                    fired_at = fired_at,
                    next_run_at = fired.next_run_at,
                    "Schedule fired"
                );
                metrics.schedule_promises_total.inc();
            }
            None => {
                // Idempotency guard fired or schedule was deleted — skip.
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::PromiseValue;
    use std::collections::HashMap;

    /// A schedule record with the fields these tests care about.
    fn schedule(id: &str, promise_id: &str, tags: &[(&str, &str)]) -> ScheduleRecord {
        ScheduleRecord {
            id: id.to_string(),
            cron: "0 * * * * *".to_string(), // every minute, on the second
            promise_id: promise_id.to_string(),
            promise_timeout: 60_000,
            promise_param: PromiseValue::default(),
            promise_tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            created_at: 0,
            next_run_at: 0,
            last_run_at: None,
        }
    }

    // ---- promise id templating ----

    #[test]
    fn a_plain_promise_id_is_used_verbatim() {
        let fired = schedule_promise(&schedule("s1", "my-promise", &[]), 60_000);
        assert_eq!(fired.promise_id, "my-promise");
    }

    #[test]
    fn the_id_placeholder_expands_to_the_schedule_id() {
        let fired = schedule_promise(&schedule("nightly", "job-{{.id}}", &[]), 60_000);
        assert_eq!(fired.promise_id, "job-nightly");
    }

    #[test]
    fn the_timestamp_placeholder_expands_to_the_fire_time() {
        let fired = schedule_promise(
            &schedule("s1", "job-{{.timestamp}}", &[]),
            1_700_000_000_000,
        );
        assert_eq!(fired.promise_id, "job-1700000000000");
    }

    #[test]
    fn both_placeholders_expand_together_and_repeatedly() {
        let fired = schedule_promise(
            &schedule("s1", "{{.id}}/{{.timestamp}}/{{.id}}", &[]),
            42_000,
        );
        assert_eq!(fired.promise_id, "s1/42000/s1");
    }

    #[test]
    fn an_unknown_placeholder_is_left_alone() {
        let fired = schedule_promise(&schedule("s1", "job-{{.nope}}", &[]), 60_000);
        assert_eq!(
            fired.promise_id, "job-{{.nope}}",
            "only .id and .timestamp are substituted"
        );
    }

    /// Two fires of the same schedule at different times must produce different
    /// promise ids when templated on the timestamp — otherwise the second fire
    /// is swallowed by promise-create idempotency.
    #[test]
    fn timestamp_templating_makes_each_fire_a_distinct_promise() {
        let s = schedule("s1", "job-{{.timestamp}}", &[]);
        let first = schedule_promise(&s, 60_000);
        let second = schedule_promise(&s, 120_000);
        assert_ne!(first.promise_id, second.promise_id);
    }

    // ---- tags ----

    #[test]
    fn the_schedule_tag_names_the_schedule_and_the_rest_name_the_promise() {
        let fired = schedule_promise(&schedule("nightly", "job-{{.id}}", &[]), 60_000);

        assert_eq!(fired.tags["resonate:schedule"], "nightly");
        for key in [
            "resonate:origin",
            "resonate:branch",
            "resonate:parent",
            "resonate:prefix",
        ] {
            assert_eq!(
                fired.tags[key], "job-nightly",
                "{key} names the promise, not the schedule"
            );
        }
    }

    #[test]
    fn user_tags_are_preserved() {
        let fired = schedule_promise(
            &schedule("s1", "p", &[("team", "payments"), ("env", "prod")]),
            60_000,
        );
        assert_eq!(fired.tags["team"], "payments");
        assert_eq!(fired.tags["env"], "prod");
    }

    #[test]
    fn injected_tags_win_over_user_tags_of_the_same_name() {
        // A user cannot spoof the call-graph tags by setting them on the
        // schedule.
        let fired = schedule_promise(
            &schedule(
                "s1",
                "p",
                &[
                    ("resonate:schedule", "spoofed"),
                    ("resonate:parent", "spoofed"),
                ],
            ),
            60_000,
        );
        assert_eq!(fired.tags["resonate:schedule"], "s1");
        assert_eq!(fired.tags["resonate:parent"], "p");
    }

    #[test]
    fn a_scheduled_promise_is_the_root_of_its_own_call_graph() {
        let fired = schedule_promise(&schedule("s1", "p", &[]), 60_000);
        let graph: Vec<&str> = ["resonate:origin", "resonate:branch", "resonate:parent"]
            .iter()
            .map(|k| fired.tags[*k].as_str())
            .collect();
        assert_eq!(graph, vec!["p", "p", "p"]);
    }

    // ---- next run ----

    #[test]
    fn the_next_run_is_strictly_after_the_fire_time() {
        let fired = schedule_promise(&schedule("s1", "p", &[]), 60_000);
        assert!(
            fired.next_run_at > 60_000,
            "a schedule must advance, or the loop spins: got {}",
            fired.next_run_at
        );
    }

    #[test]
    fn a_five_field_cron_is_accepted_as_well_as_a_six_field_one() {
        let mut five = schedule("s1", "p", &[]);
        five.cron = "* * * * *".to_string();
        let a = schedule_promise(&five, 60_000);

        let mut six = schedule("s1", "p", &[]);
        six.cron = "0 * * * * *".to_string();
        let b = schedule_promise(&six, 60_000);

        assert_eq!(
            a.next_run_at, b.next_run_at,
            "a 5-field expression is promoted by prepending a 0 seconds field"
        );
    }

    #[test]
    fn an_invalid_cron_falls_back_to_a_sixty_second_retry() {
        let mut bad = schedule("s1", "p", &[]);
        bad.cron = "not a cron expression".to_string();
        let fired = schedule_promise(&bad, 60_000);
        assert_eq!(
            fired.next_run_at, 120_000,
            "the schedule must keep moving rather than wedge"
        );
    }

    #[test]
    fn tags_and_id_do_not_depend_on_the_cron_expression_parsing() {
        // Even when the cron is unparseable, the promise it would create is
        // still well-formed.
        let mut bad = schedule("s1", "job-{{.id}}", &[]);
        bad.cron = "nonsense".to_string();
        let fired = schedule_promise(&bad, 60_000);
        assert_eq!(fired.promise_id, "job-s1");
        assert_eq!(fired.tags["resonate:schedule"], "s1");
    }

    #[test]
    fn expansion_is_deterministic() {
        let s = schedule("s1", "job-{{.id}}-{{.timestamp}}", &[("k", "v")]);
        assert_eq!(schedule_promise(&s, 60_000), schedule_promise(&s, 60_000));
    }

    #[test]
    fn no_placeholder_expansion_leaks_between_schedules() {
        let a = schedule_promise(&schedule("alpha", "{{.id}}", &[]), 60_000);
        let b = schedule_promise(&schedule("beta", "{{.id}}", &[]), 60_000);
        assert_eq!(a.promise_id, "alpha");
        assert_eq!(b.promise_id, "beta");
    }

    #[test]
    fn tag_maps_are_independent_per_fire() {
        let s = schedule("s1", "p", &[]);
        let mut first = schedule_promise(&s, 60_000);
        first.tags.insert("mutated".to_string(), "yes".to_string());
        let second = schedule_promise(&s, 60_000);
        assert!(
            !second.tags.contains_key("mutated"),
            "each fire gets its own tag map"
        );
    }

    #[test]
    fn a_schedule_with_no_tags_still_gets_the_five_injected_ones() {
        let fired = schedule_promise(&schedule("s1", "p", &[]), 60_000);
        let injected: HashMap<_, _> = fired
            .tags
            .iter()
            .filter(|(k, _)| k.starts_with("resonate:"))
            .collect();
        assert_eq!(injected.len(), 5);
    }
}
