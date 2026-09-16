//! The sweep: expired promises, retry and lease deadlines, due schedules —
//! in bulk for `tick` and `debug.tick`, narrowed to one row for a timer
//! firing.
//!
//! Every queue here is a predicate over the promise node, as it is a partial
//! index in Postgres: a promise deadline is live exactly while the promise is
//! pending and targeted, a retry deadline while its task is pending, a lease
//! while acquired, and a schedule's `next_run_at` is the queue itself.

use std::collections::HashMap;

use neo4rs::query;

use crate::db::{p_cols, PromiseRow, Tx};
use crate::engine::{Output, Scheduled, Timeout};
use crate::ops_schedule::s_cols;
use crate::ops_task::redispatch;
use crate::{Neo4jEngine, StorageResult};
use resonate_core::types::PromiseCreateData;
use resonate_core::util;

/// The ids one queue holds past `time`, narrowed to `only` when the sweep is
/// firing a single deadline.
async fn due_ids(
    tx: &mut Tx<'_>,
    predicate: &str,
    time: i64,
    only: Option<&str>,
) -> StorageResult<Vec<String>> {
    let q = query(&format!(
        "MATCH (p:Promise) WHERE {predicate} AND ($id IS NULL OR p.id = $id) \
         RETURN p.id AS id ORDER BY id"
    ))
    .param("time", time)
    .param("id", only.map(str::to_string));
    tx.rows(q)
        .await?
        .iter()
        .map(|r| {
            r.get::<String>("id")
                .map_err(|e| crate::StorageError::Backend(format!("column id: {e}")))
        })
        .collect()
}

/// Fire expired timeouts, either all of them or one named.
///
/// `only` is what makes the precise form precise: each queue's scan is
/// narrowed to a single id, and a named timeout runs the scan for its own
/// queue and skips the other two. The narrow form is the sweep restricted to
/// one row rather than a second implementation of it.
pub(crate) async fn process_timeouts(
    tx: &mut Tx<'_>,
    time: i64,
    only: Option<&Timeout>,
) -> StorageResult<()> {
    let selected = |kind: &str| match only {
        None => Some(None::<&str>),
        Some(t) if t.kind() == kind => Some(Some(t.id())),
        Some(_) => None,
    };

    // Expired promises. Pending and targeted is the whole of what
    // promise_timeouts held; an untargeted promise times out lazily.
    if let Some(id) = selected("promise") {
        let ids = due_ids(
            tx,
            "p.state = 'pending' AND p.target IS NOT NULL AND p.timeout_at <= $time",
            time,
            id,
        )
        .await?;
        let rows: Vec<PromiseRow> = tx
            .lock_many(&ids)
            .await?
            .into_iter()
            .filter(|r| r.is_pending() && r.target.is_some() && r.timeout_at <= time)
            .collect();
        if !rows.is_empty() {
            tx.expire_batch(&rows, time).await?;
        }
    }

    // Expired retry deadlines: re-emit the execute and push the deadline out.
    if let Some(id) = selected("retry") {
        let ids = due_ids(
            tx,
            "p.task_state = 'pending' AND p.retry_timeout_at IS NOT NULL AND p.retry_timeout_at <= $time",
            time,
            id,
        )
        .await?;
        let rows = tx.lock_many(&ids).await?;
        for row in rows
            .iter()
            .filter(|r| r.task_is("pending") && r.retry_timeout_at.is_some_and(|at| at <= time))
        {
            let at = time + tx.trt;
            tx.run(
                query("MATCH (p:Promise {id: $id}) SET p.retry_timeout_at = $at, p.pid = null")
                    .param("id", row.id.as_str())
                    .param("at", at),
            )
            .await?;
            tx.emit_execute(row.target.as_deref(), &row.id, row.task_version);
            tx.arm_retry(&row.id, at);
        }
    }

    // Expired leases: the holder went away, hand the task back.
    if let Some(id) = selected("lease") {
        let ids = due_ids(
            tx,
            "p.task_state = 'acquired' AND p.lease_timeout_at IS NOT NULL AND p.lease_timeout_at <= $time",
            time,
            id,
        )
        .await?;
        let rows = tx.lock_many(&ids).await?;
        for row in rows
            .iter()
            .filter(|r| r.task_is("acquired") && r.lease_timeout_at.is_some_and(|at| at <= time))
        {
            redispatch(tx, row, time).await?;
        }
    }

    Ok(())
}

/// Fire every schedule due at `time`, or the one named. Returns how many fired.
///
/// Firing creates the schedule's promise — templated id, the schedule's param
/// and tags plus the lineage tags that make it a root — and advances
/// `next_run_at`. A promise that already exists is left alone; the schedule
/// still advances.
pub(crate) async fn process_schedule_timeouts(
    tx: &mut Tx<'_>,
    time: i64,
    only: Option<&str>,
) -> StorageResult<usize> {
    let q = query(&format!(
        "MATCH (s:Schedule) WHERE s.next_run_at <= $time AND ($id IS NULL OR s.id = $id) \
         RETURN {} ORDER BY s.id",
        s_cols("s")
    ))
    .param("time", time)
    .param("id", only.map(str::to_string));
    let due = tx.schedule_rows(q).await?;

    let mut fired = 0usize;
    for candidate in &due {
        let Some(schedule) = tx.lock_schedule(&candidate.id).await? else {
            continue;
        };
        // Someone else advanced it between the scan and the lock.
        if schedule.next_run_at != candidate.next_run_at || schedule.next_run_at > time {
            continue;
        }
        let fired_at = schedule.next_run_at;
        let next_run_at = util::compute_next_cron(&schedule.cron, fired_at);

        let promise_id = schedule
            .promise_id
            .replace("{{.id}}", &schedule.id)
            .replace("{{.timestamp}}", &fired_at.to_string());
        let mut tags: HashMap<String, String> = schedule.promise_tags.clone();
        tags.insert("resonate:schedule".to_string(), schedule.id.clone());
        tags.insert("resonate:origin".to_string(), promise_id.clone());
        tags.insert("resonate:branch".to_string(), promise_id.clone());
        tags.insert("resonate:parent".to_string(), promise_id.clone());
        tags.insert("resonate:prefix".to_string(), promise_id.clone());

        // The scheduled promise is created at `fired_at` with the deadline
        // `fired_at + promise_timeout`, and — because the sweep may run late —
        // may already be past it, in which case it is born settled. Its retry
        // deadline, when it gets one, counts from the sweep's `time`.
        create_scheduled_promise(
            tx,
            &PromiseCreateData {
                id: promise_id,
                timeout_at: fired_at + schedule.promise_timeout,
                param: schedule.promise_param.clone(),
                tags,
            },
            fired_at,
            time,
        )
        .await?;

        tx.run(
            query(
                "MATCH (s:Schedule {id: $id}) SET s.last_run_at = $fired_at, s.next_run_at = $next",
            )
            .param("id", schedule.id.as_str())
            .param("fired_at", fired_at)
            .param("next", next_run_at),
        )
        .await?;
        tx.arm(
            next_run_at,
            Timeout::ScheduleDue {
                schedule_id: schedule.id.clone(),
            },
        );
        tracing::info!(schedule_id = %schedule.id, fired_at, next_run_at, "Schedule fired");
        fired += 1;
    }
    Ok(fired)
}

/// A schedule's promise, as Postgres inserts it: `created_at` is the firing
/// time, the deadline is measured from it, but the retry deadline and the
/// already-timed-out test use the sweep's own `time`.
async fn create_scheduled_promise(
    tx: &mut Tx<'_>,
    d: &PromiseCreateData,
    fired_at: i64,
    time: i64,
) -> StorageResult<bool> {
    if tx.lock(&d.id).await?.is_some() {
        return Ok(false);
    }
    let address = d.tags.get("resonate:target").map(String::as_str);
    let already_timedout = time >= d.timeout_at;
    let (state, settled_at) = if already_timedout {
        let state = if resonate_core::types::is_timer(&d.tags) {
            "resolved"
        } else {
            "rejected_timedout"
        };
        (state, Some(d.timeout_at))
    } else {
        ("pending", None)
    };
    let task_state = address.map(|_| {
        if already_timedout {
            "fulfilled"
        } else {
            "pending"
        }
    });
    let retry_at = (task_state == Some("pending")).then_some(time + tx.trt);
    tx.create_promise(&crate::db::NewPromise {
        id: &d.id,
        state,
        param_headers: crate::db::headers_json(d.param.headers.as_ref()),
        param_data: d.param.data.as_deref(),
        tags: &d.tags,
        timeout_at: d.timeout_at,
        created_at: fired_at,
        settled_at,
        task_state,
        task_version: 0,
        retry_timeout_at: retry_at,
        lease_timeout_at: None,
        ttl: None,
        pid: None,
    })
    .await?;
    if let Some(at) = retry_at {
        tx.emit_execute(address, &d.id, 0);
        tx.arm_retry(&d.id, at);
        // The promise joins the eager sweep under exactly the condition its
        // task gets a retry deadline.
        tx.arm_promise_timeout(&d.id, d.timeout_at, true);
    }
    Ok(true)
}

/// One tick: the three timeout sweeps, then the due schedules. Returns how
/// many schedules fired, for the caller to record.
pub(crate) async fn process_all_timeouts(tx: &mut Tx<'_>, time: i64) -> StorageResult<usize> {
    tracing::debug!(time, "Processing expired timeouts");
    process_timeouts(tx, time, None).await?;
    process_schedule_timeouts(tx, time, None).await
}

/// The `limit` nearest deadlines the graph holds, soonest first.
///
/// Four indexed scans, each capped at `limit`, merged here — the union the
/// SQL engines write in one statement. Overdue deadlines sort first, which is
/// what a restarting timer wants.
pub(crate) async fn upcoming(tx: &mut Tx<'_>, limit: usize) -> StorageResult<Vec<Scheduled>> {
    let n = limit as i64;
    let mut out: Vec<Scheduled> = Vec::new();

    let promises = tx
        .promise_rows(
            query(&format!(
                "MATCH (p:Promise) WHERE p.state = 'pending' AND p.target IS NOT NULL \
                 RETURN {} ORDER BY p.timeout_at ASC, p.id ASC LIMIT $n",
                p_cols("p")
            ))
            .param("n", n),
        )
        .await?;
    out.extend(promises.into_iter().map(|p| Scheduled {
        at: p.timeout_at,
        timeout: Timeout::PromiseTimeout { promise_id: p.id },
    }));

    let retries = tx
        .promise_rows(
            query(&format!(
                "MATCH (p:Promise) WHERE p.task_state = 'pending' AND p.retry_timeout_at IS NOT NULL \
                 RETURN {} ORDER BY p.retry_timeout_at ASC, p.id ASC LIMIT $n",
                p_cols("p")
            ))
            .param("n", n),
        )
        .await?;
    out.extend(retries.into_iter().filter_map(|p| {
        p.retry_timeout_at.map(|at| Scheduled {
            at,
            timeout: Timeout::TaskRetryTimeout { task_id: p.id },
        })
    }));

    let leases = tx
        .promise_rows(
            query(&format!(
                "MATCH (p:Promise) WHERE p.task_state = 'acquired' AND p.lease_timeout_at IS NOT NULL \
                 RETURN {} ORDER BY p.lease_timeout_at ASC, p.id ASC LIMIT $n",
                p_cols("p")
            ))
            .param("n", n),
        )
        .await?;
    out.extend(leases.into_iter().filter_map(|p| {
        p.lease_timeout_at.map(|at| Scheduled {
            at,
            timeout: Timeout::TaskLeaseTimeout {
                task_id: p.id,
                pid: p.pid.unwrap_or_default(),
            },
        })
    }));

    let schedules = tx
        .schedule_rows(
            query(&format!(
                "MATCH (s:Schedule) RETURN {} ORDER BY s.next_run_at ASC, s.id ASC LIMIT $n",
                s_cols("s")
            ))
            .param("n", n),
        )
        .await?;
    out.extend(schedules.into_iter().map(|s| Scheduled {
        at: s.next_run_at,
        timeout: Timeout::ScheduleDue { schedule_id: s.id },
    }));

    out.sort_by(|a, b| {
        a.at.cmp(&b.at)
            .then_with(|| a.timeout.id().cmp(b.timeout.id()))
    });
    out.truncate(limit);
    Ok(out)
}

impl Neo4jEngine {
    /// Fire one timeout the system asked of itself: the sweep, restricted to
    /// the row it names. Idempotent — a deadline that moved or a row that
    /// settled is found not due and nothing happens.
    pub(crate) async fn fire(&self, timeout: Timeout, now: i64) -> Output {
        let timeout = &timeout;
        let swept = self
            .transact(|tx| {
                Box::pin(async move {
                    match timeout {
                        Timeout::ScheduleDue { schedule_id } => {
                            process_schedule_timeouts(tx, now, Some(schedule_id))
                                .await
                                .map(|_| ())
                        }
                        other => process_timeouts(tx, now, Some(other)).await,
                    }
                })
            })
            .await;
        match swept {
            Ok(((), messages, timeouts)) => Output {
                response: None,
                messages,
                timeouts,
            },
            Err(e) => {
                tracing::error!(error = %e, "Timeout sweep failed");
                Output::default()
            }
        }
    }
}
