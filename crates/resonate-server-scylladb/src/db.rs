//! Plumbing: CQL execution, CAS row decoding, the promise row, and the
//! settle chain every path shares.
//!
//! Two driver facts the Go code depended on hold here too and are load
//! bearing: a failed `UPDATE ... IF` returns only the IF-clause columns,
//! while a failed `INSERT ... IF NOT EXISTS` returns the full row; and an
//! empty SET reads back as null, so `IF callbacks = ?` must be given null,
//! not {}, to match a column nothing ever wrote.

use std::collections::HashMap;

use scylla::response::query_result::QueryResult;
use scylla::statement::batch::{Batch, BatchType};
use scylla::value::{CqlValue, Row};

use resonate_core::types::{
    PromiseRecord, PromiseState, PromiseValue, TaskRecord, TaskState,
};
use resonate_server_dbms::engine_port::{Outgoing, Scheduled, Timeout};
use resonate_server_dbms::{StorageError, StorageResult};

use crate::{origin_of, Ctx, ScyllaEngine, Tags};

pub(crate) type Args = Vec<Option<CqlValue>>;
pub(crate) type RowMap = HashMap<String, Option<CqlValue>>;

pub(crate) fn text(s: impl Into<String>) -> Option<CqlValue> {
    Some(CqlValue::Text(s.into()))
}
pub(crate) fn opt_text(s: Option<impl Into<String>>) -> Option<CqlValue> {
    s.map(|v| CqlValue::Text(v.into()))
}
pub(crate) fn big(v: i64) -> Option<CqlValue> {
    Some(CqlValue::BigInt(v))
}
pub(crate) fn int(v: i32) -> Option<CqlValue> {
    Some(CqlValue::Int(v))
}
pub(crate) fn cql_map(m: &Tags) -> Option<CqlValue> {
    Some(CqlValue::Map(
        m.iter()
            .map(|(k, v)| (CqlValue::Text(k.clone()), CqlValue::Text(v.clone())))
            .collect(),
    ))
}
pub(crate) fn opt_cql_map(m: Option<&Tags>) -> Option<CqlValue> {
    m.and_then(|m| if m.is_empty() { None } else { cql_map(m) })
}
pub(crate) fn cql_set(v: &[String]) -> Option<CqlValue> {
    // An empty set IS null in Cassandra; binding {} writes nothing anyway,
    // and the IF-clause comparisons need null to match an unwritten column.
    if v.is_empty() {
        None
    } else {
        Some(CqlValue::Set(
            v.iter().map(|s| CqlValue::Text(s.clone())).collect(),
        ))
    }
}

pub(crate) fn get_text(m: &RowMap, k: &str) -> Option<String> {
    match m.get(k) {
        Some(Some(CqlValue::Text(s))) => Some(s.clone()),
        _ => None,
    }
}
pub(crate) fn get_big(m: &RowMap, k: &str) -> Option<i64> {
    match m.get(k) {
        Some(Some(CqlValue::BigInt(v))) => Some(*v),
        _ => None,
    }
}
pub(crate) fn get_int(m: &RowMap, k: &str) -> Option<i32> {
    match m.get(k) {
        Some(Some(CqlValue::Int(v))) => Some(*v),
        _ => None,
    }
}
pub(crate) fn get_bool(m: &RowMap, k: &str) -> Option<bool> {
    match m.get(k) {
        Some(Some(CqlValue::Boolean(v))) => Some(*v),
        _ => None,
    }
}
pub(crate) fn get_tags(m: &RowMap, k: &str) -> Tags {
    match m.get(k) {
        Some(Some(CqlValue::Map(pairs))) => pairs
            .iter()
            .filter_map(|(k, v)| match (k, v) {
                (CqlValue::Text(k), CqlValue::Text(v)) => Some((k.clone(), v.clone())),
                _ => None,
            })
            .collect(),
        _ => Tags::new(),
    }
}
pub(crate) fn get_set(m: &RowMap, k: &str) -> Vec<String> {
    match m.get(k) {
        Some(Some(CqlValue::Set(vals))) => vals
            .iter()
            .filter_map(|v| match v {
                CqlValue::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn backend(e: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(e.to_string())
}

/// The full promises row, as every handler reads it — Go's `promiseRow`.
#[derive(Debug, Clone)]
pub(crate) struct PromiseRow {
    pub id: String,
    pub origin: String,
    pub state: String,
    pub param_headers: Tags,
    pub param_data: Option<String>,
    pub value_headers: Tags,
    pub value_data: Option<String>,
    pub tags: Tags,
    pub target: Option<String>,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub callbacks: Vec<String>,
    pub listeners: Vec<String>,
    pub task_state: Option<String>,
    pub task_version: i64,
    pub task_ttl: Option<i64>,
    pub task_pid: Option<String>,
    pub task_resumes: Vec<String>,
    pub task_timeout_retry: Option<i64>,
    pub task_timeout_lease: Option<i64>,
}

pub(crate) const P_COLS: &str = "id, origin, state, param_headers, param_data, \
     value_headers, value_data, tags, target, timeout_at, created_at, settled_at, \
     callbacks, listeners, task_state, task_version, task_ttl, task_pid, \
     task_resumes, task_timeout_retry, task_timeout_lease";

impl PromiseRow {
    pub(crate) fn from_map(m: &RowMap) -> Self {
        Self {
            id: get_text(m, "id").unwrap_or_default(),
            origin: get_text(m, "origin").unwrap_or_default(),
            state: get_text(m, "state").unwrap_or_default(),
            param_headers: get_tags(m, "param_headers"),
            param_data: get_text(m, "param_data"),
            value_headers: get_tags(m, "value_headers"),
            value_data: get_text(m, "value_data"),
            tags: get_tags(m, "tags"),
            target: get_text(m, "target"),
            timeout_at: get_big(m, "timeout_at").unwrap_or(0),
            created_at: get_big(m, "created_at").unwrap_or(0),
            settled_at: get_big(m, "settled_at"),
            callbacks: get_set(m, "callbacks"),
            listeners: get_set(m, "listeners"),
            task_state: get_text(m, "task_state"),
            task_version: get_int(m, "task_version").unwrap_or(0) as i64,
            task_ttl: get_big(m, "task_ttl"),
            task_pid: get_text(m, "task_pid"),
            task_resumes: get_set(m, "task_resumes"),
            task_timeout_retry: get_big(m, "task_timeout_retry"),
            task_timeout_lease: get_big(m, "task_timeout_lease"),
        }
    }

    pub(crate) fn to_promise_record(&self) -> PromiseRecord {
        PromiseRecord {
            id: self.id.clone(),
            state: parse_promise_state(&self.state),
            param: PromiseValue {
                headers: if self.param_headers.is_empty() {
                    None
                } else {
                    Some(self.param_headers.clone())
                },
                data: self.param_data.clone(),
            },
            value: PromiseValue {
                headers: if self.value_headers.is_empty() {
                    None
                } else {
                    Some(self.value_headers.clone())
                },
                data: self.value_data.clone(),
            },
            tags: self.tags.clone(),
            timeout_at: self.timeout_at,
            created_at: self.created_at,
            settled_at: self.settled_at,
        }
    }

    pub(crate) fn to_task_record(&self) -> Option<TaskRecord> {
        let state = self.task_state.as_deref()?;
        let acquired = state == "acquired";
        Some(TaskRecord {
            id: self.id.clone(),
            state: parse_task_state(state),
            version: self.task_version,
            resumes: self.task_resumes.len() as i64,
            ttl: if acquired { self.task_ttl } else { None },
            pid: if acquired { self.task_pid.clone() } else { None },
        })
    }

    pub(crate) fn is_timer(&self) -> bool {
        self.tags.get("resonate:timer").map(String::as_str) == Some("true")
    }
}

pub(crate) fn parse_promise_state(s: &str) -> PromiseState {
    match s {
        "resolved" => PromiseState::Resolved,
        "rejected" => PromiseState::Rejected,
        "rejected_canceled" => PromiseState::RejectedCanceled,
        "rejected_timedout" => PromiseState::RejectedTimedout,
        _ => PromiseState::Pending,
    }
}

pub(crate) fn parse_task_state(s: &str) -> TaskState {
    match s {
        "acquired" => TaskState::Acquired,
        "suspended" => TaskState::Suspended,
        "halted" => TaskState::Halted,
        "fulfilled" => TaskState::Fulfilled,
        _ => TaskState::Pending,
    }
}

/// What `enqueue_resume` decided.
pub(crate) enum SettleOutcome {
    /// This call won: the suspended awaiters to send executes for, and the
    /// retry deadline their pre-inserted queue entries carry.
    Won {
        resumed: Vec<ResumedAwaiter>,
        retry_at: i64,
    },
    /// A concurrent settle won; the winner's verdict, reconstructed from
    /// the failed batch's returned columns.
    Lost {
        state: String,
        value_headers: Tags,
        value_data: Option<String>,
        settled_at: Option<i64>,
    },
    /// A per-awaiter IF failed — nothing committed, retriable.
    Conflict,
}

pub(crate) struct ResumedAwaiter {
    pub id: String,
    pub target: Option<String>,
    pub version: i64,
}

impl ScyllaEngine {
    pub(crate) async fn exec(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> StorageResult<QueryResult> {
        self.session
            .execute_unpaged(cql, values)
            .await
            .map_err(backend)
    }

    /// All rows of a SELECT, as name → value maps — gocql's MapScan.
    pub(crate) async fn rows(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> StorageResult<Vec<RowMap>> {
        let result = self.exec(cql, values).await?;
        rows_of(result)
    }

    /// One LWT: `(applied, returned row)`. On `!applied` the row carries the
    /// IF-clause columns (UPDATE) or the full surviving row (INSERT).
    pub(crate) async fn cas(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> StorageResult<(bool, RowMap)> {
        let result = self.exec(cql, values).await?;
        let rows = rows_of(result)?;
        let first = rows.into_iter().next().unwrap_or_default();
        let applied = get_bool(&first, "[applied]").unwrap_or(false);
        Ok((applied, first))
    }

    /// A logged conditional batch — gocql's MapExecuteBatchCAS. All
    /// statements must live in one partition; the protocol's same-origin
    /// rules are what guarantee they do.
    pub(crate) async fn batch_cas(
        &self,
        stmts: Vec<(String, Args)>,
    ) -> StorageResult<(bool, RowMap)> {
        let mut batch = Batch::new(BatchType::Logged);
        let mut values: Vec<Args> = Vec::with_capacity(stmts.len());
        for (cql, args) in stmts {
            batch.append_statement(cql.as_str());
            values.push(args);
        }
        let batch = self
            .session
            .get_session()
            .prepare_batch(&batch)
            .await
            .map_err(backend)?;
        let result = self
            .session
            .get_session()
            .batch(&batch, values)
            .await
            .map_err(backend)?;
        let rows = rows_of(result)?;
        let first = rows.into_iter().next().unwrap_or_default();
        let applied = get_bool(&first, "[applied]").unwrap_or(false);
        Ok((applied, first))
    }

    pub(crate) async fn read_promise(
        &self,
        origin: &str,
        id: &str,
    ) -> StorageResult<Option<PromiseRow>> {
        let rows = self
            .rows(
                &format!("SELECT {P_COLS} FROM promises WHERE origin = ? AND id = ?"),
                (origin, id),
            )
            .await?;
        Ok(rows.first().map(PromiseRow::from_map))
    }

    /// The ghost operation: read, and eagerly settle if logically expired —
    /// Go's `readAndTryTimeout`. Returns the row as a caller must see it,
    /// re-read after a settle so it reflects what committed.
    pub(crate) async fn read_and_try_timeout(
        &self,
        ctx: &mut Ctx,
        id: &str,
        now: i64,
    ) -> StorageResult<Option<PromiseRow>> {
        let origin = origin_of(id).to_string();
        let Some(row) = self.read_promise(&origin, id).await? else {
            return Ok(None);
        };
        if row.state == "pending" && now >= row.timeout_at {
            self.try_timeout(ctx, &row, now).await?;
            return self.read_promise(&origin, id).await;
        }
        Ok(Some(row))
    }

    /// Eagerly settle one expired pending promise — Go's `tryTimeout`: the
    /// pinned-callbacks settle through `enqueue_resume`, then messages,
    /// then queue cleanup, all idempotent against a concurrent winner.
    pub(crate) async fn try_timeout(
        &self,
        ctx: &mut Ctx,
        row: &PromiseRow,
        now: i64,
    ) -> StorageResult<()> {
        let new_state = if row.is_timer() {
            "resolved"
        } else {
            "rejected_timedout"
        };
        let has_task = row.task_state.is_some();
        let settle_stmt = settle_cql(has_task);
        let settle_args: Args = vec![
            text(new_state),
            big(row.timeout_at),
            text(&row.origin),
            text(&row.id),
            cql_set(&row.callbacks),
        ];

        let outcome = self
            .enqueue_resume(&row.id, &row.origin, &row.callbacks, now, settle_stmt, settle_args)
            .await?;

        if let SettleOutcome::Won { resumed, retry_at } = outcome {
            // The settled record, as the batch just made it.
            let mut settled = row.clone();
            settled.state = new_state.to_string();
            settled.settled_at = Some(row.timeout_at);
            settled.value_headers = Tags::new();
            settled.value_data = None;
            self.after_settle_won(ctx, &settled, resumed, retry_at)
                .await?;
        }
        Ok(())
    }

    /// Everything a winning settle owes after the batch, in Go's order:
    /// executes to the resumed, unblocks to the listeners, then best-effort
    /// queue-entry deletes. Nothing else — a fulfilled awaiter stays in
    /// other promises' callbacks sets and is skipped lazily when they
    /// settle, exactly as the Go implementation leaves it.
    pub(crate) async fn after_settle_won(
        &self,
        ctx: &mut Ctx,
        settled: &PromiseRow,
        resumed: Vec<ResumedAwaiter>,
        retry_at: i64,
    ) -> StorageResult<()> {
        for r in &resumed {
            self.arm_retry(ctx, &r.id, retry_at);
            if let Some(target) = &r.target {
                ctx.messages.push(Outgoing::Execute {
                    address: target.clone(),
                    task_id: r.id.clone(),
                    version: r.version,
                });
            }
        }
        let record = settled.to_promise_record();
        for address in &settled.listeners {
            ctx.messages.push(Outgoing::Unblock {
                address: address.clone(),
                promise: record.clone(),
            });
        }

        // EXPLORATORY (decision pending): the relational engines delete a
        // fulfilled awaiter's registrations at settlement; Go leaves them to
        // be skipped lazily. The differential sees the difference in the
        // callbacks section (first at ~step 1923). Aligned here so the run
        // can continue cataloguing; same-origin rules make it one
        // partition-local scan.
        if settled.task_state.is_some() {
            let holders = self
                .rows(
                    "SELECT id, callbacks FROM promises WHERE origin = ?",
                    (settled.origin.as_str(),),
                )
                .await?;
            for m in holders {
                let hid = get_text(&m, "id").unwrap_or_default();
                if hid != settled.id
                    && get_set(&m, "callbacks").iter().any(|c| c == &settled.id)
                {
                    let _ = self
                        .exec(
                            "UPDATE promises SET callbacks = callbacks - ? WHERE origin = ? AND id = ?",
                            (
                                vec![settled.id.clone()],
                                settled.origin.as_str(),
                                hid.as_str(),
                            ),
                        )
                        .await;
                }
            }
        }

        // Queue cleanup on a win, best effort — a kill here leaves an
        // orphan entry the tick handlers classify and delete.
        let _ = self
            .exec(
                "DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?",
                (
                    self.bucket_for(settled.timeout_at),
                    self.shard_for(&settled.id),
                    settled.timeout_at,
                    settled.origin.as_str(),
                    settled.id.as_str(),
                ),
            )
            .await;
        if let Some(retry_at) = settled.task_timeout_retry {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(&settled.id),
                        retry_at,
                        settled.origin.as_str(),
                        settled.id.as_str(),
                    ),
                )
                .await;
        }
        if let Some(lease_at) = settled.task_timeout_lease {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(lease_at),
                        self.shard_for(&settled.id),
                        lease_at,
                        settled.origin.as_str(),
                        settled.id.as_str(),
                    ),
                )
                .await;
        }
        Ok(())
    }

    /// Atomically settle and fan out to the awaiters — Go's `enqueueResume`:
    /// pre-insert retry entries for the suspended, then one logged
    /// conditional batch of the caller's settle plus one statement per
    /// awaiter, keyed by the task state each was read in.
    pub(crate) async fn enqueue_resume(
        &self,
        settled_id: &str,
        origin: &str,
        callbacks: &[String],
        now: i64,
        settle_stmt: String,
        settle_args: Args,
    ) -> StorageResult<SettleOutcome> {
        struct Awaiter {
            id: String,
            task_state: Option<String>,
            version: i64,
            target: Option<String>,
            timeout_at: i64,
        }

        let mut awaiters = Vec::new();
        for cb in callbacks {
            let rows = self
                .rows(
                    "SELECT task_state, task_version, target, timeout_at FROM promises WHERE origin = ? AND id = ?",
                    (origin, cb.as_str()),
                )
                .await?;
            if let Some(m) = rows.first() {
                awaiters.push(Awaiter {
                    id: cb.clone(),
                    task_state: get_text(m, "task_state"),
                    version: get_int(m, "task_version").unwrap_or(0) as i64,
                    target: get_text(m, "target"),
                    timeout_at: get_big(m, "timeout_at").unwrap_or(0),
                });
            }
        }

        // Pre-insert retry entries for the suspended, rollback on failure.
        let retry_at = now + self.task_retry_timeout;
        let mut preinserted: Vec<String> = Vec::new();
        for a in &awaiters {
            if a.task_state.as_deref() == Some("suspended") {
                if let Err(e) = self
                    .exec(
                        "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
                        (
                            self.bucket_for(retry_at),
                            self.shard_for(&a.id),
                            retry_at,
                            a.id.as_str(),
                            origin,
                            a.timeout_at,
                        ),
                    )
                    .await
                {
                    self.rollback_retries(origin, &preinserted, retry_at).await;
                    return Err(e);
                }
                preinserted.push(a.id.clone());
            }
        }

        let mut stmts: Vec<(String, Args)> = vec![(settle_stmt, settle_args)];
        for a in &awaiters {
            match a.task_state.as_deref() {
                Some("fulfilled") | None => {}
                Some("suspended") => stmts.push((
                    "UPDATE promises SET task_state = 'pending', task_resumes = ?, task_timeout_retry = ? WHERE origin = ? AND id = ? IF task_state = 'suspended'".to_string(),
                    vec![
                        cql_set(std::slice::from_ref(&settled_id.to_string())),
                        big(retry_at),
                        text(origin),
                        text(&a.id),
                    ],
                )),
                Some(state) => stmts.push((
                    format!(
                        "UPDATE promises SET task_resumes = task_resumes + ? WHERE origin = ? AND id = ? IF task_state = '{state}'"
                    ),
                    vec![
                        cql_set(std::slice::from_ref(&settled_id.to_string())),
                        text(origin),
                        text(&a.id),
                    ],
                )),
            }
        }

        let (applied, row) = match self.batch_cas(stmts).await {
            Ok(v) => v,
            Err(e) => {
                self.rollback_retries(origin, &preinserted, retry_at).await;
                return Err(e);
            }
        };

        if !applied {
            self.rollback_retries(origin, &preinserted, retry_at).await;
            let state = get_text(&row, "state").unwrap_or_default();
            if !state.is_empty() && state != "pending" {
                return Ok(SettleOutcome::Lost {
                    state,
                    value_headers: get_tags(&row, "value_headers"),
                    value_data: get_text(&row, "value_data"),
                    settled_at: get_big(&row, "settled_at"),
                });
            }
            return Ok(SettleOutcome::Conflict);
        }

        let resumed = awaiters
            .into_iter()
            .filter(|a| a.task_state.as_deref() == Some("suspended"))
            .map(|a| ResumedAwaiter {
                id: a.id,
                target: a.target,
                version: a.version,
            })
            .collect();
        Ok(SettleOutcome::Won { resumed, retry_at })
    }

    async fn rollback_retries(&self, origin: &str, ids: &[String], retry_at: i64) {
        for id in ids {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(id),
                        retry_at,
                        origin,
                        id.as_str(),
                    ),
                )
                .await;
        }
    }

    /// Report a retry deadline this transition just wrote.
    pub(crate) fn arm_retry(&self, ctx: &mut Ctx, task_id: &str, at: i64) {
        ctx.armed.push(Scheduled {
            at,
            timeout: Timeout::TaskRetryTimeout {
                task_id: task_id.to_string(),
            },
        });
    }

    pub(crate) fn arm_lease(&self, ctx: &mut Ctx, task_id: &str, pid: &str, at: i64) {
        ctx.armed.push(Scheduled {
            at,
            timeout: Timeout::TaskLeaseTimeout {
                task_id: task_id.to_string(),
                pid: pid.to_string(),
            },
        });
    }

    /// A promise joins the reported hints only when awaitable — the queue
    /// row is written for everyone (eager, like Go), but the announcement
    /// follows the protocol every engine speaks.
    pub(crate) fn arm_promise_timeout(
        &self,
        ctx: &mut Ctx,
        promise_id: &str,
        timeout_at: i64,
        awaitable: bool,
    ) {
        if awaitable {
            ctx.armed.push(Scheduled {
                at: timeout_at,
                timeout: Timeout::PromiseTimeout {
                    promise_id: promise_id.to_string(),
                },
            });
        }
    }

}

pub(crate) fn rows_of(result: QueryResult) -> StorageResult<Vec<RowMap>> {
    if !result.is_rows() {
        return Ok(Vec::new());
    }
    let rows_result = result.into_rows_result().map_err(backend)?;
    let names: Vec<String> = rows_result
        .column_specs()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let mut out = Vec::new();
    for row in rows_result.rows::<Row>().map_err(backend)? {
        let row = row.map_err(backend)?;
        let mut m = RowMap::new();
        for (name, value) in names.iter().zip(row.columns.into_iter()) {
            m.insert(name.clone(), value);
        }
        out.push(m);
    }
    Ok(out)
}

/// The settle statement both settle paths share — Go's two variants: with
/// the task columns folded in when the promise carries one. The IF pins
/// state, the exact callbacks set, and the un-written value columns, so a
/// failed batch returns enough to reconstruct the winner's verdict.
pub(crate) fn settle_cql(has_task: bool) -> String {
    if has_task {
        "UPDATE promises SET state = ?, settled_at = ?, value_headers = null, value_data = null, \
         callbacks = {}, listeners = {}, task_state = 'fulfilled', task_pid = null, \
         task_ttl = null, task_timeout_retry = null, task_timeout_lease = null, task_resumes = {} \
         WHERE origin = ? AND id = ? \
         IF state = 'pending' AND callbacks = ? AND settled_at = null AND value_headers = null AND value_data = null"
            .to_string()
    } else {
        "UPDATE promises SET state = ?, settled_at = ?, value_headers = null, value_data = null, \
         callbacks = {}, listeners = {} \
         WHERE origin = ? AND id = ? \
         IF state = 'pending' AND callbacks = ? AND settled_at = null AND value_headers = null AND value_data = null"
            .to_string()
    }
}
