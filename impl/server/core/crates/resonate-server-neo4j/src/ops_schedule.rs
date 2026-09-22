//! The `schedule.*` operations, and the schedule node as every read of it
//! projects.
//!
//! A schedule's `next_run_at` *is* its queue: there is no separate deadline
//! row, exactly as in the relational engines.

use std::collections::HashMap;

use neo4rs::{query, BoltType, Row};

use crate::db::{headers_json, tag_pairs, tags_json, Tx};
use crate::engine::{Output, Timeout};
use crate::ops_promise::{fail, ok, ok_empty, parse, search_limit};
use crate::{Neo4jEngine, StorageError, StorageResult};
use resonate_core::types::{
    PromiseValue, RequestEnvelope, ScheduleCreateData, ScheduleDeleteData, ScheduleGetData,
    ScheduleRecord, ScheduleResponseData, ScheduleSearchData, ScheduleSearchResponseData,
};
use resonate_core::util;

/// Every property a read of a schedule projects.
pub(crate) fn s_cols(a: &str) -> String {
    [
        "id",
        "cron",
        "promise_id",
        "promise_timeout",
        "promise_param_headers",
        "promise_param_data",
        "promise_tags",
        "created_at",
        "next_run_at",
        "last_run_at",
    ]
    .iter()
    .map(|f| format!("{a}.{f} AS {f}"))
    .collect::<Vec<_>>()
    .join(", ")
}

fn col<T: serde::de::DeserializeOwned>(row: &Row, key: &str) -> StorageResult<T> {
    row.get::<T>(key)
        .map_err(|e| StorageError::Backend(format!("column {key}: {e}")))
}

pub(crate) fn row_to_schedule(row: &Row) -> StorageResult<ScheduleRecord> {
    let headers: Option<String> = col(row, "promise_param_headers")?;
    let tags: Option<String> = col(row, "promise_tags")?;
    Ok(ScheduleRecord {
        id: col(row, "id")?,
        cron: col(row, "cron")?,
        promise_id: col(row, "promise_id")?,
        promise_timeout: col(row, "promise_timeout")?,
        promise_param: PromiseValue {
            headers: headers.map(|h| serde_json::from_str(&h).unwrap_or_default()),
            data: col(row, "promise_param_data")?,
        },
        promise_tags: tags
            .map(|t| serde_json::from_str(&t).unwrap_or_default())
            .unwrap_or_default(),
        created_at: col(row, "created_at")?,
        next_run_at: col(row, "next_run_at")?,
        last_run_at: col(row, "last_run_at")?,
    })
}

impl Tx<'_> {
    /// Read one schedule and hold its write lock — the same `rev` bump the
    /// promise lock uses.
    pub(crate) async fn lock_schedule(
        &mut self,
        id: &str,
    ) -> StorageResult<Option<ScheduleRecord>> {
        let q = query(&format!(
            "MATCH (s:Schedule {{id: $id}}) SET s.rev = s.rev + 1 RETURN {}",
            s_cols("s")
        ))
        .param("id", id);
        match self.one(q).await? {
            Some(row) => Ok(Some(row_to_schedule(&row)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn read_schedule(
        &mut self,
        id: &str,
    ) -> StorageResult<Option<ScheduleRecord>> {
        let q = query(&format!(
            "MATCH (s:Schedule {{id: $id}}) RETURN {}",
            s_cols("s")
        ))
        .param("id", id);
        match self.one(q).await? {
            Some(row) => Ok(Some(row_to_schedule(&row)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn schedule_rows(
        &mut self,
        q: neo4rs::Query,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        self.rows(q).await?.iter().map(row_to_schedule).collect()
    }
}

impl Neo4jEngine {
    pub(crate) async fn op_schedule_get(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let r: ScheduleGetData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                match tx.read_schedule(&r.id).await? {
                    Some(schedule) => {
                        tracing::debug!(schedule_id = %r.id, cron = %schedule.cron, next_run_at = schedule.next_run_at, "Schedule found");
                        Ok(ok(req, &ScheduleResponseData { schedule }))
                    }
                    None => {
                        tracing::debug!(schedule_id = %r.id, "Schedule not found");
                        Ok(fail(req, 404, "Schedule not found"))
                    }
                }
            })
        })
        .await
    }

    pub(crate) async fn op_schedule_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: ScheduleCreateData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        // Every promise this schedule fires carries the target, so it is held
        // to the same standard promise create holds a target to.
        if let Some(addr) = r.promise_tags.get("resonate:target") {
            if !resonate_core::is_valid_address(addr) {
                tracing::warn!(schedule_id = %r.id, address = %addr, "Schedule create rejected: invalid resonate:target address");
                return Output::response(fail(req, 400, "Invalid resonate:target address"));
            }
        }
        if !util::is_valid_cron(&r.cron) {
            tracing::warn!(schedule_id = %r.id, cron = %r.cron, "Schedule create rejected: invalid cron expression");
            return Output::response(fail(req, 400, "Invalid cron expression"));
        }
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                // Idempotent: an existing schedule is returned as it is, and
                // its deadline is left where it was.
                if let Some(schedule) = tx.lock_schedule(&r.id).await? {
                    return Ok(ok(req, &ScheduleResponseData { schedule }));
                }
                let next_run_at = util::compute_next_cron(&r.cron, now);
                let mut props: HashMap<&str, BoltType> = HashMap::new();
                props.insert("id", r.id.as_str().into());
                props.insert("cron", r.cron.as_str().into());
                props.insert("promise_id", r.promise_id.as_str().into());
                props.insert("promise_timeout", r.promise_timeout.into());
                props.insert(
                    "promise_param_headers",
                    headers_json(r.promise_param.headers.as_ref()).into(),
                );
                props.insert("promise_param_data", r.promise_param.data.clone().into());
                props.insert("promise_tags", tags_json(&r.promise_tags).into());
                props.insert("tag_kv", tag_pairs(&r.promise_tags).into());
                props.insert("created_at", now.into());
                props.insert("next_run_at", next_run_at.into());
                props.insert("last_run_at", None::<i64>.into());
                props.insert("rev", 0i64.into());
                tx.run(query("CREATE (s:Schedule) SET s = $props").param("props", props))
                    .await?;
                tx.arm(
                    next_run_at,
                    Timeout::ScheduleDue {
                        schedule_id: r.id.clone(),
                    },
                );
                let schedule = tx
                    .read_schedule(&r.id)
                    .await?
                    .ok_or_else(|| StorageError::Backend("created schedule not readable".into()))?;
                tracing::info!(schedule_id = %schedule.id, cron = %schedule.cron, next_run_at = schedule.next_run_at, "Schedule created");
                Ok(ok(req, &ScheduleResponseData { schedule }))
            })
        })
        .await
    }

    pub(crate) async fn op_schedule_delete(&self, req: &RequestEnvelope) -> Output {
        let r: ScheduleDeleteData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                let deleted = tx
                    .one(
                        query(
                            "MATCH (s:Schedule {id: $id}) WITH s, s.id AS id \
                             DETACH DELETE s RETURN id",
                        )
                        .param("id", r.id.as_str()),
                    )
                    .await?
                    .is_some();
                if deleted {
                    tracing::info!(schedule_id = %r.id, "Schedule deleted");
                    Ok(ok_empty(req))
                } else {
                    tracing::debug!(schedule_id = %r.id, "Schedule delete: not found");
                    Ok(fail(req, 404, "Schedule not found"))
                }
            })
        })
        .await
    }

    pub(crate) async fn op_schedule_search(&self, req: &RequestEnvelope) -> Output {
        let r: ScheduleSearchData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let limit = match search_limit(req, r.limit, 10) {
            Ok(n) => n,
            Err(resp) => return Output::response(resp),
        };
        let pairs = r.tags.as_ref().map(tag_pairs).unwrap_or_default();
        let r = &r;
        let pairs = &pairs;
        self.run(req, |tx| {
            Box::pin(async move {
                let q = query(&format!(
                    "MATCH (s:Schedule) \
                     WHERE ALL(kv IN $pairs WHERE kv IN s.tag_kv) \
                       AND ($cursor IS NULL OR s.id > $cursor) \
                     RETURN {} ORDER BY s.id ASC LIMIT $limit",
                    s_cols("s")
                ))
                .param("pairs", pairs.clone())
                .param("cursor", r.cursor.clone())
                .param("limit", limit + 1);
                let mut schedules = tx.schedule_rows(q).await?;
                let has_more = schedules.len() as i64 > limit;
                schedules.truncate(limit as usize);
                let cursor = if has_more {
                    schedules.last().map(|s| s.id.clone())
                } else {
                    None
                };
                tracing::debug!(
                    found = schedules.len(),
                    has_more,
                    "Schedule search completed"
                );
                Ok(ok(req, &ScheduleSearchResponseData { schedules, cursor }))
            })
        })
        .await
    }
}
