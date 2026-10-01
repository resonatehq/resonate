//! The `schedule.*` operations, and the schedule document as every read of it
//! projects.
//!
//! A schedule's `next_run_at` *is* its queue: there is no separate deadline
//! row, exactly as in the relational engines.

use mongodb::bson::{doc, Bson, Document};

use crate::db::{
    contains_pairs, headers, i64_of, map_of, pairs, required_i64, required_str, str_of, Tx,
};
use crate::engine::{Output, Timeout};
use crate::ops_promise::{fail, ok, ok_empty, parse, search_limit};
use crate::{MongoDbEngine, StorageError, StorageResult};
use resonate_core::types::{
    PromiseValue, RequestEnvelope, ScheduleCreateData, ScheduleDeleteData, ScheduleGetData,
    ScheduleRecord, ScheduleResponseData, ScheduleSearchData, ScheduleSearchResponseData,
};
use resonate_core::util;

pub(crate) fn doc_to_schedule(d: &Document) -> StorageResult<ScheduleRecord> {
    Ok(ScheduleRecord {
        id: required_str(d, "_id")?,
        cron: required_str(d, "cron")?,
        promise_id: required_str(d, "promise_id")?,
        promise_timeout: required_i64(d, "promise_timeout")?,
        promise_param: PromiseValue {
            headers: map_of(d, "promise_param_headers"),
            data: str_of(d, "promise_param_data"),
        },
        promise_tags: map_of(d, "promise_tags").unwrap_or_default(),
        created_at: required_i64(d, "created_at")?,
        next_run_at: required_i64(d, "next_run_at")?,
        last_run_at: i64_of(d, "last_run_at"),
    })
}

impl Tx<'_> {
    /// Read one schedule and hold it to commit — the same `rev` bump the
    /// promise lock uses.
    pub(crate) async fn lock_schedule(
        &mut self,
        id: &str,
    ) -> StorageResult<Option<ScheduleRecord>> {
        let schedules = self.schedules.clone();
        match self.lock_one(&schedules, id).await? {
            Some(d) => Ok(Some(doc_to_schedule(&d)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn read_schedule(
        &mut self,
        id: &str,
    ) -> StorageResult<Option<ScheduleRecord>> {
        let schedules = self.schedules.clone();
        match self.find_one(&schedules, doc! { "_id": id }).await? {
            Some(d) => Ok(Some(doc_to_schedule(&d)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn schedule_rows(
        &mut self,
        filter: Document,
        sort: Document,
        limit: Option<i64>,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let schedules = self.schedules.clone();
        self.find(&schedules, filter, sort, limit)
            .await?
            .iter()
            .map(doc_to_schedule)
            .collect()
    }
}

impl MongoDbEngine {
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
                let schedules = tx.schedules.clone();
                tx.insert(
                    &schedules,
                    doc! {
                        "_id": r.id.as_str(),
                        "cron": r.cron.as_str(),
                        "promise_id": r.promise_id.as_str(),
                        "promise_timeout": r.promise_timeout,
                        "promise_param_headers": headers(r.promise_param.headers.as_ref()),
                        "promise_param_data": r.promise_param.data.as_deref(),
                        "promise_tags": pairs(&r.promise_tags),
                        "created_at": now,
                        "next_run_at": next_run_at,
                        "last_run_at": Bson::Null,
                        "rev": 0_i64,
                    },
                )
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
                let schedules = tx.schedules.clone();
                let deleted = tx
                    .delete_one(&schedules, doc! { "_id": r.id.as_str() })
                    .await?;
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
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                let mut and = Vec::new();
                if let Some(tags) = r
                    .tags
                    .as_ref()
                    .and_then(|t| contains_pairs("promise_tags", t))
                {
                    and.push(tags);
                }
                if let Some(cursor) = &r.cursor {
                    and.push(doc! { "_id": { "$gt": cursor.as_str() } });
                }
                let filter = if and.is_empty() {
                    doc! {}
                } else {
                    doc! { "$and": and }
                };
                let mut schedules = tx
                    .schedule_rows(filter, doc! { "_id": 1 }, Some(limit + 1))
                    .await?;
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
