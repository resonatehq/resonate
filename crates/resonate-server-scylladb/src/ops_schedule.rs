//! The schedule operations — Go's `handler_schedule.go`, translated.
//!
//! `create_token` is the pre-insert protocol's answer to racing creators:
//! it is part of the schedule_timeouts clustering key, so two racers write
//! two rows and each rolls back only its own. The winner's token is stored
//! on the schedules row and is the identity of the live anchor entry.

use resonate_core::types::{
    RequestEnvelope, ScheduleCreateData, ScheduleDeleteData, ScheduleGetData, ScheduleRecord,
    ScheduleResponseData, ScheduleSearchData, ScheduleSearchResponseData,
};
use resonate_server_dbms::engine_port::{Output, Scheduled, Timeout};
use resonate_server_dbms::StorageResult;
use scylla::value::CqlValue;
use serde_json::json;

use crate::db::{get_big, get_tags, get_text, RowMap};
use crate::ops_promise::{err, ok, parse, storage_err};
use crate::{Ctx, ScyllaEngine};

pub(crate) const S_COLS: &str = "id, origin, cron, promise_id, promise_timeout, \
     promise_param_headers, promise_param_data, promise_tags, next_run_at, last_run_at, \
     created_at, create_token";

pub(crate) fn row_to_schedule(m: &RowMap) -> ScheduleRecord {
    let param_headers = get_tags(m, "promise_param_headers");
    let last_run_at = get_big(m, "last_run_at").filter(|v| *v != 0);
    ScheduleRecord {
        id: get_text(m, "id").unwrap_or_default(),
        cron: get_text(m, "cron").unwrap_or_default(),
        promise_id: get_text(m, "promise_id").unwrap_or_default(),
        promise_timeout: get_big(m, "promise_timeout").unwrap_or(0),
        promise_param: resonate_core::types::PromiseValue {
            headers: if param_headers.is_empty() {
                None
            } else {
                Some(param_headers)
            },
            data: get_text(m, "promise_param_data"),
        },
        promise_tags: get_tags(m, "promise_tags"),
        created_at: get_big(m, "created_at").unwrap_or(0),
        next_run_at: get_big(m, "next_run_at").unwrap_or(0),
        last_run_at,
    }
}

impl ScyllaEngine {
    pub(crate) async fn read_schedule(&self, id: &str) -> StorageResult<Option<RowMap>> {
        let rows = self
            .rows(
                &format!("SELECT {S_COLS} FROM schedules WHERE origin = ? AND id = ?"),
                (id, id),
            )
            .await?;
        Ok(rows.into_iter().next())
    }

    pub(crate) async fn op_schedule_get(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let r: ScheduleGetData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        match self.read_schedule(&r.id).await {
            Err(e) => storage_err(req, e),
            Ok(None) => err(req, 404, "Schedule not found"),
            Ok(Some(m)) => ok(
                req,
                Ctx::default(),
                &ScheduleResponseData {
                    schedule: row_to_schedule(&m),
                },
            ),
        }
    }

    pub(crate) async fn op_schedule_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: ScheduleCreateData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        if !resonate_core::util::is_valid_cron(&r.cron) {
            return err(req, 400, "Invalid cron expression");
        }
        let next_run_at = resonate_core::util::compute_next_cron(&r.cron, now);
        let token = uuid::Uuid::new_v4();
        let origin = r.id.as_str(); // a schedule id carries no ':', so it is its own origin

        // Pre-insert the anchor entry, keyed by our token.
        if let Err(e) = self
            .exec(
                "INSERT INTO schedule_timeouts (bucket, shard, timeout_at, origin, schedule_id, create_token) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    self.bucket_for(next_run_at),
                    self.shard_for(&r.id),
                    next_run_at,
                    origin,
                    r.id.as_str(),
                    CqlValue::Uuid(token),
                ),
            )
            .await
        {
            return storage_err(req, e);
        }

        let (applied, lwt) = match self
            .cas(
                "INSERT INTO schedules (id, origin, cron, promise_id, promise_timeout, promise_param_headers, promise_param_data, promise_tags, next_run_at, last_run_at, created_at, create_token) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, null, ?, ?) IF NOT EXISTS",
                (
                    r.id.as_str(),
                    origin,
                    r.cron.as_str(),
                    r.promise_id.as_str(),
                    r.promise_timeout,
                    r.promise_param.headers.clone().unwrap_or_default(),
                    r.promise_param.data.clone(),
                    r.promise_tags.clone(),
                    next_run_at,
                    now,
                    CqlValue::Uuid(token),
                ),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };

        if !applied {
            // A schedule already exists: roll back our anchor (tokens are
            // random, so ours is never the live one) and return the winner.
            let _ = self
                .exec(
                    "DELETE FROM schedule_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?",
                    (
                        self.bucket_for(next_run_at),
                        self.shard_for(&r.id),
                        next_run_at,
                        origin,
                        r.id.as_str(),
                        CqlValue::Uuid(token),
                    ),
                )
                .await;
            return ok(
                req,
                Ctx::default(),
                &ScheduleResponseData {
                    schedule: row_to_schedule(&lwt),
                },
            );
        }

        let mut ctx = Ctx::default();
        ctx.armed.push(Scheduled {
            at: next_run_at,
            timeout: Timeout::ScheduleDue {
                schedule_id: r.id.clone(),
            },
        });
        ok(
            req,
            ctx,
            &ScheduleResponseData {
                schedule: ScheduleRecord {
                    id: r.id.clone(),
                    cron: r.cron.clone(),
                    promise_id: r.promise_id.clone(),
                    promise_timeout: r.promise_timeout,
                    promise_param: r.promise_param.clone(),
                    promise_tags: r.promise_tags.clone(),
                    created_at: now,
                    next_run_at,
                    last_run_at: None,
                },
            },
        )
    }

    pub(crate) async fn op_schedule_delete(&self, req: &RequestEnvelope) -> Output {
        let r: ScheduleDeleteData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let origin = r.id.as_str();
        let row = match self.read_schedule(&r.id).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Schedule not found"),
            Ok(Some(m)) => m,
        };
        let next_run_at = get_big(&row, "next_run_at").unwrap_or(0);
        let token = match row.get("create_token") {
            Some(Some(CqlValue::Uuid(u))) => Some(*u),
            _ => None,
        };

        let (applied, lwt) = match self
            .cas(
                "DELETE FROM schedules WHERE origin = ? AND id = ? IF next_run_at = ?",
                (origin, r.id.as_str(), next_run_at),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            if get_big(&lwt, "next_run_at").is_none() {
                return err(req, 404, "Schedule not found");
            }
            return err(req, 500, "Schedule modified concurrently, retry");
        }
        if let Some(token) = token {
            let _ = self
                .exec(
                    "DELETE FROM schedule_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?",
                    (
                        self.bucket_for(next_run_at),
                        self.shard_for(&r.id),
                        next_run_at,
                        origin,
                        r.id.as_str(),
                        CqlValue::Uuid(token),
                    ),
                )
                .await;
        }
        ok(req, Ctx::default(), &json!({}))
    }

    pub(crate) async fn op_schedule_search(&self, req: &RequestEnvelope) -> Output {
        let r: ScheduleSearchData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let limit = match r.limit {
            Some(n) if n > 1000 => {
                return err(req, 400, "Invalid 'limit' — must be between 1 and 1000")
            }
            Some(n) => n as usize,
            None => 10,
        };
        let rows = match self
            .rows(&format!("SELECT {S_COLS} FROM schedules"), ())
            .await
        {
            Ok(rows) => rows,
            Err(e) => return storage_err(req, e),
        };
        let mut schedules: Vec<ScheduleRecord> = rows
            .iter()
            .map(row_to_schedule)
            .filter(|s| {
                r.tags
                    .as_ref()
                    .map(|ft| {
                        ft.iter()
                            .all(|(k, v)| s.promise_tags.get(k).map(|sv| sv == v).unwrap_or(false))
                    })
                    .unwrap_or(true)
            })
            .collect();
        schedules.sort_by(|a, b| a.id.cmp(&b.id));
        if let Some(cursor) = r.cursor.as_deref() {
            schedules.retain(|s| s.id.as_str() > cursor);
        }
        let has_more = schedules.len() > limit;
        schedules.truncate(limit);
        let cursor = if has_more {
            schedules.last().map(|s| s.id.clone())
        } else {
            None
        };
        ok(
            req,
            Ctx::default(),
            &ScheduleSearchResponseData { schedules, cursor },
        )
    }
}
