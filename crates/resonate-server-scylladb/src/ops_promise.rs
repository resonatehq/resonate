//! The promise operations — Go's `handler_promise.go`, translated.
//!
//! Flow, guards and statuses are the Go implementation's; what differs is
//! the envelope (this repo's request/response types), messages riding out
//! on the `Output` instead of a dispatcher, and `now` arriving as a
//! parameter.

use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRecord,
    PromiseRegisterCallbackData, PromiseRegisterListenerData, PromiseResponseData,
    PromiseSearchData, PromiseSearchResponseData, PromiseSettleData, PromiseValue,
    RequestEnvelope, ResponseEnvelope,
};
use resonate_server_dbms::engine_port::{Outgoing, Output};
use validator::Validate;

use crate::db::{
    big, cql_map, cql_set, get_big, int, opt_cql_map, opt_text, text, Args,
    PromiseRow, SettleOutcome,
};
use crate::{origin_of, Ctx, ScyllaEngine};

/// Parse and validate one request's data, or produce the 400 the envelope
/// owes — the same two-step every engine starts with.
pub(crate) fn parse<T>(req: &RequestEnvelope) -> Result<T, ResponseEnvelope>
where
    T: serde::de::DeserializeOwned + Validate,
{
    let d: T = match serde_json::from_value(req.data.clone()) {
        Ok(d) => d,
        Err(e) => {
            return Err(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Invalid request: {}", e),
            ))
        }
    };
    if let Err(e) = d.validate() {
        return Err(ResponseEnvelope::error(
            req.kind.clone(),
            req.head.corr_id.clone(),
            400,
            &format_validation_errors(&e),
        ));
    }
    Ok(d)
}

pub(crate) fn err(req: &RequestEnvelope, status: i32, msg: &str) -> Output {
    Output::response(ResponseEnvelope::error(
        req.kind.clone(),
        req.head.corr_id.clone(),
        status,
        msg,
    ))
}

pub(crate) fn ok<T: serde::Serialize>(req: &RequestEnvelope, ctx: Ctx, data: &T) -> Output {
    Output {
        response: Some(ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            data,
        )),
        messages: ctx.messages,
        timeouts: ctx.armed,
    }
}

pub(crate) fn storage_err(req: &RequestEnvelope, e: impl std::fmt::Display) -> Output {
    Output::response(ResponseEnvelope::error(
        req.kind.clone(),
        req.head.corr_id.clone(),
        500,
        &format!("Internal error: {}", e),
    ))
}

impl ScyllaEngine {
    pub(crate) async fn op_promise_get(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseGetData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => storage_err(req, e),
            Ok(None) => err(req, 404, "Promise not found"),
            Ok(Some(row)) => {
                let promise = row.to_promise_record();
                ok(req, ctx, &PromiseResponseData { promise })
            }
        }
    }

    pub(crate) async fn op_promise_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseCreateData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        if let Some(addr) = r.tags.get("resonate:target") {
            if !resonate_core::is_valid_address(addr) {
                return err(req, 400, "Invalid resonate:target address");
            }
        }
        let mut ctx = Ctx::default();
        match self.promise_create_impl(&mut ctx, &r, now).await {
            Ok(promise) => ok(req, ctx, &PromiseResponseData { promise }),
            Err(e) => storage_err(req, e),
        }
    }

    /// Go's `PromiseCreate` past validation: pre-insert queue entries, one
    /// `INSERT ... IF NOT EXISTS`, and the loser's response rebuilt from the
    /// returned row — eagerly settled when the existing row has expired.
    /// Also the fence's inner create, which is the same transition.
    pub(crate) async fn promise_create_impl(
        &self,
        ctx: &mut Ctx,
        r: &PromiseCreateData,
        now: i64,
    ) -> Result<PromiseRecord, resonate_server_dbms::StorageError> {
        let id = r.id.as_str();
        let origin = origin_of(id).to_string();
        let target = r.tags.get("resonate:target").cloned();
        let has_task = target.is_some();
        let awaitable = resonate_core::types::is_awaitable(&r.tags);
        let is_timer = r.tags.get("resonate:timer").map(String::as_str) == Some("true");
        let already_timedout = now >= r.timeout_at;

        let (state, created_at, settled_at) = if already_timedout {
            let s = if is_timer { "resolved" } else { "rejected_timedout" };
            (s, r.timeout_at, Some(r.timeout_at))
        } else {
            ("pending", now, None)
        };

        // Retry deadline for a pending task: the delay tag when it names a
        // future instant, else immediately (now + retry timeout).
        let mut task_retry_immediate = true;
        let mut task_retry_at = now + self.task_retry_timeout;
        if let Some(ds) = r.tags.get("resonate:delay") {
            if let Ok(d) = ds.parse::<i64>() {
                if d > now {
                    task_retry_at = d;
                    task_retry_immediate = false;
                }
            }
        }

        // Pre-insert queue entries before the LWT so a kill leaves orphans,
        // never a committed row with no entry. EVERY pending promise gets a
        // promise_timeouts entry — eager, like the Go implementation.
        if state == "pending" {
            self.exec(
                "INSERT INTO promise_timeouts (bucket, shard, timeout_at, promise_id, origin) VALUES (?, ?, ?, ?, ?)",
                (
                    self.bucket_for(r.timeout_at),
                    self.shard_for(id),
                    r.timeout_at,
                    id,
                    origin.as_str(),
                ),
            )
            .await?;
            if has_task {
                self.exec(
                    "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
                    (
                        self.bucket_for(task_retry_at),
                        self.shard_for(id),
                        task_retry_at,
                        id,
                        origin.as_str(),
                        r.timeout_at,
                    ),
                )
                .await?;
            }
        }

        // The LWT INSERT — task_timeout_retry included atomically.
        let (task_state, task_version): (Option<&str>, Option<i32>) = if has_task {
            if already_timedout {
                (Some("fulfilled"), Some(0))
            } else {
                (Some("pending"), Some(0))
            }
        } else {
            (None, None)
        };
        let task_retry_arg = if has_task && !already_timedout {
            Some(task_retry_at)
        } else {
            None
        };

        let insert_args: Args = vec![
            text(id),
            text(&origin),
            opt_text(r.tags.get("resonate:branch").cloned()),
            opt_text(r.tags.get("resonate:parent").cloned()),
            opt_text(target.clone()),
            text(state),
            opt_cql_map(r.param.headers.as_ref()),
            opt_text(r.param.data.clone()),
            cql_map(&r.tags),
            big(r.timeout_at),
            big(created_at),
            settled_at.and_then(|v| big(v)),
            opt_text(task_state),
            task_version.and_then(|v| int(v)),
            task_retry_arg.and_then(|v| big(v)),
        ];
        let (applied, row) = self
            .cas(
                "INSERT INTO promises (id, origin, branch, parent, target, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at, callbacks, listeners, task_state, task_version, task_ttl, task_pid, task_resumes, task_timeout_retry) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, null, null, ?, ?, ?, ?, {}, {}, ?, ?, null, null, null, ?) IF NOT EXISTS",
                insert_args,
            )
            .await?;

        if !applied {
            // The failed LWT returns the full existing row.
            let existing = PromiseRow::from_map(&row);

            // Roll back pre-inserts unless the existing row legitimately
            // owns an entry at that PK.
            if state == "pending" {
                if !(existing.state == "pending" && existing.timeout_at == r.timeout_at) {
                    let _ = self
                        .exec(
                            "DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?",
                            (
                                self.bucket_for(r.timeout_at),
                                self.shard_for(id),
                                r.timeout_at,
                                origin.as_str(),
                                id,
                            ),
                        )
                        .await;
                }
                if has_task {
                    let existing_retry = get_big(&row, "task_timeout_retry").unwrap_or(0);
                    if !(existing.task_state.as_deref() == Some("pending")
                        && existing_retry == task_retry_at)
                    {
                        let _ = self
                            .exec(
                                "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                                (
                                    self.bucket_for(task_retry_at),
                                    self.shard_for(id),
                                    task_retry_at,
                                    origin.as_str(),
                                    id,
                                ),
                            )
                            .await;
                    }
                }
            }

            // Eagerly settle the existing row if it has expired.
            let mut existing = existing;
            existing.id = id.to_string();
            existing.origin = origin.clone();
            if existing.state == "pending" && now >= existing.timeout_at {
                self.try_timeout(ctx, &existing, now).await?;
                let settled = self
                    .read_promise(&origin, id)
                    .await?
                    .map(|p| p.to_promise_record());
                if let Some(p) = settled {
                    return Ok(p);
                }
            }
            return Ok(existing.to_promise_record());
        }

        if state == "pending" {
            self.arm_promise_timeout(ctx, id, r.timeout_at, awaitable);
            if has_task {
                self.arm_retry(ctx, id, task_retry_at);
                if task_retry_immediate {
                    ctx.messages.push(Outgoing::Execute {
                        address: target.clone().unwrap(),
                        task_id: id.to_string(),
                        version: 0,
                    });
                }
            }
        }

        Ok(PromiseRecord {
            id: id.to_string(),
            state: crate::db::parse_promise_state(state),
            param: r.param.clone(),
            value: PromiseValue::default(),
            tags: r.tags.clone(),
            timeout_at: r.timeout_at,
            created_at,
            settled_at,
        })
    }

    pub(crate) async fn op_promise_settle(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseSettleData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Promise not found"),
            Ok(Some(row)) => row,
        };
        if row.state != "pending" {
            let promise = row.to_promise_record();
            return ok(req, ctx, &PromiseResponseData { promise });
        }

        let settle_state = r.state.to_string();
        let settled_at_val = now.max(row.created_at);
        let has_task = row.target.is_some();

        // Go's explicit-settle variant of the pinned statement: the caller's
        // value rides in, the IF pins state, the exact callbacks set, and
        // the un-written value columns.
        let stmt = if has_task {
            "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ?, \
             callbacks = {}, listeners = {}, task_state = 'fulfilled', task_pid = null, \
             task_ttl = null, task_timeout_retry = null, task_timeout_lease = null, task_resumes = {} \
             WHERE origin = ? AND id = ? \
             IF state = 'pending' AND callbacks = ? AND settled_at = null AND value_headers = null AND value_data = null"
        } else {
            "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ?, \
             callbacks = {}, listeners = {} \
             WHERE origin = ? AND id = ? \
             IF state = 'pending' AND callbacks = ? AND settled_at = null AND value_headers = null AND value_data = null"
        };
        let args: Args = vec![
            text(&settle_state),
            opt_cql_map(r.value.headers.as_ref()),
            opt_text(r.value.data.clone()),
            big(settled_at_val),
            text(&row.origin),
            text(&row.id),
            cql_set(&row.callbacks),
        ];

        let outcome = match self
            .enqueue_resume(
                &row.id,
                &row.origin,
                &row.callbacks,
                settled_at_val,
                stmt.to_string(),
                args,
            )
            .await
        {
            Ok(o) => o,
            Err(e) => return storage_err(req, e),
        };

        let promise = match outcome {
            SettleOutcome::Won { resumed, retry_at } => {
                let mut settled = row.clone();
                settled.state = settle_state.clone();
                settled.value_headers = r.value.headers.clone().unwrap_or_default();
                settled.value_data = r.value.data.clone();
                settled.settled_at = Some(settled_at_val);
                if let Err(e) = self.after_settle_won(&mut ctx, &settled, resumed, retry_at).await
                {
                    return storage_err(req, e);
                }
                settled.to_promise_record()
            }
            SettleOutcome::Lost {
                state,
                value_headers,
                value_data,
                settled_at,
            } => {
                let mut lost = row.clone();
                lost.state = state;
                lost.value_headers = value_headers;
                lost.value_data = value_data;
                lost.settled_at = settled_at;
                lost.to_promise_record()
            }
            SettleOutcome::Conflict => {
                return err(req, 500, "concurrent modification; please retry")
            }
        };
        ok(req, ctx, &PromiseResponseData { promise })
    }

    pub(crate) async fn op_promise_register_callback(
        &self,
        req: &RequestEnvelope,
        now: i64,
    ) -> Output {
        let r: PromiseRegisterCallbackData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();

        let awaited = match self.read_and_try_timeout(&mut ctx, &r.awaited, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Awaited promise not found"),
            Ok(Some(row)) => row,
        };
        let awaiter = match self.read_and_try_timeout(&mut ctx, &r.awaiter, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 422, "Awaiter promise not found"),
            Ok(Some(row)) => row,
        };
        if awaiter.target.is_none() {
            return err(req, 422, "Awaiter promise has no resonate:target tag");
        }
        if !resonate_core::types::is_awaitable(&awaited.tags) {
            return err(req, 422, "Awaited promise is not awaitable");
        }

        if awaiter.state != "pending" || awaited.state != "pending" {
            if awaited.state != "pending" {
                if let Err(e) = self
                    .resume_callback_awaiter(&mut ctx, &awaited, &awaiter, now)
                    .await
                {
                    return storage_err(req, e);
                }
            }
            let promise = awaited.to_promise_record();
            return ok(req, ctx, &PromiseResponseData { promise });
        }

        // Register via LWT, guarded against a concurrent settle.
        let (applied, _row) = match self
            .cas(
                "UPDATE promises SET callbacks = callbacks + ? WHERE origin = ? AND id = ? IF state = 'pending'",
                (
                    vec![r.awaiter.clone()],
                    awaited.origin.as_str(),
                    awaited.id.as_str(),
                ),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            // Concurrent settle won — re-read and return current state.
            return match self.read_promise(&awaited.origin, &awaited.id).await {
                Err(e) => storage_err(req, e),
                Ok(None) => err(req, 404, "Awaited promise not found"),
                Ok(Some(row)) => {
                    let promise = row.to_promise_record();
                    ok(req, ctx, &PromiseResponseData { promise })
                }
            };
        }
        let promise = awaited.to_promise_record();
        ok(req, ctx, &PromiseResponseData { promise })
    }

    /// Go's `resumeCallbackAwaiter`: the per-awaiter half of the fanout,
    /// for an awaited found already settled at registration time.
    async fn resume_callback_awaiter(
        &self,
        ctx: &mut Ctx,
        awaited: &PromiseRow,
        awaiter: &PromiseRow,
        now: i64,
    ) -> Result<(), resonate_server_dbms::StorageError> {
        let origin = &awaited.origin;
        match awaiter.task_state.as_deref() {
            Some("fulfilled") | None => {}
            Some("suspended") => {
                let retry_at = now + self.task_retry_timeout;
                self.exec(
                    "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(&awaiter.id),
                        retry_at,
                        awaiter.id.as_str(),
                        origin.as_str(),
                        awaiter.timeout_at,
                    ),
                )
                .await?;
                let (applied, _row) = match self
                    .cas(
                        "UPDATE promises SET task_state = 'pending', task_resumes = ?, task_timeout_retry = ? WHERE origin = ? AND id = ? IF task_state = 'suspended'",
                        (
                            vec![awaited.id.clone()],
                            retry_at,
                            origin.as_str(),
                            awaiter.id.as_str(),
                        ),
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = self
                            .exec(
                                "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                                (
                                    self.bucket_for(retry_at),
                                    self.shard_for(&awaiter.id),
                                    retry_at,
                                    origin.as_str(),
                                    awaiter.id.as_str(),
                                ),
                            )
                            .await;
                        return Err(e);
                    }
                };
                if !applied {
                    let _ = self
                        .exec(
                            "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                            (
                                self.bucket_for(retry_at),
                                self.shard_for(&awaiter.id),
                                retry_at,
                                origin.as_str(),
                                awaiter.id.as_str(),
                            ),
                        )
                        .await;
                    return Err(resonate_server_dbms::StorageError::Backend(
                        "concurrent modification".to_string(),
                    ));
                }
                self.arm_retry(ctx, &awaiter.id, retry_at);
                if let Some(target) = &awaiter.target {
                    ctx.messages.push(Outgoing::Execute {
                        address: target.clone(),
                        task_id: awaiter.id.clone(),
                        version: awaiter.task_version,
                    });
                }
            }
            Some(state) => {
                let (applied, _row) = self
                    .cas(
                        &format!(
                            "UPDATE promises SET task_resumes = task_resumes + ? WHERE origin = ? AND id = ? IF task_state = '{state}'"
                        ),
                        (
                            vec![awaited.id.clone()],
                            origin.as_str(),
                            awaiter.id.as_str(),
                        ),
                    )
                    .await?;
                if !applied {
                    return Err(resonate_server_dbms::StorageError::Backend(
                        "concurrent modification".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn op_promise_register_listener(
        &self,
        req: &RequestEnvelope,
        now: i64,
    ) -> Output {
        let r: PromiseRegisterListenerData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        if !resonate_core::is_valid_address(&r.address) {
            return err(req, 400, "Invalid listener address");
        }
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.awaited, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Awaited promise not found"),
            Ok(Some(row)) => row,
        };
        if !resonate_core::types::is_awaitable(&row.tags) {
            return err(req, 422, "Awaited promise is not awaitable");
        }
        if row.state != "pending" {
            let promise = row.to_promise_record();
            return ok(req, ctx, &PromiseResponseData { promise });
        }
        let (applied, _r) = match self
            .cas(
                "UPDATE promises SET listeners = listeners + ? WHERE origin = ? AND id = ? IF state = 'pending'",
                (
                    vec![r.address.clone()],
                    row.origin.as_str(),
                    row.id.as_str(),
                ),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            return match self.read_promise(&row.origin, &row.id).await {
                Err(e) => storage_err(req, e),
                Ok(None) => err(req, 404, "Awaited promise not found"),
                Ok(Some(row)) => {
                    let promise = row.to_promise_record();
                    ok(req, ctx, &PromiseResponseData { promise })
                }
            };
        }
        let promise = row.to_promise_record();
        ok(req, ctx, &PromiseResponseData { promise })
    }

    /// A full scan, filtered and sorted in memory: search is an operator's
    /// read, and the table is partitioned for transitions, not for it.
    pub(crate) async fn op_promise_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let r: PromiseSearchData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let limit = match r.limit {
            Some(n) if n > 1000 => {
                return err(req, 400, "Invalid 'limit' — must be between 1 and 1000")
            }
            Some(n) => n,
            None => 100,
        };
        let rows = match self
            .rows(
                &format!("SELECT {} FROM promises", crate::db::P_COLS),
                (),
            )
            .await
        {
            Ok(rows) => rows,
            Err(e) => return storage_err(req, e),
        };
        let state_filter = r.state.map(|s| s.to_string());
        let mut promises: Vec<PromiseRecord> = rows
            .iter()
            .map(PromiseRow::from_map)
            .filter(|p| {
                state_filter
                    .as_ref()
                    .map(|s| &p.state == s)
                    .unwrap_or(true)
                    && r.tags
                        .as_ref()
                        .map(|ft| {
                            ft.iter()
                                .all(|(k, v)| p.tags.get(k).map(|pv| pv == v).unwrap_or(false))
                        })
                        .unwrap_or(true)
            })
            .map(|p| p.to_promise_record())
            .collect();
        promises.sort_by(|a, b| a.id.cmp(&b.id));
        if let Some(cursor) = r.cursor.as_deref() {
            promises.retain(|p| p.id.as_str() > cursor);
        }
        let has_more = promises.len() as i64 > limit;
        promises.truncate(limit as usize);
        let cursor = if has_more {
            promises.last().map(|p| p.id.clone())
        } else {
            None
        };
        ok(
            req,
            Ctx::default(),
            &PromiseSearchResponseData { promises, cursor },
        )
    }
}
