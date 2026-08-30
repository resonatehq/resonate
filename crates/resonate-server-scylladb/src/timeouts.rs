//! The timeout machinery — Go's `handler_timeouts.go` and the schedule
//! firing, driven by the port instead of goroutines.
//!
//! Every handler is idempotent against stale and orphan entries: the
//! pre-insert protocol only ever leaves an extra entry, never a missing
//! one, and the guard ladders here classify and delete them. `Keep` is the
//! Go "skip cleanup" idiom — the entry stays as the retry anchor.

use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_server_dbms::engine_port::{Outgoing, Output, Scheduled, Timeout};
use resonate_server_dbms::StorageResult;
use scylla::value::CqlValue;
use serde_json::Value;

use crate::db::{get_big, get_text, get_tags, PromiseRow};
use crate::ops_promise::err;
use crate::{origin_of, Ctx, ScyllaEngine, Tags};

enum Cleanup {
    Delete,
    Keep,
}

impl ScyllaEngine {
    pub(crate) async fn process_internal(
        &self,
        timeout: Timeout,
        now: i64,
    ) -> Output {
        let mut ctx = Ctx::default();
        let result = match &timeout {
            Timeout::PromiseTimeout { promise_id } => {
                self.internal_promise_timeout(&mut ctx, promise_id, now).await
            }
            Timeout::TaskRetryTimeout { task_id } => {
                self.internal_retry_timeout(&mut ctx, task_id, now).await
            }
            Timeout::TaskLeaseTimeout { task_id, .. } => {
                self.internal_lease_timeout(&mut ctx, task_id, now).await
            }
            Timeout::ScheduleDue { schedule_id } => self
                .internal_schedule_due(&mut ctx, schedule_id, now)
                .await
                .map(|_| ()),
        };
        if let Err(e) = result {
            tracing::error!(error = %e, "internal timeout failed");
        }
        Output {
            response: None,
            messages: ctx.messages,
            timeouts: ctx.armed,
        }
    }

    /// The narrow form: fire the one named deadline if the durable state
    /// still holds it due. A moved deadline or settled row is a no-op.
    async fn internal_promise_timeout(
        &self,
        ctx: &mut Ctx,
        id: &str,
        now: i64,
    ) -> StorageResult<()> {
        let origin = origin_of(id).to_string();
        if let Some(row) = self.read_promise(&origin, id).await? {
            if row.state == "pending" && row.timeout_at <= now {
                self.on_promise_timeout(ctx, &origin, id, row.timeout_at, now).await?;
            }
        }
        Ok(())
    }

    async fn internal_retry_timeout(&self, ctx: &mut Ctx, id: &str, now: i64) -> StorageResult<()> {
        let origin = origin_of(id).to_string();
        if let Some(row) = self.read_promise(&origin, id).await? {
            if row.task_state.as_deref() == Some("pending") {
                if let Some(retry_at) = row.task_timeout_retry {
                    if retry_at <= now {
                        self.on_task_retry(ctx, &origin, id, retry_at, row.timeout_at, now)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn internal_lease_timeout(&self, ctx: &mut Ctx, id: &str, now: i64) -> StorageResult<()> {
        let origin = origin_of(id).to_string();
        if let Some(row) = self.read_promise(&origin, id).await? {
            if row.task_state.as_deref() == Some("acquired") {
                if let Some(lease_at) = row.task_timeout_lease {
                    if lease_at <= now {
                        self.on_task_lease(ctx, &origin, id, lease_at, row.timeout_at, now)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn internal_schedule_due(
        &self,
        ctx: &mut Ctx,
        schedule_id: &str,
        now: i64,
    ) -> StorageResult<usize> {
        let Some(m) = self.read_schedule(schedule_id).await? else {
            return Ok(0);
        };
        let next_run_at = get_big(&m, "next_run_at").unwrap_or(0);
        if next_run_at > now {
            return Ok(0);
        }
        let token = match m.get("create_token") {
            Some(Some(CqlValue::Uuid(u))) => *u,
            _ => uuid::Uuid::nil(),
        };
        self.on_schedule_due(ctx, schedule_id, next_run_at, token, now)
            .await
    }

    /// One eager settle of an entry the sweep found — Go's `onPromiseTimeout`
    /// with its three stale-entry exits. `now` is the sweep's clock, for the
    /// follow-up deadlines a resumed awaiter gets.
    async fn on_promise_timeout(
        &self,
        ctx: &mut Ctx,
        origin: &str,
        id: &str,
        entry_at: i64,
        now: i64,
    ) -> StorageResult<()> {
        let delete_entry = |bucket: i64, shard: i16| {
            (
                "DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?",
                bucket,
                shard,
            )
        };
        let (cql, bucket, shard) = delete_entry(self.bucket_for(entry_at), self.shard_for(id));
        match self.read_promise(origin, id).await? {
            None => {
                let _ = self
                    .exec(cql, (bucket, shard, entry_at, origin, id))
                    .await;
            }
            Some(row) if row.state != "pending" => {
                let _ = self
                    .exec(cql, (bucket, shard, entry_at, origin, id))
                    .await;
            }
            Some(row) if row.timeout_at != entry_at => {
                // Orphan: the row's deadline is not this entry's.
                let _ = self
                    .exec(cql, (bucket, shard, entry_at, origin, id))
                    .await;
            }
            Some(row) => {
                // EXPLORATORY (decision pending): Go anchors a resumed
                // awaiter's retry at the entry's own deadline (timeoutAt +
                // retry); the relational engines anchor at the sweep's
                // clock. The differential sees the difference in the armed
                // hints (first at ~step 1976). Aligned to the sweep clock so
                // the run can continue cataloguing.
                self.try_timeout(ctx, &row, now).await?;
            }
        }
        Ok(())
    }

    /// Go's `onTaskRetryTimeout`: re-enqueue a pending task for execution.
    async fn on_task_retry(
        &self,
        ctx: &mut Ctx,
        origin: &str,
        id: &str,
        entry_at: i64,
        promise_timeout_at: i64,
        now: i64,
    ) -> StorageResult<()> {
        let cleanup = self
            .task_retry_transition(ctx, origin, id, entry_at, promise_timeout_at, now)
            .await?;
        if let Cleanup::Delete = cleanup {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(entry_at),
                        self.shard_for(id),
                        entry_at,
                        origin,
                        id,
                    ),
                )
                .await;
        }
        Ok(())
    }

    async fn task_retry_transition(
        &self,
        ctx: &mut Ctx,
        origin: &str,
        id: &str,
        entry_at: i64,
        promise_timeout_at: i64,
        now: i64,
    ) -> StorageResult<Cleanup> {
        let Some(row) = self.read_promise(origin, id).await? else {
            // Row gone: logically timed out entries clean up; anything else
            // may be a create's pre-insert mid-flight.
            return Ok(if promise_timeout_at <= now {
                Cleanup::Delete
            } else {
                Cleanup::Keep
            });
        };
        match row.task_state.as_deref() {
            Some("acquired") => {
                // A lease newer than this entry means a re-acquire happened
                // after the entry was written: definitely stale. A lease at
                // or before it may be onTaskLeaseTimeout's pre-insert still
                // in flight — leave it.
                return Ok(match row.task_timeout_lease {
                    Some(lease) if lease > entry_at => Cleanup::Delete,
                    _ => Cleanup::Keep,
                });
            }
            Some("pending") => {}
            _ => return Ok(Cleanup::Delete),
        }
        match row.task_timeout_retry {
            None => return Ok(Cleanup::Delete),
            Some(retry) if retry > entry_at => return Ok(Cleanup::Delete),
            Some(retry) if retry < entry_at => return Ok(Cleanup::Keep),
            _ => {}
        }

        let retry_at = now + self.task_retry_timeout;
        self.exec(
            "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
            (
                self.bucket_for(retry_at),
                self.shard_for(id),
                retry_at,
                id,
                origin,
                row.timeout_at,
            ),
        )
        .await?;
        let (applied, _lwt) = self
            .cas(
                "UPDATE promises SET task_timeout_retry = ? WHERE origin = ? AND id = ? IF task_state = 'pending' AND task_timeout_retry = ?",
                (retry_at, origin, id, entry_at),
            )
            .await?;
        if !applied {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(id),
                        retry_at,
                        origin,
                        id,
                    ),
                )
                .await;
            return Ok(Cleanup::Delete);
        }
        self.arm_retry(ctx, id, retry_at);
        if let Some(target) = &row.target {
            ctx.messages.push(Outgoing::Execute {
                address: target.clone(),
                task_id: id.to_string(),
                version: row.task_version,
            });
        }
        Ok(Cleanup::Delete)
    }

    /// Go's `onTaskLeaseTimeout`: hand an expired lease back to the retry
    /// queue, version unchanged.
    async fn on_task_lease(
        &self,
        ctx: &mut Ctx,
        origin: &str,
        id: &str,
        entry_at: i64,
        promise_timeout_at: i64,
        now: i64,
    ) -> StorageResult<()> {
        let cleanup = self
            .task_lease_transition(ctx, origin, id, entry_at, promise_timeout_at, now)
            .await?;
        if let Cleanup::Delete = cleanup {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(entry_at),
                        self.shard_for(id),
                        entry_at,
                        origin,
                        id,
                    ),
                )
                .await;
        }
        Ok(())
    }

    async fn task_lease_transition(
        &self,
        ctx: &mut Ctx,
        origin: &str,
        id: &str,
        entry_at: i64,
        promise_timeout_at: i64,
        now: i64,
    ) -> StorageResult<Cleanup> {
        let Some(row) = self.read_promise(origin, id).await? else {
            return Ok(if promise_timeout_at <= now {
                Cleanup::Delete
            } else {
                Cleanup::Keep
            });
        };
        if row.task_state.as_deref() != Some("acquired") {
            return Ok(Cleanup::Delete);
        }
        if row.task_timeout_lease != Some(entry_at) {
            return Ok(Cleanup::Delete);
        }

        let retry_at = now + self.task_retry_timeout;
        self.exec(
            "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
            (
                self.bucket_for(retry_at),
                self.shard_for(id),
                retry_at,
                id,
                origin,
                row.timeout_at,
            ),
        )
        .await?;
        // Pin the lease to defeat a release-then-reacquire ABA between the
        // read and this statement.
        let (applied, _lwt) = self
            .cas(
                "UPDATE promises SET task_state = 'pending', task_pid = null, task_ttl = null, task_timeout_retry = ?, task_timeout_lease = null WHERE origin = ? AND id = ? IF task_state = 'acquired' AND task_timeout_lease = ?",
                (retry_at, origin, id, entry_at),
            )
            .await?;
        if !applied {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(id),
                        retry_at,
                        origin,
                        id,
                    ),
                )
                .await;
            return Ok(Cleanup::Delete);
        }
        self.arm_retry(ctx, id, retry_at);
        if let Some(target) = &row.target {
            ctx.messages.push(Outgoing::Execute {
                address: target.clone(),
                task_id: id.to_string(),
                version: row.task_version,
            });
        }
        Ok(Cleanup::Delete)
    }

    /// One schedule firing: walk the cron forward creating every due
    /// occurrence, then advance the row pinned on the fired instant, and
    /// re-anchor the queue with the schedule's own token.
    async fn on_schedule_due(
        &self,
        ctx: &mut Ctx,
        schedule_id: &str,
        entry_at: i64,
        token: uuid::Uuid,
        now: i64,
    ) -> StorageResult<usize> {
        let origin = schedule_id.to_string();
        let Some(m) = self.read_schedule(schedule_id).await? else {
            // A create's pre-insert may still be in flight — leave the entry.
            return Ok(0);
        };
        let next_run_at = get_big(&m, "next_run_at").unwrap_or(0);
        let delete_entry = |at: i64, tok: uuid::Uuid| {
            (
                self.bucket_for(at),
                self.shard_for(schedule_id),
                at,
                origin.clone(),
                schedule_id.to_string(),
                CqlValue::Uuid(tok),
            )
        };
        if entry_at < next_run_at {
            let args = delete_entry(entry_at, token);
            let _ = self
                .exec(
                    "DELETE FROM schedule_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?",
                    args,
                )
                .await;
            return Ok(0);
        }
        if entry_at > next_run_at {
            return Ok(0);
        }

        let cron = get_text(&m, "cron").unwrap_or_default();
        let template = get_text(&m, "promise_id").unwrap_or_default();
        let promise_timeout = get_big(&m, "promise_timeout").unwrap_or(0);
        let param_headers = get_tags(&m, "promise_param_headers");
        let param_data = get_text(&m, "promise_param_data");
        let promise_tags = get_tags(&m, "promise_tags");
        let row_token = match m.get("create_token") {
            Some(Some(CqlValue::Uuid(u))) => *u,
            _ => token,
        };

        let mut fired = 0usize;
        let mut current = entry_at;
        let mut last_run_at = None;
        while current <= now {
            let promise_id = template
                .replace("{{.id}}", schedule_id)
                .replace("{{.timestamp}}", &current.to_string());
            let mut tags = promise_tags.clone();
            tags.insert("resonate:schedule".to_string(), schedule_id.to_string());
            tags.insert("resonate:origin".to_string(), promise_id.clone());
            tags.insert("resonate:branch".to_string(), promise_id.clone());
            tags.insert("resonate:parent".to_string(), promise_id.clone());
            tags.insert("resonate:prefix".to_string(), promise_id.clone());
            self.create_schedule_promise(
                ctx,
                &promise_id,
                &tags,
                &param_headers,
                param_data.as_deref(),
                current,
                current + promise_timeout,
                now,
            )
            .await?;
            fired += 1;
            last_run_at = Some(current);
            current = resonate_core::util::compute_next_cron(&cron, current);
        }
        let final_next = current;

        // Re-anchor with the schedule's own token, then advance the row.
        self.exec(
            "INSERT INTO schedule_timeouts (bucket, shard, timeout_at, origin, schedule_id, create_token) VALUES (?, ?, ?, ?, ?, ?)",
            (
                self.bucket_for(final_next),
                self.shard_for(schedule_id),
                final_next,
                origin.as_str(),
                schedule_id,
                CqlValue::Uuid(row_token),
            ),
        )
        .await?;
        let (applied, _lwt) = self
            .cas(
                "UPDATE schedules SET next_run_at = ?, last_run_at = ? WHERE origin = ? AND id = ? IF next_run_at = ?",
                (
                    final_next,
                    last_run_at,
                    origin.as_str(),
                    schedule_id,
                    entry_at,
                ),
            )
            .await?;
        // Applied or lost to a concurrent advance: either way the fired
        // entry is consumed. The promises above are IF NOT EXISTS, so a
        // duplicate firing re-created them as no-ops.
        let _ = applied;
        let args = delete_entry(entry_at, token);
        let _ = self
            .exec(
                "DELETE FROM schedule_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?",
                args,
            )
            .await;
        ctx.armed.push(Scheduled {
            at: final_next,
            timeout: Timeout::ScheduleDue {
                schedule_id: schedule_id.to_string(),
            },
        });
        Ok(fired)
    }

    /// A schedule occurrence's promise — its own call-graph root in its own
    /// partition, with created/timeout derived from the cron instant so
    /// catch-up is deterministic.
    #[allow(clippy::too_many_arguments)]
    async fn create_schedule_promise(
        &self,
        ctx: &mut Ctx,
        promise_id: &str,
        tags: &Tags,
        param_headers: &Tags,
        param_data: Option<&str>,
        fired_at: i64,
        timeout_at: i64,
        now: i64,
    ) -> StorageResult<()> {
        let origin = promise_id.to_string();
        let target = tags.get("resonate:target").cloned();
        let is_timer = tags.get("resonate:timer").map(String::as_str) == Some("true");
        let already_timedout = now >= timeout_at;
        let (state, settled_at) = if already_timedout {
            (
                if is_timer { "resolved" } else { "rejected_timedout" },
                Some(timeout_at),
            )
        } else {
            ("pending", None)
        };
        let retry_at = now + self.task_retry_timeout;

        if !already_timedout {
            self.exec(
                "INSERT INTO promise_timeouts (bucket, shard, timeout_at, promise_id, origin) VALUES (?, ?, ?, ?, ?)",
                (
                    self.bucket_for(timeout_at),
                    self.shard_for(promise_id),
                    timeout_at,
                    promise_id,
                    origin.as_str(),
                ),
            )
            .await?;
            if target.is_some() {
                self.exec(
                    "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(promise_id),
                        retry_at,
                        promise_id,
                        origin.as_str(),
                        timeout_at,
                    ),
                )
                .await?;
            }
        }

        let (task_state, task_version): (Option<&str>, Option<i32>) = if target.is_some() {
            if already_timedout {
                (Some("fulfilled"), Some(0))
            } else {
                (Some("pending"), Some(0))
            }
        } else {
            (None, None)
        };
        let args: crate::db::Args = vec![
            crate::db::text(promise_id),
            crate::db::text(&origin),
            crate::db::opt_text(target.clone()),
            crate::db::text(state),
            crate::db::opt_cql_map(if param_headers.is_empty() {
                None
            } else {
                Some(param_headers)
            }),
            crate::db::opt_text(param_data.map(|s| s.to_string())),
            crate::db::cql_map(tags),
            crate::db::big(timeout_at),
            crate::db::big(fired_at),
            settled_at.and_then(crate::db::big),
            crate::db::opt_text(task_state),
            task_version.and_then(crate::db::int),
            if target.is_some() && !already_timedout {
                crate::db::big(retry_at)
            } else {
                None
            },
        ];
        let (applied, lwt) = self
            .cas(
                "INSERT INTO promises (id, origin, branch, parent, target, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at, callbacks, listeners, task_state, task_version, task_ttl, task_pid, task_resumes, task_timeout_retry) \
                 VALUES (?, ?, null, null, ?, ?, ?, ?, null, null, ?, ?, ?, ?, {}, {}, ?, ?, null, null, null, ?) IF NOT EXISTS",
                args,
            )
            .await?;

        if !applied {
            let existing = PromiseRow::from_map(&lwt);
            if !already_timedout {
                if !(existing.state == "pending" && existing.timeout_at == timeout_at) {
                    let _ = self
                        .exec(
                            "DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?",
                            (
                                self.bucket_for(timeout_at),
                                self.shard_for(promise_id),
                                timeout_at,
                                origin.as_str(),
                                promise_id,
                            ),
                        )
                        .await;
                }
                if target.is_some() {
                    let existing_retry = existing.task_timeout_retry.unwrap_or(0);
                    if !(existing.task_state.as_deref() == Some("pending")
                        && existing_retry == retry_at)
                    {
                        let _ = self
                            .exec(
                                "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                                (
                                    self.bucket_for(retry_at),
                                    self.shard_for(promise_id),
                                    retry_at,
                                    origin.as_str(),
                                    promise_id,
                                ),
                            )
                            .await;
                    }
                }
            }
            return Ok(());
        }

        if !already_timedout {
            self.arm_promise_timeout(
                ctx,
                promise_id,
                timeout_at,
                resonate_core::types::is_awaitable(tags),
            );
            if let Some(addr) = &target {
                self.arm_retry(ctx, promise_id, retry_at);
                ctx.messages.push(Outgoing::Execute {
                    address: addr.clone(),
                    task_id: promise_id.to_string(),
                    version: 0,
                });
            }
        }
        Ok(())
    }

    /// The bulk sweep: every due entry in every shard's due buckets, all
    /// three queues, deterministically ordered.
    pub(crate) async fn tick_impl(
        &self,
        now: i64,
    ) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        let mut ctx = Ctx::default();
        let fired = self.sweep(&mut ctx, now, false).await?;
        Ok((fired, ctx.messages, ctx.armed))
    }

    /// One sweep at `t`. `filtering` selects the debug path: full-table
    /// ALLOW FILTERING scans so arbitrary test timestamps do not walk
    /// millions of buckets. Both paths collect-then-sort, because rows come
    /// back in token order and the seed contract needs determinism.
    pub(crate) async fn sweep(
        &self,
        ctx: &mut Ctx,
        t: i64,
        filtering: bool,
    ) -> StorageResult<usize> {
        // promise_timeouts
        let mut promise_entries: Vec<(i64, String, String)> = Vec::new();
        for m in self.scan_queue("promise_timeouts", "timeout_at, origin, promise_id", t, filtering).await? {
            promise_entries.push((
                get_big(&m, "timeout_at").unwrap_or(0),
                get_text(&m, "origin").unwrap_or_default(),
                get_text(&m, "promise_id").unwrap_or_default(),
            ));
        }
        promise_entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.2.cmp(&b.2)));
        for (at, origin, id) in &promise_entries {
            let origin = if origin.is_empty() { id.clone() } else { origin.clone() };
            self.on_promise_timeout(ctx, &origin, id, *at, t).await?;
        }

        // task_timeouts
        let mut task_entries: Vec<(i64, i8, String, String, i64)> = Vec::new();
        for m in self
            .scan_queue(
                "task_timeouts",
                "timeout_at, timeout_type, origin, task_id, promise_timeout_at",
                t,
                filtering,
            )
            .await?
        {
            let ttype = match m.get("timeout_type") {
                Some(Some(CqlValue::TinyInt(v))) => *v,
                _ => 0,
            };
            task_entries.push((
                get_big(&m, "timeout_at").unwrap_or(0),
                ttype,
                get_text(&m, "origin").unwrap_or_default(),
                get_text(&m, "task_id").unwrap_or_default(),
                get_big(&m, "promise_timeout_at").unwrap_or(0),
            ));
        }
        task_entries.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.3.cmp(&b.3))
        });
        for (at, ttype, origin, id, p_timeout) in &task_entries {
            let origin = if origin.is_empty() { id.clone() } else { origin.clone() };
            if *ttype == 0 {
                self.on_task_retry(ctx, &origin, id, *at, *p_timeout, t).await?;
            } else {
                self.on_task_lease(ctx, &origin, id, *at, *p_timeout, t).await?;
            }
        }

        // schedule_timeouts
        let mut schedule_entries: Vec<(i64, String, String, uuid::Uuid)> = Vec::new();
        for m in self
            .scan_queue(
                "schedule_timeouts",
                "timeout_at, origin, schedule_id, create_token",
                t,
                filtering,
            )
            .await?
        {
            let token = match m.get("create_token") {
                Some(Some(CqlValue::Uuid(u))) => *u,
                _ => uuid::Uuid::nil(),
            };
            schedule_entries.push((
                get_big(&m, "timeout_at").unwrap_or(0),
                get_text(&m, "origin").unwrap_or_default(),
                get_text(&m, "schedule_id").unwrap_or_default(),
                token,
            ));
        }
        schedule_entries.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.3.cmp(&b.3))
        });
        let mut fired = 0usize;
        for (at, origin, id, token) in &schedule_entries {
            let _origin = if origin.is_empty() { id.clone() } else { origin.clone() };
            fired += self.on_schedule_due(ctx, id, *at, *token, t).await?;
        }
        Ok(fired)
    }

    async fn scan_queue(
        &self,
        table: &str,
        cols: &str,
        t: i64,
        filtering: bool,
    ) -> StorageResult<Vec<crate::db::RowMap>> {
        let mut out = Vec::new();
        if filtering {
            out.extend(
                self.rows(
                    &format!("SELECT {cols} FROM {table} WHERE timeout_at <= ? ALLOW FILTERING"),
                    (t,),
                )
                .await?,
            );
        } else {
            for shard in 0..self.shards {
                for bucket in self.buckets_to_scan(t) {
                    out.extend(
                        self.rows(
                            &format!(
                                "SELECT {cols} FROM {table} WHERE bucket = ? AND shard = ? AND timeout_at <= ?"
                            ),
                            (bucket, shard, t),
                        )
                        .await?,
                    );
                }
            }
        }
        Ok(out)
    }

    /// The nearest deadlines the durable state holds — projected from the
    /// promises and schedules tables with the same liveness tests every
    /// engine's `upcoming` applies, so the hint is the protocol's view, not
    /// the queue tables' physical one.
    pub(crate) async fn upcoming_impl(&self, limit: usize) -> StorageResult<Vec<Scheduled>> {
        let mut out: Vec<Scheduled> = Vec::new();
        let rows = self
            .rows(
                "SELECT id, state, tags, timeout_at, task_state, task_timeout_retry, task_timeout_lease, task_pid FROM promises",
                (),
            )
            .await?;
        for m in &rows {
            let id = get_text(m, "id").unwrap_or_default();
            let state = get_text(m, "state").unwrap_or_default();
            let tags = get_tags(m, "tags");
            if state == "pending" && resonate_core::types::is_awaitable(&tags) {
                if let Some(at) = get_big(m, "timeout_at") {
                    out.push(Scheduled {
                        at,
                        timeout: Timeout::PromiseTimeout {
                            promise_id: id.clone(),
                        },
                    });
                }
            }
            match get_text(m, "task_state").as_deref() {
                Some("pending") => {
                    if let Some(at) = get_big(m, "task_timeout_retry") {
                        out.push(Scheduled {
                            at,
                            timeout: Timeout::TaskRetryTimeout {
                                task_id: id.clone(),
                            },
                        });
                    }
                }
                Some("acquired") => {
                    if let Some(at) = get_big(m, "task_timeout_lease") {
                        out.push(Scheduled {
                            at,
                            timeout: Timeout::TaskLeaseTimeout {
                                task_id: id.clone(),
                                pid: get_text(m, "task_pid").unwrap_or_default(),
                            },
                        });
                    }
                }
                _ => {}
            }
        }
        let schedules = self
            .rows("SELECT id, next_run_at FROM schedules", ())
            .await?;
        for m in &schedules {
            if let (Some(id), Some(at)) = (get_text(m, "id"), get_big(m, "next_run_at")) {
                out.push(Scheduled {
                    at,
                    timeout: Timeout::ScheduleDue { schedule_id: id },
                });
            }
        }
        out.sort_by(|a, b| {
            a.at.cmp(&b.at)
                .then_with(|| a.timeout.id().cmp(b.timeout.id()))
        });
        out.truncate(limit);
        Ok(out)
    }

    pub(crate) async fn op_debug_tick(&self, req: &RequestEnvelope) -> Output {
        let time = match req.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => return err(req, 400, "Missing or invalid 'time' field"),
        };
        if let Some(debug_time) = req.head.debug_time {
            if debug_time != time {
                return err(req, 400, "resonate:debug_time must equal data.time");
            }
        }
        let mut ctx = Ctx::default();
        if let Err(e) = self.sweep(&mut ctx, time, true).await {
            return crate::ops_promise::storage_err(req, e);
        }
        Output {
            response: Some(ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                Value::Array(vec![]),
            )),
            messages: ctx.messages,
            timeouts: ctx.armed,
        }
    }
}
