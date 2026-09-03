//! The task operations — Go's `handler_task.go`, translated.
//!
//! Mechanics are Go's: pre-insert → LWT → conditional rollback, version
//! pinned in every IF, acquire the only bump. Statuses and message strings
//! follow this repo's protocol so a divergence the differential reports is
//! a behavioral one, not a spelling one.

use crate::engine::{Outgoing, Output};
use resonate_core::types::{
    PromiseCreateData, PromiseRecord, PromiseSettleData, RequestEnvelope, TaskAcquireData,
    TaskAcquireResponseData, TaskContinueData, TaskCreateData, TaskCreateResponseData,
    TaskFenceData, TaskFulfillData, TaskFulfillResponseData, TaskGetData, TaskHaltData,
    TaskHeartbeatData, TaskRecord, TaskReleaseData, TaskResponseData, TaskSearchData,
    TaskSearchResponseData, TaskSuspendData, TaskSuspendPreloadData, PROTOCOL_VERSION,
};
use serde_json::json;
use validator::Validate;

use crate::db::{big, cql_set, get_text, int, opt_text, text, Args, PromiseRow, SettleOutcome};
use crate::ops_promise::{err, ok, parse, storage_err};
use crate::{Ctx, ScyllaEngine};

impl ScyllaEngine {
    /// Branch siblings, capped — what a task response preloads. Branch
    /// members share the origin partition, so this is a partition scan.
    pub(crate) async fn preload(
        &self,
        origin: &str,
        promise_id: &str,
    ) -> Result<Vec<PromiseRecord>, crate::StorageError> {
        let rows = self
            .rows(
                &format!(
                    "SELECT {} FROM promises WHERE origin = ?",
                    crate::db::P_COLS
                ),
                (origin,),
            )
            .await?;
        let all: Vec<PromiseRow> = rows.iter().map(PromiseRow::from_map).collect();
        let branch = match all
            .iter()
            .find(|p| p.id == promise_id)
            .and_then(|p| p.tags.get("resonate:branch"))
        {
            Some(b) if !b.is_empty() => b.clone(),
            _ => return Ok(vec![]),
        };
        let mut siblings: Vec<&PromiseRow> = all
            .iter()
            .filter(|p| {
                p.id != promise_id
                    && p.tags
                        .get("resonate:branch")
                        .map(|b| b == &branch)
                        .unwrap_or(false)
            })
            .collect();
        siblings.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(siblings
            .into_iter()
            .take(self.preload_limit as usize)
            .map(|p| p.to_promise_record())
            .collect())
    }

    pub(crate) async fn op_task_get(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskGetData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => storage_err(req, e),
            Ok(None) => err(req, 404, "Task not found"),
            Ok(Some(row)) => match row.to_task_record() {
                None => err(req, 404, "Task not found"),
                Some(task) => ok(req, ctx, &TaskResponseData { task }),
            },
        }
    }

    pub(crate) async fn op_task_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskCreateData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let action = &r.action.data;
        if let Some(addr) = action.tags.get("resonate:target") {
            if !resonate_core::is_valid_address(addr) {
                return err(req, 400, "Invalid resonate:target address");
            }
        }
        let id = action.id.clone();
        let origin = crate::origin_of(&id).to_string();
        let mut ctx = Ctx::default();

        let existing = match self.read_and_try_timeout(&mut ctx, &id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(v) => v,
        };

        if let Some(row) = existing {
            if row.task_state.is_none() {
                return err(req, 422, "The promise does not have a resonate:target tag");
            }
            match row.task_state.as_deref() {
                Some("pending") => {
                    // Re-acquire: pre-insert the lease, bump the version.
                    let lease_at = now + r.ttl;
                    if let Err(e) = self
                        .exec(
                            "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)",
                            (
                                self.bucket_for(lease_at),
                                self.shard_for(&id),
                                lease_at,
                                id.as_str(),
                                origin.as_str(),
                                row.timeout_at,
                            ),
                        )
                        .await
                    {
                        return storage_err(req, e);
                    }
                    let new_version = row.task_version + 1;
                    let (applied, lwt) = match self
                        .cas(
                            "UPDATE promises SET task_state = 'acquired', task_pid = ?, task_ttl = ?, task_version = ?, task_resumes = {}, task_timeout_retry = null, task_timeout_lease = ? WHERE origin = ? AND id = ? IF task_state = 'pending' AND task_version = ?",
                            (
                                r.pid.as_str(),
                                r.ttl,
                                new_version as i32,
                                lease_at,
                                origin.as_str(),
                                id.as_str(),
                                row.task_version as i32,
                            ),
                        )
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return storage_err(req, e),
                    };
                    if !applied {
                        if crate::db::get_big(&lwt, "task_timeout_lease") != Some(lease_at) {
                            let _ = self
                                .exec(
                                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                                    (
                                        self.bucket_for(lease_at),
                                        self.shard_for(&id),
                                        lease_at,
                                        origin.as_str(),
                                        id.as_str(),
                                    ),
                                )
                                .await;
                        }
                        return err(req, 500, "Concurrent modification; please retry");
                    }
                    if let Some(old_retry) = row.task_timeout_retry {
                        let _ = self
                            .exec(
                                "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                                (
                                    self.bucket_for(old_retry),
                                    self.shard_for(&id),
                                    old_retry,
                                    origin.as_str(),
                                    id.as_str(),
                                ),
                            )
                            .await;
                    }
                    self.arm_lease(&mut ctx, &id, &r.pid, lease_at);
                    let task = TaskRecord {
                        id: id.clone(),
                        state: crate::db::parse_task_state("acquired"),
                        version: new_version,
                        resumes: 0,
                        ttl: Some(r.ttl),
                        pid: Some(r.pid.clone()),
                    };
                    let promise = row.to_promise_record();
                    let preload = match self.preload(&origin, &id).await {
                        Ok(p) => p,
                        Err(e) => return storage_err(req, e),
                    };
                    return ok(
                        req,
                        ctx,
                        &TaskCreateResponseData {
                            task,
                            promise,
                            preload,
                        },
                    );
                }
                Some("fulfilled") => {
                    let task = row.to_task_record().unwrap();
                    let promise = row.to_promise_record();
                    let preload = match self.preload(&origin, &id).await {
                        Ok(p) => p,
                        Err(e) => return storage_err(req, e),
                    };
                    return ok(
                        req,
                        ctx,
                        &TaskCreateResponseData {
                            task,
                            promise,
                            preload,
                        },
                    );
                }
                _ => return err(req, 409, "Already exists"),
            }
        }

        // Fresh create: promise + acquired task in one LWT INSERT.
        let already_timedout = now >= action.timeout_at;
        let is_timer = action.tags.get("resonate:timer").map(String::as_str) == Some("true");
        let (p_state, created_at, settled_at) = if already_timedout {
            let s = if is_timer {
                "resolved"
            } else {
                "rejected_timedout"
            };
            (s, action.timeout_at, Some(action.timeout_at))
        } else {
            ("pending", now, None)
        };
        let lease_at = now + r.ttl;

        if !already_timedout {
            if let Err(e) = self
                .exec(
                    "INSERT INTO promise_timeouts (bucket, shard, timeout_at, promise_id, origin) VALUES (?, ?, ?, ?, ?)",
                    (
                        self.bucket_for(action.timeout_at),
                        self.shard_for(&id),
                        action.timeout_at,
                        id.as_str(),
                        origin.as_str(),
                    ),
                )
                .await
            {
                return storage_err(req, e);
            }
            if let Err(e) = self
                .exec(
                    "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)",
                    (
                        self.bucket_for(lease_at),
                        self.shard_for(&id),
                        lease_at,
                        id.as_str(),
                        origin.as_str(),
                        action.timeout_at,
                    ),
                )
                .await
            {
                return storage_err(req, e);
            }
        }

        let (t_state, t_version): (&str, i32) = if already_timedout {
            ("fulfilled", 0)
        } else {
            ("acquired", 1)
        };
        let insert_args: Args = vec![
            text(&id),
            text(&origin),
            opt_text(action.tags.get("resonate:branch").cloned()),
            opt_text(action.tags.get("resonate:parent").cloned()),
            opt_text(action.tags.get("resonate:target").cloned()),
            text(p_state),
            crate::db::opt_cql_map(action.param.headers.as_ref()),
            opt_text(action.param.data.clone()),
            crate::db::cql_map(&action.tags),
            big(action.timeout_at),
            big(created_at),
            settled_at.and_then(big),
            text(t_state),
            int(t_version),
            if already_timedout { None } else { big(r.ttl) },
            if already_timedout { None } else { text(&r.pid) },
            if already_timedout {
                None
            } else {
                big(lease_at)
            },
        ];
        let (applied, lwt) = match self
            .cas(
                "INSERT INTO promises (id, origin, branch, parent, target, state, param_headers, param_data, value_headers, value_data, tags, timeout_at, created_at, settled_at, callbacks, listeners, task_state, task_version, task_ttl, task_pid, task_resumes, task_timeout_retry, task_timeout_lease) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, null, null, ?, ?, ?, ?, {}, {}, ?, ?, ?, ?, null, null, ?) IF NOT EXISTS",
                insert_args,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };

        if !applied {
            // A concurrent create won between the ghost read and the LWT.
            let existing = PromiseRow::from_map(&lwt);
            if !already_timedout {
                if !(existing.state == "pending" && existing.timeout_at == action.timeout_at) {
                    let _ = self
                        .exec(
                            "DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?",
                            (
                                self.bucket_for(action.timeout_at),
                                self.shard_for(&id),
                                action.timeout_at,
                                origin.as_str(),
                                id.as_str(),
                            ),
                        )
                        .await;
                }
                if !(existing.task_state.as_deref() == Some("acquired")
                    && existing.task_timeout_lease == Some(lease_at))
                {
                    let _ = self
                        .exec(
                            "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                            (
                                self.bucket_for(lease_at),
                                self.shard_for(&id),
                                lease_at,
                                origin.as_str(),
                                id.as_str(),
                            ),
                        )
                        .await;
                }
            }
            return err(req, 500, "Concurrent modification; please retry");
        }

        if !already_timedout {
            self.arm_promise_timeout(
                &mut ctx,
                &id,
                action.timeout_at,
                resonate_core::types::is_external(&action.tags),
            );
            self.arm_lease(&mut ctx, &id, &r.pid, lease_at);
            // No execute: task.create returns an already-acquired task to
            // the caller, who IS the worker.
        }

        let promise = PromiseRecord {
            id: id.clone(),
            state: crate::db::parse_promise_state(p_state),
            param: action.param.clone(),
            value: Default::default(),
            tags: action.tags.clone(),
            timeout_at: action.timeout_at,
            created_at,
            settled_at,
        };
        let task = TaskRecord {
            id: id.clone(),
            state: crate::db::parse_task_state(t_state),
            version: t_version as i64,
            resumes: 0,
            ttl: if already_timedout { None } else { Some(r.ttl) },
            pid: if already_timedout {
                None
            } else {
                Some(r.pid.clone())
            },
        };
        let preload = match self.preload(&origin, &id).await {
            Ok(p) => p,
            Err(e) => return storage_err(req, e),
        };
        ok(
            req,
            ctx,
            &TaskCreateResponseData {
                task,
                promise,
                preload,
            },
        )
    }

    pub(crate) async fn op_task_acquire(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskAcquireData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        if row.task_state.is_none() {
            return err(req, 404, "Task not found");
        }
        if row.task_state.as_deref() != Some("pending") {
            return err(req, 409, "Task is not pending");
        }
        if row.task_version != r.version {
            return err(req, 409, "Version mismatch");
        }

        let lease_at = now + r.ttl;
        if let Err(e) = self
            .exec(
                "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)",
                (
                    self.bucket_for(lease_at),
                    self.shard_for(&r.id),
                    lease_at,
                    r.id.as_str(),
                    row.origin.as_str(),
                    row.timeout_at,
                ),
            )
            .await
        {
            return storage_err(req, e);
        }
        let new_version = r.version + 1;
        let (applied, lwt) = match self
            .cas(
                "UPDATE promises SET task_state = 'acquired', task_version = ?, task_pid = ?, task_ttl = ?, task_resumes = {}, task_timeout_retry = null, task_timeout_lease = ? WHERE origin = ? AND id = ? IF task_state = 'pending' AND task_version = ?",
                (
                    new_version as i32,
                    r.pid.as_str(),
                    r.ttl,
                    lease_at,
                    row.origin.as_str(),
                    r.id.as_str(),
                    r.version as i32,
                ),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            let survivor_owns = get_text(&lwt, "task_state").as_deref() == Some("acquired")
                && crate::db::get_big(&lwt, "task_timeout_lease") == Some(lease_at);
            if !survivor_owns {
                let _ = self
                    .exec(
                        "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                        (
                            self.bucket_for(lease_at),
                            self.shard_for(&r.id),
                            lease_at,
                            row.origin.as_str(),
                            r.id.as_str(),
                        ),
                    )
                    .await;
            }
            let still_pending = get_text(&lwt, "task_state").as_deref() == Some("pending");
            if still_pending {
                return err(req, 409, "Version mismatch");
            }
            return err(req, 409, "Task is not pending");
        }
        if let Some(old_retry) = row.task_timeout_retry {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(old_retry),
                        self.shard_for(&r.id),
                        old_retry,
                        row.origin.as_str(),
                        r.id.as_str(),
                    ),
                )
                .await;
        }
        self.arm_lease(&mut ctx, &r.id, &r.pid, lease_at);
        let task = TaskRecord {
            id: r.id.clone(),
            state: crate::db::parse_task_state("acquired"),
            version: new_version,
            resumes: 0,
            ttl: Some(r.ttl),
            pid: Some(r.pid.clone()),
        };
        let promise = row.to_promise_record();
        let preload = match self.preload(&row.origin, &r.id).await {
            Ok(p) => p,
            Err(e) => return storage_err(req, e),
        };
        ok(
            req,
            ctx,
            &TaskAcquireResponseData {
                task,
                promise,
                preload,
            },
        )
    }

    pub(crate) async fn op_task_release(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskReleaseData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        if row.task_state.is_none() {
            return err(req, 404, "Task not found");
        }
        if row.task_state.as_deref() != Some("acquired") || row.task_version != r.version {
            return err(req, 409, "Task version mismatch or invalid state");
        }

        let retry_at = now + self.task_retry_timeout;
        if let Err(e) = self
            .exec(
                "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
                (
                    self.bucket_for(retry_at),
                    self.shard_for(&r.id),
                    retry_at,
                    r.id.as_str(),
                    row.origin.as_str(),
                    row.timeout_at,
                ),
            )
            .await
        {
            return storage_err(req, e);
        }
        let (applied, _lwt) = match self
            .cas(
                "UPDATE promises SET task_state = 'pending', task_pid = null, task_ttl = null, task_timeout_retry = ?, task_timeout_lease = null WHERE origin = ? AND id = ? IF task_state = 'acquired' AND task_version = ?",
                (
                    retry_at,
                    row.origin.as_str(),
                    r.id.as_str(),
                    r.version as i32,
                ),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(retry_at),
                        self.shard_for(&r.id),
                        retry_at,
                        row.origin.as_str(),
                        r.id.as_str(),
                    ),
                )
                .await;
            return err(req, 409, "Task version mismatch or invalid state");
        }
        if let Some(old_lease) = row.task_timeout_lease {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(old_lease),
                        self.shard_for(&r.id),
                        old_lease,
                        row.origin.as_str(),
                        r.id.as_str(),
                    ),
                )
                .await;
        }
        self.arm_retry(&mut ctx, &r.id, retry_at);
        if let Some(target) = &row.target {
            ctx.messages.push(Outgoing::Execute {
                address: target.clone(),
                task_id: r.id.clone(),
                version: r.version,
            });
        }
        ok(req, ctx, &json!({}))
    }

    pub(crate) async fn op_task_fulfill(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskFulfillData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let action = &r.action.data;
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &action.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        if row.task_state.is_none() {
            return err(req, 404, "Task not found");
        }
        if row.task_state.as_deref() != Some("acquired") || row.task_version != r.version {
            return err(req, 409, "Task version mismatch or invalid state");
        }

        let settle_state = action.state.to_string();
        let settled_at_val = now;
        // The fence folded into the settle: state, task state AND version
        // pinned together, plus the un-written value columns so a failed
        // batch names the winner.
        let stmt = "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ?, \
             callbacks = {}, listeners = {}, task_state = 'fulfilled', task_pid = null, \
             task_ttl = null, task_timeout_retry = null, task_timeout_lease = null, task_resumes = {} \
             WHERE origin = ? AND id = ? \
             IF state = 'pending' AND task_state = 'acquired' AND task_version = ? AND callbacks = ? \
             AND settled_at = null AND value_headers = null AND value_data = null";
        let args: Args = vec![
            text(&settle_state),
            crate::db::opt_cql_map(action.value.headers.as_ref()),
            opt_text(action.value.data.clone()),
            big(settled_at_val),
            text(&row.origin),
            text(&row.id),
            int(r.version as i32),
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
        match outcome {
            SettleOutcome::Won { resumed, retry_at } => {
                let mut settled = row.clone();
                settled.state = settle_state;
                settled.value_headers = action.value.headers.clone().unwrap_or_default();
                settled.value_data = action.value.data.clone();
                settled.settled_at = Some(settled_at_val);
                if let Err(e) = self
                    .after_settle_won(&mut ctx, &settled, resumed, retry_at)
                    .await
                {
                    return storage_err(req, e);
                }
                let promise = settled.to_promise_record();
                ok(req, ctx, &TaskFulfillResponseData { promise })
            }
            _ => err(req, 409, "Task version mismatch or invalid state"),
        }
    }

    pub(crate) async fn op_task_suspend(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskSuspendData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        if row.task_state.is_none() {
            return err(req, 404, "Task not found");
        }
        if row.task_state.as_deref() != Some("acquired") || row.task_version != r.version {
            return err(req, 409, "Task is not acquired or version mismatch");
        }

        // Read every awaited (ghosting each), all in the task's partition.
        let mut awaited_rows = Vec::new();
        for action in &r.actions {
            match self
                .read_and_try_timeout(&mut ctx, &action.data.awaited, now)
                .await
            {
                Err(e) => return storage_err(req, e),
                Ok(None) => return err(req, 422, "Awaited promise not found"),
                Ok(Some(a)) => awaited_rows.push(a),
            }
        }
        for a in &awaited_rows {
            if !resonate_core::types::is_external(&a.tags) {
                return err(req, 422, "Awaited promise is not awaitable");
            }
        }

        if awaited_rows.iter().any(|a| a.state != "pending") {
            // Something already settled: no suspend — clear the resume
            // ledger and hand back the preload so the worker re-reads.
            let (applied, _lwt) = match self
                .cas(
                    "UPDATE promises SET task_resumes = {} WHERE origin = ? AND id = ? IF task_state = 'acquired' AND task_version = ?",
                    (row.origin.as_str(), r.id.as_str(), r.version as i32),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => return storage_err(req, e),
            };
            if !applied {
                return err(req, 409, "Task is not acquired or version mismatch");
            }
            let preload = match self.preload(&row.origin, &r.id).await {
                Ok(p) => p,
                Err(e) => return storage_err(req, e),
            };
            let mut out = ok(req, ctx, &TaskSuspendPreloadData { preload });
            if let Some(resp) = &mut out.response {
                resp.head.status = 300;
            }
            return out;
        }

        // One conditional batch, single partition: the suspend plus one
        // callbacks registration per awaited.
        let mut stmts: Vec<(String, Args)> = vec![(
            "UPDATE promises SET task_state = 'suspended', task_pid = null, task_ttl = null, task_resumes = {}, task_timeout_lease = null WHERE origin = ? AND id = ? IF task_state = 'acquired' AND task_version = ?".to_string(),
            vec![text(&row.origin), text(&r.id), int(r.version as i32)],
        )];
        for a in &awaited_rows {
            stmts.push((
                "UPDATE promises SET callbacks = callbacks + ? WHERE origin = ? AND id = ? IF state = 'pending'".to_string(),
                vec![
                    cql_set(std::slice::from_ref(&r.id)),
                    text(&row.origin),
                    text(&a.id),
                ],
            ));
        }
        let (applied, _lwt) = match self.batch_cas(stmts).await {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            return err(req, 500, "Concurrent modification; please retry");
        }
        if let Some(old_lease) = row.task_timeout_lease {
            let _ = self
                .exec(
                    "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                    (
                        self.bucket_for(old_lease),
                        self.shard_for(&r.id),
                        old_lease,
                        row.origin.as_str(),
                        r.id.as_str(),
                    ),
                )
                .await;
        }
        ok(req, ctx, &json!({}))
    }

    pub(crate) async fn op_task_fence(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskFenceData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        if row.task_state.is_none() {
            return err(req, 404, "Task not found");
        }
        if row.task_state.as_deref() != Some("acquired") || row.task_version != r.version {
            return err(req, 409, "Version mismatch");
        }

        let inner_promise: Option<PromiseRecord> = match r.action.kind.as_str() {
            "promise.create" => {
                let create_data: PromiseCreateData =
                    match serde_json::from_value(r.action.data.clone()) {
                        Ok(d) => d,
                        Err(e) => return err(req, 400, &format!("Invalid action data: {}", e)),
                    };
                if let Err(e) = create_data.validate() {
                    return err(
                        req,
                        400,
                        &resonate_core::types::format_validation_errors(&e),
                    );
                }
                if let Some(addr) = create_data.tags.get("resonate:target") {
                    if !resonate_core::is_valid_address(addr) {
                        return err(req, 400, "Invalid resonate:target address");
                    }
                }
                match self.fence_inner_create(&mut ctx, &create_data, now).await {
                    Ok(p) => p,
                    Err(e) => return storage_err(req, e),
                }
            }
            "promise.settle" => {
                let settle_data: PromiseSettleData =
                    match serde_json::from_value(r.action.data.clone()) {
                        Ok(d) => d,
                        Err(e) => return err(req, 400, &format!("Invalid action data: {}", e)),
                    };
                if let Err(e) = settle_data.validate() {
                    return err(
                        req,
                        400,
                        &resonate_core::types::format_validation_errors(&e),
                    );
                }
                match self.fence_inner_settle(&mut ctx, &settle_data, now).await {
                    Ok(p) => p,
                    Err(e) => return storage_err(req, e),
                }
            }
            _ => return err(req, 400, "Invalid fence action kind"),
        };

        let (inner_status, inner_data) = match inner_promise {
            Some(ref p) => (200i32, json!({ "promise": p })),
            None => (404, json!("Promise not found")),
        };
        let inner_envelope = json!({
            "kind": r.action.kind,
            "head": { "corrId": req.head.corr_id, "status": inner_status, "version": PROTOCOL_VERSION },
            "data": inner_data,
        });
        let preload = match self.preload(&row.origin, &r.id).await {
            Ok(p) => p,
            Err(e) => return storage_err(req, e),
        };
        ok(
            req,
            ctx,
            &resonate_core::types::TaskFenceResponseData {
                action: inner_envelope,
                preload,
            },
        )
    }

    async fn fence_inner_create(
        &self,
        ctx: &mut Ctx,
        create_data: &PromiseCreateData,
        now: i64,
    ) -> Result<Option<PromiseRecord>, crate::StorageError> {
        // The inner action is a plain promise.create against its own row.
        let record = self.promise_create_impl(ctx, create_data, now).await?;
        Ok(Some(record))
    }

    async fn fence_inner_settle(
        &self,
        ctx: &mut Ctx,
        settle_data: &PromiseSettleData,
        now: i64,
    ) -> Result<Option<PromiseRecord>, crate::StorageError> {
        let Some(row) = self.read_and_try_timeout(ctx, &settle_data.id, now).await? else {
            return Ok(None);
        };
        if row.state != "pending" {
            return Ok(Some(row.to_promise_record()));
        }
        let settle_state = settle_data.state.to_string();
        let has_task = row.target.is_some();
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
            crate::db::opt_cql_map(settle_data.value.headers.as_ref()),
            opt_text(settle_data.value.data.clone()),
            big(now),
            text(&row.origin),
            text(&row.id),
            cql_set(&row.callbacks),
        ];
        let outcome = self
            .enqueue_resume(
                &row.id,
                &row.origin,
                &row.callbacks,
                now,
                stmt.to_string(),
                args,
            )
            .await?;
        match outcome {
            SettleOutcome::Won { resumed, retry_at } => {
                let mut settled = row.clone();
                settled.state = settle_state;
                settled.value_headers = settle_data.value.headers.clone().unwrap_or_default();
                settled.value_data = settle_data.value.data.clone();
                settled.settled_at = Some(now);
                self.after_settle_won(ctx, &settled, resumed, retry_at)
                    .await?;
                Ok(Some(settled.to_promise_record()))
            }
            _ => Ok(self
                .read_promise(&row.origin, &row.id)
                .await?
                .map(|p| p.to_promise_record())),
        }
    }

    pub(crate) async fn op_task_heartbeat(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskHeartbeatData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        for task_ref in &r.tasks {
            // task.heartbeat is the one operation that does not sweep first:
            // a plain read, and a lease is refused an extension when the
            // promise has logically expired — otherwise a heartbeat in the
            // window before the sweep would extend a dead task's lease.
            let origin = crate::origin_of(&task_ref.id).to_string();
            let row = match self.read_promise(&origin, &task_ref.id).await {
                Err(e) => return storage_err(req, e),
                Ok(None) => continue,
                Ok(Some(row)) => row,
            };
            let promise_live = row.state != "pending" || row.timeout_at > now;
            let eligible = promise_live
                && row.task_state.as_deref() == Some("acquired")
                && row.task_version == task_ref.version
                && row.task_pid.as_deref() == Some(r.pid.as_str())
                && row.task_ttl.is_some();
            if !eligible {
                continue;
            }
            let ttl = row.task_ttl.unwrap();
            let new_lease = now + ttl;
            if let Err(e) = self
                .exec(
                    "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)",
                    (
                        self.bucket_for(new_lease),
                        self.shard_for(&task_ref.id),
                        new_lease,
                        task_ref.id.as_str(),
                        row.origin.as_str(),
                        row.timeout_at,
                    ),
                )
                .await
            {
                return storage_err(req, e);
            }
            let (applied, _lwt) = match self
                .cas(
                    "UPDATE promises SET task_timeout_lease = ? WHERE origin = ? AND id = ? IF task_state = 'acquired' AND task_version = ?",
                    (
                        new_lease,
                        row.origin.as_str(),
                        task_ref.id.as_str(),
                        task_ref.version as i32,
                    ),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => return storage_err(req, e),
            };
            if applied {
                if let Some(old_lease) = row.task_timeout_lease {
                    if old_lease != new_lease {
                        let _ = self
                            .exec(
                                "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                                (
                                    self.bucket_for(old_lease),
                                    self.shard_for(&task_ref.id),
                                    old_lease,
                                    row.origin.as_str(),
                                    task_ref.id.as_str(),
                                ),
                            )
                            .await;
                    }
                }
                self.arm_lease(
                    &mut ctx,
                    &task_ref.id,
                    row.task_pid.as_deref().unwrap_or(""),
                    new_lease,
                );
            } else {
                let _ = self
                    .exec(
                        "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                        (
                            self.bucket_for(new_lease),
                            self.shard_for(&task_ref.id),
                            new_lease,
                            row.origin.as_str(),
                            task_ref.id.as_str(),
                        ),
                    )
                    .await;
            }
        }
        ok(req, ctx, &json!({}))
    }

    pub(crate) async fn op_task_halt(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskHaltData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        match row.task_state.as_deref() {
            None => return err(req, 404, "Task not found"),
            Some("fulfilled") => return err(req, 409, "Task is fulfilled"),
            Some("halted") => return ok(req, ctx, &json!({})),
            _ => {}
        }
        let (applied, lwt) = match self
            .cas(
                "UPDATE promises SET task_state = 'halted', task_pid = null, task_ttl = null, task_timeout_retry = null, task_timeout_lease = null WHERE origin = ? AND id = ? IF task_state IN ('pending', 'acquired', 'suspended')",
                (row.origin.as_str(), r.id.as_str()),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            return match get_text(&lwt, "task_state").as_deref() {
                Some("halted") => ok(req, ctx, &json!({})),
                Some("fulfilled") => err(req, 409, "Task is fulfilled"),
                _ => err(req, 500, "Concurrent modification; please retry"),
            };
        }
        match row.task_state.as_deref() {
            Some("acquired") => {
                if let Some(old_lease) = row.task_timeout_lease {
                    let _ = self
                        .exec(
                            "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?",
                            (
                                self.bucket_for(old_lease),
                                self.shard_for(&r.id),
                                old_lease,
                                row.origin.as_str(),
                                r.id.as_str(),
                            ),
                        )
                        .await;
                }
            }
            Some("pending") => {
                if let Some(old_retry) = row.task_timeout_retry {
                    let _ = self
                        .exec(
                            "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                            (
                                self.bucket_for(old_retry),
                                self.shard_for(&r.id),
                                old_retry,
                                row.origin.as_str(),
                                r.id.as_str(),
                            ),
                        )
                        .await;
                }
            }
            _ => {}
        }
        ok(req, ctx, &json!({}))
    }

    pub(crate) async fn op_task_continue(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskContinueData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let mut ctx = Ctx::default();
        let row = match self.read_and_try_timeout(&mut ctx, &r.id, now).await {
            Err(e) => return storage_err(req, e),
            Ok(None) => return err(req, 404, "Task not found"),
            Ok(Some(row)) => row,
        };
        if row.task_state.is_none() {
            return err(req, 404, "Task not found");
        }
        if row.task_state.as_deref() != Some("halted") {
            return err(req, 409, "Task is not halted");
        }
        let retry_at = now + self.task_retry_timeout;
        if let Err(e) = self
            .exec(
                "INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)",
                (
                    self.bucket_for(retry_at),
                    self.shard_for(&r.id),
                    retry_at,
                    r.id.as_str(),
                    row.origin.as_str(),
                    row.timeout_at,
                ),
            )
            .await
        {
            return storage_err(req, e);
        }
        let (applied, lwt) = match self
            .cas(
                "UPDATE promises SET task_state = 'pending', task_timeout_retry = ?, task_timeout_lease = null WHERE origin = ? AND id = ? IF task_state = 'halted'",
                (retry_at, row.origin.as_str(), r.id.as_str()),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => return storage_err(req, e),
        };
        if !applied {
            if crate::db::get_big(&lwt, "task_timeout_retry") != Some(retry_at) {
                let _ = self
                    .exec(
                        "DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?",
                        (
                            self.bucket_for(retry_at),
                            self.shard_for(&r.id),
                            retry_at,
                            row.origin.as_str(),
                            r.id.as_str(),
                        ),
                    )
                    .await;
            }
            return err(req, 409, "Task is not halted");
        }
        self.arm_retry(&mut ctx, &r.id, retry_at);
        if let Some(target) = &row.target {
            ctx.messages.push(Outgoing::Execute {
                address: target.clone(),
                task_id: r.id.clone(),
                version: row.task_version,
            });
        }
        ok(req, ctx, &json!({}))
    }

    pub(crate) async fn op_task_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let r: TaskSearchData = match parse(req) {
            Ok(r) => r,
            Err(e) => return Output::response(e),
        };
        let limit = match r.limit {
            Some(n) if n > 1000 => {
                return err(req, 400, "Invalid 'limit' — must be between 1 and 1000")
            }
            Some(n) => n as usize,
            None => 100,
        };
        let rows = match self
            .rows(&format!("SELECT {} FROM promises", crate::db::P_COLS), ())
            .await
        {
            Ok(rows) => rows,
            Err(e) => return storage_err(req, e),
        };
        let mut tasks: Vec<TaskRecord> = rows
            .iter()
            .map(PromiseRow::from_map)
            .filter_map(|p| p.to_task_record())
            .filter(|t| r.state.map(|s| t.state == s).unwrap_or(true))
            .collect();
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        if let Some(cursor) = r.cursor.as_deref() {
            tasks.retain(|t| t.id.as_str() > cursor);
        }
        let has_more = tasks.len() > limit;
        tasks.truncate(limit);
        let cursor = if has_more {
            tasks.last().map(|t| t.id.clone())
        } else {
            None
        };
        ok(
            req,
            Ctx::default(),
            &TaskSearchResponseData { tasks, cursor },
        )
    }
}
