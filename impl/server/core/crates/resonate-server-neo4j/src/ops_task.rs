//! The `task.*` operations.
//!
//! A task is the node its promise is on, so "lock the task" and "lock the
//! promise" are the same statement, and the fence, the acquire and the fulfil
//! all read one node before they decide.

use std::collections::HashSet;

use neo4rs::query;
use validator::Validate;

use crate::db::{headers_json, p_cols, PromiseRow, Tx};
use crate::engine::Output;
use crate::ops_promise::{create_promise, fail, ok, ok_empty, parse, search_limit};
use crate::{Neo4jEngine, StorageResult};
use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseSettleData, PromiseState, RequestEnvelope,
    ResponseEnvelope, TaskAcquireData, TaskAcquireResponseData, TaskContinueData, TaskCreateData,
    TaskCreateResponseData, TaskFenceData, TaskFenceResponseData, TaskFulfillData,
    TaskFulfillResponseData, TaskGetData, TaskHaltData, TaskHeartbeatData, TaskRecord,
    TaskReleaseData, TaskResponseData, TaskSearchData, TaskSearchResponseData, TaskState,
    TaskSuspendData, TaskSuspendPreloadData, PROTOCOL_VERSION,
};

/// Acquire a locked task at `version`: acquired, one version up, with a fresh
/// lease and its ready callbacks dropped. `false` when the task is not pending
/// at that version.
async fn acquire(
    tx: &mut Tx<'_>,
    row: &PromiseRow,
    version: i64,
    now: i64,
    ttl: i64,
    pid: &str,
) -> StorageResult<bool> {
    if !(row.task_is("pending") && row.task_version == version) {
        return Ok(false);
    }
    tx.run(
        query(
            "MATCH (p:Promise {id: $id}) SET p.task_state = 'acquired', \
             p.task_version = $version, p.lease_timeout_at = $lease, p.ttl = $ttl, \
             p.pid = $pid, p.retry_timeout_at = null",
        )
        .param("id", row.id.as_str())
        .param("version", version + 1)
        .param("lease", now + ttl)
        .param("ttl", ttl)
        .param("pid", pid),
    )
    .await?;
    tx.clear_resumes(&row.id).await?;
    tx.arm_lease(&row.id, pid, now + ttl);
    Ok(true)
}

/// Hand a task back to pending: retry deadline armed, execute re-emitted at
/// the task's current version. Release, lease expiry and continue all end here.
pub(crate) async fn redispatch(tx: &mut Tx<'_>, row: &PromiseRow, now: i64) -> StorageResult<()> {
    let at = now + tx.trt;
    tx.run(
        query(
            "MATCH (p:Promise {id: $id}) SET p.task_state = 'pending', p.retry_timeout_at = $at, \
             p.lease_timeout_at = null, p.ttl = null, p.pid = null",
        )
        .param("id", row.id.as_str())
        .param("at", at),
    )
    .await?;
    tx.emit_execute(row.target.as_deref(), &row.id, row.task_version);
    tx.arm_retry(&row.id, at);
    Ok(())
}

fn fulfilled_record(id: &str, version: i64) -> TaskRecord {
    TaskRecord {
        id: id.to_string(),
        state: TaskState::Fulfilled,
        version,
        resumes: 0,
        ttl: None,
        pid: None,
    }
}

fn acquired_record(id: &str, version: i64, ttl: i64, pid: &str) -> TaskRecord {
    TaskRecord {
        id: id.to_string(),
        state: TaskState::Acquired,
        version,
        resumes: 0,
        ttl: Some(ttl),
        pid: Some(pid.to_string()),
    }
}

impl Neo4jEngine {
    pub(crate) async fn op_task_get(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskGetData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                match tx.read(&r.id).await?.and_then(|row| row.to_task_record()) {
                    Some(task) => {
                        tracing::debug!(task_id = %r.id, state = %task.state, version = task.version, "Task found");
                        Ok(ok(req, &TaskResponseData { task }))
                    }
                    None => {
                        tracing::debug!(task_id = %r.id, "Task not found");
                        Ok(fail(req, 404, "Task not found"))
                    }
                }
            })
        })
        .await
    }

    pub(crate) async fn op_task_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskCreateData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let action = &r.action.data;
        if let Some(addr) = action.tags.get("resonate:target") {
            if !resonate_core::is_valid_address(addr) {
                tracing::warn!(task_id = %action.id, address = %addr, "Task create rejected: invalid resonate:target address");
                return Output::response(fail(req, 400, "Invalid resonate:target address"));
            }
        }
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                let action = &r.action.data;
                let id = action.id.as_str();
                tx.try_timeout(&[id], now).await?;

                let existing = tx.lock(id).await?;
                let row = match existing {
                    None => {
                        // Born acquired by this caller — or, past its deadline,
                        // born settled with a fulfilled task.
                        let already_timedout = now >= action.timeout_at;
                        let (state, created_at, settled_at) = if already_timedout {
                            let state = if resonate_core::types::is_timer(&action.tags) {
                                PromiseState::Resolved
                            } else {
                                PromiseState::RejectedTimedout
                            };
                            (state, action.timeout_at, Some(action.timeout_at))
                        } else {
                            (PromiseState::Pending, now, None)
                        };
                        let (task_state, task_version) = if already_timedout {
                            ("fulfilled", 0)
                        } else {
                            ("acquired", 1)
                        };
                        tx.create_promise(&crate::db::NewPromise {
                            id,
                            state: state.as_str(),
                            param_headers: headers_json(action.param.headers.as_ref()),
                            param_data: action.param.data.as_deref(),
                            tags: &action.tags,
                            timeout_at: action.timeout_at,
                            created_at,
                            settled_at,
                            task_state: Some(task_state),
                            task_version,
                            retry_timeout_at: None,
                            lease_timeout_at: (!already_timedout).then_some(created_at + r.ttl),
                            ttl: (!already_timedout).then_some(r.ttl),
                            pid: (!already_timedout).then_some(r.pid.as_str()),
                        })
                        .await?;
                        if !already_timedout {
                            tx.arm_promise_timeout(id, action.timeout_at, true);
                            tx.arm_lease(id, &r.pid, created_at + r.ttl);
                        }
                        let row = tx.read(id).await?.expect("just created");
                        let promise = row.to_promise_record();
                        let task = if already_timedout {
                            fulfilled_record(id, 0)
                        } else {
                            acquired_record(id, 1, r.ttl, &r.pid)
                        };
                        let preload = tx.compute_preload(id).await?;
                        tracing::info!(task_id = %id, state = %task.state, "Task created");
                        return Ok(ok(
                            req,
                            &TaskCreateResponseData {
                                task,
                                promise,
                                preload,
                            },
                        ));
                    }
                    Some(row) => row,
                };

                // The promise already existed. If it is settled, fire whatever
                // registered against it meanwhile, as Postgres does after its
                // insert.
                if !row.is_pending() {
                    tx.process_callbacks(id, now).await?;
                }
                let promise = row.to_promise_record();
                match row.task_state.as_deref() {
                    None => Ok(fail(
                        req,
                        422,
                        "The promise does not have a resonate:target tag",
                    )),
                    Some("fulfilled") => {
                        assert_ne!(
                            promise.state,
                            PromiseState::Pending,
                            "invariant: pending promise with fulfilled task"
                        );
                        let preload = tx.compute_preload(id).await?;
                        Ok(ok(
                            req,
                            &TaskCreateResponseData {
                                task: fulfilled_record(id, row.task_version),
                                promise,
                                preload,
                            },
                        ))
                    }
                    Some("pending") => {
                        let version = row.task_version;
                        if acquire(tx, &row, version, now, r.ttl, &r.pid).await? {
                            let preload = tx.compute_preload(id).await?;
                            Ok(ok(
                                req,
                                &TaskCreateResponseData {
                                    task: acquired_record(id, version + 1, r.ttl, &r.pid),
                                    promise,
                                    preload,
                                },
                            ))
                        } else {
                            Ok(fail(req, 409, "Already exists"))
                        }
                    }
                    Some(_) => Ok(fail(req, 409, "Already exists")),
                }
            })
        })
        .await
    }

    pub(crate) async fn op_task_acquire(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskAcquireData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                let Some(row) = tx.lock(&r.id).await?.filter(PromiseRow::has_task) else {
                    tracing::debug!(task_id = %r.id, "Task acquire: task not found");
                    return Ok(fail(req, 404, "Task not found"));
                };
                if !acquire(tx, &row, r.version, now, r.ttl, &r.pid).await? {
                    if !row.task_is("pending") {
                        tracing::debug!(task_id = %r.id, current_state = ?row.task_state, "Task acquire rejected: not pending");
                        return Ok(fail(req, 409, "Task is not pending"));
                    }
                    tracing::debug!(task_id = %r.id, expected_version = r.version, actual_version = row.task_version, "Task acquire rejected: version mismatch");
                    return Ok(fail(req, 409, "Version mismatch"));
                }
                let preload = tx.compute_preload(&r.id).await?;
                Ok(ok(
                    req,
                    &TaskAcquireResponseData {
                        task: acquired_record(&r.id, r.version + 1, r.ttl, &r.pid),
                        promise: row.to_promise_record(),
                        preload,
                    },
                ))
            })
        })
        .await
    }

    pub(crate) async fn op_task_release(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskReleaseData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                let Some(row) = tx.lock(&r.id).await?.filter(PromiseRow::has_task) else {
                    tracing::debug!(task_id = %r.id, "Task release: task not found");
                    return Ok(fail(req, 404, "Task not found"));
                };
                if row.task_is("acquired") && row.task_version == r.version {
                    redispatch(tx, &row, now).await?;
                    tracing::info!(task_id = %r.id, version = r.version, "Task released back to pending");
                    return Ok(ok_empty(req));
                }
                tracing::debug!(task_id = %r.id, version = r.version, "Task release rejected: version mismatch or invalid state");
                Ok(fail(req, 409, "Task version mismatch or invalid state"))
            })
        })
        .await
    }

    pub(crate) async fn op_task_fulfill(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskFulfillData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                let action = &r.action.data;
                tx.try_timeout(&[&action.id], now).await?;
                let Some(row) = tx.lock(&r.id).await?.filter(PromiseRow::has_task) else {
                    tracing::debug!(task_id = %r.id, "Task fulfill: task not found");
                    return Ok(fail(req, 404, "Task not found"));
                };
                if !(row.task_is("acquired") && row.task_version == r.version) {
                    tracing::debug!(task_id = %r.id, version = r.version, "Task fulfill rejected: version mismatch or invalid state");
                    return Ok(fail(req, 409, "Task version mismatch or invalid state"));
                }
                let promise = if row.is_pending() {
                    tx.settle_locked(
                        &row,
                        action.state.as_str(),
                        headers_json(action.value.headers.as_ref()),
                        action.value.data.as_deref(),
                        now,
                        true,
                        &HashSet::new(),
                        now,
                    )
                    .await?
                    .to_promise_record()
                } else {
                    // Unreachable by the storage invariants — a settled promise
                    // has a fulfilled task — but the Postgres statement still
                    // fulfils the task in this case, so this does too.
                    tx.run(
                        query(
                            "MATCH (p:Promise {id: $id}) SET p.task_state = 'fulfilled', \
                             p.retry_timeout_at = null, p.lease_timeout_at = null, \
                             p.ttl = null, p.pid = null",
                        )
                        .param("id", r.id.as_str()),
                    )
                    .await?;
                    tx.run(
                        query("MATCH (p:Promise {id: $id})-[e:AWAITS]->() DELETE e")
                            .param("id", r.id.as_str()),
                    )
                    .await?;
                    row.to_promise_record()
                };
                assert_ne!(promise.state, PromiseState::Pending, "invariant: returning 200 but promise is still pending");
                tracing::info!(task_id = %r.id, version = r.version, promise_state = %promise.state, "Task fulfilled and promise settled");
                Ok(ok(req, &TaskFulfillResponseData { promise }))
            })
        })
        .await
    }

    pub(crate) async fn op_task_suspend(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskSuspendData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let awaited_ids: Vec<String> = r.actions.iter().map(|a| a.data.awaited.clone()).collect();
        let r = &r;
        let awaited_ids = &awaited_ids;
        self.run(req, |tx| {
            Box::pin(async move {
                // Lock the task before the ghost sweep, so the sweep cannot
                // fulfil it out from under the version check.
                let task_exists = tx.lock(&r.id).await?.is_some_and(|row| row.has_task());
                let mut timeout_ids: Vec<&str> = vec![r.id.as_str()];
                timeout_ids.extend(awaited_ids.iter().map(String::as_str));
                tx.try_timeout(&timeout_ids, now).await?;

                // Fresh, after the sweep: the task may have been fulfilled by it.
                let me = tx.read(&r.id).await?;
                let matched = me
                    .as_ref()
                    .is_some_and(|m| m.task_is("acquired") && m.task_version == r.version);
                if !matched {
                    if !task_exists {
                        return Ok(fail(req, 404, "Task not found"));
                    }
                    tracing::debug!(task_id = %r.id, version = r.version, "Task suspend rejected: not acquired or version mismatch");
                    return Ok(fail(req, 409, "Task is not acquired or version mismatch"));
                }

                let awaited = tx.lock_many(awaited_ids).await?;
                let missing = awaited_ids.len() - awaited.len();
                let non_awaitable = awaited.iter().filter(|a| !a.external).count();
                if missing > 0 {
                    tracing::debug!(task_id = %r.id, missing_count = missing, "Task suspend rejected: awaited promise(s) not found");
                    return Ok(fail(req, 422, "Awaited promise not found"));
                }
                if non_awaitable > 0 {
                    tracing::debug!(task_id = %r.id, non_awaitable_count = non_awaitable, "Task suspend rejected: awaited promise(s) not awaitable");
                    return Ok(fail(req, 422, "Awaited promise is not awaitable"));
                }

                // The ready callbacks are consumed by a matching suspend
                // whether or not it goes on to suspend.
                tx.clear_resumes(&r.id).await?;

                if awaited.iter().all(PromiseRow::is_pending) {
                    for a in &awaited {
                        if a.id != r.id {
                            tx.link(&r.id, &a.id).await?;
                        }
                    }
                    tx.run(
                        query(
                            "MATCH (p:Promise {id: $id}) SET p.task_state = 'suspended', \
                             p.retry_timeout_at = null, p.lease_timeout_at = null, \
                             p.ttl = null, p.pid = null",
                        )
                        .param("id", r.id.as_str()),
                    )
                    .await?;
                    tracing::info!(task_id = %r.id, version = r.version, awaited_count = awaited.len(), "Task suspended, waiting on promises");
                    return Ok(ok_empty(req));
                }

                tracing::info!(task_id = %r.id, version = r.version, "Task suspend: immediate resume, awaited promises already settled");
                let preload = tx.compute_preload(&r.id).await?;
                Ok(ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    300,
                    serde_json::to_value(&TaskSuspendPreloadData { preload }).unwrap(),
                ))
            })
        })
        .await
    }

    pub(crate) async fn op_task_fence(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskFenceData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let action_id = r.action.data["id"].as_str().unwrap_or("").to_string();
        let r = &r;
        let action_id = &action_id;
        self.run(req, |tx| {
            Box::pin(async move {
                let action_kind = r.action.kind.as_str();
                // The ghost sweep and the fence lock come first, as they do in
                // Postgres: a malformed action is still a 400, but not before
                // whatever the sweep owed has been settled.
                tx.try_timeout(&[r.id.as_str(), action_id.as_str()], now).await?;
                let fence = tx.lock(&r.id).await?.filter(PromiseRow::has_task);

                enum Fenced {
                    Create(PromiseCreateData),
                    Settle(PromiseSettleData),
                }
                let fenced = match action_kind {
                    "promise.create" => {
                        let d: PromiseCreateData =
                            match serde_json::from_value(r.action.data.clone()) {
                                Ok(d) => d,
                                Err(e) => {
                                    return Ok(fail(req, 400, &format!("Invalid action data: {}", e)))
                                }
                            };
                        if let Err(e) = d.validate() {
                            return Ok(fail(req, 400, &format_validation_errors(&e)));
                        }
                        if let Some(addr) = d.tags.get("resonate:target") {
                            if !resonate_core::is_valid_address(addr) {
                                tracing::warn!(task_id = %r.id, address = %addr, "Task fence rejected: invalid resonate:target address in fenced promise.create");
                                return Ok(fail(req, 400, "Invalid resonate:target address"));
                            }
                        }
                        Fenced::Create(d)
                    }
                    "promise.settle" => {
                        let d: PromiseSettleData =
                            match serde_json::from_value(r.action.data.clone()) {
                                Ok(d) => d,
                                Err(e) => {
                                    return Ok(fail(req, 400, &format!("Invalid action data: {}", e)))
                                }
                            };
                        if let Err(e) = d.validate() {
                            return Ok(fail(req, 400, &format_validation_errors(&e)));
                        }
                        Fenced::Settle(d)
                    }
                    _ => {
                        tracing::warn!(task_id = %r.id, action_kind = %action_kind, "Task fence rejected: invalid fence action kind");
                        return Ok(fail(req, 400, "Invalid fence action kind"));
                    }
                };

                let Some(fence) = fence else {
                    tracing::debug!(task_id = %r.id, fenced_action = %action_kind, "Task fence rejected: task not found");
                    return Ok(fail(req, 404, "Task not found"));
                };
                if !(fence.task_is("acquired") && fence.task_version == r.version) {
                    tracing::debug!(task_id = %r.id, version = r.version, fenced_action = %action_kind, "Task fence rejected: version mismatch");
                    return Ok(fail(req, 409, "Version mismatch"));
                }

                let inner_envelope = |status: i32, data: serde_json::Value| {
                    serde_json::json!({
                        "kind": action_kind,
                        "head": { "corrId": req.head.corr_id, "status": status, "version": PROTOCOL_VERSION },
                        "data": data,
                    })
                };

                let action = match &fenced {
                    Fenced::Create(d) => {
                        let outcome = create_promise(tx, d, now).await?;
                        let p = outcome.row.to_promise_record();
                        tracing::info!(task_id = %r.id, version = r.version, fenced_action = "promise.create", promise_id = %d.id, "Task fence: promise.create executed");
                        inner_envelope(200, serde_json::json!({ "promise": p }))
                    }
                    Fenced::Settle(d) => match tx.lock(&d.id).await? {
                        None => inner_envelope(404, serde_json::json!("Promise not found")),
                        Some(row) => {
                            let p = if row.is_pending() {
                                tx.settle_locked(
                                    &row,
                                    d.state.as_str(),
                                    headers_json(d.value.headers.as_ref()),
                                    d.value.data.as_deref(),
                                    now,
                                    row.settle_fulfils(),
                                    &HashSet::new(),
                                    now,
                                )
                                .await?
                                .to_promise_record()
                            } else {
                                row.to_promise_record()
                            };
                            assert_ne!(p.state, PromiseState::Pending, "invariant: returning 200 but promise is still pending");
                            tracing::info!(task_id = %r.id, version = r.version, fenced_action = "promise.settle", promise_id = %d.id, settle_state = %d.state, "Task fence: promise.settle executed");
                            inner_envelope(200, serde_json::json!({ "promise": p }))
                        }
                    },
                };
                let preload = tx.compute_preload(&r.id).await?;
                Ok(ok(req, &TaskFenceResponseData { action, preload }))
            })
        })
        .await
    }

    pub(crate) async fn op_task_heartbeat(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskHeartbeatData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                for t in &r.tasks {
                    let Some(row) = tx.lock(&t.id).await? else {
                        continue;
                    };
                    // The promise-liveness guard: a heartbeat on a task whose
                    // promise is pending-but-expired is a no-op. This is the
                    // one operation that does not sweep first.
                    let live = !row.is_pending() || row.timeout_at > now;
                    if row.task_is("acquired")
                        && row.task_version == t.version
                        && row.pid.as_deref() == Some(r.pid.as_str())
                        && live
                    {
                        let Some(ttl) = row.ttl else { continue };
                        let at = now + ttl;
                        tx.run(
                            query("MATCH (p:Promise {id: $id}) SET p.lease_timeout_at = $at")
                                .param("id", t.id.as_str())
                                .param("at", at),
                        )
                        .await?;
                        tx.arm_lease(&t.id, &r.pid, at);
                    }
                }
                tracing::debug!(pid = %r.pid, task_count = r.tasks.len(), "Task heartbeat processed");
                Ok(ok_empty(req))
            })
        })
        .await
    }

    pub(crate) async fn op_task_halt(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskHaltData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                let Some(row) = tx.lock(&r.id).await?.filter(PromiseRow::has_task) else {
                    tracing::debug!(task_id = %r.id, "Task halt: not found");
                    return Ok(fail(req, 404, "Task not found"));
                };
                if row.task_is("fulfilled") {
                    tracing::debug!(task_id = %r.id, "Task halt rejected: already fulfilled");
                    return Ok(fail(req, 409, "Task is fulfilled"));
                }
                if !row.task_is("halted") {
                    tx.run(
                        query(
                            "MATCH (p:Promise {id: $id}) SET p.task_state = 'halted', \
                             p.retry_timeout_at = null, p.lease_timeout_at = null, \
                             p.ttl = null, p.pid = null",
                        )
                        .param("id", r.id.as_str()),
                    )
                    .await?;
                }
                tracing::info!(task_id = %r.id, "Task halted");
                Ok(ok_empty(req))
            })
        })
        .await
    }

    pub(crate) async fn op_task_continue(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: TaskContinueData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                let Some(row) = tx.lock(&r.id).await?.filter(PromiseRow::has_task) else {
                    tracing::debug!(task_id = %r.id, "Task continue: not found");
                    return Ok(fail(req, 404, "Task not found"));
                };
                if !row.task_is("halted") {
                    tracing::debug!(task_id = %r.id, "Task continue rejected: not halted");
                    return Ok(fail(req, 409, "Task is not halted"));
                }
                redispatch(tx, &row, now).await?;
                tracing::info!(task_id = %r.id, "Task continued from halted state");
                Ok(ok_empty(req))
            })
        })
        .await
    }

    pub(crate) async fn op_task_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let r: TaskSearchData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let limit = match search_limit(req, r.limit, 100) {
            Ok(n) => n,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                let q = query(&format!(
                    "MATCH (p:Promise) \
                     WHERE p.task_state IS NOT NULL \
                       AND ($state IS NULL OR p.task_state = $state) \
                       AND ($cursor IS NULL OR p.id > $cursor) \
                     RETURN {} ORDER BY p.id ASC LIMIT $limit",
                    p_cols("p")
                ))
                .param("state", r.state.map(|s| s.as_str().to_string()))
                .param("cursor", r.cursor.clone())
                .param("limit", limit + 1);
                let rows = tx.promise_rows(q).await?;
                let has_more = rows.len() as i64 > limit;
                let tasks: Vec<TaskRecord> = rows
                    .iter()
                    .take(limit as usize)
                    .filter_map(PromiseRow::to_task_record)
                    .collect();
                let cursor = if has_more {
                    tasks.last().map(|t| t.id.clone())
                } else {
                    None
                };
                tracing::debug!(found = tasks.len(), has_more, "Task search completed");
                Ok(ok(req, &TaskSearchResponseData { tasks, cursor }))
            })
        })
        .await
    }
}
