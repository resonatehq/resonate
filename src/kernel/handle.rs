//! The decider: one request in, effects and a reply out.
//!
//! `handle` never mutates the caller's document. It works on a copy, and the
//! copy comes back as a [`Effect::SetDocument`] — so the transition *is*
//! `apply_effects(handle(..).0)` and there is no second updater to drift.
//!
//! Two things happen before every operation, in this order:
//!
//! 1. **Ghost timeouts.** Any promise the request *names* whose deadline has
//!    passed is settled first, with its settlement chain run in full. This
//!    mirrors `Db::try_timeout` (`persistence_sqlite.rs:363-419`), which every
//!    operation in `server.rs` calls before touching state. Only the named ids
//!    are swept — a full sweep happens on [`drain`](super::drain) — because
//!    the SQL backends behave that way and `debug.snap` must agree byte for
//!    byte.
//! 2. **The operation itself**, against the post-ghost state.
//!
//! Statuses and messages are `Server::dispatch`'s, verbatim: a rejection is a
//! [`Reply`] with a status, never an `Err`.

use std::collections::BTreeSet;

use serde_json::json;
use validator::Validate;

use crate::core::is_valid_address;
use crate::core::types::{
    format_validation_errors, PromiseCreateData, PromiseRecord, PromiseResponseData, PromiseState,
    PromiseValue, SettleState, TaskAcquireResponseData, TaskCreateResponseData,
    TaskFenceResponseData, TaskFulfillResponseData, TaskRecord, TaskResponseData, TaskState,
    TaskSuspendPreloadData, PROTOCOL_VERSION,
};

use super::state::{
    min_deadline, Effect, KernelCfg, OriginDoc, OutEntry, PromiseDoc, Reply, Req, TaskDoc,
    TAG_BRANCH, TAG_DELAY, TAG_TARGET,
};

/// A decision in progress: the working document plus the messages it owes.
pub(crate) struct Tx {
    pub(crate) doc: OriginDoc,
    pub(crate) sends: Vec<(String, OutEntry)>,
}

impl Tx {
    pub(crate) fn new(doc: &OriginDoc) -> Self {
        Self {
            doc: doc.clone(),
            sends: Vec::new(),
        }
    }

    /// Turn the working state into effects, in the order the shell performs
    /// them: arm the new timer, commit the document, clear the old timer, send.
    pub(crate) fn finish(mut self, before: &OriginDoc) -> Vec<Effect> {
        let old = before.timer_at;
        let new = min_deadline(&self.doc);
        self.doc.timer_at = new;

        let mut fx = Vec::new();
        if old != new {
            if let Some(at) = new {
                fx.push(Effect::SetTimeout { at });
            }
        }
        fx.push(Effect::SetDocument(self.doc));
        if old != new {
            if let Some(at) = old {
                fx.push(Effect::DelTimeout { at });
            }
        }
        for (address, out) in self.sends {
            fx.push(Effect::Send { address, out });
        }
        fx
    }

    /// Queue a dispatch for `task_id`. A promise with no `resonate:target` has
    /// nowhere to send, exactly as `outgoing_execute`'s insert is guarded on a
    /// non-null target.
    pub(crate) fn send_execute(&mut self, task_id: &str, version: i64) {
        let address = match self.doc.promises.get(task_id).and_then(|p| p.target()) {
            Some(a) => a.to_string(),
            None => return,
        };
        self.sends.push((
            address,
            OutEntry::Execute {
                task_id: task_id.to_string(),
                version,
            },
        ));
    }
}

/// Decide one request.
pub fn handle(doc: &OriginDoc, req: &Req, now: i64, cfg: &KernelCfg) -> (Vec<Effect>, Reply) {
    let mut tx = Tx::new(doc);
    let reply = match req {
        Req::PromiseGet(r) => op_promise_get(&mut tx, r, now, cfg),
        Req::PromiseCreate(r) => op_promise_create(&mut tx, r, now, cfg),
        Req::PromiseSettle(r) => op_promise_settle(&mut tx, r, now, cfg),
        Req::PromiseRegisterCallback(r) => op_promise_register_callback(&mut tx, r, now, cfg),
        Req::PromiseRegisterListener(r) => op_promise_register_listener(&mut tx, r, now, cfg),
        Req::TaskGet(r) => op_task_get(&mut tx, r, now, cfg),
        Req::TaskCreate(r) => op_task_create(&mut tx, r, now, cfg),
        Req::TaskAcquire(r) => op_task_acquire(&mut tx, r, now, cfg),
        Req::TaskRelease(r) => op_task_release(&mut tx, r, now, cfg),
        Req::TaskFulfill(r) => op_task_fulfill(&mut tx, r, now, cfg),
        Req::TaskSuspend(r) => op_task_suspend(&mut tx, r, now, cfg),
        Req::TaskFence { data, corr_id } => op_task_fence(&mut tx, data, corr_id, now, cfg),
        Req::TaskFencePrepare(r) => op_task_fence_prepare(&mut tx, r, now, cfg),
        Req::TaskHeartbeat(r) => op_task_heartbeat(&mut tx, r, now),
        Req::TaskHalt(r) => op_task_halt(&mut tx, r, now, cfg),
        Req::TaskContinue(r) => op_task_continue(&mut tx, r, now, cfg),
        Req::ScheduleFire(r) => op_schedule_fire(&mut tx, r, now, cfg),
    };
    (tx.finish(doc), reply)
}

// ============================================================================
// Promise operations
// ============================================================================

fn op_promise_get(
    tx: &mut Tx,
    r: &crate::core::types::PromiseGetData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    match tx.doc.promises.get(&r.id) {
        Some(p) => Reply::ok(&PromiseResponseData {
            promise: p.to_record(&r.id),
        }),
        None => Reply::err(404, "Promise not found"),
    }
}

fn op_promise_create(tx: &mut Tx, r: &PromiseCreateData, now: i64, cfg: &KernelCfg) -> Reply {
    if let Some(addr) = r.tags.get(TAG_TARGET) {
        if !is_valid_address(addr) {
            return Reply::err(400, "Invalid resonate:target address");
        }
    }
    try_timeout(tx, &[&r.id], now, cfg);
    if let Some(p) = tx.doc.promises.get(&r.id) {
        // Create is idempotent on id alone: the stored promise wins.
        return Reply::ok(&PromiseResponseData {
            promise: p.to_record(&r.id),
        });
    }
    let record = create_promise(tx, &r.id, r, now, cfg);
    Reply::ok(&PromiseResponseData { promise: record })
}

fn op_promise_settle(
    tx: &mut Tx,
    r: &crate::core::types::PromiseSettleData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let pending = match tx.doc.promises.get(&r.id) {
        Some(p) => p.state == PromiseState::Pending,
        None => return Reply::err(404, "Promise not found"),
    };
    if !pending {
        // Settlement is terminal: a second settle reports the first one.
        let p = &tx.doc.promises[&r.id];
        return Reply::ok(&PromiseResponseData {
            promise: p.to_record(&r.id),
        });
    }
    let record = settle(tx, &r.id, r.state, &r.value, now, cfg);
    Reply::ok(&PromiseResponseData { promise: record })
}

fn op_promise_register_callback(
    tx: &mut Tx,
    r: &crate::core::types::PromiseRegisterCallbackData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.awaited, &r.awaiter], now, cfg);

    let awaited_record = match tx.doc.promises.get(&r.awaited) {
        Some(p) => p.to_record(&r.awaited),
        None => return Reply::err(404, "Awaited promise not found"),
    };
    let (awaiter_state, awaiter_has_target) = match tx.doc.promises.get(&r.awaiter) {
        Some(p) => (p.state, p.target().is_some()),
        None => return Reply::err(422, "Awaiter promise not found"),
    };
    if !awaiter_has_target {
        return Reply::err(422, "Awaiter promise has no resonate:target tag");
    }

    let awaited_pending = awaited_record.state == PromiseState::Pending;
    let awaiter_pending = awaiter_state == PromiseState::Pending;

    if awaited_pending && awaiter_pending {
        register_callback(tx, &r.awaited, &r.awaiter);
    } else if !awaited_pending && awaiter_pending {
        // The awaited promise is already settled, so there is nothing to wait
        // for: wake the awaiter now instead of registering.
        resume_awaiter(tx, &r.awaiter, &r.awaited, now, cfg, Wake::Registration);
    }

    Reply::ok(&PromiseResponseData {
        promise: awaited_record,
    })
}

fn op_promise_register_listener(
    tx: &mut Tx,
    r: &crate::core::types::PromiseRegisterListenerData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    if !is_valid_address(&r.address) {
        return Reply::err(400, "Invalid listener address");
    }
    try_timeout(tx, &[&r.awaited], now, cfg);
    let pending = match tx.doc.promises.get(&r.awaited) {
        Some(p) => p.state == PromiseState::Pending,
        None => return Reply::err(404, "Awaited promise not found"),
    };
    if pending {
        let p = tx.doc.promises.get_mut(&r.awaited).expect("checked above");
        if !p.listeners.iter().any(|a| a == &r.address) {
            p.listeners.push(r.address.clone());
        }
    }
    let p = &tx.doc.promises[&r.awaited];
    Reply::ok(&PromiseResponseData {
        promise: p.to_record(&r.awaited),
    })
}

// ============================================================================
// Task operations
// ============================================================================

fn op_task_get(
    tx: &mut Tx,
    r: &crate::core::types::TaskGetData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    match tx.doc.tasks.get(&r.id) {
        Some(t) => Reply::ok(&TaskResponseData {
            task: t.to_record(&r.id),
        }),
        None => Reply::err(404, "Task not found"),
    }
}

/// `task.create` is a worker claiming work by describing it: it creates the
/// promise if absent and hands back a task already acquired by the caller — no
/// dispatch, because the caller *is* the worker.
///
/// Mirrors `Server::op_task_create` (`server.rs:1086-1300`).
fn op_task_create(
    tx: &mut Tx,
    r: &crate::core::types::TaskCreateData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    // Every task.create action carries a resonate:target (its validator
    // requires one), so the promise it creates always has a task.
    let action = &r.action.data;
    if let Some(addr) = action.tags.get(TAG_TARGET) {
        if !is_valid_address(addr) {
            return Reply::err(400, "Invalid resonate:target address");
        }
    }
    let id = action.id.as_str();
    try_timeout(tx, &[id], now, cfg);

    if let Some(t) = tx.doc.tasks.get(id) {
        match t.state {
            TaskState::Pending => {
                let task = acquire(tx, id, r.ttl, &r.pid, now);
                return Reply::ok(&TaskCreateResponseData {
                    task,
                    promise: tx.doc.promises[id].to_record(id),
                    preload: preload(&tx.doc, id),
                });
            }
            TaskState::Fulfilled => {
                // The work is already done. server.rs sends no preload on this
                // branch, so neither do we.
                return Reply::ok(&TaskCreateResponseData {
                    task: t.to_record(id),
                    promise: tx.doc.promises[id].to_record(id),
                    preload: Vec::new(),
                });
            }
            TaskState::Acquired | TaskState::Suspended | TaskState::Halted => {
                return Reply::err(409, "Already exists");
            }
        }
    }
    if tx.doc.promises.contains_key(id) {
        // A promise without a task is a promise nobody can be dispatched for.
        return Reply::err(422, "The promise does not have a resonate:target tag");
    }

    // Neither exists: create both. The task is born acquired by the caller —
    // never pending — so no dispatch is emitted, which is why this builds the
    // task itself rather than going through create_promise.
    let promise = insert_promise(tx, id, action, now);
    let settled = promise.state != PromiseState::Pending;
    let mut t = TaskDoc {
        state: TaskState::Fulfilled,
        version: 0,
        pid: None,
        ttl: None,
        resumes: BTreeSet::new(),
        retry_at: None,
        lease_at: None,
    };
    if !settled {
        t.state = TaskState::Acquired;
        t.version = 1;
        t.pid = Some(r.pid.clone());
        t.ttl = Some(r.ttl);
        t.arm_lease(now + r.ttl);
    }
    let task = t.to_record(id);
    tx.doc.tasks.insert(id.to_string(), t);
    Reply::ok(&TaskCreateResponseData {
        task,
        promise,
        preload: preload(&tx.doc, id),
    })
}

fn op_task_acquire(
    tx: &mut Tx,
    r: &crate::core::types::TaskAcquireData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Pending {
        return Reply::err(409, "Task is not pending");
    }
    if version != r.version {
        return Reply::err(409, "Version mismatch");
    }
    let task = acquire(tx, &r.id, r.ttl, &r.pid, now);
    Reply::ok(&TaskAcquireResponseData {
        task,
        promise: tx.doc.promises[&r.id].to_record(&r.id),
        preload: preload(&tx.doc, &r.id),
    })
}

fn op_task_release(
    tx: &mut Tx,
    r: &crate::core::types::TaskReleaseData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Acquired || version != r.version {
        return Reply::err(409, "Task version mismatch or invalid state");
    }
    // Releasing hands the task back unclaimed at the *same* version — only a
    // claim bumps it — so the next worker acquires with the version it saw.
    let t = tx.doc.tasks.get_mut(&r.id).expect("checked above");
    t.state = TaskState::Pending;
    t.pid = None;
    t.ttl = None;
    t.arm_retry(now + cfg.retry_timeout);
    tx.send_execute(&r.id, version);
    Reply::status(200, serde_json::json!({}))
}

fn op_task_fulfill(
    tx: &mut Tx,
    r: &crate::core::types::TaskFulfillData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    let action = &r.action.data;
    try_timeout(tx, &[&action.id], now, cfg);
    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Acquired || version != r.version {
        return Reply::err(409, "Task version mismatch or invalid state");
    }
    let pending = match tx.doc.promises.get(&action.id) {
        Some(p) => p.state == PromiseState::Pending,
        None => return Reply::err(404, "Promise not found"),
    };
    if !pending {
        // Unreachable while the invariants hold — an acquired task's promise is
        // pending — but the SQL path fulfils the task regardless, so mirror it.
        trigger_fulfilled(tx, &r.id);
        return Reply::ok(&TaskFulfillResponseData {
            promise: tx.doc.promises[&action.id].to_record(&action.id),
        });
    }
    let promise = settle(tx, &action.id, action.state, &action.value, now, cfg);
    Reply::ok(&TaskFulfillResponseData { promise })
}

/// `task.suspend` parks a task on a set of promises — unless one of them has
/// already settled, in which case there is nothing to wait for and the caller
/// is told to carry on (300).
fn op_task_suspend(
    tx: &mut Tx,
    r: &crate::core::types::TaskSuspendData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    let mut named: Vec<&str> = vec![r.id.as_str()];
    for action in &r.actions {
        named.push(action.data.awaited.as_str());
    }
    try_timeout(tx, &named, now, cfg);

    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Acquired || version != r.version {
        return Reply::err(409, "Task is not acquired or version mismatch");
    }
    for action in &r.actions {
        if !tx.doc.promises.contains_key(&action.data.awaited) {
            return Reply::err(422, "Awaited promise not found");
        }
    }
    let mut awaited: Vec<String> = Vec::new();
    for action in &r.actions {
        if !awaited.contains(&action.data.awaited) {
            awaited.push(action.data.awaited.clone());
        }
    }
    let any_settled = awaited
        .iter()
        .any(|id| tx.doc.promises[id].state != PromiseState::Pending);

    // Either way the resumes buffered by a previous suspension are stale.
    tx.doc
        .tasks
        .get_mut(&r.id)
        .expect("checked above")
        .resumes
        .clear();

    if any_settled {
        return Reply::status(
            300,
            serde_json::to_value(TaskSuspendPreloadData {
                preload: preload(&tx.doc, &r.id),
            })
            .expect("preload serializes"),
        );
    }
    for id in &awaited {
        register_callback(tx, id, &r.id);
    }
    let t = tx.doc.tasks.get_mut(&r.id).expect("checked above");
    t.state = TaskState::Suspended;
    t.pid = None;
    t.ttl = None;
    t.disarm();
    Reply::status(200, serde_json::json!({}))
}

/// `task.fence` runs one promise operation under the task's version, so a
/// worker that lost its lease cannot write.
fn op_task_fence(
    tx: &mut Tx,
    r: &crate::core::types::TaskFenceData,
    corr_id: &str,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    let action_id = r
        .action
        .data
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    try_timeout(tx, &[&r.id, &action_id], now, cfg);

    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Acquired || version != r.version {
        return Reply::err(409, "Version mismatch");
    }

    match r.action.kind.as_str() {
        "promise.create" => {
            let create: PromiseCreateData = match serde_json::from_value(r.action.data.clone()) {
                Ok(d) => d,
                Err(e) => return Reply::err(400, &format!("Invalid action data: {}", e)),
            };
            if let Err(e) = create.validate() {
                return Reply::err(400, &format_validation_errors(&e));
            }
            if let Some(addr) = create.tags.get(TAG_TARGET) {
                if !is_valid_address(addr) {
                    return Reply::err(400, "Invalid resonate:target address");
                }
            }
            let record = match tx.doc.promises.get(&create.id) {
                Some(p) => p.to_record(&create.id),
                None => create_promise(tx, &create.id, &create, now, cfg),
            };
            fence_reply(tx, &r.id, &r.action.kind, corr_id, 200, json!({ "promise": record }))
        }
        "promise.settle" => {
            let settle_data: crate::core::types::PromiseSettleData =
                match serde_json::from_value(r.action.data.clone()) {
                    Ok(d) => d,
                    Err(e) => return Reply::err(400, &format!("Invalid action data: {}", e)),
                };
            if let Err(e) = settle_data.validate() {
                return Reply::err(400, &format_validation_errors(&e));
            }
            let pending = tx
                .doc
                .promises
                .get(&settle_data.id)
                .is_some_and(|p| p.state == PromiseState::Pending);
            if pending {
                settle(
                    tx,
                    &settle_data.id,
                    settle_data.state,
                    &settle_data.value,
                    now,
                    cfg,
                );
            }
            let (status, data) = match tx.doc.promises.get(&settle_data.id) {
                Some(p) => (200, json!({ "promise": p.to_record(&settle_data.id) })),
                None => (404, json!("Promise not found")),
            };
            fence_reply(tx, &r.id, &r.action.kind, corr_id, status, data)
        }
        _ => Reply::err(400, "Invalid fence action kind"),
    }
}

/// The fence check on its own: does this caller still hold the task?
///
/// Reports the preload on success, because the preload is drawn from the
/// *task's* document and the action the shell is about to apply is not.
///
/// The rejections are `op_task_fence`'s, in the same order, so a caller cannot
/// tell from the status whether the action was in this origin or another.
fn op_task_fence_prepare(
    tx: &mut Tx,
    r: &crate::kernel::state::TaskFencePrepareData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Acquired || version != r.version {
        return Reply::err(409, "Version mismatch");
    }
    Reply::status(200, json!({ "preload": preload(&tx.doc, &r.id) }))
}

/// Wrap a fenced action's outcome as a nested response envelope.
fn fence_reply(
    tx: &Tx,
    task_id: &str,
    kind: &str,
    corr_id: &str,
    status: i32,
    data: serde_json::Value,
) -> Reply {
    Reply::ok(&TaskFenceResponseData {
        action: json!({
            "kind": kind,
            "head": { "corrId": corr_id, "status": status, "version": PROTOCOL_VERSION },
            "data": data,
        }),
        preload: preload(&tx.doc, task_id),
    })
}

/// A heartbeat extends the lease of every task in the batch the caller still
/// owns, and silently ignores the rest — it is a liveness signal, not a query.
fn op_task_heartbeat(tx: &mut Tx, r: &crate::core::types::TaskHeartbeatData, now: i64) -> Reply {
    for want in &r.tasks {
        let ttl = tx
            .doc
            .tasks
            .get(&want.id)
            .filter(|t| {
                t.state == TaskState::Acquired
                    && t.version == want.version
                    && t.pid.as_deref() == Some(r.pid.as_str())
            })
            .and_then(|t| t.ttl);
        if let Some(ttl) = ttl {
            tx.doc
                .tasks
                .get_mut(&want.id)
                .expect("checked above")
                .arm_lease(now + ttl);
        }
    }
    Reply::status(200, serde_json::json!({}))
}

fn op_task_halt(
    tx: &mut Tx,
    r: &crate::core::types::TaskHaltData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let state = match tx.doc.tasks.get(&r.id) {
        Some(t) => t.state,
        None => return Reply::err(404, "Task not found"),
    };
    if state == TaskState::Fulfilled {
        return Reply::err(409, "Task is fulfilled");
    }
    if state == TaskState::Halted {
        return Reply::status(200, serde_json::json!({}));
    }
    let t = tx.doc.tasks.get_mut(&r.id).expect("checked above");
    t.state = TaskState::Halted;
    t.pid = None;
    t.ttl = None;
    t.disarm();
    Reply::status(200, serde_json::json!({}))
}

fn op_task_continue(
    tx: &mut Tx,
    r: &crate::core::types::TaskContinueData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    try_timeout(tx, &[&r.id], now, cfg);
    let (state, version) = match tx.doc.tasks.get(&r.id) {
        Some(t) => (t.state, t.version),
        None => return Reply::err(404, "Task not found"),
    };
    if state != TaskState::Halted {
        return Reply::err(409, "Task is not halted");
    }
    let t = tx.doc.tasks.get_mut(&r.id).expect("checked above");
    t.state = TaskState::Pending;
    t.arm_retry(now + cfg.retry_timeout);
    tx.send_execute(&r.id, version);
    Reply::status(200, serde_json::json!({}))
}

/// Claim a pending task: bump the version, take the lease, and drop the
/// resumes the previous run buffered.
///
/// The version bump is the fence — every later write by the previous holder
/// fails its version check.
pub(crate) fn acquire(tx: &mut Tx, id: &str, ttl: i64, pid: &str, now: i64) -> TaskRecord {
    let t = tx.doc.tasks.get_mut(id).expect("caller checked presence");
    t.state = TaskState::Acquired;
    t.version += 1;
    t.pid = Some(pid.to_string());
    t.ttl = Some(ttl);
    t.resumes.clear();
    t.arm_lease(now + ttl);
    t.to_record(id)
}

// ============================================================================
// Schedules
// ============================================================================

/// Create the promise a due schedule owes, if it is not already there.
///
/// Two stamps differ from an ordinary create, and both come from
/// `process_schedule_timeout` (`persistence_sqlite.rs:1329-1400`):
///
/// - `created_at` is the *occurrence*, not `now`, and stays the occurrence even
///   when the promise is born settled — where an ordinary create would stamp
///   the deadline. A schedule's promise is dated by the tick it represents.
/// - the first dispatch is timed from `now`, the sweep that noticed, not from
///   `created_at` — an occurrence noticed late is retried from when it was
///   noticed.
///
/// Idempotent on the promise id, which is what lets the firing path create the
/// promise before advancing the schedule: a crash in between refires, and the
/// second attempt finds the promise already there.
fn op_schedule_fire(
    tx: &mut Tx,
    r: &crate::kernel::state::ScheduleFireData,
    now: i64,
    cfg: &KernelCfg,
) -> Reply {
    if tx.doc.promises.contains_key(&r.id) {
        return Reply::status(200, json!({}));
    }
    let already_timedout = now >= r.timeout_at;
    let mut promise = PromiseDoc {
        state: PromiseState::Pending,
        param: r.param.clone(),
        value: PromiseValue::default(),
        tags: r.tags.clone(),
        timeout_at: r.timeout_at,
        created_at: r.fired_at,
        settled_at: None,
        callbacks: Vec::new(),
        listeners: Vec::new(),
    };
    if already_timedout {
        promise.state = promise.timeout_state();
        promise.settled_at = Some(r.timeout_at);
    }
    let has_target = promise.target().is_some();
    tx.doc.promises.insert(r.id.clone(), promise);

    if !has_target {
        return Reply::status(200, json!({}));
    }
    let mut task = TaskDoc {
        state: TaskState::Fulfilled,
        version: 0,
        pid: None,
        ttl: None,
        resumes: BTreeSet::new(),
        retry_at: None,
        lease_at: None,
    };
    if already_timedout {
        tx.doc.tasks.insert(r.id.clone(), task);
        return Reply::status(200, json!({}));
    }
    task.state = TaskState::Pending;
    task.arm_retry(now + cfg.retry_timeout);
    tx.doc.tasks.insert(r.id.clone(), task);
    tx.send_execute(&r.id, 0);
    Reply::status(200, json!({}))
}

// ============================================================================
// Shared state transitions
// ============================================================================

/// Create a promise and, when it carries a `resonate:target`, its task.
///
/// Mirrors `Server::op_promise_create` + `Db::promise_create`
/// (`persistence_sqlite.rs:434-500`): a promise created past its own deadline
/// is born settled — resolved if it is a timer, timed out otherwise — with
/// `created_at` and `settled_at` both stamped at the deadline, and its task
/// born already fulfilled.
pub(crate) fn create_promise(
    tx: &mut Tx,
    id: &str,
    r: &PromiseCreateData,
    now: i64,
    cfg: &KernelCfg,
) -> PromiseRecord {
    let record = insert_promise(tx, id, r, now);
    let p = &tx.doc.promises[id];
    if p.target().is_none() {
        // No target means no task and no armed deadline: such a promise only
        // ever expires lazily, when someone reads it.
        return record;
    }
    let settled = p.state != PromiseState::Pending;
    let created_at = p.created_at;
    let delay_at = p.tags.get(TAG_DELAY).and_then(|v| v.parse::<i64>().ok());

    let mut task = TaskDoc {
        state: TaskState::Fulfilled,
        version: 0,
        pid: None,
        ttl: None,
        resumes: BTreeSet::new(),
        retry_at: None,
        lease_at: None,
    };
    if settled {
        // Born settled, so its task is born done.
        tx.doc.tasks.insert(id.to_string(), task);
        return record;
    }
    task.state = TaskState::Pending;
    // `resonate:delay` is an absolute instant before which the task must not be
    // dispatched: arm the retry timer there and send nothing. This follows
    // `src/oracle.rs:281-299`; the SQL backends do not implement it.
    match delay_at {
        Some(at) if now < at => {
            task.arm_retry(at);
            tx.doc.tasks.insert(id.to_string(), task);
        }
        _ => {
            task.arm_retry(created_at + cfg.retry_timeout);
            tx.doc.tasks.insert(id.to_string(), task);
            tx.send_execute(id, 0);
        }
    }
    record
}

/// Insert the promise row alone, with no task and no dispatch.
///
/// A promise created past its own deadline is born settled — resolved if it is
/// a timer, timed out otherwise — with `created_at` and `settled_at` both
/// stamped at the deadline rather than at `now`.
pub(crate) fn insert_promise(
    tx: &mut Tx,
    id: &str,
    r: &PromiseCreateData,
    now: i64,
) -> PromiseRecord {
    let already_timedout = now >= r.timeout_at;
    let mut doc = PromiseDoc {
        state: PromiseState::Pending,
        param: r.param.clone(),
        value: PromiseValue::default(),
        tags: r.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        timeout_at: r.timeout_at,
        created_at: if already_timedout { r.timeout_at } else { now },
        settled_at: None,
        callbacks: Vec::new(),
        listeners: Vec::new(),
    };
    if already_timedout {
        doc.state = doc.timeout_state();
        doc.settled_at = Some(r.timeout_at);
    }
    let record = doc.to_record(id);
    tx.doc.promises.insert(id.to_string(), doc);
    record
}

/// Settle a pending promise and run its settlement chain.
///
/// Returns the settled record as the caller must report it — captured before
/// the chain runs, which only touches registrations, never the promise's own
/// fields.
pub(crate) fn settle(
    tx: &mut Tx,
    id: &str,
    state: SettleState,
    value: &PromiseValue,
    now: i64,
    cfg: &KernelCfg,
) -> PromiseRecord {
    {
        let p = tx.doc.promises.get_mut(id).expect("caller checked presence");
        debug_assert_eq!(p.state, PromiseState::Pending);
        p.state = match state {
            SettleState::Resolved => PromiseState::Resolved,
            SettleState::Rejected => PromiseState::Rejected,
            SettleState::RejectedCanceled => PromiseState::RejectedCanceled,
        };
        p.value = value.clone();
        p.settled_at = Some(now);
    }
    let record = tx.doc.promises[id].to_record(id);
    trigger_settlement(tx, id, now, cfg);
    record
}

/// Settle every named promise whose deadline has passed.
///
/// `Db::try_timeout`: a timer resolves, everything else becomes
/// `rejected_timedout`, and `settled_at` is the *deadline*, not `now` — so the
/// record is identical whenever the expiry is noticed.
pub(crate) fn try_timeout(tx: &mut Tx, ids: &[&str], now: i64, cfg: &KernelCfg) {
    let expired: Vec<(String, PromiseState, i64)> = ids
        .iter()
        .filter_map(|id| {
            tx.doc
                .promises
                .get(*id)
                .filter(|p| p.state == PromiseState::Pending && now >= p.timeout_at)
                .map(|p| (id.to_string(), p.timeout_state(), p.timeout_at))
        })
        .collect();
    for (id, state, timeout_at) in expired {
        if let Some(p) = tx.doc.promises.get_mut(&id) {
            p.state = state;
            p.settled_at = Some(timeout_at);
        }
        trigger_settlement(tx, &id, now, cfg);
    }
}

/// The settlement chain: fulfil the promise's own task, wake its awaiters,
/// notify its listeners.
///
/// One pass, in this order, matching `settle_promise`
/// (`persistence_sqlite.rs:314-340`): `settlement_enqueued`, then
/// `resumption_enqueued`, then `listener_unblocked`.
pub(crate) fn trigger_settlement(tx: &mut Tx, id: &str, now: i64, cfg: &KernelCfg) {
    trigger_fulfilled(tx, id);
    trigger_callbacks(tx, id, now, cfg);
    trigger_listeners(tx, id);
}

/// `settlement_enqueued`: the settled promise's own task is done, and its
/// registrations against other promises are dropped.
pub(crate) fn trigger_fulfilled(tx: &mut Tx, id: &str) {
    let live = tx
        .doc
        .tasks
        .get(id)
        .is_some_and(|t| t.state != TaskState::Fulfilled);
    if !live {
        return;
    }
    let t = tx.doc.tasks.get_mut(id).expect("checked above");
    t.state = TaskState::Fulfilled;
    t.pid = None;
    t.ttl = None;
    t.resumes.clear();
    t.disarm();
    // DELETE FROM callbacks WHERE awaiter_id = id — a finished task waits on
    // nothing.
    for p in tx.doc.promises.values_mut() {
        p.callbacks.retain(|awaiter| awaiter != id);
    }
}

/// `resumption_enqueued`: every awaiter registered against `awaited` observes
/// the settlement, in registration order.
pub(crate) fn trigger_callbacks(tx: &mut Tx, awaited: &str, now: i64, cfg: &KernelCfg) {
    let awaiters = match tx.doc.promises.get_mut(awaited) {
        Some(p) => std::mem::take(&mut p.callbacks),
        None => return,
    };
    for awaiter in awaiters {
        let live = tx
            .doc
            .promises
            .get(&awaiter)
            .is_some_and(|p| p.state == PromiseState::Pending && now < p.timeout_at);
        if !live {
            // The awaiter is itself settled or already past its deadline; this
            // sweep will fulfil it rather than resume it.
            continue;
        }
        resume_awaiter(tx, &awaiter, awaited, now, cfg, Wake::Fanout);
    }
}

/// Which path is waking an awaiter. They agree on everything but one case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wake {
    /// A settlement fanning out to everything registered against it.
    ///
    /// `resumption_enqueued` marks *every* callback of the settled promise
    /// ready, whatever state the awaiter's task is in, so a halted awaiter
    /// buffers the resume and sees it when it continues.
    Fanout,
    /// A registration against a promise that has already settled.
    ///
    /// `promise_register_callback`'s ready-callback insert is guarded on the
    /// awaiter's task being pending or acquired
    /// (`persistence_sqlite.rs:592-599`), so a halted awaiter buffers nothing.
    Registration,
}

/// Wake one awaiter because `awaited` settled.
///
/// A suspended task goes back to pending and is re-dispatched; a task that is
/// already running only records the resume, which is what `resumes` counts.
pub(crate) fn resume_awaiter(
    tx: &mut Tx,
    awaiter: &str,
    awaited: &str,
    now: i64,
    cfg: &KernelCfg,
    wake: Wake,
) {
    let state = match tx.doc.tasks.get(awaiter) {
        Some(t) => t.state,
        None => return,
    };
    match state {
        TaskState::Suspended => {
            let version = {
                let t = tx.doc.tasks.get_mut(awaiter).expect("checked above");
                t.state = TaskState::Pending;
                t.resumes.clear();
                t.resumes.insert(awaited.to_string());
                t.arm_retry(now + cfg.retry_timeout);
                t.version
            };
            tx.send_execute(awaiter, version);
        }
        TaskState::Pending | TaskState::Acquired => {
            let t = tx.doc.tasks.get_mut(awaiter).expect("checked above");
            t.resumes.insert(awaited.to_string());
        }
        TaskState::Halted if wake == Wake::Fanout => {
            let t = tx.doc.tasks.get_mut(awaiter).expect("checked above");
            t.resumes.insert(awaited.to_string());
        }
        // A halted task registering after the fact buffers nothing, and a
        // fulfilled one is done either way.
        TaskState::Halted | TaskState::Fulfilled => {}
    }
}

/// Register `awaiter` against `awaited`, keeping registration order and
/// rejecting duplicates — `callbacks`' primary key is `(awaited, awaiter)`.
pub(crate) fn register_callback(tx: &mut Tx, awaited: &str, awaiter: &str) {
    let has_target = tx
        .doc
        .promises
        .get(awaiter)
        .is_some_and(|p| p.target().is_some());
    if !has_target {
        return;
    }
    let p = match tx.doc.promises.get_mut(awaited) {
        Some(p) => p,
        None => return,
    };
    if !p.callbacks.iter().any(|a| a == awaiter) {
        p.callbacks.push(awaiter.to_string());
    }
}

/// `listener_unblocked`: hand the settled promise to everyone listening, then
/// forget them.
pub(crate) fn trigger_listeners(tx: &mut Tx, id: &str) {
    let listeners = match tx.doc.promises.get_mut(id) {
        Some(p) => std::mem::take(&mut p.listeners),
        None => return,
    };
    if listeners.is_empty() {
        return;
    }
    let promise = tx.doc.promises[id].to_record(id);
    for address in listeners {
        tx.sends.push((
            address,
            OutEntry::Unblock {
                promise_id: id.to_string(),
                promise: promise.clone(),
            },
        ));
    }
}

/// The promises a worker is handed alongside a task: everything sharing the
/// task promise's `resonate:branch`, itself excluded.
///
/// `Db::compute_preload` (`persistence_sqlite.rs:1176-1200`). A branch is
/// always a prefix of the ids under it, so a branch never spans origins and
/// this stays a single-document read.
pub(crate) fn preload(doc: &OriginDoc, id: &str) -> Vec<PromiseRecord> {
    let branch = match doc.promises.get(id).and_then(|p| p.tags.get(TAG_BRANCH)) {
        Some(b) if !b.is_empty() => b.clone(),
        _ => return Vec::new(),
    };
    doc.promises
        .iter()
        .filter(|(other, p)| {
            other.as_str() != id && p.tags.get(TAG_BRANCH).is_some_and(|b| *b == branch)
        })
        .map(|(other, p)| p.to_record(other))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::state::{apply_effects, TAG_TIMER};
    use serde_json::json;

    const W: &str = "http://worker:9999";

    fn cfg() -> KernelCfg {
        KernelCfg {
            retry_timeout: 30_000,
        }
    }

    fn parse<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
        serde_json::from_value(v).expect("test fixture deserializes")
    }

    /// Apply a request and return the new document, the sends, and the reply.
    fn step(doc: &OriginDoc, req: Req, now: i64) -> (OriginDoc, Vec<(String, OutEntry)>, Reply) {
        let (fx, reply) = handle(doc, &req, now, &cfg());
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        let sends = fx
            .into_iter()
            .filter_map(|e| match e {
                Effect::Send { address, out } => Some((address, out)),
                _ => None,
            })
            .collect();
        (next, sends, reply)
    }

    fn create(id: &str, timeout_at: i64, tags: serde_json::Value) -> Req {
        Req::PromiseCreate(parse(
            json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags }),
        ))
    }

    fn get(id: &str) -> Req {
        Req::PromiseGet(parse(json!({ "id": id })))
    }

    fn settle_req(id: &str, state: &str) -> Req {
        Req::PromiseSettle(parse(json!({ "id": id, "state": state, "value": {} })))
    }

    fn callback(awaited: &str, awaiter: &str) -> Req {
        Req::PromiseRegisterCallback(parse(json!({ "awaited": awaited, "awaiter": awaiter })))
    }

    fn listener(awaited: &str, address: &str) -> Req {
        Req::PromiseRegisterListener(parse(json!({ "awaited": awaited, "address": address })))
    }

    /// Park a task in `suspended` — the state `task.suspend` produces, which
    /// this commit cannot reach yet.
    fn suspend(doc: &mut OriginDoc, id: &str) {
        let t = doc.tasks.get_mut(id).expect("task exists");
        t.state = TaskState::Suspended;
        t.disarm();
    }

    /// A document holding one pending, targeted promise `o:a` created at 0 with
    /// deadline 100_000 — so it has a task, a retry timer, and one dispatch.
    fn with_targeted(id: &str, timeout_at: i64) -> OriginDoc {
        let (doc, _, reply) = step(
            &OriginDoc::default(),
            create(id, timeout_at, json!({ "resonate:target": W })),
            0,
        );
        assert_eq!(reply.status, 200);
        doc
    }

    // --- create ------------------------------------------------------------

    #[test]
    fn creating_an_untargeted_promise_makes_no_task_and_arms_no_timer() {
        let (doc, sends, reply) = step(&OriginDoc::default(), create("o:a", 100, json!({})), 0);
        assert_eq!(reply.status, 200);
        assert_eq!(doc.promises["o:a"].state, PromiseState::Pending);
        assert!(doc.tasks.is_empty());
        assert_eq!(doc.timer_at, None);
        assert!(sends.is_empty());
    }

    #[test]
    fn creating_a_targeted_promise_arms_a_retry_timer_and_dispatches() {
        let (doc, sends, _) = step(
            &OriginDoc::default(),
            create("o:a", 100_000, json!({ "resonate:target": W })),
            1_000,
        );
        let task = &doc.tasks["o:a"];
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.version, 0);
        assert_eq!(task.retry_at, Some(31_000));
        assert_eq!(task.lease_at, None);
        // The armed set is the retry timer and the promise deadline; the timer
        // object sits at the nearer of the two.
        assert_eq!(doc.timer_at, Some(31_000));
        assert_eq!(
            sends,
            vec![(
                W.to_string(),
                OutEntry::Execute {
                    task_id: "o:a".into(),
                    version: 0
                }
            )]
        );
    }

    #[test]
    fn a_promise_created_past_its_deadline_is_born_timed_out() {
        let (doc, sends, reply) = step(
            &OriginDoc::default(),
            create("o:a", 500, json!({ "resonate:target": W })),
            900,
        );
        let p = &doc.promises["o:a"];
        assert_eq!(p.state, PromiseState::RejectedTimedout);
        // Both stamps are the deadline, not `now`: the record reads the same
        // whenever the expiry is noticed.
        assert_eq!((p.created_at, p.settled_at), (500, Some(500)));
        assert_eq!(doc.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(doc.timer_at, None);
        assert!(sends.is_empty());
        assert_eq!(reply.data["promise"]["state"], "rejected_timedout");
    }

    #[test]
    fn a_timer_promise_created_past_its_deadline_resolves_instead() {
        let (doc, _, _) = step(
            &OriginDoc::default(),
            create("o:a", 500, json!({ TAG_TIMER: "true" })),
            900,
        );
        assert_eq!(doc.promises["o:a"].state, PromiseState::Resolved);
    }

    #[test]
    fn create_is_idempotent_on_the_id_alone() {
        let doc = with_targeted("o:a", 100_000);
        let (next, sends, reply) = step(
            &doc,
            create("o:a", 999_999, json!({ "resonate:target": W })),
            1,
        );
        assert_eq!(reply.status, 200);
        // The stored promise wins: the second create's timeout is ignored.
        assert_eq!(reply.data["promise"]["timeoutAt"], 100_000);
        assert_eq!(next, doc);
        assert!(sends.is_empty());
    }

    #[test]
    fn create_rejects_a_target_that_is_not_an_address() {
        let (doc, _, reply) = step(
            &OriginDoc::default(),
            create("o:a", 100, json!({ "resonate:target": "not a url" })),
            0,
        );
        assert_eq!(reply.status, 400);
        assert_eq!(reply.data, json!("Invalid resonate:target address"));
        assert!(doc.promises.is_empty());
    }

    #[test]
    fn a_delay_tag_defers_the_first_dispatch_to_its_instant() {
        // resonate:delay is an absolute instant, per src/oracle.rs:281-299.
        let (doc, sends, _) = step(
            &OriginDoc::default(),
            create(
                "o:a",
                100_000,
                json!({ "resonate:target": W, "resonate:delay": "5000" }),
            ),
            1_000,
        );
        assert_eq!(doc.tasks["o:a"].retry_at, Some(5_000));
        assert!(sends.is_empty(), "a delayed task is not dispatched yet");
        assert_eq!(doc.timer_at, Some(5_000));
    }

    #[test]
    fn a_delay_already_past_dispatches_immediately() {
        let (doc, sends, _) = step(
            &OriginDoc::default(),
            create(
                "o:a",
                100_000,
                json!({ "resonate:target": W, "resonate:delay": "500" }),
            ),
            1_000,
        );
        assert_eq!(doc.tasks["o:a"].retry_at, Some(31_000));
        assert_eq!(sends.len(), 1);
    }

    // --- get ---------------------------------------------------------------

    #[test]
    fn getting_an_unknown_promise_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), get("o:a"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Promise not found"));
    }

    #[test]
    fn getting_an_expired_promise_settles_it_first() {
        let doc = with_targeted("o:a", 1_000);
        let (next, _, reply) = step(&doc, get("o:a"), 1_500);
        assert_eq!(reply.data["promise"]["state"], "rejected_timedout");
        assert_eq!(reply.data["promise"]["settledAt"], 1_000);
        // The read is a real transition: the task is fulfilled and the origin's
        // timer disarmed.
        assert_eq!(next.promises["o:a"].state, PromiseState::RejectedTimedout);
        assert_eq!(next.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(next.timer_at, None);
    }

    #[test]
    fn getting_a_live_promise_changes_nothing() {
        let doc = with_targeted("o:a", 100_000);
        let (next, sends, reply) = step(&doc, get("o:a"), 500);
        assert_eq!(reply.status, 200);
        assert_eq!(next, doc);
        assert!(sends.is_empty());
    }

    // --- settle ------------------------------------------------------------

    #[test]
    fn settling_an_unknown_promise_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), settle_req("o:a", "resolved"), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn settling_stamps_now_and_fulfils_the_promises_own_task() {
        let doc = with_targeted("o:a", 100_000);
        let (next, _, reply) = step(&doc, settle_req("o:a", "resolved"), 700);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert_eq!(reply.data["promise"]["settledAt"], 700);
        assert_eq!(next.tasks["o:a"].state, TaskState::Fulfilled);
        assert_eq!(next.timer_at, None);
    }

    #[test]
    fn settling_twice_reports_the_first_settlement() {
        let doc = with_targeted("o:a", 100_000);
        let (once, _, _) = step(&doc, settle_req("o:a", "resolved"), 700);
        let (twice, sends, reply) = step(&once, settle_req("o:a", "rejected"), 800);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert_eq!(reply.data["promise"]["settledAt"], 700);
        assert_eq!(twice, once);
        assert!(sends.is_empty());
    }

    #[test]
    fn settling_unblocks_listeners_and_forgets_them() {
        let doc = with_targeted("o:a", 100_000);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 1);
        let (doc, _, _) = step(&doc, listener("o:a", "http://two"), 2);
        assert_eq!(doc.promises["o:a"].listeners, vec!["http://one", "http://two"]);

        let (next, sends, _) = step(&doc, settle_req("o:a", "resolved"), 700);
        assert_eq!(sends.len(), 2);
        // Registration order is preserved on the way out.
        assert_eq!(sends[0].0, "http://one");
        assert_eq!(sends[1].0, "http://two");
        match &sends[0].1 {
            OutEntry::Unblock { promise_id, promise } => {
                assert_eq!(promise_id, "o:a");
                assert_eq!(promise.state, PromiseState::Resolved);
            }
            other => panic!("expected unblock, got {other:?}"),
        }
        assert!(next.promises["o:a"].listeners.is_empty());
    }

    #[test]
    fn settling_fans_out_to_awaiters_in_registration_order() {
        // Two suspended awaiters, registered in a known order, both waiting on
        // the same promise: the dispatches must come back in that order.
        let mut doc = with_targeted("o:awaited", 100_000);
        for awaiter in ["o:z", "o:a"] {
            let (next, _, _) = step(
                &doc,
                create(awaiter, 100_000, json!({ "resonate:target": W })),
                0,
            );
            doc = next;
            suspend(&mut doc, awaiter);
            let (next, _, reply) = step(&doc, callback("o:awaited", awaiter), 1);
            assert_eq!(reply.status, 200);
            doc = next;
        }
        assert_eq!(doc.promises["o:awaited"].callbacks, vec!["o:z", "o:a"]);

        let (next, sends, _) = step(&doc, settle_req("o:awaited", "resolved"), 700);
        let executed: Vec<&str> = sends
            .iter()
            .filter_map(|(_, o)| match o {
                OutEntry::Execute { task_id, .. } => Some(task_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(executed, vec!["o:z", "o:a"], "fanout follows registration");
        for awaiter in ["o:z", "o:a"] {
            let t = &next.tasks[awaiter];
            assert_eq!(t.state, TaskState::Pending);
            assert_eq!(t.retry_at, Some(30_700));
            assert_eq!(t.resumes.iter().collect::<Vec<_>>(), vec!["o:awaited"]);
        }
        assert!(next.promises["o:awaited"].callbacks.is_empty());
    }

    #[test]
    fn a_settled_awaiter_is_not_resumed() {
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        suspend(&mut doc, "o:awaiter");
        let (next, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        // Settle the awaiter itself, then the awaited promise.
        let (next, _, _) = step(&next, settle_req("o:awaiter", "resolved"), 2);
        let (next, sends, _) = step(&next, settle_req("o:awaited", "resolved"), 3);
        assert!(sends.is_empty());
        assert_eq!(next.tasks["o:awaiter"].state, TaskState::Fulfilled);
    }

    #[test]
    fn fulfilling_a_task_drops_its_registrations_against_others() {
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        suspend(&mut doc, "o:awaiter");
        let (doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        assert_eq!(doc.promises["o:awaited"].callbacks, vec!["o:awaiter"]);

        // Settling the awaiter fulfils its task, which drops the registration.
        let (next, _, _) = step(&doc, settle_req("o:awaiter", "resolved"), 2);
        assert!(next.promises["o:awaited"].callbacks.is_empty());
    }

    #[test]
    fn a_halted_awaiter_buffers_a_resume_when_a_settlement_fans_out() {
        // resumption_enqueued marks every callback of the settled promise
        // ready, whatever the awaiter's task is doing, so a halted awaiter sees
        // the resume when it continues.
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        suspend(&mut doc, "o:awaiter");
        let (mut doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        doc.tasks.get_mut("o:awaiter").unwrap().state = TaskState::Halted;

        let (next, sends, _) = step(&doc, settle_req("o:awaited", "resolved"), 2);
        assert!(sends.is_empty(), "a halted task is not re-dispatched");
        assert_eq!(
            next.tasks["o:awaiter"].resumes.iter().collect::<Vec<_>>(),
            vec!["o:awaited"]
        );
        assert_eq!(next.tasks["o:awaiter"].state, TaskState::Halted);
    }

    #[test]
    fn a_halted_awaiter_registering_after_the_fact_buffers_nothing() {
        // The other guard: promise_register_callback's ready-callback insert
        // requires the awaiter's task to be pending or acquired.
        let doc = with_targeted("o:awaited", 100_000);
        let (mut doc, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc.tasks.get_mut("o:awaiter").unwrap().state = TaskState::Halted;
        doc.tasks.get_mut("o:awaiter").unwrap().disarm();
        let (doc, _, _) = step(&doc, settle_req("o:awaited", "resolved"), 1);

        let (next, sends, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(reply.status, 200);
        assert!(sends.is_empty());
        assert!(next.tasks["o:awaiter"].resumes.is_empty());
    }

    // --- register_callback -------------------------------------------------

    #[test]
    fn registering_against_an_unknown_awaited_is_a_404() {
        let doc = with_targeted("o:awaiter", 100_000);
        let (_, _, reply) = step(&doc, callback("o:missing", "o:awaiter"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Awaited promise not found"));
    }

    #[test]
    fn registering_an_unknown_awaiter_is_a_422() {
        let doc = with_targeted("o:awaited", 100_000);
        let (_, _, reply) = step(&doc, callback("o:awaited", "o:missing"), 0);
        assert_eq!(reply.status, 422);
        assert_eq!(reply.data, json!("Awaiter promise not found"));
    }

    #[test]
    fn an_awaiter_without_a_target_cannot_register() {
        let doc = with_targeted("o:awaited", 100_000);
        let (doc, _, _) = step(&doc, create("o:awaiter", 100_000, json!({})), 0);
        let (next, _, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 0);
        assert_eq!(reply.status, 422);
        assert_eq!(reply.data, json!("Awaiter promise has no resonate:target tag"));
        assert!(next.promises["o:awaited"].callbacks.is_empty());
    }

    #[test]
    fn registering_twice_registers_once() {
        let doc = with_targeted("o:awaited", 100_000);
        let (doc, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        let (doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 1);
        let (doc, _, _) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(doc.promises["o:awaited"].callbacks, vec!["o:awaiter"]);
    }

    #[test]
    fn registering_against_a_settled_promise_resumes_instead() {
        let mut doc = with_targeted("o:awaited", 100_000);
        let (next, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        doc = next;
        let (mut doc, _, _) = step(&doc, settle_req("o:awaited", "resolved"), 1);
        suspend(&mut doc, "o:awaiter");

        let (next, sends, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert!(next.promises["o:awaited"].callbacks.is_empty());
        let t = &next.tasks["o:awaiter"];
        assert_eq!(t.state, TaskState::Pending);
        assert_eq!(t.retry_at, Some(30_002));
        assert_eq!(sends.len(), 1);
    }

    #[test]
    fn registering_against_a_settled_promise_records_a_resume_for_a_running_task() {
        let doc = with_targeted("o:awaited", 100_000);
        let (doc, _, _) = step(
            &doc,
            create("o:awaiter", 100_000, json!({ "resonate:target": W })),
            0,
        );
        let (doc, _, _) = step(&doc, settle_req("o:awaited", "resolved"), 1);
        // The awaiter's task is still pending — already running, so it is not
        // re-dispatched; the resume is only recorded.
        let (next, sends, reply) = step(&doc, callback("o:awaited", "o:awaiter"), 2);
        assert_eq!(reply.status, 200);
        assert!(sends.is_empty());
        assert_eq!(
            next.tasks["o:awaiter"].resumes.iter().collect::<Vec<_>>(),
            vec!["o:awaited"]
        );
    }

    // --- register_listener -------------------------------------------------

    #[test]
    fn a_listener_address_must_be_an_address() {
        let doc = with_targeted("o:a", 100_000);
        let (next, _, reply) = step(&doc, listener("o:a", "not a url"), 0);
        assert_eq!(reply.status, 400);
        assert_eq!(reply.data, json!("Invalid listener address"));
        assert_eq!(next, doc);
    }

    #[test]
    fn listening_to_an_unknown_promise_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), listener("o:a", "http://one"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Awaited promise not found"));
    }

    #[test]
    fn listening_to_a_settled_promise_registers_nothing() {
        let doc = with_targeted("o:a", 100_000);
        let (doc, _, _) = step(&doc, settle_req("o:a", "resolved"), 1);
        let (next, sends, reply) = step(&doc, listener("o:a", "http://one"), 2);
        assert_eq!(reply.status, 200);
        assert!(next.promises["o:a"].listeners.is_empty());
        assert!(sends.is_empty());
    }

    #[test]
    fn listening_twice_from_one_address_registers_once() {
        let doc = with_targeted("o:a", 100_000);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 1);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 2);
        assert_eq!(doc.promises["o:a"].listeners, vec!["http://one"]);
    }

    // --- task.get ----------------------------------------------------------

    fn task_get(id: &str) -> Req {
        Req::TaskGet(parse(json!({ "id": id })))
    }

    fn task_create(id: &str, timeout_at: i64, ttl: i64) -> Req {
        Req::TaskCreate(parse(json!({
            "pid": PID, "ttl": ttl,
            "action": { "kind": "promise.create", "head": {}, "data": {
                "id": id, "timeoutAt": timeout_at, "param": {},
                "tags": { "resonate:target": W } } }
        })))
    }

    fn task_acquire(id: &str, version: i64, ttl: i64) -> Req {
        Req::TaskAcquire(parse(
            json!({ "id": id, "version": version, "pid": PID, "ttl": ttl }),
        ))
    }

    fn task_release(id: &str, version: i64) -> Req {
        Req::TaskRelease(parse(json!({ "id": id, "version": version })))
    }

    fn task_fulfill(id: &str, version: i64, state: &str) -> Req {
        Req::TaskFulfill(parse(json!({
            "id": id, "version": version,
            "action": { "kind": "promise.settle", "head": {}, "data": {
                "id": id, "state": state, "value": {} } }
        })))
    }

    fn task_suspend(id: &str, version: i64, awaited: &[&str]) -> Req {
        let actions: Vec<serde_json::Value> = awaited
            .iter()
            .map(|a| {
                json!({ "kind": "promise.register_callback", "head": {},
                        "data": { "awaited": a, "awaiter": id } })
            })
            .collect();
        Req::TaskSuspend(parse(
            json!({ "id": id, "version": version, "actions": actions }),
        ))
    }

    fn task_fence(id: &str, version: i64, action: serde_json::Value) -> Req {
        Req::TaskFence {
            data: parse(json!({ "id": id, "version": version, "action": action })),
            corr_id: "corr-1".to_string(),
        }
    }

    fn task_heartbeat(pid: &str, tasks: &[(&str, i64)]) -> Req {
        let tasks: Vec<serde_json::Value> = tasks
            .iter()
            .map(|(id, v)| json!({ "id": id, "version": v }))
            .collect();
        Req::TaskHeartbeat(parse(json!({ "pid": pid, "tasks": tasks })))
    }

    const PID: &str = "pid-1";
    const TTL: i64 = 60_000;

    /// A document with `o:t` created and acquired by `task.create` at time 0.
    fn with_acquired(id: &str) -> OriginDoc {
        let (doc, sends, reply) = step(&OriginDoc::default(), task_create(id, 100_000, TTL), 0);
        assert_eq!(reply.status, 200);
        assert!(sends.is_empty(), "task.create never dispatches");
        doc
    }

    #[test]
    fn getting_an_unknown_task_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), task_get("o:t"), 0);
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Task not found"));
    }

    #[test]
    fn getting_a_task_whose_promise_expired_reports_it_fulfilled() {
        let doc = with_acquired("o:t");
        let (next, _, reply) = step(&doc, task_get("o:t"), 200_000);
        assert_eq!(reply.data["task"]["state"], "fulfilled");
        assert_eq!(reply.data["task"].get("pid"), None);
        assert_eq!(reply.data["task"].get("ttl"), None);
        assert_eq!(next.timer_at, None);
    }

    // --- task.create -------------------------------------------------------

    #[test]
    fn task_create_hands_back_an_already_acquired_task() {
        let (doc, sends, reply) = step(&OriginDoc::default(), task_create("o:t", 100_000, TTL), 1_000);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["task"]["state"], "acquired");
        assert_eq!(reply.data["task"]["version"], 1);
        assert_eq!(reply.data["task"]["pid"], PID);
        assert_eq!(reply.data["promise"]["state"], "pending");
        // The caller is the worker, so nothing is dispatched.
        assert!(sends.is_empty());
        let t = &doc.tasks["o:t"];
        assert_eq!(t.lease_at, Some(61_000));
        assert_eq!(t.retry_at, None);
        // Both the lease and the promise deadline are armed; the nearer wins.
        assert_eq!(doc.timer_at, Some(61_000));
    }

    #[test]
    fn task_create_past_the_deadline_hands_back_a_fulfilled_task() {
        let (doc, sends, reply) = step(&OriginDoc::default(), task_create("o:t", 500, TTL), 900);
        assert_eq!(reply.data["task"]["state"], "fulfilled");
        assert_eq!(reply.data["task"]["version"], 0);
        assert_eq!(reply.data["promise"]["state"], "rejected_timedout");
        assert!(sends.is_empty());
        assert_eq!(doc.timer_at, None);
    }

    #[test]
    fn task_create_claims_a_pending_task_and_bumps_its_version() {
        // A targeted promise.create leaves a pending task at version 0.
        let doc = with_targeted("o:t", 100_000);
        let (next, sends, reply) = step(&doc, task_create("o:t", 999, TTL), 1_000);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["task"]["version"], 1);
        assert_eq!(reply.data["task"]["state"], "acquired");
        // The stored promise wins, so the action's own timeout is ignored.
        assert_eq!(reply.data["promise"]["timeoutAt"], 100_000);
        assert!(sends.is_empty());
        assert_eq!(next.tasks["o:t"].lease_at, Some(61_000));
    }

    #[test]
    fn task_create_on_a_claimed_task_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (_, _, reply) = step(&doc, task_create("o:t", 100_000, TTL), 1);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Already exists"));
    }

    #[test]
    fn task_create_on_a_promise_without_a_task_is_a_422() {
        let (doc, _, _) = step(&OriginDoc::default(), create("o:t", 100_000, json!({})), 0);
        let (_, _, reply) = step(&doc, task_create("o:t", 100_000, TTL), 1);
        assert_eq!(reply.status, 422);
        assert_eq!(
            reply.data,
            json!("The promise does not have a resonate:target tag")
        );
    }

    // --- task.acquire ------------------------------------------------------

    #[test]
    fn acquiring_an_unknown_task_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), task_acquire("o:t", 0, TTL), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn acquiring_a_task_that_is_not_pending_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (_, _, reply) = step(&doc, task_acquire("o:t", 1, TTL), 1);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Task is not pending"));
    }

    #[test]
    fn acquiring_at_the_wrong_version_is_a_conflict() {
        let doc = with_targeted("o:t", 100_000);
        let (_, _, reply) = step(&doc, task_acquire("o:t", 7, TTL), 1);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Version mismatch"));
    }

    #[test]
    fn acquiring_takes_the_lease_and_drops_buffered_resumes() {
        let doc = with_targeted("o:t", 100_000);
        let (mut doc, _, _) = step(&doc, create("o:x", 100_000, json!({})), 0);
        doc.tasks.get_mut("o:t").unwrap().resumes.insert("o:x".into());
        let (next, sends, reply) = step(&doc, task_acquire("o:t", 0, TTL), 1_000);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["task"]["version"], 1);
        assert_eq!(reply.data["task"]["resumes"], 0);
        assert!(sends.is_empty());
        let t = &next.tasks["o:t"];
        assert_eq!(t.state, TaskState::Acquired);
        assert_eq!(t.lease_at, Some(61_000));
        assert!(t.resumes.is_empty());
    }

    // --- task.release ------------------------------------------------------

    #[test]
    fn releasing_an_unknown_task_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), task_release("o:t", 1), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn releasing_at_the_wrong_version_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (_, _, reply) = step(&doc, task_release("o:t", 9), 1);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Task version mismatch or invalid state"));
    }

    #[test]
    fn releasing_re_dispatches_at_the_same_version() {
        let doc = with_acquired("o:t");
        let (next, sends, reply) = step(&doc, task_release("o:t", 1), 1_000);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data, json!({}));
        let t = &next.tasks["o:t"];
        assert_eq!(t.state, TaskState::Pending);
        assert_eq!((t.pid.as_deref(), t.ttl), (None, None));
        assert_eq!(t.retry_at, Some(31_000));
        // Only a claim bumps the version, so the next worker acquires at 1.
        assert_eq!(
            sends,
            vec![(
                W.to_string(),
                OutEntry::Execute {
                    task_id: "o:t".into(),
                    version: 1
                }
            )]
        );
    }

    // --- task.fulfill ------------------------------------------------------

    #[test]
    fn fulfilling_an_unknown_task_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), task_fulfill("o:t", 1, "resolved"), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn fulfilling_at_the_wrong_version_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (_, _, reply) = step(&doc, task_fulfill("o:t", 9, "resolved"), 1);
        assert_eq!(reply.status, 409);
    }

    #[test]
    fn fulfilling_settles_the_promise_and_runs_the_chain() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, listener("o:t", "http://one"), 1);
        let (next, sends, reply) = step(&doc, task_fulfill("o:t", 1, "resolved"), 700);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["promise"]["state"], "resolved");
        assert_eq!(reply.data["promise"]["settledAt"], 700);
        assert_eq!(next.tasks["o:t"].state, TaskState::Fulfilled);
        assert_eq!(next.timer_at, None);
        assert_eq!(sends.len(), 1);
    }

    // --- task.suspend ------------------------------------------------------

    #[test]
    fn suspending_an_unknown_task_is_a_404() {
        let doc = with_targeted("o:a", 100_000);
        let (_, _, reply) = step(&doc, task_suspend("o:t", 1, &["o:a"]), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn suspending_at_the_wrong_version_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, create("o:a", 100_000, json!({})), 1);
        let (_, _, reply) = step(&doc, task_suspend("o:t", 9, &["o:a"]), 2);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Task is not acquired or version mismatch"));
    }

    #[test]
    fn suspending_on_a_missing_promise_is_a_422() {
        let doc = with_acquired("o:t");
        let (_, _, reply) = step(&doc, task_suspend("o:t", 1, &["o:missing"]), 1);
        assert_eq!(reply.status, 422);
        assert_eq!(reply.data, json!("Awaited promise not found"));
    }

    #[test]
    fn suspending_parks_the_task_and_registers_each_awaited_once() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, create("o:a", 100_000, json!({})), 1);
        let (doc, _, _) = step(&doc, create("o:b", 100_000, json!({})), 1);
        let (next, sends, reply) = step(&doc, task_suspend("o:t", 1, &["o:a", "o:b", "o:a"]), 2);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data, json!({}));
        assert!(sends.is_empty());
        let t = &next.tasks["o:t"];
        assert_eq!(t.state, TaskState::Suspended);
        assert_eq!((t.pid.as_deref(), t.ttl), (None, None));
        assert_eq!((t.retry_at, t.lease_at), (None, None));
        assert_eq!(next.promises["o:a"].callbacks, vec!["o:t"]);
        assert_eq!(next.promises["o:b"].callbacks, vec!["o:t"]);
        // The lease is gone, so only the promise deadline remains armed.
        assert_eq!(next.timer_at, Some(100_000));
    }

    #[test]
    fn suspending_on_an_already_settled_promise_tells_the_caller_to_carry_on() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, create("o:a", 100_000, json!({})), 1);
        let (doc, _, _) = step(&doc, settle_req("o:a", "resolved"), 2);
        let (next, _, reply) = step(&doc, task_suspend("o:t", 1, &["o:a"]), 3);
        assert_eq!(reply.status, 300);
        assert_eq!(reply.data["preload"], json!([]));
        // The task stays acquired: it never parked.
        assert_eq!(next.tasks["o:t"].state, TaskState::Acquired);
        assert!(next.promises["o:a"].callbacks.is_empty());
    }

    #[test]
    fn suspending_drops_the_resumes_a_previous_run_buffered() {
        let doc = with_acquired("o:t");
        let (mut doc, _, _) = step(&doc, create("o:a", 100_000, json!({})), 1);
        doc.tasks.get_mut("o:t").unwrap().resumes.insert("o:a".into());
        let (next, _, reply) = step(&doc, task_suspend("o:t", 1, &["o:a"]), 2);
        assert_eq!(reply.status, 200);
        assert!(next.tasks["o:t"].resumes.is_empty());
    }

    // --- task.fence --------------------------------------------------------

    #[test]
    fn fencing_an_unknown_task_is_a_404() {
        let action = json!({ "kind": "promise.create", "head": {},
                             "data": { "id": "o:a", "timeoutAt": 100_000, "param": {}, "tags": {} } });
        let (_, _, reply) = step(&OriginDoc::default(), task_fence("o:t", 1, action), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn fencing_at_the_wrong_version_is_a_conflict() {
        let doc = with_acquired("o:t");
        let action = json!({ "kind": "promise.create", "head": {},
                             "data": { "id": "o:a", "timeoutAt": 100_000, "param": {}, "tags": {} } });
        let (_, _, reply) = step(&doc, task_fence("o:t", 9, action), 1);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Version mismatch"));
    }

    #[test]
    fn a_fenced_create_returns_a_nested_envelope() {
        let doc = with_acquired("o:t");
        let action = json!({ "kind": "promise.create", "head": {},
                             "data": { "id": "o:a", "timeoutAt": 100_000, "param": {}, "tags": {} } });
        let (next, _, reply) = step(&doc, task_fence("o:t", 1, action), 1_000);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["action"]["kind"], "promise.create");
        assert_eq!(reply.data["action"]["head"]["status"], 200);
        assert_eq!(reply.data["action"]["head"]["corrId"], "corr-1");
        assert_eq!(reply.data["action"]["data"]["promise"]["id"], "o:a");
        assert_eq!(next.promises["o:a"].state, PromiseState::Pending);
    }

    #[test]
    fn a_fenced_settle_of_a_missing_promise_reports_404_inside_a_200() {
        let doc = with_acquired("o:t");
        let action = json!({ "kind": "promise.settle", "head": {},
                             "data": { "id": "o:a", "state": "resolved", "value": {} } });
        let (_, _, reply) = step(&doc, task_fence("o:t", 1, action), 1);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["action"]["head"]["status"], 404);
        assert_eq!(reply.data["action"]["data"], json!("Promise not found"));
    }

    #[test]
    fn a_fenced_settle_runs_the_settlement_chain() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, create("o:a", 100_000, json!({})), 1);
        let (doc, _, _) = step(&doc, listener("o:a", "http://one"), 2);
        let action = json!({ "kind": "promise.settle", "head": {},
                             "data": { "id": "o:a", "state": "rejected", "value": {} } });
        let (next, sends, reply) = step(&doc, task_fence("o:t", 1, action), 700);
        assert_eq!(reply.data["action"]["head"]["status"], 200);
        assert_eq!(next.promises["o:a"].state, PromiseState::Rejected);
        assert_eq!(sends.len(), 1);
    }

    #[test]
    fn fencing_an_unknown_action_kind_is_a_400() {
        let doc = with_acquired("o:t");
        let action = json!({ "kind": "promise.frobnicate", "head": {}, "data": { "id": "o:a" } });
        let (_, _, reply) = step(&doc, task_fence("o:t", 1, action), 1);
        assert_eq!(reply.status, 400);
        assert_eq!(reply.data, json!("Invalid fence action kind"));
    }

    // --- task.heartbeat ----------------------------------------------------

    #[test]
    fn a_heartbeat_extends_the_lease_of_a_task_the_caller_owns() {
        let doc = with_acquired("o:t");
        assert_eq!(doc.tasks["o:t"].lease_at, Some(TTL));
        let (next, _, reply) = step(&doc, task_heartbeat(PID, &[("o:t", 1)]), 5_000);
        assert_eq!(reply.status, 200);
        assert_eq!(next.tasks["o:t"].lease_at, Some(65_000));
    }

    #[test]
    fn a_heartbeat_from_another_process_changes_nothing() {
        let doc = with_acquired("o:t");
        let (next, _, reply) = step(&doc, task_heartbeat("someone-else", &[("o:t", 1)]), 5_000);
        assert_eq!(reply.status, 200);
        assert_eq!(next, doc);
    }

    #[test]
    fn a_heartbeat_at_a_stale_version_changes_nothing() {
        let doc = with_acquired("o:t");
        let (next, _, _) = step(&doc, task_heartbeat(PID, &[("o:t", 0)]), 5_000);
        assert_eq!(next, doc);
    }

    #[test]
    fn a_heartbeat_for_an_unknown_task_is_still_a_200() {
        let doc = with_acquired("o:t");
        let (next, _, reply) = step(&doc, task_heartbeat(PID, &[("o:missing", 1)]), 5_000);
        assert_eq!(reply.status, 200);
        assert_eq!(next, doc);
    }

    // --- task.halt / task.continue ----------------------------------------

    #[test]
    fn halting_an_unknown_task_is_a_404() {
        let (_, _, reply) = step(&OriginDoc::default(), Req::TaskHalt(parse(json!({ "id": "o:t" }))), 0);
        assert_eq!(reply.status, 404);
    }

    #[test]
    fn halting_disarms_the_task() {
        let doc = with_acquired("o:t");
        let (next, _, reply) = step(&doc, Req::TaskHalt(parse(json!({ "id": "o:t" }))), 1);
        assert_eq!(reply.status, 200);
        let t = &next.tasks["o:t"];
        assert_eq!(t.state, TaskState::Halted);
        assert_eq!((t.retry_at, t.lease_at, t.pid.as_deref(), t.ttl), (None, None, None, None));
        assert_eq!(next.timer_at, Some(100_000));
    }

    #[test]
    fn halting_twice_is_idempotent() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, Req::TaskHalt(parse(json!({ "id": "o:t" }))), 1);
        let (next, _, reply) = step(&doc, Req::TaskHalt(parse(json!({ "id": "o:t" }))), 2);
        assert_eq!(reply.status, 200);
        assert_eq!(next, doc);
    }

    #[test]
    fn halting_a_finished_task_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, task_fulfill("o:t", 1, "resolved"), 1);
        let (_, _, reply) = step(&doc, Req::TaskHalt(parse(json!({ "id": "o:t" }))), 2);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Task is fulfilled"));
    }

    #[test]
    fn continuing_a_task_that_is_not_halted_is_a_conflict() {
        let doc = with_acquired("o:t");
        let (_, _, reply) = step(&doc, Req::TaskContinue(parse(json!({ "id": "o:t" }))), 1);
        assert_eq!(reply.status, 409);
        assert_eq!(reply.data, json!("Task is not halted"));
    }

    #[test]
    fn continuing_re_dispatches_a_halted_task() {
        let doc = with_acquired("o:t");
        let (doc, _, _) = step(&doc, Req::TaskHalt(parse(json!({ "id": "o:t" }))), 1);
        let (next, sends, reply) =
            step(&doc, Req::TaskContinue(parse(json!({ "id": "o:t" }))), 1_000);
        assert_eq!(reply.status, 200);
        let t = &next.tasks["o:t"];
        assert_eq!(t.state, TaskState::Pending);
        assert_eq!(t.retry_at, Some(31_000));
        assert_eq!(
            sends,
            vec![(
                W.to_string(),
                OutEntry::Execute {
                    task_id: "o:t".into(),
                    version: 1
                }
            )]
        );
    }

    // --- preload -----------------------------------------------------------

    #[test]
    fn preload_is_the_rest_of_the_branch() {
        let branch = json!({ "resonate:target": W, "resonate:branch": "o" });
        let (doc, _, _) = step(&OriginDoc::default(), create("o:a", 100_000, branch.clone()), 0);
        let (doc, _, _) = step(&doc, create("o:b", 100_000, branch), 0);
        let (doc, _, _) = step(&doc, create("o:c", 100_000, json!({})), 0);
        let ids: Vec<String> = preload(&doc, "o:a").into_iter().map(|p| p.id).collect();
        assert_eq!(ids, vec!["o:b"]);
    }

    #[test]
    fn a_promise_without_a_branch_preloads_nothing() {
        let doc = with_targeted("o:a", 100_000);
        assert!(preload(&doc, "o:a").is_empty());
    }
}
