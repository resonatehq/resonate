use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use validator::Validate;

use crate::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRecord,
    PromiseRegisterCallbackData, PromiseRegisterListenerData, PromiseResponseData,
    PromiseSearchData, PromiseSearchResponseData, PromiseSettleData, PromiseState, PromiseValue,
    RequestEnvelope, ResponseEnvelope, ScheduleCreateData, ScheduleDeleteData, ScheduleGetData,
    ScheduleRecord, ScheduleResponseData, ScheduleSearchData, ScheduleSearchResponseData,
    SettleState, Snapshot, SnapshotCallback, SnapshotListener, SnapshotMessage,
    SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskAcquireData, TaskAcquireResponseData,
    TaskContinueData, TaskCreateData, TaskCreateResponseData, TaskFenceData, TaskFenceResponseData,
    TaskFulfillData, TaskFulfillResponseData, TaskGetData, TaskHaltData, TaskHeartbeatData,
    TaskRecord, TaskReleaseData, TaskResponseData, TaskSearchData, TaskSearchResponseData,
    TaskState, TaskSuspendData, TaskSuspendPreloadData, PROTOCOL_VERSION,
};
use crate::util;

const PENDING_RETRY_TTL: i64 = 30_000;

// ─── Internal state types ─────────────────────────────────────────────────────

struct Promise {
    state: PromiseState,
    param: PromiseValue,
    value: PromiseValue,
    tags: HashMap<String, String>,
    timeout_at: i64,
    created_at: i64,
    settled_at: Option<i64>,
    callbacks: HashSet<String>,
    listeners: HashSet<String>,
}

struct Task {
    state: TaskState,
    version: i64,
    pid: Option<String>,
    ttl: Option<i64>,
    resumes: HashSet<String>,
}

struct Schedule {
    cron: String,
    promise_id: String,
    promise_timeout: i64,
    promise_param: PromiseValue,
    promise_tags: HashMap<String, String>,
    created_at: i64,
    last_run_at: Option<i64>,
}

#[derive(Clone, Copy)]
enum TTimeoutKind {
    Retry = 0,
    Lease = 1,
}

struct PTimeout {
    id: String,
    timeout: i64,
}
struct TTimeout {
    id: String,
    kind: TTimeoutKind,
    timeout: i64,
}
struct STimeout {
    id: String,
    timeout: i64,
}

// ─── Oracle ───────────────────────────────────────────────────────────────────

pub struct Oracle {
    promises: HashMap<String, Promise>,
    tasks: HashMap<String, Task>,
    schedules: HashMap<String, Schedule>,
    p_timeouts: Vec<PTimeout>,
    t_timeouts: Vec<TTimeout>,
    s_timeouts: Vec<STimeout>,
    outgoing: Vec<(String, Value)>,
}

impl Default for Oracle {
    fn default() -> Self {
        Self::new()
    }
}

impl Oracle {
    pub fn new() -> Self {
        Self {
            promises: HashMap::new(),
            tasks: HashMap::new(),
            schedules: HashMap::new(),
            p_timeouts: Vec::new(),
            t_timeouts: Vec::new(),
            s_timeouts: Vec::new(),
            outgoing: Vec::new(),
        }
    }

    pub fn apply(&mut self, req: &RequestEnvelope) -> ResponseEnvelope {
        let now = util::resolve_time(req.head.debug_time);
        match req.kind.as_str() {
            "promise.get" => self.op_promise_get(req, now),
            "promise.create" => self.op_promise_create(req, now),
            "promise.settle" => self.op_promise_settle(req, now),
            "promise.register_callback" => self.op_promise_register_callback(req, now),
            "promise.register_listener" => self.op_promise_register_listener(req, now),
            "promise.search" => self.op_promise_search(req),
            "task.get" => self.op_task_get(req, now),
            "task.create" => self.op_task_create(req, now),
            "task.acquire" => self.op_task_acquire(req, now),
            "task.release" => self.op_task_release(req, now),
            "task.fulfill" => self.op_task_fulfill(req, now),
            "task.suspend" => self.op_task_suspend(req, now),
            "task.fence" => self.op_task_fence(req, now),
            "task.heartbeat" => self.op_task_heartbeat(req, now),
            "task.halt" => self.op_task_halt(req, now),
            "task.continue" => self.op_task_continue(req, now),
            "task.search" => self.op_task_search(req),
            "schedule.get" => self.op_schedule_get(req),
            "schedule.create" => self.op_schedule_create(req, now),
            "schedule.delete" => self.op_schedule_delete(req),
            "schedule.search" => self.op_schedule_search(req),
            "debug.start" | "debug.stop" => ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                Value::Object(serde_json::Map::new()),
            ),
            "debug.reset" => self.op_debug_reset(req),
            "debug.snap" => self.op_debug_snap(req),
            "debug.tick" => self.op_debug_tick(req),
            _ => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format!("Unknown operation: {}", req.kind),
            ),
        }
    }

    // ─── Parse helper ─────────────────────────────────────────────────────────

    fn parse<T>(&self, req: &RequestEnvelope) -> Result<T, ResponseEnvelope>
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

    // ─── Promise operations ────────────────────────────────────────────────────

    fn op_promise_get(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: PromiseGetData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        match self.promises.get(&r.id) {
            None => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                404,
                "Promise not found",
            ),
            Some(p) => ResponseEnvelope::success(
                req.kind.clone(),
                req.head.corr_id.clone(),
                &PromiseResponseData {
                    promise: Self::to_promise_record(now, &r.id, p),
                },
            ),
        }
    }

    fn op_promise_create(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: PromiseCreateData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        if let Some(addr) = r.tags.get("resonate:target") {
            if !crate::transport::is_valid_address(addr) {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Invalid resonate:target address",
                );
            }
        }
        self.try_timeout(std::slice::from_ref(&r.id), now);
        if let Some(p) = self.promises.get(&r.id) {
            return ResponseEnvelope::success(
                req.kind.clone(),
                req.head.corr_id.clone(),
                &PromiseResponseData {
                    promise: Self::to_promise_record(now, &r.id, p),
                },
            );
        }
        let addr = r.tags.get("resonate:target").cloned();
        let already_timedout = now >= r.timeout_at;
        let (state, created_at, settled_at) = if already_timedout {
            (
                Self::timeout_state(&r.tags),
                r.timeout_at,
                Some(r.timeout_at),
            )
        } else {
            (PromiseState::Pending, now, None)
        };
        let promise = Promise {
            state,
            param: r.param.clone(),
            value: PromiseValue::default(),
            tags: r.tags.clone(),
            timeout_at: r.timeout_at,
            created_at,
            settled_at,
            callbacks: HashSet::new(),
            listeners: HashSet::new(),
        };
        let record = Self::to_promise_record(now, &r.id, &promise);
        self.promises.insert(r.id.clone(), promise);
        if already_timedout {
            if addr.is_some() {
                self.tasks.insert(
                    r.id.clone(),
                    Task {
                        state: TaskState::Fulfilled,
                        version: 0,
                        pid: None,
                        ttl: None,
                        resumes: HashSet::new(),
                    },
                );
            }
        } else {
            self.set_p_timeout(&r.id, r.timeout_at);
            if let Some(ref addr) = addr {
                self.tasks.insert(
                    r.id.clone(),
                    Task {
                        state: TaskState::Pending,
                        version: 0,
                        pid: None,
                        ttl: None,
                        resumes: HashSet::new(),
                    },
                );
                self.set_t_timeout(&r.id, TTimeoutKind::Retry, created_at + PENDING_RETRY_TTL);
                self.send_execute(addr, &r.id, 0);
            }
        }
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &PromiseResponseData { promise: record },
        )
    }

    fn op_promise_settle(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: PromiseSettleData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        let is_pending = match self.promises.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Promise not found",
                )
            }
            Some(p) => p.state == PromiseState::Pending,
        };
        if !is_pending {
            let record = self
                .promises
                .get(&r.id)
                .map(|p| Self::to_promise_record(now, &r.id, p))
                .unwrap();
            return ResponseEnvelope::success(
                req.kind.clone(),
                req.head.corr_id.clone(),
                &PromiseResponseData { promise: record },
            );
        }
        if let Some(p) = self.promises.get_mut(&r.id) {
            p.state = Self::settle_to_promise_state(r.state);
            p.value = r.value.clone();
            p.settled_at = Some(now);
        }
        let record = self
            .promises
            .get(&r.id)
            .map(|p| Self::to_promise_record(now, &r.id, p))
            .unwrap();
        self.del_p_timeout(&r.id);
        self.trigger_settlement(&r.id, now);
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &PromiseResponseData { promise: record },
        )
    }

    fn op_promise_register_callback(
        &mut self,
        req: &RequestEnvelope,
        now: i64,
    ) -> ResponseEnvelope {
        let r: PromiseRegisterCallbackData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(&[r.awaited.clone(), r.awaiter.clone()], now);

        let awaited_record = match self.promises.get(&r.awaited) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Awaited promise not found",
                )
            }
            Some(p) => Self::to_promise_record(now, &r.awaited, p),
        };
        let (awaiter_state, awaiter_has_target) = match self.promises.get(&r.awaiter) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    422,
                    "Awaiter promise not found",
                )
            }
            Some(p) => (p.state, p.tags.contains_key("resonate:target")),
        };
        if !awaiter_has_target {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                422,
                "Awaiter promise has no resonate:target tag",
            );
        }

        let awaited_pending = awaited_record.state == PromiseState::Pending;
        let awaiter_pending = awaiter_state == PromiseState::Pending;

        if awaited_pending && awaiter_pending {
            if let Some(p) = self.promises.get_mut(&r.awaited) {
                p.callbacks.insert(r.awaiter.clone());
            }
        } else if !awaited_pending && awaiter_pending {
            // Direct resume: awaited already settled.
            // Only Suspended awaiters need to be woken up. Pending/Acquired/Halted awaiters
            // are already running; the caller sees the settled promise in the response.
            // SQLite inserts no ready-callback entry in this path, so we must not touch
            // t.resumes here — get_resumes would return 0 for all non-Suspended awaiters.
            let task_state = self.tasks.get(&r.awaiter).map(|t| t.state);
            let version = self.tasks.get(&r.awaiter).map(|t| t.version).unwrap_or(0);
            let addr = self
                .promises
                .get(&r.awaiter)
                .and_then(|p| p.tags.get("resonate:target").cloned());
            if task_state == Some(TaskState::Suspended) {
                if let Some(t) = self.tasks.get_mut(&r.awaiter) {
                    t.state = TaskState::Pending;
                    // Don't add to t.resumes: SQLite has no ready-callback entry for direct resumes.
                }
                self.set_t_timeout(&r.awaiter, TTimeoutKind::Retry, now + PENDING_RETRY_TTL);
                if let Some(addr) = addr {
                    self.send_execute(&addr, &r.awaiter, version);
                }
            }
        }

        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &PromiseResponseData {
                promise: awaited_record,
            },
        )
    }

    fn op_promise_register_listener(
        &mut self,
        req: &RequestEnvelope,
        now: i64,
    ) -> ResponseEnvelope {
        let r: PromiseRegisterListenerData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        if !crate::transport::is_valid_address(&r.address) {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                "Invalid listener address",
            );
        }
        self.try_timeout(std::slice::from_ref(&r.awaited), now);
        let is_pending = match self.promises.get(&r.awaited) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Awaited promise not found",
                )
            }
            Some(p) => p.state == PromiseState::Pending,
        };
        if is_pending {
            if let Some(p) = self.promises.get_mut(&r.awaited) {
                p.listeners.insert(r.address.clone());
            }
        }
        let record = self
            .promises
            .get(&r.awaited)
            .map(|p| Self::to_promise_record(now, &r.awaited, p))
            .unwrap();
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &PromiseResponseData { promise: record },
        )
    }

    fn op_promise_search(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let r: PromiseSearchData = match serde_json::from_value(req.data.clone()) {
            Ok(r) => r,
            Err(e) => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    &format!("Invalid request: {}", e),
                )
            }
        };
        if let Err(e) = r.validate() {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format_validation_errors(&e),
            );
        }
        let limit = match r.limit {
            Some(n) if n > 1000 => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Invalid 'limit' — must be between 1 and 1000",
                )
            }
            Some(n) => n as usize,
            None => 100,
        };
        let mut promises: Vec<PromiseRecord> = self
            .promises
            .iter()
            .filter(|(_, p)| {
                r.state.map(|s| p.state == s).unwrap_or(true)
                    && r.tags
                        .as_ref()
                        .map(|ft| {
                            ft.iter()
                                .all(|(k, v)| p.tags.get(k).map(|pv| pv == v).unwrap_or(false))
                        })
                        .unwrap_or(true)
            })
            .map(|(id, p)| Self::to_promise_record(0, id, p))
            .collect();
        promises.sort_by(|a, b| a.id.cmp(&b.id));
        let start = r
            .cursor
            .as_deref()
            .and_then(|c| promises.iter().position(|p| p.id.as_str() > c))
            .unwrap_or(0);
        let page: Vec<_> = promises.into_iter().skip(start).take(limit + 1).collect();
        let has_more = page.len() > limit;
        let result: Vec<_> = page.into_iter().take(limit).collect();
        let cursor = if has_more {
            result.last().map(|p| p.id.clone())
        } else {
            None
        };
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &PromiseSearchResponseData {
                promises: result,
                cursor,
            },
        )
    }

    // ─── Task operations ───────────────────────────────────────────────────────

    fn op_task_get(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskGetData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        let (state, version, resumes, ttl, pid) = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => {
                let eff = self.effective_task_state(now, &r.id).unwrap_or(t.state);
                (eff, t.version, t.resumes.len() as i64, t.ttl, t.pid.clone())
            }
        };
        let task = TaskRecord {
            id: r.id.clone(),
            state,
            version,
            resumes,
            ttl: if state == TaskState::Fulfilled {
                None
            } else {
                ttl
            },
            pid: if state == TaskState::Fulfilled {
                None
            } else {
                pid
            },
        };
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &TaskResponseData { task },
        )
    }

    fn op_task_create(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskCreateData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let action = &r.action.data;
        if let Some(addr) = action.tags.get("resonate:target") {
            if !crate::transport::is_valid_address(addr) {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Invalid resonate:target address",
                );
            }
        }
        let promise_id = action.id.clone();
        self.try_timeout(std::slice::from_ref(&promise_id), now);

        // Task already exists
        if let Some(t) = self.tasks.get(&promise_id) {
            let eff = self
                .effective_task_state(now, &promise_id)
                .unwrap_or(t.state);
            match eff {
                TaskState::Pending => {
                    let new_version = t.version + 1;
                    if let Some(t) = self.tasks.get_mut(&promise_id) {
                        t.state = TaskState::Acquired;
                        t.version = new_version;
                        t.pid = Some(r.pid.clone());
                        t.ttl = Some(r.ttl);
                        t.resumes = HashSet::new();
                    }
                    self.del_t_timeout(&promise_id);
                    self.set_t_timeout(&promise_id, TTimeoutKind::Lease, now + r.ttl);
                    let task =
                        Self::to_task_record(&promise_id, self.tasks.get(&promise_id).unwrap());
                    let promise = Self::to_promise_record(
                        now,
                        &promise_id,
                        self.promises.get(&promise_id).unwrap(),
                    );
                    let preload = self.preload(&promise_id);
                    return ResponseEnvelope::success(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        &TaskCreateResponseData {
                            task,
                            promise,
                            preload,
                        },
                    );
                }
                TaskState::Fulfilled => {
                    let task =
                        Self::to_task_record(&promise_id, self.tasks.get(&promise_id).unwrap());
                    let promise = Self::to_promise_record(
                        now,
                        &promise_id,
                        self.promises.get(&promise_id).unwrap(),
                    );
                    let preload = self.preload(&promise_id);
                    return ResponseEnvelope::success(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        &TaskCreateResponseData {
                            task,
                            promise,
                            preload,
                        },
                    );
                }
                _ => {
                    return ResponseEnvelope::error(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        409,
                        "Already exists",
                    )
                }
            }
        }

        // Promise exists without a task
        if self.promises.contains_key(&promise_id) {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                422,
                "Promise exists without a target task",
            );
        }

        // Create new promise + task
        let _addr = action.tags.get("resonate:target").cloned();
        let already_timedout = now >= action.timeout_at;
        let (p_state, created_at, settled_at) = if already_timedout {
            (
                Self::timeout_state(&action.tags),
                action.timeout_at,
                Some(action.timeout_at),
            )
        } else {
            (PromiseState::Pending, now, None)
        };
        let promise = Promise {
            state: p_state,
            param: action.param.clone(),
            value: PromiseValue::default(),
            tags: action.tags.clone(),
            timeout_at: action.timeout_at,
            created_at,
            settled_at,
            callbacks: HashSet::new(),
            listeners: HashSet::new(),
        };
        let promise_record = Self::to_promise_record(now, &promise_id, &promise);
        self.promises.insert(promise_id.clone(), promise);

        let (task_state, task_version, task_ttl, task_pid) = if already_timedout {
            (TaskState::Fulfilled, 0i64, None, None)
        } else {
            (TaskState::Acquired, 1i64, Some(r.ttl), Some(r.pid.clone()))
        };
        self.tasks.insert(
            promise_id.clone(),
            Task {
                state: task_state,
                version: task_version,
                pid: task_pid,
                ttl: task_ttl,
                resumes: HashSet::new(),
            },
        );
        if !already_timedout {
            self.set_p_timeout(&promise_id, action.timeout_at);
            self.set_t_timeout(&promise_id, TTimeoutKind::Lease, now + r.ttl);
            // task.create returns an already-acquired task to the caller (who IS the
            // worker), so no execute notification is needed. SQLite likewise does not
            // insert into outgoing_execute for this path.
        }
        let task = Self::to_task_record(&promise_id, self.tasks.get(&promise_id).unwrap());
        let preload = self.preload(&promise_id);
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &TaskCreateResponseData {
                task,
                promise: promise_record,
                preload,
            },
        )
    }

    fn op_task_acquire(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskAcquireData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        let (task_state, task_version) = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => (t.state, t.version),
        };
        if task_state != TaskState::Pending {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task is not pending",
            );
        }
        if task_version != r.version {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Version mismatch",
            );
        }
        let promise = match self.promises.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(p) if p.state != PromiseState::Pending => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    409,
                    "Task is not pending",
                )
            }
            Some(p) => Self::to_promise_record(now, &r.id, p),
        };
        let new_version = r.version + 1;
        if let Some(t) = self.tasks.get_mut(&r.id) {
            t.state = TaskState::Acquired;
            t.version = new_version;
            t.pid = Some(r.pid.clone());
            t.ttl = Some(r.ttl);
            t.resumes = HashSet::new();
        }
        self.del_t_timeout(&r.id);
        self.set_t_timeout(&r.id, TTimeoutKind::Lease, now + r.ttl);
        let task = TaskRecord {
            id: r.id.clone(),
            state: TaskState::Acquired,
            version: new_version,
            resumes: 0,
            ttl: Some(r.ttl),
            pid: Some(r.pid.clone()),
        };
        let preload = self.preload(&r.id);
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &TaskAcquireResponseData {
                task,
                promise,
                preload,
            },
        )
    }

    fn op_task_release(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskReleaseData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        let (task_state, task_version) = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => (t.state, t.version),
        };
        if task_state != TaskState::Acquired || task_version != r.version {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task version mismatch or invalid state",
            );
        }
        let addr = self
            .promises
            .get(&r.id)
            .and_then(|p| p.tags.get("resonate:target").cloned());
        let version = self.tasks.get(&r.id).map(|t| t.version).unwrap_or(0);
        if let Some(t) = self.tasks.get_mut(&r.id) {
            t.state = TaskState::Pending;
            t.pid = None;
            t.ttl = None;
        }
        self.del_t_timeout(&r.id);
        self.set_t_timeout(&r.id, TTimeoutKind::Retry, now + PENDING_RETRY_TTL);
        if let Some(addr) = addr {
            self.send_execute(&addr, &r.id, version);
        }
        ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, json!({}))
    }

    fn op_task_fulfill(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskFulfillData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let action = &r.action.data;
        self.try_timeout(std::slice::from_ref(&action.id), now);
        let (task_state, task_version) = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => (t.state, t.version),
        };
        if task_state != TaskState::Acquired || task_version != r.version {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task version mismatch or invalid state",
            );
        }
        let promise_is_pending = match self.promises.get(&action.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Promise not found",
                )
            }
            Some(p) => p.state == PromiseState::Pending,
        };
        if !promise_is_pending {
            let record = self
                .promises
                .get(&action.id)
                .map(|p| Self::to_promise_record(now, &action.id, p))
                .unwrap();
            self.trigger_fulfilled(&r.id);
            return ResponseEnvelope::success(
                req.kind.clone(),
                req.head.corr_id.clone(),
                &TaskFulfillResponseData { promise: record },
            );
        }
        let settle_state = Self::settle_to_promise_state(action.state);
        let value = action.value.clone();
        if let Some(p) = self.promises.get_mut(&action.id) {
            p.state = settle_state;
            p.value = value;
            p.settled_at = Some(now);
        }
        let record = self
            .promises
            .get(&action.id)
            .map(|p| Self::to_promise_record(now, &action.id, p))
            .unwrap();
        self.del_p_timeout(&action.id);
        self.trigger_settlement(&action.id, now);
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &TaskFulfillResponseData { promise: record },
        )
    }

    fn op_task_suspend(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskSuspendData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let mut timeout_ids: Vec<String> = vec![r.id.clone()];
        for action in &r.actions {
            timeout_ids.push(action.data.awaited.clone());
        }
        self.try_timeout(&timeout_ids, now);
        let (task_state, task_version) = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => (t.state, t.version),
        };
        if task_state != TaskState::Acquired {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task is not acquired or version mismatch",
            );
        }
        if task_version != r.version {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task is not acquired or version mismatch",
            );
        }
        for action in &r.actions {
            if !self.promises.contains_key(&action.data.awaited) {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    422,
                    "Awaited promise not found",
                );
            }
        }
        let unique_awaited: Vec<String> = {
            let mut seen = HashSet::new();
            r.actions
                .iter()
                .map(|a| a.data.awaited.clone())
                .filter(|id| seen.insert(id.clone()))
                .collect()
        };
        let has_settled = unique_awaited.iter().any(|id| {
            self.promises
                .get(id)
                .map(|p| p.state != PromiseState::Pending)
                .unwrap_or(false)
        });
        if has_settled {
            if let Some(t) = self.tasks.get_mut(&r.id) {
                t.resumes = HashSet::new();
            }
            let preload = self.preload(&r.id);
            return ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                300,
                serde_json::to_value(&TaskSuspendPreloadData { preload }).unwrap(),
            );
        }
        for awaited_id in &unique_awaited {
            if let Some(p) = self.promises.get_mut(awaited_id) {
                p.callbacks.insert(r.id.clone());
            }
        }
        if let Some(t) = self.tasks.get_mut(&r.id) {
            t.state = TaskState::Suspended;
            t.pid = None;
            t.ttl = None;
            t.resumes = HashSet::new();
        }
        self.del_t_timeout(&r.id);
        ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, json!({}))
    }

    fn op_task_fence(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskFenceData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let action_id = r
            .action
            .data
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        self.try_timeout(&[r.id.clone(), action_id.clone()], now);
        let (task_state, task_version) = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => (t.state, t.version),
        };
        if task_state != TaskState::Acquired {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Version mismatch",
            );
        }
        if task_version != r.version {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Version mismatch",
            );
        }
        match r.action.kind.as_str() {
            "promise.create" => {
                let create_data: PromiseCreateData =
                    match serde_json::from_value(r.action.data.clone()) {
                        Ok(d) => d,
                        Err(e) => {
                            return ResponseEnvelope::error(
                                req.kind.clone(),
                                req.head.corr_id.clone(),
                                400,
                                &format!("Invalid action data: {}", e),
                            )
                        }
                    };
                if let Err(e) = create_data.validate() {
                    return ResponseEnvelope::error(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    );
                }
                if let Some(addr) = create_data.tags.get("resonate:target") {
                    if !crate::transport::is_valid_address(addr) {
                        return ResponseEnvelope::error(
                            req.kind.clone(),
                            req.head.corr_id.clone(),
                            400,
                            "Invalid resonate:target address",
                        );
                    }
                }
                self.try_timeout(std::slice::from_ref(&create_data.id), now);
                let inner_record = if let Some(p) = self.promises.get(&create_data.id) {
                    Some(Self::to_promise_record(now, &create_data.id, p))
                } else {
                    let addr = create_data.tags.get("resonate:target").cloned();
                    let already_timedout = now >= create_data.timeout_at;
                    let (state, created_at, settled_at) = if already_timedout {
                        (
                            Self::timeout_state(&create_data.tags),
                            create_data.timeout_at,
                            Some(create_data.timeout_at),
                        )
                    } else {
                        (PromiseState::Pending, now, None)
                    };
                    let promise = Promise {
                        state,
                        param: create_data.param.clone(),
                        value: PromiseValue::default(),
                        tags: create_data.tags.clone(),
                        timeout_at: create_data.timeout_at,
                        created_at,
                        settled_at,
                        callbacks: HashSet::new(),
                        listeners: HashSet::new(),
                    };
                    let record = Self::to_promise_record(now, &create_data.id, &promise);
                    self.promises.insert(create_data.id.clone(), promise);
                    if already_timedout {
                        if addr.is_some() {
                            self.tasks.insert(
                                create_data.id.clone(),
                                Task {
                                    state: TaskState::Fulfilled,
                                    version: 0,
                                    pid: None,
                                    ttl: None,
                                    resumes: HashSet::new(),
                                },
                            );
                        }
                    } else {
                        self.set_p_timeout(&create_data.id, create_data.timeout_at);
                        if let Some(ref a) = addr {
                            self.tasks.insert(
                                create_data.id.clone(),
                                Task {
                                    state: TaskState::Pending,
                                    version: 0,
                                    pid: None,
                                    ttl: None,
                                    resumes: HashSet::new(),
                                },
                            );
                            self.set_t_timeout(
                                &create_data.id,
                                TTimeoutKind::Retry,
                                created_at + PENDING_RETRY_TTL,
                            );
                            self.send_execute(a, &create_data.id, 0);
                        }
                    }
                    Some(record)
                };
                let (inner_status, inner_data) = match inner_record {
                    Some(ref p) => (200i32, json!({ "promise": p })),
                    None => (404, json!("Promise not found")),
                };
                let inner_envelope = json!({
                    "kind": r.action.kind,
                    "head": { "corrId": req.head.corr_id, "status": inner_status, "version": PROTOCOL_VERSION },
                    "data": inner_data,
                });
                let preload = self.preload(&r.id);
                ResponseEnvelope::success(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    &TaskFenceResponseData {
                        action: inner_envelope,
                        preload,
                    },
                )
            }
            "promise.settle" => {
                let settle_data: PromiseSettleData =
                    match serde_json::from_value(r.action.data.clone()) {
                        Ok(d) => d,
                        Err(e) => {
                            return ResponseEnvelope::error(
                                req.kind.clone(),
                                req.head.corr_id.clone(),
                                400,
                                &format!("Invalid action data: {}", e),
                            )
                        }
                    };
                if let Err(e) = settle_data.validate() {
                    return ResponseEnvelope::error(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        400,
                        &format_validation_errors(&e),
                    );
                }
                let is_pending = match self.promises.get(&settle_data.id) {
                    None => false,
                    Some(p) => p.state == PromiseState::Pending,
                };
                if is_pending {
                    let settle_state = Self::settle_to_promise_state(settle_data.state);
                    let value = settle_data.value.clone();
                    if let Some(p) = self.promises.get_mut(&settle_data.id) {
                        p.state = settle_state;
                        p.value = value;
                        p.settled_at = Some(now);
                    }
                    self.del_p_timeout(&settle_data.id);
                    self.trigger_settlement(&settle_data.id, now);
                }
                let inner_record = self
                    .promises
                    .get(&settle_data.id)
                    .map(|p| Self::to_promise_record(now, &settle_data.id, p));
                let (inner_status, inner_data) = match inner_record {
                    Some(ref p) => (200i32, json!({ "promise": p })),
                    None => (404, json!("Promise not found")),
                };
                let inner_envelope = json!({
                    "kind": r.action.kind,
                    "head": { "corrId": req.head.corr_id, "status": inner_status, "version": PROTOCOL_VERSION },
                    "data": inner_data,
                });
                let preload = self.preload(&r.id);
                ResponseEnvelope::success(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    &TaskFenceResponseData {
                        action: inner_envelope,
                        preload,
                    },
                )
            }
            _ => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                "Invalid fence action kind",
            ),
        }
    }

    fn op_task_heartbeat(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskHeartbeatData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        for task_ref in &r.tasks {
            let ttl = self
                .tasks
                .get(&task_ref.id)
                .filter(|t| {
                    t.state == TaskState::Acquired
                        && t.version == task_ref.version
                        && t.pid.as_deref() == Some(&r.pid)
                })
                .and_then(|t| t.ttl);
            if let Some(ttl) = ttl {
                let promise_pending = self
                    .promises
                    .get(&task_ref.id)
                    .map(|p| p.state == PromiseState::Pending)
                    .unwrap_or(false);
                if promise_pending {
                    self.set_t_timeout(&task_ref.id, TTimeoutKind::Lease, now + ttl);
                }
            }
        }
        ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, json!({}))
    }

    fn op_task_halt(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskHaltData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        let task_state = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => t.state,
        };
        if task_state == TaskState::Fulfilled {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task is fulfilled",
            );
        }
        if task_state == TaskState::Halted {
            return ResponseEnvelope::new(
                req.kind.clone(),
                req.head.corr_id.clone(),
                200,
                json!({}),
            );
        }
        if let Some(t) = self.tasks.get_mut(&r.id) {
            t.state = TaskState::Halted;
            t.pid = None;
            t.ttl = None;
        }
        self.del_t_timeout(&r.id);
        ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, json!({}))
    }

    fn op_task_continue(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: TaskContinueData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        self.try_timeout(std::slice::from_ref(&r.id), now);
        let task_state = match self.tasks.get(&r.id) {
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    404,
                    "Task not found",
                )
            }
            Some(t) => t.state,
        };
        if task_state != TaskState::Halted {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task is not halted",
            );
        }
        let promise_pending = self
            .promises
            .get(&r.id)
            .map(|p| p.state == PromiseState::Pending)
            .unwrap_or(false);
        if !promise_pending {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                409,
                "Task is not halted",
            );
        }
        let version = self.tasks.get(&r.id).map(|t| t.version).unwrap_or(0);
        let addr = self
            .promises
            .get(&r.id)
            .and_then(|p| p.tags.get("resonate:target").cloned());
        if let Some(t) = self.tasks.get_mut(&r.id) {
            t.state = TaskState::Pending;
        }
        self.set_t_timeout(&r.id, TTimeoutKind::Retry, now + PENDING_RETRY_TTL);
        if let Some(addr) = addr {
            self.send_execute(&addr, &r.id, version);
        }
        ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, json!({}))
    }

    fn op_task_search(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let r: TaskSearchData = match serde_json::from_value(req.data.clone()) {
            Ok(r) => r,
            Err(e) => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    &format!("Invalid request: {}", e),
                )
            }
        };
        if let Err(e) = r.validate() {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format_validation_errors(&e),
            );
        }
        let limit = match r.limit {
            Some(n) if n > 1000 => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Invalid 'limit' — must be between 1 and 1000",
                )
            }
            Some(n) => n as usize,
            None => 100,
        };
        let mut tasks: Vec<TaskRecord> = self
            .tasks
            .iter()
            .filter(|(_, t)| r.state.map(|s| t.state == s).unwrap_or(true))
            .map(|(id, t)| Self::to_task_record(id, t))
            .collect();
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        let start = r
            .cursor
            .as_deref()
            .and_then(|c| tasks.iter().position(|t| t.id.as_str() > c))
            .unwrap_or(0);
        let page: Vec<_> = tasks.into_iter().skip(start).take(limit + 1).collect();
        let has_more = page.len() > limit;
        let result: Vec<_> = page.into_iter().take(limit).collect();
        let cursor = if has_more {
            result.last().map(|t| t.id.clone())
        } else {
            None
        };
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &TaskSearchResponseData {
                tasks: result,
                cursor,
            },
        )
    }

    // ─── Schedule operations ───────────────────────────────────────────────────

    fn op_schedule_get(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let r: ScheduleGetData = match serde_json::from_value(req.data.clone()) {
            Ok(r) => r,
            Err(e) => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    &format!("Invalid request: {}", e),
                )
            }
        };
        if let Err(e) = r.validate() {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format_validation_errors(&e),
            );
        }
        match self.schedules.get(&r.id) {
            None => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                404,
                "Schedule not found",
            ),
            Some(s) => {
                let record = self.to_schedule_record(&r.id, s);
                ResponseEnvelope::success(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    &ScheduleResponseData { schedule: record },
                )
            }
        }
    }

    fn op_schedule_create(&mut self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope {
        let r: ScheduleCreateData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        if !util::is_valid_cron(&r.cron) {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                "Invalid cron expression",
            );
        }
        if let Some(s) = self.schedules.get(&r.id) {
            let record = self.to_schedule_record(&r.id, s);
            return ResponseEnvelope::success(
                req.kind.clone(),
                req.head.corr_id.clone(),
                &ScheduleResponseData { schedule: record },
            );
        }
        let next_run_at = util::compute_next_cron(&r.cron, now);
        let schedule = Schedule {
            cron: r.cron.clone(),
            promise_id: r.promise_id.clone(),
            promise_timeout: r.promise_timeout,
            promise_param: r.promise_param.clone(),
            promise_tags: r.promise_tags.clone(),
            created_at: now,
            last_run_at: None,
        };
        self.schedules.insert(r.id.clone(), schedule);
        self.set_s_timeout(&r.id, next_run_at);
        let record = self.to_schedule_record(&r.id, self.schedules.get(&r.id).unwrap());
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &ScheduleResponseData { schedule: record },
        )
    }

    fn op_schedule_delete(&mut self, req: &RequestEnvelope) -> ResponseEnvelope {
        let r: ScheduleDeleteData = match self.parse(req) {
            Ok(r) => r,
            Err(e) => return e,
        };
        if self.schedules.remove(&r.id).is_none() {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                404,
                "Schedule not found",
            );
        }
        self.del_s_timeout(&r.id);
        ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, json!({}))
    }

    fn op_schedule_search(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let r: ScheduleSearchData = match serde_json::from_value(req.data.clone()) {
            Ok(r) => r,
            Err(e) => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    &format!("Invalid request: {}", e),
                )
            }
        };
        if let Err(e) = r.validate() {
            return ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                400,
                &format_validation_errors(&e),
            );
        }
        let limit = match r.limit {
            Some(n) if n > 1000 => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Invalid 'limit' — must be between 1 and 1000",
                )
            }
            Some(n) => n as usize,
            None => 10,
        };
        let mut schedules: Vec<ScheduleRecord> = self
            .schedules
            .iter()
            .filter(|(_, s)| {
                r.tags
                    .as_ref()
                    .map(|ft| {
                        ft.iter()
                            .all(|(k, v)| s.promise_tags.get(k).map(|sv| sv == v).unwrap_or(false))
                    })
                    .unwrap_or(true)
            })
            .map(|(id, s)| self.to_schedule_record(id, s))
            .collect();
        schedules.sort_by(|a, b| a.id.cmp(&b.id));
        let start = r
            .cursor
            .as_deref()
            .and_then(|c| schedules.iter().position(|s| s.id.as_str() > c))
            .unwrap_or(0);
        let page: Vec<_> = schedules.into_iter().skip(start).take(limit + 1).collect();
        let has_more = page.len() > limit;
        let result: Vec<_> = page.into_iter().take(limit).collect();
        let cursor = if has_more {
            result.last().map(|s| s.id.clone())
        } else {
            None
        };
        ResponseEnvelope::success(
            req.kind.clone(),
            req.head.corr_id.clone(),
            &ScheduleSearchResponseData {
                schedules: result,
                cursor,
            },
        )
    }

    // ─── Debug operations ──────────────────────────────────────────────────────

    fn op_debug_reset(&mut self, req: &RequestEnvelope) -> ResponseEnvelope {
        self.promises.clear();
        self.tasks.clear();
        self.schedules.clear();
        self.p_timeouts.clear();
        self.t_timeouts.clear();
        self.s_timeouts.clear();
        self.outgoing.clear();
        ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            Value::Object(serde_json::Map::new()),
        )
    }

    fn op_debug_snap(&self, req: &RequestEnvelope) -> ResponseEnvelope {
        let mut promises: Vec<PromiseRecord> = self
            .promises
            .iter()
            .map(|(id, p)| Self::to_promise_record(0, id, p))
            .collect();
        promises.sort_by(|a, b| a.id.cmp(&b.id));

        let mut callbacks: Vec<SnapshotCallback> = self
            .promises
            .iter()
            .flat_map(|(id, p)| {
                p.callbacks.iter().map(move |awaiter| SnapshotCallback {
                    awaiter: awaiter.clone(),
                    awaited: id.clone(),
                })
            })
            .collect();
        callbacks.sort_by(|a, b| a.awaited.cmp(&b.awaited).then(a.awaiter.cmp(&b.awaiter)));

        let mut listeners: Vec<SnapshotListener> = self
            .promises
            .iter()
            .flat_map(|(id, p)| {
                p.listeners.iter().map(move |addr| SnapshotListener {
                    promise_id: id.clone(),
                    address: addr.clone(),
                })
            })
            .collect();
        listeners.sort_by(|a, b| {
            a.promise_id
                .cmp(&b.promise_id)
                .then(a.address.cmp(&b.address))
        });

        let mut tasks: Vec<TaskRecord> = self
            .tasks
            .iter()
            .map(|(id, t)| Self::to_task_record(id, t))
            .collect();
        tasks.sort_by(|a, b| a.id.cmp(&b.id));

        let mut promise_timeouts: Vec<SnapshotPromiseTimeout> = self
            .p_timeouts
            .iter()
            .map(|pt| SnapshotPromiseTimeout {
                id: pt.id.clone(),
                timeout: pt.timeout,
            })
            .collect();
        promise_timeouts.sort_by(|a, b| a.id.cmp(&b.id));

        let mut task_timeouts: Vec<SnapshotTaskTimeout> = self
            .t_timeouts
            .iter()
            .map(|tt| SnapshotTaskTimeout {
                id: tt.id.clone(),
                timeout_type: tt.kind as i32,
                timeout: tt.timeout,
            })
            .collect();
        task_timeouts.sort_by(|a, b| a.id.cmp(&b.id));

        let messages: Vec<SnapshotMessage> = self
            .outgoing
            .iter()
            .map(|(addr, msg)| SnapshotMessage {
                address: addr.clone(),
                message: msg.clone(),
            })
            .collect();

        let snapshot = Snapshot {
            promises,
            promise_timeouts,
            callbacks,
            listeners,
            tasks,
            task_timeouts,
            messages,
        };
        ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            serde_json::to_value(snapshot).unwrap(),
        )
    }

    fn op_debug_tick(&mut self, req: &RequestEnvelope) -> ResponseEnvelope {
        let time = match req.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => {
                return ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Missing or invalid 'time' field",
                )
            }
        };

        // Collect expired promise timeouts
        let expired_promises: Vec<(String, PromiseState, i64)> = self
            .p_timeouts
            .iter()
            .filter(|pt| time >= pt.timeout)
            .filter_map(|pt| {
                self.promises
                    .get(&pt.id)
                    .filter(|p| p.state == PromiseState::Pending)
                    .map(|p| (pt.id.clone(), Self::timeout_state(&p.tags), p.timeout_at))
            })
            .collect();

        // Collect expired task lease timeouts
        let expired_leases: Vec<(String, i64)> = self
            .t_timeouts
            .iter()
            .filter(|tt| time >= tt.timeout && matches!(tt.kind, TTimeoutKind::Lease))
            .filter_map(|tt| {
                self.tasks
                    .get(&tt.id)
                    .filter(|t| t.state == TaskState::Acquired)
                    .map(|t| (tt.id.clone(), t.version))
            })
            .collect();

        // Collect expired task retry timeouts
        let expired_retries: Vec<String> = self
            .t_timeouts
            .iter()
            .filter(|tt| time >= tt.timeout && matches!(tt.kind, TTimeoutKind::Retry))
            .filter_map(|tt| {
                self.tasks
                    .get(&tt.id)
                    .filter(|t| t.state == TaskState::Pending)
                    .map(|_t| tt.id.clone())
            })
            .collect();

        // Phase 1: settle expired promises
        for (id, state, timeout_at) in &expired_promises {
            if let Some(p) = self.promises.get_mut(id) {
                if p.state == PromiseState::Pending {
                    p.state = *state;
                    p.settled_at = Some(*timeout_at);
                }
            }
            self.del_p_timeout(id);
        }

        // Phase 2+3: fulfill tasks, fire callbacks and listeners
        for (id, _, _) in &expired_promises {
            self.trigger_settlement(id, time);
        }

        // Task lease releases
        for (id, version) in &expired_leases {
            let still_valid = self
                .tasks
                .get(id)
                .map(|t| t.state == TaskState::Acquired && t.version == *version)
                .unwrap_or(false);
            if still_valid {
                let addr = self
                    .promises
                    .get(id)
                    .and_then(|p| p.tags.get("resonate:target").cloned());
                let curr_version = self.tasks.get(id).map(|t| t.version).unwrap_or(0);
                if let Some(t) = self.tasks.get_mut(id) {
                    t.state = TaskState::Pending;
                    t.pid = None;
                    t.ttl = None;
                }
                self.del_t_timeout(id);
                self.set_t_timeout(id, TTimeoutKind::Retry, time + PENDING_RETRY_TTL);
                if let Some(addr) = addr {
                    self.send_execute(&addr, id, curr_version);
                }
            }
        }

        // Task retry re-sends
        for id in &expired_retries {
            let still_pending = self
                .tasks
                .get(id)
                .map(|t| t.state == TaskState::Pending)
                .unwrap_or(false);
            if still_pending {
                let addr = self
                    .promises
                    .get(id)
                    .and_then(|p| p.tags.get("resonate:target").cloned());
                let version = self.tasks.get(id).map(|t| t.version).unwrap_or(0);
                self.set_t_timeout(id, TTimeoutKind::Retry, time + PENDING_RETRY_TTL);
                if let Some(addr) = addr {
                    self.send_execute(&addr, id, version);
                }
            }
        }

        // Schedule timeouts
        let expired_schedules: Vec<(String, i64)> = self
            .s_timeouts
            .iter()
            .filter(|st| time >= st.timeout)
            .map(|st| (st.id.clone(), st.timeout))
            .collect();

        for (schedule_id, fired_at) in expired_schedules {
            let (cron, promise_id_template, promise_timeout, promise_param, promise_tags) = {
                let s = match self.schedules.get(&schedule_id) {
                    Some(s) => s,
                    None => continue,
                };
                (
                    s.cron.clone(),
                    s.promise_id.clone(),
                    s.promise_timeout,
                    s.promise_param.clone(),
                    s.promise_tags.clone(),
                )
            };
            let mut current_timeout = fired_at;
            while current_timeout <= time {
                let mut tags = promise_tags.clone();
                tags.insert("resonate:schedule".to_string(), schedule_id.clone());
                let promise_id = promise_id_template
                    .replace("{{.id}}", &schedule_id)
                    .replace("{{.timestamp}}", &current_timeout.to_string());
                if !self.promises.contains_key(&promise_id) {
                    let timeout_at = current_timeout + promise_timeout;
                    let already_timedout = current_timeout >= timeout_at;
                    let (state, created_at, settled_at) = if already_timedout {
                        (
                            Self::timeout_state(&tags),
                            current_timeout,
                            Some(timeout_at),
                        )
                    } else {
                        (PromiseState::Pending, current_timeout, None)
                    };
                    let addr = tags.get("resonate:target").cloned();
                    let promise = Promise {
                        state,
                        param: promise_param.clone(),
                        value: PromiseValue::default(),
                        tags: tags.clone(),
                        timeout_at,
                        created_at,
                        settled_at,
                        callbacks: HashSet::new(),
                        listeners: HashSet::new(),
                    };
                    self.promises.insert(promise_id.clone(), promise);
                    if already_timedout {
                        if addr.is_some() {
                            self.tasks.insert(
                                promise_id.clone(),
                                Task {
                                    state: TaskState::Fulfilled,
                                    version: 0,
                                    pid: None,
                                    ttl: None,
                                    resumes: HashSet::new(),
                                },
                            );
                        }
                    } else {
                        self.set_p_timeout(&promise_id, timeout_at);
                        if let Some(ref a) = addr {
                            self.tasks.insert(
                                promise_id.clone(),
                                Task {
                                    state: TaskState::Pending,
                                    version: 0,
                                    pid: None,
                                    ttl: None,
                                    resumes: HashSet::new(),
                                },
                            );
                            self.set_t_timeout(
                                &promise_id,
                                TTimeoutKind::Retry,
                                created_at + PENDING_RETRY_TTL,
                            );
                            self.send_execute(a, &promise_id, 0);
                        }
                    }
                }
                if let Some(s) = self.schedules.get_mut(&schedule_id) {
                    s.last_run_at = Some(current_timeout);
                }
                current_timeout = util::compute_next_cron(&cron, current_timeout);
            }
            self.set_s_timeout(&schedule_id, current_timeout);
        }

        ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            Value::Array(vec![]),
        )
    }

    // ─── Internal helpers ──────────────────────────────────────────────────────

    fn try_timeout(&mut self, ids: &[String], now: i64) {
        let to_settle: Vec<(String, PromiseState, i64)> = ids
            .iter()
            .filter_map(|id| {
                self.promises
                    .get(id)
                    .filter(|p| p.state == PromiseState::Pending && now >= p.timeout_at)
                    .map(|p| (id.clone(), Self::timeout_state(&p.tags), p.timeout_at))
            })
            .collect();
        for (id, state, timeout_at) in to_settle {
            if let Some(p) = self.promises.get_mut(&id) {
                p.state = state;
                p.settled_at = Some(timeout_at);
            }
            self.del_p_timeout(&id);
            self.trigger_settlement(&id, now);
        }
    }

    fn trigger_settlement(&mut self, promise_id: &str, now: i64) {
        self.trigger_fulfilled(promise_id);
        self.trigger_callbacks(promise_id, now);
        self.trigger_listeners(promise_id, now);
    }

    fn trigger_fulfilled(&mut self, promise_id: &str) {
        let should_fulfill = self
            .tasks
            .get(promise_id)
            .map(|t| t.state != TaskState::Fulfilled)
            .unwrap_or(false);
        if should_fulfill {
            if let Some(t) = self.tasks.get_mut(promise_id) {
                t.state = TaskState::Fulfilled;
                t.pid = None;
                t.ttl = None;
                t.resumes = HashSet::new();
            }
            self.del_t_timeout(promise_id);
            // Remove this task from every promise's callbacks set.
            // SQLite does DELETE FROM callbacks WHERE awaiter_id = promise_id.
            for p in self.promises.values_mut() {
                p.callbacks.remove(promise_id);
            }
        }
    }

    fn trigger_callbacks(&mut self, promise_id: &str, now: i64) {
        let callbacks: Vec<String> = match self.promises.get_mut(promise_id) {
            Some(p) => std::mem::take(&mut p.callbacks).into_iter().collect(),
            None => return,
        };
        for awaiter_id in callbacks {
            let (awaiter_state, awaiter_timeout_at) = match self.promises.get(&awaiter_id) {
                Some(p) => (p.state, p.timeout_at),
                None => continue,
            };
            if awaiter_state != PromiseState::Pending || now >= awaiter_timeout_at {
                continue;
            }
            let task_state = self.tasks.get(&awaiter_id).map(|t| t.state);
            let version = self.tasks.get(&awaiter_id).map(|t| t.version).unwrap_or(0);
            let addr = self
                .promises
                .get(&awaiter_id)
                .and_then(|p| p.tags.get("resonate:target").cloned());
            match task_state {
                Some(TaskState::Suspended) => {
                    if let Some(t) = self.tasks.get_mut(&awaiter_id) {
                        t.state = TaskState::Pending;
                        t.resumes = std::iter::once(promise_id.to_string()).collect();
                    }
                    self.set_t_timeout(&awaiter_id, TTimeoutKind::Retry, now + PENDING_RETRY_TTL);
                    if let Some(addr) = addr {
                        self.send_execute(&addr, &awaiter_id, version);
                    }
                }
                Some(TaskState::Pending | TaskState::Acquired | TaskState::Halted) => {
                    if let Some(t) = self.tasks.get_mut(&awaiter_id) {
                        t.resumes.insert(promise_id.to_string());
                    }
                }
                _ => {}
            }
        }
    }

    fn trigger_listeners(&mut self, promise_id: &str, now: i64) {
        let listeners: Vec<String> = match self.promises.get_mut(promise_id) {
            Some(p) => std::mem::take(&mut p.listeners).into_iter().collect(),
            None => return,
        };
        if listeners.is_empty() {
            return;
        }
        let record = self
            .promises
            .get(promise_id)
            .map(|p| Self::to_promise_record(now, promise_id, p));
        if let Some(record) = record {
            let message = json!({ "kind": "unblock", "head": {}, "data": { "promise": record } });
            for addr in listeners {
                self.outgoing.push((addr, message.clone()));
            }
        }
    }

    fn effective_task_state(&self, now: i64, id: &str) -> Option<TaskState> {
        let t = self.tasks.get(id)?;
        if t.state == TaskState::Fulfilled {
            return Some(TaskState::Fulfilled);
        }
        let promise_settled = self
            .promises
            .get(id)
            .map(|p| p.state != PromiseState::Pending || now >= p.timeout_at)
            .unwrap_or(false);
        if promise_settled {
            Some(TaskState::Fulfilled)
        } else {
            Some(t.state)
        }
    }

    fn send_execute(&mut self, address: &str, task_id: &str, version: i64) {
        let msg = json!({ "kind": "execute", "head": {}, "data": { "task": { "id": task_id, "version": version } } });
        for (addr, existing) in &mut self.outgoing {
            if existing
                .get("kind")
                .and_then(|v| v.as_str())
                .map(|k| k == "execute")
                .unwrap_or(false)
                && existing
                    .pointer("/data/task/id")
                    .and_then(|v| v.as_str())
                    .map(|id| id == task_id)
                    .unwrap_or(false)
            {
                *addr = address.to_string();
                *existing = msg;
                return;
            }
        }
        self.outgoing.push((address.to_string(), msg));
    }

    fn preload(&self, promise_id: &str) -> Vec<PromiseRecord> {
        let branch = match self
            .promises
            .get(promise_id)
            .and_then(|p| p.tags.get("resonate:branch"))
        {
            Some(b) if !b.is_empty() => b.clone(),
            _ => return vec![],
        };
        self.promises
            .iter()
            .filter(|(id, p)| {
                *id != promise_id
                    && p.tags
                        .get("resonate:branch")
                        .map(|b| b == &branch)
                        .unwrap_or(false)
            })
            .map(|(id, p)| Self::to_promise_record(0, id, p))
            .collect()
    }

    fn timeout_state(tags: &HashMap<String, String>) -> PromiseState {
        if tags
            .get("resonate:timer")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            PromiseState::Resolved
        } else {
            PromiseState::RejectedTimedout
        }
    }

    fn settle_to_promise_state(s: SettleState) -> PromiseState {
        match s {
            SettleState::Resolved => PromiseState::Resolved,
            SettleState::Rejected => PromiseState::Rejected,
            SettleState::RejectedCanceled => PromiseState::RejectedCanceled,
        }
    }

    fn to_promise_record(now: i64, id: &str, p: &Promise) -> PromiseRecord {
        let (state, settled_at) =
            if p.state == PromiseState::Pending && now > 0 && now >= p.timeout_at {
                (Self::timeout_state(&p.tags), Some(p.timeout_at))
            } else {
                (p.state, p.settled_at)
            };
        PromiseRecord {
            id: id.to_string(),
            state,
            param: p.param.clone(),
            value: p.value.clone(),
            tags: p.tags.clone(),
            timeout_at: p.timeout_at,
            created_at: p.created_at,
            settled_at,
        }
    }

    fn to_task_record(id: &str, t: &Task) -> TaskRecord {
        TaskRecord {
            id: id.to_string(),
            state: t.state,
            version: t.version,
            resumes: t.resumes.len() as i64,
            ttl: t.ttl,
            pid: t.pid.clone(),
        }
    }

    fn to_schedule_record(&self, id: &str, s: &Schedule) -> ScheduleRecord {
        let next_run_at = self
            .s_timeouts
            .iter()
            .find(|st| st.id == id)
            .map(|st| st.timeout)
            .unwrap_or(0);
        ScheduleRecord {
            id: id.to_string(),
            cron: s.cron.clone(),
            promise_id: s.promise_id.clone(),
            promise_timeout: s.promise_timeout,
            promise_param: s.promise_param.clone(),
            promise_tags: s.promise_tags.clone(),
            created_at: s.created_at,
            next_run_at,
            last_run_at: s.last_run_at,
        }
    }

    // ─── Timeout list helpers ──────────────────────────────────────────────────

    fn set_p_timeout(&mut self, id: &str, timeout: i64) {
        for e in &mut self.p_timeouts {
            if e.id == id {
                e.timeout = timeout;
                return;
            }
        }
        self.p_timeouts.push(PTimeout {
            id: id.to_string(),
            timeout,
        });
    }

    fn del_p_timeout(&mut self, id: &str) {
        self.p_timeouts.retain(|e| e.id != id);
    }

    fn set_t_timeout(&mut self, id: &str, kind: TTimeoutKind, timeout: i64) {
        for e in &mut self.t_timeouts {
            if e.id == id {
                e.kind = kind;
                e.timeout = timeout;
                return;
            }
        }
        self.t_timeouts.push(TTimeout {
            id: id.to_string(),
            kind,
            timeout,
        });
    }

    fn del_t_timeout(&mut self, id: &str) {
        self.t_timeouts.retain(|e| e.id != id);
    }

    fn set_s_timeout(&mut self, id: &str, timeout: i64) {
        for e in &mut self.s_timeouts {
            if e.id == id {
                e.timeout = timeout;
                return;
            }
        }
        self.s_timeouts.push(STimeout {
            id: id.to_string(),
            timeout,
        });
    }

    fn del_s_timeout(&mut self, id: &str) {
        self.s_timeouts.retain(|e| e.id != id);
    }

    // ── Query methods for test generation ────────────────────────────────────

    pub fn all_promise_ids(&self) -> Vec<String> {
        self.promises.keys().cloned().collect()
    }

    pub fn pending_promise_ids(&self) -> Vec<String> {
        self.promises
            .iter()
            .filter(|(_, p)| p.state == PromiseState::Pending)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn tasks_by_state(&self, state: TaskState) -> Vec<(String, i64)> {
        self.tasks
            .iter()
            .filter(|(_, t)| t.state == state)
            .map(|(id, t)| (id.clone(), t.version))
            .collect()
    }

    pub fn schedule_ids(&self) -> Vec<String> {
        self.schedules.keys().cloned().collect()
    }

    pub fn has_tasks_in_state(&self, state: TaskState) -> bool {
        self.tasks.values().any(|t| t.state == state)
    }

    pub fn has_pending_promises(&self) -> bool {
        self.promises
            .values()
            .any(|p| p.state == PromiseState::Pending)
    }

    pub fn has_schedules(&self) -> bool {
        !self.schedules.is_empty()
    }
}
