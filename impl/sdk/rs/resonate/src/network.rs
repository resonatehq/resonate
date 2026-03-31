use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::error::{Error, Result};

/// The Network trait is the transport abstraction. All communication between
/// Resonate and the server (local or remote) flows through it as JSON strings.
#[async_trait::async_trait]
pub trait Network: Send + Sync {
    fn pid(&self) -> &str;
    fn group(&self) -> &str;
    fn unicast(&self) -> &str;
    fn anycast(&self) -> &str;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    async fn send(&self, req: String) -> Result<String>;
    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>);
    fn target_resolver(&self, target: &str) -> String;
}

type Subscribers = Arc<RwLock<Vec<Box<dyn Fn(String) + Send + Sync>>>>;

// =============================================================================
// CONSTANTS
// =============================================================================

const PENDING_RETRY_TTL: i64 = 30_000;

// =============================================================================
// SERVER STATE TYPES (ported from TS Server class in local.ts)
// =============================================================================

struct DurablePromise {
    id: String,
    state: String,
    param: serde_json::Value,
    value: serde_json::Value,
    tags: HashMap<String, String>,
    timeout_at: i64,
    created_at: i64,
    settled_at: Option<i64>,
    awaiters: HashSet<String>,
    subscribers: HashSet<String>,
}

impl DurablePromise {
    fn to_record(&self) -> serde_json::Value {
        let mut obj = serde_json::json!({
            "id": self.id,
            "state": self.state,
            "param": self.param,
            "value": self.value,
            "tags": self.tags,
            "timeoutAt": self.timeout_at,
            "createdAt": self.created_at,
        });
        if let Some(settled_at) = self.settled_at {
            obj["settledAt"] = serde_json::json!(settled_at);
        }
        obj
    }
}

struct Task {
    id: String,
    state: String,
    version: i64,
    pid: Option<String>,
    ttl: Option<i64>,
    resumes: HashSet<String>,
}

impl Task {
    fn to_record(&self) -> serde_json::Value {
        let mut obj = serde_json::json!({
            "id": self.id,
            "state": self.state,
            "version": self.version,
            "promiseId": self.id,
        });
        if let Some(ref pid) = self.pid {
            obj["pid"] = serde_json::json!(pid);
        }
        if let Some(ttl) = self.ttl {
            obj["ttl"] = serde_json::json!(ttl);
        }
        obj
    }
}

#[allow(dead_code)]
struct Schedule {
    id: String,
    cron: String,
    promise_id: String,
    promise_timeout: i64,
    promise_param: serde_json::Value,
    promise_tags: HashMap<String, String>,
    created_at: i64,
    last_run_at: Option<i64>,
}

struct PTimeout {
    id: String,
    timeout: i64,
}
struct TTimeout {
    id: String,
    typ: u8,
    timeout: i64,
}
#[allow(dead_code)]
struct STimeout {
    id: String,
    timeout: i64,
}

struct OutgoingMessage {
    address: String,
    message: serde_json::Value,
}

// =============================================================================
// SERVER STATE MACHINE
// =============================================================================

struct ServerState {
    promises: HashMap<String, DurablePromise>,
    tasks: HashMap<String, Task>,
    schedules: HashMap<String, Schedule>,
    p_timeouts: Vec<PTimeout>,
    t_timeouts: Vec<TTimeout>,
    #[allow(dead_code)]
    s_timeouts: Vec<STimeout>,
    outgoing: Vec<OutgoingMessage>,
}

/// Extract a required non-empty string field from a JSON value.
fn require_str<'a>(obj: &'a serde_json::Value, field: &str) -> std::result::Result<&'a str, Error> {
    match obj.get(field).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => Ok(s),
        _ => Err(Error::ServerError {
            code: 400,
            message: format!("missing or empty required field: {}", field),
        }),
    }
}

/// Extract task ID from request.
fn require_task_id(obj: &serde_json::Value) -> std::result::Result<&str, Error> {
    require_str(obj, "id")
}

impl ServerState {
    fn new() -> Self {
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

    fn apply(
        &mut self,
        now: i64,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        self.outgoing.clear();

        let kind = req.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        let corr_id = req
            .get("corrId")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        // Auto-timeout relevant promises before processing
        match kind {
            "promise.get" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
            }
            "promise.create" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
            }
            "promise.settle" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
            }
            "promise.register_listener" => {
                let id = req.get("awaited").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
            }
            "task.create" => {
                let action = req.get("action").unwrap_or(&serde_json::Value::Null);
                let pd = extract_action_data(action);
                let id = pd.get("id").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
            }
            "task.acquire" | "task.release" | "task.fulfill" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
            }
            "task.suspend" => {
                let id = req.get("id").and_then(|v| v.as_str()).unwrap_or("");
                self.try_auto_timeout(now, id);
                // actions is an array of PromiseRegisterCallbackReq envelopes
                if let Some(actions) = req.get("actions").and_then(|v| v.as_array()) {
                    for action in actions {
                        let action_data = extract_action_data(action);
                        let awaited = action_data
                            .get("awaited")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if !awaited.is_empty() {
                            self.try_auto_timeout(now, awaited);
                        }
                    }
                }
            }
            _ => {}
        }

        match kind {
            "promise.get" => self.promise_get(&corr_id, req),
            "promise.create" => self.promise_create(now, &corr_id, req),
            "promise.settle" => self.promise_settle(now, &corr_id, req),
            "promise.register_listener" => self.promise_register_listener(&corr_id, req),
            "task.create" => self.task_create(now, &corr_id, req),
            "task.acquire" => self.task_acquire(now, &corr_id, req),
            "task.release" => self.task_release(now, &corr_id, req),
            "task.fulfill" => self.task_fulfill(now, &corr_id, req),
            "task.suspend" => self.task_suspend(now, &corr_id, req),
            "task.heartbeat" => self.task_heartbeat(now, &corr_id, req),
            "schedule.create" => self.schedule_create(now, &corr_id, req),
            "schedule.get" => self.schedule_get(&corr_id, req),
            "schedule.delete" => self.schedule_delete(&corr_id, req),
            _ => Err(Error::ServerError {
                code: 400,
                message: format!("unknown request kind: {}", kind),
            }),
        }
    }

    fn tick(&mut self, now: i64) {
        // Collect actions
        let mut promise_settles: Vec<String> = Vec::new();
        let mut task_releases: Vec<(String, i64)> = Vec::new();
        let mut task_retries: Vec<(String, i64)> = Vec::new();

        for pt in &self.p_timeouts {
            if now >= pt.timeout {
                if let Some(p) = self.promises.get(&pt.id) {
                    if p.state == "pending" {
                        promise_settles.push(pt.id.clone());
                    }
                }
            }
        }
        for tt in &self.t_timeouts {
            if now < tt.timeout {
                continue;
            }
            if tt.typ == 1 {
                if let Some(t) = self.tasks.get(&tt.id) {
                    if t.state == "acquired" {
                        task_releases.push((tt.id.clone(), t.version));
                    }
                }
            } else if tt.typ == 0 {
                if let Some(t) = self.tasks.get(&tt.id) {
                    if t.state == "pending" {
                        task_retries.push((tt.id.clone(), t.version));
                    }
                }
            }
        }

        // Phase 1: Settle promises
        for id in &promise_settles {
            if let Some(p) = self.promises.get(id) {
                if p.state != "pending" {
                    continue;
                }
                let state = self.timeout_state(&p.tags.clone());
                let timeout_at = p.timeout_at;
                if let Some(p) = self.promises.get_mut(id) {
                    p.state = state;
                    p.value = serde_json::Value::Null;
                    p.settled_at = Some(timeout_at);
                }
                self.del_p_timeout(id);
            }
        }
        // Phase 2: Fulfill tasks whose own promise settled
        for id in &promise_settles {
            self.enqueue_settle(id);
        }
        // Phase 3: Resume awaiters and notify subscribers
        for id in &promise_settles {
            self.resume_awaiters(id, now);
            self.notify_subscribers(id);
        }

        // Phase 4: Release expired leases
        for (id, version) in &task_releases {
            if let Some(t) = self.tasks.get(id) {
                if t.state == "acquired" && t.version == *version {
                    let new_version = t.version + 1;
                    if let Some(t) = self.tasks.get_mut(id) {
                        t.state = "pending".to_string();
                        t.version = new_version;
                        t.pid = None;
                        t.ttl = None;
                    }
                    self.set_t_timeout(id, 0, now + PENDING_RETRY_TTL);
                    if let Some(p) = self.promises.get(id) {
                        if let Some(addr) = p.tags.get("resonate:target").cloned() {
                            self.send_execute_message(&addr, id, new_version);
                        }
                    }
                }
            }
        }

        // Phase 5: Retry pending tasks
        for (id, _version) in &task_retries {
            if let Some(t) = self.tasks.get(id) {
                if t.state == "pending" {
                    let v = t.version;
                    self.set_t_timeout(id, 0, now + PENDING_RETRY_TTL);
                    if let Some(p) = self.promises.get(id) {
                        if let Some(addr) = p.tags.get("resonate:target").cloned() {
                            self.send_execute_message(&addr, id, v);
                        }
                    }
                }
            }
        }
    }

    // =========================================================================
    // PROMISE OPERATIONS
    // =========================================================================

    fn promise_get(
        &self,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let id = require_str(req, "id")?;
        match self.promises.get(id) {
            Some(p) => Ok(serde_json::json!({
                "kind": "promise.get", "corrId": corr_id, "status": 200,
                "promise": p.to_record(),
            })),
            None => Ok(serde_json::json!({
                "kind": "promise.get", "corrId": corr_id, "status": 404,
                "error": format!("promise {} not found", id),
            })),
        }
    }

    fn promise_create(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let id = require_str(req, "id")?;

        if let Some(existing) = self.promises.get(id) {
            return Ok(serde_json::json!({
                "kind": "promise.create", "corrId": corr_id, "status": 200,
                "promise": existing.to_record(),
            }));
        }

        let timeout_at = req
            .get("timeoutAt")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX);
        let param = req
            .get("param")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let tags = extract_tags(req);

        if now >= timeout_at {
            let state = self.timeout_state(&tags);
            let promise = DurablePromise {
                id: id.to_string(),
                state: state.clone(),
                param,
                value: serde_json::Value::Null,
                tags,
                timeout_at,
                created_at: timeout_at,
                settled_at: Some(timeout_at),
                awaiters: HashSet::new(),
                subscribers: HashSet::new(),
            };
            let record = promise.to_record();
            self.promises.insert(id.to_string(), promise);
            self.enqueue_settle(id);
            self.resume_awaiters(id, now);
            self.notify_subscribers(id);
            return Ok(serde_json::json!({
                "kind": "promise.create", "corrId": corr_id, "status": 200,
                "promise": record,
            }));
        }

        let promise = DurablePromise {
            id: id.to_string(),
            state: "pending".to_string(),
            param,
            value: serde_json::Value::Null,
            tags: tags.clone(),
            timeout_at,
            created_at: now,
            settled_at: None,
            awaiters: HashSet::new(),
            subscribers: HashSet::new(),
        };
        let record = promise.to_record();
        self.promises.insert(id.to_string(), promise);
        self.set_p_timeout(id, timeout_at);

        // Auto-create task and dispatch execute when target tag is present
        if let Some(address) = tags.get("resonate:target").cloned() {
            let delay = tags
                .get("resonate:delay")
                .and_then(|d| d.parse::<i64>().ok());
            let deferred = delay.is_some_and(|d| now < d);

            let task = Task {
                id: id.to_string(),
                state: "pending".to_string(),
                version: 0,
                pid: None,
                ttl: None,
                resumes: HashSet::new(),
            };
            self.tasks.insert(id.to_string(), task);
            self.set_t_timeout(
                id,
                0,
                if deferred {
                    delay.unwrap()
                } else {
                    now + PENDING_RETRY_TTL
                },
            );

            if !deferred {
                self.send_execute_message(&address, id, 0);
            }
        }

        Ok(serde_json::json!({
            "kind": "promise.create", "corrId": corr_id, "status": 201,
            "promise": record,
        }))
    }

    fn promise_settle(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let id = require_str(req, "id")?;
        let settle_state = req
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("resolved");
        let value = req.get("value").cloned().unwrap_or(serde_json::Value::Null);

        match self.promises.get(id) {
            None => Ok(serde_json::json!({
                "kind": "promise.settle", "corrId": corr_id, "status": 404,
                "error": format!("promise {} not found", id),
            })),
            Some(p) if p.state != "pending" => Ok(serde_json::json!({
                "kind": "promise.settle", "corrId": corr_id, "status": 200,
                "promise": p.to_record(),
            })),
            Some(_) => {
                if let Some(p) = self.promises.get_mut(id) {
                    p.state = settle_state.to_string();
                    p.value = value;
                    p.settled_at = Some(now);
                }
                let record = self.promises.get(id).unwrap().to_record();
                self.del_p_timeout(id);
                self.enqueue_settle(id);
                self.resume_awaiters(id, now);
                self.notify_subscribers(id);
                Ok(serde_json::json!({
                    "kind": "promise.settle", "corrId": corr_id, "status": 200,
                    "promise": record,
                }))
            }
        }
    }

    fn promise_register_listener(
        &mut self,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let awaited = require_str(req, "awaited")?;
        let address = require_str(req, "address")?;

        match self.promises.get_mut(awaited) {
            Some(p) => {
                if p.state == "pending" {
                    p.subscribers.insert(address.to_string());
                }
                Ok(serde_json::json!({
                    "kind": "promise.register_listener", "corrId": corr_id, "status": 200,
                    "promise": p.to_record(),
                }))
            }
            None => Ok(serde_json::json!({
                "kind": "promise.register_listener", "corrId": corr_id, "status": 404,
                "error": format!("promise {} not found", awaited),
            })),
        }
    }

    // =========================================================================
    // TASK OPERATIONS
    // =========================================================================

    fn task_create(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let pid = require_str(req, "pid")?;
        let ttl = req
            .get("ttl")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_u64().map(|u| u.min(i64::MAX as u64) as i64))
            })
            .unwrap_or(60_000);
        // action is a PromiseCreateReq envelope
        let action_raw = req.get("action").unwrap_or(&serde_json::Value::Null);
        let promise_req = extract_action_data(action_raw);
        let promise_id = require_str(promise_req, "id")?;

        // Task already exists?
        if let Some(existing_task) = self.tasks.get(promise_id) {
            let task_state = existing_task.state.clone();
            let promise_record = self
                .promises
                .get(promise_id)
                .map(|p| p.to_record())
                .unwrap_or(serde_json::Value::Null);

            match task_state.as_str() {
                "pending" => {
                    let preload = self.preload(promise_id);
                    if let Some(t) = self.tasks.get_mut(promise_id) {
                        t.state = "acquired".to_string();
                        t.pid = Some(pid.to_string());
                        t.ttl = Some(ttl);
                        t.resumes.clear();
                    }
                    self.set_t_timeout(promise_id, 1, now.saturating_add(ttl));
                    let task_record = self.tasks.get(promise_id).unwrap().to_record();
                    return Ok(serde_json::json!({
                        "kind": "task.create", "corrId": corr_id, "status": 200,
                        "task": task_record, "promise": promise_record, "preload": preload,
                    }));
                }
                "fulfilled" => {
                    let task_record = existing_task.to_record();
                    let preload = self.preload(promise_id);
                    return Ok(serde_json::json!({
                        "kind": "task.create", "corrId": corr_id, "status": 200,
                        "task": task_record, "promise": promise_record, "preload": preload,
                    }));
                }
                _ => {
                    return Ok(serde_json::json!({
                        "kind": "task.create", "corrId": corr_id, "status": 409,
                        "promise": promise_record,
                    }));
                }
            }
        }

        // Promise already exists but no task?
        if let Some(existing) = self.promises.get(promise_id) {
            return Ok(serde_json::json!({
                "kind": "task.create", "corrId": corr_id, "status": 409,
                "promise": existing.to_record(),
            }));
        }

        let timeout_at = promise_req
            .get("timeoutAt")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX);
        let param = promise_req
            .get("param")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let tags = extract_tags(promise_req);

        // Already timed out?
        if now >= timeout_at {
            let state = self.timeout_state(&tags);
            let promise = DurablePromise {
                id: promise_id.to_string(),
                state,
                param,
                value: serde_json::Value::Null,
                tags,
                timeout_at,
                created_at: timeout_at,
                settled_at: Some(timeout_at),
                awaiters: HashSet::new(),
                subscribers: HashSet::new(),
            };
            let pr = promise.to_record();
            self.promises.insert(promise_id.to_string(), promise);
            let task = Task {
                id: promise_id.to_string(),
                state: "fulfilled".to_string(),
                version: 0,
                pid: None,
                ttl: None,
                resumes: HashSet::new(),
            };
            let tr = task.to_record();
            self.tasks.insert(promise_id.to_string(), task);
            return Ok(serde_json::json!({
                "kind": "task.create", "corrId": corr_id, "status": 200,
                "task": tr, "promise": pr, "preload": [],
            }));
        }

        // Create promise + task (acquired)
        let promise = DurablePromise {
            id: promise_id.to_string(),
            state: "pending".to_string(),
            param,
            value: serde_json::Value::Null,
            tags,
            timeout_at,
            created_at: now,
            settled_at: None,
            awaiters: HashSet::new(),
            subscribers: HashSet::new(),
        };
        let pr = promise.to_record();
        self.promises.insert(promise_id.to_string(), promise);
        self.set_p_timeout(promise_id, timeout_at);

        let task = Task {
            id: promise_id.to_string(),
            state: "acquired".to_string(),
            version: 0,
            pid: Some(pid.to_string()),
            ttl: Some(ttl),
            resumes: HashSet::new(),
        };
        let tr = task.to_record();
        self.tasks.insert(promise_id.to_string(), task);
        self.set_t_timeout(promise_id, 1, now.saturating_add(ttl));

        let preload = self.preload(promise_id);

        Ok(serde_json::json!({
            "kind": "task.create", "corrId": corr_id, "status": 201,
            "task": tr, "promise": pr, "preload": preload,
        }))
    }

    fn task_acquire(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let task_id = require_task_id(req)?;
        let pid = req.get("pid").and_then(|v| v.as_str()).unwrap_or("");
        let ttl = req
            .get("ttl")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_u64().map(|u| u.min(i64::MAX as u64) as i64))
            })
            .unwrap_or(60_000)
            .max(1); // Ensure TTL is always positive to prevent lease timeouts in the past

        match self.tasks.get(task_id) {
            None => Ok(serde_json::json!({
                "kind": "task.acquire", "corrId": corr_id, "status": 404,
                "error": format!("task {} not found", task_id),
            })),
            Some(t) if t.state != "pending" => Ok(serde_json::json!({
                "kind": "task.acquire", "corrId": corr_id, "status": 409,
                "error": format!("task not in pending state (state: {})", t.state),
            })),
            Some(_) => {
                let preload = self.preload(task_id);
                if let Some(t) = self.tasks.get_mut(task_id) {
                    t.state = "acquired".to_string();
                    t.pid = Some(pid.to_string());
                    t.ttl = Some(ttl);
                    t.resumes.clear();
                }
                self.set_t_timeout(task_id, 1, now.saturating_add(ttl));
                let task_record = self.tasks.get(task_id).unwrap().to_record();
                let promise_record = self
                    .promises
                    .get(task_id)
                    .map(|p| p.to_record())
                    .unwrap_or(serde_json::Value::Null);
                Ok(serde_json::json!({
                    "kind": "task.acquire", "corrId": corr_id, "status": 200,
                    "task": task_record, "promise": promise_record, "preload": preload,
                }))
            }
        }
    }

    fn task_release(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let task_id = require_task_id(req)?;

        match self.tasks.get(task_id) {
            None => Ok(serde_json::json!({
                "kind": "task.release", "corrId": corr_id, "status": 404,
            })),
            Some(t) if t.state != "acquired" => Ok(serde_json::json!({
                "kind": "task.release", "corrId": corr_id, "status": 409,
            })),
            Some(t) => {
                let new_version = t.version + 1;
                if let Some(t) = self.tasks.get_mut(task_id) {
                    t.state = "pending".to_string();
                    t.version = new_version;
                    t.pid = None;
                    t.ttl = None;
                }
                self.set_t_timeout(task_id, 0, now + PENDING_RETRY_TTL);
                if let Some(p) = self.promises.get(task_id) {
                    if let Some(addr) = p.tags.get("resonate:target").cloned() {
                        self.send_execute_message(&addr, task_id, new_version);
                    }
                }
                Ok(serde_json::json!({
                    "kind": "task.release", "corrId": corr_id, "status": 200,
                }))
            }
        }
    }

    fn task_fulfill(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let task_id = require_task_id(req)?;

        match self.tasks.get(task_id) {
            None => {
                return Ok(serde_json::json!({
                    "kind": "task.fulfill", "corrId": corr_id, "status": 404,
                }))
            }
            Some(t) if t.state != "acquired" => {
                return Ok(serde_json::json!({
                    "kind": "task.fulfill", "corrId": corr_id, "status": 409,
                }))
            }
            _ => {}
        }

        // action is a PromiseSettleReq envelope
        let action_raw = req
            .get("action")
            .unwrap_or(&serde_json::Value::Null);
        let settle = extract_action_data(action_raw);
        let promise_id = settle.get("id").and_then(|v| v.as_str()).unwrap_or(task_id);
        let settle_state = settle
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("resolved");
        let value = settle
            .get("value")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        // Settle the promise if still pending
        let was_pending = self
            .promises
            .get(promise_id)
            .is_some_and(|p| p.state == "pending");
        if was_pending {
            if let Some(p) = self.promises.get_mut(promise_id) {
                p.state = settle_state.to_string();
                p.value = value;
                p.settled_at = Some(now);
            }
            self.del_p_timeout(promise_id);
        }

        let promise_record = self
            .promises
            .get(promise_id)
            .map(|p| p.to_record())
            .unwrap_or(serde_json::Value::Null);

        self.enqueue_settle(task_id);
        self.resume_awaiters(promise_id, now);
        self.notify_subscribers(promise_id);

        Ok(serde_json::json!({
            "kind": "task.fulfill", "corrId": corr_id, "status": 200,
            "promise": promise_record,
        }))
    }

    fn task_suspend(
        &mut self,
        _now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let task_id = require_task_id(req)?;

        match self.tasks.get(task_id) {
            None => {
                return Ok(serde_json::json!({
                    "kind": "task.suspend", "corrId": corr_id, "status": 404,
                }))
            }
            Some(t) if t.state != "acquired" => {
                return Ok(serde_json::json!({
                    "kind": "task.suspend", "corrId": corr_id, "status": 409,
                }))
            }
            _ => {}
        }

        // If task already has resumes (dependency resolved while acquired), redirect
        let has_resumes = self
            .tasks
            .get(task_id)
            .is_some_and(|t| !t.resumes.is_empty());
        if has_resumes {
            if let Some(t) = self.tasks.get_mut(task_id) {
                t.resumes.clear();
            }
            let preload = self.preload(task_id);
            return Ok(serde_json::json!({
                "kind": "task.suspend", "corrId": corr_id, "status": 300,
                "redirect": true, "preload": preload,
            }));
        }

        // Parse actions — array of PromiseRegisterCallbackReq envelopes
        let callbacks: Vec<String> = req
            .get("actions")
            .and_then(|v| v.as_array())
            .map(|actions| {
                actions
                    .iter()
                    .filter_map(|a| {
                        let action_data = extract_action_data(a);
                        action_data
                            .get("awaited")
                            .and_then(|v| v.as_str())
                            .map(String::from)
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Register this task as an awaiter on each awaited promise.
        // If any awaited promise is already settled, redirect immediately.
        let mut any_settled = false;
        for awaited_id in &callbacks {
            match self.promises.get_mut(awaited_id) {
                Some(p) if p.state == "pending" => {
                    p.awaiters.insert(task_id.to_string());
                }
                Some(_) => {
                    // Already settled
                    any_settled = true;
                }
                None => {
                    // Promise doesn't exist — shouldn't happen but handle gracefully
                }
            }
        }

        if any_settled {
            let preload = self.preload(task_id);
            return Ok(serde_json::json!({
                "kind": "task.suspend", "corrId": corr_id, "status": 300,
                "redirect": true, "preload": preload,
            }));
        }

        // Actually suspend
        if let Some(t) = self.tasks.get_mut(task_id) {
            t.state = "suspended".to_string();
            t.pid = None;
            t.ttl = None;
            t.resumes.clear();
        }
        self.del_t_timeout(task_id);

        Ok(serde_json::json!({
            "kind": "task.suspend", "corrId": corr_id, "status": 200,
        }))
    }

    fn task_heartbeat(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let pid = require_str(req, "pid")?;

        if let Some(tasks) = req.get("tasks").and_then(|v| v.as_array()) {
            for task_ref in tasks {
                let id = require_str(task_ref, "id")?;
                let version = task_ref.get("version").and_then(|v| v.as_i64());
                if let Some(t) = self.tasks.get(id) {
                    if t.state == "acquired"
                        && t.pid.as_deref() == Some(pid)
                        && version.is_none_or(|v| v == t.version)
                    {
                        let ttl = t.ttl.unwrap_or(30_000);
                        self.set_t_timeout(id, 1, now.saturating_add(ttl));
                    }
                }
            }
        }

        Ok(serde_json::json!({
            "kind": "task.heartbeat", "corrId": corr_id, "status": 200,
        }))
    }

    // =========================================================================
    // SCHEDULE OPERATIONS (stubs for local mode)
    // =========================================================================

    fn schedule_create(
        &mut self,
        now: i64,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let id = require_str(req, "id")?;
        if self.schedules.contains_key(id) {
            return Ok(serde_json::json!({
                "kind": "schedule.create", "corrId": corr_id, "status": 200,
            }));
        }
        let schedule = Schedule {
            id: id.to_string(),
            cron: require_str(req, "cron")?.to_string(),
            promise_id: req
                .get("promiseId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            promise_timeout: req
                .get("promiseTimeout")
                .and_then(|v| v.as_i64())
                .unwrap_or(60_000),
            promise_param: req
                .get("promiseParam")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            promise_tags: extract_tags(req),
            created_at: now,
            last_run_at: None,
        };
        self.schedules.insert(id.to_string(), schedule);
        Ok(serde_json::json!({
            "kind": "schedule.create", "corrId": corr_id, "status": 201,
        }))
    }

    fn schedule_get(
        &self,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let id = require_str(req, "id")?;
        if self.schedules.contains_key(id) {
            Ok(serde_json::json!({
                "kind": "schedule.get", "corrId": corr_id, "status": 200,
            }))
        } else {
            Ok(serde_json::json!({
                "kind": "schedule.get", "corrId": corr_id, "status": 404,
            }))
        }
    }

    fn schedule_delete(
        &mut self,
        corr_id: &serde_json::Value,
        req: &serde_json::Value,
    ) -> std::result::Result<serde_json::Value, Error> {
        let id = require_str(req, "id")?;
        self.schedules.remove(id);
        Ok(serde_json::json!({
            "kind": "schedule.delete", "corrId": corr_id, "status": 200,
        }))
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    /// Auto-timeout a promise if it has expired.
    fn try_auto_timeout(&mut self, now: i64, id: &str) {
        let should_timeout = self
            .promises
            .get(id)
            .is_some_and(|p| p.state == "pending" && now >= p.timeout_at);
        if !should_timeout {
            return;
        }

        let tags = self.promises.get(id).unwrap().tags.clone();
        let state = self.timeout_state(&tags);
        let timeout_at = self.promises.get(id).unwrap().timeout_at;

        if let Some(p) = self.promises.get_mut(id) {
            p.state = state;
            p.settled_at = Some(timeout_at);
        }
        self.del_p_timeout(id);
        self.enqueue_settle(id);
        self.resume_awaiters(id, now);
        self.notify_subscribers(id);
    }

    /// When a promise settles, fulfill its associated task (if any).
    fn enqueue_settle(&mut self, promise_id: &str) {
        match self.tasks.get(promise_id) {
            None => {
                // If promise has a target but no task, create a fulfilled task
                let has_target = self
                    .promises
                    .get(promise_id)
                    .is_some_and(|p| p.tags.contains_key("resonate:target"));
                if has_target {
                    self.tasks.insert(
                        promise_id.to_string(),
                        Task {
                            id: promise_id.to_string(),
                            state: "fulfilled".to_string(),
                            version: 0,
                            pid: None,
                            ttl: None,
                            resumes: HashSet::new(),
                        },
                    );
                }
            }
            Some(t) if t.state == "fulfilled" => {}
            Some(_) => {
                // Fulfill the task and remove it as an awaiter from all promises
                if let Some(t) = self.tasks.get_mut(promise_id) {
                    t.state = "fulfilled".to_string();
                    t.pid = None;
                    t.ttl = None;
                    t.resumes.clear();
                }
                self.del_t_timeout(promise_id);

                // Remove this task from all promise awaiters
                let task_id = promise_id.to_string();
                for p in self.promises.values_mut() {
                    p.awaiters.remove(&task_id);
                }
            }
        }
    }

    /// When a promise settles, resume all tasks that were awaiting it.
    fn resume_awaiters(&mut self, promise_id: &str, now: i64) {
        let awaiter_ids: Vec<String> = self
            .promises
            .get(promise_id)
            .map(|p| p.awaiters.iter().cloned().collect())
            .unwrap_or_default();

        for awaiter_id in &awaiter_ids {
            if let Some(task) = self.tasks.get(awaiter_id) {
                match task.state.as_str() {
                    "suspended" => {
                        let new_version = task.version + 1;
                        if let Some(t) = self.tasks.get_mut(awaiter_id) {
                            t.state = "pending".to_string();
                            t.version = new_version;
                            t.resumes = HashSet::from([promise_id.to_string()]);
                        }
                        self.set_t_timeout(awaiter_id, 0, now + PENDING_RETRY_TTL);

                        if let Some(p) = self.promises.get(awaiter_id) {
                            if let Some(addr) = p.tags.get("resonate:target").cloned() {
                                self.send_execute_message(&addr, awaiter_id, new_version);
                            }
                        }
                    }
                    "pending" | "acquired" | "halted" => {
                        if let Some(t) = self.tasks.get_mut(awaiter_id) {
                            t.resumes.insert(promise_id.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }

        // Clear awaiters
        if let Some(p) = self.promises.get_mut(promise_id) {
            p.awaiters.clear();
        }
    }

    /// Notify all subscribers (listener addresses) of a settled promise.
    fn notify_subscribers(&mut self, promise_id: &str) {
        let (subscribers, record) = match self.promises.get(promise_id) {
            Some(p) if !p.subscribers.is_empty() => (
                p.subscribers.iter().cloned().collect::<Vec<_>>(),
                p.to_record(),
            ),
            _ => return,
        };

        for address in &subscribers {
            self.outgoing.push(OutgoingMessage {
                address: address.clone(),
                message: serde_json::json!({
                    "kind": "unblock",
                    "data": { "promise": record },
                }),
            });
        }

        if let Some(p) = self.promises.get_mut(promise_id) {
            p.subscribers.clear();
        }
    }

    /// Return all promises sharing the same branch tag as the given promise.
    fn preload(&self, promise_id: &str) -> Vec<serde_json::Value> {
        let branch = match self.promises.get(promise_id) {
            Some(p) => p.tags.get("resonate:branch").cloned(),
            None => return vec![],
        };
        let branch = match branch {
            Some(b) => b,
            None => return vec![],
        };

        self.promises
            .values()
            .filter(|p| p.id != promise_id && (p.tags.get("resonate:branch") == Some(&branch)))
            .map(|p| p.to_record())
            .collect()
    }

    fn timeout_state(&self, tags: &HashMap<String, String>) -> String {
        if tags.get("resonate:timer").is_some_and(|v| v == "true") {
            "resolved".to_string()
        } else {
            "rejected_timedout".to_string()
        }
    }

    fn set_p_timeout(&mut self, id: &str, timeout: i64) {
        if let Some(pt) = self.p_timeouts.iter_mut().find(|pt| pt.id == id) {
            pt.timeout = timeout;
        } else {
            self.p_timeouts.push(PTimeout {
                id: id.to_string(),
                timeout,
            });
        }
    }

    fn del_p_timeout(&mut self, id: &str) {
        self.p_timeouts.retain(|pt| pt.id != id);
    }

    fn set_t_timeout(&mut self, id: &str, typ: u8, timeout: i64) {
        if let Some(tt) = self.t_timeouts.iter_mut().find(|tt| tt.id == id) {
            tt.typ = typ;
            tt.timeout = timeout;
        } else {
            self.t_timeouts.push(TTimeout {
                id: id.to_string(),
                typ,
                timeout,
            });
        }
    }

    fn del_t_timeout(&mut self, id: &str) {
        self.t_timeouts.retain(|tt| tt.id != id);
    }

    fn send_execute_message(&mut self, address: &str, task_id: &str, version: i64) {
        let msg = serde_json::json!({
            "kind": "execute",
            "data": { "task": { "id": task_id, "version": version } },
        });
        // Upsert: replace existing execute message for same task (like TS)
        if let Some(existing) = self.outgoing.iter_mut().find(|m| {
            m.message.get("kind").and_then(|k| k.as_str()) == Some("execute")
                && m.message
                    .get("data")
                    .and_then(|d| d.get("task"))
                    .and_then(|t| t.get("id"))
                    .and_then(|id| id.as_str())
                    == Some(task_id)
        }) {
            existing.address = address.to_string();
            existing.message = msg;
        } else {
            self.outgoing.push(OutgoingMessage {
                address: address.to_string(),
                message: msg,
            });
        }
    }
}

// =============================================================================
// LOCAL NETWORK
// =============================================================================

pub struct LocalNetwork {
    pid: String,
    group: String,
    unicast: String,
    anycast: String,
    state: Arc<Mutex<ServerState>>,
    subscribers: Subscribers,
    tick_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl LocalNetwork {
    pub fn new(pid: Option<String>, group: Option<String>) -> Self {
        let pid = pid.unwrap_or_else(uuid_no_dashes);
        let group = group.unwrap_or_else(|| "default".to_string());
        let unicast = format!("local://uni@{}/{}", group, pid);
        let anycast = format!("local://any@{}/{}", group, pid);

        Self {
            pid,
            group,
            unicast,
            anycast,
            state: Arc::new(Mutex::new(ServerState::new())),
            subscribers: Arc::new(RwLock::new(Vec::new())),
            tick_handle: Mutex::new(None),
        }
    }

    /// Dispatch outgoing messages to all subscribers (async, off the critical path).
    fn dispatch_messages(subscribers: Subscribers, messages: Vec<OutgoingMessage>) {
        if messages.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let subs = subscribers.read().await;
            for msg in messages {
                let msg_str = serde_json::to_string(&msg.message).unwrap_or_default();
                for cb in subs.iter() {
                    cb(msg_str.clone());
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl Network for LocalNetwork {
    fn pid(&self) -> &str {
        &self.pid
    }
    fn group(&self) -> &str {
        &self.group
    }
    fn unicast(&self) -> &str {
        &self.unicast
    }
    fn anycast(&self) -> &str {
        &self.anycast
    }

    async fn start(&self) -> Result<()> {
        let state = self.state.clone();
        let subscribers = self.subscribers.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                let now = now_ms();
                let outgoing = {
                    let mut st = state.lock().await;
                    st.outgoing.clear();
                    st.tick(now);
                    std::mem::take(&mut st.outgoing)
                };
                Self::dispatch_messages(subscribers.clone(), outgoing);
            }
        });

        *self.tick_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(handle) = self.tick_handle.lock().await.take() {
            handle.abort();
        }
        self.subscribers.write().await.clear();
        Ok(())
    }

    async fn send(&self, req: String) -> Result<String> {
        let req_json: serde_json::Value = serde_json::from_str(&req)
            .map_err(|e| Error::DecodingError(format!("invalid JSON request: {}", e)))?;

        // Unwrap envelope if present: extract flat request for internal processing
        let flat_req = unwrap_request_envelope(&req_json);

        let now = now_ms();
        let (flat_response, outgoing) = {
            let mut state = self.state.lock().await;
            let resp = state.apply(now, &flat_req)?;
            let outgoing = std::mem::take(&mut state.outgoing);
            (resp, outgoing)
        };

        // Wrap flat response in protocol envelope
        let envelope_response = wrap_response_envelope(&flat_response);
        let resp_str = serde_json::to_string(&envelope_response)?;

        // Dispatch messages asynchronously (like TS setTimeout(0))
        Self::dispatch_messages(self.subscribers.clone(), outgoing);

        Ok(resp_str)
    }

    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        let subscribers = self.subscribers.clone();
        tokio::spawn(async move {
            subscribers.write().await.push(callback);
        });
    }

    fn target_resolver(&self, target: &str) -> String {
        format!("local://any@{}", target)
    }
}

// =============================================================================
// UTILITIES
// =============================================================================

fn uuid_no_dashes() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", now ^ 0xdeadbeef_cafebabe_u128)
}

use crate::now_ms;

fn extract_tags(v: &serde_json::Value) -> HashMap<String, String> {
    v.get("tags")
        .and_then(|t| t.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

// =============================================================================
// PROTOCOL ENVELOPE HELPERS
// =============================================================================

const PROTOCOL_VERSION: &str = "2025-01-15";

/// Unwrap a protocol envelope request into the flat format expected by ServerState.
/// If the request is already in flat format, return it as-is.
/// Extract the data portion from a value that may be a protocol sub-envelope
/// `{ kind, head, data }` or a flat data object. Returns the data portion either way.
fn extract_action_data(val: &serde_json::Value) -> &serde_json::Value {
    // If it looks like an envelope, return the data portion
    if val.get("kind").is_some() && val.get("data").is_some() {
        val.get("data").unwrap_or(val)
    } else {
        val
    }
}

fn unwrap_request_envelope(req: &serde_json::Value) -> serde_json::Value {
    if req.get("head").is_some() && req.get("data").is_some() {
        // Envelope format: { kind, head: { corrId, ... }, data: { ... } }
        let mut flat = req
            .get("data")
            .and_then(|d| d.as_object())
            .cloned()
            .unwrap_or_default();
        if let Some(kind) = req.get("kind") {
            flat.insert("kind".to_string(), kind.clone());
        }
        if let Some(corr_id) = req.get("head").and_then(|h| h.get("corrId")) {
            flat.insert("corrId".to_string(), corr_id.clone());
        }
        serde_json::Value::Object(flat)
    } else {
        req.clone()
    }
}

/// Wrap a flat response from ServerState into the protocol envelope format.
fn wrap_response_envelope(flat: &serde_json::Value) -> serde_json::Value {
    let kind = flat.get("kind").cloned().unwrap_or(serde_json::Value::Null);
    let corr_id = flat
        .get("corrId")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let status = flat.get("status").and_then(|s| s.as_u64()).unwrap_or(200);

    let mut data = flat.as_object().cloned().unwrap_or_default();
    data.remove("kind");
    data.remove("corrId");
    data.remove("status");

    serde_json::json!({
        "kind": kind,
        "head": {
            "corrId": corr_id,
            "status": status,
            "version": PROTOCOL_VERSION,
        },
        "data": data,
    })
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to extract status from envelope response
    fn status(resp: &serde_json::Value) -> u64 {
        resp.get("head")
            .and_then(|h| h.get("status"))
            .and_then(|s| s.as_u64())
            .unwrap_or(0)
    }

    /// Helper to extract data from envelope response
    fn data(resp: &serde_json::Value) -> &serde_json::Value {
        resp.get("data").unwrap_or(resp)
    }

    #[tokio::test]
    async fn local_network_creates_and_gets_promise() {
        let net = LocalNetwork::new(Some("test-pid".into()), Some("default".into()));
        let req = serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "id": "p1", "timeoutAt": i64::MAX,
                "param": {"data": "test"}, "tags": {"resonate:scope": "global"},
            },
        });
        let resp: serde_json::Value =
            serde_json::from_str(&net.send(req.to_string()).await.unwrap()).unwrap();
        assert!(status(&resp) == 200 || status(&resp) == 201);
        assert_eq!(data(&resp)["promise"]["id"], "p1");
        assert_eq!(data(&resp)["promise"]["state"], "pending");

        let get_req = serde_json::json!({
            "kind": "promise.get",
            "head": { "corrId": "c2", "version": "2025-01-15" },
            "data": { "id": "p1" },
        });
        let get_resp: serde_json::Value =
            serde_json::from_str(&net.send(get_req.to_string()).await.unwrap()).unwrap();
        assert_eq!(status(&get_resp), 200);
        assert_eq!(data(&get_resp)["promise"]["id"], "p1");
    }

    #[tokio::test]
    async fn local_network_idempotent_promise_create() {
        let net = LocalNetwork::new(None, None);
        let req = serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": { "id": "p1", "timeoutAt": i64::MAX, "param": {}, "tags": {} },
        });
        let r1: serde_json::Value =
            serde_json::from_str(&net.send(req.to_string()).await.unwrap()).unwrap();
        assert!(status(&r1) == 200 || status(&r1) == 201);

        let r2: serde_json::Value =
            serde_json::from_str(&net.send(req.to_string()).await.unwrap()).unwrap();
        assert_eq!(status(&r2), 200);
        assert_eq!(data(&r2)["promise"]["id"], "p1");
    }

    #[tokio::test]
    async fn local_network_task_create_and_fulfill() {
        let net = LocalNetwork::new(Some("pid1".into()), None);
        let req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "p1", "timeoutAt": i64::MAX,
                        "param": {"data": "test"}, "tags": {},
                    },
                },
            },
        });
        let resp: serde_json::Value =
            serde_json::from_str(&net.send(req.to_string()).await.unwrap()).unwrap();
        assert!(status(&resp) == 200 || status(&resp) == 201);
        assert_eq!(data(&resp)["task"]["state"], "acquired");
        assert_eq!(data(&resp)["promise"]["id"], "p1");

        let task_id = data(&resp)["task"]["id"].as_str().unwrap();
        let fulfill = serde_json::json!({
            "kind": "task.fulfill",
            "head": { "corrId": "c2", "version": "2025-01-15" },
            "data": {
                "id": task_id,
                "version": 0,
                "action": {
                    "kind": "promise.settle",
                    "head": { "corrId": "c2a", "version": "2025-01-15" },
                    "data": { "id": "p1", "state": "resolved", "value": {"data": "result"} },
                },
            },
        });
        let f_resp: serde_json::Value =
            serde_json::from_str(&net.send(fulfill.to_string()).await.unwrap()).unwrap();
        assert_eq!(status(&f_resp), 200);
    }

    #[tokio::test]
    async fn local_network_identity() {
        let net = LocalNetwork::new(Some("mypid".into()), Some("mygroup".into()));
        assert_eq!(net.pid(), "mypid");
        assert_eq!(net.group(), "mygroup");
        assert_eq!(net.unicast(), "local://uni@mygroup/mypid");
        assert_eq!(net.anycast(), "local://any@mygroup/mypid");
        assert_eq!(net.target_resolver("target"), "local://any@target");
    }

    #[tokio::test]
    async fn promise_create_with_target_creates_task_and_dispatches_execute() {
        let net = LocalNetwork::new(Some("pid1".into()), None);
        let req = serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "id": "rpc-1", "timeoutAt": i64::MAX,
                "param": {"data": "test"},
                "tags": { "resonate:target": "local://any@hello", "resonate:scope": "global" },
            },
        });
        let resp: serde_json::Value =
            serde_json::from_str(&net.send(req.to_string()).await.unwrap()).unwrap();
        assert_eq!(data(&resp)["promise"]["state"], "pending");

        // The task should exist in pending state
        let state = net.state.lock().await;
        let task = state.tasks.get("rpc-1").expect("task should exist");
        assert_eq!(task.state, "pending");
        assert_eq!(task.id, "rpc-1");
    }

    #[tokio::test]
    async fn task_suspend_registers_awaiters_and_suspends() {
        let net = LocalNetwork::new(Some("pid1".into()), None);

        // Create a task (acquired)
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "parent", "timeoutAt": i64::MAX,
                        "tags": { "resonate:target": "local://any@wf" },
                    },
                },
            },
        });
        net.send(create_req.to_string()).await.unwrap();

        // Create a child promise (pending, represents an RPC dependency)
        let child_req = serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "c2", "version": "2025-01-15" },
            "data": {
                "id": "child-1", "timeoutAt": i64::MAX,
                "tags": { "resonate:target": "local://any@hello" },
            },
        });
        net.send(child_req.to_string()).await.unwrap();

        // Suspend the parent task waiting on child
        let suspend_req = serde_json::json!({
            "kind": "task.suspend",
            "head": { "corrId": "c3", "version": "2025-01-15" },
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [{
                    "kind": "promise.register_callback",
                    "head": { "corrId": "c3a", "version": "2025-01-15" },
                    "data": { "awaited": "child-1", "awaiter": "parent" },
                }],
            },
        });
        let resp: serde_json::Value =
            serde_json::from_str(&net.send(suspend_req.to_string()).await.unwrap()).unwrap();
        assert_eq!(status(&resp), 200);

        let state = net.state.lock().await;
        assert_eq!(state.tasks.get("parent").unwrap().state, "suspended");
        assert!(state
            .promises
            .get("child-1")
            .unwrap()
            .awaiters
            .contains("parent"));
    }

    #[tokio::test]
    async fn settling_child_resumes_suspended_parent() {
        let net = LocalNetwork::new(Some("pid1".into()), None);

        // Create parent task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "parent", "timeoutAt": i64::MAX,
                        "tags": { "resonate:target": "local://any@wf" },
                    },
                },
            },
        });
        net.send(create_req.to_string()).await.unwrap();

        // Create child promise
        let child_req = serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "c2", "version": "2025-01-15" },
            "data": {
                "id": "child", "timeoutAt": i64::MAX,
                "tags": { "resonate:target": "local://any@hello" },
            },
        });
        net.send(child_req.to_string()).await.unwrap();

        // Suspend parent on child
        let suspend_req = serde_json::json!({
            "kind": "task.suspend",
            "head": { "corrId": "c3", "version": "2025-01-15" },
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [{
                    "kind": "promise.register_callback",
                    "head": { "corrId": "c3a", "version": "2025-01-15" },
                    "data": { "awaited": "child", "awaiter": "parent" },
                }],
            },
        });
        net.send(suspend_req.to_string()).await.unwrap();

        // Acquire child task then fulfill it
        let acquire_req = serde_json::json!({
            "kind": "task.acquire",
            "head": { "corrId": "c4", "version": "2025-01-15" },
            "data": { "id": "child", "version": 0, "pid": "pid1", "ttl": 60000 },
        });
        net.send(acquire_req.to_string()).await.unwrap();

        let fulfill_req = serde_json::json!({
            "kind": "task.fulfill",
            "head": { "corrId": "c5", "version": "2025-01-15" },
            "data": {
                "id": "child",
                "version": 0,
                "action": {
                    "kind": "promise.settle",
                    "head": { "corrId": "c5a", "version": "2025-01-15" },
                    "data": { "id": "child", "state": "resolved", "value": {"data": "hello"} },
                },
            },
        });
        net.send(fulfill_req.to_string()).await.unwrap();

        // Parent should be resumed (pending, version incremented)
        let state = net.state.lock().await;
        let parent_task = state.tasks.get("parent").unwrap();
        assert_eq!(parent_task.state, "pending");
        assert_eq!(parent_task.version, 1);
    }

    #[tokio::test]
    async fn task_suspend_redirect_when_dependency_already_settled() {
        let net = LocalNetwork::new(Some("pid1".into()), None);

        // Create parent task
        let create_req = serde_json::json!({
            "kind": "task.create",
            "head": { "corrId": "c1", "version": "2025-01-15" },
            "data": {
                "pid": "pid1", "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": { "corrId": "c1a", "version": "2025-01-15" },
                    "data": {
                        "id": "parent", "timeoutAt": i64::MAX,
                        "tags": { "resonate:target": "local://any@wf" },
                    },
                },
            },
        });
        net.send(create_req.to_string()).await.unwrap();

        // Create and immediately settle child promise
        let child_req = serde_json::json!({
            "kind": "promise.create",
            "head": { "corrId": "c2", "version": "2025-01-15" },
            "data": { "id": "child", "timeoutAt": i64::MAX, "param": {}, "tags": {} },
        });
        net.send(child_req.to_string()).await.unwrap();

        let settle_req = serde_json::json!({
            "kind": "promise.settle",
            "head": { "corrId": "c3", "version": "2025-01-15" },
            "data": { "id": "child", "state": "resolved", "value": {"data": "ok"} },
        });
        net.send(settle_req.to_string()).await.unwrap();

        // Suspend parent on already-settled child → should get redirect
        let suspend_req = serde_json::json!({
            "kind": "task.suspend",
            "head": { "corrId": "c4", "version": "2025-01-15" },
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [{
                    "kind": "promise.register_callback",
                    "head": { "corrId": "c4a", "version": "2025-01-15" },
                    "data": { "awaited": "child", "awaiter": "parent" },
                }],
            },
        });
        let resp: serde_json::Value =
            serde_json::from_str(&net.send(suspend_req.to_string()).await.unwrap()).unwrap();
        assert_eq!(status(&resp), 300);
    }
}
