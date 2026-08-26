//! The kernel's state, requests, and effects.
//!
//! # Contract
//!
//! Everything here is data. The transition function lives in
//! [`handle`](super::handle) and [`drain`](super::drain), and is *defined* as
//! `apply_effects(handle(&doc, ..).0)` — there is no second updater that could
//! drift from the decider.
//!
//! One [`OriginDoc`] holds every promise and task of one origin. That is
//! possible because every promise/task operation is single-origin: a callback
//! requires `origin(awaiter) == origin(awaited)`, `task.suspend` requires the
//! awaited set to share the task's origin, and a heartbeat batch shares an
//! origin (see the validators in `core::types`).
//!
//! Collections are `BTree*` so iteration order — and therefore the encoded
//! bytes and the drain sweep — is a function of the state alone.
//!
//! # Dependencies
//!
//! `core::types` for the records, payloads and request shapes the document
//! embeds.
//!
//! # Dependants
//!
//! `handle` and `drain` decide over these types; the s3 applier applies the
//! effects and the s3 codec encodes the document.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::core::types::{
    PromiseCreateData, PromiseGetData, PromiseRecord, PromiseRegisterCallbackData,
    PromiseRegisterListenerData, PromiseState, PromiseValue, TaskAcquireData, TaskContinueData,
    TaskCreateData, TaskFenceData, TaskFulfillData, TaskGetData, TaskHaltData, TaskHeartbeatData,
    TaskRecord, TaskReleaseData, TaskState, TaskSuspendData,
};

/// The tag that makes a promise dispatchable: the address its task is sent to.
pub const TAG_TARGET: &str = "resonate:target";
/// The tag that makes an expiring promise resolve rather than reject.
pub const TAG_TIMER: &str = "resonate:timer";
/// The tag that defers a new task's first dispatch to an absolute time.
pub const TAG_DELAY: &str = "resonate:delay";
/// The tag that groups promises preloaded together.
pub const TAG_BRANCH: &str = "resonate:branch";

/// One origin's entire state.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OriginDoc {
    /// Promises by full promise id.
    pub promises: BTreeMap<String, PromiseDoc>,
    /// Tasks by full promise id — a task's id *is* its promise's id.
    pub tasks: BTreeMap<String, TaskDoc>,
    /// High-water `now` for this origin. Never decreases, so an origin's view
    /// of time is monotone even if a caller's clock regresses.
    pub clock: i64,
    /// Bumped by the shell once per committed write. Diagnostic only.
    pub gen: u64,
    /// Deadline of the timer object currently armed for this origin, if any.
    /// Maintained by the kernel as `min_deadline` of the emitted document.
    pub timer_at: Option<i64>,
}

/// A promise, plus the callback and listener registrations against it.
///
/// A superset of [`PromiseRecord`]: same fields, plus the two registration
/// lists that the SQL backends keep in `callbacks` and `listeners` tables.
#[derive(Debug, Clone, PartialEq)]
pub struct PromiseDoc {
    pub state: PromiseState,
    pub param: PromiseValue,
    pub value: PromiseValue,
    pub tags: BTreeMap<String, String>,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    /// Awaiter ids, in registration order. Order is protocol-visible: a
    /// settlement fans out to awaiters in the order they registered.
    pub callbacks: Vec<String>,
    /// Listener addresses, in registration order. Unique.
    pub listeners: Vec<String>,
}

/// A task, with the `task_timeouts` row folded in.
///
/// The SQL backends store one timeout row per task, discriminated by
/// `timeout_type` (0 = retry, 1 = lease). Here that is two `Option`s, and
/// [`check_invariants`] holds them to "at most one armed".
#[derive(Debug, Clone, PartialEq)]
pub struct TaskDoc {
    pub state: TaskState,
    pub version: i64,
    pub pid: Option<String>,
    pub ttl: Option<i64>,
    /// Awaited promise ids whose settlement this task has not yet observed.
    /// `resumes` on the wire is this set's size.
    pub resumes: BTreeSet<String>,
    /// Re-dispatch deadline while pending — `timeout_type = 0`.
    pub retry_at: Option<i64>,
    /// Lease expiry while acquired — `timeout_type = 1`.
    pub lease_at: Option<i64>,
}

impl PromiseDoc {
    /// The address this promise's task dispatches to, if it has one.
    pub fn target(&self) -> Option<&str> {
        self.tags.get(TAG_TARGET).map(|s| s.as_str())
    }

    /// The state an expiring promise settles into: a timer resolves, everything
    /// else times out.
    pub fn timeout_state(&self) -> PromiseState {
        if self.tags.get(TAG_TIMER).map(|v| v.as_str()) == Some("true") {
            PromiseState::Resolved
        } else {
            PromiseState::RejectedTimedout
        }
    }

    /// Whether this promise has a deadline the drain sweep must fire.
    ///
    /// Mirrors the `promise_timeouts` table, which the SQL backends populate
    /// only for promises carrying a `resonate:target` (an undispatched promise
    /// has nothing to notify, so its expiry is applied lazily on read).
    pub fn timeout_armed(&self) -> bool {
        self.state == PromiseState::Pending && self.target().is_some()
    }

    /// The protocol view of this promise.
    pub fn to_record(&self, id: &str) -> PromiseRecord {
        PromiseRecord {
            id: id.to_string(),
            state: self.state,
            param: self.param.clone(),
            value: self.value.clone(),
            tags: self.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            timeout_at: self.timeout_at,
            created_at: self.created_at,
            settled_at: self.settled_at,
        }
    }
}

impl TaskDoc {
    /// The protocol view of this task.
    pub fn to_record(&self, id: &str) -> TaskRecord {
        TaskRecord {
            id: id.to_string(),
            state: self.state,
            version: self.version,
            resumes: self.resumes.len() as i64,
            ttl: self.ttl,
            pid: self.pid.clone(),
        }
    }

    /// Disarm both timers. Used on every transition out of a timed state.
    pub fn disarm(&mut self) {
        self.retry_at = None;
        self.lease_at = None;
    }

    /// Arm the retry timer, replacing whatever was armed.
    pub fn arm_retry(&mut self, at: i64) {
        self.retry_at = Some(at);
        self.lease_at = None;
    }

    /// Arm the lease timer, replacing whatever was armed.
    pub fn arm_lease(&mut self, at: i64) {
        self.lease_at = Some(at);
        self.retry_at = None;
    }
}

/// Tuning the kernel reads but never chooses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelCfg {
    /// How long a pending task waits before its dispatch is re-sent.
    pub retry_timeout: i64,
}

impl Default for KernelCfg {
    fn default() -> Self {
        Self {
            retry_timeout: 30_000,
        }
    }
}

/// One request the kernel can decide, already deserialized and validated.
///
/// Every variant is single-origin, which is what lets one document answer it.
/// `promise.search`, `task.search` and the schedule operations are absent
/// deliberately: they are not origin-scoped and live in the shell.
#[derive(Debug)]
pub enum Req {
    PromiseGet(PromiseGetData),
    PromiseCreate(PromiseCreateData),
    PromiseSettle(crate::core::types::PromiseSettleData),
    PromiseRegisterCallback(PromiseRegisterCallbackData),
    PromiseRegisterListener(PromiseRegisterListenerData),
    TaskGet(TaskGetData),
    TaskCreate(TaskCreateData),
    TaskAcquire(TaskAcquireData),
    TaskRelease(TaskReleaseData),
    TaskFulfill(TaskFulfillData),
    TaskSuspend(TaskSuspendData),
    /// The one variant carrying envelope metadata: a fenced action's reply *is*
    /// a nested response envelope, and an envelope has a `corrId`.
    TaskFence {
        data: TaskFenceData,
        corr_id: String,
    },
    TaskHeartbeat(TaskHeartbeatData),
    TaskHalt(TaskHaltData),
    TaskContinue(TaskContinueData),
    /// The fence check alone, with no action attached.
    ///
    /// The shell needs this when a fenced action targets a promise in another
    /// origin — a different document, so a single decision cannot cover both.
    TaskFencePrepare(TaskFencePrepareData),
    /// A schedule's occurrence coming due.
    ///
    /// Not a protocol operation — no caller can send it — but a transition on
    /// an origin document all the same, and one whose stamps differ from an
    /// ordinary create: the promise is created *as of* the occurrence it
    /// represents, while its first dispatch is timed from the sweep that
    /// noticed. See `process_schedule_timeout`
    /// (`persistence_sqlite.rs:1329-1400`).
    ScheduleFire(ScheduleFireData),
}

/// A task's identity, for a fence check with no action attached.
#[derive(Debug, Clone)]
pub struct TaskFencePrepareData {
    pub id: String,
    pub version: i64,
}

/// The promise a due schedule creates, with its template already rendered and
/// its tags already stamped.
#[derive(Debug, Clone)]
pub struct ScheduleFireData {
    pub id: String,
    pub timeout_at: i64,
    pub param: PromiseValue,
    pub tags: BTreeMap<String, String>,
    /// The occurrence this promise represents. Becomes `created_at`.
    pub fired_at: i64,
}

/// A message the shell must deliver after the document commits.
#[derive(Debug, Clone, PartialEq)]
pub enum OutEntry {
    /// Dispatch a task to its target. Superseded by a later `Execute` for the
    /// same task id, exactly as `outgoing_execute`'s primary key implies.
    Execute { task_id: String, version: i64 },
    /// Notify a listener that a promise settled.
    Unblock {
        promise_id: String,
        promise: PromiseRecord,
    },
}

/// Something the shell must do. The kernel itself does nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// Replace the origin document. At most one per decision; when several are
    /// folded, the last one wins.
    SetDocument(OriginDoc),
    /// Arm the origin's timer object at `at`.
    SetTimeout { at: i64 },
    /// Remove the origin's timer object at `at`.
    DelTimeout { at: i64 },
    /// Deliver `out` to `address`, strictly after the document commits.
    Send { address: String, out: OutEntry },
}

/// The answer to one request. Never an `Err`: a rejection is a status.
#[derive(Debug, Clone, PartialEq)]
pub struct Reply {
    pub status: i32,
    pub data: Value,
}

impl Reply {
    pub fn ok<T: serde::Serialize>(data: &T) -> Self {
        Self {
            status: 200,
            data: serde_json::to_value(data).expect("response data serializes"),
        }
    }

    pub fn status(status: i32, data: Value) -> Self {
        Self { status, data }
    }

    pub fn err(status: i32, message: &str) -> Self {
        Self {
            status,
            data: Value::String(message.to_string()),
        }
    }
}

/// The earliest deadline this document has armed, if any.
///
/// The union of the three sets the SQL backends sweep: armed promise
/// deadlines, task retry deadlines, and task lease deadlines. The shell keeps
/// exactly one timer object per origin, at this instant.
pub fn min_deadline(doc: &OriginDoc) -> Option<i64> {
    let promises = doc
        .promises
        .values()
        .filter(|p| p.timeout_armed())
        .map(|p| p.timeout_at);
    let tasks = doc
        .tasks
        .values()
        .flat_map(|t| [t.retry_at, t.lease_at])
        .flatten();
    promises.chain(tasks).min()
}

/// Fold effects into a document: the last `SetDocument` wins, the rest are the
/// shell's business.
///
/// This is the whole updater. A decider that emits no `SetDocument` has, by
/// definition, not changed the state.
pub fn apply_effects(doc: &mut OriginDoc, fx: &[Effect]) {
    for effect in fx {
        if let Effect::SetDocument(next) = effect {
            *doc = next.clone();
        }
    }
    debug_assert!(
        check_invariants(doc).is_ok(),
        "kernel invariant violated: {}",
        check_invariants(doc).unwrap_err()
    );
}

/// Structural invariants every committed document satisfies.
///
/// Returned rather than asserted so tests can name the violation; callers use
/// it behind a `debug_assert!`.
pub fn check_invariants(doc: &OriginDoc) -> Result<(), String> {
    for (id, p) in &doc.promises {
        match p.state {
            PromiseState::Pending => {
                if p.settled_at.is_some() {
                    return Err(format!("promise {id}: pending but has settled_at"));
                }
            }
            _ => {
                if p.settled_at.is_none() {
                    return Err(format!("promise {id}: settled but has no settled_at"));
                }
            }
        }
        for awaiter in &p.callbacks {
            if !doc.promises.contains_key(awaiter) {
                return Err(format!("promise {id}: callback awaiter {awaiter} missing"));
            }
        }
        let mut seen = BTreeSet::new();
        for addr in &p.listeners {
            if !seen.insert(addr) {
                return Err(format!("promise {id}: duplicate listener {addr}"));
            }
        }
    }

    for (id, t) in &doc.tasks {
        let p = match doc.promises.get(id) {
            Some(p) => p,
            None => return Err(format!("task {id}: no promise")),
        };
        // one_timer: a task's timeout row is one row with one type.
        match t.state {
            TaskState::Pending => {
                if t.retry_at.is_none() || t.lease_at.is_some() {
                    return Err(format!("task {id}: pending without exactly a retry timer"));
                }
            }
            TaskState::Acquired => {
                if t.lease_at.is_none() || t.retry_at.is_some() {
                    return Err(format!("task {id}: acquired without exactly a lease timer"));
                }
            }
            TaskState::Suspended | TaskState::Halted | TaskState::Fulfilled => {
                if t.retry_at.is_some() || t.lease_at.is_some() {
                    return Err(format!("task {id}: {} with an armed timer", t.state));
                }
            }
        }
        // Settlement is terminal for the task that owns the promise.
        if p.state != PromiseState::Pending && t.state != TaskState::Fulfilled {
            return Err(format!("task {id}: promise settled but task is {}", t.state));
        }
        if p.state == PromiseState::Pending && t.state == TaskState::Fulfilled {
            return Err(format!("task {id}: fulfilled but promise is pending"));
        }
        for awaited in &t.resumes {
            if !doc.promises.contains_key(awaited) {
                return Err(format!("task {id}: resume {awaited} missing"));
            }
        }
    }

    if doc.timer_at != min_deadline(doc) {
        return Err(format!(
            "timer_at {:?} != min_deadline {:?}",
            doc.timer_at,
            min_deadline(doc)
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn promise(state: PromiseState, timeout_at: i64, target: bool) -> PromiseDoc {
        let mut tags = BTreeMap::new();
        if target {
            tags.insert(TAG_TARGET.to_string(), "http://w".to_string());
        }
        PromiseDoc {
            state,
            param: PromiseValue::default(),
            value: PromiseValue::default(),
            tags,
            timeout_at,
            created_at: 0,
            settled_at: if state == PromiseState::Pending {
                None
            } else {
                Some(timeout_at)
            },
            callbacks: Vec::new(),
            listeners: Vec::new(),
        }
    }

    fn task(state: TaskState, retry_at: Option<i64>, lease_at: Option<i64>) -> TaskDoc {
        TaskDoc {
            state,
            version: 0,
            pid: None,
            ttl: None,
            resumes: BTreeSet::new(),
            retry_at,
            lease_at,
        }
    }

    #[test]
    fn min_deadline_spans_promise_task_and_lease_timers() {
        let mut doc = OriginDoc::default();
        assert_eq!(min_deadline(&doc), None);

        doc.promises
            .insert("o:a".into(), promise(PromiseState::Pending, 500, true));
        assert_eq!(min_deadline(&doc), Some(500));

        doc.promises
            .insert("o:b".into(), promise(PromiseState::Pending, 400, true));
        doc.tasks
            .insert("o:b".into(), task(TaskState::Pending, Some(300), None));
        assert_eq!(min_deadline(&doc), Some(300));

        doc.tasks
            .insert("o:a".into(), task(TaskState::Acquired, None, Some(100)));
        assert_eq!(min_deadline(&doc), Some(100));
    }

    #[test]
    fn a_promise_without_a_target_arms_no_deadline() {
        // The SQL backends only insert a promise_timeouts row when the promise
        // carries a resonate:target; an undispatched promise expires lazily.
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Pending, 500, false));
        assert_eq!(min_deadline(&doc), None);
    }

    #[test]
    fn a_settled_promise_arms_no_deadline() {
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Resolved, 500, true));
        assert_eq!(min_deadline(&doc), None);
    }

    #[test]
    fn apply_effects_takes_the_last_document() {
        let mut doc = OriginDoc::default();
        let mut first = OriginDoc {
            clock: 1,
            ..Default::default()
        };
        first.timer_at = None;
        let second = OriginDoc {
            clock: 2,
            ..Default::default()
        };
        apply_effects(
            &mut doc,
            &[
                Effect::SetDocument(first),
                Effect::SetTimeout { at: 5 },
                Effect::SetDocument(second.clone()),
            ],
        );
        assert_eq!(doc, second);
    }

    #[test]
    fn apply_effects_without_a_document_is_a_no_op() {
        let mut doc = OriginDoc {
            clock: 7,
            ..Default::default()
        };
        apply_effects(&mut doc, &[Effect::DelTimeout { at: 3 }]);
        assert_eq!(doc.clock, 7);
    }

    #[test]
    fn invariants_reject_a_pending_task_without_a_retry_timer() {
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Pending, 500, true));
        doc.tasks
            .insert("o:a".into(), task(TaskState::Pending, None, None));
        doc.timer_at = min_deadline(&doc);
        assert!(check_invariants(&doc)
            .unwrap_err()
            .contains("pending without exactly a retry timer"));
    }

    #[test]
    fn invariants_reject_a_suspended_task_with_an_armed_timer() {
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Pending, 500, true));
        doc.tasks
            .insert("o:a".into(), task(TaskState::Suspended, Some(9), None));
        doc.timer_at = min_deadline(&doc);
        assert!(check_invariants(&doc)
            .unwrap_err()
            .contains("suspended with an armed timer"));
    }

    #[test]
    fn invariants_reject_a_settled_promise_with_a_live_task() {
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Resolved, 500, true));
        doc.tasks
            .insert("o:a".into(), task(TaskState::Suspended, None, None));
        doc.timer_at = min_deadline(&doc);
        assert!(check_invariants(&doc)
            .unwrap_err()
            .contains("promise settled but task is suspended"));
    }

    #[test]
    fn invariants_reject_a_stale_timer_stamp() {
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Pending, 500, true));
        doc.timer_at = Some(999);
        assert!(check_invariants(&doc).unwrap_err().contains("timer_at"));
    }

    #[test]
    fn invariants_accept_a_consistent_document() {
        let mut doc = OriginDoc::default();
        doc.promises
            .insert("o:a".into(), promise(PromiseState::Pending, 500, true));
        doc.tasks
            .insert("o:a".into(), task(TaskState::Acquired, None, Some(120)));
        doc.timer_at = min_deadline(&doc);
        assert_eq!(check_invariants(&doc), Ok(()));
    }

    #[test]
    fn a_promise_record_carries_the_tags_as_a_map() {
        let p = promise(PromiseState::Pending, 500, true);
        let r = p.to_record("o:a");
        assert_eq!(r.id, "o:a");
        assert_eq!(r.tags.get(TAG_TARGET).map(|s| s.as_str()), Some("http://w"));
        assert_eq!(r.settled_at, None);
    }

    #[test]
    fn a_task_record_reports_resumes_as_a_count() {
        let mut t = task(TaskState::Pending, Some(1), None);
        t.resumes.insert("o:x".into());
        t.resumes.insert("o:y".into());
        assert_eq!(t.to_record("o:a").resumes, 2);
    }

    #[test]
    fn arming_one_task_timer_disarms_the_other() {
        let mut t = task(TaskState::Pending, Some(1), None);
        t.arm_lease(9);
        assert_eq!((t.retry_at, t.lease_at), (None, Some(9)));
        t.arm_retry(3);
        assert_eq!((t.retry_at, t.lease_at), (Some(3), None));
        t.disarm();
        assert_eq!((t.retry_at, t.lease_at), (None, None));
    }
}
