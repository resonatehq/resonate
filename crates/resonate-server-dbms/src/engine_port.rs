//! The engine port.
//!
//! What every implementation of Resonate's durable state must do, named so the
//! differential can hold several and compare them.
//!
//! One method, because a timeout is just a request the system makes of itself:
//! an engine takes an [`Input`] and returns an [`Output`]. What a transition
//! emits is part of that output rather than a row it leaves behind for someone
//! else to find — which is what lets the outbox and the pump that drains it go
//! away, one backend at a time.

use async_trait::async_trait;

use resonate_core::types::{PromiseRecord, RequestEnvelope, ResponseEnvelope};

use crate::StorageResult;

/// One message a transition emitted, and where it goes.
///
/// The engine does not build the wire form: an execute message carries a
/// `server_url` the engine has no business knowing, and the caller that owns
/// the router owns that. This is the payload, not the envelope.
///
/// No `PartialEq`: `PromiseRecord` has none, and comparison goes through
/// [`Outgoing::to_json`] anyway, which is the form the differential already
/// knows how to diff.
#[derive(Debug, Clone)]
pub enum Outgoing {
    Execute {
        address: String,
        task_id: String,
        version: i64,
    },
    Unblock {
        address: String,
        promise: PromiseRecord,
    },
}

impl Outgoing {
    pub fn address(&self) -> &str {
        match self {
            Outgoing::Execute { address, .. } | Outgoing::Unblock { address, .. } => address,
        }
    }

    /// The shape `Snapshot::messages` uses, so a returned message and a queued
    /// one compare as the same thing while both mechanisms coexist.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Outgoing::Execute {
                task_id, version, ..
            } => serde_json::json!({
                "kind": "execute", "head": {},
                "data": { "task": { "id": task_id, "version": version } }
            }),
            Outgoing::Unblock { promise, .. } => serde_json::json!({
                "kind": "unblock", "head": {},
                "data": { "promise": promise }
            }),
        }
    }
}

/// Something the system asks of itself when a deadline passes.
///
/// The variant names track the schema: a promise timing out, a pending task
/// wanting redispatch, an acquired task's lease expiring. `ScheduleDue` stays
/// different because a schedule firing is a recurrence reaching its next run,
/// not something expiring.
#[derive(Debug, Clone, PartialEq)]
pub enum Timeout {
    PromiseTimeout { promise_id: String },
    TaskRetryTimeout { task_id: String },
    TaskLeaseTimeout { task_id: String, pid: String },
    ScheduleDue { schedule_id: String },
}

/// A timeout and when it comes due. A hint for an in-memory wheel — the
/// durable copy committed with the state change that armed it.
#[derive(Debug, Clone, PartialEq)]
pub struct Scheduled {
    pub at: i64,
    pub timeout: Timeout,
}

/// What an engine is asked to do.
pub enum Input<'a> {
    /// A request from outside, already validated — engines never see malformed
    /// input.
    External(&'a RequestEnvelope),
    /// The system asking something of itself.
    Internal(Timeout),
}

/// What a transition did.
///
/// Three properties belong to this contract:
///
/// - **`process` is atomic** across state, messages and timeouts. Either all of
///   it happened or none of it did.
/// - **`Internal` is idempotent.** An in-memory wheel and a database sweep will
///   both fire the same timeout, and neither knows about the other.
/// - **`timeouts` reports arming, not disarming.** A promise settled early
///   leaves a stale entry that fires into a no-op, which idempotency covers.
#[derive(Debug, Default)]
pub struct Output {
    /// `None` for `Internal` — nobody is waiting on it.
    pub response: Option<ResponseEnvelope>,
    /// Was an outbox row; now the result.
    pub messages: Vec<Outgoing>,
    /// A hint — the durable copy is already committed.
    pub timeouts: Vec<Scheduled>,
}

impl Output {
    /// A response and nothing else — the shape of an engine that still queues
    /// its messages rather than returning them.
    pub fn response(resp: ResponseEnvelope) -> Self {
        Self {
            response: Some(resp),
            messages: Vec::new(),
            timeouts: Vec::new(),
        }
    }
}

/// Durable state, and every transition over it.
///
/// The lock-step contract: given the same input at the same time, every
/// implementation must produce the same response and emit the same messages.
/// Nothing about *how* state is stored appears here.
#[async_trait]
pub trait ResonateEngine: Send + Sync {
    /// Apply one input at `now`.
    ///
    /// `now` is passed rather than read from a clock so a test can drive
    /// several implementations through the same sequence at the same instants.
    async fn process(&self, input: Input<'_>, now: i64) -> Output;

    /// Fire every timeout now due, and return what they emitted.
    ///
    /// The bulk form. The design replaces it with [`ResonateEngine::due`]
    /// listing what is due and `process(Internal(..))` firing them one at a
    /// time, so a wheel that knows exactly what is due does exactly that much
    /// work; until that exists this is what the timer loop calls. The count is
    /// schedules fired, which the caller records.
    async fn tick(&self, now: i64) -> StorageResult<(usize, Vec<Outgoing>)>;

    /// Is the engine paused? `debug.start` sets this, and the background loops
    /// honour it so a test can drive the clock with `debug.tick` instead of
    /// racing wall time.
    fn is_paused(&self) -> bool {
        false
    }

    /// Lightweight liveness probe.
    async fn ping(&self) -> StorageResult<()>;

    /// Whether this engine returns what it emitted.
    ///
    /// Every engine does now. Kept while the differential still holds the
    /// oracle, which answers both ways.
    fn returns_messages(&self) -> bool {
        false
    }
}
