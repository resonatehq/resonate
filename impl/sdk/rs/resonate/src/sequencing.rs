//! Creation sequencing: forcing durable promise creation onto the server in
//! terminal-op call order, even though creations run on concurrent background
//! tasks.
//!
//! Each `Context` owns a [`CreationSequencer`]. Every terminal op (`spawn()`,
//! `.await`, `create()`) synchronously claims a [`CreationSlot`] from it. A
//! slot waits for its predecessor to finish, performs its own `create_promise`,
//! then broadcasts the outcome to release the next slot. Because slots are
//! claimed in source-code order (not task-scheduling order), the server
//! observes `promise.create` calls in the order the workflow issued them — the
//! prerequisite for deterministic replay.
//!
//! The sequence is **success-gated**: if creation k fails (or its task is
//! dropped mid-flight), creation k+1 aborts without touching the server, so the
//! server's created promises are always an exact call-order prefix.

use parking_lot::Mutex;

use crate::effects::Effects;
use crate::error::{Error, Result};
use crate::types::{PromiseCreateReq, PromiseRecord};

/// Per-`Context` coordinator that hands out [`CreationSlot`]s in call order.
///
/// Holds the tail of the sequence: the creation-state receiver of the most
/// recently claimed slot. `claim_slot` captures the current tail as the new
/// slot's predecessor and installs the new slot's channel as the tail.
pub(crate) struct CreationSequencer {
    tail: Mutex<Option<tokio::sync::watch::Receiver<CreationState>>>,
}

impl CreationSequencer {
    pub(crate) fn new() -> Self {
        Self {
            tail: Mutex::new(None),
        }
    }

    /// Claim the next slot in the sequence: capture the current tail as the
    /// predecessor and install this slot's state channel as the new tail.
    ///
    /// Must be called synchronously by the terminal ops (`spawn()`, `.await`,
    /// `create()`) — never at builder construction, so a builder that is never
    /// spawned/awaited never touches the sequence (lazy-future semantics), and
    /// never inside the spawned/async body, or creation order would no longer
    /// match terminal-op call order.
    pub(crate) fn claim_slot(&self) -> CreationSlot {
        let (tx, rx) = creation_channel();
        let prev = self.tail.lock().replace(rx);
        CreationSlot { prev, tx }
    }
}

/// State of a single creation within the sequence.
///
/// Broadcast through a `tokio::sync::watch` channel shared by two observers:
/// the *next* slot (which gates on this one reaching `Created`) and the spawned
/// handle's `id()` (which only hands out the promise ID once the durable
/// promise is known to exist on the server). The owning task moves the state
/// out of `InFlight` exactly once, right after `create_promise` resolves.
#[derive(Clone, Debug)]
pub(crate) enum CreationState {
    InFlight,
    Created,
    Failed(String),
}

/// Create a `watch` channel for a creation's state, starting `InFlight`.
pub(crate) fn creation_channel() -> (
    tokio::sync::watch::Sender<CreationState>,
    tokio::sync::watch::Receiver<CreationState>,
) {
    tokio::sync::watch::channel(CreationState::InFlight)
}

/// Wait until the creation state leaves `InFlight`, then map it to the ID.
/// The ID is only returned on confirmed server-side creation. Shared by every
/// handle type's `id()` so the gating logic lives in one place alongside the
/// [`CreationState`] it interprets.
pub(crate) async fn await_created_id(
    id: &str,
    created: &tokio::sync::watch::Receiver<CreationState>,
) -> Result<String> {
    let mut rx = created.clone();
    let state = rx
        .wait_for(|s| !matches!(s, CreationState::InFlight))
        .await
        .map_err(|_| Error::JoinError(format!("task {} was dropped", id)))?;
    match &*state {
        CreationState::Created => Ok(id.to_string()),
        CreationState::Failed(msg) => Err(Error::PromiseCreation(msg.clone())),
        CreationState::InFlight => unreachable!("wait_for excludes InFlight"),
    }
}

/// One claimed slot in the creation sequence. The slot's `watch` channel
/// carries its creation state to both the successor slot and the handle's
/// `id()` gate.
pub(crate) struct CreationSlot {
    prev: Option<tokio::sync::watch::Receiver<CreationState>>,
    tx: tokio::sync::watch::Sender<CreationState>,
}

impl CreationSlot {
    /// Receiver for this slot's creation state — the handle's `id()` gate.
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<CreationState> {
        self.tx.subscribe()
    }

    /// Create a promise in sequence order.
    ///
    /// Waits for the predecessor slot, creates the promise, and broadcasts the
    /// outcome. The sequence is success-gated: if the predecessor failed (or
    /// its task was dropped, closing the channel while still `InFlight`), this
    /// slot aborts without touching the server and broadcasts `Failed`, so the
    /// server's created promises are always an exact call-order prefix — and no
    /// successor can ever deadlock waiting on a slot that never settles.
    pub(crate) async fn create(
        self,
        effects: &Effects,
        req: PromiseCreateReq,
    ) -> Result<PromiseRecord> {
        if let Some(mut prev) = self.prev {
            let predecessor_ok = match prev
                .wait_for(|s| !matches!(s, CreationState::InFlight))
                .await
            {
                Ok(state) => matches!(&*state, CreationState::Created),
                Err(_) => false, // predecessor task dropped while in flight
            };
            if !predecessor_ok {
                let e = Error::PromiseCreation(
                    "aborted: a previous promise creation in this workflow failed".to_string(),
                );
                let _ = self.tx.send(CreationState::Failed(e.to_string()));
                return Err(e);
            }
        }
        match effects.create_promise(req).await {
            Ok(record) => {
                let _ = self.tx.send(CreationState::Created); // release the next slot
                Ok(record)
            }
            Err(e) => {
                let _ = self.tx.send(CreationState::Failed(e.to_string()));
                Err(e)
            }
        }
    }
}
