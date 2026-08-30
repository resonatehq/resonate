//! The kernel — the protocol's state machine, as a pure function.
//!
//! # Contract
//!
//! `handle(&doc, req, now) -> (Vec<Effect>, Reply)` and
//! `drain(&doc, now) -> Vec<Effect>` decide; they read no clock, generate no
//! ids, and touch no I/O. Everything a decision implies — persist the
//! document, arm a timer, clear a timer, send a message — comes back as an
//! [`Effect`] for the shell in the shell modules at the crate root to perform. Ids are strings,
//! clocks are `i64` milliseconds, invariants are `debug_assert!`s, sends live
//! outside the document, and timers aggregate to one deadline per origin
//! rather than one per entity.
//!
//! Where the semantics are ambiguous, main's engines are the source of truth,
//! and the copy of their reference model in this crate's [`crate::oracle`] is
//! how disagreements are found.
//!
//! Two pure models live in this crate, and they must not be merged: this
//! kernel is written for the document shape and runs in production; the
//! oracle carries main's semantics and runs only in tests. The value of the
//! pair is that neither was derived from the other.
//!
//! # Dependencies
//!
//! `core::types` for the protocol's request and response shapes. Nothing else
//! — no store, no clock, no async.
//!
//! # Dependants
//!
//! The S3 shell: the applier runs `handle` and `drain` for every decision and
//! `apply_effects` to update its documents, and the codec encodes the state
//! declared here.

pub mod drain;
pub mod handle;
pub mod state;

pub use drain::drain;
pub use handle::handle;
pub use state::{
    apply_effects, check_invariants, min_deadline, Effect, KernelCfg, OriginDoc, OutEntry,
    PromiseDoc, Reply, Req, TaskDoc,
};
