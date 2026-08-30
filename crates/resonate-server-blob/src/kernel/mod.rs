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
//! Where the semantics are ambiguous, `src/server.rs` and
//! `src/persistence/persistence_sqlite.rs` are the source of truth — except
//! `resonate:delay`, which follows `src/oracle.rs`.
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
