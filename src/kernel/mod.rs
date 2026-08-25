//! The kernel — the protocol's state machine, as a pure function.
//!
//! `handle(&doc, req, now) -> (Vec<Effect>, Reply)` and
//! `drain(&doc, now) -> Vec<Effect>` decide; they read no clock, generate no
//! ids, and touch no I/O. Everything a decision implies — persist the
//! document, arm a timer, clear a timer, send a message — comes back as an
//! [`Effect`] for the shell in [`crate::s3`] to perform.
//!
//! Modelled on `../resonate-verus`, with the divergences that repo's
//! `format.md` and this port's plan record: string ids and `i64` milliseconds
//! instead of `nat`, invariants as `debug_assert!`s instead of proofs, no
//! durable outbox in the document, and timers aggregated to one deadline per
//! origin rather than one per entity.
//!
//! Where the semantics are ambiguous, `src/server.rs` and
//! `src/persistence/persistence_sqlite.rs` are the source of truth — except
//! `resonate:delay`, which follows `src/oracle.rs`.

pub mod handle;
pub mod state;

pub use handle::handle;
pub use state::{
    apply_effects, check_invariants, min_deadline, Effect, KernelCfg, OriginDoc, OutEntry,
    PromiseDoc, Reply, Req, TaskDoc,
};
