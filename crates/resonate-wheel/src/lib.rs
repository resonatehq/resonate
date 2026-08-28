//! A bounded, deduplicating timer wheel — specified, implemented, and proved to
//! agree.
//!
//! # What it is
//!
//! A [`TimerWheel`] holds [`Timeout`]s — a `u64` deadline plus a payload —
//! under three rules:
//!
//! - **Ordered.** Deadlines are non-decreasing, so the next timeout to fire is
//!   at index 0.
//! - **Deduplicated.** No two entries are the same logical timeout. What
//!   "same" means is the caller's to decide: a wheel is built with a
//!   [`Comparator`], and merging a timeout whose identity is already present
//!   *replaces* it. That is the operation a scheduler actually needs — a
//!   deadline moved, not a second entry for the same thing.
//! - **Bounded.** The wheel holds at most `capacity` entries. When a merge
//!   overflows, what falls off is the farthest future: the union is sorted
//!   before it is cut, so a dropped timeout is never nearer than a kept one.
//!
//! # Layout
//!
//! ```text
//! timeout.rs      Timeout<T>: a deadline and a payload.
//! comparator.rs   Comparator<T>: which payloads are the same logical timeout.
//!                 The equivalence laws are proof obligations on implementors,
//!                 not documentation. IdComparator covers the u64-keyed case.
//! spec.rs         THE SPECIFICATION, ghost throughout. `spec_merge` defines
//!                 what merging means, in terms a reader can check by eye;
//!                 `sorted` and `distinct` are the wheel invariant.
//! proof.rs        THE PROOFS. Three layers: sequence plumbing, then the spec
//!                 preserves the invariants, then the bridge -- the exec code's
//!                 single indexed Vec::remove/insert coincides with the spec's
//!                 filter and fold. Ends with the user-visible corollaries.
//! wheel.rs        THE IMPLEMENTATION, over a flat Vec. Every method's
//!                 `ensures` names `spec_merge` and friends directly, so the
//!                 spec -- not the loops -- is the definition of the behaviour.
//! ```
//!
//! # One tree, two compilers
//!
//! ```text
//! ./verify.sh     Verus: the spec, the proofs, and the tie to the code
//! cargo test      the same sources under ghost erasure -- what ships
//! ```
//!
//! Both must pass. Under plain `cargo`, `verus_builtin_macros` erases every
//! `spec fn`, `proof fn`, `requires` and `ensures` syntactically, so the
//! specification costs nothing at runtime and an exec fn can `ensure` against
//! `spec_merge` while still compiling standing alone.
//!
//! # What is proved
//!
//! [`TimerWheel::merge`] is proved equal to [`spec_merge`], and re-establishes
//! [`TimerWheel::wf`]. On top of that sit three named theorems in [`proof`]:
//!
//! - [`proof::lemma_merge_wf`] — a merge always lands sorted, deduplicated and
//!   within capacity.
//! - [`proof::lemma_merge_horizon`] — nothing kept is due later than anything
//!   dropped.
//! - [`proof::lemma_merge_ignores_far_future_newcomers`] — merging a batch of
//!   *new* timeouts whose deadlines all sit beyond the last surviving entry of
//!   a full wheel changes nothing.
//!
//! [`TimerWheel::pop_expired`] is proved to split the wheel exactly: what comes
//! back plus what stays reconstructs what was there, everything returned is
//! due, and nothing retained is.

// `verus_keep_ghost` is set by the Verus driver; plain cargo never sees it.
#![allow(unexpected_cfgs)]
// Under ghost erasure the spec and proof modules compile to nothing, so the
// imports that name them go unused. Both compilers see the same source.
#![allow(unused_imports)]

pub mod comparator;
pub mod proof;
pub mod spec;
pub mod timeout;
pub mod wheel;

// Only items that survive ghost erasure can be re-exported by name: the
// contents of `spec` and `proof` are erased under plain cargo, so they are
// reached through their modules (`resonate_wheel::spec::spec_merge`) inside
// `verus!` blocks rather than lifted to the crate root.
pub use comparator::{Comparator, HasId, IdComparator};
pub use timeout::Timeout;
pub use wheel::TimerWheel;
