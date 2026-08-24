//! Core — the ports and the vocabulary they speak.
//!
//! Everything here is definition only: traits, data types, and pure functions
//! over them. `core` depends on nothing else in this crate; every other module
//! is an adapter that depends on `core`. Keep it that way — no
//! `use crate::{server, transport, persistence, oracle}` may ever appear
//! below this module.

// The ports are defined here before their adapters exist. Remove this once
// `ResonateServer`, `ResonateWorker`, and `ResonateRouter` have implementations
// — until then the binary crate root (which declares its own module tree and
// so cannot see these as `pub`) lints every one of them as dead.
#![allow(dead_code, unused_imports)]

pub mod address;
pub mod router;
pub mod server;
pub mod types;
pub mod worker;

pub use address::{is_valid_address, parse_address, Address};
pub use router::ResonateRouter;
pub use server::ResonateServer;
pub use worker::ResonateWorker;

use std::fmt;

/// The one out-of-band failure: the peer could not be reached, so there is no
/// answer at all.
///
/// This is deliberately the *only* error a port returns. Everything the peer
/// can say about a request — not found, conflict, forbidden, internal error —
/// is an in-band outcome carried in `ResponseHead::status`. `Unavailable`
/// means the exchange did not complete.
///
/// **Retry contract:** the caller must assume the request *may already have
/// been applied*. A connection refused before the first byte and a timeout
/// after the last are both `Unavailable` and the caller cannot tell them
/// apart, so retries must be idempotent.
///
/// `Unavailable` never crosses the wire — at the HTTP edge it renders as a
/// 503. That is why it lives here and not in [`types`], which mirrors the
/// canonical protocol types.
#[derive(Debug, Clone)]
pub struct Unavailable {
    /// Human-readable cause, for logs and diagnostics.
    pub message: String,
}

impl Unavailable {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unavailable: {}", self.message)
    }
}

impl std::error::Error for Unavailable {}
