//! Core — the ports and the vocabulary they speak.
//!
//! Everything here is definition only: traits, data types, and pure functions
//! over them. `core` depends on nothing else in this crate; every other module
//! is an adapter that depends on `core`. Keep it that way — no
//! `use crate::{server, transport, persistence, oracle}` may ever appear
//! below this module.

pub mod address;
pub mod router;
pub mod server;
pub mod types;
pub mod worker;

pub use address::{is_valid_address, scheme_of};
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
    /// How far the message got. See [`Cause`].
    pub cause: Cause,
}

/// How far a message got before it failed.
///
/// The distinction is not cosmetic: `Unroutable` is a property of the address
/// and will not change on its own, while `Delivery` may well succeed on a later
/// attempt. Anything deciding whether to retry, or recording why a message did
/// not land, needs to tell them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// Never reached a worker: the address did not parse, or no worker is
    /// registered for its scheme.
    Unroutable,
    /// A worker accepted the address and could not deliver.
    Delivery,
}

impl Unavailable {
    /// A worker took the message and could not deliver it.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            cause: Cause::Delivery,
        }
    }

    /// The message never reached a worker at all.
    pub fn unroutable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            cause: Cause::Unroutable,
        }
    }
}

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unavailable: {}", self.message)
    }
}

impl std::error::Error for Unavailable {}
