//! The engine port.
//!
//! What every implementation of Resonate's durable state must do, named so the
//! differential can hold several and compare them. Today that is the three SQL
//! backends and the oracle; the shape is deliberately the smallest thing the
//! comparison needs, and grows as transitions start returning what they emit.

use async_trait::async_trait;

use resonate_core::types::{RequestEnvelope, ResponseEnvelope};

/// Durable state, and every transition over it.
///
/// The lock-step contract: given the same request at the same time, every
/// implementation must produce the same response. Nothing about *how* state is
/// stored appears here — the single-table and multi-table Postgres designs
/// differ in every column and satisfy this identically.
#[async_trait]
pub trait ResonateEngine: Send + Sync {
    /// Apply one request at `now` and return its response.
    ///
    /// `now` is passed rather than read from a clock so a test can drive
    /// several implementations through the same sequence at the same instants.
    async fn process(&self, req: &RequestEnvelope, now: i64) -> ResponseEnvelope;
}
