//! The inbound port: something that answers Resonate protocol requests.

use async_trait::async_trait;

use super::types::{RequestEnvelope, ResponseEnvelope};
use super::Unavailable;

/// A Resonate server: one request in, one response out.
///
/// Implementations are interchangeable by construction — the in-process
/// server, the in-memory reference model, and a client for a remote server all
/// satisfy this trait — so callers (CLI, MCP, workers, differential tests) can
/// be written once and pointed at any of them.
///
/// `process` resolves the effective `now` from `head.debug_time` (gated by the
/// implementation's own debug setting). There is deliberately no `now`
/// parameter — time is part of the request, which is what keeps the trait a
/// pure function of its input.
///
/// Everything about the *caller* rather than the request — authentication,
/// metrics, tracing — belongs outside, in the adapter hosting the
/// implementation.
///
/// **This is the protocol boundary.** Envelope validation — empty `kind`,
/// non-object `data`, unsupported `head.version` — is
/// [`RequestEnvelope::validate_envelope`](super::types::RequestEnvelope::validate_envelope),
/// which every implementation runs before dispatching. An in-process caller
/// and an HTTP caller therefore get identical rejections for malformed input,
/// and because the reference model applies the same function, the differential
/// harness covers them.
///
/// What remains outside is genuinely transport-level: a body that is not JSON
/// at all never becomes a `RequestEnvelope`, so the HTTP adapter still owns
/// that one.
#[async_trait]
pub trait ResonateServer: Send + Sync {
    /// Apply one request and return its response.
    ///
    /// A non-2xx outcome is still a completed exchange: it comes back as `Ok`
    /// with the status in `ResponseHead::status`. `Err` is reserved for "there
    /// is no answer" — see [`Unavailable`] for the retry contract.
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>;
}
