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
/// `process` is the protocol boundary: everything a remote caller would also
/// see belongs inside it, including envelope validation, protocol version
/// checks, and resolving the effective `now` from `head.debug_time`. There is
/// deliberately no `now` parameter — time is part of the request, which is
/// what keeps the trait a pure function of its input.
///
/// Everything about the *caller* rather than the request — authentication,
/// metrics, tracing — belongs outside, in the adapter hosting the
/// implementation.
#[async_trait]
pub trait ResonateServer: Send + Sync {
    /// Apply one request and return its response.
    ///
    /// A non-2xx outcome is still a completed exchange: it comes back as `Ok`
    /// with the status in `ResponseHead::status`. `Err` is reserved for "there
    /// is no answer" — see [`Unavailable`] for the retry contract.
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>;
}
