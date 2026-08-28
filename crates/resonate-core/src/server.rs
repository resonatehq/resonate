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
/// **Envelope validation happens before this, not in it.** Whether a message
/// is a request at all — parses, has a non-empty `kind` and an object `data`,
/// names a version this build speaks — is
/// [`parse_and_validate`](super::types::parse_and_validate), and it is the
/// gateway's to apply. A malformed message is answered at the edge and never
/// arrives here, which is why `process` has no way to report one.
///
/// The trust boundary is therefore the gateway, and that is a claim about what
/// reaches this trait rather than a gap in it: inside the boundary envelopes
/// are constructed in code, not parsed, so a worker calling back with an empty
/// `kind` is a bug in that worker rather than untrusted input.
///
/// What an implementation still answers for itself is whether `kind` names an
/// operation it has, and whether `data` carries what that operation needs. Only
/// it knows.
#[async_trait]
pub trait ResonateServer: Send + Sync {
    /// Apply one request and return its response.
    ///
    /// A non-2xx outcome is still a completed exchange: it comes back as `Ok`
    /// with the status in `ResponseHead::status`. `Err` is reserved for "there
    /// is no answer" — see [`Unavailable`] for the retry contract.
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>;
}
