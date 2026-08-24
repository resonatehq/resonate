//! The inbound port: something that answers Resonate protocol requests.

use async_trait::async_trait;

use super::types::{Request, RequestEnvelope, Response, ResponseEnvelope};
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
/// **Not yet the full protocol boundary.** Envelope validation — empty `kind`,
/// non-object `data`, unsupported `head.version` — still lives in the HTTP
/// adapter, not here. So an in-process caller reaches the operations without
/// those checks while an HTTP caller gets a 400, and the two are not yet
/// interchangeable for malformed input. Moving those checks in (and teaching
/// the reference model to apply them identically, so the differential covers
/// them) is the remaining work to make this claim true.
#[async_trait]
pub trait ResonateServer: Send + Sync {
    /// Apply one request and return its response.
    ///
    /// A non-2xx outcome is still a completed exchange: it comes back as `Ok`
    /// with the status in `ResponseHead::status`. `Err` is reserved for "there
    /// is no answer" — see [`Unavailable`] for the retry contract.
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>;

    /// The typed form of [`process`](Self::process): a [`Request`] in, a
    /// [`Response`] out.
    ///
    /// This is what an in-process caller uses. It has no socket, so it has no
    /// reason to hand-build an envelope, spell a `kind` as a string, or pick a
    /// response apart with `serde_json::from_value` — and every one of those is
    /// a way to get the protocol subtly wrong at runtime instead of at compile
    /// time.
    ///
    /// Defaulted, so every implementation gets it by converting and delegating.
    /// `process` stays the port because that is what the HTTP adapter and the
    /// reference model implement.
    async fn call(&self, request: Request) -> Result<Response, Unavailable> {
        let envelope = request.into_envelope();
        let kind = envelope.kind.clone();
        Response::from_envelope(&kind, self.process(&envelope).await?)
    }
}
