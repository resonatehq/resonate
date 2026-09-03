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
    /// Acquire resources and start background work.
    ///
    /// Called once, before anything can reach `process`, and the counterpart of
    /// [`ResonateWorker::init`](super::ResonateWorker::init) and
    /// [`ResonateGateway::init`](super::ResonateGateway::init) — every port has
    /// the same shape, because every one of them may have to open a connection
    /// to come into service and every one of them can fail doing it. A pool, a
    /// schema, a session, a seeded timer: all of it belongs here rather than in
    /// construction, so that failing to start is a startup error and not a
    /// request answered wrongly later.
    ///
    /// `debug` is the process-wide debug flag. Under it the clock belongs to the
    /// caller, so an implementation must not start work that runs on wall time.
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let _ = debug;
        Ok(())
    }

    /// Stop background work and release resources.
    ///
    /// Called once, and **first** — before the workers and before the gateways.
    /// A server's own timer is the only thing that can still hand it work of
    /// its own, so stopping it is what makes the rest of the drain finite.
    ///
    /// The consequence for an implementation: a gateway is still accepting
    /// requests and still calling [`process`](Self::process) after this
    /// returns. Halt what you spawned; do not close what answers. A pool torn
    /// down here becomes 500s for the length of the drain.
    ///
    /// Implementations must be safe to call when `init` was never called.
    async fn stop(&self) -> Result<(), Unavailable> {
        Ok(())
    }

    /// Whether this server can serve right now.
    ///
    /// What a readiness probe asks, and it is here rather than on whatever sits
    /// behind the server because a gateway has to answer `/ready` without
    /// knowing whether there is a database back there, an object store, or a
    /// socket to somewhere else. Each implementation knows what "ready" means
    /// for itself — `resonate-server-blob` asks whether its bucket answers.
    ///
    /// Defaulted to `true`, which is the honest answer for an implementation
    /// with nothing to check: a process that is up is a process that can serve.
    async fn ready(&self) -> bool {
        true
    }

    /// Apply one request and return its response.
    ///
    /// A non-2xx outcome is still a completed exchange: it comes back as `Ok`
    /// with the status in `ResponseHead::status`. `Err` is reserved for "there
    /// is no answer" — see [`Unavailable`] for the retry contract.
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable>;
}
