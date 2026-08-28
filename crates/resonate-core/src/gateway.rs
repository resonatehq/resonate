//! The hosting port: something that accepts Resonate protocol requests from
//! the outside and puts them to a server.

use async_trait::async_trait;

use super::Unavailable;

/// A Resonate gateway: an edge that turns some transport into
/// [`ResonateServer::process`](super::ResonateServer::process) calls.
///
/// The counterpart to [`ResonateWorker`](super::ResonateWorker), and thinner
/// than it: a worker is *called* with each message, so it has `send`, while a
/// gateway is driven by its own transport and has nothing we invoke per
/// request. What is left is lifecycle — which is the whole point, because
/// lifecycle is the part that has to be coordinated with everything else.
///
/// # Constructing one
///
/// Not a trait method, for the same reason as a worker: each gateway has its
/// own `Config`, and an associated type would have to be named in
/// `dyn ResonateGateway`. It is a convention:
///
/// ```ignore
/// fn new(server: Arc<dyn ResonateServer>, config: Config, ..) -> Self;
/// ```
///
/// Strong, not weak — the one place a gateway differs from a worker. A worker
/// is inside a reference cycle (a server holds its router, the router holds its
/// workers) and must hold the server weakly or nothing in the ring is ever
/// dropped. A gateway is not in that ring: it holds the server and nothing
/// holds it but the composition root. So a strong handle is both simpler and
/// the truthful statement — a gateway keeps its server alive for exactly as
/// long as it can still accept a request.
///
/// The type is `dyn ResonateServer`, not a concrete server, so the same gateway
/// can front the in-process engine, the reference model, or a client for a
/// remote server.
///
/// # Ordering
///
/// A gateway is the **last thing to start and the last thing to stop**, and the
/// asymmetry is deliberate.
///
/// Last to start, because accepting a request the rest of the process cannot
/// yet serve is worse than not accepting it. `new` is cheap and infallible and
/// may be called whenever; nothing is bound and nothing is served until
/// [`init`](Self::init), and that is what belongs after the workers.
///
/// Last to stop, because the alternative is refusing connections while
/// in-flight work is still draining, and a client would rather have a 503 than
/// a closed socket. It also removes a deadlock that the mirror ordering would
/// create: a long-lived response — an SSE stream a poll transport is writing
/// into — ends when the *transport* stops and drops its sender. Stop the
/// gateway first and its graceful drain waits on a stream only a worker that
/// has not stopped yet can release.
///
/// The cost is a window, between the workers stopping and the gateway
/// stopping, in which a request is still accepted but a message it emits has
/// nowhere to go. That is the ordinary best-effort delivery contract — an
/// execute message is re-emitted by its retry deadline — and it is a better
/// failure than a refused connection.
#[async_trait]
pub trait ResonateGateway: Send + Sync {
    /// Acquire resources and begin accepting requests.
    ///
    /// Called once, after every worker's `init`. Binding a port can fail and
    /// serving is a background task, so both belong here rather than in `new`:
    /// a failure to listen is a startup failure, not a request that quietly
    /// goes unanswered later.
    async fn init(&self) -> Result<(), Unavailable> {
        Ok(())
    }

    /// Stop accepting, drain what is in flight, and release the transport.
    ///
    /// Called once, last. Implementations should be safe to call when `init`
    /// was never called.
    async fn stop(&self) -> Result<(), Unavailable> {
        Ok(())
    }
}
