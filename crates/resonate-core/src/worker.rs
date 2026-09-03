//! The outbound port: something that consumes the messages a server emits.

use async_trait::async_trait;

use super::types::Message;
use super::Unavailable;

/// A Resonate worker: the thing at the far end of an address.
///
/// The dual of [`ResonateServer`](super::ResonateServer). A server receives
/// requests and returns responses; a worker receives messages and — for the
/// ones that do real work — issues requests back at a server.
///
/// Most implementations are *proxies* for a worker running elsewhere: HTTP
/// push, poll/SSE, and Pub/Sub each hand the message off and return. One,
/// bash exec, is a real worker that runs in process. `process` therefore means
/// **accepted for delivery**, not **executed**; a worker that happens to run
/// to completion synchronously is a special case, not the contract.
///
/// # Constructing one
///
/// Not a trait method — each worker has its own `Config` type, and an
/// associated type would have to be named in `dyn ResonateWorker`, which would
/// make `Arc<dyn ResonateWorker>` unusable. It is a convention instead, and
/// every implementation follows it:
///
/// ```ignore
/// fn new(server: Weak<dyn ResonateServer>, config: Config) -> Self;
/// ```
///
/// The server handle is [`Weak`](std::sync::Weak) deliberately. A router holds
/// its workers and a server holds its router, so a strong handle back would
/// close a reference cycle and nothing in it would ever be dropped — not the
/// server, not its storage, not the background tasks. Upgrade per message; if
/// the upgrade fails the server is gone and there is no work worth doing.
///
/// `new` builds a value and cannot fail. Everything that can fail, and
/// everything that starts a background task, belongs in [`init`](Self::init).
#[async_trait]
pub trait ResonateWorker: Send + Sync {
    /// Acquire resources and start background work.
    ///
    /// Called once before any `process`. Connection pools, delivery queues and
    /// their dispatcher tasks are set up here rather than in `new`, so that
    /// failure is reported at startup instead of surfacing later as a message
    /// that quietly went nowhere.
    ///
    /// `debug` is the process-wide debug flag. Under it the clock belongs to
    /// the caller — a test drives time with `debug.tick` — so an implementation
    /// must not start work that runs on wall time. Anything driven by a queue
    /// or by a request still runs; it is only the timer-shaped work that has to
    /// stay out of the way of a clock it does not control.
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let _ = debug;
        Ok(())
    }

    /// Stop background work and release resources.
    ///
    /// Called once, after which `process` may fail. Implementations should drain
    /// what they can and be safe to call when `init` was never called.
    async fn stop(&self) -> Result<(), Unavailable> {
        Ok(())
    }

    /// Process one message: deliver it to the worker at `address`.
    ///
    /// The dual of [`ResonateServer::process`](super::ResonateServer::process),
    /// and named for it. A server processes a request and returns a response; a
    /// worker processes a message and returns nothing, because there is nothing
    /// to return — see the note above on what acceptance means here.
    ///
    /// The router guarantees only that `address` carries this worker's
    /// registered scheme; everything past the scheme is this worker's to
    /// parse and to reject.
    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable>;
}
