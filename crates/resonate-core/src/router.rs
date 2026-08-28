//! The routing port: an address string in, delivery to the right worker out.

use async_trait::async_trait;

use super::types::Message;
use super::Unavailable;

/// A Resonate router: resolves an address to a worker and delivers to it.
///
/// The router's knowledge of an address stops at the scheme. It parses the
/// string as a URL, reads the scheme, and hands the whole untouched address to
/// the [`ResonateWorker`](super::ResonateWorker) registered for it — so a new
/// scheme is a registration, never a change to `core`.
#[async_trait]
pub trait ResonateRouter: Send + Sync {
    /// Start whatever the router owns, and hand each worker the debug flag.
    ///
    /// The router holds the workers, so it is the natural place for their
    /// `init` to be driven from — and the one place that knows every scheme,
    /// which is what a startup failure needs to name.
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let _ = debug;
        Ok(())
    }

    /// Stop whatever the router owns, and stop each worker.
    async fn stop(&self) -> Result<(), Unavailable> {
        Ok(())
    }

    /// Route and deliver one message.
    ///
    /// Returns `Err(Unavailable)` when the message could not be handed off:
    /// the address does not parse as a URL, no worker is registered for its
    /// scheme, or the worker itself was unreachable. Today's dispatcher logs
    /// and drops in all three cases; returning them lets the caller decide.
    async fn route(&self, address: &str, msg: &Message) -> Result<(), Unavailable>;
}
