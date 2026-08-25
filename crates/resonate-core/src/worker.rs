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
/// bash exec, is a real worker that runs in process. `send` therefore means
/// **accepted for delivery**, not **executed**; a worker that happens to run
/// to completion synchronously is a special case, not the contract.
///
/// The address arrives as an unparsed string on purpose. A worker owns the
/// syntax of its own scheme — what a `poll://` or `gcps://` address means is
/// the poll or Pub/Sub worker's business, not `core`'s. The
/// [`ResonateRouter`](super::ResonateRouter) reads only the scheme, so adding
/// a worker never requires editing `core`.
#[async_trait]
pub trait ResonateWorker: Send + Sync {
    /// Deliver one message to the worker at `address`.
    ///
    /// The router guarantees only that `address` carries this worker's
    /// registered scheme; everything past the scheme is this worker's to
    /// parse and to reject.
    async fn send(&self, address: &str, msg: &Message) -> Result<(), Unavailable>;
}
