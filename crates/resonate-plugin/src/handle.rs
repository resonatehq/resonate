//! The server's place in the wiring, allocated before the server exists.

use std::sync::{Arc, OnceLock};

use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};

/// A `ResonateServer` that will exist shortly, and that workers can already
/// hold a `Weak` to.
///
/// The wiring is a cycle: a worker needs the server, the router holds the
/// workers, and the server is built *from* the router. With a single concrete
/// server type that is [`Arc::new_cyclic`] — the framework allocates the `Arc`,
/// so it knows the type, and can hand out the `Weak` before the value exists.
///
/// Once the server is a plugin, the framework no longer knows the type. So the
/// place is allocated instead of the server: workers and gateways hold this, a
/// plugin builds whatever it likes behind it — including its own `new_cyclic`,
/// which an engine-backed server needs for its timer — and
/// [`fulfil`](Self::fulfil) closes the cycle.
///
/// The indirection is one `OnceLock` read and one extra dynamic call per
/// request, against a database round trip. What it buys is that a plugin owns
/// its own construction, which was the point.
#[derive(Default)]
pub struct ServerHandle {
    inner: OnceLock<Arc<dyn ResonateServer>>,
}

impl ServerHandle {
    /// An unfulfilled place in the ring.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Put the server in its place. A second call is refused rather than
    /// ignored: two servers behind one handle is a wiring bug, and the one that
    /// lost would be silently unreachable.
    pub fn fulfil(&self, server: Arc<dyn ResonateServer>) -> Result<(), &'static str> {
        self.inner
            .set(server)
            .map_err(|_| "this handle already holds a server")
    }

    /// Whether the server is in place.
    pub fn is_fulfilled(&self) -> bool {
        self.inner.get().is_some()
    }

    fn server(&self) -> Result<&Arc<dyn ResonateServer>, Unavailable> {
        // Nothing can route before startup finishes — a gateway binds last, and
        // workers start after the server is in place — so this is unreachable in
        // a running process. It is an error rather than a panic because a wiring
        // mistake should be reported, not abort.
        self.inner
            .get()
            .ok_or_else(|| Unavailable::new("server is not started"))
    }
}

#[async_trait::async_trait]
impl ResonateServer for ServerHandle {
    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.server()?.process(req).await
    }

    async fn ready(&self) -> bool {
        match self.inner.get() {
            Some(server) => server.ready().await,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Weak;

    struct Yes;

    #[async_trait::async_trait]
    impl ResonateServer for Yes {
        async fn process(&self, _req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn a_handle_is_not_ready_until_the_server_is_in_place() {
        let handle = ServerHandle::new();
        assert!(!handle.is_fulfilled());
        assert!(!handle.ready().await, "nothing is behind it yet");

        // What a worker is given, before the server exists.
        let weak: Weak<dyn ResonateServer> = Arc::downgrade(&handle) as Weak<dyn ResonateServer>;
        assert!(weak.upgrade().is_some());

        handle.fulfil(Arc::new(Yes)).unwrap();
        assert!(handle.is_fulfilled());
        assert!(handle.ready().await);
    }

    #[test]
    fn a_second_server_behind_one_handle_is_refused() {
        let handle = ServerHandle::new();
        handle.fulfil(Arc::new(Yes)).unwrap();
        handle
            .fulfil(Arc::new(Yes))
            .expect_err("the loser would be silently unreachable");
    }
}
