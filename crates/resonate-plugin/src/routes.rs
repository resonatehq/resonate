//! Routes a plugin serves, on the listener the HTTP gateway already has.
//!
//! The same idea as [`prometheus`](crate::prometheus), one layer up. There is
//! one metrics registry that every plugin declares into and one endpoint that
//! serves it; here there is one router that every plugin contributes to and one
//! gateway that binds it. A plugin that needs an HTTP route — a poll transport
//! handing out SSE connections, a worker taking a callback, the console —
//! registers it and is done. It opens no socket and owns no port.
//!
//! That is not a convenience. A second listener is a second port to expose, a
//! second thing to put behind a proxy, a second origin for a browser, and a
//! second address every SDK has to be told about. One process serving one port
//! is the property worth keeping, and it is only keepable if contributing a
//! route costs nothing.
//!
//! # Registering one
//!
//! In `configure`, which is where a plugin decides what it is. Registration is
//! an in-memory push — it opens nothing and cannot fail, so it does not belong
//! in `init`:
//!
//! ```ignore
//! fn configure(settings: &Settings, deps: WorkerDependencies) -> Result<...> {
//!     let config: Config = settings.extract()?;
//!     let worker = Arc::new(Callbacks::new(deps.server, config));
//!
//!     let handler = Arc::clone(&worker);
//!     deps.routes.add(PLUGIN.id(), move |auth| {
//!         axum::Router::new()
//!             .route("/callback/:id", axum::routing::post(handle))
//!             .with_state(State { worker: handler, auth })
//!     });
//!
//!     Ok(Some(worker))
//! }
//! ```
//!
//! # Why a builder and not a router
//!
//! Because of *when* the auth policy exists. The key material is read from disk
//! by the gateway's `init` — that is the whole reason `init` is fallible — so a
//! route that must authenticate the same way as everything else cannot be built
//! before then. The gateway calls each builder with the policy it just loaded,
//! `None` when auth is off, and that is what keeps a merged route from being a
//! hole in it.
//!
//! It also means one policy at one edge. A plugin does not carry its own `auth`
//! setting for a listener it does not own.

// Note the two `Router`s. What a plugin registers here is an `axum::Router` —
// HTTP path to handler. The workspace's bare `Router` is the other one, which
// routes an address to a worker and has nothing to do with HTTP.
use std::sync::{Arc, Mutex};

/// A router, built once the gateway's auth policy is known.
pub type RouteBuilder = Box<dyn FnOnce(Option<resonate_auth::AuthMode>) -> axum::Router + Send>;

/// What the plugins in this binary want served, alongside the protocol's own.
///
/// Handed to every worker and every gateway in its dependencies rather than
/// reached for through a global. The registry this crate rejects `inventory`
/// for is the same registry: something a plugin writes into invisibly, that
/// fails as "my route isn't there" with nothing to read. A field in a struct
/// the composition root passes is the version of this that can be tested.
#[derive(Default)]
pub struct Routes {
    pending: Mutex<Vec<(String, RouteBuilder)>>,
}

impl Routes {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Serve these routes on the gateway's listener.
    ///
    /// `plugin` is the registering plugin's id, which is what a path collision
    /// has to name — axum refuses two handlers for one method on one path,
    /// loudly, at startup, and "some plugin already claims POST /callback" is
    /// not an answer anyone can act on.
    ///
    /// Paths must not collide with the protocol's own (`/`, `/ready`) either.
    pub fn add(
        &self,
        plugin: impl Into<String>,
        build: impl FnOnce(Option<resonate_auth::AuthMode>) -> axum::Router + Send + 'static,
    ) {
        self.pending
            .lock()
            .expect("routes mutex")
            .push((plugin.into(), Box::new(build)));
    }

    /// Everything registered, for the gateway that serves it.
    ///
    /// Draining, because a builder is `FnOnce` and there is one listener. A
    /// second gateway asking gets nothing, which is the honest answer: the
    /// routes are already on the first one.
    pub fn take(&self) -> Vec<(String, RouteBuilder)> {
        std::mem::take(&mut *self.pending.lock().expect("routes mutex"))
    }

    /// Whether anything is registered, without taking it.
    pub fn is_empty(&self) -> bool {
        self.pending.lock().expect("routes mutex").is_empty()
    }
}

impl std::fmt::Debug for Routes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending = self.pending.lock().expect("routes mutex");
        f.debug_struct("Routes")
            .field(
                "pending",
                &pending.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            )
            .finish()
    }
}
