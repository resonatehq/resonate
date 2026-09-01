//! What a worker plugin is, and what it is handed when it is built.

use std::sync::{Arc, Weak};

use resonate_core::{ResonateServer, ResonateWorker};

use crate::config::Settings;
use crate::error::ConfigError;
use crate::manifest::Manifest;

/// What a worker is handed.
///
/// The server handle is [`Weak`] deliberately: a router holds its workers and a
/// server holds its router, so a strong handle back would close a reference
/// cycle and nothing in it would ever be dropped. Upgrade per message; a failed
/// upgrade means the server is gone and there is no work worth doing.
#[non_exhaustive]
pub struct WorkerCtx {
    pub server: Weak<dyn ResonateServer>,
    /// `tasks.lease_timeout` — what an in-process worker requests when it
    /// acquires, unless it has an opinion of its own. Server-owned, so it
    /// cannot come out of the plugin's own settings.
    pub task_lease_timeout: i64,
}

impl WorkerCtx {
    pub fn new(server: Weak<dyn ResonateServer>, task_lease_timeout: i64) -> Self {
        Self {
            server,
            task_lease_timeout,
        }
    }
}

/// Phase two: build the worker. Sync and infallible, because it runs where the
/// server's own construction closes the reference cycle, and nothing there can
/// fail or await.
pub type WorkerFactory = Box<dyn FnOnce(&WorkerCtx) -> Arc<dyn ResonateWorker> + Send>;

/// A plugin that consumes what a server emits.
///
/// A `static` with no `impl` block: less ceremony than a trait for someone
/// writing their first plugin, and `const`-constructible, so the manifest is
/// data in the binary rather than something built at startup.
pub struct WorkerPlugin {
    pub manifest: Manifest,
    /// This plugin's settings when nobody has said anything. Seeded into the
    /// defaults layer, so the server's own defaults name no plugin.
    pub defaults: fn() -> serde_json::Value,
    /// Phase one: extract and validate. Runs before anything is constructed, so
    /// a bad setting is a startup error rather than a message that later goes
    /// quietly nowhere. The typed `Config` is captured by the returned closure
    /// and never named outside the plugin's own crate.
    pub configure: fn(&Settings<'_>) -> Result<WorkerFactory, ConfigError>,
}

impl WorkerPlugin {
    pub const fn new(
        manifest: Manifest,
        defaults: fn() -> serde_json::Value,
        configure: fn(&Settings<'_>) -> Result<WorkerFactory, ConfigError>,
    ) -> Self {
        Self {
            manifest,
            defaults,
            configure,
        }
    }
}

impl std::fmt::Debug for WorkerPlugin {
    /// The manifest, which is the printable half. A factory has nothing to show
    /// and a log line wants the name anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPlugin")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}
