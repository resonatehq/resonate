//! The set of plugins a binary was assembled from.
//!
//! Not a list in the server's repository. A binary names its own plugins, which
//! is what lets a plugin be published by anyone without an edit to the server —
//! and what lets a build carry four of them rather than four hundred.

use crate::error::RegistryError;
use crate::plugin::WorkerPlugin;

/// What a binary is assembled from.
///
/// ```ignore
/// resonate::run(Registry::new()
///     .worker(&resonate_worker_kafka::PLUGIN)
///     .worker(&resonate_worker_nats::PLUGIN))
/// ```
///
/// Explicit registration, not link-time collection. `inventory` and `linkme`
/// look like they would let a plugin announce itself with no list at all, but a
/// dependency whose items are never referenced contributes no object code to the
/// link — so the crate has to be named in the binary regardless. That buys
/// nothing and adds a mechanism dead-code elimination can strip silently,
/// failing as "the plugin isn't there" with no error to read.
#[derive(Default, Debug)]
pub struct Registry {
    workers: Vec<&'static WorkerPlugin>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker(mut self, plugin: &'static WorkerPlugin) -> Self {
        self.workers.push(plugin);
        self
    }

    pub fn workers(&self) -> &[&'static WorkerPlugin] {
        &self.workers
    }

    /// Everything wrong with this *set* of plugins, from manifests alone.
    ///
    /// No configuration is read and nothing is constructed.
    pub fn check(&self) -> Result<(), Vec<RegistryError>> {
        let mut errors = Vec::new();

        for (i, a) in self.workers.iter().enumerate() {
            for b in &self.workers[i + 1..] {
                if a.manifest.id == b.manifest.id {
                    errors.push(RegistryError::DuplicateId {
                        id: a.manifest.id.to_string(),
                        krates: (a.manifest.krate.to_string(), b.manifest.krate.to_string()),
                    });
                }
            }
        }

        // A worker that claims nothing can never be reached; two that claim the
        // same scheme make routing a coin flip. Both are the binary's problem,
        // not the operator's, so both are caught here.
        let mut seen: Vec<(&str, &'static WorkerPlugin)> = Vec::new();
        for p in &self.workers {
            if p.manifest.schemes.is_empty() {
                errors.push(RegistryError::NoSchemes {
                    id: p.manifest.id.to_string(),
                    krate: p.manifest.krate.to_string(),
                });
            }
            for scheme in p.manifest.schemes {
                if let Some((_, other)) = seen.iter().find(|(s, _)| s == scheme) {
                    errors.push(RegistryError::DuplicateScheme {
                        scheme: (*scheme).to_string(),
                        krates: (
                            other.manifest.krate.to_string(),
                            p.manifest.krate.to_string(),
                        ),
                    });
                } else {
                    seen.push((scheme, p));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
