//! The set of plugins a binary was assembled from.
//!
//! Not a list in the server's repository. A binary names its own plugins, which
//! is what lets a plugin be published by anyone without an edit to the server —
//! and what lets a build carry four of them rather than four hundred.

use crate::error::RegistryError;
#[rustfmt::skip]
use crate::plugin::{ServerPlugin, WorkerPlugin, GatewayPlugin};

/// What a binary is assembled from.
///
/// ```ignore
/// resonate::run(Registry::new()
///     .server(&resonate_server_dbms::SQLITE)
///     .worker(&resonate_worker_kafka::PLUGIN)
///     .gateway(&resonate_gateway_http::PLUGIN))
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
    servers: Vec<&'static ServerPlugin>,
    workers: Vec<&'static WorkerPlugin>,
    gateways: Vec<&'static GatewayPlugin>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn server(mut self, plugin: &'static ServerPlugin) -> Self {
        self.servers.push(plugin);
        self
    }

    pub fn worker(mut self, plugin: &'static WorkerPlugin) -> Self {
        self.workers.push(plugin);
        self
    }

    pub fn gateway(mut self, plugin: &'static GatewayPlugin) -> Self {
        self.gateways.push(plugin);
        self
    }

    pub fn servers(&self) -> &[&'static ServerPlugin] {
        &self.servers
    }

    pub fn workers(&self) -> &[&'static WorkerPlugin] {
        &self.workers
    }

    pub fn gateways(&self) -> &[&'static GatewayPlugin] {
        &self.gateways
    }

    /// The server this configuration selected, or a report naming both what was
    /// asked for and what this binary actually carries.
    ///
    /// The distinction a string match cannot make: a backend that is *not
    /// compiled in* is a different problem from one that is misspelled, and
    /// neither should quietly fall through to a different backend.
    pub fn select_server(&self, id: &str) -> Result<&'static ServerPlugin, RegistryError> {
        self.servers
            .iter()
            .copied()
            .find(|p| p.id() == id)
            .ok_or_else(|| RegistryError::NotCompiledIn {
                requested: id.to_string(),
                available: self.servers.iter().map(|p| p.id()).collect(),
            })
    }

    /// Everything wrong with this *set* of plugins, from the plugins alone.
    ///
    /// No configuration is read and nothing is constructed.
    pub fn check(&self) -> Result<(), Vec<RegistryError>> {
        let mut errors = Vec::new();

        let ids = self
            .servers
            .iter()
            .map(|p| ("server", p.id(), p.krate))
            .chain(self.workers.iter().map(|p| ("worker", p.id(), p.krate)))
            .chain(self.gateways.iter().map(|p| ("gateway", p.id(), p.krate)))
            .collect::<Vec<_>>();
        for (i, (kind, id, krate)) in ids.iter().enumerate() {
            for (other_kind, other_id, other_krate) in &ids[i + 1..] {
                if kind == other_kind && id == other_id {
                    errors.push(RegistryError::DuplicateId {
                        kind,
                        id: id.clone(),
                        krates: ((*krate).to_string(), (*other_krate).to_string()),
                    });
                }
            }
        }

        // A worker that claims nothing can never be reached; two that claim the
        // same scheme make routing a coin flip. Both are the binary's problem,
        // not the operator's, so both are caught here.
        let mut seen: Vec<(&str, &'static str)> = Vec::new();
        for p in &self.workers {
            if p.schemes.is_empty() {
                errors.push(RegistryError::NoSchemes {
                    id: p.id(),
                    krate: p.krate.to_string(),
                });
            }
            for scheme in p.schemes {
                if let Some((_, other)) = seen.iter().find(|(s, _)| s == scheme) {
                    errors.push(RegistryError::DuplicateScheme {
                        scheme: (*scheme).to_string(),
                        krates: ((*other).to_string(), p.krate.to_string()),
                    });
                } else {
                    seen.push((scheme, p.krate));
                }
            }
        }

        if self.servers.is_empty() {
            errors.push(RegistryError::NoServer);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
