//! The set of plugins a binary was assembled from.
//!
//! Not a list in the server's repository. A binary names its own plugins, which
//! is what lets a plugin be published by anyone, to anywhere, without an edit
//! to the server — and what lets a build carry four of them rather than four
//! hundred.

use crate::error::RegistryError;
use crate::manifest::{Manifest, Port};
use crate::plugin::{GatewayPlugin, RouterPlugin, ServerPlugin, WorkerPlugin};

/// A plugin as the registry holds it: what it says about itself, and which
/// node of the ring it fills.
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub port: Port,
    pub manifest: Manifest,
}

impl Entry {
    pub fn config_key(&self) -> String {
        self.manifest.config_key(self.port)
    }
}

/// What a binary is assembled from.
///
/// ```ignore
/// resonate::run(Registry::new()
///     .worker(&resonate_worker_kafka::PLUGIN)
///     .worker(&resonate_worker_nats::PLUGIN)
///     .server(&resonate_server_dbms::SQLITE))
/// ```
///
/// Explicit registration, not link-time collection. `inventory` and `linkme`
/// look like they would let a plugin announce itself with no list at all, but
/// a dependency whose items are never referenced contributes no object code to
/// the link — so the crate has to be named in the binary regardless. That buys
/// nothing and adds a mechanism that aggressive dead-code elimination can strip
/// silently, failing as "the plugin isn't there" with no error to read.
#[derive(Default)]
pub struct Registry {
    servers: Vec<&'static ServerPlugin>,
    routers: Vec<&'static RouterPlugin>,
    gateways: Vec<&'static GatewayPlugin>,
    workers: Vec<&'static WorkerPlugin>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn server(mut self, plugin: &'static ServerPlugin) -> Self {
        self.servers.push(plugin);
        self
    }

    pub fn router(mut self, plugin: &'static RouterPlugin) -> Self {
        self.routers.push(plugin);
        self
    }

    pub fn gateway(mut self, plugin: &'static GatewayPlugin) -> Self {
        self.gateways.push(plugin);
        self
    }

    pub fn worker(mut self, plugin: &'static WorkerPlugin) -> Self {
        self.workers.push(plugin);
        self
    }

    pub fn servers(&self) -> &[&'static ServerPlugin] {
        &self.servers
    }
    pub fn routers(&self) -> &[&'static RouterPlugin] {
        &self.routers
    }
    pub fn gateways(&self) -> &[&'static GatewayPlugin] {
        &self.gateways
    }
    pub fn workers(&self) -> &[&'static WorkerPlugin] {
        &self.workers
    }

    /// Every plugin, in ring order. What `resonate plugins` prints and what
    /// the startup log names.
    pub fn entries(&self) -> Vec<Entry> {
        let mut out = Vec::new();
        for p in &self.gateways {
            out.push(Entry {
                port: Port::Gateway,
                manifest: p.manifest,
            });
        }
        for p in &self.servers {
            out.push(Entry {
                port: Port::Server,
                manifest: p.manifest,
            });
        }
        for p in &self.routers {
            out.push(Entry {
                port: Port::Router,
                manifest: p.manifest,
            });
        }
        for p in &self.workers {
            out.push(Entry {
                port: Port::Worker,
                manifest: p.manifest,
            });
        }
        out
    }

    /// The ids registered at one port.
    pub fn ids(&self, port: Port) -> Vec<String> {
        self.entries()
            .into_iter()
            .filter(|e| e.port == port)
            .map(|e| e.manifest.id.to_string())
            .collect()
    }

    /// Every scheme this binary can route, and the plugin that claims it.
    pub fn schemes(&self) -> Vec<(&'static str, &'static str)> {
        self.workers
            .iter()
            .flat_map(|p| p.manifest.schemes.iter().map(|s| (*s, p.manifest.id)))
            .collect()
    }

    /// What the configuration's defaults layer is built from:
    /// `(port, id, that plugin's defaults, whether it is on by default)`.
    pub fn default_layers(&self) -> Vec<(Port, &'static str, serde_json::Value, bool)> {
        let mut out = Vec::new();
        for p in &self.gateways {
            out.push((
                Port::Gateway,
                p.manifest.id,
                (p.defaults)(),
                p.manifest.default_enabled,
            ));
        }
        for p in &self.servers {
            out.push((
                Port::Server,
                p.manifest.id,
                (p.defaults)(),
                p.manifest.default_enabled,
            ));
        }
        for p in &self.routers {
            out.push((
                Port::Router,
                p.manifest.id,
                (p.defaults)(),
                p.manifest.default_enabled,
            ));
        }
        for p in &self.workers {
            out.push((
                Port::Worker,
                p.manifest.id,
                (p.defaults)(),
                p.manifest.default_enabled,
            ));
        }
        out
    }

    /// Everything wrong with this *set* of plugins, from manifests alone.
    ///
    /// No configuration is read and nothing is constructed, so a builder can
    /// run exactly this check when it generates a binary's composition root —
    /// failing in a second rather than after the compile.
    pub fn check(&self) -> Result<(), Vec<RegistryError>> {
        let mut errors = Vec::new();

        for port in [Port::Gateway, Port::Server, Port::Router, Port::Worker] {
            let entries: Vec<Entry> = self
                .entries()
                .into_iter()
                .filter(|e| e.port == port)
                .collect();
            for (i, a) in entries.iter().enumerate() {
                for b in &entries[i + 1..] {
                    if a.manifest.id == b.manifest.id {
                        errors.push(RegistryError::DuplicateId {
                            port,
                            id: a.manifest.id.to_string(),
                            krates: (a.manifest.krate.to_string(), b.manifest.krate.to_string()),
                        });
                    }
                }
            }
        }

        // A worker that claims nothing can never be reached; two that claim the
        // same scheme make routing a coin flip. Both are the binary's problem,
        // not the operator's, so both are caught here.
        let mut seen: Vec<(&str, &'static Manifest)> = Vec::new();
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
                        krates: (other.krate.to_string(), p.manifest.krate.to_string()),
                    });
                } else {
                    seen.push((scheme, &p.manifest));
                }
            }
        }

        // The ring has to be whole. A gateway is an edge into it and may be
        // absent — a server driven only by its own workers is a real shape.
        if self.servers.is_empty() {
            errors.push(RegistryError::MissingPort { port: Port::Server });
        }
        if self.routers.is_empty() {
            errors.push(RegistryError::MissingPort { port: Port::Router });
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// The server plugin configuration selected, or a report that names both
    /// what was asked for and what this binary actually carries.
    pub fn select_server(&self, id: &str) -> Result<&'static ServerPlugin, RegistryError> {
        self.servers
            .iter()
            .copied()
            .find(|p| p.manifest.id == id)
            .ok_or_else(|| RegistryError::NotCompiledIn {
                port: Port::Server,
                requested: id.to_string(),
                available: self.ids(Port::Server),
            })
    }

    pub fn select_router(&self, id: &str) -> Result<&'static RouterPlugin, RegistryError> {
        self.routers
            .iter()
            .copied()
            .find(|p| p.manifest.id == id)
            .ok_or_else(|| RegistryError::NotCompiledIn {
                port: Port::Router,
                requested: id.to_string(),
                available: self.ids(Port::Router),
            })
    }
}
