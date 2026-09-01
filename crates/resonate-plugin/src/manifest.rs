//! What a plugin says about itself, before anything runs.

/// Which node of the ring a plugin fills.
///
/// The ring is fixed — gateways put requests to a server, a server hands what
/// it emits to a router, a router delivers to workers, and a worker calls back
/// into a server — so a plugin never declares what it *consumes*. It declares
/// what it *is*, and the ring says the rest: what it is handed, whether that
/// handle is strong or weak, when it starts, when it stops, and where its
/// configuration lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Port {
    /// Accepts requests from outside and puts them to a server.
    Gateway,
    /// Answers requests. Exactly one is active.
    Server,
    /// Resolves an address to a worker. Exactly one is active.
    Router,
    /// Consumes what a server emits. Many, keyed by address scheme.
    Worker,
}

impl Port {
    /// Whether the ring has one of these (selected) or many (registered).
    pub const fn is_singleton(self) -> bool {
        matches!(self, Port::Server | Port::Router)
    }

    /// The configuration section this node's plugins live under.
    pub const fn section(self) -> &'static str {
        match self {
            Port::Gateway => "gateways",
            Port::Server => "servers",
            Port::Router => "routers",
            Port::Worker => "transports",
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Port::Gateway => "gateway",
            Port::Server => "server",
            Port::Router => "router",
            Port::Worker => "worker",
        }
    }
}

impl std::fmt::Display for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A plugin's own description of itself: pure data, no behaviour.
///
/// Separate from the factory beside it in the same static, and deliberately:
///
/// - **Reading a plugin must never require running it.** `resonate plugins`
///   prints a row for a plugin that is disabled, or whose configuration is
///   wrong — which is exactly when someone is looking.
/// - **Collisions are detectable before anything exists.** Two plugins
///   claiming one scheme is a manifest-only check: no config file, no
///   construction, and a builder can run it before a long compile.
/// - **It is `const`.** Only `&'static str`, so the whole registry is data in
///   the binary — no allocation and no lazy initialisation.
///
/// Built through [`Manifest::new`] and the `with_*` methods rather than as a
/// struct literal, so that a field added later is a new method rather than a
/// break for every plugin that exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Manifest {
    /// The name this plugin is known by everywhere: its configuration key, its
    /// `--set` path, its log field, its row in `resonate plugins`. One name, so
    /// the surfaces cannot drift apart.
    pub id: &'static str,

    /// `env!("CARGO_PKG_NAME")`. The guard against `id` drifting from the name
    /// a user types into `cargo add` — the conformance suite asserts the
    /// convention `resonate-<port>-<id>`.
    pub krate: &'static str,

    /// `env!("CARGO_PKG_VERSION")` of the plugin crate, not of the server.
    pub version: &'static str,

    /// One line, for `resonate plugins`.
    pub summary: &'static str,

    /// Address schemes a worker claims. Empty for every other port.
    ///
    /// Two workers claiming one scheme is a startup error naming both, never
    /// a silent last-registration-wins.
    pub schemes: &'static [&'static str],

    /// Whether this plugin is active when configuration says nothing.
    ///
    /// Read by the framework from `<key>.enabled`, so a plugin's own `Config`
    /// does not repeat the field and the registration loop stays uniform.
    /// Ignored for the singleton ports, which are selected rather than enabled.
    pub default_enabled: bool,
}

impl Manifest {
    /// The three things every plugin must say. Everything else has a default
    /// and its own `with_*`.
    pub const fn new(id: &'static str, krate: &'static str, version: &'static str) -> Self {
        Self {
            id,
            krate,
            version,
            summary: "",
            schemes: &[],
            default_enabled: true,
        }
    }

    pub const fn with_summary(mut self, summary: &'static str) -> Self {
        self.summary = summary;
        self
    }

    /// The address schemes a worker serves.
    pub const fn with_schemes(mut self, schemes: &'static [&'static str]) -> Self {
        self.schemes = schemes;
        self
    }

    /// Off unless configuration turns it on — for a plugin that needs
    /// credentials, or that costs something to run.
    pub const fn disabled_by_default(mut self) -> Self {
        self.default_enabled = false;
        self
    }

    /// Where this plugin's settings live, e.g. `transports.kafka`.
    ///
    /// Derived from the port and the id rather than declared, so the key in a
    /// config file, the `RESONATE_*` variable and the `--set` path cannot
    /// disagree with each other or with the crate.
    pub fn config_key(&self, port: Port) -> String {
        format!("{}.{}", port.section(), self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_key_is_derived_not_declared() {
        let m = Manifest::new("kafka", "resonate-worker-kafka", "1.0.0");
        assert_eq!(m.config_key(Port::Worker), "transports.kafka");
    }

    #[test]
    fn manifest_is_const_constructible() {
        static M: Manifest = Manifest::new("nats", "resonate-worker-nats", "1.0.0")
            .with_summary("Deliver by publishing to a NATS subject")
            .with_schemes(&["nats"])
            .disabled_by_default();
        assert_eq!(M.schemes, &["nats"]);
        assert!(!M.default_enabled);
    }
}
