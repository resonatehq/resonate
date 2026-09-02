//! What a plugin says about itself, before anything runs.

/// Which of the three things a plugin is.
///
/// The kind decides how a binary composes it: a server is *selected*, one per
/// binary, because a server is what the whole process is; workers are
/// *registered*, keyed by the address schemes they claim; gateways are
/// *registered* too, each switched on or off independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// Answers Resonate protocol requests. Exactly one is active.
    Server,
    /// Consumes what a server emits, for the address schemes it claims.
    Worker,
    /// Accepts requests from outside and puts them to the server.
    Gateway,
}

impl Kind {
    /// Whether a binary has one of these, chosen by name, or many.
    pub const fn is_selected(self) -> bool {
        matches!(self, Kind::Server)
    }

    /// The configuration section this kind lives under.
    pub const fn section(self) -> &'static str {
        match self {
            Kind::Server => "servers",
            Kind::Worker => "transports",
            Kind::Gateway => "gateways",
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Kind::Server => "server",
            Kind::Worker => "worker",
            Kind::Gateway => "gateway",
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A plugin's own description of itself: pure data, no behaviour.
///
/// Separate from the factory beside it in the same static, so that reading a
/// plugin never requires running it — a plugin whose configuration is wrong
/// still has a name, a version and a summary, which is exactly when someone is
/// looking for them.
///
/// Built through [`Manifest::new`] and the `with_*` methods rather than as a
/// struct literal, so a field added later is a new method rather than a break
/// for every plugin that exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Manifest {
    /// The name this plugin is known by everywhere: its configuration key, its
    /// `--set` path, its log field. One name, so the surfaces cannot drift.
    pub id: &'static str,

    /// `env!("CARGO_PKG_NAME")`. What a collision has to name, because the
    /// person who can fix one is the person assembling the binary.
    pub krate: &'static str,

    /// `env!("CARGO_PKG_VERSION")` of the plugin crate, not of the server.
    pub version: &'static str,

    /// One line, for whoever is reading the list.
    pub summary: &'static str,

    /// The address schemes this plugin claims. Workers only.
    pub schemes: &'static [&'static str],

    /// Whether this plugin is active when configuration says nothing.
    ///
    /// Read by the framework from `<key>.enabled`, so a plugin's own `Config`
    /// does not repeat the field and the registration loop stays uniform.
    /// Ignored for a server, which is chosen by name rather than switched on.
    pub default_enabled: bool,
}

impl Manifest {
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

    /// Where this plugin's settings live: `transports.kafka`, `servers.sqlite`.
    ///
    /// Derived from the kind and the id rather than declared, so the key in a
    /// config file, the `RESONATE_*` variable and the `--set` path cannot
    /// disagree with each other or with the plugin.
    pub fn config_key(&self, kind: Kind) -> String {
        format!("{}.{}", kind.section(), self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_key_is_derived_not_declared() {
        let m = Manifest::new("kafka", "resonate-worker-kafka", "1.0.0");
        assert_eq!(m.config_key(Kind::Worker), "transports.kafka");
        let s = Manifest::new("sqlite", "resonate-server-dbms", "1.0.0");
        assert_eq!(s.config_key(Kind::Server), "servers.sqlite");
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
