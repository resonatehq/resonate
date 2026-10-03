//! Resonate sandbox: what the sandbox plugin and rn8 agree on.
//!
//! Two things, both small. [`frame`] is the wire between the plugin, on the
//! host, and rn8, the relay baked into the guest image: newline-delimited JSON
//! on rn8's stdin and stdout. [`backend`] is what the plugin asks of a sandbox
//! runtime: create one from an image, run one command in it with live stdio,
//! destroy it.
//!
//! Its own crate because the two ends are different programs. The plugin runs
//! inside the Resonate server and rn8 is a static binary inside someone else's
//! image; this is the one thing they have to share, and it must not drag either
//! one's dependencies into the other.

pub mod backend;
pub mod frame;

pub use backend::{Backend, ChildProcess, Command, Egress, Limits, Process, Stderr, Stdin, Stdout};
pub use frame::{FrameError, FrameReader, FromGuest, LogStream, ToGuest, VERSION};

/// rn8's exit status, and what the plugin makes of it.
///
/// Only three, because the plugin acts on only three. Whether a step that
/// ended normally completed or suspended is the server's to know, not rn8's:
/// the SDK told it, through the relay, before the push returned.
pub mod exit {
    /// The step ended normally.
    pub const OK: i32 = 0;
    /// The worker crashed, or the push failed.
    pub const WORKER: i32 = 1;
    /// A frame was malformed, out of order, or of a version rn8 does not speak.
    pub const FRAMING: i32 = 2;
}
