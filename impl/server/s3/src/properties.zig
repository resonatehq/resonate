//! Every property this server reports to skulld, declared once.
//!
//! The static list is this file: `skull.catalog(@This())` walks it at compile
//! time, `zig build skull-catalog` prints it, and the server announces it at
//! startup. A property is a `pub const` here and a `check` or `reached` at the
//! place it is decided; see `skull.zig`.

const skull = @import("skull.zig");

// ── Safety ────────────────────────────────────────────────────────────────────

/// A document that does not decode is either a bug in this server or a store
/// that returned something other than what was written. Neither may happen.
pub const document_decodes = skull.Always("every document read from the store decodes");

// ── Evidence the hard cases were exercised ───────────────────────────────────

/// A commit lost a race to another writer and was decided again.
pub const commit_contended = skull.Sometimes("a commit lost a race and was decided again");

/// The store did not answer a commit, and the core decided again.
pub const commit_timed_out = skull.Sometimes("a commit the store did not answer was decided again");

/// The deadline queue was rebuilt from the bucket at startup.
pub const deadlines_seeded = skull.Reachable("the deadline queue was seeded from the store");

/// Every attempt went unanswered and the caller was told "may or may not".
pub const store_never_answered = skull.Reachable("a request ran out of attempts and was answered may-or-may-not");
