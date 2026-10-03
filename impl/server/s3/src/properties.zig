//! Every property this server reports to skulld, declared once.
//!
//! The static list is this file: `skull.catalog(@This())` walks it at compile
//! time, `zig build skulld-catalog` prints it, and the server announces it at
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

// ── Invariants ────────────────────────────────────────────────────────────────
//
// The program's own claims about itself, each `assert`ed where it is relied on.
// Without skulld each is exactly the `stdx.assert` it replaced: false means
// stop. Under skulld the report goes out first, naming which claim failed.

/// Anything that panics — every bare `unreachable` included — is reported by
/// the panic handler before the process ends. See `skull.Panic`.
pub const panicked = skull.Unreachable("the server panicked");

/// An origin's queue links work through the work itself, so one item in two
/// queues would corrupt both.
pub const work_queued_once = skull.Always("a work item is in at most one queue at a time");

/// An actor takes a batch only when the last one is fully done.
pub const actor_starts_idle = skull.Always("an actor takes a batch only when idle and empty-handed");

/// Each store operation is submitted from one phase and answered in it; an
/// answer in another phase is a reply to a question nobody is asking.
pub const completion_matches_phase = skull.Always("a store completion arrives in the phase that submitted it");

/// The store answers "not modified" only to a read that offered a version, and
/// offering one means holding the bytes it names.
pub const not_modified_has_cache = skull.Always("a not-modified answer comes only for a read that offered a cached copy");

/// An actor is freed only when nothing refers to it.
pub const actor_retired_clean = skull.Always("an actor is retired only when idle, empty and unqueued");

/// Ids are the document's keys; two entries under one id would make every
/// lookup a guess.
pub const document_ids_unique = skull.Always("a document never holds two promises, or two tasks, with one id");

/// Every completion — the ring's, the store's, a delivery's, a sweep's — is for
/// something counted as in flight. A count that goes below zero lost track.
pub const completion_was_in_flight = skull.Always("nothing completes that was not in flight");

/// The store must answer each operation exactly once; a second answer would
/// run a callback whose owner may already be gone.
pub const operation_completes_once = skull.Always("a store operation is completed exactly once");

/// An operation goes out unanswered and with somewhere to send the answer.
pub const operation_submitted_ready = skull.Always("a store operation is submitted unanswered and with a callback");

/// Keys are `prefix ++ name`; a prefix without its slash would run into names.
pub const key_prefix_normalized = skull.Always("a key prefix is empty or ends in a slash");

/// A timer holds a timeout in an intrusive list; arming one twice corrupts it.
pub const timeout_armed_once = skull.Always("a timeout is armed only when not armed, and with a callback");

/// As for store operations, for messages to workers.
pub const delivery_completes_once = skull.Always("a message delivery is completed exactly once");

/// A message goes out with somewhere to send its outcome.
pub const delivery_has_callback = skull.Always("a message is sent only with a callback");

/// Cron fields are 64-bit sets and a day is 86400 seconds; outside either, a
/// shift is undefined and a time of day is not one.
pub const cron_values_in_range = skull.Always("cron values stay inside their fields, and times inside their day");

/// The JSON writer keeps a fixed stack of open containers.
pub const json_depth_balanced = skull.Always("the JSON writer nests within its limit and closes only what it opened");

/// An HTTP exchange is answered once, while its handler holds it.
pub const exchange_answered_while_handling = skull.Always("an HTTP exchange is answered only while it is being handled");
