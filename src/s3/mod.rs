//! The S3 backend: the kernel's shell.
//!
//! One CAS'd object per origin holds that origin's whole document; the kernel
//! decides over it and the shell here performs what it decides. Nothing in this
//! module imports `persistence`, `server`, or `oracle` — this is a fourth
//! server implementation, not a fourth `Db`.
//!
//! The internal graph, bottom up: [`store`] is the port to the bucket and
//! [`codec`] the bytes on it; [`applier`] decides and writes through both,
//! backed by [`cache`] and [`timer_queue`] and handing post-commit sends to
//! [`outbox`]; [`timerd`] and [`schedules`] drive deadlines through the
//! applier; [`scan`] reads the whole store; and [`server`] wires all of it
//! behind the `ResonateServer` port. Each module's own comment carries its
//! contract, dependencies and dependants.

pub mod applier;
pub mod cache;
pub mod codec;
pub mod outbox;
pub mod scan;
pub mod schedules;
pub mod server;
pub mod store;
pub mod timer_queue;
pub mod timerd;
