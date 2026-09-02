//! The blob backend: a complete `ResonateServer` over conditional-write
//! object storage.
//!
//! Two layers. The **kernel** is the protocol's state machine as a pure
//! function — `handle(&doc, req, now) -> (Vec<Effect>, Reply)` and
//! `drain(&doc, now) -> Vec<Effect>` — reading no clock, generating no ids,
//! touching no I/O. The **shell** performs what the kernel decides against one
//! CAS'd object per origin, with one actor per origin so decisions on a
//! document are serialized.
//!
//! The internal graph, bottom up: [`store`] is the port to the bucket and
//! [`codec`] the bytes on it; [`applier`] decides and writes through both,
//! backed by [`cache`] and [`timer_queue`] and handing post-commit sends to
//! [`sender`]; [`timerd`] and [`schedules`] drive deadlines through the
//! origin actors; [`scan`] reads the whole store; and [`server`] wires all of
//! it behind the `ResonateServer` port.
//!
//! This crate depends on `resonate-core` and third-party crates only. It is a
//! complete server — its own message delivery, its own timers, its own
//! snapshot — not a storage engine behind one.

pub mod applier;
pub mod cache;
pub mod codec;
pub mod kernel;
pub mod metrics;
pub mod oracle;
pub mod scan;
pub mod schedules;
pub mod sender;
pub mod server;
pub mod store;
pub mod timer_queue;
pub mod timerd;
