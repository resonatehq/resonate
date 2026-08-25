//! The S3 backend: the kernel's shell.
//!
//! One CAS'd object per origin holds that origin's whole document; the kernel
//! decides over it and the shell here performs what it decides. Nothing in this
//! module imports `persistence`, `server`, or `oracle` — this is a fourth
//! server implementation, not a fourth `Db`.

pub mod cache;
pub mod codec;
pub mod store;
