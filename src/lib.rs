//! Resonate — durable promise engine.
//!
//! The crate is laid out as a hexagon: [`core`] holds the ports and the
//! vocabulary they speak, and every other module is an adapter that depends on
//! it. See `core`'s module docs for the dependency rule.
//!
//! The binary in `main.rs` is a thin shell over this library — it parses
//! arguments and calls in. Everything it can do, an integration test can do
//! too, which is the point.

pub mod auth;
pub mod cli;
pub mod config;
pub mod core;
pub mod mcp;
pub mod metrics;
pub mod oracle;
pub mod persistence;
pub mod processing;
pub mod server;
pub mod testing;
pub mod transport;
pub mod util;
