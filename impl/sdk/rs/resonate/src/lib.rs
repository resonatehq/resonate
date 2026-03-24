pub mod codec;
pub mod context;
pub mod core;
pub mod durable;
pub mod effects;
pub mod error;
pub mod futures;
pub mod handle;
pub mod heartbeat;
pub mod info;
pub mod network;
pub mod options;
pub mod promises;
pub mod registry;
pub mod resonate;
pub mod send;
pub mod transport;
pub mod types;

#[cfg(test)]
mod test_utils;

/// Re-export the proc macro.
pub use resonate_macros::function;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::codec::Codec;
    pub use crate::context::{Context, RpcTask, RunTask};
    pub use crate::durable::Durable;
    pub use crate::effects::Effects;
    pub use crate::error::{Error, Result};
    pub use crate::futures::{DurableFuture, RemoteFuture};
    pub use crate::handle::ResonateHandle;
    pub use crate::heartbeat::Heartbeat;
    pub use crate::info::Info;
    pub use crate::network::Network;
    pub use crate::options::{Options, PartialOptions};
    pub use crate::promises::{Promises, Schedules};
    pub use crate::registry::Registry;
    pub use crate::codec::{Encryptor, NoopEncryptor};
    pub use crate::resonate::{BasicAuth, Resonate, ResonateConfig, ResonateSchedule};
    pub use crate::transport::Transport;
    pub use crate::types::{DurableKind, Outcome, PromiseState};
    pub use resonate_macros::function;
}
