pub mod codec;
pub mod context;
pub mod core;
pub mod durable;
pub mod effects;
pub mod error;
pub mod futures;
pub mod handle;
pub mod heartbeat;
pub mod http_network;
pub mod info;
pub mod network;
pub mod options;
pub mod promises;
pub mod registry;
pub mod resonate;
pub mod send;
pub mod transport;
pub mod types;

/// Protocol version string sent in all requests.
pub(crate) const PROTOCOL_VERSION: &str = "2025-01-15";

/// Current time in milliseconds since UNIX epoch.
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod test_utils;

/// Re-export the proc macro.
pub use resonate_macros::function;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::codec::Codec;
    pub use crate::codec::{Encryptor, NoopEncryptor};
    pub use crate::context::{Context, RpcTask, RunTask};
    pub use crate::durable::{Durable, ExecutionEnv};
    pub use crate::effects::Effects;
    pub use crate::error::{Error, Result};
    pub use crate::futures::{DurableFuture, RemoteFuture};
    pub use crate::handle::ResonateHandle;
    pub use crate::heartbeat::Heartbeat;
    pub use crate::http_network::HttpNetwork;
    pub use crate::info::Info;
    pub use crate::network::Network;
    pub use crate::options::Options;
    pub use crate::promises::{Promises, Schedules};
    pub use crate::registry::Registry;
    pub use crate::resonate::{
        ResRpcTask, ResRunTask, ResScheduleTask, Resonate, ResonateConfig, ResonateSchedule,
    };
    pub use crate::transport::Transport;
    pub use crate::types::{DurableKind, Outcome, PromiseState};
    pub use resonate_macros::function;
}
