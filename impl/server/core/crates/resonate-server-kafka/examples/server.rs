//! A Resonate server on Kafka: this plugin, the HTTP gateway and the
//! Prometheus endpoint — the shape a binary takes when it names its plugins,
//! and what `examples/load.rs` drives end to end.
//!
//! Configured like any binary built on `resonate-base`, by `resonate.toml` or
//! by environment:
//!
//!   RESONATE_SERVERS__SERVER_KAFKA__BROKERS=localhost:9092 \
//!   RESONATE_SERVERS__SERVER_KAFKA__NODE_ID=a \
//!   RESONATE_SERVERS__SERVER_KAFKA__PEER_BIND=127.0.0.1:8102 \
//!   RESONATE_SERVERS__SERVER_KAFKA__PEER_URL=http://127.0.0.1:8102 \
//!   RESONATE_GATEWAYS__GATEWAY_HTTP__BIND=127.0.0.1:8101 \
//!   RESONATE_GATEWAYS__GATEWAY_METRICS__BIND=127.0.0.1:9101 \
//!     cargo run --release -p resonate-server-kafka --example server

use resonate_base::{Options, Registry};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    resonate_base::main(
        Registry::new()
            .server(&resonate_server_kafka::PLUGIN)
            .gateway(&resonate_gateway_http::PLUGIN)
            .gateway(&resonate_gateway_metrics::PLUGIN),
        Options::default(),
    )
    .await
}
