pub mod transport_gcps;
pub mod transport_http_poll;
pub mod transport_http_push;
pub mod transport_nats;

use transport_gcps::GcpsPubSubTransport;
use transport_http_poll::PollRegistry;
use transport_http_push::HttpPushTransport;
use transport_nats::NatsTransport;

use std::sync::Arc;

/// Dispatches messages to the appropriate transport by parsing the address once.
/// Routes by URL scheme: http/https → push, poll → SSE, gcps → GCP Pub/Sub, nats → NATS.
pub struct TransportDispatcher {
    http: Arc<HttpPushTransport>,
    poll: Arc<PollRegistry>,
    gcps: Option<Arc<GcpsPubSubTransport>>,
    nats: Option<Arc<NatsTransport>>,
}

impl TransportDispatcher {
    pub fn new(
        http: Arc<HttpPushTransport>,
        poll: Arc<PollRegistry>,
        gcps: Option<Arc<GcpsPubSubTransport>>,
        nats: Option<Arc<NatsTransport>>,
    ) -> Self {
        Self { http, poll, gcps, nats }
    }

    /// Parse the address, route to the correct transport, deliver.
    pub async fn send(&self, address: &str, payload: &serde_json::Value) {
        let kind = payload
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        match parse_address(address) {
            Some(Address::Http(addr)) => {
                tracing::debug!(transport = "http", address = %addr.url, kind = kind, "Dispatching message via HTTP push");
                self.http.send(&addr, payload).await;
            }
            Some(Address::Poll(addr)) => {
                tracing::debug!(transport = "poll", group = %addr.group, kind = kind, "Dispatching message via poll/SSE");
                let sse_data = serde_json::to_string(payload).unwrap_or_default();
                self.poll.send_poll(&addr, &sse_data).await;
            }
            Some(Address::Gcps(addr)) => match &self.gcps {
                Some(gcps) => {
                    tracing::debug!(transport = "gcps", project = %addr.project, topic = %addr.topic, kind = kind, "Dispatching message via GCP Pub/Sub");
                    gcps.send(&addr, payload).await;
                }
                None => {
                    tracing::warn!(address = %address, "GCP Pub/Sub transport not configured, message dropped")
                }
            },
            Some(Address::Nats(addr)) => match &self.nats {
                Some(nats) => {
                    tracing::debug!(transport = "nats", subject = %addr.subject, kind = kind, "Dispatching message via NATS");
                    nats.send(&addr, payload).await;
                }
                None => {
                    tracing::warn!(address = %address, "NATS transport not configured, message dropped")
                }
            },
            None => {
                tracing::warn!(address = %address, "Invalid address, message cannot be routed");
            }
        }
    }
}

/// Parsed address — determines which transport and where to deliver.
#[derive(Debug, Clone)]
pub enum Address {
    /// HTTP/HTTPS webhook delivery
    Http(HttpAddress),
    /// Poll SSE delivery
    Poll(PollAddress),
    /// Google Cloud Pub/Sub delivery
    Gcps(GcpsAddress),
    /// NATS subject delivery
    Nats(NatsAddress),
}

#[derive(Debug, Clone)]
pub struct HttpAddress {
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct PollAddress {
    pub cast: PollCast,
    pub group: String,
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PollCast {
    Uni,
    Any,
}

#[derive(Debug, Clone)]
pub struct GcpsAddress {
    pub project: String,
    pub topic: String,
}

#[derive(Debug, Clone)]
pub struct NatsAddress {
    pub subject: String,
}

/// Returns true if the address is a valid, routable URL.
pub fn is_valid_address(address: &str) -> bool {
    parse_address(address).is_some()
}

/// Parse an address string into a typed Address.
///
/// Supports:
/// - `http://...` / `https://...` — HTTP webhook delivery
/// - `poll://cast@group[/id]` — Poll SSE delivery
/// - `gcps://project/topic` — Google Cloud Pub/Sub delivery
/// - `nats://subject` — NATS subject delivery
pub fn parse_address(address: &str) -> Option<Address> {
    let parsed = url::Url::parse(address).ok()?;

    match parsed.scheme() {
        "http" | "https" => Some(Address::Http(HttpAddress {
            url: address.to_string(),
        })),
        "poll" => {
            let cast = match parsed.username() {
                "uni" => PollCast::Uni,
                "any" => PollCast::Any,
                _ => return None,
            };
            let group = parsed.host_str()?.to_string();
            let path = parsed.path();
            let id = if path.len() > 1 {
                Some(path[1..].to_string())
            } else {
                None
            };
            Some(Address::Poll(PollAddress { cast, group, id }))
        }
        "gcps" => {
            let project = parsed.host_str()?.to_string();
            let path = parsed.path();
            if path.len() <= 1 {
                return None; // need at least /topic
            }
            let topic = path[1..].to_string();
            if topic.is_empty() {
                return None;
            }
            Some(Address::Gcps(GcpsAddress { project, topic }))
        }
        "nats" => {
            let subject = parsed.host_str()?.to_string();
            if subject.is_empty() {
                return None;
            }
            Some(Address::Nats(NatsAddress { subject }))
        }
        _ => None,
    }
}
