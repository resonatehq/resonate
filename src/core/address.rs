//! Addresses — where a message is sent, and how the string form is parsed.
//!
//! Shared vocabulary rather than transport detail: the server validates
//! addresses on promises and tasks, the reference model validates them
//! identically, and the router parses them to select a worker.

/// Parsed address — determines which transport and where to deliver.
#[derive(Debug, Clone)]
pub enum Address {
    /// HTTP/HTTPS webhook delivery
    Http(HttpAddress),
    /// Poll SSE delivery
    Poll(PollAddress),
    /// Google Cloud Pub/Sub delivery
    Gcps(GcpsAddress),
    /// Bash script execution (script is in param.data).
    /// The bash transport re-parses the address to pick a backend
    /// (local / docker / tensorlake), so we only mark it routable here.
    #[allow(dead_code)]
    Bash(BashAddress),
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
#[allow(dead_code)]
pub struct BashAddress;

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
/// - `bash://` (local), `bash://docker/<image>`, `bash://tensorlake/<image>`
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
        "bash" => Some(Address::Bash(BashAddress)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bash_address_parses() {
        for addr in [
            "bash://",
            "bash://bash",
            "bash://docker/alpine",
            "bash://docker/library/ubuntu:latest",
            "bash://tensorlake/python-3.11",
        ] {
            assert!(
                matches!(parse_address(addr), Some(Address::Bash(_))),
                "expected Bash for {addr}"
            );
        }
    }
}
