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

/// Returns true if `address` is a valid Resonate address.
///
/// Validity is deliberately shallow: an address is valid if it parses as a URI
/// with a scheme. Everything past the scheme belongs to whichever worker is
/// registered for it, and this function must not know about it.
///
/// That shallowness is a requirement, not a simplification. Validation has to
/// be a pure function of the string — identical on every deployment — because
/// the reference model has no workers at all, and because a server's enabled
/// transports must never change which requests it accepts. A scheme-aware
/// check would make both untrue.
///
/// The consequence is that syntax errors past the scheme surface at delivery
/// rather than at admission.
pub fn is_valid_address(address: &str) -> bool {
    url::Url::parse(address).is_ok()
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
    fn valid_address_is_any_uri_with_a_scheme() {
        for addr in [
            "http://worker:9999",
            "https://worker/path",
            "poll://uni@group",
            "poll://any@group/id",
            "gcps://project/topic",
            "bash://",
            "bash://docker/alpine",
            // Schemes core knows nothing about are valid — that is the point.
            "unknown://x/y",
            "mailto:a@b.c",
            "foo:bar",
            // Well-formed URI, malformed for its scheme. Admitted here; the
            // worker registered for the scheme rejects it at delivery.
            "poll://group",
            "poll://bogus@group",
            "gcps://project",
        ] {
            assert!(is_valid_address(addr), "expected valid: {addr}");
        }
    }

    #[test]
    fn invalid_address_is_not_a_uri() {
        for addr in ["", "not a url", "/relative", "http://"] {
            assert!(!is_valid_address(addr), "expected invalid: {addr}");
        }
    }

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
