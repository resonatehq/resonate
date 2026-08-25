//! Addresses — the only thing about an address that is not a worker's business.
//!
//! A worker owns the syntax of its own scheme: what a `poll://` or `gcps://`
//! address means belongs to the poll or Pub/Sub worker, not here. `core` knows
//! only that an address is a URI, and the router knows only its scheme.

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

/// The scheme of `address`, or `None` if it is not a URI.
///
/// This is all the routing information there is: the router maps a scheme to a
/// worker and hands over the untouched address.
pub fn scheme_of(address: &str) -> Option<String> {
    url::Url::parse(address)
        .ok()
        .map(|u| u.scheme().to_string())
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
    fn scheme_of_extracts_the_routing_key() {
        for (addr, want) in [
            ("http://w:1", Some("http")),
            ("https://w/x", Some("https")),
            ("poll://any@g", Some("poll")),
            ("gcps://p/t", Some("gcps")),
            ("bash://docker/alpine", Some("bash")),
            ("not a url", None),
        ] {
            assert_eq!(scheme_of(addr).as_deref(), want, "for {addr}");
        }
    }
}
