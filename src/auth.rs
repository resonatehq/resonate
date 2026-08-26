use std::collections::HashSet;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;

use crate::core::types::{RequestEnvelope, ResponseEnvelope};

// ---------------------------------------------------------------------------
// Public key — tagged by key family so we know which algorithms to accept
// ---------------------------------------------------------------------------

/// Runtime auth configuration.
///
/// When `key` is Some, tokens are verified against the public key using the
/// auto-detected algorithm family. When `key` is None, unsigned tokens
/// (alg: none) are accepted — useful for debug/testing.
pub struct AuthConfig {
    pub key: Option<VerificationKey>,
    pub iss: Option<String>,
    pub aud: Option<String>,
}

pub struct VerificationKey {
    pub decoding_key: DecodingKey,
    pub algorithms: Vec<Algorithm>,
}

// `DecodingKey` is deliberately opaque (it holds key material), so show only
// the accepted algorithms. Enough for a test failure message; nothing secret.
impl std::fmt::Debug for VerificationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerificationKey")
            .field("algorithms", &self.algorithms)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// JWT claims
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // iss/aud are deserialized for jsonwebtoken validation, not read directly
struct Claims {
    role: Option<String>,
    /// `prefix` can be absent (None), null (Some(Value::Null)), or a string.
    prefix: Option<Value>,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    aud: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Key loading
// ---------------------------------------------------------------------------

/// The key family a PEM turned out to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Rsa,
    Ec,
    Ed25519,
}

impl KeyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::Rsa => "RSA",
            KeyType::Ec => "EC",
            KeyType::Ed25519 => "Ed25519",
        }
    }

    /// The algorithms a key of this family may be used to verify.
    fn algorithms(&self) -> Vec<Algorithm> {
        match self {
            KeyType::Rsa => vec![
                Algorithm::RS256,
                Algorithm::RS384,
                Algorithm::RS512,
                Algorithm::PS256,
                Algorithm::PS384,
                Algorithm::PS512,
            ],
            KeyType::Ec => vec![Algorithm::ES256, Algorithm::ES384],
            KeyType::Ed25519 => vec![Algorithm::EdDSA],
        }
    }
}

/// Parse a public key PEM, auto-detecting the key type and accepted algorithms.
///
/// Pure: no file system, no logging. [`load_public_key`] is the thin IO wrapper
/// around it, which keeps every parse branch reachable from a test with an
/// inline fixture.
pub fn parse_public_key(pem: &[u8]) -> Result<(VerificationKey, KeyType), String> {
    for key_type in [KeyType::Rsa, KeyType::Ec, KeyType::Ed25519] {
        let parsed = match key_type {
            KeyType::Rsa => DecodingKey::from_rsa_pem(pem),
            KeyType::Ec => DecodingKey::from_ec_pem(pem),
            KeyType::Ed25519 => DecodingKey::from_ed_pem(pem),
        };
        if let Ok(decoding_key) = parsed {
            return Ok((
                VerificationKey {
                    decoding_key,
                    algorithms: key_type.algorithms(),
                },
                key_type,
            ));
        }
    }

    Err("Unsupported or invalid public key. \
         Supported types: RSA, EC (P-256/P-384), Ed25519."
        .to_string())
}

/// Load a public key PEM file, auto-detecting the key type and accepted algorithms.
pub fn load_public_key(path: &str) -> Result<VerificationKey, String> {
    let pem = std::fs::read(path)
        .map_err(|e| format!("Failed to read public key file '{}': {}", path, e))?;

    let (key, key_type) =
        parse_public_key(&pem).map_err(|e| format!("{e} (while reading '{path}')"))?;
    tracing::info!(path = %path, key_type = key_type.as_str(), "Loaded public key");
    Ok(key)
}

// ---------------------------------------------------------------------------
// Auth check — the main entry point called from server.rs
// ---------------------------------------------------------------------------

/// Perform authentication only — verify that the token is a valid JWT signed
/// by the configured public key.  Used for endpoints (like `/poll`) that don't
/// carry a protocol envelope and therefore cannot do prefix-based authorization.
///
/// Returns `Ok(())` if the token is valid.
/// Returns `Err(())` if the token is missing, empty, or fails verification.
#[allow(clippy::result_unit_err)]
pub fn auth_check_token(auth: &AuthConfig, token: Option<&str>) -> Result<(), ()> {
    match token {
        Some(t) if !t.is_empty() => verify_jwt(auth, t).map(|_| ()).map_err(|_| ()),
        _ => Err(()),
    }
}

/// Perform authentication and prefix-based authorization for a request.
///
/// Returns `Ok(())` if the request is allowed.
/// Returns `Err(ResponseEnvelope)` with the appropriate error status (401 / 403 / 501).
pub fn auth_check(auth: &AuthConfig, req: &RequestEnvelope) -> Result<(), Box<ResponseEnvelope>> {
    let kind = req.kind.as_str();
    let kind_str = req.kind.clone();
    let corr_id = req.head.corr_id.clone();

    // --- Authentication ---
    let token = match &req.head.auth {
        Some(t) => t,
        None => {
            tracing::warn!(kind = %kind, "Auth rejected: no token provided");
            return Err(Box::new(ResponseEnvelope::error(
                kind_str,
                corr_id,
                401,
                "Unauthorized",
            )));
        }
    };

    let claims = match verify_jwt(auth, token) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(kind = %kind, error = %e, "Auth rejected: token verification failed");
            return Err(Box::new(ResponseEnvelope::error(
                kind_str,
                corr_id,
                401,
                "Unauthorized",
            )));
        }
    };

    // --- Authorization ---

    // Admin role bypasses all prefix checks (case-insensitive).
    if let Some(role) = &claims.role {
        if role.to_lowercase() == "admin" {
            return Ok(());
        }
    }

    tracing::debug!(kind = %kind, role = ?claims.role, "Auth verified successfully");

    // Non-admin: evaluate prefix claim.
    match &claims.prefix {
        // Absent or null → always forbidden
        None => {
            tracing::warn!(kind = %kind, "Auth forbidden: no prefix claim in token");
            Err(Box::new(ResponseEnvelope::error(
                kind_str,
                corr_id,
                403,
                "Forbidden",
            )))
        }
        Some(Value::Null) => {
            tracing::warn!(kind = %kind, "Auth forbidden: null prefix claim");
            Err(Box::new(ResponseEnvelope::error(
                kind_str,
                corr_id,
                403,
                "Forbidden",
            )))
        }

        Some(Value::String(prefix)) => {
            // Empty string → wildcard, access to all resources
            if prefix.is_empty() {
                return Ok(());
            }

            // task.heartbeat carries multiple task IDs; all must match the prefix.
            if kind == "task.heartbeat" {
                if let Some(Value::Array(tasks)) = req.data.get("tasks") {
                    for task in tasks {
                        if let Some(task_id) = task.get("id").and_then(|v| v.as_str()) {
                            if !task_id.starts_with(prefix.as_str()) {
                                tracing::warn!(
                                    kind = %kind,
                                    prefix = %prefix,
                                    task_id = %task_id,
                                    "Auth forbidden: heartbeat task ID does not match prefix"
                                );
                                return Err(Box::new(ResponseEnvelope::error(
                                    kind_str,
                                    corr_id,
                                    403,
                                    "Forbidden",
                                )));
                            }
                        }
                    }
                }
                return Ok(());
            }

            // All other operations: check the single resource ID.
            match extract_resource_id(kind, &req.data) {
                // No resource ID for this operation → allow
                None => Ok(()),
                Some(resource_id) => {
                    if resource_id.starts_with(prefix.as_str()) {
                        return Ok(());
                    }
                    tracing::warn!(
                        kind = %kind,
                        prefix = %prefix,
                        resource_id = %resource_id,
                        "Auth forbidden: resource ID does not match prefix"
                    );
                    Err(Box::new(ResponseEnvelope::error(
                        kind_str,
                        corr_id,
                        403,
                        "Forbidden",
                    )))
                }
            }
        }

        // prefix is present but is not a string or null → forbidden
        Some(_) => {
            tracing::warn!(kind = %kind, "Auth forbidden: prefix claim has unexpected type");
            Err(Box::new(ResponseEnvelope::error(
                kind_str,
                corr_id,
                403,
                "Forbidden",
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// JWT verification
// ---------------------------------------------------------------------------

fn verify_jwt(auth: &AuthConfig, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let (validation, decoding_key) = match &auth.key {
        Some(vk) => {
            // Signed mode: read header alg, verify it's in the accepted set
            let header = jsonwebtoken::decode_header(token)?;
            if !vk.algorithms.contains(&header.alg) {
                return Err(jsonwebtoken::errors::Error::from(
                    jsonwebtoken::errors::ErrorKind::InvalidAlgorithm,
                ));
            }
            (Validation::new(header.alg), &vk.decoding_key)
        }
        None => {
            // Unsigned mode: accept alg=none only
            let mut v = Validation::default();
            v.algorithms = vec![Algorithm::HS256]; // placeholder, overridden below
            v.insecure_disable_signature_validation();
            (v, &DecodingKey::from_secret(&[]))
        }
    };

    let mut validation = validation;
    validation.validate_exp = true;
    let mut required = HashSet::new();
    required.insert("exp".to_string());

    if let Some(iss) = &auth.iss {
        required.insert("iss".to_string());
        validation.set_issuer(&[iss]);
    }

    if let Some(aud) = &auth.aud {
        required.insert("aud".to_string());
        validation.set_audience(&[aud]);
    } else {
        validation.validate_aud = false;
    }

    validation.required_spec_claims = required;

    let token_data = decode::<Claims>(token, decoding_key, &validation)?;
    Ok(token_data.claims)
}

// ---------------------------------------------------------------------------
// Resource ID extraction
// ---------------------------------------------------------------------------

/// Extract the resource ID from request data based on operation kind.
/// Returns `None` for operations that don't require a prefix check (e.g. heartbeat).
fn extract_resource_id(kind: &str, data: &Value) -> Option<String> {
    match kind {
        // Search operations: no single resource ID to check, so deny for
        // prefix-restricted tokens by returning an empty string (which can
        // never satisfy a non-empty prefix check).
        "promise.search" | "task.search" | "schedule.search" => Some(String::new()),

        // Operations whose resource ID is data.id
        "promise.get" | "promise.create" | "promise.settle" | "task.get" | "task.acquire"
        | "task.release" | "task.fulfill" | "task.suspend" | "task.fence" | "task.halt"
        | "task.continue" | "schedule.get" | "schedule.delete" => {
            data.get("id").and_then(|v| v.as_str()).map(str::to_owned)
        }

        // Operations whose resource ID is data.awaited
        "promise.register_callback" | "promise.register_listener" => data
            .get("awaited")
            .and_then(|v| v.as_str())
            .map(str::to_owned),

        // Operations whose resource ID is data.promiseId
        "schedule.create" => data
            .get("promiseId")
            .and_then(|v| v.as_str())
            .map(str::to_owned),

        // task.create: data.action.data.id
        "task.create" => data
            .get("action")
            .and_then(|a| a.get("data"))
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_owned),

        // task.heartbeat: no prefix check needed
        "task.heartbeat" => None,

        // Unknown commands: fail-closed by requiring a prefix check against an
        // empty string, which will always fail unless the token has no prefix
        // restriction.  This ensures newly added commands are denied by default
        // until explicitly handled here.
        _ => Some(String::new()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{RequestHead, SUPPORTED_VERSIONS};
    use serde_json::json;

    // ---- fixtures ----
    //
    // Real PEMs, so `parse_public_key` is exercised against actual key material
    // rather than a mock. Public halves only; nothing here signs anything.

    const RSA_PUB: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAlUArNDiQ0RNQXKwvt1ym
3fYcs5i3KokB3r9o10npCwg9YpxXAPY6F5hcInjnMa66QwABLolGwxFqgwWfBP7M
pnc5l6Bxe12a1fAEILP67JyAgSPjwc+JK8hMriGAHXzvQY96zdClYI29jAN1tqd+
yZwnpDieiPzwd3ozo92vlOmWszArAfXX4SX/6qoL8tFQ8yMPzl9MOJUZ7bPC4nzP
uq1OOjSRlc5+Ow4s6OzgmtV/IaC8o4EczxmSxGnyhOLHhcIWtIgIXKP+xmu3zEa0
m44CCc+We6waGDfl6Em5EIJ+ZJGWp6D+Qi8igv1VkDP+z4HVlLnsd0panmR3hhma
lQIDAQAB
-----END PUBLIC KEY-----
";

    const EC_PUB: &str = "-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEfYd1fWn52AosgLoTYBGIjVp7oYI9
BoCO0PeQ7/cdFFt8LaSsVq772Nnnf13OwGPCmhYf+r7snga8m4/7hWoI/Q==
-----END PUBLIC KEY-----
";

    const ED_PUB: &str = "-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAfIpXGkRe22OZf2JSPw1Beu3xuhdwaxT5+njiaBW9eGI=
-----END PUBLIC KEY-----
";

    // ---- helpers ----
    //
    // Per the testability rule: helpers fail the test directly, they never hand
    // a Result back to the caller.

    /// Seconds since epoch, offset by `delta`.
    fn epoch(delta: i64) -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after 1970")
            .as_secs() as i64;
        now + delta
    }

    /// Mint a token carrying `claims`.
    ///
    /// Signed with HS256 and a throwaway secret. Every test here runs in
    /// unsigned mode (`key: None`), where signature and algorithm checks are
    /// disabled, so the signature is irrelevant — what matters is that the
    /// claims round-trip through a real JWT encoder.
    fn token(claims: serde_json::Value) -> String {
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(b"test-secret"),
        )
        .expect("claims encode")
    }

    /// A token that is valid apart from whatever the caller overrides.
    fn valid_token(extra: serde_json::Value) -> String {
        let mut claims = json!({ "exp": epoch(3600) });
        for (k, v) in extra.as_object().expect("object of claims") {
            claims[k] = v.clone();
        }
        token(claims)
    }

    fn unsigned_auth() -> AuthConfig {
        AuthConfig {
            key: None,
            iss: None,
            aud: None,
        }
    }

    fn request(kind: &str, data: serde_json::Value, auth: Option<String>) -> RequestEnvelope {
        RequestEnvelope {
            kind: kind.to_string(),
            head: RequestHead {
                corr_id: "test".to_string(),
                version: SUPPORTED_VERSIONS[0].to_string(),
                auth,
                debug_time: None,
            },
            data,
        }
    }

    /// Run `auth_check` and return the status: 200 for allowed, else the error status.
    fn check(auth: &AuthConfig, req: &RequestEnvelope) -> i32 {
        match auth_check(auth, req) {
            Ok(()) => 200,
            Err(resp) => resp.head.status,
        }
    }

    /// Status of `kind`/`data` when presented with a token carrying `prefix`.
    fn check_prefix(kind: &str, data: serde_json::Value, prefix: &str) -> i32 {
        let req = request(kind, data, Some(valid_token(json!({ "prefix": prefix }))));
        check(&unsigned_auth(), &req)
    }

    // ---- key parsing ----

    #[test]
    fn parses_an_rsa_public_key_and_accepts_the_rsa_algorithms() {
        let (key, key_type) = parse_public_key(RSA_PUB.as_bytes()).expect("valid RSA PEM");
        assert_eq!(key_type, KeyType::Rsa);
        assert_eq!(
            key.algorithms,
            vec![
                Algorithm::RS256,
                Algorithm::RS384,
                Algorithm::RS512,
                Algorithm::PS256,
                Algorithm::PS384,
                Algorithm::PS512,
            ]
        );
    }

    #[test]
    fn parses_an_ec_public_key_and_accepts_only_the_ec_algorithms() {
        let (key, key_type) = parse_public_key(EC_PUB.as_bytes()).expect("valid EC PEM");
        assert_eq!(key_type, KeyType::Ec);
        assert_eq!(key.algorithms, vec![Algorithm::ES256, Algorithm::ES384]);
        assert!(
            !key.algorithms.contains(&Algorithm::RS256),
            "an EC key must never be usable to verify an RSA-signed token"
        );
    }

    #[test]
    fn parses_an_ed25519_public_key_and_accepts_only_eddsa() {
        let (key, key_type) = parse_public_key(ED_PUB.as_bytes()).expect("valid Ed25519 PEM");
        assert_eq!(key_type, KeyType::Ed25519);
        assert_eq!(key.algorithms, vec![Algorithm::EdDSA]);
    }

    #[test]
    fn rejects_a_pem_that_is_not_a_supported_public_key() {
        for bad in [
            &b""[..],
            &b"not a pem at all"[..],
            b"-----BEGIN PUBLIC KEY-----\nZ m 9 v\n-----END PUBLIC KEY-----\n",
            // A certificate, not a bare public key.
            b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
        ] {
            let err = parse_public_key(bad).expect_err("must not be accepted");
            assert!(err.contains("Unsupported or invalid"), "{err}");
        }
    }

    #[test]
    fn load_public_key_reports_a_missing_file_distinctly_from_a_bad_key() {
        let err = load_public_key("/nonexistent/path/to/key.pem").expect_err("no such file");
        assert!(err.contains("Failed to read public key file"), "{err}");
    }

    // ---- authentication ----

    #[test]
    fn a_request_without_a_token_is_unauthorized() {
        let req = request("promise.get", json!({ "id": "foo" }), None);
        assert_eq!(check(&unsigned_auth(), &req), 401);
    }

    #[test]
    fn a_malformed_token_is_unauthorized() {
        for bad in ["", "not-a-jwt", "a.b.c", "Bearer x.y.z"] {
            let req = request("promise.get", json!({ "id": "foo" }), Some(bad.to_string()));
            assert_eq!(check(&unsigned_auth(), &req), 401, "for {bad:?}");
        }
    }

    #[test]
    fn an_expired_token_is_unauthorized() {
        let req = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(token(json!({ "exp": epoch(-3600), "prefix": "" }))),
        );
        assert_eq!(check(&unsigned_auth(), &req), 401);
    }

    #[test]
    fn a_token_without_an_exp_claim_is_unauthorized() {
        // `exp` is in required_spec_claims: a token that never expires is not
        // acceptable even if everything else about it is fine.
        let req = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(token(json!({ "prefix": "" }))),
        );
        assert_eq!(check(&unsigned_auth(), &req), 401);
    }

    #[test]
    fn issuer_is_enforced_only_when_configured() {
        let auth = AuthConfig {
            key: None,
            iss: Some("https://issuer.example".to_string()),
            aud: None,
        };

        let matching = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(
                json!({ "prefix": "", "iss": "https://issuer.example" }),
            )),
        );
        assert_eq!(check(&auth, &matching), 200);

        let wrong = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(
                json!({ "prefix": "", "iss": "https://evil.example" }),
            )),
        );
        assert_eq!(check(&auth, &wrong), 401);

        let missing = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(json!({ "prefix": "" }))),
        );
        assert_eq!(
            check(&auth, &missing),
            401,
            "iss is required once configured"
        );

        // With no issuer configured, the same tokens are all fine.
        assert_eq!(check(&unsigned_auth(), &wrong), 200);
    }

    #[test]
    fn audience_is_enforced_only_when_configured() {
        let auth = AuthConfig {
            key: None,
            iss: None,
            aud: Some("resonate".to_string()),
        };

        let matching = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(json!({ "prefix": "", "aud": ["resonate"] }))),
        );
        assert_eq!(check(&auth, &matching), 200);

        let wrong = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(json!({ "prefix": "", "aud": ["other"] }))),
        );
        assert_eq!(check(&auth, &wrong), 401);

        assert_eq!(
            check(&unsigned_auth(), &wrong),
            200,
            "unset means unchecked"
        );
    }

    #[test]
    fn auth_check_token_accepts_a_valid_token_and_rejects_everything_else() {
        let auth = unsigned_auth();
        let good = valid_token(json!({ "prefix": "" }));
        assert!(auth_check_token(&auth, Some(&good)).is_ok());

        assert!(auth_check_token(&auth, None).is_err(), "missing");
        assert!(auth_check_token(&auth, Some("")).is_err(), "empty");
        assert!(auth_check_token(&auth, Some("garbage")).is_err(), "garbage");

        let expired = token(json!({ "exp": epoch(-3600) }));
        assert!(auth_check_token(&auth, Some(&expired)).is_err(), "expired");
    }

    #[test]
    fn expiry_is_checked_with_the_default_sixty_second_leeway() {
        // jsonwebtoken's `Validation::leeway` defaults to 60s, so a token that
        // expired a moment ago is still accepted. Pinned because it is a
        // security-relevant default that nothing in this crate sets explicitly.
        let auth = unsigned_auth();

        let just_expired = token(json!({ "exp": epoch(-1) }));
        assert!(
            auth_check_token(&auth, Some(&just_expired)).is_ok(),
            "within the 60s leeway"
        );

        let well_expired = token(json!({ "exp": epoch(-120) }));
        assert!(
            auth_check_token(&auth, Some(&well_expired)).is_err(),
            "past the 60s leeway"
        );
    }

    #[test]
    fn auth_check_token_ignores_authorization_claims() {
        // /poll carries no envelope, so there is nothing to prefix-check. A
        // token with no prefix claim at all must still authenticate.
        let auth = unsigned_auth();
        let no_prefix = valid_token(json!({}));
        assert!(
            auth_check_token(&auth, Some(&no_prefix)).is_ok(),
            "authentication only — a missing prefix is not this function's concern"
        );
    }

    // ---- authorization: role ----

    #[test]
    fn the_admin_role_bypasses_every_prefix_check() {
        for role in ["admin", "ADMIN", "Admin", "aDmIn"] {
            let req = request(
                "promise.get",
                json!({ "id": "anything-at-all" }),
                Some(valid_token(json!({ "role": role }))),
            );
            assert_eq!(
                check(&unsigned_auth(), &req),
                200,
                "role {role:?} is admin, case-insensitively"
            );
        }
    }

    #[test]
    fn a_non_admin_role_does_not_bypass_the_prefix_check() {
        let req = request(
            "promise.get",
            json!({ "id": "anything" }),
            Some(valid_token(json!({ "role": "user" }))),
        );
        assert_eq!(
            check(&unsigned_auth(), &req),
            403,
            "no prefix claim, and 'user' is not admin"
        );
    }

    // ---- authorization: prefix claim shape ----

    #[test]
    fn an_absent_prefix_claim_is_forbidden() {
        let req = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(json!({}))),
        );
        assert_eq!(check(&unsigned_auth(), &req), 403);
    }

    #[test]
    fn a_null_prefix_claim_is_forbidden() {
        let req = request(
            "promise.get",
            json!({ "id": "foo" }),
            Some(valid_token(json!({ "prefix": null }))),
        );
        assert_eq!(check(&unsigned_auth(), &req), 403);
    }

    #[test]
    fn a_prefix_claim_of_the_wrong_type_is_forbidden() {
        for bad in [json!(42), json!(true), json!(["a"]), json!({ "a": 1 })] {
            let req = request(
                "promise.get",
                json!({ "id": "foo" }),
                Some(valid_token(json!({ "prefix": bad }))),
            );
            assert_eq!(check(&unsigned_auth(), &req), 403, "for prefix {bad}");
        }
    }

    #[test]
    fn an_empty_prefix_is_a_wildcard() {
        assert_eq!(
            check_prefix("promise.get", json!({ "id": "anything" }), ""),
            200
        );
        assert_eq!(check_prefix("promise.search", json!({}), ""), 200);
        assert_eq!(
            check_prefix("some.unknown.op", json!({ "id": "x" }), ""),
            200,
            "a wildcard token reaches even operations the table does not know"
        );
    }

    // ---- authorization: the resource-ID table ----
    //
    // One case per branch of `extract_resource_id`. This table is the whole of
    // prefix authorization, so each entry is pinned explicitly rather than
    // inferred from a couple of samples.

    #[test]
    fn operations_keyed_on_data_id_are_checked_against_that_id() {
        for kind in [
            "promise.get",
            "promise.create",
            "promise.settle",
            "task.get",
            "task.acquire",
            "task.release",
            "task.fulfill",
            "task.suspend",
            "task.fence",
            "task.halt",
            "task.continue",
            "schedule.get",
            "schedule.delete",
        ] {
            assert_eq!(
                check_prefix(kind, json!({ "id": "acme/thing" }), "acme/"),
                200,
                "{kind} should allow a matching id"
            );
            assert_eq!(
                check_prefix(kind, json!({ "id": "other/thing" }), "acme/"),
                403,
                "{kind} should reject a non-matching id"
            );
        }
    }

    #[test]
    fn callback_and_listener_registration_are_checked_against_the_awaited_promise() {
        for kind in ["promise.register_callback", "promise.register_listener"] {
            assert_eq!(
                check_prefix(
                    kind,
                    json!({ "awaited": "acme/a", "awaiter": "other/b" }),
                    "acme/"
                ),
                200,
                "{kind} keys on `awaited`"
            );
            assert_eq!(
                check_prefix(
                    kind,
                    json!({ "awaited": "other/a", "awaiter": "acme/b" }),
                    "acme/"
                ),
                403,
                "{kind} must not be satisfied by a matching `awaiter`"
            );
        }
    }

    #[test]
    fn schedule_create_is_checked_against_the_promise_id_it_will_create() {
        assert_eq!(
            check_prefix(
                "schedule.create",
                json!({ "id": "other/sched", "promiseId": "acme/p" }),
                "acme/"
            ),
            200,
            "the resource that matters is the promise it mints"
        );
        assert_eq!(
            check_prefix(
                "schedule.create",
                json!({ "id": "acme/sched", "promiseId": "other/p" }),
                "acme/"
            ),
            403,
            "a matching schedule id must not authorize an out-of-prefix promise"
        );
    }

    #[test]
    fn task_create_is_checked_against_the_nested_action_promise_id() {
        assert_eq!(
            check_prefix(
                "task.create",
                json!({ "action": { "kind": "promise.create", "data": { "id": "acme/p" } } }),
                "acme/"
            ),
            200
        );
        assert_eq!(
            check_prefix(
                "task.create",
                json!({ "action": { "kind": "promise.create", "data": { "id": "other/p" } } }),
                "acme/"
            ),
            403
        );
    }

    #[test]
    fn search_operations_are_denied_to_any_prefix_restricted_token() {
        // A search cannot be scoped to a prefix, so a restricted token must not
        // be able to enumerate.
        for kind in ["promise.search", "task.search", "schedule.search"] {
            assert_eq!(
                check_prefix(kind, json!({}), "acme/"),
                403,
                "{kind} must not leak resources outside the prefix"
            );
        }
    }

    #[test]
    fn heartbeat_requires_every_task_id_to_match_the_prefix() {
        let all_match = json!({
            "pid": "w1",
            "tasks": [{ "id": "acme/t1", "counter": 1 }, { "id": "acme/t2", "counter": 1 }]
        });
        assert_eq!(check_prefix("task.heartbeat", all_match, "acme/"), 200);

        let one_foreign = json!({
            "pid": "w1",
            "tasks": [{ "id": "acme/t1", "counter": 1 }, { "id": "other/t2", "counter": 1 }]
        });
        assert_eq!(
            check_prefix("task.heartbeat", one_foreign, "acme/"),
            403,
            "a single foreign task id poisons the whole batch"
        );

        assert_eq!(
            check_prefix(
                "task.heartbeat",
                json!({ "pid": "w1", "tasks": [] }),
                "acme/"
            ),
            200,
            "an empty batch touches nothing"
        );
    }

    #[test]
    fn unknown_operations_fail_closed() {
        // The contract documented on `extract_resource_id`: an operation added
        // to the dispatcher but not to the table must be denied, not allowed.
        for kind in [
            "promise.destroy",
            "task.teleport",
            "debug.reset",
            "debug.snap",
            "",
        ] {
            assert_eq!(
                check_prefix(kind, json!({ "id": "acme/thing" }), "acme/"),
                403,
                "{kind:?} is not in the table and must fail closed"
            );
        }
    }

    #[test]
    fn a_missing_resource_id_is_allowed_rather_than_crashing() {
        // `extract_resource_id` returns None when the field is absent; the
        // operation itself will then 400. Auth must not turn that into a 403
        // that masks the real error.
        assert_eq!(check_prefix("promise.get", json!({}), "acme/"), 200);
        assert_eq!(check_prefix("task.create", json!({}), "acme/"), 200);
    }

    #[test]
    fn prefix_matching_is_a_string_prefix_not_a_path_segment_match() {
        // Documents today's behaviour precisely: "acme" also authorizes
        // "acmecorp/...". Anything that changes this should fail here first.
        assert_eq!(
            check_prefix("promise.get", json!({ "id": "acmecorp/p" }), "acme"),
            200
        );
        assert_eq!(
            check_prefix("promise.get", json!({ "id": "acme" }), "acme/"),
            403
        );
        assert_eq!(
            check_prefix("promise.get", json!({ "id": "acme/" }), "acme/"),
            200
        );
    }
}
