//! Authentication and authorization for the Resonate protocol.
//!
//! Envelope-based rather than transport-based: [`auth_check`] reads
//! `head.auth`, so an HTTP gateway, a NATS gateway and an in-process caller
//! all authorize identically. What it needs beyond the envelope is an
//! [`AuthConfig`] — policy, not protocol, which is why this is its own crate
//! rather than part of `core`.
//!
//! Which policy is [`AuthMode`]: local JWT verification, or WorkOS. A gateway
//! picks one at startup and every endpoint behind it asks the same one.

pub mod workos;

use std::collections::HashSet;
use std::sync::Arc;

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use resonate_core::types::{RequestEnvelope, ResponseEnvelope};

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

/// Which authentication mode is active.
///
/// Exactly one — the gateway picks at startup based on config, and the two
/// are mutually exclusive there.
#[derive(Clone)]
pub enum AuthMode {
    /// Local JWT verification against a public key.
    Jwt(Arc<AuthConfig>),
    /// Remote token validation via the WorkOS API.
    WorkOs(workos::WorkOsClient),
}

impl AuthMode {
    /// Authenticate and authorize an envelope-bearing request.
    ///
    /// The caller gets back a ready-to-render error envelope on failure.
    ///
    /// The two arms do different amounts. JWT mode authenticates and then
    /// applies the prefix rule; a WorkOS key carries no prefix claim and no
    /// role — validation answers only which organization it belongs to — so
    /// that arm authenticates and matches the organization, and there is
    /// nothing further for it to authorize against.
    pub async fn check_envelope(&self, req: &RequestEnvelope) -> Result<(), Box<ResponseEnvelope>> {
        match self {
            AuthMode::Jwt(cfg) => auth_check(cfg, req),
            AuthMode::WorkOs(client) => {
                let token = req.head.auth.as_deref();
                match workos::auth_check_workos(client, token).await {
                    Ok(()) => Ok(()),
                    Err(rejection) => Err(Box::new(ResponseEnvelope::error(
                        req.kind.clone(),
                        req.head.corr_id.clone(),
                        rejection.status as i32,
                        &rejection.message,
                    ))),
                }
            }
        }
    }

    /// Verify a bearer token — no envelope, no authorization, just
    /// authentication. For endpoints like `/poll` that carry no protocol
    /// envelope and so have no resource to authorize against.
    pub async fn check_token(&self, token: Option<&str>) -> bool {
        match self {
            AuthMode::Jwt(cfg) => auth_check_token(cfg, token),
            AuthMode::WorkOs(client) => workos::auth_check_workos(client, token).await.is_ok(),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Auth as it appears in a config file.
///
/// Plain data, like every transport's `Config`: it deserializes and nothing
/// more. The key material it names is read by [`Config::load`], which is what
/// the gateway hosting it calls from `init` — where anything that touches the
/// filesystem and can fail belongs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Public key for JWT verification.
    /// Set to "none" to accept unsigned tokens (debug/testing).
    /// Set to a file path to verify signatures against a PEM key.
    ///
    /// Defaulted, like every other field in every plugin's config, so that
    /// naming an `auth` section without it is this crate's error to report
    /// rather than serde's. An absent key is not a usable policy — `load`
    /// says so, naming the setting an operator has to add.
    #[serde(default)]
    pub publickey: String,

    /// Expected issuer (`iss` claim).
    #[serde(default)]
    pub iss: Option<String>,

    /// Expected audience (`aud` claim).
    #[serde(default)]
    pub aud: Option<String>,
}

impl Config {
    /// Read the key material and produce the runtime form.
    ///
    /// Fallible and touches the disk, so a caller runs it at startup: a bad key
    /// path should stop the process, not surface later as a request that cannot
    /// be authenticated.
    pub fn load(&self) -> Result<AuthConfig, String> {
        if self.publickey.is_empty() {
            // Reachable by writing `iss` and forgetting `publickey`. Auth is on
            // — a section exists — and there is nothing to verify against, so
            // this is a startup failure rather than a policy that admits
            // everything.
            return Err(
                "publickey is required when an auth section is present: a path to a PEM \
                 file, or \"none\" to accept unsigned tokens"
                    .to_string(),
            );
        }
        let key = if self.publickey == "none" {
            tracing::warn!("Auth enabled — unsigned mode (no signature verification)");
            None
        } else {
            let vk = load_public_key(&self.publickey)?;
            tracing::info!(key = %self.publickey, "Auth enabled");
            Some(vk)
        };
        if let Some(iss) = &self.iss {
            tracing::info!(issuer = %iss, "Auth issuer configured");
        }
        if let Some(aud) = &self.aud {
            tracing::info!(audience = %aud, "Auth audience configured");
        }
        Ok(AuthConfig {
            key,
            iss: self.iss.clone(),
            aud: self.aud.clone(),
        })
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

/// Load a public key PEM file, auto-detecting the key type and accepted algorithms.
pub fn load_public_key(path: &str) -> Result<VerificationKey, String> {
    let pem = std::fs::read(path)
        .map_err(|e| format!("Failed to read public key file '{}': {}", path, e))?;

    if let Ok(key) = DecodingKey::from_rsa_pem(&pem) {
        tracing::info!(path = %path, key_type = "RSA", "Loaded public key");
        return Ok(VerificationKey {
            decoding_key: key,
            algorithms: vec![
                Algorithm::RS256,
                Algorithm::RS384,
                Algorithm::RS512,
                Algorithm::PS256,
                Algorithm::PS384,
                Algorithm::PS512,
            ],
        });
    }

    if let Ok(key) = DecodingKey::from_ec_pem(&pem) {
        tracing::info!(path = %path, key_type = "EC", "Loaded public key");
        return Ok(VerificationKey {
            decoding_key: key,
            algorithms: vec![Algorithm::ES256, Algorithm::ES384],
        });
    }

    if let Ok(key) = DecodingKey::from_ed_pem(&pem) {
        tracing::info!(path = %path, key_type = "Ed25519", "Loaded public key");
        return Ok(VerificationKey {
            decoding_key: key,
            algorithms: vec![Algorithm::EdDSA],
        });
    }

    Err(format!(
        "Unsupported or invalid public key in '{}'. \
         Supported types: RSA, EC (P-256/P-384), Ed25519.",
        path
    ))
}

// ---------------------------------------------------------------------------
// Auth check — the main entry point called from server.rs
// ---------------------------------------------------------------------------

/// Perform authentication only — verify that the token is a valid JWT signed
/// by the configured public key.  Used for endpoints (like `/poll`) that don't
/// carry a protocol envelope and therefore cannot do prefix-based authorization.
///
/// Returns `true` if the token is valid.
/// Returns `false` if the token is missing, empty, or fails verification.
pub fn auth_check_token(auth: &AuthConfig, token: Option<&str>) -> bool {
    match token {
        Some(t) if !t.is_empty() => verify_jwt(auth, t).is_ok(),
        _ => false,
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
        // Unsigned mode: no signature check, the same claim checks.
        None => return verify_unsigned(auth, token),
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

/// Unsigned mode (debug/testing): the claims without a signature check.
/// jsonwebtoken 10 builds a verifier from the key even when signature
/// validation is off, so this reads them with `insecure_decode` and applies
/// the signed path's checks by hand: `exp` required, a number, and not past
/// (60 s leeway, jsonwebtoken's default); `iss` and `aud` when configured.
fn verify_unsigned(auth: &AuthConfig, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    use jsonwebtoken::errors::{Error, ErrorKind};
    const LEEWAY: u64 = 60;

    let raw = jsonwebtoken::dangerous::insecure_decode::<Value>(token)?.claims;
    let exp = raw
        .get("exp")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::from(ErrorKind::MissingRequiredClaim("exp".to_string())))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    if exp.saturating_add(LEEWAY) < now {
        return Err(ErrorKind::ExpiredSignature.into());
    }
    if let Some(iss) = &auth.iss {
        match raw.get("iss") {
            None => return Err(ErrorKind::MissingRequiredClaim("iss".to_string()).into()),
            Some(v) if v.as_str() != Some(iss.as_str()) => {
                return Err(ErrorKind::InvalidIssuer.into());
            }
            Some(_) => {}
        }
    }
    if let Some(aud) = &auth.aud {
        let ok = match raw.get("aud") {
            None => return Err(ErrorKind::MissingRequiredClaim("aud".to_string()).into()),
            Some(Value::String(a)) => a == aud,
            Some(Value::Array(auds)) => auds.iter().any(|a| a.as_str() == Some(aud.as_str())),
            Some(_) => false,
        };
        if !ok {
            return Err(ErrorKind::InvalidAudience.into());
        }
    }
    serde_json::from_value(raw).map_err(|e| ErrorKind::Json(Arc::new(e)).into())
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

#[cfg(test)]
mod unsigned_tests {
    use super::*;

    fn b64(bytes: &[u8]) -> String {
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = chunk
                .iter()
                .enumerate()
                .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
            for i in 0..=chunk.len() {
                out.push(A[((n >> (18 - 6 * i)) & 63) as usize] as char);
            }
        }
        out
    }

    fn token(alg: &str, claims: Value) -> String {
        let header = serde_json::json!({ "alg": alg, "typ": "JWT" });
        format!(
            "{}.{}.c2ln",
            b64(header.to_string().as_bytes()),
            b64(claims.to_string().as_bytes())
        )
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn unsigned(iss: Option<&str>, aud: Option<&str>) -> AuthConfig {
        AuthConfig {
            key: None,
            iss: iss.map(str::to_owned),
            aud: aud.map(str::to_owned),
        }
    }

    #[test]
    fn unsigned_mode_accepts_any_alg_with_a_live_exp() {
        let auth = unsigned(None, None);
        for alg in ["HS256", "RS256", "EdDSA"] {
            let t = token(
                alg,
                serde_json::json!({ "exp": now() + 60, "role": "admin" }),
            );
            assert_eq!(
                verify_jwt(&auth, &t).expect(alg).role.as_deref(),
                Some("admin")
            );
        }
    }

    #[test]
    fn unsigned_mode_checks_exp() {
        let auth = unsigned(None, None);
        assert!(verify_jwt(&auth, &token("HS256", serde_json::json!({}))).is_err());
        let stale = token("HS256", serde_json::json!({ "exp": now() - 3600 }));
        assert!(verify_jwt(&auth, &stale).is_err());
        // GHSA-h395-gr6q-cpjc: a mistyped exp is an error, not an absent claim.
        let mistyped = token("HS256", serde_json::json!({ "exp": "9999999999" }));
        assert!(verify_jwt(&auth, &mistyped).is_err());
    }

    #[test]
    fn unsigned_mode_checks_iss_and_aud() {
        let auth = unsigned(Some("https://issuer"), Some("resonate"));
        let exp = now() + 60;
        let ok = token(
            "HS256",
            serde_json::json!({ "exp": exp, "iss": "https://issuer", "aud": ["resonate"] }),
        );
        assert!(verify_jwt(&auth, &ok).is_ok());
        let wrong_iss = token(
            "HS256",
            serde_json::json!({ "exp": exp, "iss": "https://other", "aud": ["resonate"] }),
        );
        assert!(verify_jwt(&auth, &wrong_iss).is_err());
        let wrong_aud = token(
            "HS256",
            serde_json::json!({ "exp": exp, "iss": "https://issuer", "aud": ["other"] }),
        );
        assert!(verify_jwt(&auth, &wrong_aud).is_err());
        let no_aud = token(
            "HS256",
            serde_json::json!({ "exp": exp, "iss": "https://issuer" }),
        );
        assert!(verify_jwt(&auth, &no_aud).is_err());
    }
}
