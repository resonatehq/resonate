//! Authentication and authorization for the Resonate protocol.
//!
//! Envelope-based rather than transport-based: [`auth_check`] reads
//! `head.auth`, so an HTTP gateway, a NATS gateway and an in-process caller
//! all authorize identically. What it needs beyond the envelope is an
//! [`AuthConfig`] — policy, not protocol, which is why this is its own crate
//! rather than part of `core`.

use std::collections::HashSet;

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
pub struct Config {
    /// Public key for JWT verification.
    /// Set to "none" to accept unsigned tokens (debug/testing).
    /// Set to a file path to verify signatures against a PEM key.
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

        // Operations whose resource ID is data.promiseId.
        //
        // A stream is checked against the promise producing it, not the origin
        // it flows back to: writing to a stream is writing on behalf of that
        // promise, so it is the same authority `promise.settle` demands over
        // `data.id`. A token scoped to a lineage covers both, since the origin
        // is a prefix of every promise in it.
        "schedule.create" | "stream.bos" | "stream.put" | "stream.eos" => data
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
