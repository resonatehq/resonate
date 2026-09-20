//! WorkOS API key validation — calls `POST /api_keys/validations` with the
//! server's own WorkOS secret key (not the client's API key) to validate
//! an incoming API key and discover which organization it belongs to.
//!
//! # Architecture
//!
//! Two keys, two roles:
//!
//! | Key                | Owner      | Purpose                              |
//! |--------------------|------------|--------------------------------------|
//! | Server secret key  | Application| Authenticate to the WorkOS API       |
//! | Client API key     | End user   | Sent as bearer token by the caller  |
//!
//! The server secret key (`sk_…`) is configured at startup and never leaves
//! the server. The client API key arrives in every request's bearer token and
//! is validated by WorkOS on every request.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Runtime config
// ---------------------------------------------------------------------------

/// Runtime WorkOS auth configuration, loaded at startup.
#[derive(Clone, Debug)]
pub struct WorkOsConfig {
    /// The server's own WorkOS secret key (`sk_…`), used to authenticate
    /// calls to the WorkOS API — specifically `POST /api_keys/validations`.
    pub api_key: String,
    /// The organization ID the client's API key must belong to.
    pub org_id: String,
    /// Base URL for the WorkOS API [default: https://api.workos.com].
    pub base_url: String,
}

// ---------------------------------------------------------------------------
/// Deserializable config, like `resonate_auth::Config`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The server's own WorkOS secret key.
    #[serde(default)]
    pub api_key: Option<String>,

    /// The organization ID every client API key must belong to.
    #[serde(default)]
    pub org_id: Option<String>,

    /// Base URL for the WorkOS API. Defaults to https://api.workos.com.
    #[serde(default = "default_base_url")]
    pub base_url: String,
}

fn default_base_url() -> String {
    "https://api.workos.com".to_string()
}

// ---------------------------------------------------------------------------
// Rejection
// ---------------------------------------------------------------------------

/// The reason a WorkOS authentication check was denied.
#[derive(Debug, Clone)]
pub struct WorkOsRejection {
    pub status: u16,
    pub message: String,
}

impl Config {
    /// Produces the runtime form. Fallible — both `api_key` and `org_id` are
    /// required when WorkOS mode is enabled.
    pub fn load(&self) -> Result<WorkOsConfig, String> {
        let api_key = self
            .api_key
            .clone()
            .ok_or_else(|| "workos.api_key is required when WorkOS auth is enabled".to_string())?;
        let org_id = self
            .org_id
            .clone()
            .ok_or_else(|| "workos.org_id is required when WorkOS auth is enabled".to_string())?;
        Ok(WorkOsConfig {
            api_key,
            org_id,
            base_url: self.base_url.clone(),
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key: None,
            org_id: None,
            base_url: default_base_url(),
        }
    }
}

// ---------------------------------------------------------------------------
// WorkOS API response shapes — POST /api_keys/validations
// ---------------------------------------------------------------------------

/// Top-level response from `POST /api_keys/validations`.
/// On success with a valid key: `api_key` is the key object.
/// On success with an invalid key: `api_key` is `null`.
#[derive(Debug, Deserialize)]
struct ValidateResponse {
    api_key: Option<ApiKeyObject>,
}

#[derive(Debug, Deserialize)]
struct ApiKeyObject {
    #[allow(dead_code)]
    id: String,
    owner: Owner,
}

#[derive(Debug, Deserialize)]
struct Owner {
    /// `"organization"` or `"user"`.
    #[serde(rename = "type")]
    owner_type: String,
    /// For org-owned keys: the organization ID.
    /// For user-owned keys: the user ID.
    id: String,
    /// Present on user-owned keys: the organization the user belongs to.
    #[serde(default)]
    organization_id: Option<String>,
}

/// Error response body from WorkOS.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    #[serde(default)]
    message: String,
}

// ---------------------------------------------------------------------------
// WorkOS client
// ---------------------------------------------------------------------------

/// A client for WorkOS API key validation.
///
/// Stateless: holds the config and an HTTP client, nothing else.
#[derive(Clone)]
pub struct WorkOsClient {
    config: WorkOsConfig,
    http: reqwest::Client,
}

impl WorkOsClient {
    /// Build a client from runtime config at startup.
    pub fn new(config: WorkOsConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("reqwest client");
        Self { config, http }
    }

    /// Validate a client API key and return the organization ID it belongs to.
    ///
    /// Calls `POST /api_keys/validations` with the **server's** secret key as
    /// Bearer auth and the **client's** API key in the request body.
    ///
    /// Returns `Ok(Some(org_id))` if the key is valid and belongs to an
    /// organization; `Ok(None)` if the key is valid but not scoped to an org;
    /// `Err(reason)` if the key is invalid or the network call failed.
    pub async fn validate_key(&self, client_api_key: &str) -> Result<Option<String>, String> {
        let url = format!("{}/api_keys/validations", self.config.base_url);

        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.config.api_key)
            .json(&serde_json::json!({"value": client_api_key}))
            .send()
            .await
            .map_err(|e| format!("WorkOS request failed: {e}"))?;

        if resp.status().as_u16() == 401 {
            return Err("WorkOS returned 401: invalid server API key".into());
        }

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let message = resp
                .json::<ErrorResponse>()
                .await
                .map(|e| e.message)
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(format!("WorkOS returned {status}: {message}"));
        }

        let body: ValidateResponse = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse WorkOS response: {e}"))?;

        let key = match body.api_key {
            Some(k) => k,
            None => return Err("WorkOS: API key is invalid".into()),
        };

        // Extract the organization ID from the owner.
        // - Org-owned keys: owner.type == "organization", owner.id is the org ID.
        // - User-owned keys: owner.type == "user", owner.organization_id is the org.
        let org_id = match key.owner.owner_type.as_str() {
            "organization" => Some(key.owner.id),
            "user" => key.owner.organization_id,
            _ => None,
        };

        Ok(org_id)
    }
}

// ---------------------------------------------------------------------------
// Auth check — the entry point called from the HTTP routes
// ---------------------------------------------------------------------------

/// Perform WorkOS authentication for an incoming request.
///
/// 1. Extract bearer token from the request.
/// 2. Call WorkOS `POST /api_keys/validations` with the server's secret key.
/// 3. Extract the organization from the validated key's owner.
/// 4. Require the key's org to match the configured `org_id`.
///
/// Returns `Ok(())` when the caller is allowed.
/// Returns `Err(WorkOsRejection)` with the HTTP status and message.
pub async fn auth_check_workos(
    client: &WorkOsClient,
    token: Option<&str>,
) -> Result<(), WorkOsRejection> {
    let api_key = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            tracing::warn!("WorkOS auth rejected: no token in request");
            return Err(WorkOsRejection {
                status: 401,
                message: "Unauthorized".into(),
            });
        }
    };

    let org_id = match client.validate_key(api_key).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            tracing::warn!("WorkOS auth rejected: token has no organization");
            return Err(WorkOsRejection {
                status: 403,
                message: "Forbidden — token not scoped to an organization".into(),
            });
        }
        Err(e) => {
            tracing::warn!(error = %e, "WorkOS auth rejected: key validation failed");
            return Err(WorkOsRejection {
                status: 401,
                message: "Unauthorized".into(),
            });
        }
    };

    if org_id != client.config.org_id {
        tracing::warn!(
            expected = %client.config.org_id,
            actual = %org_id,
            "WorkOS auth rejected: organization mismatch"
        );
        return Err(WorkOsRejection {
            status: 403,
            message: "Forbidden — organization mismatch".into(),
        });
    }

    tracing::debug!(org_id = %org_id, "WorkOS auth success");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config() -> WorkOsConfig {
        WorkOsConfig {
            api_key: "sk_server_secret".into(),
            org_id: "org_abc".into(),
            base_url: "https://api.workos.com".into(),
        }
    }

    fn client(mock_url: &str, org_id: &str) -> WorkOsClient {
        let mut cfg = config();
        cfg.base_url = mock_url.to_string();
        cfg.org_id = org_id.to_string();
        WorkOsClient::new(cfg)
    }

    fn org_owned_key_response(org_id: &str) -> serde_json::Value {
        serde_json::json!({
            "api_key": {
                "id": "api_key_123",
                "owner": {
                    "type": "organization",
                    "id": org_id
                }
            }
        })
    }

    fn user_owned_key_response(org_id: &str) -> serde_json::Value {
        serde_json::json!({
            "api_key": {
                "id": "api_key_456",
                "owner": {
                    "type": "user",
                    "id": "user_789",
                    "organization_id": org_id
                }
            }
        })
    }

    fn invalid_key_response() -> serde_json::Value {
        serde_json::json!({"api_key": null})
    }

    // ---------------------------------------------------------------
    // Token extraction
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn missing_token_is_rejected() {
        let mock = MockServer::start().await;
        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, None).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    #[tokio::test]
    async fn empty_token_is_rejected() {
        let mock = MockServer::start().await;
        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, Some("")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    // ---------------------------------------------------------------
    // Successful authentication — org-owned key
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn valid_org_owned_key() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(org_owned_key_response("org_abc")),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        assert!(auth_check_workos(&c, Some("client_key")).await.is_ok());
    }

    // ---------------------------------------------------------------
    // Successful authentication — user-owned key
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn valid_user_owned_key() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(user_owned_key_response("org_abc")),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        assert!(auth_check_workos(&c, Some("client_key")).await.is_ok());
    }

    // ---------------------------------------------------------------
    // Organization mismatch
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn org_mismatch() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(org_owned_key_response("org_xyz")),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, Some("client_key")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 403);
    }

    // ---------------------------------------------------------------
    // User-owned key with mismatched org
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn user_key_org_mismatch() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(user_owned_key_response("org_xyz")),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, Some("client_key")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 403);
    }

    // ---------------------------------------------------------------
    // Invalid client API key
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn invalid_client_key() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(invalid_key_response()))
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, Some("bad_key")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    // ---------------------------------------------------------------
    // Server secret key is wrong → WorkOS returns 401
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn server_key_rejected_by_workos() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_json(serde_json::json!({"message": "Invalid API key"})),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, Some("client_key")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    // ---------------------------------------------------------------
    // Key with no organization
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn key_without_org() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api_keys/validations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "api_key": {
                    "id": "api_key_999",
                    "owner": {
                        "type": "user",
                        "id": "user_000"
                    }
                }
            })))
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), "org_abc");
        let r = auth_check_workos(&c, Some("client_key")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 403);
    }

    // ---------------------------------------------------------------
    // Config::load rejects missing api_key
    // ---------------------------------------------------------------
    #[test]
    fn load_rejects_missing_api_key() {
        let cfg = Config {
            api_key: None,
            org_id: Some("org_abc".into()),
            base_url: default_base_url(),
        };
        let err = cfg.load().unwrap_err();
        assert!(err.contains("api_key"), "{err}");
    }

    // ---------------------------------------------------------------
    // Config::load rejects missing org_id
    // ---------------------------------------------------------------
    #[test]
    fn load_rejects_missing_org_id() {
        let cfg = Config {
            api_key: Some("sk_secret".into()),
            org_id: None,
            base_url: default_base_url(),
        };
        let err = cfg.load().unwrap_err();
        assert!(err.contains("org_id"), "{err}");
    }

    // ---------------------------------------------------------------
    // Config::load succeeds with both fields
    // ---------------------------------------------------------------
    #[test]
    fn load_succeeds_with_both_fields() {
        let cfg = Config {
            api_key: Some("sk_secret".into()),
            org_id: Some("org_abc".into()),
            base_url: default_base_url(),
        };
        let loaded = cfg.load().unwrap();
        assert_eq!(loaded.api_key, "sk_secret");
        assert_eq!(loaded.org_id, "org_abc");
    }
}
