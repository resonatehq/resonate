//! WorkOS API key validation — calls GET /organizations with the key as
//! Bearer auth to learn which organization the key belongs to.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Runtime config
// ---------------------------------------------------------------------------

/// Runtime WorkOS auth configuration, loaded at startup.
#[derive(Clone)]
pub struct WorkOsConfig {
    /// If set, the token must be scoped to this organization.
    /// `None` means any valid WorkOS API key is accepted.
    pub org_id: Option<String>,
    /// Base URL for the WorkOS API [default: https://api.workos.com].
    pub base_url: String,
}

// ---------------------------------------------------------------------------
/// Deserializable config, like `resonate_auth::Config`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// If set, the token must be scoped to this organization.
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
    /// Produces the runtime form. Fallible — an empty `api_key` means the
    /// config section was present but incomplete, which is a startup error.
    pub fn load(&self) -> Result<WorkOsConfig, String> {
        Ok(WorkOsConfig {
            org_id: self.org_id.clone(),
            base_url: self.base_url.clone(),
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            org_id: None,
            base_url: default_base_url(),
        }
    }
}

// ---------------------------------------------------------------------------
// WorkOS API response shapes
// ---------------------------------------------------------------------------

/// Paginated list response from GET /organizations.
#[derive(Debug, Deserialize)]
struct OrganizationList {
    data: Vec<Organization>,
}

#[derive(Debug, Deserialize)]
struct Organization {
    id: String,
    #[serde(default)]
    #[allow(dead_code)]
    name: String,
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

    /// Resolve which organization an API key belongs to.
    ///
    /// Calls `GET /organizations` with the API key as Bearer auth and
    /// returns the first organization ID in the response.
    ///
    /// Returns `Ok(Some(org_id))` if the key is valid and has at least one
    /// org; `Ok(None)` if valid but no orgs are accessible; `Err(reason)` if
    /// the key is invalid or the network call failed.
    pub async fn resolve_org(&self, api_key: &str) -> Result<Option<String>, String> {
        let url = format!("{}/organizations", self.config.base_url);

        let resp = self
            .http
            .get(&url)
            .bearer_auth(api_key)
            .send()
            .await
            .map_err(|e| format!("WorkOS request failed: {e}"))?;

        if resp.status().as_u16() == 401 {
            return Err("WorkOS returned 401: invalid API key".into());
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

        let list: OrganizationList = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse WorkOS response: {e}"))?;

        Ok(list.data.into_iter().next().map(|o| o.id))
    }
}

// ---------------------------------------------------------------------------
// Auth check — the entry point called from the HTTP routes
// ---------------------------------------------------------------------------

/// Perform WorkOS authentication for an incoming request.
///
/// 1. Extract bearer token from the `Authorization` header.
/// 2. Call WorkOS to validate the API key and learn the organization.
/// 3. If `config.org_id` is set, require the key's org to match.
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

    let org_id = match client.resolve_org(api_key).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            if client.config.org_id.is_some() {
                tracing::warn!("WorkOS auth rejected: token has no organization");
                return Err(WorkOsRejection {
                    status: 403,
                    message: "Forbidden — token not scoped to an organization".into(),
                });
            }
            return Ok(());
        }
        Err(e) => {
            tracing::warn!(error = %e, "WorkOS auth rejected: key validation failed");
            return Err(WorkOsRejection {
                status: 401,
                message: "Unauthorized".into(),
            });
        }
    };

    if let Some(expected) = &client.config.org_id {
        if org_id != *expected {
            tracing::warn!(
                expected = %expected,
                actual = %org_id,
                "WorkOS auth rejected: organization mismatch"
            );
            return Err(WorkOsRejection {
                status: 403,
                message: "Forbidden — organization mismatch".into(),
            });
        }
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
            org_id: None,
            base_url: "https://api.workos.com".into(),
        }
    }

    fn client(mock_url: &str, org_id: Option<&str>) -> WorkOsClient {
        let mut cfg = config();
        cfg.base_url = mock_url.to_string();
        cfg.org_id = org_id.map(str::to_owned);
        WorkOsClient::new(cfg)
    }

    // ---------------------------------------------------------------
    // Token extraction
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn missing_token_is_rejected() {
        let mock = MockServer::start().await;
        let c = client(&mock.uri(), None);
        let r = auth_check_workos(&c, None).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    #[tokio::test]
    async fn empty_token_is_rejected() {
        let mock = MockServer::start().await;
        let c = client(&mock.uri(), None);
        let r = auth_check_workos(&c, Some("")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    // ---------------------------------------------------------------
    // Successful authentication
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn valid_key_no_org_requirement() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"data": [{"id": "org_abc", "name": "test"}]}),
                ),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), None);
        assert!(auth_check_workos(&c, Some("tok")).await.is_ok());
    }

    #[tokio::test]
    async fn valid_key_matching_required_org() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"data": [{"id": "org_abc", "name": "test"}]}),
                ),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), Some("org_abc"));
        assert!(auth_check_workos(&c, Some("tok")).await.is_ok());
    }

    // ---------------------------------------------------------------
    // Organization-related rejections
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn key_without_org_when_org_is_required() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []})))
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), Some("org_abc"));
        let r = auth_check_workos(&c, Some("tok")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 403);
    }

    #[tokio::test]
    async fn org_mismatch() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"data": [{"id": "org_xyz", "name": "other"}]}),
                ),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), Some("org_abc"));
        let r = auth_check_workos(&c, Some("tok")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 403);
    }

    // ---------------------------------------------------------------
    // WorkOS API errors
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn workos_returns_401() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_json(serde_json::json!({"message": "Invalid API key"})),
            )
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), None);
        let r = auth_check_workos(&c, Some("tok")).await;
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().status, 401);
    }

    // ---------------------------------------------------------------
    // Valid key with no org when none required
    // ---------------------------------------------------------------
    #[tokio::test]
    async fn valid_key_no_org_none_required() {
        let mock = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/organizations"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"data": []})))
            .mount(&mock)
            .await;

        let c = client(&mock.uri(), None);
        assert!(auth_check_workos(&c, Some("tok")).await.is_ok());
    }
}
