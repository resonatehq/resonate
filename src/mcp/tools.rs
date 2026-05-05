use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};

fn gen_corr_id() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("mcp-{}-{}", d.as_millis(), d.subsec_nanos() % 1_000_000)
}

fn build_envelope(kind: &str, token: Option<&str>, data: Value) -> Value {
    let mut head = json!({ "corrId": gen_corr_id(), "version": "2026-04-01" });
    if let Some(t) = token {
        head["auth"] = json!(t);
    }
    json!({ "kind": kind, "head": head, "data": data })
}

async fn post(server: &str, token: Option<&str>, kind: &str, data: Value) -> Result<Value, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/", server.trim_end_matches('/'));
    let body = build_envelope(kind, token, data);
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Connection error: {}", e))?;
    let envelope: Value = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;
    let status = envelope["head"]["status"].as_i64().unwrap_or(0);
    if (200..300).contains(&status) {
        Ok(envelope["data"].clone())
    } else {
        Err(format!("{}", envelope["data"]))
    }
}

fn b64_encode_data_field(v: &mut Value) {
    if let Some(data) = v.get("data") {
        let raw = match data {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        v["data"] = Value::String(STANDARD.encode(raw));
    }
}

pub async fn promise_create(
    server: &str,
    token: Option<&str>,
    id: String,
    timeout_at: i64,
    param: Option<Value>,
    tags: Option<Value>,
) -> Result<Value, String> {
    let mut data = json!({ "id": id, "timeoutAt": timeout_at });
    if let Some(mut p) = param {
        b64_encode_data_field(&mut p);
        data["param"] = p;
    }
    if let Some(t) = tags {
        data["tags"] = t;
    }
    post(server, token, "promise.create", data).await
}

pub async fn promise_get(server: &str, token: Option<&str>, id: String) -> Result<Value, String> {
    post(server, token, "promise.get", json!({ "id": id })).await
}

pub async fn promise_settle(
    server: &str,
    token: Option<&str>,
    id: String,
    resolution: String,
    value: Option<Value>,
) -> Result<Value, String> {
    let state_str = match resolution.as_str() {
        "resolve" => "resolved",
        "reject" => "rejected",
        "cancel" => "rejected_canceled",
        other => {
            return Err(format!(
                "invalid resolution '{}'; must be resolve, reject, or cancel",
                other
            ))
        }
    };
    let mut data = json!({ "id": id, "state": state_str });
    if let Some(mut v) = value {
        b64_encode_data_field(&mut v);
        data["value"] = v;
    }
    post(server, token, "promise.settle", data).await
}

pub async fn promise_search(
    server: &str,
    token: Option<&str>,
    state_filter: Option<String>,
    tags: Option<Value>,
    limit: Option<i64>,
    cursor: Option<String>,
) -> Result<Value, String> {
    let mut data = json!({});
    if let Some(s) = state_filter {
        data["state"] = json!(s);
    }
    if let Some(t) = tags {
        data["tags"] = t;
    }
    if let Some(l) = limit {
        data["limit"] = json!(l);
    }
    if let Some(c) = cursor {
        data["cursor"] = json!(c);
    }
    post(server, token, "promise.search", data).await
}

pub async fn promise_listen(
    server: &str,
    token: Option<&str>,
    session_id: &str,
    id: String,
) -> Result<Value, String> {
    let address = format!("poll://uni@mcp/{}", session_id);
    post(
        server,
        token,
        "promise.register_listener",
        json!({
            "awaited": id,
            "address": address
        }),
    )
    .await
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub struct BashParams {
    pub id: Option<String>,
    pub timeout_ms: Option<u64>,
    pub script: String,
    /// Optional address override. Defaults to "bash://" (local).
    /// Examples: "bash://docker/ubuntu:latest", "bash://tensorlake/python-3.11".
    pub target: Option<String>,
    pub tags: Option<Value>,
}

pub async fn resonate_bash(
    server: &str,
    token: Option<&str>,
    session_id: &str,
    p: BashParams,
) -> Result<Value, String> {
    let BashParams {
        id,
        timeout_ms,
        script,
        target,
        tags,
    } = p;
    if script.is_empty() {
        return Err("`script` must not be empty".into());
    }

    let promise_id = id.unwrap_or_else(|| {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        format!("bash-{}-{}", d.as_millis(), d.subsec_nanos() % 1_000_000)
    });

    let timeout_at = now_ms() + timeout_ms.unwrap_or(5 * 60 * 1000) as i64;

    let target_address = target.unwrap_or_else(|| "bash://".to_string());
    let param_data = STANDARD.encode(script.as_bytes());

    let mut merged_tags = match tags {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    merged_tags.insert(
        "resonate:target".to_string(),
        Value::String(target_address.clone()),
    );

    let create_data = json!({
        "id": promise_id,
        "timeoutAt": timeout_at,
        "param": { "data": param_data },
        "tags": Value::Object(merged_tags),
    });

    let create_resp = post(server, token, "promise.create", create_data).await?;

    let promise = create_resp.get("promise").cloned().unwrap_or(Value::Null);
    let promise_state = promise["state"].as_str().unwrap_or("").to_string();

    let mut listener_registered = false;
    if promise_state == "pending" {
        let address = format!("poll://uni@mcp/{}", session_id);
        match post(
            server,
            token,
            "promise.register_listener",
            json!({
                "awaited": promise_id,
                "address": address,
            }),
        )
        .await
        {
            Ok(_) => listener_registered = true,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to register listener for resonate-bash promise")
            }
        }
    }

    Ok(json!({
        "promise": promise,
        "listener_registered": listener_registered,
        "target": target_address,
    }))
}
