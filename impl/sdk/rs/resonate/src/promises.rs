use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::PROTOCOL_VERSION;

/// Sub-client for promise operations.
#[derive(Clone)]
pub struct Promises {
    transport: Transport,
}

impl Promises {
    pub fn new(transport: Transport) -> Self {
        Self { transport }
    }

    /// Get a promise by ID.
    pub async fn get(&self, id: &str) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "promise.get",
            "head": {
                "corrId": format!("pg-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "id": id,
            },
        });

        let resp = self.transport.send_json(req).await?;
        check_status(&resp)?;
        let rdata = crate::transport::response_data(&resp)?;
        rdata
            .get("promise")
            .cloned()
            .ok_or_else(|| Error::DecodingError("missing 'promise' field in response".into()))
    }

    /// Create a promise.
    pub async fn create(
        &self,
        id: &str,
        timeout_at: i64,
        param: serde_json::Value,
        tags: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "promise.create",
            "head": {
                "corrId": format!("pc-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "id": id,
                "timeoutAt": timeout_at,
                "param": param,
                "tags": tags,
            },
        });

        let resp = self.transport.send_json(req).await?;
        let rdata = crate::transport::response_data(&resp)?;
        rdata
            .get("promise")
            .cloned()
            .ok_or_else(|| Error::DecodingError("missing 'promise' field in response".into()))
    }

    /// Settle (resolve or reject) a promise.
    pub async fn settle(
        &self,
        id: &str,
        state: &str,
        value: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "promise.settle",
            "head": {
                "corrId": format!("ps-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "id": id,
                "state": state,
                "value": value,
            },
        });

        let resp = self.transport.send_json(req).await?;
        let rdata = crate::transport::response_data(&resp)?;
        rdata
            .get("promise")
            .cloned()
            .ok_or_else(|| Error::DecodingError("missing 'promise' field in response".into()))
    }

    /// Register a listener on a promise.
    pub async fn register_listener(
        &self,
        awaited: &str,
        address: &str,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "promise.register_listener",
            "head": {
                "corrId": format!("prl-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "awaited": awaited,
                "address": address,
            },
        });

        let resp = self.transport.send_json(req).await?;
        let rdata = crate::transport::response_data(&resp)?;
        rdata
            .get("promise")
            .cloned()
            .ok_or_else(|| Error::DecodingError("missing 'promise' field in response".into()))
    }
}

/// Sub-client for schedule operations.
#[derive(Clone)]
pub struct Schedules {
    transport: Transport,
}

impl Schedules {
    pub fn new(transport: Transport) -> Self {
        Self { transport }
    }

    /// Create a schedule.
    pub async fn create(
        &self,
        id: &str,
        cron: &str,
        promise_id: &str,
        promise_timeout: i64,
        promise_param: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "schedule.create",
            "head": {
                "corrId": format!("sc-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "id": id,
                "cron": cron,
                "promiseId": promise_id,
                "promiseTimeout": promise_timeout,
                "promiseParam": promise_param,
                "promiseTags": {},
            },
        });

        let resp = self.transport.send_json(req).await?;
        Ok(resp)
    }

    /// Get a schedule by ID.
    pub async fn get(&self, id: &str) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "schedule.get",
            "head": {
                "corrId": format!("sg-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "id": id,
            },
        });

        let resp = self.transport.send_json(req).await?;
        Ok(resp)
    }

    /// Delete a schedule.
    pub async fn delete(&self, id: &str) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "schedule.delete",
            "head": {
                "corrId": format!("sd-{}", now_ms()),
                "version": PROTOCOL_VERSION,
            },
            "data": {
                "id": id,
            },
        });

        let resp = self.transport.send_json(req).await?;
        Ok(resp)
    }
}

fn check_status(resp: &serde_json::Value) -> Result<()> {
    let status = crate::transport::response_status(resp)?;
    if status >= 400 {
        let rdata = crate::transport::response_data(resp)?;
        let error_msg = rdata
            .as_str()
            .or_else(|| rdata.get("error").and_then(|e| e.as_str()))
            .unwrap_or("unknown error");
        return Err(Error::ServerError {
            code: status as u16,
            message: error_msg.to_string(),
        });
    }
    Ok(())
}

use crate::now_ms;
