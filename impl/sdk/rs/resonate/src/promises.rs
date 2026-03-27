use crate::error::{Error, Result};
use crate::transport::Transport;

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
            "corrId": format!("pg-{}", now_ms()),
            "id": id,
        });

        let resp = self.transport.send(req).await?;
        check_status(&resp)?;
        let rdata = crate::transport::response_data(&resp)?;
        Ok(rdata.get("promise").cloned().unwrap_or_default())
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
            "corrId": format!("pc-{}", now_ms()),
            "promise": {
                "id": id,
                "timeoutAt": timeout_at,
                "param": param,
                "tags": tags,
            },
        });

        let resp = self.transport.send(req).await?;
        let rdata = crate::transport::response_data(&resp)?;
        Ok(rdata.get("promise").cloned().unwrap_or_default())
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
            "corrId": format!("ps-{}", now_ms()),
            "id": id,
            "state": state,
            "value": value,
        });

        let resp = self.transport.send(req).await?;
        let rdata = crate::transport::response_data(&resp)?;
        Ok(rdata.get("promise").cloned().unwrap_or_default())
    }

    /// Register a listener on a promise.
    pub async fn register_listener(
        &self,
        awaited: &str,
        address: &str,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "promise.registerListener",
            "corrId": format!("prl-{}", now_ms()),
            "awaited": awaited,
            "address": address,
        });

        let resp = self.transport.send(req).await?;
        let rdata = crate::transport::response_data(&resp)?;
        Ok(rdata.get("promise").cloned().unwrap_or_default())
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
        name: &str,
        cron: &str,
        promise_id_template: &str,
        timeout: i64,
        param: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "schedule.create",
            "corrId": format!("sc-{}", now_ms()),
            "name": name,
            "cron": cron,
            "promiseIdTemplate": promise_id_template,
            "timeout": timeout,
            "param": param,
        });

        let resp = self.transport.send(req).await?;
        Ok(resp)
    }

    /// Get a schedule by name.
    pub async fn get(&self, name: &str) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "schedule.get",
            "corrId": format!("sg-{}", now_ms()),
            "name": name,
        });

        let resp = self.transport.send(req).await?;
        Ok(resp)
    }

    /// Delete a schedule.
    pub async fn delete(&self, name: &str) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "kind": "schedule.delete",
            "corrId": format!("sd-{}", now_ms()),
            "name": name,
        });

        let resp = self.transport.send(req).await?;
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
