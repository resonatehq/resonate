use std::collections::HashMap;

use crate::error::Result;
use crate::send::{PromiseSearchResult, ScheduleCreateReq, ScheduleSearchResult, Sender};
use crate::types::{
    PromiseCreateReq, PromiseRecord, PromiseSettleReq, ScheduleRecord, SettleState, Value,
};

/// Sub-client for promise operations.
#[derive(Clone)]
pub struct Promises {
    sender: Sender,
}

impl Promises {
    pub(crate) fn new(sender: Sender) -> Self {
        Self { sender }
    }

    /// Get a promise by ID.
    pub async fn get(&self, id: &str) -> Result<PromiseRecord> {
        self.sender.promise_get(id).await
    }

    /// Create a promise.
    pub async fn create(
        &self,
        id: &str,
        timeout_at: i64,
        param: Value,
        tags: HashMap<String, String>,
    ) -> Result<PromiseRecord> {
        self.sender
            .promise_create(PromiseCreateReq {
                id: id.to_string(),
                timeout_at,
                param,
                tags,
            })
            .await
    }

    /// Resolve a promise.
    pub async fn resolve(&self, id: &str, value: Value) -> Result<PromiseRecord> {
        self.settle(id, SettleState::Resolved, value).await
    }

    /// Reject a promise.
    pub async fn reject(&self, id: &str, value: Value) -> Result<PromiseRecord> {
        self.settle(id, SettleState::Rejected, value).await
    }

    /// Cancel a promise (settles as `rejected_canceled`).
    pub async fn cancel(&self, id: &str, value: Value) -> Result<PromiseRecord> {
        self.settle(id, SettleState::RejectedCanceled, value).await
    }

    async fn settle(&self, id: &str, state: SettleState, value: Value) -> Result<PromiseRecord> {
        self.sender
            .promise_settle(PromiseSettleReq {
                id: id.to_string(),
                state,
                value,
            })
            .await
    }

    /// Register a listener on a promise so `address` is notified when it settles.
    pub async fn register_listener(&self, awaited: &str, address: &str) -> Result<PromiseRecord> {
        self.sender
            .promise_register_listener(awaited, address)
            .await
    }

    /// Search for promises matching optional state/tags filters.
    pub async fn search(
        &self,
        state: Option<&str>,
        tags: Option<HashMap<String, String>>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<PromiseSearchResult> {
        self.sender.promise_search(state, tags, limit, cursor).await
    }
}

/// Sub-client for schedule operations.
#[derive(Clone)]
pub struct Schedules {
    sender: Sender,
}

impl Schedules {
    pub(crate) fn new(sender: Sender) -> Self {
        Self { sender }
    }

    /// Create a schedule.
    pub async fn create(
        &self,
        id: &str,
        cron: &str,
        promise_id: &str,
        promise_timeout: i64,
        promise_param: Value,
    ) -> Result<ScheduleRecord> {
        self.sender
            .schedule_create(ScheduleCreateReq {
                id: id.to_string(),
                cron: cron.to_string(),
                promise_id: promise_id.to_string(),
                promise_timeout,
                promise_param,
                promise_tags: HashMap::new(),
            })
            .await
    }

    /// Get a schedule by ID.
    pub async fn get(&self, id: &str) -> Result<ScheduleRecord> {
        self.sender.schedule_get(id).await
    }

    /// Delete a schedule.
    pub async fn delete(&self, id: &str) -> Result<()> {
        self.sender.schedule_delete(id).await
    }

    /// Search for schedules matching optional tag filter.
    pub async fn search(
        &self,
        tags: Option<HashMap<String, String>>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ScheduleSearchResult> {
        self.sender.schedule_search(tags, limit, cursor).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::resonate::Resonate;
    use crate::types::PromiseState;
    use serde_json::json;

    #[tokio::test]
    async fn promises_create_get_resolve_roundtrip() {
        let r = Resonate::local();

        let created = r
            .promises
            .create(
                "unit-p1",
                i64::MAX,
                Value::from_serializable(json!({"x": 1})).unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(created.id, "unit-p1");
        assert_eq!(created.state, PromiseState::Pending);

        let fetched = r.promises.get("unit-p1").await.unwrap();
        assert_eq!(fetched.id, "unit-p1");

        let settled = r
            .promises
            .resolve(
                "unit-p1",
                Value::from_serializable(json!({"result": "ok"})).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(settled.state, PromiseState::Resolved);

        let after = r.promises.get("unit-p1").await.unwrap();
        assert_eq!(after.state, PromiseState::Resolved);
    }

    #[tokio::test]
    async fn promises_get_missing_returns_server_error() {
        let r = Resonate::local();
        let err = r.promises.get("does-not-exist").await.unwrap_err();
        assert!(
            matches!(err, Error::ServerError { .. }),
            "expected ServerError, got {err:?}"
        );
    }

    #[tokio::test]
    async fn schedules_create_get_delete_roundtrip() {
        let r = Resonate::local();

        let created = r
            .schedules
            .create(
                "unit-s1",
                "*/5 * * * *",
                "unit-s1.{{.timestamp}}",
                60_000,
                Value::default(),
            )
            .await
            .unwrap();
        assert_eq!(created.id, "unit-s1");
        assert_eq!(created.cron, "*/5 * * * *");

        let fetched = r.schedules.get("unit-s1").await.unwrap();
        assert_eq!(fetched.id, "unit-s1");

        r.schedules.delete("unit-s1").await.unwrap();
    }

    #[tokio::test]
    async fn schedules_delete_missing_returns_server_error() {
        let r = Resonate::local();
        let err = r.schedules.delete("no-such-schedule").await.unwrap_err();
        assert!(
            matches!(err, Error::ServerError { .. }),
            "expected ServerError, got {err:?}"
        );
    }

    #[tokio::test]
    async fn schedules_search_returns_record() {
        let r = Resonate::local();

        r.schedules
            .create(
                "unit-s-search",
                "* * * * *",
                "unit-s-search.{{.timestamp}}",
                60_000,
                Value::default(),
            )
            .await
            .unwrap();

        let result = r.schedules.search(None, Some(100), None).await.unwrap();
        assert!(
            result.schedules.iter().any(|s| s.id == "unit-s-search"),
            "expected unit-s-search in {:?}",
            result.schedules.iter().map(|s| &s.id).collect::<Vec<_>>()
        );
    }
}
