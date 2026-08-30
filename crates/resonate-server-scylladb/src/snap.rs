//! Debug reset and snapshot.
//!
//! The snapshot's queue sections are projected from the promises table with
//! the same membership rules every engine's snapshot carries — the physical
//! bucketed queues are mechanics, not state, and are never compared. The
//! promises section IS physical: an eagerly settled internal promise shows
//! settled here where a lazy engine shows pending, and that difference is
//! exactly the experiment the differential runs.

use resonate_core::types::{
    RequestEnvelope, ResponseEnvelope, Snapshot, SnapshotCallback, SnapshotListener,
    SnapshotMessage, SnapshotPromiseTimeout, SnapshotTaskTimeout,
};
use resonate_server_dbms::engine_port::Output;
use serde_json::Value;

use crate::db::PromiseRow;
use crate::ops_promise::storage_err;
use crate::ScyllaEngine;

impl ScyllaEngine {
    pub(crate) async fn op_debug_reset(&self, req: &RequestEnvelope) -> Output {
        for table in [
            "promises",
            "schedules",
            "promise_timeouts",
            "task_timeouts",
            "schedule_timeouts",
            "workers",
        ] {
            if let Err(e) = self.exec(&format!("TRUNCATE {table}"), ()).await {
                return storage_err(req, e);
            }
        }
        Output::response(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            Value::Object(serde_json::Map::new()),
        ))
    }

    pub(crate) async fn op_debug_snap(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let rows = match self
            .rows(&format!("SELECT {} FROM promises", crate::db::P_COLS), ())
            .await
        {
            Ok(rows) => rows,
            Err(e) => return storage_err(req, e),
        };
        let mut all: Vec<PromiseRow> = rows.iter().map(PromiseRow::from_map).collect();
        all.sort_by(|a, b| a.id.cmp(&b.id));

        let promises = all.iter().map(|p| p.to_promise_record()).collect();

        let promise_timeouts: Vec<SnapshotPromiseTimeout> = all
            .iter()
            .filter(|p| p.state == "pending" && resonate_core::types::is_awaitable(&p.tags))
            .map(|p| SnapshotPromiseTimeout {
                id: p.id.clone(),
                timeout: p.timeout_at,
            })
            .collect();

        let mut callbacks: Vec<SnapshotCallback> = all
            .iter()
            .flat_map(|p| {
                p.callbacks.iter().map(|awaiter| SnapshotCallback {
                    awaiter: awaiter.clone(),
                    awaited: p.id.clone(),
                })
            })
            .collect();
        callbacks.sort_by(|a, b| a.awaited.cmp(&b.awaited).then(a.awaiter.cmp(&b.awaiter)));

        let mut listeners: Vec<SnapshotListener> = all
            .iter()
            .flat_map(|p| {
                p.listeners.iter().map(|addr| SnapshotListener {
                    promise_id: p.id.clone(),
                    address: addr.clone(),
                })
            })
            .collect();
        listeners.sort_by(|a, b| {
            a.promise_id
                .cmp(&b.promise_id)
                .then(a.address.cmp(&b.address))
        });

        let tasks = all.iter().filter_map(|p| p.to_task_record()).collect();

        let task_timeouts: Vec<SnapshotTaskTimeout> = all
            .iter()
            .filter_map(|p| match p.task_state.as_deref() {
                Some("pending") => p.task_timeout_retry.map(|t| SnapshotTaskTimeout {
                    id: p.id.clone(),
                    timeout_type: 0,
                    timeout: t,
                }),
                Some("acquired") => p.task_timeout_lease.map(|t| SnapshotTaskTimeout {
                    id: p.id.clone(),
                    timeout_type: 1,
                    timeout: t,
                }),
                _ => None,
            })
            .collect();

        let snapshot = Snapshot {
            promises,
            promise_timeouts,
            callbacks,
            listeners,
            tasks,
            task_timeouts,
            messages: Vec::<SnapshotMessage>::new(),
        };
        Output::response(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            200,
            serde_json::to_value(&snapshot).unwrap_or(Value::Null),
        ))
    }
}
