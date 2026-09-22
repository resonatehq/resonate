//! Debug reset, snapshot and tick.
//!
//! Every snapshot section is a projection of the graph with the membership
//! rules every engine's snapshot carries: a promise deadline is a pending,
//! external promise; a callback is an unready `AWAITS` edge; a task deadline is
//! the retry or lease column of a task in the matching state. Nothing is
//! queued, so `messages` is empty — the transitions returned them.

use neo4rs::query;
use serde_json::Value;

use crate::db::{p_cols, PromiseRow};
use crate::engine::Output;
use crate::{timeouts, Neo4jEngine, StorageError};
use resonate_core::types::{
    RequestEnvelope, ResponseEnvelope, Snapshot, SnapshotCallback, SnapshotListener,
    SnapshotMessage, SnapshotPromiseTimeout, SnapshotTaskTimeout,
};

impl Neo4jEngine {
    pub(crate) async fn op_debug_reset(&self, req: &RequestEnvelope) -> Output {
        // Only this server's labels, so a shared database keeps what is not
        // ours. `DETACH` takes the edges with the nodes.
        let result = self
            .transact(|tx| {
                Box::pin(async move {
                    tx.run(query(
                        "MATCH (n) WHERE n:Promise OR n:Schedule DETACH DELETE n",
                    ))
                    .await
                })
            })
            .await;
        Output::response(match result {
            Ok(((), _, _)) => {
                tracing::warn!("Debug reset: all data cleared");
                ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    200,
                    Value::Object(serde_json::Map::new()),
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "Debug reset failed");
                ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    500,
                    &format!("Reset failed: {}", e),
                )
            }
        })
    }

    pub(crate) async fn op_debug_snap(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let result = self
            .query(|tx| {
                Box::pin(async move {
                    let mut all = tx
                        .promise_rows(query(&format!(
                            "MATCH (p:Promise) RETURN {} ORDER BY p.id",
                            p_cols("p")
                        )))
                        .await?;
                    all.sort_by(|a, b| a.id.cmp(&b.id));

                    let promises = all.iter().map(PromiseRow::to_promise_record).collect();

                    let promise_timeouts: Vec<SnapshotPromiseTimeout> = all
                        .iter()
                        .filter(|p| p.is_pending() && p.external)
                        .map(|p| SnapshotPromiseTimeout {
                            id: p.id.clone(),
                            timeout: p.timeout_at,
                        })
                        .collect();

                    // Unready edges only — the ready ones are `resumes`.
                    let edges = tx
                        .rows(query(
                            "MATCH (w:Promise)-[r:AWAITS]->(a:Promise) WHERE r.ready = false \
                             RETURN w.id AS awaiter, a.id AS awaited ORDER BY awaiter, awaited",
                        ))
                        .await?;
                    let mut callbacks: Vec<SnapshotCallback> = Vec::with_capacity(edges.len());
                    for e in &edges {
                        callbacks.push(SnapshotCallback {
                            awaiter: e
                                .get::<String>("awaiter")
                                .map_err(|e| StorageError::Backend(e.to_string()))?,
                            awaited: e
                                .get::<String>("awaited")
                                .map_err(|e| StorageError::Backend(e.to_string()))?,
                        });
                    }

                    let mut listeners: Vec<SnapshotListener> = Vec::new();
                    for p in &all {
                        let mut addrs = p.listeners.clone();
                        addrs.sort();
                        for address in addrs {
                            listeners.push(SnapshotListener {
                                promise_id: p.id.clone(),
                                address,
                            });
                        }
                    }

                    let tasks = all.iter().filter_map(PromiseRow::to_task_record).collect();

                    let task_timeouts: Vec<SnapshotTaskTimeout> = all
                        .iter()
                        .filter_map(|p| {
                            if p.task_is("pending") {
                                p.retry_timeout_at.map(|t| SnapshotTaskTimeout {
                                    id: p.id.clone(),
                                    timeout_type: 0,
                                    timeout: t,
                                })
                            } else if p.task_is("acquired") {
                                p.lease_timeout_at.map(|t| SnapshotTaskTimeout {
                                    id: p.id.clone(),
                                    timeout_type: 1,
                                    timeout: t,
                                })
                            } else {
                                None
                            }
                        })
                        .collect();

                    let messages: Vec<SnapshotMessage> = Vec::new();

                    Ok(Snapshot {
                        promises,
                        promise_timeouts,
                        callbacks,
                        listeners,
                        tasks,
                        task_timeouts,
                        messages,
                    })
                })
            })
            .await;
        Output::response(match result {
            Ok(snapshot) => {
                let data = serde_json::to_value(snapshot).unwrap_or(Value::Null);
                ResponseEnvelope::new(req.kind.clone(), req.head.corr_id.clone(), 200, data)
            }
            Err(e) => ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Snap failed: {}", e),
            ),
        })
    }

    /// The sweep, and every message it emits: a redispatched task and a fired
    /// schedule both produce execute messages, which ride out on the tick's
    /// own `Output`.
    pub(crate) async fn op_debug_tick(&self, req: &RequestEnvelope) -> Output {
        let time = match req.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => {
                return Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "Missing or invalid 'time' field",
                ))
            }
        };
        if let Some(debug_time) = req.head.debug_time {
            if debug_time != time {
                return Output::response(ResponseEnvelope::error(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    400,
                    "resonate:debug_time must equal data.time",
                ));
            }
        }
        match self
            .transact(|tx| {
                Box::pin(async move { timeouts::process_all_timeouts(tx, time).await.map(|_| ()) })
            })
            .await
        {
            Ok(((), messages, timeouts)) => Output {
                response: Some(ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    200,
                    Value::Array(vec![]),
                )),
                messages,
                timeouts,
            },
            Err(e) => Output::response(ResponseEnvelope::error(
                req.kind.clone(),
                req.head.corr_id.clone(),
                500,
                &format!("Tick failed: {}", e),
            )),
        }
    }
}
