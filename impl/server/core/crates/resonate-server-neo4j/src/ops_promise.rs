//! The `promise.*` operations, and the request plumbing every operation
//! shares.
//!
//! Each operation is the Postgres engine's, read against a locked node and
//! written back: the same status codes in the same order, the same messages,
//! the same deadlines announced.

use std::collections::HashSet;

use neo4rs::query;
use serde::de::DeserializeOwned;
use serde::Serialize;
use validator::Validate;

use crate::db::{headers_json, p_cols, NewPromise, PromiseRow, Tx};
use crate::engine::Output;
use crate::{Neo4jEngine, StorageError, StorageResult};
use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRecord,
    PromiseRegisterCallbackData, PromiseRegisterListenerData, PromiseResponseData,
    PromiseSearchData, PromiseSearchResponseData, PromiseSettleData, PromiseState, RequestEnvelope,
    ResponseEnvelope,
};

// ─── Request plumbing ────────────────────────────────────────────────────────

/// Parse and validate a request's `data`, or the 400 that says why not.
pub(crate) fn parse<T: DeserializeOwned + Validate>(
    req: &RequestEnvelope,
) -> Result<T, ResponseEnvelope> {
    let r: T = serde_json::from_value(req.data.clone())
        .map_err(|e| fail(req, 400, &format!("Invalid request: {}", e)))?;
    r.validate()
        .map_err(|e| fail(req, 400, &format_validation_errors(&e)))?;
    Ok(r)
}

pub(crate) fn fail(req: &RequestEnvelope, status: i32, message: &str) -> ResponseEnvelope {
    ResponseEnvelope::error(req.kind.clone(), req.head.corr_id.clone(), status, message)
}

pub(crate) fn ok<T: Serialize>(req: &RequestEnvelope, data: &T) -> ResponseEnvelope {
    ResponseEnvelope::success(req.kind.clone(), req.head.corr_id.clone(), data)
}

/// `200 {}` — the answer to an operation that returns nothing.
pub(crate) fn ok_empty(req: &RequestEnvelope) -> ResponseEnvelope {
    ResponseEnvelope::new(
        req.kind.clone(),
        req.head.corr_id.clone(),
        200,
        serde_json::json!({}),
    )
}

/// The page size a search accepts: the caller's, up to 1000, or `default`.
pub(crate) fn search_limit(
    req: &RequestEnvelope,
    limit: Option<i64>,
    default: i64,
) -> Result<i64, ResponseEnvelope> {
    match limit {
        Some(n) if n > 1000 => Err(fail(
            req,
            400,
            "Invalid 'limit' — must be between 1 and 1000",
        )),
        Some(n) => Ok(n),
        None => Ok(default),
    }
}

// ─── Creating a promise ──────────────────────────────────────────────────────

/// What a create found or made.
pub(crate) struct CreateOutcome {
    pub was_created: bool,
    pub row: PromiseRow,
}

/// Create a promise, or find the one already there. Shared by `promise.create`
/// and the fenced create: the caller has run `try_timeout` on the id.
///
/// A targeted promise is a task from birth: pending with a retry deadline and
/// an execute message, or — when the deadline has already passed — settled
/// with its task fulfilled and nothing emitted.
pub(crate) async fn create_promise(
    tx: &mut Tx<'_>,
    d: &PromiseCreateData,
    now: i64,
) -> StorageResult<CreateOutcome> {
    if let Some(row) = tx.lock(&d.id).await? {
        return Ok(CreateOutcome {
            was_created: false,
            row,
        });
    }
    let address = d.tags.get("resonate:target").map(String::as_str);
    let already_timedout = now >= d.timeout_at;
    let (state, created_at, settled_at) = if already_timedout {
        let state = if resonate_core::types::is_timer(&d.tags) {
            PromiseState::Resolved
        } else {
            PromiseState::RejectedTimedout
        };
        (state, d.timeout_at, Some(d.timeout_at))
    } else {
        (PromiseState::Pending, now, None)
    };
    let task_state = address.map(|_| {
        if already_timedout {
            "fulfilled"
        } else {
            "pending"
        }
    });
    let retry_at = if task_state == Some("pending") {
        Some(created_at + tx.trt)
    } else {
        None
    };
    tx.create_promise(&NewPromise {
        id: &d.id,
        state: state.as_str(),
        param_headers: headers_json(d.param.headers.as_ref()),
        param_data: d.param.data.as_deref(),
        tags: &d.tags,
        timeout_at: d.timeout_at,
        created_at,
        settled_at,
        task_state,
        task_version: 0,
        retry_timeout_at: retry_at,
        lease_timeout_at: None,
        ttl: None,
        pid: None,
    })
    .await?;
    if let Some(at) = retry_at {
        tx.emit_execute(address, &d.id, 0);
        tx.arm_retry(&d.id, at);
    }
    if !already_timedout && resonate_core::types::is_external(&d.tags) {
        tx.arm_promise_timeout(&d.id, d.timeout_at);
    }
    let row = tx
        .read(&d.id)
        .await?
        .ok_or_else(|| StorageError::Backend("created promise not readable".into()))?;
    Ok(CreateOutcome {
        was_created: true,
        row,
    })
}

// ─── Operations ──────────────────────────────────────────────────────────────

impl Neo4jEngine {
    pub(crate) async fn op_promise_get(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseGetData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                match tx.read(&r.id).await? {
                    Some(row) => {
                        let promise = row.to_promise_record();
                        tracing::debug!(promise_id = %r.id, state = %promise.state, "Promise found");
                        Ok(ok(req, &PromiseResponseData { promise }))
                    }
                    None => {
                        tracing::debug!(promise_id = %r.id, "Promise not found");
                        Ok(fail(req, 404, "Promise not found"))
                    }
                }
            })
        })
        .await
    }

    pub(crate) async fn op_promise_create(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseCreateData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        if let Some(addr) = r.tags.get("resonate:target") {
            if !resonate_core::is_valid_address(addr) {
                tracing::warn!(promise_id = %r.id, address = %addr, "Promise create rejected: invalid resonate:target address");
                return Output::response(fail(req, 400, "Invalid resonate:target address"));
            }
        }
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                let outcome = create_promise(tx, r, now).await?;
                let promise = outcome.row.to_promise_record();
                if outcome.was_created {
                    tracing::info!(promise_id = %promise.id, state = %promise.state, timeout_at = promise.timeout_at, "Promise created");
                } else {
                    tracing::debug!(promise_id = %promise.id, state = %promise.state, "Promise create: already exists (idempotent)");
                }
                Ok(ok(req, &PromiseResponseData { promise }))
            })
        })
        .await
    }

    pub(crate) async fn op_promise_settle(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseSettleData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.id], now).await?;
                let Some(row) = tx.lock(&r.id).await? else {
                    tracing::debug!(promise_id = %r.id, "Promise settle: promise not found");
                    return Ok(fail(req, 404, "Promise not found"));
                };
                let promise = if row.is_pending() {
                    let settled = tx
                        .settle_locked(
                            &row,
                            r.state.as_str(),
                            headers_json(r.value.headers.as_ref()),
                            r.value.data.as_deref(),
                            now,
                            row.settle_fulfils(),
                            &HashSet::new(),
                            now,
                        )
                        .await?;
                    let promise = settled.to_promise_record();
                    tracing::info!(promise_id = %promise.id, state = %promise.state, "Promise settled");
                    promise
                } else {
                    let promise = row.to_promise_record();
                    tracing::debug!(promise_id = %promise.id, current_state = %promise.state, requested_state = %r.state, "Promise settle: already settled (idempotent)");
                    promise
                };
                assert_ne!(
                    promise.state,
                    PromiseState::Pending,
                    "invariant: returning 200 but promise is still pending"
                );
                Ok(ok(req, &PromiseResponseData { promise }))
            })
        })
        .await
    }

    pub(crate) async fn op_promise_register_callback(
        &self,
        req: &RequestEnvelope,
        now: i64,
    ) -> Output {
        let r: PromiseRegisterCallbackData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.awaited, &r.awaiter], now).await?;
                let rows = tx
                    .lock_many(&[r.awaited.clone(), r.awaiter.clone()])
                    .await?;
                let awaited = rows.iter().find(|p| p.id == r.awaited);
                let awaiter = rows.iter().find(|p| p.id == r.awaiter);

                let Some(awaited) = awaited else {
                    tracing::debug!(promise_id = %r.awaited, "Callback registration: awaited promise not found");
                    return Ok(fail(req, 404, "Awaited promise not found"));
                };
                let Some(awaiter) = awaiter else {
                    tracing::debug!(promise_id = %r.awaiter, "Callback registration: awaiter promise not found");
                    return Ok(fail(req, 422, "Awaiter promise not found"));
                };
                if awaiter.target.is_none() {
                    tracing::debug!(awaiter = %r.awaiter, "Callback registration rejected: awaiter has no resonate:target");
                    return Ok(fail(req, 422, "Awaiter promise has no resonate:target tag"));
                }
                if !awaited.external {
                    tracing::debug!(awaited = %r.awaited, "Callback registration rejected: awaited is not awaitable");
                    return Ok(fail(req, 422, "Awaited promise is not awaitable"));
                }

                if awaited.is_pending() {
                    // Link: the awaited is still pending and awaitable, the
                    // awaiter targeted and itself pending.
                    if awaiter.is_pending() {
                        tx.link(&awaiter.id, &awaited.id).await?;
                    }
                } else if awaiter.task_is("pending")
                    || awaiter.task_is("acquired")
                    || awaiter.task_is("suspended")
                {
                    // Direct resume: the awaited already settled. The ready
                    // callback is recorded; a suspended awaiter is woken.
                    tx.mark_ready(&awaiter.id, &awaited.id).await?;
                    if awaiter.task_is("suspended") {
                        tx.wake(awaiter, now).await?;
                    }
                }

                let promise = awaited.to_promise_record();
                tracing::info!(awaited = %r.awaited, awaiter = %r.awaiter, awaited_state = %promise.state, "Callback registered");
                Ok(ok(req, &PromiseResponseData { promise }))
            })
        })
        .await
    }

    pub(crate) async fn op_promise_register_listener(
        &self,
        req: &RequestEnvelope,
        now: i64,
    ) -> Output {
        let r: PromiseRegisterListenerData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        if !resonate_core::is_valid_address(&r.address) {
            tracing::warn!(awaited = %r.awaited, address = %r.address, "Listener registration rejected: invalid address");
            return Output::response(fail(req, 400, "Invalid listener address"));
        }
        let r = &r;
        self.run(req, |tx| {
            Box::pin(async move {
                tx.try_timeout(&[&r.awaited], now).await?;
                let Some(row) = tx.lock(&r.awaited).await? else {
                    tracing::debug!(awaited = %r.awaited, "Listener registration: awaited promise not found");
                    return Ok(fail(req, 404, "Awaited promise not found"));
                };
                if !row.external {
                    tracing::debug!(awaited = %r.awaited, "Listener registration rejected: awaited is not awaitable");
                    return Ok(fail(req, 422, "Awaited promise is not awaitable"));
                }
                if row.is_pending() {
                    tx.run(
                        query(
                            "MATCH (p:Promise {id: $id}) \
                             SET p.listeners = CASE WHEN $address IN p.listeners \
                                 THEN p.listeners ELSE p.listeners + $address END",
                        )
                        .param("id", r.awaited.as_str())
                        .param("address", r.address.as_str()),
                    )
                    .await?;
                }
                let promise = row.to_promise_record();
                tracing::info!(awaited = %r.awaited, address = %r.address, promise_state = %promise.state, "Listener registered");
                Ok(ok(req, &PromiseResponseData { promise }))
            })
        })
        .await
    }

    /// Search by effective state at `now`, as the SQL engines do: the filter
    /// is `effective_state`, and every record comes back projected, so a
    /// pending node past its deadline neither fills a "pending" page nor
    /// reads as pending on any other.
    pub(crate) async fn op_promise_search(&self, req: &RequestEnvelope, now: i64) -> Output {
        let r: PromiseSearchData = match parse(req) {
            Ok(r) => r,
            Err(resp) => return Output::response(resp),
        };
        let limit = match search_limit(req, r.limit, 100) {
            Ok(n) => n,
            Err(resp) => return Output::response(resp),
        };
        let pairs = r
            .tags
            .as_ref()
            .map(crate::db::tag_pairs)
            .unwrap_or_default();
        let r = &r;
        let pairs = &pairs;
        self.run(req, |tx| {
            Box::pin(async move {
                let q = query(&format!(
                    "MATCH (p:Promise) \
                     WHERE {} \
                       AND ($cursor IS NULL OR p.id > $cursor) \
                       AND ALL(kv IN $pairs WHERE kv IN p.tag_kv) \
                     RETURN {} ORDER BY p.id ASC LIMIT $limit",
                    effective_state(r.state.map(|s| s.as_str())),
                    p_cols("p")
                ))
                .param("now", now)
                .param("cursor", r.cursor.clone())
                .param("pairs", pairs.clone())
                .param("limit", limit + 1);
                let rows = tx.promise_rows(q).await?;
                let has_more = rows.len() as i64 > limit;
                let promises: Vec<PromiseRecord> = rows
                    .iter()
                    .take(limit as usize)
                    .map(|row| {
                        let mut p = row.to_promise_record();
                        p.project(now);
                        p
                    })
                    .collect();
                let cursor = if has_more {
                    promises.last().map(|p| p.id.clone())
                } else {
                    None
                };
                tracing::debug!(found = promises.len(), has_more, "Promise search completed");
                Ok(ok(req, &PromiseSearchResponseData { promises, cursor }))
            })
        })
        .await
    }
}

/// The effective promise state as a Cypher predicate over `p`, at `$now`.
///
/// `resonate_sql::effective_state_sql` over the node's properties: a pending
/// node past its deadline is expired — resolved if a timer, timed out
/// otherwise — whether or not a sweep has written that yet. `state` is one of
/// the protocol's five states, or the predicate is `false`.
fn effective_state(state: Option<&str>) -> &'static str {
    match state {
        None => "true",
        Some("pending") => "(p.state = 'pending' AND p.timeout_at > $now)",
        Some("resolved") => {
            "(p.state = 'resolved' OR (p.state = 'pending' AND p.timeout_at <= $now AND p.is_timer))"
        }
        Some("rejected_timedout") => {
            "(p.state = 'rejected_timedout' \
             OR (p.state = 'pending' AND p.timeout_at <= $now AND NOT p.is_timer))"
        }
        Some("rejected") => "p.state = 'rejected'",
        Some("rejected_canceled") => "p.state = 'rejected_canceled'",
        Some(_) => "false",
    }
}
