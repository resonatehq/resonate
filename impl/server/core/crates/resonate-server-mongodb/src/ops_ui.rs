//! The console's read model: the three `ui.*` requests.
//!
//! Everything a client can vary — sort, direction, cursor, limits — is
//! resolved in `resonate_core::ui` before it gets here, so what these build is
//! a filter document over constants and bound values, never caller text.

use mongodb::bson::{doc, Bson, Document};

use crate::db::{contains_pairs, parse_task_state, PromiseRow, Tx};
use crate::engine::Output;
use crate::ops_promise::ok;
use crate::ops_schedule::doc_to_schedule;
use crate::{MongoDbEngine, StorageResult};
use resonate_core::types::{PromiseRecord, RequestEnvelope, ResponseEnvelope, ScheduleRecord};
use resonate_core::ui::{self, Dir, ExecutionSortKey, ScheduleSortKey, UiError};

/// Parse and resolve a `ui.*` request's `data`, rendering either failure as
/// the response it is. The same function the SQL family shares; copied here
/// because a server outside that family may not reach into it.
fn ui_resolve<T, Q>(
    req: &RequestEnvelope,
    resolve: impl FnOnce(T) -> Result<Q, UiError>,
) -> Result<Q, ResponseEnvelope>
where
    T: serde::de::DeserializeOwned,
{
    let parsed: T = serde_json::from_value(req.data.clone()).map_err(|e| {
        UiError::InvalidRequest(e.to_string())
            .to_response(req.kind.clone(), req.head.corr_id.clone())
    })?;
    resolve(parsed).map_err(|e| e.to_response(req.kind.clone(), req.head.corr_id.clone()))
}

/// The sort key, as an aggregation expression. Keep in step with
/// `resonate_core::ui::UNSETTLED_KEY`: a row that has not settled sorts at the
/// end of time.
fn execution_sort_expr(key: ExecutionSortKey) -> Bson {
    match key {
        ExecutionSortKey::CreatedAt => Bson::from("$created_at"),
        ExecutionSortKey::SettledAt => Bson::from(doc! { "$ifNull": ["$settled_at", i64::MAX] }),
        ExecutionSortKey::TimeoutAt => Bson::from("$timeout_at"),
    }
}

fn schedule_sort_expr(key: ScheduleSortKey) -> Bson {
    match key {
        ScheduleSortKey::NextRunAt => Bson::from("$next_run_at"),
        ScheduleSortKey::LastRunAt => Bson::from(doc! { "$ifNull": ["$last_run_at", i64::MAX] }),
        ScheduleSortKey::CreatedAt => Bson::from("$created_at"),
    }
}

fn dir(d: Dir) -> (i32, &'static str) {
    match d {
        Dir::Asc => (1, "$gt"),
        Dir::Desc => (-1, "$lt"),
    }
}

/// The tag filter, from the JSON form `resonate_core::ui` resolves it to.
fn tags_filter(field: &str, tags_json: Option<&str>) -> Option<Document> {
    tags_json
        .and_then(|t| serde_json::from_str::<crate::db::Tags>(t).ok())
        .and_then(|t| contains_pairs(field, &t))
}

/// `_id >= id_from AND _id < id_to`, each bound when given.
fn id_range(id_from: &Option<String>, id_to: &Option<String>) -> Option<Document> {
    let mut range = Document::new();
    if let Some(from) = id_from {
        range.insert("$gte", from.as_str());
    }
    if let Some(to) = id_to {
        range.insert("$lt", to.as_str());
    }
    (!range.is_empty()).then(|| doc! { "_id": range })
}

/// The executions list: root promises, filtered.
fn executions_filter(q: &ui::ExecutionsQuery) -> Document {
    let mut and = vec![doc! { "root": true }];
    if !q.states.is_empty() {
        let states: Vec<&str> = q.states.iter().map(|s| s.as_str()).collect();
        and.push(doc! { "state": { "$in": states } });
    }
    and.extend(id_range(&q.id_from, &q.id_to));
    and.extend(tags_filter("tags", q.tags_json.as_deref()));
    if let Some(from) = q.created_from {
        and.push(doc! { "created_at": { "$gte": from } });
    }
    if let Some(to) = q.created_to {
        and.push(doc! { "created_at": { "$lte": to } });
    }
    doc! { "$and": and }
}

fn schedules_filter(q: &ui::SchedulesQuery) -> Document {
    let mut and = Vec::new();
    and.extend(id_range(&q.id_from, &q.id_to));
    and.extend(tags_filter("promise_tags", q.tags_json.as_deref()));
    if and.is_empty() {
        doc! {}
    } else {
        doc! { "$and": and }
    }
}

/// One keyset page: filter, compute the sort key, resume after the cursor,
/// sort by key then id, and take `n`.
fn keyset_pipeline(
    filter: Document,
    key: Bson,
    d: Dir,
    after: Option<&ui::Keyset>,
    n: i64,
) -> Vec<Document> {
    let (order, cmp) = dir(d);
    let mut pipeline = vec![
        doc! { "$match": filter },
        doc! { "$addFields": { "_sort": key } },
    ];
    if let Some(k) = after {
        pipeline.push(doc! { "$match": { "$or": [
            { "_sort": { cmp: k.key } },
            { "_sort": k.key, "_id": { cmp: k.id.as_str() } },
        ] } });
    }
    pipeline.push(doc! { "$sort": { "_sort": order, "_id": order } });
    pipeline.push(doc! { "$limit": n });
    pipeline
}

impl Tx<'_> {
    async fn ui_executions_search(
        &mut self,
        q: &ui::ExecutionsQuery,
    ) -> StorageResult<Vec<PromiseRecord>> {
        let pipeline = keyset_pipeline(
            executions_filter(q),
            execution_sort_expr(q.sort.key),
            q.sort.dir,
            q.after.as_ref(),
            q.fetch + 1,
        );
        let promises = self.promises.clone();
        self.aggregate(&promises, pipeline)
            .await?
            .iter()
            .map(|d| PromiseRow::from_doc(d).map(|r| r.to_promise_record()))
            .collect()
    }

    async fn ui_executions_count(&mut self, q: &ui::ExecutionsQuery) -> StorageResult<i64> {
        let promises = self.promises.clone();
        self.count(&promises, executions_filter(q)).await
    }

    /// One execution, whole: every promise sharing the root's origin, with
    /// the task fields that sit in the same document.
    async fn ui_execution_nodes(
        &mut self,
        q: &ui::ExecutionQuery,
    ) -> StorageResult<Vec<ui::NodeRow>> {
        Ok(self
            .promise_rows(
                doc! { "origin": q.root_id.as_str() },
                doc! { "created_at": 1, "_id": 1 },
                Some(q.max_nodes + 1),
            )
            .await?
            .into_iter()
            .map(|row| ui::NodeRow {
                promise: row.to_promise_record(),
                task_state: row.task_state.as_deref().map(parse_task_state),
                task_version: row.task_version,
                resumes: row.resumes_count(),
                ttl: row.ttl,
                pid: row.pid,
                retry_timeout_at: row.retry_timeout_at,
                lease_timeout_at: row.lease_timeout_at,
            })
            .collect())
    }

    async fn ui_schedules_search(
        &mut self,
        q: &ui::SchedulesQuery,
    ) -> StorageResult<Vec<ScheduleRecord>> {
        let pipeline = keyset_pipeline(
            schedules_filter(q),
            schedule_sort_expr(q.sort.key),
            q.sort.dir,
            q.after.as_ref(),
            q.limit + 1,
        );
        let schedules = self.schedules.clone();
        self.aggregate(&schedules, pipeline)
            .await?
            .iter()
            .map(doc_to_schedule)
            .collect()
    }

    async fn ui_schedules_count(&mut self, q: &ui::SchedulesQuery) -> StorageResult<i64> {
        let schedules = self.schedules.clone();
        self.count(&schedules, schedules_filter(q)).await
    }
}

impl MongoDbEngine {
    pub(crate) async fn op_ui_executions_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let q = match ui_resolve::<ui::ExecutionsSearchData, _>(req, |d| d.resolve()) {
            Ok(q) => q,
            Err(resp) => return Output::response(resp),
        };
        let q = &q;
        self.run(req, |tx| {
            Box::pin(async move {
                let rows = tx.ui_executions_search(q).await?;
                let total = if q.count_total {
                    Some(tx.ui_executions_count(q).await?)
                } else {
                    None
                };
                Ok(ok(req, &ui::finish_executions_page(q, rows, total)))
            })
        })
        .await
    }

    pub(crate) async fn op_ui_execution_get(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let q = match ui_resolve::<ui::ExecutionGetData, _>(req, |d| d.resolve()) {
            Ok(q) => q,
            Err(resp) => return Output::response(resp),
        };
        let q = &q;
        self.run(req, |tx| {
            Box::pin(async move {
                let rows = tx.ui_execution_nodes(q).await?;
                Ok(match ui::build_execution(q, rows) {
                    Ok(view) => ok(req, &view),
                    Err(e) => e.to_response(req.kind.clone(), req.head.corr_id.clone()),
                })
            })
        })
        .await
    }

    pub(crate) async fn op_ui_schedules_search(&self, req: &RequestEnvelope, _now: i64) -> Output {
        let q = match ui_resolve::<ui::SchedulesSearchData, _>(req, |d| d.resolve()) {
            Ok(q) => q,
            Err(resp) => return Output::response(resp),
        };
        let q = &q;
        self.run(req, |tx| {
            Box::pin(async move {
                let rows = tx.ui_schedules_search(q).await?;
                let total = if q.count_total {
                    Some(tx.ui_schedules_count(q).await?)
                } else {
                    None
                };
                Ok(ok(req, &ui::finish_schedules_page(q, rows, total)))
            })
        })
        .await
    }
}
