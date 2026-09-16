//! The console's read model: the three `ui.*` requests.
//!
//! Everything a client can vary — sort, direction, cursor, limits — is
//! resolved in `resonate_core::ui` before it gets here, so what these build is
//! a `format!` over constants and a parameter list, never caller text.

use neo4rs::query;

use crate::db::{p_cols, parse_task_state, PromiseRow, Tx};
use crate::engine::Output;
use crate::ops_promise::ok;
use crate::ops_schedule::s_cols;
use crate::{Neo4jEngine, StorageResult};
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

/// The sort key, as a Cypher expression over `p`. Keep in step with
/// `resonate_core::ui::UNSETTLED_KEY`: a row that has not settled sorts at the
/// end of time.
fn execution_sort_expr(key: ExecutionSortKey) -> &'static str {
    match key {
        ExecutionSortKey::CreatedAt => "p.created_at",
        ExecutionSortKey::SettledAt => "coalesce(p.settled_at, 9223372036854775807)",
        ExecutionSortKey::TimeoutAt => "p.timeout_at",
    }
}

fn schedule_sort_expr(key: ScheduleSortKey) -> &'static str {
    match key {
        ScheduleSortKey::NextRunAt => "s.next_run_at",
        ScheduleSortKey::LastRunAt => "coalesce(s.last_run_at, 9223372036854775807)",
        ScheduleSortKey::CreatedAt => "s.created_at",
    }
}

fn dir(d: Dir) -> (&'static str, &'static str) {
    match d {
        Dir::Asc => ("ASC", ">"),
        Dir::Desc => ("DESC", "<"),
    }
}

/// `AND p.state IN [...]`, or nothing. Inlined rather than bound: the values
/// come from a closed enum, so there is no caller text in the statement.
fn states_cypher(q: &ui::ExecutionsQuery) -> String {
    if q.states.is_empty() {
        return String::new();
    }
    let list = q
        .states
        .iter()
        .map(|s| format!("'{}'", s.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" AND p.state IN [{list}]")
}

/// The tag filter as the `tag_kv` pairs a node must contain.
fn pairs_of(tags_json: Option<&str>) -> Vec<String> {
    tags_json
        .and_then(|t| serde_json::from_str::<crate::db::Tags>(t).ok())
        .map(|t| crate::db::tag_pairs(&t))
        .unwrap_or_default()
}

/// The executions list: root promises — `id = origin` — sorted, one keyset
/// page.
const EXECUTIONS_WHERE: &str = "p.id = p.origin{states} \
    AND ($id_from IS NULL OR p.id >= $id_from) \
    AND ($id_to IS NULL OR p.id < $id_to) \
    AND ALL(kv IN $pairs WHERE kv IN p.tag_kv) \
    AND ($created_from IS NULL OR p.created_at >= $created_from) \
    AND ($created_to IS NULL OR p.created_at <= $created_to)";

fn executions_where(q: &ui::ExecutionsQuery) -> String {
    EXECUTIONS_WHERE.replace("{states}", &states_cypher(q))
}

fn bind_executions(query: neo4rs::Query, q: &ui::ExecutionsQuery) -> neo4rs::Query {
    query
        .param("id_from", q.id_from.clone())
        .param("id_to", q.id_to.clone())
        .param("pairs", pairs_of(q.tags_json.as_deref()))
        .param("created_from", q.created_from)
        .param("created_to", q.created_to)
}

impl Tx<'_> {
    async fn ui_executions_search(
        &mut self,
        q: &ui::ExecutionsQuery,
    ) -> StorageResult<Vec<PromiseRecord>> {
        let expr = execution_sort_expr(q.sort.key);
        let (order, cmp) = dir(q.sort.dir);
        let cypher = format!(
            "MATCH (p:Promise) WHERE {where_} \
               AND ($after_key IS NULL OR {expr} {cmp} $after_key \
                    OR ({expr} = $after_key AND p.id {cmp} $after_id)) \
             RETURN {cols} ORDER BY {expr} {order}, p.id {order} LIMIT $n",
            where_ = executions_where(q),
            cols = p_cols("p"),
        );
        let (after_key, after_id) = match &q.after {
            Some(k) => (Some(k.key), Some(k.id.clone())),
            None => (None, None),
        };
        let stmt = bind_executions(query(&cypher), q)
            .param("after_key", after_key)
            .param("after_id", after_id)
            .param("n", q.fetch + 1);
        Ok(self
            .promise_rows(stmt)
            .await?
            .iter()
            .map(PromiseRow::to_promise_record)
            .collect())
    }

    async fn ui_executions_count(&mut self, q: &ui::ExecutionsQuery) -> StorageResult<i64> {
        let cypher = format!(
            "MATCH (p:Promise) WHERE {} RETURN count(p) AS n",
            executions_where(q)
        );
        let row = self.one(bind_executions(query(&cypher), q)).await?;
        Ok(row.and_then(|r| r.get::<i64>("n").ok()).unwrap_or(0))
    }

    /// One execution, whole: every promise sharing the root's origin, with
    /// the task columns that sit on the same node.
    async fn ui_execution_nodes(
        &mut self,
        q: &ui::ExecutionQuery,
    ) -> StorageResult<Vec<ui::NodeRow>> {
        let stmt = query(&format!(
            "MATCH (p:Promise {{origin: $root}}) RETURN {} \
             ORDER BY p.created_at ASC, p.id ASC LIMIT $n",
            p_cols("p")
        ))
        .param("root", q.root_id.as_str())
        .param("n", q.max_nodes + 1);
        Ok(self
            .promise_rows(stmt)
            .await?
            .into_iter()
            .map(|row| ui::NodeRow {
                promise: row.to_promise_record(),
                task_state: row.task_state.as_deref().map(parse_task_state),
                task_version: row.task_version,
                resumes: row.resumes,
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
        let expr = schedule_sort_expr(q.sort.key);
        let (order, cmp) = dir(q.sort.dir);
        let cypher = format!(
            "MATCH (s:Schedule) \
             WHERE ($id_from IS NULL OR s.id >= $id_from) \
               AND ($id_to IS NULL OR s.id < $id_to) \
               AND ALL(kv IN $pairs WHERE kv IN s.tag_kv) \
               AND ($after_key IS NULL OR {expr} {cmp} $after_key \
                    OR ({expr} = $after_key AND s.id {cmp} $after_id)) \
             RETURN {cols} ORDER BY {expr} {order}, s.id {order} LIMIT $n",
            cols = s_cols("s"),
        );
        let (after_key, after_id) = match &q.after {
            Some(k) => (Some(k.key), Some(k.id.clone())),
            None => (None, None),
        };
        let stmt = query(&cypher)
            .param("id_from", q.id_from.clone())
            .param("id_to", q.id_to.clone())
            .param("pairs", pairs_of(q.tags_json.as_deref()))
            .param("after_key", after_key)
            .param("after_id", after_id)
            .param("n", q.limit + 1);
        self.schedule_rows(stmt).await
    }

    async fn ui_schedules_count(&mut self, q: &ui::SchedulesQuery) -> StorageResult<i64> {
        let stmt = query(
            "MATCH (s:Schedule) \
             WHERE ($id_from IS NULL OR s.id >= $id_from) \
               AND ($id_to IS NULL OR s.id < $id_to) \
               AND ALL(kv IN $pairs WHERE kv IN s.tag_kv) \
             RETURN count(s) AS n",
        )
        .param("id_from", q.id_from.clone())
        .param("id_to", q.id_to.clone())
        .param("pairs", pairs_of(q.tags_json.as_deref()));
        let row = self.one(stmt).await?;
        Ok(row.and_then(|r| r.get::<i64>("n").ok()).unwrap_or(0))
    }
}

impl Neo4jEngine {
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
