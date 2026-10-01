//! The three searches, as a scatter-gather over partitions.
//!
//! # Contract
//!
//! A search is not origin-scoped, so no one partition can answer it. Each
//! partition's owner answers for its own partitions with [`local`]: every
//! match after the cursor, sorted by id, at most `limit + 1` of them. The node
//! that took the request merges those answers with [`respond`] and pages the
//! merged list exactly as the SQL handlers page — `id > cursor`, ordered by
//! id. Taking `limit + 1` from every owner is enough: the first `limit + 1`
//! ids overall are among the first `limit + 1` of whichever owner holds them.
//!
//! Search answers in effective state, as the blob backend's does: a promise
//! past its deadline in an origin nothing has swept yet is projected before it
//! is filtered, so a search never depends on whether an expiry has been
//! written down. Searches read every record the owners hold, which is why they
//! are opt-in.
//!
//! # Dependants
//!
//! The node, which parses the query, gathers from local partitions and remote
//! owners, and answers.

use serde_json::Value;

use resonate_core::types::{
    PromiseRecord, PromiseSearchData, PromiseSearchResponseData, ScheduleRecord,
    ScheduleSearchData, ScheduleSearchResponseData, TaskRecord, TaskSearchData,
    TaskSearchResponseData,
};
use resonate_server_blob::kernel::state::Reply;

use crate::keys;
use crate::local::PartitionStore;
use crate::record;

/// The largest page any search returns, matching the SQL handlers.
const MAX_LIMIT: i64 = 1_000;

/// A parsed search.
pub enum Query {
    Promises(PromiseSearchData),
    Tasks(TaskSearchData),
    Schedules(ScheduleSearchData),
}

impl Query {
    fn cursor(&self) -> Option<&str> {
        match self {
            Query::Promises(q) => q.cursor.as_deref(),
            Query::Tasks(q) => q.cursor.as_deref(),
            Query::Schedules(q) => q.cursor.as_deref(),
        }
    }

    /// The page size, or the 400 the SQL handlers give. Ten for schedules,
    /// a hundred otherwise: their defaults.
    pub fn limit(&self) -> Result<i64, Reply> {
        let (limit, default) = match self {
            Query::Promises(q) => (q.limit, 100),
            Query::Tasks(q) => (q.limit, 100),
            Query::Schedules(q) => (q.limit, 10),
        };
        match limit {
            Some(n) if n > MAX_LIMIT => Err(Reply::err(
                400,
                "Invalid 'limit' — must be between 1 and 1000",
            )),
            Some(n) => Ok(n),
            None => Ok(default),
        }
    }
}

/// One owner's answer for one partition: matches after the cursor, by id,
/// at most `limit + 1`.
pub fn local(
    store: &dyn PartitionStore,
    query: &Query,
    limit: i64,
    now: i64,
) -> Result<Vec<(String, Value)>, String> {
    let cursor = query.cursor();
    let after = |id: &str| cursor.is_none_or(|c| id > c);
    let mut out: Vec<(String, Value)> = Vec::new();
    match query {
        Query::Promises(q) => {
            for (key, value) in store.scan(&keys::all_promises_prefix())? {
                let id = keys::id_of_promise_key(&key).ok_or("unreadable promise key")?;
                if !after(&id) {
                    continue;
                }
                let (promise, _) = record::local::decode(&value)?;
                let mut p = promise.to_record(&id);
                p.project(now);
                let state_ok = q.state.map(|s| p.state == s).unwrap_or(true);
                let tags_ok = match &q.tags {
                    Some(want) => want
                        .iter()
                        .all(|(k, v)| p.tags.get(k).is_some_and(|got| got == v)),
                    None => true,
                };
                if state_ok && tags_ok {
                    out.push((id, serde_json::to_value(p).map_err(|e| e.to_string())?));
                }
            }
        }
        Query::Tasks(q) => {
            for (key, value) in store.scan(&keys::all_promises_prefix())? {
                let id = keys::id_of_promise_key(&key).ok_or("unreadable promise key")?;
                if !after(&id) {
                    continue;
                }
                if let (_, Some(task)) = record::local::decode(&value)? {
                    if q.state.map(|s| task.state == s).unwrap_or(true) {
                        let t = task.to_record(&id);
                        out.push((id, serde_json::to_value(t).map_err(|e| e.to_string())?));
                    }
                }
            }
        }
        Query::Schedules(q) => {
            for (key, value) in store.scan(&keys::all_schedules_prefix())? {
                let id = keys::id_of_schedule_key(&key).ok_or("unreadable schedule key")?;
                if !after(&id) {
                    continue;
                }
                let s = record::decode_schedule(&value)?.to_record(&id);
                let tags_ok = match &q.tags {
                    Some(want) => want
                        .iter()
                        .all(|(k, v)| s.promise_tags.get(k).is_some_and(|got| got == v)),
                    None => true,
                };
                if tags_ok {
                    out.push((id, serde_json::to_value(s).map_err(|e| e.to_string())?));
                }
            }
        }
    }
    // Keys sort by origin before id, so sort by id before truncating.
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.truncate(limit.max(0) as usize + 1);
    Ok(out)
}

/// Merge the owners' answers into one page.
pub fn respond(query: &Query, mut items: Vec<(String, Value)>, limit: i64) -> Reply {
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items.dedup_by(|a, b| a.0 == b.0);
    let (page, cursor) = paginate(items, query.cursor(), limit);
    let values = page.into_iter().map(|(_, v)| v);
    let decoded = match query {
        Query::Promises(_) => values
            .map(serde_json::from_value::<PromiseRecord>)
            .collect::<Result<Vec<_>, _>>()
            .map(|promises| Reply::ok(&PromiseSearchResponseData { promises, cursor })),
        Query::Tasks(_) => values
            .map(serde_json::from_value::<TaskRecord>)
            .collect::<Result<Vec<_>, _>>()
            .map(|tasks| Reply::ok(&TaskSearchResponseData { tasks, cursor })),
        Query::Schedules(_) => values
            .map(serde_json::from_value::<ScheduleRecord>)
            .collect::<Result<Vec<_>, _>>()
            .map(|schedules| Reply::ok(&ScheduleSearchResponseData { schedules, cursor })),
    };
    decoded.unwrap_or_else(|e| Reply::err(500, &format!("a search result did not decode: {e}")))
}

/// Take one page after `cursor`, reporting the next cursor only when there is
/// more — `id > cursor`, ordered by id, exactly as the SQL queries page.
fn paginate<T>(
    items: Vec<(String, T)>,
    cursor: Option<&str>,
    limit: i64,
) -> (Vec<(String, T)>, Option<String>) {
    let start = match cursor {
        Some(c) => items
            .iter()
            .position(|(id, _)| id.as_str() > c)
            .unwrap_or(items.len()),
        None => 0,
    };
    let limit = limit.max(0) as usize;
    let mut page: Vec<(String, T)> = items.into_iter().skip(start).take(limit + 1).collect();
    let has_more = page.len() > limit;
    page.truncate(limit);
    let next = if has_more {
        page.last().map(|(id, _)| id.clone())
    } else {
        None
    };
    (page, next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(ids: &[&str]) -> Vec<(String, ())> {
        ids.iter().map(|s| (s.to_string(), ())).collect()
    }

    #[test]
    fn a_page_reports_a_cursor_only_when_there_is_more() {
        let (page, next) = paginate(items(&["a", "b", "c"]), None, 2);
        assert_eq!(page.len(), 2);
        assert_eq!(next.as_deref(), Some("b"));
        let (page, next) = paginate(items(&["a", "b", "c"]), Some("b"), 2);
        assert_eq!(page.len(), 1);
        assert_eq!(next, None);
    }
}
