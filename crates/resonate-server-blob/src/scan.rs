//! Whole-store reads: the three searches and `debug.snap`.
//!
//! # Contract
//!
//! Everything here is a LIST of the document prefix followed by a decode of
//! each object, filtered and projected in memory. That is O(origins) GETs per
//! query, which is honest for a small deployment and for the differential
//! suite, and plainly wrong for a large one — the production caveat, stated
//! rather than hidden (and why the searches can be disabled by config). This
//! module is the seam where a secondary index would go, and adding one would
//! not touch the applier or the kernel.
//!
//! Reads are cache-first but never *write* the cache. Only the applier does
//! that, so a scan racing a write cannot leave a stale entry behind for the
//! writer to trip over.
//!
//! Searches see stored state, not effective state: a promise whose deadline has
//! passed but which nothing has named yet still reads as pending. The SQL
//! backends behave identically — a row changes when `try_timeout` or the
//! timeout sweep touches it, not when someone looks at it.
//!
//! # Dependencies
//!
//! The store and the codec to list and read every document, the cache
//! (read-only), the schedule service for `schedule.search`, and the sender for
//! the `messages` half of `debug.snap`.
//!
//! # Dependants
//!
//! `Server` alone, which routes `promise.search`, `task.search`,
//! `schedule.search` and `debug.snap` here.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::kernel::state::{OriginDoc, Reply};
use resonate_core::types::{
    PromiseRecord, PromiseSearchData, PromiseSearchResponseData, ScheduleRecord,
    ScheduleSearchData, ScheduleSearchResponseData, Snapshot, SnapshotCallback, SnapshotListener,
    SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskRecord, TaskSearchData,
    TaskSearchResponseData,
};
use resonate_core::Unavailable;

use super::applier::KeySpace;
use super::cache::DocCache;
use super::codec;
use super::schedules::ScheduleService;
use super::sender::Sender;
use super::store::Store;

/// The largest page any search will return, matching the SQL handlers.
const MAX_LIMIT: i64 = 1_000;

pub struct ScanService {
    store: Arc<dyn Store>,
    cache: Arc<dyn DocCache>,
    schedules: Arc<ScheduleService>,
    sender: Arc<Sender>,
    keys: KeySpace,
}

impl ScanService {
    pub fn new(
        store: Arc<dyn Store>,
        cache: Arc<dyn DocCache>,
        schedules: Arc<ScheduleService>,
        sender: Arc<Sender>,
        keys: KeySpace,
    ) -> Self {
        Self {
            store,
            cache,
            schedules,
            sender,
            keys,
        }
    }

    /// Every origin document, by origin.
    async fn documents(&self) -> Result<BTreeMap<String, OriginDoc>, Unavailable> {
        let keys = self
            .store
            .list(&self.keys.doc_prefix(), usize::MAX)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        let mut out = BTreeMap::new();
        for key in keys {
            let origin = match self.keys.origin_of_doc_key(&key) {
                Some(origin) => origin,
                None => continue,
            };
            if let Some(cached) = self.cache.get(&origin) {
                out.insert(origin, cached.doc.as_ref().clone());
                continue;
            }
            match self.store.get(&key).await {
                Ok(Some((bytes, _))) => {
                    let doc = codec::decode(&bytes, &origin)
                        .map_err(|e| Unavailable::new(format!("document {key} unreadable: {e}")))?;
                    out.insert(origin, doc);
                }
                // Deleted between the listing and the read. It is simply gone.
                Ok(None) => {}
                Err(e) => return Err(Unavailable::new(e.to_string())),
            }
        }
        Ok(out)
    }

    /// Every promise, by id, across every origin.
    async fn promises(&self) -> Result<Vec<(String, PromiseRecord)>, Unavailable> {
        let mut out: Vec<(String, PromiseRecord)> = Vec::new();
        for doc in self.documents().await?.values() {
            for (id, p) in &doc.promises {
                out.push((id.clone(), p.to_record(id)));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Searches
    // -----------------------------------------------------------------------

    pub async fn search_promises(&self, q: &PromiseSearchData) -> Result<Reply, Unavailable> {
        let limit = match resolve_limit(q.limit, 100) {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };
        let matches: Vec<PromiseRecord> = self
            .promises()
            .await?
            .into_iter()
            .map(|(_, p)| p)
            .filter(|p| q.state.map(|s| p.state == s).unwrap_or(true))
            .filter(|p| match &q.tags {
                Some(want) => want
                    .iter()
                    .all(|(k, v)| p.tags.get(k).is_some_and(|got| got == v)),
                None => true,
            })
            .collect();
        let (promises, cursor) = paginate(matches, q.cursor.as_deref(), limit, |p| &p.id);
        Ok(Reply::ok(&PromiseSearchResponseData { promises, cursor }))
    }

    pub async fn search_tasks(&self, q: &TaskSearchData) -> Result<Reply, Unavailable> {
        let limit = match resolve_limit(q.limit, 100) {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };
        let mut matches: Vec<TaskRecord> = Vec::new();
        for doc in self.documents().await?.values() {
            for (id, t) in &doc.tasks {
                if q.state.map(|s| t.state == s).unwrap_or(true) {
                    matches.push(t.to_record(id));
                }
            }
        }
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        let (tasks, cursor) = paginate(matches, q.cursor.as_deref(), limit, |t| &t.id);
        Ok(Reply::ok(&TaskSearchResponseData { tasks, cursor }))
    }

    pub async fn search_schedules(&self, q: &ScheduleSearchData) -> Result<Reply, Unavailable> {
        // Ten, not a hundred: op_schedule_search's default.
        let limit = match resolve_limit(q.limit, 10) {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };
        let mut matches: Vec<ScheduleRecord> = self
            .schedules
            .list_all()
            .await?
            .into_iter()
            .map(|(id, doc)| doc.to_record(&id))
            .filter(|s| match &q.tags {
                Some(want) => want
                    .iter()
                    .all(|(k, v)| s.promise_tags.get(k).is_some_and(|got| got == v)),
                None => true,
            })
            .collect();
        matches.sort_by(|a, b| a.id.cmp(&b.id));
        let (schedules, cursor) = paginate(matches, q.cursor.as_deref(), limit, |s| &s.id);
        Ok(Reply::ok(&ScheduleSearchResponseData { schedules, cursor }))
    }

    // -----------------------------------------------------------------------
    // Snapshot
    // -----------------------------------------------------------------------

    /// The whole store, in the shape `debug.snap` compares.
    ///
    /// Each field is the projection its SQL counterpart makes, including the
    /// ones that are not simply "all of them": `promiseTimeouts` holds only
    /// deadlines that are actually armed, `callbacks` holds only the
    /// registrations that have *not* fired (`WHERE NOT ready` — a fired one is
    /// visible as a task's `resumes` count instead), and a task's `ttl` and
    /// `pid` appear only while it holds a lease.
    pub async fn snapshot(&self) -> Result<Snapshot, Unavailable> {
        let docs = self.documents().await?;

        let mut promises: Vec<PromiseRecord> = Vec::new();
        let mut promise_timeouts: Vec<SnapshotPromiseTimeout> = Vec::new();
        let mut callbacks: Vec<SnapshotCallback> = Vec::new();
        let mut listeners: Vec<SnapshotListener> = Vec::new();
        let mut tasks: Vec<TaskRecord> = Vec::new();
        let mut task_timeouts: Vec<SnapshotTaskTimeout> = Vec::new();

        for doc in docs.values() {
            for (id, p) in &doc.promises {
                promises.push(p.to_record(id));
                if p.timeout_armed() {
                    promise_timeouts.push(SnapshotPromiseTimeout {
                        id: id.clone(),
                        timeout: p.timeout_at,
                    });
                }
                for awaiter in &p.callbacks {
                    callbacks.push(SnapshotCallback {
                        awaiter: awaiter.clone(),
                        awaited: id.clone(),
                    });
                }
                for address in &p.listeners {
                    listeners.push(SnapshotListener {
                        promise_id: id.clone(),
                        address: address.clone(),
                    });
                }
            }
            for (id, t) in &doc.tasks {
                tasks.push(t.to_record(id));
                if let Some(at) = t.retry_at {
                    task_timeouts.push(SnapshotTaskTimeout {
                        id: id.clone(),
                        timeout_type: 0,
                        timeout: at,
                    });
                }
                if let Some(at) = t.lease_at {
                    task_timeouts.push(SnapshotTaskTimeout {
                        id: id.clone(),
                        timeout_type: 1,
                        timeout: at,
                    });
                }
            }
        }

        promises.sort_by(|a, b| a.id.cmp(&b.id));
        promise_timeouts.sort_by(|a, b| a.id.cmp(&b.id));
        callbacks.sort_by(|a, b| a.awaiter.cmp(&b.awaiter).then(a.awaited.cmp(&b.awaited)));
        listeners.sort_by(|a, b| {
            a.promise_id
                .cmp(&b.promise_id)
                .then(a.address.cmp(&b.address))
        });
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        task_timeouts.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(Snapshot {
            promises,
            promise_timeouts,
            callbacks,
            listeners,
            tasks,
            task_timeouts,
            messages: self.sender.snapshot(),
        })
    }
}

/// A page limit, or the 400 the SQL handlers give.
fn resolve_limit(limit: Option<i64>, default: i64) -> Result<i64, Reply> {
    match limit {
        Some(n) if n > MAX_LIMIT => Err(Reply::err(
            400,
            "Invalid 'limit' — must be between 1 and 1000",
        )),
        Some(n) => Ok(n),
        None => Ok(default),
    }
}

/// Take one page after `cursor`, reporting the next cursor only when there is
/// more — `id > cursor`, ordered by id, exactly as the SQL queries page.
fn paginate<T>(
    items: Vec<T>,
    cursor: Option<&str>,
    limit: i64,
    id_of: impl Fn(&T) -> &String,
) -> (Vec<T>, Option<String>) {
    let start = match cursor {
        Some(c) => items
            .iter()
            .position(|item| id_of(item).as_str() > c)
            .unwrap_or(items.len()),
        None => 0,
    };
    let limit = limit.max(0) as usize;
    let mut page: Vec<T> = items.into_iter().skip(start).take(limit + 1).collect();
    let has_more = page.len() > limit;
    page.truncate(limit);
    let next = if has_more {
        page.last().map(|item| id_of(item).clone())
    } else {
        None
    };
    (page, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applier::{ApplierCfg, OriginActors};
    use crate::cache::MemDocCache;
    use crate::kernel::state::Req;
    use crate::sender::NullRouter;
    use crate::store::ObjectStoreAdapter;
    use serde_json::{json, Value};

    const W: &str = "http://worker:9999";
    const PID: &str = "pid-1";
    const TTL: i64 = 60_000;

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    struct Rig {
        applier: Arc<OriginActors>,
        schedules: Arc<ScheduleService>,
        scan: ScanService,
    }

    fn rig() -> Rig {
        let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(64));
        let sender = Arc::new(Sender::new(Arc::new(NullRouter), "http://server", true));
        let applier = Arc::new(OriginActors::new(
            Arc::clone(&store),
            Arc::clone(&cache),
            Arc::clone(&sender),
            Arc::new(crate::timer_queue::TimerQueue::new()),
            keys(),
            ApplierCfg::default(),
        ));
        let schedules = Arc::new(ScheduleService::new(
            Arc::clone(&store),
            Arc::clone(&applier),
            Arc::new(crate::timer_queue::TimerQueue::new()),
            keys(),
        ));
        let scan = ScanService::new(store, cache, Arc::clone(&schedules), sender, keys());
        Rig {
            applier,
            schedules,
            scan,
        }
    }

    async fn submit(r: &Rig, origin: &str, req: Req, now: i64) -> Reply {
        r.applier.submit(origin, req, now).await.unwrap()
    }

    fn parse<T: serde::de::DeserializeOwned>(v: Value) -> T {
        serde_json::from_value(v).unwrap()
    }

    /// Build a kernel request from a protocol kind and its data.
    fn to_req(kind: &str, data: Value) -> Req {
        match kind {
            "promise.create" => Req::PromiseCreate(parse(data)),
            "promise.settle" => Req::PromiseSettle(parse(data)),
            "promise.register_callback" => Req::PromiseRegisterCallback(parse(data)),
            "promise.register_listener" => Req::PromiseRegisterListener(parse(data)),
            "task.create" => Req::TaskCreate(parse(data)),
            "task.acquire" => Req::TaskAcquire(parse(data)),
            "task.suspend" => Req::TaskSuspend(parse(data)),
            other => panic!("unhandled kind {other}"),
        }
    }

    #[tokio::test]
    async fn an_empty_store_snapshots_to_empty() {
        let r = rig();
        let snap = r.scan.snapshot().await.unwrap();
        assert!(snap.promises.is_empty());
        assert!(snap.tasks.is_empty());
        assert!(snap.messages.is_empty());
    }

    #[tokio::test]
    async fn a_snapshot_spans_every_origin() {
        let r = rig();
        for origin in ["alpha", "beta", "gamma"] {
            submit(
                &r,
                origin,
                to_req(
                    "promise.create",
                    json!({ "id": format!("{origin}:a"), "timeoutAt": 100_000,
                            "param": {}, "tags": {} }),
                ),
                0,
            )
            .await;
        }
        let snap = r.scan.snapshot().await.unwrap();
        assert_eq!(
            snap.promises
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha:a", "beta:a", "gamma:a"],
            "ordered by id across origins"
        );
    }

    // --- searches ---------------------------------------------------------

    async fn with_promises(r: &Rig) {
        for (n, tags) in [
            (0, json!({})),
            (1, json!({ "resonate:target": W })),
            (2, json!({ "k": "v" })),
            (3, json!({})),
        ] {
            submit(
                r,
                "o",
                to_req(
                    "promise.create",
                    json!({ "id": format!("o:p{n}"), "timeoutAt": 500_000,
                            "param": {}, "tags": tags }),
                ),
                1_000,
            )
            .await;
        }
        submit(
            r,
            "o",
            to_req(
                "promise.settle",
                json!({ "id": "o:p0", "state": "resolved", "value": {} }),
            ),
            2_000,
        )
        .await;
    }

    #[tokio::test]
    async fn searching_promises_filters_by_state() {
        let r = rig();
        with_promises(&r).await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "state": "resolved", "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(reply.status, 200);
        let ids: Vec<&str> = reply.data["promises"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["o:p0"]);
    }

    #[tokio::test]
    async fn searching_promises_requires_every_filter_tag_to_match() {
        let r = rig();
        with_promises(&r).await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "tags": { "k": "v" }, "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(reply.data["promises"].as_array().unwrap().len(), 1);
        let reply = r
            .scan
            .search_promises(&parse(
                json!({ "tags": { "k": "v", "other": "x" }, "limit": 10 }),
            ))
            .await
            .unwrap();
        assert!(reply.data["promises"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_search_pages_by_id_and_reports_a_cursor_only_when_there_is_more() {
        let r = rig();
        with_promises(&r).await;
        let first = r
            .scan
            .search_promises(&parse(json!({ "limit": 2 })))
            .await
            .unwrap();
        assert_eq!(
            first.data["promises"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["o:p0", "o:p1"]
        );
        assert_eq!(first.data["cursor"], "o:p1");

        let second = r
            .scan
            .search_promises(&parse(json!({ "limit": 2, "cursor": "o:p1" })))
            .await
            .unwrap();
        assert_eq!(
            second.data["promises"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["o:p2", "o:p3"]
        );
        assert!(
            second.data.get("cursor").is_none(),
            "the last page has none"
        );
    }

    #[tokio::test]
    async fn a_cursor_past_the_end_returns_nothing() {
        let r = rig();
        with_promises(&r).await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "limit": 10, "cursor": "zzz" })))
            .await
            .unwrap();
        assert!(reply.data["promises"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_oversized_limit_is_a_400() {
        let r = rig();
        for reply in [
            r.scan
                .search_promises(&parse(json!({ "limit": 1_001 })))
                .await
                .unwrap(),
            r.scan
                .search_tasks(&parse(json!({ "limit": 1_001 })))
                .await
                .unwrap(),
            r.scan
                .search_schedules(&parse(json!({ "limit": 1_001 })))
                .await
                .unwrap(),
        ] {
            assert_eq!(reply.status, 400);
            assert_eq!(
                reply.data,
                json!("Invalid 'limit' — must be between 1 and 1000")
            );
        }
    }

    #[tokio::test]
    async fn searching_tasks_filters_by_state() {
        let r = rig();
        submit(
            &r,
            "o",
            to_req(
                "promise.create",
                json!({ "id": "o:a", "timeoutAt": 500_000, "param": {},
                        "tags": { "resonate:target": W } }),
            ),
            1_000,
        )
        .await;
        submit(
            &r,
            "o",
            to_req(
                "task.create",
                json!({ "pid": PID, "ttl": TTL, "action": {
                    "kind": "promise.create", "head": {}, "data": {
                        "id": "o:b", "timeoutAt": 500_000, "param": {},
                        "tags": { "resonate:target": W } } } }),
            ),
            1_000,
        )
        .await;
        let pending = r
            .scan
            .search_tasks(&parse(json!({ "state": "pending", "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(
            pending.data["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["o:a"]
        );
        let all = r
            .scan
            .search_tasks(&parse(json!({ "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(all.data["tasks"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn searching_schedules_defaults_to_ten_per_page() {
        let r = rig();
        for n in 0..12 {
            let d = json!({
                "id": format!("s{n:02}"), "cron": "* * * * *",
                "promiseId": "p-{{.id}}", "promiseTimeout": 60_000,
                "promiseParam": {}, "promiseTags": { "resonate:target": W }
            });
            r.schedules.create(&parse(d), 1_000).await.unwrap();
        }
        let reply = r.scan.search_schedules(&parse(json!({}))).await.unwrap();
        assert_eq!(reply.data["schedules"].as_array().unwrap().len(), 10);
        assert_eq!(reply.data["cursor"], "s09");
    }

    #[tokio::test]
    async fn searching_schedules_filters_by_promise_tags() {
        let r = rig();
        for (n, extra) in [(0, json!({})), (1, json!({ "team": "core" }))] {
            let mut tags = serde_json::Map::new();
            tags.insert("resonate:target".into(), json!(W));
            if let Some(obj) = extra.as_object() {
                for (k, v) in obj {
                    tags.insert(k.clone(), v.clone());
                }
            }
            let d = json!({
                "id": format!("s{n}"), "cron": "* * * * *",
                "promiseId": "p-{{.id}}", "promiseTimeout": 60_000,
                "promiseParam": {}, "promiseTags": Value::Object(tags)
            });
            r.schedules.create(&parse(d), 1_000).await.unwrap();
        }
        let reply = r
            .scan
            .search_schedules(&parse(json!({ "tags": { "team": "core" } })))
            .await
            .unwrap();
        assert_eq!(
            reply.data["schedules"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| s["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["s1"]
        );
    }

    #[tokio::test]
    async fn a_search_reports_stored_state_not_effective_state() {
        // The deadline has passed but nothing has named the promise, so no row
        // has changed — the SQL backends read the same way.
        let r = rig();
        submit(
            &r,
            "o",
            to_req(
                "promise.create",
                json!({ "id": "o:a", "timeoutAt": 5_000, "param": {}, "tags": {} }),
            ),
            1_000,
        )
        .await;
        let reply = r
            .scan
            .search_promises(&parse(json!({ "state": "pending", "limit": 10 })))
            .await
            .unwrap();
        assert_eq!(reply.data["promises"].as_array().unwrap().len(), 1);
    }
}
