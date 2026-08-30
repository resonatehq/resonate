//! Schedules: one CAS'd object each, plus one timer object for the next run.
//!
//! A schedule is not origin-scoped — it has no promises and no tasks — so it
//! gets its own key rather than living in a document. Its timer object sits in
//! the same prefix as the origins', marked `sched:`, so one poller finds both.
//!
//! # Firing order
//!
//! The promise is created *first*, then the schedule advances. A crash between
//! the two refires the same occurrence, and `Req::ScheduleFire` is idempotent
//! on the promise id, so the second attempt is a no-op. The reverse order would
//! lose the occurrence outright — the schedule would have moved past a promise
//! that was never created.
//!
//! The advance is a conditional write, and losing it means another node fired
//! the same occurrence. Nothing to do: the promise exists either way.
//!
//! # Idempotence guard
//!
//! A schedule fires an occurrence only while its `next_run_at` still *is* that
//! occurrence. This is `process_schedule_timeout`'s guard
//! (`persistence_sqlite.rs:1332-1340`), which checks the timeout row still
//! holds the deadline that fired, and it is what makes a duplicate timer key
//! harmless.
//!
//! # Dependencies
//!
//! The store, for the schedule objects and their timer keys; the timer queue,
//! which mirrors every timer key written here; the applier, to submit
//! `Req::ScheduleFire` so the promise is created through the same serialized
//! path as everything else, and for `KeySpace`.
//!
//! # Dependants
//!
//! The timer poller fires due schedules through the [`ScheduleFirer`] impl;
//! `Server` routes `schedule.get/create/delete` here; the scan service reads
//! `list_all` for `schedule.search`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kernel::state::{Reply, Req, ScheduleFireData, TAG_TARGET};
use crate::metrics;
use resonate_core::types::{
    PromiseValue, ScheduleCreateData, ScheduleRecord, ScheduleResponseData,
};
use resonate_core::util;
use resonate_core::Unavailable;

use super::applier::{KeySpace, OriginActors};
use super::store::{Etag, Store, StoreError};
use super::timer_queue::TimerQueue;
use super::timerd::ScheduleFirer;

/// A schedule as stored.
///
/// Payload halves are kept apart, as the SQL columns keep them, and every map
/// is a `BTreeMap`, so the encoding is a function of the state — which is what
/// the write law needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleDoc {
    /// Format version, so a newer layout can be recognized rather than
    /// misread.
    pub v: u32,
    pub cron: String,
    pub promise_id: String,
    pub promise_timeout: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub promise_param_headers: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub promise_param_data: Option<String>,
    pub promise_tags: BTreeMap<String, String>,
    pub created_at: i64,
    pub next_run_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<i64>,
}

/// Current format version of a schedule object.
pub const SCHEDULE_FORMAT_VERSION: u32 = 1;

impl ScheduleDoc {
    fn promise_param(&self) -> PromiseValue {
        PromiseValue {
            headers: self
                .promise_param_headers
                .as_ref()
                .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            data: self.promise_param_data.clone(),
        }
    }

    pub fn to_record(&self, id: &str) -> ScheduleRecord {
        ScheduleRecord {
            id: id.to_string(),
            cron: self.cron.clone(),
            promise_id: self.promise_id.clone(),
            promise_timeout: self.promise_timeout,
            promise_param: self.promise_param(),
            promise_tags: self
                .promise_tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            created_at: self.created_at,
            next_run_at: self.next_run_at,
            last_run_at: self.last_run_at,
        }
    }
}

fn encode(doc: &ScheduleDoc) -> Vec<u8> {
    serde_json::to_vec(doc).expect("a schedule serializes")
}

fn decode(bytes: &[u8]) -> Result<ScheduleDoc, String> {
    let doc: ScheduleDoc = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if doc.v != SCHEDULE_FORMAT_VERSION {
        return Err(format!("unsupported schedule version {}", doc.v));
    }
    Ok(doc)
}

/// The origin a promise id belongs to: everything before the first `':'`.
fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

pub struct ScheduleService {
    store: Arc<dyn Store>,
    applier: Arc<OriginActors>,
    timers: Arc<TimerQueue>,
    keys: KeySpace,
}

impl ScheduleService {
    pub fn new(
        store: Arc<dyn Store>,
        applier: Arc<OriginActors>,
        timers: Arc<TimerQueue>,
        keys: KeySpace,
    ) -> Self {
        Self {
            store,
            applier,
            timers,
            keys,
        }
    }

    async fn load(&self, id: &str) -> Result<Option<(ScheduleDoc, Etag)>, Unavailable> {
        let key = self.keys.sched_key(id);
        match self.store.get(&key).await {
            Ok(Some((bytes, etag))) => {
                let doc = decode(&bytes)
                    .map_err(|e| Unavailable::new(format!("schedule {key} unreadable: {e}")))?;
                Ok(Some((doc, etag)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(Unavailable::new(e.to_string())),
        }
    }

    /// Every schedule, by id. Powers `schedule.search` and `debug.snap`.
    ///
    /// One GET per schedule: acceptable while schedules are few, and the seam
    /// where a secondary index would go.
    pub async fn list_all(&self) -> Result<Vec<(String, ScheduleDoc)>, Unavailable> {
        let keys = self
            .store
            .list(&self.keys.sched_prefix(), usize::MAX)
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let id = match self.keys.id_of_sched_key(&key) {
                Some(id) => id,
                None => continue,
            };
            if let Some((doc, _)) = self.load(&id).await? {
                out.push((id, doc));
            }
        }
        Ok(out)
    }

    /// `schedule.create`. Idempotent on the id: an existing schedule is
    /// reported unchanged, as `INSERT OR IGNORE` plus a read gives.
    pub async fn create(&self, r: &ScheduleCreateData, now: i64) -> Result<Reply, Unavailable> {
        // Every promise this schedule fires carries the target, so it is held
        // to the same standard promise.create holds one to.
        if let Some(addr) = r.promise_tags.get(TAG_TARGET) {
            if !resonate_core::is_valid_address(addr) {
                return Ok(Reply::err(400, "Invalid resonate:target address"));
            }
        }
        if !util::is_valid_cron(&r.cron) {
            return Ok(Reply::err(400, "Invalid cron expression"));
        }
        if let Some((doc, _)) = self.load(&r.id).await? {
            return Ok(Reply::ok(&ScheduleResponseData {
                schedule: doc.to_record(&r.id),
            }));
        }
        let next_run_at = util::compute_next_cron(&r.cron, now);
        let doc = ScheduleDoc {
            v: SCHEDULE_FORMAT_VERSION,
            cron: r.cron.clone(),
            promise_id: r.promise_id.clone(),
            promise_timeout: r.promise_timeout,
            promise_param_headers: r
                .promise_param
                .headers
                .as_ref()
                .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            promise_param_data: r.promise_param.data.clone(),
            promise_tags: r
                .promise_tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            created_at: now,
            next_run_at,
            last_run_at: None,
        };
        // The timer first, as everywhere else: a schedule with no timer would
        // never run, while a timer with no schedule is collected on its first
        // sweep.
        let timer_key = self.keys.sched_timer_key(&r.id, next_run_at);
        self.store
            .put(&timer_key, Vec::new())
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        self.timers.arm(next_run_at, timer_key);
        match self
            .store
            .put_if_none_match(&self.keys.sched_key(&r.id), encode(&doc))
            .await
        {
            Ok(_) => Ok(Reply::ok(&ScheduleResponseData {
                schedule: doc.to_record(&r.id),
            })),
            Err(StoreError::PreconditionFailed) => {
                // Someone created it first. Report theirs.
                match self.load(&r.id).await? {
                    Some((existing, _)) => Ok(Reply::ok(&ScheduleResponseData {
                        schedule: existing.to_record(&r.id),
                    })),
                    None => Err(Unavailable::new("schedule vanished during create")),
                }
            }
            Err(e) => Err(Unavailable::new(e.to_string())),
        }
    }

    pub async fn get(&self, id: &str) -> Result<Reply, Unavailable> {
        match self.load(id).await? {
            Some((doc, _)) => Ok(Reply::ok(&ScheduleResponseData {
                schedule: doc.to_record(id),
            })),
            None => Ok(Reply::err(404, "Schedule not found")),
        }
    }

    pub async fn delete(&self, id: &str) -> Result<Reply, Unavailable> {
        let doc = match self.load(id).await? {
            Some((doc, _)) => doc,
            None => return Ok(Reply::err(404, "Schedule not found")),
        };
        // The schedule first: a timer with no schedule is inert and collected,
        // whereas a schedule with no timer would sit there forever.
        self.store
            .delete(&self.keys.sched_key(id))
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        let timer_key = self.keys.sched_timer_key(id, doc.next_run_at);
        match self.store.delete(&timer_key).await {
            Ok(()) => self.timers.disarm(doc.next_run_at, &timer_key),
            Err(e) => {
                tracing::debug!(schedule_id = %id, error = %e, "Schedule timer key not collected; its armed entry will collect it");
            }
        }
        Ok(Reply::status(200, serde_json::json!({})))
    }

    /// Fire the occurrence at `deadline`, noticed at `now`.
    pub async fn fire(&self, id: &str, deadline: i64, now: i64) -> Result<(), Unavailable> {
        let (doc, etag) = match self.load(id).await? {
            Some(pair) => pair,
            // Deleted since the key was written. The poller collects the key.
            None => return Ok(()),
        };
        if doc.next_run_at != deadline {
            // Already fired by someone, or the schedule has moved on. The key
            // is stale; letting the poller collect it is the whole response.
            return Ok(());
        }

        let promise_id = doc
            .promise_id
            .replace("{{.id}}", id)
            .replace("{{.timestamp}}", &deadline.to_string());
        // The stamps processing_timeouts applies before handing the promise to
        // the backend (`processing_timeouts.rs:70-81`).
        let mut tags = doc.promise_tags.clone();
        tags.insert("resonate:schedule".to_string(), id.to_string());
        for key in [
            "resonate:origin",
            "resonate:branch",
            "resonate:parent",
            "resonate:prefix",
        ] {
            tags.insert(key.to_string(), promise_id.clone());
        }

        let fire = Req::ScheduleFire(ScheduleFireData {
            id: promise_id.clone(),
            timeout_at: deadline + doc.promise_timeout,
            param: doc.promise_param(),
            tags,
            fired_at: deadline,
        });
        // The promise first. A crash here refires the same occurrence, and
        // ScheduleFire is idempotent on the promise id.
        self.applier
            .submit(origin_of(&promise_id), fire, now)
            .await?;
        metrics::SCHEDULE_PROMISES_TOTAL.inc();

        // Advance one occurrence, exactly as the SQL path does — a schedule
        // that fell far behind catches up one sweep at a time rather than
        // firing a burst.
        let next_run_at = util::compute_next_cron(&doc.cron, deadline);
        let mut next = doc.clone();
        next.last_run_at = Some(deadline);
        next.next_run_at = next_run_at;

        let timer_key = self.keys.sched_timer_key(id, next_run_at);
        self.store
            .put(&timer_key, Vec::new())
            .await
            .map_err(|e| Unavailable::new(e.to_string()))?;
        self.timers.arm(next_run_at, timer_key);
        match self
            .store
            .put_if_match(&self.keys.sched_key(id), encode(&next), &etag)
            .await
        {
            Ok(_) => {
                tracing::info!(schedule_id = %id, fired_at = deadline, next_run_at, "Schedule fired");
                Ok(())
            }
            // Another node advanced it. The promise exists either way.
            Err(StoreError::PreconditionFailed) => Ok(()),
            Err(e) => Err(Unavailable::new(e.to_string())),
        }
    }
}

#[async_trait]
impl ScheduleFirer for ScheduleService {
    async fn fire(&self, id: &str, deadline: i64, now: i64) -> Result<(), Unavailable> {
        ScheduleService::fire(self, id, deadline, now).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applier::ApplierCfg;
    use crate::cache::{DocCache, MemDocCache};
    use crate::codec;
    use crate::sender::{NullRouter, Sender};
    use crate::store::ObjectStoreAdapter;
    use crate::timerd::{Timerd, TimerdCfg};
    use serde_json::json;

    const W: &str = "http://worker:9999";
    // Every minute, so an occurrence is 60_000 ms wide.
    const CRON: &str = "* * * * *";

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    struct Rig {
        store: Arc<dyn Store>,
        applier: Arc<OriginActors>,
        schedules: Arc<ScheduleService>,
        sender: Arc<Sender>,
        timers: Arc<TimerQueue>,
    }

    fn rig() -> Rig {
        let store: Arc<dyn Store> = Arc::new(ObjectStoreAdapter::in_memory());
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(16));
        let sender = Arc::new(Sender::new(Arc::new(NullRouter), true));
        // One queue, as `Server::build` wires it.
        let timers = Arc::new(TimerQueue::new());
        let applier = Arc::new(OriginActors::new(
            Arc::clone(&store),
            cache,
            Arc::clone(&sender),
            Arc::clone(&timers),
            keys(),
            ApplierCfg::default(),
        ));
        let schedules = Arc::new(ScheduleService::new(
            Arc::clone(&store),
            Arc::clone(&applier),
            Arc::clone(&timers),
            keys(),
        ));
        Rig {
            store,
            applier,
            schedules,
            sender,
            timers,
        }
    }

    fn create_data(id: &str, promise_id: &str, promise_timeout: i64) -> ScheduleCreateData {
        serde_json::from_value(json!({
            "id": id, "cron": CRON, "promiseId": promise_id,
            "promiseTimeout": promise_timeout, "promiseParam": {},
            "promiseTags": { "resonate:target": W }
        }))
        .unwrap()
    }

    async fn promise_doc(r: &Rig, origin: &str) -> Option<crate::kernel::state::OriginDoc> {
        let bytes = r.store.get(&keys().doc_key(origin)).await.unwrap()?;
        Some(codec::decode(&bytes.0, origin).unwrap())
    }

    #[tokio::test]
    async fn creating_a_schedule_stores_it_and_arms_its_next_run() {
        let r = rig();
        let reply = r
            .schedules
            .create(
                &create_data("s0", "p-{{.id}}-{{.timestamp}}", 60_000),
                1_000,
            )
            .await
            .unwrap();
        assert_eq!(reply.status, 200);
        assert_eq!(reply.data["schedule"]["id"], "s0");
        assert_eq!(reply.data["schedule"]["createdAt"], 1_000);
        let next = reply.data["schedule"]["nextRunAt"].as_i64().unwrap();
        assert_eq!(next, util::compute_next_cron(CRON, 1_000));
        assert!(reply.data["schedule"].get("lastRunAt").is_none());

        let armed = r.store.list(&keys().timer_prefix(), 10).await.unwrap();
        assert_eq!(armed, vec![keys().sched_timer_key("s0", next)]);
    }

    #[tokio::test]
    async fn creating_the_same_schedule_twice_reports_the_first() {
        let r = rig();
        let first = r
            .schedules
            .create(&create_data("s0", "p-{{.id}}", 60_000), 1_000)
            .await
            .unwrap();
        let second = r
            .schedules
            .create(&create_data("s0", "different", 999), 5_000)
            .await
            .unwrap();
        assert_eq!(second.status, 200);
        assert_eq!(first.data, second.data);
    }

    #[tokio::test]
    async fn an_invalid_cron_is_a_400() {
        let r = rig();
        let mut d = create_data("s0", "p", 60_000);
        d.cron = "not a cron".into();
        let reply = r.schedules.create(&d, 0).await.unwrap();
        assert_eq!(reply.status, 400);
        assert_eq!(reply.data, json!("Invalid cron expression"));
        assert!(r
            .store
            .list(&keys().sched_prefix(), 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn an_invalid_target_is_a_400() {
        let r = rig();
        let mut d = create_data("s0", "p", 60_000);
        d.promise_tags
            .insert(TAG_TARGET.to_string(), "not a url".to_string());
        let reply = r.schedules.create(&d, 0).await.unwrap();
        assert_eq!(reply.status, 400);
    }

    #[tokio::test]
    async fn getting_an_unknown_schedule_is_a_404() {
        let r = rig();
        let reply = r.schedules.get("s0").await.unwrap();
        assert_eq!(reply.status, 404);
        assert_eq!(reply.data, json!("Schedule not found"));
    }

    #[tokio::test]
    async fn deleting_removes_the_schedule_and_its_timer() {
        let r = rig();
        r.schedules
            .create(&create_data("s0", "p-{{.id}}", 60_000), 1_000)
            .await
            .unwrap();
        let reply = r.schedules.delete("s0").await.unwrap();
        assert_eq!(reply.status, 200);
        assert_eq!(r.schedules.get("s0").await.unwrap().status, 404);
        assert!(r
            .store
            .list(&keys().timer_prefix(), 10)
            .await
            .unwrap()
            .is_empty());
        // Deleting twice is a 404, not a second success.
        assert_eq!(r.schedules.delete("s0").await.unwrap().status, 404);
    }

    #[tokio::test]
    async fn schedule_timers_are_mirrored_into_the_queue() {
        let r = rig();
        let reply = r
            .schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 60_000), 1_000)
            .await
            .unwrap();
        let due = reply.data["schedule"]["nextRunAt"].as_i64().unwrap();
        assert_eq!(r.timers.next_deadline(), Some(due));

        // Firing arms the next occurrence; the fired key's entry is collected
        // by whoever swept it, not here.
        r.schedules.fire("s0", due, due).await.unwrap();
        let next = util::compute_next_cron(CRON, due);
        assert!(
            r.timers
                .take_due(next)
                .contains(&keys().sched_timer_key("s0", next)),
            "the next occurrence is armed"
        );

        // Deleting disarms the current occurrence's entry.
        let r = rig();
        let reply = r
            .schedules
            .create(&create_data("s1", "p-{{.id}}", 60_000), 1_000)
            .await
            .unwrap();
        assert_eq!(reply.status, 200);
        r.schedules.delete("s1").await.unwrap();
        assert!(r.timers.is_empty());
    }

    #[tokio::test]
    async fn firing_creates_the_promise_dated_by_its_occurrence() {
        let r = rig();
        let reply = r
            .schedules
            .create(
                &create_data("s0", "p-{{.id}}-{{.timestamp}}", 600_000),
                1_000,
            )
            .await
            .unwrap();
        let due = reply.data["schedule"]["nextRunAt"].as_i64().unwrap();

        // Noticed well after it came due.
        let now = due + 45_000;
        r.schedules.fire("s0", due, now).await.unwrap();

        let promise_id = format!("p-s0-{due}");
        let doc = promise_doc(&r, &promise_id).await.expect("promise created");
        let p = &doc.promises[&promise_id];
        // Dated by the occurrence, not by the sweep that noticed it.
        assert_eq!(p.created_at, due);
        assert_eq!(p.timeout_at, due + 600_000);
        assert_eq!(p.tags["resonate:schedule"], "s0");
        assert_eq!(p.tags["resonate:origin"], promise_id);
        assert_eq!(p.tags["resonate:branch"], promise_id);
        assert_eq!(p.tags["resonate:parent"], promise_id);
        assert_eq!(p.tags["resonate:prefix"], promise_id);
        // The first dispatch is timed from the sweep, not from created_at.
        assert_eq!(doc.tasks[&promise_id].retry_at, Some(now + 30_000));
        assert_eq!(r.sender.snapshot().len(), 1);
    }

    #[tokio::test]
    async fn firing_advances_one_occurrence() {
        let r = rig();
        let created = r
            .schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 600_000), 1_000)
            .await
            .unwrap();
        let due = created.data["schedule"]["nextRunAt"].as_i64().unwrap();
        r.schedules.fire("s0", due, due).await.unwrap();

        let after = r.schedules.get("s0").await.unwrap();
        assert_eq!(after.data["schedule"]["lastRunAt"], due);
        assert_eq!(
            after.data["schedule"]["nextRunAt"],
            util::compute_next_cron(CRON, due)
        );
        let armed = r.store.list(&keys().timer_prefix(), 10).await.unwrap();
        let next = keys().sched_timer_key("s0", util::compute_next_cron(CRON, due));
        assert!(armed.contains(&next), "the next occurrence is armed");
        // Firing does not collect the key that fired — the poller does, once
        // fire returns, which is what makes a crash mid-fire refire.
        assert!(armed.contains(&keys().sched_timer_key("s0", due)));
    }

    #[tokio::test]
    async fn firing_the_same_occurrence_twice_creates_one_promise() {
        // The guard: a schedule fires an occurrence only while next_run_at
        // still is that occurrence.
        let r = rig();
        let created = r
            .schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 600_000), 1_000)
            .await
            .unwrap();
        let due = created.data["schedule"]["nextRunAt"].as_i64().unwrap();
        r.schedules.fire("s0", due, due).await.unwrap();
        let before = r.schedules.get("s0").await.unwrap().data;
        r.schedules.fire("s0", due, due + 1).await.unwrap();
        assert_eq!(r.schedules.get("s0").await.unwrap().data, before);

        let promise_id = format!("p-{due}");
        let doc = promise_doc(&r, &promise_id).await.unwrap();
        assert_eq!(doc.promises.len(), 1);
    }

    #[tokio::test]
    async fn firing_a_deleted_schedule_does_nothing() {
        let r = rig();
        r.schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 600_000), 1_000)
            .await
            .unwrap();
        r.schedules.delete("s0").await.unwrap();
        r.schedules.fire("s0", 60_000, 60_000).await.unwrap();
        assert!(r
            .store
            .list(&keys().doc_prefix(), 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn a_promise_created_past_its_own_deadline_is_born_settled() {
        let r = rig();
        // A one-millisecond promise timeout, noticed long after the occurrence.
        let created = r
            .schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 1), 1_000)
            .await
            .unwrap();
        let due = created.data["schedule"]["nextRunAt"].as_i64().unwrap();
        r.schedules.fire("s0", due, due + 10_000).await.unwrap();

        let promise_id = format!("p-{due}");
        let doc = promise_doc(&r, &promise_id).await.unwrap();
        let p = &doc.promises[&promise_id];
        assert_eq!(p.state.as_str(), "rejected_timedout");
        // created_at stays the occurrence even when born settled — unlike an
        // ordinary create, which stamps the deadline.
        assert_eq!(p.created_at, due);
        assert_eq!(p.settled_at, Some(due + 1));
        assert_eq!(doc.tasks[&promise_id].state.as_str(), "fulfilled");
        assert!(r.sender.snapshot().is_empty());
    }

    #[tokio::test]
    async fn the_poller_fires_a_due_schedule() {
        let r = rig();
        let created = r
            .schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 600_000), 1_000)
            .await
            .unwrap();
        let due = created.data["schedule"]["nextRunAt"].as_i64().unwrap();
        let td = Timerd::new(
            Arc::clone(&r.store),
            Arc::clone(&r.applier),
            Some(Arc::clone(&r.schedules) as Arc<dyn ScheduleFirer>),
            Arc::clone(&r.timers),
            keys(),
            TimerdCfg::default(),
        );
        assert_eq!(td.round(due).await.unwrap(), 1);
        assert!(promise_doc(&r, &format!("p-{due}")).await.is_some());
        // The fired key was collected; the next occurrence and the new
        // promise's own retry deadline are armed.
        let armed = r.store.list(&keys().timer_prefix(), 10).await.unwrap();
        assert!(
            !armed.contains(&keys().sched_timer_key("s0", due)),
            "the fired key was collected"
        );
        assert!(armed.contains(&keys().sched_timer_key("s0", util::compute_next_cron(CRON, due))));
    }

    #[tokio::test]
    async fn a_schedule_that_fell_behind_catches_up_one_round_at_a_time() {
        let r = rig();
        let created = r
            .schedules
            .create(&create_data("s0", "p-{{.timestamp}}", 600_000), 1_000)
            .await
            .unwrap();
        let due = created.data["schedule"]["nextRunAt"].as_i64().unwrap();
        let td = Timerd::new(
            Arc::clone(&r.store),
            Arc::clone(&r.applier),
            Some(Arc::clone(&r.schedules) as Arc<dyn ScheduleFirer>),
            Arc::clone(&r.timers),
            keys(),
            TimerdCfg::default(),
        );
        // Three occurrences behind: one per round, as the SQL path does.
        let now = due + 3 * 60_000;
        for expected in 1..=3 {
            assert_eq!(td.round(now).await.unwrap(), 1);
            assert_eq!(
                r.store.list(&keys().doc_prefix(), 100).await.unwrap().len(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn every_schedule_can_be_listed() {
        let r = rig();
        for id in ["s2", "s0", "s1"] {
            r.schedules
                .create(&create_data(id, "p-{{.id}}", 60_000), 1_000)
                .await
                .unwrap();
        }
        let all = r.schedules.list_all().await.unwrap();
        assert_eq!(
            all.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["s0", "s1", "s2"],
            "listing is ascending by id"
        );
    }

    #[tokio::test]
    async fn a_schedule_document_round_trips() {
        let doc = ScheduleDoc {
            v: SCHEDULE_FORMAT_VERSION,
            cron: CRON.into(),
            promise_id: "p-{{.id}}".into(),
            promise_timeout: 60_000,
            promise_param_headers: Some([("a".to_string(), "1".to_string())].into_iter().collect()),
            promise_param_data: Some("aGk=".into()),
            promise_tags: [(TAG_TARGET.to_string(), W.to_string())]
                .into_iter()
                .collect(),
            created_at: 1,
            next_run_at: 60_000,
            last_run_at: Some(30_000),
        };
        assert_eq!(decode(&encode(&doc)).unwrap(), doc);
        // Deterministic bytes: the write law depends on it.
        assert_eq!(encode(&doc), encode(&doc.clone()));
    }

    #[tokio::test]
    async fn a_schedule_from_a_newer_version_is_refused() {
        let r = rig();
        let mut raw = serde_json::to_value(ScheduleDoc {
            v: SCHEDULE_FORMAT_VERSION,
            cron: CRON.into(),
            promise_id: "p".into(),
            promise_timeout: 1,
            promise_param_headers: None,
            promise_param_data: None,
            promise_tags: BTreeMap::new(),
            created_at: 0,
            next_run_at: 0,
            last_run_at: None,
        })
        .unwrap();
        raw["v"] = json!(99);
        r.store
            .put(&keys().sched_key("s0"), serde_json::to_vec(&raw).unwrap())
            .await
            .unwrap();
        let err = r.schedules.get("s0").await.expect_err("refused");
        assert!(
            err.to_string().contains("unsupported schedule version"),
            "{err}"
        );
    }
}
