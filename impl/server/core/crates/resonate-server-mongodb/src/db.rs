//! Plumbing: the transaction, the promise document as every handler reads it,
//! and the settle cascade every path shares.
//!
//! Two MongoDB facts are load-bearing here. A transaction reads from a
//! snapshot and takes no read locks, so a read that a decision depends on has
//! to be a *write* to the same document — which is what [`Tx::lock`] arranges:
//! once this transaction has written a document, any other transaction that
//! writes it fails with a write conflict rather than waiting. And a
//! transaction that fails a write — a write conflict, a duplicate `_id` another
//! writer inserted first — is aborted whole, which the shell reports as
//! [`StorageError::Serialization`] and retries.

use std::collections::{BTreeSet, HashMap, HashSet};

use mongodb::bson::{doc, Bson, Document};
use mongodb::error::{ErrorKind, WriteFailure, TRANSIENT_TRANSACTION_ERROR};
use mongodb::options::ReturnDocument;
use mongodb::{ClientSession, Collection};

use crate::engine::{Outgoing, Scheduled, Timeout};
use crate::{StorageError, StorageResult};
use resonate_core::types::{PromiseRecord, PromiseState, PromiseValue, TaskRecord, TaskState};

pub(crate) type Tags = HashMap<String, String>;

/// The origin is everything before the id's first ':' — spec-level ("an id is
/// an origin and a suffix"), and what the console calls a root.
pub(crate) fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

/// The filter that names one promise: its id, and the shard key derived from
/// it. `origin` is a function of `_id`, so adding it never changes which
/// document matches; what it changes is where a sharded cluster looks — one
/// shard instead of all of them, for every lock, read and write below.
pub(crate) fn key(id: &str) -> Document {
    doc! { "_id": id, "origin": origin_of(id) }
}

/// The filter that names several promises, targeted the same way: `mongos`
/// sends it to the shards that own those origins and no others.
pub(crate) fn keys<S: AsRef<str>>(ids: &[S]) -> Document {
    let ids: Vec<&str> = ids.iter().map(AsRef::as_ref).collect();
    let origins: BTreeSet<&str> = ids.iter().map(|id| origin_of(id)).collect();
    doc! {
        "_id": { "$in": &ids },
        "origin": { "$in": origins.into_iter().collect::<Vec<_>>() },
    }
}

/// `WriteConflict`: another transaction wrote the document first.
const WRITE_CONFLICT: i32 = 112;
/// `DuplicateKey`: two concurrent creates of one id, and this one lost.
const DUPLICATE_KEY: i32 = 11000;

/// The server's error code, wherever in the driver's error it sits.
fn error_code(e: &mongodb::error::Error) -> Option<i32> {
    match e.kind.as_ref() {
        ErrorKind::Command(c) => Some(c.code),
        ErrorKind::Write(WriteFailure::WriteError(w)) => Some(w.code),
        ErrorKind::Write(WriteFailure::WriteConcernError(w)) => Some(w.code),
        _ => None,
    }
}

/// A driver error, as this crate reports it.
///
/// Three things mean "nothing was committed, run it again": an error labelled
/// `TransientTransactionError` (every write conflict inside a transaction is),
/// a bare write conflict, and a duplicate key — which is how two concurrent
/// creates of the same id resolve, the loser re-reading the winner's document
/// on the retry.
pub(crate) fn map_err(e: mongodb::error::Error) -> StorageError {
    if e.contains_label(TRANSIENT_TRANSACTION_ERROR)
        || matches!(error_code(&e), Some(WRITE_CONFLICT | DUPLICATE_KEY))
    {
        return StorageError::Serialization;
    }
    StorageError::Backend(e.to_string())
}

// ─── Maps in a document ──────────────────────────────────────────────────────
//
// Tags and headers are string maps whose keys are caller text, and a MongoDB
// field name is not arbitrary text: a '.' is a path separator to every query
// and update operator. So a map is stored as an array of `{k, v}` pairs,
// sorted by key, which also makes tag containment a plain `$all` of
// `$elemMatch`es that a multikey index on `tags.k, tags.v` can serve.

/// A map as stored: an array of `{k, v}`, sorted by key.
pub(crate) fn pairs(map: &HashMap<String, String>) -> Bson {
    let mut kv: Vec<(&String, &String)> = map.iter().collect();
    kv.sort();
    Bson::Array(
        kv.into_iter()
            .map(|(k, v)| Bson::Document(doc! { "k": k, "v": v }))
            .collect(),
    )
}

/// Headers as stored: `null` for none *and* for empty, which is the wire-level
/// distinction the SQL engines restore with `NULLIF(.., '{}')`.
pub(crate) fn headers(h: Option<&HashMap<String, String>>) -> Bson {
    match h.filter(|h| !h.is_empty()) {
        Some(h) => pairs(h),
        None => Bson::Null,
    }
}

/// The containment filter: every wanted pair is an element of the stored
/// array. `None` when nothing is wanted.
pub(crate) fn contains_pairs(field: &str, wanted: &HashMap<String, String>) -> Option<Document> {
    if wanted.is_empty() {
        return None;
    }
    let mut kv: Vec<(&String, &String)> = wanted.iter().collect();
    kv.sort();
    let all: Vec<Bson> = kv
        .into_iter()
        .map(|(k, v)| Bson::Document(doc! { "$elemMatch": { "k": k, "v": v } }))
        .collect();
    Some(doc! { field: { "$all": all } })
}

pub(crate) fn map_of(d: &Document, key: &str) -> Option<HashMap<String, String>> {
    let Some(Bson::Array(items)) = d.get(key) else {
        return None;
    };
    Some(
        items
            .iter()
            .filter_map(|item| match item {
                Bson::Document(kv) => Some((
                    kv.get_str("k").ok()?.to_string(),
                    kv.get_str("v").ok()?.to_string(),
                )),
                _ => None,
            })
            .collect(),
    )
}

// ─── Reading fields ──────────────────────────────────────────────────────────

pub(crate) fn str_of(d: &Document, key: &str) -> Option<String> {
    match d.get(key) {
        Some(Bson::String(s)) => Some(s.clone()),
        _ => None,
    }
}

pub(crate) fn i64_of(d: &Document, key: &str) -> Option<i64> {
    match d.get(key) {
        Some(Bson::Int64(n)) => Some(*n),
        Some(Bson::Int32(n)) => Some(*n as i64),
        Some(Bson::Double(n)) => Some(*n as i64),
        _ => None,
    }
}

fn bool_of(d: &Document, key: &str) -> bool {
    matches!(d.get(key), Some(Bson::Boolean(true)))
}

fn strs_of(d: &Document, key: &str) -> Vec<String> {
    match d.get(key) {
        Some(Bson::Array(items)) => items
            .iter()
            .filter_map(|b| b.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

pub(crate) fn required_str(d: &Document, key: &str) -> StorageResult<String> {
    str_of(d, key).ok_or_else(|| StorageError::Backend(format!("field {key}: missing")))
}

pub(crate) fn required_i64(d: &Document, key: &str) -> StorageResult<i64> {
    i64_of(d, key).ok_or_else(|| StorageError::Backend(format!("field {key}: missing")))
}

pub(crate) fn parse_promise_state(s: &str) -> PromiseState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt promise state in DB: {}", e))
}

pub(crate) fn parse_task_state(s: &str) -> TaskState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt task state in DB: {}", e))
}

// ─── The promise document ────────────────────────────────────────────────────

/// The promise document, as every handler reads it: promise and task fields
/// together, because a task *is* a promise with a target.
#[derive(Debug, Clone)]
pub(crate) struct PromiseRow {
    pub id: String,
    pub state: String,
    pub param_headers: Option<HashMap<String, String>>,
    pub param_data: Option<String>,
    pub value_headers: Option<HashMap<String, String>>,
    pub value_data: Option<String>,
    pub tags: Tags,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub target: Option<String>,
    pub is_timer: bool,
    pub external: bool,
    pub task_state: Option<String>,
    pub task_version: i64,
    pub retry_timeout_at: Option<i64>,
    pub lease_timeout_at: Option<i64>,
    pub ttl: Option<i64>,
    pub pid: Option<String>,
    pub listeners: Vec<String>,
    /// The awaiters blocked on this promise.
    pub callbacks: Vec<String>,
    /// The settled promises this task has not consumed yet.
    pub resumes: Vec<String>,
}

impl PromiseRow {
    pub(crate) fn from_doc(d: &Document) -> StorageResult<Self> {
        Ok(Self {
            id: required_str(d, "_id")?,
            state: required_str(d, "state")?,
            param_headers: map_of(d, "param_headers"),
            param_data: str_of(d, "param_data"),
            value_headers: map_of(d, "value_headers"),
            value_data: str_of(d, "value_data"),
            tags: map_of(d, "tags").unwrap_or_default(),
            timeout_at: required_i64(d, "timeout_at")?,
            created_at: required_i64(d, "created_at")?,
            settled_at: i64_of(d, "settled_at"),
            target: str_of(d, "target"),
            is_timer: bool_of(d, "is_timer"),
            external: bool_of(d, "external"),
            task_state: str_of(d, "task_state"),
            task_version: i64_of(d, "task_version").unwrap_or(0),
            retry_timeout_at: i64_of(d, "retry_timeout_at"),
            lease_timeout_at: i64_of(d, "lease_timeout_at"),
            ttl: i64_of(d, "ttl"),
            pid: str_of(d, "pid"),
            listeners: strs_of(d, "listeners"),
            callbacks: strs_of(d, "callbacks"),
            resumes: strs_of(d, "resumes"),
        })
    }

    pub(crate) fn resumes_count(&self) -> i64 {
        self.resumes.len() as i64
    }

    pub(crate) fn to_promise_record(&self) -> PromiseRecord {
        PromiseRecord {
            id: self.id.clone(),
            state: parse_promise_state(&self.state),
            param: PromiseValue {
                headers: self.param_headers.clone(),
                data: self.param_data.clone(),
            },
            value: PromiseValue {
                headers: self.value_headers.clone(),
                data: self.value_data.clone(),
            },
            tags: self.tags.clone(),
            timeout_at: self.timeout_at,
            created_at: self.created_at,
            settled_at: self.settled_at,
        }
    }

    pub(crate) fn to_task_record(&self) -> Option<TaskRecord> {
        let state = self.task_state.as_deref()?;
        Some(TaskRecord {
            id: self.id.clone(),
            state: parse_task_state(state),
            version: self.task_version,
            resumes: self.resumes_count(),
            ttl: self.ttl,
            pid: self.pid.clone(),
        })
    }

    pub(crate) fn has_task(&self) -> bool {
        self.task_state.is_some()
    }

    pub(crate) fn is_pending(&self) -> bool {
        self.state == "pending"
    }

    pub(crate) fn task_is(&self, state: &str) -> bool {
        self.task_state.as_deref() == Some(state)
    }

    /// Settling this promise also fulfils its task: it has one, and the task
    /// has not been fulfilled already.
    pub(crate) fn settle_fulfils(&self) -> bool {
        self.has_task() && !self.task_is("fulfilled")
    }
}

/// What `create` writes. The projections of the tags — origin, target, parent,
/// branch, timer, external — are derived here, once, the way the Postgres
/// schema derives them as generated columns.
pub(crate) struct NewPromise<'a> {
    pub id: &'a str,
    pub state: &'a str,
    pub param_headers: Option<&'a HashMap<String, String>>,
    pub param_data: Option<&'a str>,
    pub tags: &'a Tags,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub task_state: Option<&'a str>,
    pub task_version: i64,
    pub retry_timeout_at: Option<i64>,
    pub lease_timeout_at: Option<i64>,
    pub ttl: Option<i64>,
    pub pid: Option<&'a str>,
}

/// The update that hands a task's lifecycle fields back to rest: no
/// deadlines, no lease, no holder.
fn task_rest() -> Document {
    doc! {
        "retry_timeout_at": Bson::Null,
        "lease_timeout_at": Bson::Null,
        "ttl": Bson::Null,
        "pid": Bson::Null,
    }
}

// ─── The transaction ─────────────────────────────────────────────────────────

/// One transaction, and what it has emitted and armed so far.
///
/// The messages and deadlines ride on the transaction rather than on a return
/// type so a retry starts them over: an attempt that did not commit emitted
/// nothing.
pub(crate) struct Tx<'c> {
    session: ClientSession,
    pub(crate) promises: Collection<Document>,
    pub(crate) schedules: Collection<Document>,
    /// Ties the transaction to the lifetime of what a body closure captures, so
    /// the future the closure returns may borrow its captures — see `transact`.
    _captures: std::marker::PhantomData<&'c ()>,
    pub(crate) trt: i64,
    pub(crate) preload_limit: u32,
    emitted: Vec<Outgoing>,
    armed: Vec<Scheduled>,
}

impl<'c> Tx<'c> {
    pub(crate) fn new(
        session: ClientSession,
        promises: Collection<Document>,
        schedules: Collection<Document>,
        trt: i64,
        preload_limit: u32,
    ) -> Self {
        Self {
            session,
            promises,
            schedules,
            trt,
            preload_limit,
            emitted: Vec::new(),
            armed: Vec::new(),
            _captures: std::marker::PhantomData,
        }
    }

    /// Hand the session back for commit, with what the attempt produced.
    pub(crate) fn into_parts(self) -> (ClientSession, Vec<Outgoing>, Vec<Scheduled>) {
        (self.session, self.emitted, self.armed)
    }

    /// Hand the session back after a failed attempt, dropping what it emitted.
    pub(crate) fn into_session(self) -> ClientSession {
        self.session
    }

    // ── statements ──

    /// Every document `filter` matches in `coll`, sorted, at most `limit`.
    pub(crate) async fn find(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
        sort: Document,
        limit: Option<i64>,
    ) -> StorageResult<Vec<Document>> {
        let mut action = coll.find(filter).sort(sort);
        if let Some(n) = limit {
            action = action.limit(n);
        }
        let mut cursor = action.session(&mut self.session).await.map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(d) = cursor.next(&mut self.session).await {
            out.push(d.map_err(map_err)?);
        }
        Ok(out)
    }

    /// An aggregation, run inside the transaction.
    pub(crate) async fn aggregate(
        &mut self,
        coll: &Collection<Document>,
        pipeline: Vec<Document>,
    ) -> StorageResult<Vec<Document>> {
        let mut cursor = coll
            .aggregate(pipeline)
            .session(&mut self.session)
            .await
            .map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(d) = cursor.next(&mut self.session).await {
            out.push(d.map_err(map_err)?);
        }
        Ok(out)
    }

    pub(crate) async fn count(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
    ) -> StorageResult<i64> {
        coll.count_documents(filter)
            .session(&mut self.session)
            .await
            .map(|n| n as i64)
            .map_err(map_err)
    }

    pub(crate) async fn find_one(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
    ) -> StorageResult<Option<Document>> {
        coll.find_one(filter)
            .session(&mut self.session)
            .await
            .map_err(map_err)
    }

    /// Read one document and hold it to commit: the `rev` bump is a write, so
    /// any other transaction writing the document from here on conflicts.
    pub(crate) async fn lock_one(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
    ) -> StorageResult<Option<Document>> {
        coll.find_one_and_update(filter, doc! { "$inc": { "rev": 1_i64 } })
            .return_document(ReturnDocument::After)
            .session(&mut self.session)
            .await
            .map_err(map_err)
    }

    pub(crate) async fn insert(
        &mut self,
        coll: &Collection<Document>,
        d: Document,
    ) -> StorageResult<()> {
        coll.insert_one(d)
            .session(&mut self.session)
            .await
            .map(|_| ())
            .map_err(map_err)
    }

    pub(crate) async fn update_one(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
        update: Document,
    ) -> StorageResult<u64> {
        coll.update_one(filter, update)
            .session(&mut self.session)
            .await
            .map(|r| r.matched_count)
            .map_err(map_err)
    }

    pub(crate) async fn update_many(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
        update: Document,
    ) -> StorageResult<()> {
        coll.update_many(filter, update)
            .session(&mut self.session)
            .await
            .map(|_| ())
            .map_err(map_err)
    }

    pub(crate) async fn delete_one(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
    ) -> StorageResult<bool> {
        coll.delete_one(filter)
            .session(&mut self.session)
            .await
            .map(|r| r.deleted_count > 0)
            .map_err(map_err)
    }

    pub(crate) async fn delete_many(
        &mut self,
        coll: &Collection<Document>,
        filter: Document,
    ) -> StorageResult<()> {
        coll.delete_many(filter)
            .session(&mut self.session)
            .await
            .map(|_| ())
            .map_err(map_err)
    }

    /// `$set` on one promise.
    pub(crate) async fn set_promise(&mut self, id: &str, fields: Document) -> StorageResult<()> {
        let promises = self.promises.clone();
        self.update_one(&promises, key(id), doc! { "$set": fields })
            .await
            .map(|_| ())
    }

    pub(crate) async fn promise_rows(
        &mut self,
        filter: Document,
        sort: Document,
        limit: Option<i64>,
    ) -> StorageResult<Vec<PromiseRow>> {
        let promises = self.promises.clone();
        self.find(&promises, filter, sort, limit)
            .await?
            .iter()
            .map(PromiseRow::from_doc)
            .collect()
    }

    /// The ids `filter` matches, in id order.
    pub(crate) async fn promise_ids(&mut self, filter: Document) -> StorageResult<Vec<String>> {
        let promises = self.promises.clone();
        let mut cursor = promises
            .find(filter)
            .sort(doc! { "_id": 1 })
            .projection(doc! { "_id": 1 })
            .session(&mut self.session)
            .await
            .map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(d) = cursor.next(&mut self.session).await {
            out.push(required_str(&d.map_err(map_err)?, "_id")?);
        }
        Ok(out)
    }

    // ── emissions and deadlines ──

    pub(crate) fn emit(&mut self, m: Outgoing) {
        self.emitted.push(m);
    }

    pub(crate) fn emit_execute(&mut self, address: Option<&str>, task_id: &str, version: i64) {
        if let Some(address) = address {
            self.emit(Outgoing::Execute {
                address: address.to_string(),
                task_id: task_id.to_string(),
                version,
            });
        }
    }

    pub(crate) fn arm(&mut self, at: i64, timeout: Timeout) {
        self.armed.push(Scheduled { at, timeout });
    }

    /// Announce a promise deadline the sweep watches.
    ///
    /// The queue is `state = 'pending' AND external`: every pending promise
    /// that is not internal — one a listener or an awaiter can wait on, or
    /// whose own task is redispatched — is swept eagerly. An internal promise
    /// arms nothing: it times out lazily, the first time a request names it.
    /// Callers arm exactly when they created a pending, external promise.
    pub(crate) fn arm_promise_timeout(&mut self, promise_id: &str, timeout_at: i64) {
        self.arm(
            timeout_at,
            Timeout::PromiseTimeout {
                promise_id: promise_id.to_string(),
            },
        );
    }

    pub(crate) fn arm_retry(&mut self, task_id: &str, at: i64) {
        self.arm(
            at,
            Timeout::TaskRetryTimeout {
                task_id: task_id.to_string(),
            },
        );
    }

    pub(crate) fn arm_lease(&mut self, task_id: &str, pid: &str, at: i64) {
        self.arm(
            at,
            Timeout::TaskLeaseTimeout {
                task_id: task_id.to_string(),
                pid: pid.to_string(),
            },
        );
    }

    // ── reading promises ──

    /// Read one promise and hold it to commit — `SELECT ... FOR UPDATE`, as
    /// far as any other transaction writing it can tell. Every decision below
    /// is made on a row read this way.
    pub(crate) async fn lock(&mut self, id: &str) -> StorageResult<Option<PromiseRow>> {
        let promises = self.promises.clone();
        match self.lock_one(&promises, key(id)).await? {
            Some(d) => Ok(Some(PromiseRow::from_doc(&d)?)),
            None => Ok(None),
        }
    }

    /// Lock several, and read them back in id order. Missing ids are simply
    /// absent from the result; the caller compares counts.
    ///
    /// Two statements where Neo4j needs one: MongoDB has no multi-document
    /// find-and-modify. The read follows the write inside the same
    /// transaction, so it sees the documents as locked.
    pub(crate) async fn lock_many(&mut self, ids: &[String]) -> StorageResult<Vec<PromiseRow>> {
        let ids: Vec<String> = ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let promises = self.promises.clone();
        self.update_many(&promises, keys(&ids), doc! { "$inc": { "rev": 1_i64 } })
            .await?;
        self.promise_rows(keys(&ids), doc! { "_id": 1 }, None).await
    }

    /// Read one promise without locking it. For reads that follow a lock in
    /// the same transaction, and for the searches.
    pub(crate) async fn read(&mut self, id: &str) -> StorageResult<Option<PromiseRow>> {
        let promises = self.promises.clone();
        match self.find_one(&promises, key(id)).await? {
            Some(d) => Ok(Some(PromiseRow::from_doc(&d)?)),
            None => Ok(None),
        }
    }

    // ── writing promises ──

    /// Insert the document. The caller has established, under a lock attempt,
    /// that no document with this id exists; a concurrent creator that got
    /// there first trips the `_id` index or a write conflict, which is a
    /// `Serialization` retry.
    pub(crate) async fn create_promise(&mut self, p: &NewPromise<'_>) -> StorageResult<()> {
        let tags = p.tags;
        let origin = origin_of(p.id);
        let d = doc! {
            "_id": p.id,
            "origin": origin,
            "root": origin == p.id,
            "state": p.state,
            "param_headers": headers(p.param_headers),
            "param_data": p.param_data,
            "value_headers": Bson::Null,
            "value_data": Bson::Null,
            "tags": pairs(tags),
            "timeout_at": p.timeout_at,
            "created_at": p.created_at,
            "settled_at": p.settled_at,
            "target": tags.get("resonate:target"),
            "parent_id": tags.get("resonate:parent"),
            "branch_id": tags.get("resonate:branch"),
            "is_timer": resonate_core::types::is_timer(tags),
            "external": resonate_core::types::is_external(tags),
            "task_state": p.task_state,
            "task_version": p.task_version,
            "retry_timeout_at": p.retry_timeout_at,
            "lease_timeout_at": p.lease_timeout_at,
            "ttl": p.ttl,
            "pid": p.pid,
            "listeners": [],
            "callbacks": [],
            "awaiting": [],
            "resumes": [],
            "rev": 0_i64,
        };
        let promises = self.promises.clone();
        self.insert(&promises, d).await
    }

    /// Wake a suspended awaiter: back to pending, with a fresh retry deadline
    /// and an execute message at its current version.
    pub(crate) async fn wake(&mut self, w: &PromiseRow, now: i64) -> StorageResult<()> {
        let at = now + self.trt;
        let mut fields = task_rest();
        fields.insert("task_state", "pending");
        fields.insert("retry_timeout_at", at);
        self.set_promise(&w.id, fields).await?;
        self.emit_execute(w.target.as_deref(), &w.id, w.task_version);
        self.arm_retry(&w.id, at);
        Ok(())
    }

    /// Move the callback `awaiter -> awaited` from the awaited promise's
    /// `callbacks` to the awaiter's `resumes`: the awaited promise settled.
    /// The awaiter's `awaiting` loses it in the same breath.
    async fn flip(&mut self, awaiter: &str, awaited: &str) -> StorageResult<()> {
        let promises = self.promises.clone();
        self.update_one(
            &promises,
            key(awaited),
            doc! { "$pull": { "callbacks": awaiter } },
        )
        .await?;
        self.update_one(
            &promises,
            key(awaiter),
            doc! {
                "$addToSet": { "resumes": awaited },
                "$pull": { "awaiting": awaited },
            },
        )
        .await?;
        Ok(())
    }

    /// Settle a locked, pending promise and fan out.
    ///
    /// The one cascade every settlement runs — `promise.settle`, `task.fulfill`,
    /// the fenced settle, and expiry. In order: the awaiters are locked, the
    /// document is settled (and its task fulfilled when `fulfilled`), a
    /// fulfilled task leaves every `callbacks` array it is in and drops its
    /// `resumes`, every awaiter's callback is moved to its `resumes` and a
    /// suspended awaiter is woken, and the listeners each get an unblock
    /// carrying the settled record.
    ///
    /// `skip` names awaiters that are being fulfilled in the same batch; their
    /// callbacks are removed by their own settlement rather than moved here,
    /// so an expiring task is not woken by a sibling that expired beside it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn settle_locked(
        &mut self,
        row: &PromiseRow,
        new_state: &str,
        value_headers: Option<&HashMap<String, String>>,
        value_data: Option<&str>,
        settled_at: i64,
        fulfilled: bool,
        skip: &HashSet<String>,
        now: i64,
    ) -> StorageResult<PromiseRow> {
        debug_assert!(row.is_pending(), "settle_locked on a settled promise");

        // Re-read rather than trust `row.callbacks`: an earlier settlement in
        // the same batch may have pulled a sibling out of them since the lock.
        let current = self
            .read(&row.id)
            .await?
            .ok_or_else(|| StorageError::Backend(format!("locked promise {} vanished", row.id)))?;
        let awaiter_ids: Vec<String> = current
            .callbacks
            .into_iter()
            .filter(|w| !skip.contains(w) && *w != row.id)
            .collect();
        let awaiters = self.lock_many(&awaiter_ids).await?;

        let fields = doc! {
            "state": new_state,
            "value_headers": headers(value_headers),
            "value_data": value_data,
            "settled_at": settled_at,
            "listeners": [],
        };
        self.set_promise(&row.id, fields).await?;
        if fulfilled {
            self.fulfil_task(&row.id).await?;
        }

        for w in &awaiters {
            self.flip(&w.id, &row.id).await?;
            if w.task_is("suspended") {
                self.wake(w, now).await?;
            }
        }

        let mut settled = row.clone();
        settled.state = new_state.to_string();
        settled.value_headers = value_headers.filter(|h| !h.is_empty()).cloned();
        settled.value_data = value_data.map(str::to_string);
        settled.settled_at = Some(settled_at);
        settled.listeners = Vec::new();
        if fulfilled {
            settled.task_state = Some("fulfilled".to_string());
            settled.retry_timeout_at = None;
            settled.lease_timeout_at = None;
            settled.ttl = None;
            settled.pid = None;
            settled.resumes = Vec::new();
        }

        let record = settled.to_promise_record();
        for address in &row.listeners {
            self.emit(Outgoing::Unblock {
                address: address.clone(),
                promise: record.clone(),
            });
        }
        Ok(settled)
    }

    /// Expire a batch of locked, pending, past-deadline promises.
    ///
    /// The verdict is the timer tag's: resolved for a sleep, rejected_timedout
    /// for everything else, settled at the deadline itself. Every member whose
    /// task is not yet fulfilled is fulfilled, and is excluded from the fan-out
    /// of the others — the same exclusion the Postgres cascade makes with
    /// `aw NOT IN fulfilled`.
    pub(crate) async fn expire_batch(
        &mut self,
        rows: &[PromiseRow],
        now: i64,
    ) -> StorageResult<()> {
        let fulfilled: HashSet<String> = rows
            .iter()
            .filter(|r| r.settle_fulfils())
            .map(|r| r.id.clone())
            .collect();
        for row in rows {
            let new_state = if row.is_timer {
                "resolved"
            } else {
                "rejected_timedout"
            };
            self.settle_locked(
                row,
                new_state,
                None,
                None,
                row.timeout_at,
                fulfilled.contains(&row.id),
                &fulfilled,
                now,
            )
            .await?;
        }
        Ok(())
    }

    /// The ghost operation, run before every user operation: settle whichever
    /// of the named promises are pending past their deadline.
    ///
    /// An unlocked read first, and locks only for the rows it will expire, so
    /// a read — `promise.get` included — writes nothing it does not have to
    /// and conflicts with nobody. A promise that expires between the read and
    /// the lock is caught by the operation's own lock, which re-reads it, and
    /// by the sweep.
    pub(crate) async fn try_timeout(&mut self, ids: &[&str], now: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let due = self
            .promise_ids({
                let mut filter = keys(ids);
                filter.insert("state", "pending");
                filter.insert("timeout_at", doc! { "$lte": now });
                filter
            })
            .await?;
        if due.is_empty() {
            return Ok(());
        }
        let rows: Vec<PromiseRow> = self
            .lock_many(&due)
            .await?
            .into_iter()
            .filter(|r| r.is_pending() && r.timeout_at <= now)
            .collect();
        if rows.is_empty() {
            return Ok(());
        }
        self.expire_batch(&rows, now).await
    }

    /// Fire the callbacks of an already-settled promise: whatever registered
    /// against it since it settled is moved to `resumes` and, if suspended,
    /// woken.
    pub(crate) async fn process_callbacks(&mut self, id: &str, now: i64) -> StorageResult<()> {
        let Some(row) = self.read(id).await? else {
            return Ok(());
        };
        if row.is_pending() {
            return Ok(());
        }
        let awaiters = self.lock_many(&row.callbacks).await?;
        for w in &awaiters {
            self.flip(&w.id, id).await?;
            if w.task_is("suspended") {
                self.wake(w, now).await?;
            }
        }
        Ok(())
    }

    /// Fulfil a task: its lifecycle fields to rest, its `resumes` dropped, and
    /// its callbacks withdrawn from every promise it was blocked on.
    ///
    /// Those promises are the task's own `awaiting`, read fresh: the reverse
    /// of `callbacks`, kept beside it so the withdrawal names its documents.
    /// A scan for `{callbacks: id}` would find the same ones, but on a sharded
    /// cluster it would write to — and so enlist in the commit — every shard,
    /// on every fulfil.
    pub(crate) async fn fulfil_task(&mut self, id: &str) -> StorageResult<()> {
        let promises = self.promises.clone();
        let awaiting = self
            .find_one(&promises, key(id))
            .await?
            .map(|d| strs_of(&d, "awaiting"))
            .unwrap_or_default();
        let mut fields = task_rest();
        fields.insert("task_state", "fulfilled");
        fields.insert("resumes", Bson::Array(Vec::new()));
        fields.insert("awaiting", Bson::Array(Vec::new()));
        self.set_promise(id, fields).await?;
        if awaiting.is_empty() {
            return Ok(());
        }
        self.update_many(
            &promises,
            keys(&awaiting),
            doc! { "$pull": { "callbacks": id } },
        )
        .await
    }

    /// Drop a task's ready callbacks: `resumes = '{}'`.
    pub(crate) async fn clear_resumes(&mut self, id: &str) -> StorageResult<()> {
        self.set_promise(id, doc! { "resumes": [] }).await
    }

    /// Link an awaiter to an awaited promise: the awaited's `callbacks` gains
    /// the awaiter, and the awaiter's `awaiting` the awaited, unless the
    /// callback is already there — waiting in `callbacks`, or already fired
    /// into the awaiter's `resumes`.
    pub(crate) async fn link(&mut self, awaiter: &str, awaited: &str) -> StorageResult<()> {
        let Some(w) = self.read(awaiter).await? else {
            return Ok(());
        };
        if w.resumes.iter().any(|r| r == awaited) {
            return Ok(());
        }
        let promises = self.promises.clone();
        self.update_one(
            &promises,
            key(awaited),
            doc! { "$addToSet": { "callbacks": awaiter } },
        )
        .await?;
        self.update_one(
            &promises,
            key(awaiter),
            doc! { "$addToSet": { "awaiting": awaited } },
        )
        .await
        .map(|_| ())
    }

    /// Record a ready callback on an awaiter: its `resumes` gains the awaited
    /// promise, whether or not it was ever in the awaited's `callbacks`.
    pub(crate) async fn mark_ready(&mut self, awaiter: &str, awaited: &str) -> StorageResult<()> {
        self.flip(awaiter, awaited).await
    }

    /// The branch siblings a task response preloads.
    ///
    /// The sibling query is not narrowed to the task's origin: nothing makes
    /// a branch's members share one, so on a sharded cluster this read asks
    /// every shard. It is a read, so the shards it reaches commit as read-only
    /// participants.
    pub(crate) async fn compute_preload(&mut self, id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let promises = self.promises.clone();
        let branch = self
            .find_one(&promises, key(id))
            .await?
            .and_then(|d| str_of(&d, "branch_id"));
        let Some(branch) = branch else {
            return Ok(Vec::new());
        };
        Ok(self
            .promise_rows(
                doc! { "branch_id": branch, "_id": { "$ne": id } },
                doc! { "_id": 1 },
                Some(self.preload_limit as i64),
            )
            .await?
            .iter()
            .map(PromiseRow::to_promise_record)
            .collect())
    }
}
