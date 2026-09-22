//! Plumbing: the transaction, the promise node as every handler reads it, and
//! the settle cascade every path shares.
//!
//! Two Neo4j facts are load-bearing here. A statement that modifies a node
//! takes a write lock on it and holds it to commit, but a predicate in the same
//! statement is evaluated *before* the lock — so a read that a decision depends
//! on has to happen after a write to the same node, which is what [`Tx::lock`]
//! arranges. And a transaction that fails a conditional write — a deadlock, a
//! uniqueness constraint another writer got to first — is rolled back whole,
//! which the shell reports as [`StorageError::Serialization`] and retries.

use std::collections::{BTreeSet, HashMap, HashSet};

use neo4rs::{query, BoltType, Query, Row, Txn};

use crate::engine::{Outgoing, Scheduled, Timeout};
use crate::{StorageError, StorageResult};
use resonate_core::types::{PromiseRecord, PromiseState, PromiseValue, TaskRecord, TaskState};

pub(crate) type Tags = HashMap<String, String>;

/// The origin is everything before the id's first ':' — spec-level ("an id is
/// an origin and a suffix"), and what the console calls a root.
pub(crate) fn origin_of(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

/// The lineage is everything after the first ':' — the promise's place in
/// its tree (`1.2.1`), and the empty string for a root. Stored so a graph
/// tool has something short to caption a node with.
pub(crate) fn lineage_of(id: &str) -> &str {
    id.split_once(':').map(|(_, l)| l).unwrap_or("")
}

/// A driver error, as this crate reports it.
///
/// Three things mean "nothing was committed, run it again": a transient error
/// (deadlock detection lives there), a lock client stopped by another
/// transaction's termination, and a uniqueness constraint violation — which is
/// how two concurrent creates of the same id resolve, the loser re-reading the
/// winner's node on the retry.
///
/// Classified on the rendered error rather than on the variant. A failure the
/// server reports while a result is being pulled reaches this crate as the
/// driver's "unexpected response for PULL" string, with the Neo4j code inside
/// it, rather than as a typed `Neo4j` error — and a deadlock is detected
/// exactly then, while the locking statement streams its rows.
pub(crate) fn map_err(e: neo4rs::Error) -> StorageError {
    let rendered = match &e {
        neo4rs::Error::Neo4j(err) => format!("{}: {}", err.code(), err.message()),
        other => other.to_string(),
    };
    const RETRYABLE: &[&str] = &[
        "TransientError",
        "DeadlockDetected",
        "LockClientStopped",
        "ConstraintValidationFailed",
        "Transaction.Terminated",
    ];
    if RETRYABLE.iter().any(|m| rendered.contains(m)) {
        return StorageError::Serialization;
    }
    StorageError::Backend(rendered)
}

// ─── JSON on the node ────────────────────────────────────────────────────────
//
// Neo4j properties are scalars and homogeneous lists — no maps. Headers and
// tags are therefore JSON strings on the node, and the tag map is also
// flattened into `tag_kv`, a list of JSON-encoded `[key, value]` pairs, which
// is what makes a containment search (`tags @> $1`) expressible: every wanted
// pair must be an element of the list.

pub(crate) fn tags_json(tags: &Tags) -> String {
    serde_json::to_string(tags).expect("a string map always serializes")
}

pub(crate) fn tag_pairs(tags: &Tags) -> Vec<String> {
    let mut out: Vec<String> = tags
        .iter()
        .map(|(k, v)| serde_json::to_string(&[k, v]).expect("two strings always serialize"))
        .collect();
    out.sort();
    out
}

/// Headers as stored: `None` for none *and* for empty, which is the wire-level
/// distinction the SQL engines restore with `NULLIF(.., '{}')`.
pub(crate) fn headers_json(h: Option<&HashMap<String, String>>) -> Option<String> {
    h.filter(|h| !h.is_empty())
        .map(|h| serde_json::to_string(h).expect("a string map always serializes"))
}

fn parse_headers(s: Option<String>) -> Option<HashMap<String, String>> {
    s.map(|h| serde_json::from_str(&h).unwrap_or_default())
}

fn parse_tags(s: Option<String>) -> Tags {
    s.map(|t| serde_json::from_str(&t).unwrap_or_default())
        .unwrap_or_default()
}

pub(crate) fn parse_promise_state(s: &str) -> PromiseState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt promise state in DB: {}", e))
}

pub(crate) fn parse_task_state(s: &str) -> TaskState {
    s.parse()
        .unwrap_or_else(|e| panic!("corrupt task state in DB: {}", e))
}

// ─── The promise node ────────────────────────────────────────────────────────

/// Every property a read of a promise projects, aliased to its own name, plus
/// the count of the node's ready `AWAITS` edges — the `resumes` a task record
/// reports, which is a traversal here where it is an array length in SQL.
pub(crate) fn p_cols(a: &str) -> String {
    const FIELDS: &[&str] = &[
        "id",
        "state",
        "param_headers",
        "param_data",
        "value_headers",
        "value_data",
        "tags",
        "timeout_at",
        "created_at",
        "settled_at",
        "target",
        "is_timer",
        "external",
        "task_state",
        "task_version",
        "retry_timeout_at",
        "lease_timeout_at",
        "ttl",
        "pid",
        "listeners",
    ];
    let mut cols: Vec<String> = FIELDS.iter().map(|f| format!("{a}.{f} AS {f}")).collect();
    cols.push(format!(
        "size([({a})-[ready_edge:AWAITS]->() WHERE ready_edge.ready = true | 1]) AS resumes"
    ));
    cols.join(", ")
}

/// The promise node, as every handler reads it: promise and task columns
/// together, because a task *is* a promise with a target.
#[derive(Debug, Clone)]
pub(crate) struct PromiseRow {
    pub id: String,
    pub state: String,
    pub param_headers: Option<String>,
    pub param_data: Option<String>,
    pub value_headers: Option<String>,
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
    /// Ready `AWAITS` edges out of this node.
    pub resumes: i64,
}

fn col<T: serde::de::DeserializeOwned>(row: &Row, key: &str) -> StorageResult<T> {
    row.get::<T>(key)
        .map_err(|e| StorageError::Backend(format!("column {key}: {e}")))
}

impl PromiseRow {
    pub(crate) fn from_row(row: &Row) -> StorageResult<Self> {
        Ok(Self {
            id: col(row, "id")?,
            state: col(row, "state")?,
            param_headers: col(row, "param_headers")?,
            param_data: col(row, "param_data")?,
            value_headers: col(row, "value_headers")?,
            value_data: col(row, "value_data")?,
            tags: parse_tags(col(row, "tags")?),
            timeout_at: col(row, "timeout_at")?,
            created_at: col(row, "created_at")?,
            settled_at: col(row, "settled_at")?,
            target: col(row, "target")?,
            is_timer: col::<Option<bool>>(row, "is_timer")?.unwrap_or(false),
            external: col::<Option<bool>>(row, "external")?.unwrap_or(false),
            task_state: col(row, "task_state")?,
            task_version: col::<Option<i64>>(row, "task_version")?.unwrap_or(0),
            retry_timeout_at: col(row, "retry_timeout_at")?,
            lease_timeout_at: col(row, "lease_timeout_at")?,
            ttl: col(row, "ttl")?,
            pid: col(row, "pid")?,
            listeners: col::<Option<Vec<String>>>(row, "listeners")?.unwrap_or_default(),
            resumes: col::<Option<i64>>(row, "resumes")?.unwrap_or(0),
        })
    }

    pub(crate) fn to_promise_record(&self) -> PromiseRecord {
        PromiseRecord {
            id: self.id.clone(),
            state: parse_promise_state(&self.state),
            param: PromiseValue {
                headers: parse_headers(self.param_headers.clone()),
                data: self.param_data.clone(),
            },
            value: PromiseValue {
                headers: parse_headers(self.value_headers.clone()),
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
            resumes: self.resumes,
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
    pub param_headers: Option<String>,
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

// ─── The transaction ─────────────────────────────────────────────────────────

/// One transaction, and what it has emitted and armed so far.
///
/// The messages and deadlines ride on the transaction rather than on a return
/// type so a retry starts them over: an attempt that did not commit emitted
/// nothing.
pub(crate) struct Tx<'c> {
    txn: Txn,
    /// Ties the transaction to the lifetime of what a body closure captures, so
    /// the future the closure returns may borrow its captures — see `transact`.
    _captures: std::marker::PhantomData<&'c ()>,
    pub(crate) trt: i64,
    pub(crate) preload_limit: u32,
    emitted: Vec<Outgoing>,
    armed: Vec<Scheduled>,
}

impl<'c> Tx<'c> {
    pub(crate) fn new(txn: Txn, trt: i64, preload_limit: u32) -> Self {
        Self {
            txn,
            trt,
            preload_limit,
            emitted: Vec::new(),
            armed: Vec::new(),
            _captures: std::marker::PhantomData,
        }
    }

    /// Hand the transaction back for commit, with what it produced.
    pub(crate) fn into_parts(self) -> (Txn, Vec<Outgoing>, Vec<Scheduled>) {
        (self.txn, self.emitted, self.armed)
    }

    /// Roll back. Best effort: after a failed statement the connection is
    /// already in a failed state, and the driver resets it on its way back to
    /// the pool whatever this returns.
    pub(crate) async fn abort(self) {
        let _ = self.txn.rollback().await;
    }

    // ── statements ──

    pub(crate) async fn run(&mut self, q: Query) -> StorageResult<()> {
        self.txn.run(q).await.map_err(map_err)
    }

    pub(crate) async fn rows(&mut self, q: Query) -> StorageResult<Vec<Row>> {
        let mut stream = self.txn.execute(q).await.map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(row) = stream.next(self.txn.handle()).await.map_err(map_err)? {
            out.push(row);
        }
        Ok(out)
    }

    pub(crate) async fn one(&mut self, q: Query) -> StorageResult<Option<Row>> {
        Ok(self.rows(q).await?.into_iter().next())
    }

    pub(crate) async fn promise_rows(&mut self, q: Query) -> StorageResult<Vec<PromiseRow>> {
        self.rows(q)
            .await?
            .iter()
            .map(PromiseRow::from_row)
            .collect()
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

    /// Read one promise and hold its write lock to commit.
    ///
    /// `SET p.rev = p.rev + 1` is the lock: Neo4j acquires it before evaluating
    /// the right-hand side, and the `RETURN` that follows reads under it. This
    /// is `SELECT ... FOR UPDATE`, and every decision below is made on a row
    /// read this way.
    pub(crate) async fn lock(&mut self, id: &str) -> StorageResult<Option<PromiseRow>> {
        let q = query(&format!(
            "MATCH (p:Promise {{id: $id}}) SET p.rev = p.rev + 1 RETURN {}",
            p_cols("p")
        ))
        .param("id", id);
        Ok(self.promise_rows(q).await?.into_iter().next())
    }

    /// Lock several, lowest id first, so two transactions after the same set
    /// take them in the same order. Missing ids are simply absent from the
    /// result; the caller compares counts.
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
        let q = query(&format!(
            "UNWIND $ids AS id MATCH (p:Promise {{id: id}}) SET p.rev = p.rev + 1 RETURN {}",
            p_cols("p")
        ))
        .param("ids", ids);
        let mut rows = self.promise_rows(q).await?;
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// Read one promise without locking it. For reads that follow a lock in
    /// the same transaction, and for the searches.
    pub(crate) async fn read(&mut self, id: &str) -> StorageResult<Option<PromiseRow>> {
        let q = query(&format!(
            "MATCH (p:Promise {{id: $id}}) RETURN {}",
            p_cols("p")
        ))
        .param("id", id);
        Ok(self.promise_rows(q).await?.into_iter().next())
    }

    /// The ids blocked on `id`: the awaited promise's `callbacks`, as the
    /// sources of its unready `AWAITS` edges.
    pub(crate) async fn awaiters_of(&mut self, id: &str) -> StorageResult<Vec<String>> {
        let q = query(
            "MATCH (w:Promise)-[r:AWAITS]->(p:Promise {id: $id}) WHERE r.ready = false \
             RETURN w.id AS id ORDER BY id",
        )
        .param("id", id);
        self.rows(q)
            .await?
            .iter()
            .map(|r| col::<String>(r, "id"))
            .collect()
    }

    // ── writing promises ──

    /// Create the node. The caller has established, under a lock attempt, that
    /// no node with this id exists; a concurrent creator that got there first
    /// trips the uniqueness constraint, which is a `Serialization` retry.
    pub(crate) async fn create_promise(&mut self, p: &NewPromise<'_>) -> StorageResult<()> {
        let tags = p.tags;
        let target = tags.get("resonate:target").cloned();
        let parent_id = tags.get("resonate:parent").cloned();
        let branch_id = tags.get("resonate:branch").cloned();
        let is_timer = resonate_core::types::is_timer(tags);
        let external = resonate_core::types::is_external(tags);

        let mut props: HashMap<&str, BoltType> = HashMap::new();
        props.insert("id", p.id.into());
        props.insert("origin", origin_of(p.id).into());
        props.insert("lineage", lineage_of(p.id).into());
        props.insert("state", p.state.into());
        props.insert("param_headers", p.param_headers.clone().into());
        props.insert("param_data", p.param_data.map(str::to_string).into());
        props.insert("tags", tags_json(tags).into());
        props.insert("tag_kv", tag_pairs(tags).into());
        props.insert("timeout_at", p.timeout_at.into());
        props.insert("created_at", p.created_at.into());
        props.insert("settled_at", p.settled_at.into());
        props.insert("target", target.into());
        props.insert("parent_id", parent_id.clone().into());
        props.insert("branch_id", branch_id.into());
        props.insert("is_timer", is_timer.into());
        props.insert("external", external.into());
        props.insert("task_state", p.task_state.map(str::to_string).into());
        props.insert("task_version", p.task_version.into());
        props.insert("retry_timeout_at", p.retry_timeout_at.into());
        props.insert("lease_timeout_at", p.lease_timeout_at.into());
        props.insert("ttl", p.ttl.into());
        props.insert("pid", p.pid.map(str::to_string).into());
        props.insert("listeners", Vec::<String>::new().into());
        props.insert("rev", 0i64.into());

        self.run(query("CREATE (p:Promise) SET p = $props").param("props", props))
            .await?;

        // The call tree, as an edge. Nothing reads it; a parent that does not
        // exist yet leaves no edge, and the tag stays the source of truth.
        if let Some(parent) = parent_id.filter(|parent| parent != p.id) {
            self.run(
                query(
                    "MATCH (c:Promise {id: $id}), (par:Promise {id: $parent}) \
                     MERGE (c)-[:CHILD_OF]->(par)",
                )
                .param("id", p.id)
                .param("parent", parent),
            )
            .await?;
        }
        Ok(())
    }

    /// Wake a suspended awaiter: back to pending, with a fresh retry deadline
    /// and an execute message at its current version.
    pub(crate) async fn wake(&mut self, w: &PromiseRow, now: i64) -> StorageResult<()> {
        let at = now + self.trt;
        self.run(
            query(
                "MATCH (p:Promise {id: $id}) SET p.task_state = 'pending', \
                 p.retry_timeout_at = $at, p.lease_timeout_at = null, p.ttl = null, p.pid = null",
            )
            .param("id", w.id.as_str())
            .param("at", at),
        )
        .await?;
        self.emit_execute(w.target.as_deref(), &w.id, w.task_version);
        self.arm_retry(&w.id, at);
        Ok(())
    }

    /// Settle a locked, pending promise and fan out.
    ///
    /// The one cascade every settlement runs — `promise.settle`, `task.fulfill`,
    /// the fenced settle, and expiry. In order: the awaiters are locked, the
    /// node is settled (and its task fulfilled when `fulfilled`), a fulfilled
    /// task's own `AWAITS` edges are deleted, every awaiter's edge into this
    /// node is flipped to ready and a suspended awaiter is woken, and the
    /// listeners each get an unblock carrying the settled record.
    ///
    /// `skip` names awaiters that are being fulfilled in the same batch; their
    /// edges are deleted by their own settlement rather than flipped here, so
    /// an expiring task is not woken by a sibling that expired beside it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn settle_locked(
        &mut self,
        row: &PromiseRow,
        new_state: &str,
        value_headers: Option<String>,
        value_data: Option<&str>,
        settled_at: i64,
        fulfilled: bool,
        skip: &HashSet<String>,
        now: i64,
    ) -> StorageResult<PromiseRow> {
        debug_assert!(row.is_pending(), "settle_locked on a settled promise");

        let awaiter_ids: Vec<String> = self
            .awaiters_of(&row.id)
            .await?
            .into_iter()
            .filter(|w| !skip.contains(w) && *w != row.id)
            .collect();
        let awaiters = self.lock_many(&awaiter_ids).await?;

        let mut set = String::from(
            "MATCH (p:Promise {id: $id}) SET p.state = $state, p.value_headers = $vh, \
             p.value_data = $vd, p.settled_at = $settled_at, p.listeners = []",
        );
        if fulfilled {
            set.push_str(
                ", p.task_state = 'fulfilled', p.retry_timeout_at = null, \
                 p.lease_timeout_at = null, p.ttl = null, p.pid = null",
            );
        }
        self.run(
            query(&set)
                .param("id", row.id.as_str())
                .param("state", new_state)
                .param("vh", value_headers.clone())
                .param("vd", value_data.map(str::to_string))
                .param("settled_at", settled_at),
        )
        .await?;

        if fulfilled {
            // A fulfilled task leaves every promise's callbacks and drops its
            // own resumes: its edges, both ready and not.
            self.run(
                query("MATCH (p:Promise {id: $id})-[r:AWAITS]->() DELETE r")
                    .param("id", row.id.as_str()),
            )
            .await?;
        }

        for w in &awaiters {
            self.run(
                query(
                    "MATCH (w:Promise {id: $w})-[r:AWAITS]->(p:Promise {id: $p}) \
                     SET r.ready = true",
                )
                .param("w", w.id.as_str())
                .param("p", row.id.as_str()),
            )
            .await?;
            if w.task_is("suspended") {
                self.wake(w, now).await?;
            }
        }

        let mut settled = row.clone();
        settled.state = new_state.to_string();
        settled.value_headers = value_headers;
        settled.value_data = value_data.map(str::to_string);
        settled.settled_at = Some(settled_at);
        settled.listeners = Vec::new();
        if fulfilled {
            settled.task_state = Some("fulfilled".to_string());
            settled.retry_timeout_at = None;
            settled.lease_timeout_at = None;
            settled.ttl = None;
            settled.pid = None;
            settled.resumes = 0;
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
    /// An unlocked read first, and locks only for the rows it will expire —
    /// the `FOR UPDATE` in the Postgres cascade covers the selected rows and
    /// nothing else. Locking every named promise here made every read, a
    /// `promise.get` included, a party to the write locks of everything else
    /// touching that promise, and under contention that is where the retries
    /// ran out. A promise that expires between the read and the lock is
    /// caught by the operation's own lock, which re-reads it, and by the
    /// sweep.
    pub(crate) async fn try_timeout(&mut self, ids: &[&str], now: i64) -> StorageResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let q = query(
            "UNWIND $ids AS id MATCH (p:Promise {id: id}) \
             WHERE p.state = 'pending' AND p.timeout_at <= $now RETURN p.id AS id",
        )
        .param("ids", ids)
        .param("now", now);
        let due: Vec<String> = self
            .rows(q)
            .await?
            .iter()
            .map(|r| col::<String>(r, "id"))
            .collect::<StorageResult<_>>()?;
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
    /// against it since it settled is flipped and, if suspended, woken.
    pub(crate) async fn process_callbacks(&mut self, id: &str, now: i64) -> StorageResult<()> {
        let Some(row) = self.read(id).await? else {
            return Ok(());
        };
        if row.is_pending() {
            return Ok(());
        }
        let awaiter_ids = self.awaiters_of(id).await?;
        let awaiters = self.lock_many(&awaiter_ids).await?;
        for w in &awaiters {
            self.run(
                query(
                    "MATCH (w:Promise {id: $w})-[r:AWAITS]->(p:Promise {id: $p}) \
                     SET r.ready = true",
                )
                .param("w", w.id.as_str())
                .param("p", id),
            )
            .await?;
            if w.task_is("suspended") {
                self.wake(w, now).await?;
            }
        }
        Ok(())
    }

    /// Drop a task's ready edges: `resumes = '{}'`.
    pub(crate) async fn clear_resumes(&mut self, id: &str) -> StorageResult<()> {
        self.run(
            query("MATCH (p:Promise {id: $id})-[r:AWAITS]->() WHERE r.ready = true DELETE r")
                .param("id", id),
        )
        .await
    }

    /// Link an awaiter to an awaited promise: the awaited's `callbacks` gains
    /// the awaiter, unless it is already there.
    pub(crate) async fn link(&mut self, awaiter: &str, awaited: &str) -> StorageResult<()> {
        self.run(
            query(
                "MATCH (w:Promise {id: $w}), (p:Promise {id: $p}) \
                 MERGE (w)-[r:AWAITS]->(p) ON CREATE SET r.ready = false",
            )
            .param("w", awaiter)
            .param("p", awaited),
        )
        .await
    }

    /// Record a ready callback on an awaiter: its `resumes` gains the awaited
    /// promise, whether or not it was ever in the awaited's `callbacks`.
    pub(crate) async fn mark_ready(&mut self, awaiter: &str, awaited: &str) -> StorageResult<()> {
        self.run(
            query(
                "MATCH (w:Promise {id: $w}), (p:Promise {id: $p}) \
                 MERGE (w)-[r:AWAITS]->(p) SET r.ready = true",
            )
            .param("w", awaiter)
            .param("p", awaited),
        )
        .await
    }

    /// The branch siblings a task response preloads.
    pub(crate) async fn compute_preload(&mut self, id: &str) -> StorageResult<Vec<PromiseRecord>> {
        let q = query(&format!(
            "MATCH (me:Promise {{id: $id}}) WHERE me.branch_id IS NOT NULL \
             MATCH (p:Promise {{branch_id: me.branch_id}}) WHERE p.id <> $id \
             RETURN {} ORDER BY p.id ASC LIMIT $limit",
            p_cols("p")
        ))
        .param("id", id)
        .param("limit", self.preload_limit as i64);
        Ok(self
            .promise_rows(q)
            .await?
            .iter()
            .map(PromiseRow::to_promise_record)
            .collect())
    }
}
