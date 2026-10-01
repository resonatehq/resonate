//! A node: the `ResonateServer` one process runs, serving the partitions the
//! group gave it and forwarding everything else to whoever owns it.
//!
//! # Contract
//!
//! - **Routing.** Every promise and task operation names one origin; the
//!   origin names one partition ([`keys::partition_of`]). Every schedule
//!   operation names one schedule id, routed the same way. If this node serves
//!   the partition, the partition decides; if the owner directory names
//!   another node, the request is forwarded there once; otherwise it is a 503,
//!   which the caller's retry covers — this is what a client sees during a
//!   rebalance.
//! - **Ownership.** Membership events drive everything: an assignment starts a
//!   takeover ([`Partition::take_over`]), a revocation stops the partition
//!   before it is acknowledged. A partition that stops on its own — fenced by
//!   another node, or unsure whether its last commit landed — is taken over
//!   again if the group still assigns it here, which fences whoever else
//!   is writing and re-reads the log.
//! - **Local copies.** A revoked partition's local copy is kept for a grace
//!   period, so a partition that comes straight back replays only the tail,
//!   and dropped after it.
//! - **Timers.** Outside debug, each served partition runs its own timer loop.
//!   Under debug the clock belongs to the caller and time moves only through
//!   `debug.tick`, exactly as on every other backend.
//! - **Debug operations** (`debug.reset`, `debug.snap`, `debug.tick`) act on
//!   the partitions this node serves, and so need a node that serves them all:
//!   they are the differential suite's, which runs one node.
//!
//! # Dependencies
//!
//! The log, the local store, membership, and peers — each a port — plus the
//! partition shell and the blob backend's `Sender`.
//!
//! # Dependants
//!
//! The plugin, which builds one from configuration; the tests, which build
//! several around one in-memory log.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use validator::Validate;

use resonate_core::types::{
    format_validation_errors, PromiseCreateData, PromiseGetData, PromiseRegisterCallbackData,
    PromiseRegisterListenerData, PromiseSettleData, RequestEnvelope, ResponseEnvelope,
    ScheduleCreateData, ScheduleDeleteData, ScheduleGetData, Snapshot, SnapshotCallback,
    SnapshotListener, SnapshotPromiseTimeout, SnapshotTaskTimeout, TaskAcquireData,
    TaskContinueData, TaskCreateData, TaskFenceData, TaskFulfillData, TaskGetData, TaskHaltData,
    TaskHeartbeatData, TaskReleaseData, TaskSuspendData,
};
use resonate_core::{util, ResonateServer, Unavailable};
use resonate_server_blob::kernel::state::{Reply, Req, ScheduleFireData};
use resonate_server_blob::sender::Sender;

use crate::directory::{Directory, Owner};
use crate::keys::{self, origin_of};
use crate::local::LocalStore;
use crate::log::Log;
use crate::membership::{Event, Membership};
use crate::partition::{Exit, OriginOp, Partition, PartitionCfg, ScheduleOp};
use crate::peer::{Fire, Peers, Search};
use crate::record;
use crate::search::{self, Query};
use crate::timers::Target;

/// How long a failed sweep waits before it is tried again.
const RETRY_DELAY_MS: i64 = 1_000;

/// Everything a node needs to know about itself.
#[derive(Debug, Clone)]
pub struct NodeCfg {
    /// This node's id, as the group and the directory know it. Unique in the
    /// cluster. (Where other nodes reach it is advertised through the group:
    /// see [`crate::membership::kafka::GroupCfg::peer_url`].)
    pub node_id: String,
    pub partition: PartitionCfg,
    /// The debug startup flag: `debug.*` answered, `head.debug_time` honoured,
    /// messages held, no timer loops.
    pub debug: bool,
    /// Whether searches are answered. Each reads every record.
    pub search: bool,
    /// How long a revoked partition's local copy is kept before it is dropped.
    pub drop_grace: Duration,
    /// How long `init` waits for every partition when this node is meant to
    /// own them all.
    pub startup_timeout: Duration,
}

impl Default for NodeCfg {
    fn default() -> Self {
        Self {
            node_id: "node-0".into(),
            partition: PartitionCfg::default(),
            debug: false,
            search: false,
            drop_grace: Duration::from_secs(15 * 60),
            startup_timeout: Duration::from_secs(60),
        }
    }
}

enum Slot {
    /// Assigned, and being taken over.
    Restoring,
    Serving {
        partition: Arc<Partition>,
        /// Ends the partition's timer loop, if it has one.
        timer_stop: Option<watch::Sender<bool>>,
    },
}

/// Where a partition's requests go.
enum Route {
    Local(Arc<Partition>),
    Remote(Owner),
    Nowhere(String),
}

pub struct Node {
    cfg: NodeCfg,
    log: Arc<dyn Log>,
    local: Arc<dyn LocalStore>,
    sender: Arc<Sender>,
    membership: Arc<dyn Membership>,
    directory: Arc<dyn Directory>,
    peers: Arc<dyn Peers>,
    table: RwLock<HashMap<u32, Slot>>,
    /// Partitions the group assigns here, each with the token of its current
    /// assignment, so a takeover or an exit from an older one is recognized.
    assigned: Mutex<HashMap<u32, u64>>,
    next_token: AtomicU64,
    revoked_at: Mutex<HashMap<u32, Instant>>,
    shutdown: watch::Sender<bool>,
    me: Weak<Node>,
}

impl Node {
    pub fn new(
        cfg: NodeCfg,
        log: Arc<dyn Log>,
        local: Arc<dyn LocalStore>,
        sender: Arc<Sender>,
        membership: Arc<dyn Membership>,
        directory: Arc<dyn Directory>,
        peers: Arc<dyn Peers>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            cfg,
            log,
            local,
            sender,
            membership,
            directory,
            peers,
            table: RwLock::new(HashMap::new()),
            assigned: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(1),
            revoked_at: Mutex::new(HashMap::new()),
            shutdown: watch::channel(false).0,
            me: me.clone(),
        })
    }

    /// One node owning every partition of an in-process log, with an
    /// in-memory local store — the differential suite's backend, and a
    /// development server. Not started: call [`Node::start`] (or `init`).
    pub fn in_memory(
        partitions: u32,
        router: Arc<dyn resonate_core::ResonateRouter>,
        cfg: NodeCfg,
    ) -> Arc<Self> {
        let sender = Arc::new(Sender::new(router, cfg.debug));
        Self::new(
            cfg,
            crate::log::mem::MemLog::new(partitions),
            crate::local::mem::MemLocal::new(),
            sender,
            crate::membership::StaticMembership::new(partitions),
            Arc::new(crate::directory::NoDirectory),
            crate::peer::LocalPeers::new(),
        )
    }

    pub fn id(&self) -> &str {
        &self.cfg.node_id
    }

    pub fn sender(&self) -> &Arc<Sender> {
        &self.sender
    }

    fn partitions(&self) -> u32 {
        self.log.partitions()
    }

    /// The partitions this node is serving right now.
    pub fn serving(&self) -> BTreeSet<u32> {
        self.table
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, s)| matches!(s, Slot::Serving { .. }))
            .map(|(p, _)| *p)
            .collect()
    }

    fn local_partitions(&self) -> Vec<Arc<Partition>> {
        let table = self.table.read().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<Arc<Partition>> = table
            .values()
            .filter_map(|s| match s {
                Slot::Serving { partition, .. } => Some(Arc::clone(partition)),
                Slot::Restoring => None,
            })
            .collect();
        out.sort_by_key(|p| p.id());
        out
    }

    fn route(&self, p: u32, forwarded: bool) -> Route {
        {
            let table = self.table.read().unwrap_or_else(|e| e.into_inner());
            match table.get(&p) {
                Some(Slot::Serving { partition, .. }) => {
                    return Route::Local(Arc::clone(partition))
                }
                Some(Slot::Restoring) => {
                    return Route::Nowhere(format!("partition {p} is being taken over"))
                }
                None => {}
            }
        }
        if forwarded {
            return Route::Nowhere(format!("partition {p} is not served by {}", self.id()));
        }
        match self.directory.owner(p) {
            Some(owner) if owner.node != self.cfg.node_id => Route::Remote(owner),
            _ => Route::Nowhere(format!("partition {p} has no owner yet")),
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Join the group and start serving what it assigns.
    pub async fn start(&self) -> Result<(), Unavailable> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let me = self.me.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let Some(node) = me.upgrade() else { return };
                node.on_event(event).await;
            }
        });
        self.membership.start(tx).await?;
        if self.membership.owns_everything() {
            self.wait_serving_all().await?;
        }
        Ok(())
    }

    /// Wait until every partition is served here.
    pub async fn wait_serving_all(&self) -> Result<(), Unavailable> {
        let deadline = Instant::now() + self.cfg.startup_timeout;
        while self.serving().len() < self.partitions() as usize {
            if Instant::now() > deadline {
                return Err(Unavailable::new(format!(
                    "only {} of {} partitions were taken over in time",
                    self.serving().len(),
                    self.partitions()
                )));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    /// Leave the group, which revokes and stops every partition, then stop
    /// whatever is left.
    pub async fn stop(&self) {
        let _ = self.shutdown.send(true);
        self.membership.stop().await;
        let remaining: Vec<u32> = self
            .table
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .copied()
            .collect();
        self.stop_partitions(&remaining).await;
    }

    async fn on_event(&self, event: Event) {
        match event {
            Event::Assigned(partitions) => {
                tracing::info!(node = %self.id(), ?partitions, "Partitions assigned");
                for p in partitions {
                    let token = self.next_token.fetch_add(1, Ordering::Relaxed);
                    self.assigned
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(p, token);
                    self.revoked_at
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&p);
                    self.table
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(p, Slot::Restoring);
                    self.spawn_takeover(p, token, Duration::ZERO);
                }
            }
            Event::Revoked {
                partitions,
                lost,
                done,
            } => {
                tracing::info!(node = %self.id(), ?partitions, lost, "Partitions revoked");
                {
                    let mut assigned = self.assigned.lock().unwrap_or_else(|e| e.into_inner());
                    for p in &partitions {
                        assigned.remove(p);
                    }
                }
                self.stop_partitions(&partitions).await;
                let now = Instant::now();
                {
                    let mut revoked = self.revoked_at.lock().unwrap_or_else(|e| e.into_inner());
                    for p in &partitions {
                        revoked.insert(*p, now);
                    }
                }
                for p in partitions {
                    self.schedule_drop(p, now);
                }
                let _ = done.send(());
            }
        }
    }

    async fn stop_partitions(&self, partitions: &[u32]) {
        let slots: Vec<Slot> = {
            let mut table = self.table.write().unwrap_or_else(|e| e.into_inner());
            partitions.iter().filter_map(|p| table.remove(p)).collect()
        };
        for slot in slots {
            if let Slot::Serving {
                partition,
                timer_stop,
            } = slot
            {
                if let Some(stop) = timer_stop {
                    let _ = stop.send(true);
                }
                partition.stop().await;
            }
        }
    }

    fn is_current(&self, p: u32, token: u64) -> bool {
        self.assigned
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&p)
            == Some(&token)
    }

    fn spawn_takeover(&self, p: u32, token: u64, delay: Duration) {
        let me = self.me.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let mut backoff = Duration::from_millis(100);
            loop {
                let Some(node) = me.upgrade() else { return };
                if !node.is_current(p, token) {
                    return;
                }
                match node.take_over(p, token).await {
                    Ok(()) => return,
                    Err(e) => {
                        tracing::warn!(node = %node.id(), partition = p, error = %e, "Takeover failed; retrying");
                    }
                }
                drop(node);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        });
    }

    async fn take_over(&self, p: u32, token: u64) -> Result<(), String> {
        let me = self.me.clone();
        let on_exit = Box::new(move |exit: Exit| {
            if let Some(node) = me.upgrade() {
                node.on_exit(p, token, exit);
            }
        });
        let partition = Partition::take_over(
            p,
            &self.log,
            &self.local,
            Arc::clone(&self.sender),
            self.cfg.partition.clone(),
            on_exit,
        )
        .await
        .map_err(|e| e.0)?;

        // Revoked while restoring: hand it straight back.
        if !self.is_current(p, token) {
            partition.stop().await;
            return Ok(());
        }
        let timer_stop = (!self.cfg.debug).then(|| self.spawn_timer_loop(Arc::clone(&partition)));
        self.table
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                p,
                Slot::Serving {
                    partition,
                    timer_stop,
                },
            );
        Ok(())
    }

    /// A partition stopped on its own. If the group still assigns it here,
    /// take it over again: fence, re-read, serve.
    fn on_exit(&self, p: u32, token: u64, exit: Exit) {
        if !self.is_current(p, token) {
            return;
        }
        let slot = {
            let mut table = self.table.write().unwrap_or_else(|e| e.into_inner());
            table.insert(p, Slot::Restoring)
        };
        if let Some(Slot::Serving {
            timer_stop: Some(stop),
            ..
        }) = slot
        {
            let _ = stop.send(true);
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.assigned
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(p, token);
        // Fenced: someone else is writing, and the group may be about to say
        // so. Give it a moment rather than fence straight back.
        let delay = match exit {
            Exit::Fenced(_) => Duration::from_secs(1),
            _ => Duration::ZERO,
        };
        self.spawn_takeover(p, token, delay);
    }

    /// Drop a revoked partition's local copy once the grace period passes
    /// without it coming back.
    fn schedule_drop(&self, p: u32, revoked: Instant) {
        let me = self.me.clone();
        let grace = self.cfg.drop_grace;
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let Some(node) = me.upgrade() else { return };
            let still = node
                .revoked_at
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&p)
                == Some(&revoked);
            let assigned = node
                .assigned
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&p);
            if still && !assigned {
                match node.local.drop_partition(p) {
                    Ok(()) => {
                        tracing::info!(partition = p, "Local copy of a revoked partition dropped")
                    }
                    Err(e) => tracing::warn!(partition = p, error = %e, "Local copy not dropped"),
                }
                node.revoked_at
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&p);
            }
        });
    }

    fn spawn_timer_loop(&self, partition: Arc<Partition>) -> watch::Sender<bool> {
        let (stop, mut stopped) = watch::channel(false);
        let me = self.me.clone();
        tokio::spawn(async move {
            loop {
                let sleep_for = match partition.timers().next_deadline() {
                    Some(at) => Duration::from_millis(
                        at.saturating_sub(util::system_time_ms()).max(0) as u64,
                    ),
                    None => Duration::from_secs(3_600),
                };
                tokio::select! {
                    _ = tokio::time::sleep(sleep_for) => {}
                    _ = partition.timers().armed_nearer() => {}
                    _ = stopped.changed() => return,
                }
                let Some(node) = me.upgrade() else { return };
                let now = util::system_time_ms();
                node.sweep(&partition, now).await;
            }
        });
        stop
    }

    /// Fire everything due in `partition` at `now`. Returns how many targets
    /// were swept. A target that fails is re-armed a beat later.
    async fn sweep(&self, partition: &Arc<Partition>, now: i64) -> usize {
        let due = partition.timers().take_due(now);
        let swept = due.len();
        for (deadline, target) in due {
            let outcome = match &target {
                Target::Origin(origin) => partition
                    .origin(origin, OriginOp::Tick, now)
                    .await
                    .map(|_| ()),
                Target::Schedule(id) => self.fire_schedule(partition, id, deadline, now).await,
            };
            if let Err(e) = outcome {
                tracing::warn!(partition = partition.id(), error = %e.message, "Timer sweep failed; re-armed to retry");
                partition.timers().set(target, Some(now + RETRY_DELAY_MS));
            }
        }
        swept
    }

    // -----------------------------------------------------------------------
    // Schedules firing
    // -----------------------------------------------------------------------

    /// Fire a schedule's occurrence at `deadline`: create its promise where
    /// the promise's origin lives, then advance the schedule. In that order —
    /// a crash in between refires the same occurrence, and the create is
    /// idempotent on the promise id; the reverse order would lose it.
    async fn fire_schedule(
        &self,
        partition: &Arc<Partition>,
        id: &str,
        deadline: i64,
        now: i64,
    ) -> Result<(), Unavailable> {
        let doc = match partition.committed_schedule(id)? {
            Some(doc) => doc,
            // Deleted since it was armed.
            None => return Ok(()),
        };
        if doc.next_run_at != deadline {
            // Already moved on. Keep the entry it has now.
            partition
                .timers()
                .set(Target::Schedule(id.to_string()), Some(doc.next_run_at));
            return Ok(());
        }

        let promise_id = doc
            .promise_id
            .replace("{{.id}}", id)
            .replace("{{.timestamp}}", &deadline.to_string());
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
        let fire = Fire {
            id: promise_id,
            timeout_at: deadline + doc.promise_timeout,
            param: resonate_core::types::PromiseValue {
                headers: doc
                    .promise_param_headers
                    .as_ref()
                    .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                data: doc.promise_param_data.clone(),
            },
            tags,
            fired_at: deadline,
            now,
        };
        self.fire_promise(fire, false).await?;
        partition
            .schedule(id, ScheduleOp::Advance { deadline }, now)
            .await?;
        tracing::info!(schedule_id = %id, fired_at = deadline, "Schedule fired");
        Ok(())
    }

    async fn fire_promise(&self, fire: Fire, forwarded: bool) -> Result<(), Unavailable> {
        let origin = origin_of(&fire.id).to_string();
        let p = keys::partition_of(&origin, self.partitions());
        match self.route(p, forwarded) {
            Route::Local(partition) => {
                let req = Req::ScheduleFire(ScheduleFireData {
                    id: fire.id,
                    timeout_at: fire.timeout_at,
                    param: fire.param,
                    tags: fire.tags,
                    fired_at: fire.fired_at,
                });
                partition
                    .origin(&origin, OriginOp::Req(Box::new(req)), fire.now)
                    .await
                    .map(|_| ())
            }
            Route::Remote(owner) => {
                let out = self.peers.fire(&owner, &fire).await;
                if out.is_err() {
                    self.directory.stale();
                }
                out
            }
            Route::Nowhere(why) => Err(Unavailable::new(why)),
        }
    }

    // -----------------------------------------------------------------------
    // Forwarded calls
    // -----------------------------------------------------------------------

    /// A request another node forwarded here: served locally or refused,
    /// never forwarded again.
    pub async fn process_forwarded(
        &self,
        req: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Unavailable> {
        self.process_as(req, true).await
    }

    pub async fn fire_forwarded(&self, fire: Fire) -> Result<(), Unavailable> {
        self.fire_promise(fire, true).await
    }

    /// This node's part of a search: every partition it serves.
    pub fn search_forwarded(&self, s: &Search) -> Result<Vec<(String, Value)>, Unavailable> {
        let query = match parse_query(&s.kind, &s.data) {
            Ok(q) => q,
            Err(reply) => {
                return Err(Unavailable::new(format!(
                    "a forwarded search did not parse: {}",
                    reply.data
                )))
            }
        };
        let limit = query
            .limit()
            .map_err(|r| Unavailable::new(format!("a forwarded search was refused: {}", r.data)))?;
        let mut out = Vec::new();
        for partition in self.local_partitions() {
            out.extend(
                search::local(partition.store().as_ref(), &query, limit, s.now)
                    .map_err(Unavailable::new)?,
            );
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Dispatch
    // -----------------------------------------------------------------------

    async fn process_as(
        &self,
        req: &RequestEnvelope,
        forwarded: bool,
    ) -> Result<ResponseEnvelope, Unavailable> {
        let debug_time = if self.cfg.debug {
            req.head.debug_time
        } else {
            None
        };
        let now = util::resolve_time(debug_time);
        let reply = self.dispatch(req, now, forwarded).await?;
        Ok(ResponseEnvelope::new(
            req.kind.clone(),
            req.head.corr_id.clone(),
            reply.status,
            reply.data,
        ))
    }

    /// Route one origin operation to its partition, wherever it is.
    async fn to_origin(
        &self,
        env: &RequestEnvelope,
        origin: &str,
        req: Req,
        now: i64,
        forwarded: bool,
    ) -> Result<Reply, Unavailable> {
        let p = keys::partition_of(origin, self.partitions());
        match self.route(p, forwarded) {
            Route::Local(partition) => {
                partition
                    .origin(origin, OriginOp::Req(Box::new(req)), now)
                    .await
            }
            Route::Remote(owner) => self.forward(&owner, env).await,
            Route::Nowhere(why) => Err(Unavailable::new(why)),
        }
    }

    async fn to_schedule(
        &self,
        env: &RequestEnvelope,
        id: &str,
        op: ScheduleOp,
        now: i64,
        forwarded: bool,
    ) -> Result<Reply, Unavailable> {
        let p = keys::partition_of(id, self.partitions());
        match self.route(p, forwarded) {
            Route::Local(partition) => partition.schedule(id, op, now).await,
            Route::Remote(owner) => self.forward(&owner, env).await,
            Route::Nowhere(why) => Err(Unavailable::new(why)),
        }
    }

    async fn forward(&self, owner: &Owner, env: &RequestEnvelope) -> Result<Reply, Unavailable> {
        // A failed forward means the directory may be behind the group: ask
        // again now, so the client's retry finds the new owner.
        let resp = match self.peers.process(owner, env).await {
            Ok(resp) => resp,
            Err(e) => {
                self.directory.stale();
                return Err(e);
            }
        };
        if resp.head.status == 503 {
            self.directory.stale();
        }
        Ok(Reply::status(resp.head.status, resp.data))
    }

    async fn dispatch(
        &self,
        env: &RequestEnvelope,
        now: i64,
        forwarded: bool,
    ) -> Result<Reply, Unavailable> {
        let data = &env.data;
        match env.kind.as_str() {
            "promise.search" | "task.search" | "schedule.search" if !self.cfg.search => {
                Ok(Reply::err(403, "Search operations are disabled"))
            }

            // --- promises ---------------------------------------------------
            "promise.get" => {
                let r: PromiseGetData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::PromiseGet(r), now, forwarded)
                    .await
            }
            "promise.create" => {
                let r: PromiseCreateData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::PromiseCreate(r), now, forwarded)
                    .await
            }
            "promise.settle" => {
                let r: PromiseSettleData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::PromiseSettle(r), now, forwarded)
                    .await
            }
            "promise.register_callback" => {
                let r: PromiseRegisterCallbackData = parsed!(data);
                let origin = origin_of(&r.awaiter).to_string();
                self.to_origin(
                    env,
                    &origin,
                    Req::PromiseRegisterCallback(r),
                    now,
                    forwarded,
                )
                .await
            }
            "promise.register_listener" => {
                let r: PromiseRegisterListenerData = parsed!(data);
                let origin = origin_of(&r.awaited).to_string();
                self.to_origin(
                    env,
                    &origin,
                    Req::PromiseRegisterListener(r),
                    now,
                    forwarded,
                )
                .await
            }
            "promise.search" | "task.search" | "schedule.search" => {
                self.search(&env.kind, data, now).await
            }

            // --- tasks ------------------------------------------------------
            "task.get" => {
                let r: TaskGetData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskGet(r), now, forwarded)
                    .await
            }
            "task.create" => {
                let r: TaskCreateData = parsed!(data);
                let origin = origin_of(&r.action.data.id).to_string();
                self.to_origin(env, &origin, Req::TaskCreate(r), now, forwarded)
                    .await
            }
            "task.acquire" => {
                let r: TaskAcquireData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskAcquire(r), now, forwarded)
                    .await
            }
            "task.release" => {
                let r: TaskReleaseData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskRelease(r), now, forwarded)
                    .await
            }
            "task.fulfill" => {
                let r: TaskFulfillData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskFulfill(r), now, forwarded)
                    .await
            }
            "task.suspend" => {
                let r: TaskSuspendData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskSuspend(r), now, forwarded)
                    .await
            }
            "task.fence" => {
                let r: TaskFenceData = parsed!(data);
                // As on the blob backend: a fence and its action commit
                // together, which needs them in one origin — and so in one
                // partition.
                if let Some(action_id) = r.action.data.get("id").and_then(|v| v.as_str()) {
                    if origin_of(action_id) != origin_of(&r.id) {
                        return Ok(Reply::err(400, "Action must belong to the task's origin"));
                    }
                }
                let origin = origin_of(&r.id).to_string();
                let req = Req::TaskFence {
                    data: r,
                    corr_id: env.head.corr_id.clone(),
                };
                self.to_origin(env, &origin, req, now, forwarded).await
            }
            "task.heartbeat" => {
                let r: TaskHeartbeatData = parsed!(data);
                // The validator requires a non-empty batch sharing one origin.
                let origin = origin_of(&r.tasks[0].id).to_string();
                self.to_origin(env, &origin, Req::TaskHeartbeat(r), now, forwarded)
                    .await
            }
            "task.halt" => {
                let r: TaskHaltData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskHalt(r), now, forwarded)
                    .await
            }
            "task.continue" => {
                let r: TaskContinueData = parsed!(data);
                let origin = origin_of(&r.id).to_string();
                self.to_origin(env, &origin, Req::TaskContinue(r), now, forwarded)
                    .await
            }

            // --- schedules --------------------------------------------------
            "schedule.get" => {
                let r: ScheduleGetData = parsed!(data);
                self.to_schedule(env, &r.id, ScheduleOp::Get, now, forwarded)
                    .await
            }
            "schedule.create" => {
                let r: ScheduleCreateData = parsed!(data);
                let id = r.id.clone();
                self.to_schedule(env, &id, ScheduleOp::Create(r), now, forwarded)
                    .await
            }
            "schedule.delete" => {
                let r: ScheduleDeleteData = parsed!(data);
                self.to_schedule(env, &r.id, ScheduleOp::Delete, now, forwarded)
                    .await
            }

            // --- debug ------------------------------------------------------
            "debug.reset" | "debug.snap" | "debug.tick" if !self.cfg.debug => {
                Ok(Reply::err(403, "Debug operations are disabled"))
            }
            "debug.reset" => self.reset().await,
            "debug.snap" => {
                let snapshot = self.snapshot()?;
                Ok(Reply::status(
                    200,
                    serde_json::to_value(snapshot).expect("a snapshot serializes"),
                ))
            }
            "debug.tick" => self.tick(env).await,

            other => Ok(Reply::err(400, &format!("Unknown operation: {other}"))),
        }
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    async fn search(&self, kind: &str, data: &Value, now: i64) -> Result<Reply, Unavailable> {
        let query = match parse_query(kind, data) {
            Ok(q) => q,
            Err(reply) => return Ok(reply),
        };
        let limit = match query.limit() {
            Ok(l) => l,
            Err(reply) => return Ok(reply),
        };

        // Every partition must be answerable, here or at its owner: a search
        // that silently skipped one would be wrong, not partial.
        let mut remote: BTreeMap<String, Owner> = BTreeMap::new();
        let mut items = Vec::new();
        for p in 0..self.partitions() {
            match self.route(p, false) {
                Route::Local(partition) => items.extend(
                    search::local(partition.store().as_ref(), &query, limit, now)
                        .map_err(Unavailable::new)?,
                ),
                Route::Remote(owner) => {
                    remote.insert(owner.node.clone(), owner);
                }
                Route::Nowhere(why) => return Err(Unavailable::new(why)),
            }
        }
        let s = Search {
            kind: kind.to_string(),
            data: data.clone(),
            now,
        };
        let answers =
            futures::future::join_all(remote.values().map(|owner| self.peers.search(owner, &s)))
                .await;
        for answer in answers {
            items.extend(answer?);
        }
        Ok(search::respond(&query, items, limit))
    }

    // -----------------------------------------------------------------------
    // Debug
    // -----------------------------------------------------------------------

    fn require_everything(&self) -> Result<Vec<Arc<Partition>>, Unavailable> {
        let local = self.local_partitions();
        if local.len() != self.partitions() as usize {
            return Err(Unavailable::new(format!(
                "debug operations need every partition on one node; {} serves {} of {}",
                self.id(),
                local.len(),
                self.partitions()
            )));
        }
        Ok(local)
    }

    async fn reset(&self) -> Result<Reply, Unavailable> {
        for partition in self.require_everything()? {
            partition.reset().await?;
        }
        self.sender.clear();
        tracing::warn!("Debug reset: all data cleared");
        Ok(Reply::status(200, Value::Object(serde_json::Map::new())))
    }

    /// `debug.tick`: sweep until nothing is due. Rounds terminate because
    /// each one either fires something and re-arms it strictly later than
    /// `time`, or fires nothing.
    async fn tick(&self, env: &RequestEnvelope) -> Result<Reply, Unavailable> {
        let time = match env.data.get("time").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => return Ok(Reply::err(400, "Missing or invalid 'time' field")),
        };
        if let Some(debug_time) = env.head.debug_time {
            if debug_time != time {
                return Ok(Reply::err(400, "resonate:debug_time must equal data.time"));
            }
        }
        let partitions = self.require_everything()?;
        const MAX_ROUNDS: usize = 10_000;
        for _ in 0..MAX_ROUNDS {
            let mut swept = 0;
            for partition in &partitions {
                swept += self.sweep(partition, time).await;
            }
            if swept == 0 {
                return Ok(Reply::status(200, Value::Array(vec![])));
            }
        }
        Err(Unavailable::new("tick did not converge"))
    }

    /// Every partition, in the shape `debug.snap` compares.
    fn snapshot(&self) -> Result<Snapshot, Unavailable> {
        let mut promises = Vec::new();
        let mut promise_timeouts = Vec::new();
        let mut callbacks = Vec::new();
        let mut listeners = Vec::new();
        let mut tasks = Vec::new();
        let mut task_timeouts = Vec::new();

        for partition in self.require_everything()? {
            for (key, value) in partition
                .store()
                .scan(&keys::all_promises_prefix())
                .map_err(Unavailable::new)?
            {
                let id = keys::id_of_promise_key(&key)
                    .ok_or_else(|| Unavailable::new("unreadable promise key"))?;
                let (p, t) = record::decode_promise(&id, &value).map_err(Unavailable::new)?;
                promises.push(p.to_record(&id));
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
                if let Some(t) = t {
                    tasks.push(t.to_record(&id));
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

    /// Whether the log answers, and — for a node meant to own everything —
    /// whether it serves everything.
    pub async fn is_ready(&self) -> bool {
        if !self.log.ready().await {
            return false;
        }
        !self.membership.owns_everything() || self.serving().len() == self.partitions() as usize
    }
}

fn parse_query(kind: &str, data: &Value) -> Result<Query, Reply> {
    Ok(match kind {
        "promise.search" => Query::Promises(parse(data)?),
        "task.search" => Query::Tasks(parse(data)?),
        _ => Query::Schedules(parse(data)?),
    })
}

/// Deserialize and validate `data`, or return the 400 the SQL handlers return.
macro_rules! parsed {
    ($data:expr) => {
        match parse($data) {
            Ok(r) => r,
            Err(reply) => return Ok(reply),
        }
    };
}
use parsed;

fn parse<T: DeserializeOwned + Validate>(data: &Value) -> Result<T, Reply> {
    let parsed: T = serde_json::from_value(data.clone())
        .map_err(|e| Reply::err(400, &format!("Invalid request: {e}")))?;
    parsed
        .validate()
        .map_err(|e| Reply::err(400, &format_validation_errors(&e)))?;
    Ok(parsed)
}

#[async_trait]
impl ResonateServer for Node {
    async fn init(&self, _debug: bool) -> Result<(), Unavailable> {
        self.start().await
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        Node::stop(self).await;
        Ok(())
    }

    async fn ready(&self) -> bool {
        self.is_ready().await
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.process_as(req, false).await
    }
}
