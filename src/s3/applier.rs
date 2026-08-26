//! The shell: load, decide, perform.
//!
//! One actor per origin, so every decision for a document is serialized and no
//! two writers race on one key. Each actor drains its whole mailbox before
//! deciding, which is the group commit: requests that arrive while a
//! compare-and-swap is in flight ride the next one, and a hot origin costs one
//! write per round rather than one per request.
//!
//! # Effect order
//!
//! Fixed, and the reason every crash window is recoverable:
//!
//! 1. **PUT the new timer object** (unconditional) — *before* the document, so
//!    a deadline the document is about to arm is already covered if the process
//!    dies here.
//! 2. **CAS the document.**
//! 3. **DELETE the old timer object** (best effort) — after, so nothing is
//!    left uncovered in between. A failure leaves an orphan, which the timer
//!    poller collects.
//! 4. **Hand the sends to the outbox** — strictly post-commit, at most once.
//! 5. **Answer the callers.**
//!
//! | dies after | orphan | recovery |
//! |---|---|---|
//! | timer PUT | a timer for a document that never changed | it fires, the drain finds nothing due and writes nothing, the key is collected |
//! | CAS | a stale timer | it fires early; the drain either no-ops or legitimately advances |
//! | sends | a lost Execute or Unblock | Execute: the committed `retry_at` is covered by step 1, so the drain re-emits it. Unblock: lost, as it is today |
//! | replies | an unanswered caller | it retries; the operations are idempotent and report current state |
//!
//! # The write law
//!
//! If the decision left the promises, tasks and armed deadline untouched, no
//! object is written at all — a read that changes nothing costs zero S3
//! operations on a cache hit. The document's `clock` is excluded from that
//! comparison on purpose: it is a monotonicity hint, not state, and paying a
//! PUT to advance it would make every `promise.get` a write.
//!
//! # Losing a race
//!
//! A refused conditional write means the decision was made against state that
//! is now stale. The cache entry is dropped, the document re-read, and the
//! whole batch **re-decided** — never replayed. That is the only correct
//! response, and it is why `handle` is a pure function of the document.
//!
//! # Dependencies
//!
//! The kernel (`handle`, `drain`, `apply_effects`) for every decision, the
//! codec for the bytes, the store for the objects, the cache for the read
//! path, and the outbox for post-commit sends. [`KeySpace`], defined here,
//! names every key in the bucket.
//!
//! # Dependants
//!
//! Every write in the backend goes through [`ApplierPool::submit`]: `S3Server`
//! routes each request to it, the timer poller sweeps origins through
//! [`ApplierPool::tick`], and the schedule service submits `ScheduleFire`
//! through it. The other modules share [`KeySpace`] to agree on where things
//! live.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use crate::core::Unavailable;
use crate::kernel::state::{apply_effects, Effect, KernelCfg, OriginDoc, Reply, Req};
use crate::kernel::{drain, handle};

use super::cache::DocCache;
use super::codec;
use super::outbox::Outbox;
use super::store::{Etag, Store, StoreError};

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// Zero-padded width of a deadline in a timer key. i64 needs 19 digits; 20
/// leaves the padding stable if that ever grows.
const DEADLINE_WIDTH: usize = 20;

/// Where everything lives under the bucket.
#[derive(Debug, Clone)]
pub struct KeySpace {
    /// Prefix for every key, `""` or ending in `/`.
    pub prefix: String,
    /// How many prefixes the timer keys are spread across.
    ///
    /// Timer keys are deadline-ordered, which means they are monotonically
    /// increasing, which is the classic S3 hot-prefix anti-pattern. Sharding
    /// by target spreads the writes.
    pub timer_shards: u32,
}

/// What a timer key points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerEntry {
    /// An origin document has a deadline at `deadline`.
    Origin { deadline: i64, origin: String },
    /// A schedule is due at `deadline`.
    Schedule { deadline: i64, id: String },
}

impl TimerEntry {
    pub fn deadline(&self) -> i64 {
        match self {
            TimerEntry::Origin { deadline, .. } | TimerEntry::Schedule { deadline, .. } => {
                *deadline
            }
        }
    }
}

/// Percent-encode anything that is not safe and stable in a key.
///
/// Keys must round-trip and must not grow structure: `/` would create a path
/// segment and `:` is the origin/lineage separator this design reserves, so
/// both are escaped along with everything non-alphanumeric.
fn encode_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' => out.push(*b as char),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn decode_key(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

impl KeySpace {
    pub fn new(prefix: impl Into<String>, timer_shards: u32) -> Self {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self {
            prefix,
            timer_shards: timer_shards.max(1),
        }
    }

    pub fn doc_prefix(&self) -> String {
        format!("{}wf", self.prefix)
    }

    pub fn sched_prefix(&self) -> String {
        format!("{}sched", self.prefix)
    }

    pub fn timer_prefix(&self) -> String {
        format!("{}t", self.prefix)
    }

    pub fn doc_key(&self, origin: &str) -> String {
        format!("{}/{}", self.doc_prefix(), encode_key(origin))
    }

    pub fn sched_key(&self, id: &str) -> String {
        format!("{}/{}", self.sched_prefix(), encode_key(id))
    }

    /// The origin a document key names, or `None` if the key is not one.
    pub fn origin_of_doc_key(&self, key: &str) -> Option<String> {
        decode_key(key.strip_prefix(&format!("{}/", self.doc_prefix()))?)
    }

    /// The schedule a schedule key names.
    pub fn id_of_sched_key(&self, key: &str) -> Option<String> {
        decode_key(key.strip_prefix(&format!("{}/", self.sched_prefix()))?)
    }

    pub fn timer_shard_prefix(&self, shard: u32) -> String {
        format!("{}/{:02}", self.timer_prefix(), shard % self.timer_shards)
    }

    fn shard_of(&self, target: &str) -> u32 {
        // Any stable spread will do; reuse the codec's hash so there is one
        // hash function in the backend rather than two.
        let hash = codec::origin_hash(target);
        let n = u64::from_str_radix(&hash[..8], 16).unwrap_or(0);
        (n % self.timer_shards as u64) as u32
    }

    fn timer_key_for(&self, target: &str, encoded: &str, at: i64) -> String {
        // The deadline is zero-padded so that lexicographic order *is* time
        // order: this is what makes a capped, ascending LIST return the nearest
        // deadlines. A negative deadline would break the padding, and none can
        // occur (every deadline derives from a validated non-negative
        // timeoutAt or from now plus a positive ttl), so clamping is the honest
        // floor rather than a silent reinterpretation.
        let at = at.max(0);
        format!(
            "{}/{:0width$}_{}",
            self.timer_shard_prefix(self.shard_of(target)),
            at,
            encoded,
            width = DEADLINE_WIDTH
        )
    }

    pub fn timer_key(&self, origin: &str, at: i64) -> String {
        self.timer_key_for(origin, &encode_key(origin), at)
    }

    /// A schedule's timer key. Schedule ids may not contain `':'` and an
    /// encoded origin cannot either, so the `sched:` marker is unambiguous.
    pub fn sched_timer_key(&self, id: &str, at: i64) -> String {
        self.timer_key_for(id, &format!("sched:{}", encode_key(id)), at)
    }

    pub fn parse_timer_key(&self, key: &str) -> Option<TimerEntry> {
        let name = key.rsplit('/').next()?;
        if name.len() < DEADLINE_WIDTH + 2 {
            return None;
        }
        let (deadline, rest) = name.split_at(DEADLINE_WIDTH);
        let deadline: i64 = deadline.parse().ok()?;
        let target = rest.strip_prefix('_')?;
        match target.strip_prefix("sched:") {
            Some(id) => Some(TimerEntry::Schedule {
                deadline,
                id: decode_key(id)?,
            }),
            None => Some(TimerEntry::Origin {
                deadline,
                origin: decode_key(target)?,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Applier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ApplierCfg {
    /// How many times a batch may be re-decided after losing a race before the
    /// caller is told there is no answer.
    pub max_cas_retries: u32,
    /// How long an origin's actor waits idle before it is reaped.
    pub idle_timeout: Duration,
    /// Mailbox depth. Deeper means larger group commits under load.
    pub mailbox: usize,
    pub kernel: KernelCfg,
}

impl Default for ApplierCfg {
    fn default() -> Self {
        Self {
            max_cas_retries: 8,
            idle_timeout: Duration::from_secs(60),
            mailbox: 256,
            kernel: KernelCfg::default(),
        }
    }
}

/// One unit of work for an origin's actor.
enum Work {
    Request {
        req: Req,
        now: i64,
        reply: oneshot::Sender<Result<Reply, Unavailable>>,
    },
    Tick {
        now: i64,
        done: oneshot::Sender<Result<(), Unavailable>>,
    },
}

impl Work {
    fn now(&self) -> i64 {
        match self {
            Work::Request { now, .. } | Work::Tick { now, .. } => *now,
        }
    }

    /// Report failure to whoever is waiting.
    fn fail(self, e: &Unavailable) {
        match self {
            Work::Request { reply, .. } => {
                let _ = reply.send(Err(e.clone()));
            }
            Work::Tick { done, .. } => {
                let _ = done.send(Err(e.clone()));
            }
        }
    }
}

struct Shared {
    store: Arc<dyn Store>,
    cache: Arc<dyn DocCache>,
    outbox: Arc<Outbox>,
    keys: KeySpace,
    cfg: ApplierCfg,
    /// Origin to its actor's mailbox, tagged with the actor's identity so a
    /// reaping actor cannot remove the one that replaced it.
    actors: AsyncMutex<HashMap<String, (u64, mpsc::Sender<Work>)>>,
    next_actor_id: std::sync::atomic::AtomicU64,
}

/// The per-origin actors, and the only way into them.
pub struct ApplierPool {
    shared: Arc<Shared>,
}

impl ApplierPool {
    pub fn new(
        store: Arc<dyn Store>,
        cache: Arc<dyn DocCache>,
        outbox: Arc<Outbox>,
        keys: KeySpace,
        cfg: ApplierCfg,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                store,
                cache,
                outbox,
                keys,
                cfg,
                actors: AsyncMutex::new(HashMap::new()),
                next_actor_id: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    pub fn keys(&self) -> &KeySpace {
        &self.shared.keys
    }

    pub fn outbox(&self) -> &Arc<Outbox> {
        &self.shared.outbox
    }

    pub fn store(&self) -> &Arc<dyn Store> {
        &self.shared.store
    }

    pub fn cache(&self) -> &Arc<dyn DocCache> {
        &self.shared.cache
    }

    pub fn cfg_kernel(&self) -> KernelCfg {
        self.shared.cfg.kernel
    }

    /// Decide one request against its origin's document.
    pub async fn submit(&self, origin: &str, req: Req, now: i64) -> Result<Reply, Unavailable> {
        let (tx, rx) = oneshot::channel();
        self.send(
            origin,
            Work::Request {
                req,
                now,
                reply: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| Unavailable::new("origin worker stopped before answering"))?
    }

    /// Sweep one origin's deadlines.
    pub async fn tick(&self, origin: &str, now: i64) -> Result<(), Unavailable> {
        let (tx, rx) = oneshot::channel();
        self.send(origin, Work::Tick { now, done: tx }).await?;
        rx.await
            .map_err(|_| Unavailable::new("origin worker stopped before answering"))?
    }

    /// Drop every actor and forget every cached document — `debug.reset` has
    /// deleted the objects they refer to.
    pub async fn reset(&self) {
        self.shared.actors.lock().await.clear();
        self.shared.cache.clear();
    }

    /// Hand `work` to `origin`'s actor, starting one if needed.
    ///
    /// An actor that reaped itself between the lookup and the send leaves a
    /// closed sender behind; that is indistinguishable from a full mailbox at
    /// the type level, so the send is retried once against a fresh actor.
    async fn send(&self, origin: &str, work: Work) -> Result<(), Unavailable> {
        let mut work = work;
        for attempt in 0..2 {
            let sender = self.actor(origin).await;
            match sender.send(work).await {
                Ok(()) => return Ok(()),
                Err(mpsc::error::SendError(returned)) => {
                    work = returned;
                    // The actor is gone. Drop the stale sender and try again.
                    let mut actors = self.shared.actors.lock().await;
                    if actors.get(origin).is_some_and(|(_, s)| s.is_closed()) {
                        actors.remove(origin);
                    }
                    if attempt == 1 {
                        break;
                    }
                }
            }
        }
        Err(Unavailable::new("origin worker unavailable"))
    }

    async fn actor(&self, origin: &str) -> mpsc::Sender<Work> {
        let mut actors = self.shared.actors.lock().await;
        if let Some((_, sender)) = actors.get(origin) {
            if !sender.is_closed() {
                return sender.clone();
            }
        }
        let id = self
            .shared
            .next_actor_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(self.shared.cfg.mailbox);
        actors.insert(origin.to_string(), (id, tx.clone()));
        let shared = Arc::clone(&self.shared);
        let origin = origin.to_string();
        tokio::spawn(async move { run_actor(origin, id, rx, shared).await });
        tx
    }
}

/// One origin's serialized decision loop.
async fn run_actor(origin: String, id: u64, mut rx: mpsc::Receiver<Work>, shared: Arc<Shared>) {
    loop {
        let first = match tokio::time::timeout(shared.cfg.idle_timeout, rx.recv()).await {
            Ok(Some(work)) => work,
            // Channel closed, or idle long enough to be worth reaping.
            Ok(None) | Err(_) => break,
        };
        // Group commit: take everything already queued.
        let mut batch = vec![first];
        while let Ok(work) = rx.try_recv() {
            batch.push(work);
        }
        run_batch(&origin, batch, &shared).await;
    }
    {
        let mut actors = shared.actors.lock().await;
        // Only our own entry: a racing `actor()` may already have replaced it.
        if actors.get(&origin).is_some_and(|(other, _)| *other == id) {
            actors.remove(&origin);
        }
    }
    rx.close();
    // Anything that landed between the timeout and the close still deserves an
    // answer; the caller's retry contract covers it, but failing loudly beats
    // dropping it.
    while let Ok(work) = rx.try_recv() {
        work.fail(&Unavailable::new("origin worker stopped"));
    }
}

/// What a batch decided, before anything is written.
struct Decision {
    doc: OriginDoc,
    replies: Vec<Reply>,
    sends: Vec<Effect>,
}

async fn run_batch(origin: &str, batch: Vec<Work>, shared: &Arc<Shared>) {
    let mut attempt = 0u32;
    loop {
        let loaded = match load(origin, shared).await {
            Ok(l) => l,
            Err(e) => return fail_all(batch, &e),
        };
        let decision = decide(&loaded.doc, &batch, shared);
        match perform(origin, &loaded, &decision, shared).await {
            Ok(()) => {
                for effect in &decision.sends {
                    if let Effect::Send { address, out } = effect {
                        shared.outbox.dispatch(address, out.clone()).await;
                    }
                }
                return answer(batch, decision.replies);
            }
            Err(StoreError::PreconditionFailed) => {
                // Someone else wrote first. Everything decided above was
                // decided against stale state: drop it and decide again.
                shared.cache.invalidate(origin);
                attempt += 1;
                if attempt > shared.cfg.max_cas_retries {
                    return fail_all(
                        batch,
                        &Unavailable::new(format!(
                            "origin {origin} contended for {attempt} attempts"
                        )),
                    );
                }
                let backoff = jittered_backoff(attempt);
                tokio::time::sleep(backoff).await;
            }
            Err(StoreError::Conflict) => {
                // Unorderable concurrent writes: nothing is known about
                // whether ours landed. Retry the same conditional write; if it
                // comes back refused, the branch above re-decides.
                attempt += 1;
                if attempt > shared.cfg.max_cas_retries {
                    return fail_all(
                        batch,
                        &Unavailable::new(format!("origin {origin} conflicted repeatedly")),
                    );
                }
                tokio::time::sleep(jittered_backoff(attempt)).await;
            }
            Err(StoreError::Unavailable(m)) => {
                return fail_all(batch, &Unavailable::new(m));
            }
        }
    }
}

fn jittered_backoff(attempt: u32) -> Duration {
    let base = 2u64.saturating_pow(attempt.min(6)) * 5;
    Duration::from_millis(base + fastrand::u64(0..=base))
}

fn fail_all(batch: Vec<Work>, e: &Unavailable) {
    for work in batch {
        work.fail(e);
    }
}

fn answer(batch: Vec<Work>, replies: Vec<Reply>) {
    debug_assert_eq!(batch.len(), replies.len());
    for (work, reply) in batch.into_iter().zip(replies) {
        match work {
            Work::Request { reply: tx, .. } => {
                let _ = tx.send(Ok(reply));
            }
            Work::Tick { done, .. } => {
                let _ = done.send(Ok(()));
            }
        }
    }
}

struct Loaded {
    doc: OriginDoc,
    /// `None` when the object does not exist yet, which is what selects a
    /// create rather than a conditional replace.
    etag: Option<Etag>,
}

async fn load(origin: &str, shared: &Arc<Shared>) -> Result<Loaded, Unavailable> {
    if let Some(cached) = shared.cache.get(origin) {
        return Ok(Loaded {
            doc: cached.doc.as_ref().clone(),
            etag: Some(cached.etag),
        });
    }
    let key = shared.keys.doc_key(origin);
    match shared.store.get(&key).await {
        Ok(Some((bytes, etag))) => {
            let doc = codec::decode(&bytes, origin).map_err(|e| {
                // A document that cannot be read is not something to paper
                // over: refusing beats deciding against a guess.
                Unavailable::new(format!("document {key} unreadable: {e}"))
            })?;
            shared
                .cache
                .put(origin, Arc::new(doc.clone()), etag.clone());
            Ok(Loaded {
                doc,
                etag: Some(etag),
            })
        }
        Ok(None) => Ok(Loaded {
            doc: OriginDoc::default(),
            etag: None,
        }),
        Err(e) => Err(Unavailable::new(e.to_string())),
    }
}

/// Fold the batch through the kernel. Pure: no I/O, no clock reads.
fn decide(loaded: &OriginDoc, batch: &[Work], shared: &Arc<Shared>) -> Decision {
    let mut doc = loaded.clone();
    let mut replies = Vec::with_capacity(batch.len());
    let mut sends = Vec::new();
    // An origin's view of time never goes backwards, whatever a caller's clock
    // says.
    let mut clock = doc.clock;

    for work in batch {
        let now = work.now().max(clock);
        clock = now;
        let fx = match work {
            Work::Request { req, .. } => {
                let (fx, reply) = handle(&doc, req, now, &shared.cfg.kernel);
                replies.push(reply);
                fx
            }
            Work::Tick { .. } => {
                replies.push(Reply::status(200, serde_json::Value::Array(vec![])));
                drain(&doc, now, &shared.cfg.kernel)
            }
        };
        // Request k sees request k-1's document.
        apply_effects(&mut doc, &fx);
        sends.extend(
            fx.into_iter()
                .filter(|e| matches!(e, Effect::Send { .. })),
        );
    }
    doc.clock = clock;
    Decision {
        doc,
        replies,
        sends,
    }
}

/// Did the decision touch anything worth an object write?
///
/// `clock` and `gen` are excluded: the clock is a monotonicity hint, and
/// writing an object to advance it would turn every read into a write.
fn changed(before: &OriginDoc, after: &OriginDoc) -> bool {
    before.promises != after.promises
        || before.tasks != after.tasks
        || before.timer_at != after.timer_at
}

/// Arm the new timer, commit, clear the old timer. In that order.
async fn perform(
    origin: &str,
    loaded: &Loaded,
    decision: &Decision,
    shared: &Arc<Shared>,
) -> Result<(), StoreError> {
    if !changed(&loaded.doc, &decision.doc) {
        // The write law: nothing changed, so nothing is written.
        return Ok(());
    }
    let old_timer = loaded.doc.timer_at;
    let new_timer = decision.doc.timer_at;

    // (1) The new timer first, so the deadline is covered even if we die here.
    if new_timer != old_timer {
        if let Some(at) = new_timer {
            shared
                .store
                .put(&shared.keys.timer_key(origin, at), Vec::new())
                .await?;
        }
    }

    // (2) The document.
    let mut doc = decision.doc.clone();
    doc.gen = loaded.doc.gen.saturating_add(1);
    let body = codec::encode(&doc, origin);
    let key = shared.keys.doc_key(origin);
    let etag = match &loaded.etag {
        Some(etag) => shared.store.put_if_match(&key, body, etag).await?,
        None => shared.store.put_if_none_match(&key, body).await?,
    };
    shared.cache.put(origin, Arc::new(doc), etag);

    // (3) The old timer, after the commit. A failure here only orphans a key,
    // which the poller collects, so it must not fail the request.
    if new_timer != old_timer {
        if let Some(at) = old_timer {
            if let Err(e) = shared.store.delete(&shared.keys.timer_key(origin, at)).await {
                tracing::debug!(origin = %origin, deadline = at, error = %e, "Stale timer object left behind; the poller will collect it");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::cache::{MemDocCache, NoopDocCache};
    use crate::s3::store::{FaultStore, ObjectStoreAdapter};
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    const W: &str = "http://worker:9999";
    const ORIGIN: &str = "diff";

    /// A store that counts what it was asked to do, so the write law can be
    /// asserted rather than assumed.
    struct Counting {
        inner: Arc<dyn Store>,
        gets: AtomicU64,
        puts: AtomicU64,
        deletes: AtomicU64,
    }

    impl Counting {
        fn new(inner: Arc<dyn Store>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                gets: AtomicU64::new(0),
                puts: AtomicU64::new(0),
                deletes: AtomicU64::new(0),
            })
        }
        fn counts(&self) -> (u64, u64, u64) {
            (
                self.gets.load(AtomicOrdering::SeqCst),
                self.puts.load(AtomicOrdering::SeqCst),
                self.deletes.load(AtomicOrdering::SeqCst),
            )
        }
    }

    #[async_trait]
    impl Store for Counting {
        async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
            self.gets.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.get(key).await
        }
        async fn put_if_match(
            &self,
            key: &str,
            body: Vec<u8>,
            etag: &Etag,
        ) -> Result<Etag, StoreError> {
            self.puts.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.put_if_match(key, body, etag).await
        }
        async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
            self.puts.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.put_if_none_match(key, body).await
        }
        async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
            self.puts.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.put(key, body).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.deletes.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.delete(key).await
        }
        async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
            self.inner.list(prefix, max_keys).await
        }
    }

    fn keys() -> KeySpace {
        KeySpace::new("p", 4)
    }

    fn pool_with(store: Arc<dyn Store>, cache: Arc<dyn DocCache>) -> ApplierPool {
        let outbox = Arc::new(Outbox::new(None, "http://server"));
        outbox.set_paused(true);
        ApplierPool::new(store, cache, outbox, keys(), ApplierCfg::default())
    }

    fn shared_store() -> Arc<dyn Store> {
        Arc::new(ObjectStoreAdapter::in_memory())
    }

    fn pool() -> ApplierPool {
        pool_with(shared_store(), Arc::new(MemDocCache::new(16)))
    }

    fn create(id: &str, timeout_at: i64, tags: serde_json::Value) -> Req {
        Req::PromiseCreate(
            serde_json::from_value(
                json!({ "id": id, "timeoutAt": timeout_at, "param": {}, "tags": tags }),
            )
            .unwrap(),
        )
    }

    fn get(id: &str) -> Req {
        Req::PromiseGet(serde_json::from_value(json!({ "id": id })).unwrap())
    }

    // --- KeySpace ---------------------------------------------------------

    #[test]
    fn a_prefix_gets_a_trailing_slash_once() {
        assert_eq!(KeySpace::new("p", 1).doc_key("o"), "p/wf/o");
        assert_eq!(KeySpace::new("p/", 1).doc_key("o"), "p/wf/o");
        assert_eq!(KeySpace::new("", 1).doc_key("o"), "wf/o");
    }

    #[test]
    fn a_document_key_round_trips_through_its_origin() {
        let k = keys();
        for origin in [
            "diff",
            "my.app.workflow",
            "sched-promise-s0-1000",
            "with space",
            "with/slash",
            "with%percent",
            "caf\u{e9}",
        ] {
            let key = k.doc_key(origin);
            assert!(
                !key[k.doc_prefix().len() + 1..].contains('/'),
                "{origin} created a path segment"
            );
            assert_eq!(
                k.origin_of_doc_key(&key).as_deref(),
                Some(origin),
                "round trip for {origin}"
            );
        }
        assert_eq!(k.origin_of_doc_key("p/t/00/x"), None);
    }

    #[test]
    fn a_timer_key_sorts_by_deadline_and_round_trips() {
        let k = keys();
        let a = k.timer_key("diff", 100);
        let b = k.timer_key("diff", 2_000);
        assert!(a < b, "lexicographic order is time order");
        assert_eq!(
            k.parse_timer_key(&a),
            Some(TimerEntry::Origin {
                deadline: 100,
                origin: "diff".into()
            })
        );
    }

    #[test]
    fn a_schedule_timer_key_is_distinguishable_from_an_origins() {
        let k = keys();
        let s = k.sched_timer_key("s0", 500);
        assert_eq!(
            k.parse_timer_key(&s),
            Some(TimerEntry::Schedule {
                deadline: 500,
                id: "s0".into()
            })
        );
        // An origin containing the marker text cannot be confused for one: the
        // colon is escaped.
        let o = k.timer_key("sched:evil", 500);
        assert_eq!(
            k.parse_timer_key(&o),
            Some(TimerEntry::Origin {
                deadline: 500,
                origin: "sched:evil".into()
            })
        );
    }

    #[test]
    fn timer_keys_are_spread_across_shards() {
        let k = KeySpace::new("p", 4);
        let shards: std::collections::HashSet<String> = (0..40)
            .map(|n| {
                let key = k.timer_key(&format!("origin-{n}"), 1);
                key[..key.rfind('/').unwrap()].to_string()
            })
            .collect();
        assert!(shards.len() > 1, "monotone keys would hammer one prefix");
        assert!(shards.len() <= 4);
    }

    #[test]
    fn a_shard_count_of_zero_is_treated_as_one() {
        let k = KeySpace::new("p", 0);
        assert_eq!(k.timer_shards, 1);
        assert!(k.timer_key("o", 1).starts_with("p/t/00/"));
    }

    #[test]
    fn a_malformed_timer_key_is_not_a_timer() {
        let k = keys();
        assert_eq!(k.parse_timer_key("p/t/00/short"), None);
        assert_eq!(k.parse_timer_key("p/t/00/notanumber_______x"), None);
    }

    // --- the write law ----------------------------------------------------

    #[tokio::test]
    async fn a_create_writes_the_document_and_arms_the_timer() {
        let counting = Counting::new(shared_store());
        let p = pool_with(Arc::clone(&counting) as Arc<dyn Store>, Arc::new(MemDocCache::new(16)));
        let reply = p
            .submit(ORIGIN, create("diff:a", 100_000, json!({ "resonate:target": W })), 1_000)
            .await
            .unwrap();
        assert_eq!(reply.status, 200);
        let (gets, puts, deletes) = counting.counts();
        assert_eq!(gets, 1, "one read to discover the document is absent");
        // The timer object, then the document.
        assert_eq!(puts, 2);
        assert_eq!(deletes, 0, "nothing was armed before");
        let stored = counting
            .get(&keys().doc_key(ORIGIN))
            .await
            .unwrap()
            .expect("document written");
        let doc = codec::decode(&stored.0, ORIGIN).unwrap();
        assert_eq!(doc.gen, 1);
        assert!(counting
            .list(&keys().timer_prefix(), 10)
            .await
            .unwrap()
            .iter()
            .any(|k| k.ends_with(&format!("_{}", "diff"))));
    }

    #[tokio::test]
    async fn a_read_that_changes_nothing_writes_nothing() {
        let counting = Counting::new(shared_store());
        let p = pool_with(Arc::clone(&counting) as Arc<dyn Store>, Arc::new(MemDocCache::new(16)));
        p.submit(ORIGIN, create("diff:a", 100_000, json!({})), 1_000)
            .await
            .unwrap();
        let (_, puts_after_create, _) = counting.counts();

        for _ in 0..5 {
            let reply = p.submit(ORIGIN, get("diff:a"), 2_000).await.unwrap();
            assert_eq!(reply.status, 200);
        }
        let (gets, puts, _) = counting.counts();
        assert_eq!(puts, puts_after_create, "reads wrote nothing");
        assert_eq!(gets, 1, "and never went back to the store");
    }

    #[tokio::test]
    async fn a_read_that_expires_a_promise_does_write() {
        let counting = Counting::new(shared_store());
        let p = pool_with(Arc::clone(&counting) as Arc<dyn Store>, Arc::new(MemDocCache::new(16)));
        p.submit(ORIGIN, create("diff:a", 5_000, json!({ "resonate:target": W })), 1_000)
            .await
            .unwrap();
        let (_, before, _) = counting.counts();
        let reply = p.submit(ORIGIN, get("diff:a"), 9_000).await.unwrap();
        assert_eq!(reply.data["promise"]["state"], "rejected_timedout");
        let (_, after, deletes) = counting.counts();
        assert!(after > before, "ghost-settling is a real transition");
        assert_eq!(deletes, 1, "the origin's timer was disarmed");
    }

    #[tokio::test]
    async fn the_timer_object_moves_with_the_earliest_deadline() {
        let store = shared_store();
        let p = pool_with(Arc::clone(&store), Arc::new(MemDocCache::new(16)));
        p.submit(ORIGIN, create("diff:a", 50_000, json!({ "resonate:target": W })), 0)
            .await
            .unwrap();
        let armed = |store: Arc<dyn Store>| async move {
            store.list(&keys().timer_prefix(), 10).await.unwrap()
        };
        let first = armed(Arc::clone(&store)).await;
        assert_eq!(first.len(), 1, "exactly one timer per origin");
        // The retry deadline (30_000) is nearer than the promise deadline.
        assert!(first[0].contains(&format!("{:020}", 30_000)));

        // Settling removes both deadlines, so the timer object goes away.
        p.submit(
            ORIGIN,
            Req::PromiseSettle(
                serde_json::from_value(
                    json!({ "id": "diff:a", "state": "resolved", "value": {} }),
                )
                .unwrap(),
            ),
            1,
        )
        .await
        .unwrap();
        assert!(armed(store).await.is_empty());
    }

    // --- group commit -----------------------------------------------------

    #[tokio::test]
    async fn concurrent_requests_ride_one_commit() {
        let counting = Counting::new(shared_store());
        let p = Arc::new(pool_with(
            Arc::clone(&counting) as Arc<dyn Store>,
            Arc::new(MemDocCache::new(16)),
        ));
        // Warm the actor and the cache so the batching is not confounded by the
        // first load.
        p.submit(ORIGIN, create("diff:warm", 100_000, json!({})), 0)
            .await
            .unwrap();
        let (_, before, _) = counting.counts();

        let mut handles = Vec::new();
        for n in 0..20 {
            let p = Arc::clone(&p);
            handles.push(tokio::spawn(async move {
                p.submit(
                    ORIGIN,
                    create(&format!("diff:p{n}"), 100_000, json!({})),
                    1_000,
                )
                .await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap().status, 200);
        }
        let (_, after, _) = counting.counts();
        let writes = after - before;
        assert!(writes >= 1);
        assert!(
            writes < 20,
            "group commit should collapse 20 requests into fewer writes, got {writes}"
        );
        // Every promise still landed.
        let stored = counting.get(&keys().doc_key(ORIGIN)).await.unwrap().unwrap();
        let doc = codec::decode(&stored.0, ORIGIN).unwrap();
        assert_eq!(doc.promises.len(), 21);
    }

    #[tokio::test]
    async fn each_request_in_a_batch_sees_the_one_before_it() {
        let p = Arc::new(pool());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let p = Arc::clone(&p);
            handles.push(tokio::spawn(async move {
                p.submit(ORIGIN, create("diff:same", 100_000, json!({})), 1_000)
                    .await
                    .unwrap()
            }));
        }
        let replies: Vec<Reply> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        // Create is idempotent, so all ten must report the same promise.
        for reply in &replies {
            assert_eq!(reply.status, 200);
            assert_eq!(reply.data["promise"]["createdAt"], replies[0].data["promise"]["createdAt"]);
        }
    }

    // --- contention -------------------------------------------------------

    #[tokio::test]
    async fn two_pools_over_one_store_converge_without_losing_a_transition() {
        // Two "nodes": separate caches and actors, one store. Whoever loses the
        // conditional write re-reads and re-decides, so no transition is lost
        // and none is applied twice.
        let store = shared_store();
        let a = Arc::new(pool_with(Arc::clone(&store), Arc::new(MemDocCache::new(16))));
        let b = Arc::new(pool_with(Arc::clone(&store), Arc::new(MemDocCache::new(16))));

        let mut handles = Vec::new();
        for n in 0..12 {
            let pool = if n % 2 == 0 {
                Arc::clone(&a)
            } else {
                Arc::clone(&b)
            };
            handles.push(tokio::spawn(async move {
                pool.submit(
                    ORIGIN,
                    create(&format!("diff:p{n}"), 100_000, json!({})),
                    1_000,
                )
                .await
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap().status, 200);
        }
        let stored = store.get(&keys().doc_key(ORIGIN)).await.unwrap().unwrap();
        let doc = codec::decode(&stored.0, ORIGIN).unwrap();
        assert_eq!(doc.promises.len(), 12, "every create survived");
    }

    #[tokio::test]
    async fn a_stale_cache_entry_costs_a_round_trip_not_correctness() {
        let store = shared_store();
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(16));
        let a = pool_with(Arc::clone(&store), Arc::clone(&cache));
        let b = pool_with(Arc::clone(&store), Arc::new(NoopDocCache));

        a.submit(ORIGIN, create("diff:a", 100_000, json!({})), 0)
            .await
            .unwrap();
        // `b` writes behind `a`'s back, so `a`'s cache entry is now stale.
        b.submit(ORIGIN, create("diff:b", 100_000, json!({})), 0)
            .await
            .unwrap();
        // `a` decides against the stale document, loses the write, re-decides.
        let reply = a
            .submit(ORIGIN, create("diff:c", 100_000, json!({})), 0)
            .await
            .unwrap();
        assert_eq!(reply.status, 200);
        let stored = store.get(&keys().doc_key(ORIGIN)).await.unwrap().unwrap();
        let doc = codec::decode(&stored.0, ORIGIN).unwrap();
        assert_eq!(
            doc.promises.keys().cloned().collect::<Vec<_>>(),
            vec!["diff:a", "diff:b", "diff:c"]
        );
    }

    #[tokio::test]
    async fn giving_up_after_too_many_lost_races_is_a_503() {
        // A cache that hands back the right document under a version that is
        // never current, so every conditional write is refused however many
        // times the batch is re-decided.
        struct StaleVersion {
            held: std::sync::Mutex<Option<Arc<OriginDoc>>>,
        }
        impl DocCache for StaleVersion {
            fn get(&self, _origin: &str) -> Option<super::super::cache::Cached> {
                let doc = self.held.lock().unwrap().clone()?;
                Some(super::super::cache::Cached {
                    doc,
                    etag: Etag("\"never-current\"".into()),
                })
            }
            fn put(&self, _origin: &str, doc: Arc<OriginDoc>, _etag: Etag) {
                *self.held.lock().unwrap() = Some(doc);
            }
            fn invalidate(&self, _origin: &str) {}
            fn clear(&self) {}
        }

        let store = shared_store();
        let cache: Arc<dyn DocCache> = Arc::new(StaleVersion {
            held: std::sync::Mutex::new(None),
        });
        let outbox = Arc::new(Outbox::new(None, "http://server"));
        outbox.set_paused(true);
        let p = ApplierPool::new(
            Arc::clone(&store),
            cache,
            outbox,
            keys(),
            ApplierCfg {
                max_cas_retries: 2,
                ..Default::default()
            },
        );
        p.submit(ORIGIN, create("diff:a", 100_000, json!({})), 0)
            .await
            .unwrap();
        let err = p
            .submit(ORIGIN, create("diff:b", 100_000, json!({})), 0)
            .await
            .expect_err("contended out");
        assert!(err.to_string().contains("contended"), "{err}");
    }

    // --- crash windows ----------------------------------------------------

    #[tokio::test]
    async fn dying_after_the_timer_put_leaves_a_key_the_drain_ignores() {
        let inner = shared_store();
        let faulty = Arc::new(FaultStore::new(Arc::clone(&inner)));
        // Let the timer PUT through and kill the document CAS.
        faulty.fail_after(1);
        let p = pool_with(Arc::clone(&faulty) as Arc<dyn Store>, Arc::new(NoopDocCache));
        let err = p
            .submit(ORIGIN, create("diff:a", 100_000, json!({ "resonate:target": W })), 0)
            .await
            .expect_err("the commit died");
        assert!(err.to_string().contains("injected fault"));

        // The timer is armed for a document that was never written.
        let armed = inner.list(&keys().timer_prefix(), 10).await.unwrap();
        assert_eq!(armed.len(), 1);
        assert_eq!(inner.get(&keys().doc_key(ORIGIN)).await.unwrap(), None);

        // A sweep of that origin finds nothing due and writes nothing, so the
        // orphan is inert until the poller collects it.
        faulty.heal();
        let p = pool_with(Arc::clone(&inner), Arc::new(NoopDocCache));
        p.tick(ORIGIN, 1_000_000).await.unwrap();
        assert_eq!(inner.get(&keys().doc_key(ORIGIN)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn dying_before_the_old_timer_delete_leaves_a_stale_key_only() {
        let inner = shared_store();
        let faulty = Arc::new(FaultStore::new(Arc::clone(&inner)));
        let p = pool_with(Arc::clone(&faulty) as Arc<dyn Store>, Arc::new(MemDocCache::new(4)));
        p.submit(ORIGIN, create("diff:a", 50_000, json!({ "resonate:target": W })), 0)
            .await
            .unwrap();
        let before = inner.list(&keys().timer_prefix(), 10).await.unwrap();
        assert_eq!(before.len(), 1);

        // Settling disarms everything, so there is no new timer to PUT: allow
        // the document CAS, then fail the old timer's DELETE.
        faulty.fail_after(1);
        let reply = p
            .submit(
                ORIGIN,
                Req::PromiseSettle(
                    serde_json::from_value(
                        json!({ "id": "diff:a", "state": "resolved", "value": {} }),
                    )
                    .unwrap(),
                ),
                1_000,
            )
            .await
            .unwrap();
        // The DELETE failing must not fail the request: it only orphans a key.
        assert_eq!(reply.status, 200);
        let stored = inner.get(&keys().doc_key(ORIGIN)).await.unwrap().unwrap();
        let doc = codec::decode(&stored.0, ORIGIN).unwrap();
        assert_eq!(doc.promises["diff:a"].state.as_str(), "resolved");
        assert_eq!(
            inner.list(&keys().timer_prefix(), 10).await.unwrap(),
            before,
            "the stale key is still there, waiting to be collected"
        );
    }

    #[tokio::test]
    async fn an_unanswered_caller_can_retry_the_same_request() {
        // The reply is the last step, so a caller may see Unavailable for a
        // request that committed. Every operation is idempotent, so the retry
        // reports current state.
        let inner = shared_store();
        let faulty = Arc::new(FaultStore::new(Arc::clone(&inner)));
        let p = pool_with(Arc::clone(&faulty) as Arc<dyn Store>, Arc::new(NoopDocCache));
        faulty.fail_after(0);
        assert!(p
            .submit(ORIGIN, create("diff:a", 100_000, json!({})), 0)
            .await
            .is_err());
        faulty.heal();
        let first = p
            .submit(ORIGIN, create("diff:a", 100_000, json!({})), 5)
            .await
            .unwrap();
        let second = p
            .submit(ORIGIN, create("diff:a", 100_000, json!({})), 9)
            .await
            .unwrap();
        assert_eq!(first.data, second.data);
    }

    // --- clock ------------------------------------------------------------

    #[tokio::test]
    async fn an_origins_clock_never_goes_backwards() {
        let store = shared_store();
        let p = pool_with(Arc::clone(&store), Arc::new(MemDocCache::new(4)));
        p.submit(ORIGIN, create("diff:a", 500_000, json!({ "resonate:target": W })), 10_000)
            .await
            .unwrap();
        // A caller with a regressed clock must not un-expire anything.
        let reply = p
            .submit(ORIGIN, create("diff:b", 500_000, json!({})), 1)
            .await
            .unwrap();
        assert_eq!(
            reply.data["promise"]["createdAt"], 10_000,
            "the origin's high-water clock won"
        );
    }

    #[tokio::test]
    async fn a_tick_sweeps_the_origin() {
        let store = shared_store();
        let p = pool_with(Arc::clone(&store), Arc::new(MemDocCache::new(4)));
        p.submit(ORIGIN, create("diff:a", 5_000, json!({ "resonate:target": W })), 0)
            .await
            .unwrap();
        p.tick(ORIGIN, 9_000).await.unwrap();
        let stored = store.get(&keys().doc_key(ORIGIN)).await.unwrap().unwrap();
        let doc = codec::decode(&stored.0, ORIGIN).unwrap();
        assert_eq!(doc.promises["diff:a"].state.as_str(), "rejected_timedout");
        assert_eq!(doc.tasks["diff:a"].state.as_str(), "fulfilled");
    }

    #[tokio::test]
    async fn a_tick_with_nothing_due_writes_nothing() {
        let counting = Counting::new(shared_store());
        let p = pool_with(Arc::clone(&counting) as Arc<dyn Store>, Arc::new(MemDocCache::new(4)));
        p.submit(ORIGIN, create("diff:a", 500_000, json!({})), 0)
            .await
            .unwrap();
        let (_, before, _) = counting.counts();
        for _ in 0..3 {
            p.tick(ORIGIN, 1_000).await.unwrap();
        }
        assert_eq!(counting.counts().1, before);
    }

    #[tokio::test]
    async fn resetting_drops_the_actors_and_the_cache() {
        let store = shared_store();
        let cache: Arc<dyn DocCache> = Arc::new(MemDocCache::new(4));
        let p = pool_with(Arc::clone(&store), Arc::clone(&cache));
        p.submit(ORIGIN, create("diff:a", 500_000, json!({})), 0)
            .await
            .unwrap();
        store.delete_prefix(&keys().doc_prefix()).await.unwrap();
        p.reset().await;
        // Without the reset the cache would still answer with the deleted
        // document.
        let reply = p.submit(ORIGIN, get("diff:a"), 1).await.unwrap();
        assert_eq!(reply.status, 404);
    }

    #[tokio::test]
    async fn a_reaped_actor_is_replaced_on_the_next_request() {
        let store = shared_store();
        let outbox = Arc::new(Outbox::new(None, "http://server"));
        outbox.set_paused(true);
        let p = ApplierPool::new(
            Arc::clone(&store),
            Arc::new(MemDocCache::new(4)),
            outbox,
            keys(),
            ApplierCfg {
                idle_timeout: Duration::from_millis(20),
                ..Default::default()
            },
        );
        p.submit(ORIGIN, create("diff:a", 500_000, json!({})), 0)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(p.shared.actors.lock().await.is_empty(), "idle actor reaped");
        let reply = p.submit(ORIGIN, get("diff:a"), 1).await.unwrap();
        assert_eq!(reply.status, 200);
    }

    #[tokio::test]
    async fn a_corrupt_document_is_refused_rather_than_guessed_at() {
        let store = shared_store();
        store
            .put(&keys().doc_key(ORIGIN), b"not a document".to_vec())
            .await
            .unwrap();
        let p = pool_with(Arc::clone(&store), Arc::new(NoopDocCache));
        let err = p.submit(ORIGIN, get("diff:a"), 0).await.expect_err("refused");
        assert!(err.to_string().contains("unreadable"), "{err}");
    }

    #[tokio::test]
    async fn a_document_that_names_another_origin_is_refused() {
        let store = shared_store();
        let doc = OriginDoc::default();
        store
            .put(&keys().doc_key(ORIGIN), codec::encode(&doc, "somewhere-else"))
            .await
            .unwrap();
        let p = pool_with(Arc::clone(&store), Arc::new(NoopDocCache));
        let err = p.submit(ORIGIN, get("diff:a"), 0).await.expect_err("refused");
        assert!(err.to_string().contains("origin"), "{err}");
    }

    #[tokio::test]
    async fn sends_reach_the_outbox_only_after_the_commit() {
        let inner = shared_store();
        let faulty = Arc::new(FaultStore::new(Arc::clone(&inner)));
        let outbox = Arc::new(Outbox::new(None, "http://server"));
        outbox.set_paused(true);
        let p = ApplierPool::new(
            Arc::clone(&faulty) as Arc<dyn Store>,
            Arc::new(NoopDocCache),
            Arc::clone(&outbox),
            keys(),
            ApplierCfg::default(),
        );
        // Kill the document CAS, after the timer PUT.
        faulty.fail_after(1);
        assert!(p
            .submit(ORIGIN, create("diff:a", 100_000, json!({ "resonate:target": W })), 0)
            .await
            .is_err());
        assert!(
            outbox.snapshot().is_empty(),
            "nothing is sent for a commit that did not happen"
        );

        faulty.heal();
        p.submit(ORIGIN, create("diff:a", 100_000, json!({ "resonate:target": W })), 0)
            .await
            .unwrap();
        assert_eq!(outbox.snapshot().len(), 1);
    }
}
