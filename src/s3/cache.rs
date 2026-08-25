//! The document read cache.
//!
//! Safe only because every write is conditional. A stale entry, or a missing
//! one, costs a round trip — the conditional write fails, the applier re-reads
//! and re-decides — and never costs correctness. That is what lets an
//! implementation evict, drop, or persist entries however it likes, and what
//! would let a future tier spill to local NVMe under this same trait.
//!
//! The API is deliberately synchronous: a cache lookup must never await, or the
//! hit path stops being a hit. A tiered implementation that needs I/O does it
//! internally and reports a miss.
//!
//! Only the applier writes here, with either a post-write ETag or a fresh read,
//! and only the applier invalidates — on a lost conditional write. Because the
//! applier serializes per origin, no two writers race on one key.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::kernel::state::OriginDoc;

use super::store::Etag;

/// A document and the version it was read at.
#[derive(Debug, Clone)]
pub struct Cached {
    pub doc: Arc<OriginDoc>,
    pub etag: Etag,
}

pub trait DocCache: Send + Sync {
    fn get(&self, origin: &str) -> Option<Cached>;
    fn put(&self, origin: &str, doc: Arc<OriginDoc>, etag: Etag);
    fn invalidate(&self, origin: &str);
    /// Drop everything. `debug.reset` deletes the objects out from under the
    /// cache, so it has to say so.
    fn clear(&self);
}

struct Inner {
    entries: HashMap<String, (Cached, u64)>,
    /// Access sequence to origin, so the least recently used is the first key.
    order: BTreeMap<u64, String>,
}

/// A bounded in-memory cache, least-recently-used first out.
pub struct MemDocCache {
    capacity: usize,
    seq: AtomicU64,
    inner: Mutex<Inner>,
}

impl MemDocCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seq: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                order: BTreeMap::new(),
            }),
        }
    }

    /// Entries held. Diagnostics and tests.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock cannot corrupt a pure read cache:
        // the worst an inconsistent entry can do is force one more round trip.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }
}

impl DocCache for MemDocCache {
    fn get(&self, origin: &str) -> Option<Cached> {
        let seq = self.next_seq();
        let mut inner = self.lock();
        let (cached, old_seq) = inner.entries.get_mut(origin)?;
        let cached = cached.clone();
        let previous = std::mem::replace(old_seq, seq);
        inner.order.remove(&previous);
        inner.order.insert(seq, origin.to_string());
        Some(cached)
    }

    fn put(&self, origin: &str, doc: Arc<OriginDoc>, etag: Etag) {
        let seq = self.next_seq();
        let mut inner = self.lock();
        if let Some((_, previous)) = inner.entries.get(origin) {
            let previous = *previous;
            inner.order.remove(&previous);
        }
        inner
            .entries
            .insert(origin.to_string(), (Cached { doc, etag }, seq));
        inner.order.insert(seq, origin.to_string());
        while inner.entries.len() > self.capacity {
            let (oldest_seq, oldest) = match inner.order.iter().next() {
                Some((s, o)) => (*s, o.clone()),
                None => break,
            };
            inner.order.remove(&oldest_seq);
            inner.entries.remove(&oldest);
        }
    }

    fn invalidate(&self, origin: &str) {
        let mut inner = self.lock();
        if let Some((_, seq)) = inner.entries.remove(origin) {
            inner.order.remove(&seq);
        }
    }

    fn clear(&self) {
        let mut inner = self.lock();
        inner.entries.clear();
        inner.order.clear();
    }
}

/// A cache that remembers nothing.
///
/// Every read goes to the store, which is the point: it turns "is this bug a
/// caching bug?" into a one-line change.
pub struct NoopDocCache;

impl DocCache for NoopDocCache {
    fn get(&self, _origin: &str) -> Option<Cached> {
        None
    }
    fn put(&self, _origin: &str, _doc: Arc<OriginDoc>, _etag: Etag) {}
    fn invalidate(&self, _origin: &str) {}
    fn clear(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(clock: i64) -> Arc<OriginDoc> {
        Arc::new(OriginDoc {
            clock,
            ..Default::default()
        })
    }

    fn etag(s: &str) -> Etag {
        Etag(s.to_string())
    }

    #[test]
    fn a_miss_is_none() {
        let c = MemDocCache::new(4);
        assert!(c.get("o").is_none());
    }

    #[test]
    fn a_hit_returns_the_document_and_its_version() {
        let c = MemDocCache::new(4);
        c.put("o", doc(5), etag("\"1\""));
        let hit = c.get("o").expect("hit");
        assert_eq!(hit.doc.clock, 5);
        assert_eq!(hit.etag, etag("\"1\""));
    }

    #[test]
    fn a_second_put_replaces_the_first() {
        let c = MemDocCache::new(4);
        c.put("o", doc(5), etag("\"1\""));
        c.put("o", doc(6), etag("\"2\""));
        assert_eq!(c.len(), 1);
        assert_eq!(c.get("o").unwrap().doc.clock, 6);
    }

    #[test]
    fn invalidation_forces_the_next_read_to_the_store() {
        let c = MemDocCache::new(4);
        c.put("o", doc(5), etag("\"1\""));
        c.invalidate("o");
        assert!(c.get("o").is_none());
        assert!(c.is_empty());
        // Invalidating what is not cached is fine.
        c.invalidate("o");
    }

    #[test]
    fn the_least_recently_used_entry_is_evicted() {
        let c = MemDocCache::new(2);
        c.put("a", doc(1), etag("\"1\""));
        c.put("b", doc(2), etag("\"2\""));
        // Touch `a`, so `b` becomes the oldest.
        assert!(c.get("a").is_some());
        c.put("c", doc(3), etag("\"3\""));
        assert_eq!(c.len(), 2);
        assert!(c.get("a").is_some(), "recently used survives");
        assert!(c.get("c").is_some());
        assert!(c.get("b").is_none(), "least recently used was evicted");
    }

    #[test]
    fn re_putting_an_entry_makes_it_the_newest() {
        let c = MemDocCache::new(2);
        c.put("a", doc(1), etag("\"1\""));
        c.put("b", doc(2), etag("\"2\""));
        c.put("a", doc(3), etag("\"3\""));
        c.put("c", doc(4), etag("\"4\""));
        assert!(c.get("b").is_none());
        assert_eq!(c.get("a").unwrap().doc.clock, 3);
    }

    #[test]
    fn a_zero_capacity_cache_still_holds_one_entry() {
        // Rounding up beats a cache that silently never hits.
        let c = MemDocCache::new(0);
        c.put("a", doc(1), etag("\"1\""));
        assert!(c.get("a").is_some());
    }

    #[test]
    fn clearing_drops_everything() {
        let c = MemDocCache::new(4);
        c.put("a", doc(1), etag("\"1\""));
        c.put("b", doc(2), etag("\"2\""));
        c.clear();
        assert!(c.is_empty());
        assert!(c.get("a").is_none());
    }

    #[test]
    fn the_noop_cache_never_hits() {
        let c = NoopDocCache;
        c.put("a", doc(1), etag("\"1\""));
        assert!(c.get("a").is_none());
        c.invalidate("a");
        c.clear();
    }

    #[test]
    fn eviction_keeps_the_order_index_in_step_with_the_entries() {
        // A leak here would grow without bound and evict the wrong entry.
        let c = MemDocCache::new(3);
        for n in 0..50 {
            let key = format!("o{}", n % 5);
            c.put(&key, doc(n), etag("\"e\""));
            let _ = c.get("o0");
        }
        let inner = c.lock();
        assert_eq!(inner.entries.len(), inner.order.len());
        assert!(inner.entries.len() <= 3);
    }
}
