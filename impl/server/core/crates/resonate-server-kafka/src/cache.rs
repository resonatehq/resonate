//! A partition's hot documents, decoded: an LRU bounded by promises held.
//!
//! # Contract
//!
//! RocksDB caches bytes; this caches what the kernel decides over. Loading an
//! origin from the store means scanning its records and decoding every one,
//! so a busy origin pays its own size on every request. With the decoded
//! document kept here, it pays nothing to load.
//!
//! **Owned by the partition's actor, and only by it.** The actor is the
//! partition's one writer, so it is the one place that knows when a document
//! changes; it takes a document out at the start of a round and puts the
//! decided one back only once the round has committed. A round that fails
//! leaves nothing behind, so the next load reads the store, which the failed
//! round never touched. And the cache lives and dies with the actor: a
//! partition taken over again — after a fence, after an uncertain commit,
//! after moving away and back — starts with an empty one, so nothing a
//! previous owner did can be served stale.
//!
//! **Bounded by promises, not documents.** Origins range from one promise to
//! thousands, so the budget is the number of promises held across all cached
//! documents, a better proxy for memory than a count of entries. A document
//! larger than the whole budget is not cached at all.

use std::collections::{BTreeMap, HashMap};

use resonate_server_blob::kernel::state::OriginDoc;

struct Entry {
    doc: OriginDoc,
    tick: u64,
    weight: usize,
}

pub struct DocCache {
    budget: usize,
    used: usize,
    tick: u64,
    entries: HashMap<String, Entry>,
    /// Last use to origin: the least recently used is the first key.
    order: BTreeMap<u64, String>,
    hits: u64,
    misses: u64,
}

/// What a document costs against the budget. An empty document still costs
/// one: it is worth caching (it answers a 404 without a scan) but not free.
fn weight(doc: &OriginDoc) -> usize {
    doc.promises.len().max(1)
}

impl DocCache {
    /// A cache holding at most `budget` promises. Zero turns it off.
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            used: 0,
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
            hits: 0,
            misses: 0,
        }
    }

    /// Take `origin`'s document out, if it is here. Taking, not borrowing:
    /// the round decides over it, and only a committed round puts it back.
    pub fn take(&mut self, origin: &str) -> Option<OriginDoc> {
        match self.entries.remove(origin) {
            Some(entry) => {
                self.order.remove(&entry.tick);
                self.used -= entry.weight;
                self.hits += 1;
                crate::metrics::DOC_CACHE.with_label_values(&["hit"]).inc();
                Some(entry.doc)
            }
            None => {
                self.misses += 1;
                crate::metrics::DOC_CACHE.with_label_values(&["miss"]).inc();
                None
            }
        }
    }

    /// Hold `doc` as `origin`'s committed document, evicting the least
    /// recently used until it fits.
    pub fn put(&mut self, origin: String, doc: OriginDoc) {
        if let Some(old) = self.entries.remove(&origin) {
            self.order.remove(&old.tick);
            self.used -= old.weight;
        }
        let weight = weight(&doc);
        if weight > self.budget {
            return;
        }
        while self.used + weight > self.budget {
            let Some((_, victim)) = self.order.pop_first() else {
                break;
            };
            if let Some(e) = self.entries.remove(&victim) {
                self.used -= e.weight;
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, origin.clone());
        self.entries.insert(
            origin,
            Entry {
                doc,
                tick: self.tick,
                weight,
            },
        );
        self.used += weight;
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.used = 0;
    }

    /// Documents held.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Promises held, against the budget.
    pub fn used(&self) -> usize {
        self.used
    }

    /// `(hits, misses)` since the cache was made.
    pub fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::{PromiseState, PromiseValue};
    use resonate_server_blob::kernel::state::PromiseDoc;
    use std::collections::BTreeMap;

    fn doc(promises: usize) -> OriginDoc {
        let mut d = OriginDoc::default();
        for i in 0..promises {
            d.promises.insert(
                format!("o:{i}"),
                PromiseDoc {
                    state: PromiseState::Pending,
                    param: PromiseValue::default(),
                    value: PromiseValue::default(),
                    tags: BTreeMap::new(),
                    timeout_at: 1,
                    created_at: 0,
                    settled_at: None,
                    callbacks: vec![],
                    listeners: vec![],
                },
            );
        }
        d
    }

    #[test]
    fn a_taken_document_is_gone_until_it_is_put_back() {
        let mut c = DocCache::new(100);
        c.put("a".into(), doc(3));
        assert_eq!(c.used(), 3);
        let d = c.take("a").expect("cached");
        assert_eq!(d.promises.len(), 3);
        assert!(c.take("a").is_none(), "a round holds it; nobody else may");
        assert_eq!(c.used(), 0);
        c.put("a".into(), d);
        assert_eq!(c.stats(), (1, 1));
    }

    #[test]
    fn the_least_recently_used_goes_first() {
        let mut c = DocCache::new(10);
        c.put("a".into(), doc(4));
        c.put("b".into(), doc(4));
        // Using a moves it to the back.
        let a = c.take("a").unwrap();
        c.put("a".into(), a);
        c.put("c".into(), doc(4));
        assert!(c.take("b").is_none(), "b was least recently used");
        assert!(c.take("a").is_some());
        assert!(c.take("c").is_some());
    }

    #[test]
    fn the_budget_counts_promises_and_holds() {
        let mut c = DocCache::new(10);
        for i in 0..20 {
            c.put(format!("o{i}"), doc(3));
            assert!(c.used() <= 10);
        }
        assert_eq!(c.len(), 3);
        // Too big for the whole budget: not cached, and nothing evicted for it.
        c.put("huge".into(), doc(11));
        assert!(c.take("huge").is_none());
        assert_eq!(c.len(), 3);
        // An empty document still costs one.
        c.put("empty".into(), doc(0));
        assert!(c.used() <= 10);
    }

    #[test]
    fn a_zero_budget_caches_nothing() {
        let mut c = DocCache::new(0);
        c.put("a".into(), doc(1));
        assert!(c.is_empty());
    }
}
