//! The object-store port: bytes in, bytes out, one conditional write.
//!
//! # Contract
//!
//! Six operations, because that is all the shell needs: read an object with
//! its version, write it conditionally (on a version, or on absence), write it
//! unconditionally, delete it, and list a prefix in ascending key order —
//! read-after-write consistent, capped at `max_keys` smallest keys. Everything
//! about *what* the bytes mean lives in [`codec`](super::codec).
//!
//! The error taxonomy is the load-bearing part. A conditional write can fail
//! two ways and they demand opposite responses:
//!
//! - [`StoreError::PreconditionFailed`] — someone else wrote first. The
//!   decision was made against state that is now stale, so it must be
//!   *re-decided* against the current state. Replaying it would be wrong.
//! - [`StoreError::Conflict`] — S3's `ConditionalRequestConflict`, a concurrent
//!   conditional write the service could not order. Nothing is known about
//!   whether it landed; retry the same conditional write, and if *that* comes
//!   back `PreconditionFailed`, fall into the re-decide path.
//!
//! Handling only the first is the classic footgun, which is why they are
//! separate variants rather than one "write failed".
//!
//! **Not every S3-compatible store qualifies.** Real conditional writes are
//! required: S3, R2, GCS and Azure have them; MinIO, B2 and Spaces do not, and
//! silently lose writes if pointed at.
//!
//! # Dependencies
//!
//! The `object_store` crate alone. This module knows nothing about documents,
//! keys' meanings, or the kernel.
//!
//! # Dependants
//!
//! Everything in the backend that touches the bucket goes through [`Store`]:
//! the applier (documents and timer objects), the timer poller (listing and
//! collecting due keys), the schedule service, the scan service, and
//! `S3Server` itself (readiness, `debug.reset`). Tests use [`FaultStore`] to
//! cut the power between two effects and inspect exactly what landed.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use object_store::{
    path::Path, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};

/// An object's version, as the store reports it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Etag(pub String);

/// Why a store operation did not do what was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// Lost the race: the object is not at the version the write required.
    /// Re-read, re-decide, never replay.
    PreconditionFailed,
    /// The service could not order two concurrent conditional writes. Nothing
    /// is known about whether this one landed; retry it.
    Conflict,
    /// No answer at all. Surfaces as a 503 to the caller.
    Unavailable(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::PreconditionFailed => f.write_str("precondition failed"),
            StoreError::Conflict => f.write_str("conditional request conflict"),
            StoreError::Unavailable(m) => write!(f, "store unavailable: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// An object store with real conditional writes.
#[async_trait]
pub trait Store: Send + Sync {
    /// Read an object and its version. `Ok(None)` if it does not exist.
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError>;

    /// Replace the object only if it is still at `etag`.
    async fn put_if_match(&self, key: &str, body: Vec<u8>, etag: &Etag)
        -> Result<Etag, StoreError>;

    /// Create the object only if nothing is there.
    async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError>;

    /// Write unconditionally. Used for timer objects, where the *key* carries
    /// the value being written and a blind overwrite is therefore idempotent.
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError>;

    /// Remove the object. Removing what is not there succeeds.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    /// The `max_keys` lexicographically smallest keys under `prefix`, ascending.
    ///
    /// Ascending order is what makes the timer prefix work: deadlines are
    /// zero-padded into the key, so the smallest keys *are* the nearest
    /// deadlines. `object_store` promises no order, so implementations must
    /// impose it.
    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError>;

    /// Delete everything under `prefix`.
    ///
    /// Provided rather than required: paging through [`Store::list`] and
    /// deleting is correct for any implementation, and only `debug.reset` and
    /// tests need it.
    async fn delete_prefix(&self, prefix: &str) -> Result<(), StoreError> {
        loop {
            let keys = self.list(prefix, 1_000).await?;
            if keys.is_empty() {
                return Ok(());
            }
            for key in keys {
                self.delete(&key).await?;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// object_store adapter
// ---------------------------------------------------------------------------

/// [`Store`] over anything `object_store` can talk to.
///
/// Conditional writes go through `PutMode::Update`/`PutMode::Create`, which is
/// why this crate was chosen: the CAS is built in, and its `InMemory` store
/// implements it faithfully enough to drive the differential suite.
///
/// Generic over the concrete store rather than holding an `Arc<dyn
/// ObjectStore>`: `get` and `delete` live on `ObjectStoreExt`, which is not
/// object-safe. [`Store`] is the dynamic boundary, so nothing is lost.
pub struct ObjectStoreAdapter<T: ObjectStore> {
    inner: T,
}

impl ObjectStoreAdapter<object_store::memory::InMemory> {
    /// An in-process store with real conditional-write semantics — the backing
    /// store for tests and the differential suite.
    pub fn in_memory() -> Self {
        Self::new(object_store::memory::InMemory::new())
    }
}

impl<T: ObjectStore> ObjectStoreAdapter<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    fn path(key: &str) -> Result<Path, StoreError> {
        Path::parse(key).map_err(|e| StoreError::Unavailable(format!("bad key {key}: {e}")))
    }

    fn etag(e_tag: Option<String>, key: &str) -> Result<Etag, StoreError> {
        e_tag.map(Etag).ok_or_else(|| {
            StoreError::Unavailable(format!(
                "store reported no ETag for {key}; conditional writes are impossible without one"
            ))
        })
    }

    async fn put_with(&self, key: &str, body: Vec<u8>, mode: PutMode) -> Result<Etag, StoreError> {
        let path = Self::path(key)?;
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        let result = self
            .inner
            .put_opts(&path, PutPayload::from(body), opts)
            .await
            .map_err(map_error)?;
        Self::etag(result.e_tag, key)
    }
}

/// Map `object_store`'s errors onto the taxonomy the shell branches on.
fn map_error(e: object_store::Error) -> StoreError {
    match &e {
        object_store::Error::Precondition { .. }
        | object_store::Error::AlreadyExists { .. }
        | object_store::Error::NotModified { .. } => StoreError::PreconditionFailed,
        // S3 reports an unorderable pair of conditional writes as a 409
        // ConditionalRequestConflict, which arrives as a generic error.
        other => {
            let text = other.to_string();
            if text.contains("ConditionalRequestConflict") || text.contains("409 Conflict") {
                StoreError::Conflict
            } else {
                StoreError::Unavailable(text)
            }
        }
    }
}

#[async_trait]
impl<T: ObjectStore> Store for ObjectStoreAdapter<T> {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        let path = Self::path(key)?;
        let result = match self.inner.get(&path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(map_error(e)),
        };
        let etag = Self::etag(result.meta.e_tag.clone(), key)?;
        let bytes = result.bytes().await.map_err(map_error)?;
        Ok(Some((bytes.to_vec(), etag)))
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        self.put_with(
            key,
            body,
            PutMode::Update(UpdateVersion {
                e_tag: Some(etag.0.clone()),
                version: None,
            }),
        )
        .await
    }

    async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.put_with(key, body, PutMode::Create).await
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.put_with(key, body, PutMode::Overwrite).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let path = Self::path(key)?;
        match self.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_error(e)),
        }
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        use futures::StreamExt;
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        let path = Self::path(prefix)?;
        let mut stream = self.inner.list(Some(&path));
        // `object_store` guarantees no order, so keep the smallest `max_keys`
        // seen. Bounded memory, and correct whatever order the stream arrives
        // in.
        let mut smallest: BTreeSet<String> = BTreeSet::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(map_error)?;
            smallest.insert(meta.location.to_string());
            if smallest.len() > max_keys {
                let last = smallest.iter().next_back().cloned().expect("non-empty");
                smallest.remove(&last);
            }
        }
        Ok(smallest.into_iter().collect())
    }
}

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

/// A [`Store`] that stops writing after a set number of writes.
///
/// The crash windows the shell claims to survive are only claims until
/// something cuts the power between two effects. This is that something: the
/// reads keep working, so a test can inspect exactly what landed.
pub struct FaultStore {
    inner: Arc<dyn Store>,
    /// Writes left before failing; negative means never fail.
    budget: AtomicI64,
    writes: AtomicU64,
}

impl FaultStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            budget: AtomicI64::new(-1),
            writes: AtomicU64::new(0),
        }
    }

    /// Let `n` more writes through, then fail every write after them.
    pub fn fail_after(&self, n: u64) {
        self.budget.store(n as i64, Ordering::SeqCst);
    }

    /// Stop failing.
    pub fn heal(&self) {
        self.budget.store(-1, Ordering::SeqCst);
    }

    /// Writes attempted so far, failed ones included.
    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }

    fn check(&self) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let budget = self.budget.load(Ordering::SeqCst);
        if budget < 0 {
            return Ok(());
        }
        if self.budget.fetch_sub(1, Ordering::SeqCst) <= 0 {
            return Err(StoreError::Unavailable("injected fault".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl Store for FaultStore {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        self.inner.get(key).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        self.check()?;
        self.inner.put_if_match(key, body, etag).await
    }

    async fn put_if_none_match(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.check()?;
        self.inner.put_if_none_match(key, body).await
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.check()?;
        self.inner.put(key, body).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.check()?;
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix, max_keys).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> ObjectStoreAdapter<object_store::memory::InMemory> {
        ObjectStoreAdapter::in_memory()
    }

    #[tokio::test]
    async fn reading_what_is_not_there_is_not_an_error() {
        assert_eq!(store().get("wf/a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_create_writes_once() {
        let s = store();
        let etag = s.put_if_none_match("wf/a", b"one".to_vec()).await.unwrap();
        let (body, read_etag) = s.get("wf/a").await.unwrap().expect("present");
        assert_eq!(body, b"one");
        assert_eq!(read_etag, etag);

        // The second create loses: the object is already there.
        assert_eq!(
            s.put_if_none_match("wf/a", b"two".to_vec()).await,
            Err(StoreError::PreconditionFailed)
        );
        assert_eq!(s.get("wf/a").await.unwrap().unwrap().0, b"one");
    }

    #[tokio::test]
    async fn a_conditional_write_needs_the_current_version() {
        let s = store();
        let first = s.put_if_none_match("wf/a", b"one".to_vec()).await.unwrap();
        let second = s
            .put_if_match("wf/a", b"two".to_vec(), &first)
            .await
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(s.get("wf/a").await.unwrap().unwrap().0, b"two");

        // The stale version is refused — this is the signal to re-read and
        // re-decide, never to replay.
        assert_eq!(
            s.put_if_match("wf/a", b"three".to_vec(), &first).await,
            Err(StoreError::PreconditionFailed)
        );
        assert_eq!(s.get("wf/a").await.unwrap().unwrap().0, b"two");
    }

    #[tokio::test]
    async fn a_conditional_write_to_a_missing_object_is_refused() {
        let s = store();
        assert_eq!(
            s.put_if_match("wf/a", b"x".to_vec(), &Etag("\"1\"".into()))
                .await,
            Err(StoreError::PreconditionFailed)
        );
    }

    #[tokio::test]
    async fn an_unconditional_write_always_lands() {
        // Timer objects are written this way: the key carries the deadline, so
        // a blind overwrite cannot lose information.
        let s = store();
        s.put("t/00/1", b"".to_vec()).await.unwrap();
        s.put("t/00/1", b"".to_vec()).await.unwrap();
        assert_eq!(s.get("t/00/1").await.unwrap().unwrap().0, b"");
    }

    #[tokio::test]
    async fn deleting_what_is_not_there_succeeds() {
        let s = store();
        s.delete("wf/missing").await.unwrap();
        s.put("wf/a", b"x".to_vec()).await.unwrap();
        s.delete("wf/a").await.unwrap();
        s.delete("wf/a").await.unwrap();
        assert_eq!(s.get("wf/a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn listing_is_ascending_and_scoped_to_the_prefix() {
        let s = store();
        for key in ["t/00/00000000000000000300_b", "t/00/00000000000000000100_a"] {
            s.put(key, b"".to_vec()).await.unwrap();
        }
        s.put("t/01/00000000000000000200_c", b"".to_vec())
            .await
            .unwrap();
        s.put("wf/elsewhere", b"".to_vec()).await.unwrap();

        assert_eq!(
            s.list("t/00", 10).await.unwrap(),
            vec![
                "t/00/00000000000000000100_a".to_string(),
                "t/00/00000000000000000300_b".to_string()
            ]
        );
        assert_eq!(s.list("t", 10).await.unwrap().len(), 3);
        assert_eq!(s.list("wf", 10).await.unwrap(), vec!["wf/elsewhere"]);
    }

    #[tokio::test]
    async fn a_capped_list_returns_the_smallest_keys() {
        // The cap is how the timer poller reads nearest-deadline-first, so it
        // must be the *smallest* keys, not the first ones the store happens to
        // hand over.
        let s = store();
        for n in [500, 100, 400, 200, 300] {
            s.put(&format!("t/00/{n:020}_o"), b"".to_vec())
                .await
                .unwrap();
        }
        assert_eq!(
            s.list("t/00", 2).await.unwrap(),
            vec![format!("t/00/{:020}_o", 100), format!("t/00/{:020}_o", 200)]
        );
        assert!(s.list("t/00", 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_prefix_can_be_emptied() {
        let s = store();
        for n in 0..5 {
            s.put(&format!("p/wf/{n}"), b"x".to_vec()).await.unwrap();
        }
        s.put("q/wf/keep", b"x".to_vec()).await.unwrap();
        s.delete_prefix("p").await.unwrap();
        assert!(s.list("p", 10).await.unwrap().is_empty());
        assert_eq!(s.list("q", 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_fault_store_fails_writes_after_its_budget_and_keeps_reading() {
        let inner: Arc<dyn Store> = Arc::new(store());
        let faulty = FaultStore::new(Arc::clone(&inner));
        faulty.fail_after(2);

        faulty.put("a", b"1".to_vec()).await.unwrap();
        faulty.put("b", b"2".to_vec()).await.unwrap();
        assert_eq!(
            faulty.put("c", b"3".to_vec()).await,
            Err(StoreError::Unavailable("injected fault".into()))
        );
        // Reads are untouched, so a test can see exactly what landed.
        assert_eq!(faulty.get("b").await.unwrap().unwrap().0, b"2");
        assert_eq!(faulty.get("c").await.unwrap(), None);
        assert_eq!(faulty.writes(), 3);

        faulty.heal();
        faulty.put("c", b"3".to_vec()).await.unwrap();
        assert_eq!(faulty.get("c").await.unwrap().unwrap().0, b"3");
    }

    #[tokio::test]
    async fn a_fault_store_without_a_budget_never_fails() {
        let inner: Arc<dyn Store> = Arc::new(store());
        let faulty = FaultStore::new(inner);
        for n in 0..10 {
            faulty.put(&format!("k{n}"), b"x".to_vec()).await.unwrap();
        }
        assert_eq!(faulty.writes(), 10);
    }
}
