//! The local store in RocksDB: one database per node, one column family per
//! partition, and no write-ahead log.
//!
//! # Why no WAL
//!
//! Kafka is already the log. Every batch applied here was committed there
//! first, and a restart resumes reading Kafka from the checkpoint this store
//! holds. So the only property needed of a crash is that the checkpoint on disk
//! never gets ahead of the data on disk — and that holds without a WAL,
//! because each batch writes its records *and* its checkpoint into the
//! partition's own column family in one write, and RocksDB flushes a column
//! family's memtable as a unit. A crash loses the unflushed tail of batches,
//! checkpoint and data together, and the restore re-reads exactly that tail.
//! Data ahead of the checkpoint (never the other way round) is harmless:
//! every record is a whole version of its object, so re-applying it converges.
//!
//! A batch touches one column family only, which is why `atomic_flush` is not
//! needed.
//!
//! # Why column families
//!
//! A partition moves between nodes on its own, and a column family gives it a
//! life of its own inside one database: created on first assignment, kept
//! across a revocation so a quick return only replays the tail, and dropped in
//! one cheap operation — no range tombstones — when its copy is no longer
//! wanted. One shared block cache and one write-buffer manager bound memory
//! across all of them, so memory does not grow with the partition count.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode,
    MultiThreaded, Options, WriteBatch, WriteBufferManager, WriteOptions,
};

use super::{LocalStore, Op, PartitionStore, Row};
use crate::log::Checkpoint;

type Db = rocksdb::DBWithThreadMode<MultiThreaded>;

/// Where a column family keeps the checkpoint it has applied up to. Outside
/// the `o` and `s` prefixes, so no scan ever returns it.
const CHECKPOINT_KEY: &[u8] = b"m/checkpoint";

/// Tuning for the node's database.
#[derive(Debug, Clone)]
pub struct RocksCfg {
    /// Block cache shared by every column family.
    pub block_cache_bytes: usize,
    /// Memtable budget shared by every column family.
    pub write_buffer_bytes: usize,
}

impl Default for RocksCfg {
    fn default() -> Self {
        Self {
            block_cache_bytes: 256 << 20,
            write_buffer_bytes: 128 << 20,
        }
    }
}

fn cf_name(partition: u32) -> String {
    format!("p-{partition:05}")
}

fn partition_of_cf(name: &str) -> Option<u32> {
    name.strip_prefix("p-")?.parse().ok()
}

pub struct RocksLocal {
    db: Arc<Db>,
    cf_opts: Options,
    known: Mutex<BTreeSet<u32>>,
    path: PathBuf,
    // Held so they outlive every column family that refers to them.
    _cache: Cache,
    _buffers: WriteBufferManager,
}

impl RocksLocal {
    /// Open the node's database at `path`, with every column family it has.
    pub fn open(path: &Path, cfg: &RocksCfg) -> Result<Arc<Self>, String> {
        let cache = Cache::new_lru_cache(cfg.block_cache_bytes);
        let buffers = WriteBufferManager::new_write_buffer_manager_with_cache(
            cfg.write_buffer_bytes,
            false,
            cache.clone(),
        );

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_manager(&buffers);

        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        let mut cf_opts = Options::default();
        cf_opts.set_block_based_table_factory(&table);
        cf_opts.set_compression_type(DBCompressionType::Lz4);

        // Every column family that exists must be opened, owned or not.
        let existing = Db::list_cf(&db_opts, path).unwrap_or_else(|_| vec!["default".to_string()]);
        let known: BTreeSet<u32> = existing.iter().filter_map(|n| partition_of_cf(n)).collect();
        let descriptors = existing
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, cf_opts.clone()));
        let db = Db::open_cf_descriptors(&db_opts, path, descriptors)
            .map_err(|e| format!("cannot open RocksDB at {}: {e}", path.display()))?;

        Ok(Arc::new(Self {
            db: Arc::new(db),
            cf_opts,
            known: Mutex::new(known),
            path: path.to_path_buf(),
            _cache: cache,
            _buffers: buffers,
        }))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn known(&self) -> std::sync::MutexGuard<'_, BTreeSet<u32>> {
        self.known.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl LocalStore for RocksLocal {
    fn open(&self, partition: u32) -> Result<Arc<dyn PartitionStore>, String> {
        let name = cf_name(partition);
        let mut known = self.known();
        if !known.contains(&partition) {
            self.db
                .create_cf(&name, &self.cf_opts)
                .map_err(|e| format!("cannot create column family {name}: {e}"))?;
            known.insert(partition);
        }
        Ok(Arc::new(RocksPartition {
            db: Arc::clone(&self.db),
            name,
        }))
    }

    fn drop_partition(&self, partition: u32) -> Result<(), String> {
        let mut known = self.known();
        if known.remove(&partition) {
            let name = cf_name(partition);
            self.db
                .drop_cf(&name)
                .map_err(|e| format!("cannot drop column family {name}: {e}"))?;
        }
        Ok(())
    }

    fn partitions(&self) -> Result<Vec<u32>, String> {
        Ok(self.known().iter().copied().collect())
    }
}

pub struct RocksPartition {
    db: Arc<Db>,
    name: String,
}

impl RocksPartition {
    fn cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, String> {
        self.db
            .cf_handle(&self.name)
            .ok_or_else(|| format!("column family {} was dropped", self.name))
    }
}

impl PartitionStore for RocksPartition {
    fn checkpoint(&self) -> Result<Option<Checkpoint>, String> {
        let cf = self.cf()?;
        match self
            .db
            .get_cf(&cf, CHECKPOINT_KEY)
            .map_err(|e| e.to_string())?
        {
            Some(bytes) => Checkpoint::from_bytes(&bytes)
                .map(Some)
                .ok_or_else(|| format!("{}: unreadable checkpoint", self.name)),
            None => Ok(None),
        }
    }

    fn apply(&self, ops: Vec<Op>, checkpoint: Checkpoint) -> Result<(), String> {
        let cf = self.cf()?;
        let mut batch = WriteBatch::default();
        for (key, value) in ops {
            match value {
                Some(v) => batch.put_cf(&cf, key, v),
                None => batch.delete_cf(&cf, key),
            }
        }
        // In the same batch as the data it describes: see the module docs.
        batch.put_cf(&cf, CHECKPOINT_KEY, checkpoint.to_bytes());
        let mut opts = WriteOptions::default();
        opts.disable_wal(true);
        self.db.write_opt(batch, &opts).map_err(|e| e.to_string())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let cf = self.cf()?;
        self.db.get_cf(&cf, key).map_err(|e| e.to_string())
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<Row>, String> {
        let cf = self.cf()?;
        let mut out = Vec::new();
        for item in self
            .db
            .iterator_cf(&cf, IteratorMode::From(prefix, Direction::Forward))
        {
            let (key, value) = item.map_err(|e| e.to_string())?;
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key.to_vec(), value.to_vec()));
        }
        Ok(out)
    }

    fn flush(&self) -> Result<(), String> {
        let cf = self.cf()?;
        self.db.flush_cf(&cf).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(p: i64, s: i64) -> Checkpoint {
        Checkpoint {
            promises: p,
            schedules: s,
        }
    }

    #[test]
    fn a_batch_and_its_checkpoint_are_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksLocal::open(dir.path(), &RocksCfg::default()).unwrap();
        let p = store.open(3).unwrap();
        assert_eq!(p.checkpoint().unwrap(), None);

        p.apply(
            vec![
                (b"oa".to_vec(), Some(b"1".to_vec())),
                (b"ob".to_vec(), Some(b"2".to_vec())),
                (b"sx".to_vec(), Some(b"3".to_vec())),
            ],
            cp(5, 1),
        )
        .unwrap();
        assert_eq!(p.checkpoint().unwrap(), Some(cp(5, 1)));
        assert_eq!(p.get(b"ob").unwrap(), Some(b"2".to_vec()));
        assert_eq!(
            p.scan(b"o").unwrap(),
            vec![
                (b"oa".to_vec(), b"1".to_vec()),
                (b"ob".to_vec(), b"2".to_vec())
            ]
        );

        p.apply(vec![(b"oa".to_vec(), None)], cp(7, 1)).unwrap();
        assert_eq!(p.scan(b"o").unwrap().len(), 1);
        // The checkpoint lives outside every data prefix.
        assert!(p.scan(b"m").unwrap().len() == 1 && p.scan(b"s").unwrap().len() == 1);
    }

    #[test]
    fn partitions_are_isolated_and_survive_a_clean_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = RocksLocal::open(dir.path(), &RocksCfg::default()).unwrap();
            let a = store.open(0).unwrap();
            let b = store.open(1).unwrap();
            a.apply(vec![(b"ok".to_vec(), Some(b"a".to_vec()))], cp(1, 0))
                .unwrap();
            b.apply(vec![(b"ok".to_vec(), Some(b"b".to_vec()))], cp(9, 0))
                .unwrap();
            a.flush().unwrap();
            b.flush().unwrap();
        }
        let store = RocksLocal::open(dir.path(), &RocksCfg::default()).unwrap();
        assert_eq!(store.partitions().unwrap(), vec![0, 1]);
        assert_eq!(
            store.open(0).unwrap().get(b"ok").unwrap(),
            Some(b"a".to_vec())
        );
        assert_eq!(store.open(1).unwrap().checkpoint().unwrap(), Some(cp(9, 0)));
    }

    #[test]
    fn a_dropped_partition_comes_back_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksLocal::open(dir.path(), &RocksCfg::default()).unwrap();
        let p = store.open(2).unwrap();
        p.apply(vec![(b"ok".to_vec(), Some(b"v".to_vec()))], cp(1, 0))
            .unwrap();
        store.drop_partition(2).unwrap();
        assert!(p.get(b"ok").is_err(), "a stale handle is refused");
        assert_eq!(store.partitions().unwrap(), Vec::<u32>::new());
        let p = store.open(2).unwrap();
        assert_eq!(p.checkpoint().unwrap(), None);
        assert_eq!(p.get(b"ok").unwrap(), None);
        // Dropping twice is fine.
        store.drop_partition(7).unwrap();
    }
}
