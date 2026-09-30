//! The local store in memory: nothing survives the process, which is exactly
//! right for tests and for a node that restores from the log on every start.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use super::{LocalStore, Op, PartitionStore, Row};
use crate::log::Checkpoint;

#[derive(Default)]
pub struct MemLocal {
    partitions: Mutex<HashMap<u32, Arc<MemPartition>>>,
}

impl MemLocal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl LocalStore for MemLocal {
    fn open(&self, partition: u32) -> Result<Arc<dyn PartitionStore>, String> {
        let mut partitions = self.partitions.lock().unwrap_or_else(|e| e.into_inner());
        let p = partitions
            .entry(partition)
            .or_insert_with(|| Arc::new(MemPartition::default()));
        Ok(Arc::clone(p) as Arc<dyn PartitionStore>)
    }

    fn drop_partition(&self, partition: u32) -> Result<(), String> {
        self.partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&partition);
        Ok(())
    }

    fn partitions(&self) -> Result<Vec<u32>, String> {
        let mut out: Vec<u32> = self
            .partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .copied()
            .collect();
        out.sort_unstable();
        Ok(out)
    }
}

#[derive(Default)]
struct State {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    checkpoint: Option<Checkpoint>,
}

#[derive(Default)]
pub struct MemPartition {
    state: Mutex<State>,
}

impl MemPartition {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl PartitionStore for MemPartition {
    fn checkpoint(&self) -> Result<Option<Checkpoint>, String> {
        Ok(self.lock().checkpoint)
    }

    fn apply(&self, ops: Vec<Op>, checkpoint: Checkpoint) -> Result<(), String> {
        let mut state = self.lock();
        for (key, value) in ops {
            match value {
                Some(v) => {
                    state.data.insert(key, v);
                }
                None => {
                    state.data.remove(&key);
                }
            }
        }
        state.checkpoint = Some(checkpoint);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.lock().data.get(key).cloned())
    }

    fn scan(&self, prefix: &[u8]) -> Result<Vec<Row>, String> {
        Ok(self
            .lock()
            .data
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}
