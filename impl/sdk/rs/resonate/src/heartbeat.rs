use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::send::{Sender, TaskRef};

/// Heartbeat trait for keeping task leases alive.
///
/// Implementations track a set of active tasks and periodically
/// send heartbeat requests to the server for all of them.
pub trait Heartbeat: Send + Sync {
    /// Add a task to the heartbeat set. Starts the heartbeat loop
    /// if this is the first tracked task.
    fn start(&self, task_id: &str, task_version: i64);

    /// Remove a task from the heartbeat set. Stops the heartbeat loop
    /// if no tasks remain.
    fn stop(&self, task_id: &str);

    /// Shut down the heartbeat entirely, clearing all tracked tasks
    /// and aborting the loop. Called on graceful shutdown.
    fn shutdown(&self);
}

/// No-op heartbeat for local mode.
pub struct NoopHeartbeat;

impl Heartbeat for NoopHeartbeat {
    fn start(&self, _task_id: &str, _task_version: i64) {}
    fn stop(&self, _task_id: &str) {}
    fn shutdown(&self) {}
}

/// Async heartbeat that sends task.heartbeat requests at regular intervals
/// for all currently tracked tasks.
///
/// Uses `Sender` (not raw `Transport`) so the request goes through the
/// standard protocol envelope with corrId, version header, etc.
pub struct AsyncHeartbeat {
    pid: String,
    interval_ms: u64,
    sender: Sender,
    active_tasks: Arc<Mutex<HashMap<String, i64>>>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl AsyncHeartbeat {
    pub fn new(pid: String, interval_ms: u64, sender: Sender) -> Self {
        Self {
            pid,
            interval_ms,
            sender,
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            handle: Mutex::new(None),
        }
    }

    /// Returns the number of currently tracked tasks.
    pub fn task_count(&self) -> usize {
        self.active_tasks.lock().len()
    }

    /// Returns whether the heartbeat loop is currently running.
    pub fn is_running(&self) -> bool {
        self.handle.lock().is_some()
    }

    /// Returns a snapshot of the currently tracked tasks (id → version).
    pub fn tracked_tasks(&self) -> HashMap<String, i64> {
        self.active_tasks.lock().clone()
    }

    /// Spawn the heartbeat loop if not already running.
    fn ensure_loop_running(&self) {
        let mut handle = self.handle.lock();
        if handle.is_some() {
            return;
        }

        let pid = self.pid.clone();
        let interval = std::time::Duration::from_millis(self.interval_ms);
        let sender = self.sender.clone();
        let active_tasks = self.active_tasks.clone();

        *handle = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;

                // Snapshot active tasks under lock
                let tasks: Vec<TaskRef> = {
                    let map = active_tasks.lock();
                    map.iter()
                        .map(|(id, version)| TaskRef {
                            id: id.clone(),
                            version: *version,
                        })
                        .collect()
                };

                if tasks.is_empty() {
                    continue;
                }

                if let Err(e) = sender.task_heartbeat(&pid, tasks).await {
                    tracing::warn!(error = %e, "heartbeat failed");
                }
            }
        }));
    }
}

impl Heartbeat for AsyncHeartbeat {
    fn start(&self, task_id: &str, task_version: i64) {
        {
            let mut map = self.active_tasks.lock();
            map.insert(task_id.to_string(), task_version);
        }
        self.ensure_loop_running();
    }

    fn stop(&self, task_id: &str) {
        let is_empty = {
            let mut map = self.active_tasks.lock();
            map.remove(task_id);
            map.is_empty()
        };

        if is_empty {
            let mut handle = self.handle.lock();
            if let Some(h) = handle.take() {
                h.abort();
            }
        }
    }

    fn shutdown(&self) {
        self.active_tasks.lock().clear();
        let mut handle = self.handle.lock();
        if let Some(h) = handle.take() {
            h.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestHarness;

    fn test_heartbeat(sender: Sender) -> AsyncHeartbeat {
        AsyncHeartbeat::new("test-pid".to_string(), 50, sender)
    }

    // ── Task tracking ──────────────────────────────────────────────

    #[tokio::test]
    async fn start_adds_task_to_tracked_set() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        assert_eq!(hb.task_count(), 1);

        let tasks = hb.tracked_tasks();
        assert_eq!(tasks.get("task-1"), Some(&1));

        hb.shutdown();
    }

    #[tokio::test]
    async fn start_multiple_tasks_all_tracked() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 5);
        hb.start("task-3", 10);
        assert_eq!(hb.task_count(), 3);

        let tasks = hb.tracked_tasks();
        assert_eq!(tasks.get("task-1"), Some(&1));
        assert_eq!(tasks.get("task-2"), Some(&5));
        assert_eq!(tasks.get("task-3"), Some(&10));

        hb.shutdown();
    }

    #[tokio::test]
    async fn stop_removes_task_from_tracked_set() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 2);
        assert_eq!(hb.task_count(), 2);

        hb.stop("task-1");
        assert_eq!(hb.task_count(), 1);
        assert!(!hb.tracked_tasks().contains_key("task-1"));
        assert_eq!(hb.tracked_tasks().get("task-2"), Some(&2));

        hb.shutdown();
    }

    #[tokio::test]
    async fn stop_nonexistent_task_is_harmless() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.stop("nonexistent");
        assert_eq!(hb.task_count(), 1);

        hb.shutdown();
    }

    #[tokio::test]
    async fn start_same_task_twice_updates_version() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-1", 5);
        assert_eq!(hb.task_count(), 1);
        assert_eq!(hb.tracked_tasks().get("task-1"), Some(&5));

        hb.shutdown();
    }

    // ── Loop lifecycle ─────────────────────────────────────────────

    #[tokio::test]
    async fn loop_not_running_initially() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        assert!(!hb.is_running());
    }

    #[tokio::test]
    async fn loop_starts_on_first_task() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        assert!(hb.is_running());

        hb.shutdown();
    }

    #[tokio::test]
    async fn loop_stays_running_while_tasks_remain() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 2);
        assert!(hb.is_running());

        hb.stop("task-1");
        assert!(
            hb.is_running(),
            "loop should stay running with task-2 still active"
        );

        hb.shutdown();
    }

    #[tokio::test]
    async fn loop_stops_when_last_task_removed() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        assert!(hb.is_running());

        hb.stop("task-1");
        assert!(!hb.is_running());
    }

    #[tokio::test]
    async fn loop_stops_when_last_of_multiple_tasks_removed() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 2);
        hb.stop("task-1");
        assert!(hb.is_running());

        hb.stop("task-2");
        assert!(!hb.is_running());
    }

    #[tokio::test]
    async fn loop_restarts_after_stop_and_new_start() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        assert!(hb.is_running());

        hb.stop("task-1");
        assert!(!hb.is_running());

        hb.start("task-2", 2);
        assert!(hb.is_running());

        hb.shutdown();
    }

    // ── Shutdown ───────────────────────────────────────────────────

    #[tokio::test]
    async fn shutdown_clears_all_tasks_and_stops_loop() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 2);
        hb.start("task-3", 3);
        assert_eq!(hb.task_count(), 3);
        assert!(hb.is_running());

        hb.shutdown();
        assert_eq!(hb.task_count(), 0);
        assert!(!hb.is_running());
    }

    #[tokio::test]
    async fn shutdown_when_already_idle_is_harmless() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.shutdown();
        assert_eq!(hb.task_count(), 0);
        assert!(!hb.is_running());
    }

    // ── Heartbeat sends ────────────────────────────────────────────

    #[tokio::test]
    async fn heartbeat_sends_request_with_tracked_tasks() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 5);

        // Wait for at least one heartbeat tick
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        hb.shutdown();

        let requests = harness.sent_requests_json().await;
        let heartbeats: Vec<_> = requests
            .iter()
            .filter(|r| r["kind"] == "task.heartbeat")
            .collect();

        assert!(
            !heartbeats.is_empty(),
            "should have sent at least one heartbeat"
        );

        // Check the last heartbeat contains both tasks
        let last_hb = heartbeats.last().unwrap();
        assert_eq!(last_hb["pid"], "test-pid");

        let tasks = last_hb["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 2);

        let ids: Vec<&str> = tasks.iter().map(|t| t["id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"task-1"));
        assert!(ids.contains(&"task-2"));
    }

    #[tokio::test]
    async fn heartbeat_reflects_task_removal() {
        let harness = TestHarness::new();
        let hb = test_heartbeat(harness.build_sender());

        hb.start("task-1", 1);
        hb.start("task-2", 2);

        // Wait for a heartbeat with both tasks
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        // Remove task-1
        hb.stop("task-1");

        // Wait for another heartbeat with only task-2
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        hb.shutdown();

        let requests = harness.sent_requests_json().await;
        let heartbeats: Vec<_> = requests
            .iter()
            .filter(|r| r["kind"] == "task.heartbeat")
            .collect();

        // The last heartbeat should only contain task-2
        let last_hb = heartbeats.last().unwrap();
        let tasks = last_hb["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["id"], "task-2");
    }

    // ── NoopHeartbeat ──────────────────────────────────────────────

    #[test]
    fn noop_heartbeat_start_stop_shutdown_are_harmless() {
        let hb = NoopHeartbeat;
        hb.start("task-1", 1);
        hb.start("task-2", 2);
        hb.stop("task-1");
        hb.stop("nonexistent");
        hb.shutdown();
    }
}
