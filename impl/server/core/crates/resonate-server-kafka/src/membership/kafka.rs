//! Membership through a Kafka consumer group.
//!
//! Every node subscribes one consumer, in one group, to the promise topic, with
//! the cooperative-sticky assignor: the group spreads the partitions over the
//! live nodes, moves as few as it can when one joins or leaves, and moves them
//! incrementally — a revoke in one round, the assign in the next — so the
//! partitions that are not moving keep being served throughout.
//!
//! The consumer never consumes. It is there for its membership: every assigned
//! partition is paused at once, and the poll loop runs only to heartbeat and to
//! deliver rebalance callbacks. The callbacks run on the poll thread, which is
//! what makes a revoke clean: the callback hands the revocation to the node and
//! **blocks until the node has stopped the partitions**, and the next round —
//! the one that gives them to someone else — waits for it. A revocation of a
//! lost assignment (the session expired) is delivered the same way, marked
//! `lost`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::{ClientContext, TopicPartitionList};
use tokio::sync::{mpsc, oneshot};

use resonate_core::Unavailable;

use super::{Event, Membership};
use crate::log::kafka::KafkaCfg;
use crate::log::Topic;

/// The group's settings.
#[derive(Debug, Clone)]
pub struct GroupCfg {
    /// `group.id`. One group per deployment.
    pub group_id: String,
    /// How long the group waits on a silent node before its partitions move.
    pub session_timeout: Duration,
    /// `group.instance.id`, for static membership: a restart within the
    /// session timeout keeps its partitions without a rebalance.
    pub instance_id: Option<String>,
}

impl Default for GroupCfg {
    fn default() -> Self {
        Self {
            group_id: "resonate".into(),
            session_timeout: Duration::from_secs(10),
            instance_id: None,
        }
    }
}

struct GroupContext {
    events: mpsc::UnboundedSender<Event>,
}

fn partitions_of(tpl: &TopicPartitionList) -> Vec<u32> {
    let mut out: Vec<u32> = tpl
        .elements()
        .iter()
        .filter(|e| e.partition() >= 0)
        .map(|e| e.partition() as u32)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

impl ClientContext for GroupContext {}

impl ConsumerContext for GroupContext {
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(tpl) = rebalance {
            let partitions = partitions_of(tpl);
            if partitions.is_empty() {
                return;
            }
            let lost = consumer.assignment_lost();
            let (done, wait) = oneshot::channel();
            if self
                .events
                .send(Event::Revoked {
                    partitions,
                    lost,
                    done,
                })
                .is_ok()
            {
                // Block the poll thread — and with it the rebalance — until the
                // node has stopped serving them.
                let _ = wait.blocking_recv();
            }
        }
    }

    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = rebalance {
            let partitions = partitions_of(tpl);
            if partitions.is_empty() {
                return;
            }
            // Membership only: nothing is ever read from here.
            if let Err(e) = consumer.pause(tpl) {
                tracing::warn!(error = %e, "Assigned partitions not paused");
            }
            let _ = self.events.send(Event::Assigned(partitions));
        }
    }
}

pub struct KafkaMembership {
    log: KafkaCfg,
    group: GroupCfg,
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl KafkaMembership {
    pub fn new(log: KafkaCfg, group: GroupCfg) -> Arc<Self> {
        Arc::new(Self {
            log,
            group,
            stop: Arc::new(AtomicBool::new(false)),
            thread: Mutex::new(None),
        })
    }
}

#[async_trait]
impl Membership for KafkaMembership {
    async fn start(&self, events: mpsc::UnboundedSender<Event>) -> Result<(), Unavailable> {
        let mut config = rdkafka::ClientConfig::new();
        config
            .set("bootstrap.servers", &self.log.brokers)
            .set("client.id", format!("resonate-{}-group", self.log.node_id))
            .set("group.id", &self.group.group_id)
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set(
                "session.timeout.ms",
                self.group.session_timeout.as_millis().to_string(),
            );
        if let Some(id) = &self.group.instance_id {
            config.set("group.instance.id", id);
        }
        for (k, v) in &self.log.properties {
            config.set(k, v);
        }
        let consumer: BaseConsumer<GroupContext> = config
            .create_with_context(GroupContext { events })
            .map_err(|e| Unavailable::new(format!("cannot create the group consumer: {e}")))?;
        consumer
            .subscribe(&[&self.log.topic(Topic::Promises)])
            .map_err(|e| Unavailable::new(format!("cannot join the group: {e}")))?;

        let stop = Arc::clone(&self.stop);
        let handle = std::thread::Builder::new()
            .name("resonate-group".into())
            .spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // Heartbeats and callbacks; no records, everything is paused.
                    let _ = consumer.poll(Duration::from_millis(200));
                }
                // Leave: hand everything back through the revoke callback
                // before the consumer goes.
                consumer.unsubscribe();
                let deadline = Instant::now() + Duration::from_secs(30);
                while Instant::now() < deadline {
                    let _ = consumer.poll(Duration::from_millis(100));
                    if consumer
                        .assignment()
                        .map(|a| a.count() == 0)
                        .unwrap_or(true)
                    {
                        break;
                    }
                }
            })
            .map_err(|e| Unavailable::new(e.to_string()))?;
        *self.thread.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
        Ok(())
    }

    async fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        let handle = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(handle) = handle {
            let _ = tokio::task::spawn_blocking(move || handle.join()).await;
        }
    }
}
