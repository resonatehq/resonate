//! The log over Kafka.
//!
//! # Topics
//!
//! Two, both compacted, named from one prefix:
//!
//! | topic | partitions | key | value |
//! |---|---|---|---|
//! | `<prefix>.promises` | N | promise id | the promise and its task ([`crate::record`]) |
//! | `<prefix>.schedules` | N | schedule id | the schedule |
//!
//! Partition *p* of the promise topic and partition *p* of the schedule topic
//! are one partition of this backend: one owner, one writer. Records are
//! produced to an explicit partition ([`crate::keys::partition_of`] over the
//! origin, or the schedule id), never to the producer's own choice.
//!
//! # Fencing
//!
//! By claim, as [`super::epoch`] describes: `fence` reads each log's high
//! watermark and produces a claim that must land exactly there. Records go
//! out through an idempotent producer (`acks=all`), so they land in order and
//! once, and each delivery report's offset is checked against where this
//! writer last left the log. No transactions: one costs about ten times a
//! produce (`examples/txn_cost.rs`).
//!
//! Connecting checks both topics are compacted with a
//! `min.compaction.lag.ms` long enough for an owner to write over a fenced
//! writer's late record before compaction can keep it — and refuses to
//! start otherwise.
//!
//! # Reading
//!
//! One topic after the other, each to end-of-partition, through the epoch
//! filter. The checkpoint after a topic is its high watermark at that moment.
//!
//! Who owns a partition is the consumer group's to say, not the log's: see
//! [`crate::directory::kafka`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};

use super::{epoch, Checkpoint, Consumed, Log, LogError, Reader, Record, Topic, Writer};

/// Everything the Kafka log needs.
#[derive(Debug, Clone)]
pub struct KafkaCfg {
    /// `bootstrap.servers`.
    pub brokers: String,
    /// Topic names are `<topic_prefix>.promises` and `.schedules`.
    pub topic_prefix: String,
    /// Partitions of the promise and schedule topics. Fixed: an origin's
    /// partition is a function of it.
    pub partitions: u32,
    /// Replication factor for topics this log creates.
    pub replication_factor: i32,
    /// Create the topics if they do not exist.
    pub create_topics: bool,
    /// How long to wait on the broker for anything else.
    pub request_timeout: Duration,
    /// Extra librdkafka properties for every client (security, tuning).
    pub properties: BTreeMap<String, String>,
    /// This node's id, for client ids.
    pub node_id: String,
    /// How long a record may take to land before the producer gives up on
    /// it — so how late a fenced writer's records can still arrive.
    pub delivery_timeout: Duration,
    /// `min.compaction.lag.ms` for topics this log creates. An existing topic
    /// must have at least [`KafkaCfg::compaction_lag_floor`].
    pub min_compaction_lag: Duration,
}

impl Default for KafkaCfg {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".into(),
            topic_prefix: "resonate".into(),
            partitions: 64,
            replication_factor: 3,
            create_topics: true,
            request_timeout: Duration::from_secs(30),
            properties: BTreeMap::new(),
            node_id: "node-0".into(),
            delivery_timeout: Duration::from_secs(10),
            min_compaction_lag: Duration::from_secs(3_600),
        }
    }
}

impl KafkaCfg {
    pub fn topic(&self, topic: Topic) -> String {
        match topic {
            Topic::Promises => format!("{}.promises", self.topic_prefix),
            Topic::Schedules => format!("{}.schedules", self.topic_prefix),
        }
    }

    /// The shortest `min.compaction.lag.ms` that keeps a fenced writer's
    /// record from being compacted before its owner writes over it: as late as
    /// such a record can land, plus a minute for the owner's idle check and a
    /// takeover.
    pub fn compaction_lag_floor(&self) -> Duration {
        self.delivery_timeout + Duration::from_secs(60)
    }

    fn client(&self) -> ClientConfig {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &self.brokers);
        c.set("client.id", format!("resonate-{}", self.node_id));
        for (k, v) in &self.properties {
            c.set(k, v);
        }
        c
    }

    /// A consumer that reads committed records by assignment, never joining a
    /// group and never committing offsets.
    fn reader_config(&self) -> ClientConfig {
        let mut c = self.client();
        c.set(
            "group.id",
            format!("{}-reader-{}", self.topic_prefix, self.node_id),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
        c
    }
}

fn unavailable(e: impl std::fmt::Display) -> LogError {
    LogError::Unavailable(e.to_string())
}

pub struct KafkaLog {
    cfg: KafkaCfg,
    probe: Arc<BaseConsumer>,
}

impl KafkaLog {
    /// Connect: create the topics, or check the ones that exist.
    pub async fn connect(cfg: KafkaCfg) -> Result<Arc<Self>, LogError> {
        if cfg.create_topics {
            create_topics(&cfg).await?;
        }
        let probe: BaseConsumer = cfg.reader_config().create().map_err(unavailable)?;
        let log = Arc::new(Self {
            probe: Arc::new(probe),
            cfg,
        });
        log.check_partition_counts().await?;
        log.check_topic_configs().await?;
        Ok(log)
    }

    /// Refuse a topic that is not compacted, or compacted sooner than an
    /// owner can write over a fenced writer's late record
    /// ([`KafkaCfg::compaction_lag_floor`]).
    async fn check_topic_configs(&self) -> Result<(), LogError> {
        let admin: AdminClient<DefaultClientContext> =
            self.cfg.client().create().map_err(unavailable)?;
        let names = [
            self.cfg.topic(Topic::Promises),
            self.cfg.topic(Topic::Schedules),
        ];
        let specs: Vec<ResourceSpecifier<'_>> =
            names.iter().map(|n| ResourceSpecifier::Topic(n)).collect();
        let results = admin
            .describe_configs(
                specs.iter(),
                &AdminOptions::new().request_timeout(Some(self.cfg.request_timeout)),
            )
            .await
            .map_err(unavailable)?;
        let floor = self.cfg.compaction_lag_floor().as_millis() as i64;
        for (name, result) in names.iter().zip(results) {
            let config = result.map_err(|e| {
                LogError::Unavailable(format!("cannot read the config of topic {name}: {e}"))
            })?;
            let value = |key: &str| config.get(key).and_then(|e| e.value.clone());
            let policy = value("cleanup.policy").unwrap_or_default();
            if !policy.split(',').any(|p| p.trim() == "compact") {
                return Err(LogError::Unavailable(format!(
                    "topic {name} has cleanup.policy={policy}; it must be compacted"
                )));
            }
            let lag: i64 = value("min.compaction.lag.ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            if lag < floor {
                return Err(LogError::Unavailable(format!(
                    "topic {name} has min.compaction.lag.ms={lag}; it must be at least {floor} \
                     so a fenced writer's late record is written over before compaction"
                )));
            }
        }
        Ok(())
    }

    /// Refuse a topic whose partition count differs from the configured one:
    /// routing would disagree with where the records are.
    async fn check_partition_counts(&self) -> Result<(), LogError> {
        let cfg = self.cfg.clone();
        let consumer: BaseConsumer = cfg.reader_config().create().map_err(unavailable)?;
        tokio::task::spawn_blocking(move || {
            for (topic, want) in [
                (cfg.topic(Topic::Promises), cfg.partitions as usize),
                (cfg.topic(Topic::Schedules), cfg.partitions as usize),
            ] {
                let md = consumer
                    .fetch_metadata(Some(&topic), cfg.request_timeout)
                    .map_err(unavailable)?;
                let got = md
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic)
                    .map(|t| t.partitions().len())
                    .unwrap_or(0);
                if got != want {
                    return Err(LogError::Unavailable(format!(
                        "topic {topic} has {got} partitions; this deployment needs {want}"
                    )));
                }
            }
            Ok(())
        })
        .await
        .map_err(unavailable)?
    }

    pub fn cfg(&self) -> &KafkaCfg {
        &self.cfg
    }
}

async fn create_topics(cfg: &KafkaCfg) -> Result<(), LogError> {
    let admin: AdminClient<DefaultClientContext> = cfg.client().create().map_err(unavailable)?;
    let promises = cfg.topic(Topic::Promises);
    let schedules = cfg.topic(Topic::Schedules);
    let isr = if cfg.replication_factor >= 3 {
        "2"
    } else {
        "1"
    };
    let lag = cfg.min_compaction_lag.as_millis().to_string();
    fn compacted<'a>(
        name: &'a str,
        partitions: i32,
        rf: i32,
        isr: &'a str,
        lag: Option<&'a str>,
    ) -> NewTopic<'a> {
        let t = NewTopic::new(name, partitions, TopicReplication::Fixed(rf))
            .set("cleanup.policy", "compact")
            .set("min.insync.replicas", isr);
        match lag {
            Some(lag) => t.set("min.compaction.lag.ms", lag),
            None => t,
        }
    }
    let n = cfg.partitions as i32;
    let rf = cfg.replication_factor;
    let lag = Some(lag.as_str());
    let topics = [
        compacted(&promises, n, rf, isr, lag),
        compacted(&schedules, n, rf, isr, lag),
    ];
    let results = admin
        .create_topics(
            topics.iter(),
            &AdminOptions::new().operation_timeout(Some(cfg.request_timeout)),
        )
        .await
        .map_err(unavailable)?;
    for result in results {
        match result {
            Ok(name) => tracing::info!(topic = %name, "Topic created"),
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => {
                return Err(LogError::Unavailable(format!(
                    "cannot create topic {name}: {code}"
                )))
            }
        }
    }
    Ok(())
}

#[async_trait]
impl Log for KafkaLog {
    fn partitions(&self) -> u32 {
        self.cfg.partitions
    }

    async fn fence(&self, partition: u32) -> Result<Arc<dyn Writer>, LogError> {
        Ok(Arc::new(self.claim(partition).await?))
    }

    async fn start(&self, partition: u32) -> Result<Checkpoint, LogError> {
        let cfg = self.cfg.clone();
        let consumer: BaseConsumer = cfg.reader_config().create().map_err(unavailable)?;
        tokio::task::spawn_blocking(move || {
            let mut out = Checkpoint::default();
            for topic in [Topic::Promises, Topic::Schedules] {
                let (low, _) = consumer
                    .fetch_watermarks(&cfg.topic(topic), partition as i32, cfg.request_timeout)
                    .map_err(unavailable)?;
                out.set(topic, low);
            }
            Ok(out)
        })
        .await
        .map_err(unavailable)?
    }

    async fn reader(&self, partition: u32, from: Checkpoint) -> Result<Box<dyn Reader>, LogError> {
        let raw = Box::new(KafkaReader {
            cfg: self.cfg.clone(),
            partition,
            position: from,
            topics: vec![Topic::Promises, Topic::Schedules],
            consumer: None,
        });
        Ok(Box::new(epoch::Filtered::new(raw, &from)))
    }

    async fn ready(&self) -> bool {
        let topic = self.cfg.topic(Topic::Promises);
        let probe = Arc::clone(&self.probe);
        tokio::task::spawn_blocking(move || {
            probe
                .fetch_metadata(Some(&topic), Duration::from_secs(5))
                .is_ok()
        })
        .await
        .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// How many times a claim is tried before the takeover gives up: each retry
/// means something landed between reading the end and claiming it.
const CLAIM_ATTEMPTS: usize = 8;

impl KafkaLog {
    /// The high watermarks of `partition`'s two logs.
    async fn ends(&self, partition: u32) -> Result<Checkpoint, LogError> {
        high_watermarks(&self.cfg, &self.probe, partition).await
    }

    /// Become the writer of `partition`: claim each log where it ends, and
    /// check the claim landed there ([`super::epoch`]).
    async fn claim(&self, partition: u32) -> Result<KafkaWriter, LogError> {
        let mut c = self.cfg.client();
        c.set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("linger.ms", "0")
            .set(
                "message.timeout.ms",
                self.cfg.delivery_timeout.as_millis().to_string(),
            );
        let producer: FutureProducer = c.create().map_err(unavailable)?;
        let mut epochs = [-1i64; 2];
        for topic in [Topic::Promises, Topic::Schedules] {
            let name = self.cfg.topic(topic);
            let mut claimed = None;
            for _ in 0..CLAIM_ATTEMPTS {
                let at = self.ends(partition).await?.get(topic);
                let key = epoch::claim_key(at);
                let headers = OwnedHeaders::new()
                    .insert(Header {
                        key: epoch::EPOCH_HEADER,
                        value: Some(&epoch::encode_epoch(at)),
                    })
                    .insert(Header {
                        key: epoch::CLAIM_HEADER,
                        value: Some(&[1u8][..]),
                    });
                let record = FutureRecord::<str, [u8]>::to(&name)
                    .key(key.as_str())
                    .payload(self.cfg.node_id.as_bytes())
                    .headers(headers)
                    .partition(partition as i32);
                let landed = producer
                    .send(record, self.cfg.delivery_timeout)
                    .await
                    .map_err(|(e, _)| LogError::Unavailable(format!("claim not written: {e}")))?;
                if landed.offset == at {
                    claimed = Some(at);
                    break;
                }
                tracing::debug!(
                    partition,
                    ?topic,
                    at,
                    landed = landed.offset,
                    "Claim landed late; claiming again"
                );
            }
            epochs[topic as usize] = claimed.ok_or_else(|| {
                LogError::Unavailable(format!(
                    "partition {partition}: no claim landed where it meant to in {CLAIM_ATTEMPTS} attempts"
                ))
            })?;
        }
        Ok(KafkaWriter {
            producer,
            cfg: self.cfg.clone(),
            probe: Arc::clone(&self.probe),
            partition,
            epochs,
        })
    }
}

async fn high_watermarks(
    cfg: &KafkaCfg,
    probe: &Arc<BaseConsumer>,
    partition: u32,
) -> Result<Checkpoint, LogError> {
    let cfg = cfg.clone();
    let probe = Arc::clone(probe);
    tokio::task::spawn_blocking(move || {
        let mut out = Checkpoint::default();
        for topic in [Topic::Promises, Topic::Schedules] {
            let (_, high) = probe
                .fetch_watermarks(&cfg.topic(topic), partition as i32, cfg.request_timeout)
                .map_err(unavailable)?;
            out.set(topic, high);
        }
        Ok(out)
    })
    .await
    .map_err(unavailable)?
}

/// The writer of a partition: its records carry its epochs, and every one must land exactly where the last one left the log.
struct KafkaWriter {
    producer: FutureProducer,
    cfg: KafkaCfg,
    probe: Arc<BaseConsumer>,
    partition: u32,
    epochs: [i64; 2],
}

#[async_trait]
impl Writer for KafkaWriter {
    async fn commit(&self, records: Vec<Record>, base: Checkpoint) -> Result<Checkpoint, LogError> {
        if records.is_empty() {
            return Ok(base);
        }
        let partition = self.partition as i32;
        let promises = self.cfg.topic(Topic::Promises);
        let schedules = self.cfg.topic(Topic::Schedules);
        let mut deliveries = Vec::with_capacity(records.len());
        for (i, r) in records.iter().enumerate() {
            let topic = match r.topic {
                Topic::Promises => &promises,
                Topic::Schedules => &schedules,
            };
            let headers = OwnedHeaders::new().insert(Header {
                key: epoch::EPOCH_HEADER,
                value: Some(&epoch::encode_epoch(self.epochs[r.topic as usize])),
            });
            let mut record = FutureRecord::<str, [u8]>::to(topic)
                .key(r.key.as_str())
                .headers(headers)
                .partition(partition);
            if let Some(v) = &r.value {
                record = record.payload(v.as_slice());
            }
            match self.producer.send_result(record) {
                Ok(d) => deliveries.push((r.topic, d)),
                // Nothing is in flight yet: nothing landed.
                Err((e, _)) if i == 0 => return Err(unavailable(e)),
                // The records before it are on their way.
                Err((e, _)) => {
                    return Err(LogError::Uncertain(format!(
                        "record {i} of {} not sent: {e}",
                        records.len()
                    )))
                }
            }
        }
        // In order, per log: each must land just past the one before.
        let mut after = base;
        let mut failure = None;
        for (topic, delivery) in deliveries {
            match delivery.await {
                Ok(Ok(d)) => {
                    let want = after.get(topic);
                    if d.offset != want && failure.is_none() {
                        failure = Some(LogError::Uncertain(format!(
                            "partition {} {topic:?}: a record landed at {}, not {want}; \
                             someone else wrote to the log",
                            self.partition, d.offset
                        )));
                    }
                    after.set(topic, d.offset + 1);
                }
                Ok(Err((e, _))) => {
                    failure.get_or_insert(LogError::Uncertain(format!("delivery failed: {e}")));
                }
                Err(_) => {
                    failure.get_or_insert(LogError::Uncertain(
                        "the producer dropped a delivery report".into(),
                    ));
                }
            }
        }
        if let Some(e) = failure {
            return Err(e);
        }
        after.epochs = self.epochs;
        Ok(after)
    }

    async fn check(&self, at: Checkpoint) -> Result<(), LogError> {
        let ends = high_watermarks(&self.cfg, &self.probe, self.partition).await?;
        if ends.promises != at.promises || ends.schedules != at.schedules {
            return Err(LogError::Uncertain(format!(
                "partition {} ends at {:?}, not where this writer left it ({:?})",
                self.partition,
                (ends.promises, ends.schedules),
                (at.promises, at.schedules)
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

struct KafkaReader {
    cfg: KafkaCfg,
    partition: u32,
    position: Checkpoint,
    /// Topics still to read, in order.
    topics: Vec<Topic>,
    consumer: Option<BaseConsumer>,
}

const BATCH: usize = 1_000;

/// A record's epoch and whether it is a claim, from its headers.
fn epoch_of(m: &rdkafka::message::BorrowedMessage<'_>) -> (Option<i64>, bool) {
    let Some(headers) = m.headers() else {
        return (None, false);
    };
    let mut epoch = None;
    let mut claim = false;
    for h in headers.iter() {
        match h.key {
            epoch::EPOCH_HEADER => epoch = h.value.and_then(epoch::decode_epoch),
            epoch::CLAIM_HEADER => claim = true,
            _ => {}
        }
    }
    (epoch, claim)
}

#[async_trait]
impl Reader for KafkaReader {
    async fn next(&mut self) -> Result<Option<(Vec<Consumed>, Checkpoint)>, LogError> {
        let Some(&topic) = self.topics.first() else {
            return Ok(None);
        };
        let consumer = match self.consumer.take() {
            Some(c) => c,
            None => {
                let c: BaseConsumer = self.cfg.reader_config().create().map_err(unavailable)?;
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(
                    &self.cfg.topic(topic),
                    self.partition as i32,
                    Offset::Offset(self.position.get(topic)),
                )
                .map_err(unavailable)?;
                c.assign(&tpl).map_err(unavailable)?;
                c
            }
        };
        let cfg = self.cfg.clone();
        let partition = self.partition;
        let mut position = self.position;
        let (consumer, batch, at_end) = tokio::task::spawn_blocking(move || {
            let name = cfg.topic(topic);
            let mut batch = Vec::new();
            let mut idle = Instant::now();
            loop {
                match consumer.poll(Duration::from_millis(500)) {
                    Some(Ok(m)) => {
                        idle = Instant::now();
                        let Some(key) = m.key().and_then(|k| std::str::from_utf8(k).ok()) else {
                            continue;
                        };
                        let (epoch, claim) = epoch_of(&m);
                        batch.push(Consumed {
                            topic,
                            key: key.to_string(),
                            value: m.payload().map(|v| v.to_vec()),
                            offset: m.offset(),
                            epoch,
                            claim,
                        });
                        position.set(topic, m.offset() + 1);
                        if batch.len() >= BATCH {
                            return Ok((consumer, batch, position, false));
                        }
                    }
                    Some(Err(KafkaError::PartitionEOF(_))) => {
                        // At the end: resume at the high watermark.
                        let (_, high) = consumer
                            .fetch_watermarks(&name, partition as i32, cfg.request_timeout)
                            .map_err(unavailable)?;
                        position.set(topic, high.max(position.get(topic)));
                        return Ok((consumer, batch, position, true));
                    }
                    Some(Err(e)) => return Err(unavailable(e)),
                    None => {
                        if idle.elapsed() > cfg.request_timeout {
                            return Err(LogError::Unavailable(format!(
                                "no progress reading {name}/{partition}"
                            )));
                        }
                    }
                }
            }
        })
        .await
        .map_err(unavailable)?
        .map(|(c, b, p, end)| {
            position = p;
            (c, b, end)
        })?;
        self.position = position;
        if at_end {
            self.topics.remove(0);
        } else {
            self.consumer = Some(consumer);
        }
        Ok(Some((batch, self.position)))
    }
}
