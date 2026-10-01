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
//! are one partition of this backend: one owner, one writer, one transaction.
//! Records are produced to an explicit partition ([`crate::keys::partition_of`]
//! over the origin, or the schedule id), never to the producer's own choice.
//!
//! # Fencing
//!
//! Partition *p* has one transactional id, `<txn_prefix>-p<p>`. Fencing is
//! `init_transactions` on a fresh producer with that id: the broker bumps the
//! id's epoch, which refuses every earlier producer with it, and completes or
//! aborts whatever transaction such a producer left open. So after `fence`
//! returns, the log's committed end is final until this writer adds to it.
//!
//! # Reading
//!
//! `read_committed`, one topic after the other, each to end-of-partition. The
//! checkpoint after a topic is its high watermark at that moment, which is
//! past any trailing transaction marker, so a resume never starts on one.
//!
//! Who owns a partition is the consumer group's to say, not the log's: see
//! [`crate::directory::kafka`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};

use super::{epoch, Checkpoint, Consumed, Log, LogError, Reader, Record, Topic, Writer};

/// Everything the Kafka log needs.
#[derive(Debug, Clone)]
pub struct KafkaCfg {
    /// `bootstrap.servers`.
    pub brokers: String,
    /// Topic names are `<topic_prefix>.promises` and `.schedules`.
    pub topic_prefix: String,
    /// Transactional ids are `<txn_prefix>-p<partition>`. One per deployment.
    pub txn_prefix: String,
    /// Partitions of the promise and schedule topics. Fixed: an origin's
    /// partition is a function of it.
    pub partitions: u32,
    /// Replication factor for topics this log creates.
    pub replication_factor: i32,
    /// Create the topics if they do not exist.
    pub create_topics: bool,
    /// How long a transaction may take before it is abandoned.
    pub transaction_timeout: Duration,
    /// How long to wait on the broker for anything else.
    pub request_timeout: Duration,
    /// Extra librdkafka properties for every client (security, tuning).
    pub properties: BTreeMap<String, String>,
    /// This node's id, for client ids.
    pub node_id: String,
    /// Write in transactions (`true`), or without them — claims, epochs and
    /// offset checks ([`super::epoch`]).
    pub transactional: bool,
    /// Without transactions: how long a record may take to land before the
    /// producer gives up on it — so how late a fenced writer's records can
    /// still arrive.
    pub delivery_timeout: Duration,
    /// Without transactions: the topics' `min.compaction.lag.ms`, which must
    /// outlast `delivery_timeout` plus the owner's idle check, so a fenced
    /// writer's record is never compacted before its key is written again.
    pub min_compaction_lag: Duration,
}

impl Default for KafkaCfg {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".into(),
            topic_prefix: "resonate".into(),
            txn_prefix: "resonate".into(),
            partitions: 64,
            replication_factor: 3,
            create_topics: true,
            transaction_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(30),
            properties: BTreeMap::new(),
            node_id: "node-0".into(),
            transactional: true,
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
            format!("{}-reader-{}", self.txn_prefix, self.node_id),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
        c
    }
}

fn unavailable(e: impl std::fmt::Display) -> LogError {
    LogError::Unavailable(e.to_string())
}

fn is_fenced_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::ProducerFenced
            | RDKafkaErrorCode::Fenced
            | RDKafkaErrorCode::InvalidProducerEpoch
            | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
    )
}

/// How a transactional error must be handled.
#[derive(Debug, PartialEq, Eq)]
enum Class {
    /// Abort the transaction; nothing landed.
    Abort,
    /// Try the same call again.
    Retry,
    /// Someone else owns the transactional id now.
    Fenced,
    /// The producer is unusable and nothing is known.
    Fatal,
}

fn classify(e: &KafkaError) -> Class {
    if let KafkaError::Transaction(t) = e {
        if is_fenced_code(t.code()) {
            return Class::Fenced;
        }
        if t.txn_requires_abort() {
            return Class::Abort;
        }
        if t.is_retriable() {
            return Class::Retry;
        }
        return Class::Fatal;
    }
    match e.rdkafka_error_code() {
        Some(code) if is_fenced_code(code) => Class::Fenced,
        // A produce that failed inside a transaction poisons the transaction:
        // abort it, and nothing it held lands.
        _ => Class::Abort,
    }
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
        Ok(log)
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
    let lag = (!cfg.transactional).then_some(lag.as_str());
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
        if !self.cfg.transactional {
            return Ok(Arc::new(self.claim(partition).await?));
        }
        let mut c = self.cfg.client();
        c.set(
            "transactional.id",
            format!("{}-p{partition}", self.cfg.txn_prefix),
        )
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("compression.type", "lz4")
        // A round already is the batch: waiting for more only adds latency
        // (about 1ms per commit, measured with examples/txn_cost.rs).
        .set("linger.ms", "0")
        .set(
            "transaction.timeout.ms",
            self.cfg.transaction_timeout.as_millis().to_string(),
        )
        .set(
            "message.timeout.ms",
            self.cfg.transaction_timeout.as_millis().to_string(),
        );
        let producer: FutureProducer = c.create().map_err(unavailable)?;
        let timeout = self.cfg.request_timeout;
        let p = producer.clone();
        tokio::task::spawn_blocking(move || p.init_transactions(timeout))
            .await
            .map_err(unavailable)?
            .map_err(|e| match classify(&e) {
                Class::Fenced => LogError::Fenced(e.to_string()),
                _ => LogError::Unavailable(format!("cannot fence partition {partition}: {e}")),
            })?;
        Ok(Arc::new(KafkaWriter {
            producer,
            partition,
            promises: self.cfg.topic(Topic::Promises),
            schedules: self.cfg.topic(Topic::Schedules),
            timeout: self.cfg.transaction_timeout,
        }))
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
        if self.cfg.transactional {
            Ok(raw)
        } else {
            Ok(Box::new(epoch::Filtered::new(raw, &from)))
        }
    }

    fn atomic(&self) -> bool {
        self.cfg.transactional
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

struct KafkaWriter {
    producer: FutureProducer,
    partition: u32,
    promises: String,
    schedules: String,
    timeout: Duration,
}

impl KafkaWriter {
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(FutureProducer) -> Result<T, KafkaError> + Send + 'static,
    ) -> Result<Result<T, KafkaError>, LogError> {
        let p = self.producer.clone();
        tokio::task::spawn_blocking(move || f(p))
            .await
            .map_err(|e| LogError::Uncertain(format!("transaction call panicked: {e}")))
    }

    /// Abort after an abortable error: nothing landed.
    async fn abort(&self, cause: KafkaError) -> LogError {
        let timeout = self.timeout;
        match self.blocking(move |p| p.abort_transaction(timeout)).await {
            Ok(Ok(())) => LogError::Unavailable(format!("transaction aborted: {cause}")),
            Ok(Err(e)) if classify(&e) == Class::Fenced => LogError::Fenced(e.to_string()),
            // Could not even abort: the producer's state is unknown.
            Ok(Err(e)) => LogError::Uncertain(format!("abort failed after {cause}: {e}")),
            Err(e) => e,
        }
    }
}

#[async_trait]
impl Writer for KafkaWriter {
    async fn commit(&self, records: Vec<Record>, base: Checkpoint) -> Result<Checkpoint, LogError> {
        if records.is_empty() {
            return Ok(base);
        }
        match self.blocking(|p| p.begin_transaction()).await? {
            Ok(()) => {}
            Err(e) => {
                return Err(match classify(&e) {
                    Class::Fenced => LogError::Fenced(e.to_string()),
                    // Nothing was begun, so nothing can have landed.
                    _ => LogError::Uncertain(format!("cannot begin a transaction: {e}")),
                });
            }
        }

        let partition = self.partition as i32;
        let mut deliveries = Vec::with_capacity(records.len());
        for r in &records {
            let topic = match r.topic {
                Topic::Promises => &self.promises,
                Topic::Schedules => &self.schedules,
            };
            let mut record = FutureRecord::<str, [u8]>::to(topic)
                .key(r.key.as_str())
                .partition(partition);
            if let Some(v) = &r.value {
                record = record.payload(v.as_slice());
            }
            match self.producer.send_result(record) {
                Ok(d) => deliveries.push((r.topic, d)),
                Err((e, _)) => return Err(self.abort(e).await),
            }
        }

        let mut after = base;
        for (topic, delivery) in deliveries {
            match delivery.await {
                Ok(Ok(d)) => {
                    if d.offset + 1 > after.get(topic) {
                        after.set(topic, d.offset + 1);
                    }
                }
                Ok(Err((e, _))) => return Err(self.abort(e).await),
                Err(_) => {
                    return Err(LogError::Uncertain(
                        "the producer dropped a delivery report".into(),
                    ))
                }
            }
        }

        // Commit, retrying what librdkafka says may be retried. Only an
        // outright abort is known not to have landed.
        let deadline = Instant::now() + self.timeout;
        loop {
            let timeout = self.timeout;
            match self
                .blocking(move |p| p.commit_transaction(timeout))
                .await?
            {
                Ok(()) => return Ok(after),
                Err(e) => match classify(&e) {
                    Class::Abort => return Err(self.abort(e).await),
                    Class::Fenced => return Err(LogError::Fenced(e.to_string())),
                    Class::Retry if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Class::Retry | Class::Fatal => {
                        return Err(LogError::Uncertain(format!("commit outcome unknown: {e}")))
                    }
                },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Without transactions
// ---------------------------------------------------------------------------

/// How many times a claim is tried before the takeover gives up: each retry
/// means something landed between reading the end and claiming it.
const CLAIM_ATTEMPTS: usize = 8;

impl KafkaLog {
    /// The high watermarks of `partition`'s two logs.
    async fn ends(&self, partition: u32) -> Result<Checkpoint, LogError> {
        high_watermarks(&self.cfg, &self.probe, partition).await
    }

    /// Become the writer of `partition` without transactions: claim each log
    /// where it ends, and check the claim landed there ([`super::epoch`]).
    async fn claim(&self, partition: u32) -> Result<PlainWriter, LogError> {
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
        Ok(PlainWriter {
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

/// The writer of a partition without transactions: its records carry its
/// epochs, and every one must land exactly where the last one left the log.
struct PlainWriter {
    producer: FutureProducer,
    cfg: KafkaCfg,
    probe: Arc<BaseConsumer>,
    partition: u32,
    epochs: [i64; 2],
}

#[async_trait]
impl Writer for PlainWriter {
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
                        // At the end. Resume past any trailing marker.
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
