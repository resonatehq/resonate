//! What does a Kafka transaction cost over a plain produce?
//!
//! The same records, sent and acknowledged (`acks=all`, idempotent), with and
//! without a transaction around them; one loop per partition, each with its
//! own producer, as the partition shell runs. For the transactional runs the
//! time is split into its phases:
//!
//!   begin    begin_transaction (local)
//!   produce  send + delivery reports (the first send also adds the
//!            partition to the transaction: AddPartitionsToTxn)
//!   commit   commit_transaction (EndTxn; markers are written after)
//!
//!   cargo run --release -p resonate-server-kafka --example txn_cost -- \
//!     --brokers localhost:9092 --seconds 8

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;

#[derive(Clone, Copy)]
struct Run {
    txn: bool,
    linger_ms: u32,
    batch: usize,
    loops: usize,
}

#[derive(Default)]
struct Times {
    total: Vec<f64>,
    begin: Vec<f64>,
    produce: Vec<f64>,
    commit: Vec<f64>,
}

fn stats(v: &mut [f64]) -> String {
    if v.is_empty() {
        return "-".into();
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = v.iter().sum::<f64>() / v.len() as f64;
    let p = |q: f64| v[((v.len() - 1) as f64 * q).round() as usize];
    format!(
        "{:6.2} {:6.2} {:6.2}",
        mean * 1e3,
        p(0.5) * 1e3,
        p(0.99) * 1e3
    )
}

async fn run(brokers: &str, topic: &str, r: Run, seconds: u64, tag: &str) -> (f64, Times) {
    let payload = Arc::new(vec![b'x'; 700]);
    let mut handles = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(seconds);
    for l in 0..r.loops {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", r.linger_ms.to_string())
            .set("compression.type", "lz4");
        if r.txn {
            c.set("transactional.id", format!("txcost-{tag}-{l}"));
        }
        let producer: FutureProducer = c.create().unwrap();
        if r.txn {
            let p = producer.clone();
            tokio::task::spawn_blocking(move || p.init_transactions(Duration::from_secs(30)))
                .await
                .unwrap()
                .unwrap();
        }
        let (topic, payload) = (topic.to_string(), Arc::clone(&payload));
        handles.push(tokio::spawn(async move {
            let mut t = Times::default();
            let mut n = 0u64;
            while Instant::now() < deadline {
                let start = Instant::now();
                if r.txn {
                    let p = producer.clone();
                    tokio::task::spawn_blocking(move || p.begin_transaction())
                        .await
                        .unwrap()
                        .unwrap();
                }
                let began = Instant::now();
                let mut deliveries = Vec::with_capacity(r.batch);
                for _ in 0..r.batch {
                    n += 1;
                    let key = format!("k{n}");
                    let rec = FutureRecord::to(&topic)
                        .key(&key)
                        .payload(payload.as_slice())
                        .partition(l as i32);
                    deliveries.push(producer.send_result(rec).map_err(|(e, _)| e).unwrap());
                }
                for d in deliveries {
                    d.await.unwrap().map_err(|(e, _)| e).unwrap();
                }
                let produced = Instant::now();
                if r.txn {
                    let p = producer.clone();
                    tokio::task::spawn_blocking(move || {
                        p.commit_transaction(Duration::from_secs(30))
                    })
                    .await
                    .unwrap()
                    .unwrap();
                }
                let done = Instant::now();
                t.total.push((done - start).as_secs_f64());
                t.begin.push((began - start).as_secs_f64());
                t.produce.push((produced - began).as_secs_f64());
                t.commit.push((done - produced).as_secs_f64());
            }
            t
        }));
    }
    let started = Instant::now();
    let mut all = Times::default();
    for h in handles {
        let t = h.await.unwrap();
        all.total.extend(t.total);
        all.begin.extend(t.begin);
        all.produce.extend(t.produce);
        all.commit.extend(t.commit);
    }
    let elapsed = started.elapsed().as_secs_f64().max(seconds as f64);
    (all.total.len() as f64 / elapsed, all)
}

#[tokio::main]
async fn main() {
    let mut brokers = "localhost:9092".to_string();
    let mut seconds = 8u64;
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let v = it.next().unwrap_or_default();
        match f.as_str() {
            "--brokers" => brokers = v,
            "--seconds" => seconds = v.parse().unwrap(),
            other => panic!("unknown flag {other}"),
        }
    }

    let topic = format!("txcost-{:08x}", fastrand::u32(..));
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new(&topic, 16, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!(
        "{:<34} {:>9} {:>11}   {:>20}   {:>20}   {:>20}",
        "", "batches/s", "records/s", "total ms mean/p50/p99", "produce", "commit"
    );
    let runs = [
        (
            "plain, 1 record",
            Run {
                txn: false,
                linger_ms: 0,
                batch: 1,
                loops: 1,
            },
        ),
        (
            "txn,   1 record",
            Run {
                txn: true,
                linger_ms: 0,
                batch: 1,
                loops: 1,
            },
        ),
        (
            "txn,   1 record, linger 2ms",
            Run {
                txn: true,
                linger_ms: 2,
                batch: 1,
                loops: 1,
            },
        ),
        (
            "plain, 8 records",
            Run {
                txn: false,
                linger_ms: 0,
                batch: 8,
                loops: 1,
            },
        ),
        (
            "txn,   8 records",
            Run {
                txn: true,
                linger_ms: 0,
                batch: 8,
                loops: 1,
            },
        ),
        (
            "plain, 1 record, 16 partitions",
            Run {
                txn: false,
                linger_ms: 0,
                batch: 1,
                loops: 16,
            },
        ),
        (
            "txn,   1 record, 16 partitions",
            Run {
                txn: true,
                linger_ms: 0,
                batch: 1,
                loops: 16,
            },
        ),
        (
            "txn,   8 records, 16 partitions",
            Run {
                txn: true,
                linger_ms: 0,
                batch: 8,
                loops: 16,
            },
        ),
    ];
    for (i, (name, r)) in runs.iter().enumerate() {
        let (rate, mut t) = run(
            &brokers,
            &topic,
            *r,
            seconds,
            &format!("{i}-{:x}", fastrand::u32(..)),
        )
        .await;
        println!(
            "{:<34} {:>9.0} {:>11.0}   {}   {}   {}",
            name,
            rate,
            rate * r.batch as f64,
            stats(&mut t.total),
            stats(&mut t.produce),
            if r.txn {
                stats(&mut t.commit)
            } else {
                "-".into()
            },
        );
    }
    let _ = admin.delete_topics(&[&topic], &AdminOptions::new()).await;
}
