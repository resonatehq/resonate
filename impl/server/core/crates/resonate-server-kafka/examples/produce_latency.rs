//! Where a commit's time goes, without Resonate in the way.
//!
//! `--loops` concurrent loops, each shaped like a partition actor: send a
//! round's records (`--batch`) to its own partition, await every delivery
//! report, repeat. The producers are configured as `log/kafka.rs` configures
//! the writer (idempotent, `acks=all`, lz4, `linger.ms=0`) unless overridden
//! with `--set key=value` (repeatable). Prints commit latency, then
//! librdkafka's own split of it, per broker:
//!
//!   int_latency     produce() to the record leaving the partition queue
//!   outbuf_latency  in the broker connection's output buffer
//!   rtt             request sent to response received (includes the broker
//!                   waiting for its followers when acks=all)
//!
//!   cargo run --release -p resonate-server-kafka --example produce_latency -- \
//!     --brokers localhost:9092 --replication 3 --loops 12 --batch 2 --seconds 20 \
//!     --set socket.nagle.disable=true

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::producer::{FutureProducer, FutureRecord, ProducerContext};
use rdkafka::statistics::Statistics;
use rdkafka::ClientConfig;

/// Per broker: (int_latency, outbuf_latency, rtt) averages in µs, last window.
type BrokerStats = BTreeMap<String, (i64, i64, i64, i64)>;

#[derive(Clone, Default)]
struct Stats(Arc<Mutex<BrokerStats>>);

impl ClientContext for Stats {
    fn stats(&self, s: Statistics) {
        let mut out = self.0.lock().unwrap();
        for (name, b) in s.brokers {
            if b.nodeid < 0 {
                continue;
            }
            let avg = |w: &Option<rdkafka::statistics::Window>| w.as_ref().map_or(0, |w| w.avg);
            let p99 = |w: &Option<rdkafka::statistics::Window>| w.as_ref().map_or(0, |w| w.p99);
            out.insert(
                name,
                (avg(&b.int_latency), avg(&b.outbuf_latency), avg(&b.rtt), p99(&b.rtt)),
            );
        }
    }
}

impl ProducerContext for Stats {
    type DeliveryOpaque = ();
    fn delivery(&self, _: &rdkafka::message::DeliveryResult<'_>, _: ()) {}
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() - 1) as f64 * q).round() as usize]
}

#[tokio::main]
async fn main() {
    let (mut brokers, mut seconds, mut loops, mut batch, mut replication, mut size) =
        ("localhost:9092".to_string(), 20u64, 12usize, 2usize, 3i32, 600usize);
    let mut sets: Vec<(String, String)> = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_default();
        match flag.as_str() {
            "--brokers" => brokers = v,
            "--seconds" => seconds = v.parse().unwrap(),
            "--loops" => loops = v.parse().unwrap(),
            "--batch" => batch = v.parse().unwrap(),
            "--replication" => replication = v.parse().unwrap(),
            "--size" => size = v.parse().unwrap(),
            "--set" => {
                let (k, val) = v.split_once('=').expect("--set key=value");
                sets.push((k.into(), val.into()));
            }
            other => panic!("unknown flag {other}"),
        }
    }

    let topic = format!("latency-{:08x}", fastrand::u32(..));
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new(&topic, loops as i32, TopicReplication::Fixed(replication))
                .set("min.insync.replicas", if replication >= 3 { "2" } else { "1" })],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let payload = Arc::new(vec![b'x'; size]);
    let stats = Stats::default();
    let mut handles = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(seconds + 3);
    let measure_from = Instant::now() + Duration::from_secs(3);
    for l in 0..loops {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("linger.ms", "0")
            .set("statistics.interval.ms", if l == 0 { "1000" } else { "0" });
        for (k, v) in &sets {
            c.set(k, v);
        }
        let producer: FutureProducer<Stats> = c.create_with_context(stats.clone()).unwrap();
        let (topic, payload) = (topic.clone(), payload.clone());
        handles.push(tokio::spawn(async move {
            let mut times = Vec::new();
            let mut n = 0u64;
            while Instant::now() < deadline {
                let t = Instant::now();
                let mut deliveries = Vec::with_capacity(batch);
                for _ in 0..batch {
                    n += 1;
                    let key = format!("k{n}");
                    let r = FutureRecord::<str, [u8]>::to(&topic)
                        .key(&key)
                        .payload(payload.as_slice())
                        .partition(l as i32);
                    deliveries.push(producer.send_result(r).map_err(|(e, _)| e).unwrap());
                }
                for d in deliveries {
                    d.await.unwrap().map_err(|(e, _)| e).unwrap();
                }
                if t >= measure_from {
                    times.push(t.elapsed().as_secs_f64() * 1e3);
                }
            }
            times
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    let mean = all.iter().sum::<f64>() / all.len().max(1) as f64;
    let n = all.len();
    println!(
        "replication {replication}, {loops} loops x {batch} records of {size} B, {}",
        if sets.is_empty() {
            "writer defaults".to_string()
        } else {
            sets.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")
        }
    );
    println!(
        "  commit  n={n}  {:.0}/s  mean {mean:.2} ms  p50 {:.2}  p90 {:.2}  p99 {:.2}  max {:.2}",
        n as f64 / seconds as f64,
        pct(&mut all, 0.5),
        pct(&mut all, 0.9),
        pct(&mut all, 0.99),
        pct(&mut all, 1.0)
    );
    for (name, (int, outbuf, rtt, rtt99)) in stats.0.lock().unwrap().iter() {
        println!(
            "  {name:<28} int_latency {:>6.2} ms  outbuf {:>6.2} ms  rtt {:>6.2} ms (p99 {:.2})",
            *int as f64 / 1e3,
            *outbuf as f64 / 1e3,
            *rtt as f64 / 1e3,
            *rtt99 as f64 / 1e3
        );
    }
    let _ = admin.delete_topics(&[&topic], &AdminOptions::new()).await;
}
