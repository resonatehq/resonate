//! `Server::fire` works on due deadlines side by side, and no more than
//! `FIRE_CONCURRENCY` at once.
//!
//! The engine here is a stand-in whose `Internal` takes 25 ms, the shape of a
//! store with a network round trip per transaction. Fired one after another,
//! 64 of them take 1.6 s, and the timer cannot look at its wheel again until
//! they are done.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::Message;
use resonate_core::{ResonateServer, Unavailable};
use resonate_sql::engine::{Engine, Input, Outgoing, Output, Scheduled, Timeout};
use resonate_sql::server::{Options, Server, FIRE_CONCURRENCY};
use resonate_sql::StorageResult;

const WORK: Duration = Duration::from_millis(25);

#[derive(Default)]
struct SlowEngine {
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    done: AtomicUsize,
}

#[async_trait]
impl Engine for SlowEngine {
    async fn process(&self, input: Input<'_>, _now: i64) -> Output {
        assert!(matches!(input, Input::Internal(_)));
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(WORK).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.done.fetch_add(1, Ordering::SeqCst);
        Output {
            response: None,
            messages: Vec::new(),
            timeouts: Vec::new(),
        }
    }

    async fn tick(&self, _now: i64) -> StorageResult<(usize, Vec<Outgoing>, Vec<Scheduled>)> {
        Ok((0, Vec::new(), Vec::new()))
    }

    async fn upcoming(&self, _limit: usize) -> StorageResult<Vec<Scheduled>> {
        Ok(Vec::new())
    }
}

struct NoRouter;

#[async_trait]
impl ResonateRouter for NoRouter {
    async fn route(&self, _address: &str, _msg: &Message) -> Result<(), Unavailable> {
        Ok(())
    }
}

#[tokio::test]
async fn due_deadlines_fire_side_by_side_within_the_bound() {
    const N: usize = 64;
    let engine = Arc::new(SlowEngine::default());
    let handle: Arc<dyn Engine> = engine.clone();
    let server = Server::new(
        Box::new(move |_debug| Box::pin(async move { Ok(handle) })),
        Arc::new(NoRouter),
        Options {
            server_url: String::new(),
            wheel_capacity: 16,
            wheel_refresh: 30_000,
            sweep_interval: 60_000,
        },
    );
    // Debug: the engine is opened, and no timer or sweep runs beside the test.
    server.init(true).await.unwrap();

    let timeouts = (0..N)
        .map(|i| Timeout::TaskRetryTimeout {
            task_id: format!("t{i}"),
        })
        .collect();
    let start = Instant::now();
    server.fire(timeouts).await;
    let elapsed = start.elapsed();

    assert_eq!(
        engine.done.load(Ordering::SeqCst),
        N,
        "every deadline fired"
    );
    assert_eq!(engine.in_flight.load(Ordering::SeqCst), 0);
    let peak = engine.peak.load(Ordering::SeqCst);
    assert_eq!(
        peak, FIRE_CONCURRENCY,
        "at most, and up to, the bound at once"
    );
    // Serially this is N × 25 ms = 1.6 s; at the bound it is about 200 ms.
    // Half of serial is generous, and still fails on one at a time.
    assert!(
        elapsed < WORK * N as u32 / 2,
        "{N} fires of {WORK:?} took {elapsed:?}"
    );
    server.stop().await.unwrap();
}
