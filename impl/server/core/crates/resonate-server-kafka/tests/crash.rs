// The RocksDB copy runs without a write-ahead log. The claim that makes that
// safe: after a crash, the checkpoint on disk is never ahead of the data on
// disk — so a restart that replays Kafka from it misses nothing.
//
// A claim about a crash is only a claim until something crashes. This test
// re-runs itself as a child process that writes batches, flushes part-way,
// keeps writing, and then aborts with no shutdown at all. The parent reopens
// the database and checks every record the surviving checkpoint names.
//
// Run:
//   cargo test -p resonate-server-kafka --test crash

use std::process::Command;

use resonate_server_kafka::local::rocks::{RocksCfg, RocksLocal};
use resonate_server_kafka::local::LocalStore;
use resonate_server_kafka::log::Checkpoint;

const CHILD: &str = "RESONATE_KAFKA_CRASH_CHILD";
const TEST: &str = "an_aborted_process_leaves_no_checkpoint_ahead_of_its_data";
const BATCHES: i64 = 2_000;
const FLUSH_AT: i64 = 700;

fn key(i: i64) -> Vec<u8> {
    format!("o{i:08}").into_bytes()
}

/// The child: write, flush part-way, write more, die.
fn child(dir: &str) -> ! {
    let store = RocksLocal::open(
        std::path::Path::new(dir),
        // A small memtable budget, so RocksDB also flushes on its own.
        &RocksCfg {
            block_cache_bytes: 8 << 20,
            write_buffer_bytes: 1 << 20,
        },
    )
    .unwrap();
    let p = store.open(0).unwrap();
    for i in 0..BATCHES {
        // One record per batch, a checkpoint just past it — as a commit does.
        p.apply(
            vec![(key(i), Some(vec![b'x'; 512]))],
            Checkpoint {
                promises: i + 1,
                schedules: 0,
            },
        )
        .unwrap();
        if i == FLUSH_AT {
            p.flush().unwrap();
        }
    }
    // No flush, no close, no destructors.
    std::process::abort();
}

#[test]
fn an_aborted_process_leaves_no_checkpoint_ahead_of_its_data() {
    if let Ok(dir) = std::env::var(CHILD) {
        child(&dir);
    }
    let dir = tempfile::tempdir().unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--test-threads=1", "--nocapture"])
        .env(CHILD, dir.path())
        .status()
        .unwrap();
    assert!(!status.success(), "the child was meant to abort");

    let store = RocksLocal::open(dir.path(), &RocksCfg::default()).unwrap();
    let p = store.open(0).unwrap();
    let cp = p
        .checkpoint()
        .unwrap()
        .expect("the explicit flush made a checkpoint durable");
    assert!(
        cp.promises > FLUSH_AT,
        "the flushed checkpoint survived: {cp:?}"
    );
    // Everything the checkpoint says was applied, was.
    for i in 0..cp.promises {
        assert!(
            p.get(&key(i)).unwrap().is_some(),
            "checkpoint {} names record {i}, which is gone",
            cp.promises
        );
    }
    eprintln!(
        "[crash] checkpoint {} of {BATCHES} survived; every record it names is present",
        cp.promises
    );
}
