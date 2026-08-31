// Differential random testing: drive the same random operation sequence through
// multiple backends simultaneously and assert identical responses and state
// snapshots at every step.
//
// Backends:
//   SQLite   — always active (in-memory, :memory:)
//   Oracle   — always active (in-memory reference model)
//   Postgres — active when TEST_POSTGRES_URL env var is set
//   MySQL    — active when TEST_MYSQL_URL env var is set
//
// Coverage requirement: the test runs until every operation kind has produced
// at least one 2xx response, guaranteeing that we are not trivially passing by
// only exercising failure paths.
//
// Run (SQLite + Oracle only):
//   cargo test --test differential -- --nocapture
//
// Run (all backends):
//   TEST_POSTGRES_URL=postgres://resonate:resonate@localhost:5432/resonate \
//   TEST_MYSQL_URL=mysql://resonate:resonate@localhost:3306/resonate \
//     cargo test --test differential -- --nocapture

mod harness;

use std::collections::{HashMap, HashSet};

use harness::{print_timing_summary, Entropy, Harness, Op, ALL_OPS};
use resonate_core::types::TaskState;
use resonate_server_dbms::oracle::Oracle;

#[tokio::test(flavor = "multi_thread")]
async fn differential_random() {
    debug_assert_eq!(22, ALL_OPS.len(), "Op has 22 variants; ALL_OPS must match");

    let mut h = Harness::open().await;
    h.verbose = true;

    const MAX_STEPS: usize = 200_000;
    const BATCH_SIZE: usize = 200;
    const PLATEAU_BATCHES: usize = 20;

    let mut rng = fastrand::Rng::with_seed(0x00c0_ffee_dead_beef);
    let mut covered: HashMap<String, usize> = HashMap::new();
    let mut total_steps = 0usize;
    let mut seen_sigs: HashSet<(String, u16, u8)> = HashSet::new();
    let mut plateau_count = 0usize;

    'outer: loop {
        h.reset().await;

        let sigs_before = seen_sigs.len();

        for _ in 0..BATCH_SIZE {
            if total_steps >= MAX_STEPS {
                break 'outer;
            }

            // Query oracle state to choose the next operation, then release the
            // lock before the step so the oracle backend can reacquire it.
            let op = {
                let o = h.oracle.lock();
                pick_op(&mut rng, &o, &covered)
            };
            total_steps += 1;

            let out = h.step(&mut rng, op).await;

            if out.status < 300 {
                covered.entry(out.kind.clone()).or_insert(total_steps);
            }
            seen_sigs.insert((out.kind, out.status, out.pre_class));
        }

        h.assert_settled(&format!("step={total_steps}")).await;

        let new_sigs = seen_sigs.len().saturating_sub(sigs_before);
        if covered.len() == ALL_OPS.len() {
            if new_sigs == 0 {
                plateau_count += 1;
                eprintln!(
                    "[diff] plateau {plateau_count}/{PLATEAU_BATCHES} — {} total signatures, no new in this batch",
                    seen_sigs.len()
                );
            } else {
                plateau_count = 0;
            }
            if plateau_count >= PLATEAU_BATCHES {
                eprintln!(
                    "[diff] coverage plateau reached after {total_steps} steps ({} signatures)",
                    seen_sigs.len()
                );
                break 'outer;
            }
        }
    }

    h.assert_settled("final").await;

    eprintln!("[diff] coverage after {total_steps} steps:");
    let mut missing = Vec::new();
    for op in ALL_OPS {
        if let Some(step) = covered.get(*op) {
            eprintln!("  [OK ] {op} (first 2xx at step {step})");
        } else {
            eprintln!("  [MISS] {op}");
            missing.push(*op);
        }
    }

    if !missing.is_empty() {
        panic!(
            "Coverage incomplete after {total_steps} steps — these ops never produced a 2xx: {:?}",
            missing
        );
    }

    eprintln!(
        "[diff] PASSED — {total_steps} steps, {} timeouts fired, {} backends, all {} ops covered, {} behavioral signatures",
        h.fired,
        h.backends.len(),
        ALL_OPS.len(),
        seen_sigs.len(),
    );

    print_timing_summary(&mut h.timings, &h.backends);
}

fn pick_op(rng: &mut dyn Entropy, oracle: &Oracle, covered: &HashMap<String, usize>) -> Op {
    let uncovered = |kind: &str| !covered.contains_key(kind);

    let has_acquired = oracle.has_tasks_in_state(TaskState::Acquired);
    let has_pending_t = oracle.has_tasks_in_state(TaskState::Pending);
    let has_suspended = oracle.has_tasks_in_state(TaskState::Suspended);
    let has_halted = oracle.has_tasks_in_state(TaskState::Halted);
    let has_pending_p = oracle.has_pending_promises();
    let has_pending_p_with_target = oracle.has_pending_promises_with_target();
    let has_schedules = oracle.has_schedules();

    if uncovered("task.suspend") && has_acquired && has_pending_p_with_target {
        return Op::TaskSuspend;
    }
    if uncovered("task.continue") && has_halted {
        return Op::TaskContinue;
    }
    if uncovered("task.release") && has_acquired {
        return Op::TaskRelease;
    }
    if uncovered("task.fulfill") && has_acquired {
        return Op::TaskFulfill;
    }
    if uncovered("task.halt") && (has_acquired || has_pending_t || has_suspended) {
        return Op::TaskHalt;
    }
    if uncovered("task.acquire") && has_pending_t {
        return Op::TaskAcquire;
    }
    if uncovered("promise.register_callback")
        && has_pending_p_with_target
        && (has_acquired || has_pending_t)
    {
        return Op::PromiseRegisterCallback;
    }
    if uncovered("promise.register_listener") && has_pending_p {
        return Op::PromiseRegisterListener;
    }
    if uncovered("schedule.delete") && has_schedules {
        return Op::ScheduleDelete;
    }
    if uncovered("task.fence") && has_acquired {
        return Op::TaskFence;
    }

    match rng.u32(0..100) {
        0..=14 => Op::PromiseCreate,
        15..=19 => Op::PromiseGet,
        20..=24 => Op::PromiseSettle,
        25..=27 => Op::PromiseRegisterCallback,
        28..=29 => Op::PromiseRegisterListener,
        30..=31 => Op::PromiseSearch,
        32..=39 => Op::TaskCreate,
        40..=41 => Op::TaskGet,
        42..=44 => Op::TaskAcquire,
        45..=47 => Op::TaskRelease,
        48..=52 => Op::TaskFulfill,
        53..=57 => Op::TaskSuspend,
        58..=60 => Op::TaskFence,
        61..=63 => Op::TaskHeartbeat,
        64..=66 => Op::TaskHalt,
        67..=69 => Op::TaskContinue,
        70..=71 => Op::TaskSearch,
        72..=77 => Op::ScheduleCreate,
        78..=80 => Op::ScheduleGet,
        81..=83 => Op::ScheduleDelete,
        84..=85 => Op::ScheduleSearch,
        _ => Op::DebugTick,
    }
}
