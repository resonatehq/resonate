// Property-based testing of the engines, with shrinking.
//
// Same comparison as the differential, generated differently. proptest builds
// a sequence of transitions, runs it, and — when it fails — spends the rest of
// its budget making the sequence shorter and the numbers in it smaller. What
// you get back is not the run that failed but the smallest run that still
// fails, which is usually a handful of steps you can read.
//
// The two halves of `proptest-state-machine` map onto what is already here:
//
//   ReferenceStateMachine — `World` below, the model that decides which
//                           operations are worth generating next
//   StateMachineTest      — the engines, driven through the shared harness
//
// `World` is deliberately thin. The real model is the oracle, and the oracle
// is right there in the harness, in lockstep with the engines — so the
// reference only has to be good enough to keep proptest from generating an
// acquire before anything has been created. Its counters are optimistic (an
// acquire it permits may still 404) and that is fine: preconditions have to be
// deterministic, not accurate.
//
// Run:
//   cargo test --test prop -- --nocapture
//   PROP_CASES=200 cargo test --test prop -- --nocapture

mod harness;

use std::sync::{Mutex, OnceLock};

use harness::{Harness, Op, Script};
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence};
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use tokio::runtime::Runtime;

/// How many entropy draws one step can make.
///
/// The longest generator is `task.heartbeat`: three draws to shuffle, three
/// for the versions, one for the pid, and one more for the deadline the step
/// fires afterwards. Fourteen leaves room for a generator to grow.
const DRAWS: usize = 14;

// ---------------------------------------------------------------------------
// The reference
// ---------------------------------------------------------------------------

/// What has been asked for so far.
///
/// Counters, not contents: enough to say an operation has a subject, not which
/// one. Which one is the oracle's job, at apply time.
#[derive(Clone, Debug, Default)]
struct World {
    promises: u32,
    tasks: u32,
    acquires: u32,
    halts: u32,
    schedules: u32,
}

/// One step: which operation, and the numbers its generator will draw.
///
/// proptest shrinks both. The operation index shrinks toward `promise.create`
/// and the draws shrink toward zero, so a minimized failure tends to be the
/// simplest request of each kind rather than an arbitrary one.
#[derive(Clone)]
struct Move {
    op: usize,
    draws: Vec<u64>,
}

/// A shrunk sequence is the report, so print it as a sequence of operations.
///
/// Derived `Debug` prints fourteen u64s per step, most of them zero after
/// shrinking, which buries the one thing worth reading.
impl std::fmt::Debug for Move {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Op::ALL[self.op].kind())?;
        if let Some(last) = self.draws.iter().rposition(|d| *d != 0) {
            write!(f, "{:?}", &self.draws[..=last])?;
        }
        Ok(())
    }
}

impl ReferenceStateMachine for World {
    type State = World;
    type Transition = Move;

    fn init_state() -> BoxedStrategy<World> {
        Just(World::default()).boxed()
    }

    fn transitions(_: &World) -> BoxedStrategy<Move> {
        (0..Op::ALL.len(), prop::collection::vec(any::<u64>(), DRAWS))
            .prop_map(|(op, draws)| Move { op, draws })
            .boxed()
    }

    fn apply(mut state: World, transition: &Move) -> World {
        match Op::ALL[transition.op] {
            Op::PromiseCreate => state.promises += 1,
            Op::TaskCreate => state.tasks += 1,
            Op::TaskAcquire => state.acquires += 1,
            Op::TaskHalt => state.halts += 1,
            Op::ScheduleCreate => state.schedules += 1,
            _ => {}
        }
        state
    }

    fn preconditions(state: &World, transition: &Move) -> bool {
        use Op::*;
        match Op::ALL[transition.op] {
            PromiseGet | PromiseSettle | PromiseRegisterCallback | PromiseRegisterListener => {
                state.promises > 0
            }
            TaskGet | TaskAcquire | TaskHalt | TaskHeartbeat => state.tasks > 0,
            TaskRelease | TaskFulfill | TaskSuspend | TaskFence => state.acquires > 0,
            TaskContinue => state.halts > 0,
            ScheduleGet | ScheduleDelete => state.schedules > 0,
            _ => true,
        }
    }
}

// ---------------------------------------------------------------------------
// The system under test
// ---------------------------------------------------------------------------

/// One harness for the whole run.
///
/// Opening it means connecting to Postgres and MySQL and applying migrations,
/// which is far too slow to do per case. proptest runs cases one at a time, so
/// a single harness reset between them is the same thing, much cheaper.
fn engines() -> &'static Mutex<Harness> {
    static ENGINES: OnceLock<Mutex<Harness>> = OnceLock::new();
    ENGINES.get_or_init(|| Mutex::new(runtime().block_on(Harness::open())))
}

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime")
    })
}

/// A case panicked while holding the harness. The next case still needs it.
fn harness() -> std::sync::MutexGuard<'static, Harness> {
    engines().lock().unwrap_or_else(|e| e.into_inner())
}

struct Engines;

impl StateMachineTest for Engines {
    type SystemUnderTest = ();
    type Reference = World;

    fn init_test(_: &World) {
        // Take the harness *before* entering the runtime. Opening it the first
        // time blocks on the same runtime, and tokio refuses to block a thread
        // that is already driving it.
        let mut h = harness();
        runtime().block_on(h.reset());
        // Per-case numbering, to match the transition list proptest prints.
        h.steps = 0;
    }

    fn apply(_: (), _: &World, transition: Move) {
        let mut script = Script::new(transition.draws);
        let op = Op::ALL[transition.op];
        let mut h = harness();
        runtime().block_on(h.step(&mut script, op));
    }

    fn check_invariants(_: &(), _: &World) {
        let h = harness();
        runtime().block_on(h.assert_settled("prop"));
    }
}

fn cases() -> u32 {
    std::env::var("PROP_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32)
}

prop_state_machine! {
    #![proptest_config(Config {
        cases: cases(),
        // `diff/` is not a source root, so proptest cannot work out where to
        // keep regressions on its own. A failing case belongs in the repo.
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "diff/proptest-regressions/prop.txt",
        ))),
        // Shrinking a divergence is the whole reason this target exists, so
        // give it room; every iteration is one more step removed from the
        // report someone has to read.
        max_shrink_iters: 4096,
        .. Config::default()
    })]

    #[test]
    fn engines_agree(sequential 1..40 => Engines);
}
