// Coverage-guided fuzzing of the engines.
//
// The input is a byte tape. `Tape` reads it as a stream of decisions, so the
// bytes choose the operation and every number inside it, and the same bytes
// always produce the same run. That is what makes the two halves of a fuzzer
// possible: a crash is a tape you can replay, and a tape can be mutated into a
// neighbouring test case.
//
// The steering is real, not a bootstrap. Every step reports what it did as a
// set of features; a tape whose run produced a feature no earlier run produced
// is kept in the corpus and mutated further. So coverage decides what gets
// generated next, and the loop walks outward from whatever it has already
// managed to reach.
//
// Run:
//   cargo test --test fuzz -- --nocapture
//   FUZZ_SECS=300 cargo test --test fuzz -- --nocapture
//   FUZZ_REPLAY=<hex> cargo test --test fuzz -- --nocapture

mod harness;

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use harness::{Entropy, Harness, Op, Tape, ALL_OPS};
use tokio::runtime::Runtime;

/// Longest run one tape may drive.
///
/// Short runs are the point. A minimized failure has to be readable, and a
/// thousand-step tape shrinks to a hundred-step tape long before it shrinks to
/// the three steps that actually matter.
const MAX_STEPS: usize = 48;

/// How far a mutation may push a tape past the length it needs.
const MAX_TAPE: usize = 4096;

/// How many tapes to keep.
///
/// Unbounded, the corpus grows for as long as the campaign runs and every
/// entry dilutes the chance of mutating any other. At the cap the
/// lowest-scoring tape makes way for the new one.
const MAX_CORPUS: usize = 384;

// ---------------------------------------------------------------------------
// Features
// ---------------------------------------------------------------------------

/// One observable thing a run did.
///
/// Not source coverage — behaviour coverage. Source coverage of four engines
/// is four different things and none of them is the protocol; these are the
/// protocol, so they mean the same thing to every backend.
type Feature = u64;

fn hash(parts: &[u64]) -> Feature {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for p in parts {
        for b in p.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    h
}

fn str_key(s: &str) -> u64 {
    hash(&s.bytes().map(u64::from).collect::<Vec<_>>())
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Run {
    features: Vec<Feature>,
    /// `kind` and status of each step, for the report and the repro listing.
    trace: Vec<(String, u16)>,
    consumed: usize,
}

/// The byte span of each completed step of the last execution.
///
/// Kept outside the `Run` because the run that matters is the one that
/// panicked, and a panic takes its return value with it.
static MARKS: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());

fn marks() -> Vec<(usize, usize)> {
    MARKS.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Drive one tape through every backend.
///
/// Panics on divergence — that *is* the bug report. The caller catches it.
fn execute(rt: &Runtime, h: &mut Harness, bytes: &[u8]) -> Run {
    rt.block_on(async {
        h.reset().await;
        // Per-tape numbering: the step labels in a divergence should line up
        // with the trace printed beside them, not with the campaign.
        h.steps = 0;
        MARKS.lock().unwrap_or_else(|e| e.into_inner()).clear();
        let mut tape = Tape::new(bytes);
        let mut run = Run::default();
        // The previous step, so a feature can be a pair of steps rather than
        // one: reaching `task.fulfill` after `task.acquire` is a different
        // thing from reaching it cold, and only the pair says so.
        let mut prev: u64 = 0;

        while !tape.exhausted() && run.trace.len() < MAX_STEPS {
            let start = tape.consumed();
            let idx = Entropy::usize(&mut tape, 0..Op::ALL.len());
            if tape.exhausted() {
                break;
            }
            let op = Op::ALL[idx];
            let out = h.step(&mut tape, op).await;
            MARKS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((start, tape.consumed()));

            let o = idx as u64;
            let s = out.status as u64;
            run.features.push(hash(&[1, o, s, out.pre_class as u64]));
            run.features.push(hash(&[2, prev, o, s]));
            run.features.push(hash(&[3, o, s, out.post_census as u64]));
            run.features
                .push(hash(&[4, o, out.pre_class as u64, out.post_class as u64]));
            if let Some(kind) = out.fired {
                run.features.push(hash(&[5, o, str_key(kind)]));
            }
            prev = hash(&[o, s]);
            run.trace.push((out.kind, out.status));
        }
        run.consumed = tape.consumed();
        run
    })
}

/// The panic message of the last caught failure.
static LAST_PANIC: Mutex<Option<String>> = Mutex::new(None);

/// Run a tape, returning the panic message if it diverged.
fn try_execute(rt: &Runtime, h: &mut Harness, bytes: &[u8]) -> Result<Run, String> {
    match std::panic::catch_unwind(AssertUnwindSafe(|| execute(rt, h, bytes))) {
        Ok(run) => Ok(run),
        Err(_) => Err(LAST_PANIC
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or_else(|| "panicked with no message".into())),
    }
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

struct Entry {
    tape: Vec<u8>,
    /// Mutations between this tape and the random one it descends from.
    generation: u32,
    /// Times this tape has been chosen as a mutation base.
    execs: u32,
    /// Times mutating it produced something new.
    finds: u32,
}

/// How much attention a tape deserves.
///
/// A tape that has paid off before is worth more attempts; a tape that has had
/// many attempts already is worth fewer; a long tape is worth fewer still,
/// because it is slower to run and its mutations are less likely to be local.
fn score(e: &Entry) -> f64 {
    (1.0 + e.finds as f64) / ((1.0 + e.execs as f64) * (1.0 + e.tape.len() as f64 / 256.0))
}

/// Pick the tape to mutate next, weighted by `score`.
fn choose(rng: &mut fastrand::Rng, corpus: &[Entry]) -> usize {
    let total: f64 = corpus.iter().map(score).sum();
    let mut target = rng.f64() * total;
    for (i, e) in corpus.iter().enumerate() {
        target -= score(e);
        if target <= 0.0 {
            return i;
        }
    }
    corpus.len() - 1
}

/// Derive a neighbouring tape.
///
/// Stacked small edits, AFL's havoc stage in miniature. Splice is the one that
/// matters most here: two tapes that each reached somewhere interesting can
/// produce one that reaches both, which is how the loop composes progress it
/// made separately.
fn mutate(rng: &mut fastrand::Rng, base: &[u8], other: &[u8]) -> Vec<u8> {
    let mut t = base.to_vec();
    if t.is_empty() {
        t.push(rng.u8(..));
    }
    for _ in 0..rng.u32(1..7) {
        match rng.u32(0..8) {
            0 => {
                let i = rng.usize(0..t.len());
                t[i] ^= 1 << rng.u32(0..8);
            }
            1 => {
                let i = rng.usize(0..t.len());
                t[i] = rng.u8(..);
            }
            2 => {
                let i = rng.usize(0..t.len());
                t[i] = t[i].wrapping_add(rng.u8(1..8));
            }
            3 => {
                let i = rng.usize(0..t.len());
                t[i] = t[i].wrapping_sub(rng.u8(1..8));
            }
            4 if t.len() < MAX_TAPE => {
                let i = rng.usize(0..t.len());
                let n = rng.usize(1..17);
                let chunk: Vec<u8> = (0..n).map(|_| rng.u8(..)).collect();
                t.splice(i..i, chunk);
            }
            5 if t.len() > 4 => {
                let n = rng.usize(1..(t.len() / 2).max(2));
                let i = rng.usize(0..t.len() - n);
                t.drain(i..i + n);
            }
            6 if t.len() < MAX_TAPE && t.len() > 2 => {
                let n = rng.usize(1..(t.len() / 2).max(2));
                let from = rng.usize(0..t.len() - n);
                let chunk = t[from..from + n].to_vec();
                let at = rng.usize(0..t.len());
                t.splice(at..at, chunk);
            }
            _ if !other.is_empty() => {
                // Splice: keep this tape's head, take the other one's tail.
                let cut = rng.usize(0..t.len());
                let take = rng.usize(0..other.len());
                t.truncate(cut);
                t.extend_from_slice(&other[take..]);
            }
            _ => {
                let i = rng.usize(0..t.len());
                t[i] = rng.u8(..);
            }
        }
    }
    t.truncate(MAX_TAPE);
    t
}

// ---------------------------------------------------------------------------
// Minimization
// ---------------------------------------------------------------------------

const MAX_TRIALS: usize = 600;

/// Cut a failing tape down to what the failure needs.
///
/// Three passes, in the order that pays off. Deleting a whole step is the one
/// that matters: a step occupies a known byte span, so removing that span
/// leaves every other step decoding from the same bytes it did before. Deleting
/// an arbitrary byte range instead shifts every decision after it, which
/// produces a different run that happens to also fail — smaller, but no longer
/// an explanation of anything.
fn minimize(rt: &Runtime, h: &mut Harness, bytes: &[u8]) -> Vec<u8> {
    let mut trials = 0;
    let mut best = drop_steps(rt, h, bytes.to_vec(), &mut trials);
    best = zero_bytes(rt, h, best, &mut trials);
    best = drop_chunks(rt, h, best, &mut trials);
    drop_steps(rt, h, best, &mut trials)
}

/// Remove one whole step at a time, latest first, while the failure survives.
fn drop_steps(rt: &Runtime, h: &mut Harness, mut best: Vec<u8>, trials: &mut usize) -> Vec<u8> {
    'again: while *trials < MAX_TRIALS {
        // The spans are whatever the last execution recorded, so re-run the
        // current best to make them describe it.
        if try_execute(rt, h, &best).is_ok() {
            break;
        }
        for (start, end) in marks().into_iter().rev() {
            if end > best.len() {
                continue;
            }
            let mut candidate = best.clone();
            candidate.drain(start..end);
            *trials += 1;
            if try_execute(rt, h, &candidate).is_err() {
                best = candidate;
                continue 'again;
            }
            if *trials >= MAX_TRIALS {
                break;
            }
        }
        break;
    }
    best
}

/// Push every byte to zero, so the numbers in the repro are the simple ones.
fn zero_bytes(rt: &Runtime, h: &mut Harness, mut best: Vec<u8>, trials: &mut usize) -> Vec<u8> {
    for i in 0..best.len() {
        if *trials >= MAX_TRIALS {
            break;
        }
        if best[i] == 0 {
            continue;
        }
        let mut candidate = best.clone();
        candidate[i] = 0;
        *trials += 1;
        if try_execute(rt, h, &candidate).is_err() {
            best = candidate;
        }
    }
    best
}

/// Delta debugging on halving chunk sizes, for the bytes no step claimed.
fn drop_chunks(rt: &Runtime, h: &mut Harness, mut best: Vec<u8>, trials: &mut usize) -> Vec<u8> {
    let mut chunk = (best.len() / 2).max(1);
    loop {
        let mut i = 0;
        while i + chunk <= best.len() && *trials < MAX_TRIALS {
            let mut candidate = best.clone();
            candidate.drain(i..i + chunk);
            *trials += 1;
            if try_execute(rt, h, &candidate).is_err() {
                best = candidate;
            } else {
                i += chunk;
            }
        }
        if chunk == 1 || *trials >= MAX_TRIALS {
            break;
        }
        chunk /= 2;
    }
    best
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .filter_map(|p| u8::from_str_radix(std::str::from_utf8(p).ok()?, 16).ok())
        .collect()
}

/// Print a failure the way you would want to receive it.
fn report_failure(tape: &[u8], run: &Run, message: &str) -> ! {
    // Put the default hook back, or the panic below is swallowed by the one
    // installed to keep minimization quiet.
    let _ = std::panic::take_hook();
    eprintln!("\n[fuzz] DIVERGENCE — minimized to {} bytes\n", tape.len());
    eprintln!("[fuzz] replay with:");
    eprintln!(
        "[fuzz]   FUZZ_REPLAY={} cargo test --test fuzz -- --nocapture\n",
        hex(tape)
    );
    eprintln!("[fuzz] {} steps before the failure:", run.trace.len());
    for (i, (kind, status)) in run.trace.iter().enumerate() {
        eprintln!("[fuzz]   {i:>3}. {kind} -> {status}");
    }
    panic!("{message}");
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

#[test]
fn fuzz_guided() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let mut h = rt.block_on(Harness::open());

    // Failures are caught and re-run, hundreds of times while minimizing, so
    // the default hook would bury the report under its own output.
    std::panic::set_hook(Box::new(|info| {
        let payload = info.payload();
        let msg = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_else(|| "panicked with no message".into());
        *LAST_PANIC.lock().unwrap_or_else(|e| e.into_inner()) = Some(msg);
    }));

    if let Ok(spec) = std::env::var("FUZZ_REPLAY") {
        let tape = unhex(spec.trim());
        eprintln!("[fuzz] replaying {} bytes", tape.len());
        let outcome = try_execute(&rt, &mut h, &tape);
        let _ = std::panic::take_hook();
        match outcome {
            Ok(run) => {
                for (i, (kind, status)) in run.trace.iter().enumerate() {
                    eprintln!("[fuzz]   {i:>3}. {kind} -> {status}");
                }
                eprintln!("[fuzz] replay agreed on all {} steps", run.trace.len());
            }
            Err(message) => panic!("{message}"),
        }
        return;
    }

    let budget = Duration::from_secs(
        std::env::var("FUZZ_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30),
    );
    let seed = std::env::var("FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x00c0_ffee_dead_beef);
    let mut rng = fastrand::Rng::with_seed(seed);

    let mut seen: HashSet<Feature> = HashSet::new();
    let mut corpus: Vec<Entry> = Vec::new();
    let mut covered: HashMap<String, usize> = HashMap::new();
    let mut execs = 0usize;
    let mut steps = 0usize;

    // Seeds are random tapes, run once each before any mutation. Everything
    // the corpus holds after that is descended from one of them.
    const SEEDS: usize = 8;
    for _ in 0..SEEDS {
        let n = rng.usize(64..256);
        corpus.push(Entry {
            tape: (0..n).map(|_| rng.u8(..)).collect(),
            generation: 0,
            execs: 0,
            finds: 0,
        });
    }
    let mut seeded = 0usize;

    eprintln!("[fuzz] budget {}s, seed {seed:#x}", budget.as_secs());
    let start = Instant::now();
    let mut next_report = Duration::from_secs(5);

    while start.elapsed() < budget {
        // Seeds first, then mutations of whatever the corpus has grown into.
        let (tape, generation, parent) = if seeded < SEEDS {
            seeded += 1;
            (corpus[seeded - 1].tape.clone(), 0, Some(seeded - 1))
        } else {
            let base = choose(&mut rng, &corpus);
            let other = corpus[choose(&mut rng, &corpus)].tape.clone();
            corpus[base].execs += 1;
            let t = mutate(&mut rng, &corpus[base].tape, &other);
            (t, corpus[base].generation + 1, Some(base))
        };

        let run = match try_execute(&rt, &mut h, &tape) {
            Ok(run) => run,
            Err(message) => {
                eprintln!("\n[fuzz] failure after {execs} executions — minimizing");
                let small = minimize(&rt, &mut h, &tape);
                let (run, message) = match try_execute(&rt, &mut h, &small) {
                    Ok(_) => (Run::default(), message),
                    Err(m) => (execute_trace(&rt, &mut h, &small), m),
                };
                report_failure(&small, &run, &message);
            }
        };

        execs += 1;
        steps += run.trace.len();
        for (kind, status) in &run.trace {
            if *status < 300 {
                covered.entry(kind.clone()).or_insert(execs);
            }
        }

        // The feedback: a tape that showed us something new is worth keeping,
        // and worth mutating. Nothing else enters the corpus.
        let fresh = run.features.iter().filter(|f| !seen.contains(f)).count();
        if fresh > 0 {
            for f in &run.features {
                seen.insert(*f);
            }
            if let Some(p) = parent {
                corpus[p].finds += 1;
            }
            // Keep only the prefix the run actually read. The tail was never
            // looked at, so carrying it would only slow every descendant down.
            let mut kept = tape;
            kept.truncate(run.consumed.max(1));
            corpus.push(Entry {
                tape: kept,
                generation,
                execs: 0,
                finds: 0,
            });
            if corpus.len() > MAX_CORPUS {
                let worst = corpus
                    .iter()
                    .enumerate()
                    .min_by(|a, b| score(a.1).total_cmp(&score(b.1)))
                    .map(|(i, _)| i)
                    .unwrap_or(0);
                corpus.remove(worst);
            }
        }

        if start.elapsed() > next_report {
            eprintln!(
                "[fuzz] {:>3}s  execs {execs:>5}  steps {steps:>6}  features {:>4}  corpus {:>3}  ops {}/{}",
                start.elapsed().as_secs(),
                seen.len(),
                corpus.len(),
                covered.len(),
                ALL_OPS.len(),
            );
            next_report += Duration::from_secs(5);
        }
    }

    let _ = std::panic::take_hook();

    let max_generation = corpus.iter().map(|e| e.generation).max().unwrap_or(0);
    let descended = corpus.iter().filter(|e| e.generation > 0).count();

    eprintln!("\n[fuzz] coverage after {execs} executions ({steps} steps):");
    let mut missing = Vec::new();
    for op in ALL_OPS {
        match covered.get(*op) {
            Some(exec) => eprintln!("  [OK ] {op} (first 2xx in execution {exec})"),
            None => {
                eprintln!("  [MISS] {op}");
                missing.push(*op);
            }
        }
    }
    assert!(
        missing.is_empty(),
        "these ops never produced a 2xx: {missing:?}"
    );

    // If nothing descended from a kept tape ever found anything, the corpus is
    // decoration and this is a random tester wearing a fuzzer's clothes.
    assert!(
        descended > 0,
        "no mutated tape ever found a new feature — the feedback loop is not closed"
    );

    eprintln!(
        "\n[fuzz] PASSED — {execs} executions, {steps} steps, {} features, \
         corpus {} ({descended} found by mutation, deepest {max_generation} mutations from a seed)",
        seen.len(),
        corpus.len(),
    );
}

/// Re-run a tape that is known to fail, keeping the trace it produced.
///
/// `execute` panics part-way through, so the trace has to be rebuilt from the
/// steps that did complete: run it one step shorter until it survives.
fn execute_trace(rt: &Runtime, h: &mut Harness, bytes: &[u8]) -> Run {
    for cut in (1..=bytes.len()).rev() {
        if let Ok(run) = try_execute(rt, h, &bytes[..cut]) {
            return run;
        }
    }
    Run::default()
}
