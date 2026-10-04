//! resonate-fuzz: the guided, oracle-informed fuzzer against a real server.
//!
//! The generators and the feedback loop are `examples/genexp.rs`'s, which ran
//! only against the in-memory model. Here every request goes over HTTP to a
//! real server — or to one of several sharing a store — and the oracle runs
//! alongside as the expected answer:
//!
//! * **Answers.** The server's answer to every request must equal the oracle's:
//!   status and `data`. Time is an input, not a race: every request carries the
//!   program's instant in `resonate:debug_time`, so the server must run in debug
//!   mode (`--debug`, and `--deliver` so it sends its messages).
//! * **State.** After each program, `debug.snap` must equal the oracle's —
//!   promises, tasks, callbacks, listeners, timeouts. The outbox is left out:
//!   the server delivers what the oracle holds.
//! * **Messages.** The fuzzer is its own worker: every target and listener
//!   address is its callback listener, so the server's `execute` and `unblock`
//!   messages come back here. They are compared with what the oracle emitted,
//!   and an `execute` offer goes into the inbox the informed generator acquires
//!   from — at the version offered, which may have gone stale.
//!
//! Guidance is genexp's: a program is a byte tape, and one that reached an
//! (operation, status class, store shape) signature no program reached before
//! is kept and mutated. Each program runs in its own id namespace after a
//! `debug.reset`, so programs are independent of each other.
//!
//! A request with no definite answer — a transport error, a 5xx — ends the
//! program: the oracle cannot know whether it took effect, so the two can no
//! longer be compared. That is not a failure; under fault injection it is the
//! normal case.
//!
//! Under skulld (the agent's socket is present) every request and response is
//! also streamed out as `{"resonate_trace": …}` for the spec checkers on the
//! host, and the comparisons are skulld assertions.

mod generate;
mod guide;
mod skull;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use resonate_core::types::{RequestEnvelope, PROTOCOL_VERSION};
use resonate_oracle::Oracle;
use resonate_sql::engine::Outgoing;
use serde_json::{json, Value};

use generate::{Names, Tape};
use guide::Corpus;
use skull::Skull;

#[derive(Parser)]
#[command(about = "Guided, oracle-informed fuzzing of a Resonate server over HTTP")]
struct Args {
    /// The server, or a comma-separated list sharing one store; each request
    /// goes to one of them at random. All must run with --debug --deliver.
    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:8001")]
    url: Vec<String>,
    /// informed (operands read from the oracle) or blind (operands guessed).
    #[arg(long, default_value = "informed")]
    generator: String,
    /// full: every operation, judged by the oracle. spec: only what the spec
    /// checkers decode, and the trace is streamed to skulld for them.
    #[arg(long, default_value = "full")]
    alphabet: String,
    /// Keep tapes that reach new signatures and mutate them.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    guided: bool,
    /// Programs to run; the deadline may end the run sooner.
    #[arg(long, default_value_t = 100)]
    programs: usize,
    /// Requests in one program, after which the server and oracle are reset.
    #[arg(long, default_value_t = 128)]
    steps: usize,
    /// Stop starting programs after this long (skulld sets it per command).
    #[arg(long, env = "SKULL_DEADLINE_MS")]
    deadline_ms: Option<u64>,
    /// 0 takes one from the clock — which is deterministic inside a skulld guest.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// The callback listener's port; 0 takes any free one.
    #[arg(long, default_value_t = 0)]
    listen_port: u16,
    /// How the server reaches the callback listener.
    #[arg(long, default_value = "127.0.0.1")]
    advertise_host: String,
    /// Send debug.reset before each program (needed for searches to agree).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    reset: bool,
    /// Include promise.search, task.search and schedule.search (needs --reset).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    searches: bool,
    /// Compare debug.snap with the oracle after each program.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    snap: bool,
    /// Write <out>.ndjson and <out>.history (for lincheck and conccheck) and
    /// <out>.diffs.jsonl.
    #[arg(long)]
    out: Option<String>,
    #[arg(long, default_value_t = 10_000)]
    request_timeout_ms: u64,
}

// ─── talking to the servers ──────────────────────────────────────────────────

enum Answer {
    Definite(Value),
    /// No definite answer: transport error, unreadable body, or a 5xx.
    Ambiguous(String),
}

async fn post(client: &reqwest::Client, url: &str, envelope: &Value) -> Answer {
    let res = client
        .post(url)
        .header("content-type", "application/json")
        .body(envelope.to_string())
        .send()
        .await;
    let res = match res {
        Ok(r) => r,
        Err(e) => return Answer::Ambiguous(format!("transport: {e}")),
    };
    let text = match res.text().await {
        Ok(t) => t,
        Err(e) => return Answer::Ambiguous(format!("body: {e}")),
    };
    let body: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return Answer::Ambiguous(format!("unreadable body: {text:.200}")),
    };
    let status = body["head"]["status"].as_i64().unwrap_or(0);
    if status >= 500 || status == 0 {
        return Answer::Ambiguous(format!("status {status}"));
    }
    Answer::Definite(body)
}

fn envelope(kind: &str, data: &Value, now: i64, corr: &str) -> Value {
    json!({
        "kind": kind,
        "head": { "corrId": corr, "version": PROTOCOL_VERSION, "resonate:debug_time": now },
        "data": data,
    })
}

// ─── the callback listener ───────────────────────────────────────────────────

type Mailbox = Arc<Mutex<Vec<Value>>>;

async fn listen(port: u16, mailbox: Mailbox) -> std::io::Result<u16> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    let bound = listener.local_addr()?.port();
    let app = axum::Router::new().fallback(move |body: String| {
        let mailbox = mailbox.clone();
        async move {
            if let Ok(v) = serde_json::from_str::<Value>(&body) {
                mailbox.lock().unwrap().push(v);
            }
            // Taken, not done: the work it offers is answered through the
            // protocol like any other.
            axum::http::StatusCode::OK
        }
    });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(bound)
}

/// A message, reduced to what two implementations must agree on.
fn message_key(kind: &str, id: &str, detail: &str) -> String {
    format!("{kind} {id} {detail}")
}

fn received_key(m: &Value) -> Option<(String, String)> {
    let kind = m["kind"].as_str()?;
    match kind {
        "execute" => {
            let task = &m["data"]["task"];
            let id = task["id"].as_str()?;
            let v = task["version"].as_i64()?;
            Some((id.to_string(), message_key(kind, id, &v.to_string())))
        }
        "unblock" => {
            let p = &m["data"]["promise"];
            let id = p["id"].as_str()?;
            Some((
                id.to_string(),
                message_key(kind, id, p["state"].as_str().unwrap_or("")),
            ))
        }
        _ => None,
    }
}

fn expected_key(o: &Outgoing) -> String {
    match o {
        Outgoing::Execute {
            task_id, version, ..
        } => message_key("execute", task_id, &version.to_string()),
        Outgoing::Unblock { promise, .. } => {
            let p = serde_json::to_value(promise).unwrap_or(Value::Null);
            message_key("unblock", &promise.id, p["state"].as_str().unwrap_or(""))
        }
    }
}

// ─── comparing ───────────────────────────────────────────────────────────────

/// The oracle's answer to an envelope, as JSON.
fn oracle_apply(o: &mut Oracle, env: &Value) -> Value {
    let req: RequestEnvelope = serde_json::from_value(env.clone()).expect("an envelope we built");
    serde_json::to_value(o.apply(&req)).expect("a response serializes")
}

/// A snapshot reduced to one program's rows: without a reset between
/// programs the server still holds the others', which the oracle never saw.
fn own_rows(snap: &mut Value, names: &Names) {
    let Some(sections) = snap.as_object_mut() else {
        return;
    };
    for rows in sections.values_mut() {
        if let Some(rows) = rows.as_array_mut() {
            rows.retain(|row| {
                ["id", "promiseId", "awaited", "awaiter", "taskId"]
                    .iter()
                    .filter_map(|k| row.get(*k).and_then(|v| v.as_str()))
                    .any(|id| names.owns(id))
            });
        }
    }
}

/// None if the two answers agree; otherwise what differs.
fn disagreement(kind: &str, expected: &Value, actual: &Value, names: &Names) -> Option<String> {
    let es = &expected["head"]["status"];
    let as_ = &actual["head"]["status"];
    let mut e = expected["data"].clone();
    let mut a = actual["data"].clone();
    if kind == "debug.snap" {
        // The server delivers what the oracle holds: its outbox is empty.
        if let Some(m) = e.as_object_mut() {
            m.remove("messages");
        }
        if let Some(m) = a.as_object_mut() {
            m.remove("messages");
        }
        own_rows(&mut e, names);
        own_rows(&mut a, names);
    }
    if es != as_ || e != a {
        Some(format!(
            "expected {es} {}\n    actual   {as_} {}",
            truncate(&e.to_string(), 600),
            truncate(&a.to_string(), 600)
        ))
    } else {
        None
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

/// The kinds both spec checkers decode (spec/work/go/record.go `recordable`).
fn recordable(kind: &str) -> bool {
    matches!(
        kind,
        "promise.create"
            | "promise.get"
            | "promise.settle"
            | "promise.register_callback"
            | "promise.register_listener"
            | "task.get"
            | "task.acquire"
            | "task.suspend"
            | "task.fulfill"
            | "task.create"
            | "task.fence"
            | "task.release"
            | "task.heartbeat"
    )
}

// ─── the run ─────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Stats {
    programs: usize,
    steps: usize,
    ok_steps: usize,
    ambiguous: usize,
    disagreements: usize,
    state_disagreements: usize,
    shapes: HashSet<u64>,
    tried: BTreeMap<String, usize>,
    ok: BTreeMap<String, usize>,
    offers_received: usize,
    offers_taken: usize,
    messages_missing: usize,
    messages_unexpected: usize,
    message_examples: Vec<String>,
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let started = Instant::now();
    let deadline = args
        .deadline_ms
        .map(|ms| started + Duration::from_millis(ms));
    let seed = if args.seed == 0 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1)
    } else {
        args.seed
    };
    let mut rng = fastrand::Rng::with_seed(seed);
    let informed = args.generator != "blind";
    // A trace is only kept, and streamed for the spec checkers, when every
    // operation in it is one they can decode.
    let spec = args.alphabet == "spec";
    let invocation = format!("z{:x}", seed & 0xffff_ffff);

    let mailbox: Mailbox = Arc::new(Mutex::new(Vec::new()));
    let port = match listen(args.listen_port, mailbox.clone()).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("resonate-fuzz: callback listener: {e}");
            std::process::exit(2);
        }
    };
    let worker = format!("http://{}:{port}/", args.advertise_host);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(args.request_timeout_ms))
        .build()
        .expect("an http client");

    let mut skull = Skull::connect();
    skull.catalog();
    println!(
        "resonate-fuzz: {} {} against {} — seed {seed}, worker {worker}{}",
        if args.guided { "guided" } else { "unguided" },
        args.generator,
        args.url.join(","),
        if skull.active() {
            ", streaming to skulld"
        } else {
            ""
        },
    );

    let mut corpus = Corpus::new(args.guided);
    let mut stats = Stats::default();
    let mut rows: Vec<Value> = Vec::new();
    let mut diffs: Vec<Value> = Vec::new();
    // Monotone across programs: the servers' debug clocks only move forward.
    let mut now: i64 = unix_ms().max(1_000_000_000);
    let mut corr = 0u64;

    'programs: for program in 0..args.programs {
        if deadline.is_some_and(|d| Instant::now() + Duration::from_secs(2) > d) {
            break;
        }
        let tape = corpus.next(&mut rng);
        let mut t = Tape::new(&tape);
        let names = Names {
            ns: format!("{invocation}x{program}"),
            worker: worker.clone(),
            searches: args.searches && args.reset && args.alphabet != "spec",
            spec_only: args.alphabet == "spec",
        };
        stats.programs += 1;

        // A clean slate on every server, and in the oracle.
        let mut names_searches = names.searches;
        if args.reset {
            for url in &args.url {
                corr += 1;
                let env = envelope("debug.reset", &json!({}), now, &format!("r{corr}"));
                if !matches!(post(&client, url, &env).await, Answer::Definite(ref b) if b["head"]["status"].as_i64() == Some(200))
                {
                    // Rows another program left could answer a search now.
                    names_searches = false;
                }
            }
        }
        let names = Names {
            searches: names_searches,
            ..names
        };
        let mut o = Oracle::new();
        let mut inbox: Vec<(String, i64)> = Vec::new();
        let mut expected_msgs: HashMap<String, usize> = HashMap::new();
        let mut received_msgs: HashMap<String, usize> = HashMap::new();
        let mut reached: HashSet<u64> = HashSet::new();
        let mut comparable = true;

        let mut step = 0;
        while !t.done() && step < args.steps {
            if deadline.is_some_and(|d| Instant::now() > d) {
                break 'programs;
            }
            // What the server pushed since the last step.
            for m in mailbox.lock().unwrap().drain(..) {
                let Some((id, key)) = received_key(&m) else {
                    continue;
                };
                if !names.owns(&id) {
                    continue; // a program that is over
                }
                *received_msgs.entry(key).or_insert(0) += 1;
                if m["kind"] == "execute" {
                    stats.offers_received += 1;
                    let v = m["data"]["task"]["version"].as_i64().unwrap_or(0);
                    inbox.push((id, v));
                }
            }

            let req = if informed {
                generate::informed(&mut t, &o, &names, &mut inbox, now)
            } else {
                generate::blind(&mut t, &names, now)
            };
            corr += 1;
            let env = envelope(&req.kind, &req.data, now, &format!("f{corr}"));

            // A tick moves every server's clock; anything else goes to one.
            let call = unix_ms() * 1_000_000;
            let answer = if req.kind == "debug.tick" {
                let mut first = None;
                for url in &args.url {
                    let a = post(&client, url, &env).await;
                    if first.is_none() || matches!(a, Answer::Ambiguous(_)) {
                        first = Some(a);
                    }
                }
                first.unwrap()
            } else {
                let url = &args.url[rng.usize(0..args.url.len())];
                post(&client, url, &env).await
            };
            let ret = unix_ms() * 1_000_000;
            step += 1;
            stats.steps += 1;
            *stats.tried.entry(req.kind.clone()).or_insert(0) += 1;

            let actual = match answer {
                Answer::Definite(b) => b,
                Answer::Ambiguous(why) => {
                    stats.ambiguous += 1;
                    if std::env::var_os("FUZZ_VERBOSE").is_some() {
                        println!(
                            "AMBIGUOUS program {program} step {step} ({}): {why}\n  request  {env}",
                            req.kind
                        );
                    }
                    skull.check(
                        &skull::AMBIGUOUS,
                        true,
                        json!({ "kind": req.kind, "why": why }),
                    );
                    if spec && recordable(&req.kind) {
                        let row = json!({
                            "invocation": invocation, "kind": req.kind, "now": now,
                            "req": req.data,
                            "res": { "kind": req.kind, "head": { "status": 500 },
                                     "data": "ambiguous: no definite answer" },
                            "client": "fuzz", "call": call, "return": i64::MAX, "ambiguous": true,
                        });
                        skull.trace(&row);
                        rows.push(row);
                    }
                    // The oracle cannot know whether it took effect.
                    comparable = false;
                    break;
                }
            };
            let status = actual["head"]["status"].as_i64().unwrap_or(0) as i32;
            if (200..300).contains(&status) {
                stats.ok_steps += 1;
                *stats.ok.entry(req.kind.clone()).or_insert(0) += 1;
                if req.from_inbox {
                    stats.offers_taken += 1;
                    skull.check(&skull::OFFER_TAKEN, true, json!({ "task": req.data["id"] }));
                }
            }
            if spec && recordable(&req.kind) {
                let row = json!({
                    "invocation": invocation, "kind": req.kind, "now": now, "req": req.data,
                    "res": actual, "client": "fuzz", "call": call, "return": ret,
                });
                skull.trace(&row);
                rows.push(row);
            }

            let expected = oracle_apply(&mut o, &env);
            for m in o.take_emitted() {
                *expected_msgs.entry(expected_key(&m)).or_insert(0) += 1;
            }
            if let Some(diff) = disagreement(&req.kind, &expected, &actual, &names) {
                stats.disagreements += 1;
                println!(
                    "\nDISAGREE program {program} step {step} ({}):\n  request  {env}\n    {diff}",
                    req.kind
                );
                let d = json!({ "program": program, "step": step, "request": env,
                                "expected": expected, "actual": actual });
                skull.check(&skull::AGREES, false, d.clone());
                diffs.push(d);
                // The two have parted ways; nothing after this compares.
                comparable = false;
                break;
            }
            skull.check(&skull::AGREES, true, Value::Null);

            let sh = guide::shape(&o);
            stats.shapes.insert(sh);
            reached.insert(guide::signature(generate::op_index(&req.kind), status, sh));
            now = now.max(req.next_now);
        }

        if comparable && args.snap {
            corr += 1;
            let env = envelope("debug.snap", &json!({}), now, &format!("s{corr}"));
            let url = &args.url[rng.usize(0..args.url.len())];
            if let Answer::Definite(actual) = post(&client, url, &env).await {
                let expected = oracle_apply(&mut o, &env);
                if let Some(diff) = disagreement("debug.snap", &expected, &actual, &names) {
                    stats.state_disagreements += 1;
                    println!("\nSTATE DISAGREES after program {program}:\n    {diff}");
                    let d = json!({ "program": program, "snap": true,
                                    "expected": expected, "actual": actual });
                    skull.check(&skull::STATE_AGREES, false, d.clone());
                    diffs.push(d);
                } else {
                    skull.check(&skull::STATE_AGREES, true, Value::Null);
                }
            }
        }

        // Messages still on their way, then the comparison.
        if comparable {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for m in mailbox.lock().unwrap().drain(..) {
                if let Some((id, key)) = received_key(&m) {
                    if names.owns(&id) {
                        *received_msgs.entry(key).or_insert(0) += 1;
                    }
                }
            }
            for (k, n) in &expected_msgs {
                let missing = n.saturating_sub(*received_msgs.get(k).unwrap_or(&0));
                stats.messages_missing += missing;
                if missing > 0 && stats.message_examples.len() < 6 {
                    stats
                        .message_examples
                        .push(format!("never came: {k} (x{missing})"));
                }
            }
            for (k, n) in &received_msgs {
                let extra = n.saturating_sub(*expected_msgs.get(k).unwrap_or(&0));
                stats.messages_unexpected += extra;
                if extra > 0 && stats.message_examples.len() < 6 {
                    stats
                        .message_examples
                        .push(format!("unexpected: {k} (x{extra})"));
                }
            }
        }

        corpus.observe(&mut rng, tape, &reached);
    }

    skull.check(&skull::FINISHED, true, Value::Null);
    report(&args, &stats, &corpus, started.elapsed());

    if let Some(out) = &args.out {
        write_traces(out, &rows, &diffs);
    }
    if stats.disagreements + stats.state_disagreements > 0 {
        std::process::exit(1);
    }
    if stats.ok_steps == 0 {
        std::process::exit(2);
    }
}

fn report(args: &Args, s: &Stats, corpus: &Corpus, elapsed: Duration) {
    println!(
        "\n{} programs, {} requests in {:.1}s — {:.1}% answered 2xx",
        s.programs,
        s.steps,
        elapsed.as_secs_f64(),
        if s.steps == 0 {
            0.0
        } else {
            s.ok_steps as f64 / s.steps as f64 * 100.0
        }
    );
    println!(
        "  reach        {} signatures, {} store shapes, corpus {}",
        corpus.seen.len(),
        s.shapes.len(),
        corpus.tapes.len()
    );
    let alphabet: Vec<&str> = generate::OPS
        .iter()
        .copied()
        .filter(|o| args.alphabet != "spec" || generate::in_spec_alphabet(o))
        .collect();
    let covered = alphabet.iter().filter(|o| s.ok.contains_key(**o)).count();
    print!("  2xx reached  {covered}/{} operations", alphabet.len());
    let missed: Vec<&str> = generate::OPS
        .iter()
        .copied()
        .filter(|o| !s.ok.contains_key(*o) && (args.reset || !o.ends_with(".search")))
        .filter(|o| args.alphabet != "spec" || generate::in_spec_alphabet(o))
        .collect();
    if !missed.is_empty() {
        print!("; never: {}", missed.join(" "));
    }
    println!();
    println!(
        "  messages     {} offers received, {} acquired from the inbox; {} the oracle expected never came, {} came unexpected",
        s.offers_received, s.offers_taken, s.messages_missing, s.messages_unexpected
    );
    for e in &s.message_examples {
        println!("               {e}");
    }
    println!(
        "  ambiguous    {} programs ended on an answer that was not definite",
        s.ambiguous
    );
    println!(
        "  verdict      {} answers and {} states disagreed with the oracle — {}",
        s.disagreements,
        s.state_disagreements,
        if s.disagreements + s.state_disagreements > 0 {
            "DISAGREED"
        } else if s.ok_steps == 0 {
            // Nothing answered: agreement with nothing is not a verdict.
            "NO VERDICT (no request got a definite answer)"
        } else {
            "AGREED"
        }
    );
}

fn write_traces(out: &str, rows: &[Value], diffs: &[Value]) {
    use std::io::Write;
    let mut sorted: Vec<&Value> = rows.iter().collect();
    sorted.sort_by_key(|r| {
        (
            r["now"].as_i64().unwrap_or(0),
            r["return"].as_i64().unwrap_or(0),
        )
    });
    let mut nd = std::fs::File::create(format!("{out}.ndjson")).expect("ndjson");
    let mut hi = std::fs::File::create(format!("{out}.history")).expect("history");
    for r in sorted {
        let base = json!({ "kind": r["kind"], "now": r["now"], "req": r["req"], "res": r["res"] });
        writeln!(nd, "{base}").ok();
        let mut h = base;
        h["call"] = r["call"].clone();
        h["return"] = r["return"].clone();
        h["client"] = json!(0);
        writeln!(hi, "{h}").ok();
    }
    let mut df = std::fs::File::create(format!("{out}.diffs.jsonl")).expect("diffs");
    for d in diffs {
        writeln!(df, "{d}").ok();
    }
}
