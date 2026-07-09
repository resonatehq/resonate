// Differential simulation: run the SAME (seed, workload, fault schedule) on both
// engines under the deterministic simulator and assert they converge.
//
// Why not compare messages byte-for-byte? The async engine creates promises
// eagerly while the generator engine creates them lazily, so the two emit
// different message sequences even on a clean run. Fault injection is keyed on
// messages, so the same seed hits *different* logical messages on each engine.
// The invariant that survives is the FINAL durable state: at quiescence the two
// engines must agree on the canonical promise/task/callback store and the root
// outcome, and each run must independently satisfy a set of structural
// invariants. Absolute time is excluded (the engines emit different numbers of
// ticks → different clock reads); it is asserted per-run instead.

import { randomUUID } from "node:crypto";
import { Command } from "commander";
import { AsyncCore } from "../../src/async/core.js";
import { StepClock } from "../../src/clock.js";
import { Codec } from "../../src/codec.js";
import type { Server } from "../../src/network/local.js";
import type { Request } from "../../src/network/types.js";
import { Registry } from "../../src/registry.js";
import { VERSION } from "../../src/util.js";
import { type CanonSnapshot, canonicalize, normalizeErrors } from "../../tests/equivalence/oracle.js";
import { ServerProcess } from "./server.js";
import { type DeliveryOptions, Message, Random, Simulator, unicast } from "./simulator.js";
import { type EngineFactory, genEngine, WorkerProcess } from "./worker.js";
import { workloads } from "./workloads.js";

const asyncEngine: EngineFactory = (deps) => new AsyncCore(deps);

const codec = new Codec();

// The runner's own output must survive the per-run console suppression below.
const report = console.log.bind(console);

// Both engines guard onMessage with `util.assert(msg.kind === "execute")`, and
// `util.assert` calls `process.exit(1)`. Message corruption (charFlipProb) can
// mangle a message's `kind` past the worker's JSON-only validation, tripping
// that assert. Rather than let a single corrupted message kill the whole fuzz
// loop, we convert process.exit into a thrown SimAbort during a run: a
// worker-path assert is then swallowed like a dropped message (the server
// re-dispatches), and a server-/runner-path assert surfaces as an aborted run.
class SimAbort extends Error {
  constructor(public code?: number) {
    super(`simulation aborted via process.exit(${code})`);
  }
}

export interface Fault extends DeliveryOptions {
  charFlipProb?: number;
}

export interface DifferentialOptions {
  seed: number;
  funcName: string;
  arg: number;
  steps: number;
  workers: number;
  fault: Fault;
}

interface EngineResult {
  settled: boolean;
  root: { state: string; value: unknown } | undefined;
  canonical: CanonSnapshot;
  /** Per-run structural invariant violations (engine-independent properties). */
  violations: string[];
}

function apply(server: Server, time: number, req: any): any {
  return server.apply(time, req);
}

function rootState(server: Server, time: number, id: string): string | undefined {
  const res = apply(server, time, { kind: "promise.get", head: { corrId: "q", version: VERSION }, data: { id } });
  if (res.response.head.status !== 200) return undefined;
  return res.response.data.promise.state;
}

type SnapData = Parameters<typeof canonicalize>[0];

function snapshotServer(server: Server, time: number): SnapData {
  const res = apply(server, time, { kind: "debug.snap", head: { corrId: "snap", version: VERSION }, data: {} });
  if (res.response.head.status !== 200) throw new Error(`debug.snap failed: ${res.response.head.status}`);
  return res.response.data as SnapData;
}

function buildRegistry(engine: "gen" | "async"): Registry {
  const registry = new Registry();
  const set = engine === "gen" ? workloads.gen : workloads.async;
  for (const [name, fn] of Object.entries(set)) registry.add(fn, name, 1);
  return registry;
}

/** Engine-independent properties every healthy run must satisfy. */
function structuralInvariants(snap: ReturnType<typeof snapshotServer>, rootId: string): string[] {
  const out: string[] = [];
  const root = snap.promises.find((p) => p.id === rootId);
  if (!root) {
    out.push(`root ${rootId} missing`);
    return out;
  }
  // The DST root is created externally (like main.ts) with only a target tag, so
  // it carries no lineage tags — only assert consistency if origin is present.
  const origin = root.tags["resonate:origin"];
  if (origin !== undefined && origin !== rootId) {
    out.push(`root origin tag ${origin} != ${rootId}`);
  }
  for (const t of snap.tasks) {
    if (t.state === "acquired") out.push(`task ${t.id} left acquired at quiescence`);
  }
  const ids = new Set(snap.promises.map((p) => p.id));
  for (const p of snap.promises) {
    if (p.id === rootId) continue;
    const parent = p.tags["resonate:parent"];
    if (parent && !ids.has(parent)) out.push(`promise ${p.id} parent ${parent} not in store`);
  }
  return out;
}

async function runEngine(engine: "gen" | "async", opts: DifferentialOptions): Promise<EngineResult> {
  // The simulator's Process.log writes to console.log every tick, and the worker
  // logger reports the (expected, handled) timeouts/corruption that faults induce
  // to console.error — far too noisy for a fuzz loop. Silence both for the run.
  const orig = { log: console.log, error: console.error, warn: console.warn, exit: process.exit };
  console.log = () => {};
  console.error = () => {};
  console.warn = () => {};
  process.exit = ((code?: number) => {
    throw new SimAbort(code);
  }) as typeof process.exit;
  try {
    return await runEngineInner(engine, opts);
  } catch (e) {
    if (e instanceof SimAbort) {
      return {
        settled: false,
        root: undefined,
        canonical: { promises: [], tasks: [], callbacks: [] },
        violations: [e.message],
      };
    }
    throw e;
  } finally {
    console.log = orig.log;
    console.error = orig.error;
    console.warn = orig.warn;
    process.exit = orig.exit;
  }
}

async function runEngineInner(engine: "gen" | "async", opts: DifferentialOptions): Promise<EngineResult> {
  const { seed, funcName, arg, steps, workers, fault } = opts;
  const rnd = new Random(seed);
  const sim = new Simulator(rnd, {
    dropProb: fault.dropProb ?? 0,
    randomDelay: fault.randomDelay ?? 0,
    duplProb: fault.duplProb ?? 0,
    deactivateProb: fault.deactivateProb ?? 0,
    activateProb: fault.activateProb ?? 0,
  });

  const clock = new StepClock();
  const registry = buildRegistry(engine);
  const factory = engine === "gen" ? genEngine : asyncEngine;

  const server = new ServerProcess(clock, "server");
  sim.register(server);

  // Drive server-side time forward so timeouts/retries can fire (mirrors main.ts).
  sim.repeat(1, () => {
    sim.send(
      new Message(
        unicast("environment"),
        unicast("server"),
        { kind: "debug.tick", head: { corrId: randomUUID(), version: VERSION }, data: { time: clock.time } },
        { requ: true },
      ),
    );
  });

  for (let i = 1; i <= workers; i++) {
    sim.register(
      new WorkerProcess(
        rnd,
        clock,
        registry,
        { charFlipProb: fault.charFlipProb ?? 0 },
        `worker-${i}`,
        "default",
        factory,
      ),
    );
  }

  const rootId = `${funcName}-0`;
  const rootCreate = (): Message<Request> =>
    new Message<Request>(
      unicast("environment"),
      unicast("server"),
      {
        kind: "promise.create",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          id: rootId,
          timeoutAt: Number.MAX_SAFE_INTEGER,
          tags: { "resonate:target": "sim://any@default" },
          param: codec.encode({ func: funcName, args: [arg], version: 1 }),
        },
      },
      { requ: true },
    );

  // Like a real invoker, keep (idempotently) creating the root until it lands —
  // otherwise a single dropped initial message dooms the whole run.
  sim.send(rootCreate());
  sim.repeat(200, () => {
    if (rootState(server.server, clock.time, rootId) === undefined) sim.send(rootCreate());
  });

  const settled = await sim.execUntil(steps, () => {
    const s = rootState(server.server, clock.time, rootId);
    return s !== undefined && s !== "pending";
  });

  const snap = snapshotServer(server.server, clock.time);
  const root = snap.promises.find((p) => p.id === rootId);

  return {
    settled,
    root: root ? { state: root.state, value: normalizeErrors(codec.decodePromise(root).value?.data) } : undefined,
    canonical: canonicalize(snap, { mode: "stepclock" }),
    violations: structuralInvariants(snap, rootId),
  };
}

export interface DifferentialResult {
  seed: number;
  verdict: "pass" | "fail" | "inconclusive";
  reasons: string[];
}

export async function runDifferential(opts: DifferentialOptions): Promise<DifferentialResult> {
  const gen = await runEngine("gen", opts);
  const async = await runEngine("async", opts);
  const reasons: string[] = [];

  if (!gen.settled || !async.settled) {
    return {
      seed: opts.seed,
      verdict: "inconclusive",
      reasons: [`did not quiesce within ${opts.steps} steps (gen=${gen.settled}, async=${async.settled})`],
    };
  }

  if (gen.violations.length) reasons.push(`gen invariants: ${gen.violations.join("; ")}`);
  if (async.violations.length) reasons.push(`async invariants: ${async.violations.join("; ")}`);

  if (JSON.stringify(gen.root) !== JSON.stringify(async.root)) {
    reasons.push(`root outcome differs: gen=${JSON.stringify(gen.root)} async=${JSON.stringify(async.root)}`);
  }

  if (JSON.stringify(gen.canonical) !== JSON.stringify(async.canonical)) {
    reasons.push(canonicalDiff(gen.canonical, async.canonical));
  }

  return { seed: opts.seed, verdict: reasons.length ? "fail" : "pass", reasons };
}

/** Compact human-readable diff of two canonical snapshots. */
function canonicalDiff(gen: CanonSnapshot, async: CanonSnapshot): string {
  const lines: string[] = ["canonical snapshot differs:"];
  const cmp = (label: string, a: unknown[], b: unknown[]) => {
    if (a.length !== b.length) lines.push(`  ${label}: gen=${a.length} async=${b.length}`);
    const max = Math.max(a.length, b.length);
    for (let i = 0; i < max; i++) {
      const sa = JSON.stringify(a[i]);
      const sb = JSON.stringify(b[i]);
      if (sa !== sb) {
        lines.push(`  ${label}[${i}] gen: ${sa}`);
        lines.push(`  ${label}[${i}] async: ${sb}`);
      }
    }
  };
  cmp("promises", gen.promises, async.promises);
  cmp("tasks", gen.tasks, async.tasks);
  cmp("callbacks", gen.callbacks, async.callbacks);
  return lines.join("\n");
}

// --- CLI -------------------------------------------------------------------

function prob(value: string): number {
  const n = Number.parseFloat(value);
  if (Number.isNaN(n) || n < 0 || n > 1) throw new Error(`must be 0-1: ${value}`);
  return n;
}
function int(value: string): number {
  const n = Number.parseInt(value, 10);
  if (Number.isNaN(n)) throw new Error(`invalid integer: ${value}`);
  return n;
}

const program = new Command();
program
  .name("differential")
  .description("Run a workload on both engines under the same fault schedule and compare durable state")
  .option("--seed <n>", "starting seed", int, 0)
  .option("--seeds <n>", "number of consecutive seeds to run", int, 50)
  .option("--func <name>", `workload (${Object.keys(workloads.gen).join(", ")})`, "fibRfc")
  .option("--arg <n>", "argument passed to the workload", int, 6)
  .option("--steps <n>", "max simulation steps", int, 500_000)
  .option("--workers <n>", "number of worker processes", int, 3)
  .option("--dropProb <p>", "message drop probability", prob, 0.05)
  .option("--duplProb <p>", "message duplicate probability", prob, 0.05)
  .option("--randomDelay <p>", "message delay probability", prob, 0.05)
  // Corruption is off by default: it can mangle a message's `kind`, which trips
  // the shared `util.assert(msg.kind === "execute")` in BOTH engines' onMessage
  // (neutralized here as a dropped message). Enable to stress that path.
  .option("--charFlipProb <p>", "response corruption probability", prob, 0)
  .option("--deactivateProb <p>", "worker crash probability", prob, 0.005)
  .option("--activateProb <p>", "worker recovery probability", prob, 0.5);

export async function main(argv: string[]): Promise<void> {
  program.parse(argv);
  const o = program.opts();

  if (!(o.func in workloads.gen)) {
    throw new Error(`unknown func '${o.func}'. choices: ${Object.keys(workloads.gen).join(", ")}`);
  }

  const fault: Fault = {
    dropProb: o.dropProb,
    duplProb: o.duplProb,
    randomDelay: o.randomDelay,
    charFlipProb: o.charFlipProb,
    deactivateProb: o.deactivateProb,
    activateProb: o.activateProb,
  };

  let pass = 0;
  let fail = 0;
  let inconclusive = 0;

  for (let i = 0; i < o.seeds; i++) {
    const seed = o.seed + i;
    const result = await runDifferential({
      seed,
      funcName: o.func,
      arg: o.arg,
      steps: o.steps,
      workers: o.workers,
      fault,
    });

    if (result.verdict === "pass") {
      pass++;
    } else if (result.verdict === "inconclusive") {
      inconclusive++;
      report(`seed ${seed}: INCONCLUSIVE — ${result.reasons.join("; ")}`);
    } else {
      fail++;
      report(`seed ${seed}: FAIL`);
      for (const r of result.reasons) report(r);
    }
  }

  report(
    `\n${o.func}(${o.arg}) over seeds ${o.seed}..${o.seed + o.seeds - 1}: ${pass} pass, ${fail} fail, ${inconclusive} inconclusive`,
  );
  if (fail > 0) process.exit(1);
}

// Run only when invoked directly (not when imported by a test).
if (import.meta.url === `file://${process.argv[1]}`) {
  main(process.argv).catch((e) => {
    process.stderr.write(`${e instanceof Error ? e.stack : String(e)}\n`);
    process.exit(1);
  });
}
