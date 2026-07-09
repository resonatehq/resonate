// Layer A equivalence harness.
//
// Runs a single matched workload against BOTH engines — each on its own fresh
// in-memory server — then snapshots the durable state and the root outcome of
// each and diffs them. The generator engine is the reference; we assert the
// async engine matches it.
//
// A matched workload registers the SAME function names on both engines (so the
// server-stored `func` and generated ids line up) with bodies that express the
// same computation in each engine's idiom (`yield* ctx.run` vs `await ctx.run`).

import { AsyncResonate } from "../../src/async/resonate.js";
import { Codec } from "../../src/codec.js";
import { LocalNetwork } from "../../src/network/local.js";
import { Resonate } from "../../src/resonate.js";
import { type CanonSnapshot, canonicalize, captureOutcome, type Outcome, rawSnapshot } from "./oracle.js";

const codec = new Codec();

type Handle = { id: string; result(): Promise<unknown> };

/** The engine-agnostic surface a workload's `drive` hook may use. The generator
 * engine starts a root via `beginRun`; the async engine via `run`. Both return
 * the identical handle shape. */
export interface EngineApi {
  beginRoot(id: string, name: string, args: unknown[]): Promise<Handle>;
  get(id: string): Promise<Handle>;
  resolvePromise(id: string, value: unknown): Promise<void>;
}

type AnyFn = (...args: any[]) => any;

export interface MatchedWorkload {
  /** Test label. */
  name: string;
  /** Registered name of the entry function (must exist in both `gen` and `async`). */
  rootName: string;
  /** Root promise id. */
  rootId: string;
  /** Args passed to the root. */
  args?: unknown[];
  /** name -> generator function (workflows) or plain/async function (leaves). */
  gen: Record<string, AnyFn>;
  /** name -> async function. */
  async: Record<string, AnyFn>;
  /** Optional out-of-band driver: resolve a DPC, await a detached child, etc.
   * Runs CONCURRENTLY with awaiting the root (a human-in-the-loop workflow only
   * settles once `drive` resolves its external promise), and is given the root
   * outcome as a promise for cases that need the resolved value (e.g. awaiting a
   * detached child by the id the root returns). Completes before the snapshot. */
  drive?: (api: EngineApi, outcome: Promise<Outcome>) => Promise<void>;
}

export interface EngineRun {
  outcome: Outcome;
  snapshot: CanonSnapshot;
}

const drain = async (times: number) => {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
};

/** Resolving a DPC races the workflow pass that creates the latent promise, so
 * the id may not exist yet — retry briefly until the create lands. */
async function resolveWithRetry(fn: () => Promise<unknown>): Promise<void> {
  for (let i = 0; i < 50; i++) {
    try {
      await fn();
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 20));
    }
  }
  await fn();
}

/** Snapshots once the server has gone quiet: after the root subtree settles
 * there may still be in-flight cleanup messages (dispatched via setTimeout(0)),
 * so we drain until two consecutive canonical snapshots agree. */
async function stableSnapshot(network: LocalNetwork): Promise<CanonSnapshot> {
  await drain(10);
  let prev = canonicalize(await rawSnapshot(network), { mode: "wallclock" });
  for (let i = 0; i < 30; i++) {
    await drain(5);
    const cur = canonicalize(await rawSnapshot(network), { mode: "wallclock" });
    if (JSON.stringify(cur) === JSON.stringify(prev)) return cur;
    prev = cur;
  }
  return prev;
}

/** Forces a function's JS `.name` to equal its registered name. Local calls
 * store `func.name` in the durable promise param (see context.ts), so without
 * this the two engines would differ purely because the generator and async
 * function objects have different JS names (e.g. `fibGen` vs `fibAsync`). */
function named(name: string, fn: AnyFn): AnyFn {
  Object.defineProperty(fn, "name", { value: name, configurable: true });
  return fn;
}

// A single shared pid keeps the `resonate:target` anycast (which embeds the pid)
// identical across the two engine runs.
const PID = "eq";

// Long enough that nothing times out mid-test, but not so large it overflows the
// heartbeat interval timer (passing a custom network enables the heartbeat).
const TTL = 60 * 60 * 1000;

async function runOne(engine: "gen" | "async", w: MatchedWorkload): Promise<EngineRun> {
  const network = new LocalNetwork({ pid: PID, group: "default" });

  let api: EngineApi;
  let stop: () => Promise<void>;

  if (engine === "gen") {
    const r = new Resonate({ network, ttl: TTL });
    for (const [name, fn] of Object.entries(w.gen)) r.register(name, named(name, fn));
    api = {
      beginRoot: (id, name, args) => r.beginRun(id, name, ...args),
      get: (id) => r.get(id),
      resolvePromise: (id, value) => resolveWithRetry(() => r.promises.resolve(id, { data: codec.encode(value).data })),
    };
    stop = () => r.stop();
  } else {
    const r = new AsyncResonate({ network, ttl: TTL });
    for (const [name, fn] of Object.entries(w.async)) r.register(name, named(name, fn));
    api = {
      beginRoot: (id, name, args) => r.run(id, name, ...args),
      get: (id) => r.get(id),
      resolvePromise: (id, value) => resolveWithRetry(() => r.promises.resolve(id, { data: codec.encode(value).data })),
    };
    stop = () => r.stop();
  }

  try {
    const root = await api.beginRoot(w.rootId, w.rootName, w.args ?? []);
    const outcomePromise = captureOutcome(root.result());
    const drivePromise = w.drive ? w.drive(api, outcomePromise) : Promise.resolve();
    const [outcome] = await Promise.all([outcomePromise, drivePromise]);
    const snapshot = await stableSnapshot(network);
    return { outcome, snapshot };
  } finally {
    await stop();
  }
}

/** Runs the workload on both engines (generator first, as the reference). */
export async function runEngines(w: MatchedWorkload): Promise<{ gen: EngineRun; async: EngineRun }> {
  const gen = await runOne("gen", w);
  const async = await runOne("async", w);
  return { gen, async };
}

/** Asserts the async engine produced the same outcome and durable state as the
 * generator engine. */
export async function expectEquivalent(w: MatchedWorkload): Promise<void> {
  const { gen, async } = await runEngines(w);
  expect(async.outcome).toEqual(gen.outcome);
  expect(async.snapshot).toEqual(gen.snapshot);
}
