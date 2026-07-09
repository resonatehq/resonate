// Matched workloads: the same computation expressed in each engine's idiom and
// registered under identical names. Children pin an explicit retry policy
// (`Never`) on both sides so the engines' differing in-engine defaults
// (generator leaf=Exponential, async=Never — divergence #1) don't leak into the
// stored promise param and produce a spurious diff.

import type { Context as AsyncContext, Info as AsyncInfo } from "../../src/async/index.js";
import type { Context as GenContext } from "../../src/context.js";
import { Never } from "../../src/retries.js";
import type { MatchedWorkload } from "./harness.js";

type Gen = Generator<any, any, any>;

const never = (ctx: GenContext | AsyncContext) => ctx.options({ retryPolicy: new Never() });

// --- 1. Fibonacci via local run -------------------------------------------
// Exercises child id generation, dedup/replay, and value propagation. All
// calls are local (`ctx.run`), so a single root task fans out into a tree of
// resolved durable promises with no remote suspension.

function* fibGen(ctx: GenContext, n: number): Gen {
  if (n <= 1) return n;
  const a = yield* ctx.run<number>("fib", n - 1, never(ctx));
  const b = yield* ctx.run<number>("fib", n - 2, never(ctx));
  return a + b;
}

async function fibAsync(ctx: AsyncContext, n: number): Promise<number> {
  if (n <= 1) return n;
  const a = await ctx.run<number>("fib", n - 1, never(ctx));
  const b = await ctx.run<number>("fib", n - 2, never(ctx));
  return a + b;
}

export const fibonacci: MatchedWorkload = {
  name: "fibonacci (local run)",
  rootName: "fib",
  rootId: "fib",
  args: [6],
  gen: { fib: fibGen },
  async: { fib: fibAsync },
};

// --- 2. Fan-out / fan-in ---------------------------------------------------
// Start all children eagerly, then join. Exercises the creation sequencer
// (children must be created in source order) and the joined value.

function* fanoutGen(ctx: GenContext, items: number[]): Gen {
  // Start all children eagerly (collect futures), then join — concurrent fan-out.
  const futures = [];
  for (const n of items) futures.push(yield* ctx.beginRun<number>("sq", n, never(ctx)));
  const out: number[] = [];
  for (const f of futures) out.push(yield* f);
  return out;
}

async function fanoutAsync(ctx: AsyncContext, items: number[]): Promise<number[]> {
  const handles = items.map((n) => ctx.run<number>("sq", n, never(ctx)));
  return Promise.all(handles);
}

function* sqGen(_ctx: GenContext, n: number): Gen {
  return n * n;
}
async function sqAsync(_info: AsyncInfo, n: number): Promise<number> {
  return n * n;
}

export const fanout: MatchedWorkload = {
  name: "fan-out / fan-in",
  rootName: "fanout",
  rootId: "fanout",
  args: [[1, 2, 3, 4]],
  gen: { fanout: fanoutGen, sq: sqGen },
  async: { fanout: fanoutAsync, sq: sqAsync },
};

// --- 3a. Error propagation, caught ----------------------------------------
// A leaf throws (retry pinned to Never), the parent catches and returns a
// sentinel. The child promise is rejected; the root resolves.

function* caughtGen(ctx: GenContext): Gen {
  try {
    yield* ctx.run<void>("boom", never(ctx));
    return "no-throw";
  } catch (e) {
    return `caught:${(e as Error).message}`;
  }
}
async function caughtAsync(ctx: AsyncContext): Promise<string> {
  try {
    await ctx.run<void>("boom", never(ctx));
    return "no-throw";
  } catch (e) {
    return `caught:${(e as Error).message}`;
  }
}
function* boomGen(_ctx: GenContext): Gen {
  throw new Error("kaboom");
}
async function boomAsync(_info: AsyncInfo): Promise<void> {
  throw new Error("kaboom");
}

export const errorCaught: MatchedWorkload = {
  name: "error propagation (caught)",
  rootName: "caught",
  rootId: "caught",
  gen: { caught: caughtGen, boom: boomGen },
  async: { caught: caughtAsync, boom: boomAsync },
};

// --- 3b. Error propagation, uncaught --------------------------------------
// The parent does NOT catch, so the root rejects with the leaf's error.

function* uncaughtGen(ctx: GenContext): Gen {
  yield* ctx.run<void>("boom", never(ctx));
}
async function uncaughtAsync(ctx: AsyncContext): Promise<void> {
  await ctx.run<void>("boom", never(ctx));
}

export const errorUncaught: MatchedWorkload = {
  name: "error propagation (uncaught)",
  rootName: "uncaught",
  rootId: "uncaught",
  gen: { uncaught: uncaughtGen, boom: boomGen },
  async: { uncaught: uncaughtAsync, boom: boomAsync },
};

// --- 4. Sleep --------------------------------------------------------------
// A durable timer promise is created and resolved on the server tick, then the
// workflow proceeds. `result()` only resolves after the timer fires, so the
// snapshot taken afterward sees the resolved timer.

function* sleepGen(ctx: GenContext): Gen {
  yield* ctx.sleep(1);
  return "awake";
}
async function sleepAsync(ctx: AsyncContext): Promise<string> {
  await ctx.sleep(1);
  return "awake";
}

export const sleep: MatchedWorkload = {
  name: "sleep",
  rootName: "sleeper",
  rootId: "sleeper",
  gen: { sleeper: sleepGen },
  async: { sleeper: sleepAsync },
};

// --- 5. Detached (known divergence #4) -------------------------------------
// Parent spawns an independent child and returns its id without awaiting. The
// detached id is hashed from the prefix id in the generator engine and the
// origin id in the async engine. For a top-level parent those coincide, so this
// may pass; it is the regression test that will catch the divergence once a
// nested-detached variant is added.

function* detachedParentGen(ctx: GenContext): Gen {
  const handle = yield* ctx.detached<number>("dchild", 5, never(ctx));
  return handle.id;
}
async function detachedParentAsync(ctx: AsyncContext): Promise<string> {
  const handle = await ctx.detached("dchild", 5, never(ctx));
  return handle.id;
}
function* dchildGen(_ctx: GenContext, n: number): Gen {
  return n * 10;
}
async function dchildAsync(_info: AsyncInfo, n: number): Promise<number> {
  return n * 10;
}

export const detached: MatchedWorkload = {
  name: "detached",
  rootName: "dparent",
  rootId: "dparent",
  gen: { dparent: detachedParentGen, dchild: dchildGen },
  async: { dparent: detachedParentAsync, dchild: dchildAsync },
  drive: async (api, outcome) => {
    const o = await outcome;
    if (o.kind === "resolved" && typeof o.value === "string") {
      await (await api.get(o.value)).result();
    }
  },
};

// --- 6. Human-in-the-loop (DPC) -------------------------------------------
// The workflow awaits a bare durable promise that is resolved out of band. The
// root's child `<root>.0` is the latent promise.

function* dpcGen(ctx: GenContext): Gen {
  const p = yield* ctx.promise<string>();
  return yield* p;
}
async function dpcAsync(ctx: AsyncContext): Promise<string> {
  return ctx.promise<string>();
}

export const humanInTheLoop: MatchedWorkload = {
  name: "human-in-the-loop (DPC)",
  rootName: "dpc",
  rootId: "dpc",
  gen: { dpc: dpcGen },
  async: { dpc: dpcAsync },
  drive: async (api) => {
    await api.resolvePromise("dpc.0", "signal");
  },
};
