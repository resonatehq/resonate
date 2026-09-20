// Matched workloads for the differential simulation: the same computation in
// each engine's idiom, registered under identical names so the server-stored
// `func` and the generated child ids line up across engines.
//
// Only call-and-await forms are included (`ctx.run`/`ctx.rpc` + await). The
// `begin*` (LFI/RFI) forms have no async analog and stay generator-only in
// sim/main.ts — they are excluded from cross-engine comparison.
//
// Children pin `Never` so the engines' differing in-engine retry defaults don't
// leak into the stored promise param. Network-level faults (drop/dup/corrupt)
// are recovered by the transport + server task retry, independent of this.

import type { Context as AsyncContext } from "../../src/async/index.js";
import type { Context as GenContext } from "../../src/context.js";
import { Never } from "../../src/retries.js";

type Gen = Generator<any, any, any>;
type AnyFn = (...args: any[]) => any;

const never = (ctx: GenContext | AsyncContext) => ctx.options({ retryPolicy: new Never() });

// --- fibLfc: recursive fib via local run ----------------------------------
function* fibLfcGen(ctx: GenContext, n: number): Gen {
  if (n <= 1) return n;
  const a = yield* ctx.run<number>("fibLfc", n - 1, never(ctx));
  const b = yield* ctx.run<number>("fibLfc", n - 2, never(ctx));
  return a + b;
}
async function fibLfcAsync(ctx: AsyncContext, n: number): Promise<number> {
  if (n <= 1) return n;
  const a = await ctx.run<number>("fibLfc", n - 1, never(ctx));
  const b = await ctx.run<number>("fibLfc", n - 2, never(ctx));
  return a + b;
}

// --- fibRfc: recursive fib via remote rpc (suspends + resumes) -------------
function* fibRfcGen(ctx: GenContext, n: number): Gen {
  if (n <= 1) return n;
  const a = yield* ctx.rpc<number>("fibRfc", n - 1, never(ctx));
  const b = yield* ctx.rpc<number>("fibRfc", n - 2, never(ctx));
  return a + b;
}
async function fibRfcAsync(ctx: AsyncContext, n: number): Promise<number> {
  if (n <= 1) return n;
  const a = await ctx.rpc<number>("fibRfc", n - 1, never(ctx));
  const b = await ctx.rpc<number>("fibRfc", n - 2, never(ctx));
  return a + b;
}

export interface MatchedSet {
  gen: Record<string, AnyFn>;
  async: Record<string, AnyFn>;
}

/** name -> { gen, async }. Names are forced onto the function objects so local
 * calls (which store `func.name`) agree across engines. */
export const workloads: MatchedSet = {
  gen: { fibLfc: fibLfcGen, fibRfc: fibRfcGen },
  async: { fibLfc: fibLfcAsync, fibRfc: fibRfcAsync },
};

for (const set of [workloads.gen, workloads.async]) {
  for (const [name, fn] of Object.entries(set)) {
    Object.defineProperty(fn, "name", { value: name, configurable: true });
  }
}
