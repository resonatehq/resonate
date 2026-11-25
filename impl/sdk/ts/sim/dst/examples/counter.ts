// A planted read-modify-write race between two parallel async leaves.
//
// `increment` reads a counter in one leaf and writes it back in another. Both
// leaves await real asynchronous work (a timer and a promise chain), like a
// database client would. `twice` runs two increments in parallel on the same
// worker. If both reads run before either write's outcome is resumed, one
// update is lost. Whether that happens is decided only by which leaf the
// scheduler runs next and when it resumes each outcome: a tape decision.

import type { Context } from "../../../src/context.js";

export type Counter = { count: number };

export function counter(): Counter {
  return { count: 0 };
}

const io = () => new Promise<void>((resolve) => setTimeout(resolve, 1)).then(() => Promise.resolve());

async function getCount(ctx: Context): Promise<number> {
  await io();
  return ctx.getDependency<Counter>("world")!.count;
}

async function setCount(ctx: Context, value: number): Promise<void> {
  await io();
  ctx.getDependency<Counter>("world")!.count = value;
}

async function bump(ctx: Context): Promise<void> {
  await io();
  ctx.getDependency<Counter>("world")!.count += 1;
}

export function* increment(ctx: Context): Generator<any, void, any> {
  const v: number = yield* ctx.run(getCount);
  yield* ctx.run(setCount, v + 1);
}

export function* incrementAtomic(ctx: Context): Generator<any, void, any> {
  yield* ctx.run(bump);
}

export function* twice(ctx: Context, atomic: boolean): Generator<any, number, any> {
  const f = atomic ? incrementAtomic : increment;
  const a = yield* ctx.beginRun(f);
  const b = yield* ctx.beginRun(f);
  yield* a;
  yield* b;
  return 2;
}

/** A leaf that leaves a timer running after it returns. */
async function leaky(_ctx: Context): Promise<string> {
  void new Promise((resolve) => setTimeout(resolve, 50));
  return "done";
}

/** A leaf that never settles. */
async function stuck(_ctx: Context): Promise<string> {
  return new Promise<string>(() => {});
}

export function* callsLeaky(ctx: Context): Generator<any, string, any> {
  return yield* ctx.run(leaky);
}

export function* callsStuck(ctx: Context): Generator<any, string, any> {
  return yield* ctx.run(stuck);
}

export const functions = {
  getCount,
  setCount,
  bump,
  increment,
  incrementAtomic,
  twice,
  leaky,
  stuck,
  callsLeaky,
  callsStuck,
};

export function counted(c: Counter): void {
  if (c.count !== 2) throw new Error(`lost update: count is ${c.count}, expected 2`);
}
