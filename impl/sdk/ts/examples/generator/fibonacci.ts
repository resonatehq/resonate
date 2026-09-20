// fibonacci shows three ways to compose recursive durable invocations with the
// Resonate SDK's generator engine:
//
//   --mode rpc   every recursive call goes through ctx.rpc (server-dispatched,
//                may execute on any worker in the group)
//   --mode run   every recursive call goes through ctx.run (local, same worker)
//   --mode mix   one branch via rpc, the other via run
//
// beginRpc / beginRun start the work and return a Future without blocking; hold
// both futures, then `yield*` them, to get a concurrent fan-out / fan-in.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then e.g.:
//
//   npx tsx examples/generator/fibonacci.ts --mode rpc --n 10

import { type Context, Resonate } from "../../src/index.js";

function* fibRpc(ctx: Context, n: number): Generator<any, number, any> {
  if (n < 2) return n;
  const f1 = yield* ctx.beginRpc<number>("fibRpc", n - 1);
  const f2 = yield* ctx.beginRpc<number>("fibRpc", n - 2);
  return (yield* f1) + (yield* f2);
}

function* fibRun(ctx: Context, n: number): Generator<any, number, any> {
  if (n < 2) return n;
  const f1 = yield* ctx.beginRun(fibRun, n - 1);
  const f2 = yield* ctx.beginRun(fibRun, n - 2);
  return (yield* f1) + (yield* f2);
}

function* fibMix(ctx: Context, n: number): Generator<any, number, any> {
  if (n < 2) return n;
  const f1 = yield* ctx.beginRpc<number>("fibMix", n - 1);
  const f2 = yield* ctx.beginRun(fibMix, n - 2);
  return (yield* f1) + (yield* f2);
}

function flag(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

function fib(n: number): number {
  return n <= 1 ? n : fib(n - 1) + fib(n - 2);
}

const mode = flag("mode", "run");
const n = Number(flag("n", "10"));

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
const fns: Record<string, (ctx: Context, n: number) => Generator<any, number, any>> = {
  rpc: fibRpc,
  run: fibRun,
  mix: fibMix,
};
resonate.register("fibRpc", fibRpc);
resonate.register("fibRun", fibRun);
resonate.register("fibMix", fibMix);

const fn = fns[mode];
if (!fn) {
  console.error(`unknown --mode ${mode} (want rpc | run | mix)`);
  process.exit(1);
}

const id = `fib-${mode}-${n}-${Date.now()}`;
const out = await resonate.run(id, fn, n);
if (out !== fib(n)) throw new Error(`expected ${fib(n)}, got ${out}`);

console.log(`fib(${n}) = ${out}  [mode=${mode}]`);

await resonate.stop();
