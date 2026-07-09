// fibonacci shows three ways to compose recursive durable invocations with the
// Resonate SDK's async/await engine:
//
//   --mode=rpc   every recursive call goes through ctx.rpc (server-dispatched,
//                may execute on any worker in the group)
//   --mode=run   every recursive call goes through ctx.run (local, same worker)
//   --mode=mix   one branch via rpc, the other via run
//
// Unlike the generator SDK there are no `begin*` variants: ctx.run / ctx.rpc are
// eager — calling them starts the work immediately and returns a Promise. Hold
// both promises, then await them, to get a concurrent fan-out / fan-in.
//
// Run a Resonate server on localhost:8001 first, then e.g.:
//
//   npx tsx examples/async/fibonacci.ts --mode=rpc --n=10

import { AsyncResonate, type Context } from "../../src/async/index.js";

async function fibRpc(ctx: Context, n: number): Promise<number> {
  if (n < 2) return n;
  const f1 = ctx.rpc<number>("fibRpc", n - 1);
  const f2 = ctx.rpc<number>("fibRpc", n - 2);
  return (await f1) + (await f2);
}

async function fibRun(ctx: Context, n: number): Promise<number> {
  if (n < 2) return n;
  const f1 = ctx.run<number>("fibRun", n - 1);
  const f2 = ctx.run<number>("fibRun", n - 2);
  return (await f1) + (await f2);
}

async function fibMix(ctx: Context, n: number): Promise<number> {
  if (n < 2) return n;
  const f1 = ctx.rpc<number>("fibMix", n - 1);
  const f2 = ctx.run<number>("fibMix", n - 2);
  return (await f1) + (await f2);
}

function flag(name: string, fallback: string): string {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}

const mode = flag("mode", "run");
const n = Number(flag("n", "10"));

const resonate = new AsyncResonate({ url: "http://localhost:8001" });
const fns: Record<string, ReturnType<typeof resonate.register>> = {
  rpc: resonate.register("fibRpc", fibRpc),
  run: resonate.register("fibRun", fibRun),
  mix: resonate.register("fibMix", fibMix),
};

const fn = fns[mode];
if (!fn) {
  console.error(`unknown --mode ${mode} (want rpc | run | mix)`);
  process.exit(1);
}

const id = `fib-${mode}-${n}-${Date.now()}`;
const out = await (await fn.run(id, n)).result();

console.log(`fib(${n}) = ${out}  [mode=${mode}]`);

await resonate.stop();
