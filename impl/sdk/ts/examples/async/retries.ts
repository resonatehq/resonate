// retries shows Resonate retrying a flaky function until it succeeds.
//
// The rule: Resonate retries **leaf** functions -- those that perform no
// durable op. A function that *does* perform one is a workflow, recovered by
// replay, and is never retried. The async engine defaults to **Never** (retries
// are opt-in), so here we pass an explicit fast Constant policy on each call.
//
// `charge` is a leaf that fails twice before it succeeds. `checkout` is a
// workflow that invokes it via ctx.run and ctx.rpc -- each of those runs charge
// as a leaf, so each is retried; checkout itself is never retried.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/async/retries.ts

import { Constant, type Context, type Info, Resonate } from "../../src/async/index.js";

// Retry up to 5 times with no delay between attempts (keeps the demo instant).
const POLICY = new Constant({ delay: 0, maxRetries: 5 });

// A flaky service: fails the first two calls per key, then succeeds.
class Gateway {
  private attempts = new Map<string, number>();

  charge(key: string, amount: number): string {
    const n = (this.attempts.get(key) ?? 0) + 1;
    this.attempts.set(key, n);
    console.log(`  [${key}] attempt ${n}...`);
    if (n <= 2) throw new Error(`timeout (attempt ${n})`);
    return `charged $${amount}`;
  }
}

// A leaf -- no ctx.* durable op -- so Resonate retries it (with the opt-in
// policy passed by each caller).
async function charge(info: Info, key: string, amount: number): Promise<string> {
  return info.getDependency<Gateway>("gateway")!.charge(key, amount);
}

// checkout is a workflow (it calls ctx.run / ctx.rpc), so it is never retried
// -- but each flaky leaf it invokes is.
async function checkout(ctx: Context): Promise<string> {
  const viaRun = await ctx.run(charge, "ctx.run", 300, ctx.options({ retryPolicy: POLICY }));
  const viaRpc = await ctx.rpc<string>("charge", "ctx.rpc", 400, ctx.options({ retryPolicy: POLICY }));
  return `${viaRun} | ${viaRpc}`;
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.setDependency("gateway", new Gateway());
resonate.register("checkout", checkout);
resonate.register("charge", charge);

const ts = Date.now();
try {
  console.log("resonate.run:");
  const a = await (
    await resonate.run(`retries-run-${ts}`, charge, "resonate.run", 100, resonate.options({ retryPolicy: POLICY }))
  ).result();
  console.log(`  -> ${a}`);

  console.log("resonate.rpc:");
  const b = await (
    await resonate.rpc(`retries-rpc-${ts}`, "charge", "resonate.rpc", 200, resonate.options({ retryPolicy: POLICY }))
  ).result();
  console.log(`  -> ${b}`);

  console.log("ctx.run + ctx.rpc (inside the checkout workflow):");
  const c = await (await resonate.run(`retries-checkout-${ts}`, checkout)).result();
  console.log(`  -> ${c}`);
} finally {
  await resonate.stop();
}
