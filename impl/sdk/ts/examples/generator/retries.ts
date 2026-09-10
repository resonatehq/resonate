// retries shows Resonate retrying a flaky function until it succeeds.
//
// The rule: Resonate retries **leaf** functions -- those that perform no
// durable op. A function that *does* perform one is a workflow, recovered by
// replay, and is never retried. In the generator engine the default policy is
// Exponential for leaves and Never for generator workflows; here we set an
// explicit fast Constant policy so the demo is instant.
//
// `charge` is a leaf that fails twice before it succeeds. `checkout` is a
// workflow that invokes it via ctx.run and ctx.rpc -- each of those runs charge
// as a leaf, so each is retried; checkout itself is never retried.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/retries.ts

import { Constant, type Context, Resonate } from "../../src/index.js";

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

// A leaf -- no ctx.* durable op -- so Resonate retries it on failure.
async function charge(ctx: Context, key: string, amount: number): Promise<string> {
  return ctx.getDependency<Gateway>("gateway")!.charge(key, amount);
}

// checkout is a workflow (it calls ctx.run / ctx.rpc), so it is never retried
// -- but each flaky leaf it invokes is.
function* checkout(ctx: Context): Generator<any, string, any> {
  const viaRun: string = yield* ctx.run(charge, "ctx.run", 300, ctx.options({ retryPolicy: POLICY }));
  const viaRpc: string = yield* ctx.rpc<string>("charge", "ctx.rpc", 400, ctx.options({ retryPolicy: POLICY }));
  return `${viaRun} | ${viaRpc}`;
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.setDependency("gateway", new Gateway());
resonate.register("checkout", checkout);
resonate.register("charge", charge);

const ts = Date.now();
try {
  console.log("resonate.run:");
  const a = await resonate.run(
    `retries-run-${ts}`,
    charge,
    "resonate.run",
    100,
    resonate.options({ retryPolicy: POLICY }),
  );
  console.log(`  -> ${a}`);

  console.log("resonate.rpc:");
  const b = await resonate.rpc(
    `retries-rpc-${ts}`,
    "charge",
    "resonate.rpc",
    200,
    resonate.options({ retryPolicy: POLICY }),
  );
  console.log(`  -> ${b}`);

  console.log("ctx.run + ctx.rpc (inside the checkout workflow):");
  const c = await resonate.run(`retries-checkout-${ts}`, checkout);
  console.log(`  -> ${c}`);
} finally {
  await resonate.stop();
}
