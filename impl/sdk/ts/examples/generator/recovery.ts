// recovery shows serialize/deserialize across the durability boundary.
//
// Every value a durable function exchanges -- its arguments and its return --
// is written to a durable promise as JSON. **Recovery needs no special path**:
// the same (de)serialization runs on every invocation, so a value rebuilt after
// a crash is rebuilt by the exact steps that ran the first time.
//
// This example is a plain checkout -- no crash, no forced suspend. It then
// re-runs with the same id, so the result is served from the durable promise
// (genuine recovery) and comes back equal to the original -- same code, no
// recovery-only branch.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/recovery.ts
//
// Note on replay: a durable orchestrator re-executes from the top each time it
// awaits a not-yet-settled future, so every side effect (a `console.log`) lives
// in a leaf step -- which settles once and never re-runs -- not in `checkout`.

import { type Context, Resonate } from "../../src/index.js";

interface Cart {
  items: string[];
  total: number;
}

interface Receipt {
  cart: Cart; // a nested object -- the whole tree round-trips as JSON
  paid: boolean;
}

// -- Leaf steps (each prints once, settles once) --------------------------

async function summarize(_ctx: Context, items: string[]): Promise<Cart> {
  const cart = { items, total: items.length * 10 };
  console.log(`  [summarize] ${JSON.stringify(items)} -> ${JSON.stringify(cart)}`);
  return cart;
}

async function pay(_ctx: Context, cart: Cart): Promise<Receipt> {
  console.log(`  [pay] charging ${cart.total} for ${cart.items.length} items`);
  return { cart, paid: true };
}

// -- Orchestrator ---------------------------------------------------------

function* checkout(ctx: Context, items: string[]): Generator<any, Receipt, any> {
  const cart: Cart = yield* ctx.run(summarize, items);
  // The Cart is handed straight back across the boundary as an argument to the
  // next step; `pay` receives it rebuilt from JSON and returns a Receipt.
  return yield* ctx.run(pay, cart);
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("checkout", checkout);

try {
  const id = `recovery-${Date.now()}`;
  console.log(`[checkout] run id=${id}`);
  const receipt: Receipt = await resonate.run(id, checkout, ["apple", "pear", "plum"]);
  if (receipt.cart.total !== 30 || receipt.paid !== true) throw new Error("unexpected receipt");
  console.log(`[checkout] result: ${JSON.stringify(receipt)}`);

  // Re-run with the SAME id. Nothing re-executes (no leaf lines print); the
  // value is served from the durable promise -- genuine recovery -- and comes
  // back through the very same deserialize, yielding an equal value.
  console.log(`[checkout] re-run id=${id} (served from the durable promise)`);
  const again: Receipt = await resonate.run(id, checkout, ["apple", "pear", "plum"]);
  console.log(`[checkout] recovered result equals original: ${JSON.stringify(again) === JSON.stringify(receipt)}`);
} finally {
  await resonate.stop();
}
