// detached shows fire-and-forget durable invocations whose lifetime is
// **decoupled** from the parent.
//
//   placeOrder:
//     1. reserveStock   (ctx.run -- parent waits)
//     2. chargeCard     (ctx.run -- parent waits)
//     3. writeAuditLog  (ctx.detached -- parent does NOT wait)
//
// The first two steps are ordinary durable children whose results feed the
// orchestrator. The audit log is a side-effect workflow that must *survive* the
// order completing: the order should not block on it, and an audit that runs
// longer than the order must still finish.
//
// `ctx.detached` is the tool. Awaiting it resolves with a DetachedHandle once
// the spawn is durably created -- NOT its result -- so the parent never blocks
// on the audit. The handle's `.id` is the audit's own root promise id, which
// you can re-attach to later -- from another process, hours later -- via
// `resonate.get(id)`.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/async/detached.ts

import { type Context, type Info, Resonate } from "../../src/async/index.js";

// -- Order steps (leaves: each prints once, settles once) -----------------

async function reserveStock(_info: Info, sku: string, qty: number): Promise<string> {
  const ref = `STK-${sku}-${qty}`;
  console.log(`  [reserveStock] reserved ${ref}`);
  return ref;
}

async function chargeCard(_info: Info, customer: string, amount: number): Promise<string> {
  const ref = `CH-${customer}-${amount}`;
  console.log(`  [chargeCard] charged ${ref}`);
  return ref;
}

// -- Audit workflow (its own root, its own timeout, decoupled from placeOrder)

async function hashPayload(_info: Info, customer: string, sku: string, amount: number): Promise<string> {
  const digest = Buffer.from(`${customer}:${sku}:${amount}`).toString("hex");
  console.log(`  [hashPayload] ${digest}`);
  return digest;
}

async function shipToWarehouse(_info: Info, digest: string): Promise<string> {
  console.log(`  [shipToWarehouse] persisted ${digest}`);
  return `audit-${digest}`;
}

async function writeAuditLog(
  ctx: Context,
  customer: string,
  sku: string,
  amount: number,
  stockRef: string,
  chargeRef: string,
): Promise<string> {
  // A two-step child workflow. `placeOrder` never sees these steps run -- they
  // execute *after* placeOrder returns, on whichever worker the server hands
  // the task to. That decoupling is the whole point of `detached`.
  const digest = await ctx.run(hashPayload, customer, sku, amount);
  const location = await ctx.run(shipToWarehouse, digest);
  console.log(`  [writeAuditLog] stock=${stockRef} charge=${chargeRef} -> ${location}`);
  return location;
}

// -- Orchestrator ---------------------------------------------------------

async function placeOrder(
  ctx: Context,
  customer: string,
  sku: string,
  qty: number,
  amount: number,
): Promise<[string, string, string]> {
  // Foreground work -- the order needs both of these to commit.
  const stockRef = await ctx.run(reserveStock, sku, qty);
  const chargeRef = await ctx.run(chargeCard, customer, amount);

  // Fire-and-forget: dispatch the audit workflow by NAME. Awaiting the detached
  // call resolves with a handle once the audit's root promise is created -- we
  // never await its result. `.id` is the audit's durable id, returned so the
  // caller can attach to it.
  const audit = await ctx.detached("writeAuditLog", customer, sku, amount, stockRef, chargeRef);
  console.log(`  [placeOrder] dispatched audit id=${audit.id} (not waiting on it)`);

  return [stockRef, chargeRef, audit.id];
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("placeOrder", placeOrder);
resonate.register("writeAuditLog", writeAuditLog);

try {
  const orderId = `order-${Date.now()}`;
  console.log(`[placeOrder] starting workflow id=${orderId}`);
  const [stockRef, chargeRef, auditId] = await (
    await resonate.run(orderId, placeOrder, "alice", "WIDGET-7", 2, 199)
  ).result();
  console.log(`[placeOrder] OK: stock=${stockRef} charge=${chargeRef}`);

  // The order committed. Attach to the audit's durable promise by id and await
  // it *separately* from the order -- in a real system, a different process.
  console.log(`[audit] attaching to detached workflow id=${auditId}`);
  const location = await (await resonate.get(auditId)).result();
  console.log(`[audit] OK: ${location}`);
  if (typeof location !== "string" || !location.startsWith("audit-")) throw new Error("unexpected audit result");
} finally {
  await resonate.stop();
}
