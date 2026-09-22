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
// `ctx.detached` is the tool. Yielding it dispatches the workflow and returns a
// Future you can choose not to await; its `.id` is the durable id of the
// audit's own root promise, which you can re-attach to later -- from another
// process, hours later -- via `resonate.get(id)`.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/detached.ts

import { type Context, Resonate } from "../../src/index.js";

// -- Order steps (leaves: each prints once, settles once) -----------------

async function reserveStock(_ctx: Context, sku: string, qty: number): Promise<string> {
  const ref = `STK-${sku}-${qty}`;
  console.log(`  [reserveStock] reserved ${ref}`);
  return ref;
}

async function chargeCard(_ctx: Context, customer: string, amount: number): Promise<string> {
  const ref = `CH-${customer}-${amount}`;
  console.log(`  [chargeCard] charged ${ref}`);
  return ref;
}

// -- Audit workflow (its own root, its own timeout, exempt from the parent's
//    suspension frontier) -------------------------------------------------

async function hashPayload(_ctx: Context, customer: string, sku: string, amount: number): Promise<string> {
  const digest = Buffer.from(`${customer}:${sku}:${amount}`).toString("hex");
  console.log(`  [hashPayload] ${digest}`);
  return digest;
}

async function shipToWarehouse(_ctx: Context, digest: string): Promise<string> {
  console.log(`  [shipToWarehouse] persisted ${digest}`);
  return `audit-${digest}`;
}

function* writeAuditLog(
  ctx: Context,
  customer: string,
  sku: string,
  amount: number,
  stockRef: string,
  chargeRef: string,
): Generator<any, string, any> {
  // A two-step child workflow. `placeOrder` never sees these steps run -- they
  // execute *after* placeOrder returns, on whichever worker the server hands
  // the task to. That decoupling is the whole point of `detached`.
  const digest: string = yield* ctx.run(hashPayload, customer, sku, amount);
  const location: string = yield* ctx.run(shipToWarehouse, digest);
  console.log(`  [writeAuditLog] stock=${stockRef} charge=${chargeRef} -> ${location}`);
  return location;
}

// -- Orchestrator ---------------------------------------------------------

function* placeOrder(
  ctx: Context,
  customer: string,
  sku: string,
  qty: number,
  amount: number,
): Generator<any, [string, string, string], any> {
  // Foreground work -- the order needs both of these to commit.
  const stockRef: string = yield* ctx.run(reserveStock, sku, qty);
  const chargeRef: string = yield* ctx.run(chargeCard, customer, amount);

  // Fire-and-forget: dispatch the audit workflow by NAME. Yielding it creates
  // the audit's root promise and returns a Future we do NOT await; its `.id` is
  // the audit's durable id, which we return so the caller can attach to it.
  const audit = yield* ctx.detached("writeAuditLog", customer, sku, amount, stockRef, chargeRef);
  const auditId = audit.id;
  console.log(`  [placeOrder] dispatched audit id=${auditId} (not waiting on it)`);

  return [stockRef, chargeRef, auditId];
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("placeOrder", placeOrder);
resonate.register("writeAuditLog", writeAuditLog);

try {
  const orderId = `order-${Date.now()}`;
  console.log(`[placeOrder] starting workflow id=${orderId}`);
  const [stockRef, chargeRef, auditId] = await resonate.run(orderId, placeOrder, "alice", "WIDGET-7", 2, 199);
  console.log(`[placeOrder] OK: stock=${stockRef} charge=${chargeRef}`);

  // The order committed. Attach to the audit's durable promise by id and await
  // it *separately* from the order -- in a real system, a different process.
  console.log(`[audit] attaching to detached workflow id=${auditId}`);
  const auditHandle = await resonate.get(auditId);
  const location = await auditHandle.result();
  console.log(`[audit] OK: ${location}`);
  if (typeof location !== "string" || !location.startsWith("audit-")) throw new Error("unexpected audit result");
} finally {
  await resonate.stop();
}
