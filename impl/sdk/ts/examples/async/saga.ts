// saga shows a multi-step durable workflow with compensation on failure
// (the canonical "distributed transactions" pattern):
//
//   bookTrip:
//       1. reserveFlight
//       2. reserveHotel   (on failure: releaseFlight)
//       3. chargeCard     (on failure: releaseHotel, releaseFlight)
//
// Each step is its own registered function dispatched via ctx.rpc. Step
// settlement is recorded in a durable promise, so if the worker crashes between
// two steps a restart skips the steps that already settled and runs only the
// missing ones — including the compensations. A failing step rejects its
// awaited promise, which we catch to drive the rollback.
//
// Run a Resonate server on localhost:8001 first, then either of:
//
//   npx tsx examples/async/saga.ts                 # happy path
//   npx tsx examples/async/saga.ts --fail=charge   # forces chargeCard to fail;
//                                                  # both compensations run in
//                                                  # reverse order

import { AsyncResonate, type Context, type Info } from "../../src/async/index.js";

type FailAt = "" | "hotel" | "charge";

// The only genuine composite: the trip's three reservation refs. Every step
// just takes/returns a string, so the steps need no named types.
interface TripResult {
  flightRef: string;
  hotelRef: string;
  chargeRef: string;
}

// ── Step functions ──────────────────────────────────────────────────────
//
// Each step (and each compensation) is a leaf: it performs one action and
// returns its ref without spawning children, so it takes `Info` rather than
// the full `Context`. Only the bookTrip orchestrator drives them via ctx.rpc.

async function reserveFlight(_info: Info, customer: string, from: string, to: string): Promise<string> {
  const ref = `FL-${customer}-${from}-${to}`;
  console.log(`  [reserveFlight] reserved ${ref}`);
  return ref;
}

async function reserveHotel(_info: Info, customer: string, city: string, fail: boolean): Promise<string> {
  if (fail) {
    console.log(`  [reserveHotel] FAILED for ${customer} in ${city}`);
    throw new Error(`no rooms available in ${city}`);
  }
  const ref = `HT-${customer}-${city}`;
  console.log(`  [reserveHotel] reserved ${ref}`);
  return ref;
}

async function chargeCard(_info: Info, customer: string, amount: number, fail: boolean): Promise<string> {
  if (fail) {
    console.log(`  [chargeCard] FAILED for ${customer} ($${amount})`);
    throw new Error(`card declined for $${amount}`);
  }
  const ref = `CH-${customer}-${amount}`;
  console.log(`  [chargeCard] charged ${ref}`);
  return ref;
}

async function releaseFlight(_info: Info, ref: string): Promise<string> {
  console.log(`  [releaseFlight] released ${ref}`);
  return ref;
}

async function releaseHotel(_info: Info, ref: string): Promise<string> {
  console.log(`  [releaseHotel] released ${ref}`);
  return ref;
}

// ── Saga orchestrator ───────────────────────────────────────────────────

async function bookTrip(
  ctx: Context,
  customer: string,
  from: string,
  to: string,
  amount: number,
  failAt: FailAt,
): Promise<TripResult> {
  // Step 1: flight
  const flightRef = await ctx.rpc<string>("reserveFlight", customer, from, to);

  // Step 2: hotel — on failure, release the flight
  let hotelRef: string;
  try {
    hotelRef = await ctx.rpc<string>("reserveHotel", customer, to, failAt === "hotel");
  } catch (err) {
    await compensate(ctx, "", flightRef);
    throw new Error(`reserveHotel: ${(err as Error).message}`);
  }

  // Step 3: charge — on failure, release hotel then flight
  let chargeRef: string;
  try {
    chargeRef = await ctx.rpc<string>("chargeCard", customer, amount, failAt === "charge");
  } catch (err) {
    await compensate(ctx, hotelRef, flightRef);
    throw new Error(`chargeCard: ${(err as Error).message}`);
  }

  return { flightRef, hotelRef, chargeRef };
}

// compensate runs the inverse of any completed steps in reverse order. Empty
// refs are skipped. Compensation failures are logged but not surfaced — the
// saga has already failed; the goal is best-effort rollback.
async function compensate(ctx: Context, hotelRef: string, flightRef: string): Promise<void> {
  console.log("  [bookTrip] running compensations...");
  if (hotelRef) {
    try {
      await ctx.rpc<string>("releaseHotel", hotelRef);
    } catch (err) {
      console.log(`  [bookTrip] releaseHotel failed: ${(err as Error).message}`);
    }
  }
  if (flightRef) {
    try {
      await ctx.rpc<string>("releaseFlight", flightRef);
    } catch (err) {
      console.log(`  [bookTrip] releaseFlight failed: ${(err as Error).message}`);
    }
  }
}

// ── main ────────────────────────────────────────────────────────────────

function flag(name: string, fallback: string): string {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}

const failAt = flag("fail", "") as FailAt;

const resonate = new AsyncResonate({ url: "http://localhost:8001" });

const bookFn = resonate.register("bookTrip", bookTrip);
resonate.register("reserveFlight", reserveFlight);
resonate.register("reserveHotel", reserveHotel);
resonate.register("chargeCard", chargeCard);
resonate.register("releaseFlight", releaseFlight);
resonate.register("releaseHotel", releaseHotel);

const id = `saga-${Date.now()}`;
console.log(`[bookTrip] starting workflow id=${id} fail_at=${JSON.stringify(failAt)}`);

try {
  const out = await (await bookFn.run(id, "alice", "SFO", "JFK", 850, failAt)).result();
  console.log(`[bookTrip] OK: flight=${out.flightRef} hotel=${out.hotelRef} charge=${out.chargeRef}`);
} catch (err) {
  console.log(`[bookTrip] FAILED: ${(err as Error).message}`);
}

await resonate.stop();
