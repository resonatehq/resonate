// saga shows a multi-step durable workflow with compensation on failure
// (the canonical "distributed transactions" pattern):
//
//   bookTrip:
//     1. reserveFlight
//     2. reserveHotel   (on failure: releaseFlight)
//     3. chargeCard     (on failure: releaseHotel, releaseFlight)
//
// Each step is its own registered function dispatched via ctx.rpc. Step
// settlement is recorded in a durable promise, so if the worker crashes between
// two steps a restart skips the settled steps and runs only the missing ones --
// including the compensations. A failing step rejects, which we catch to drive
// the rollback.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/saga.ts                # happy path
//   npx tsx examples/generator/saga.ts --fail charge  # forces chargeCard to
//                                                     # fail; both compensations
//                                                     # run in reverse order

import { type Context, Never, Resonate } from "../../src/index.js";

type FailAt = "" | "hotel" | "charge";

interface TripResult {
  flightRef: string;
  hotelRef: string;
  chargeRef: string;
}

// -- Step functions (leaves: each performs one action) --------------------

async function reserveFlight(_ctx: Context, customer: string, from: string, to: string): Promise<string> {
  const ref = `FL-${customer}-${from}-${to}`;
  console.log(`  [reserveFlight] reserved ${ref}`);
  return ref;
}

async function reserveHotel(_ctx: Context, customer: string, city: string, fail: boolean): Promise<string> {
  if (fail) {
    console.log(`  [reserveHotel] FAILED for ${customer} in ${city}`);
    throw new Error(`no rooms available in ${city}`);
  }
  const ref = `HT-${customer}-${city}`;
  console.log(`  [reserveHotel] reserved ${ref}`);
  return ref;
}

async function chargeCard(_ctx: Context, customer: string, amount: number, fail: boolean): Promise<string> {
  if (fail) {
    console.log(`  [chargeCard] FAILED for ${customer} ($${amount})`);
    throw new Error(`card declined for $${amount}`);
  }
  const ref = `CH-${customer}-${amount}`;
  console.log(`  [chargeCard] charged ${ref}`);
  return ref;
}

async function releaseFlight(_ctx: Context, ref: string): Promise<string> {
  console.log(`  [releaseFlight] released ${ref}`);
  return ref;
}

async function releaseHotel(_ctx: Context, ref: string): Promise<string> {
  console.log(`  [releaseHotel] released ${ref}`);
  return ref;
}

// -- Saga orchestrator ----------------------------------------------------

function* bookTrip(
  ctx: Context,
  customer: string,
  from: string,
  to: string,
  amount: number,
  failAt: FailAt,
): Generator<any, TripResult, any> {
  // Step 1: flight
  const flightRef: string = yield* ctx.rpc<string>("reserveFlight", customer, from, to);

  // Step 2: hotel -- on failure, release the flight. Never so a forced failure
  // surfaces at once instead of retrying the leaf.
  const noRetry = ctx.options({ retryPolicy: new Never() });
  let hotelRef: string;
  try {
    hotelRef = yield* ctx.rpc<string>("reserveHotel", customer, to, failAt === "hotel", noRetry);
  } catch (err) {
    yield* compensate(ctx, "", flightRef);
    throw new Error(`reserveHotel: ${(err as Error).message}`);
  }

  // Step 3: charge -- on failure, release hotel then flight
  let chargeRef: string;
  try {
    chargeRef = yield* ctx.rpc<string>("chargeCard", customer, amount, failAt === "charge", noRetry);
  } catch (err) {
    yield* compensate(ctx, hotelRef, flightRef);
    throw new Error(`chargeCard: ${(err as Error).message}`);
  }

  return { flightRef, hotelRef, chargeRef };
}

// compensate runs the inverse of completed steps in reverse order. Empty refs
// are skipped. Compensation failures are logged, not surfaced -- the saga has
// already failed; the goal is best-effort rollback.
function* compensate(ctx: Context, hotelRef: string, flightRef: string): Generator<any, void, any> {
  console.log("  [bookTrip] running compensations...");
  if (hotelRef) {
    try {
      yield* ctx.rpc<string>("releaseHotel", hotelRef);
    } catch (err) {
      console.log(`  [bookTrip] releaseHotel failed: ${(err as Error).message}`);
    }
  }
  if (flightRef) {
    try {
      yield* ctx.rpc<string>("releaseFlight", flightRef);
    } catch (err) {
      console.log(`  [bookTrip] releaseFlight failed: ${(err as Error).message}`);
    }
  }
}

// -- main -----------------------------------------------------------------

function flag(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const failAt = flag("fail", "") as FailAt;

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("bookTrip", bookTrip);
resonate.register("reserveFlight", reserveFlight);
resonate.register("reserveHotel", reserveHotel);
resonate.register("chargeCard", chargeCard);
resonate.register("releaseFlight", releaseFlight);
resonate.register("releaseHotel", releaseHotel);

const id = `saga-${Date.now()}`;
console.log(`[bookTrip] starting workflow id=${id} fail_at=${JSON.stringify(failAt)}`);

try {
  const out: TripResult = await resonate.run(id, bookTrip, "alice", "SFO", "JFK", 850, failAt);
  console.log(`[bookTrip] OK: flight=${out.flightRef} hotel=${out.hotelRef} charge=${out.chargeRef}`);
} catch (err) {
  console.log(`[bookTrip] FAILED: ${(err as Error).message}`);
} finally {
  await resonate.stop();
}
