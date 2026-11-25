// Example user workflows with planted bugs, for the deterministic simulator.
//
// The "bank" is external state (the world). Each leaf function is one durable
// step. Two bugs are planted, each only visible under some schedules:
//
//   transferRacy  reads the balances in one step and writes them in a later
//                 step. Two transfers that interleave between the read and the
//                 write lose an update (an interleaving bug).
//
//   payNotIdempotent charges a card in a step that is not idempotent. If the
//                 worker crashes after the charge but before the step's
//                 result is recorded, the step runs again and charges twice
//                 (a fault bug).
//
// `transferSafe` and `paySafe` are the fixed versions.

import type { Context } from "../../../src/context.js";
import { Constant } from "../../../src/retries.js";

export type Bank = {
  accounts: Record<string, number>;
  /** Charges actually made, keyed by idempotency key. */
  charges: { key: string; amount: number }[];
  /** Keys already charged, for the idempotent variant. */
  seen: Set<string>;
};

export function bank(): Bank {
  return { accounts: { alice: 100, bob: 100, carol: 100 }, charges: [], seen: new Set() };
}

function world(ctx: Context): Bank {
  return ctx.getDependency<Bank>("world")!;
}

// -- leaves ------------------------------------------------------------------

async function readBalances(ctx: Context, from: string, to: string): Promise<[number, number]> {
  const b = world(ctx);
  return [b.accounts[from], b.accounts[to]];
}

async function writeBalances(ctx: Context, from: string, fromBalance: number, to: string, toBalance: number) {
  const b = world(ctx);
  b.accounts[from] = fromBalance;
  b.accounts[to] = toBalance;
}

async function move(ctx: Context, from: string, to: string, amount: number) {
  const b = world(ctx);
  b.accounts[from] -= amount;
  b.accounts[to] += amount;
}

async function charge(ctx: Context, key: string, amount: number) {
  world(ctx).charges.push({ key, amount });
  return `ch-${key}`;
}

async function chargeOnce(ctx: Context, key: string, amount: number) {
  const b = world(ctx);
  if (!b.seen.has(key)) {
    b.seen.add(key);
    b.charges.push({ key, amount });
  }
  return `ch-${key}`;
}

// -- workflows -----------------------------------------------------------------

export function* transferRacy(ctx: Context, from: string, to: string, amount: number): Generator<any, string, any> {
  const [a, b]: [number, number] = yield* ctx.rpc<[number, number]>("readBalances", from, to);
  yield* ctx.rpc("writeBalances", from, a - amount, to, b + amount);
  return "ok";
}

export function* transferSafe(ctx: Context, from: string, to: string, amount: number): Generator<any, string, any> {
  yield* ctx.rpc("move", from, to, amount);
  return "ok";
}

export function* payNotIdempotent(ctx: Context, order: string, amount: number): Generator<any, string, any> {
  return yield* ctx.rpc<string>("charge", order, amount);
}

export function* paySafe(ctx: Context, order: string, amount: number): Generator<any, string, any> {
  return yield* ctx.rpc<string>("chargeOnce", order, amount);
}

/** Uses randomness in user code: picks a random account to receive a bonus. */
export function* lottery(ctx: Context): Generator<any, string, any> {
  const r: number = yield* ctx.math.random();
  const names = ["alice", "bob", "carol"];
  const winner = names[Math.floor(r * names.length)];
  yield* ctx.rpc("move", "carol", winner, 1);
  return winner;
}

/** Fails its first attempt; retried in-process after a backoff timer. */
async function flakyMove(ctx: Context, from: string, to: string, amount: number) {
  if (ctx.info.attempt <= 1) throw new Error("transient");
  const b = world(ctx);
  b.accounts[from] -= amount;
  b.accounts[to] += amount;
}

/** Runs a flaky local step with a 500ms constant backoff (a setTimeout in the engine). */
export function* retryingTransfer(ctx: Context, from: string, to: string, amount: number): Generator<any, string, any> {
  yield* ctx.run(
    flakyMove,
    from,
    to,
    amount,
    ctx.options({ retryPolicy: new Constant({ delay: 500, maxRetries: 3 }) }),
  );
  return "ok";
}

/** Sleeps durably between two steps. */
export function* delayedTransfer(
  ctx: Context,
  from: string,
  to: string,
  amount: number,
  ms: number,
): Generator<any, string, any> {
  yield* ctx.sleep(ms);
  yield* ctx.rpc("move", from, to, amount);
  return "ok";
}

export const functions = {
  readBalances,
  writeBalances,
  move,
  charge,
  chargeOnce,
  transferRacy,
  transferSafe,
  payNotIdempotent,
  paySafe,
  lottery,
  delayedTransfer,
  retryingTransfer,
};

/** Money is conserved: the total across all accounts never changes. */
export function conserved(b: Bank): void {
  const total = Object.values(b.accounts).reduce((x, y) => x + y, 0);
  if (total !== 300) throw new Error(`money not conserved: total is ${total}, expected 300`);
}

/** Every order is charged at most once. */
export function chargedOnce(b: Bank): void {
  const keys = b.charges.map((c) => c.key);
  const dup = keys.find((k, i) => keys.indexOf(k) !== i);
  if (dup) throw new Error(`order ${dup} charged ${keys.filter((k) => k === dup).length} times`);
}
