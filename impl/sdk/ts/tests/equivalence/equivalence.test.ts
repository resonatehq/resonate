// Layer A: cross-engine equivalence on clean (no-failure) runs.
//
// Each matched workload runs on both the generator engine and the async engine
// against fresh in-memory servers; we assert the async engine produces the same
// root outcome and the same canonical durable state as the generator engine
// (the reference). Known divergences are documented inline.

import { expectEquivalent } from "./harness.js";
import { detached, errorCaught, errorUncaught, fanout, fibonacci, humanInTheLoop, sleep } from "./workloads.js";

describe("engine equivalence (generator vs async)", () => {
  test("fibonacci (local run)", () => expectEquivalent(fibonacci), 30_000);
  test("fan-out / fan-in", () => expectEquivalent(fanout), 30_000);
  test("error propagation (caught)", () => expectEquivalent(errorCaught), 30_000);
  test("error propagation (uncaught)", () => expectEquivalent(errorUncaught), 30_000);
  test("sleep", () => expectEquivalent(sleep), 30_000);
  test("human-in-the-loop (DPC)", () => expectEquivalent(humanInTheLoop), 30_000);

  // Detached id is hashed from prefixId (generator) vs originId (async). For a
  // top-level parent those coincide, so this currently passes; a nested-detached
  // variant would surface the divergence. Kept as a live test to catch a
  // regression either way.
  test("detached", () => expectEquivalent(detached), 30_000);
});
