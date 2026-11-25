/**
 * Deterministic simulation runtime (sim/dst).
 *
 * Several simulated workers run the real SDK engine against the in-memory
 * server; every nondeterministic decision goes through a driver and is recorded
 * on a tape. These tests cover the runtime's guarantees: determinism per seed,
 * scripted interleavings, fault injection, bug finding and exact replay.
 */

import { type Bank, bank, chargedOnce, conserved, functions } from "../sim/dst/examples/bank.js";
import { type Counter, counted, counter, functions as counterFunctions } from "../sim/dst/examples/counter.js";
import {
  type Action,
  parseTape,
  type Result,
  replay,
  type Scenario,
  scripted,
  seeded,
  serializeTape,
  simulate,
  type Tape,
} from "../sim/dst/index.js";

jest.setTimeout(120_000);

const decisions = (tape: Tape) => tape.entries.filter((e) => e.t !== "random") as ({ step: number } & Action)[];
const under = (id: string) => new RegExp(`^${id}(:|$)`);

function transfers(workers: number): Scenario<Bank> {
  return {
    workers,
    functions,
    world: bank,
    start: [
      { id: "t1", func: "transferSafe", args: ["alice", "bob", 10] },
      { id: "t2", func: "transferSafe", args: ["bob", "carol", 20] },
      { id: "t3", func: "transferSafe", args: ["carol", "alice", 30] },
    ],
    check: conserved,
  };
}

const racy: Scenario<Bank> = {
  workers: ["w1", "w2"],
  functions,
  world: bank,
  start: [
    { id: "t1", func: "transferRacy", args: ["alice", "bob", 10] },
    { id: "t2", func: "transferRacy", args: ["bob", "carol", 20] },
  ],
  check: conserved,
};

const payment = (func: "payNotIdempotent" | "paySafe"): Scenario<Bank> => ({
  workers: ["w1", "w2"],
  functions,
  world: bank,
  start: [{ id: "o1", func, args: ["o1", 5] }],
  check: chargedOnce,
});

describe("deterministic core and tape", () => {
  test("one workflow on one worker runs to completion and records every step", async () => {
    const r = await simulate(
      { workers: 1, functions, world: bank, start: [{ id: "t1", func: "transferSafe", args: ["alice", "bob", 10] }] },
      seeded(1),
    );
    expect(r.status).toBe("completed");
    expect(r.results.t1).toEqual({ state: "resolved", value: "ok" });
    expect(decisions(r.tape)).toHaveLength(r.steps);
    expect(decisions(r.tape).map((e) => e.step)).toEqual(Array.from({ length: r.steps }, (_, i) => i + 1));
  });

  test("the same seed yields a byte-identical tape every run", async () => {
    const runs = await Promise.all([1, 2, 3].map(() => Promise.resolve()));
    const tapes: string[] = [];
    for (const _ of runs) tapes.push(serializeTape((await simulate(transfers(1), seeded(42))).tape));
    expect(new Set(tapes).size).toBe(1);
  });

  test("ambient Date.now, setTimeout and crypto.randomUUID are restored after a run", async () => {
    const before = [Date.now, Math.random, globalThis.setTimeout];
    await simulate(transfers(1), seeded(1));
    expect([Date.now, Math.random, globalThis.setTimeout]).toEqual(before);
    expect(crypto.randomUUID()).not.toMatch(/^00000000-0000-4000-8000-/);
  });
});

describe("multiple workers via the in-memory server", () => {
  test("work spreads across workers that only talk through the server", async () => {
    const r = await simulate(transfers(3), seeded(7));
    expect(r.status).toBe("completed");
    const receivers = new Set(decisions(r.tape).flatMap((e) => (e.t === "deliver" && e.to ? [e.to] : [])));
    expect(receivers.size).toBeGreaterThan(1);
    expect(r.log.every((l) => !l.includes("deliver") || l.includes("server"))).toBe(true);
  });

  test("a multi-worker run is identical across repeated same-seed runs", async () => {
    for (const seed of [3, 11, 99]) {
      const a = serializeTape((await simulate(transfers(3), seeded(seed))).tape);
      const b = serializeTape((await simulate(transfers(3), seeded(seed))).tape);
      expect(a).toBe(b);
    }
    const a = serializeTape((await simulate(transfers(3), seeded(3))).tape);
    const b = serializeTape((await simulate(transfers(3), seeded(4))).tape);
    expect(a).not.toBe(b);
  });
});

describe("fault injection", () => {
  test("seeded crash, drop and delay faults are recorded on the tape and the run still converges", async () => {
    const kinds = new Set<string>();
    for (let seed = 1; seed <= 10; seed++) {
      const r = await simulate(transfers(3), seeded(seed, { crash: 0.02, drop: 0.03, delay: 0.03 }), {
        maxSteps: 20_000,
        requireCompletion: true,
      });
      expect(r.failure).toBeUndefined();
      for (const e of r.tape.entries) kinds.add(e.t);
    }
    for (const k of ["crash", "restart", "drop", "delay", "advance"]) expect(kinds).toContain(k);
  });

  test("a dropped execute message is redelivered after the server's retry timeout", async () => {
    const r = await simulate(
      { ...transfers(1), start: [transfers(1).start[0]] },
      scripted([{ drop: { kind: "execute", subject: "t1" } }, { advance: true }]),
    );
    expect(r.status).toBe("completed");
    expect(decisions(r.tape).slice(0, 2)).toEqual([
      { step: 1, t: "drop", env: 1 },
      { step: 2, t: "advance", to: 30_000 },
    ]);
    expect(r.log[2]).toContain("execute(t1)");
  });

  test("a delayed message is held until the clock reaches it", async () => {
    const r = await simulate(
      { ...transfers(1), start: [transfers(1).start[0]] },
      scripted([{ delay: { kind: "execute", subject: "t1" }, ms: 1234 }, { advance: true }]),
    );
    expect(r.status).toBe("completed");
    expect(decisions(r.tape)[1]).toEqual({ step: 2, t: "advance", to: 1234 });
    expect(r.log[2]).toMatch(/t=1234 deliver #1 -> worker-1: message execute\(t1\)/);
  });
});

describe("scripted scenarios", () => {
  // Both transfers read the balances before either writes: t2 overwrites bob with a
  // stale balance, losing t1's deposit (300 -> 290).
  const bothReadFirst = [
    { deliver: { kind: "execute", subject: "t1" }, to: "w1" },
    { deliver: { subject: under("t1") }, count: 4 },
    { deliver: { kind: "execute", subject: "t1:0" }, to: "w2" },
    { deliver: { subject: under("t1") }, count: 4 },
    { run: { subject: "t1:0" } },
    { resume: { subject: "t1:0" } },
    { deliver: { subject: under("t1") }, count: 2 },
    { deliver: { kind: "execute", subject: "t2" }, to: "w2" },
    { deliver: { subject: under("t2") }, count: 4 },
    { deliver: { kind: "execute", subject: "t2:0" }, to: "w1" },
    { deliver: { subject: under("t2") }, count: 4 },
    { run: { subject: "t2:0" } },
    { resume: { subject: "t2:0" } },
    { deliver: { subject: under("t2") }, count: 2 },
  ] as const;

  test("a script drives an exact interleaving", async () => {
    const r = await simulate(racy, scripted([...bothReadFirst], { after: "stop" }));
    expect(r.status).toBe("stopped");
    expect(r.steps).toBe(28);
    expect(
      r.log.map((l) =>
        l.replace(/^\[\d+\] t=\d+ deliver #\d+(?: -> \w+)?: /, "").replace(/^\[\d+\] t=\d+ (\w+) leaf \d+/, "$1 leaf"),
      ),
    ).toEqual([
      "message execute(t1) server -> w1",
      "request task.acquire(t1) w1 -> server",
      "response task.acquire(t1) server -> w1",
      "request task.fence(t1) w1 -> server",
      "response task.fence(t1) server -> w1",
      "message execute(t1:0) server -> w2",
      "request task.suspend(t1) w1 -> server",
      "request task.acquire(t1:0) w2 -> server",
      "response task.suspend(t1) server -> w1",
      "response task.acquire(t1:0) server -> w2",
      "run leaf: readBalances(t1:0) on w2",
      "resume leaf: readBalances(t1:0) on w2",
      "request task.fulfill(t1:0) w2 -> server",
      "response task.fulfill(t1:0) server -> w2",
      "message execute(t2) server -> w2",
      "request task.acquire(t2) w2 -> server",
      "response task.acquire(t2) server -> w2",
      "request task.fence(t2) w2 -> server",
      "response task.fence(t2) server -> w2",
      "message execute(t2:0) server -> w1",
      "request task.suspend(t2) w2 -> server",
      "request task.acquire(t2:0) w1 -> server",
      "response task.suspend(t2) server -> w2",
      "response task.acquire(t2:0) server -> w1",
      "run leaf: readBalances(t2:0) on w1",
      "resume leaf: readBalances(t2:0) on w1",
      "request task.fulfill(t2:0) w1 -> server",
      "response task.fulfill(t2:0) server -> w1",
    ]);
  });

  test("the scripted interleaving exposes the lost update", async () => {
    const r = await simulate(racy, scripted([...bothReadFirst]));
    expect(r.status).toBe("failed");
    expect(r.failure).toMatchObject({ kind: "check", message: "money not conserved: total is 290, expected 300" });
  });

  test("a scripted crash between a side effect and its record charges twice", async () => {
    const r = await simulate(
      payment("payNotIdempotent"),
      scripted([
        { deliver: { kind: "execute", subject: "o1" }, to: "w1" },
        { deliver: { subject: under("o1") }, count: 4 },
        { deliver: { kind: "execute", subject: "o1:0" }, to: "w2" },
        { deliver: { subject: under("o1") }, count: 3 },
        { deliver: { kind: "task.acquire", type: "response", subject: "o1:0" } },
        { run: { subject: "o1:0" } },
        // w2 has charged the card but has not seen the leaf's result yet.
        { crash: "w2" },
        { advance: true },
        { restart: "w2" },
      ]),
    );
    expect(r.status).toBe("failed");
    expect(r.failure?.message).toBe("order o1 charged 2 times");
    expect(decisions(r.tape).filter((e) => e.t === "crash")).toEqual([{ step: 12, t: "crash", worker: "w2" }]);
  });

  test("scripted values for Math.random reach user code through the tape", async () => {
    const scenario: Scenario<Bank> = { workers: 1, functions, world: bank, start: [{ id: "l", func: "lottery" }] };
    for (const [value, winner] of [
      [0.1, "alice"],
      [0.5, "bob"],
      [0.9, "carol"],
    ] as const) {
      const r = await simulate(scenario, scripted([{ random: [value] }]));
      expect(r.results.l).toEqual({ state: "resolved", value: winner });
      expect(r.tape.entries.filter((e) => e.t === "random").map((e) => (e as any).value)).toEqual([value]);
    }
  });

  test("a durable sleep only wakes when the script advances the clock", async () => {
    const scenario: Scenario<Bank> = {
      workers: 1,
      functions,
      world: bank,
      start: [{ id: "d", func: "delayedTransfer", args: ["alice", "bob", 1, 60_000] }],
    };
    const r = await simulate(scenario, scripted([{ settle: true }], { after: "stop" }));
    expect(r.status).toBe("stopped");
    expect(r.results.d.state).toBe("pending");
    expect(r.now).toBe(0);
    const done = await simulate(scenario, scripted([{ settle: true }, { advance: true }]));
    expect(done.status).toBe("completed");
    expect(done.now).toBe(60_000);
  });

  test("engine timers (retry backoff) fire only when the clock reaches them", async () => {
    const scenario: Scenario<Bank> = {
      workers: 1,
      functions,
      world: bank,
      start: [{ id: "r", func: "retryingTransfer", args: ["alice", "bob", 5] }],
      check: conserved,
    };
    const r = await simulate(scenario, scripted([{ settle: true }, { advance: true }, { fire: "worker-1" }]));
    expect(r.status).toBe("completed");
    expect(r.now).toBe(500);
    const d = decisions(r.tape);
    const advance = d.findIndex((e) => e.t === "advance");
    expect(d[advance]).toMatchObject({ t: "advance", to: 500 });
    expect(d[advance + 1].t).toBe("fire");
    const again = await simulate(scenario, replay(r.tape));
    expect(serializeTape(again.tape)).toBe(serializeTape(r.tape));
  });

  test("a script step that cannot happen is reported as a divergence", async () => {
    const r = await simulate(transfers(1), scripted([{ deliver: { kind: "task.fulfill" } }]));
    expect(r.failure).toMatchObject({ kind: "divergence", step: 1 });
    expect(r.failure?.message).toContain("no in-flight envelope matches");
  });
});

describe("seeded exploration and replay", () => {
  async function explore(scenario: Scenario<Bank>, faults = {}, seeds = 300) {
    for (let seed = 1; seed <= seeds; seed++) {
      const r = await simulate(scenario, seeded(seed, faults), { maxSteps: 5_000, requireCompletion: true });
      if (r.status === "failed") return { seed, r };
    }
    return undefined;
  }

  test("seeded exploration finds the planted interleaving bug", async () => {
    const found = await explore(racy);
    expect(found).toBeDefined();
    expect(found!.r.failure?.message).toMatch(/money not conserved/);
  });

  test("seeded exploration finds the planted crash bug, and not in the fixed workflow", async () => {
    const found = await explore(payment("payNotIdempotent"), { crash: 0.1 });
    expect(found).toBeDefined();
    expect(found!.r.failure?.message).toMatch(/^order o1 charged [2-9] times$/);
    expect(found!.r.tape.entries.some((e) => e.t === "crash")).toBe(true);

    // The same seed against the fixed workflow passes.
    const fixed = await simulate(payment("paySafe"), seeded(found!.seed, { crash: 0.1 }), {
      maxSteps: 5_000,
      requireCompletion: true,
    });
    expect(fixed.failure).toBeUndefined();
  });

  test("a failing tape replays to the same failure at the same step", async () => {
    for (const [scenario, faults] of [
      [racy, {}],
      [payment("payNotIdempotent"), { crash: 0.1 }],
    ] as const) {
      const found = await explore(scenario, faults);
      const text = serializeTape(found!.r.tape);
      const again = await simulate(scenario, replay(parseTape(text)));
      expect(again.failure).toEqual(found!.r.failure);
      expect(again.steps).toBe(found!.r.steps);
      expect(serializeTape(again.tape)).toBe(text);
      expect(again.log).toEqual(found!.r.log);
    }
  });

  test("replay reproduces random values and faults exactly", async () => {
    const scenario: Scenario<Bank> = {
      workers: 3,
      functions,
      world: bank,
      start: [
        { id: "l1", func: "lottery" },
        { id: "l2", func: "lottery" },
        { id: "d", func: "delayedTransfer", args: ["alice", "bob", 1, 5_000] },
      ],
      check: conserved,
    };
    const original = await simulate(scenario, seeded(5, { crash: 0.02, drop: 0.03, delay: 0.03 }), {
      maxSteps: 20_000,
    });
    expect(original.status).toBe("completed");
    expect(original.tape.entries.some((e) => e.t === "random")).toBe(true);
    const again = await simulate(scenario, replay(original.tape), { maxSteps: 20_000 });
    expect(again.results).toEqual(original.results);
    expect(serializeTape(again.tape)).toBe(serializeTape(original.tape));
  });

  test("a scripted run's tape replays identically too", async () => {
    const r = await simulate(racy, scripted([{ deliver: { kind: "execute", subject: "t2" }, to: "w2" }]));
    const again = await simulate(racy, replay(r.tape));
    expect(serializeTape(again.tape)).toBe(serializeTape(r.tape));
    expect(again.results).toEqual(r.results);
  });

  test("a tampered tape is detected as a divergence", async () => {
    const r = await simulate(transfers(2), seeded(9));
    const tape = parseTape(serializeTape(r.tape));
    const i = tape.entries.findIndex((e) => e.t === "deliver" && e.to);
    (tape.entries[i] as any).to = "nobody";
    const again = await simulate(transfers(2), replay(tape));
    expect(again.failure).toMatchObject({ kind: "divergence", step: tape.entries[i].step });
  });
});

describe("leaves under the global scheduler", () => {
  // `increment` reads the counter in one async leaf and writes it in another.
  // `twice` runs two increments in parallel on one worker, so no crash, drop or
  // timeout is needed: the update is lost only when the scheduler runs both
  // reads before either read's result is resumed.
  const race = (atomic: boolean): Scenario<Counter> => ({
    workers: 1,
    functions: counterFunctions,
    world: counter,
    start: [{ id: "c", func: "twice", args: [atomic] }],
    check: counted,
  });
  // No early clock advances, so no request times out and nothing re-executes.
  const ordersOnly = { advanceEarly: 0 };

  test("a leaf's execution and the delivery of its result are separate decisions", async () => {
    const r = await simulate(race(false), seeded(1, ordersOnly));
    const leafSteps = decisions(r.tape).filter((e) => e.t === "run" || e.t === "resume");
    const ids = new Set(leafSteps.map((e) => (e as any).leaf));
    expect(ids.size).toBe(4);
    for (const id of ids) {
      const steps = leafSteps.filter((e) => (e as any).leaf === id).map((e) => e.t);
      expect(steps).toEqual(["run", "resume"]);
    }
  });

  test("resuming each read before the other read runs keeps both updates", async () => {
    const r = await simulate(
      race(false),
      scripted([
        { run: { func: "getCount", subject: "c:0.0" } },
        { resume: { subject: "c:0.0" } },
        { run: { func: "setCount", subject: "c:0.1" } },
        { resume: { subject: "c:0.1" } },
        { run: { func: "getCount", subject: "c:1.0" } },
        { resume: { subject: "c:1.0" } },
      ]),
    );
    // `counted` checks the counter reached 2.
    expect(r.status).toBe("completed");
    expect(r.failure).toBeUndefined();
  });

  test("running both reads before resuming either loses an update", async () => {
    const r = await simulate(
      race(false),
      scripted([
        { run: { func: "getCount", subject: "c:0.0" } },
        { run: { func: "getCount", subject: "c:1.0" } },
        { resume: { subject: "c:0.0" } },
        { resume: { subject: "c:1.0" } },
      ]),
    );
    expect(r.status).toBe("failed");
    expect(r.failure).toMatchObject({ kind: "check", message: "lost update: count is 1, expected 2" });
  });

  test("seeded exploration finds the parallel-leaf race, replay reproduces it at the same step", async () => {
    let found: { seed: number; r: Result } | undefined;
    for (let seed = 1; seed <= 50 && !found; seed++) {
      const r = await simulate(race(false), seeded(seed, ordersOnly), { requireCompletion: true });
      if (r.status === "failed") found = { seed, r };
    }
    expect(found).toBeDefined();
    const { r } = found!;
    expect(r.failure).toMatchObject({ kind: "check", message: "lost update: count is 1, expected 2" });
    // Only scheduling decisions: no faults and no timeouts were involved.
    expect(decisions(r.tape).every((e) => ["deliver", "run", "resume"].includes(e.t))).toBe(true);

    const again = await simulate(race(false), replay(parseTape(serializeTape(r.tape))));
    expect(again.failure).toEqual(r.failure);
    expect(again.steps).toBe(r.steps);
    expect(serializeTape(again.tape)).toBe(serializeTape(r.tape));
  });

  test("the atomic increment survives the same exploration", async () => {
    for (let seed = 1; seed <= 50; seed++) {
      const r = await simulate(race(true), seeded(seed, ordersOnly), { requireCompletion: true });
      expect(r.status).toBe("completed");
    }
  });

  test("a leaf that leaves async work running is reported", async () => {
    const r = await simulate(
      { workers: 1, functions: counterFunctions, world: counter, start: [{ id: "l", func: "callsLeaky" }] },
      seeded(1),
    );
    expect(r.failure?.kind).toBe("leaf");
    expect(r.failure?.message).toContain("returned with async work still running");
  });

  test("a leaf that never settles is reported", async () => {
    const r = await simulate(
      {
        workers: 1,
        functions: counterFunctions,
        world: counter,
        start: [{ id: "s", func: "callsStuck" }],
        leafTimeout: 200,
      },
      seeded(1),
    );
    expect(r.failure).toMatchObject({ kind: "leaf", message: "leaf stuck (s:0) did not settle within 200ms" });
  });
});
