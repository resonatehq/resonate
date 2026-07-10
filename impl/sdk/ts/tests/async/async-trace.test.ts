// Lifecycle-trace tests for the async/await engine.
//
// The async engine emits the spawn/run/rpc/block/dedup/return/suspend subset of
// the spec's event model — but NOT await/resume: a user's bare `await` on a
// durable handle is opaque to the engine (there is no yield to intercept). So we
// validate each trace with `isWellFormedLifecycle` (everything except the
// await→resume/suspend pairing) and additionally assert no await/resume events
// are ever emitted.
//
// Mirrors tests/trace.test.ts, but drives `AsyncCore.executeUntilBlocked`
// directly (the async engine folds the per-task driver into the core, so there
// is no separate Computation class to call). A root promise + acquired task are
// seeded via `task.create`; children/settles flow through the real local server.

import { randomUUID } from "node:crypto";
import { describe, expect, test } from "@jest/globals";
import { Core } from "../../src/async/core.js";
import type { Context, Info } from "../../src/async/index.js";
import { WallClock } from "../../src/clock.js";
import { Codec } from "../../src/codec.js";
import { NoopHeartbeat } from "../../src/heartbeat.js";
import { ConsoleLogger } from "../../src/logger.js";
import { LocalNetwork } from "../../src/network/local.js";
import { isSuccess, type PromiseRecord, type TaskRecord } from "../../src/network/types.js";
import { OptionsBuilder } from "../../src/options.js";
import { Registry } from "../../src/registry.js";
import { Never } from "../../src/retries.js";
import { dedupIsSole, exclusiveLifecycle, isWellFormedLifecycle, type Trace } from "../../src/trace.js";
import type { Effects } from "../../src/types.js";
import * as util from "../../src/util.js";

const PID = "trace-pid";
const TTL = 60_000;
const codec = new Codec();

type AnyFunc = (...args: any[]) => any;
type Status = Awaited<ReturnType<Core["executeUntilBlocked"]>>;

function registryOf(fns: Record<string, AnyFunc>): Registry {
  const r = new Registry();
  for (const [name, fn] of Object.entries(fns)) r.add(fn, name, 1);
  return r;
}

function newCore(registry: Registry, network: LocalNetwork): Core {
  return new Core({
    pid: PID,
    ttl: TTL,
    clock: new WallClock(),
    send: network.send,
    codec,
    registry,
    heartbeat: new NoopHeartbeat(),
    dependencies: new Map(),
    optsBuilder: new OptionsBuilder({ match: (target: string) => target, idPrefix: "" }),
    logger: new ConsoleLogger("error"),
  });
}

/** Seed the root promise + its acquired task in one shot (what the server does
 * when a root is created with a target). Returns the decoded root + the task. */
async function seedRoot(
  network: LocalNetwork,
  id: string,
  func: string,
  args: unknown[],
): Promise<{ task: TaskRecord; promise: PromiseRecord; preload: PromiseRecord[] }> {
  const now = Date.now();
  const res = await network.send({
    kind: "task.create",
    head: { corrId: randomUUID(), version: util.VERSION },
    data: {
      pid: PID,
      ttl: TTL,
      action: {
        kind: "promise.create",
        head: { corrId: randomUUID(), version: util.VERSION },
        data: {
          id,
          param: codec.encode({ func, args, version: 1 }),
          tags: { "resonate:target": "default" },
          timeoutAt: now + TTL,
        },
      },
    },
  });
  if (!isSuccess(res) || !res.data.task) {
    throw new Error(`task.create failed: ${res.head.status} ${JSON.stringify(res.data)}`);
  }
  return { task: res.data.task, promise: codec.decodePromise(res.data.promise), preload: res.data.preload };
}

/** Pre-create + settle a durable promise (via the fenced effects path, the only
 * one whose responses round-trip `isResponse`) so a later create dedups it. */
async function presettle(effects: Effects, id: string, value: unknown): Promise<void> {
  const now = Date.now();
  await effects.promiseCreate(
    {
      kind: "promise.create",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: { id, timeoutAt: now + TTL, param: { headers: {}, data: {} }, tags: { "resonate:scope": "global" } },
    },
    "presettle",
  );
  await effects.promiseSettle(
    {
      kind: "promise.settle",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: { id, state: "resolved", value: { headers: {}, data: value } },
    },
    "presettle",
  );
}

/** Run a registered root to its first done/suspend and return the status. The
 * root id equals its registered name. */
async function runRoot(fns: Record<string, AnyFunc>, root: string, args: unknown[] = []): Promise<Status> {
  const network = new LocalNetwork({ pid: PID, group: "default" });
  try {
    const core = newCore(registryOf(fns), network);
    const { task, promise, preload } = await seedRoot(network, root, root, args);
    return await core.executeUntilBlocked(task, promise, preload);
  } finally {
    await network.stop();
  }
}

function counts(t: Trace) {
  const by = (kind: string) => t.filter((e) => e.kind === kind);
  return {
    spawn: by("spawn"),
    run: by("run"),
    rpc: by("rpc"),
    block: by("block"),
    dedup: by("dedup"),
    return: by("return"),
    suspend: by("suspend"),
    await: by("await"),
    resume: by("resume"),
  };
}

/** Every async-engine trace must be lifecycle-well-formed and contain no
 * await/resume events (those are unobservable to the async engine). */
function expectLifecycleWellFormed(t: Trace): void {
  expect(isWellFormedLifecycle(t)).toBe(true);
  const c = counts(t);
  expect(c.await.length).toBe(0);
  expect(c.resume.length).toBe(0);
}

const opts = (ctx: Context) => ctx.options({ retryPolicy: new Never() });

describe("async engine lifecycle trace", () => {
  test("single local run: spawn root, run, spawn child, return child, return root", async () => {
    const fns = {
      add: async (_info: Info, a: number, b: number): Promise<number> => a + b,
      main: async (ctx: Context): Promise<number> => ctx.run<number>("add", 3, 4, opts(ctx)),
    };
    const res = await runRoot(fns, "main");

    expect(res.kind).toBe("done");
    const t = res.trace;

    // Root spawn is first.
    expect(t[0]).toEqual({ kind: "spawn", id: "main" });

    // run main -> main.0
    const run = t.find((e) => e.kind === "run");
    expect(run).toEqual({ kind: "run", id: "main", callee: "main.0" });

    // Child spawns once and returns 7.
    expect(counts(t).spawn.filter((e) => e.id !== "main").length).toBe(1);
    const childReturn = t.find((e) => e.kind === "return" && e.id === "main.0");
    expect(childReturn).toEqual({ kind: "return", id: "main.0", state: "resolved", value: 7 });

    // Root return is last.
    expect(t[t.length - 1]).toEqual({ kind: "return", id: "main", state: "resolved", value: 7 });

    expectLifecycleWellFormed(t);
  });

  test("recursive fib (all local) produces a balanced spawn/return tree", async () => {
    const fns = {
      fib: async (ctx: Context, n: number): Promise<number> => {
        if (n <= 1) return n;
        const a = await ctx.run<number>("fib", n - 1, opts(ctx));
        const b = await ctx.run<number>("fib", n - 2, opts(ctx));
        return a + b;
      },
    };
    const res = await runRoot(fns, "fib", [5]);

    expect(res.kind).toBe("done");
    if (res.kind === "done") expect(res.value).toBe(5);
    const c = counts(res.trace);

    // Every spawned promise returns exactly once; one run per parent->child edge.
    expect(c.spawn.length).toBe(c.return.length);
    expect(c.run.length).toBe(c.spawn.length - 1); // root has no inbound run
    expect(c.rpc.length).toBe(0);
    expect(c.suspend.length).toBe(0);

    expectLifecycleWellFormed(res.trace);
  });

  test("eager fan-out: one run+spawn+return per child, root returns", async () => {
    const fns = {
      sq: async (_info: Info, n: number): Promise<number> => n * n,
      fanout: async (ctx: Context, items: number[]): Promise<number[]> =>
        Promise.all(items.map((n) => ctx.run<number>("sq", n, opts(ctx)))),
    };
    const res = await runRoot(fns, "fanout", [[1, 2, 3, 4]]);

    expect(res.kind).toBe("done");
    if (res.kind === "done") expect(res.value).toEqual([1, 4, 9, 16]);
    const c = counts(res.trace);

    expect(c.run.length).toBe(4);
    expect(c.spawn.length).toBe(5); // root + 4 children
    expect(c.return.length).toBe(5);

    expectLifecycleWellFormed(res.trace);
  });

  test("uncaught child error rejects the root", async () => {
    const fns = {
      boom: async (_info: Info): Promise<void> => {
        throw new Error("kaboom");
      },
      main: async (ctx: Context): Promise<void> => {
        await ctx.run<void>("boom", opts(ctx));
      },
    };
    const res = await runRoot(fns, "main");

    expect(res.kind).toBe("done");
    if (res.kind === "done") expect(res.state).toBe("rejected");

    const childReturn = res.trace.find((e) => e.kind === "return" && e.id === "main.0");
    expect(childReturn?.kind === "return" && childReturn.state).toBe("rejected");
    const rootReturn = res.trace[res.trace.length - 1];
    expect(rootReturn.kind === "return" && rootReturn.state).toBe("rejected");

    expectLifecycleWellFormed(res.trace);
  });

  test("caught child error: child returns rejected, root resolves", async () => {
    const fns = {
      boom: async (_info: Info): Promise<void> => {
        throw new Error("kaboom");
      },
      main: async (ctx: Context): Promise<string> => {
        try {
          await ctx.run<void>("boom", opts(ctx));
          return "no-throw";
        } catch (e) {
          return `caught:${(e as Error).message}`;
        }
      },
    };
    const res = await runRoot(fns, "main");

    expect(res.kind).toBe("done");
    if (res.kind === "done") expect(res.value).toBe("caught:kaboom");

    const childReturn = res.trace.find((e) => e.kind === "return" && e.id === "main.0");
    expect(childReturn?.kind === "return" && childReturn.state).toBe("rejected");
    const rootReturn = res.trace[res.trace.length - 1];
    expect(rootReturn).toMatchObject({ kind: "return", id: "main", state: "resolved" });

    expectLifecycleWellFormed(res.trace);
  });

  test("single rpc: spawn, rpc, block, suspend (no await/resume)", async () => {
    const fns = {
      main: async (ctx: Context): Promise<number> => ctx.rpc<number>("remote", opts(ctx)),
    };
    const res = await runRoot(fns, "main");

    expect(res.kind).toBe("suspended");
    if (res.kind === "suspended") expect(res.awaited).toEqual(["main.0"]);
    const t = res.trace;

    expect(t[0]).toEqual({ kind: "spawn", id: "main" });
    expect(t.find((e) => e.kind === "rpc")).toEqual({ kind: "rpc", id: "main", callee: "main.0" });
    expect(t.find((e) => e.kind === "block")).toEqual({ kind: "block", id: "main.0" });
    expect(t[t.length - 1]).toEqual({ kind: "suspend", id: "main" });

    expectLifecycleWellFormed(t);
  });

  test("mixed run + rpc: local child returns, remote child blocks, root suspends", async () => {
    const fns = {
      compute: async (_info: Info): Promise<number> => 42,
      main: async (ctx: Context): Promise<number> => {
        const local = ctx.run<number>("compute", opts(ctx));
        const remote = ctx.rpc<number>("remote", opts(ctx));
        return (await local) + (await remote);
      },
    };
    const res = await runRoot(fns, "main");

    expect(res.kind).toBe("suspended");
    const c = counts(res.trace);

    expect(c.run.length).toBe(1);
    expect(c.rpc.length).toBe(1);
    expect(c.spawn.filter((e) => e.id !== "main").length).toBe(1); // only the local child spawns
    expect(c.return.filter((e) => e.id !== "main").length).toBe(1); // local child returns
    expect(c.block.length).toBe(1); // remote child blocks
    expect(c.suspend).toEqual([{ kind: "suspend", id: "main" }]);

    expectLifecycleWellFormed(res.trace);
  });

  test("dedup root: an already-settled boundary promise emits a single dedup", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const core = newCore(registryOf({ main: async (_ctx: Context): Promise<number> => 42 }), network);
      const { task } = await seedRoot(network, "host", "main", []);
      const now = Date.now();
      const root: PromiseRecord = {
        id: "main",
        state: "resolved",
        param: { headers: {}, data: { func: "main", args: [], version: 1 } as any },
        value: { headers: {}, data: 42 },
        tags: { "resonate:target": "default" },
        timeoutAt: now + TTL,
        createdAt: now,
      };
      const res = await core.executeUntilBlocked(task, root, []);

      expect(res.kind).toBe("done");
      expect(res.trace.length).toBe(1);
      expect(res.trace[0]).toMatchObject({ kind: "dedup", id: "main", state: "resolved" });
      // A dedup-only root does not start with spawn, so rootSpawn (and thus the
      // full lifecycle check) does not apply — only these subset predicates do.
      expect(dedupIsSole(res.trace)).toBe(true);
      expect(exclusiveLifecycle(res.trace)).toBe(true);
    } finally {
      await network.stop();
    }
  });

  test("dedup child: a run against an already-settled child emits dedup, not spawn", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const fns = {
        compute: async (_info: Info): Promise<number> => 1, // not invoked; child is pre-settled
        main: async (ctx: Context): Promise<number> => ctx.run<number>("compute", opts(ctx)),
      };
      const core = newCore(registryOf(fns), network);
      const { task, promise, preload } = await seedRoot(network, "main", "main", []);

      // Pre-settle the child (main.0) so the engine's create dedups against it.
      const effects = util.buildEffects(network.send, codec, { id: task.id, version: task.version });
      await presettle(effects, "main.0", 99);

      const res = await core.executeUntilBlocked(task, promise, preload);

      expect(res.kind).toBe("done");
      if (res.kind === "done") expect(res.value).toBe(99); // deduped to the stored value

      const c = counts(res.trace);
      expect(c.run).toEqual([{ kind: "run", id: "main", callee: "main.0" }]);
      expect(c.dedup.length).toBe(1);
      expect(c.dedup[0]).toMatchObject({ kind: "dedup", id: "main.0", state: "resolved" });
      expect(c.spawn.filter((e) => e.id === "main.0").length).toBe(0); // deduped, never spawned

      expectLifecycleWellFormed(res.trace);
    } finally {
      await network.stop();
    }
  });

  test("detached: takes the remote path (rpc + block) but never suspends", async () => {
    const fns = {
      dchild: async (_info: Info, n: number): Promise<number> => n * 10,
      main: async (ctx: Context): Promise<string> => {
        const h = await ctx.detached("dchild", 5, opts(ctx));
        return h.id;
      },
    };
    const res = await runRoot(fns, "main");

    expect(res.kind).toBe("done");
    const c = counts(res.trace);

    // The detached child is created remotely (rpc + block) and is never run by
    // this computation, so the root still returns without suspending.
    expect(c.rpc.length).toBe(1);
    expect(c.block.length).toBe(1);
    expect(c.suspend.length).toBe(0);
    expect(t0AfterRpcIsBlockOfSameId(res.trace)).toBe(true);
    expect(res.trace[res.trace.length - 1]).toMatchObject({ kind: "return", id: "main", state: "resolved" });

    expectLifecycleWellFormed(res.trace);
  });
});

/** The rpc's callee is exactly the id that gets blocked (detached child). */
function t0AfterRpcIsBlockOfSameId(t: Trace): boolean {
  const rpc = t.find((e) => e.kind === "rpc");
  const block = t.find((e) => e.kind === "block");
  return !!rpc && rpc.kind === "rpc" && !!block && block.kind === "block" && rpc.callee === block.id;
}
