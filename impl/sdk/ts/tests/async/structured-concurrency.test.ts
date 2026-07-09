// Structured-concurrency tests for the async/await engine's pass finalization.
//
// The drain in `run` (src/async/context.ts) must absorb durable ops started by
// continuations DURING the pass (a chained ctx.run behind an earlier op's
// await), and must refuse ops started AFTER the pass is finalized (a
// continuation parked on a non-durable await — timer, I/O — that outlives the
// pass). Before the re-drain + close fix, the former were invisible to the
// suspend decision and the latter ran as zombies racing task.suspend/fulfill.
//
// These workflows are deliberately OFF-CONTRACT (chained plain-promise IIFEs,
// `await null` hops, timer awaits): they pin how the runtime behaves when the
// only-await-durable-promises contract is violated.

import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { describe, expect, test } from "@jest/globals";
import { AsyncCore } from "../../src/async/core.js";
import type { Context, Info } from "../../src/async/index.js";
import { WallClock } from "../../src/clock.js";
import { Codec } from "../../src/codec.js";
import { NoopHeartbeat } from "../../src/heartbeat.js";
import { ConsoleLogger } from "../../src/logger.js";
import { LocalNetwork } from "../../src/network/local.js";
import { isSuccess, type PromiseRecord, type TaskRecord } from "../../src/network/types.js";
import { OptionsBuilder } from "../../src/options.js";
import { Registry } from "../../src/registry.js";
import type { Trace } from "../../src/trace.js";
import * as util from "../../src/util.js";

const PID = "sc-pid";
const TTL = 60_000;
const codec = new Codec();

type AnyFunc = (...args: any[]) => any;
type Status = Awaited<ReturnType<AsyncCore["executeUntilBlocked"]>>;

function registryOf(fns: Record<string, AnyFunc>): Registry {
  const r = new Registry();
  for (const [name, fn] of Object.entries(fns)) r.add(fn, name, 1);
  return r;
}

function newCore(registry: Registry, network: LocalNetwork): AsyncCore {
  return new AsyncCore({
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

/** Run a registered root (id == registered name) to its first done/suspend. */
async function runRoot(
  network: LocalNetwork,
  fns: Record<string, AnyFunc>,
  root: string,
  args: unknown[] = [],
): Promise<Status> {
  const core = newCore(registryOf(fns), network);
  const { task, promise, preload } = await seedRoot(network, root, root, args);
  return core.executeUntilBlocked(task, promise, preload);
}

async function getPromise(network: LocalNetwork, id: string): Promise<PromiseRecord | undefined> {
  const res = await network.send({
    kind: "promise.get",
    head: { corrId: randomUUID(), version: util.VERSION },
    data: { id },
  });
  return isSuccess(res) ? codec.decodePromise(res.data.promise) : undefined;
}

const returnsOf = (t: Trace) => t.filter((e) => e.kind === "return");

describe("async engine structured concurrency", () => {
  test("a fire-and-forget local is drained and settled before the pass completes", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const side = async (_i: Info) => "side-effect";
      const wf = async (ctx: Context) => {
        void ctx.run<string>("side"); // fire-and-forget: never awaited
        return "done";
      };

      const status = await runRoot(network, { "sc-fire": wf, side }, "sc-fire");

      // Mirrors the generator coroutine's flush-at-return: the unawaited local
      // still runs to completion inside the pass and is durably settled before
      // executeUntilBlocked returns.
      expect(status.kind).toBe("done");
      if (status.kind !== "done") return;
      expect(status.value).toBe("done");
      expect(returnsOf(status.trace).map((e) => e.id)).toContain("sc-fire.0");
      const rec = await getPromise(network, "sc-fire.0");
      expect(rec?.state).toBe("resolved");
      expect(rec?.value?.data).toBe("side-effect");
    } finally {
      await network.stop();
    }
  });

  test("a chained run started behind an earlier op's await is drained into the pass", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const leafA = async (_i: Info) => "a";
      const leafB = async (_i: Info, x: string) => `b:${x}`;
      const wf = async (ctx: Context) => {
        const timer = ctx.sleep(60_000); // sc-local.0 — pending remote, ends the pass
        const chained = (async () => {
          const x = await ctx.run<string>("leafA"); // sc-local.1
          return ctx.run<string>("leafB", x); // sc-local.2 — starts mid-drain
        })();
        await Promise.all([timer, chained]);
        return "done";
      };

      const status = await runRoot(network, { "sc-local": wf, leafA, leafB }, "sc-local");

      // The pass suspends on the timer only; the chained locals both completed.
      expect(status.kind).toBe("suspended");
      if (status.kind !== "suspended") return;
      expect(status.awaited).toEqual(["sc-local.0"]);

      // The chained run finished INSIDE the pass: its return event is in the
      // trace (not emitted after getTrace) and its durable promise is settled
      // before executeUntilBlocked returned (no settle racing task.suspend).
      expect(returnsOf(status.trace).map((e) => e.id)).toContain("sc-local.2");
      const rec = await getPromise(network, "sc-local.2");
      expect(rec?.state).toBe("resolved");
      expect(rec?.value?.data).toBe("b:a");
    } finally {
      await network.stop();
    }
  });

  test("a chained rpc started mid-drain contributes its remote todo to the suspend set", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const leafA = async (_i: Info) => "a";
      const wf = async (ctx: Context) => {
        const timer = ctx.sleep(60_000); // sc-remote.0
        const chained = (async () => {
          await ctx.run<string>("leafA"); // sc-remote.1
          return ctx.rpc<string>("remoteB"); // sc-remote.2 — remote, pending
        })();
        await Promise.all([timer, chained]);
      };

      const status = await runRoot(network, { "sc-remote": wf, leafA }, "sc-remote");

      // Before the re-drain fix the suspend set was only the timer; the rpc's
      // todo was lost (self-healing on resume, but a missed callback).
      expect(status.kind).toBe("suspended");
      if (status.kind !== "suspended") return;
      expect([...status.awaited].sort()).toEqual(["sc-remote.0", "sc-remote.2"]);
    } finally {
      await network.stop();
    }
  });

  test("a continuation chained through non-durable microtask hops still lands in the pass", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const leafA = async (_i: Info) => "a";
      const leafB = async (_i: Info) => "b";
      const wf = async (ctx: Context) => {
        const timer = ctx.sleep(60_000); // sc-hops.0
        const chained = (async () => {
          await ctx.run<string>("leafA"); // sc-hops.1
          await null; // non-durable microtask hops between durable ops
          await null;
          await null;
          return ctx.run<string>("leafB"); // sc-hops.2
        })();
        await Promise.all([timer, chained]);
      };

      const status = await runRoot(network, { "sc-hops": wf, leafA, leafB }, "sc-hops");

      expect(status.kind).toBe("suspended");
      if (status.kind !== "suspended") return;
      expect(status.awaited).toEqual(["sc-hops.0"]);
      expect(returnsOf(status.trace).map((e) => e.id)).toContain("sc-hops.2");
      expect((await getPromise(network, "sc-hops.2"))?.state).toBe("resolved");
    } finally {
      await network.stop();
    }
  });

  test("an op started after a suspended pass ends panics and creates no durable promise", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      const leafA = async (_i: Info) => "a";
      let leaked: unknown;
      const wf = async (ctx: Context) => {
        void (async () => {
          await delay(60); // real timer — outlives the pass
          try {
            await ctx.run<string>("leafA");
          } catch (e) {
            leaked = e;
          }
        })();
        await ctx.sleep(60_000); // sc-zombie.0 — the pass suspends here
      };

      const status = await runRoot(network, { "sc-zombie": wf, leafA }, "sc-zombie");
      expect(status.kind).toBe("suspended");

      await delay(120);
      expect(String(leaked)).toContain("after the pass ended");
      // The panicked op never reached the server: no child promise was created.
      expect(await getPromise(network, "sc-zombie.1")).toBeUndefined();
    } finally {
      await network.stop();
    }
  });

  test("an op started after a completed pass ends panics and creates no durable promise", async () => {
    const network = new LocalNetwork({ pid: PID, group: "default" });
    try {
      let leaked: unknown;
      const wf = async (ctx: Context) => {
        void (async () => {
          await delay(60);
          try {
            await ctx.rpc<string>("anything");
          } catch (e) {
            leaked = e;
          }
        })();
        return 42;
      };

      const status = await runRoot(network, { "sc-done": wf }, "sc-done");
      expect(status.kind).toBe("done");
      if (status.kind !== "done") return;
      expect(status.value).toBe(42);

      await delay(120);
      expect(String(leaked)).toContain("after the pass ended");
      expect(await getPromise(network, "sc-done.0")).toBeUndefined();
    } finally {
      await network.stop();
    }
  });
});
