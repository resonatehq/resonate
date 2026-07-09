// Task-lifecycle tests for the async engine's AsyncCore.
//
// Mirrors tests/core.test.ts. The generator Core exposes a `createComputation`
// seam those tests mock; AsyncCore folds the per-task driver in as
// `executeUntilBlockedInner` (no Computation class), so these tests register
// real async functions and assert on the network traffic instead: the same
// task.fulfill / task.release / task.suspend / task.fence contract, observed
// through an intercepted send.

import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, jest, test } from "@jest/globals";
import { AsyncCore } from "../../src/async/core.js";
import type { Context, Info } from "../../src/async/index.js";
import { WallClock } from "../../src/clock.js";
import { Codec } from "../../src/codec.js";
import { NoopHeartbeat } from "../../src/heartbeat.js";
import { ConsoleLogger } from "../../src/logger.js";
import { LocalNetwork } from "../../src/network/local.js";
import { isSuccess, type PromiseRecord, type Request, type TaskRecord } from "../../src/network/types.js";
import { OptionsBuilder } from "../../src/options.js";
import { Registry } from "../../src/registry.js";
import type { Send } from "../../src/types.js";
import { VERSION } from "../../src/util.js";

const PID = "test-pid";
const TTL = 60_000;
const codec = new Codec();

type AnyFunc = (...args: any[]) => any;

let network: LocalNetwork | undefined;

afterEach(async () => {
  await network?.stop();
  network = undefined;
  jest.restoreAllMocks();
});

function buildCore(fns: Record<string, AnyFunc>): {
  core: AsyncCore;
  network: LocalNetwork;
  sendHolder: { fn: Send };
} {
  network = new LocalNetwork({ pid: PID, group: "default" });

  // Mutable holder so interceptSend can swap the inner function
  const sendHolder = { fn: network.send as Send };
  const proxiedSend: Send = (req: any) => sendHolder.fn(req);

  const registry = new Registry();
  for (const [name, fn] of Object.entries(fns)) registry.add(fn, name, 1);

  const core = new AsyncCore({
    pid: PID,
    ttl: TTL,
    clock: new WallClock(),
    send: proxiedSend,
    codec,
    registry,
    heartbeat: new NoopHeartbeat(),
    dependencies: new Map(),
    optsBuilder: new OptionsBuilder({ match: (t: string) => t, idPrefix: "test-" }),
    logger: new ConsoleLogger("error"),
  });

  return { core, network, sendHolder };
}

async function seedAcquiredTask(
  send: Send,
  id: string,
  func: string,
  args: any[],
): Promise<{ task: TaskRecord; rootPromise: PromiseRecord }> {
  const encoded = codec.encode({ func, args, version: 1 });

  const res = await send({
    kind: "task.create",
    head: { corrId: randomUUID(), version: VERSION },
    data: {
      pid: PID,
      ttl: TTL,
      action: {
        kind: "promise.create",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          id,
          param: encoded,
          tags: { "resonate:target": "default" },
          timeoutAt: Date.now() + TTL,
        },
      },
    },
  });
  if (!isSuccess(res) || res.data.task === undefined) {
    throw new Error(`Failed to create task: ${res.head.status}`);
  }
  return { task: res.data.task, rootPromise: codec.decodePromise(res.data.promise) };
}

async function seedPendingTask(
  send: Send,
  id: string,
  func: string,
  args: any[],
  network: LocalNetwork,
): Promise<TaskRecord> {
  const { task } = await seedAcquiredTask(send, id, func, args);
  const releaseRes = await send({
    kind: "task.release",
    head: { corrId: randomUUID(), version: VERSION },
    data: { id: task.id, version: task.version },
  });
  if (releaseRes.head.status !== 200) {
    throw new Error(`Failed to release task: ${releaseRes.head.status}`);
  }
  const serverTask = (network as any).server?.tasks?.get(task.id);
  return {
    id: task.id,
    state: "pending",
    version: serverTask?.version ?? task.version + 1,
    resumes: [...serverTask.resumes],
  };
}

function interceptSend(sendHolder: { fn: Send }): { sent: Request[] } {
  const sent: Request[] = [];
  const origSend = sendHolder.fn;
  sendHolder.fn = ((req: any) => {
    sent.push(req);
    return origSend(req);
  }) as Send;
  return { sent };
}

describe("AsyncCore", () => {
  describe("executeUntilBlocked", () => {
    test("function resolves - sends task.fulfill with resolved value", async () => {
      const { core, sendHolder } = buildCore({
        main: async (_info: Info): Promise<number> => 42,
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "p1", "main", []);
      const { sent } = interceptSend(sendHolder);

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("done");
      if (res.kind === "done") {
        expect(res.state).toBe("resolved");
        expect(res.value).toBe(42);
      }

      const fulfill = sent.find((r) => r.kind === "task.fulfill");
      expect(fulfill).toBeDefined();
      if (fulfill && fulfill.kind === "task.fulfill") {
        expect(fulfill.data.action.data.state).toBe("resolved");
      }
    });

    test("function rejects - sends task.fulfill with rejected value", async () => {
      const { core, sendHolder } = buildCore({
        main: async (_info: Info): Promise<never> => {
          throw new Error("err");
        },
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "p2", "main", []);
      const { sent } = interceptSend(sendHolder);

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("done");
      if (res.kind === "done") expect(res.state).toBe("rejected");
      const fulfill = sent.find((r) => r.kind === "task.fulfill");
      expect(fulfill).toBeDefined();
      if (fulfill && fulfill.kind === "task.fulfill") {
        expect(fulfill.data.action.data.state).toBe("rejected");
      }
    });

    test("execution errors (unregistered root) - sends task.release", async () => {
      jest.spyOn(console, "error").mockImplementation(() => {});
      jest.spyOn(console, "warn").mockImplementation(() => {});
      const { core, sendHolder } = buildCore({
        main: async (_info: Info): Promise<number> => 0,
      });

      // The root promise names a function the registry does not know — the pass
      // fails at the engine level (no user function ran), which must release the
      // task instead of settling anything.
      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "p3", "missing", []);
      const { sent } = interceptSend(sendHolder);

      await expect(core.executeUntilBlocked(task, rootPromise)).rejects.toThrow();
      const release = sent.find((r) => r.kind === "task.release");
      expect(release).toBeDefined();
      expect(sent.some((r) => r.kind === "task.fulfill")).toBe(false);
    });

    test("function suspends - sends task.suspend with awaited IDs", async () => {
      const { core, sendHolder } = buildCore({
        main: async (ctx: Context): Promise<void> => {
          await Promise.all([ctx.rpc<number>("depFunc", 1), ctx.rpc<number>("depFunc", 2)]);
        },
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "p4", "main", []);
      const { sent } = interceptSend(sendHolder);

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("suspended");
      if (res.kind === "suspended") {
        expect(res.awaited).toContain("p4.0");
        expect(res.awaited).toContain("p4.1");
      }

      const suspend = sent.find((r) => r.kind === "task.suspend");
      expect(suspend).toBeDefined();
      if (suspend && suspend.kind === "task.suspend") {
        const awaitedIds = suspend.data.actions.map((a) => a.data.awaited);
        expect(awaitedIds).toContain("p4.0");
        expect(awaitedIds).toContain("p4.1");
      }
    });

    test("suspend with redirect (continue) - re-executes and completes via replay", async () => {
      let invocations = 0;
      const { core, sendHolder } = buildCore({
        main: async (ctx: Context): Promise<number> => {
          invocations++;
          return await ctx.rpc<number>("depFunc", 1);
        },
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "p5", "main", []);

      // Redirect the suspend: settle the awaited promise behind the engine's
      // back, then answer 300/continue so the same acquired task replays.
      const origFn = sendHolder.fn;
      sendHolder.fn = (async (req: any) => {
        if (req.kind === "task.suspend") {
          await origFn({
            kind: "promise.settle",
            head: { corrId: randomUUID(), version: VERSION },
            data: { id: "p5.0", state: "resolved", value: codec.encode(7) },
          });
          return {
            kind: "task.suspend",
            head: { corrId: randomUUID(), status: 300, version: VERSION },
            data: { preload: [] },
          };
        }
        return origFn(req);
      }) as Send;

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("done");
      if (res.kind === "done") {
        expect(res.state).toBe("resolved");
        expect(res.value).toBe(7);
      }
      expect(invocations).toBe(2);
    });
  });

  describe("onMessage", () => {
    test("acquires task then delegates to executeUntilBlocked", async () => {
      let invocations = 0;
      const { core, network, sendHolder } = buildCore({
        main: async (_info: Info): Promise<string> => {
          invocations++;
          return "ok";
        },
      });

      const task = await seedPendingTask(sendHolder.fn, "on-msg-1", "main", [], network);

      const res = await core.onMessage({ kind: "execute", head: {}, data: { task } });

      expect(invocations).toBe(1);
      expect(res?.kind).toBe("done");
    });

    test("acquire failure (409) throws without hanging", async () => {
      let invocations = 0;
      const { core, sendHolder } = buildCore({
        main: async (_info: Info): Promise<number> => {
          invocations++;
          return 0;
        },
      });

      // The task is still acquired — a second acquire must fail.
      const { task } = await seedAcquiredTask(sendHolder.fn, "fail-acq", "main", []);

      await expect(
        core.onMessage({ kind: "execute", head: {}, data: { task: { id: task.id, version: task.version } } }),
      ).rejects.toThrow();

      expect(invocations).toBe(0);
    });

    test("fulfill encodes value via the codec", async () => {
      const { core, network, sendHolder } = buildCore({
        main: async (_info: Info): Promise<{ x: number }> => ({ x: 1 }),
      });

      const task = await seedPendingTask(sendHolder.fn, "encode-test", "main", [], network);
      const { sent } = interceptSend(sendHolder);

      await core.onMessage({ kind: "execute", head: {}, data: { task } });

      const fulfill = sent.find((r) => r.kind === "task.fulfill");
      expect(fulfill).toBeDefined();
      if (fulfill && fulfill.kind === "task.fulfill") {
        expect(fulfill.data.action.data.value.data).toBeDefined();
        expect(typeof fulfill.data.action.data.value.data).toBe("string");
      }
    });

    test("drops a message with an unexpected kind instead of executing", async () => {
      jest.spyOn(console, "warn").mockImplementation(() => {});
      let invocations = 0;
      const { core } = buildCore({
        main: async (_info: Info): Promise<void> => {
          invocations++;
        },
      });

      // The network is a trust boundary: an unexpected kind is warn-and-drop
      // (see diff-testing.md), never an assert/exit and never an execution.
      const res = await core.onMessage({ kind: "bogus", head: {}, data: {} } as any);

      expect(res).toBeUndefined();
      expect(invocations).toBe(0);
    });
  });

  describe("task.fence", () => {
    test("promise.create issued during execution is wrapped in task.fence with acquired task id and version", async () => {
      const { core, sendHolder } = buildCore({
        child: async (_info: Info): Promise<string> => "c",
        main: async (ctx: Context): Promise<string> => ctx.run<string>("child"),
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "fence-1", "main", []);
      const { sent } = interceptSend(sendHolder);

      await core.executeUntilBlocked(task, rootPromise);

      const fences = sent.filter((r): r is Extract<Request, { kind: "task.fence" }> => r.kind === "task.fence");
      const creates = fences.filter((f) => f.data.action.kind === "promise.create");
      expect(creates.length).toBe(1);
      const fence = creates[0];
      expect(fence.data.id).toBe(task.id);
      expect(fence.data.version).toBe(task.version);
      expect(fence.data.action.data.id).toBe("fence-1.0");

      // Ensure no bare promise.create was sent during execution
      expect(sent.some((r) => r.kind === "promise.create")).toBe(false);
    });

    test("promise.settle issued during execution is wrapped in task.fence", async () => {
      const { core, sendHolder } = buildCore({
        child: async (_info: Info): Promise<string> => "result",
        main: async (ctx: Context): Promise<string> => ctx.run<string>("child"),
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "fence-2", "main", []);
      const { sent } = interceptSend(sendHolder);

      await core.executeUntilBlocked(task, rootPromise);

      const fences = sent.filter((r): r is Extract<Request, { kind: "task.fence" }> => r.kind === "task.fence");
      const settles = fences.filter((f) => f.data.action.kind === "promise.settle");
      expect(settles.length).toBe(1);
      const fence = settles[0];
      expect(fence.data.id).toBe(task.id);
      expect(fence.data.version).toBe(task.version);
      expect(fence.data.action.data.id).toBe("fence-2.0");

      // No bare promise.settle should have been sent during execution
      expect(sent.some((r) => r.kind === "promise.settle")).toBe(false);
    });

    test("fence fails when task version is stale (another process reacquired)", async () => {
      jest.spyOn(console, "error").mockImplementation(() => {});
      jest.spyOn(console, "warn").mockImplementation(() => {});
      const { core, sendHolder } = buildCore({
        child: async (_info: Info): Promise<number> => 1,
        main: async (ctx: Context): Promise<number> => ctx.run<number>("child"),
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, "fence-3", "main", []);

      // Release and reacquire the task behind the SDK's back, bumping its version.
      const releaseRes = await sendHolder.fn({
        kind: "task.release",
        head: { corrId: randomUUID(), version: VERSION },
        data: { id: task.id, version: task.version },
      });
      expect(releaseRes.head.status).toBe(200);
      const reAcquire = await sendHolder.fn({
        kind: "task.acquire",
        head: { corrId: randomUUID(), version: VERSION },
        data: { id: task.id, version: task.version, pid: "other-pid", ttl: TTL },
      });
      expect(reAcquire.head.status).toBe(200);

      // The SDK still holds the old version — the fenced create rejects and the
      // whole pass fails without settling anything.
      const { sent } = interceptSend(sendHolder);
      await expect(core.executeUntilBlocked(task, rootPromise)).rejects.toThrow();
      expect(sent.some((r) => r.kind === "task.fulfill")).toBe(false);
    });

    test("fence is not used outside of an acquired task (no fence when no task)", async () => {
      // A bare promise.create outside any task context must not be fenced —
      // Core's fence wrapping is scoped to acquired-task execution.
      const { sendHolder } = buildCore({});
      const { sent } = interceptSend(sendHolder);

      const res = await sendHolder.fn({
        kind: "promise.create",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          id: "fence-4",
          timeoutAt: Date.now() + TTL,
          param: codec.encode({ func: "anything", args: [], version: 1 }),
          tags: {},
        },
      });
      expect(isSuccess(res)).toBe(true);

      expect(sent.some((r) => r.kind === "promise.create")).toBe(true);
      expect(sent.some((r) => r.kind === "task.fence")).toBe(false);
    });
  });
});
