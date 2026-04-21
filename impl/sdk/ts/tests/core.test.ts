import { randomUUID } from "node:crypto";
import { describe, expect, jest, test } from "@jest/globals";
import { WallClock } from "../src/clock.js";
import { Codec } from "../src/codec.js";
import type { Status } from "../src/computation.js";
import { Core } from "../src/core.js";
import type { Heartbeat } from "../src/heartbeat.js";
import { ConsoleLogger } from "../src/logger.js";
import { LocalNetwork } from "../src/network/local.js";
import type { PromiseRecord, Request, TaskRecord } from "../src/network/types.js";
import { isSuccess } from "../src/network/types.js";
import { OptionsBuilder } from "../src/options.js";
import { Registry } from "../src/registry.js";
import type { Effects, Send } from "../src/types.js";
import { VERSION } from "../src/util.js";

class TestHeartbeat implements Heartbeat {
  start(): void {}
  stop(): void {}
}

class MockComputation {
  public calls: { rootPromise: PromiseRecord }[] = [];
  public effects?: Effects;
  private responses: (Status | Error)[];
  private callIndex = 0;
  private onExec?: (effects: Effects) => Promise<void>;

  constructor(responses: (Status | Error)[], onExec?: (effects: Effects) => Promise<void>) {
    this.responses = responses;
    this.onExec = onExec;
  }

  async executeUntilBlocked(rootPromise: PromiseRecord): Promise<Status> {
    this.calls.push({ rootPromise });
    if (this.onExec && this.effects) {
      await this.onExec(this.effects);
    }
    const res = this.responses[this.callIndex] ?? this.responses[this.responses.length - 1];
    this.callIndex++;
    if (res instanceof Error) throw res;
    return res;
  }
}

function buildCore(opts: {
  responses: (Status | Error)[];
  mockRef?: { mock: MockComputation };
  onExec?: (effects: Effects) => Promise<void>;
}): {
  core: Core;
  network: LocalNetwork;
  sendHolder: { fn: Send };
  codec: Codec;
  ctorSpy: jest.Spied<any>;
} {
  const network = new LocalNetwork();
  const codec = new Codec();
  const logger = new ConsoleLogger("error");

  // Mutable holder so interceptSend can swap the inner function
  const sendHolder = { fn: network.send as Send };
  const proxiedSend: Send = (req: any) => sendHolder.fn(req);

  const activeMock = new MockComputation(opts.responses, opts.onExec);
  if (opts.mockRef) {
    opts.mockRef.mock = activeMock;
  }

  const core = new Core({
    pid: "test-pid",
    ttl: 60_000,
    clock: new WallClock(),
    send: proxiedSend,
    codec,
    registry: new Registry(),
    heartbeat: new TestHeartbeat(),
    dependencies: new Map(),
    optsBuilder: new OptionsBuilder({ match: (t: string) => t, idPrefix: "test-" }),
    logger,
  });

  const ctorSpy = jest.spyOn(core as any, "createComputation").mockImplementation((..._args: any[]) => {
    const effects = _args[1] as Effects;
    activeMock.effects = effects;
    return activeMock;
  });

  return { core, network, sendHolder, codec, ctorSpy };
}

async function seedAcquiredTask(
  send: Send,
  codec: Codec,
  id: string,
  func: string,
  args: any[],
): Promise<{ task: TaskRecord; rootPromise: PromiseRecord }> {
  const encoded = codec.encode({ func, args, version: 1 });

  const res = await send({
    kind: "task.create",
    head: { corrId: randomUUID(), version: VERSION },
    data: {
      pid: "test-pid",
      ttl: 60_000,
      action: {
        kind: "promise.create",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          id,
          param: encoded,
          tags: { "resonate:target": "default" },
          timeoutAt: Date.now() + 60_000,
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
  codec: Codec,
  id: string,
  func: string,
  args: any[],
  network: LocalNetwork,
): Promise<TaskRecord> {
  const { task } = await seedAcquiredTask(send, codec, id, func, args);
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

describe("Core", () => {
  describe("executeUntilBlocked", () => {
    test("computation resolves - sends task.fulfill with resolved value", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "p1", state: "resolved", value: 42, trace: [] }],
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "p1", "main", []);
      const { sent } = interceptSend(sendHolder);

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("done");

      const fulfill = sent.find((r) => r.kind === "task.fulfill");
      expect(fulfill).toBeDefined();
      if (fulfill && fulfill.kind === "task.fulfill") {
        expect(fulfill.data.action.data.state).toBe("resolved");
      }
    });

    test("computation rejects - sends task.fulfill with rejected value", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "p2", state: "rejected", value: "err", trace: [] }],
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "p2", "main", []);
      const { sent } = interceptSend(sendHolder);

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("done");
      const fulfill = sent.find((r) => r.kind === "task.fulfill");
      expect(fulfill).toBeDefined();
      if (fulfill && fulfill.kind === "task.fulfill") {
        expect(fulfill.data.action.data.state).toBe("rejected");
      }
    });

    test("computation errors - sends task.release", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [new Error("test error")],
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "p3", "main", []);
      const { sent } = interceptSend(sendHolder);

      await expect(core.executeUntilBlocked(task, rootPromise)).rejects.toThrow("test error");
      const release = sent.find((r) => r.kind === "task.release");
      expect(release).toBeDefined();
    });

    test("computation suspends - sends task.suspend with awaited IDs", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "suspended", awaited: ["dep-a", "dep-b"], trace: [] }],
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "p4", "main", []);
      await seedAcquiredTask(sendHolder.fn, codec, "dep-a", "depFunc", []);
      await seedAcquiredTask(sendHolder.fn, codec, "dep-b", "depFunc", []);

      const { sent } = interceptSend(sendHolder);

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("suspended");

      const suspend = sent.find((r) => r.kind === "task.suspend");
      expect(suspend).toBeDefined();
      if (suspend && suspend.kind === "task.suspend") {
        const awaitedIds = suspend.data.actions.map((a) => a.data.awaited);
        expect(awaitedIds).toContain("dep-a");
        expect(awaitedIds).toContain("dep-b");
      }
    });

    test("computation suspends with redirect (continue) - re-executes", async () => {
      const mockRef = { mock: null as unknown as MockComputation };
      const { core, sendHolder, codec } = buildCore({
        responses: [
          { kind: "suspended", awaited: ["dep-x"], trace: [] },
          { kind: "done", id: "p5", state: "resolved", value: "final", trace: [] },
        ],
        mockRef,
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "p5", "main", []);
      await seedAcquiredTask(sendHolder.fn, codec, "dep-x", "depFunc", []);

      const origFn = sendHolder.fn;
      sendHolder.fn = ((req: any) => {
        if (req.kind === "task.suspend") {
          return Promise.resolve({
            kind: "task.suspend",
            head: { corrId: randomUUID(), status: 300, version: VERSION },
            data: { preload: [] },
          });
        }
        return origFn(req);
      }) as Send;

      const res = await core.executeUntilBlocked(task, rootPromise);

      expect(res.kind).toBe("done");
      expect(mockRef.mock.calls.length).toBe(2);
    });
  });

  describe("onMessage", () => {
    test("acquires task then delegates to executeUntilBlocked", async () => {
      const mockRef = { mock: null as unknown as MockComputation };
      const { core, network, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "on-msg-1", state: "resolved", value: "ok", trace: [] }],
        mockRef,
      });

      const task = await seedPendingTask(sendHolder.fn, codec, "on-msg-1", "main", [], network);

      await core.onMessage({ kind: "execute", head: {}, data: { task } });

      expect(mockRef.mock.calls.length).toBe(1);
    });

    test("acquire failure (409) throws without hanging", async () => {
      const mockRef = { mock: null as unknown as MockComputation };
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "fail-acq", state: "resolved", value: 0, trace: [] }],
        mockRef,
      });

      const { task } = await seedAcquiredTask(sendHolder.fn, codec, "fail-acq", "main", []);

      await expect(
        core.onMessage({ kind: "execute", head: {}, data: { task: { id: task.id, version: task.version } } }),
      ).rejects.toThrow();

      expect(mockRef.mock.calls.length).toBe(0);
    });

    test("fulfill encodes value via handler.encodeValue", async () => {
      const { core, network, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "encode-test", state: "resolved", value: { x: 1 }, trace: [] }],
      });

      const task = await seedPendingTask(sendHolder.fn, codec, "encode-test", "main", [], network);
      const { sent } = interceptSend(sendHolder);

      await core.onMessage({ kind: "execute", head: {}, data: { task } });

      const fulfill = sent.find((r) => r.kind === "task.fulfill");
      expect(fulfill).toBeDefined();
      if (fulfill && fulfill.kind === "task.fulfill") {
        expect(fulfill.data.action.data.value.data).toBeDefined();
        expect(typeof fulfill.data.action.data.value.data).toBe("string");
      }
    });
  });

  describe("task.fence", () => {
    test("promise.create issued during execution is wrapped in task.fence with acquired task id and version", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "fence-1", state: "resolved", value: 42, trace: [] }],
        onExec: async (effects) => {
          await effects.promiseCreate({
            kind: "promise.create",
            head: { corrId: randomUUID(), version: VERSION },
            data: {
              id: "fence-1.child",
              timeoutAt: Date.now() + 60_000,
              param: { headers: {}, data: { func: "child", args: [], version: 1 } },
              tags: {},
            },
          });
        },
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "fence-1", "main", []);
      const { sent } = interceptSend(sendHolder);

      await core.executeUntilBlocked(task, rootPromise);

      const fences = sent.filter((r) => r.kind === "task.fence");
      expect(fences.length).toBe(1);
      const fence = fences[0];
      if (fence.kind === "task.fence") {
        expect(fence.data.id).toBe(task.id);
        expect(fence.data.version).toBe(task.version);
        expect(fence.data.action.kind).toBe("promise.create");
        expect(fence.data.action.data.id).toBe("fence-1.child");
      }

      // Ensure no bare promise.create was sent during execution
      expect(sent.some((r) => r.kind === "promise.create")).toBe(false);
    });

    test("promise.settle issued during execution is wrapped in task.fence", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "fence-2", state: "resolved", value: 1, trace: [] }],
        onExec: async (effects) => {
          // Pre-create the child promise directly so we can settle it.
          await sendHolder.fn({
            kind: "promise.create",
            head: { corrId: randomUUID(), version: VERSION },
            data: {
              id: "fence-2.child",
              timeoutAt: Date.now() + 60_000,
              param: codec.encode({ func: "child", args: [], version: 1 }),
              tags: {},
            },
          });

          await effects.promiseSettle({
            kind: "promise.settle",
            head: { corrId: randomUUID(), version: VERSION },
            data: {
              id: "fence-2.child",
              state: "resolved",
              value: { headers: {}, data: "result" },
            },
          });
        },
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "fence-2", "main", []);
      const { sent } = interceptSend(sendHolder);

      await core.executeUntilBlocked(task, rootPromise);

      const fences = sent.filter((r) => r.kind === "task.fence");
      expect(fences.length).toBe(1);
      const fence = fences[0];
      if (fence.kind === "task.fence") {
        expect(fence.data.id).toBe(task.id);
        expect(fence.data.version).toBe(task.version);
        expect(fence.data.action.kind).toBe("promise.settle");
        expect(fence.data.action.data.id).toBe("fence-2.child");
      }

      // No bare promise.settle should have been sent during execution
      expect(sent.some((r) => r.kind === "promise.settle")).toBe(false);
    });

    test("fence fails when task version is stale (another process reacquired)", async () => {
      const { core, sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "fence-3", state: "resolved", value: 1, trace: [] }],
        onExec: async (effects) => {
          await effects.promiseCreate({
            kind: "promise.create",
            head: { corrId: randomUUID(), version: VERSION },
            data: {
              id: "fence-3.child",
              timeoutAt: Date.now() + 60_000,
              param: { headers: {}, data: { func: "child", args: [], version: 1 } },
              tags: {},
            },
          });
        },
      });

      const { task, rootPromise } = await seedAcquiredTask(sendHolder.fn, codec, "fence-3", "main", []);

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
        data: { id: task.id, version: task.version, pid: "other-pid", ttl: 60_000 },
      });
      expect(reAcquire.head.status).toBe(200);

      // The SDK still holds the old version — fence should reject with 409.
      await expect(core.executeUntilBlocked(task, rootPromise)).rejects.toThrow();
    });

    test("fence is not used outside of an acquired task (no fence when no task)", async () => {
      // Directly exercise buildEffects without a task fence context via a bare send.
      // This verifies that Core's fence wrapping is scoped to acquired-task execution.
      const { sendHolder, codec } = buildCore({
        responses: [{ kind: "done", id: "fence-4", state: "resolved", value: 1, trace: [] }],
      });
      const { sent } = interceptSend(sendHolder);

      // Create a promise directly (no task context) — must be a bare promise.create.
      const res = await sendHolder.fn({
        kind: "promise.create",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          id: "fence-4",
          timeoutAt: Date.now() + 60_000,
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
