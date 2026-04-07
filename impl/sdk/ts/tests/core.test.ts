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
import type { Send } from "../src/types.js";
import { VERSION } from "../src/util.js";

class TestHeartbeat implements Heartbeat {
  start(): void {}
  stop(): void {}
}

class MockComputation {
  public calls: { rootPromise: PromiseRecord }[] = [];
  private responses: (Status | Error)[];
  private callIndex = 0;

  constructor(responses: (Status | Error)[]) {
    this.responses = responses;
  }

  executeUntilBlocked(rootPromise: PromiseRecord): Promise<Status> {
    this.calls.push({ rootPromise });
    const res = this.responses[this.callIndex] ?? this.responses[this.responses.length - 1];
    this.callIndex++;
    if (res instanceof Error) return Promise.reject(res);
    return Promise.resolve(res);
  }
}

function buildCore(opts: { responses: (Status | Error)[]; mockRef?: { mock: MockComputation } }): {
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

  const activeMock = new MockComputation(opts.responses);
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

  const ctorSpy = jest.spyOn(core as any, "createComputation").mockReturnValue(activeMock);

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
});
