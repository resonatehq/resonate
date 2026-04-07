import { randomUUID } from "node:crypto";
import { Codec } from "../src/codec.js";
import type { PromiseRecord, Request, Response } from "../src/network/types.js";
import type { Send } from "../src/types.js";
import { buildEffects, VERSION } from "../src/util.js";

// A simple in-memory stub that handles promise.create and promise.settle.
class StubNetwork {
  private promises = new Map<string, PromiseRecord>();
  sendCount = 0;

  send: Send = <K extends Request["kind"]>(
    req: Extract<Request, { kind: K }>,
  ): Promise<Extract<Response, { kind: K }>> => {
    this.sendCount++;

    switch (req.kind) {
      case "promise.create": {
        const createReq = req as Extract<Request, { kind: "promise.create" }>;
        const p: PromiseRecord = {
          id: createReq.data.id,
          state: "pending",
          timeoutAt: createReq.data.timeoutAt,
          param: createReq.data.param,
          value: { headers: {}, data: "" },
          tags: createReq.data.tags,
          createdAt: Date.now(),
        };
        this.promises.set(p.id, p);
        return Promise.resolve({
          kind: req.kind,
          head: { corrId: req.head.corrId, status: 200, version: req.head.version },
          data: { promise: p },
        } as Extract<Response, { kind: K }>);
      }

      case "promise.settle": {
        const settleReq = req as Extract<Request, { kind: "promise.settle" }>;
        const p = this.promises.get(settleReq.data.id)!;
        p.state = settleReq.data.state;
        p.value = settleReq.data.value;
        p.settledAt = Date.now();
        this.promises.set(p.id, p);
        return Promise.resolve({
          kind: req.kind,
          head: { corrId: req.head.corrId, status: 200, version: req.head.version },
          data: { promise: p },
        } as Extract<Response, { kind: K }>);
      }

      default:
        throw new Error(`Unexpected request kind: ${req.kind}`);
    }
  };
}

const codec = new Codec();
const head = { corrId: randomUUID(), version: VERSION };

function encodeOrThrow(value: any) {
  return codec.encode(value);
}

function pendingPromise(id: string): PromiseRecord {
  return {
    id,
    state: "pending",
    param: encodeOrThrow({ func: "f", args: [] }),
    value: { headers: {}, data: "" },
    tags: {},
    timeoutAt: Date.now() + 60_000,
    createdAt: Date.now(),
  };
}

function resolvedPromise(id: string, value: any): PromiseRecord {
  return {
    ...pendingPromise(id),
    state: "resolved",
    value: encodeOrThrow(value),
    settledAt: Date.now(),
  };
}

describe("Effects", () => {
  describe("promiseCreate", () => {
    test("returns cached promise from preload without hitting network", async () => {
      const network = new StubNetwork();
      const preloaded = pendingPromise("p1");
      const effects = buildEffects(network.send, codec, [preloaded]);

      const res = await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "p1", timeoutAt: 0, param: { data: undefined }, tags: {} },
      });

      expect(res.state).toBe("pending");
      expect(network.sendCount).toBe(0);
    });

    test("hits network when promise is not in preload", async () => {
      const network = new StubNetwork();
      const effects = buildEffects(network.send, codec);

      const res = await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "p2", timeoutAt: Date.now() + 60_000, param: { data: { func: "f", args: [] } }, tags: {} },
      });

      expect(res.state).toBe("pending");
      expect(network.sendCount).toBe(1);
    });

    test("adds created promise to cache so second create is cached", async () => {
      const network = new StubNetwork();
      const effects = buildEffects(network.send, codec);

      // first call hits network
      await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "p3", timeoutAt: Date.now() + 60_000, param: { data: { func: "f", args: [] } }, tags: {} },
      });
      expect(network.sendCount).toBe(1);

      // second call should use cache
      const res = await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "p3", timeoutAt: 0, param: { data: undefined }, tags: {} },
      });

      expect(res.state).toBe("pending");
      expect(network.sendCount).toBe(1);
    });
  });

  describe("promiseSettle", () => {
    test("returns cached promise when already settled in preload", async () => {
      const network = new StubNetwork();
      const preloaded = resolvedPromise("s1", 42);
      const effects = buildEffects(network.send, codec, [preloaded]);

      const res = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "s1", state: "resolved", value: { data: 99 } },
      });

      expect(res.state).toBe("resolved");
      expect(network.sendCount).toBe(0);
    });

    test("hits network when preloaded promise is still pending", async () => {
      const network = new StubNetwork();
      const preloaded = pendingPromise("s2");
      const effects = buildEffects(network.send, codec, [preloaded]);

      // seed the stub network so settle can find the promise
      await network.send({
        kind: "promise.create",
        head,
        data: { id: "s2", timeoutAt: Date.now() + 60_000, param: { data: "" }, tags: {} },
      } as any);
      const beforeCount = network.sendCount;

      const res = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "s2", state: "resolved", value: { data: "ok" } },
      });

      expect(res.state).toBe("resolved");
      expect(network.sendCount).toBe(beforeCount + 1);
    });

    test("updates cache after settling so second settle is cached", async () => {
      const network = new StubNetwork();
      const effects = buildEffects(network.send, codec);

      // create via network
      await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "s3", timeoutAt: Date.now() + 60_000, param: { data: { func: "f", args: [] } }, tags: {} },
      });
      expect(network.sendCount).toBe(1);

      // first settle hits network
      await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "s3", state: "resolved", value: { data: "v" } },
      });
      expect(network.sendCount).toBe(2);

      // second settle should use cache (now settled)
      const res = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "s3", state: "resolved", value: { data: "v" } },
      });

      expect(res.state).toBe("resolved");
      expect(network.sendCount).toBe(2);
    });

    test("hits network when promise is not in cache at all", async () => {
      const network = new StubNetwork();
      const effects = buildEffects(network.send, codec);

      // seed the promise directly in the stub so settle doesn't crash
      await network.send({
        kind: "promise.create",
        head,
        data: { id: "s4", timeoutAt: Date.now() + 60_000, param: { data: "" }, tags: {} },
      } as any);
      const beforeCount = network.sendCount;

      const res = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "s4", state: "resolved", value: { data: "x" } },
      });

      expect(res.state).toBe("resolved");
      expect(network.sendCount).toBe(beforeCount + 1);
    });
  });

  describe("cached promise values", () => {
    test("preloaded pending promise has decoded param", async () => {
      const network = new StubNetwork();
      const preloaded = pendingPromise("v1");
      const effects = buildEffects(network.send, codec, [preloaded]);

      const res = await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "v1", timeoutAt: 0, param: { data: undefined }, tags: {} },
      });

      expect(res.id).toBe("v1");
      expect(res.state).toBe("pending");
      expect(res.param.data).toEqual({ func: "f", args: [] });
    });

    test("preloaded resolved promise has decoded value", async () => {
      const network = new StubNetwork();
      const preloaded = resolvedPromise("v2", { answer: 42 });
      const effects = buildEffects(network.send, codec, [preloaded]);

      const res = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "v2", state: "resolved", value: { data: "ignored" } },
      });

      expect(res.id).toBe("v2");
      expect(res.state).toBe("resolved");
      expect(res.value.data).toEqual({ answer: 42 });
    });

    test("promise created via network has correct decoded values in cache", async () => {
      const network = new StubNetwork();
      const effects = buildEffects(network.send, codec);

      const paramData = { func: "myFunc", args: [1, "two"] };

      // first call goes to network and populates cache
      await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "v3", timeoutAt: Date.now() + 60_000, param: { data: paramData }, tags: {} },
      });

      // second call returns from cache
      const res = await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "v3", timeoutAt: 0, param: { data: undefined }, tags: {} },
      });

      expect(res.id).toBe("v3");
      expect(res.state).toBe("pending");
      expect(res.param.data).toEqual(paramData);
    });

    test("promise settled via network has correct decoded values in cache", async () => {
      const network = new StubNetwork();
      const effects = buildEffects(network.send, codec);

      // create promise via network
      await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "v4", timeoutAt: Date.now() + 60_000, param: { data: { func: "f", args: [] } }, tags: {} },
      });

      const settleValue = { result: "success", count: 7 };

      // settle via network, populates cache with settled state
      await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "v4", state: "resolved", value: { data: settleValue } },
      });

      // second settle returns from cache
      const res = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "v4", state: "resolved", value: { data: "ignored" } },
      });

      expect(res.id).toBe("v4");
      expect(res.state).toBe("resolved");
      expect(res.value.data).toEqual(settleValue);
    });

    test("multiple preloaded promises each have correct values", async () => {
      const network = new StubNetwork();
      const p1 = pendingPromise("m1");
      const p2 = resolvedPromise("m2", "hello");
      const p3 = resolvedPromise("m3", [1, 2, 3]);
      const effects = buildEffects(network.send, codec, [p1, p2, p3]);

      const res1 = await effects.promiseCreate({
        kind: "promise.create",
        head,
        data: { id: "m1", timeoutAt: 0, param: { data: undefined }, tags: {} },
      });
      const res2 = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "m2", state: "resolved", value: { data: "ignored" } },
      });
      const res3 = await effects.promiseSettle({
        kind: "promise.settle",
        head,
        data: { id: "m3", state: "resolved", value: { data: "ignored" } },
      });

      expect(network.sendCount).toBe(0);

      expect(res1.state).toBe("pending");
      expect(res1.param.data).toEqual({ func: "f", args: [] });

      expect(res2.state).toBe("resolved");
      expect(res2.value.data).toBe("hello");

      expect(res3.state).toBe("resolved");
      expect(res3.value.data).toEqual([1, 2, 3]);
    });
  });
});
