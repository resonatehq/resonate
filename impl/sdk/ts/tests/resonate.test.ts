import { setTimeout } from "node:timers/promises";
import { LocalNetwork } from "../dev/network";
import type { Context } from "../src/context";
import { Promises } from "../src/promises";
import { Resonate } from "../src/resonate";
import * as util from "../src/util";

describe("Resonate usage tests", () => {
  test("concurrent execution must be concurrent", async () => {
    const resonate = Resonate.local();

    const g = async (ctx: Context, msg: string) => {
      await setTimeout(500);
      return msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fa = yield* ctx.beginRun(g, "a");
      const fb = yield* ctx.beginRun(g, "b");
      const a = yield* fa;
      const b = yield* fb;
      return [a, b];
    });

    const startTime = Date.now();
    const h = await f.beginRun("test");
    const r = await h.result;
    const endTime = Date.now();
    const executionTime = endTime - startTime;

    // Allow some buffer, this buffer is kind of arbitrary, if this tests fail consider increasing the buffer time.
    expect(executionTime).toBeLessThan(600);
    expect(executionTime).toBeGreaterThan(400);

    expect(r).toStrictEqual(["a", "b"]);
    resonate.stop();
  });
  test("sequential execution must be sequential", async () => {
    const resonate = Resonate.local();

    const g = async (_ctx: Context, msg: string) => {
      await setTimeout(250);
      return msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const a = yield* ctx.run(g, "a");
      const b = yield* ctx.run(g, "b");
      return [a, b];
    });

    const startTime = Date.now();
    const h = await f.beginRun("test");
    const r = await h.result;
    const endTime = Date.now();
    const executionTime = endTime - startTime;

    // Allow some buffer, this buffer is kind of arbitrary, if this tests fail consider increasing the buffer time.
    expect(executionTime).toBeLessThan(600);
    expect(executionTime).toBeGreaterThan(475);

    expect(r).toStrictEqual(["a", "b"]);
    resonate.stop();
  });

  test("Correctly rejects a top level function using ctx.beginRun", async () => {
    const resonate = Resonate.local();

    const g = async (_ctx: Context, msg: string) => {
      throw msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const future = yield* ctx.beginRun(g, "this is an error");
      yield* future;
    });

    const h = await f.beginRun("f");
    await expect(h.result).rejects.toBe("this is an error");
    resonate.stop();
  });

  test("Correctly rejects a top level function using ctx.run", async () => {
    const resonate = Resonate.local();

    const g = async (_ctx: Context, msg: string) => {
      throw msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.run(g, "this is an error");
    });

    const h = await f.beginRun("f");
    await expect(h.result).rejects.toBe("this is an error");
    resonate.stop();
  });

  test("Correctly sets options on inner functions", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate(network, { group: "default", pid: "0", ttl: 50_000 });
    const promises = new Promises(network);

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.beginRun(g, "this is a function", ctx.options({ id: "altId", tags: { myTag: "value" } }));
      expect(fu.id).toBe("altId");
      return yield* fu;
    });

    const v = await f.run("f");
    expect(v.msg).toBe("this is a function");
    const durable = await promises.get("altId");
    expect(durable.id).toBe("altId");
    expect(durable.tags).toMatchObject({ myTag: "value", "resonate:scope": "local" });
    resonate.stop();
  });

  test("Correctly sets options on inner functions without defined opts", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate(network, { group: "default", pid: "0", ttl: 50_000 });
    const promises = new Promises(network);

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.beginRun(g, "this is a function");
      expect(fu.id).toBe("f.0");
      return yield* fu;
    });

    const v = await f.run("f");
    expect(v.msg).toBe("this is a function");
    const durable = await promises.get("f.0");
    expect(durable.id).toBe("f.0");
    expect(durable.tags).toStrictEqual({ "resonate:scope": "local", "resonate:root": "f", "resonate:parent": "f" });
    resonate.stop();
  });

  test("Basic human in the loop", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate(network, { group: "default", pid: "0", ttl: 50_000 });
    const promises = new Promises(network);

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise(ctx.options({ id: "myId" }));
      expect(fu.id).toBe("myId");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await promises.resolve("myId", "myId", false, "myValue");
    const v = await p.result;
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Correctly sets timeout", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate(network, { group: "default", pid: "0", ttl: 50_000 });
    const promises = new Promises(network);

    const time = Date.now();

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise(ctx.options({ id: "myId", timeout: 5 * util.HOUR }));
      expect(fu.id).toBe("myId");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    const durable = await promises.get("myId");
    expect(durable.timeout).toBeGreaterThanOrEqual(time + 5 * util.HOUR);
    expect(durable.timeout).toBeLessThan(time + 5 * util.HOUR + 1000);

    await promises.resolve("myId", "myId", false, "myValue");
    const v = await p.result;
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Basic Durable sleep", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate(network, { group: "default", pid: "0", ttl: 50_000 });
    const promises = new Promises(network);

    const time = Date.now();
    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.sleep(1 * util.SEC);
      return "myValue";
    });

    const p = await f.beginRun("f");
    const durable = await promises.get("f.0");
    expect(durable.tags).toMatchObject({ "resonate:timeout": "true" });
    expect(durable.timeout).toBeLessThan(time + 1 * util.SEC + 100);

    const v = await p.result;
    expect(v).toBe("myValue");
    resonate.stop();
  });
});
