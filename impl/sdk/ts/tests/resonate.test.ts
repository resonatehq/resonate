import { setTimeout } from "node:timers/promises";
import { LocalNetwork } from "../dev/network";
import type { Context } from "../src/context";
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
    const r = await h.result();
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
    const r = await h.result();
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
    await expect(h.result()).rejects.toBe("this is an error");
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
    await expect(h.result()).rejects.toBe("this is an error");
    resonate.stop();
  });

  test("Correctly sets options on inner functions", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 }, network);

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
    const durable = await resonate.promises.get("altId");
    expect(durable.id).toBe("altId");
    expect(durable.tags).toMatchObject({ myTag: "value", "resonate:scope": "local" });
    resonate.stop();
  });

  test("Correctly sets options on inner functions without defined opts", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 }, network);

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
    const durable = await resonate.promises.get("f.0");
    expect(durable.id).toBe("f.0");
    expect(durable.tags).toStrictEqual({ "resonate:scope": "local", "resonate:root": "f", "resonate:parent": "f" });
    resonate.stop();
  });

  test("Basic human in the loop", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 }, network);

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise({ id: "myId" });
      expect(fu.id).toBe("myId");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure myId promise is created

    await resonate.promises.resolve("myId", { iKey: "myId", strict: false, value: "myValue" });
    const v = await p.result();
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Correctly sets timeout", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 }, network);

    const time = Date.now();

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise(ctx.options({ id: "myId", timeout: 5 * util.HOUR }));
      expect(fu.id).toBe("myId");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure myId promise is created

    const durable = await resonate.promises.get("myId");
    expect(durable.timeout).toBeGreaterThanOrEqual(time + 5 * util.HOUR);
    expect(durable.timeout).toBeLessThan(time + 5 * util.HOUR + 1000);

    await resonate.promises.resolve("myId", { iKey: "myId", strict: false, value: "myValue" });
    const v = await p.result();
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Basic Durable sleep", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 }, network);

    const time = Date.now();
    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.sleep(1 * util.SEC);
      return "myValue";
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure f.0 promise is created

    const durable = await resonate.promises.get("f.0");
    expect(durable.tags).toMatchObject({ "resonate:timeout": "true" });
    expect(durable.timeout).toBeLessThan(time + 1 * util.SEC + 100);

    const v = await p.result();
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Basic Detached", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 }, network);

    resonate.register("d", async (_ctx: Context): Promise<void> => {
      await setTimeout(1000);
    });

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.detached("d");
      return "myValue";
    });

    const v = await f.run("f");
    expect(v).toBe("myValue");

    const durable = await resonate.promises.get("f.0");
    expect(durable).toMatchObject({ state: "pending" });

    resonate.stop();
  });

  test("Basic use of dependencies", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 }, network);

    const g = (ctx: Context, name: string): string => {
      const greeting = ctx.getDependency("greeting") as string;
      return `${greeting} ${name}`;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const v = yield* ctx.run(g, "World!");
      return v;
    });

    resonate.setDependency("greeting", "Hello");
    const v = await f.run("f");
    expect(v).toBe("Hello World!");

    resonate.stop();
  });

  test("Basic get", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 }, network);

    // get throws when promise does not exist
    expect(resonate.get("foo")).rejects.toThrow();

    // get returns the promise value
    await resonate.promises.create("foo", Number.MAX_SAFE_INTEGER);
    await resonate.promises.resolve("foo", { iKey: "foo", strict: false, value: "foo" });

    const handle = await resonate.get("foo");
    expect(await handle.result()).toBe("foo");
  });

  test("Date", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 }, network);

    // with default date
    const f = resonate.register("f", function* foo(ctx: Context) {
      return yield* ctx.date.now();
    });

    for (let i = 0; i < 5; i++) {
      const n = await f.run(`f${i}`);
      expect(typeof n).toBe("number");
    }

    // with custom date
    resonate.setDependency("resonate:date", {
      now: () => 0,
    });

    for (let i = 0; i < 5; i++) {
      const n = await f.run(`g${i}`);
      expect(n).toBe(0);
    }
  });

  test("Math", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 }, network);

    // with default math
    const f = resonate.register("f", function* foo(ctx: Context) {
      return yield* ctx.math.random();
    });

    for (let i = 0; i < 5; i++) {
      const r = await f.run(`f${i}`);
      expect(r).toBeGreaterThanOrEqual(0);
      expect(r).toBeLessThan(1);
    }

    // with custom math
    resonate.setDependency("resonate:math", {
      random: () => 1,
    });

    for (let i = 0; i < 5; i++) {
      const n = await f.run(`g${i}`);
      expect(n).toBe(1);
    }
  });
});
