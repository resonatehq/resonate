import { setTimeout } from "node:timers/promises";
import { LocalNetwork } from "../dev/network";
import type { Context } from "../src/context";
import { HttpNetwork } from "../src/network/remote";
import { Resonate } from "../src/resonate";
import { Constant, Never } from "../src/retries";
import * as util from "../src/util";

describe("Resonate usage tests", () => {
  test("function retries", async () => {
    const resonate = Resonate.local();

    let tries = 0;
    const g = async (_ctx: Context, msg: string) => {
      tries++;
      throw msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const future = yield* ctx.beginRun(
        g,
        "this is an error",
        ctx.options({ retryPolicy: new Constant({ delay: 0, maxRetries: 3 }) }),
      );
      yield* future;
    });

    const h = await f.beginRun("f");

    await expect(h.result()).rejects.toBe("this is an error");
    expect(tries).toBe(4); // 1 initial try + 3 retries

    resonate.stop();
  });

  test("function is executed on schedule", async () => {
    const resonate = Resonate.local();

    // A promise that resolves when the scheduled function runs
    let resolveRun!: () => void;
    const ran = new Promise<void>((resolve) => {
      resolveRun = resolve;
    });

    resonate.register("onSchedule", async (ctx: Context) => {
      resolveRun();
    });

    const scheduleId = `on-schedule-${crypto.randomUUID().replace(/-/g, "")}`;
    const schedule = await resonate.schedule(
      scheduleId,
      "* * * * * *", // every second (if seconds are supported)
      "onSchedule",
    );

    // Wait until the scheduled function fires
    await ran;

    // If we reach here, the function executed
    expect(true).toBe(true);

    await schedule.delete();
    resonate.stop();
  });

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
      const future = yield* ctx.beginRun(g, "this is an error", ctx.options({ retryPolicy: new Never() }));
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
      yield* ctx.run(g, "this is an error", ctx.options({ retryPolicy: new Never() }));
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

  test("Correctly matches target", async () => {
    const network = new LocalNetwork();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 }, network);

    resonate.register("foo", function* (ctx: Context, target: string) {
      yield* ctx.rfi("bar", ctx.options({ target }));
    });

    resonate.register("bar", () => "bar");

    // test matched targets
    for (const [i, target] of ["default", "foo", "bar", "baz"].entries()) {
      await resonate.rpc(`f${i}`, "foo", target, resonate.options({ target }));
      const p1 = await resonate.promises.get(`f${i}`);
      const p2 = await resonate.promises.get(`f${i}.0`);

      expect(p1.tags["resonate:invoke"]).toBe(`poll://any@${target}`);
      expect(p2.tags["resonate:invoke"]).toBe(`poll://any@${target}`);
    }

    // test unmatched targets (urls)
    for (const [i, target] of [
      "poll://default",
      "poll://any@default",
      "poll://any@default/0",
      "poll://uni@default/0",
      "http://resonatehq.io",
      "https://resonatehq.io",
      "sqs://region/queue",
    ].entries()) {
      await resonate.rpc(`g${i}`, "foo", target, resonate.options({ target }));
      const p1 = await resonate.promises.get(`g${i}`);
      const p2 = await resonate.promises.get(`g${i}.0`);

      expect(p1.tags["resonate:invoke"]).toBe(target);
      expect(p2.tags["resonate:invoke"]).toBe(target);
    }
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

    await resonate.promises.resolve("myId", { ikey: "myId", strict: false, value: "myValue" });
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

    await resonate.promises.resolve("myId", { ikey: "myId", strict: false, value: "myValue" });
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
    await resonate.promises.resolve("foo", { ikey: "foo", strict: false, value: "foo" });

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

  test("Basic auth", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    // mock fetch
    global.fetch = jest.fn((url, options) => {
      if (url === "http://localhost:9998/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic Zm9vOmJhcg==");
        p1.resolve(null);
      }

      if (url instanceof URL && url.origin === "http://localhost:9999") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic YmF6OnF1eA==");
        p2.resolve(null);
      }

      // leave on read
      return new Promise(() => {});
    });

    const network = new HttpNetwork({
      host: "http://localhost",
      storePort: "9998",
      messageSourcePort: "9999",
      pid: "0",
      group: "default",
      auth: { username: "foo", password: "bar" },
      messageSourceAuth: { username: "baz", password: "qux" },
    });
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 }, network);
    resonate.promises.create("foo", 0);

    await p1.promise;
    await p2.promise;
  });
});
