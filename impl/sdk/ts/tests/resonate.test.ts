import { setTimeout } from "node:timers/promises";
import type { Context, InnerContext } from "../src/context";
import { JsonEncoder } from "../src/encoder";
import { Resonate } from "../src/resonate";
import { Constant, Exponential, Linear, Never, type RetryPolicy } from "../src/retries";
import type { Value } from "../src/types";
import * as util from "../src/util";

describe("Resonate usage tests", () => {
  test("try versions", async () => {
    const resonate = Resonate.local();

    const f1 = resonate.register(
      "f",
      function foo(ctx: Context): number {
        return 1;
      },
      { version: 1 },
    );
    const f2 = resonate.register(
      "f",
      function bar(ctx: Context): number {
        return 2;
      },
      { version: 2 },
    );

    expect(await resonate.run(crypto.randomUUID().replace(/-/g, ""), "f", resonate.options({ version: 1 }))).toBe(1);
    expect(await resonate.run(crypto.randomUUID().replace(/-/g, ""), "f", resonate.options({ version: 2 }))).toBe(2);
    expect(await resonate.run(crypto.randomUUID().replace(/-/g, ""), "f")).toBe(2);
    expect(await resonate.rpc(crypto.randomUUID().replace(/-/g, ""), "f", resonate.options({ version: 1 }))).toBe(1);
    expect(await resonate.rpc(crypto.randomUUID().replace(/-/g, ""), "f", resonate.options({ version: 2 }))).toBe(2);
    expect(await resonate.rpc(crypto.randomUUID().replace(/-/g, ""), "f")).toBe(2);
    expect(await f1.run(crypto.randomUUID().replace(/-/g, ""))).toBe(1);
    await expect(f1.run(crypto.randomUUID().replace(/-/g, ""), resonate.options({ version: 2 }))).rejects.toThrow(
      "Function 'foo' (version 2) is not registered",
    );
    expect(await f2.run(crypto.randomUUID().replace(/-/g, ""))).toBe(2);
    await expect(f2.run(crypto.randomUUID().replace(/-/g, ""), resonate.options({ version: 1 }))).rejects.toThrow(
      "Function 'bar' (version 1) is not registered",
    );
  });

  test("done check", async () => {
    const resonate = Resonate.local();

    const f = resonate.register("f", function foo(ctx: Context): string {
      return "hello world";
    });

    const h = await f.beginRun("f");

    expect(await h.done()).toBe(false);
    await h.result();
    expect(await h.done()).toBe(true);

    const h1 = await f.beginRun("f");
    expect(await h1.done()).toBe(true);
  });

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

  test("test promises search api", async () => {
    const resonate = Resonate.local();

    // Create test promises
    const foo = await resonate.promises.create("foo", 10_000_000);
    const bar = await resonate.promises.create("bar", 10_000_000);

    const results: any[] = [];
    for await (const page of resonate.promises.search("*", { limit: 1 })) {
      expect(Array.isArray(page)).toBe(true);
      results.push(...page);
    }

    const ids = results.map((r) => r.id);
    expect(ids).toContain(foo.id);
    expect(ids).toContain(bar.id);

    resonate.stop();
  });

  test("test schedules search api", async () => {
    const resonate = Resonate.local();

    // Create test promises
    const foo = await resonate.schedules.create("foo", "0 * * * *", "{{.id}}.{{.timestamp}}", Number.MAX_SAFE_INTEGER);
    const bar = await resonate.schedules.create("bar", "0 * * * *", "{{.id}}.{{.timestamp}}", Number.MAX_SAFE_INTEGER);

    const results: any[] = [];
    for await (const page of resonate.schedules.search("*", { limit: 1 })) {
      expect(Array.isArray(page)).toBe(true);
      results.push(...page);
    }

    const ids = results.map((r) => r.id);
    expect(ids).toContain(foo.id);
    expect(ids).toContain(bar.id);

    resonate.stop();
  });

  test("Correctly sets options on inner functions", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

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
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

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
    resonate.stop();
  });

  test("Correctly sets options on inner functions without defined opts", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

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
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "local",
      "resonate:root": "f",
      "resonate:parent": "f",
    });
    resonate.stop();
  });

  test("Basic human in the loop", async () => {
    const encoder = new JsonEncoder();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise({ id: "myId" });
      expect(fu.id).toBe("myId");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure myId promise is created

    await resonate.promises.resolve("myId", { data: encoder.encode("myValue").data });
    const v = await p.result();
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Correctly sets timeout", async () => {
    const encoder = new JsonEncoder();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

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

    await resonate.promises.resolve("myId", { data: encoder.encode("myValue").data });
    const v = await p.result();
    expect(v).toBe("myValue");
    resonate.stop();
  });

  test("Basic Durable sleep", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

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
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

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
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

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
    const encoder = new JsonEncoder();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    // get throws when promise does not exist
    expect(resonate.get("foo")).rejects.toThrow();

    // get returns the promise value
    await resonate.promises.create("foo", Number.MAX_SAFE_INTEGER);
    await resonate.promises.resolve("foo", { data: encoder.encode("foo").data });

    const handle = await resonate.get("foo");
    expect(await handle.result()).toBe("foo");
    resonate.stop();
  });

  test("Date", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

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

    resonate.stop();
  });

  test("Math", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

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
    resonate.stop();
  });

  test("Basic auth", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    // mock fetch
    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic Zm9vOmJhcg==");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic Zm9vOmJhcg==");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }

      // leave on read
      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
      auth: { username: "foo", password: "bar" },
    });

    resonate.promises.create("foo", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("Target is set to anycast without preference by default", async () => {
    const resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };
    resonate.register("g", g);

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.rpc("g", "this is a function");
    });

    await f.run("f");
    const durable = await resonate.promises.get("f.0");
    expect(durable.id).toBe("f.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:root": "f",
      "resonate:parent": "f",
      "resonate:invoke": "poll://any@default",
    });
    resonate.stop();
  });

  test("Target is set to the target option", async () => {
    const resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };
    resonate.register("g", g);

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.rpc("g", "this is a function", ctx.options({ target: "remoteTarget" }));
    });

    await f.run("f");
    const durable = await resonate.promises.get("f.0");
    expect(durable.id).toBe("f.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:root": "f",
      "resonate:parent": "f",
      "resonate:invoke": "poll://any@remoteTarget",
    });
    resonate.stop();
  });

  test("Target is set to the target option when it is a url", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };
    resonate.register("g", g);

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.rpc("g", "this is a function", ctx.options({ target: "http://faasurl.com" }));
    });

    await f.run("f");
    const durable = await resonate.promises.get("f.0");
    expect(durable.id).toBe("f.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:root": "f",
      "resonate:parent": "f",
      "resonate:invoke": "http://faasurl.com",
    });
    resonate.stop();
  });

  test("Target is set when using options in resonate class", async () => {
    const resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };
    resonate.register("g", g);

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.rpc("g", "this is a function");
    });

    await f.rpc("fid", resonate.options({ target: "http://faasurl.com" }));
    const durable = await resonate.promises.get("fid");
    expect(durable.id).toBe("fid");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:root": "fid",
      "resonate:parent": "fid",
      "resonate:invoke": "http://faasurl.com",
    });
    resonate.stop();
  });

  test("Target is set in root promise to the unicast without preference address by default", async () => {
    const resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };
    resonate.register("g", g);

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.rpc("g", "this is a function");
    });

    await f.rpc("fid");
    const durable = await resonate.promises.get("fid");
    expect(durable.id).toBe("fid");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:root": "fid",
      "resonate:parent": "fid",
      "resonate:invoke": "poll://any@default",
    });
    resonate.stop();
  });

  test("Target is set in root promise to the the poller and group when only group defined in opts", async () => {
    const resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };
    resonate.register("g", g);

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.rpc("g", "this is a function");
    });

    await f.rpc("fid", resonate.options({ target: "anotherNode" }));
    const durable = await resonate.promises.get("fid");
    expect(durable.id).toBe("fid");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:root": "fid",
      "resonate:parent": "fid",
      "resonate:invoke": "poll://any@anotherNode",
    });
    resonate.stop();
  });

  test("run/rpc with function pointer and string are equivalent", async () => {
    const resonate = new Resonate();

    function* foo() {
      return "foo";
    }

    function bar() {
      return "bar";
    }

    const f = resonate.register(foo);
    const g = resonate.register(bar);

    const r1 = [
      await resonate.run("f1", foo),
      await resonate.rpc("f2", foo),
      await resonate.run("f3", "foo"),
      await resonate.rpc("f4", "foo"),
      await f.run("f5"),
      await f.rpc("f6"),
    ];

    const r2 = [
      await resonate.run("g1", bar),
      await resonate.rpc("g2", bar),
      await resonate.run("g3", "bar"),
      await resonate.rpc("g4", "bar"),
      await g.run("g5"),
      await g.rpc("g6"),
    ];

    expect(r1.every((r) => r === "foo")).toBe(true);
    expect(r2.every((r) => r === "bar")).toBe(true);

    resonate.stop();
  });

  test("run/rpc with version specified", async () => {
    const resonate = new Resonate();

    function* foo(ctx: Context) {
      return ctx.info.version;
    }

    function bar(ctx: Context) {
      return ctx.info.version;
    }

    const f1 = resonate.register(foo);
    const f2 = resonate.register(foo, { version: 2 });
    const f3 = resonate.register(foo, { version: 3 });

    const b1 = resonate.register(bar);
    const b2 = resonate.register(bar, { version: 2 });
    const b3 = resonate.register(bar, { version: 3 });

    const rf1 = [
      await resonate.run("f1", foo, resonate.options({ version: 1 })),
      await resonate.rpc("f2", foo, resonate.options({ version: 1 })),
      await resonate.run("f3", "foo", resonate.options({ version: 1 })),
      await resonate.rpc("f4", "foo", resonate.options({ version: 1 })),
      await f1.run("f5"),
      await f1.rpc("f6"),
    ];

    const rf2 = [
      await resonate.run("g1", foo, resonate.options({ version: 2 })),
      await resonate.rpc("g2", foo, resonate.options({ version: 2 })),
      await resonate.run("g3", "foo", resonate.options({ version: 2 })),
      await resonate.rpc("g4", "foo", resonate.options({ version: 2 })),
      await f2.run("g5"),
      await f2.rpc("g6"),
    ];

    const rf3 = [
      await resonate.run("h1", foo),
      await resonate.rpc("h2", foo),
      await resonate.run("h3", "foo"),
      await resonate.rpc("h4", "foo"),
      await resonate.run("h5", foo, resonate.options({ version: 3 })),
      await resonate.rpc("h6", foo, resonate.options({ version: 3 })),
      await resonate.run("h7", "foo", resonate.options({ version: 3 })),
      await resonate.rpc("h8", "foo", resonate.options({ version: 3 })),
      await f3.run("h9"),
      await f3.rpc("h10"),
    ];

    const rb1 = [
      await resonate.run("i1", bar, resonate.options({ version: 1 })),
      await resonate.rpc("i2", bar, resonate.options({ version: 1 })),
      await resonate.run("i3", "bar", resonate.options({ version: 1 })),
      await resonate.rpc("i4", "bar", resonate.options({ version: 1 })),
      await b1.run("i5"),
      await b1.rpc("i6"),
    ];

    const rb2 = [
      await resonate.run("j1", bar, resonate.options({ version: 2 })),
      await resonate.rpc("j2", bar, resonate.options({ version: 2 })),
      await resonate.run("j3", "bar", resonate.options({ version: 2 })),
      await resonate.rpc("j4", "bar", resonate.options({ version: 2 })),
      await b2.run("j5"),
      await b2.rpc("j6"),
    ];

    const rb3 = [
      await resonate.run("k1", bar),
      await resonate.rpc("k2", bar),
      await resonate.run("k3", "bar"),
      await resonate.rpc("k4", "bar"),
      await resonate.run("k5", bar, resonate.options({ version: 3 })),
      await resonate.rpc("k6", bar, resonate.options({ version: 3 })),
      await resonate.run("k7", "bar", resonate.options({ version: 3 })),
      await resonate.rpc("k8", "bar", resonate.options({ version: 3 })),
      await b3.run("k9"),
      await b3.rpc("k10"),
    ];

    expect([...rf1, ...rb1].every((r) => r === 1)).toBe(true);
    expect([...rf2, ...rb2].every((r) => r === 2)).toBe(true);
    expect([...rf3, ...rb3].every((r) => r === 3)).toBe(true);

    resonate.stop();
  });

  test.each([Constant, Linear, Exponential, Never])("run/rpc with %p retry policy", async (policyCtor) => {
    const resonate = new Resonate();
    const retryPolicy = new policyCtor();

    let ctxRetryPolicy: RetryPolicy | undefined;

    resonate.register("foo", (ctx: Context) => {
      ctxRetryPolicy = (ctx as InnerContext).retryPolicy;
    });

    for (const [i, f] of [resonate.run.bind(resonate), resonate.rpc.bind(resonate)].entries()) {
      ctxRetryPolicy = undefined;
      await f(`f${i}`, "foo", resonate.options({ retryPolicy }));
      expect(ctxRetryPolicy).toBeDefined();
      expect(ctxRetryPolicy).toEqual(retryPolicy);

      const p = await resonate.promises.get(`f${i}`);
      expect(JSON.parse(util.base64Decode((p.param as Value<string>).data!)).retry).toEqual(retryPolicy.encode());
    }

    resonate.stop();
  });

  test("Using prefix at Resonate class prefixes all the promises", async () => {
    const prefix = "myPrefix";
    const resonate = new Resonate({ prefix });

    function qux(ctx: Context) {
      expect(ctx.id.startsWith(prefix));
      expect(ctx.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      return "qux";
    }

    function* baz(ctx: Context) {
      expect(ctx.id.startsWith(prefix)).toBe(true);
      expect(ctx.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      yield* ctx.run(qux);
      return "baz";
    }

    function* bar(ctx: Context) {
      console.log(ctx.id);
      expect(ctx.id.startsWith(prefix)).toBe(true);
      expect(ctx.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      return "bar";
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.beginRun(bar);
      yield* ctx.run(baz, ctx.options({ id: "bazId" }));
      yield* ctx.run(qux);
      yield* p;
      return "ok";
    }
    const f = resonate.register("foo", foo);
    await f.run("fooId");

    const results = [];
    for await (const page of resonate.promises.search("*")) {
      expect(Array.isArray(page)).toBe(true);
      results.push(...page);
    }

    for (const promise of results) {
      expect(promise.id.startsWith(prefix)).toBe(true);
    }

    resonate.stop();
  });
});

describe("Context usage tests", () => {
  test("ctx.panic aborts execution when condition is true", async () => {
    const resonate = Resonate.local();

    let completed = false;
    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.panic(true, "This should abort");
      completed = true;
      return "should not reach here";
    });

    await f.beginRun("test");
    await setTimeout(100); // Give time for function to run

    // Promise is dropped, but execution after panic should not run
    expect(completed).toBe(false);

    resonate.stop();
  });

  test("ctx.panic continues execution when condition is false", async () => {
    const resonate = Resonate.local();

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.panic(false, "This should not abort");
      return "success";
    });

    const result = await f.run("test");
    expect(result).toBe("success");

    resonate.stop();
  });

  test("ctx.assert aborts execution when condition is false", async () => {
    const resonate = Resonate.local();

    let completed = false;
    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.assert(false, "Assertion failed");
      completed = true;
      return "should not reach here";
    });

    await f.beginRun("test");
    await setTimeout(100); // Give time for function to run

    // Promise is dropped, but execution after assert should not run
    expect(completed).toBe(false);

    resonate.stop();
  });

  test("ctx.assert continues execution when condition is true", async () => {
    const resonate = Resonate.local();

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.assert(true, "This should pass");
      return "success";
    });

    const result = await f.run("test");
    expect(result).toBe("success");

    resonate.stop();
  });

  test("lfi/lfc/rfi/rfc/detached with function pointer and string are equivalent", async () => {
    const resonate = new Resonate();

    function* foo1(ctx: Context) {
      return [
        yield* yield* ctx.lfi(bar),
        yield* yield* ctx.lfi("bar"),
        yield* ctx.lfc(bar),
        yield* ctx.lfc("bar"),
        yield* yield* ctx.rfi(bar),
        yield* yield* ctx.rfi("bar"),
        yield* ctx.rfc(bar),
        yield* ctx.rfc("bar"),
        yield* yield* ctx.detached(bar),
        yield* yield* ctx.detached("bar"),
      ];
    }

    function* foo2(ctx: Context) {
      return [
        yield* yield* ctx.lfi(baz),
        yield* yield* ctx.lfi("baz"),
        yield* ctx.lfc(baz),
        yield* ctx.lfc("baz"),
        yield* yield* ctx.rfi(baz),
        yield* yield* ctx.rfi("baz"),
        yield* ctx.rfc(baz),
        yield* ctx.rfc("baz"),
        yield* yield* ctx.detached(baz),
        yield* yield* ctx.detached("baz"),
      ];
    }

    function* bar() {
      return "bar";
    }

    function baz() {
      return "baz";
    }

    resonate.register(foo1);
    resonate.register(foo2);
    resonate.register(bar);
    resonate.register(baz);

    const r1 = await resonate.run("f1", foo1);
    const r2 = await resonate.run("f2", foo2);

    expect(r1.every((r) => r === "bar")).toBe(true);
    expect(r2.every((r) => r === "baz")).toBe(true);

    resonate.stop();
  });

  test.each([Constant])("lfi/lfc/rfi/rfc/detached with %p retry policy", async (policyCtor) => {
    const resonate = new Resonate();
    const retryPolicy = new policyCtor();

    let ctxRetryPolicy: RetryPolicy | undefined;

    resonate.register("foo", function* (ctx: Context, method: string) {
      if (method === "lfi") {
        yield* ctx.lfi("bar", ctx.options({ retryPolicy }));
      } else if (method === "lfc") {
        yield* ctx.lfc("bar", ctx.options({ retryPolicy }));
      } else if (method === "rfi") {
        yield* ctx.rfi("bar", ctx.options({ retryPolicy }));
      } else if (method === "rfc") {
        yield* ctx.rfc("bar", ctx.options({ retryPolicy }));
      } else if (method === "detached") {
        yield* yield* ctx.detached("bar", ctx.options({ retryPolicy }));
      }
    });

    resonate.register("bar", (ctx: Context) => {
      ctxRetryPolicy = (ctx as InnerContext).retryPolicy;
    });

    for (const [i, f] of ["lfi", "lfc", "rfi", "rfc", "detached"].entries()) {
      ctxRetryPolicy = undefined;
      await resonate.run(`f${i}`, "foo", f);
      expect(ctxRetryPolicy).toBeDefined();
      expect(ctxRetryPolicy).toEqual(retryPolicy);

      if (f === "rfi" || f === "rfc" || f === "detached") {
        const p = await resonate.promises.get(`f${i}.0`);
        expect(JSON.parse(util.base64Decode((p.param as Value<string>).data!)).retry).toEqual(retryPolicy.encode());
      }
    }

    resonate.stop();
  });
});

describe("Resonate environment variable initialization", () => {
  const originalEnv = process.env;
  const originalFetch = global.fetch;

  beforeEach(() => {
    process.env = { ...originalEnv };
    delete process.env.RESONATE_HOST;
    delete process.env.RESONATE_PORT;
    delete process.env.RESONATE_URL;
    delete process.env.RESONATE_SCHEME;
    delete process.env.RESONATE_USERNAME;
    delete process.env.RESONATE_PASSWORD;
  });

  afterAll(() => {
    process.env = originalEnv;
    global.fetch = originalFetch;
  });

  test("url arg takes priority over env vars", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "envhost";
    process.env.RESONATE_PORT = "8080";
    process.env.RESONATE_URL = "http://url-from-env:9000";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://arg-url:3000/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://arg-url:3000/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ url: "http://arg-url:3000", group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_HOST + RESONATE_PORT used when url arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "localhost";
    process.env.RESONATE_PORT = "8001";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:8001/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://localhost:8001/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_URL used when url arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_URL = "http://resonate-server:9000";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://resonate-server:9000/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://resonate-server:9000/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("LocalNetwork used when no url sources are set", async () => {
    const mockFetch = jest.spyOn(global, "fetch").mockImplementation(() => {
      throw new Error("Fetch should not be called for LocalNetwork");
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    // Should work without calling fetch
    const f = resonate.register("f", async (_ctx: Context) => "result");
    const result = await f.run("test");
    expect(result).toBe("result");

    // Verify fetch was never called
    expect(mockFetch).not.toHaveBeenCalled();
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_URL takes priority over HOST and PORT", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "should-not-use";
    process.env.RESONATE_PORT = "7000";
    process.env.RESONATE_URL = "http://priority-url:9000";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://priority-url:9000/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://priority-url:9000/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_URL empty fallbacks to HOST and PORT", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "fallback";
    process.env.RESONATE_PORT = "8080";
    process.env.RESONATE_URL = "";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://fallback:8080/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://fallback:8080/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("Empty RESONATE_URL and no url arg falls back to LocalNetwork", async () => {
    process.env.RESONATE_URL = "";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation(() => {
      throw new Error("Fetch should not be called for LocalNetwork");
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    const f = resonate.register("f", async (_ctx: Context) => "result");
    const result = await f.run("test");
    expect(result).toBe("result");
    expect(mockFetch).not.toHaveBeenCalled();
    mockFetch.mockReset();
    resonate.stop();
  });

  test("auth arg takes priority over env vars", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_USERNAME = "envuser";
    process.env.RESONATE_PASSWORD = "envpass";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic YXJndXNlcjphcmdwYXNz");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic YXJndXNlcjphcmdwYXNz");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
      auth: { username: "arguser", password: "argpass" },
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_USERNAME and RESONATE_PASSWORD used when auth arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_USERNAME = "envuser";
    process.env.RESONATE_PASSWORD = "envpass";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic ZW52dXNlcjplbnZwYXNz");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic ZW52dXNlcjplbnZwYXNz");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("auth is defined when only RESONATE_USERNAME is set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_USERNAME = "envuser";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic ZW52dXNlcjo=");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Basic ZW52dXNlcjo=");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("auth is undefined when only RESONATE_PASSWORD is set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_PASSWORD = "envpass";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBeUndefined();
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBeUndefined();
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("auth is undefined when no env vars or arg are set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBeUndefined();
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBeUndefined();
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_SCHEME defaults to http when not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "localhost";
    process.env.RESONATE_PORT = "8001";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:8001/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://localhost:8001/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_SCHEME can be set to https", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_SCHEME = "https";
    process.env.RESONATE_HOST = "secure-host";
    process.env.RESONATE_PORT = "8443";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "https://secure-host:8443/promises") {
        p1.resolve(null);
      } else if (urlStr === "https://secure-host:8443/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_PORT defaults to 8001 when not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "default-port-host";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://default-port-host:8001/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://default-port-host:8001/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_SCHEME and RESONATE_PORT both use defaults", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "defaults-host";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://defaults-host:8001/promises") {
        p1.resolve(null);
      } else if (urlStr === "http://defaults-host:8001/poll/default/0") {
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }
      return new Promise(() => {});
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });
    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });
});

describe("Bearer token authentication", () => {
  const originalEnv = process.env;
  const originalFetch = global.fetch;

  beforeEach(() => {
    process.env = { ...originalEnv };
    delete process.env.RESONATE_TOKEN;
    delete process.env.RESONATE_USERNAME;
    delete process.env.RESONATE_PASSWORD;
  });

  afterAll(() => {
    process.env = originalEnv;
    global.fetch = originalFetch;
  });

  test("Bearer token auth", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer test-token-123");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer test-token-123");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }

      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
      token: "test-token-123",
    });

    resonate.promises.create("foo", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("Bearer token takes priority over basic auth", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer priority-token");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer priority-token");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }

      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
      token: "priority-token",
      auth: { username: "ignored", password: "ignored" },
    });

    resonate.promises.create("foo", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_TOKEN used when token arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_TOKEN = "env-token-456";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer env-token-456");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer env-token-456");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }

      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });

  test("RESONATE_TOKEN takes priority over RESONATE_USERNAME and RESONATE_PASSWORD", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_TOKEN = "env-token-priority";
    process.env.RESONATE_USERNAME = "ignored";
    process.env.RESONATE_PASSWORD = "ignored";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999/promises") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer env-token-priority");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as Record<string, string>).Authorization).toBe("Bearer env-token-priority");
        p2.resolve(null);
      } else {
        throw new Error(`Unexpected URL called: ${urlStr}`);
      }

      return new Promise(() => {});
    });

    const resonate = new Resonate({
      url: "http://localhost:9999",
      group: "default",
      pid: "0",
      ttl: 60_000,
    });

    resonate.promises.create("test", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    resonate.stop();
  });
});
