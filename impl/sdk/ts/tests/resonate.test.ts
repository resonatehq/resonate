import { setTimeout } from "node:timers/promises";
import type { Context } from "../src/context";
import { JsonEncoder } from "../src/encoder";
import { Resonate } from "../src/resonate";
import { Constant, Never } from "../src/retries";
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
    expect(durable.tags).toStrictEqual({ "resonate:scope": "local", "resonate:root": "f", "resonate:parent": "f" });
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

    await resonate.promises.resolve("myId", { ikey: "myId", strict: false, data: encoder.encode("myValue").data });
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

    await resonate.promises.resolve("myId", { ikey: "myId", strict: false, data: encoder.encode("myValue").data });
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
    await resonate.promises.resolve("foo", { ikey: "foo", strict: false, data: encoder.encode("foo").data });

    const handle = await resonate.get("foo");
    expect(await handle.result()).toBe("foo");
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
  });

  test("Basic auth", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    // mock fetch
    global.fetch = jest.fn((url, options) => {
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
      "resonate:invoke": "poll://any@test",
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
      "resonate:invoke": "poll://any@test",
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
      "resonate:invoke": "poll://any@anotherNode",
    });
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

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("RESONATE_HOST + RESONATE_PORT used when url arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "localhost";
    process.env.RESONATE_PORT = "8001";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("RESONATE_URL used when url arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_URL = "http://resonate-server:9000";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("LocalNetwork used when no url sources are set", async () => {
    global.fetch = jest.fn(() => {
      throw new Error("Fetch should not be called for LocalNetwork");
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    // Should work without calling fetch
    const f = resonate.register("f", async (_ctx: Context) => "result");
    const result = await f.run("test");
    expect(result).toBe("result");

    // Verify fetch was never called
    expect(global.fetch).not.toHaveBeenCalled();
    resonate.stop();
  });

  test("RESONATE_URL takes priority over HOST and PORT", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "should-not-use";
    process.env.RESONATE_PORT = "7000";
    process.env.RESONATE_URL = "http://priority-url:9000";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("RESONATE_URL empty fallbacks to HOST and PORT", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "fallback";
    process.env.RESONATE_PORT = "8080";
    process.env.RESONATE_URL = "";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("Empty RESONATE_URL and no url arg falls back to LocalNetwork", async () => {
    process.env.RESONATE_URL = "";

    global.fetch = jest.fn(() => {
      throw new Error("Fetch should not be called for LocalNetwork");
    });

    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    const f = resonate.register("f", async (_ctx: Context) => "result");
    const result = await f.run("test");
    expect(result).toBe("result");

    expect(global.fetch).not.toHaveBeenCalled();
    resonate.stop();
  });

  test("auth arg takes priority over env vars", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_USERNAME = "envuser";
    process.env.RESONATE_PASSWORD = "envpass";

    global.fetch = jest.fn((url, options) => {
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
    resonate.stop();
  });

  test("RESONATE_USERNAME and RESONATE_PASSWORD used when auth arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_USERNAME = "envuser";
    process.env.RESONATE_PASSWORD = "envpass";

    global.fetch = jest.fn((url, options) => {
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
    resonate.stop();
  });

  test("auth is defined when only RESONATE_USERNAME is set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_USERNAME = "envuser";

    global.fetch = jest.fn((url, options) => {
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
    resonate.stop();
  });

  test("auth is undefined when only RESONATE_PASSWORD is set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_PASSWORD = "envpass";

    global.fetch = jest.fn((url, options) => {
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
    resonate.stop();
  });

  test("auth is undefined when no env vars or arg are set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    global.fetch = jest.fn((url, options) => {
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
    resonate.stop();
  });

  test("RESONATE_SCHEME defaults to http when not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "localhost";
    process.env.RESONATE_PORT = "8001";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("RESONATE_SCHEME can be set to https", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_SCHEME = "https";
    process.env.RESONATE_HOST = "secure-host";
    process.env.RESONATE_PORT = "8443";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("RESONATE_PORT defaults to 8001 when not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "default-port-host";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

  test("RESONATE_SCHEME and RESONATE_PORT both use defaults", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_HOST = "defaults-host";

    global.fetch = jest.fn((url) => {
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
    resonate.stop();
  });

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
});
