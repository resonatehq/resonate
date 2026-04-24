import { setTimeout } from "node:timers/promises";
import { Codec } from "../src/codec.js";
import type { Context, InnerContext } from "../src/context.js";
import type { Value } from "../src/network/types.js";
import { Resonate } from "../src/resonate.js";
import { Constant, Exponential, Linear, Never, type RetryPolicy } from "../src/retries.js";
import * as util from "../src/util.js";

function encodeValue(codec: Codec, value: any): Value {
  return codec.encode(value);
}

describe("Resonate usage tests", () => {
  test("only rpc", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    const f1 = resonate.register(
      "f",
      function foo(ctx: Context): number {
        return 1;
      },
      { version: 1 },
    );

    expect(await resonate.rpc(crypto.randomUUID().replace(/-/g, ""), "f", resonate.options({ version: 1 }))).toBe(1);
  });

  test("try versions", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

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

  test("test lineage rfc", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    const f = resonate.register("f", function* foo(ctx: Context): Generator {
      // origin: foo.1
      // parent: foo.1
      // branch: foo.1
      const v = yield ctx.rpc(bar);
      return v;
    });

    function* bar(ctx: Context): Generator {
      // origin: foo.1
      // parent: foo.1
      // branch: foo.1.0
      const v = yield ctx.rpc(baz);
      return v;
    }

    async function baz(ctx: Context): Promise<string> {
      // origin: foo.1
      // parent: foo.1.0
      // branch: foo.1.0.0
      return "hello";
    }

    resonate.register(bar);
    resonate.register(baz);

    const v = await f.run("foo.1");
    expect(await v).toBe("hello");
    expect((await resonate.promises.get("foo.1")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1",
      "resonate:scope": "global",
      "resonate:target": "local://any@default/default",
    });
    expect((await resonate.promises.get("foo.1.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:branch": "foo.1.0",
      "resonate:parent": "foo.1",
      "resonate:scope": "global",
      "resonate:target": "local://any@default",
    });
    expect((await resonate.promises.get("foo.1.0.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:branch": "foo.1.0.0",
      "resonate:parent": "foo.1.0",
      "resonate:scope": "global",
      "resonate:target": "local://any@default",
    });
  });
  test("test lineage rfc set ids", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    const f = resonate.register("f", function* foo(ctx: Context): Generator {
      const v = yield ctx.rpc(bar);
      return v;
    });

    function* bar(ctx: Context): Generator {
      const v = yield ctx.rpc(baz);
      return v;
    }

    async function baz(ctx: Context): Promise<string> {
      return "hello";
    }

    resonate.register(bar);
    resonate.register(baz);

    const v = await f.run("foo");
    expect(await v).toBe("hello");
    expect((await resonate.promises.get("foo")).tags).toEqual({
      "resonate:origin": "foo",
      "resonate:branch": "foo",
      "resonate:parent": "foo",
      "resonate:scope": "global",
      "resonate:target": "local://any@default/default",
    });
    expect((await resonate.promises.get("foo.0")).tags).toEqual({
      "resonate:origin": "foo",
      "resonate:branch": "foo.0",
      "resonate:parent": "foo",
      "resonate:scope": "global",
      "resonate:target": "local://any@default",
    });
    expect((await resonate.promises.get("foo.0.0")).tags).toEqual({
      "resonate:origin": "foo",
      "resonate:branch": "foo.0.0",
      "resonate:parent": "foo.0",
      "resonate:scope": "global",
      "resonate:target": "local://any@default",
    });
  });

  test("test lineage lfc", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    const f = resonate.register("f", function* foo(ctx: Context): Generator {
      const v = yield ctx.lfc(bar);
      return v;
    });

    function* bar(ctx: Context): Generator {
      const v = yield ctx.lfc(baz);
      return v;
    }

    async function baz(ctx: Context): Promise<string> {
      return "hello";
    }

    const v = await f.run("foo.1");
    expect(await v).toBe("hello");
    expect((await resonate.promises.get("foo.1")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1",
      "resonate:scope": "global",
      "resonate:target": "local://any@default/default",
    });
    expect((await resonate.promises.get("foo.1.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1",
      "resonate:scope": "local",
    });
    expect((await resonate.promises.get("foo.1.0.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1.0",
      "resonate:scope": "local",
    });
  });
  test("test lineage lfc set ids", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    const f = resonate.register("f", function* foo(ctx: Context): Generator {
      const v = yield ctx.lfc(bar);
      return v;
    });

    function* bar(ctx: Context): Generator {
      const v = yield ctx.lfc(baz);
      return v;
    }

    async function baz(ctx: Context): Promise<string> {
      return "hello";
    }

    const v = await f.run("foo");
    expect(await v).toBe("hello");
    expect((await resonate.promises.get("foo")).tags).toEqual({
      "resonate:origin": "foo",
      "resonate:branch": "foo",
      "resonate:parent": "foo",
      "resonate:scope": "global",
      "resonate:target": "local://any@default/default",
    });
    expect((await resonate.promises.get("foo.0")).tags).toEqual({
      "resonate:origin": "foo",
      "resonate:branch": "foo",
      "resonate:parent": "foo",
      "resonate:scope": "local",
    });
    expect((await resonate.promises.get("foo.0.0")).tags).toEqual({
      "resonate:origin": "foo",
      "resonate:branch": "foo",
      "resonate:parent": "foo.0",
      "resonate:scope": "local",
    });
  });

  test("done check", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    const f = resonate.register("f", function foo(_: Context): string {
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
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

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

    await resonate.stop();
  });

  test("function is executed on schedule", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

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
    await resonate.stop();
  });

  test("concurrent execution must be concurrent", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

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
    await resonate.stop();
  });

  test("sequential execution must be sequential", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

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
    await resonate.stop();
  });

  test("Correctly rejects a top level function using ctx.beginRun", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    const g = async (_ctx: Context, msg: string) => {
      throw msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const future = yield* ctx.beginRun(g, "this is an error", ctx.options({ retryPolicy: new Never() }));
      yield* future;
    });

    const h = await f.beginRun("f");
    await expect(h.result()).rejects.toBe("this is an error");
    await resonate.stop();
  });

  test("Correctly rejects a top level function using ctx.run", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    const g = async (_ctx: Context, msg: string) => {
      throw msg;
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.run(g, "this is an error", ctx.options({ retryPolicy: new Never() }));
    });

    const h = await f.beginRun("f");
    await expect(h.result()).rejects.toBe("this is an error");
    await resonate.stop();
  });

  test("Correctly sets options on inner functions", async () => {
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const g = async (_ctx: Context, msg: string) => {
      return { msg };
    };

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.beginRun(g, "this is a function", ctx.options({ tags: { myTag: "value" } }));
      expect(fu.id).toBe("f.0");
      return yield* fu;
    });

    const v = await f.run("f");
    expect(v.msg).toBe("this is a function");
    const durable = await resonate.promises.get("f.0");
    expect(durable.id).toBe("f.0");
    expect(durable.tags).toMatchObject({ myTag: "value", "resonate:scope": "local" });
    await resonate.stop();
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

      expect(p1.tags["resonate:target"]).toBe(`local://any@${target}`);
      expect(p2.tags["resonate:target"]).toBe(`local://any@${target}`);
    }

    // test unmatched targets (urls)
    for (const [i, target] of [
      "local://default",
      "local://any@default",
      "local://any@default/0",
      "local://uni@default/0",
      "http://resonatehq.io",
      "https://resonatehq.io",
      "sqs://region/queue",
    ].entries()) {
      await resonate.rpc(`g${i}`, "foo", target, resonate.options({ target }));
      const p1 = await resonate.promises.get(`g${i}`);
      const p2 = await resonate.promises.get(`g${i}.0`);

      expect(p1.tags["resonate:target"]).toBe(target);
      expect(p2.tags["resonate:target"]).toBe(target);
    }
    await resonate.stop();
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
      "resonate:branch": "f",
      "resonate:parent": "f",
      "resonate:origin": "f",
    });
    await resonate.stop();
  });

  test("Basic human in the loop", async () => {
    const codec = new Codec();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise();
      expect(fu.id).toBe("f.0");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure f.0 promise is created

    await resonate.promises.resolve("f.0", { data: encodeValue(codec, "myValue").data });
    const v = await p.result();
    expect(v).toBe("myValue");
    await resonate.stop();
    expect((await resonate.promises.get("f.0")).tags).toEqual({
      "resonate:branch": "f.0",
      "resonate:origin": "f",
      "resonate:parent": "f",
      "resonate:scope": "global",
    });
  });
  test("Basic human in the loop without setting ids", async () => {
    const codec = new Codec();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise();
      expect(fu.id).toBe(`${ctx.id}.0`);
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure myId promise is created

    await resonate.promises.resolve("f.0", { data: encodeValue(codec, "myValue").data });
    const v = await p.result();
    expect(v).toBe("myValue");
    await resonate.stop();
    expect((await resonate.promises.get("f.0")).tags).toEqual({
      "resonate:branch": "f.0",
      "resonate:origin": "f",
      "resonate:parent": "f",
      "resonate:scope": "global",
    });
  });

  test("Correctly sets timeout", async () => {
    const codec = new Codec();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const time = Date.now();

    const f = resonate.register("f", function* foo(ctx: Context) {
      const fu = yield* ctx.promise({ timeout: 5 * util.HOUR });
      expect(fu.id).toBe("f.0");
      return yield* fu;
    });

    const p = await f.beginRun("f");
    await setTimeout(100); // Ensure f.0 promise is created

    const durable = await resonate.promises.get("f.0");
    expect(durable.timeoutAt).toBeGreaterThanOrEqual(time + 5 * util.HOUR);
    expect(durable.timeoutAt).toBeLessThan(time + 5 * util.HOUR + 1000);
    expect(durable.tags).toEqual({
      "resonate:branch": "f.0",
      "resonate:origin": "f",
      "resonate:parent": "f",
      "resonate:scope": "global",
    });
    await resonate.promises.resolve("f.0", { data: encodeValue(codec, "myValue").data });
    const v = await p.result();
    expect(v).toBe("myValue");
    await resonate.stop();
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
    expect(durable.tags).toEqual({
      "resonate:timer": "true",
      "resonate:branch": "f.0",
      "resonate:origin": "f",
      "resonate:parent": "f",
      "resonate:scope": "global",
    });
    expect(durable.timeoutAt).toBeLessThan(time + 1 * util.SEC + 100);

    const v = await p.result();
    expect(v).toBe("myValue");
    await resonate.stop();
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

    const durable = await resonate.promises.get(util.detachedId("f", "f.0"));
    expect(durable).toMatchObject({ state: "pending" });

    await resonate.stop();
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

    await resonate.stop();
  });

  test("Basic get", async () => {
    const codec = new Codec();
    const resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    // get throws when promise does not exist
    expect(resonate.get("foo")).rejects.toThrow();

    // get returns the promise value
    await resonate.promises.create("foo", Number.MAX_SAFE_INTEGER);
    await resonate.promises.resolve("foo", { data: encodeValue(codec, "foo").data });

    const handle = await resonate.get("foo");
    expect(await handle.result()).toBe("foo");
    await resonate.stop();
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

    await resonate.stop();
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
    await resonate.stop();
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
      "resonate:branch": "f.0",
      "resonate:parent": "f",
      "resonate:origin": "f",
      "resonate:target": "local://any@default",
    });
    await resonate.stop();
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
      "resonate:branch": "f.0",
      "resonate:parent": "f",
      "resonate:origin": "f",
      "resonate:target": "local://any@remoteTarget",
    });
    await resonate.stop();
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
      "resonate:branch": "f.0",
      "resonate:parent": "f",
      "resonate:origin": "f",
      "resonate:target": "http://faasurl.com",
    });
    await resonate.stop();
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
      "resonate:branch": "fid",
      "resonate:parent": "fid",
      "resonate:origin": "fid",
      "resonate:target": "http://faasurl.com",
    });
    await resonate.stop();
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
      "resonate:branch": "fid",
      "resonate:parent": "fid",
      "resonate:origin": "fid",
      "resonate:target": "local://any@default",
    });
    await resonate.stop();
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
      "resonate:branch": "fid",
      "resonate:parent": "fid",
      "resonate:origin": "fid",
      "resonate:target": "local://any@anotherNode",
    });
    await resonate.stop();
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

    await resonate.stop();
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

    await resonate.stop();
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
      expect(JSON.parse(util.base64Decode(p.param.data!)).retry).toEqual(retryPolicy.encode());
    }

    await resonate.stop();
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
      expect(ctx.id.startsWith(prefix)).toBe(true);
      expect(ctx.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      return "bar";
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.beginRun(bar);
      yield* ctx.run(baz);
      yield* ctx.run(qux);
      yield* p;
      return "ok";
    }
    const f = resonate.register("foo", foo);
    await f.run("fooId");

    await resonate.stop();
  });
});

describe("Context usage tests", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test("ctx.panic aborts execution when condition is true", async () => {
    jest.spyOn(console, "error").mockImplementation(() => {});

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    let completed = false;
    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.panic(true, "This should abort");
      completed = true;
      return "should not reach here";
    });

    await f.beginRun("test");
    await setTimeout(50); // Give time for function to run

    // Promise is dropped, but execution after panic should not run
    expect(completed).toBe(false);

    await resonate.stop();
  });

  test("ctx.panic continues execution when condition is false", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.panic(false, "This should not abort");
      return "success";
    });

    const result = await f.run("test");
    expect(result).toBe("success");

    await resonate.stop();
  });

  test("ctx.assert aborts execution when condition is false", async () => {
    jest.spyOn(console, "error").mockImplementation(() => {});

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    let completed = false;
    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.assert(false, "Assertion failed");
      completed = true;
      return "should not reach here";
    });

    await f.beginRun("test");
    await setTimeout(50); // Give time for function to run

    // Promise is dropped, but execution after assert should not run
    expect(completed).toBe(false);

    await resonate.stop();
  });

  test("ctx.assert continues execution when condition is true", async () => {
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });

    const f = resonate.register("f", function* foo(ctx: Context) {
      yield* ctx.assert(true, "This should pass");
      return "success";
    });

    const result = await f.run("test");
    expect(result).toBe("success");

    await resonate.stop();
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

    await resonate.stop();
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

      if (f === "rfi" || f === "rfc") {
        const p = await resonate.promises.get(`f${i}.0`);
        expect(JSON.parse(util.base64Decode(p.param.data!)).retry).toEqual(retryPolicy.encode());
      } else if (f === "detached") {
        const p = await resonate.promises.get(util.detachedId(`f${i}`, `f${i}.0`));
        expect(JSON.parse(util.base64Decode(p.param.data!)).retry).toEqual(retryPolicy.encode());
      }
    }

    await resonate.stop();
  });
});

describe("Resonate environment variable initialization", () => {
  const originalEnv = process.env;
  const originalFetch = global.fetch;

  beforeEach(() => {
    process.env = { ...originalEnv };
    delete process.env.RESONATE_URL;
  });

  afterAll(() => {
    process.env = originalEnv;
    global.fetch = originalFetch;
  });

  test("url arg takes priority over RESONATE_URL env var", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_URL = "http://url-from-env:9000";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://arg-url:3000") {
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
    await resonate.stop();
  });

  test("RESONATE_URL used when url arg not set", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    process.env.RESONATE_URL = "http://resonate-server:9000";

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://resonate-server:9000") {
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
    await resonate.stop();
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
    await resonate.stop();
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
    await resonate.stop();
  });
});

describe("Bearer token authentication", () => {
  const originalFetch = global.fetch;

  afterAll(() => {
    global.fetch = originalFetch;
  });

  test("Bearer token is passed through to HttpNetwork", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999") {
        expect((options?.headers as { [key: string]: string }).Authorization).toBe("Bearer test-token-123");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as { [key: string]: string }).Authorization).toBe("Bearer test-token-123");
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
    await resonate.stop();
  });

  test("Bearer token takes priority", async () => {
    const p1 = Promise.withResolvers();
    const p2 = Promise.withResolvers();

    const mockFetch = jest.spyOn(global, "fetch").mockImplementation((url, options) => {
      const urlStr = url instanceof URL ? url.href : url;
      if (urlStr === "http://localhost:9999") {
        expect((options?.headers as { [key: string]: string }).Authorization).toBe("Bearer priority-token");
        p1.resolve(null);
      } else if (urlStr === "http://localhost:9999/poll/default/0") {
        expect((options?.headers as { [key: string]: string }).Authorization).toBe("Bearer priority-token");
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
    });

    resonate.promises.create("foo", 0);

    await p1.promise;
    await p2.promise;
    mockFetch.mockReset();
    await resonate.stop();
  });
});

describe("nonRetryableErrors", () => {
  it("ctx.run: stops retrying when a non-retryable error is thrown in a child function", async () => {
    class FatalError extends Error {}
    let attempts = 0;

    async function alwaysFatal(_ctx: Context): Promise<never> {
      attempts++;
      throw new FatalError("fatal");
    }

    function* workflow(ctx: Context): Generator {
      yield* ctx.run(alwaysFatal, ctx.options({ nonRetryableErrors: [FatalError] }));
    }

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    resonate.register(workflow);

    await expect(resonate.run(crypto.randomUUID().replace(/-/g, ""), workflow)).rejects.toThrow("fatal");

    expect(attempts).toBe(1);
  });

  it("ctx.run: nonRetryableErrors apply to unregistered local helper functions", async () => {
    class ValidationError extends Error {}
    let attempts = 0;

    async function helper(_ctx: Context): Promise<never> {
      attempts++;
      throw new ValidationError("bad input");
    }

    function* workflow(ctx: Context): Generator {
      yield* ctx.run(helper, ctx.options({ nonRetryableErrors: [ValidationError] }));
    }

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    resonate.register(workflow);

    await expect(resonate.run(crypto.randomUUID().replace(/-/g, ""), workflow)).rejects.toThrow("bad input");

    expect(attempts).toBe(1);
  });

  it("ctx.run: non-retryable error propagates through nested ctx.run() calls", async () => {
    class DomainError extends Error {}
    let childAttempts = 0;

    async function child(_ctx: Context): Promise<never> {
      childAttempts++;
      throw new DomainError("domain error");
    }

    function* parent(ctx: Context): Generator {
      yield* ctx.run(child, ctx.options({ nonRetryableErrors: [DomainError] }));
    }

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    resonate.register(parent);

    await expect(resonate.run(crypto.randomUUID().replace(/-/g, ""), parent)).rejects.toThrow("domain error");

    expect(childAttempts).toBe(1);
  });

  it("ctx.run: subclass of a non-retryable class is also non-retryable", async () => {
    class BaseError extends Error {}
    class SubError extends BaseError {}
    let attempts = 0;

    async function thrower(_ctx: Context): Promise<never> {
      attempts++;
      throw new SubError("sub error");
    }

    function* workflow(ctx: Context): Generator {
      yield* ctx.run(thrower, ctx.options({ nonRetryableErrors: [BaseError] }));
    }

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    resonate.register(workflow);

    await expect(resonate.run(crypto.randomUUID().replace(/-/g, ""), workflow)).rejects.toThrow("sub error");

    expect(attempts).toBe(1);
  });

  it("ctx.run: empty nonRetryableErrors falls back to normal retry behavior", async () => {
    let attempts = 0;

    async function failTwice(_ctx: Context): Promise<string> {
      attempts++;
      if (attempts < 3) throw new Error("transient");
      return "success";
    }

    function* workflow(ctx: Context): Generator<any, string, any> {
      return yield* ctx.run(failTwice, ctx.options({ retryPolicy: new Constant({ delay: 0 }) }));
    }

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
    resonate.register(workflow);

    const result = await resonate.run(crypto.randomUUID().replace(/-/g, ""), workflow);
    expect(result).toBe("success");
    expect(attempts).toBe(3);
  });
});
