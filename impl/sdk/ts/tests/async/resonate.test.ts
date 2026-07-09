// Top-level API tests for the async engine's Resonate.
//
// Mirrors tests/resonate.test.ts, adapted to the async idioms:
// - `resonate.run`/`rpc` return a handle (there are no `begin*` forms — run IS
//   begin-run); values come from `handle.result()`.
// - `yield* ctx.run(...)` becomes `await ctx.run(...)`; lfi/lfc/rfi/rfc map to
//   the eager run/rpc (call now, await now-or-later).
// - Retries are opt-in (default Never), so tests that relied on the generator's
//   default Exponential pin an explicit policy.
//
// Not portable (by design, documented in diff-testing.md):
// - "set ids" lineage variants: the async ctx.options has no `id` override.
// - InnerContext.retryPolicy introspection: the async engine resolves retries in
//   the driver, not on the context; the encoded policy is asserted on the
//   durable promise param instead.

import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, afterEach, beforeEach, describe, expect, jest, test } from "@jest/globals";
import type { Context, Info, ResonateHandle } from "../../src/async/index.js";
import { Resonate } from "../../src/async/index.js";
import { Codec } from "../../src/codec.js";
import { Constant, Exponential, Linear, Never } from "../../src/retries.js";
import * as util from "../../src/util.js";

const codec = new Codec();

function newResonate(): Resonate {
  return new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
}

function rid(): string {
  return crypto.randomUUID().replace(/-/g, "");
}

/** Await a run/rpc handle's result in one step. */
async function res<T>(hp: Promise<ResonateHandle<T>>): Promise<T> {
  return (await hp).result();
}

describe("Resonate usage tests", () => {
  let resonate: Resonate | undefined;

  afterEach(async () => {
    await resonate?.stop();
    resonate = undefined;
    jest.restoreAllMocks();
  });

  test("only rpc", async () => {
    resonate = newResonate();
    resonate.register("f", async (_info: Info): Promise<number> => 1, { version: 1 });

    expect(await res(resonate.rpc(rid(), "f", resonate.options({ version: 1 })))).toBe(1);
  });

  test("try versions", async () => {
    resonate = newResonate();

    const f1 = resonate.register(
      "f",
      async function foo(_info: Info): Promise<number> {
        return 1;
      },
      { version: 1 },
    );
    const f2 = resonate.register(
      "f",
      async function bar(_info: Info): Promise<number> {
        return 2;
      },
      { version: 2 },
    );

    expect(await res(resonate.run(rid(), "f", resonate.options({ version: 1 })))).toBe(1);
    expect(await res(resonate.run(rid(), "f", resonate.options({ version: 2 })))).toBe(2);
    expect(await res(resonate.run(rid(), "f"))).toBe(2);
    expect(await res(resonate.rpc(rid(), "f", resonate.options({ version: 1 })))).toBe(1);
    expect(await res(resonate.rpc(rid(), "f", resonate.options({ version: 2 })))).toBe(2);
    expect(await res(resonate.rpc(rid(), "f"))).toBe(2);
    expect(await res(f1.run(rid()))).toBe(1);
    await expect(f1.run(rid(), resonate.options({ version: 2 }))).rejects.toThrow(
      "Function 'foo' (version 2) is not registered",
    );
    expect(await res(f2.run(rid()))).toBe(2);
    await expect(f2.run(rid(), resonate.options({ version: 1 }))).rejects.toThrow(
      "Function 'bar' (version 1) is not registered",
    );
  });

  test("registered handle passes args through run/rpc", async () => {
    resonate = newResonate();
    const f = resonate.register("argf", async (_info: Info, a: number, b: string): Promise<string> => `${b}:${a}`);

    expect(await res(f.run("args-1", 21, "x"))).toBe("x:21");
    expect(await res(f.rpc("args-2", 22, "y"))).toBe("y:22");
    // A trailing options argument is split off, not passed to the function.
    expect(await res(f.run("args-3", 23, "z", resonate.options({ version: 1 })))).toBe("z:23");
  });

  test("test lineage rfc", async () => {
    resonate = newResonate();
    const baz = async (_info: Info): Promise<string> => {
      // origin: foo.1 / parent: foo.1.0 / branch: foo.1.0.0
      return "hello";
    };
    const bar = async (ctx: Context): Promise<string> => {
      // origin: foo.1 / parent: foo.1 / branch: foo.1.0
      return ctx.rpc<string>("baz");
    };
    const foo = async (ctx: Context): Promise<string> => {
      // origin: foo.1 / parent: foo.1 / branch: foo.1
      return ctx.rpc<string>("bar");
    };
    resonate.register("foo", foo);
    resonate.register("bar", bar);
    resonate.register("baz", baz);

    expect(await res(resonate.run("foo.1", foo))).toBe("hello");
    expect((await resonate.promises.get("foo.1")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:prefix": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1",
      "resonate:scope": "global",
      "resonate:target": "local://any@default/default",
    });
    expect((await resonate.promises.get("foo.1.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:prefix": "foo.1",
      "resonate:branch": "foo.1.0",
      "resonate:parent": "foo.1",
      "resonate:scope": "global",
      "resonate:target": "local://any@default",
    });
    expect((await resonate.promises.get("foo.1.0.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:prefix": "foo.1",
      "resonate:branch": "foo.1.0.0",
      "resonate:parent": "foo.1.0",
      "resonate:scope": "global",
      "resonate:target": "local://any@default",
    });
  });

  test("test lineage lfc", async () => {
    resonate = newResonate();
    const baz = async (_info: Info): Promise<string> => "hello";
    const bar = async (ctx: Context): Promise<string> => ctx.run(baz);
    const foo = async (ctx: Context): Promise<string> => ctx.run(bar);
    const f = resonate.register("lfcfoo", foo);

    expect(await res(f.run("foo.1"))).toBe("hello");
    expect((await resonate.promises.get("foo.1")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:prefix": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1",
      "resonate:scope": "global",
      "resonate:target": "local://any@default/default",
    });
    expect((await resonate.promises.get("foo.1.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:prefix": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1",
      "resonate:scope": "local",
    });
    expect((await resonate.promises.get("foo.1.0.0")).tags).toEqual({
      "resonate:origin": "foo.1",
      "resonate:prefix": "foo.1",
      "resonate:branch": "foo.1",
      "resonate:parent": "foo.1.0",
      "resonate:scope": "local",
    });
  });

  test("done check", async () => {
    resonate = newResonate();

    // A DPC gate makes done() deterministic: the root cannot settle until the
    // latent promise is resolved out of band.
    const wf = async (ctx: Context): Promise<string> => ctx.promise<string>();
    resonate.register("donewf", wf);

    const h = await resonate.run("done-1", wf);
    expect(await h.done()).toBe(false);

    await sleep(100); // ensure the latent promise done-1.0 exists
    await resonate.promises.resolve("done-1.0", { data: codec.encode("v").data });
    expect(await h.result()).toBe("v");
    expect(await h.done()).toBe(true);

    const h1 = await resonate.run("done-1", wf);
    expect(await h1.done()).toBe(true);
  });

  test("function retries (root retry policy decoded from the promise param)", async () => {
    resonate = newResonate();

    let tries = 0;
    const g = async (_info: Info, msg: string): Promise<never> => {
      tries++;
      throw msg;
    };
    resonate.register("retryroot", g);

    const retryPolicy = new Constant({ delay: 0, maxRetries: 3 });
    const h = await resonate.run("retry-root-1", g, "this is an error", resonate.options({ retryPolicy }));

    await expect(h.result()).rejects.toBe("this is an error");
    expect(tries).toBe(4); // 1 initial try + 3 retries

    const p = await resonate.promises.get("retry-root-1");
    expect(JSON.parse(util.base64Decode(p.param.data!)).retry).toEqual(retryPolicy.encode());
  });

  test.each([Constant, Linear, Exponential, Never])("run/rpc with %p retry policy", async (policyCtor) => {
    resonate = newResonate();
    const retryPolicy = new policyCtor();
    resonate.register("noopf", async (_info: Info): Promise<void> => {});

    for (const [i, mode] of (["run", "rpc"] as const).entries()) {
      const id = `rp-${policyCtor.name}-${i}`;
      const h =
        mode === "run"
          ? await resonate.run(id, "noopf", resonate.options({ retryPolicy }))
          : await resonate.rpc(id, "noopf", resonate.options({ retryPolicy }));
      await h.result();

      const p = await resonate.promises.get(id);
      expect(JSON.parse(util.base64Decode(p.param.data!)).retry).toEqual(retryPolicy.encode());
    }
  });

  test("function is executed on schedule", async () => {
    resonate = newResonate();

    const ran = Promise.withResolvers<void>();
    resonate.register("onSchedule", async (_info: Info): Promise<void> => {
      ran.resolve();
    });

    const scheduleId = `on-schedule-${rid()}`;
    const schedule = await resonate.schedule(
      scheduleId,
      "* * * * * *", // every second (if seconds are supported)
      "onSchedule",
    );

    await ran.promise;
    await schedule.delete();
  });

  test("concurrent execution must be concurrent", async () => {
    resonate = newResonate();

    const g = async (_info: Info, msg: string): Promise<string> => {
      await sleep(500);
      return msg;
    };
    resonate.register("cc", g);

    const wf = async (ctx: Context): Promise<string[]> => {
      // eager: both leaves start before either is awaited
      const fa = ctx.run<string>("cc", "a");
      const fb = ctx.run<string>("cc", "b");
      return [await fa, await fb];
    };
    resonate.register("ccwf", wf);

    const startTime = Date.now();
    const r = await res(resonate.run("cc-test", wf));
    const executionTime = Date.now() - startTime;

    // Allow some buffer, this buffer is kind of arbitrary, if this tests fail consider increasing the buffer time.
    expect(executionTime).toBeLessThan(600);
    expect(executionTime).toBeGreaterThan(400);

    expect(r).toStrictEqual(["a", "b"]);
  });

  test("sequential execution must be sequential", async () => {
    resonate = newResonate();

    const g = async (_info: Info, msg: string): Promise<string> => {
      await sleep(250);
      return msg;
    };
    resonate.register("sq", g);

    const wf = async (ctx: Context): Promise<string[]> => {
      const a = await ctx.run<string>("sq", "a");
      const b = await ctx.run<string>("sq", "b");
      return [a, b];
    };
    resonate.register("sqwf", wf);

    const startTime = Date.now();
    const r = await res(resonate.run("sq-test", wf));
    const executionTime = Date.now() - startTime;

    // Allow some buffer, this buffer is kind of arbitrary, if this tests fail consider increasing the buffer time.
    expect(executionTime).toBeLessThan(600);
    expect(executionTime).toBeGreaterThan(475);

    expect(r).toStrictEqual(["a", "b"]);
  });

  test("Correctly rejects a top level function using ctx.run", async () => {
    resonate = newResonate();

    const g = async (_info: Info, msg: string): Promise<never> => {
      throw msg;
    };

    const wf = async (ctx: Context): Promise<void> => {
      await ctx.run(g, "this is an error", ctx.options({ retryPolicy: new Never() }));
    };
    resonate.register("rejwf", wf);

    const h = await resonate.run("rej-1", wf);
    await expect(h.result()).rejects.toBe("this is an error");
  });

  test("rejects unregistered functions (top-level and ctx string call)", async () => {
    resonate = newResonate();

    await expect(resonate.run("unreg-1", "missing")).rejects.toThrow("Function 'missing' is not registered");

    const wf = async (ctx: Context): Promise<string> => {
      try {
        await ctx.run<void>("alsoMissing");
        return "no-throw";
      } catch (e) {
        return `caught:${(e as Error).message}`;
      }
    };
    resonate.register("unregwf", wf);

    expect(await res(resonate.run("unreg-2", wf))).toBe("caught:Function 'alsoMissing' is not registered");
  });

  test("Correctly sets options on inner functions", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const g = async (_info: Info, msg: string): Promise<{ msg: string }> => {
      return { msg };
    };

    const wf = async (ctx: Context): Promise<{ msg: string }> => {
      const fu = ctx.run(g, "this is a function", ctx.options({ tags: { myTag: "value" } }));
      expect(fu.id).toBe("opts-1.0");
      return fu;
    };
    resonate.register("optswf", wf);

    const v = await res(resonate.run("opts-1", wf));
    expect(v.msg).toBe("this is a function");
    const durable = await resonate.promises.get("opts-1.0");
    expect(durable.id).toBe("opts-1.0");
    expect(durable.tags).toMatchObject({ myTag: "value", "resonate:scope": "local" });
  });

  test("Correctly sets options on inner functions without defined opts", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const g = async (_info: Info, msg: string): Promise<{ msg: string }> => {
      return { msg };
    };

    const wf = async (ctx: Context): Promise<{ msg: string }> => {
      const fu = ctx.run(g, "this is a function");
      expect(fu.id).toBe("noopts-1.0");
      return fu;
    };
    resonate.register("nooptswf", wf);

    const v = await res(resonate.run("noopts-1", wf));
    expect(v.msg).toBe("this is a function");
    const durable = await resonate.promises.get("noopts-1.0");
    expect(durable.id).toBe("noopts-1.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "local",
      "resonate:branch": "noopts-1",
      "resonate:parent": "noopts-1",
      "resonate:origin": "noopts-1",
      "resonate:prefix": "noopts-1",
    });
  });

  test("Correctly matches target", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    resonate.register("tbar", async (_info: Info): Promise<string> => "bar");
    resonate.register("tfoo", async (ctx: Context, target: string): Promise<string> => {
      return ctx.rpc<string>("tbar", ctx.options({ target }));
    });

    // test matched targets
    for (const [i, target] of ["default", "foo", "bar", "baz"].entries()) {
      await res(resonate.rpc(`t${i}`, "tfoo", target, resonate.options({ target })));
      const p1 = await resonate.promises.get(`t${i}`);
      const p2 = await resonate.promises.get(`t${i}.0`);

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
      await res(resonate.rpc(`u${i}`, "tfoo", target, resonate.options({ target })));
      const p1 = await resonate.promises.get(`u${i}`);
      const p2 = await resonate.promises.get(`u${i}.0`);

      expect(p1.tags["resonate:target"]).toBe(target);
      expect(p2.tags["resonate:target"]).toBe(target);
    }
  });

  test("Basic human in the loop", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const wf = async (ctx: Context): Promise<string> => {
      const fu = ctx.promise<string>();
      expect(fu.id).toBe("hitl-1.0");
      return fu;
    };
    resonate.register("hitlwf", wf);

    const h = await resonate.run("hitl-1", wf);
    await sleep(100); // ensure hitl-1.0 promise is created

    await resonate.promises.resolve("hitl-1.0", { data: codec.encode("myValue").data });
    expect(await h.result()).toBe("myValue");
    expect((await resonate.promises.get("hitl-1.0")).tags).toEqual({
      "resonate:branch": "hitl-1.0",
      "resonate:origin": "hitl-1",
      "resonate:prefix": "hitl-1",
      "resonate:parent": "hitl-1",
      "resonate:scope": "global",
    });
  });

  test("Correctly sets timeout", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const time = Date.now();

    const wf = async (ctx: Context): Promise<string> => {
      const fu = ctx.promise<string>({ timeout: 5 * util.HOUR });
      expect(fu.id).toBe("timeout-1.0");
      return fu;
    };
    resonate.register("timeoutwf", wf);

    const h = await resonate.run("timeout-1", wf);
    await sleep(100); // ensure timeout-1.0 promise is created

    const durable = await resonate.promises.get("timeout-1.0");
    expect(durable.timeoutAt).toBeGreaterThanOrEqual(time + 5 * util.HOUR);
    expect(durable.timeoutAt).toBeLessThan(time + 5 * util.HOUR + 1000);
    expect(durable.tags).toEqual({
      "resonate:branch": "timeout-1.0",
      "resonate:origin": "timeout-1",
      "resonate:prefix": "timeout-1",
      "resonate:parent": "timeout-1",
      "resonate:scope": "global",
    });
    await resonate.promises.resolve("timeout-1.0", { data: codec.encode("myValue").data });
    expect(await h.result()).toBe("myValue");
  });

  test("Basic Durable sleep", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    const time = Date.now();
    const wf = async (ctx: Context): Promise<string> => {
      await ctx.sleep(1 * util.SEC);
      return "myValue";
    };
    resonate.register("sleepwf", wf);

    const h = await resonate.run("sleep-1", wf);
    await sleep(100); // ensure sleep-1.0 promise is created

    const durable = await resonate.promises.get("sleep-1.0");
    expect(durable.tags).toEqual({
      "resonate:timer": "true",
      "resonate:branch": "sleep-1.0",
      "resonate:origin": "sleep-1",
      "resonate:prefix": "sleep-1",
      "resonate:parent": "sleep-1",
      "resonate:scope": "global",
    });
    expect(durable.timeoutAt).toBeLessThan(time + 1 * util.SEC + 100);

    expect(await h.result()).toBe("myValue");
  });

  test("Basic use of dependencies", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    const g = (info: Info, name: string): string => {
      const greeting = info.getDependency("greeting") as string;
      return `${greeting} ${name}`;
    };

    const wf = async (ctx: Context): Promise<string> => ctx.run(g, "World!");
    resonate.register("depwf", wf);

    resonate.setDependency("greeting", "Hello");
    expect(await res(resonate.run("dep-1", wf))).toBe("Hello World!");
  });

  test("get throws when promise does not exist", async () => {
    resonate = newResonate();
    await expect(resonate.get("nonexistent")).rejects.toThrow();
  });

  test("Date", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    // with default date
    const wf = async (ctx: Context): Promise<number> => ctx.date.now();
    resonate.register("datewf", wf);

    for (let i = 0; i < 5; i++) {
      const n = await res(resonate.run(`date-f${i}`, wf));
      expect(typeof n).toBe("number");
    }

    // with custom date
    resonate.setDependency("resonate:date", {
      now: () => 0,
    });

    for (let i = 0; i < 5; i++) {
      expect(await res(resonate.run(`date-g${i}`, wf))).toBe(0);
    }
  });

  test("Math", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 60_000 });

    // with default math
    const wf = async (ctx: Context): Promise<number> => ctx.math.random();
    resonate.register("mathwf", wf);

    for (let i = 0; i < 5; i++) {
      const r = await res(resonate.run(`math-f${i}`, wf));
      expect(r).toBeGreaterThanOrEqual(0);
      expect(r).toBeLessThan(1);
    }

    // with custom math
    resonate.setDependency("resonate:math", {
      random: () => 1,
    });

    for (let i = 0; i < 5; i++) {
      expect(await res(resonate.run(`math-g${i}`, wf))).toBe(1);
    }
  });

  test("Target is set to anycast without preference by default", async () => {
    resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    resonate.register("tg", async (_info: Info, msg: string): Promise<{ msg: string }> => ({ msg }));

    const wf = async (ctx: Context): Promise<void> => {
      await ctx.rpc("tg", "this is a function");
    };
    resonate.register("twf1", wf);

    await res(resonate.run("tdef-1", wf));
    const durable = await resonate.promises.get("tdef-1.0");
    expect(durable.id).toBe("tdef-1.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:branch": "tdef-1.0",
      "resonate:parent": "tdef-1",
      "resonate:origin": "tdef-1",
      "resonate:prefix": "tdef-1",
      "resonate:target": "local://any@default",
    });
  });

  test("Target is set to the target option", async () => {
    resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    resonate.register("tg", async (_info: Info, msg: string): Promise<{ msg: string }> => ({ msg }));

    const wf = async (ctx: Context): Promise<void> => {
      await ctx.rpc("tg", "this is a function", ctx.options({ target: "remoteTarget" }));
    };
    resonate.register("twf2", wf);

    await res(resonate.run("topt-1", wf));
    const durable = await resonate.promises.get("topt-1.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:branch": "topt-1.0",
      "resonate:parent": "topt-1",
      "resonate:origin": "topt-1",
      "resonate:prefix": "topt-1",
      "resonate:target": "local://any@remoteTarget",
    });
  });

  test("Target is set to the target option when it is a url", async () => {
    resonate = new Resonate({ group: "default", pid: "0", ttl: 50_000 });

    resonate.register("tg", async (_info: Info, msg: string): Promise<{ msg: string }> => ({ msg }));

    const wf = async (ctx: Context): Promise<void> => {
      await ctx.rpc("tg", "this is a function", ctx.options({ target: "http://faasurl.com" }));
    };
    resonate.register("twf3", wf);

    await res(resonate.run("turl-1", wf));
    const durable = await resonate.promises.get("turl-1.0");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:branch": "turl-1.0",
      "resonate:parent": "turl-1",
      "resonate:origin": "turl-1",
      "resonate:prefix": "turl-1",
      "resonate:target": "http://faasurl.com",
    });
  });

  test("Target is set when using options in resonate class", async () => {
    resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    resonate.register("tg", async (_info: Info, msg: string): Promise<{ msg: string }> => ({ msg }));
    const wf = async (ctx: Context): Promise<void> => {
      await ctx.rpc("tg", "this is a function");
    };
    const f = resonate.register("twf4", wf);

    await res(f.rpc("troot-1", resonate.options({ target: "http://faasurl.com" })));
    const durable = await resonate.promises.get("troot-1");
    expect(durable.id).toBe("troot-1");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:branch": "troot-1",
      "resonate:parent": "troot-1",
      "resonate:origin": "troot-1",
      "resonate:prefix": "troot-1",
      "resonate:target": "http://faasurl.com",
    });
  });

  test("Target is set in root promise to anycast without preference by default", async () => {
    resonate = new Resonate({ group: "test", pid: "0", ttl: 50_000 });

    resonate.register("tg", async (_info: Info, msg: string): Promise<{ msg: string }> => ({ msg }));
    const wf = async (ctx: Context): Promise<void> => {
      await ctx.rpc("tg", "this is a function");
    };
    const f = resonate.register("twf5", wf);

    await res(f.rpc("troot-2"));
    const durable = await resonate.promises.get("troot-2");
    expect(durable.tags).toStrictEqual({
      "resonate:scope": "global",
      "resonate:branch": "troot-2",
      "resonate:parent": "troot-2",
      "resonate:origin": "troot-2",
      "resonate:prefix": "troot-2",
      "resonate:target": "local://any@default",
    });
  });

  test("run/rpc with function pointer and string are equivalent", async () => {
    resonate = newResonate();

    async function pfoo(): Promise<string> {
      return "foo";
    }

    async function pbar(): Promise<string> {
      return "bar";
    }

    const f = resonate.register(pfoo);
    const g = resonate.register(pbar);

    const r1 = [
      await res(resonate.run("pf1", pfoo)),
      await res(resonate.rpc("pf2", pfoo)),
      await res(resonate.run<string>("pf3", "pfoo")),
      await res(resonate.rpc<string>("pf4", "pfoo")),
      await res(f.run("pf5")),
      await res(f.rpc("pf6")),
    ];

    const r2 = [
      await res(resonate.run("pg1", pbar)),
      await res(resonate.rpc("pg2", pbar)),
      await res(resonate.run<string>("pg3", "pbar")),
      await res(resonate.rpc<string>("pg4", "pbar")),
      await res(g.run("pg5")),
      await res(g.rpc("pg6")),
    ];

    expect(r1.every((r) => r === "foo")).toBe(true);
    expect(r2.every((r) => r === "bar")).toBe(true);
  });

  test("run/rpc with version specified", async () => {
    resonate = newResonate();

    async function vfoo(info: Info): Promise<number> {
      return info.version;
    }

    function vbar(info: Info): number {
      return info.version;
    }

    const f1 = resonate.register(vfoo);
    const f2 = resonate.register(vfoo, { version: 2 });
    const f3 = resonate.register(vfoo, { version: 3 });

    const b1 = resonate.register(vbar);
    const b2 = resonate.register(vbar, { version: 2 });
    const b3 = resonate.register(vbar, { version: 3 });

    const rf1 = [
      await res(resonate.run("vf1", vfoo, resonate.options({ version: 1 }))),
      await res(resonate.rpc("vf2", vfoo, resonate.options({ version: 1 }))),
      await res(resonate.run<number>("vf3", "vfoo", resonate.options({ version: 1 }))),
      await res(resonate.rpc<number>("vf4", "vfoo", resonate.options({ version: 1 }))),
      await res(f1.run("vf5")),
      await res(f1.rpc("vf6")),
    ];

    const rf2 = [
      await res(resonate.run("vg1", vfoo, resonate.options({ version: 2 }))),
      await res(resonate.rpc("vg2", vfoo, resonate.options({ version: 2 }))),
      await res(resonate.run<number>("vg3", "vfoo", resonate.options({ version: 2 }))),
      await res(resonate.rpc<number>("vg4", "vfoo", resonate.options({ version: 2 }))),
      await res(f2.run("vg5")),
      await res(f2.rpc("vg6")),
    ];

    const rf3 = [
      await res(resonate.run("vh1", vfoo)),
      await res(resonate.rpc("vh2", vfoo)),
      await res(resonate.run<number>("vh3", "vfoo")),
      await res(resonate.rpc<number>("vh4", "vfoo")),
      await res(resonate.run("vh5", vfoo, resonate.options({ version: 3 }))),
      await res(resonate.rpc("vh6", vfoo, resonate.options({ version: 3 }))),
      await res(resonate.run<number>("vh7", "vfoo", resonate.options({ version: 3 }))),
      await res(resonate.rpc<number>("vh8", "vfoo", resonate.options({ version: 3 }))),
      await res(f3.run("vh9")),
      await res(f3.rpc("vh10")),
    ];

    const rb1 = [
      await res(resonate.run("vi1", vbar, resonate.options({ version: 1 }))),
      await res(resonate.rpc("vi2", vbar, resonate.options({ version: 1 }))),
      await res(resonate.run<number>("vi3", "vbar", resonate.options({ version: 1 }))),
      await res(resonate.rpc<number>("vi4", "vbar", resonate.options({ version: 1 }))),
      await res(b1.run("vi5")),
      await res(b1.rpc("vi6")),
    ];

    const rb2 = [
      await res(resonate.run("vj1", vbar, resonate.options({ version: 2 }))),
      await res(resonate.rpc("vj2", vbar, resonate.options({ version: 2 }))),
      await res(resonate.run<number>("vj3", "vbar", resonate.options({ version: 2 }))),
      await res(resonate.rpc<number>("vj4", "vbar", resonate.options({ version: 2 }))),
      await res(b2.run("vj5")),
      await res(b2.rpc("vj6")),
    ];

    const rb3 = [
      await res(resonate.run("vk1", vbar)),
      await res(resonate.rpc("vk2", vbar)),
      await res(resonate.run<number>("vk3", "vbar")),
      await res(resonate.rpc<number>("vk4", "vbar")),
      await res(resonate.run("vk5", vbar, resonate.options({ version: 3 }))),
      await res(resonate.rpc("vk6", vbar, resonate.options({ version: 3 }))),
      await res(resonate.run<number>("vk7", "vbar", resonate.options({ version: 3 }))),
      await res(resonate.rpc<number>("vk8", "vbar", resonate.options({ version: 3 }))),
      await res(b3.run("vk9")),
      await res(b3.rpc("vk10")),
    ];

    expect([...rf1, ...rb1].every((r) => r === 1)).toBe(true);
    expect([...rf2, ...rb2].every((r) => r === 2)).toBe(true);
    expect([...rf3, ...rb3].every((r) => r === 3)).toBe(true);
  });

  test("run/rpc/detached with function pointer and string are equivalent (ctx level)", async () => {
    resonate = newResonate();

    async function ebar(): Promise<string> {
      return "bar";
    }
    resonate.register(ebar);

    const wf = async (ctx: Context): Promise<{ vals: string[]; ids: string[] }> => {
      const vals = [
        await ctx.run(ebar),
        await ctx.run<string>("ebar"),
        await ctx.rpc(ebar),
        await ctx.rpc<string>("ebar"),
      ];
      const d1 = await ctx.detached(ebar);
      const d2 = await ctx.detached("ebar");
      return { vals, ids: [d1.id, d2.id] };
    };
    resonate.register("eqwf", wf);

    const { vals, ids } = await res(resonate.run("equiv-1", wf));
    expect(vals.every((v) => v === "bar")).toBe(true);

    // The detached children run on their own root tasks; their results are
    // awaited out of band by id.
    expect(new Set(ids).size).toBe(2);
    for (const id of ids) {
      expect(id).toMatch(/^equiv-1\.d[0-9a-f]+$/);
      expect(await (await resonate.get<string>(id)).result()).toBe("bar");
    }
  });

  test("Using prefix at Resonate class prefixes all the promises", async () => {
    const prefix = "myPrefix";
    resonate = new Resonate({ prefix });

    const qux = async (info: Info): Promise<string> => {
      expect(info.id.startsWith(prefix)).toBe(true);
      expect(info.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      return "qux";
    };

    const baz = async (ctx: Context): Promise<string> => {
      expect(ctx.id.startsWith(prefix)).toBe(true);
      expect(ctx.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      await ctx.run(qux);
      return "baz";
    };

    const bar = async (ctx: Context): Promise<string> => {
      expect(ctx.id.startsWith(prefix)).toBe(true);
      expect(ctx.id.startsWith(`${prefix}:${prefix}`)).toBe(false);
      return "bar";
    };

    const foo = async (ctx: Context): Promise<string> => {
      const p = ctx.run(bar);
      await ctx.run(baz);
      await ctx.run(qux);
      await p;
      return "ok";
    };
    const f = resonate.register("pfxfoo", foo);

    const h = await f.run("fooId");
    expect(h.id).toBe(`${prefix}:fooId`);
    expect(await h.result()).toBe("ok");
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
    const f = resonate.register("envf", async (_info: Info): Promise<string> => "result");
    const result = await (await f.run("test")).result();
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

    const f = resonate.register("envf", async (_info: Info): Promise<string> => "result");
    const result = await (await f.run("test")).result();
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
});

describe("nonRetryableErrors", () => {
  let resonate: Resonate | undefined;

  afterEach(async () => {
    await resonate?.stop();
    resonate = undefined;
  });

  // Retries are opt-in for the async engine (default Never), so every test pins
  // an explicit retry policy — without nonRetryableErrors taking effect the
  // attempt counts below would exceed 1.
  const retrying = (ctx: Context, nonRetryableErrors: Array<new (...args: any[]) => Error>) =>
    ctx.options({ retryPolicy: new Constant({ delay: 1, maxRetries: 5 }), nonRetryableErrors });

  test("ctx.run: nonRetryableErrors apply to unregistered local helper functions", async () => {
    class ValidationError extends Error {}
    let attempts = 0;

    const helper = async (_info: Info): Promise<never> => {
      attempts++;
      throw new ValidationError("bad input");
    };

    const workflow = async (ctx: Context): Promise<void> => {
      await ctx.run(helper, retrying(ctx, [ValidationError]));
    };

    resonate = newResonate();
    resonate.register("nrewf1", workflow);

    await expect(res(resonate.run(rid(), workflow))).rejects.toThrow("bad input");
    expect(attempts).toBe(1);
  });

  test("ctx.run: non-retryable error propagates through nested ctx.run() calls", async () => {
    class DomainError extends Error {}
    let childAttempts = 0;

    const child = async (_info: Info): Promise<never> => {
      childAttempts++;
      throw new DomainError("domain error");
    };

    const mid = async (ctx: Context): Promise<void> => {
      await ctx.run(child, retrying(ctx, [DomainError]));
    };

    const parent = async (ctx: Context): Promise<void> => {
      await ctx.run(mid);
    };

    resonate = newResonate();
    resonate.register("nrewf2", parent);

    await expect(res(resonate.run(rid(), parent))).rejects.toThrow("domain error");
    expect(childAttempts).toBe(1);
  });

  test("ctx.run: subclass of a non-retryable class is also non-retryable", async () => {
    class BaseError extends Error {}
    class SubError extends BaseError {}
    let attempts = 0;

    const thrower = async (_info: Info): Promise<never> => {
      attempts++;
      throw new SubError("sub error");
    };

    const workflow = async (ctx: Context): Promise<void> => {
      await ctx.run(thrower, retrying(ctx, [BaseError]));
    };

    resonate = newResonate();
    resonate.register("nrewf3", workflow);

    await expect(res(resonate.run(rid(), workflow))).rejects.toThrow("sub error");
    expect(attempts).toBe(1);
  });

  test("ctx.run: empty nonRetryableErrors falls back to normal retry behavior", async () => {
    let attempts = 0;

    const failTwice = async (_info: Info): Promise<string> => {
      attempts++;
      if (attempts < 3) throw new Error("transient");
      return "success";
    };

    const workflow = async (ctx: Context): Promise<string> => {
      return ctx.run(failTwice, ctx.options({ retryPolicy: new Constant({ delay: 0 }) }));
    };

    resonate = newResonate();
    resonate.register("nrewf4", workflow);

    expect(await res(resonate.run(rid(), workflow))).toBe("success");
    expect(attempts).toBe(3);
  });
});
