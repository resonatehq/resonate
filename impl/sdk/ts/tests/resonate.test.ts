import { setTimeout } from "node:timers/promises";
import type { Context } from "../src/context";
import { Resonate } from "../src/resonate";

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
});
