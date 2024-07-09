import { describe, test, expect, jest } from "@jest/globals";
import { Resonate, Context } from "../lib/async";
import * as retry from "../lib/core/retry";
import * as utils from "../lib/core/utils";

jest.setTimeout(10000);

describe("Functions: async", () => {
  async function run(ctx: Context, func: any) {
    return await ctx.run(func);
  }

  async function runFC(ctx: Context, func: any) {
    return await ctx.run({ func: func });
  }

  function ordinarySuccess() {
    return "foo";
  }

  function ordinaryFailure() {
    throw new Error("foo");
  }

  async function ordinarySuccessAsync() {
    return "foo";
  }

  async function ordinaryFailureAsync() {
    throw new Error("foo");
  }

  async function deferredSuccess(ctx: Context) {
    return await ctx.run("success", ctx.options({ pollFrequency: 0 }));
  }

  async function deferredSuccessRFC(ctx: Context) {
    return await ctx.run({ funcName: "success", opts: ctx.options({ pollFrequency: 0 }) });
  }

  async function deferredFailure(ctx: Context) {
    return await ctx.run("failure", ctx.options({ pollFrequency: 0 }));
  }

  async function deferredFailureRFC(ctx: Context) {
    return await ctx.run({ funcName: "failure", opts: ctx.options({ pollFrequency: 0 }) });
  }

  test("success", async () => {
    const resonate = new Resonate({
      timeout: 1000,
      retryPolicy: retry.exponential(
        100, // initial delay (in ms)
        2, // backoff factor
        Infinity, // max attempts
        60000, // max delay (in ms, 1 minute)
      ),
    });

    resonate.register("run", run);
    resonate.register("runFC", runFC);
    resonate.register("success", ordinarySuccess);
    resonate.register("successAsync", ordinarySuccessAsync);

    // pre-resolve deferred
    const deferred = await resonate.promises.create("success", Date.now() + 1000, {
      idempotencyKey: utils.hash("success"),
    });
    deferred.resolve("foo");

    const results: string[] = [
      ordinarySuccess(),
      await ordinarySuccessAsync(),
      await resonate.run("run", "run.a", ordinarySuccess),
      await resonate.run("run", "run.b", ordinarySuccessAsync),
      await resonate.run("run", "run.c", deferredSuccess),
      await resonate.run("success", "run.d"),
      await resonate.run("successAsync", "run.e"),
      await resonate.run({ funcName: "run", id: "run.a", args: [ordinarySuccess] }),
      await resonate.run({ funcName: "run", id: "run.b", args: [ordinarySuccessAsync] }),
      await resonate.run({ funcName: "run", id: "run.c", args: [deferredSuccess] }),
      await resonate.run({ funcName: "success", id: "run.d" }),
      await resonate.run({ funcName: "successAsync", id: "run.e" }),
      await resonate.run({ funcName: "runFC", id: "run.f", args: [deferredSuccessRFC] }),
    ];

    expect(results.every((r) => r === "foo")).toBe(true);
  });

  test("failure", async () => {
    const resonate = new Resonate({
      timeout: 1000,
      retryPolicy: retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("runFC", runFC);
    resonate.register("failure", ordinaryFailure);
    resonate.register("failureAsync", ordinaryFailureAsync);

    // pre-reject deferred
    const deferred = await resonate.promises.create("failure", Date.now() + 1000, {
      idempotencyKey: utils.hash("failure"),
    });
    deferred.reject(new Error("foo"));

    const functions = [
      () => ordinaryFailureAsync(),
      () => resonate.run("run", "run.a", ordinaryFailure),
      () => resonate.run("run", "run.b", ordinaryFailureAsync),
      () => resonate.run("run", "run.c", deferredFailure),
      () => resonate.run("failure", "run.d"),
      () => resonate.run("failureAsync", "run.e"),
      () => resonate.run({ funcName: "run", id: "run.a", args: [ordinaryFailure] }),
      () => resonate.run({ funcName: "run", id: "run.b", args: [ordinaryFailureAsync] }),
      () => resonate.run({ funcName: "run", id: "run.c", args: [deferredFailure] }),
      () => resonate.run({ funcName: "failure", id: "run.d" }),
      () => resonate.run({ funcName: "failureAsync", id: "run.e" }),
      () => resonate.run({ funcName: "runFC", id: "run.f", args: [deferredFailureRFC] }),
    ];

    for (const f of functions) {
      await expect(f()).rejects.toThrow("foo");
    }
  });
  test("FC and non FC apis produce same result", async () => {
    const resonate = new Resonate({
      timeout: 1000,
      retryPolicy: retry.linear(0, 3),
    });

    resonate.register("runFC", async (ctx: Context, arg1: string, arg2: string) => {
      const a = await ctx.run({
        func: (ctx: Context, arg: string) => {
          return arg;
        },
        args: [arg1],
      });
      const b = await ctx.run({
        func: (ctx: Context, arg: string) => {
          return arg;
        },
        args: [arg2],
      });
      return a + b;
    });

    resonate.register("run", async (ctx: Context, arg1: string, arg2: string) => {
      const a = await ctx.run((ctx: Context, arg: string) => {
        return arg;
      }, arg1);
      const b = await ctx.run((ctx: Context, arg: string) => {
        return arg;
      }, arg2);
      return a + b;
    });

    const fcResult = await resonate.run({ funcName: "runFC", id: "run.a", args: ["foo", "bar"] });
    const regularResult = await resonate.run("run", "run.b", "foo", "bar");

    expect(fcResult).toBe(regularResult);
  });
});
