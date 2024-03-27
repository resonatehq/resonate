import { describe, test, expect, jest } from "@jest/globals";
import { Retry } from "../lib/core/retries/retry";
import * as utils from "../lib/core/utils";
import { Resonate, Context } from "../lib/generator";

jest.setTimeout(10000);

describe("Functions: generator", () => {
  function* run(ctx: Context, func: any): Generator<any> {
    return yield ctx.run(func);
  }

  function* call(ctx: Context, func: any): Generator<any> {
    return yield yield ctx.call(func);
  }

  function* callFuture(ctx: Context, func: any): Generator<any> {
    return yield ctx.call(func);
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

  function* deferredSuccess(ctx: Context): Generator<any> {
    return yield ctx.run("success", ctx.options({ poll: 0 }));
  }

  function* deferredFailure(ctx: Context): Generator<any> {
    return yield ctx.run("failure", ctx.options({ poll: 0 }));
  }

  function* generatorSuccess() {
    return "foo";
  }

  function* generatorFailure() {
    throw new Error("foo");
  }

  test("success", async () => {
    const resonate = new Resonate({
      timeout: 1000,
      retry: Retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("call", call);
    resonate.register("callFuture", callFuture);
    resonate.register("success", generatorSuccess);

    // pre-resolve deferred
    const deferred = await resonate.promises.create("success", 1000, {
      idempotencyKey: utils.hash("success"),
    });
    deferred.resolve("foo");

    const results: string[] = [
      ordinarySuccess(),
      await ordinarySuccessAsync(),
      await resonate.run("run", "run.a", ordinarySuccess),
      await resonate.run("run", "run.b", ordinarySuccessAsync),
      await resonate.run("run", "run.c", deferredSuccess),
      await resonate.run("run", "run.d", generatorSuccess),
      await resonate.run("call", "run.e", ordinarySuccess),
      await resonate.run("call", "run.f", ordinarySuccessAsync),
      await resonate.run("call", "run.g", deferredSuccess),
      await resonate.run("call", "run.h", generatorSuccess),
      await resonate.run("callFuture", "run.i", ordinarySuccess),
      await resonate.run("callFuture", "run.j", ordinarySuccessAsync),
      await resonate.run("callFuture", "run.k", deferredSuccess),
      await resonate.run("callFuture", "run.l", generatorSuccess),
      await resonate.run("success", "run.m"),
    ];

    expect(results.every((r) => r === "foo")).toBe(true);
  });

  test("failure", async () => {
    const resonate = new Resonate({
      timeout: 1000,
      retry: Retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("call", call);
    resonate.register("callFuture", callFuture);
    resonate.register("failure", generatorFailure);

    // pre-reject deferred
    const deferred = await resonate.promises.create("failure", 1000, {
      idempotencyKey: utils.hash("failure"),
    });
    deferred.reject(new Error("foo"));

    const functions = [
      () => ordinaryFailureAsync(),
      () => resonate.run("run", "run.a", ordinaryFailure),
      () => resonate.run("run", "run.b", ordinaryFailureAsync),
      () => resonate.run("run", "run.c", deferredFailure),
      () => resonate.run("run", "run.d", generatorFailure),
      () => resonate.run("call", "run.e", ordinaryFailure),
      () => resonate.run("call", "run.f", ordinaryFailureAsync),
      () => resonate.run("call", "run.g", deferredFailure),
      () => resonate.run("call", "run.h", generatorFailure),
      () => resonate.run("callFuture", "run.i", ordinaryFailure),
      () => resonate.run("callFuture", "run.j", ordinaryFailureAsync),
      () => resonate.run("callFuture", "run.k", deferredFailure),
      () => resonate.run("callFuture", "run.l", generatorFailure),
      () => resonate.run("failure", "run.m"),
    ];

    for (const f of functions) {
      await expect(f()).rejects.toThrow("foo");
    }
  });
});
