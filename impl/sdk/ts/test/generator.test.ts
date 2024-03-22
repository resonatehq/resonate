import { describe, test, expect, jest } from "@jest/globals";
import { Retry } from "../lib/core/retries/retry";
import * as generator from "../lib/generator";

jest.setTimeout(10000);

describe("Generator functions", () => {
  function* run(ctx: generator.Context, func: any): Generator<any> {
    return yield ctx.run(func);
  }

  function* call(ctx: generator.Context, func: any): Generator<any> {
    return yield yield ctx.call(func);
  }

  function* callFuture(ctx: generator.Context, func: any): Generator<any> {
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

  test("success", async () => {
    const resonate = new generator.Resonate({
      timeout: 1000,
      retry: Retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("call", call);
    resonate.register("callFuture", callFuture);

    const results: string[] = [
      ordinarySuccess(),
      await ordinarySuccessAsync(),
      await resonate.run("run", "run.a", ordinarySuccess),
      await resonate.run("run", "run.b", ordinarySuccessAsync),
      await resonate.run("call", "run.c", ordinarySuccess),
      await resonate.run("call", "run.d", ordinarySuccessAsync),
      await resonate.run("callFuture", "run.e", ordinarySuccess),
      await resonate.run("callFuture", "run.f", ordinarySuccessAsync),
    ];

    expect(results.every((r) => r === "foo")).toBe(true);
  });

  test("failure", async () => {
    const resonate = new generator.Resonate({
      timeout: 1000,
      retry: Retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("call", call);
    resonate.register("callFuture", callFuture);

    const functions = [
      () => ordinaryFailureAsync(),
      () => resonate.run("run", "run.a", ordinaryFailure),
      () => resonate.run("run", "run.b", ordinaryFailureAsync),
      () => resonate.run("call", "run.c", ordinaryFailure),
      () => resonate.run("call", "run.d", ordinaryFailureAsync),
      () => resonate.run("callFuture", "run.e", ordinaryFailure),
      () => resonate.run("callFuture", "run.f", ordinaryFailureAsync),
    ];

    for (const f of functions) {
      await expect(f()).rejects.toThrow("foo");
    }
  });
});
