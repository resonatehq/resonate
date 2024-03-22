import { describe, test, expect, jest } from "@jest/globals";
import { Resonate, Context } from "../lib/async";
import { Retry } from "../lib/core/retries/retry";

jest.setTimeout(10000);

describe("Async functions", () => {
  async function run(ctx: Context, func: any) {
    return await ctx.run(func);
  }

  async function io(ctx: Context, func: any) {
    return await ctx.io(func);
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
    const resonate = new Resonate({
      timeout: 1000,
      retry: Retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("io", io);

    const results: string[] = [
      ordinarySuccess(),
      await ordinarySuccessAsync(),
      await resonate.run("run", "run.a", ordinarySuccess),
      await resonate.run("run", "run.b", ordinarySuccessAsync),
      await resonate.run("io", "run.c", ordinarySuccess),
      await resonate.run("io", "run.d", ordinarySuccessAsync),
    ];

    expect(results.every((r) => r === "foo")).toBe(true);
  });

  test("failure", async () => {
    const resonate = new Resonate({
      timeout: 1000,
      retry: Retry.linear(0, 3),
    });

    resonate.register("run", run);
    resonate.register("io", io);

    const functions = [
      () => ordinaryFailureAsync(),
      () => resonate.run("run", "run.a", ordinaryFailure),
      () => resonate.run("run", "run.b", ordinaryFailureAsync),
      () => resonate.run("io", "run.c", ordinaryFailure),
      () => resonate.run("io", "run.d", ordinaryFailureAsync),
    ];

    for (const f of functions) {
      await expect(f()).rejects.toThrow("foo");
    }
  });
});
