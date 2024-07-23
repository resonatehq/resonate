import { beforeEach, describe, expect, jest, test } from "@jest/globals";
import { options } from "../lib/core/options";
import { never } from "../lib/core/retry";
import { Resonate, Context } from "../lib/resonate";

jest.setTimeout(10000);

describe("Durability tests", () => {
  const resonate = new Resonate();
  const createSpy = jest.spyOn(resonate.store.promises, "create");
  const resolveSpy = jest.spyOn(resonate.store.promises, "resolve");
  const rejectSpy = jest.spyOn(resonate.store.promises, "reject");

  resonate.register("default", async (ctx: Context, val: string) => {
    return await ctx.run(() => val);
  });

  resonate.register(
    "durable",
    async (ctx: Context, val: string) => {
      return await ctx.run(() => val, options({ durable: true }));
    },
    { durable: false },
  );

  resonate.register(
    "fails-durable",
    async (ctx: Context, val: string) => {
      await ctx.run(
        (ctx: Context) => {
          throw new Error(val);
        },
        options({ durable: true, retryPolicy: never() }),
      );
    },
    { durable: true, retryPolicy: never() },
  );

  resonate.register(
    "volatile",
    async (ctx: Context, val: string) => {
      return await ctx.run(() => val, options({ durable: false }));
    },
    { durable: false },
  );

  resonate.register(
    "fails-volatile",
    async (ctx: Context, val: string) => {
      return await ctx.run(
        () => {
          throw new Error(val);
        },
        options({ durable: false, retryPolicy: never() }),
      );
    },
    { durable: false, retryPolicy: never() },
  );

  beforeEach(() => {
    createSpy.mockClear();
    resolveSpy.mockClear();
    rejectSpy.mockClear();
  });

  test("default should be durable", async () => {
    // Test the function executes correctly
    expect(await resonate.run("default", "default.a", "a")).toBe("a");
    expect(await resonate.run("default", "default.a", "b")).toBe("a");
    expect(await resonate.run("default", "default.b", "b")).toBe("b");

    // Test the store was called
    // TODO(avillega): test that it has been called more than once, since the top level is always durable
    expect(createSpy).toHaveBeenCalled();
    expect(resolveSpy).toHaveBeenCalled();
  });

  test("durable should be durable", async () => {
    expect(await resonate.run("durable", "durable.a", "a")).toBe("a");
    expect(await resonate.run("durable", "durable.a", "b")).toBe("a");
    expect(await resonate.run("durable", "durable.b", "b")).toBe("b");
    await expect(resonate.run("fails-durable", "fails-durable.a", "a")).rejects.toThrow("a");

    // Test the store was called
    // TODO(avillega): test that it has been called more than once, since the top level is always durable
    expect(createSpy).toHaveBeenCalled();
    expect(resolveSpy).toHaveBeenCalled();
    expect(rejectSpy).toHaveBeenCalled();
  });

  test("volatile should not be durable", async () => {
    // Top level functions are always durable so we check that the spy has been called once per top level invocation

    expect(await resonate.run("volatile", "volatile.a", "a")).toBe("a");
    expect(createSpy).toHaveBeenCalledTimes(1);
    expect(resolveSpy).toHaveBeenCalledTimes(1);

    // Even non-durable results are locally cached and we don't need to call the store again
    expect(await resonate.run("volatile", "volatile.a", "b")).toBe("a");
    expect(createSpy).toHaveBeenCalledTimes(1);
    expect(resolveSpy).toHaveBeenCalledTimes(1);

    expect(await resonate.run("volatile", "volatile.b", "b")).toBe("b");
    expect(createSpy).toHaveBeenCalledTimes(2);
    expect(resolveSpy).toHaveBeenCalledTimes(2);

    // We throw in the internal function, we still only call the store for the rejection of the top level
    await expect(resonate.run("fails-volatile", "fails-volatile.a", "a")).rejects.toThrow("a");
    expect(createSpy).toHaveBeenCalledTimes(3);
    expect(rejectSpy).toHaveBeenCalledTimes(1);
  });
});
