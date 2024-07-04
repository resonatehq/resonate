import { describe, test, expect, jest } from "@jest/globals";
import { Resonate, Context } from "../lib/async";

jest.setTimeout(10000);

describe("Functions: versions", () => {
  const resonate = new Resonate();
  resonate.register("default", async (ctx: Context, val: string) => {
    return await ctx.run(() => val);
  });
  resonate.register(
    "durable",
    async (ctx: Context, val: string) => {
      return await ctx.run(() => val, ctx.options({ durable: true }));
    },
    { durable: false },
  );
  resonate.register(
    "volatile",
    async (ctx: Context, val: string) => {
      return await ctx.run(() => val, ctx.options({ durable: false }));
    },
    { durable: false },
  );

  test("default should be durable", async () => {
    expect(await resonate.run("default", "default.a", "a")).toBe("a");
    expect(await resonate.run("default", "default.a", "b")).toBe("a");
    expect(await resonate.run("default", "default.b", "b")).toBe("b");
  });

  test("durable should be durable", async () => {
    expect(await resonate.run("durable", "durable.a", "a")).toBe("a");
    expect(await resonate.run("durable", "durable.a", "b")).toBe("a");
    expect(await resonate.run("durable", "durable.b", "b")).toBe("b");
  });

  test("volatile should not be durable", async () => {
    expect(await resonate.run("volatile", "volatile.a", "a")).toBe("a");
    expect(await resonate.run("volatile", "volatile.a", "b")).toBe("b");
    expect(await resonate.run("volatile", "volatile.b", "b")).toBe("b");
  });
});
