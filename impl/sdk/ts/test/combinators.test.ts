import { jest, describe, test, expect } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { Retry } from "../lib/core/retries/retry";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

function identity(ctx: Context, x: number): number {
  return x;
}

function nope(ctx: Context) {
  throw new Error("nope");
}

describe("All combinator", () => {
  const resonate = new Resonate();

  test("All returns array of values when all promises resolve", async () => {
    const f = resonate.register(
      "test-1",
      (ctx: Context) => {
        return ctx.all([ctx.run(identity, 1), ctx.run(identity, 2), ctx.run(identity, 3)]);
      },
      { retry: Retry.never() },
    );

    const r = await f("test");
    expect(r).toStrictEqual([1, 2, 3]);
  });

  test("All returns empty array when provided empty array", async () => {
    const f = resonate.register(
      "test-2",
      (ctx: Context) => {
        return ctx.all([]);
      },
      { retry: Retry.never() },
    );

    const r = await f("test");
    expect(r).toStrictEqual([]);
  });

  test("All throws error when one promise rejects", async () => {
    const f = resonate.register(
      "test-3",
      (ctx: Context) => {
        return ctx.all([ctx.run(identity, 1), ctx.run(nope), ctx.run(identity, 3)]);
      },
      { retry: Retry.never() },
    );

    await expect(f("test")).rejects.toThrow("nope");
  });
});

describe("Any combinator", () => {
  const resonate = new Resonate();

  test("Any returns value of first resolved promise", async () => {
    const f = resonate.register(
      "test-1",
      (ctx: Context) => {
        return ctx.any([ctx.run(nope), ctx.run(identity, 2), ctx.run(nope)]);
      },
      { retry: Retry.never() },
    );

    const r = await f("test");
    expect(r).toBe(2);
  });

  test("Any returns aggregate error when provided empty array", async () => {
    const f = resonate.register(
      "test-2",
      (ctx: Context) => {
        return ctx.any([]);
      },
      { retry: Retry.never() },
    );

    await expect(f("test")).rejects.toBeInstanceOf(AggregateError);
  });

  test("Any returns aggregate error when all promises reject", async () => {
    const f = resonate.register(
      "test-3",
      (ctx: Context) => {
        return ctx.any([ctx.run(nope), ctx.run(nope), ctx.run(nope)]);
      },
      { retry: Retry.never() },
    );

    await expect(f("test")).rejects.toBeInstanceOf(AggregateError);
  });
});

describe("Race combinator", () => {
  const resonate = new Resonate();

  test("Race returns value if first promise resolves", async () => {
    const f = resonate.register(
      "test-1",
      (ctx: Context) => {
        return ctx.race([ctx.run(identity, 1), ctx.run(nope), ctx.run(nope)]);
      },
      { retry: Retry.never() },
    );

    const r = await f("test");
    expect(r).toBe(1);
  });

  test("Race throws error if first promise rejects", async () => {
    const f = resonate.register(
      "test-2",
      (ctx: Context) => {
        return ctx.race([ctx.run(nope), ctx.run(identity, 2), ctx.run(identity, 3)]);
      },
      { retry: Retry.never() },
    );

    await expect(f("test")).rejects.toThrow("nope");
  });
});
