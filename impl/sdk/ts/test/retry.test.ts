import { jest, describe, test, expect, beforeEach } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { IRetry } from "../lib/core/retry";
import { Retry } from "../lib/core/retries/retry";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

describe("Retry delays", () => {
  for (let i = 1; i <= 10; i++) {
    test(`Exponential (${i})`, () => {
      const ctx = { attempt: 0, timeout: Number.MAX_SAFE_INTEGER };
      const initialDelay = 100 * i;
      const backoffFactor = i + 1;
      const retry = Retry.exponential(initialDelay, backoffFactor, 10, Infinity);

      for (const d of retry.iterator(ctx)) {
        expect(d).toBe(ctx.attempt === 0 ? 0 : initialDelay * Math.pow(backoffFactor, ctx.attempt - 1));
        ctx.attempt++;
      }

      expect(ctx.attempt).toBe(10);
      expect(retry.next(ctx)).toStrictEqual({ done: true });
    });

    test(`Linear (${i})`, () => {
      const ctx = { attempt: 0, timeout: Number.MAX_SAFE_INTEGER };
      const delay = 100 * i;
      const retry = Retry.linear(delay, 10);

      for (const d of retry.iterator(ctx)) {
        expect(d).toBe(ctx.attempt === 0 ? 0 : delay);
        ctx.attempt++;
      }

      expect(ctx.attempt).toBe(10);
      expect(retry.next(ctx)).toStrictEqual({ done: true });
    });

    test(`Never (${i})`, () => {
      const ctx = { attempt: 0, timeout: Number.MAX_SAFE_INTEGER };
      const retry = Retry.never();

      for (const d of retry.iterator(ctx)) {
        expect(d).toBe(0);
        ctx.attempt++;
      }

      expect(ctx.attempt).toBe(1);
      expect(retry.next(ctx)).toStrictEqual({ done: true });
    });
  }
});

describe("Context retries", () => {
  const resonate = new Resonate();
  resonate.register(
    "async",
    foo,
    resonate.options({
      timeout: Number.MAX_SAFE_INTEGER,
      retry: Retry.never(),
    }),
  );
  resonate.register(
    "generator",
    bar,
    resonate.options({
      timeout: Number.MAX_SAFE_INTEGER,
      retry: Retry.never(),
    }),
  );

  const spy = jest.fn(nope);

  beforeEach(() => {
    spy.mockClear();
  });

  for (const f of ["async", "generator"]) {
    for (let i = 1; i <= 10; i++) {
      test(`Exponential (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `e-alo-${f}-${i}`, spy, Retry.exponential(0, 0, i, 0))).rejects.toThrow("nope");
        expect(spy).toHaveBeenCalledTimes(i);
      });
      test(`Linear (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `l-${f}-${i}`, spy, Retry.linear(0, i))).rejects.toThrow("nope");
        expect(spy).toHaveBeenCalledTimes(i);
      });
      test(`Never (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `e-amo-${f}-${i}`, spy, Retry.never())).rejects.toThrow("nope");
        expect(spy).toHaveBeenCalledTimes(1);
      });
    }
  }
});

async function foo(ctx: Context, next: (c: Context) => void, retry: IRetry) {
  return await ctx.run(next, ctx.options({ retry }));
}

function* bar(ctx: Context, next: (c: Context) => void, retry: IRetry): Generator {
  return yield ctx.run(next, ctx.options({ retry }));
}

function nope() {
  throw new Error("nope");
}
