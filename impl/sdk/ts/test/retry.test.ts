import { jest, describe, test, expect, beforeEach } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { IRetry } from "../lib/core/retry";
import { Retry } from "../lib/core/retries/retry";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

describe("Retry policies", () => {
  const resonate = new Resonate({ timeout: Number.MAX_SAFE_INTEGER, retry: () => Retry.atMostOnce() });
  resonate.register("async", foo);
  resonate.register("generator", bar);

  const spy = jest.fn(nope);

  beforeEach(() => {
    spy.mockClear();
  });

  for (const f of ["async", "generator"]) {
    test(`At most once (${f})`, async () => {
      await expect(resonate.run(f, `${f}-1`, spy, Retry.atMostOnce())).rejects.toThrow("nope");
      expect(spy).toHaveBeenCalledTimes(1);
    });

    for (let i = 2; i < 10; i++) {
      test(`At least once (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `${f}-${i}`, spy, Retry.atLeastOnce(i, 0))).rejects.toThrow("nope");
        expect(spy).toHaveBeenCalledTimes(i);
      });
    }
  }
});

async function foo(context: Context, next: (c: Context) => void, retry: IRetry) {
  return await context.run(next, { retry });
}

function* bar(context: Context, next: (c: Context) => void, retry: IRetry): Generator {
  return yield context.run(next, { retry });
}

function nope(context: Context) {
  throw new Error("nope");
}
