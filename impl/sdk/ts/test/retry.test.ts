import { jest, describe, test, expect, beforeEach } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { IRetry } from "../lib/core/retry";
import { ExponentialRetry } from "../lib/core/retries/exponential";
import { LinearRetry } from "../lib/core/retries/linear";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

describe("Retry policies", () => {
  const resonate = new Resonate({
    timeout: Number.MAX_SAFE_INTEGER,
    retry: () => ExponentialRetry.atMostOnce(),
  });
  resonate.register("async", foo);
  resonate.register("generator", bar);

  const spy = jest.fn(nope);

  beforeEach(() => {
    spy.mockClear();
  });

  for (const f of ["async", "generator"]) {
    for (let i = 1; i <= 10; i++) {
      test(`Exponential at most once (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `e-amo-${f}-${i}`, spy, ExponentialRetry.atMostOnce())).rejects.toThrow("nope");
        expect(spy).toHaveBeenCalledTimes(1);
      });
      test(`Exponential at least once (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `e-alo-${f}-${i}`, spy, ExponentialRetry.atLeastOnce(i, 0))).rejects.toThrow(
          "nope",
        );
        expect(spy).toHaveBeenCalledTimes(i);
      });
      test(`Linear (${f}, ${i})`, async () => {
        await expect(resonate.run(f, `l-${f}-${i}`, spy, new LinearRetry(0, i))).rejects.toThrow("nope");
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
