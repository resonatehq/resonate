import { jest, describe, test, expect, beforeEach } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { ExponentialRetry } from "../lib/core/retries/exponential";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

describe("Cache", () => {
  const resonate = new Resonate();

  const resolveSpy = jest.fn((ctx: Context, x: any) => x);
  const resolve = resonate.register("resolve", resolveSpy);

  const rejectSpy = jest.fn((ctx: Context, e: string) => {
    throw new Error(e);
  });
  const reject = resonate.register("reject", rejectSpy, {
    retry: ExponentialRetry.atMostOnce(),
  });

  beforeEach(() => {
    resolveSpy.mockClear();
    rejectSpy.mockClear();
  });

  test("Cached runs return same resolved promise", async () => {
    const p1 = resolve("p1", 1);
    const p2 = resolve("p1", 2);
    const p3 = resonate.run("resolve", "p1", 3);
    const p4 = resonate.run("resolve", "p1", 4);

    expect(await p1).toBe(1);
    expect(await p2).toBe(1);
    expect(await p3).toBe(1);
    expect(await p4).toBe(1);

    expect(resolveSpy).toHaveBeenCalledTimes(1);
    expect(rejectSpy).toHaveBeenCalledTimes(0);
  });

  test("Cached runs return same rejected promise", async () => {
    const p1 = reject("p2", "1");
    const p2 = reject("p2", "2");
    const p3 = resonate.run("reject", "p2", "3");
    const p4 = resonate.run("reject", "p2", "4");

    expect(p1).rejects.toThrow("1");
    expect(p2).rejects.toThrow("1");
    expect(p3).rejects.toThrow("1");
    expect(p4).rejects.toThrow("1");

    try {
      await p1;
    } catch (e) {
      // ignore
    }

    expect(rejectSpy).toHaveBeenCalledTimes(1);
    expect(resolveSpy).toHaveBeenCalledTimes(0);
  });
});
