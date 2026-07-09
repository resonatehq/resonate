// Unit tests for the DurablePromise handle — the async engine's user-facing
// analogue of the generator engine's Future (tests/future.test.ts). Its Promise
// behavior must be indistinguishable from the promise it wraps.

import { describe, expect, test } from "@jest/globals";
import { DurablePromise } from "../../src/async/index.js";

describe("DurablePromise", () => {
  test("exposes the durable promise id and the brand tag", () => {
    const dp = new DurablePromise<number>("my-id", Promise.resolve(42));
    expect(dp.id).toBe("my-id");
    expect(String(dp)).toBe("[object DurablePromise]");
    expect(dp).toBeInstanceOf(DurablePromise);
  });

  test("awaiting resolves with the wrapped promise's value", async () => {
    const dp = new DurablePromise<number>("my-id", Promise.resolve(42));
    expect(await dp).toBe(42);
  });

  test("then/catch/finally delegate to the wrapped promise", async () => {
    const dp = new DurablePromise<number>("ok-id", Promise.resolve(42));
    expect(await dp.then((v) => v + 1)).toBe(43);

    const bad = new DurablePromise<number>("bad-id", Promise.reject(new Error("nope")));
    expect(await bad.catch((e) => `caught:${(e as Error).message}`)).toBe("caught:nope");

    let ran = false;
    await dp.finally(() => {
      ran = true;
    });
    expect(ran).toBe(true);
  });

  test("awaiting a rejected handle throws the wrapped rejection", async () => {
    const bad = new DurablePromise<number>("bad-id", Promise.reject(new Error("kaboom")));
    await expect(async () => bad.then((v) => v)).rejects.toThrow("kaboom");
  });
});
