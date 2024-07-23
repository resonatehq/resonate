import { describe, test, expect, jest } from "@jest/globals";
import { mergeObjects, sleep, promiseState } from "../lib/core/utils";

jest.setTimeout(2000);

describe("mergeObjects", () => {
  test("merges two objects with non-overlapping keys", () => {
    const obj1 = { a: 1, b: 2 };
    const obj2 = { c: 3, d: 4 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1, b: 2, c: 3, d: 4 });
  });

  test("prefers values from obj1 when keys overlap and neither is undefined", () => {
    const obj1 = { a: 1, b: 2 };
    const obj2 = { b: 3, c: 4 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1, b: 2, c: 4 });
  });

  test("uses obj2 value when obj1 value is undefined", () => {
    const obj1 = { a: 1, b: undefined as number | undefined };
    const obj2 = { b: 2, c: 3 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1, b: 2, c: 3 });
  });

  test("handles nested objects", () => {
    const obj1 = { a: { x: 1 }, b: 2 };
    const obj2 = { a: { y: 2 }, c: 3 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: { x: 1 }, b: 2, c: 3 });
  });

  test("handles arrays", () => {
    const obj1 = { a: [1, 2], b: 2 };
    const obj2 = { a: [3, 4], c: 3 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: [1, 2], b: 2, c: 3 });
  });

  test("handles empty objects", () => {
    const obj1 = {};
    const obj2 = { a: 1 };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: 1 });
  });

  test("handles objects with null values", () => {
    const obj1 = { a: null, b: 2 };
    const obj2 = { a: 1, c: null };
    const result = mergeObjects(obj1, obj2);
    expect(result).toEqual({ a: null, b: 2, c: null });
  });
});

describe("sleep function", () => {
  // Helper function to measure time
  const measureTime = async (fn: () => Promise<void>): Promise<number> => {
    const start = Date.now();
    await fn();
    return Date.now() - start;
  };

  test("should resolve after specified milliseconds", async () => {
    const duration = 500;
    const elapsed = await measureTime(() => sleep(duration));
    expect(elapsed).toBeGreaterThanOrEqual(duration - 50); // Allow 50ms tolerance because of event loop
    expect(elapsed).toBeLessThan(duration + 50); // Allow 50ms tolerance because of the event loop
  });

  test("should resolve in order", async () => {
    const results: number[] = [];
    await Promise.all([
      sleep(300).then(() => results.push(3)),
      sleep(100).then(() => results.push(1)),
      sleep(200).then(() => results.push(2)),
    ]);
    expect(results).toEqual([1, 2, 3]);
  });

  test("should work with zero milliseconds", async () => {
    const start = Date.now();
    await sleep(0);
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(50); // Should resolve almost immediately
  });

  test("should reject for negative milliseconds", async () => {
    await expect(sleep(-100)).rejects.toThrow();
  });
});

describe("promiseState", () => {
  test('returns "pending" for a pending promise', async () => {
    const pendingPromise = new Promise(() => {});
    const result = await promiseState(pendingPromise);
    expect(result).toBe("pending");
  });

  test('returns "fulfilled" for a resolved promise', async () => {
    const fulfilledPromise = Promise.resolve("success");
    const result = await promiseState(fulfilledPromise);
    expect(result).toBe("resolved");
  });

  test('returns "rejected" for a rejected promise', async () => {
    const rejectedPromise = Promise.reject("error");
    const result = await promiseState(rejectedPromise);
    expect(result).toBe("rejected");
  });

  test("handles promises that resolve after a delay", async () => {
    const delayedPromise = new Promise((resolve) => setTimeout(() => resolve("delayed success"), 100));
    const immediatePendingResult = await promiseState(delayedPromise);
    expect(immediatePendingResult).toBe("pending");

    await new Promise((resolve) => setTimeout(resolve, 150));
    const laterFulfilledResult = await promiseState(delayedPromise);
    expect(laterFulfilledResult).toBe("resolved");
  });

  test("handles promises that reject after a delay", async () => {
    const delayedRejection = new Promise((_, reject) => setTimeout(() => reject("delayed error"), 100));
    const immediatePendingResult = await promiseState(delayedRejection);
    expect(immediatePendingResult).toBe("pending");

    await new Promise((resolve) => setTimeout(resolve, 150));
    const laterRejectedResult = await promiseState(delayedRejection);
    expect(laterRejectedResult).toBe("rejected");
  });
});
