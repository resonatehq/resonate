import { Future } from "../src/context.js";

describe("Future", () => {
  describe("Symbol.iterator", () => {
    it("yields itself when completed with value", () => {
      const future = new Future<number>("f1", "completed", { kind: "value", value: 42 });

      const iter = future[Symbol.iterator]();
      const result = iter.next();

      // Should yield the future — not done
      expect(result.done).toBe(false);
      expect(result.value).toBe(future);
    });

    it("yields itself when completed with error", () => {
      const error = new Error("boom");
      const future = new Future<number>("f1", "completed", { kind: "error", error });

      const iter = future[Symbol.iterator]();
      const result = iter.next();

      // Should yield the future — not done
      expect(result.done).toBe(false);
      expect(result.value).toBe(future);
    });

    it("yields itself when pending", () => {
      const future = new Future<number>("f1", "pending");

      const iter = future[Symbol.iterator]();
      const result = iter.next();

      // Should yield the future — not done
      expect(result.done).toBe(false);
      expect(result.value).toBe(future);
    });

    it("pending future completes after receiving value via yield", () => {
      const future = new Future<number>("f1", "pending");

      const iter = future[Symbol.iterator]();

      // First call: yields the future
      const yieldResult = iter.next();
      expect(yieldResult.done).toBe(false);
      expect(yieldResult.value).toBe(future);

      // Second call: feed the resolved value
      const doneResult = iter.next(99);
      expect(doneResult.done).toBe(true);
      expect(doneResult.value).toBe(99);

      // Future state should be updated
      expect(future.state).toBe("completed");
      expect(future.value).toEqual({ kind: "value", value: 99 });
      expect(future.getValue()).toBe(99);
    });

    it("pending future completes with error after throw", () => {
      const future = new Future<number>("f1", "pending");

      const iter = future[Symbol.iterator]();

      // First call: yields the future
      const yieldResult = iter.next();
      expect(yieldResult.done).toBe(false);

      // Second call: throw an error into the generator
      const error = new Error("failed");
      expect(() => iter.throw(error)).toThrow("failed");

      // Future state should be updated
      expect(future.state).toBe("completed");
      expect(future.value).toEqual({ kind: "error", error });
    });

    it("double-yield on completed future yields the second time", () => {
      const future = new Future<number>("f1", "pending");

      // First yield* — pending, goes through yield path
      const iter1 = future[Symbol.iterator]();
      const r1 = iter1.next();
      expect(r1.done).toBe(false);
      // Simulate coroutine feeding the value back
      const d1 = iter1.next(42);
      expect(d1.done).toBe(true);
      expect(d1.value).toBe(42);

      // Future is now completed
      expect(future.state).toBe("completed");

      // Second yield* — completed, should still yield
      const iter2 = future[Symbol.iterator]();
      const r2 = iter2.next();
      expect(r2.done).toBe(false);
      expect(r2.value).toBe(future);
    });

    it("double-yield on completed-with-error future yields the second time", () => {
      const future = new Future<number>("f1", "pending");

      // First yield* — pending, error fed back
      const iter1 = future[Symbol.iterator]();
      iter1.next(); // yields
      const error = new Error("oops");
      expect(() => iter1.throw(error)).toThrow("oops");

      // Future is now completed with error
      expect(future.state).toBe("completed");

      // Second yield* — completed with error, should still yield
      const iter2 = future[Symbol.iterator]();
      const r2 = iter2.next();
      expect(r2.done).toBe(false);
      expect(r2.value).toBe(future);
    });
  });
});
