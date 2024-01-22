import { jest, describe, test, expect } from "@jest/globals";

import { RemotePromiseStore } from "../lib/core/stores/remote";
import { LocalPromiseStore } from "../lib/core/stores/local";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

describe("Resonate Server Tests", () => {
  const useDurable = process.env.USE_DURABLE === "true";
  const url = process.env.RESONATE_URL || "http://localhost:8001";

  const store = useDurable ? new RemotePromiseStore(url) : new LocalPromiseStore();

  describe("State Transition Tests", () => {
    test("Test Case 0: transitions from Init to Pending via Create", async () => {
      const promise = await store.create(
        "id0",
        undefined,
        true,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("PENDING");
      expect(promise.id).toBe("id0");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 1: transitions from Init to Pending via Create", async () => {
      const promise = await store.create(
        "id1",
        undefined,
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("PENDING");
      expect(promise.id).toBe("id1");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 2: transitions from Init to Pending via Create", async () => {
      const promise = await store.create("id2", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      expect(promise.state).toBe("PENDING");
      expect(promise.id).toBe("id2");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 3: transitions from Init to Pending via Create", async () => {
      const promise = await store.create("id3", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      expect(promise.state).toBe("PENDING");
      expect(promise.id).toBe("id3");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 4: transitions from Init to Init via Resolve", async () => {
      await expect(store.resolve("id4", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 5: transitions from Init to Init via Resolve", async () => {
      await expect(store.resolve("id5", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 6: transitions from Init to Init via Resolve", async () => {
      await expect(store.resolve("id6", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 7: transitions from Init to Init via Resolve", async () => {
      await expect(store.resolve("id7", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 8: transitions from Init to Init via Reject", async () => {
      await expect(store.reject("id8", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 9: transitions from Init to Init via Reject", async () => {
      await expect(store.reject("id9", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 10: transitions from Init to Init via Reject", async () => {
      await expect(store.reject("id10", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 11: transitions from Init to Init via Reject", async () => {
      await expect(store.reject("id11", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 12: transitions from Init to Init via Cancel", async () => {
      await expect(store.cancel("id12", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 13: transitions from Init to Init via Cancel", async () => {
      await expect(store.cancel("id13", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 14: transitions from Init to Init via Cancel", async () => {
      await expect(store.cancel("id14", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 15: transitions from Init to Init via Cancel", async () => {
      await expect(store.cancel("id15", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 16: transitions from Pending to Pending via Create", async () => {
      await store.create("id16", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id16", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 17: transitions from Pending to Pending via Create", async () => {
      await store.create("id17", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id17", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 18: transitions from Pending to Pending via Create", async () => {
      await store.create("id18", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id18", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 19: transitions from Pending to Pending via Create", async () => {
      await store.create("id19", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id19", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 20: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id20", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id20", undefined, true, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id20");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 21: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id21", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id21", undefined, false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id21");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 22: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id22", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id22", "iku", true, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id22");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 23: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id23", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id23", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id23");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 24: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id24", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id24", undefined, true, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id24");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 25: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id25", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id25", undefined, false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id25");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 26: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id26", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id26", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id26");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 27: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id27", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id27", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id27");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 28: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id28", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id28", undefined, true, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id28");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 29: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id29", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id29", undefined, false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id29");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 30: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id30", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id30", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id30");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 31: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id31", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id31", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id31");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 32: transitions from Pending to Pending via Create", async () => {
      await store.create("id32", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id32", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 33: transitions from Pending to Pending via Create", async () => {
      await store.create("id33", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id33", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 34: transitions from Pending to Pending via Create", async () => {
      await store.create("id34", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.create("id34", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      expect(promise.state).toBe("PENDING");
      expect(promise.id).toBe("id34");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 35: transitions from Pending to Pending via Create", async () => {
      await store.create("id35", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.create(
        "id35",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("PENDING");
      expect(promise.id).toBe("id35");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 36: transitions from Pending to Pending via Create", async () => {
      await store.create("id36", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id36", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 37: transitions from Pending to Pending via Create", async () => {
      await store.create("id37", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      await expect(
        store.create("id37", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 38: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id38", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id38", undefined, true, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id38");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 39: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id39", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id39", undefined, false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id39");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 40: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id40", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id40", "iku", true, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id40");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 41: transitions from Pending to Resolved via Resolve", async () => {
      await store.create("id41", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.resolve("id41", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id41");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 42: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id42", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id42", undefined, true, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id42");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 43: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id43", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id43", undefined, false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id43");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 44: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id44", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id44", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id44");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 45: transitions from Pending to Rejected via Reject", async () => {
      await store.create("id45", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.reject("id45", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id45");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 46: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id46", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id46", undefined, true, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id46");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 47: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id47", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id47", undefined, false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id47");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 48: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id48", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id48", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id48");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 49: transitions from Pending to Canceled via Cancel", async () => {
      await store.create("id49", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

      const promise = await store.cancel("id49", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id49");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 50: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id50", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id50", undefined, false, undefined, undefined);

      await expect(
        store.create("id50", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 51: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id51", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id51", undefined, false, undefined, undefined);

      await expect(
        store.create("id51", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 52: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id52", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id52", undefined, false, undefined, undefined);

      await expect(
        store.create("id52", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 53: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id53", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id53", undefined, false, undefined, undefined);

      await expect(
        store.create("id53", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 54: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id54", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id54", undefined, false, undefined, undefined);

      await expect(store.resolve("id54", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 55: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id55", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id55", undefined, false, undefined, undefined);

      await expect(store.resolve("id55", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 56: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id56", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id56", undefined, false, undefined, undefined);

      await expect(store.resolve("id56", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 57: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id57", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id57", undefined, false, undefined, undefined);

      await expect(store.resolve("id57", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 58: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id58", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id58", undefined, false, undefined, undefined);

      await expect(store.reject("id58", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 59: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id59", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id59", undefined, false, undefined, undefined);

      await expect(store.reject("id59", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 60: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id60", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id60", undefined, false, undefined, undefined);

      await expect(store.reject("id60", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 61: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id61", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id61", undefined, false, undefined, undefined);

      await expect(store.reject("id61", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 62: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id62", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id62", undefined, false, undefined, undefined);

      await expect(store.cancel("id62", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 63: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id63", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id63", undefined, false, undefined, undefined);

      await expect(store.cancel("id63", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 64: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id64", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id64", undefined, false, undefined, undefined);

      await expect(store.cancel("id64", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 65: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id65", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id65", undefined, false, undefined, undefined);

      await expect(store.cancel("id65", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 66: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id66", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id66", "iku", false, undefined, undefined);

      await expect(
        store.create("id66", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 67: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id67", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id67", "iku", false, undefined, undefined);

      await expect(
        store.create("id67", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 68: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id68", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id68", "iku", false, undefined, undefined);

      await expect(
        store.create("id68", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 69: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id69", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id69", "iku", false, undefined, undefined);

      await expect(
        store.create("id69", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 70: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id70", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id70", "iku", false, undefined, undefined);

      await expect(store.resolve("id70", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 71: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id71", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id71", "iku", false, undefined, undefined);

      await expect(store.resolve("id71", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 72: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id72", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id72", "iku", false, undefined, undefined);

      const promise = await store.resolve("id72", "iku", true, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id72");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 73: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id73", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id73", "iku", false, undefined, undefined);

      const promise = await store.resolve("id73", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id73");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 74: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id74", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id74", "iku", false, undefined, undefined);

      await expect(store.resolve("id74", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 75: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id75", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id75", "iku", false, undefined, undefined);

      await expect(store.resolve("id75", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 76: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id76", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id76", "iku", false, undefined, undefined);

      await expect(store.reject("id76", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 77: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id77", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id77", "iku", false, undefined, undefined);

      await expect(store.reject("id77", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 78: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id78", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id78", "iku", false, undefined, undefined);

      await expect(store.reject("id78", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 79: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id79", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id79", "iku", false, undefined, undefined);

      const promise = await store.reject("id79", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id79");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 80: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id80", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id80", "iku", false, undefined, undefined);

      await expect(store.reject("id80", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 81: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id81", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id81", "iku", false, undefined, undefined);

      await expect(store.reject("id81", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 82: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id82", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id82", "iku", false, undefined, undefined);

      await expect(store.cancel("id82", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 83: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id83", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id83", "iku", false, undefined, undefined);

      await expect(store.cancel("id83", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 84: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id84", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id84", "iku", false, undefined, undefined);

      await expect(store.cancel("id84", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 85: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id85", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id85", "iku", false, undefined, undefined);

      const promise = await store.cancel("id85", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id85");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 86: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id86", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id86", "iku", false, undefined, undefined);

      await expect(store.cancel("id86", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 87: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id87", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id87", "iku", false, undefined, undefined);

      await expect(store.cancel("id87", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 88: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id88", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id88", undefined, false, undefined, undefined);

      await expect(
        store.create("id88", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 89: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id89", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id89", undefined, false, undefined, undefined);

      await expect(
        store.create("id89", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 90: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id90", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id90", undefined, false, undefined, undefined);

      await expect(
        store.create("id90", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 91: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id91", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id91", undefined, false, undefined, undefined);

      const promise = await store.create(
        "id91",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id91");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 92: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id92", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id92", undefined, false, undefined, undefined);

      await expect(
        store.create("id92", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 93: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id93", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id93", undefined, false, undefined, undefined);

      await expect(
        store.create("id93", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 94: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id94", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id94", undefined, false, undefined, undefined);

      await expect(store.resolve("id94", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 95: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id95", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id95", undefined, false, undefined, undefined);

      await expect(store.resolve("id95", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 96: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id96", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id96", undefined, false, undefined, undefined);

      await expect(store.resolve("id96", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 97: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id97", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id97", undefined, false, undefined, undefined);

      await expect(store.resolve("id97", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 98: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id98", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id98", undefined, false, undefined, undefined);

      await expect(store.reject("id98", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 99: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id99", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id99", undefined, false, undefined, undefined);

      await expect(store.reject("id99", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 100: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id100", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id100", undefined, false, undefined, undefined);

      await expect(store.reject("id100", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 101: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id101", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id101", undefined, false, undefined, undefined);

      await expect(store.reject("id101", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 102: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id102", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id102", undefined, false, undefined, undefined);

      await expect(store.cancel("id102", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 103: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id103", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id103", undefined, false, undefined, undefined);

      await expect(store.cancel("id103", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 104: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id104", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id104", undefined, false, undefined, undefined);

      await expect(store.cancel("id104", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 105: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id105", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id105", undefined, false, undefined, undefined);

      await expect(store.cancel("id105", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 106: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id106", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id106", "iku", false, undefined, undefined);

      await expect(
        store.create("id106", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 107: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id107", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id107", "iku", false, undefined, undefined);

      await expect(
        store.create("id107", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 108: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id108", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id108", "iku", false, undefined, undefined);

      await expect(
        store.create("id108", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 109: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id109", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id109", "iku", false, undefined, undefined);

      const promise = await store.create(
        "id109",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id109");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 110: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id110", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id110", "iku", false, undefined, undefined);

      await expect(
        store.create("id110", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 111: transitions from Resolved to Resolved via Create", async () => {
      await store.create("id111", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id111", "iku", false, undefined, undefined);

      await expect(
        store.create("id111", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 112: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id112", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id112", "iku", false, undefined, undefined);

      await expect(store.resolve("id112", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 113: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id113", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id113", "iku", false, undefined, undefined);

      await expect(store.resolve("id113", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 114: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id114", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id114", "iku", false, undefined, undefined);

      const promise = await store.resolve("id114", "iku", true, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id114");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 115: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id115", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id115", "iku", false, undefined, undefined);

      const promise = await store.resolve("id115", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id115");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 116: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id116", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id116", "iku", false, undefined, undefined);

      await expect(store.resolve("id116", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 117: transitions from Resolved to Resolved via Resolve", async () => {
      await store.create("id117", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id117", "iku", false, undefined, undefined);

      await expect(store.resolve("id117", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 118: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id118", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id118", "iku", false, undefined, undefined);

      await expect(store.reject("id118", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 119: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id119", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id119", "iku", false, undefined, undefined);

      await expect(store.reject("id119", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 120: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id120", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id120", "iku", false, undefined, undefined);

      await expect(store.reject("id120", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 121: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id121", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id121", "iku", false, undefined, undefined);

      const promise = await store.reject("id121", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id121");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 122: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id122", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id122", "iku", false, undefined, undefined);

      await expect(store.reject("id122", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 123: transitions from Resolved to Resolved via Reject", async () => {
      await store.create("id123", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id123", "iku", false, undefined, undefined);

      await expect(store.reject("id123", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 124: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id124", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id124", "iku", false, undefined, undefined);

      await expect(store.cancel("id124", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 125: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id125", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id125", "iku", false, undefined, undefined);

      await expect(store.cancel("id125", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 126: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id126", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id126", "iku", false, undefined, undefined);

      await expect(store.cancel("id126", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 127: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id127", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id127", "iku", false, undefined, undefined);

      const promise = await store.cancel("id127", "iku", false, undefined, undefined);

      expect(promise.state).toBe("RESOLVED");
      expect(promise.id).toBe("id127");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 128: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id128", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id128", "iku", false, undefined, undefined);

      await expect(store.cancel("id128", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 129: transitions from Resolved to Resolved via Cancel", async () => {
      await store.create("id129", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.resolve("id129", "iku", false, undefined, undefined);

      await expect(store.cancel("id129", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 130: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id130", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id130", undefined, false, undefined, undefined);

      await expect(
        store.create("id130", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 131: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id131", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id131", undefined, false, undefined, undefined);

      await expect(
        store.create("id131", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 132: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id132", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id132", undefined, false, undefined, undefined);

      await expect(
        store.create("id132", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 133: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id133", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id133", undefined, false, undefined, undefined);

      await expect(
        store.create("id133", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 134: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id134", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id134", undefined, false, undefined, undefined);

      await expect(store.resolve("id134", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 135: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id135", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id135", undefined, false, undefined, undefined);

      await expect(store.resolve("id135", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 136: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id136", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id136", undefined, false, undefined, undefined);

      await expect(store.resolve("id136", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 137: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id137", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id137", undefined, false, undefined, undefined);

      await expect(store.resolve("id137", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 138: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id138", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id138", undefined, false, undefined, undefined);

      await expect(store.reject("id138", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 139: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id139", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id139", undefined, false, undefined, undefined);

      await expect(store.reject("id139", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 140: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id140", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id140", undefined, false, undefined, undefined);

      await expect(store.reject("id140", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 141: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id141", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id141", undefined, false, undefined, undefined);

      await expect(store.reject("id141", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 142: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id142", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id142", undefined, false, undefined, undefined);

      await expect(store.cancel("id142", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 143: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id143", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id143", undefined, false, undefined, undefined);

      await expect(store.cancel("id143", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 144: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id144", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id144", undefined, false, undefined, undefined);

      await expect(store.cancel("id144", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 145: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id145", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id145", undefined, false, undefined, undefined);

      await expect(store.cancel("id145", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 146: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id146", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id146", "iku", false, undefined, undefined);

      await expect(
        store.create("id146", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 147: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id147", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id147", "iku", false, undefined, undefined);

      await expect(
        store.create("id147", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 148: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id148", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id148", "iku", false, undefined, undefined);

      await expect(
        store.create("id148", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 149: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id149", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id149", "iku", false, undefined, undefined);

      await expect(
        store.create("id149", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 150: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id150", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id150", "iku", false, undefined, undefined);

      await expect(store.resolve("id150", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 151: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id151", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id151", "iku", false, undefined, undefined);

      await expect(store.resolve("id151", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 152: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id152", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id152", "iku", false, undefined, undefined);

      await expect(store.resolve("id152", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 153: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id153", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id153", "iku", false, undefined, undefined);

      const promise = await store.resolve("id153", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id153");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 154: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id154", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id154", "iku", false, undefined, undefined);

      await expect(store.resolve("id154", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 155: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id155", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id155", "iku", false, undefined, undefined);

      await expect(store.resolve("id155", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 156: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id156", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id156", "iku", false, undefined, undefined);

      await expect(store.reject("id156", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 157: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id157", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id157", "iku", false, undefined, undefined);

      await expect(store.reject("id157", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 158: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id158", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id158", "iku", false, undefined, undefined);

      const promise = await store.reject("id158", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id158");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 159: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id159", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id159", "iku", false, undefined, undefined);

      const promise = await store.reject("id159", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id159");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 160: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id160", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id160", "iku", false, undefined, undefined);

      await expect(store.reject("id160", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 161: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id161", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id161", "iku", false, undefined, undefined);

      await expect(store.reject("id161", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 162: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id162", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id162", "iku", false, undefined, undefined);

      await expect(store.cancel("id162", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 163: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id163", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id163", "iku", false, undefined, undefined);

      await expect(store.cancel("id163", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 164: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id164", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id164", "iku", false, undefined, undefined);

      await expect(store.cancel("id164", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 165: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id165", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id165", "iku", false, undefined, undefined);

      const promise = await store.cancel("id165", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id165");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 166: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id166", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id166", "iku", false, undefined, undefined);

      await expect(store.cancel("id166", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 167: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id167", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id167", "iku", false, undefined, undefined);

      await expect(store.cancel("id167", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 168: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id168", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id168", undefined, false, undefined, undefined);

      await expect(
        store.create("id168", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 169: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id169", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id169", undefined, false, undefined, undefined);

      await expect(
        store.create("id169", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 170: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id170", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id170", undefined, false, undefined, undefined);

      await expect(
        store.create("id170", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 171: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id171", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id171", undefined, false, undefined, undefined);

      const promise = await store.create(
        "id171",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id171");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 172: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id172", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id172", undefined, false, undefined, undefined);

      await expect(
        store.create("id172", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 173: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id173", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id173", undefined, false, undefined, undefined);

      await expect(
        store.create("id173", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 174: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id174", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id174", undefined, false, undefined, undefined);

      await expect(store.resolve("id174", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 175: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id175", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id175", undefined, false, undefined, undefined);

      await expect(store.resolve("id175", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 176: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id176", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id176", undefined, false, undefined, undefined);

      await expect(store.resolve("id176", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 177: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id177", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id177", undefined, false, undefined, undefined);

      await expect(store.resolve("id177", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 178: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id178", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id178", undefined, false, undefined, undefined);

      await expect(store.reject("id178", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 179: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id179", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id179", undefined, false, undefined, undefined);

      await expect(store.reject("id179", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 180: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id180", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id180", undefined, false, undefined, undefined);

      await expect(store.reject("id180", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 181: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id181", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id181", undefined, false, undefined, undefined);

      await expect(store.reject("id181", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 182: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id182", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id182", undefined, false, undefined, undefined);

      await expect(store.cancel("id182", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 183: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id183", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id183", undefined, false, undefined, undefined);

      await expect(store.cancel("id183", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 184: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id184", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id184", undefined, false, undefined, undefined);

      await expect(store.cancel("id184", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 185: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id185", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id185", undefined, false, undefined, undefined);

      await expect(store.cancel("id185", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 186: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id186", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id186", "iku", false, undefined, undefined);

      await expect(
        store.create("id186", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 187: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id187", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id187", "iku", false, undefined, undefined);

      await expect(
        store.create("id187", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 188: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id188", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id188", "iku", false, undefined, undefined);

      await expect(
        store.create("id188", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 189: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id189", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id189", "iku", false, undefined, undefined);

      const promise = await store.create(
        "id189",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id189");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 190: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id190", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id190", "iku", false, undefined, undefined);

      await expect(
        store.create("id190", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 191: transitions from Rejected to Rejected via Create", async () => {
      await store.create("id191", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id191", "iku", false, undefined, undefined);

      await expect(
        store.create("id191", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 192: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id192", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id192", "iku", false, undefined, undefined);

      await expect(store.resolve("id192", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 193: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id193", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id193", "iku", false, undefined, undefined);

      await expect(store.resolve("id193", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 194: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id194", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id194", "iku", false, undefined, undefined);

      await expect(store.resolve("id194", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 195: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id195", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id195", "iku", false, undefined, undefined);

      const promise = await store.resolve("id195", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id195");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 196: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id196", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id196", "iku", false, undefined, undefined);

      await expect(store.resolve("id196", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 197: transitions from Rejected to Rejected via Resolve", async () => {
      await store.create("id197", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id197", "iku", false, undefined, undefined);

      await expect(store.resolve("id197", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 198: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id198", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id198", "iku", false, undefined, undefined);

      await expect(store.reject("id198", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 199: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id199", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id199", "iku", false, undefined, undefined);

      await expect(store.reject("id199", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 200: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id200", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id200", "iku", false, undefined, undefined);

      const promise = await store.reject("id200", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id200");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 201: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id201", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id201", "iku", false, undefined, undefined);

      const promise = await store.reject("id201", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id201");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 202: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id202", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id202", "iku", false, undefined, undefined);

      await expect(store.reject("id202", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 203: transitions from Rejected to Rejected via Reject", async () => {
      await store.create("id203", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id203", "iku", false, undefined, undefined);

      await expect(store.reject("id203", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 204: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id204", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id204", "iku", false, undefined, undefined);

      await expect(store.cancel("id204", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 205: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id205", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id205", "iku", false, undefined, undefined);

      await expect(store.cancel("id205", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 206: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id206", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id206", "iku", false, undefined, undefined);

      await expect(store.cancel("id206", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 207: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id207", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id207", "iku", false, undefined, undefined);

      const promise = await store.cancel("id207", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED");
      expect(promise.id).toBe("id207");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 208: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id208", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id208", "iku", false, undefined, undefined);

      await expect(store.cancel("id208", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 209: transitions from Rejected to Rejected via Cancel", async () => {
      await store.create("id209", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.reject("id209", "iku", false, undefined, undefined);

      await expect(store.cancel("id209", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 210: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id210", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id210", undefined, false, undefined, undefined);

      await expect(
        store.create("id210", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 211: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id211", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id211", undefined, false, undefined, undefined);

      await expect(
        store.create("id211", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 212: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id212", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id212", undefined, false, undefined, undefined);

      await expect(
        store.create("id212", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 213: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id213", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id213", undefined, false, undefined, undefined);

      await expect(
        store.create("id213", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 214: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id214", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id214", undefined, false, undefined, undefined);

      await expect(store.resolve("id214", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 215: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id215", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id215", undefined, false, undefined, undefined);

      await expect(store.resolve("id215", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 216: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id216", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id216", undefined, false, undefined, undefined);

      await expect(store.resolve("id216", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 217: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id217", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id217", undefined, false, undefined, undefined);

      await expect(store.resolve("id217", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 218: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id218", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id218", undefined, false, undefined, undefined);

      await expect(store.reject("id218", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 219: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id219", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id219", undefined, false, undefined, undefined);

      await expect(store.reject("id219", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 220: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id220", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id220", undefined, false, undefined, undefined);

      await expect(store.reject("id220", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 221: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id221", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id221", undefined, false, undefined, undefined);

      await expect(store.reject("id221", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 222: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id222", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id222", undefined, false, undefined, undefined);

      await expect(store.cancel("id222", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 223: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id223", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id223", undefined, false, undefined, undefined);

      await expect(store.cancel("id223", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 224: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id224", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id224", undefined, false, undefined, undefined);

      await expect(store.cancel("id224", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 225: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id225", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id225", undefined, false, undefined, undefined);

      await expect(store.cancel("id225", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 226: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id226", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id226", "iku", false, undefined, undefined);

      await expect(
        store.create("id226", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 227: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id227", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id227", "iku", false, undefined, undefined);

      await expect(
        store.create("id227", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 228: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id228", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id228", "iku", false, undefined, undefined);

      await expect(
        store.create("id228", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 229: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id229", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id229", "iku", false, undefined, undefined);

      await expect(
        store.create("id229", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 230: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id230", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id230", "iku", false, undefined, undefined);

      await expect(store.resolve("id230", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 231: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id231", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id231", "iku", false, undefined, undefined);

      await expect(store.resolve("id231", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 232: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id232", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id232", "iku", false, undefined, undefined);

      await expect(store.resolve("id232", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 233: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id233", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id233", "iku", false, undefined, undefined);

      const promise = await store.resolve("id233", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id233");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 234: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id234", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id234", "iku", false, undefined, undefined);

      await expect(store.resolve("id234", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 235: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id235", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id235", "iku", false, undefined, undefined);

      await expect(store.resolve("id235", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 236: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id236", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id236", "iku", false, undefined, undefined);

      await expect(store.reject("id236", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 237: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id237", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id237", "iku", false, undefined, undefined);

      await expect(store.reject("id237", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 238: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id238", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id238", "iku", false, undefined, undefined);

      await expect(store.reject("id238", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 239: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id239", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id239", "iku", false, undefined, undefined);

      const promise = await store.reject("id239", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id239");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 240: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id240", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id240", "iku", false, undefined, undefined);

      await expect(store.reject("id240", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 241: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id241", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id241", "iku", false, undefined, undefined);

      await expect(store.reject("id241", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 242: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id242", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id242", "iku", false, undefined, undefined);

      await expect(store.cancel("id242", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 243: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id243", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id243", "iku", false, undefined, undefined);

      await expect(store.cancel("id243", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 244: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id244", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id244", "iku", false, undefined, undefined);

      const promise = await store.cancel("id244", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id244");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 245: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id245", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id245", "iku", false, undefined, undefined);

      const promise = await store.cancel("id245", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id245");
      expect(promise.idempotencyKeyForCreate).toBe(undefined);
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 246: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id246", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id246", "iku", false, undefined, undefined);

      await expect(store.cancel("id246", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 247: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id247", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id247", "iku", false, undefined, undefined);

      await expect(store.cancel("id247", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 248: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id248", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id248", undefined, false, undefined, undefined);

      await expect(
        store.create("id248", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 249: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id249", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id249", undefined, false, undefined, undefined);

      await expect(
        store.create("id249", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 250: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id250", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id250", undefined, false, undefined, undefined);

      await expect(
        store.create("id250", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 251: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id251", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id251", undefined, false, undefined, undefined);

      const promise = await store.create(
        "id251",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id251");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe(undefined);
    });

    test("Test Case 252: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id252", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id252", undefined, false, undefined, undefined);

      await expect(
        store.create("id252", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 253: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id253", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id253", undefined, false, undefined, undefined);

      await expect(
        store.create("id253", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 254: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id254", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id254", undefined, false, undefined, undefined);

      await expect(store.resolve("id254", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 255: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id255", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id255", undefined, false, undefined, undefined);

      await expect(store.resolve("id255", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 256: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id256", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id256", undefined, false, undefined, undefined);

      await expect(store.resolve("id256", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 257: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id257", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id257", undefined, false, undefined, undefined);

      await expect(store.resolve("id257", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 258: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id258", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id258", undefined, false, undefined, undefined);

      await expect(store.reject("id258", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 259: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id259", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id259", undefined, false, undefined, undefined);

      await expect(store.reject("id259", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 260: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id260", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id260", undefined, false, undefined, undefined);

      await expect(store.reject("id260", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 261: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id261", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id261", undefined, false, undefined, undefined);

      await expect(store.reject("id261", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 262: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id262", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id262", undefined, false, undefined, undefined);

      await expect(store.cancel("id262", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 263: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id263", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id263", undefined, false, undefined, undefined);

      await expect(store.cancel("id263", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 264: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id264", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id264", undefined, false, undefined, undefined);

      await expect(store.cancel("id264", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 265: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id265", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id265", undefined, false, undefined, undefined);

      await expect(store.cancel("id265", "iku", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 266: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id266", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id266", "iku", false, undefined, undefined);

      await expect(
        store.create("id266", undefined, true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 267: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id267", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id267", "iku", false, undefined, undefined);

      await expect(
        store.create("id267", undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 268: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id268", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id268", "iku", false, undefined, undefined);

      await expect(
        store.create("id268", "ikc", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 269: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id269", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id269", "iku", false, undefined, undefined);

      const promise = await store.create(
        "id269",
        "ikc",
        false,
        undefined,
        undefined,
        Number.MAX_SAFE_INTEGER,
        undefined,
      );

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id269");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 270: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id270", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id270", "iku", false, undefined, undefined);

      await expect(
        store.create("id270", "ikc*", true, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 271: transitions from Canceled to Canceled via Create", async () => {
      await store.create("id271", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id271", "iku", false, undefined, undefined);

      await expect(
        store.create("id271", "ikc*", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined),
      ).rejects.toThrow();
    });

    test("Test Case 272: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id272", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id272", "iku", false, undefined, undefined);

      await expect(store.resolve("id272", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 273: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id273", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id273", "iku", false, undefined, undefined);

      await expect(store.resolve("id273", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 274: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id274", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id274", "iku", false, undefined, undefined);

      await expect(store.resolve("id274", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 275: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id275", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id275", "iku", false, undefined, undefined);

      const promise = await store.resolve("id275", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id275");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 276: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id276", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id276", "iku", false, undefined, undefined);

      await expect(store.resolve("id276", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 277: transitions from Canceled to Canceled via Resolve", async () => {
      await store.create("id277", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id277", "iku", false, undefined, undefined);

      await expect(store.resolve("id277", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 278: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id278", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id278", "iku", false, undefined, undefined);

      await expect(store.reject("id278", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 279: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id279", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id279", "iku", false, undefined, undefined);

      await expect(store.reject("id279", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 280: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id280", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id280", "iku", false, undefined, undefined);

      await expect(store.reject("id280", "iku", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 281: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id281", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id281", "iku", false, undefined, undefined);

      const promise = await store.reject("id281", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id281");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 282: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id282", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id282", "iku", false, undefined, undefined);

      await expect(store.reject("id282", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 283: transitions from Canceled to Canceled via Reject", async () => {
      await store.create("id283", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id283", "iku", false, undefined, undefined);

      await expect(store.reject("id283", "iku*", false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 284: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id284", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id284", "iku", false, undefined, undefined);

      await expect(store.cancel("id284", undefined, true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 285: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id285", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id285", "iku", false, undefined, undefined);

      await expect(store.cancel("id285", undefined, false, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 286: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id286", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id286", "iku", false, undefined, undefined);

      const promise = await store.cancel("id286", "iku", true, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id286");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 287: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id287", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id287", "iku", false, undefined, undefined);

      const promise = await store.cancel("id287", "iku", false, undefined, undefined);

      expect(promise.state).toBe("REJECTED_CANCELED");
      expect(promise.id).toBe("id287");
      expect(promise.idempotencyKeyForCreate).toBe("ikc");
      expect(promise.idempotencyKeyForComplete).toBe("iku");
    });

    test("Test Case 288: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id288", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id288", "iku", false, undefined, undefined);

      await expect(store.cancel("id288", "iku*", true, undefined, undefined)).rejects.toThrow();
    });

    test("Test Case 289: transitions from Canceled to Canceled via Cancel", async () => {
      await store.create("id289", "ikc", false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);
      await store.cancel("id289", "iku", false, undefined, undefined);

      await expect(store.cancel("id289", "iku*", false, undefined, undefined)).rejects.toThrow();
    });
  });
});
