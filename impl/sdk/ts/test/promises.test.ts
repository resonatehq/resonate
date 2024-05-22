import { jest, describe, test, expect } from "@jest/globals";

import { IStore } from "../lib/core/store";
import { LocalStore } from "../lib/core/stores/local";
import { RemoteStore } from "../lib/core/stores/remote";

jest.setTimeout(10000);

describe("Store: Promise", () => {
  const stores: IStore[] = [new LocalStore()];

  if (process.env.RESONATE_STORE_URL) {
    stores.push(new RemoteStore(process.env.RESONATE_STORE_URL));
  }

  for (const store of stores.map((s) => s.promises)) {
    describe(store.constructor.name, () => {
      test("Promise Store: Get promise that exists", async () => {
        const promiseId = "existing-promise";
        const createdPromise = await store.create(
          promiseId,
          undefined,
          false,
          undefined,
          undefined,
          Number.MAX_SAFE_INTEGER,
          undefined,
        );

        const retrievedPromise = await store.get(promiseId);

        expect(retrievedPromise.id).toBe(createdPromise.id);
      });

      test("Promise Store: Get promise that does not exist", async () => {
        const nonExistingPromiseId = "non-existing-promise";

        await expect(store.get(nonExistingPromiseId)).rejects.toThrowError("Not found");
      });

      test("Promise Store: Search by id", async () => {
        const promiseId = "search-by-id-promise";
        await store.create(promiseId, undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, undefined);

        const promises = [];
        for await (const results of store.search(promiseId, undefined, undefined, undefined)) {
          promises.push(...results);
        }

        expect(promises.length).toBe(1);
        expect(promises[0].id).toBe(promiseId);
      });

      test("Promise Store: Search by id with wildcard(s)", async () => {
        const promiseIdPrefix = "search-by-id-prefix";
        const wildcardSearch = `${promiseIdPrefix}-*`;

        for (let i = 1; i <= 3; i++) {
          await store.create(
            `${promiseIdPrefix}-${i}`,
            undefined,
            false,
            undefined,
            undefined,
            Number.MAX_SAFE_INTEGER,
            undefined,
          );
        }

        const promises = [];
        for await (const results of store.search(wildcardSearch, undefined, undefined, undefined)) {
          promises.push(...results);
        }

        expect(promises.length).toBe(3);
      });

      test("Promise Store: Search by state", async () => {
        const promiseIdPrefix = "search-by-state";
        await store.create(
          `${promiseIdPrefix}-pending`,
          undefined,
          true,
          undefined,
          undefined,
          Number.MAX_SAFE_INTEGER,
          undefined,
        );
        await store.create(
          `${promiseIdPrefix}-resolved`,
          undefined,
          true,
          undefined,
          undefined,
          Number.MAX_SAFE_INTEGER,
          undefined,
        );
        await store.create(
          `${promiseIdPrefix}-rejected`,
          undefined,
          true,
          undefined,
          undefined,
          Number.MAX_SAFE_INTEGER,
          undefined,
        );
        await store.create(
          `${promiseIdPrefix}-canceled`,
          undefined,
          true,
          undefined,
          undefined,
          Number.MAX_SAFE_INTEGER,
          undefined,
        );
        await store.create(`${promiseIdPrefix}-timedout`, undefined, true, undefined, undefined, 0, undefined);

        await store.resolve(`${promiseIdPrefix}-resolved`, undefined, true, undefined, undefined);
        await store.reject(`${promiseIdPrefix}-rejected`, undefined, true, undefined, undefined);
        await store.cancel(`${promiseIdPrefix}-canceled`, undefined, true, undefined, undefined);

        // pending
        const pendingPromises = [];
        for await (const results of store.search(`${promiseIdPrefix}-*`, "pending", undefined)) {
          pendingPromises.push(...results);
        }

        expect(pendingPromises.length).toBe(1);
        expect(pendingPromises[0].id).toBe(`${promiseIdPrefix}-pending`);
        expect(pendingPromises[0].state).toBe("PENDING");

        // resolved
        const resolvedPromises = [];
        for await (const results of store.search(`${promiseIdPrefix}-*`, "resolved", undefined)) {
          resolvedPromises.push(...results);
        }

        expect(resolvedPromises.length).toBe(1);
        expect(resolvedPromises[0].id).toBe(`${promiseIdPrefix}-resolved`);
        expect(resolvedPromises[0].state).toBe("RESOLVED");

        // rejected
        const rejectedPromises = [];
        for await (const results of store.search(`${promiseIdPrefix}-*`, "rejected", undefined)) {
          rejectedPromises.push(...results);
        }

        const ids = rejectedPromises.map((p) => p.id);
        const states = rejectedPromises.map((p) => p.state);

        expect(rejectedPromises.length).toBe(3);
        expect(ids).toContain(`${promiseIdPrefix}-rejected`);
        expect(ids).toContain(`${promiseIdPrefix}-canceled`);
        expect(ids).toContain(`${promiseIdPrefix}-timedout`);
        expect(states).toContain("REJECTED");
        expect(states).toContain("REJECTED_CANCELED");
        expect(states).toContain("REJECTED_TIMEDOUT");
      });

      test("Promise Store: Search by tags", async () => {
        const promiseId = "search-by-tags-promise";
        const tags = { category: "search testing", priority: "high" };
        await store.create(promiseId, undefined, false, undefined, undefined, Number.MAX_SAFE_INTEGER, tags);

        const promises = [];
        for await (const results of store.search(promiseId, undefined, tags, undefined)) {
          promises.push(...results);
        }

        expect(promises.length).toBe(1);
        expect(promises[0].id).toBe(promiseId);
        expect(promises[0].tags).toEqual(tags);
      });
    });
  }
});
