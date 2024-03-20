import { jest, describe, test, expect } from "@jest/globals";

import { DurablePromise } from "../lib/core/promises/types";
import { MemoryStorage } from "../lib/core/storages/memory";
import { WithTimeout } from "../lib/core/storages/withTimeout";
import { LocalPromiseStore } from "../lib/core/stores/local";
import { RemotePromiseStore } from "../lib/core/stores/remote";

jest.setTimeout(10000);

describe("Store: Promise", () => {
  const useDurable = process.env.USE_DURABLE === "true";
  const url = process.env.RESONATE_URL || "http://localhost:8001";

  const storage = new WithTimeout(new MemoryStorage<DurablePromise>());
  const store = useDurable ? new RemotePromiseStore(url) : new LocalPromiseStore(storage);

  test("Promise Store: Get promise that exists", async () => {
    const promiseId = "existing-promise";
    const createdPromise = await store.create(promiseId, undefined, false, {}, "{}", 60000, {});

    const retrievedPromise = await store.get(promiseId);

    expect(retrievedPromise.id).toBe(createdPromise.id);
  });

  test("Promise Store: Get promise that does not exist", async () => {
    const nonExistingPromiseId = "non-existing-promise";

    await expect(store.get(nonExistingPromiseId)).rejects.toThrowError("Not found");
  });

  test("Promise Store: Search by id", async () => {
    const promiseId = "search-by-id-promise";
    await store.create(promiseId, undefined, false, {}, "{}", 60000, {});

    const promises = [];
    for await (const searchResults of store.search(promiseId, undefined, undefined, undefined)) {
      promises.push(...searchResults);
    }

    expect(promises.length).toBe(1);
    expect(promises[0].id).toBe(promiseId);
  });

  test("Promise Store: Search by id with wildcard(s)", async () => {
    const promiseIdPrefix = "search-by-id-prefix";
    const wildcardSearch = `${promiseIdPrefix}*`;

    for (let i = 1; i <= 3; i++) {
      await store.create(`${promiseIdPrefix}-${i}`, undefined, false, {}, "{}", 60000, {});
    }

    const promises = [];
    for await (const searchResults of store.search(wildcardSearch, undefined, undefined, undefined)) {
      promises.push(...searchResults);
    }

    expect(promises.length).toBe(3);
  });

  test("Promise Store: Search by state", async () => {
    const promiseId = "search-by-state-promise";
    await store.create(promiseId, undefined, false, {}, "{}", 10, {});

    const promises = [];
    for await (const searchResults of store.search(promiseId, "rejected", undefined, undefined)) {
      promises.push(...searchResults);
    }

    expect(promises.length).toBe(1);
    expect(promises[0].id).toBe(promiseId);
    expect(promises[0].state).toBe("REJECTED_TIMEDOUT");
  });

  test("Promise Store: Search by tags", async () => {
    const promiseId = "search-by-tags-promise";
    const tags = { category: "search testing", priority: "high" };
    await store.create(promiseId, undefined, false, {}, "{}", 60000, tags);

    const promises = [];
    for await (const searchResults of store.search(promiseId, undefined, tags, undefined)) {
      promises.push(...searchResults);
    }

    expect(promises.length).toBe(1);
    expect(promises[0].id).toBe(promiseId);
    expect(promises[0].tags).toEqual(tags);
  });
});
