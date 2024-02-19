import { jest, describe, test, expect } from "@jest/globals";
import { LocalStore } from "../lib/core/stores/local";
import { RemoteStore } from "../lib/core/stores/remote";
import { Logger } from "../lib/core/loggers/logger";

jest.setTimeout(50000);

describe("Store: Locks", () => {
  const useDurable = process.env.USE_DURABLE === "true";
  const url = process.env.RESONATE_URL || "http://localhost:8001";

  const store = useDurable ? new RemoteStore(url, "process-id", new Logger()) : new LocalStore();

  test("Lock Store: Acquire lock that exists", async () => {
    // register a lock
    const resourceId = "existing-resource-id";
    const executionId = "existing-execution-id";

    // Acquire the lock
    const acquireResult = await store.locks.tryAcquire(resourceId, executionId);
    expect(acquireResult).toBe(true);

    // Attempt to acquire the lock again, should succeed
    const secondAcquireResult = await store.locks.tryAcquire(resourceId, executionId);
    expect(secondAcquireResult).toBe(true);

    // Release the lock to clean up
    await store.locks.release(resourceId, executionId);
  });

  test("Lock Store: Acquire lock that exists with different executionId", async () => {
    const resourceId = "existing-resource-id";
    const executionId1 = "existing-execution-id-1";
    const executionId2 = "existing-execution-id-2";

    // Acquire the lock with executionId1
    const acquireResult1 = await store.locks.tryAcquire(resourceId, executionId1);
    expect(acquireResult1).toBe(true);

    // Attempt to acquire the lock with executionId2, should fail
    await expect(store.locks.tryAcquire(resourceId, executionId2)).rejects.toThrow("Forbidden request");

    // Release the lock to clean up
    await store.locks.release(resourceId, executionId1);
  });

  test("Lock Store: Release lock that exists with same executionId", async () => {
    const resourceId = "existing-resource-id";
    const executionId = "existing-execution-id";

    // Acquire the lock
    await store.locks.tryAcquire(resourceId, executionId);

    // Release the lock, should succeed
    await store.locks.release(resourceId, executionId);
  });

  test("Lock Store: Release lock that exists with different executionId", async () => {
    const resourceId = "existing-resource-id";
    const executionId1 = "existing-execution-id-1";
    const executionId2 = "existing-execution-id-2";

    // Acquire the lock with executionId1
    await store.locks.tryAcquire(resourceId, executionId1);

    // Attempt to release the lock with executionId2, should fail
    await expect(store.locks.release(resourceId, executionId2)).rejects.toThrow("Not found");

    // Release the lock to clean up
    await store.locks.release(resourceId, executionId1);
  });

  test("Lock Store: Release lock that does not exist", async () => {
    const nonExistingResourceId = "non-existing-resource-id";
    const nonExistingExecutionId = "non-existing-execution-id";

    // Attempt to release a lock that does not exist, should fail
    await expect(store.locks.release(nonExistingResourceId, nonExistingExecutionId)).rejects.toThrow("Not found");
  });
});
