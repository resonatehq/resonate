import { Schedule } from "../lib/core/schedule";
import { LocalStore } from "../lib/core/stores/local";
import { describe, beforeEach, test, expect } from "@jest/globals";

describe("LocalPromiseStore", () => {
  let store: LocalStore;

  beforeEach(() => {
    // Initialize a new LocalPromiseStore with a MemoryStorage for testing
    store = new LocalStore();
  });

  test("creates a schedule", async () => {
    const scheduleId = "schedule-1";
    const cronExpression = "* * * * *"; // Every minute
    const tags = { category: "testing" };
    const promiseId = "promise-1";
    const promiseTimeout = 60000; // 1 minute
    const promiseHeaders = { "Content-Type": "application/json" };
    const promiseData = '{"message": "Test"}';
    const promiseTags = { priority: "high" };

    await store.schedules.create(
      scheduleId,
      scheduleId,
      "Test Schedule",
      cronExpression,
      tags,
      promiseId,
      promiseTimeout,
      promiseHeaders,
      promiseData,
      promiseTags,
    );

    // get the schedule
    const createdSchedule = await store.schedules.get(scheduleId);

    expect(createdSchedule.id).toBe(scheduleId);
    expect(createdSchedule.description).toBe("Test Schedule");
    expect(createdSchedule.cron).toBe(cronExpression);
    expect(createdSchedule.tags).toEqual(tags);
    expect(createdSchedule.promiseId).toBe(promiseId);
    expect(createdSchedule.promiseTimeout).toBe(promiseTimeout);
    expect(createdSchedule.promiseParam?.headers).toEqual(promiseHeaders);
    expect(createdSchedule.promiseParam?.data).toBe(promiseData);
    expect(createdSchedule.promiseTags).toEqual(promiseTags);
  });

  test("deletes a schedule", async () => {
    const scheduleId = "schedule-to-delete";

    // Create a schedule first
    await store.schedules.create(
      scheduleId,
      scheduleId,
      "Test Schedule",
      "* * * * *",
      {},
      "promise-1",
      60000,
      {},
      '{"message": "Test"}',
      {},
    );

    // search for the schedule
    const searchResults = await store.schedules.get(scheduleId);
    expect(searchResults.id).toBe(scheduleId);

    const isDeleted = await store.schedules.delete(scheduleId);

    expect(isDeleted).toBe(true);

    // search for the schedule again
    let schedules: Schedule[] = [];
    for await (const searchResults of store.schedules.search(scheduleId)) {
      schedules = schedules.concat(searchResults);
    }
    expect(schedules.length).toBe(0);
  });

  test("searches for schedules", async () => {
    // Create multiple schedules for testing
    await store.schedules.create(
      "schedule-1",
      "schedule-1",
      "Test Schedule",
      "* * * * *",
      { category: "search testing" },
      "promise-1",
      60000,
      {},
      '{"message": "Test"}',
      {},
    );

    await store.schedules.create(
      "schedule-2",
      "schedule-2",
      "Test Schedule",
      "* * * * *",
      { category: "search testing" },
      "promise-2",
      60000,
      {},
      '{"message": "Test"}',
      {},
    );

    // Expecting 1 schedules based on the search criteria
    let schedules: Schedule[] = [];
    for await (const searchResults of store.schedules.search("schedule-1", { category: "search testing" })) {
      schedules = schedules.concat(searchResults);
    }
    expect(schedules.length).toBe(1);
  });
});
