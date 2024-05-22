import { describe, test, expect, jest } from "@jest/globals";
import { Schedule } from "../lib/core/schedules/types";
import { IStore } from "../lib/core/store";
import { LocalStore } from "../lib/core/stores/local";
import { RemoteStore } from "../lib/core/stores/remote";

jest.setTimeout(10000);

describe("Store: Schedules", () => {
  const stores: IStore[] = [new LocalStore()];

  if (process.env.RESONATE_STORE_URL) {
    stores.push(new RemoteStore(process.env.RESONATE_STORE_URL));
  }

  for (const store of stores.map((s) => s.schedules)) {
    describe(store.constructor.name, () => {
      test("Schedule Store: Create schedule", async () => {
        const scheduleId = "new-schedule";
        const cronExpression = "* * * * *"; // Every minute

        const createdSchedule = await store.create(
          scheduleId,
          scheduleId,
          undefined,
          cronExpression,
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        expect(createdSchedule.id).toBe(scheduleId);
        expect(createdSchedule.cron).toBe(cronExpression);

        // Clean up
        await store.delete(scheduleId);
      });

      // this test needs to be discussed
      test("Schedule Store: Create schedule that exists with same idempotency key", async () => {
        const scheduleId = "existing-schedule";
        const cronExpression = "* * * * *"; // Every minute

        // Create the initial schedule
        await store.create(
          scheduleId,
          scheduleId,
          undefined,
          cronExpression,
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        const schedule = await store.create(
          scheduleId,
          scheduleId,
          undefined,
          cronExpression,
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        expect(schedule.id).toBe(scheduleId);
        expect(schedule.cron).toBe(cronExpression);
        expect(schedule.idempotencyKey).toBe(scheduleId);

        // Clean up
        await store.delete(scheduleId);
      });

      test("Schedule Store: Create schedule that exists with different idempotency key", async () => {
        const scheduleId = "existing-schedule";
        const cronExpression = "* * * * *"; // Every minute

        // Create the initial schedule
        await store.create(
          scheduleId,
          scheduleId,
          undefined,
          cronExpression,
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        // Attempt to create a schedule with a different idempotency key, should throw error
        await expect(
          store.create(
            scheduleId,
            "new-idempotency-key",
            undefined,
            cronExpression,
            undefined,
            "promise-1",
            1,
            undefined,
            undefined,
            undefined,
          ),
        ).rejects.toThrowError("Already exists");

        // Clean up
        await store.delete(scheduleId);
      });

      test("Schedule Store: Get schedule that exists", async () => {
        const scheduleId = "existing-schedule";
        const cronExpression = "* * * * *"; // Every minute

        // Create the schedule
        await store.create(
          scheduleId,
          scheduleId,
          undefined,
          cronExpression,
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        // Get the existing schedule
        const existingSchedule = await store.get(scheduleId);
        expect(existingSchedule.id).toBe(scheduleId);

        // Clean up
        await store.delete(scheduleId);
      });

      test("Schedule Store: Get schedule that does not exist", async () => {
        const nonExistingScheduleId = "non-existing-schedule-id";

        // Attempt to get a schedule that does not exist, should throw NOT_FOUND error
        await expect(store.get(nonExistingScheduleId)).rejects.toThrowError("Not found");
      });

      test("Schedule Store: Delete schedule that exists", async () => {
        const scheduleId = "schedule-to-delete";

        // Create the schedule first
        await store.create(
          scheduleId,
          scheduleId,
          undefined,
          "* * * * *",
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        // Attempt to delete the schedule
        const isDeleted = await store.delete(scheduleId);
        expect(isDeleted).toBeUndefined();

        // Attempt to get the deleted schedule, should throw NOT_FOUND error
        await expect(store.get(scheduleId)).rejects.toThrowError("Not found");
      });

      test("Schedule Store: Delete schedule that does not exist", async () => {
        const nonExistingScheduleId = "non-existing-schedule-id";

        // Attempt to delete a schedule that does not exist, should throw NOT_FOUND error
        await expect(store.delete(nonExistingScheduleId)).rejects.toThrowError("Not found");
      });

      test("Schedule Store: Search by id", async () => {
        const scheduleId = "search-by-id-schedule";

        // Create the schedule
        await store.create(
          scheduleId,
          scheduleId,
          undefined,
          "* * * * *",
          undefined,
          "promise-1",
          1,
          undefined,
          undefined,
          undefined,
        );

        // Search for the schedule by id
        let schedules: Schedule[] = [];
        for await (const searchResults of store.search(scheduleId, undefined, undefined)) {
          schedules = schedules.concat(searchResults);
        }
        expect(schedules.length).toBe(1);
        expect(schedules[0].id).toBe(scheduleId);

        // Clean up
        await store.delete(scheduleId);
      });

      test("Schedule Store: Search by id with wildcard(s)", async () => {
        const scheduleIdPrefix = "should-match";
        const wildcardSearch = `${scheduleIdPrefix}*`;

        const scheduleIds = [
          "should-match-1",
          "should-match-2",
          "should-match-3",
          "should-not-match-1",
          "should-not-match-2",
          "should-not-match-3",
        ];

        for (const i in scheduleIds) {
          await store.create(
            scheduleIds[i],
            scheduleIds[i],
            "Search by ID Prefix Schedule",
            "* * * * *",
            undefined,
            `promise-${scheduleIds[i]}`,
            1,
            undefined,
            undefined,
            undefined,
          );
        }

        // Search for schedules by id prefix with wildcard
        let schedules: Schedule[] = [];
        for await (const searchResults of store.search(wildcardSearch, undefined, undefined)) {
          schedules = schedules.concat(searchResults);
        }
        expect(schedules.length).toBe(3);
        // assert ids
        expect(schedules.map((s) => s.id)).toEqual(expect.arrayContaining(scheduleIds.slice(0, 3)));

        // Clean up
        for (const i in scheduleIds) {
          await store.delete(scheduleIds[i]);
        }
      });

      test("Schedule Store: Search by tags", async () => {
        const scheduleIds = ["search-by-tags-schedule", "should-not-match-1", "should-not-match-2"];

        const scheduleId = "search-by-tags-schedule";
        const searchtag = { category: "search testing", priority: "high" };

        // Create the schedule with specific tags
        for (const i in scheduleIds) {
          let tags = { category: "search don't match", priority: "med" };
          if (scheduleIds[i] === scheduleId) {
            tags = searchtag;
          }
          await store.create(
            scheduleIds[i],
            scheduleIds[i],
            undefined,
            "* * * * *",
            tags,
            `promise-${scheduleIds[i]}`,
            1,
            undefined,
            undefined,
            undefined,
          );
        }

        // Search for the schedule by tags
        let schedules: Schedule[] = [];
        for await (const searchResults of store.search(scheduleId, searchtag, undefined)) {
          schedules = schedules.concat(searchResults);
        }
        expect(schedules.length).toBe(1);

        // Clean up
        for (const i in scheduleIds) {
          await store.delete(scheduleIds[i]);
        }
      });
    });
  }
});
