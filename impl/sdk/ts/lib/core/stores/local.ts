import * as cronParser from "cron-parser";
import {
  DurablePromise,
  PendingPromise,
  ResolvedPromise,
  RejectedPromise,
  CanceledPromise,
  TimedoutPromise,
  isPendingPromise,
  isResolvedPromise,
  isRejectedPromise,
  isCanceledPromise,
} from "../promise";
import { Schedule } from "../schedule";
import { IPromiseStore, IScheduleStore } from "../store";
import { ErrorCodes, ResonateError } from "../error";
import { IPromiseStorage, IScheduleStorage } from "../storage";
import { WithTimeout } from "../storages/withTimeout";
import { MemoryPromiseStorage, MemoryScheduleStorage } from "../storages/memory";

export class LocalPromiseStore implements IPromiseStore {
  constructor(private storage: IPromiseStorage = new WithTimeout(new MemoryPromiseStorage())) {}

  async create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<PendingPromise | ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (promise.idempotencyKeyForCreate === undefined || ikey != promise.idempotencyKeyForCreate) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        return {
          state: "PENDING",
          id: id,
          timeout: timeout,
          param: {
            headers: headers,
            data: data,
          },
          value: {
            headers: undefined,
            data: undefined,
          },
          createdOn: Date.now(),
          completedOn: undefined,
          idempotencyKeyForCreate: ikey,
          idempotencyKeyForComplete: undefined,
          tags: tags,
        };
      }
    });
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise) && !isResolvedPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (isPendingPromise(promise)) {
          return {
            state: "RESOLVED",
            id: promise.id,
            timeout: promise.timeout,
            param: promise.param,
            value: {
              headers: headers,
              data: data,
            },
            createdOn: promise.createdOn,
            completedOn: Date.now(),
            idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
            idempotencyKeyForComplete: ikey,
            tags: promise.tags,
          };
        }

        if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise) && !isRejectedPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (isPendingPromise(promise)) {
          return {
            state: "REJECTED",
            id: promise.id,
            timeout: promise.timeout,
            param: promise.param,
            value: {
              headers: headers,
              data: data,
            },
            createdOn: promise.createdOn,
            completedOn: Date.now(),
            idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
            idempotencyKeyForComplete: ikey,
            tags: promise.tags,
          };
        }

        if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    return this.storage.rmw(id, (promise) => {
      if (promise) {
        if (strict && !isPendingPromise(promise) && !isCanceledPromise(promise)) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        if (isPendingPromise(promise)) {
          return {
            state: "REJECTED_CANCELED",
            id: promise.id,
            timeout: promise.timeout,
            param: promise.param,
            value: {
              headers: headers,
              data: data,
            },
            createdOn: promise.createdOn,
            completedOn: Date.now(),
            idempotencyKeyForCreate: promise.idempotencyKeyForCreate,
            idempotencyKeyForComplete: ikey,
            tags: promise.tags,
          };
        }

        if (promise.idempotencyKeyForComplete === undefined || ikey != promise.idempotencyKeyForComplete) {
          throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request");
        }

        return promise;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  async get(id: string): Promise<DurablePromise> {
    const promise = await this.storage.rmw(id, (p) => p);

    if (promise) {
      return promise;
    }

    throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
  }

  search(
    id: string,
    state?: string,
    tags?: Record<string, string>,
    limit?: number,
  ): AsyncGenerator<DurablePromise[], void> {
    return this.storage.search(id, state, tags, limit);
  }
}

export class LocalScheduleStore implements IScheduleStore {
  private storage: IScheduleStorage;

  constructor(storage: IScheduleStorage = new MemoryScheduleStorage()) {
    this.storage = storage;
  }

  async create(
    id: string,
    ikey: string | undefined,
    description: string | undefined,
    cron: string,
    tags: Record<string, string> | undefined,
    promiseId: string,
    promiseTimeout: number,
    promiseHeaders: Record<string, string>,
    promiseData: string | undefined,
    promiseTags: Record<string, string> | undefined,
  ): Promise<Schedule> {
    return this.storage.rmw(id, (s) => {
      if (s) {
        if (s.idempotencyKey === undefined || ikey != s.idempotencyKey) {
          throw new ResonateError(ErrorCodes.ALREADY_EXISTS, "Already exists");
        }
        return s;
      }
      let validateCron: Date;
      try {
        validateCron = this.parseCronExpression(cron);
      } catch (error) {
        throw new ResonateError(ErrorCodes.UNKNOWN, "Bad request");
      }

      const schedule = {
        id,
        description,
        cron,
        tags,
        promiseId,
        promiseTimeout,
        promiseParam: {
          headers: promiseHeaders,
          data: promiseData,
        },
        promiseTags,
        lastRunTime: undefined,
        nextRunTime: validateCron.getTime(),
        idempotencyKey: ikey,
        createdOn: Date.now(),
      };

      return schedule;
    });
  }

  async delete(id: string): Promise<boolean> {
    return this.storage.delete(id);
  }

  async get(id: string): Promise<Schedule> {
    return await this.storage.rmw<Schedule>(id, (s) => {
      if (s) {
        return s;
      } else {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }
    });
  }

  search(id: string, tags?: Record<string, string>, limit?: number): AsyncGenerator<Schedule[], void> {
    return this.storage.search(id, tags, limit);
  }

  private parseCronExpression(cron: string): Date {
    try {
      return cronParser.parseExpression(cron).next().toDate();
    } catch (error) {
      throw new Error(`Error parsing cron expression: ${error}`);
    }
  }
}

export class LocalStore {
  private queuedSchedules: Schedule[] = [];
  constructor(
    public promises: LocalPromiseStore = new LocalPromiseStore(),
    public schedules: LocalScheduleStore = new LocalScheduleStore(),
  ) {
    this.startControlLoop();
  }

  private startControlLoop() {
    // TODO: make this loop non-busy
    setInterval(() => {
      this.controlLoopSchedules();
    }, 1000);
  }

  private async controlLoopSchedules() {
    for await (const schedules of this.schedules.search("*", undefined, undefined)) {
      this.queuedSchedules = this.queuedSchedules.concat(schedules);
    }

    this.queuedSchedules.sort((a, b) => (a.nextRunTime ?? 0) - (b.nextRunTime ?? 0));

    const schedule = this.queuedSchedules.shift();
    if (schedule) {
      const delay = Math.max(0, schedule ? -Date.now() : 0);

      setTimeout(async () => {
        try {
          await this.createPromiseFromSchedule(schedule);
        } catch (error) {
          console.error("Error creating promise:", error);
        }
      }, delay);
    }
  }

  private async createPromiseFromSchedule(schedule: Schedule): Promise<DurablePromise | undefined> {
    const expandedPromiseId = this.expandPromiseIdTemplate(schedule.promiseId, schedule);

    return this.promises.create(
      expandedPromiseId,
      expandedPromiseId,
      false,
      schedule.promiseParam?.headers,
      schedule.promiseParam?.data,
      schedule.promiseTimeout,
      schedule.promiseTags,
    );
  }

  private expandPromiseIdTemplate(template: string, schedule: Schedule): string {
    const expandedTemplate = template.replace("{{id}}", schedule.id).replace("{{timestamp}}", Date.now().toString());

    return expandedTemplate;
  }
}
