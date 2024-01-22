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
import { IStore, IPromiseStore, IScheduleStore, ILockStore } from "../store";
import { ErrorCodes, ResonateError } from "../error";
import { IPromiseStorage, IScheduleStorage } from "../storage";
import { WithTimeout } from "../storages/withTimeout";
import { MemoryLockStore, MemoryPromiseStorage, MemoryScheduleStorage } from "../storages/memory";
import { ILogger } from "../logger";
import { Logger } from "../loggers/logger";

export class LocalStore implements IStore {
  public promises: LocalPromiseStore;
  public schedules: LocalScheduleStore;
  public locks: ILockStore;

  private toSchedule: Schedule[] = [];
  private next: number | undefined = undefined;

  constructor(
    private logger: ILogger = new Logger(),
    promiseStorage: IPromiseStorage = new WithTimeout(new MemoryPromiseStorage()),
    scheduleStorage: IScheduleStorage = new MemoryScheduleStorage(),
    lockStore: ILockStore = new MemoryLockStore(),
  ) {
    this.promises = new LocalPromiseStore(promiseStorage);
    this.schedules = new LocalScheduleStore(scheduleStorage, this);
    this.locks = lockStore;

    this.init();
  }

  // handler the schedule store can call
  addSchedule(schedule: Schedule) {
    this.toSchedule = this.toSchedule.filter((s) => s.id != schedule.id).concat(schedule);
    this.setSchedule();
  }

  // handler the schedule store can call
  deleteSchedule(id: string) {
    this.toSchedule = this.toSchedule.filter((s) => s.id != id);
    this.setSchedule();
  }

  private async init() {
    for await (const schedules of this.schedules.search("*", undefined, undefined)) {
      this.toSchedule = this.toSchedule.concat(schedules);
    }

    this.setSchedule();
  }

  private setSchedule() {
    // clear timeout
    if (this.next) {
      clearTimeout(this.next);
    }

    // sort array in ascending order by nextRunTime
    this.toSchedule.sort((a, b) => a.nextRunTime - b.nextRunTime);

    if (this.toSchedule.length > 0) {
      // set new timeout to schedule promise
      // + converts to number
      this.next = +setTimeout(() => this.schedulePromise(), this.toSchedule[0].nextRunTime - Date.now());
    }
  }

  private schedulePromise() {
    this.next = undefined;
    const schedule = this.toSchedule.shift();

    if (schedule) {
      const id = this.generatePromiseId(schedule);

      // create promise
      try {
        this.promises.create(
          id,
          id,
          false,
          schedule.promiseParam?.headers,
          schedule.promiseParam?.data,
          schedule.promiseTimeout,
          schedule.promiseTags,
        );
      } catch (error) {
        this.logger.warn("error creating scheduled promise", error);
      }

      // update schedule
      try {
        this.schedules.update(schedule.id, schedule.nextRunTime);
      } catch (error) {
        this.logger.warn("error updating schedule", error);
      }
    }
  }

  private generatePromiseId(schedule: Schedule): string {
    return schedule.promiseId.replace("{{id}}", schedule.id).replace("{{timestamp}}", schedule.nextRunTime.toString());
  }
}

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
  constructor(
    private storage: IScheduleStorage = new MemoryScheduleStorage(),
    private store: LocalStore | undefined = undefined,
  ) {}

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
    const schedule = await this.storage.rmw(id, (schedule) => {
      if (schedule) {
        if (schedule.idempotencyKey === undefined || ikey != schedule.idempotencyKey) {
          throw new ResonateError(ErrorCodes.ALREADY_EXISTS, "Already exists");
        }
        return schedule;
      }

      const createdOn = Date.now();

      let nextRunTime: number;
      try {
        nextRunTime = this.nextRunTime(cron, createdOn);
      } catch (error) {
        throw ResonateError.fromError(error);
      }

      return {
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
        nextRunTime: nextRunTime,
        idempotencyKey: ikey,
        createdOn: createdOn,
      };
    });

    if (this.store) {
      this.store.addSchedule(schedule);
    }

    return schedule;
  }

  async update(id: string, lastRunTime: number): Promise<Schedule | undefined> {
    const schedule = await this.storage.rmw(id, (schedule) => {
      if (!schedule) {
        throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
      }

      // update iff not already updated
      if (schedule.nextRunTime === lastRunTime) {
        let nextRunTime: number;
        try {
          nextRunTime = this.nextRunTime(schedule.cron, lastRunTime);
        } catch (error) {
          throw ResonateError.fromError(error);
        }

        schedule.lastRunTime = lastRunTime;
        schedule.nextRunTime = nextRunTime;
      }

      return schedule;
    });

    if (this.store) {
      this.store.addSchedule(schedule);
    }

    return schedule;
  }

  async delete(id: string): Promise<boolean> {
    const ok = await this.storage.delete(id);

    if (ok && this.store) {
      this.store.deleteSchedule(id);
    }

    return ok;
  }

  async get(id: string): Promise<Schedule> {
    const schedule = await this.storage.rmw(id, (s) => s);

    if (schedule) {
      return schedule;
    }

    throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found");
  }

  search(id: string, tags?: Record<string, string>, limit?: number): AsyncGenerator<Schedule[], void> {
    return this.storage.search(id, tags, limit);
  }

  private nextRunTime(cron: string, lastRunTime: number): number {
    return cronParser
      .parseExpression(cron, { currentDate: new Date(lastRunTime) })
      .next()
      .getTime();
  }
}
