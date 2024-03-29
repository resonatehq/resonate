import * as cronParser from "cron-parser";
import { ErrorCodes, ResonateError } from "../errors";
import { ILogger } from "../logger";
import { Logger } from "../loggers/logger";
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
  isTimedoutPromise,
} from "../promises/types";
import { Schedule } from "../schedules/types";
import { IStorage } from "../storage";
import { MemoryStorage } from "../storages/memory";
import { WithTimeout } from "../storages/withTimeout";
import { IStore, IPromiseStore, IScheduleStore, ILockStore } from "../store";

export class LocalStore implements IStore {
  public promises: LocalPromiseStore;
  public schedules: LocalScheduleStore;
  public locks: LocalLockStore;

  private toSchedule: Schedule[] = [];
  private next: number | undefined = undefined;

  constructor(
    private logger: ILogger = new Logger(),
    promiseStorage: IStorage<DurablePromise> = new WithTimeout(new MemoryStorage<DurablePromise>()),
    scheduleStorage: IStorage<Schedule> = new MemoryStorage<Schedule>(),
    lockStorage: IStorage<{ id: string; eid: string }> = new MemoryStorage<{ id: string; eid: string }>(),
  ) {
    this.promises = new LocalPromiseStore(promiseStorage);
    this.schedules = new LocalScheduleStore(scheduleStorage, this);
    this.locks = new LocalLockStore(lockStorage);

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
    for await (const schedules of this.schedules.search("*")) {
      this.toSchedule = this.toSchedule.concat(schedules);
    }

    this.setSchedule();
  }

  private setSchedule() {
    // clear timeout
    clearTimeout(this.next);

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
          Date.now() + schedule.promiseTimeout,
          { ...schedule.promiseTags, "resonate:schedule": schedule.id, "resonate:invocation": "true" },
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
    return schedule.promiseId
      .replace("{{.id}}", schedule.id)
      .replace("{{.timestamp}}", schedule.nextRunTime.toString());
  }
}

export class LocalPromiseStore implements IPromiseStore {
  constructor(private storage: IStorage<DurablePromise> = new MemoryStorage<DurablePromise>()) {}

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
      if (!promise) {
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

      if (strict && !isPendingPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      if (promise.idempotencyKeyForCreate === undefined || ikey !== promise.idempotencyKeyForCreate) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      return promise;
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
      if (!promise) {
        throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
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

      if (strict && !isResolvedPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      if (
        !isTimedoutPromise(promise) &&
        (promise.idempotencyKeyForComplete === undefined || ikey !== promise.idempotencyKeyForComplete)
      ) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      return promise;
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
      if (!promise) {
        throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
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

      if (strict && !isRejectedPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      if (
        !isTimedoutPromise(promise) &&
        (promise.idempotencyKeyForComplete === undefined || ikey !== promise.idempotencyKeyForComplete)
      ) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      return promise;
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
      if (!promise) {
        throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
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

      if (strict && !isCanceledPromise(promise)) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      if (
        !isTimedoutPromise(promise) &&
        (promise.idempotencyKeyForComplete === undefined || ikey !== promise.idempotencyKeyForComplete)
      ) {
        throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
      }

      return promise;
    });
  }

  async get(id: string): Promise<DurablePromise> {
    const promise = await this.storage.rmw(id, (p) => p);

    if (!promise) {
      throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
    }

    return promise;
  }

  async *search(
    id: string,
    state?: string,
    tags?: Record<string, string>,
    limit?: number,
  ): AsyncGenerator<DurablePromise[], void> {
    //  filter the promises returned from all storage
    const regex = new RegExp(id.replaceAll("*", ".*"));
    const states = searchStates(state);
    const tagEntries = Object.entries(tags ?? {});

    for await (const promises of this.storage.all()) {
      yield promises
        .filter((p) => states.includes(p.state))
        .filter((p) => regex.test(p.id))
        .filter((p) => tagEntries.every(([k, v]) => p.tags?.[k] == v));
    }
  }
}

export class LocalScheduleStore implements IScheduleStore {
  constructor(
    private storage: IStorage<Schedule> = new MemoryStorage<Schedule>(),
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
          throw new ResonateError("Already exists", ErrorCodes.STORE_ALREADY_EXISTS);
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
        throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
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

  async delete(id: string): Promise<void> {
    const result = await this.storage.rmd(id, () => true);
    if (!result) {
      throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
    }

    if (this.store) {
      this.store.deleteSchedule(id);
    }
  }

  async get(id: string): Promise<Schedule> {
    const schedule = await this.storage.rmw(id, (s) => s);

    if (!schedule) {
      throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
    }

    return schedule;
  }

  async *search(id: string, tags?: Record<string, string>, limit?: number): AsyncGenerator<Schedule[], void> {
    // filter the schedules returned from storage
    const regex = new RegExp(id.replaceAll("*", ".*"));
    const tagEntries = Object.entries(tags ?? {});

    for await (const schedules of this.storage.all()) {
      yield schedules.filter((s) => regex.test(s.id)).filter((s) => tagEntries.every(([k, v]) => s.tags?.[k] == v));
    }
  }

  private nextRunTime(cron: string, lastRunTime: number): number {
    return cronParser
      .parseExpression(cron, { currentDate: new Date(lastRunTime) })
      .next()
      .getTime();
  }
}

export class LocalLockStore implements ILockStore {
  constructor(
    private storage: IStorage<{ id: string; eid: string }> = new MemoryStorage<{ id: string; eid: string }>(),
  ) {}

  async tryAcquire(id: string, eid: string): Promise<boolean> {
    const lock = await this.storage.rmw(id, (lock) => {
      if (!lock || lock.eid === eid) {
        return {
          id,
          eid,
        };
      }

      return lock;
    });

    if (lock.eid !== eid) {
      throw new ResonateError("Forbidden request", ErrorCodes.STORE_FORBIDDEN);
    }

    return lock.eid === eid;
  }

  async release(id: string, eid: string) {
    const result = await this.storage.rmd(id, (lock) => lock.eid === eid);
    if (!result) {
      throw new ResonateError("Not found", ErrorCodes.STORE_NOT_FOUND);
    }
  }
}

// Utils

function searchStates(state: string | undefined): string[] {
  if (state?.toLowerCase() == "pending") {
    return ["PENDING"];
  } else if (state?.toLowerCase() == "resolved") {
    return ["RESOLVED"];
  } else if (state?.toLowerCase() == "rejected") {
    return ["REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"];
  } else {
    return ["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"];
  }
}
