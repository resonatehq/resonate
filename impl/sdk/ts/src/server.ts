import { CronExpressionParser } from "cron-parser";

import type { CallbackRecord, DurablePromiseRecord, Mesg, ScheduleRecord, TaskRecord } from "./network/network";
import * as util from "./util";

interface DurablePromise {
  id: string;
  state: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
  timeout: number;
  param: any;
  value: any;
  tags: Record<string, string>;
  iKeyForCreate?: string;
  iKeyForComplete?: string;
  createdOn?: number;
  completedOn?: number;
  callbacks?: Map<string, Callback>;
}

interface Callback {
  id: string;
  type: "resume" | "notify";
  promiseId: string;
  rootPromiseId: string;
  recv: string;
  timeout: number;
  createdOn: number;
}

interface Task {
  id: string;
  counter: number;
  state: "init" | "enqueued" | "claimed" | "completed";
  type: "invoke" | "resume" | "notify";
  recv: string;
  rootPromiseId: string;
  leafPromiseId: string;
  pid?: string;
  ttl?: number;
  expiry?: number;
  createdOn: number;
  completedOn?: number;
}

interface Schedule {
  id: string;
  description?: string;
  cron: string;
  tags: Record<string, string>;
  promiseId: string;
  promiseTimeout: number;
  promiseParam: any;
  promiseTags: Record<string, string>;
  lastRunTime?: number;
  nextRunTime?: number;
  iKey?: string;
  createdOn?: number;
}

interface Router {
  route(promise: DurablePromise): any;
}
class TagRouter implements Router {
  private tag: string;

  constructor(tag = "resonate:invoke") {
    this.tag = tag;
  }

  route(promise: DurablePromise): any {
    return promise.tags?.[this.tag];
  }
}

export class Server {
  private promises: Map<string, DurablePromise>;
  private tasks: Map<string, Task>;
  private schedules: Map<string, Schedule>;
  private routers: Array<Router>;
  private targets: Record<string, string>;

  constructor() {
    this.promises = new Map();
    this.tasks = new Map();
    this.schedules = new Map();
    this.routers = new Array(new TagRouter());
    this.targets = { default: "local://any@default" };
  }

  next(time: number = Date.now()): number {
    let timeout: number | undefined = undefined;
    for (const promise of this.promises.values()) {
      if (promise.state === "pending") {
        timeout = timeout === undefined ? promise.timeout : Math.min(promise.timeout, timeout);
      }
    }

    for (const task of this.tasks.values()) {
      if (["init", "enqueued", "claimed"].includes(task.state)) {
        util.assert(task.expiry !== undefined);
        timeout = timeout === undefined ? task.expiry : Math.min(task.expiry!, timeout);
      }
    }

    for (const schedule of this.schedules.values()) {
      util.assert(schedule.nextRunTime !== undefined);
      timeout = timeout === undefined ? schedule.nextRunTime : Math.min(schedule.nextRunTime!, timeout);
    }

    if (timeout !== undefined) {
      timeout = Math.max(0, timeout - time);
    }
    util.assert(timeout !== undefined);
    return timeout!;
  }

  *step(time: number = Date.now()): Generator<
    {
      recv: string;
      msg:
        | { kind: "invoke" | "resume"; id: string; counter: number }
        | { kind: "notify"; promise: DurablePromiseRecord };
    },
    void,
    boolean | undefined
  > {
    for (const schedule of this.schedules.values()) {
      if (time < schedule.nextRunTime!) {
        continue;
      }
      try {
        this.createPromise(
          schedule.promiseId.replace("{{.timestamp}}", time.toString()),
          time + schedule.promiseTimeout,
          schedule.promiseParam,
          schedule.promiseTags,
          undefined,
          false,
          time,
        );
      } catch {}

      const { applied } = this.transitionSchedule(
        schedule.id,
        "created",
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        true,
        time,
      );
      util.assert(applied);
    }
    for (const promise of this.promises.values()) {
      if (promise.state === "pending" && time >= promise.timeout) {
        const { applied } = this.transitionPromise(
          promise.id,
          "rejected_timedout",
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          time,
        );
        util.assert(applied, "expected promise to be timedout");
      }
    }

    // transition expired tasks to init
    for (const task of this.tasks.values()) {
      if (["enqueued", "claimed"].includes(task.state)) {
        util.assert(task.expiry !== undefined);

        if (time >= task.expiry!) {
          const { applied } = this.transitionTask(
            task.id,
            "init",
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            true,
            time,
          );
          util.assert(applied);
        }
      }
    }

    for (const task of this.tasks.values()) {
      if (task.state !== "init") {
        continue;
      }
      let msg:
        | { kind: "invoke" | "resume"; id: string; counter: number }
        | { kind: "notify"; promise: DurablePromiseRecord };
      if (task.type === "invoke") {
        msg = {
          kind: "invoke",
          id: task.id,
          counter: task.counter,
        };
      } else if (task.type === "resume") {
        msg = { kind: "resume", id: task.id, counter: task.counter };
      } else {
        util.assert(task.type === "notify");
        msg = { kind: "notify", promise: this.getPromise(task.rootPromiseId) };
      }

      if (yield { recv: task.recv, msg: msg }) {
        if (task.type === "notify") {
          const { applied } = this.transitionTask(
            task.id,
            "completed",
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            time,
          );
          util.assert(applied);
        } else {
          const { applied } = this.transitionTask(
            task.id,
            "enqueued",
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            time,
          );
          util.assert(applied);
        }
      } else {
        // TODO(dfarr): implement this
        // _, applied = self.tasks.transition(task.id, to="INIT", expiry=0)
        // assert applied
      }
    }
  }

  private _createPromise(
    id: string,
    timeout: number,
    param?: any,
    tags?: Record<string, string>,
    iKey?: string,
    strict?: boolean,
    processId?: string,
    ttl?: number,
    time: number = Date.now(),
  ): { promise: DurablePromiseRecord; task?: Task } {
    const { promise, task, applied } = this.transitionPromise(id, "pending", strict, timeout, iKey, param, tags, time);
    util.assert(!applied || ["pending", "rejected_timedout"].includes(promise.state));
    if (applied && task !== undefined && processId !== undefined) {
      const { task: newTask, applied: appliedTask } = this.transitionTask(task.id, "claimed", undefined);
      util.assert(appliedTask);
      return { promise: promise, task: newTask };
    }
    return { promise, task };
  }
  createPromise(
    id: string,
    timeout: number,
    param?: any,
    tags?: Record<string, string>,
    iKey?: string,
    strict?: boolean,
    time: number = Date.now(),
  ): DurablePromiseRecord {
    return this._createPromise(id, timeout, param, tags, iKey, strict, undefined, undefined, time).promise;
  }

  createPromiseAndTask(
    id: string,
    timeout: number,
    processId: string,
    ttl: number,
    param?: any,
    tags?: Record<string, string>,
    iKey?: string,
    strict?: boolean,
    time: number = Date.now(),
  ): { promise: DurablePromiseRecord; task?: TaskRecord } {
    return this._createPromise(id, timeout, param, tags, iKey, strict, processId, ttl, time) as {
      promise: DurablePromiseRecord;
      task?: TaskRecord;
    };
  }

  readPromise(id: string, time: number = Date.now()): DurablePromiseRecord {
    return this.getPromise(id);
  }

  completePromise(
    id: string,
    state: "resolved" | "rejected" | "rejected_canceled",
    value?: any,
    iKey?: string,
    strict?: boolean,
    time: number = Date.now(),
  ): DurablePromiseRecord {
    const { promise, applied } = this.transitionPromise(id, state, strict, undefined, iKey, value, undefined, time);
    util.assert(!applied || [state, "rejected_timedout"].includes(promise.state));
    return promise;
  }

  createSubscription(
    id: string,
    timeout: number,
    recv: string,
    time: number = Date.now(),
  ): { promise: DurablePromiseRecord; callback?: CallbackRecord } {
    {
      const record = this.promises.get(id);

      if (!record) {
        throw new Error("not found");
      }

      const cbId = `__notify:${id}:${id}`;

      if (record.state !== "pending" || record.callbacks?.has(cbId)) {
        return { promise: record, callback: undefined };
      }

      const callback: Callback = {
        id: cbId,
        type: "notify",
        promiseId: id,
        rootPromiseId: id,
        recv,
        timeout,
        createdOn: time,
      };

      if (!record.callbacks) {
        record.callbacks = new Map<string, Callback>();
      }

      // register and return
      record.callbacks.set(cbId, callback);
      return {
        promise: record,
        callback: callback,
      };
    }
  }

  createCallback(
    id: string,
    rootPromiseId: string,
    timeout: number,
    recv: string,
    time: number = Date.now(),
  ): { promise: DurablePromiseRecord; callback?: CallbackRecord } {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    if (record.state !== "pending" || record.callbacks?.has(id)) {
      return { promise: record, callback: undefined };
    }

    const callback: Callback = {
      id: `__resume:${rootPromiseId}:${id}`,
      type: "resume",
      promiseId: id,
      rootPromiseId: rootPromiseId,
      recv,
      timeout,
      createdOn: time,
    };

    if (!record.callbacks) {
      record.callbacks = new Map<string, Callback>();
    }

    record.callbacks.set(callback.id, callback);
    return { promise: record, callback: callback };
  }

  claimTask(id: string, counter: number, processId: string, ttl: number, time: number = Date.now()): Mesg {
    const { task, applied } = this.transitionTask(
      id,
      "claimed",
      undefined,
      undefined,
      undefined,
      undefined,
      counter,
      processId,
      ttl,
      undefined,
      time,
    );

    util.assert(applied);

    switch (task.type) {
      case "invoke": {
        const promise = this.getPromise(task.rootPromiseId);
        return {
          kind: task.type,
          promises: {
            root: { id: promise.id, data: promise },
          },
        };
      }
      case "resume": {
        return {
          kind: task.type,
          promises: {
            root: {
              id: task.rootPromiseId,
              data: this.getPromise(task.rootPromiseId),
            },
            leaf: {
              id: task.leafPromiseId,
              data: this.getPromise(task.leafPromiseId),
            },
          },
        };
      }
      default:
        throw new Error("unreachable");
    }
  }

  completeTask(id: string, counter: number, time: number = Date.now()): TaskRecord {
    const { task } = this.transitionTask(
      id,
      "completed",
      undefined,
      undefined,
      undefined,
      undefined,
      counter,
      undefined,
      time,
    );

    return {
      id: task.id,
      counter: task.counter,
      timeout: 0,
      processId: task.pid,
      createdOn: task.createdOn,
      completedOn: task.completedOn,
    };
  }

  hearbeatTasks(processId: string, time: number = Date.now()): number {
    let affectedTasks = 0;

    for (const task of this.tasks.values()) {
      if (task.state !== "claimed" || task.pid !== processId) {
        continue;
      }

      const { applied } = this.transitionTask(
        task.id,
        "claimed",
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        true,
        time,
      );

      util.assert(applied);
      affectedTasks += 1;
    }

    return affectedTasks;
  }

  createSchedule(
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    iKey?: string,
    description?: string,
    tags?: Record<string, string>,
    promiseParam?: any,
    promiseTags?: Record<string, string>,
    time: number = Date.now(),
  ): ScheduleRecord {
    return this.transitionSchedule(
      id,
      "created",
      cron,
      promiseId,
      promiseTimeout,
      iKey,
      description,
      tags,
      promiseParam,
      promiseTags,
      undefined,
      time,
    ).schedule;
  }

  readSchedule(id: string): ScheduleRecord {
    const schedule = this.schedules.get(id);
    if (schedule === undefined) {
      throw new Error("schedule not found");
    }
    return schedule;
  }

  deleteSchedule(id: string, time: number = Date.now()): void {
    const { applied } = this.transitionSchedule(id, "deleted");
    util.assert(applied);
  }

  private getPromise(id: string): DurablePromise {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    return record;
  }

  private transitionPromise(
    id: string,
    to: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout",
    strict?: boolean,
    timeout?: number,
    ikey?: string,
    value?: any,
    tags?: Record<string, string>,
    time: number = Date.now(),
  ): { promise: DurablePromise; task?: Task; applied: boolean } {
    const { promise, applied } = this._transitionPromise(id, to, strict, timeout, ikey, value, tags, time);

    if (applied && promise.state === "pending") {
      for (const router of this.routers) {
        const recv = router.route(promise);
        if (recv !== undefined) {
          const { task, applied } = this.transitionTask(
            `__invoke:${id}`,
            "init",
            "invoke",
            this.targets[recv],
            promise.id,
            promise.id,
            time,
          );
          util.assert(applied);
          return { promise: promise, task: task, applied: applied };
        }
      }
    }

    if (applied && ["resolved", "rejected", "rejected_canceled", "rejected_timedout"].includes(promise.state)) {
      for (const task of this.tasks.values()) {
        if (task.rootPromiseId === id && ["init", "enqueued", "claimed"].includes(task.state)) {
          const { applied } = this.transitionTask(
            task.id,
            "completed",
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            true,
            time,
          );
          util.assert(applied);
        }
      }

      if (promise.callbacks) {
        for (const callback of promise.callbacks.values()) {
          const { applied } = this.transitionTask(
            callback.id,
            "init",
            callback.type,
            callback.recv,
            callback.rootPromiseId,
            callback.promiseId,
            time,
          );

          util.assert(applied);
        }
        promise.callbacks.clear();
      }
    }

    return { promise: promise, applied: applied };
  }

  private _transitionPromise(
    id: string,
    to: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout",
    strict?: boolean,
    timeout?: number,
    ikey?: string,
    value?: any,
    tags?: Record<string, string>,
    time: number = Date.now(),
  ): { promise: DurablePromise; applied: boolean } {
    let record = this.promises.get(id);

    if (record === undefined && to === "pending") {
      util.assert(timeout !== undefined);

      record = {
        id: id,
        state: to,
        timeout: timeout!,
        iKeyForCreate: ikey,
        param: value,
        value: undefined,
        tags: tags ?? {},
        createdOn: time,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    if (record === undefined && ["resolved", "rejected", "rejected_canceled"].includes(to)) {
      throw new Error("promise not found");
    }

    if (
      record?.state === "pending" &&
      to === "pending" &&
      time < record.timeout &&
      ikeyMatch(record.iKeyForCreate, ikey)
    ) {
      return { promise: record, applied: false };
    }

    if (
      record?.state === "pending" &&
      to === "pending" &&
      !strict &&
      time >= record.timeout &&
      ikeyMatch(record.iKeyForCreate, ikey)
    ) {
      return this._transitionPromise(
        id,
        "rejected_timedout",
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        time,
      );
    }

    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      time < record.timeout
    ) {
      record = {
        ...record,
        state: to,
        iKeyForComplete: ikey,
        value: value,
        completedOn: time,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict &&
      time >= record.timeout
    ) {
      return this._transitionPromise(
        id,
        "rejected_timedout",
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        time,
      );
    }

    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      strict &&
      time >= record.timeout
    ) {
      throw new Error("the promise is already timeout");
    }

    if (record?.state === "pending" && to === "rejected_timedout") {
      util.assert(time >= record.timeout);

      record = {
        ...record,
        state: record.tags?.["resonate:timeout"] === "true" ? "resolved" : to,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    if (
      record?.state !== undefined &&
      ["resolved", "rejected", "rejected_canceled", "rejected_timedout"].includes(record.state) &&
      to === "pending" &&
      !strict &&
      ikeyMatch(record.iKeyForCreate, ikey)
    ) {
      return { promise: record, applied: false };
    }

    if (
      record !== undefined &&
      ["resolved", "rejected", "rejected_canceled"].includes(record.state) &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict &&
      ikeyMatch(record.iKeyForComplete, ikey)
    ) {
      return { promise: record, applied: false };
    }

    if (
      record?.state === "rejected_timedout" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict
    ) {
      return { promise: record, applied: false };
    }

    if (
      record !== undefined &&
      ["resolved", "rejected", "rejected_canceled"].includes(record.state) &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      strict &&
      ikeyMatch(record.iKeyForComplete, ikey) &&
      record.state === to
    ) {
      return { promise: record, applied: false };
    }

    throw new Error("unexpected transition");
  }

  private transitionTask(
    id: string,
    to: "init" | "enqueued" | "claimed" | "completed",
    type?: "invoke" | "resume" | "notify",
    recv?: string,
    rootPromiseId?: string,
    leafPromiseId?: string,
    counter?: number,
    pid?: string,
    ttl?: number,
    force = false,
    time: number = Date.now(),
  ): { task: Task; applied: boolean } {
    let record = this.tasks.get(id);

    if (record === undefined && to === "init") {
      util.assert(type !== undefined);
      util.assert(recv !== undefined);
      util.assert(rootPromiseId !== undefined);
      util.assert(leafPromiseId !== undefined);

      record = {
        id: id,
        counter: 1,
        state: to,
        type: type!,
        recv: recv!,
        rootPromiseId: rootPromiseId!,
        leafPromiseId: leafPromiseId!,
        createdOn: time,
      };
      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "init" && to === "enqueued") {
      record = {
        ...record,
        state: to,
        expiry: time + 5000,
      };
      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "init" && to === "claimed" && record.counter === counter) {
      util.assert(ttl !== undefined);
      util.assert(pid !== undefined);

      record = {
        ...record,
        state: to,
        pid: pid!,
        ttl: ttl!,
        expiry: time + ttl!,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "enqueued" && to === "claimed" && record.counter === counter) {
      util.assert(ttl !== undefined);
      util.assert(pid !== undefined);

      record = {
        ...record,
        state: to,
        pid: pid!,
        ttl: ttl!,
        expiry: time + ttl!,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record !== undefined &&
      ["init", "enqueued"].includes(record.state) &&
      record.type === "notify" &&
      to === "completed"
    ) {
      record = {
        ...record,
        state: to,
        completedOn: time,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record !== undefined && ["enqueued", "claimed"].includes(record.state) && to === "init") {
      util.assert(record.expiry !== undefined);
      util.assert(time >= record.expiry!);

      record = {
        ...record,
        counter: record.counter + 1,
        state: to,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "claimed" && to === "claimed" && force) {
      util.assert(record.ttl !== undefined);

      record = {
        ...record,
        pid: record.pid!,
        ttl: record.ttl!,
        expiry: time + record.ttl!,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record?.state === "claimed" &&
      to === "completed" &&
      record.counter === counter &&
      record.expiry !== undefined &&
      record.expiry >= time
    ) {
      record = {
        ...record,
        state: to,
        completedOn: time,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record !== undefined &&
      ["init", "enqueued", "claimed"].includes(record?.state) &&
      to === "completed" &&
      force
    ) {
      record = {
        ...record,
        state: to,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "completed" && to === "completed") {
      return { task: record, applied: false };
    }

    if (record === undefined) {
      throw new Error("Task not found");
    }

    throw new Error("task is already claimed, completed, or an invalid counter was provided");
  }

  private transitionSchedule(
    id: string,
    to: "created" | "deleted",
    cron?: string,
    promiseId?: string,
    promiseTimeout?: number,
    iKey?: string,
    description?: string,
    tags?: Record<string, string>,
    promiseParam?: any,
    promiseTags?: Record<string, string>,
    updating?: boolean,
    time: number = Date.now(),
  ): { schedule: Schedule; applied: boolean } {
    let record = this.schedules.get(id);

    if (record === undefined && to === "created") {
      util.assert(cron !== undefined);
      util.assert(promiseId !== undefined);
      util.assert(promiseTimeout !== undefined);
      util.assert(promiseTimeout! >= 0);

      record = {
        id: id,
        description: description,
        cron: cron!,
        tags: tags ?? {},
        promiseId: promiseId!,
        promiseTimeout: promiseTimeout!,
        promiseParam: promiseParam,
        promiseTags: promiseTags ?? {},
        lastRunTime: undefined,
        nextRunTime: CronExpressionParser.parse(cron!).next().getMilliseconds(),
        iKey: iKey,
        createdOn: time,
      };
      this.schedules.set(id, record);
      return { schedule: record, applied: true };
    }

    if (record !== undefined && to === "created" && ikeyMatch(iKey, record.iKey)) {
      return { schedule: record, applied: false };
    }

    if (record !== undefined && to === "created" && updating) {
      record = {
        ...record,
        lastRunTime: record.nextRunTime,
        nextRunTime: CronExpressionParser.parse(record.cron!).next().getMilliseconds(),
      };
      this.schedules.set(id, record);
      return { schedule: record, applied: true };
    }

    if (record !== undefined && to === "created") {
      throw new Error("schedule already exists");
    }

    if (record === undefined && to === "deleted") {
      throw new Error("schedule not found");
    }

    if (record !== undefined && to === "deleted") {
      this.schedules.delete(id);
      return { schedule: record, applied: true };
    }

    throw new Error("Unexpected transition");
  }
}

function ikeyMatch(left: string | undefined, right: string | undefined): boolean {
  return left !== undefined && right !== undefined && left === right;
}
