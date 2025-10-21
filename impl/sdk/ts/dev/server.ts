import { CronExpressionParser } from "cron-parser";

import exceptions from "../src/exceptions";
import type {
  CallbackRecord,
  ClaimTaskRes,
  DurablePromiseRecord,
  Message,
  Request,
  Response,
  ScheduleRecord,
  TaskRecord,
} from "../src/network/network";
import * as util from "../src/util";

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
  timeout: number;
  processId?: string;
  ttl?: number;
  expiry: number;
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
  readonly promises: Map<string, DurablePromise>;
  readonly tasks: Map<string, Task>;
  readonly schedules: Map<string, Schedule>;
  private routers: Array<Router>;
  private targets: Record<string, string>;

  constructor() {
    this.promises = new Map();
    this.tasks = new Map();
    this.schedules = new Map();
    this.routers = new Array(new TagRouter());
    this.targets = { default: "local://any@default" };
  }

  next(time: number): number | undefined {
    let timeout: number | undefined;

    // Check promises
    for (const promise of this.promises.values()) {
      if (promise.state === "pending") {
        timeout = Math.min(promise.timeout, timeout ?? promise.timeout);
      }
    }

    // Check schedules
    for (const schedule of this.schedules.values()) {
      util.assertDefined(schedule.nextRunTime);
      timeout = Math.min(schedule.nextRunTime, timeout ?? schedule.nextRunTime);
    }

    // Check tasks
    for (const task of this.tasks.values()) {
      if (["init", "claimed", "enqueued"].includes(task.state)) {
        timeout = Math.min(task.expiry, timeout ?? task.expiry);
      }
    }

    // Convert to delay relative to `time`, clamped to signed 32-bit range
    return timeout !== undefined ? Math.min(Math.max(0, timeout - time), 2147483647) : timeout;
  }

  step(time: number): { msg: Message; recv: string }[] {
    for (const schedule of this.schedules.values()) {
      util.assertDefined(schedule.nextRunTime);
      if (time < schedule.nextRunTime) {
        continue;
      }

      try {
        this.createPromise({
          id: schedule.promiseId.replace("{{.timestamp}}", time.toString()),
          timeout: time + schedule.promiseTimeout,
          param: schedule.promiseParam,
          tags: schedule.promiseTags,
          strict: false,
          time,
        });
      } catch {}

      const { applied } = this.transitionSchedule({ id: schedule.id, to: "created", updating: true, time });

      util.assert(applied, `step(): failed to transition schedule '${schedule.id}' to 'created' state`);
    }

    // Reject timed-out promises
    for (const promise of this.promises.values()) {
      if (promise.state === "pending" && time >= promise.timeout) {
        const { applied } = this.transitionPromise({ id: promise.id, to: "rejected_timedout", time });
        util.assert(applied, `step(): promise '${promise.id}' expected to be timed out but transition did not apply`);
      }
    }

    // Transition expired tasks back to init
    for (const task of this.tasks.values()) {
      if (["enqueued", "claimed"].includes(task.state)) {
        util.assertDefined(task.expiry);

        if (time >= task.expiry) {
          const { applied } = this.transitionTask({ id: task.id, to: "init", force: true, time });
          util.assert(applied, `step(): failed to force-reinit expired task '${task.id}'`);
        }
      }
    }

    const inFlightRootPromiseIds = new Set<string>();

    for (const task of this.tasks.values()) {
      if (["enqueued", "claimed"].includes(task.state)) {
        inFlightRootPromiseIds.add(task.rootPromiseId);
      }
    }

    const msgs: { msg: Message; recv: string }[] = [];
    for (const task of this.tasks.values()) {
      if (task.state !== "init" || task.expiry > time || inFlightRootPromiseIds.has(task.rootPromiseId)) {
        continue;
      }

      let msg: { msg: Message; recv: string };
      if (task.type === "invoke") {
        msg = {
          msg: {
            type: "invoke",
            task: {
              ...task,
              timeout: this.getPromise({ id: task.rootPromiseId }).timeout,
            },
          },
          recv: task.recv,
        };
      } else if (task.type === "resume") {
        msg = {
          msg: {
            type: "resume",
            task: {
              ...task,
              timeout: this.getPromise({ id: task.rootPromiseId }).timeout,
            },
          },
          recv: task.recv,
        };
      } else {
        util.assert(task.type === "notify", `step(): unexpected task type '${task.type}' for notify message`);
        msg = {
          msg: {
            type: "notify",
            promise: this.getPromise({ id: task.rootPromiseId }),
          },
          recv: task.recv,
        };
      }

      msgs.push(msg);

      if (task.type === "notify") {
        const { applied } = this.transitionTask({ id: task.id, to: "completed", time });
        util.assert(applied, `step(): failed to complete notify task '${task.id}'`);
      } else {
        const { applied } = this.transitionTask({ id: task.id, to: "enqueued", time });
        util.assert(applied, `step(): failed to enqueue task '${task.id}' after invoking/resuming`);
      }
      inFlightRootPromiseIds.add(task.rootPromiseId);
    }

    return msgs;
  }

  process(requ: Request, time: number): Response {
    switch (requ.kind) {
      case "createPromise": {
        return {
          kind: requ.kind,
          promise: this.createPromise({
            ...requ,
            time,
          }),
        };
      }
      case "createPromiseAndTask": {
        const { promise, task } = this.createPromiseAndTask({
          id: requ.promise.id,
          timeout: requ.promise.timeout,
          processId: requ.task.processId,
          ttl: requ.task.ttl,
          param: requ.promise.param,
          tags: requ.promise.tags,
          iKey: requ.iKey,
          strict: requ.strict,
          time,
        });
        return {
          kind: requ.kind,
          promise: promise,
          task: task,
        };
      }
      case "readPromise": {
        return {
          kind: requ.kind,
          promise: this.readPromise({ ...requ }),
        };
      }

      case "completePromise": {
        return {
          kind: requ.kind,
          promise: this.completePromise({
            ...requ,
            time,
          }),
        };
      }

      case "createCallback": {
        return {
          kind: requ.kind,
          ...this.createCallback({
            ...requ,
            time,
          }),
        };
      }

      case "createSubscription": {
        return {
          kind: requ.kind,
          ...this.createSubscription({ ...requ, time }),
        };
      }

      case "createSchedule": {
        return {
          kind: requ.kind,
          schedule: this.createSchedule({
            id: requ.id!,
            cron: requ.cron!,
            promiseId: requ.promiseId!,
            promiseTimeout: requ.promiseTimeout!,
            iKey: requ.iKey,
            description: requ.description,
            tags: requ.tags,
            promiseParam: requ.promiseParam,
            promiseTags: requ.promiseTags,
            time,
          }),
        };
      }

      case "readSchedule": {
        return { kind: requ.kind, schedule: this.readSchedule({ ...requ }) };
      }

      case "deleteSchedule": {
        this.deleteSchedule({ ...requ, time });
        return { kind: requ.kind };
      }

      case "claimTask": {
        return {
          kind: "claimTask",
          message: this.claimTask({
            ...requ,
            time,
          }),
        };
      }

      case "completeTask": {
        return {
          kind: "completeTask",
          task: this.completeTask({ ...requ, time }),
        };
      }

      case "heartbeatTasks": {
        return {
          kind: "heartbeatTasks",
          tasksAffected: this.heartbeatTasks({ ...requ, time }),
        };
      }

      case "dropTask": {
        this.dropTask({ ...requ, time });
        return { kind: "dropTask" };
      }

      case "searchPromises": {
        return { kind: "searchPromises", promises: this.searchPromises({ ...requ }) };
      }

      case "searchSchedules": {
        return { kind: "searchSchedules", schedules: this.searchSchedules({ ...requ }) };
      }

      default:
        throw exceptions.SERVER_ERROR(`Unsupported request kind ${(requ as any).kind}`);
    }
  }

  private createPromise({
    id,
    timeout,
    param,
    tags,
    iKey,
    strict,
    time,
  }: {
    id: string;
    timeout: number;
    param?: any;
    tags?: Record<string, string>;
    iKey?: string;
    strict?: boolean;
    time: number;
  }): DurablePromiseRecord {
    return this._createPromise({
      id,
      timeout,
      param,
      tags,
      iKey,
      strict,
      time,
    }).promise;
  }

  private createPromiseAndTask({
    id,
    timeout,
    processId,
    ttl,
    param,
    tags,
    iKey,
    strict,
    time,
  }: {
    id: string;
    timeout: number;
    processId: string;
    ttl: number;
    param?: any;
    tags?: Record<string, string>;
    iKey?: string;
    strict?: boolean;
    time: number;
  }): { promise: DurablePromiseRecord; task?: TaskRecord } {
    const { promise, task } = this._createPromise({
      id,
      timeout,
      processId,
      ttl,
      param,
      tags,
      iKey,
      strict,
      time,
    });
    if (task === undefined) {
      return { promise };
    }
    return { promise, task: { ...task, timeout: promise.timeout } };
  }

  private _createPromise({
    id,
    timeout,
    param,
    tags,
    iKey,
    strict,
    processId,
    ttl,
    time,
  }: {
    id: string;
    timeout: number;
    param?: any;
    tags?: Record<string, string>;
    iKey?: string;
    strict?: boolean;
    processId?: string;
    ttl?: number;
    time: number;
  }): { promise: DurablePromiseRecord; task?: Task } {
    const { promise, task, applied } = this.transitionPromise({
      id,
      to: "pending",
      strict,
      timeout,
      iKey,
      value: param,
      tags,
      time,
    });

    util.assert(
      !applied || ["pending", "rejected_timedout"].includes(promise.state),
      `createPromise: unexpected promise state '${promise.state}' after transition to 'pending' for promise '${id}'`,
    );

    if (applied && task !== undefined && processId !== undefined) {
      const { task: newTask, applied: appliedTask } = this.transitionTask({
        id: task.id,
        to: "claimed",
        counter: 1,
        processId,
        ttl,
        time,
      });
      util.assert(appliedTask, `createPromise: failed to claim task '${task.id}' for subsequent processing`);
      return { promise: promise, task: newTask };
    }

    return { promise, task };
  }

  private readPromise({ id }: { id: string }): DurablePromiseRecord {
    return this.getPromise({ id });
  }

  private completePromise({
    id,
    state,
    value,
    iKey,
    strict,
    time,
  }: {
    id: string;
    state: "resolved" | "rejected" | "rejected_canceled";
    value?: any;
    iKey?: string;
    strict?: boolean;
    time: number;
  }): DurablePromiseRecord {
    const { promise, applied } = this.transitionPromise({
      id,
      to: state,
      strict,
      iKey,
      value,
      time,
    });
    util.assert(
      !applied || [state, "rejected_timedout"].includes(promise.state),
      `completePromise: after transition to '${state}', promise '${id}' is in unexpected state '${promise.state}'`,
    );
    return promise;
  }

  private createSubscription({
    id,
    promiseId,
    timeout,
    recv,
    time,
  }: {
    id: string;
    promiseId: string;
    timeout: number;
    recv: string;
    time: number;
  }): {
    promise: DurablePromiseRecord;
    callback?: CallbackRecord;
  } {
    {
      const record = this.promises.get(promiseId);
      if (!record) {
        throw exceptions.SERVER_ERROR("Promise not found");
      }

      const cbId = `__notify:${promiseId}:${id}`;

      if (record.state !== "pending" || record.callbacks?.has(cbId)) {
        return { promise: record, callback: undefined };
      }

      const callback: Callback = {
        id: cbId,
        type: "notify",
        promiseId: promiseId,
        rootPromiseId: promiseId,
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

  private createCallback({
    promiseId,
    rootPromiseId,
    timeout,
    recv,
    time,
  }: {
    promiseId: string;
    rootPromiseId: string;
    timeout: number;
    recv: string;
    time: number;
  }): {
    promise: DurablePromiseRecord;
    callback?: CallbackRecord;
  } {
    const record = this.promises.get(promiseId);
    if (!record) {
      throw exceptions.SERVER_ERROR("Promise not found");
    }

    if (record.state !== "pending" || record.callbacks?.has(promiseId)) {
      return { promise: record, callback: undefined };
    }

    const callback: Callback = {
      id: `__resume:${rootPromiseId}:${promiseId}`,
      type: "resume",
      promiseId,
      rootPromiseId,
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

  private claimTask({
    id,
    counter,
    processId,
    ttl,
    time,
  }: {
    id: string;
    counter: number;
    processId: string;
    ttl: number;
    time: number;
  }): ClaimTaskRes["message"] {
    const { task, applied } = this.transitionTask({
      id,
      to: "claimed",
      counter,
      processId,
      ttl,
      time,
    });

    util.assert(
      applied,
      `claimTask: failed to claim task '${id}' with counter ${counter} using processId '${processId}'`,
    );

    switch (task.type) {
      case "invoke": {
        const promise = this.getPromise({ id: task.rootPromiseId });
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
              data: this.getPromise({ id: task.rootPromiseId }),
            },
            leaf: {
              id: task.leafPromiseId,
              data: this.getPromise({ id: task.leafPromiseId }),
            },
          },
        };
      }
      default:
        throw exceptions.SERVER_ERROR(`Unexpected task type '${task.type}'`);
    }
  }

  private completeTask({ id, counter, time }: { id: string; counter: number; time: number }): TaskRecord {
    const { task } = this.transitionTask({ id, to: "completed", counter, time });

    return task;
  }

  private dropTask({ id, counter, time }: { id: string; counter: number; time: number }) {
    const { applied } = this.transitionTask({ id, to: "init", counter, time });
    util.assert(applied);
    return;
  }

  private searchPromises({ id }: { id: string }): DurablePromiseRecord[] {
    return Array.from(this.promises.values());
  }

  private searchSchedules({ id }: { id: string }): ScheduleRecord[] {
    return Array.from(this.schedules.values());
  }

  private heartbeatTasks({ processId, time }: { processId: string; time: number }): number {
    let affectedTasks = 0;

    for (const task of this.tasks.values()) {
      if (task.state !== "claimed" || task.processId !== processId) {
        continue;
      }

      const { applied } = this.transitionTask({ id: task.id, to: "claimed", force: true, time });

      util.assert(
        applied,
        `heartbeatTasks: failed to refresh heartbeat for task '${task.id}' owned by process '${processId}'`,
      );

      affectedTasks += 1;
    }

    return affectedTasks;
  }

  private createSchedule({
    id,
    cron,
    promiseId,
    promiseTimeout,
    iKey,
    description,
    tags,
    promiseParam,
    promiseTags,
    time,
  }: {
    id: string;
    cron: string;
    promiseId: string;
    promiseTimeout: number;
    iKey?: string;
    description?: string;
    tags?: Record<string, string>;
    promiseParam?: any;
    promiseTags?: Record<string, string>;
    time: number;
  }): ScheduleRecord {
    return this.transitionSchedule({
      id,
      to: "created",
      cron,
      promiseId,
      promiseTimeout,
      iKey,
      description,
      tags,
      promiseParam,
      promiseTags,
      time,
    }).schedule;
  }

  private readSchedule({ id }: { id: string }): ScheduleRecord {
    const schedule = this.schedules.get(id);
    if (schedule === undefined) {
      throw exceptions.SERVER_ERROR("Schedule not found");
    }

    return schedule;
  }

  private deleteSchedule({ id, time }: { id: string; time: number }): void {
    const { applied } = this.transitionSchedule({ id, to: "deleted", time });

    util.assert(applied, `deleteSchedule: failed to delete schedule '${id}'`);
  }

  private getPromise({ id }: { id: string }): DurablePromise {
    const record = this.promises.get(id);
    if (!record) {
      throw exceptions.SERVER_ERROR("Promise not found");
    }

    return record;
  }

  private transitionPromise({
    id,
    to,
    strict,
    timeout,
    iKey,
    value,
    tags,
    time,
  }: {
    id: string;
    to: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
    strict?: boolean;
    timeout?: number;
    iKey?: string;
    value?: any;
    tags?: Record<string, string>;
    time: number;
  }): { promise: DurablePromise; task?: Task; applied: boolean } {
    const { promise, applied } = this._transitionPromise({
      id,
      to,
      strict,
      timeout,
      iKey,
      value,
      tags,
      time,
    });

    // Initialize invocation tasks on pending
    if (applied && promise.state === "pending") {
      for (const router of this.routers) {
        const recv = router.route(promise);
        if (recv !== undefined) {
          const { task, applied } = this.transitionTask({
            id: `__invoke:${id}`,
            to: "init",
            type: "invoke",
            recv: this.targets[recv] ?? recv,
            rootPromiseId: promise.id,
            leafPromiseId: promise.id,
            timeout: promise.timeout,
            time,
          });
          util.assert(applied, `transitionPromise: failed to init invoke task for promise '${id}' on route '${recv}'`);
          return { promise, task, applied };
        }
      }
    }

    // Complete any tasks and schedule callbacks after resolution
    if (applied && ["resolved", "rejected", "rejected_canceled", "rejected_timedout"].includes(promise.state)) {
      // Mark existing tasks as completed
      for (const task of this.tasks.values()) {
        if (task.rootPromiseId === id && ["init", "enqueued", "claimed"].includes(task.state)) {
          const { applied } = this.transitionTask({
            id: task.id,
            to: "completed",
            force: true,
            time,
          });
          util.assert(applied, `transitionPromise: failed to complete task '${task.id}' for promise '${id}'`);
        }
      }

      // Initialize callback tasks
      if (promise.callbacks) {
        for (const callback of promise.callbacks.values()) {
          const { applied } = this.transitionTask({
            ...callback,
            to: "init",
            leafPromiseId: callback.promiseId,
            time,
          });
          util.assert(applied, `transitionPromise: failed to init callback task '${callback.id}' for promise '${id}'`);
        }
        promise.callbacks.clear();
      }
    }

    return { promise, applied };
  }

  private _transitionPromise({
    id,
    to,
    strict,
    timeout,
    iKey,
    value,
    tags,
    time,
  }: {
    id: string;
    to: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
    strict?: boolean;
    timeout?: number;
    iKey?: string;
    value?: any;
    tags?: Record<string, string>;
    time: number;
  }): { promise: DurablePromise; applied: boolean } {
    let record = this.promises.get(id);

    // Create new promise
    if (record === undefined && to === "pending") {
      util.assertDefined(timeout);
      record = {
        id,
        state: to,
        timeout,
        iKeyForCreate: iKey,
        param: value,
        value: undefined,
        tags: tags ?? {},
        createdOn: time,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    // Cannot complete non-existent promise
    if (record === undefined && ["resolved", "rejected", "rejected_canceled"].includes(to)) {
      throw exceptions.SERVER_ERROR("Promise not found");
    }

    // No-op re-create pending if before timeout and same iKey
    if (
      record?.state === "pending" &&
      to === "pending" &&
      time < record.timeout &&
      ikeyMatch(record.iKeyForCreate, iKey)
    ) {
      return { promise: record, applied: false };
    }

    // Auto-timeout transition
    if (
      record?.state === "pending" &&
      to === "pending" &&
      !strict &&
      time >= record.timeout &&
      ikeyMatch(record.iKeyForCreate, iKey)
    ) {
      return this._transitionPromise({ id, to: "rejected_timedout", time });
    }

    // Resolve or reject before timeout
    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      time < record.timeout
    ) {
      record = {
        ...record,
        state: to,
        iKeyForComplete: iKey,
        value: value,
        completedOn: time,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    // Attempt completion after timeout without strict -> treat as timeout
    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict &&
      time >= record.timeout
    ) {
      return this._transitionPromise({ id, to: "rejected_timedout", time });
    }

    // Strict completion after timeout -> error
    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      strict &&
      time >= record.timeout
    ) {
      throw exceptions.SERVER_ERROR("Promise already timedout");
    }

    // Transition to timed-out
    if (record?.state === "pending" && to === "rejected_timedout") {
      util.assert(
        time >= record.timeout,
        `transitionPromise(rejected_timedout): cannot time out promise '${id}' before its timeout (${record.timeout})`,
      );

      record = {
        ...record,
        state: record.tags?.["resonate:timeout"] === "true" ? "resolved" : to,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    // No-op re-pending or re-completing without strict and matching iKey
    if (
      record?.state !== undefined &&
      ["resolved", "rejected", "rejected_canceled", "rejected_timedout"].includes(record.state) &&
      to === "pending" &&
      !strict &&
      ikeyMatch(record.iKeyForCreate, iKey)
    ) {
      return { promise: record, applied: false };
    }

    if (
      record !== undefined &&
      ["resolved", "rejected", "rejected_canceled"].includes(record.state) &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict &&
      ikeyMatch(record.iKeyForComplete, iKey)
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
      ikeyMatch(record.iKeyForComplete, iKey) &&
      record.state === to
    ) {
      return { promise: record, applied: false };
    }

    // Fallback
    throw exceptions.SERVER_ERROR("Unexpected promise transition");
  }

  private transitionTask({
    id,
    to,
    type,
    recv,
    timeout,
    rootPromiseId,
    leafPromiseId,
    counter,
    processId,
    ttl,
    force,
    time,
  }: {
    id: string;
    to: "init" | "enqueued" | "claimed" | "completed";
    type?: "invoke" | "resume" | "notify";
    recv?: string;
    rootPromiseId?: string;
    leafPromiseId?: string;
    timeout?: number;
    counter?: number;
    processId?: string;
    ttl?: number;
    force?: boolean;
    time: number;
  }): { task: Task; applied: boolean } {
    let record = this.tasks.get(id);

    if (record === undefined && to === "init") {
      util.assertDefined(type);
      util.assertDefined(recv);
      util.assertDefined(rootPromiseId);
      util.assertDefined(leafPromiseId);
      util.assertDefined(timeout);

      record = {
        id,
        counter: 1,
        timeout,
        state: to,
        type,
        recv,
        rootPromiseId,
        leafPromiseId,
        expiry: 0,
        createdOn: time,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "init" && to === "enqueued") {
      record = {
        ...record,
        state: to,
        expiry: time + 5000, // wait 5s before transitioning back to init if unclaimed
      };
      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "init" && to === "claimed" && record.counter === counter) {
      util.assertDefined(processId);
      util.assertDefined(ttl);
      util.assertDefined(counter);

      record = {
        ...record,
        state: to,
        processId,
        ttl,
        expiry: time + ttl,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "enqueued" && to === "claimed" && record.counter === counter) {
      util.assertDefined(ttl);
      util.assertDefined(counter);
      util.assertDefined(processId);

      record = {
        ...record,
        state: to,
        processId,
        ttl,
        expiry: time + ttl,
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

    if (record !== undefined && record.state === "claimed" && record.counter === counter && to === "init") {
      record = {
        ...record,
        counter: record.counter + 1,
        state: to,
        processId: undefined,
        ttl: undefined,
        expiry: time + 5000, // wait 5s before sending again
      };
      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record !== undefined && ["enqueued", "claimed"].includes(record.state) && to === "init") {
      util.assertDefined(record.expiry);

      record = {
        ...record,
        counter: record.counter + 1,
        state: to,
        processId: undefined,
        ttl: undefined,
        expiry: 0,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "claimed" && to === "claimed" && force) {
      util.assertDefined(record.ttl);

      record = {
        ...record,
        expiry: time + record.ttl,
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
        completedOn: time,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "completed" && to === "completed") {
      return { task: record, applied: false };
    }

    if (record === undefined) {
      throw exceptions.SERVER_ERROR("Task not found");
    }

    throw exceptions.SERVER_ERROR("Task is already claimed, completed, or an invalid counter was provided");
  }

  private transitionSchedule({
    id,
    to,
    cron,
    promiseId,
    promiseTimeout,
    iKey,
    description,
    tags,
    promiseParam,
    promiseTags,
    updating,
    time,
  }: {
    id: string;
    to: "created" | "deleted";
    cron?: string;
    promiseId?: string;
    promiseTimeout?: number;
    iKey?: string;
    description?: string;
    tags?: Record<string, string>;
    promiseParam?: any;
    promiseTags?: Record<string, string>;
    updating?: boolean;
    time: number;
  }): { schedule: Schedule; applied: boolean } {
    let record = this.schedules.get(id);

    // Create new schedule
    if (record === undefined && to === "created") {
      util.assertDefined(cron);
      util.assertDefined(promiseId);
      util.assertDefined(promiseTimeout);
      util.assert(promiseTimeout >= 0, "promiseTimeout must be non-negative");

      record = {
        id,
        description,
        cron,
        tags: tags ?? {},
        promiseId,
        promiseTimeout,
        promiseParam,
        promiseTags: promiseTags ?? {},
        nextRunTime: CronExpressionParser.parse(cron, { currentDate: time }).next().getTime(),
        iKey,
        createdOn: time,
        lastRunTime: 0,
      };
      this.schedules.set(id, record);

      return { schedule: record, applied: true };
    }

    // No-op if same iKey
    if (record !== undefined && to === "created" && ikeyMatch(iKey, record.iKey)) {
      return { schedule: record, applied: false };
    }

    // Update existing schedule
    if (record !== undefined && to === "created" && updating) {
      const nextRunTime = CronExpressionParser.parse(record.cron, { currentDate: time }).next().getTime();
      record = {
        ...record,
        lastRunTime: record.nextRunTime,
        nextRunTime: nextRunTime,
      };
      this.schedules.set(id, record);
      return { schedule: record, applied: true };
    }

    // Schedule exists and not updating
    if (record !== undefined && to === "created") {
      throw exceptions.SERVER_ERROR("Schedule already exists");
    }

    // Delete non-existent
    if (record === undefined && to === "deleted") {
      throw exceptions.SERVER_ERROR("Schedule not found");
    }

    // Delete existing
    if (record !== undefined && to === "deleted") {
      this.schedules.delete(id);
      return { schedule: record, applied: true };
    }

    // Fallback error
    throw exceptions.SERVER_ERROR("Unexpected schedule transition");
  }
}

function ikeyMatch(left: string | undefined, right: string | undefined): boolean {
  return left !== undefined && right !== undefined && left === right;
}
