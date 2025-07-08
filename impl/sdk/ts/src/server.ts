import type {
  CallbackRecord,
  DurablePromiseRecord,
  Mesg,
  TaskRecord,
} from "./network/network";
import * as util from "./util";

interface DurablePromise {
  id: string;
  state:
    | "pending"
    | "resolved"
    | "rejected"
    | "rejected_canceled"
    | "rejected_timedout";
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
  private routers: Array<Router>;
  private targets: Record<string, string>;

  constructor() {
    this.promises = new Map();
    this.tasks = new Map();
    this.routers = new Array(new TagRouter());
    this.targets = { default: "local://any@defaul" };
  }

  next(): [() => void, number] {
    throw new Error("not implemented");
  }

  step(time: number) {
    for (const promise of this.promises.values()) {
      if (promise.state === "pending" && time >= promise.timeout) {
        let applied = this.transitionPromise(
          time,
          promise.id,
          "rejected_timedout",
        ).applied;
        util.assert(applied, "expected promise to be timedout");
      }
    }

    // transition expired tasks to init
    for (const task of this.tasks.values()) {
      if (["enqueued", "claimed"].includes(task.state)) {
        if (task.expiry === undefined) {
          throw new Error("task must have an expiry");
        }
        util.assert(task.expiry !== undefined);

        if (time >= task.expiry) {
          const applied = this.transitionTask(
            time,
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
          ).applied;
          util.assert(applied);
        }
      }

      //
    }
  }

  createPromise(
    time: number,
    id: string,
    timeout: number,
    param?: any,
    tags?: Record<string, string>,
    iKey?: string,
    strict?: boolean,
  ): DurablePromiseRecord {
    const { promise, task, applied } = this.transitionPromise(
      time,
      id,
      "pending",
      strict,
      timeout,
      iKey,
      param,
      tags,
    );
    util.assert(
      !applied || ["pending", "rejected_timedout"].includes(promise.state),
    );

    return promise;
  }

  readPromise(time: number, id: string): DurablePromiseRecord {
    return this.getPromise(id);
  }

  completePromise(
    time: number,
    id: string,
    state: "resolved" | "rejected" | "rejected_canceled",
    value?: any,
    iKey?: string,
    strict?: boolean,
  ): DurablePromiseRecord {
    const { promise, task, applied } = this.transitionPromise(
      time,
      id,
      state,
      strict,
      undefined,
      iKey,
      value,
    );
    util.assert(
      !applied || [state, "rejected_timedout"].includes(promise.state),
    );
    return promise;
  }

  createSubscription(
    time: number,
    id: string,
    timeout: number,
    recv: string,
  ): { promise: DurablePromiseRecord; callback: CallbackRecord | undefined } {
    const { promise, callback } = this.subscribeToPromise(
      time,
      id,
      id,
      recv,
      timeout,
    );

    return {
      promise: promise,
      callback: callback,
    };
  }

  createCallback(
    time: number,
    id: string,
    rootPromiseId: string,
    timeout: number,
    recv: string,
  ): { promise: DurablePromiseRecord; callback: CallbackRecord | undefined } {
    const { promise, callback } = this.callbackToPromise(
      time,
      id,
      rootPromiseId,
      recv,
      timeout,
    );

    return { promise: promise, callback: callback };
  }

  claimTask(
    time: number,
    id: string,
    counter: number,
    processId: string,
    ttl: number,
  ): Mesg {
    const { task, applied } = this.transitionTask(
      time,
      id,
      "claimed",
      undefined,
      undefined,
      undefined,
      undefined,
      counter,
      processId,
      ttl,
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
        const rootPromise = this.getPromise(task.rootPromiseId);
        const learPromise = this.getPromise(task.leafPromiseId);

        return {
          kind: task.type,
          promises: {
            root: { id: rootPromise.id, data: rootPromise },
            leaf: { id: learPromise.id, data: learPromise },
          },
        };
      }
      default:
        throw new Error("unreachable");
    }
  }

  completeTask(time: number, id: string, counter: number): TaskRecord {
    const { task, applied } = this.transitionTask(
      time,
      id,
      "completed",
      undefined,
      undefined,
      undefined,
      undefined,
      counter,
    );

    util.assert(applied);

    return {
      id: task.id,
      counter: task.counter,
      timeout: 0,
      processId: task.pid,
      createdOn: task.createdOn,
      completedOn: task.completedOn,
    };
  }

  hearbeatTasks(time: number, processId: string): number {
    let applied = false;
    let affectedTasks = 0;

    for (const task of this.tasks.values()) {
      if (task.state !== "claimed" || task.pid !== processId) {
        continue;
      }

      applied = this.transitionTask(
        time,
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
      ).applied;

      util.assert(applied);
      affectedTasks += 1;
    }

    return affectedTasks;
  }

  private getPromise(id: string): DurablePromise {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    return record;
  }

  private subscribeToPromise(
    time: number,
    id: string,
    promiseId: string,
    recv: string,
    timeout: number,
  ): { promise: DurablePromise; callback?: Callback } {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    const cbId = `__notify:${promiseId}:${id}`;

    if (record.state !== "pending" || record.callbacks?.has(cbId)) {
      return { promise: record };
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

  private callbackToPromise(
    time: number,
    id: string,
    rootId: string,
    recv: string,
    timeout: number,
  ): { promise: DurablePromise; callback?: Callback } {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    if (record.state !== "pending" || record.callbacks?.has(id)) {
      return { promise: record };
    }

    const callback: Callback = {
      id: `__resume:${rootId}:${id}`,
      type: "resume",
      promiseId: id,
      rootPromiseId: rootId,
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

  private transitionPromise(
    time: number,
    id: string,
    to:
      | "pending"
      | "resolved"
      | "rejected"
      | "rejected_canceled"
      | "rejected_timedout",
    strict?: boolean,
    timeout?: number,
    ikey?: string,
    value?: any,
    tags?: Record<string, string>,
  ): { promise: DurablePromise; task?: Task; applied: boolean } {
    const { promise, applied } = this._transitionPromise(
      time,
      id,
      to,
      strict,
      timeout,
      ikey,
      value,
      tags,
    );

    if (applied && promise.state === "pending") {
      for (const router of this.routers) {
        const recv = router.route(promise);
        if (recv !== undefined) {
          const { task, applied } = this.transitionTask(
            time,
            `__invoke:${id}`,
            "init",
            "invoke",
            this.targets[recv],
            promise.id,
            promise.id,
          );
          util.assert(applied);
          return { promise: promise, task: task, applied: applied };
        }
      }
    }

    if (
      applied &&
      [
        "resolved",
        "rejected",
        "rejected_canceled",
        "rejected_timedout",
      ].includes(promise.state) &&
      promise.callbacks
    ) {
      for (const callback of promise.callbacks.values()) {
        const { task, applied } = this.transitionTask(
          time,
          callback.id,
          "init",
          callback.type,
          callback.recv,
          callback.rootPromiseId,
          callback.promiseId,
        );

        util.assert(applied);
      }
      promise.callbacks.clear();
    }
    return { promise: promise, applied: applied };
  }

  private _transitionPromise(
    time: number,
    id: string,
    to:
      | "pending"
      | "resolved"
      | "rejected"
      | "rejected_canceled"
      | "rejected_timedout",
    strict?: boolean,
    timeout?: number,
    ikey?: string,
    value?: any,
    tags?: Record<string, string>,
  ): { promise: DurablePromise; applied: boolean } {
    let record = this.promises.get(id);

    if (record === undefined && to === "pending") {
      if (timeout === undefined) {
        throw new Error("timeout not set");
      }

      record = {
        id: id,
        state: to,
        timeout: timeout,
        iKeyForCreate: ikey,
        param: value,
        value: undefined,
        tags: tags ?? {},
        createdOn: time,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    if (
      record === undefined &&
      ["resolved", "rejected", "rejected_canceled"].includes(to)
    ) {
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
      return this._transitionPromise(time, id, "rejected_timedout");
    }

    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      time < record.timeout
    ) {
      record = {
        id: record.id,
        state: to,
        timeout: record.timeout,
        iKeyForCreate: record.iKeyForCreate,
        iKeyForComplete: ikey,
        param: record.param,
        value: value,
        tags: record.tags,
        createdOn: record.createdOn,
        completedOn: time,
        callbacks: record.callbacks,
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
      return this._transitionPromise(time, id, "rejected_timedout");
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
        id: record.id,
        state: record.tags?.["resonate:timeout"] === "true" ? "resolved" : to,
        timeout: record.timeout,
        iKeyForCreate: record.iKeyForCreate,
        param: record.param,
        value: record.value,
        tags: record.tags,
        createdOn: record.createdOn,
        completedOn: record.completedOn,
        callbacks: record.callbacks,
      };

      this.promises.set(id, record);
      return { promise: record, applied: true };
    }

    if (
      record?.state !== undefined &&
      [
        "resolved",
        "rejected",
        "rejected_canceled",
        "rejected_timedout",
      ].includes(record.state) &&
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
      record !== undefined &&
      record.state === "rejected_timedout" &&
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
    time: number,
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
  ): { task: Task; applied: boolean } {
    let record = this.tasks.get(id);

    if (record === undefined && to === "init") {
      if (!type || !recv || !rootPromiseId || !leafPromiseId) {
        throw new Error("Missing required fields from init task");
      }
      record = {
        id: id,
        counter: 1,
        state: to,
        type: type,
        recv: recv,
        rootPromiseId: rootPromiseId,
        leafPromiseId: leafPromiseId,
        createdOn: time,
      };
      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (record?.state === "init" && to === "enqueued") {
      record = {
        id: record.id,
        counter: record.counter,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        expiry: time + 500,
        createdOn: record.createdOn,
        completedOn: record.completedOn,
      };
      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record?.state === "init" &&
      to === "claimed" &&
      record.counter === counter
    ) {
      if (!ttl || !pid) {
        throw new Error("Missing required fields from init task");
      }

      record = {
        id: record.id,
        counter: record.counter,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        pid: pid,
        ttl: ttl,
        expiry: time + ttl,
        createdOn: record.createdOn,
        completedOn: record.completedOn,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record?.state === "enqueued" &&
      to === "claimed" &&
      record.counter === counter
    ) {
      if (!ttl || !pid) {
        throw new Error("Missing required fields from init task");
      }

      record = {
        id: record.id,
        counter: record.counter,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        pid: pid,
        ttl: ttl,
        expiry: time + ttl,
        createdOn: record.createdOn,
        completedOn: record.completedOn,
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
        id: record.id,
        counter: record.counter,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        createdOn: record.createdOn,
        completedOn: time,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record !== undefined &&
      ["enqueued", "claimed"].includes(record.state) &&
      to === "init"
    ) {
      if (record.expiry === undefined || time < record.expiry) {
        throw new Error("Missing required fields from init task");
      }

      record = {
        id: record.id,
        counter: record.counter + 1,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        createdOn: record.createdOn,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record !== undefined &&
      record.state === "claimed" &&
      to === "claimed" &&
      force
    ) {
      if (record.ttl === undefined) {
        throw new Error("record.ttl must exist");
      }

      record = {
        id: record.id,
        counter: record.counter,
        state: record.state,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        pid: record.pid,
        ttl: record.ttl,
        expiry: time + record.ttl,
        createdOn: record.createdOn,
        completedOn: record.completedOn,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record !== undefined &&
      record.state === "claimed" &&
      to === "completed" &&
      record.counter === counter &&
      record.expiry !== undefined &&
      record.expiry >= time
    ) {
      record = {
        id: record.id,
        counter: record.counter,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        createdOn: record.createdOn,
        completedOn: time,
      };

      this.tasks.set(id, record);
      return { task: record, applied: true };
    }

    if (
      record !== undefined &&
      ["init", "enqueued", "claimed"].includes(record.state) &&
      to === "completed" &&
      force
    ) {
      record = {
        id: id,
        counter: record.counter,
        state: to,
        type: record.type,
        recv: record.recv,
        rootPromiseId: record.rootPromiseId,
        leafPromiseId: record.leafPromiseId,
        createdOn: record.createdOn,
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

    throw new Error(
      "task is already claimed, completed, or an invalid counter was provided",
    );
  }
}

function ikeyMatch(left?: string, right?: string): boolean {
  return left !== undefined && right !== undefined && left === right;
}
