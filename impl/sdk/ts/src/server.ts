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
    this.targets = { default: "local://any@default" };
  }

  next(time: number = Date.now()): number {
    let timeout: number | undefined = undefined;
    for (const promise of this.promises.values()) {
      if (promise.state === "pending") {
        if (timeout === undefined) {
          timeout = promise.timeout;
        } else {
          timeout = Math.min(promise.timeout, timeout);
        }
      }
    }

    for (const task of this.tasks.values()) {
      if (["init", "enqueued", "claimed"].includes(task.state)) {
        if (task.expiry === undefined) {
          throw new Error("unexpected path");
        }

        if (timeout === undefined) {
          timeout = task.expiry;
        } else {
          timeout = Math.min(task.expiry, timeout);
        }
      }
    }

    if (timeout !== undefined) {
      timeout = Math.max(0, timeout - time);
    }

    if (timeout === undefined) {
      throw new Error("timeout should have been set by this point");
    }

    return timeout;
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
    for (const promise of this.promises.values()) {
      if (promise.state === "pending" && time >= promise.timeout) {
        let applied = this.transitionPromise(
          promise.id,
          "rejected_timedout",
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          time,
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

        if (time >= task.expiry) {
          const applied = this.transitionTask(
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
          ).applied;
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
        var applied: boolean;
        if (task.type === "notify") {
          applied = this.transitionTask(
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
          ).applied;
        } else {
          applied = this.transitionTask(
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
          ).applied;
        }
        util.assert(applied);
      } else {
        // TODO(dfarr): implement this
        // _, applied = self.tasks.transition(task.id, to="INIT", expiry=0)
        // assert applied
      }
    }
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
    const { promise, task, applied } = this.transitionPromise(
      id,
      "pending",
      strict,
      timeout,
      iKey,
      param,
      tags,
      time,
    );
    util.assert(
      !applied || ["pending", "rejected_timedout"].includes(promise.state),
    );

    return promise;
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
    const { promise, task, applied } = this.transitionPromise(
      id,
      state,
      strict,
      undefined,
      iKey,
      value,
      undefined,
      time,
    );
    util.assert(
      !applied || [state, "rejected_timedout"].includes(promise.state),
    );
    return promise;
  }

  createSubscription(
    id: string,
    timeout: number,
    recv: string,
    time: number = Date.now(),
  ): { promise: DurablePromiseRecord; callback: CallbackRecord | undefined } {
    const { promise, callback } = this.subscribeToPromise(
      id,
      id,
      recv,
      timeout,
      time,
    );

    return {
      promise: promise,
      callback: callback,
    };
  }

  createCallback(
    id: string,
    rootPromiseId: string,
    timeout: number,
    recv: string,
    time: number = Date.now(),
  ): { promise: DurablePromiseRecord; callback: CallbackRecord | undefined } {
    const { promise, callback } = this.callbackToPromise(
      id,
      rootPromiseId,
      recv,
      timeout,
      time,
    );

    return { promise: promise, callback: callback };
  }

  claimTask(
    id: string,
    counter: number,
    processId: string,
    ttl: number,
    time: number = Date.now(),
  ): Mesg {
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

  completeTask(
    id: string,
    counter: number,
    time: number = Date.now(),
  ): TaskRecord {
    const { task, applied } = this.transitionTask(
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
    let applied = false;
    let affectedTasks = 0;

    for (const task of this.tasks.values()) {
      if (task.state !== "claimed" || task.pid !== processId) {
        continue;
      }

      applied = this.transitionTask(
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
    id: string,
    promiseId: string,
    recv: string,
    timeout: number,
    time: number = Date.now(),
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
    id: string,
    rootId: string,
    recv: string,
    timeout: number,
    time: number = Date.now(),
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
    time: number = Date.now(),
  ): { promise: DurablePromise; task?: Task; applied: boolean } {
    const { promise, applied } = this._transitionPromise(
      id,
      to,
      strict,
      timeout,
      ikey,
      value,
      tags,
      time,
    );

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

    if (
      applied &&
      [
        "resolved",
        "rejected",
        "rejected_canceled",
        "rejected_timedout",
      ].includes(promise.state)
    ) {
      if (promise.callbacks) {
        for (const callback of promise.callbacks.values()) {
          const { task, applied } = this.transitionTask(
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

      for (const task of this.tasks.values()) {
        if (
          task.leafPromiseId === id &&
          ["init", "enqueued", "claimed"].includes(task.state) &&
          ["invoke", "resume"].includes(task.type)
        ) {
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
    }

    return { promise: promise, applied: applied };
  }

  private _transitionPromise(
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
    time: number = Date.now(),
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
        expiry: time + 5000,
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
      if (ttl === undefined || pid === undefined) {
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
      if (ttl === undefined || pid === undefined) {
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
