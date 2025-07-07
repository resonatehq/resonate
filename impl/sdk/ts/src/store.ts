import { Heapq } from "./heapq";
import * as util from "./util";
import {
  CompletePromiseReq,
  CompletePromiseRes,
  CreateCallbackReq,
  CreateCallbackRes,
  CreatePromiseReq,
  CreatePromiseRes,
  CreateSubscriptionReq,
  CreateSubscriptionRes,
  ReadPromiseReq,
  ReadPromiseRes,
  ClaimTaskReq,
  ClaimTaskRes,
  CompleteTaskReq,
  CompleteTaskRes,
  HeartbeatTasksRes,
  HeartbeatTasksReq,
} from "./network/network";

interface DurablePromiseData {
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
  callbacks?: Map<string, CallbackData>;
}

interface CallbackData {
  id: string;
  type: "resume" | "notify";
  promiseId: string;
  rootPromiseId: string;
  recv: string;
  timeout: number;
  createdOn: number;
}

interface TaskData {
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
  route(promise: DurablePromiseData): any;
}
class TagRouter implements Router {
  private tag: string;

  constructor(tag: string = "resonate:invoke") {
    this.tag = tag;
  }

  route(promise: DurablePromiseData): any {
    return promise.tags?.[this.tag];
  }
}

export class Server {
  private promises: PromiseStore;
  private tasks: TaskStore;
  private timeouts: Heapq<number>;
  private routers: Array<Router>;
  private targets: Record<string, string>;

  constructor() {
    this.promises = new PromiseStore();
    this.tasks = new TaskStore();
    this.timeouts = new Heapq();
    this.routers = new Array(new TagRouter());
    this.targets = { default: "local://any@defaul" };
  }

  next(): [() => void, number] {
    return [this.step, this.timeouts.top()];
  }

  step() {
    const time = Date.now();

    for (const promise of this.promises.scan()) {
      if (promise.state === "pending" && time >= promise.timeout) {
        var applied = this.transition(promise.id, "rejected_timedout")[2];
        util.assert(applied, "expected promise to be timedout");
      }
    }

    // transition expired tasks to init
    for (const task of this.tasks.scan()) {
      if (["enqueued", "claimed"].includes(task.state)) {
        if (task.expiry === undefined) {
          throw new Error("task must have an expiry");
        }
        util.assert(task.expiry !== undefined);

        if (time >= task.expiry) {
          var [_, applied] = this.tasks.transition(
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
          );
          util.assert(applied);
        }
      }

      //
    }
  }

  private transition(
    id: string,
    to: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout",
    strict?: boolean,
    timeout?: number,
    ikey?: string,
    value?: any,
    tags?: Record<string, string>,
  ): [DurablePromiseData, TaskData | undefined, boolean] {
    var [promise, applied] = this.promises.transition(id, to, strict, timeout, ikey, value, tags);

    if (applied && promise.state === "pending") {
      for (const router of this.routers) {
        const recv = router.route(promise);
        if (recv !== undefined) {
          var [task, applied] = this.tasks.transition(
            `__invoke:${id}`,
            "init",
            "invoke",
            this.targets[recv],
            promise.id,
            promise.id,
          );
          util.assert(applied);
          return [promise, task, applied];
        }
      }
    }

    if (
      applied &&
      ["resolved", "rejected", "rejected_canceled", "rejected_timedout"].includes(promise.state) &&
      promise.callbacks
    ) {
      for (const callback of promise.callbacks.values()) {
        var [task, applied] = this.tasks.transition(
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
    return [promise, undefined, applied];
  }

  process(req: CreatePromiseReq): CreatePromiseRes;
  process(req: ReadPromiseReq): ReadPromiseRes;
  process(req: CompletePromiseReq): CompletePromiseRes;
  process(req: CreateSubscriptionReq): CreateSubscriptionRes;
  process(req: CreateCallbackReq): CreateCallbackRes;
  process(req: ClaimTaskReq): ClaimTaskRes;
  process(req: CompleteTaskReq): CompleteTaskRes;
  process(req: HeartbeatTasksReq): HeartbeatTasksRes;
  process(
    req:
      | CreatePromiseReq
      | ReadPromiseReq
      | CompletePromiseReq
      | CreateSubscriptionReq
      | CreateCallbackReq
      | ClaimTaskReq
      | CompleteTaskReq
      | HeartbeatTasksReq,
  ):
    | CreatePromiseRes
    | ReadPromiseRes
    | CompletePromiseRes
    | CreateSubscriptionRes
    | CreateCallbackRes
    | ClaimTaskRes
    | CompleteTaskRes
    | HeartbeatTasksRes {
    switch (req.kind) {
      case "createPromise":
        var [promise, task, applied] = this.transition(
          req.id,
          "pending",
          req.strict,
          req.timeout,
          req.iKey,
          req.param,
          req.tags,
        );
        util.assert(!applied || ["pending", "rejected_timedout"].includes(promise.state));

        return {
          kind: "createPromise",
          promise: promise,
        };
      case "readPromise":
        return { kind: "readPromise", promise: this.promises.get(req.id) };
      case "completePromise":
        var [promise, _, applied] = this.transition(req.id, req.state, req.strict, undefined, req.iKey, req.value);
        util.assert(!applied || [req.state, "rejected_timedout"].includes(promise.state));
        return {
          kind: "completePromise",
          promise: promise,
        };
      case "createSubscription":
        var [promise, callback] = this.promises.subscribe(req.id, req.id, req.recv, req.timeout);

        return {
          kind: "createSubscription",
          promise: promise,
          callback: callback,
        };
      case "createCallback":
        var [promise, callback] = this.promises.callback(req.id, req.rootPromiseId, req.recv, req.timeout);

        return { kind: "createCallback", promise: promise, callback: callback };

      case "claimTask":
        [task, applied] = this.tasks.transition(
          req.id,
          "claimed",
          undefined,
          undefined,
          undefined,
          undefined,
          req.counter,
          req.processId,
          req.ttl,
        );

        util.assert(applied);

        switch (task.type) {
          case "invoke":
            var promise = this.promises.get(task.rootPromiseId);
            return {
              kind: "claimedtask",
              message: {
                kind: task.type,
                promises: {
                  root: { id: promise.id, data: promise },
                },
              },
            };
          case "resume":
            var rootPromise = this.promises.get(task.rootPromiseId);
            var learPromise = this.promises.get(task.leafPromiseId);

            return {
              kind: "claimedtask",
              message: {
                kind: task.type,
                promises: {
                  root: { id: rootPromise.id, data: rootPromise },
                  leaf: { id: learPromise.id, data: learPromise },
                },
              },
            };
        }
      case "completeTask":
        [task, applied] = this.tasks.transition(
          req.id,
          "completed",
          undefined,
          undefined,
          undefined,
          undefined,
          req.counter,
        );

        return {
          kind: "completedtask",
          task: {
            id: task.id,
            counter: task.counter,
            timeout: 0,
            processId: task.pid,
            createdOn: task.createdOn,
            completedOn: task.completedOn,
          },
        };
      case "heartbeatTasks":
        applied = false;
        var affectedTasks: number = 0;

        for (const task of this.tasks.scan()) {
          if (task.state !== "claimed" || task.pid !== req.processId) {
            continue;
          }

          applied = this.tasks.transition(
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
          )[1];

          util.assert(applied);
          affectedTasks += 1;
        }

        return { kind: "heartbeatTasks", tasksAffected: affectedTasks };
      default:
        throw new Error(`Network request not processed: ${(req as any).kind}`);
    }
  }
}

export class TaskStore {
  private tasks: Map<string, TaskData>;

  constructor() {
    this.tasks = new Map();
  }

  scan(): IterableIterator<TaskData> {
    return this.tasks.values();
  }

  transition(
    id: string,
    to: "init" | "enqueued" | "claimed" | "completed",
    type?: "invoke" | "resume" | "notify",
    recv?: string,
    rootPromiseId?: string,
    leafPromiseId?: string,
    counter?: number,
    pid?: string,
    ttl?: number,
    force: boolean = false,
  ): [TaskData, boolean] {
    const time = Date.now();
    var record = this.tasks.get(id);

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
      return [record, true];
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
      return [record, true];
    }

    if (record?.state === "init" && to === "claimed" && record.counter === counter) {
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
      return [record, true];
    }

    if (record?.state === "enqueued" && to === "claimed" && record.counter === counter) {
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
      return [record, true];
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
      return [record, true];
    }

    if (record !== undefined && ["enqueued", "claimed"].includes(record.state) && to === "init") {
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
      return [record, true];
    }

    if (record !== undefined && record.state === "claimed" && to === "claimed" && force) {
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
      return [record, true];
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
      return [record, true];
    }

    if (record !== undefined && ["init", "enqueued", "claimed"].includes(record.state) && to === "completed" && force) {
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
      return [record, true];
    }

    if (record?.state === "completed" && to === "completed") {
      return [record, false];
    }

    if (record === undefined) {
      throw new Error("Task not found");
    }

    throw new Error("task is already claimed, completed, or an invalid counter was provided");
  }
}
export class PromiseStore {
  private promises: Map<string, DurablePromiseData>;

  constructor() {
    this.promises = new Map();
  }

  scan(): IterableIterator<DurablePromiseData> {
    return this.promises.values();
  }

  get(id: string): DurablePromiseData {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    return record;
  }

  subscribe(
    id: string,
    promiseId: string,
    recv: string,
    timeout: number,
  ): [DurablePromiseData, CallbackData | undefined] {
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    const cbId = `__notify:${promiseId}:${id}`;

    if (record.state !== "pending" || record.callbacks?.has(cbId)) {
      return [record, undefined];
    }

    const callback: CallbackData = {
      id: cbId,
      type: "notify",
      promiseId: promiseId,
      rootPromiseId: promiseId,
      recv,
      timeout,
      createdOn: Date.now(),
    };

    if (!record.callbacks) {
      record.callbacks = new Map<string, CallbackData>();
    }

    // register and return
    record.callbacks.set(cbId, callback);
    return [record, callback];
  }

  callback(id: string, rootId: string, recv: string, timeout: number): [DurablePromiseData, CallbackData | undefined] {
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    if (record.state !== "pending" || record.callbacks?.has(id)) {
      return [record, undefined];
    }

    const callback: CallbackData = {
      id: `__resume:${rootId}:${id}`,
      type: "resume",
      promiseId: id,
      rootPromiseId: rootId,
      recv,
      timeout,
      createdOn: Date.now(),
    };

    if (!record.callbacks) {
      record.callbacks = new Map<string, CallbackData>();
    }

    record.callbacks.set(callback.id, callback);
    return [record, callback];
  }

  transition(
    id: string,
    to: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout",
    strict?: boolean,
    timeout?: number,
    ikey?: string,
    value?: any,
    tags?: Record<string, string>,
  ): [DurablePromiseData, boolean] {
    const time = Date.now();
    var record = this.promises.get(id);

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
      return [record, true];
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
      return [record, false];
    }

    if (
      record?.state === "pending" &&
      to === "pending" &&
      !strict &&
      time >= record.timeout &&
      ikeyMatch(record.iKeyForCreate, ikey)
    ) {
      return this.transition(id, "rejected_timedout");
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
      return [record, true];
    }

    if (
      record?.state === "pending" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict &&
      time >= record.timeout
    ) {
      return this.transition(id, "rejected_timedout");
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
      return [record, true];
    }

    if (
      record?.state !== undefined &&
      ["resolved", "rejected", "rejected_canceled", "rejected_timedout"].includes(record.state) &&
      to === "pending" &&
      !strict &&
      ikeyMatch(record.iKeyForCreate, ikey)
    ) {
      return [record, false];
    }

    if (
      record !== undefined &&
      ["resolved", "rejected", "rejected_canceled"].includes(record.state) &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict &&
      ikeyMatch(record.iKeyForComplete, ikey)
    ) {
      return [record, false];
    }

    if (
      record !== undefined &&
      record.state === "rejected_timedout" &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      !strict
    ) {
      return [record, false];
    }

    if (
      record !== undefined &&
      ["resolved", "rejected", "rejected_canceled"].includes(record.state) &&
      ["resolved", "rejected", "rejected_canceled"].includes(to) &&
      strict &&
      ikeyMatch(record.iKeyForComplete, ikey) &&
      record.state === to
    ) {
      return [record, false];
    }

    throw new Error("unexpected transition");
  }
}

function ikeyMatch(left?: string, right?: string): boolean {
  return left !== undefined && right !== undefined && left === right;
}
