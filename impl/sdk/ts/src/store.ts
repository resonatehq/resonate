import { Heapq } from "./heapq";
import * as util from "./util";
import {
  CallbackRecord,
  CompletePromiseReq,
  CompletePromiseRes,
  CreateCallbackReq,
  CreateCallbackRes,
  CreatePromiseReq,
  CreatePromiseRes,
  CreateSubscriptionReq,
  CreateSubscriptionRes,
  DurablePromiseRecord,
  ReadPromiseReq,
  ReadPromiseRes,
  TaskRecord,
} from "./network/network";

interface Router {
  route(promise: DurablePromiseRecord): any;
}
class TagRouter implements Router {
  private tag: string;

  constructor(tag: string = "resonate:invoke") {
    this.tag = tag;
  }

  route(promise: DurablePromiseRecord): any {
    return promise.tags?.[this.tag];
  }
}

export class Server {
  private promises: PromiseStore;
  private timeouts: Heapq<number>;
  private routers: Array<Router>;

  constructor() {
    this.promises = new PromiseStore();
    this.timeouts = new Heapq();
    this.routers = new Array(new TagRouter());
  }

  next(): [() => void, number] {
    return [this.step, this.timeouts.top()];
  }

  step() {
    const now = Date.now();

    for (const promise of this.promises.scan()) {
      if (promise.state === "pending" && now >= promise.timeout) {
        var applied = this.promises.expire(promise.id);
        util.assert(applied, "expected promise to be timedout");
      }
    }
  }

  process(
    req:
      | CreatePromiseReq
      | ReadPromiseReq
      | CompletePromiseReq
      | CreateSubscriptionReq
      | CreateCallbackReq,
  ):
    | CreatePromiseRes
    | ReadPromiseRes
    | CompletePromiseRes
    | CreateSubscriptionRes
    | CreateCallbackRes {
    switch (req.kind) {
      case "createPromise":
        this.timeouts.push(req.timeout);
        var [promise, _] = this.promises.create(
          req.id,
          req.timeout,
          req.iKey,
          req.strict ?? false,
          req.param,
          req.tags,
        );

        return {
          kind: "createPromise",
          promise: promise,
        };
      case "readPromise":
        return { kind: "readPromise", promise: this.promises.get(req.id) };
      case "completePromise":
        var [promise, _] = this.promises.complete(
          req.id,
          req.iKey,
          req.strict ?? false,
          req.value,
          req.state,
        );
        return {
          kind: "completePromise",
          promise: promise,
        };
      case "createSubscription":
        var [promise, callback] = this.promises.subscribe(
          req.id,
          req.id,
          req.recv,
          req.timeout,
        );

        return {
          kind: "createSubscription",
          promise: promise,
          callback: callback,
        };
      case "createCallback":
        var [promise, callback] = this.promises.callback(
          req.id,
          req.rootPromiseId,
          req.recv,
          req.timeout,
        );

        return { kind: "createCallback", promise: promise, callback: callback };
      default:
        throw new Error(`Network request not processed: ${(req as any).kind}`);
    }
  }
}

export class TaskStore {
  private tasks: Map<string, TaskRecord>;

  constructor() {
    this.tasks = new Map();
  }

  transition(
    id: string,
    to: "init" | "enqueued" | "claimed" | "completed",
    type: "invoke" | "resume" | "notify" | undefined,
    recv: string | undefined,
    rootPromiseId: string | undefined,
    leafPromiseId: string | undefined,
    counter: number | undefined,
    pid: string | undefined,
    ttl: number | undefined,
    force: boolean = false,
  ): [TaskRecord, boolean] {
    const time = Date.now();
    var record = this.tasks.get(id);

    throw new Error("hos");
  }
}
export class PromiseStore {
  private promises: Map<string, DurablePromiseRecord>;

  constructor() {
    this.promises = new Map();
  }

  scan(): IterableIterator<DurablePromiseRecord> {
    return this.promises.values();
  }

  get(id: string): DurablePromiseRecord {
    const record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    return record;
  }

  create(
    id: string,
    timeout: number,
    ikey: string | undefined,
    strict: boolean,
    param: any | undefined,
    tags: Record<string, string> | undefined,
  ): [DurablePromiseRecord, boolean] {
    var applied: boolean;
    var record = this.promises.get(id);

    if (!record) {
      record = {
        id: id,
        state: "pending",
        timeout: timeout,
        param: param,
        value: undefined,
        tags: tags ?? {},
        iKeyForCreate: ikey,
        createdOn: Date.now(),
      };
      this.promises.set(id, record);
      return [record, true];
    }

    [record, applied] = this.timeout(record);

    if (strict && !(record.state === "pending")) {
      throw new Error("Durable promise previously created");
    }
    if (record.iKeyForCreate === undefined || ikey !== record.iKeyForCreate) {
      throw new Error("missing idempotency key for create");
    }

    return [record, applied];
  }

  complete(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    value: any | undefined,
    to: "resolved" | "rejected" | "rejected_canceled",
  ): [DurablePromiseRecord, boolean] {
    var applied: boolean;
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    [record, applied] = this.timeout(record);

    if (record.state === "pending") {
      record = {
        id: id,
        state: to,
        timeout: record.timeout,
        param: record.param,
        value: value,
        createdOn: record.createdOn,
        completedOn: Date.now(),
        iKeyForCreate: record.iKeyForCreate,
        iKeyForComplete: ikey,
        tags: record.tags,
      };
      this.promises.set(id, record);
      return [record, true];
    }

    if (strict && !(record.state === to)) {
      throw new Error("forbidden");
    }

    if (
      !(record.state === "rejected_timedout") &&
      (record.iKeyForComplete === undefined || ikey !== record.iKeyForComplete)
    ) {
      throw new Error("forbidden");
    }

    return [record, applied];
  }

  subscribe(
    id: string,
    promiseId: string,
    recv: string,
    timeout: number,
  ): [DurablePromiseRecord, CallbackRecord | undefined] {
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    const cbId = `__notify:${promiseId}:${id}`;

    if (record.state !== "pending" || record.callbacks?.has(cbId)) {
      return [record, undefined];
    }

    const callback: CallbackRecord = {
      id: cbId,
      type: "notify",
      promiseId: promiseId,
      rootPromiseId: promiseId,
      recv,
      timeout,
      createdOn: Date.now(),
    };

    if (!record.callbacks) {
      record.callbacks = new Map<string, CallbackRecord>();
    }

    // register and return
    record.callbacks.set(cbId, callback);
    return [record, callback];
  }

  callback(
    id: string,
    rootId: string,
    recv: string,
    timeout: number,
  ): [DurablePromiseRecord, CallbackRecord | undefined] {
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    if (record.state !== "pending" || record.callbacks?.has(id)) {
      return [record, undefined];
    }

    const callback: CallbackRecord = {
      id: `__resume:${rootId}:${id}`,
      type: "resume",
      promiseId: id,
      rootPromiseId: rootId,
      recv,
      timeout,
      createdOn: Date.now(),
    };

    if (!record.callbacks) {
      record.callbacks = new Map<string, CallbackRecord>();
    }

    record.callbacks.set(callback.id, callback);
    return [record, callback];
  }

  expire(id: string): boolean {
    var applied: boolean;
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }
    [record, applied] = this.timeout(record);
    if (record.state === "pending") {
      throw new Error("unexpected state");
    }
    return applied;
  }

  private timeout(
    record: DurablePromiseRecord,
  ): [DurablePromiseRecord, boolean] {
    if (record.state === "pending" && Date.now() >= record.timeout) {
      record = {
        id: record.id,
        state:
          record.tags?.["resonate:timeout"] === "true"
            ? "resolved"
            : "rejected_timedout",
        timeout: record.timeout,
        param: record.param,
        value: undefined,
        createdOn: record.createdOn,
        completedOn: record.timeout,
        iKeyForCreate: record.iKeyForCreate,
        iKeyForComplete: undefined,
        tags: record.tags,
      };

      this.promises.set(record.id, record);
      return [record, true];
    }
    return [record, false];
  }
}
