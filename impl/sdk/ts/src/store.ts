import {
  CallbackRecord,
  CompletePromiseReq,
  CompletePromiseRes,
  CreatePromiseReq,
  CreatePromiseRes,
  DurablePromiseRecord,
  ReadPromiseReq,
  ReadPromiseRes,
} from "./network/network";

export class Server {
  private promises: PromiseStore;

  constructor() {
    this.promises = new PromiseStore(this);
  }

  process(
    req: CreatePromiseReq | ReadPromiseReq | CompletePromiseReq,
  ): CreatePromiseRes | ReadPromiseRes | CompletePromiseRes {
    if (req.kind === "createPromise") {
      const resp = this.promises.create(
        req.id,
        req.timeout,
        req.iKey,
        req.strict ?? false,
        req.param,
        req.tags,
      );

      return { kind: "createPromise", promise: resp };
    } else if (req.kind === "readPromise") {
      const resp = this.promises.get(req.id);

      return { kind: "readPromise", promise: resp };
    } else if (req.kind === "completePromise") {
      const resp = this.promises.complete(
        req.id,
        req.iKey,
        req.strict ?? false,
        req.value,
        req.state,
      );

      return { kind: "completePromise", promise: resp };
    } else {
      throw new Error(`Unhandled kind`);
    }
  }
}

export class PromiseStore {
  private promises: Map<string, DurablePromiseRecord>;
  private store: Server;

  constructor(store: Server) {
    this.promises = new Map();
    this.store = store;
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
  ): DurablePromiseRecord {
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
      return record;
    }

    record = this.timeout(record);

    if (strict && !(record.state === "pending")) {
      throw new Error("Durable promise previously created");
    }
    if (record.iKeyForCreate === undefined || ikey !== record.iKeyForCreate) {
      throw new Error("missing idempotency key for create");
    }

    return record;
  }

  complete(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    value: any | undefined,
    to: "resolved" | "rejected" | "rejected_canceled",
  ): DurablePromiseRecord {
    var record = this.promises.get(id);

    if (!record) {
      throw new Error("not found");
    }

    record = this.timeout(record);

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
      return record;
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

    return record;
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

  private timeout(record: DurablePromiseRecord): DurablePromiseRecord {
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
      return record;
    }
    return record;
  }
}
