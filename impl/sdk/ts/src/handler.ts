import type { Encoder } from "./encoder";
import type { Encryptor } from "./encryptor";
import exceptions, { type ResonateError } from "./exceptions";
import type {
  CallbackRecord,
  ClaimTaskReq,
  CompletePromiseReq,
  CreateCallbackReq,
  CreatePromiseAndTaskReq,
  CreatePromiseReq,
  CreateSubscriptionReq,
  DurablePromiseRecord,
  Network,
  ReadPromiseReq,
  TaskRecord,
} from "./network/network";
import type { Result, Value } from "./types";
import * as util from "./util";

export class Cache {
  private promises: Map<string, DurablePromiseRecord<any>> = new Map();
  private callbacks: Map<string, CallbackRecord> = new Map();
  private tasks: Map<string, { id: string; counter: number }> = new Map();

  public getPromise(id: string): DurablePromiseRecord<any> | undefined {
    return this.promises.get(id);
  }

  public setPromise(promise: DurablePromiseRecord<any>): void {
    // do not set when promise is already completed
    if (this.promises.get(promise.id) !== undefined && this.promises.get(promise.id)?.state !== "pending") {
      return;
    }
    this.promises.set(promise.id, promise);
  }

  public getCallback(id: string): CallbackRecord | undefined {
    return this.callbacks.get(id);
  }

  public setCallback(id: string, callback: CallbackRecord): void {
    this.callbacks.set(id, callback);
  }

  public getTask(id: string): { id: string; counter: number } | undefined {
    return this.tasks.get(id);
  }

  public setTask(task: { id: string; counter: number }): void {
    // do not set when counter is greater
    if ((this.tasks.get(task.id)?.counter || 0) >= task.counter) {
      return;
    }
    this.tasks.set(task.id, task);
  }
}

export class Handler {
  private cache: Cache = new Cache();
  private network: Network;
  private encoder: Encoder;
  private encryptor: Encryptor;

  constructor(network: Network, encoder: Encoder, encryptor: Encryptor) {
    this.network = network;
    this.encoder = encoder;
    this.encryptor = encryptor;
  }

  public readPromise(req: ReadPromiseReq, done: (res: Result<DurablePromiseRecord<any>, ResonateError>) => void): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done({ kind: "value", value: promise });
      return;
    }

    this.network.send(req, (res) => {
      if (res.kind === "error") return done(res);

      let promise: DurablePromiseRecord<any>;
      try {
        promise = this.decode(res.value.promise);
      } catch (e) {
        return done({ kind: "error", error: e as ResonateError });
      }

      this.cache.setPromise(promise);
      done({ kind: "value", value: promise });
    });
  }

  public createPromise(
    req: CreatePromiseReq<any>,
    done: (res: Result<DurablePromiseRecord<any>, ResonateError>) => void,
    func = "unknown",
    headers: Record<string, string> = {},
    retryForever = false,
  ): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done({ kind: "value", value: promise });
      return;
    }

    let param: Value<string>;
    try {
      param = this.encryptor.encrypt(this.encoder.encode(req.param?.data));
    } catch (e) {
      done({ kind: "error", error: exceptions.ENCODING_ARGS_UNENCODEABLE(req.param?.data?.func ?? func, e) });
      return;
    }

    this.network.send(
      { ...req, param },
      (res) => {
        if (res.kind === "error") return done(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.value.promise, req.param?.data?.func ?? func);
        } catch (e) {
          return done({ kind: "error", error: e as ResonateError });
        }

        this.cache.setPromise(promise);
        done({ kind: "value", value: promise });
      },
      headers,
      retryForever,
    );
  }

  public createPromiseAndTask(
    req: CreatePromiseAndTaskReq<any>,
    done: (res: Result<{ promise: DurablePromiseRecord<any>; task?: TaskRecord }, ResonateError>) => void,
    func = "unknown",
    headers: Record<string, string> = {},
    retryForever = false,
  ) {
    const promise = this.cache.getPromise(req.promise.id);
    if (promise) {
      done({ kind: "value", value: { promise } });
      return;
    }

    let param: Value<string>;
    try {
      param = this.encryptor.encrypt(this.encoder.encode(req.promise.param?.data));
    } catch (e) {
      done({ kind: "error", error: exceptions.ENCODING_ARGS_UNENCODEABLE(req.promise.param?.data?.func ?? func, e) });
      return;
    }

    this.network.send(
      { ...req, promise: { ...req.promise, param } },
      (res) => {
        if (res.kind === "error") return done(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.value.promise, req.promise.param?.data?.func ?? func);
        } catch (e) {
          return done({ kind: "error", error: e as ResonateError });
        }

        this.cache.setPromise(promise);

        if (res.value.task) {
          this.cache.setTask(res.value.task);
        }

        done({ kind: "value", value: { promise, task: res.value.task } });
      },
      headers,
      retryForever,
    );
  }

  public completePromise(
    req: CompletePromiseReq<any>,
    done: (res: Result<DurablePromiseRecord<any>, ResonateError>) => void,
    func = "unknown",
  ): void {
    const promise = this.cache.getPromise(req.id);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done({ kind: "value", value: promise });
      return;
    }

    let value: Value<string>;
    try {
      value = this.encryptor.encrypt(this.encoder.encode(req.value?.data));
    } catch (e) {
      done({ kind: "error", error: exceptions.ENCODING_RETV_UNENCODEABLE(func, e) });
      return;
    }

    this.network.send({ ...req, value }, (res) => {
      if (res.kind === "error") return done(res);

      let promise: DurablePromiseRecord<any>;
      try {
        promise = this.decode(res.value.promise, func);
      } catch (e) {
        return done({ kind: "error", error: e as ResonateError });
      }

      this.cache.setPromise(promise);
      done({ kind: "value", value: promise });
    });
  }

  public claimTask(
    req: ClaimTaskReq,
    done: (res: Result<{ root: DurablePromiseRecord<any>; leaf?: DurablePromiseRecord<any> }, ResonateError>) => void,
  ): void {
    const task = this.cache.getTask(req.id);
    if (task && task.counter >= req.counter) {
      done({
        kind: "error",
        error: exceptions.ENCODING_RETV_UNDECODEABLE("The task counter is invalid", {
          code: 40307,
          message: "The task counter is invalid",
        }),
      });
      return;
    }

    this.network.send(req, (res) => {
      if (res.kind === "error") return done(res);
      util.assertDefined(res);
      util.assertDefined(res.value.message.promises.root);

      let rootPromise: DurablePromiseRecord<any>;
      let leafPromise: DurablePromiseRecord<any> | undefined;

      try {
        rootPromise = this.decode(res.value.message.promises.root.data);
        if (res.value.message.promises.leaf) {
          leafPromise = this.decode(res.value.message.promises.leaf.data);
        }
      } catch (e) {
        return done({ kind: "error", error: e as ResonateError });
      }

      this.cache.setPromise(rootPromise);
      if (leafPromise) {
        this.cache.setPromise(leafPromise);
      }

      this.cache.setTask({ id: req.id, counter: req.counter });

      done({ kind: "value", value: { root: rootPromise, leaf: leafPromise } });
    });
  }

  public createCallback(
    req: CreateCallbackReq,
    done: (
      res: Result<
        { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord<any> },
        ResonateError
      >,
    ) => void,
    headers: Record<string, string> = {},
  ): void {
    const id = `__resume:${req.rootPromiseId}:${req.promiseId}`;
    const promise = this.cache.getPromise(req.promiseId);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done({ kind: "value", value: { kind: "promise", promise } });
      return;
    }

    const callback = this.cache.getCallback(id);
    if (callback) {
      done({ kind: "value", value: { kind: "callback", callback } });
      return;
    }

    this.network.send(
      req,
      (res) => {
        if (res.kind === "error") return done(res);

        if (res.value.promise) {
          let promise: DurablePromiseRecord<any>;
          try {
            promise = this.decode(res.value.promise);
          } catch (e) {
            return done({ kind: "error", error: e as ResonateError });
          }

          this.cache.setPromise(promise);
        }

        if (res.value.callback) {
          this.cache.setCallback(id, res.value.callback);
        }

        done({
          kind: "value",
          value: res.value.callback
            ? { kind: "callback", callback: res.value.callback }
            : { kind: "promise", promise: res.value.promise },
        });
      },
      headers,
    );
  }

  public createSubscription(
    req: CreateSubscriptionReq,
    done: (res: Result<DurablePromiseRecord<any>, ResonateError>) => void,
    retryForever = false,
  ) {
    const id = `__notify:${req.promiseId}:${req.id}`;
    const promise = this.cache.getPromise(req.promiseId);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done({ kind: "value", value: promise });
      return;
    }

    const callback = this.cache.getCallback(id);
    if (callback) {
      done({ kind: "value", value: promise });
      return;
    }

    this.network.send(
      req,
      (res) => {
        if (res.kind === "error") return done(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.value.promise);
        } catch (e) {
          return done({ kind: "error", error: e as ResonateError });
        }

        this.cache.setPromise(promise);

        if (res.value.callback) {
          this.cache.setCallback(id, res.value.callback);
        }

        done({ kind: "value", value: promise });
      },
      {},
      retryForever,
    );
  }

  private decode(promise: DurablePromiseRecord, func = "unknown"): DurablePromiseRecord<any> {
    let paramData: any;
    let valueData: any;

    try {
      paramData = this.encoder.decode(this.encryptor.decrypt(promise.param));
    } catch (e) {
      throw exceptions.ENCODING_ARGS_UNDECODEABLE(func, e);
    }

    try {
      valueData = this.encoder.decode(this.encryptor.decrypt(promise.value));
    } catch (e) {
      throw exceptions.ENCODING_RETV_UNDECODEABLE(paramData?.func ?? func, e);
    }

    return {
      ...promise,
      param: { headers: promise.param?.headers, data: paramData },
      value: { headers: promise.value?.headers, data: valueData },
    };
  }
}
