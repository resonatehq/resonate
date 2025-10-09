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
import * as util from "./util";

import exceptions, { type ResonateError } from "./exceptions";

import type { Encoder } from "./encoder";
import type { Value } from "./types";

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

  constructor(network: Network, encoder: Encoder) {
    this.network = network;
    this.encoder = encoder;
  }

  public readPromise(req: ReadPromiseReq, done: (err?: ResonateError, res?: DurablePromiseRecord<any>) => void): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done(undefined, promise);
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      let promise: DurablePromiseRecord<any>;
      try {
        promise = this.decode(res.promise);
      } catch (e) {
        return done(e as ResonateError);
      }

      this.cache.setPromise(promise);
      done(undefined, promise);
    });
  }

  public createPromise(
    req: CreatePromiseReq<any>,
    done: (err?: ResonateError, res?: DurablePromiseRecord<any>) => void,
    func = "unknown",
    retryForever = false,
  ): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done(undefined, promise);
      return;
    }

    let param: Value<string>;
    try {
      param = this.encoder.encode(req.param?.data);
    } catch (e) {
      done(exceptions.ENCODING_ARGS_UNENCODEABLE(req.param?.data?.func ?? func, e));
      return;
    }

    this.network.send(
      { ...req, param },
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise, req.param?.data?.func ?? func);
        } catch (e) {
          return done(e as ResonateError);
        }

        this.cache.setPromise(promise);
        done(undefined, promise);
      },
      retryForever,
    );
  }

  public createPromiseAndTask(
    req: CreatePromiseAndTaskReq<any>,
    done: (err?: ResonateError, res?: { promise: DurablePromiseRecord<any>; task?: TaskRecord }) => void,
    func = "unknown",
    retryForever = false,
  ) {
    const promise = this.cache.getPromise(req.promise.id);
    if (promise) {
      done(undefined, { promise });
      return;
    }

    let param: Value<string>;
    try {
      param = this.encoder.encode(req.promise.param?.data);
    } catch (e) {
      done(exceptions.ENCODING_ARGS_UNENCODEABLE(req.promise.param?.data?.func ?? func, e));
      return;
    }

    this.network.send(
      { ...req, promise: { ...req.promise, param } },
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise, req.promise.param?.data?.func ?? func);
        } catch (e) {
          return done(e as ResonateError);
        }

        this.cache.setPromise(promise);

        if (res.task) {
          this.cache.setTask(res.task);
        }

        done(undefined, { promise, task: res.task });
      },
      retryForever,
    );
  }

  public completePromise(
    req: CompletePromiseReq<any>,
    done: (err?: ResonateError, res?: DurablePromiseRecord<any>) => void,
    func = "unknown",
  ): void {
    const promise = this.cache.getPromise(req.id);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(undefined, promise);
      return;
    }

    let value: Value<string>;
    try {
      value = this.encoder.encode(req.value?.data);
    } catch (e) {
      done(exceptions.ENCODING_RETV_UNENCODEABLE(func, e));
      return;
    }

    this.network.send({ ...req, value }, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      let promise: DurablePromiseRecord<any>;
      try {
        promise = this.decode(res.promise, func);
      } catch (e) {
        return done(e as ResonateError);
      }

      this.cache.setPromise(promise);
      done(undefined, promise);
    });
  }

  public claimTask(req: ClaimTaskReq, done: (err?: ResonateError, res?: DurablePromiseRecord<any>) => void): void {
    const task = this.cache.getTask(req.id);
    if (task && task.counter >= req.counter) {
      done(
        exceptions.ENCODING_RETV_UNDECODEABLE("The task counter is invalid", {
          code: 40307,
          message: "The task counter is invalid",
        }),
      );
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);
      util.assertDefined(res.message.promises.root);

      let rootPromise: DurablePromiseRecord<any>;
      let leafPromise: DurablePromiseRecord<any> | undefined;

      try {
        rootPromise = this.decode(res.message.promises.root.data);
        if (res.message.promises.leaf) {
          leafPromise = this.decode(res.message.promises.leaf.data);
        }
      } catch (e) {
        return done(e as ResonateError);
      }

      this.cache.setPromise(rootPromise);
      if (leafPromise) {
        this.cache.setPromise(leafPromise);
      }

      this.cache.setTask({ id: req.id, counter: req.counter });

      done(undefined, rootPromise);
    });
  }

  public createCallback(
    req: CreateCallbackReq,
    done: (
      err?: ResonateError,
      res?: { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord<any> },
    ) => void,
  ): void {
    const id = `__resume:${req.rootPromiseId}:${req.promiseId}`;
    const promise = this.cache.getPromise(req.promiseId);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(undefined, { kind: "promise", promise });
      return;
    }

    const callback = this.cache.getCallback(id);
    if (callback) {
      done(undefined, { kind: "callback", callback });
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      if (res.promise) {
        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise);
        } catch (e) {
          return done(e as ResonateError);
        }

        this.cache.setPromise(promise);
      }

      if (res.callback) {
        this.cache.setCallback(id, res.callback);
      }

      done(
        undefined,
        res.callback ? { kind: "callback", callback: res.callback } : { kind: "promise", promise: res.promise },
      );
    });
  }

  public createSubscription(
    req: CreateSubscriptionReq,
    done: (err?: ResonateError, res?: DurablePromiseRecord<any>) => void,
    retryForever = false,
  ) {
    const id = `__notify:${req.promiseId}:${req.id}`;
    const promise = this.cache.getPromise(req.promiseId);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(undefined, promise);
      return;
    }

    const callback = this.cache.getCallback(id);
    if (callback) {
      done(undefined, promise);
      return;
    }

    this.network.send(
      req,
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise);
        } catch (e) {
          return done(e as ResonateError);
        }

        this.cache.setPromise(promise);

        if (res.callback) {
          this.cache.setCallback(id, res.callback);
        }

        done(undefined, promise);
      },
      retryForever,
    );
  }

  private decode(promise: DurablePromiseRecord, func = "unknown"): DurablePromiseRecord<any> {
    let paramData: any;
    let valueData: any;

    try {
      paramData = this.encoder.decode(promise.param);
    } catch (e) {
      throw exceptions.ENCODING_ARGS_UNDECODEABLE(func, e);
    }

    try {
      valueData = this.encoder.decode(promise.value);
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
