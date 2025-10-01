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

import type { Encoder } from "./encoder";
import type { Callback, Value } from "./types";

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

  public readPromise(req: ReadPromiseReq, done: Callback<DurablePromiseRecord<any>>): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done(false, promise);
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      let promise: DurablePromiseRecord<any>;
      try {
        promise = this.decode(res.promise);
      } catch {
        return done(true);
      }

      this.cache.setPromise(promise);
      done(false, promise);
    });
  }

  public createPromise(
    req: CreatePromiseReq<any>,
    done: Callback<DurablePromiseRecord<any>>,
    retryForever = false,
  ): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done(false, promise);
      return;
    }

    let param: Value<string>;
    try {
      param = this.encoder.encode(req.param?.data);
    } catch (e) {
      // TODO: log something useful
      done(true);
      return;
    }

    this.network.send(
      { ...req, param },
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise);
        } catch {
          return done(true);
        }

        this.cache.setPromise(promise);
        done(false, promise);
      },
      retryForever,
    );
  }

  public createPromiseAndTask(
    req: CreatePromiseAndTaskReq<any>,
    done: Callback<{ promise: DurablePromiseRecord<any>; task?: TaskRecord }>,
    retryForever = false,
  ) {
    const promise = this.cache.getPromise(req.promise.id);
    if (promise) {
      done(false, { promise });
      return;
    }

    let param: Value<string>;
    try {
      param = this.encoder.encode(req.promise.param?.data);
    } catch (e) {
      // TODO: log something useful
      done(true);
      return;
    }

    this.network.send(
      { ...req, promise: { ...req.promise, param } },
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise);
        } catch {
          return done(true);
        }

        this.cache.setPromise(promise);

        if (res.task) {
          this.cache.setTask(res.task);
        }

        done(false, { promise, task: res.task });
      },
      retryForever,
    );
  }

  public completePromise(req: CompletePromiseReq<any>, done: Callback<DurablePromiseRecord<any>>): void {
    const promise = this.cache.getPromise(req.id);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(false, promise);
      return;
    }

    let value: Value<string>;
    try {
      value = this.encoder.encode(req.value?.data);
    } catch (e) {
      // TODO: log something useful
      done(true);
      return;
    }

    this.network.send({ ...req, value }, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      let promise: DurablePromiseRecord<any>;
      try {
        promise = this.decode(res.promise);
      } catch {
        return done(true);
      }

      this.cache.setPromise(promise);
      done(false, promise);
    });
  }

  public claimTask(req: ClaimTaskReq, done: Callback<DurablePromiseRecord<any>>): void {
    const task = this.cache.getTask(req.id);
    if (task && task.counter >= req.counter) {
      done(true);
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
      } catch {
        return done(true);
      }

      this.cache.setPromise(rootPromise);
      if (leafPromise) {
        this.cache.setPromise(leafPromise);
      }

      this.cache.setTask({ id: req.id, counter: req.counter });

      done(false, rootPromise);
    });
  }

  public createCallback(
    req: CreateCallbackReq,
    done: Callback<
      { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord<any> }
    >,
  ): void {
    const id = `__resume:${req.rootPromiseId}:${req.promiseId}`;
    const promise = this.cache.getPromise(req.promiseId);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(false, { kind: "promise", promise });
      return;
    }

    const callback = this.cache.getCallback(id);
    if (callback) {
      done(false, { kind: "callback", callback });
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(true);
      util.assertDefined(res);

      if (res.promise) {
        let promise: DurablePromiseRecord<any>;
        try {
          promise = this.decode(res.promise);
        } catch {
          return done(true);
        }

        this.cache.setPromise(promise);
      }

      if (res.callback) {
        this.cache.setCallback(id, res.callback);
      }

      done(
        false,
        res.callback ? { kind: "callback", callback: res.callback } : { kind: "promise", promise: res.promise },
      );
    });
  }

  public createSubscription(
    req: CreateSubscriptionReq,
    done: Callback<DurablePromiseRecord<any>>,
    retryForever = false,
  ) {
    const id = `__notify:${req.promiseId}:${req.id}`;
    const promise = this.cache.getPromise(req.promiseId);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(false, promise);
      return;
    }

    const callback = this.cache.getCallback(id);
    if (callback) {
      done(false, promise);
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
        } catch {
          return done(true);
        }

        this.cache.setPromise(promise);

        if (res.callback) {
          this.cache.setCallback(id, res.callback);
        }

        done(false, promise);
      },
      retryForever,
    );
  }

  private decode(promise: DurablePromiseRecord): DurablePromiseRecord<any> {
    let paramData: any;
    let valueData: any;

    try {
      paramData = this.encoder.decode(promise.param);
    } catch (e) {
      // TODO: log something useful

      // biome-ignore lint/complexity/noUselessCatch: will log observation
      throw e;
    }

    try {
      valueData = this.encoder.decode(promise.value);
    } catch (e) {
      // TODO: log something useful

      // biome-ignore lint/complexity/noUselessCatch: will log observation
      throw e;
    }

    return {
      ...promise,
      param: { headers: promise.param?.headers, data: paramData },
      value: { headers: promise.value?.headers, data: valueData },
    };
  }
}
