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

import type { Callback } from "./types";

export class Cache {
  private promises: Map<string, DurablePromiseRecord> = new Map();
  private callbacks: Map<string, CallbackRecord> = new Map();
  private tasks: Map<string, { id: string; counter: number }> = new Map();

  public getPromise(id: string): DurablePromiseRecord | undefined {
    return this.promises.get(id);
  }

  public setPromise(promise: DurablePromiseRecord): void {
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

  constructor(network: Network, initialPromises?: DurablePromiseRecord[]) {
    this.network = network;

    for (const p of initialPromises ?? []) {
      this.cache.setPromise(p);
    }
  }

  public readPromise(req: ReadPromiseReq, done: Callback<DurablePromiseRecord>): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done(false, promise);
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      this.cache.setPromise(res.promise);
      done(false, res.promise);
    });
  }

  public createPromise(req: CreatePromiseReq, done: Callback<DurablePromiseRecord>, retryForever = false): void {
    const promise = this.cache.getPromise(req.id);
    if (promise) {
      done(false, promise);
      return;
    }

    this.network.send(
      req,
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        this.cache.setPromise(res.promise);
        done(false, res.promise);
      },
      retryForever,
    );
  }

  public createPromiseAndTask(
    req: CreatePromiseAndTaskReq,
    done: Callback<{ promise: DurablePromiseRecord; task?: TaskRecord }>,
    retryForever = false,
  ) {
    const promise = this.cache.getPromise(req.promise.id);
    if (promise) {
      done(false, { promise });
      return;
    }

    this.network.send(
      req,
      (err, res) => {
        if (err) return done(err);
        util.assertDefined(res);

        this.cache.setPromise(res.promise);

        if (res.task) {
          this.cache.setTask(res.task);
        }

        done(false, { promise: res.promise, task: res.task });
      },
      retryForever,
    );
  }

  public completePromise(req: CompletePromiseReq, done: Callback<DurablePromiseRecord>): void {
    const promise = this.cache.getPromise(req.id);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      done(false, promise);
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      this.cache.setPromise(res.promise);
      done(false, res.promise);
    });
  }

  public claimTask(req: ClaimTaskReq, done: Callback<DurablePromiseRecord>): void {
    const task = this.cache.getTask(req.id);
    if (task && task.counter >= req.counter) {
      done(true);
      return;
    }

    this.network.send(req, (err, res) => {
      if (err) return done(err);
      util.assertDefined(res);

      this.cache.setTask({ id: req.id, counter: req.counter });

      if (res.message.promises.root) {
        this.cache.setPromise(res.message.promises.root.data);
      }

      if (res.message.promises.leaf) {
        this.cache.setPromise(res.message.promises.leaf.data);
      }

      util.assertDefined(res.message.promises.root);
      done(false, res.message.promises.root.data);
    });
  }

  public createCallback(
    req: CreateCallbackReq,
    done: Callback<{ kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord }>,
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
        this.cache.setPromise(res.promise);
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

  public createSubscription(req: CreateSubscriptionReq, done: Callback<DurablePromiseRecord>, retryForever = false) {
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

        if (res.callback) {
          this.cache.setCallback(id, res.callback);
        }

        this.cache.setPromise(res.promise);
        done(false, res.promise);
      },
      retryForever,
    );
  }
}
