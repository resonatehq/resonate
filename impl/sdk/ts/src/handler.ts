import type {
  CallbackRecord,
  CompletePromiseRes,
  CreateCallbackRes,
  CreatePromiseRes,
  DurablePromiseRecord,
  Network,
} from "./network/network";
import type { Result } from "./types";
import * as util from "./util";

import type { Callback } from "./types";

export class Handler {
  private network: Network;
  private promises: Map<string, DurablePromiseRecord>;

  constructor(network: Network, initialPromises?: DurablePromiseRecord[]) {
    this.network = network;
    this.promises = new Map();
    for (const p of initialPromises ?? []) {
      this.promises.set(p.id, p);
    }
  }

  public updateCache(durablePromise: DurablePromiseRecord) {
    this.promises.set(durablePromise.id, durablePromise);
  }

  public createPromise(
    id: string,
    timeout: number,
    param: any,
    tags: Record<string, string>,
    callback: Callback<DurablePromiseRecord>,
  ): void {
    const promise = this.promises.get(id);
    if (promise) {
      callback(false, promise);
      return;
    }

    this.network.send(
      {
        kind: "createPromise",
        id,
        iKey: id,
        timeout,
        param,
        tags,
        strict: false,
      },
      (err, res) => {
        if (err) {
          return callback(err);
        }

        util.assert(res.kind === "createPromise", "response must be kind createPromise");
        const { promise } = res as CreatePromiseRes;
        this.promises.set(promise.id, promise);
        callback(false, promise);
      },
    );
  }

  public completePromise(id: string, result: Result<any>, callback: Callback<DurablePromiseRecord>): void {
    const promise = this.promises.get(id);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      callback(false, promise);
      return;
    }

    const state = result.success ? "resolved" : "rejected";
    const value = result.success ? result.data : result.error;

    this.network.send(
      {
        kind: "completePromise",
        id,
        iKey: id,
        state,
        value,
        strict: false,
      },
      (err, res) => {
        if (err) {
          return callback(err);
        }

        util.assert(res.kind === "completePromise", "response must be completePromise");
        const { promise } = res as CompletePromiseRes;
        this.promises.set(promise.id, promise);
        callback(false, promise);
      },
    );
  }

  public createCallback(
    id: string,
    rootPromiseId: string,
    timeout: number,
    recv: string,
    callback: Callback<
      { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord }
    >,
  ): void {
    this.network.send(
      {
        kind: "createCallback",
        id,
        rootPromiseId,
        timeout,
        recv,
      },
      (err, res) => {
        if (err) {
          return callback(true);
        }

        util.assert(res.kind === "createCallback", "response must be createCallback");
        const { callback: cb, promise } = res as CreateCallbackRes;

        if (promise) {
          this.promises.set(promise.id, promise);
        }

        callback(false, cb ? { kind: "callback", callback: cb } : { kind: "promise", promise });
      },
    );
  }
}
