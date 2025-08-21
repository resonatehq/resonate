import type { CallbackRecord, CreatePromiseReq, DurablePromiseRecord, Network } from "./network/network";
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

  public createPromise(createReq: CreatePromiseReq, callback: Callback<DurablePromiseRecord>): void {
    const promise = this.promises.get(createReq.id);
    if (promise) {
      callback(false, promise);
      return;
    }

    this.network.send(createReq, (err, res) => {
      if (err) return callback(err);
      util.assertDefined(res);

      this.promises.set(res.promise.id, res.promise);
      callback(false, res.promise);
    });
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
        if (err) return callback(err);
        util.assertDefined(res);

        this.promises.set(res.promise.id, res.promise);
        callback(false, res.promise);
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
        if (err) return callback(true);
        util.assertDefined(res);

        if (res.promise) {
          this.promises.set(res.promise.id, res.promise);
        }

        callback(
          false,
          res.callback ? { kind: "callback", callback: res.callback } : { kind: "promise", promise: res.promise },
        );
      },
    );
  }
}
