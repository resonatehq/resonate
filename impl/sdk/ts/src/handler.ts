import type { DurablePromiseRecord, CreatePromiseRes, Network, CompletePromiseRes } from "./network/network";
import * as util from "./util";

interface DurablePromiseProto {
  id: string;
  timeout: number;
  tags: Record<string, string>;
  fn?: string;
  args?: any[];
}

interface DurablePromise<T> {
  id: string;
  state: "pending" | "resolved" | "rejected" | "rejected_canceled" | "rejected_timedout";
  value?: T;
}

export class Handler {
  private promises: Map<string, DurablePromise<any>>;
  private network: Network;

  constructor(network: Network, initialPromises?: DurablePromise<any>[]) {
    this.network = network;
    this.promises = new Map();
    for (const p of initialPromises ?? []) {
      this.promises.set(p.id, p);
    }
  }

  public createPromise<T>(
    { id, timeout, tags, fn, args }: DurablePromiseProto,
    callback: (res: DurablePromise<T>) => void,
  ): void {
    const promise = this.promises.get(id);
    if (promise) {
      callback(promise);
      return;
    }

    this.network.send(
      {
        kind: "createPromise",
        id,
        iKey: id,
        timeout,
        tags,
        param: {
          fn,
          args,
        },
        strict: false,
      },
      (timeout, response) => {
        if (timeout) {
          console.log("got a timeout nope out of here, what does it mean?");
          return;
        }

        util.assert(response.kind === "createPromise");
        const createPromiseRes = response as CreatePromiseRes;
        this.promises.set(createPromiseRes.promise.id, createPromiseRes.promise);
        callback(createPromiseRes.promise);
      },
    );
  }

  public resolvePromise<T>(id: string, value: T, callback: (res: DurablePromise<T>) => void): void {
    const promise = this.promises.get(id);
    util.assertDefined(promise);

    if (promise.state !== "pending") {
      callback(promise);
      return;
    }

    this.network.send(
      {
        kind: "completePromise",
        id,
        state: "resolved",
        value: value,
        iKey: id,
        strict: false,
      },
      (timeout, response) => {
        if (timeout) {
          console.log("got a timeout, nope out of here, what does it mean?");
        }
        util.assert(response.kind === "completePromise");
        const completePromiseRes = response as CompletePromiseRes;
        this.promises.set(completePromiseRes.promise.id, completePromiseRes.promise);
        callback(completePromiseRes.promise);
      },
    );
  }
}
