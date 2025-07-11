import { LocalNetwork } from "./network/local";
import type { CallbackRecord, DurablePromiseRecord, Network, TaskRecord } from "./network/network";

import * as util from "./util";
export class Promises {
  private network: Network;
  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  get(id: string): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "readPromise", id: id }, (timeout, response) => {
        if (timeout) {
          util.assert(response.kind === "error");
          throw new Error("not implemented");
        }

        if (response.kind === "error") {
          util.assert(!timeout);
          reject(response.message);
        } else {
          if (response.kind !== "readPromise") {
            throw new Error("unexpected response");
          }
          resolve(response.promise);
        }
      });
    });
  }

  create(
    id: string,
    timeout: number,
    iKey: string | undefined = undefined,
    strict = false,
    param: any | undefined = undefined,
    tags: Record<string, string> | undefined = undefined,
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "createPromise",
          id: id,
          timeout: timeout,
          param: param,
          tags: tags,
          iKey: iKey,
          strict: strict,
        },
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "createPromise") {
              throw new Error("unexpected response");
            }
            resolve(response.promise);
          }
        },
      );
    });
  }

  createWithTask(
    id: string,
    timeout: number,
    processId: string,
    ttl: number,
    iKey: string | undefined = undefined,
    strict = false,
    param: any | undefined = undefined,
    tags: Record<string, string> | undefined = undefined,
  ): Promise<{ promise: DurablePromiseRecord; task?: TaskRecord }> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "createPromiseAndTask",
          promise: {
            id: id,
            timeout: timeout,
            param: param,
            tags: tags,
          },
          task: {
            processId: processId,
            ttl: ttl,
          },
          iKey: iKey,
          strict: strict,
        },
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "createPromiseAndTask") {
              throw new Error("unexpected response");
            }
            resolve(response);
          }
        },
      );
    });
  }

  resolve(
    id: string,
    iKey: string | undefined = undefined,
    strict = false,
    value: any | undefined = undefined,
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "resolved",
          value: value,
          iKey: iKey,
          strict: strict,
        },
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "completePromise") {
              throw new Error("unexpected response");
            }
            resolve(response.promise);
          }
        },
      );
    });
  }
  reject(
    id: string,
    iKey: string | undefined = undefined,
    strict = false,
    value: any | undefined = undefined,
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "rejected",
          value: value,
          iKey: iKey,
          strict: strict,
        },
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "completePromise") {
              throw new Error("unexpected response");
            }
            resolve(response.promise);
          }
        },
      );
    });
  }
  cancel(
    id: string,
    iKey: string | undefined = undefined,
    strict = false,
    value: any | undefined = undefined,
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "rejected_canceled",
          value: value,
          iKey: iKey,
          strict: strict,
        },
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "completePromise") {
              throw new Error("unexpected response");
            }
            resolve(response.promise);
          }
        },
      );
    });
  }

  callback(
    promiseId: string,
    rootPromiseId: string,
    recv: string,
    timeout: number,
  ): Promise<{
    promise: DurablePromiseRecord;
    callback: CallbackRecord | undefined;
  }> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "createCallback",
          id: promiseId,
          rootPromiseId: rootPromiseId,
          timeout: timeout,
          recv: recv,
        },
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "createCallback") {
              throw new Error("unexpected response");
            }
            resolve({ promise: response.promise, callback: response.callback });
          }
        },
      );
    });
  }

  subscribe(
    id: string,
    timeout: number,
    recv: string,
  ): Promise<{
    promise: DurablePromiseRecord;
    callback: CallbackRecord | undefined;
  }> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "createSubscription", id: id, timeout: timeout, recv: recv }, (timeout, response) => {
        if (timeout) {
          util.assert(response.kind === "error");
          throw new Error("not implemented");
        }

        if (response.kind === "error") {
          util.assert(!timeout);
          reject(response.message);
        } else {
          if (response.kind !== "createSubscription") {
            throw new Error("unexpected response");
          }
          resolve({ promise: response.promise, callback: response.callback });
        }
      });
    });
  }
}
