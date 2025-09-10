import { LocalNetwork } from "../dev/network";
import type { CallbackRecord, DurablePromiseRecord, Network, TaskRecord } from "./network/network";

export class Promises {
  private network: Network;
  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  get(id: string): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "readPromise", id }, (err, res) => {
        if (err) {
          // TODO: reject with more information
          reject(Error("not implemented"));
          return;
        }

        resolve(res!.promise);
      });
    });
  }

  create(
    id: string,
    timeout: number,
    {
      ikey = undefined,
      strict = false,
      param = undefined,
      tags = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      param?: any;
      tags?: Record<string, string>;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "createPromise",
          id: id,
          timeout: timeout,
          param: param,
          tags: tags,
          iKey: ikey,
          strict: strict,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.promise);
        },
      );
    });
  }

  createWithTask(
    id: string,
    timeout: number,
    pid: string,
    ttl: number,
    {
      ikey = undefined,
      strict = false,
      param = undefined,
      tags = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      param?: any;
      tags?: Record<string, string>;
    } = {},
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
            processId: pid,
            ttl: ttl,
          },
          iKey: ikey,
          strict: strict,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve({ promise: res!.promise, task: res!.task });
        },
      );
    });
  }

  resolve(
    id: string,
    {
      ikey = undefined,
      strict = false,
      value = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      value?: any;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "resolved",
          value: value,
          iKey: ikey,
          strict: strict,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.promise);
        },
      );
    });
  }
  reject(
    id: string,
    {
      ikey = undefined,
      strict = false,
      value = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      value?: any;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "rejected",
          value: value,
          iKey: ikey,
          strict: strict,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.promise);
        },
      );
    });
  }

  cancel(
    id: string,
    {
      ikey = undefined,
      strict = false,
      value = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      value?: any;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "rejected_canceled",
          value: value,
          iKey: ikey,
          strict: strict,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.promise);
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
          promiseId: promiseId,
          rootPromiseId: rootPromiseId,
          timeout: timeout,
          recv: recv,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve({ promise: res!.promise, callback: res!.callback });
        },
      );
    });
  }

  subscribe(
    id: string,
    promiseId: string,
    timeout: number,
    recv: string,
  ): Promise<{
    promise: DurablePromiseRecord;
    callback: CallbackRecord | undefined;
  }> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "createSubscription", id, promiseId, timeout, recv }, (err, res) => {
        if (err) {
          // TODO: reject with more information
          reject(Error("not implemented"));
          return;
        }

        resolve({ promise: res!.promise, callback: res!.callback });
      });
    });
  }
}
