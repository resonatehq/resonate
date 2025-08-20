import { LocalNetwork } from "../dev/network";
import type { CallbackRecord, DurablePromiseRecord, Network, TaskRecord } from "./network/network";

export class Promises {
  private network: Network;
  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  get(id: string): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "readPromise", id: id }, (err, res) => {
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
          id: promiseId,
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
    timeout: number,
    recv: string,
  ): Promise<{
    promise: DurablePromiseRecord;
    callback: CallbackRecord | undefined;
  }> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "createSubscription", id: id, timeout: timeout, recv: recv }, (err, res) => {
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
