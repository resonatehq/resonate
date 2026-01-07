import { LocalNetwork } from "../dev/network";
import type { CallbackRecord, DurablePromiseRecord, Network, TaskRecord } from "./network/network";

export class Promises {
  private network: Network;
  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  get(id: string): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send({ kind: "readPromise", id }, (res) => {
        if (res.kind === "error") {
          reject(res.error);
          return;
        }

        resolve(res.value.promise);
      });
    });
  }

  create(
    id: string,
    timeout: number,
    {
      ikey = undefined,
      strict = false,
      headers = undefined,
      data = undefined,
      tags = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      headers?: Record<string, string>;
      data?: string;
      tags?: Record<string, string>;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "createPromise",
          id: id,
          timeout: timeout,
          param: { headers, data },
          tags: tags,
          iKey: ikey,
          strict: strict,
        },
        (res) => {
          if (res.kind === "error") {
            reject(res.error);
            return;
          }

          resolve(res.value.promise);
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
      headers = undefined,
      data = undefined,
      tags = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      headers?: Record<string, string>;
      data?: string;
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
            param: { headers, data },
            tags: tags,
          },
          task: {
            processId: pid,
            ttl: ttl,
          },
          iKey: ikey,
          strict: strict,
        },
        (res) => {
          if (res.kind === "error") {
            reject(res.error);
            return;
          }

          resolve({ promise: res.value.promise, task: res.value.task });
        },
      );
    });
  }

  resolve(
    id: string,
    {
      ikey = undefined,
      strict = false,
      headers = undefined,
      data = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      headers?: Record<string, string>;
      data?: string;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "resolved",
          value: { headers, data },
          iKey: ikey,
          strict: strict,
        },
        (res) => {
          if (res.kind === "error") {
            reject(res.error);
            return;
          }

          resolve(res.value.promise);
        },
      );
    });
  }
  reject(
    id: string,
    {
      ikey = undefined,
      strict = false,
      headers = undefined,
      data = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      headers?: Record<string, string>;
      data?: string;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "rejected",
          value: { headers, data },
          iKey: ikey,
          strict: strict,
        },
        (res) => {
          if (res.kind === "error") {
            reject(res.error);
            return;
          }

          resolve(res.value.promise);
        },
      );
    });
  }
  cancel(
    id: string,
    {
      ikey = undefined,
      strict = false,
      headers = undefined,
      data = undefined,
    }: {
      ikey?: string;
      strict?: boolean;
      headers?: Record<string, string>;
      data?: string;
    } = {},
  ): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completePromise",
          id: id,
          state: "rejected_canceled",
          value: { headers, data },
          iKey: ikey,
          strict: strict,
        },
        (res) => {
          if (res.kind === "error") {
            reject(res.error);
            return;
          }

          resolve(res.value.promise);
        },
      );
    });
  }
  async *search(
    id: string,
    { state = undefined, limit = undefined }: { state?: "pending" | "resolved" | "rejected"; limit?: number } = {},
  ): AsyncGenerator<DurablePromiseRecord[], void> {
    let cursor: string | undefined;

    do {
      const res = await new Promise<{ promises: DurablePromiseRecord[]; cursor?: string }>((resolve, reject) => {
        this.network.send(
          {
            kind: "searchPromises",
            id,
            state,
            limit,
            cursor,
          },
          (res) => {
            if (res.kind === "error") return reject(res.error);
            resolve(res.value);
          },
        );
      });

      cursor = res.cursor;
      yield res.promises;
    } while (cursor !== null && cursor !== undefined);
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
        (res) => {
          if (res.kind === "error") {
            reject(res.error);
            return;
          }

          resolve({ promise: res.value.promise, callback: res.value.callback });
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
      this.network.send({ kind: "createSubscription", id, promiseId, timeout, recv }, (res) => {
        if (res.kind === "error") {
          reject(res.error);
          return;
        }

        resolve({ promise: res.value.promise, callback: res.value.callback });
      });
    });
  }
}
