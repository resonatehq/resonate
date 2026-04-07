import { randomUUID } from "node:crypto";
import exceptions from "./exceptions.js";
import { LocalNetwork } from "./network/local.js";
import { isSuccess, type PromiseRecord, type TaskRecord } from "./network/types.js";
import type { Send } from "./types.js";
import { VERSION } from "./util.js";

export class Promises {
  private send: Send;
  constructor(send: Send = new LocalNetwork().send) {
    this.send = send;
  }

  async get(id: string): Promise<PromiseRecord> {
    const res = await this.send({
      kind: "promise.get",
      head: { corrId: randomUUID(), version: VERSION },
      data: { id },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return res.data.promise;
  }

  async create(
    id: string,
    timeoutAt: number,
    {
      headers = {},
      data = "",
      tags = {},
    }: {
      headers?: { [key: string]: string };
      data?: string;
      tags?: { [key: string]: string };
    } = {},
  ): Promise<PromiseRecord> {
    const res = await this.send({
      kind: "promise.create",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        id,
        timeoutAt,
        param: { headers, data },
        tags,
      },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return res.data.promise;
  }

  async createWithTask(
    id: string,
    timeoutAt: number,
    pid: string,
    ttl: number,
    {
      headers = {},
      data = "",
      tags = {},
    }: {
      headers?: { [key: string]: string };
      data?: string;
      tags?: { [key: string]: string };
    } = {},
  ): Promise<{ promise: PromiseRecord; task?: TaskRecord }> {
    const res = await this.send({
      kind: "task.create",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        pid,
        ttl,
        action: {
          kind: "promise.create",
          head: { corrId: randomUUID(), version: VERSION },
          data: { id, timeoutAt, param: { headers, data }, tags },
        },
      },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return { promise: res.data.promise, task: res.data.task };
  }

  async settle(
    id: string,
    state: "resolved" | "rejected" | "rejected_canceled",
    {
      headers = {},
      data = "",
    }: {
      headers?: { [key: string]: string };
      data?: string;
    } = {},
  ): Promise<PromiseRecord> {
    const res = await this.send({
      kind: "promise.settle",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        id,
        state,
        value: { headers, data },
      },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return res.data.promise;
  }

  async registerCallback(
    awaited: string,
    awaiter: string,
  ): Promise<{
    promise: PromiseRecord;
  }> {
    const res = await this.send({
      kind: "promise.register_callback",
      head: { corrId: randomUUID(), version: VERSION },
      data: { awaited, awaiter },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return { promise: res.data.promise };
  }

  async registerListener(
    awaited: string,
    address: string,
  ): Promise<{
    promise: PromiseRecord;
  }> {
    const res = await this.send({
      kind: "promise.register_listener",
      head: { corrId: randomUUID(), version: VERSION },
      data: { awaited, address },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return { promise: res.data.promise };
  }
}
