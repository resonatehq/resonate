import { randomUUID } from "node:crypto";
import exceptions from "./exceptions.js";
import { LocalNetwork } from "./network/local.js";
import { isSuccess, type ScheduleRecord } from "./network/types.js";
import type { Send } from "./types.js";
import { VERSION } from "./util.js";

export class Schedules {
  private send: Send;

  constructor(send: Send = new LocalNetwork().send) {
    this.send = send;
  }

  async get(id: string): Promise<ScheduleRecord> {
    const res = await this.send({
      kind: "schedule.get",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        id,
      },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return res.data.schedule;
  }

  async create(
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    {
      promiseHeaders = {},
      promiseData = "",
      promiseTags = {},
    }: {
      description?: string;
      tags?: { [key: string]: string };
      promiseHeaders?: { [key: string]: string };
      promiseData?: string;
      promiseTags?: { [key: string]: string };
    } = {},
  ): Promise<ScheduleRecord> {
    const res = await this.send({
      kind: "schedule.create",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        id: id,
        cron: cron,
        promiseId: promiseId,
        promiseTimeout: promiseTimeout,
        promiseParam: { headers: promiseHeaders, data: promiseData },
        promiseTags: promiseTags,
      },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return res.data.schedule;
  }

  async delete(id: string): Promise<undefined> {
    const res = await this.send({
      kind: "schedule.delete",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        id,
      },
    });
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }
    return undefined;
  }
}
