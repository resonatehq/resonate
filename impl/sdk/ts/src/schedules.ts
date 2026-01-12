import { LocalNetwork } from "../dev/network";
import type { Network, ScheduleRecord } from "./network/network";

export class Schedules {
  private network: Network;

  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  get(id: string): Promise<ScheduleRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "readSchedule",
          id: id,
        },
        (res) => {
          if (res.kind === "error") {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res.value.schedule);
        },
      );
    });
  }

  create(
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    {
      description = undefined,
      tags = undefined,
      promiseHeaders = undefined,
      promiseData = undefined,
      promiseTags = undefined,
    }: {
      description?: string;
      tags?: Record<string, string>;
      promiseHeaders?: Record<string, string>;
      promiseData?: string;
      promiseTags?: Record<string, string>;
    } = {},
  ): Promise<ScheduleRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "createSchedule",
          id: id,
          description: description,
          cron: cron,
          tags: tags,
          promiseId: promiseId,
          promiseTimeout: promiseTimeout,
          promiseParam: { headers: promiseHeaders, data: promiseData },
          promiseTags: promiseTags,
        },
        (res) => {
          if (res.kind === "error") {
            console.log(res.error);
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res.value.schedule);
        },
      );
    });
  }

  delete(id: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "deleteSchedule",
          id: id,
        },
        (res) => {
          if (res.kind === "error") {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve();
        },
      );
    });
  }

  async *search(id: string, { limit = undefined }: { limit?: number } = {}): AsyncGenerator<ScheduleRecord[], void> {
    let cursor: string | undefined;

    do {
      const res = await new Promise<{ schedules: ScheduleRecord[]; cursor?: string }>((resolve, reject) => {
        this.network.send(
          {
            kind: "searchSchedules",
            id,
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
      yield res.schedules;
    } while (cursor !== null && cursor !== undefined);
  }
}
