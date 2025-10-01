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
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.schedule);
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
      ikey = undefined,
      description = undefined,
      tags = undefined,
      promiseHeaders = undefined,
      promiseData = undefined,
      promiseTags = undefined,
    }: {
      ikey?: string;
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
          iKey: ikey,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.schedule);
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
        (err) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve();
        },
      );
    });
  }
}
