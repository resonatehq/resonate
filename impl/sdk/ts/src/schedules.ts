import { LocalNetwork } from "./network/local";
import type { Network, ScheduleRecord } from "./network/network";

import * as util from "./util";

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
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "readSchedule") {
              throw new Error("unexpected response");
            }
            resolve(response.schedule);
          }
        },
      );
    });
  }

  create(
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    iKey: string | undefined = undefined,
    description: string | undefined = undefined,
    tags: Record<string, string> | undefined = undefined,
    promiseParam: any | undefined = undefined,
    promiseTags: Record<string, string> | undefined = undefined,
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
          promiseParam: promiseParam,
          promiseTags: promiseTags,
          iKey: iKey,
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
            if (response.kind !== "createSchedule") {
              throw new Error("unexpected response");
            }
            resolve(response.schedule);
          }
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
        (timeout, response) => {
          if (timeout) {
            util.assert(response.kind === "error");
            throw new Error("not implemented");
          }

          if (response.kind === "error") {
            util.assert(!timeout);
            reject(response.message);
          } else {
            if (response.kind !== "deleteSchedule") {
              throw new Error("unexpected response");
            }
            resolve();
          }
        },
      );
    });
  }
}
