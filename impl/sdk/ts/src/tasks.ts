import { LocalNetwork } from "./network/local";
import type { Mesg, Network, TaskRecord } from "./network/network";

import * as util from "./util";

export class Tasks {
  private network: Network;

  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  claim(id: string, counter: number, processId: string, ttl: number): Promise<Mesg> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "claimTask",
          id: id,
          counter: counter,
          processId: processId,
          ttl: ttl,
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
            if (response.kind !== "claimedtask") {
              throw new Error("unexpected response");
            }
            resolve(response.message);
          }
        },
      );
    });
  }

  complete(id: string, counter: number): Promise<TaskRecord> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "completeTask",
          id: id,
          counter: counter,
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
            if (response.kind !== "completedtask") {
              throw new Error("unexpected response");
            }
            resolve(response.task);
          }
        },
      );
    });
  }

  heartbeat(processId: string): Promise<number> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "heartbeatTasks",
          processId: processId,
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
            if (response.kind !== "heartbeatTasks") {
              throw new Error("unexpected response");
            }

            resolve(response.tasksAffected);
          }
        },
      );
    });
  }
}
