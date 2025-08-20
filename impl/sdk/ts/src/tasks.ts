import { LocalNetwork } from "../dev/network";
import type { ClaimTaskRes, Network, TaskRecord } from "./network/network";

export class Tasks {
  private network: Network;

  constructor(network: Network = new LocalNetwork()) {
    this.network = network;
  }

  claim(id: string, counter: number, processId: string, ttl: number): Promise<ClaimTaskRes["message"]> {
    return new Promise((resolve, reject) => {
      this.network.send(
        {
          kind: "claimTask",
          id: id,
          counter: counter,
          processId: processId,
          ttl: ttl,
        },
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.message);
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
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.task);
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
        (err, res) => {
          if (err) {
            // TODO: reject with more information
            reject(Error("not implemented"));
            return;
          }

          resolve(res!.tasksAffected);
        },
      );
    });
  }
}
