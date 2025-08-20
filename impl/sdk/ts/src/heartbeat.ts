import type { Network } from "./network/network";
import * as util from "./util";

export interface Heartbeat {
  start(delay: number): void;
  stop(): void;
}

export class AsyncHeartbeat implements Heartbeat {
  private network: Network;
  private intervalId: ReturnType<typeof setInterval> | undefined;
  private pid: string;
  private counter = 0;

  constructor(network: Network, pid: string) {
    this.network = network;
    this.pid = pid;
  }

  start(delay: number): void {
    this.counter++;
    if (!this.intervalId) {
      this.heartbeat(delay);
    }
  }

  private heartbeat(delay: number): void {
    this.intervalId = setInterval(
      (intervalId, counter) => {
        this.network.send(
          {
            kind: "heartbeatTasks",
            processId: this.pid,
          },
          (err, res) => {
            if (err) return;
            util.assertDefined(res);

            if (res.tasksAffected === 0) {
              this.clearIntervalIfMatch(intervalId, counter);
            }
          },
        );
      },
      delay,
      this.intervalId,
      this.counter,
    );
  }

  stop(): void {
    this.clearIntervalIfMatch(this.intervalId, this.counter);
  }

  private clearIntervalIfMatch(interval: ReturnType<typeof setInterval> | undefined, counter: number) {
    if (this.intervalId === interval && this.counter === counter) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
  }
}

export class NoHeartbeat implements Heartbeat {
  start(delay: number): void {}
  stop(): void {}
}
