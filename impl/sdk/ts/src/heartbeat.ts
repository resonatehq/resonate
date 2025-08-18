import type { Network } from "./network/network";

export interface Heartbeat {
  startHeartbeat(delay: number): void;
  stopHeartbeat(): void;
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

  startHeartbeat(delay: number): void {
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
          (_timeout, response) => {
            if (response.kind === "heartbeatTasks" && response.tasksAffected === 0) {
              this.clearIntervalIfMatch(intervalId, counter);
              return;
            }
            // Ignore any errors and keep heartbeating
          },
        );
      },
      delay,
      this.intervalId,
      this.counter,
    );
  }

  stopHeartbeat(): void {
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
  startHeartbeat(delay: number): void {}
  stopHeartbeat(): void {}
}
