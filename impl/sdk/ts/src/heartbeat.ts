import { randomUUID } from "node:crypto";
import type { Logger } from "./logger.js";
import type { Send } from "./types.js";
import { VERSION } from "./util.js";

export interface Heartbeat {
  start(): void;
  stop(): void;
}

export class AsyncHeartbeat implements Heartbeat {
  private intervalId: ReturnType<typeof setInterval> | undefined;
  private send: Send;
  private pid: string;
  private counter = 0;
  private delay: number;
  private logger: Logger;

  constructor(pid: string, delay: number, send: Send, logger: Logger) {
    this.pid = pid;
    this.delay = delay;
    this.send = send;
    this.logger = logger;
  }

  start(): void {
    this.counter++;
    if (!this.intervalId) {
      this.heartbeat();
    }
  }

  private heartbeat(): void {
    this.intervalId = setInterval(() => {
      this.send({
        kind: "task.heartbeat",
        head: { corrId: randomUUID(), version: VERSION },
        data: {
          pid: this.pid,
          tasks: [],
        },
      }).catch((err) => {
        this.logger.warn(
          { component: "heartbeat", pid: this.pid, error: err instanceof Error ? err.message : String(err) },
          "Failed to send heartbeat",
        );
      });
    }, this.delay);
  }

  stop(): void {
    this.clearIntervalIfMatch(this.counter);
  }

  private clearIntervalIfMatch(counter: number) {
    if (this.counter === counter) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
  }
}

export class NoopHeartbeat implements Heartbeat {
  start(): void {}
  stop(): void {}
}
