import type { Logger } from "./logger.js";
import { randomUUID } from "./platform.js";
import type { Send } from "./types.js";
import { VERSION } from "./util.js";

export type HeartbeatTask = {
  id: string;
  version: number;
};

export interface Heartbeat {
  start(task: HeartbeatTask): void;
  stop(task?: HeartbeatTask): void;
}

export class AsyncHeartbeat implements Heartbeat {
  private intervalId: ReturnType<typeof setInterval> | undefined;
  private readonly tasks = new Map<string, number>();

  constructor(
    private readonly pid: string,
    private readonly delay: number,
    private readonly send: Send,
    private readonly logger: Logger,
  ) {}

  start(task: HeartbeatTask): void {
    this.tasks.set(task.id, task.version);

    if (!this.intervalId) {
      this.intervalId = setInterval(() => this.heartbeat(), this.delay);
    }
  }

  private heartbeat(): void {
    if (this.tasks.size === 0) return;

    this.send({
      kind: "task.heartbeat",
      head: { corrId: randomUUID(), version: VERSION },
      data: {
        pid: this.pid,
        tasks: [...this.tasks].map(([id, version]) => ({ id, version })),
      },
    }).catch((err) => {
      this.logger.warn(
        { component: "heartbeat", pid: this.pid, error: err instanceof Error ? err.message : String(err) },
        "Failed to send heartbeat",
      );
    });
  }

  stop(task?: HeartbeatTask): void {
    if (task) {
      if (this.tasks.get(task.id) === task.version) {
        this.tasks.delete(task.id);
      }
    } else {
      this.tasks.clear();
    }

    if (this.tasks.size === 0 && this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
  }
}

export class NoopHeartbeat implements Heartbeat {
  start(_task: HeartbeatTask): void {}
  stop(_task?: HeartbeatTask): void {}
}
