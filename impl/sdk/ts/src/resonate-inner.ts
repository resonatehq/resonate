import type { Clock } from "./clock";
import { Computation, type Status } from "./computation";
import type { Handler } from "./handler";
import type { Heartbeat } from "./heartbeat";
import type { DurablePromiseRecord, Message, Network, TaskRecord } from "./network/network";
import type { Registry } from "./registry";
import type { Callback } from "./types";
import * as util from "./util";

export type PromiseHandler = {
  addEventListener: (event: "created" | "completed", callback: (p: DurablePromiseRecord) => void) => void;
  subscribe: () => Promise<void>;
};

export type Task = ClaimedTask | UnclaimedTask;

export type ClaimedTask = {
  kind: "claimed";
  task: TaskRecord;
  rootPromise: DurablePromiseRecord;
};

export type UnclaimedTask = {
  kind: "unclaimed";
  task: TaskRecord;
};

export class ResonateInner {
  private unicast: string;
  private anycast: string;
  private pid: string;
  private ttl: number;
  private clock: Clock;
  private network: Network;
  private handler: Handler;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private computations: Map<string, Computation> = new Map();

  constructor({
    unicast,
    anycast,
    pid,
    ttl,
    clock,
    network,
    handler,
    registry,
    heartbeat,
    dependencies,
  }: {
    unicast: string;
    anycast: string;
    pid: string;
    ttl: number;
    clock: Clock;
    network: Network;
    handler: Handler;
    registry: Registry;
    heartbeat: Heartbeat;
    dependencies: Map<string, any>;
  }) {
    this.unicast = unicast;
    this.anycast = anycast;
    this.pid = pid;
    this.ttl = ttl;
    this.clock = clock;
    this.network = network;
    this.handler = handler;
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.dependencies = dependencies;

    // subscribe to invoke and resume
    this.network.subscribe("invoke", this.onMessage.bind(this));
    this.network.subscribe("resume", this.onMessage.bind(this));
  }

  public process(task: Task, done: Callback<Status>) {
    let computation = this.computations.get(task.task.rootPromiseId);
    if (!computation) {
      computation = new Computation(
        task.task.rootPromiseId,
        this.unicast,
        this.anycast,
        this.pid,
        this.ttl,
        this.clock,
        this.network,
        this.handler,
        this.registry,
        this.heartbeat,
        this.dependencies,
      );
      this.computations.set(task.task.rootPromiseId, computation);
    }

    computation.process(task, done);
  }

  private onMessage(msg: Message): void {
    util.assert(msg.type === "invoke" || msg.type === "resume");

    if (msg.type === "invoke" || msg.type === "resume") {
      this.process({ kind: "unclaimed", task: msg.task }, () => {});
    }
  }
}
