import type { Heartbeat } from "heartbeat";
import { Computation, type Status } from "./computation";
import type { DurablePromiseRecord, Message, Network } from "./network/network";
import { Registry } from "./registry";
import type { Callback, Func, Task } from "./types";
import * as util from "./util";

export class ResonateInner {
  public registry: Registry; // TODO(avillega): Who should own the registry? the resonate class?

  private computations: Map<string, Computation>;
  private network: Network;
  private group: string;
  private pid: string;
  private ttl: number;
  private heartbeat: Heartbeat;
  private notifications: Map<string, DurablePromiseRecord> = new Map();
  private subscriptions: Map<string, Array<(promise: DurablePromiseRecord) => void>> = new Map();

  constructor(network: Network, config: { group: string; pid: string; ttl: number; heartbeat: Heartbeat }) {
    const { group, pid, ttl, heartbeat } = config;
    this.computations = new Map();
    this.group = group;
    this.pid = pid;
    this.ttl = ttl;
    this.heartbeat = heartbeat;
    this.registry = new Registry();
    this.network = network;
    this.network.subscribe(this.onMessage.bind(this));
  }

  public process(task: Task, callback: Callback<Status>) {
    let computation = this.computations.get(task.rootPromiseId);
    if (!computation) {
      computation = new Computation(
        task.rootPromiseId,
        this.pid,
        this.ttl,
        this.group,
        this.network,
        this.registry,
        this.heartbeat,
      );
      this.computations.set(task.rootPromiseId, computation);
    }

    computation.process(task, (err, status) => {
      if (err) callback(err);
      util.assertDefined(status);

      callback(false, status);

      if (status.kind === "completed") {
        // notify subscribers of the completed promise
        this.notify(status.promise);
      }
    });
  }

  private onMessage(msg: Message): void {
    switch (msg.type) {
      case "invoke":
      case "resume":
        this.process({ ...msg.task, kind: "unclaimed" }, () => {});
        break;

      case "notify":
        // TODO(avillega): assert that the promise is completed
        this.notify(msg.promise);
        break;
    }
  }

  public register<F extends Func>(name: string, func: F) {
    if (this.registry.has(name)) {
      throw new Error(`'${name}' already registered`);
    }

    this.registry.set(name, func);
  }

  public subscribe(id: string, callback: (promise: DurablePromiseRecord) => void): void {
    // immediately notify if we already have a notification
    if (this.notifications.has(id)) {
      callback(this.notifications.get(id)!);
      return;
    }

    // otherwise add callback to the subscriptions
    const subscriptions = this.subscriptions.get(id) ?? [];
    this.subscriptions.set(id, subscriptions);
    subscriptions.push(callback);
  }

  private notify(promise: DurablePromiseRecord) {
    // store the notification
    this.notifications.set(promise.id, promise);

    // notify subscribers
    for (const callback of this.subscriptions.get(promise.id) ?? []) {
      callback(promise);
    }

    // clear subscribers
    this.subscriptions.delete(promise.id);
  }

  public stop() {
    this.network.stop();
    this.heartbeat.stopHeartbeat();
  }
}
