import { Computation } from "./computation";
import type { DurablePromiseRecord, Network, RecvMsg } from "./network/network";
import { Registry } from "./registry";
import type { CompResult, Func, Task } from "./types";

export class ResonateInner {
  public registry: Registry; // TODO(avillega): Who should own the registry? the resonate class?

  private computations: Map<string, Computation>;
  private network: Network;
  private group: string;
  private pid: string;
  private ttl: number;
  private notifications: Map<string, DurablePromiseRecord> = new Map();
  private subscriptions: Map<string, Array<(promise: DurablePromiseRecord) => void>> = new Map();

  constructor(network: Network, config: { group: string; pid: string; ttl: number }) {
    const { group, pid, ttl } = config;
    this.computations = new Map();
    this.group = group;
    this.pid = pid;
    this.ttl = ttl;
    this.registry = new Registry();
    this.network = network;
    this.network.onMessage = this.onMessage.bind(this);
  }

  public process(t: Task, cb: (res: CompResult) => void) {
    let computation = this.computations.get(t.rootPromiseId);
    if (!computation) {
      computation = new Computation(t.rootPromiseId, this.network, this.registry, this.group, this.pid, this.ttl);
      this.computations.set(t.rootPromiseId, computation);
    }

    computation.process(t, (res) => {
      // notify subscribers
      if (res.kind === "completed") {
        this.notify(res.durablePromise);
      }
      cb(res);
    });
  }

  private onMessage(msg: RecvMsg, cb: (res: CompResult) => void): void {
    switch (msg.type) {
      case "invoke":
      case "resume":
        this.process({ ...msg.task, kind: "unclaimed" }, cb);
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
  }
}
