import { Computation } from "./computation";
import type { Network, RecvMsg } from "./network/network";
import { Registry } from "./registry";
import type { CompResult, Func, Task } from "./types";

export class ResonateInner {
  public registry: Registry; // TODO(avillega): Who should own the registry? the resonate class?

  private computations: Map<string, Computation>;
  private network: Network;
  private group: string;
  private pid: string;
  private ttl: number;
  private listeners: Array<(data: { promiseId: string; value: any }) => void> = [];

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
    let comp = this.computations.get(t.rootPromiseId);
    if (!comp) {
      comp = this.makeComputation(t.rootPromiseId);
      this.computations.set(t.rootPromiseId, comp);
    }

    comp.process(t, (res) => {
      if (res.kind === "completed") {
        this.emit({ promiseId: res.durablePromise.id, value: res.durablePromise.value });
      }
      cb(res);
    });
  }

  private makeComputation(promiseId: string): Computation {
    return new Computation(promiseId, this.network, this.registry, this.group, this.pid, this.ttl);
  }

  private onMessage(msg: RecvMsg, cb: (res: CompResult) => void): void {
    switch (msg.type) {
      case "invoke":
      case "resume":
        this.process({ ...msg.task, kind: "unclaimed" }, cb);
        break;
      case "notify":
        // TODO(avillega): assert that the promise is completed
        if (msg.promise.state !== "pending") {
          // TODO(avillega): handle rejection too, make the complete result be able to reject, probably just return the promise
          this.emit({ promiseId: msg.promise.id, value: msg.promise.value });
        }
        break;
    }
  }

  public register<F extends Func>(name: string, func: F) {
    if (this.registry.has(name)) {
      throw new Error(`'${name}' already registered`);
    }

    this.registry.set(name, func);
  }

  private emit(data: { promiseId: string; value: any }) {
    for (const listener of this.listeners) {
      listener(data);
    }
  }

  public onComplete(listener: (data: { promiseId: string; value: any }) => void) {
    this.listeners.push(listener);
  }

  public stop() {
    this.network.stop();
  }
}
