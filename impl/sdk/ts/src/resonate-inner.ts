import { Computation, type Status } from "./computation";
import type { Heartbeat } from "./heartbeat";
import type {
  CreatePromiseAndTaskReq,
  CreatePromiseReq,
  DurablePromiseRecord,
  Message,
  Network,
  ResponseFor,
} from "./network/network";
import { Registry } from "./registry";
import type { Callback, Func, Task } from "./types";
import * as util from "./util";

export type PromiseHandler = {
  addEventListener: (event: "created" | "completed", callback: (p: DurablePromiseRecord) => void) => void;
  subscribe: () => Promise<void>;
};

export class ResonateInner {
  public registry: Registry; // TODO(avillega): Who should own the registry? the resonate class?

  private computations: Map<string, Computation>;
  private network: Network;
  private group: string;
  private pid: string;
  private ttl: number;
  private heartbeat: Heartbeat;
  private notifications: Map<string, DurablePromiseRecord> = new Map();
  private subscriptions: Map<string, Array<(promise: DurablePromiseRecord) => boolean>> = new Map();

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

  public run(req: CreatePromiseAndTaskReq): PromiseHandler {
    return this.runOrRpc(req, (err, res) => {
      util.assert(!err, "retry forever ensures err is false");
      util.assertDefined(res);

      // notify
      this.notify(res.promise);

      // if we have the task, process it
      if (res.task) {
        this.process({ kind: "claimed", task: res.task, rootPromise: res.promise }, () => {});
      }
    });
  }

  public rpc(req: CreatePromiseReq): PromiseHandler {
    return this.runOrRpc(req, (err, res) => {
      util.assert(!err, "retry forever ensures err is false");
      util.assertDefined(res);

      // notify
      this.notify(res.promise);
    });
  }

  public process(task: Task, done: Callback<Status>) {
    let computation = this.computations.get(task.task.rootPromiseId);
    if (!computation) {
      computation = new Computation(
        task.task.rootPromiseId,
        this.pid,
        this.ttl,
        this.group,
        this.network,
        this.registry,
        this.heartbeat,
      );
      this.computations.set(task.task.rootPromiseId, computation);
    }

    computation.process(task, done);
  }

  private runOrRpc<T extends CreatePromiseReq | CreatePromiseAndTaskReq>(
    req: T,
    done: Callback<ResponseFor<T>>,
  ): PromiseHandler {
    const id = req.kind === "createPromise" ? req.id : req.promise.id;
    const timeout = req.kind === "createPromise" ? req.timeout : req.promise.timeout;

    this.network.send(req, done, true);

    return {
      addEventListener: (event: "created" | "completed", callback: (p: DurablePromiseRecord) => void) => {
        const subscriptions = this.subscriptions.get(id) || [];

        subscriptions.push((p: DurablePromiseRecord): boolean => {
          if (event === "created" || (event === "completed" && p.state !== "pending")) {
            callback(p);
            return true;
          }

          return false;
        });

        this.subscriptions.set(id, subscriptions);

        // immediately notify if we already have a promise
        const promise = this.notifications.get(id);
        if (promise) {
          this.notify(promise);
        }
      },
      subscribe: () =>
        new Promise<void>((resolve) => {
          // attempt to subscribe forever
          this.network.send(
            {
              kind: "createSubscription",
              id: id,
              timeout: timeout,
              recv: `poll://uni@${this.group}/${this.pid}`,
            },
            (err, res) => {
              util.assert(!err, "retry forever ensures err is false");
              util.assertDefined(res);

              this.notify(res.promise);
              resolve();
            },
            true,
          );
        }),
    };
  }

  public register<F extends Func>(name: string, func: F) {
    if (this.registry.has(name)) {
      throw new Error(`'${name}' already registered`);
    }

    this.registry.set(name, func);
  }

  private onMessage(msg: Message): void {
    switch (msg.type) {
      case "invoke":
      case "resume":
        this.process({ kind: "unclaimed", task: msg.task }, () => {});
        break;

      case "notify":
        // TODO(avillega): assert that the promise is completed
        this.notify(msg.promise);
        break;
    }
  }

  private notify(promise: DurablePromiseRecord) {
    // store the notification
    this.notifications.set(promise.id, promise);

    // notify subscribers
    const subscriptions = this.subscriptions.get(promise.id) ?? [];
    for (const [i, f] of subscriptions.entries()) {
      if (f(promise)) {
        subscriptions.splice(i, 1);
      }
    }

    // remove any subscriber that was notified
    this.subscriptions.set(promise.id, subscriptions);
  }

  public stop() {
    this.network.stop();
    this.heartbeat.stop();
  }
}
