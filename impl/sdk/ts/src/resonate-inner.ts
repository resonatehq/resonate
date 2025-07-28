import { Server } from "../dev/server";
import { Computation } from "./computation";
import { LocalNetwork } from "./network/local";
import type { CreatePromiseAndTaskRes, Network, RecvMsg } from "./network/network";
import { HttpNetwork } from "./network/remote";
import { Registry } from "./registry";
import type { Func, Params, Ret } from "./types";
import * as util from "./util";

export interface InvocationHandler<T> {
  result: Promise<T>;
}

export interface InnerRegisteredFunc<F extends Func> {
  options: () => void;
  beginRun: (id: string, args: Params<F>, cb: (invocationHandler: InvocationHandler<Ret<F>>) => void) => void;
}

export class ResonateInner {
  private network: Network;
  private computations: Map<string, Computation>;
  private invocationHandlers: Map<string, InvocationHandler<any>>;
  private registry: Registry;
  private ttl: number;
  private pid: string;
  private group: string;

  constructor(network: Network, config: { pid: string; group: string; ttl: number }) {
    this.registry = new Registry();
    this.computations = new Map();
    this.invocationHandlers = new Map();
    this.group = config.group;
    this.pid = config.pid;
    this.ttl = config.ttl;
    this.network = network;
    this.network.onMessage = this.onMessage;
  }

  static local(): ResonateInner {
    return new ResonateInner(new LocalNetwork(new Server()), { pid: "default", group: "default", ttl: 1 * util.SEC });
  }

  static remote(config: {
    host?: string;
    storePort?: string;
    messageSourcePort?: string;
    group?: string;
    pid?: string;
    ttl?: number;
  }): ResonateInner {
    const pid = config.pid ?? crypto.randomUUID();
    const group = config.group ?? "default";
    const ttl = config.ttl ?? 10 * util.SEC;

    const { host, storePort, messageSourcePort } = config;
    const network = new HttpNetwork({
      host: host ?? "http://localhost",
      storePort: storePort ?? "8001",
      msgSrcPort: messageSourcePort ?? "8002",
      pid: pid,
      group: group,
      timeout: 1 * util.MIN,
      headers: {},
    });
    return new ResonateInner(network, { pid, group, ttl });
  }

  public beginRun<T>(
    id: string,
    funcName: string,
    args: any[],
    cb: (invocationHandler: InvocationHandler<T>) => void,
  ): void {
    const func = this.registry.get(funcName);
    if (!func) {
      throw new Error(`${funcName} does not exists`);
    }

    this.network.send(
      {
        kind: "createPromiseAndTask",
        promise: {
          id: id,
          timeout: Number.MAX_SAFE_INTEGER, // TODO(avillega): use proper timeout from the options or 24h
          param: {
            fn: funcName,
            args,
          },
          tags: { "resonate:invoke": `poll://any@${this.group}/${this.pid}` }, // TODO(avillega): use the real anycast address
        },
        task: {
          processId: this.pid,
          ttl: this.ttl,
        },
        iKey: id,
        strict: false,
      },
      (timeout, res) => {
        if (timeout) {
          console.error("Something went wrong reaching the resonate server");
          // TODO(avillega): Handle platform level error
        }

        const { promise: durable, task } = res as CreatePromiseAndTaskRes;
        const resultPromise = Promise.withResolvers<T>();
        const iHandler = { result: resultPromise.promise };
        this.invocationHandlers.set(durable.id, iHandler);

        if (durable.state === "resolved") {
          resultPromise.resolve(durable.value);
          cb(iHandler);
          return;
        }

        if (!task) {
          console.log("got no task, should create a sub for the promise instead");
          // TODO(avillega): Create sub for the promise and return
          return;
        }

        const compu = new Computation(this.network, this.group, this.pid);
        this.computations.set(durable.id, compu);
        compu.handler.updateCache(durable); // Note: we can update the cache because we are holding a claimed task
        compu.invoke(task, { id: durable.id, fn: func, args }, (_err, result) => resultPromise.resolve(result));

        cb(iHandler);
      },
    );
  }

  public register<T extends Func>(name: string, func: T): InnerRegisteredFunc<T> {
    if (this.registry.has(name)) {
      throw new Error(`'${name}' already registered`);
    }

    // Set the the function in the registry and create a closure to invoke the registered function
    this.registry.set(name, func);
    return {
      beginRun: (id: string, args: Params<T>, cb: (invocationHandler: InvocationHandler<Ret<T>>) => void) => {
        this.beginRun(id, name, args, cb);
      },
      options: () => {
        return undefined;
      },
    };
  }

  private onMessage = (msg: RecvMsg): void => {
    if (msg.type === "resume" || msg.type === "invoke") {
      this.network.send(
        {
          kind: "claimTask",
          id: msg.task.id,
          counter: msg.task.counter,
          processId: this.pid,
          ttl: this.ttl,
        },
        (_timeout, response) => {
          if (response.kind === "claimedtask") {
            const { root, leaf } = response.message.promises;
            util.assertDefined(root);
            const comp = this.computations.get(root.id);

            if (comp && response.message.kind === "resume") {
              // if we already have a computation for that is very probable that we are reaciving a resume
              util.assertDefined(leaf);
              comp.handler.updateCache(leaf.data);
              comp.resume(msg.task);
              return;
            }

            // Handle it as a new invocation
            const { fn: funcName, args } = root.data.param;
            const func = this.registry.get(funcName);

            if (!func) {
              // TODO(avillega) Drop the task, can not invoke this function in this node
              console.log("got a task that can not be executed in this node, dropping task");
              return;
            }

            const compu = new Computation(this.network, this.group, this.pid);
            this.computations.set(root.id, compu);
            compu.invoke(msg.task!, { id: root.data.id, fn: func, args }, () => {});
          }
        },
      );
    } else {
      // TODO(avillega): Get the subs handlers and complete them with the promise info
    }
  };
}
