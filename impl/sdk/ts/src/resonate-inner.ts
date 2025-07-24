import { Computation } from "./computation";
import type { Context } from "./context";
import { LocalNetwork } from "./network/local";
import type { CreatePromiseAndTaskRes, DurablePromiseRecord, Mesg, Network, RecvMsg } from "./network/network";
import { HttpNetwork } from "./network/remote";
import { Registry } from "./registry";
import { Server } from "./server";
import type { Func, Params, Ret } from "./types";
import * as util from "./util";

export interface InvocationHandler<T> {
  result: Promise<T>;
}

export interface RegisteredFunc<F extends Func> {
  options: () => void;
  run: (id: string, args: Params<F>, cb: (invocationHandler: InvocationHandler<Ret<F>>) => void) => void;
}

export class ResonateInner {
  private network: Network;
  private computations: Map<string, Computation>;
  private invocationHandlers: Map<string, InvocationHandler<any>>;
  private registry: Registry;

  constructor(network: Network, opts: {}) {
    this.registry = new Registry();
    this.computations = new Map();
    this.invocationHandlers = new Map();
    this.network = network;
    this.network.onMessage = this.onMessage;
  }

  static local(): ResonateInner {
    return new ResonateInner(new LocalNetwork(new Server()), {});
  }

  static remote(opts: { host?: string; storePort?: number; messageSourcePort?: number; pid?: string }): ResonateInner {
    // const { host, storePort, messageSourcePort } = opts; // TODO(avillega): use this values to create the httpnetwork
    const network = new HttpNetwork({}); // TODO(avillega): initialize httpsNetwork with the right args
    return new ResonateInner(network, {});
  }

  public invoke<T>(
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
          tags: { "resonate:invoke": "default" }, // TODO(avillega): use the real anycast address
        },
        task: {
          processId: "0", // TODO(avillega): use the real processId
          ttl: 300_000, // TODO(avillega): use ttl from options
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
          return;
        }

        if (!task) {
          console.log("got no task, should create a sub for the promise instead");
          // TODO(avillega): Create sub for the promise and return
          return;
        }

        const compu = new Computation(this.network);
        this.computations.set(durable.id, compu);
        compu.invoke(task!, { id: durable.id, fn: func, args }, (_err, result) => resultPromise.resolve(result));

        cb(iHandler);
      },
    );
  }

  public register<T extends Func>(name: string, func: T): RegisteredFunc<T> {
    if (this.registry.has(name)) {
      throw new Error(`'${name}' already registered`);
    }

    // Set the the function in the registry and create a closure to invoke the registered function
    this.registry.set(name, func);
    return {
      run: (id: string, args: Params<T>, cb: (invocationHandler: InvocationHandler<Ret<T>>) => void) => {
        this.invoke(id, name, args, cb);
      },
      options: () => {
        return undefined;
      },
    };
  }

  private onMessage = (msg: RecvMsg): void => {
    console.log({ msg });
    if (msg.type === "resume" || msg.type === "invoke") {
      // - Could try to find the computation at this point and use the computation handle to claim the task
      // and update the root and leaf promise for that computation (mostly in the resume case)
      this.network.send(
        {
          kind: "claimTask",
          id: msg.task.id,
          counter: msg.task.counter,
          processId: "0", // TODO (avillega): use right processId
          ttl: 30_000, // TODO  (avillega): parametrize
        },
        (_timeout, response) => {
          if (response.kind === "claimedtask") {
            console.log("claimed task=", response);
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
            //
            const { fn: funcName, args } = root.data.param;
            const func = this.registry.get(funcName);

            if (!func) {
              // Drop the task, can not invoke this function in this node
              console.log("got a task that can not be executed in this node, dropping task");
              return;
            }

            const compu = new Computation(this.network);
            this.computations.set(root.id, compu);
            compu.invoke(msg.task!, { id: root.data.id, fn: func, args }, () => {});
          }
        },
      );
    } else {
      // Get the subs handlers and complete them with the promise info
    }
  };
}
