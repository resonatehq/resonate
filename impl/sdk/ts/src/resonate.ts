import { LocalNetwork } from "../dev/network";
import type { CreatePromiseAndTaskRes, Network } from "./network/network";
import { HttpNetwork } from "./network/remote";
import { ResonateInner } from "./resonate-inner";
import type { Func, Params, Ret } from "./types";
import * as util from "./util";

export interface InvocationHandler<T> {
  result: Promise<T>;
}

export interface RegisteredFunc<F extends Func> {
  fn: F;
  options: () => void;
  beginRun: (id: string, ...args: Params<F>) => Promise<InvocationHandler<Ret<F>>>;
}

export class Resonate {
  private inner: ResonateInner;
  private network: Network;
  private group: string;
  private pid: string;
  private ttl: number;
  private handlerPromises: Map<string, PromiseWithResolvers<any>>;

  constructor(network: Network, config: { group: string; pid: string; ttl: number }) {
    this.network = network;
    this.handlerPromises = new Map();
    this.group = config.group;
    this.pid = config.pid;
    this.ttl = config.ttl;
    this.inner = new ResonateInner(network, config);
    this.inner.onComplete(({ promiseId, value }) => {
      const p = this.handlerPromises.get(promiseId);
      if (p) {
        // TODO(avillega): Handler rejections
        p.resolve(value);
      }
    });
  }

  /**
   * Create a local Resonate instance
   */
  static local(): Resonate {
    return new Resonate(new LocalNetwork(), { group: "default", pid: "default", ttl: 1 * util.MIN });
  }

  /**
   * Create a remote Resonate instance
   */
  static remote(config: {
    host?: string;
    storePort?: string;
    messageSourcePort?: string;
    group?: string;
    pid?: string;
    ttl?: number;
  }): Resonate {
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
    return new Resonate(network, { pid, group, ttl });
  }

  /**
   * Invoke a function and return a Promise
   */
  public async beginRun<T>(id: string, funcName: string, ...args: any[]): Promise<InvocationHandler<T>> {
    const func = this.inner.registry.get(funcName); // TODO(avillega): should the register be owned by Resonate?
    if (!func) {
      throw new Error(`${funcName} does not exists`);
    }

    return new Promise<InvocationHandler<T>>((resolve, _reject) => {
      this.network.send(
        {
          kind: "createPromiseAndTask",
          promise: {
            id: id,
            timeout: 24 * util.HOUR + Date.now(), // TODO(avillega): use option timeout or 24h. Check the  usage of Date here, seems fine
            param: {
              fn: funcName,
              args,
            },
            tags: { "resonate:invoke": `poll://any@${this.group}/${this.pid}` }, // TODO(avillega): use the real anycast address or change the server to not require `poll://`
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
          this.handlerPromises.set(durable.id, resultPromise);

          if (durable.state === "resolved") {
            resultPromise.resolve(durable.value);
            resolve(iHandler);
            return;
          }

          if (!task) {
            console.log("got no task, should create a sub for the promise instead");
            // TODO(avillega): Create sub for the promise and return
            return;
          }

          this.inner.process({ ...task, kind: "claimed", rootPromise: durable }, () => {}); // TODO(avillega): Handle failure and platform errors in callback
          resolve(iHandler);
        },
      );
    });
  }

  /**
   * Register a function and returns a registered function
   */
  public register<F extends Func>(name: string, func: F): RegisteredFunc<F> {
    this.inner.register(name, func);

    return {
      fn: func,
      beginRun: (id: string, ...args: Params<F>): Promise<InvocationHandler<Ret<F>>> => {
        return this.beginRun(id, name, ...args);
      },
      options: () => {},
    };
  }

  public stop() {
    this.inner.stop();
  }
}
