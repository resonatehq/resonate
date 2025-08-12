import { LocalNetwork } from "../dev/network";
import type {
  CreatePromiseAndTaskRes,
  CreatePromiseRes,
  CreateSubscriptionRes,
  DurablePromiseRecord,
  Network,
} from "./network/network";
import { HttpNetwork } from "./network/remote";
import { ResonateInner } from "./resonate-inner";
import { type Func, type Options, type ParamsWithOptions, RESONATE_OPTIONS, type Return } from "./types";
import * as util from "./util";

export interface Handle<T> {
  result: Promise<T>;
}

export interface ResonateFunc<F extends Func> {
  run: (id: string, ...args: ParamsWithOptions<F>) => Promise<Return<F>>;
  rpc: (id: string, ...args: ParamsWithOptions<F>) => Promise<Return<F>>;
  beginRun: (id: string, ...args: ParamsWithOptions<F>) => Promise<Handle<Return<F>>>;
  beginRpc: (id: string, ...args: ParamsWithOptions<F>) => Promise<Handle<Return<F>>>;
  options: (opts?: Partial<Options>) => Partial<Options> & { [RESONATE_OPTIONS]: true };
}

export class Resonate {
  private inner: ResonateInner;
  private network: Network;
  private group: string;
  private pid: string;
  private ttl: number;

  constructor(network: Network, config: { group: string; pid: string; ttl: number }) {
    this.network = network;
    this.group = config.group;
    this.pid = config.pid;
    this.ttl = config.ttl;
    this.inner = new ResonateInner(network, config);
  }

  /**
   * Create a local Resonate instance
   */
  static local(): Resonate {
    return new Resonate(new LocalNetwork(), {
      group: "default",
      pid: "default",
      ttl: 1 * util.MIN,
    });
  }

  /**
   * Create a remote Resonate instance
   */
  static remote(
    config: {
      host?: string;
      storePort?: string;
      messageSourcePort?: string;
      group?: string;
      pid?: string;
      ttl?: number;
    } = {},
  ): Resonate {
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
   * Register a function and returns a registered function
   */
  public register<F extends Func>(name: string, func: F): ResonateFunc<F>;
  public register<F extends Func>(func: F): ResonateFunc<F>;
  public register<F extends Func>(nameOrFunc: string | F, maybeFunc?: F): ResonateFunc<F> {
    const name = typeof nameOrFunc === "string" ? nameOrFunc : nameOrFunc.name;
    const func = typeof nameOrFunc === "string" ? maybeFunc! : nameOrFunc;

    this.inner.register(name ?? func.name, func);

    return {
      run: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> => this.run(id, func, ...args),
      rpc: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> => this.rpc(id, func, ...args),
      beginRun: (id: string, ...args: ParamsWithOptions<F>): Promise<Handle<Return<F>>> =>
        this.beginRun(id, func, ...args),
      beginRpc: (id: string, ...args: ParamsWithOptions<F>): Promise<Handle<Return<F>>> =>
        this.beginRpc(id, func, ...args),
      options: this.options,
    };
  }

  /**
   * Invoke a function and return a value
   */
  public async run<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async run<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async run<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async run(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRun(id, funcOrName, ...args)).result;
  }

  /**
   * Invoke a function and return a promise
   */
  public async beginRun<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Handle<Return<F>>>;
  public async beginRun<T>(id: string, func: string, ...args: any[]): Promise<Handle<T>>;
  public async beginRun(id: string, funcOrName: Func | string, ...args: any[]): Promise<Handle<any>>;
  public async beginRun(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<Handle<any>> {
    const registered = this.inner.registry.get(funcOrName); // TODO(avillega): should the register be owned by Resonate?
    if (!registered) {
      throw new Error(`${funcOrName} does not exist`);
    }

    // TODO(dfarr): use the options
    const [args, _] = util.splitArgsAndOpts(argsWithOpts, this.options());

    return new Promise<Handle<any>>((resolve) => {
      this.network.send(
        {
          kind: "createPromiseAndTask",
          promise: {
            id: id,
            timeout: 24 * util.HOUR + Date.now(), // TODO(avillega): use option timeout or 24h. Check the  usage of Date here, seems fine
            param: { func: registered.name, args },
            tags: { "resonate:invoke": `poll://any@${this.group}/${this.pid}` }, // TODO(avillega): use the real anycast address or change the server to not require `poll://`
          },
          task: {
            processId: this.pid,
            ttl: this.ttl,
          },
          iKey: id,
          strict: false,
        },
        (timeout, response) => {
          const res = response as CreatePromiseAndTaskRes;

          if (timeout) {
            // TODO(avillega): Handle platform level error
            console.error("Platform error");
            return;
          }

          // create and resolve a handle now that the durable promise
          // has been created
          const promise = Promise.withResolvers();
          resolve({ result: promise.promise });

          // check if the promise is complete and early exit
          if (this.complete(res.promise, promise.resolve, promise.reject)) {
            return;
          }

          // if there is no task create subscription
          if (!res.task) {
            this.subscribe(id, promise.resolve, promise.reject);
            return;
          }

          // TODO(avillega): Handle failure and platform errors in callback
          this.inner.process(
            {
              kind: "claimed",
              rootPromise: res.promise,
              ...res.task,
            },
            (res) => {
              if (res.kind === "completed") {
                this.complete(res.durablePromise, promise.resolve, promise.reject);
              }
            },
          );
        },
      );
    });
  }

  /**
   * Invoke a remote function and return a value
   */
  public async rpc<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async rpc<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async rpc<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async rpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRpc(id, funcOrName, ...args)).result;
  }

  /**
   * Invoke a remote function and return a promise
   */
  public async beginRpc<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Handle<Return<F>>>;
  public async beginRpc<T>(id: string, func: string, ...args: any[]): Promise<Handle<T>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<Handle<any>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<Handle<any>> {
    const registered = this.inner.registry.get(funcOrName); // TODO(avillega): should the register be owned by Resonate?
    if (!registered) {
      throw new Error(`${funcOrName} does not exist`);
    }

    // TODO(dfarr): use the options
    const [args, _] = util.splitArgsAndOpts(argsWithOpts, this.options());

    return new Promise<Handle<any>>((resolve) => {
      this.network.send(
        {
          kind: "createPromise",
          id: id,
          timeout: 24 * util.HOUR + Date.now(), // TODO(avillega): use option timeout or 24h. Check the  usage of Date here, seems fine
          param: { func: registered.name, args },
          tags: { "resonate:invoke": `poll://any@${this.group}` }, // TODO(avillega): use the real anycast address or change the server to not require `poll://`
          iKey: id,
          strict: false,
        },
        (timeout, response) => {
          const res = response as CreatePromiseRes;

          if (timeout) {
            // TODO(avillega): Handle platform level error
            console.error("Platform error");
            return;
          }

          // create and resolve a handle now that the durable promise
          // has been created
          const promise = Promise.withResolvers();
          resolve({ result: promise.promise });

          // check if the promise is complete and early exit
          if (this.complete(res.promise, promise.resolve, promise.reject)) {
            return;
          }

          // otherwise create subscription
          this.subscribe(id, promise.resolve, promise.reject);
        },
      );
    });
  }

  public options(opts: Partial<Options> = {}): Options & { [RESONATE_OPTIONS]: true } {
    return {
      id: "",
      target: "default",
      timeout: 24 * util.HOUR,
      ...opts,
      [RESONATE_OPTIONS]: true,
    };
  }

  private subscribe(id: string, resolve: (v: any) => void, reject: (e?: any) => void) {
    this.network.send(
      {
        kind: "createSubscription",
        id: id,
        timeout: 24 * util.HOUR + Date.now(), // TODO(avillega): use option timeout or 24h. Check the  usage of Date here, seems fine
        recv: `poll://uni@${this.group}/${this.pid}`,
      },
      (timeout, response) => {
        const res = response as CreateSubscriptionRes;

        if (timeout) {
          // TODO(avillega): Handle platform level error
          console.error("Platform error");
          return;
        }

        // once again check if the promise is complete and early exit
        if (this.complete(res.promise, resolve, reject)) {
          return;
        }

        // otherwise register a subscription
        this.inner.subscribe(id, (promise) => this.complete(promise, resolve, reject));
      },
    );
  }

  private complete(promise: DurablePromiseRecord, resolve: (v: any) => void, reject: (e?: any) => void) {
    if (promise.state === "resolved") {
      resolve(promise.value);
      return true;
    }
    if (promise.state === "rejected") {
      reject(promise.value);
      return true;
    }
    if (promise.state === "rejected_canceled") {
      // TODO: reject with specific error
      reject(new Error("Promise canceled"));
      return true;
    }
    if (promise.state === "rejected_timedout") {
      // TODO: reject with specific error
      reject(new Error("Promise timedout"));
      return true;
    }

    return false;
  }

  public stop() {
    this.inner.stop();
  }
}
