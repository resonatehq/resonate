import { LocalNetwork } from "../dev/network";
import { AsyncHeartbeat, NoHeartbeat } from "./heartbeat";
import type { Network } from "./network/network";
import { HttpNetwork } from "./network/remote";
import { Promises } from "./promises";
import type { PromiseHandler } from "./resonate-inner";
import { ResonateInner } from "./resonate-inner";
import { Schedules } from "./schedules";
import { type Func, type Options, type ParamsWithOptions, RESONATE_OPTIONS, type Return } from "./types";
import * as util from "./util";

export interface Handle<T> {
  id: string;
  result(): Promise<T>;
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
  private group: string;
  private pid: string;
  private ttl: number;
  public readonly promises: Promises;
  public readonly schedules: Schedules;

  constructor(config: { group: string; pid: string; ttl: number }, network: Network) {
    this.group = config.group;
    this.pid = config.pid;
    this.ttl = config.ttl;

    const heartbeat =
      network instanceof LocalNetwork ? new NoHeartbeat() : new AsyncHeartbeat(config.pid, this.ttl / 2, network);

    this.inner = new ResonateInner(network, {
      ...config,
      heartbeat: heartbeat,
    });

    this.promises = new Promises(network);
    this.schedules = new Schedules(network);
  }

  /**
   * Create a local Resonate instance
   */
  static local(): Resonate {
    return new Resonate(
      {
        group: "default",
        pid: "default",
        ttl: Number.MAX_SAFE_INTEGER,
      },
      new LocalNetwork(),
    );
  }

  /**
   * Create a remote Resonate instance
   */
  static remote({
    host = "http://localhost",
    storePort = "8001",
    messageSourcePort = "8002",
    group = "default",
    pid = crypto.randomUUID().replace(/-/g, ""),
    ttl = 1 * util.MIN,
  }: {
    host?: string;
    storePort?: string;
    messageSourcePort?: string;
    group?: string;
    pid?: string;
    ttl?: number;
  } = {}): Resonate {
    const network = new HttpNetwork({
      host,
      storePort,
      messageSourcePort,
      pid,
      group,
      timeout: 1 * util.MIN,
      headers: {},
    });
    return new Resonate({ pid, group, ttl }, network);
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
    return (await this.beginRun(id, funcOrName, ...args)).result();
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

    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());

    const promiseHandler = this.inner.run({
      kind: "createPromiseAndTask",
      promise: {
        id: id,
        timeout: opts.timeout + Date.now(),
        param: { func: registered.name, args },
        tags: {
          ...opts.tags,
          "resonate:invoke": `poll://any@${this.group}/${this.pid}`,
          "resonate:scope": "global",
        }, // TODO(avillega): use the real anycast address or change the server to not require `poll://`
      },
      task: {
        processId: this.pid,
        ttl: this.ttl,
      },
      iKey: id,
      strict: false,
    });

    return this.handle(promiseHandler);
  }

  /**
   * Invoke a remote function and return a value
   */
  public async rpc<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async rpc<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async rpc<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async rpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRpc(id, funcOrName, ...args)).result();
  }

  /**
   * Invoke a remote function and return a promise
   */
  public async beginRpc<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Handle<Return<F>>>;
  public async beginRpc<T>(id: string, func: string, ...args: any[]): Promise<Handle<T>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<Handle<any>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<Handle<any>> {
    let name: string;
    if (typeof funcOrName === "string") {
      name = funcOrName;
    } else {
      const registered = this.inner.registry.get(funcOrName);
      if (!registered) {
        throw new Error(`${funcOrName} is not registered`);
      }
      name = registered.name;
    }

    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());

    const promiseHandler = this.inner.rpc({
      kind: "createPromise",
      id: id,
      timeout: Date.now() + opts.timeout,
      param: { func: name, args },
      tags: { ...opts.tags, "resonate:invoke": opts.target, "resonate:scope": "global" },
      iKey: id,
      strict: false,
    });

    return this.handle(promiseHandler);
  }

  public options(opts: Partial<Options> = {}): Options & { [RESONATE_OPTIONS]: true } {
    return {
      id: "",
      target: `poll://any@${this.group}`,
      timeout: 24 * util.HOUR,
      tags: {},
      ...opts,
      [RESONATE_OPTIONS]: true,
    };
  }

  public stop() {
    this.inner.stop();
  }

  private handle(promiseHandler: PromiseHandler): Promise<Handle<any>> {
    // resolves with a handle
    const handle = Promise.withResolvers<Handle<any>>();

    // resolves with the result
    const result = Promise.withResolvers<any>();

    // listen for created and resolve p1 with a handle
    promiseHandler.addEventListener("created", (promise) => {
      handle.resolve({
        id: promise.id,
        result: async () => {
          // subscribe lazily, no need to await
          promiseHandler.subscribe();
          return await result.promise;
        },
      });
    });

    // listen for completed and resolve p2 with the value
    promiseHandler.addEventListener("completed", (promise) => {
      util.assert(promise.state !== "pending", "promise must be completed");

      if (promise.state === "resolved") {
        result.resolve(promise.value);
      } else if (promise.state === "rejected") {
        result.reject(promise.value);
      } else if (promise.state === "rejected_canceled") {
        result.reject(new Error("Promise canceled"));
      } else if (promise.state === "rejected_timedout") {
        result.reject(new Error("Promise timedout"));
      }
    });

    return handle.promise;
  }
}
