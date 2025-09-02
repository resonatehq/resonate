import { LocalNetwork } from "../dev/network";
import { Handler } from "../src/handler";
import { Registry } from "../src/registry";
import { WallClock } from "./clock";
import { AsyncHeartbeat, type Heartbeat, NoopHeartbeat } from "./heartbeat";
import type { Network } from "./network/network";
import { HttpNetwork } from "./network/remote";
import { Promises } from "./promises";
import type { PromiseHandler } from "./resonate-inner";
import { ResonateInner } from "./resonate-inner";
import { Schedules } from "./schedules";
import { type Func, type Options, type ParamsWithOptions, RESONATE_OPTIONS, type Return } from "./types";
import * as util from "./util";

export interface ResonateHandle<T> {
  id: string;
  result(): Promise<T>;
}

export interface ResonateFunc<F extends Func> {
  run: (id: string, ...args: ParamsWithOptions<F>) => Promise<Return<F>>;
  rpc: (id: string, ...args: ParamsWithOptions<F>) => Promise<Return<F>>;
  beginRun: (id: string, ...args: ParamsWithOptions<F>) => Promise<ResonateHandle<Return<F>>>;
  beginRpc: (id: string, ...args: ParamsWithOptions<F>) => Promise<ResonateHandle<Return<F>>>;
  options: (opts?: Partial<Options>) => Partial<Options> & { [RESONATE_OPTIONS]: true };
}

export class Resonate {
  private unicast: string;
  private anycast: string;
  private pid: string;
  private ttl: number;

  private inner: ResonateInner;
  private network: Network;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;

  public readonly promises: Promises;
  public readonly schedules: Schedules;

  constructor({ group, pid, ttl }: { group: string; pid: string; ttl: number }, network: Network) {
    this.unicast = `poll://uni@${group}/${pid}`;
    this.anycast = `poll://any@${group}/${pid}`;
    this.pid = pid;
    this.ttl = ttl;

    this.network = network;
    this.registry = new Registry();
    this.heartbeat = network instanceof LocalNetwork ? new NoopHeartbeat() : new AsyncHeartbeat(pid, ttl / 2, network);
    this.dependencies = new Map();

    this.inner = new ResonateInner({
      unicast: this.unicast,
      anycast: this.anycast,
      pid: this.pid,
      ttl: this.ttl,
      clock: new WallClock(),
      network: this.network,
      handler: new Handler(this.network),
      registry: this.registry,
      heartbeat: this.heartbeat,
      dependencies: this.dependencies,
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

    if (this.registry.has(name)) {
      throw new Error(`'${name}' already registered`);
    }

    this.registry.set(name, func);

    return {
      run: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> => this.run(id, func, ...args),
      rpc: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> => this.rpc(id, func, ...args),
      beginRun: (id: string, ...args: ParamsWithOptions<F>): Promise<ResonateHandle<Return<F>>> =>
        this.beginRun(id, func, ...args),
      beginRpc: (id: string, ...args: ParamsWithOptions<F>): Promise<ResonateHandle<Return<F>>> =>
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
  public async beginRun<F extends Func>(
    id: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateHandle<Return<F>>>;
  public async beginRun<T>(id: string, func: string, ...args: any[]): Promise<ResonateHandle<T>>;
  public async beginRun(id: string, funcOrName: Func | string, ...args: any[]): Promise<ResonateHandle<any>>;
  public async beginRun(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<ResonateHandle<any>> {
    const registered = this.registry.get(funcOrName);
    if (!registered) {
      throw new Error(`${funcOrName} does not exist`);
    }

    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());

    const promiseHandler = this.inner.run({
      kind: "createPromiseAndTask",
      promise: {
        id: id,
        timeout: Date.now() + opts.timeout,
        param: { func: registered.name, args },
        tags: {
          ...opts.tags,
          "resonate:invoke": this.anycast,
          "resonate:scope": "global",
        },
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
  public async beginRpc<F extends Func>(
    id: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateHandle<Return<F>>>;
  public async beginRpc<T>(id: string, func: string, ...args: any[]): Promise<ResonateHandle<T>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<ResonateHandle<any>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<ResonateHandle<any>> {
    let name: string;
    if (typeof funcOrName === "string") {
      name = funcOrName;
    } else {
      const registered = this.registry.get(funcOrName);
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
      target: "poll://any@default",
      timeout: 24 * util.HOUR,
      tags: {},
      ...opts,
      [RESONATE_OPTIONS]: true,
    };
  }

  /** Store a named dependency for use with `Context`.
   *  The dependency is made available to all functions via
   *  their execution `Context`.
   *  Setting a dependency for a name that already exists will
   *  overwrite the previously set dependency.
   */
  public setDependency(name: string, obj: any): void {
    this.dependencies.set(name, obj);
  }

  public stop() {
    this.network.stop();
    this.heartbeat.stop();
  }

  private handle(promiseHandler: PromiseHandler): Promise<ResonateHandle<any>> {
    // resolves with a handle
    const handle = Promise.withResolvers<ResonateHandle<any>>();

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
