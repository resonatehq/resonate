import { LocalNetwork } from "../dev/network";
import { Handler } from "../src/handler";
import { Registry } from "../src/registry";
import { WallClock } from "./clock";
import { AsyncHeartbeat, type Heartbeat, NoopHeartbeat } from "./heartbeat";
import type {
  CreatePromiseAndTaskReq,
  CreatePromiseReq,
  DurablePromiseRecord,
  Message,
  Network,
  ReadPromiseReq,
  TaskRecord,
} from "./network/network";
import { HttpNetwork } from "./network/remote";
import { Options } from "./options";
import { Promises } from "./promises";
import { ResonateInner } from "./resonate-inner";
import { Schedules } from "./schedules";
import type { Func, ParamsWithOptions, Return } from "./types";
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
  options: (opts?: Partial<Options>) => Options;
}

export interface ResonateSchedule {
  delete(): Promise<void>;
}

export class Resonate {
  private unicast: string;
  private anycast: string;
  private pid: string;
  private ttl: number;

  private inner: ResonateInner;
  private network: Network;
  private handler: Handler;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private subscriptions: Map<string, Array<(promise: DurablePromiseRecord) => void>> = new Map();

  public readonly promises: Promises;
  public readonly schedules: Schedules;

  constructor({ group, pid, ttl }: { group: string; pid: string; ttl: number }, network: Network) {
    this.unicast = `poll://uni@${group}/${pid}`;
    this.anycast = `poll://any@${group}/${pid}`;
    this.pid = pid;
    this.ttl = ttl;

    this.network = network;
    this.handler = new Handler(this.network);
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
      handler: this.handler,
      registry: this.registry,
      heartbeat: this.heartbeat,
      dependencies: this.dependencies,
    });

    this.promises = new Promises(network);
    this.schedules = new Schedules(network);

    // subscribe to notify
    this.network.subscribe("notify", this.onMessage.bind(this));
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

    const { promise, task } = await this.createPromiseAndTask({
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

    if (task) {
      this.inner.process({ kind: "claimed", task: task, rootPromise: promise }, () => {});
    }

    return this.createHandle(promise);
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

    const promise = await this.createPromise({
      kind: "createPromise",
      id: id,
      timeout: Date.now() + opts.timeout,
      param: { func: name, args },
      tags: { ...opts.tags, "resonate:invoke": opts.target, "resonate:scope": "global" },
      iKey: id,
      strict: false,
    });

    return this.createHandle(promise);
  }

  public async schedule<F extends Func>(
    name: string,
    cron: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateSchedule>;
  public async schedule(name: string, cron: string, func: string, ...args: any[]): Promise<ResonateSchedule>;
  public async schedule(
    name: string,
    cron: string,
    func: Func | string,
    ...argsWithOpts: any[]
  ): Promise<ResonateSchedule> {
    let funcName: string;
    if (typeof func === "string") {
      funcName = func;
    } else {
      const registered = this.registry.get(func);
      if (!registered) {
        throw new Error(`${func} is not registered`);
      }
      funcName = registered.name;
    }

    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());

    await this.schedules.create(name, cron, "{{.id}}.{{.timestamp}}", opts.timeout, {
      ikey: name,
      promiseParam: { func: funcName, args },
      promiseTags: { ...opts.tags, "resonate:invoke": opts.target },
    });

    return {
      delete: () => this.schedules.delete(name),
    };
  }

  /**
   * Get a promise and return a value
   */
  public async get<T = any>(id: string): Promise<ResonateHandle<T>> {
    const promise = await this.readPromise({
      kind: "readPromise",
      id: id,
    });

    return this.createHandle(promise);
  }

  public options(opts: Partial<Options> = {}): Options {
    return new Options(opts);
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

  private createPromiseAndTask(
    req: CreatePromiseAndTaskReq,
  ): Promise<{ promise: DurablePromiseRecord; task?: TaskRecord }> {
    return new Promise((resolve, reject) => {
      this.handler.createPromiseAndTask(
        req,
        (err, res) => {
          if (err) {
            reject(new Error(`Promise '${req.promise.id}' could not be created`));
          } else {
            resolve({ promise: res!.promise, task: res!.task });
          }
        },
        true,
      );
    });
  }

  private createPromise(req: CreatePromiseReq): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.handler.createPromise(
        req,
        (err, res) => {
          if (err) {
            reject(new Error(`Promise '${req.id}' could not be created`));
          } else {
            resolve(res!);
          }
        },
        true,
      );
    });
  }

  private readPromise(req: ReadPromiseReq): Promise<DurablePromiseRecord> {
    return new Promise((resolve, reject) => {
      this.handler.readPromise(req, (err, res) => {
        if (err) {
          reject(new Error(`Promise '${req.id}' not found`));
        } else {
          resolve(res!);
        }
      });
    });
  }

  private createHandle(promise: DurablePromiseRecord): ResonateHandle<any> {
    return {
      id: promise.id,
      result: () =>
        new Promise((resolve, reject) => {
          this.subscribe(promise, (promise) => {
            util.assert(promise.state !== "pending", "promise must be completed");

            if (promise.state === "resolved") {
              resolve(promise.value);
            } else if (promise.state === "rejected") {
              reject(promise.value);
            } else if (promise.state === "rejected_canceled") {
              reject(new Error("Promise canceled"));
            } else if (promise.state === "rejected_timedout") {
              reject(new Error("Promise timedout"));
            }
          });
        }),
    };
  }

  private onMessage(msg: Message): void {
    util.assert(msg.type === "notify");
    if (msg.type === "notify") this.notify(msg.promise);
  }

  private subscribe(promise: DurablePromiseRecord, callback: (p: DurablePromiseRecord) => void) {
    const subscriptions = this.subscriptions.get(promise.id) || [];
    subscriptions.push(callback);

    // store local subscription
    this.subscriptions.set(promise.id, subscriptions);

    // create remote subscription
    this.handler.createSubscription(
      {
        kind: "createSubscription",
        id: this.pid,
        promiseId: promise.id,
        timeout: promise.timeout + 1 * util.MIN, // add a buffer
        recv: this.unicast,
      },
      (err, res) => {
        if (err) {
          // TODO
        } else if (res!.state !== "pending") {
          this.notify(res!);
        }
      },
      true,
    );
  }

  private notify(promise: DurablePromiseRecord) {
    util.assert(promise.state !== "pending", "promise must be completed");

    // notify subscribers
    for (const callback of this.subscriptions.get(promise.id) ?? []) {
      callback(promise);
    }

    // remove subscriptions
    this.subscriptions.delete(promise.id);
  }
}
