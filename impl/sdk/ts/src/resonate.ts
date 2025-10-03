import { LocalNetwork } from "../dev/network";
import { Handler } from "../src/handler";
import { Registry } from "../src/registry";
import { WallClock } from "./clock";
import { type Encoder, JsonEncoder } from "./encoder";
import { AsyncHeartbeat, type Heartbeat, NoopHeartbeat } from "./heartbeat";
import type {
  CreatePromiseAndTaskReq,
  CreatePromiseReq,
  CreateSubscriptionReq,
  DurablePromiseRecord,
  Message,
  MessageSource,
  Network,
  ReadPromiseReq,
  TaskRecord,
} from "./network/network";
import { HttpMessageSource, HttpNetwork } from "./network/remote";
import { Options } from "./options";
import { Promises } from "./promises";
import { ResonateInner } from "./resonate-inner";
import { Schedules } from "./schedules";
import type { Func, ParamsWithOptions, Return } from "./types";
import * as util from "./util";

export interface ResonateHandle<T> {
  id: string;
  result(): Promise<T>;
  done(): Promise<boolean>;
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
  private anycastPreference: string;
  private anycastNoPreference: string;
  private pid: string;
  private ttl: number;

  private inner: ResonateInner;
  private network: Network;
  private encoder: Encoder;
  private messageSource: MessageSource;
  private handler: Handler;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private subscriptions: Map<string, PromiseWithResolvers<DurablePromiseRecord<any>>> = new Map();

  public readonly promises: Promises;
  public readonly schedules: Schedules;

  constructor({
    url = undefined,
    group = "default",
    pid = crypto.randomUUID().replace(/-/g, ""),
    ttl = 1 * util.MIN,
    auth = undefined,
  }: {
    url?: string;
    group?: string;
    pid?: string;
    ttl?: number;
    auth?: { username: string; password: string };
  } = {}) {
    this.unicast = `poll://uni@${group}/${pid}`;
    this.anycastPreference = `poll://any@${group}/${pid}`;
    this.anycastNoPreference = `poll://any@${group}`;
    this.pid = pid;
    this.ttl = ttl;
    this.encoder = new JsonEncoder();

    if (!url) {
      const localNetwork = new LocalNetwork();
      this.network = localNetwork;
      this.messageSource = localNetwork.getMessageSource();
      this.heartbeat = new NoopHeartbeat();
    } else {
      this.network = new HttpNetwork({ url, auth, timeout: 1 * util.MIN, headers: {} });
      this.messageSource = new HttpMessageSource({ url, pid, group, auth });
      this.heartbeat = new AsyncHeartbeat(pid, ttl / 2, this.network);
    }

    this.handler = new Handler(this.network, this.encoder);
    this.registry = new Registry();
    this.dependencies = new Map();

    this.inner = new ResonateInner({
      unicast: this.unicast,
      anycastPreference: this.anycastPreference,
      anycastNoPreference: this.anycastNoPreference,
      pid: this.pid,
      ttl: this.ttl,
      clock: new WallClock(),
      network: this.network,
      messageSource: this.messageSource,
      handler: this.handler,
      registry: this.registry,
      heartbeat: this.heartbeat,
      dependencies: this.dependencies,
    });

    this.promises = new Promises(this.network);
    this.schedules = new Schedules(this.network);

    // subscribe to notify
    this.messageSource.subscribe("notify", this.onMessage.bind(this));
  }

  /**
   * Create a local Resonate instance
   */
  static local(): Resonate {
    return new Resonate({
      group: "default",
      pid: "default",
      ttl: Number.MAX_SAFE_INTEGER,
    });
  }

  /**
   * Create a remote Resonate instance
   */
  static remote({
    url = "http://localhost:8001",
    group = "default",
    pid = crypto.randomUUID().replace(/-/g, ""),
    ttl = 1 * util.MIN,
    auth = undefined,
  }: {
    url?: string;
    group?: string;
    pid?: string;
    ttl?: number;
    auth?: { username: string; password: string };
    messageSourceAuth?: { username: string; password: string };
  } = {}): Resonate {
    return new Resonate({ url, group, pid, ttl, auth });
  }

  /**
   * Register a function and returns a registered function
   */
  public register<F extends Func>(
    name: string,
    func: F,
    options?: {
      version?: number;
    },
  ): ResonateFunc<F>;
  public register<F extends Func>(
    func: F,
    options?: {
      version?: number;
    },
  ): ResonateFunc<F>;
  public register<F extends Func>(
    nameOrFunc: string | F,
    funcOrOptions?:
      | F
      | {
          version?: number;
        },
    maybeOptions: {
      version?: number;
    } = {},
  ): ResonateFunc<F> {
    const { version = 1 } = (typeof funcOrOptions === "object" ? funcOrOptions : maybeOptions) ?? {};
    const func = typeof nameOrFunc === "function" ? nameOrFunc : (funcOrOptions as F);
    const name = typeof nameOrFunc === "string" ? nameOrFunc : (func.name ?? "anonymous");

    this.registry.add(func, name, version);

    return {
      run: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> =>
        this.run(id, func, ...util.splitArgsAndOpts(args, this.options({ version: version }))),
      rpc: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> =>
        this.rpc(id, func, ...util.splitArgsAndOpts(args, this.options({ version: version }))),
      beginRun: (id: string, ...args: ParamsWithOptions<F>): Promise<ResonateHandle<Return<F>>> =>
        this.beginRun(id, func, ...util.splitArgsAndOpts(args, this.options({ version: version }))),
      beginRpc: (id: string, ...args: ParamsWithOptions<F>): Promise<ResonateHandle<Return<F>>> =>
        this.beginRpc(id, func, ...util.splitArgsAndOpts(args, this.options({ version: version }))),
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
    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    const { promise, task } = await this.createPromiseAndTask({
      kind: "createPromiseAndTask",
      promise: {
        id: id,
        timeout: Date.now() + opts.timeout,
        param: {
          data: {
            func: registered.name,
            args: args,
            version: opts.version,
          },
        },
        tags: {
          ...opts.tags,
          "resonate:invoke": this.anycastPreference,
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
    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());

    let name: string;
    if (typeof funcOrName === "string") {
      name = funcOrName;
    } else {
      name = this.registry.get(funcOrName, opts.version).name;
    }

    const promise = await this.createPromise({
      kind: "createPromise",
      id: id,
      timeout: Date.now() + opts.timeout,
      param: {
        data: {
          func: name,
          args: args,
          version: opts.version,
        },
      },
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
    const [args, opts] = util.splitArgsAndOpts(argsWithOpts, this.options());

    let funcName: string;
    if (typeof func === "string") {
      funcName = func;
    } else {
      funcName = this.registry.get(func, opts.version).name;
    }

    // TODO: move this into the handler?
    const { headers, data } = this.encoder.encode({ func: funcName, args, version: opts.version });

    await this.schedules.create(name, cron, "{{.id}}.{{.timestamp}}", opts.timeout, {
      ikey: name,
      promiseHeaders: headers,
      promiseData: data,
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
    const target = opts.target ?? this.anycastNoPreference;
    return new Options({ target, ...opts });
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
    this.messageSource.stop();
    this.heartbeat.stop();
  }

  private createPromiseAndTask(
    req: CreatePromiseAndTaskReq<any>,
  ): Promise<{ promise: DurablePromiseRecord; task?: TaskRecord }> {
    return new Promise((resolve, reject) =>
      this.handler.createPromiseAndTask(
        req,
        (err, res) => {
          if (err) {
            // TODO: improve this message
            reject(new Error(`Promise '${req.promise.id}' could not be created`));
          } else {
            resolve({ promise: res!.promise, task: res!.task });
          }
        },
        true,
      ),
    );
  }

  private createPromise(req: CreatePromiseReq<any>): Promise<DurablePromiseRecord<any>> {
    return new Promise((resolve, reject) =>
      this.handler.createPromise(
        req,
        (err, res) => {
          if (err) {
            // TODO: improve this message
            reject(new Error(`Promise '${req.id}' could not be created`));
          } else {
            resolve(res!);
          }
        },
        true,
      ),
    );
  }

  private createSubscription(req: CreateSubscriptionReq): Promise<DurablePromiseRecord<any>> {
    return new Promise((resolve, reject) =>
      this.handler.createSubscription(
        req,
        (err, res) => {
          if (err) {
            // TODO: improve this message
            reject(new Error(`Subscription for promise '${req.promiseId}' could not be created`));
          } else {
            resolve(res!);
          }
        },
        true,
      ),
    );
  }

  private readPromise(req: ReadPromiseReq): Promise<DurablePromiseRecord<any>> {
    return new Promise((resolve, reject) =>
      this.handler.readPromise(req, (err, res) => {
        if (err) {
          // TODO: improve this message
          reject(new Error(`Promise '${req.id}' not found`));
        } else {
          resolve(res!);
        }
      }),
    );
  }

  private createHandle(promise: DurablePromiseRecord<any>): ResonateHandle<any> {
    const createSubscriptionReq: CreateSubscriptionReq = {
      kind: "createSubscription",
      id: this.pid,
      promiseId: promise.id,
      timeout: promise.timeout + 1 * util.MIN, // add a buffer
      recv: this.unicast,
    };

    return {
      id: promise.id,
      done: () => this.createSubscription(createSubscriptionReq).then((res) => res.state !== "pending"),
      result: () => this.createSubscription(createSubscriptionReq).then((res) => this.subscribe(promise.id, res)),
    };
  }

  private onMessage(msg: Message): void {
    util.assert(msg.type === "notify");
    if (msg.type === "notify") {
      let paramData: any;
      let valueData: any;

      try {
        paramData = this.encoder.decode(msg.promise.param);
      } catch (e) {
        // TODO: improve this message
        this.notify(msg.promise.id, new Error("Failed to decode promise param"));
        return;
      }

      try {
        valueData = this.encoder.decode(msg.promise.value);
      } catch (e) {
        // TODO: improve this message
        this.notify(msg.promise.id, new Error("Failed to decode promise value"));
        return;
      }

      this.notify(msg.promise.id, undefined, {
        ...msg.promise,
        param: { headers: msg.promise.param?.headers, data: paramData },
        value: { headers: msg.promise.value?.headers, data: valueData },
      });
    }
  }

  private async subscribe(id: string, res: DurablePromiseRecord) {
    const { promise, resolve, reject } =
      this.subscriptions.get(id) ?? Promise.withResolvers<DurablePromiseRecord<any>>();

    if (res.state === "pending") {
      this.subscriptions.set(id, { promise, resolve, reject });
    } else {
      resolve(res);
      this.subscriptions.delete(id);
    }

    const p = await promise;
    util.assert(p.state !== "pending", "promise must be completed");

    if (p.state === "resolved") {
      return p.value?.data;
    }
    if (p.state === "rejected") {
      throw p.value?.data;
    }
    if (p.state === "rejected_canceled") {
      throw new Error("Promise canceled");
    }
    if (p.state === "rejected_timedout") {
      throw new Error("Promise timedout");
    }
  }

  private notify(id: string, err: any, res?: DurablePromiseRecord<any>) {
    const subscription = this.subscriptions.get(id);

    // notify subscribers
    if (res) {
      util.assert(res.state !== "pending", "promise must be completed");
      subscription?.resolve(res);
    } else {
      subscription?.reject(err);
    }

    // remove subscription
    this.subscriptions.delete(id);
  }
}
