import { WallClock } from "../clock.js";
import { Codec } from "../codec.js";
import { type Encryptor, NoopEncryptor } from "../encryptor.js";
import exceptions, { ResonateTimeoutException } from "../exceptions.js";
import { AsyncHeartbeat, type Heartbeat, NoopHeartbeat } from "../heartbeat.js";
import { ConsoleLogger, type Logger, type LogLevel } from "../logger.js";
import { HttpNetwork, PollMessageSource } from "../network/http.js";
import { LocalNetwork } from "../network/local.js";
import type { Network } from "../network/network.js";
import {
  isConflict,
  isSuccess,
  type Message,
  type PromiseCreateReq,
  type PromiseGetReq,
  type PromiseRecord,
  type PromiseRegisterListenerReq,
  type TaskCreateReq,
  type TaskRecord,
} from "../network/types.js";
import { type Options, OptionsBuilder } from "../options.js";
import { delay, getEnv, randomUUID } from "../platform.js";
import { Promises } from "../promises.js";
import { Registry } from "../registry.js";
import { Schedules } from "../schedules.js";
import type { Func, Send } from "../types.js";
import * as util from "../util.js";
import type { AnyFunc, ParamsWithOptions, Return } from "./context.js";
import { Core } from "./core.js";

export interface ResonateHandle<T> {
  id: string;
  result(): Promise<T>;
  done(): Promise<boolean>;
}

export interface ResonateFunc<F extends AnyFunc> {
  run: (id: string, ...args: ParamsWithOptions<F>) => Promise<ResonateHandle<Return<F>>>;
  rpc: (id: string, ...args: ParamsWithOptions<F>) => Promise<ResonateHandle<Return<F>>>;
  options: (opts?: Partial<Options>) => Options;
}

export interface ResonateSchedule {
  delete(): Promise<void>;
}

type SubscriptionEntry = {
  promise: Promise<PromiseRecord>;
  resolve: (r: PromiseRecord) => void;
  reject: (e: any) => void;
  timeout: number;
};

/**
 * Opt-in entry point for the async/await execution engine. Lives alongside the
 * generator engine's `Resonate` (the package root export) and reuses the same
 * network/codec/registry/task plumbing, routing execution through {@link Core}.
 */
export class Resonate {
  private clock: WallClock;
  private pid: string;
  private ttl: number;
  private idPrefix: string;

  private core: Core;
  private codec: Codec;
  private network: Network;
  private send: Send;
  private logger: Logger;

  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private subscriptions: Map<string, SubscriptionEntry> = new Map();
  private subscribeEvery: number;
  private intervalId: ReturnType<typeof setInterval>;

  public readonly promises: Promises;
  public readonly schedules: Schedules;

  constructor({
    url = undefined,
    group = "default",
    pid = undefined,
    ttl = 1 * util.MIN,
    token = undefined,
    timeout = undefined,
    verbose = false,
    logLevel = undefined,
    logger = undefined,
    encryptor = undefined,
    network = undefined,
    prefix = undefined,
  }: {
    url?: string;
    group?: string;
    pid?: string;
    ttl?: number;
    token?: string;
    timeout?: number;
    verbose?: boolean;
    logLevel?: LogLevel;
    logger?: Logger;
    encryptor?: Encryptor;
    network?: Network;
    prefix?: string;
  } = {}) {
    this.clock = new WallClock();
    this.ttl = ttl;
    this.codec = new Codec(encryptor ?? new NoopEncryptor());

    const resolvedPrefix = prefix ?? getEnv("RESONATE_PREFIX");
    this.idPrefix = resolvedPrefix ? `${resolvedPrefix}:` : "";

    const resolvedLogLevel: LogLevel = logLevel ?? (verbose ? "debug" : "warn");
    this.logger = logger ?? new ConsoleLogger(resolvedLogLevel);

    this.subscribeEvery = util.MIN;

    const resolvedUrl = url ?? (getEnv("RESONATE_URL") || undefined);
    this.pid = pid ?? randomUUID().replace(/-/g, "");

    let heartbeat: boolean;
    if (network) {
      this.network = network;
      heartbeat = true;
    } else if (resolvedUrl) {
      const adapter = new PollMessageSource({
        url: `${resolvedUrl}/poll/${encodeURIComponent(group)}/${encodeURIComponent(this.pid)}`,
        token,
        logger: this.logger,
      });
      this.network = new HttpNetwork({
        url: resolvedUrl,
        token,
        timeout,
        headers: {},
        adapter,
        logger: this.logger,
      });
      heartbeat = true;
    } else {
      this.network = new LocalNetwork({ pid: this.pid, group });
      heartbeat = false;
    }

    this.send = this.network.send;

    if (heartbeat) {
      this.heartbeat = new AsyncHeartbeat(this.pid, ttl / 2, this.send, this.logger);
    } else {
      this.heartbeat = new NoopHeartbeat();
    }

    this.registry = new Registry();
    this.dependencies = new Map();
    this.optsBuilder = new OptionsBuilder({ match: this.network.match.bind(this.network), idPrefix: this.idPrefix });

    this.core = new Core({
      pid: this.pid,
      ttl: this.ttl,
      clock: this.clock,
      send: this.send,
      codec: this.codec,
      registry: this.registry,
      heartbeat: this.heartbeat,
      dependencies: this.dependencies,
      optsBuilder: this.optsBuilder,
      logger: this.logger,
    });

    this.promises = new Promises(this.send);
    this.schedules = new Schedules(this.send);

    this.network.recv(this.onMessage.bind(this));
    this.network.init().catch((err) => {
      this.logger.error(
        { component: "async-resonate", error: err instanceof Error ? err.message : String(err) },
        "Failed to start network",
      );
    });

    this.intervalId = setInterval(async () => {
      for (const [id, sub] of this.subscriptions.entries()) {
        try {
          const res = await this.promiseRegisterListener({
            kind: "promise.register_listener",
            head: { corrId: randomUUID(), version: util.VERSION },
            data: { awaited: id, address: this.network.unicast },
          });
          if (res.state !== "pending") {
            sub.resolve(res);
            this.subscriptions.delete(id);
          }
        } catch (err) {
          this.logger.warn(
            { component: "async-resonate", error: err instanceof Error ? err.message : String(err) },
            "subscription poll failed",
          );
        }
      }
    }, this.subscribeEvery);
  }

  /** Registers an async function (leaf or workflow) for execution. */
  public register<F extends AnyFunc>(name: string, func: F, options?: { version?: number }): ResonateFunc<F>;
  public register<F extends AnyFunc>(func: F, options?: { version?: number }): ResonateFunc<F>;
  public register<F extends AnyFunc>(
    nameOrFunc: string | F,
    funcOrOptions?: F | { version?: number },
    maybeOptions: { version?: number } = {},
  ): ResonateFunc<F> {
    const { version = 1 } = (typeof funcOrOptions === "object" ? funcOrOptions : maybeOptions) ?? {};
    const func = (typeof nameOrFunc === "function" ? nameOrFunc : (funcOrOptions as F)) as AnyFunc;
    const name = typeof nameOrFunc === "string" ? nameOrFunc : func.name;

    this.registry.add(func as unknown as Func, name, version);

    // Re-flatten the split: spreading the [args, opts] tuple itself would pass
    // the args array as a single argument to the function.
    return {
      run: (id, ...args) => {
        const [argu, opts] = this.getArgsAndOpts(args, version);
        return this.run(id, func, ...argu, opts);
      },
      rpc: (id, ...args) => {
        const [argu, opts] = this.getArgsAndOpts(args, version);
        return this.rpc(id, func, ...argu, opts);
      },
      options: this.options,
    };
  }

  /**
   * Begins a workflow as a locally-executed root task and returns a handle.
   * Await `.result()` for the value (delivered via the durable-promise
   * subscription, never via the in-memory frame — see the GC rules).
   */
  public async run<F extends AnyFunc>(
    id: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateHandle<Return<F>>>;
  public async run<T>(id: string, funcOrName: AnyFunc | string, ...args: any[]): Promise<ResonateHandle<T>>;
  public async run(id: string, funcOrName: AnyFunc | string, ...argsWithOpts: any[]): Promise<ResonateHandle<any>> {
    const [args, opts] = this.getArgsAndOpts(argsWithOpts);
    const registered = this.registry.get(funcOrName, opts.version);

    if (!registered) {
      throw exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(
        typeof funcOrName === "string" ? funcOrName : funcOrName.name,
        opts.version,
      );
    }

    id = `${this.idPrefix}${id}`;
    util.assert(registered.version > 0, "function version must be greater than zero");

    const { promise, task } = await this.taskCreate({
      kind: "task.create",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: {
        pid: this.pid,
        ttl: this.ttl,
        action: {
          kind: "promise.create",
          head: { corrId: randomUUID(), version: util.VERSION },
          data: {
            id,
            timeoutAt: Date.now() + opts.timeout,
            param: {
              data: { func: registered.name, args, retry: opts.retryPolicy?.encode(), version: registered.version },
              headers: {},
            },
            tags: {
              ...opts.tags,
              "resonate:origin": id,
              // A genuine top-level root is its own lineage origin AND its own
              // id-generation prefix; the prefix then propagates down unchanged.
              "resonate:prefix": id,
              "resonate:branch": id,
              "resonate:parent": id,
              "resonate:scope": "global",
              "resonate:target": this.network.anycast,
            },
          },
        },
      },
    });

    if (task && task.state === "acquired") {
      this.core
        .executeUntilBlocked(task, promise)
        .catch((err) =>
          this.logger.warn(
            { component: "async-resonate", error: err instanceof Error ? err.message : String(err) },
            "executeUntilBlocked failed",
          ),
        );
    }

    return this.createHandle(promise);
  }

  /** Begins a workflow as a remote (globally-targeted) promise and returns a handle. */
  public async rpc<F extends AnyFunc>(
    id: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateHandle<Return<F>>>;
  public async rpc<T>(id: string, funcOrName: AnyFunc | string, ...args: any[]): Promise<ResonateHandle<T>>;
  public async rpc(id: string, funcOrName: AnyFunc | string, ...argsWithOpts: any[]): Promise<ResonateHandle<any>> {
    const [args, opts] = this.getArgsAndOpts(argsWithOpts);
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      throw exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version);
    }

    id = `${this.idPrefix}${id}`;
    const func = registered ? registered.name : (funcOrName as string);
    const version = registered ? registered.version : opts.version || 1;

    const promise = await this.promiseCreate({
      kind: "promise.create",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: {
        id,
        timeoutAt: Date.now() + opts.timeout,
        param: { data: { func, args, retry: opts.retryPolicy?.encode(), version }, headers: {} },
        tags: {
          ...opts.tags,
          "resonate:origin": id,
          // A genuine top-level root is its own lineage origin AND its own
          // id-generation prefix; the prefix then propagates down unchanged.
          "resonate:prefix": id,
          "resonate:branch": id,
          "resonate:parent": id,
          "resonate:scope": "global",
          "resonate:target": opts.target,
        },
      },
    });

    return this.createHandle(promise);
  }

  /**
   * Creates a recurring schedule that invokes a registered function on a cron
   * interval. Each tick creates a fresh durable promise whose id embeds the
   * schedule name and timestamp. Returns a handle with `delete()`.
   */
  public async schedule<F extends AnyFunc>(
    name: string,
    cron: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateSchedule>;
  public async schedule(name: string, cron: string, func: string, ...args: any[]): Promise<ResonateSchedule>;
  public async schedule(
    name: string,
    cron: string,
    funcOrName: AnyFunc | string,
    ...argsWithOpts: any[]
  ): Promise<ResonateSchedule> {
    const [args, opts] = this.getArgsAndOpts(argsWithOpts);
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      throw exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version);
    }

    const { headers, data } = this.codec.encode({
      func: registered ? registered.name : (funcOrName as string),
      args,
      version: registered ? registered.version : opts.version || 1,
    });

    await this.schedules.create(name, cron, `${this.idPrefix}{{.id}}.{{.timestamp}}`, opts.timeout, {
      promiseHeaders: headers,
      promiseData: data,
      promiseTags: { ...opts.tags, "resonate:target": opts.target },
    });

    return {
      delete: () => this.schedules.delete(name),
    };
  }

  /** Returns a handle to an existing durable promise by id. */
  public async get<T = any>(id: string): Promise<ResonateHandle<T>> {
    id = `${this.idPrefix}${id}`;
    const promise = await this.promiseGet({
      kind: "promise.get",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: { id },
    });

    return this.createHandle(promise);
  }

  public options(
    opts: Partial<Pick<Options, "tags" | "target" | "timeout" | "version" | "retryPolicy">> = {},
  ): Options {
    return this.optsBuilder.build(opts);
  }

  public setDependency(name: string, obj: any): void {
    this.dependencies.set(name, obj);
  }

  public async stop(): Promise<void> {
    await this.network.stop();
    this.heartbeat.stop();
    clearInterval(this.intervalId);
  }

  private getArgsAndOpts(args: any[], version?: number): [any[], Options] {
    return util.splitArgsAndOpts(args, this.options({ version }));
  }

  private async taskCreate(req: TaskCreateReq): Promise<{ promise: PromiseRecord; task?: TaskRecord }> {
    req.data.action.data.param = this.codec.encode(req.data.action.data.param.data);

    const res = await this.send(req);

    if (!isSuccess(res) && !isConflict(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, { code: res.head.status, message: res.data });
    }

    if (isConflict(res)) {
      const promise = await this.promiseRegisterListener({
        kind: "promise.register_listener",
        head: { corrId: randomUUID(), version: util.VERSION },
        data: { awaited: req.data.action.data.id, address: this.network.unicast },
      });
      return { promise, task: undefined };
    }

    const promise = this.codec.decodePromise(res.data.promise);
    return { promise, task: res.data.task };
  }

  private async promiseCreate(req: PromiseCreateReq): Promise<PromiseRecord> {
    req.data.param = this.codec.encode(req.data.param.data);

    const res = await this.send(req);
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, { code: res.head.status, message: res.data });
    }

    return this.codec.decodePromise(res.data.promise);
  }

  private async promiseRegisterListener(req: PromiseRegisterListenerReq): Promise<PromiseRecord> {
    const retryDelay = 5 * util.SEC;
    while (true) {
      try {
        const res = await this.send(req);
        if (!isSuccess(res)) {
          await delay(retryDelay);
          continue;
        }
        return this.codec.decodePromise(res.data.promise);
      } catch (e) {
        if (e instanceof ResonateTimeoutException) {
          await delay(retryDelay);
          continue;
        }
        throw e;
      }
    }
  }

  private async promiseGet(req: PromiseGetReq): Promise<PromiseRecord> {
    const res = await this.send(req);
    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, { code: res.head.status, message: res.data });
    }
    return this.codec.decodePromise(res.data.promise);
  }

  private createHandle(promise: PromiseRecord): ResonateHandle<any> {
    const registerListenerReq: PromiseRegisterListenerReq = {
      kind: "promise.register_listener",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: { awaited: promise.id, address: this.network.unicast },
    };

    return {
      id: promise.id,
      done: () => this.promiseRegisterListener(registerListenerReq).then((res) => res.state !== "pending"),
      result: () => this.promiseRegisterListener(registerListenerReq).then((res) => this.subscribe(promise.id, res)),
    };
  }

  private onMessage(msg: Message): void {
    if (msg.kind === "execute") {
      this.core
        .onMessage(msg)
        .catch((err) =>
          this.logger.warn(
            { component: "async-resonate", error: err instanceof Error ? err.message : String(err) },
            "onMessage failed",
          ),
        );
      return;
    }
    util.assert(msg.kind === "unblock");

    try {
      const decoded = this.codec.decodePromise(msg.data.promise);
      this.notify(msg.data.promise.id, undefined, decoded);
    } catch {
      this.notify(msg.data.promise.id, new Error("Failed to decode promise"));
    }
  }

  private async subscribe(id: string, res: PromiseRecord) {
    const { promise, resolve, reject } = this.subscriptions.get(id) ?? Promise.withResolvers<PromiseRecord>();

    if (res.state === "pending") {
      this.subscriptions.set(id, { promise, resolve, reject, timeout: res.timeoutAt });
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

  private notify(id: string, err: any, res?: PromiseRecord) {
    let subscription = this.subscriptions.get(id);
    if (!subscription) {
      const { promise, resolve, reject } = Promise.withResolvers<PromiseRecord>();
      subscription = { promise, resolve, reject, timeout: res ? res.timeoutAt : 100000000 };
      this.subscriptions.set(id, subscription);
    } else {
      this.subscriptions.delete(id);
    }
    if (res) {
      util.assert(res.state !== "pending", "promise must be completed");
      subscription.resolve(res);
    } else {
      subscription.reject(err);
    }
  }
}
