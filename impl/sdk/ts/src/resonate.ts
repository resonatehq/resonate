import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { WallClock } from "./clock.js";
import { Codec } from "./codec.js";
import { Core } from "./core.js";
import { type Encryptor, NoopEncryptor } from "./encryptor.js";
import exceptions, { ResonateTimeoutException } from "./exceptions.js";
import { AsyncHeartbeat, type Heartbeat, NoopHeartbeat } from "./heartbeat.js";
import { ConsoleLogger, type Logger, type LogLevel } from "./logger.js";
import { HttpNetwork, PollMessageSource } from "./network/http.js";
import { LocalNetwork } from "./network/local.js";
import type { Network } from "./network/network.js";
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
} from "./network/types.js";
import { type Options, OptionsBuilder } from "./options.js";
import { Promises } from "./promises.js";
import { Registry } from "./registry.js";
import { Schedules } from "./schedules.js";
import type { Func, ParamsWithOptions, Return, Send } from "./types.js";
import * as util from "./util.js";

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

type SubscriptionEntry = {
  promise: Promise<PromiseRecord>;
  resolve: (r: PromiseRecord) => void;
  reject: (e: any) => void;
  timeout: number;
};

export class Resonate {
  private clock: WallClock;

  private pid: string;
  private ttl: number;
  private idPrefix;

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

  /**
   * Creates a new Resonate client instance.
   *
   * @param options - Configuration options for the client.
   * @param options.url - Resonate server URL. Falls back to `process.env.RESONATE_URL`.
   *   If no URL is resolved, a local in-memory network is used.
   * @param options.group - Worker group name. Defaults to `"default"`.
   * @param options.pid - Process identifier for the client. Defaults to a random UUID.
   * @param options.ttl - Time-to-live (in seconds) for claimed tasks. Defaults to `1 * util.MIN`.
   * @param options.token - Bearer token for authentication. Passed through to HttpNetwork
   *   which falls back to `RESONATE_TOKEN` env var.
   * @param options.timeout - Network request timeout. Passed through to HttpNetwork
   *   which falls back to `RESONATE_TIMEOUT` env var (default: 10s).
   * @param options.verbose - Enables verbose logging (shorthand for `logLevel: "debug"`). Defaults to `false`.
   * @param options.logLevel - Log level for the default ConsoleLogger. Defaults to `"warn"`. Takes precedence over `verbose`.
   * @param options.logger - Custom logger implementation. Defaults to {@link ConsoleLogger}.
   * @param options.encryptor - Payload encryptor. Defaults to {@link NoopEncryptor}.
   * @param options.network - Custom network implementation. Defaults to `undefined`.
   * @param options.prefix - ID prefix applied to generated IDs. Defaults to
   *   `process.env.RESONATE_PREFIX` when set.
   */
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

    const resolvedPrefix = prefix ?? process.env.RESONATE_PREFIX;
    this.idPrefix = resolvedPrefix ? `${resolvedPrefix}:` : "";

    // Resolve logger: explicit logger > ConsoleLogger with resolved level
    // logLevel takes precedence over verbose; verbose: true -> "debug"
    const resolvedLogLevel: LogLevel = logLevel ?? (verbose ? "debug" : "warn");
    this.logger = logger ?? new ConsoleLogger(resolvedLogLevel);

    this.subscribeEvery = 60 * 1000; // make this configurable

    // Determine the URL: url arg > RESONATE_URL env var
    // Only used to decide between LocalNetwork and HttpNetwork.
    // Full URL/bun fmt
    // bun check
    // /token/timeout env var resolution is HttpNetwork's responsibility (3.7).
    const resolvedUrl = url ?? (process.env.RESONATE_URL || undefined);

    this.pid = pid ?? crypto.randomUUID().replace(/-/g, "");

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

    // subscribe to network
    this.network.recv(this.onMessage.bind(this));
    this.network.init().catch((err) => {
      this.logger.error(
        { component: "resonate", error: err instanceof Error ? err.message : String(err) },
        "Failed to start network",
      );
    });
    // periodically refresh subscriptions
    this.intervalId = setInterval(async () => {
      for (const [id, sub] of this.subscriptions.entries()) {
        try {
          const registerListenerReq: PromiseRegisterListenerReq = {
            kind: "promise.register_listener",
            head: { corrId: randomUUID(), version: util.VERSION },
            data: {
              awaited: id,
              address: this.network.unicast,
            },
          };

          const res = await this.promiseRegisterListener(registerListenerReq);
          if (res.state !== "pending") {
            sub.resolve(res);
            this.subscriptions.delete(id);
          }
        } catch {
          // silently skip on error
        }
      }
    }, this.subscribeEvery);
  }

  /**
   * Registers a function with Resonate for execution and version control.
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
    const name = typeof nameOrFunc === "string" ? nameOrFunc : func.name;

    this.registry.add(func, name, version);

    return {
      run: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> =>
        this.run(id, func, ...this.getArgsAndOpts(args, version)),
      rpc: (id: string, ...args: ParamsWithOptions<F>): Promise<Return<F>> =>
        this.rpc(id, func, ...this.getArgsAndOpts(args, version)),
      beginRun: (id: string, ...args: ParamsWithOptions<F>): Promise<ResonateHandle<Return<F>>> =>
        this.beginRun(id, func, ...this.getArgsAndOpts(args, version)),
      beginRpc: (id: string, ...args: ParamsWithOptions<F>): Promise<ResonateHandle<Return<F>>> =>
        this.beginRpc(id, func, ...this.getArgsAndOpts(args, version)),
      options: this.options,
    };
  }

  public async run<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async run<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async run<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async run(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRun(id, funcOrName, ...args)).result();
  }

  public async beginRun<F extends Func>(
    id: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateHandle<Return<F>>>;
  public async beginRun<T>(id: string, func: string, ...args: any[]): Promise<ResonateHandle<T>>;
  public async beginRun(id: string, funcOrName: Func | string, ...args: any[]): Promise<ResonateHandle<any>>;
  public async beginRun(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<ResonateHandle<any>> {
    const [args, opts] = this.getArgsAndOpts(argsWithOpts);
    const registered = this.registry.get(funcOrName, opts.version);

    // function must be registered
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
            id: id,
            timeoutAt: Date.now() + opts.timeout,
            param: {
              data: {
                func: registered.name,
                args: args,
                retry: opts.retryPolicy?.encode(),
                version: registered.version,
              },
              headers: {},
            },
            tags: {
              ...opts.tags,
              "resonate:origin": id,
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
            { component: "resonate", error: err instanceof Error ? err.message : String(err) },
            "executeUntilBlocked failed",
          ),
        );
    }

    return this.createHandle(promise);
  }

  public async rpc<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async rpc<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async rpc<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async rpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRpc(id, funcOrName, ...args)).result();
  }

  public async beginRpc<F extends Func>(
    id: string,
    func: F,
    ...args: ParamsWithOptions<F>
  ): Promise<ResonateHandle<Return<F>>>;
  public async beginRpc<T>(id: string, func: string, ...args: any[]): Promise<ResonateHandle<T>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<ResonateHandle<any>>;
  public async beginRpc(id: string, funcOrName: Func | string, ...argsWithOpts: any[]): Promise<ResonateHandle<any>> {
    const [args, opts] = this.getArgsAndOpts(argsWithOpts);
    const registered = this.registry.get(funcOrName, opts.version);

    // function must be registered if function pointer is provided
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
        id: id,
        timeoutAt: Date.now() + opts.timeout,
        param: {
          data: {
            func,
            args,
            retry: opts.retryPolicy?.encode(),
            version,
          },
          headers: {},
        },
        tags: {
          ...opts.tags,
          "resonate:origin": id,
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
   * Creates a recurring schedule that invokes a registered function on a cron interval.
   *
   * The schedule is persisted on the Resonate Server and triggers a new durable promise
   * for each cron tick. The generated promise IDs include the schedule name and timestamp
   * to ensure uniqueness across invocations.
   *
   * If a function reference is provided, it must already be registered via {@link register}.
   * Alternatively, a function name (string) can be used for remote dispatch.
   *
   * @param name - A unique name identifying this schedule.
   * @param cron - A cron expression defining the recurrence interval (e.g., `"* * * * *"` for every minute).
   * @param funcOrName - Either the registered function reference or its string name to invoke on each tick.
   * @param args - Positional arguments and optional configuration parameters passed to the function.
   *
   * @returns A {@link ResonateSchedule} handle with a `delete()` method to remove the schedule.
   *
   * @example
   * ```ts
   * const schedule = await resonate.schedule("daily-report", "0 0 * * *", generateReport);
   * // Later, to remove:
   * await schedule.delete();
   * ```
   */
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
    funcOrName: Func | string,
    ...argsWithOpts: any[]
  ): Promise<ResonateSchedule> {
    const [args, opts] = this.getArgsAndOpts(argsWithOpts);
    const registered = this.registry.get(funcOrName, opts.version);

    // function must be registered if function pointer is provided
    if (typeof funcOrName === "function" && !registered) {
      throw exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version);
    }

    // TODO: move this into the handler?
    const { headers, data } = this.codec.encode({
      func: registered ? registered.name : (funcOrName as string),
      args: args,
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

  public async get<T = any>(id: string): Promise<ResonateHandle<T>> {
    id = `${this.idPrefix}${id}`;
    const promise = await this.promiseGet({
      kind: "promise.get",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: {
        id,
      },
    });

    return this.createHandle(promise);
  }

  /**
   * Builds a resolved {@link Options} object by merging the provided partial options
   * with the client's defaults.
   *
   * This is useful for inspecting the effective configuration that would be applied
   * to a `run`, `rpc`, or `schedule` call, or for passing explicit options to the
   * convenience methods on a {@link ResonateFunc} handle.
   *
   * @param opts - Partial options to merge. Supports `tags`, `target`, `timeout`,
   *   `version`, and `retryPolicy`.
   *
   * @returns A fully resolved {@link Options} object.
   */
  public options(
    opts: Partial<Pick<Options, "tags" | "target" | "timeout" | "version" | "retryPolicy">> = {},
  ): Options {
    return this.optsBuilder.build(opts);
  }

  private getArgsAndOpts(args: any[], version?: number): [any[], Options] {
    return util.splitArgsAndOpts(args, this.options({ version }));
  }

  /**
   * Registers a named dependency that will be available to all Resonate functions
   * via `context.getDependency(name)`.
   *
   * Use this to inject services, clients, or configuration objects into Resonate
   * functions without tight coupling.
   *
   * @param name - A unique key identifying the dependency.
   * @param obj - The dependency value (any object or primitive).
   */
  public setDependency(name: string, obj: any): void {
    this.dependencies.set(name, obj);
  }

  /**
   * Stops the Resonate client, releasing all resources.
   *
   * This shuts down the underlying network transport and message source, stops the
   * heartbeat loop, and clears the subscription refresh interval. After calling
   * `stop()`, the client should not be used for further operations.
   */
  public async stop(): Promise<void> {
    await this.network.stop();
    this.heartbeat.stop();
    clearInterval(this.intervalId);
  }

  private async taskCreate(req: TaskCreateReq): Promise<{ promise: PromiseRecord; task?: TaskRecord }> {
    req.data.action.data.param = this.codec.encode(req.data.action.data.param.data);

    const res = await this.send(req);

    if (!isSuccess(res) && !isConflict(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }

    if (isConflict(res)) {
      const promise = await this.promiseRegisterListener({
        kind: "promise.register_listener",
        head: { corrId: randomUUID(), version: util.VERSION },
        data: {
          awaited: req.data.action.data.id,
          address: this.network.unicast,
        },
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
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }

    return this.codec.decodePromise(res.data.promise);
  }

  private async promiseRegisterListener(req: PromiseRegisterListenerReq): Promise<PromiseRecord> {
    const retryDelay = 5000;
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
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }

    return this.codec.decodePromise(res.data.promise);
  }

  private createHandle(promise: PromiseRecord): ResonateHandle<any> {
    const registerListenerReq: PromiseRegisterListenerReq = {
      kind: "promise.register_listener",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: {
        awaited: promise.id,
        address: this.network.unicast,
      },
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
            { component: "resonate", error: err instanceof Error ? err.message : String(err) },
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
      // if no res, we cannot extract timeoutAt information. So we fallback to a large number
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
