import { LocalNetwork } from "../dev/network";
import { Handler } from "../src/handler";
import { Registry } from "../src/registry";
import { WallClock } from "./clock";
import { type Encoder, JsonEncoder } from "./encoder";
import exceptions from "./exceptions";
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

    // Determine the URL based on priority: url arg > RESONATE_URL > RESONATE_HOST+PORT
    let resolvedUrl = url;
    if (!resolvedUrl) {
      if (process.env.RESONATE_URL) {
        resolvedUrl = process.env.RESONATE_URL;
      } else {
        const resonateScheme = process.env.RESONATE_SCHEME ?? "http";
        const resonateHost = process.env.RESONATE_HOST;
        const resonatePort = process.env.RESONATE_PORT ?? "8001";

        if (resonateHost) {
          resolvedUrl = `${resonateScheme}://${resonateHost}:${resonatePort}`;
        }
      }
    }

    // Determine the auth based on priority: auth arg > RESONATE_USERNAME+RESONATE_PASSWORD
    let resolvedAuth = auth;
    if (!resolvedAuth) {
      const resonateUsername = process.env.RESONATE_USERNAME;
      const resonatePassword = process.env.RESONATE_PASSWORD ?? "";

      if (resonateUsername) {
        resolvedAuth = { username: resonateUsername, password: resonatePassword };
      }
    }

    if (!resolvedUrl) {
      const localNetwork = new LocalNetwork();
      this.network = localNetwork;
      this.messageSource = localNetwork.getMessageSource();
      this.heartbeat = new NoopHeartbeat();
    } else {
      this.network = new HttpNetwork({ url: resolvedUrl, auth: resolvedAuth, timeout: 1 * util.MIN, headers: {} });
      this.messageSource = new HttpMessageSource({ url: resolvedUrl, pid, group, auth: resolvedAuth });
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
   * Initializes a Resonate client instance for local development.
   *
   * Creates and returns a Resonate client configured for **local-only execution**
   * with zero external dependencies. All state is stored in local memory — no
   * network or external persistence is required. This mode is ideal for rapid
   * testing, debugging, and experimentation before connecting to a Resonate server.
   *
   * The client runs with a `"default"` worker group, a `"default"` process ID,
   * and an effectively infinite TTL (`Number.MAX_SAFE_INTEGER`) for tasks.
   *
   * @returns A {@link Resonate} client instance configured for local development.
   *
   * @example
   * ```ts
   * const resonate = Resonate.local();
   * resonate.register(foo);
   * const result = await resonate.run("foo.1", foo, { data: "test" });
   * console.log(result);
   * ```
   */
  static local(): Resonate {
    return new Resonate({
      group: "default",
      pid: "default",
      ttl: Number.MAX_SAFE_INTEGER,
    });
  }

  /**
   * Initializes a Resonate client instance with remote configuration.
   *
   * Creates and returns a Resonate client that connects to a **Resonate Server**
   * and optional remote message sources. This configuration enables distributed,
   * durable workers to cooperate and execute functions via **durable RPCs**.
   *
   * By default, the client connects to a Resonate Server running locally
   * (`http://localhost:8001`) and joins the `"default"` worker group.
   *
   * The client is identified by a unique process ID (`pid`) and maintains
   * claimed task leases for the duration specified by `ttl`.
   *
   * @param options - Configuration options for the remote client.
   * @param options.url - The base URL of the remote Resonate Server. Defaults to `"http://localhost:8001"`.
   * @param options.group - The worker group name. Defaults to `"default"`.
   * @param options.pid - Optional process identifier for the client. Defaults to a randomly generated UUID.
   * @param options.ttl - Time-to-live (in seconds) for claimed tasks. Defaults to `1 * util.MIN`.
   * @param options.auth - Optional authentication credentials for connecting to the remote server.
   *
   * @returns A {@link Resonate} client instance configured for remote operation.
   *
   * @example
   * ```ts
   * const resonate = Resonate.remote({
   *   url: "https://resonate.example.com",
   *   group: "analytics",
   *   ttl: 30,
   *   auth: { username: "user", password: "secret" },
   * });
   *
   * const result = await resonate.run("task-42", "processData", { input: "dataset.csv" });
   * console.log(result);
   * ```
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
   * Registers a function with Resonate for execution and version control.
   *
   * This method makes a function available for distributed or top-level execution
   * under a specific name and version.
   *
   * Providing explicit `name` or `version` options allows precise control over
   * function identification and versioning, enabling repeatable, distributed
   * invocation and backward-compatible deployments.
   *
   * @param nameOrFunc - Either the function name (string) or the function itself.
   *   When passing a name, provide the function and optional options as additional parameters.
   * @param funcOrOptions - The function to register, or an optional configuration object
   *   with versioning information when the first argument is a name.
   * @param maybeOptions - Optional configuration object when both name and function are provided.
   *   Supports a `version` field to specify the registered function version.
   *
   * @returns A {@link ResonateFunc} wrapper for the registered function.
   *   When used as a decorator, returns a decorator that registers the target function
   *   upon definition.
   *
   * @example
   * ```ts
   * function greet(ctx: Context, name: string): string {
   *   return `Hello, ${name}!`;
   * }
   *
   * resonate.register("greet_user", greet, { version: 2 });
   * ```
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

  /**
   * Runs a registered function with Resonate and waits for the result.
   *
   * This method executes the specified function under a **durable promise**
   * identified by the provided `id`. If a promise with the same `id` already exists,
   * Resonate subscribes to its result or returns it immediately if it has already completed.
   *
   * Duplicate executions for the same `id` are automatically prevented, ensuring
   * idempotent and consistent behavior across distributed runs.
   *
   * This is a **blocking operation** — execution will not continue until the
   * function result is available.
   *
   * @param id - The unique identifier of the durable promise. Reusing an ID ensures
   *   idempotent execution.
   * @param funcOrName - Either the registered function reference or its string name
   *   to execute.
   * @param args - Positional arguments passed to the function.
   *
   * @returns A promise resolving to the final result returned from the function execution.
   *
   * @example
   * ```ts
   * const result = await client.run("job-123", "processData", { input: "records.csv" });
   * console.log("Result:", result);
   * ```
   */
  public async run<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async run<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async run<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async run(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRun(id, funcOrName, ...args)).result();
  }

  /**
   * Runs a registered function asynchronously with Resonate.
   *
   * This method schedules the specified function for execution under a **durable promise**
   * identified by the provided `id`. If a promise with the same `id` already exists,
   * Resonate subscribes to its result or returns it immediately if it has already completed.
   *
   * Unlike {@link run}, this method is **non-blocking** and immediately returns a
   * {@link ResonateHandle} that can be awaited or queried later to retrieve the final result
   * once execution completes.
   *
   * Duplicate executions for the same `id` are automatically prevented, ensuring idempotent
   * and consistent behavior across distributed runs.
   *
   * @param id - The unique identifier of the durable promise. Reusing an ID ensures
   *   idempotent execution.
   * @param funcOrName - Either the registered function reference or its string name
   *   to execute.
   * @param argsWithOpts - Positional arguments and optional configuration parameters
   *   passed to the function.
   *
   * @returns A {@link ResonateHandle} representing the asynchronous execution.
   *   The handle can be awaited or inspected for status and results.
   *
   * @example
   * ```ts
   * const handle = await client.beginRun("run-001", "generateReport", { period: "Q3" });
   * const result = await handle.getResult();
   * console.log(result);
   * ```
   */
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

    util.assert(registered.version > 0, "function version must be greater than zero");

    const { promise, task } = await this.createPromiseAndTask({
      kind: "createPromiseAndTask",
      promise: {
        id: id,
        timeout: Date.now() + opts.timeout,
        param: {
          data: {
            func: registered.name,
            args: args,
            version: registered.version,
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
   * Executes a registered function remotely with Resonate and waits for the result.
   *
   * This method runs the specified function on a remote worker or process under a
   * **durable promise** identified by the provided `id`. If a promise with the same
   * `id` already exists, Resonate subscribes to its result or returns it immediately
   * if it has already completed.
   *
   * Unlike {@link beginRpc}, this method is **blocking** — it waits for the remote
   * function to complete and returns the final result before continuing execution.
   *
   * Duplicate executions for the same `id` are automatically prevented, ensuring
   * idempotent and consistent behavior across distributed runs.
   *
   * @param id - The unique identifier of the durable promise. Reusing an ID ensures
   *   idempotent remote execution.
   * @param funcOrName - Either the registered function reference or its string name
   *   to execute remotely.
   * @param args - Positional arguments passed to the remote function.
   *
   * @returns A promise resolving to the final result returned from the remote
   *   function execution.
   *
   * @example
   * ```ts
   * const result = await client.rpc("job-42", "analyzeData", { file: "input.csv" });
   * console.log("Remote result:", result);
   * ```
   */
  public async rpc<F extends Func>(id: string, func: F, ...args: ParamsWithOptions<F>): Promise<Return<F>>;
  public async rpc<T>(id: string, name: string, ...args: any[]): Promise<T>;
  public async rpc<T>(id: string, funcOrName: Func | string, ...args: any[]): Promise<T>;
  public async rpc(id: string, funcOrName: Func | string, ...args: any[]): Promise<any> {
    return (await this.beginRpc(id, funcOrName, ...args)).result();
  }

  /**
   * Initiates a remote procedure call (RPC) with Resonate and returns a handle to the execution.
   *
   * This method schedules a registered function for **remote execution** under a durable promise
   * identified by the provided `id`. The function runs on a remote worker or process as part of
   * Resonate’s distributed execution environment.
   *
   * Unlike {@link rpc}, this method is **non-blocking** and immediately returns a
   * {@link ResonateHandle} that can be awaited or queried later to retrieve the final result once
   * remote execution completes.
   *
   * If a durable promise with the same `id` already exists, Resonate subscribes to its result or
   * returns it immediately if it has already completed. Duplicate executions for the same `id`
   * are automatically prevented, ensuring idempotent and consistent behavior.
   *
   * @param id - The unique identifier of the durable promise. Reusing an ID ensures
   *   idempotent remote execution.
   * @param funcOrName - Either the registered function reference or its string name to execute remotely.
   * @param argsWithOpts - Positional arguments and optional configuration parameters
   *   passed to the remote function.
   *
   * @returns A {@link ResonateHandle} representing the asynchronous remote execution.
   *   The handle can be awaited or inspected for completion and results.
   *
   * @example
   * ```ts
   * const handle = await client.beginRpc("task-123", "processData", { input: "hello" });
   * const result = await handle.getResult();
   * console.log(result);
   * ```
   */
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

    const promise = await this.createPromise({
      kind: "createPromise",
      id: id,
      timeout: Date.now() + opts.timeout,
      param: {
        data: {
          func: registered ? registered.name : (funcOrName as string),
          args: args,
          version: registered ? registered.version : opts.version || 1,
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
    const { headers, data } = this.encoder.encode({
      func: registered ? registered.name : (funcOrName as string),
      args: args,
      version: registered ? registered.version : opts.version || 1,
    });

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
   * Retrieves or subscribes to an existing execution by its unique ID.
   *
   * This method attaches to an existing **durable promise** identified by `id`.
   * If the associated execution is still in progress, it returns a {@link ResonateHandle}
   * that can be awaited or observed until completion. If the execution has already
   * finished, the handle is immediately resolved with the stored result.
   *
   * Notes:
   * - A durable promise with the given `id` must already exist.
   * - This operation is **non-blocking**; awaiting the returned handle will block
   *   only if the execution is still running.
   *
   * @param id - Unique identifier of the target execution or durable promise.
   *
   * @returns A {@link ResonateHandle} representing the existing execution.
   *   The handle can be awaited or queried to retrieve the final result.
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

  private getArgsAndOpts(args: any[], version?: number): [any[], Options] {
    return util.splitArgsAndOpts(args, this.options({ version }));
  }

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
            reject(err);
          } else {
            resolve({ promise: res!.promise, task: res!.task });
          }
        },
        undefined,
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
            reject(err);
          } else {
            resolve(res!);
          }
        },
        undefined,
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
            reject(err);
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
          reject(err);
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
