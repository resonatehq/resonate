import { JSONEncoder } from "./core/encoders/json";
import { ErrorCodes, ResonateError } from "./core/errors";
import { ILogger } from "./core/logger";
import { Logger } from "./core/loggers/logger";
import { PartialOptions, Options, InvocationOverrides, ResonateOptions, options } from "./core/options";
import * as durablePromises from "./core/promises/promises";
import { DurablePromiseRecord, handleCompletedPromise } from "./core/promises/types";
import * as retryPolicies from "./core/retry";
import { runWithRetry } from "./core/retry";
import * as schedules from "./core/schedules/schedules";
import { ILockStore, IPromiseStore, IStore } from "./core/store";
import { LocalStore } from "./core/stores/local";
import { RemoteStore } from "./core/stores/remote";
import * as utils from "./core/utils";
import { sleep } from "./core/utils";

/////////////////////////////////////////////////////////////////////
// Types
/////////////////////////////////////////////////////////////////////

type InvocationData = {
  id: string;
  eid: string;
  name: string;
  opts: Options;
};

// The type of a resonate function
export type Func = (ctx: Context, ...args: any[]) => any;

// The args of a resonate function excluding the context argument
export type Params<F> = F extends (ctx: any, ...args: infer P) => any ? P : never;

// The return type of a resonate function
export type Return<F> = F extends (...args: any[]) => infer T ? Awaited<T> : never;

//////////////////////////////////////////////////////////////////////

export class Resonate {
  #registeredFunctions: Record<string, Record<number, { func: Func; opts: Options }>> = {};
  #invocationHandles: Map<string, InvocationHandle<any>>;
  #interval: NodeJS.Timeout | undefined;
  readonly store: IStore;
  readonly logger: ILogger;
  readonly defaultInvocationOptions: Options;
  readonly promises: ResonatePromises;
  readonly schedules: ResonateSchedules;

  constructor({
    auth = undefined,
    encoder = new JSONEncoder(),
    heartbeat = 15000, // 15s
    logger = new Logger(),
    pid = utils.randomId(),
    pollFrequency = 5000, // 5s
    retryPolicy = retryPolicies.exponential(),
    store = undefined,
    tags = {},
    timeout = 10000, // 10s
    url = undefined,
  }: Partial<ResonateOptions> = {}) {
    if (store) {
      this.store = store;
    } else if (url) {
      this.store = new RemoteStore(url, {
        auth,
        heartbeat,
        logger,
        pid,
      });
    } else {
      this.store = new LocalStore({
        auth,
        heartbeat,
        logger,
        pid,
      });
    }

    this.logger = logger;
    this.#invocationHandles = new Map();

    this.defaultInvocationOptions = {
      __resonate: true,
      durable: true,
      eidFn: utils.randomId,
      encoder: encoder,
      idempotencyKeyFn: utils.hash,
      shouldLock: undefined,
      pollFrequency,
      retryPolicy,
      tags,
      timeout,
      version: 0,
    };

    // promises
    this.promises = {
      create: <T>(id: string, timeout: number, opts: Partial<durablePromises.CreateOptions> = {}) =>
        durablePromises.DurablePromise.create<T>(this.promisesStore, encoder, id, timeout, opts),

      resolve: <T>(id: string, value: T, opts: Partial<durablePromises.CompleteOptions> = {}) =>
        durablePromises.DurablePromise.resolve<T>(this.promisesStore, encoder, id, value, opts),

      reject: <T>(id: string, error: any, opts: Partial<durablePromises.CompleteOptions> = {}) =>
        durablePromises.DurablePromise.reject<T>(this.promisesStore, encoder, id, error, opts),

      cancel: <T>(id: string, error: any, opts: Partial<durablePromises.CompleteOptions> = {}) =>
        durablePromises.DurablePromise.cancel<T>(this.promisesStore, encoder, id, error, opts),

      get: <T>(id: string) => durablePromises.DurablePromise.get<T>(this.promisesStore, encoder, id),

      search: (id: string, state?: string, tags?: Record<string, string>, limit?: number) =>
        durablePromises.DurablePromise.search(this.promisesStore, encoder, id, state, tags, limit),
    };

    // schedules
    this.schedules = {
      create: (
        id: string,
        cron: string,
        promiseId: string,
        promiseTimeout: number,
        opts: Partial<schedules.Options> = {},
      ) => schedules.Schedule.create(this.store.schedules, encoder, id, cron, promiseId, promiseTimeout, opts),

      get: (id: string) => schedules.Schedule.get(this.store.schedules, encoder, id),
      search: (id: string, tags?: Record<string, string>, limit?: number) =>
        schedules.Schedule.search(this.store.schedules, encoder, id, tags, limit),
    };
  }

  get promisesStore() {
    return this.store.promises;
  }

  get locksStore() {
    return this.store.locks;
  }

  options(opts: Partial<Options>): PartialOptions {
    return options(opts);
  }

  registeredFunction(funcName: string, version: number): { func: Func; opts: Options } {
    if (!this.#registeredFunctions[funcName]?.[version]) {
      throw new Error(`Function ${funcName} version ${version} not registered`);
    }

    return this.#registeredFunctions[funcName][version];
  }

  register(name: string, func: Func, opts: Partial<Options> = {}): void {
    // set default version
    opts.version = opts.version ?? 1;

    // set default values for the options options
    const options = this.withDefaultOpts(opts);

    if (options.version <= 0) {
      throw new Error("Version must be greater than 0");
    }

    if (!this.#registeredFunctions[name]) {
      this.#registeredFunctions[name] = {};
    }

    if (this.#registeredFunctions[name][options.version]) {
      throw new Error(`Function ${name} version ${options.version} already registered`);
    }

    // Get the highest version number of existing functions with this name
    const latestVersion = Math.max(...Object.values(this.#registeredFunctions[name]).map((f) => f.opts.version));

    // If the new function's version is higher, register it as the latest (index 0)
    if (options.version > latestVersion) {
      this.#registeredFunctions[name][0] = { func, opts: options };
    }

    // register specific version
    this.#registeredFunctions[name][options.version] = { func, opts: options };
  }

  registerModule(module: Record<string, Func>, opts: Partial<Options> = {}) {
    for (const key in module) {
      this.register(key, module[key], opts);
    }
  }

  /**
   * Start the resonate service which continually checks for pending promises
   * every `delay` ms.
   *
   * @param delay Frequency in ms to check for pending promises.
   */
  async start(delay: number = 5000) {
    clearInterval(this.#interval);
    // await the first run of the recovery path to avoid races with the normal flow of the program
    await this.#_start();
    this.#interval = setInterval(this.#_start.bind(this), delay);
  }

  /**
   * Stop the resonate service.
   */
  async stop() {
    clearInterval(this.#interval);
  }

  async #_start() {
    try {
      for await (const promises of this.promisesStore.search("*", "pending", { "resonate:invocation": "true" })) {
        for (const promiseRecord of promises) {
          const param = this.defaultInvocationOptions.encoder.decode(promiseRecord.param.data);
          if (
            param &&
            typeof param === "object" &&
            "func" in param &&
            typeof param.func === "string" &&
            "version" in param &&
            typeof param.version === "number" &&
            "args" in param &&
            Array.isArray(param.args) &&
            "retryPolicy" in param &&
            retryPolicies.isRetryPolicy(param.retryPolicy)
          ) {
            // Since the promise is already created on the server, we should use that idempotencyKey.
            // If for whatever reason it is not, we should recalculate it using our defaults.
            const idempotencyKeyFn = (_: string) => {
              return (
                promiseRecord.idempotencyKeyForCreate ??
                this.defaultInvocationOptions.idempotencyKeyFn(promiseRecord.id)
              );
            };
            await this.invokeLocal(
              param.func,
              promiseRecord.id,
              ...param.args,
              options({
                retryPolicy: param.retryPolicy,
                version: param.version,
                idempotencyKeyFn,
              }),
            );
          }
        }
      }
    } catch (e) {
      // squash all errors and log,
      // transient errors will be ironed out in the next interval
      this.logger.error(e);
    }
  }

  async run<R>(name: string, id: string, ...argsWithOverrides: [...any, InvocationOverrides?]): Promise<R> {
    const handle = await this.invokeLocal<R>(name, id, ...argsWithOverrides);
    return await handle.result();
  }

  async invokeLocal<R>(
    name: string,
    id: string,
    ...argsWithOverrides: [...any, InvocationOverrides?]
  ): Promise<InvocationHandle<R>> {
    if (this.#invocationHandles.has(id)) {
      return this.#invocationHandles.get(id) as InvocationHandle<R>;
    }
    const { args, opts: optionOverrides } = utils.split(argsWithOverrides);

    // version 0 means the latest registered version
    const givenVersion = optionOverrides?.version ?? 0;

    // guarantees we only use the overrides in case the user pass an object with more properties
    const { eidFn, idempotencyKeyFn, retryPolicy, tags, timeout, version } = optionOverrides;
    const { func, opts: registeredOpts } = this.registeredFunction(name, givenVersion);

    const opts: Options = utils.mergeObjects(
      {
        eidFn,
        idempotencyKeyFn,
        retryPolicy,
        tags,
        timeout,
        version,
      },
      registeredOpts,
    );

    // We want to preserve the registered version.
    opts.version = registeredOpts.version;

    // For tags we need to merge the objects themselves
    // giving priority to the passed tags and add the
    // resonate:invocation tag to identify a top level invocation
    opts.tags = { ...registeredOpts.tags, ...tags, "resonate:invocation": "true" };

    // locking is false by default
    opts.shouldLock = opts.shouldLock ?? false;

    const param = {
      func: name,
      version: opts.version,
      retryPolicy: opts.retryPolicy,
      args,
    };

    const idempotencyKey = opts.idempotencyKeyFn(id);
    const storedPromise: DurablePromiseRecord = await this.promisesStore.create(
      id,
      idempotencyKey,
      false,
      undefined,
      opts.encoder.encode(param),
      Date.now() + opts.timeout,
      opts.tags,
    );

    const ctx = Context.createRootContext(this, { id, name, opts, eid: opts.eidFn(id) });
    const resultPromise: Promise<R> = _runFunc<R>(
      func,
      ctx,
      args,
      idempotencyKey,
      storedPromise,
      this.store.locks,
      this.store.promises,
    );

    const handle = new InvocationHandle(id, resultPromise);
    this.#invocationHandles.set(id, handle);
    return handle;
  }

  /**
   * Schedule a resonate function.
   *
   * @param name The schedule name.
   * @param cron The schedule cron expression.
   * @param func The function to schedule.
   * @param args The function arguments.
   * @returns The schedule object.
   */
  async schedule<F extends Func>(
    name: string,
    cron: string,
    func: F,
    ...args: [...Params<F>, PartialOptions?]
  ): Promise<schedules.Schedule>;

  /**
   * Schedule a resonate function that is already registered.
   *
   * @param name The schedule name.
   * @param cron The schedule cron expression.
   * @param func The registered function name.
   * @param args The function arguments.
   * @returns The schedule object.
   */
  async schedule(
    name: string,
    cron: string,
    func: string,
    ...args: [...any, PartialOptions?]
  ): Promise<schedules.Schedule>;

  async schedule(name: string, cron: string, func: Func | string, ...argsWithOpts: any[]): Promise<schedules.Schedule> {
    const { args, opts: givenOpts } = utils.split(argsWithOpts);

    const opts = this.withDefaultOpts(givenOpts);

    if (typeof func === "function") {
      // if function is provided, the default version is 1
      // as opposed to 0 (alias for latest version)
      opts.version = opts.version || 1;
      this.register(name, func, opts);
    }

    const funcName = typeof func === "string" ? func : name;

    const {
      opts: { retryPolicy: retry, version, timeout, tags: promiseTags },
    } = this.registeredFunction(funcName, opts.version);

    const idempotencyKey = opts.idempotencyKeyFn(funcName);

    const promiseParam = {
      func: funcName,
      version,
      retryPolicy: retry,
      args,
    };

    return await this.schedules.create(name, cron, "{{.id}}.{{.timestamp}}", timeout, {
      idempotencyKey,
      promiseParam,
      promiseTags,
    });
  }

  withDefaultOpts(givenOpts: Partial<Options> = {}): Options {
    // merge tags
    const tags = { ...this.defaultInvocationOptions.tags, ...givenOpts.tags };
    return {
      ...this.defaultInvocationOptions,
      ...givenOpts,
      tags,
    };
  }
}

export class Context {
  #resonate: Resonate;
  #stopAllPolling: boolean = false;
  #invocationHandles: Map<string, InvocationHandle<any>>;
  #aborted: boolean;
  #abortCause: any;
  #resources: Map<string, any>;
  #finalizers: (() => Promise<void>)[];
  childrenCount: number;
  readonly invocationData: InvocationData;
  parent: Context | undefined;
  root: Context;

  private constructor(resonate: Resonate, invocationData: InvocationData, parent: Context | undefined) {
    this.#resonate = resonate;
    this.#invocationHandles = new Map();
    this.#resources = new Map();
    this.#finalizers = [];
    this.#aborted = false;
    this.parent = parent;
    this.root = !parent ? this : parent.root;
    this.invocationData = invocationData;
    this.childrenCount = 0;
  }

  static createRootContext(resonate: Resonate, invocationData: InvocationData): Context {
    return new Context(resonate, invocationData, undefined);
  }

  static createChildrenContext(parentCtx: Context, invocationData: InvocationData): Context {
    return new Context(parentCtx.#resonate, invocationData, parentCtx);
  }

  async onRetry(): Promise<void> {
    this.childrenCount = 0;
    await this.finalize();
  }

  async finalize() {
    // It is important to await all promises before finalizing the resources
    // doing it the other way around could cause problems
    await Promise.allSettled(Array.from(this.#invocationHandles, ([_, handle]) => handle.result()));

    // We need to run the finalizers in reverse insertion order since later set finalizers might have
    // a dependency in early set resources
    for (const finalizer of this.#finalizers.reverse()) {
      await finalizer();
    }

    this.#resources.clear();
    this.#finalizers = [];
  }

  abort(cause: any) {
    this.#aborted = true;
    this.#abortCause = cause;
    this.root.#aborted = true;
    this.root.#abortCause = cause;
  }

  get aborted() {
    return this.#aborted;
  }

  get abortCause() {
    return this.#abortCause;
  }

  /**
   * Adds a finalizer function to be executed at the end of the current context.
   * Finalizers are run in reverse order of their definition (last-in, first-out).
   *
   * @param fn - An asynchronous function to be executed as a finalizer.
   *             It should return a Promise that resolves to void.
   *
   * @remarks
   * Finalizer functions must be non fallible.
   */
  addFinalizer(fn: () => Promise<void>) {
    this.#finalizers.push(fn);
  }

  /**
   * Sets a named resource for the current context and optionally adds a finalizer.
   *
   * @param name - A unique string identifier for the resource.
   * @param resource - The resource to be stored. Can be of any type.
   * @param finalizer - Optional. An asynchronous function to be executed when the context ends.
   *                    Finalizers are run in reverse order of their addition to the context and
   *                    must not fail.
   * @throws {Error} Throws an error if a resource with the same name already exists in the current context.
   *
   * This method associates a resource with a unique name in the current context.
   * If a finalizer is provided, it will be executed when the context ends.
   * Finalizers are useful for cleanup operations, such as closing connections or freeing resources.
   */
  setResource(name: string, resource: any, finalizer?: () => Promise<void>): void {
    if (this.#resources.has(name)) {
      throw new Error("Resource already set for this context");
    }

    this.#resources.set(name, resource);
    if (finalizer) {
      this.#finalizers.push(finalizer);
    }
  }

  /**
   * Retrieves a resource by name from the current context or its parent contexts.
   *
   * @template R - The expected type of the resource.
   * @param name - The unique string identifier of the resource to retrieve.
   * @returns The resource of type R if found, or undefined if not found.
   *
   * This method searches for a resource in the following order:
   * 1. In the current context.
   * 2. If not found, it recursively searches in parent contexts.
   * 3. Returns undefined if the resource is not found in any context.
   *
   * @remarks
   * The method uses type assertion to cast the resource to type R.
   * Ensure that the type parameter R matches the actual type of the stored resource
   * to avoid runtime type errors.
   */
  getResource<R>(name: string): R | undefined {
    const resource = this.#resources.get(name);
    if (resource) {
      return resource as R;
    }

    return this.parent ? this.parent.getResource<R>(name) : undefined;
  }

  options(opts: Partial<Options>): PartialOptions {
    return options(opts);
  }

  /**
   * Invoke a remote function.
   *
   * @template R The return type of the remote function.
   * @param func The id of the remote function.
   * @param args The arguments to pass to the remote function.
   * @param opts Optional {@link options}.
   * @returns A promise that resolves to the resolved value of the remote function.
   */
  async run<R>(funcId: string, args: any, opts?: PartialOptions): Promise<R>;

  /**
   * Invoke a function.
   *
   * @template F The type of the function.
   * @param func The function to invoke.
   * @param args The function arguments, optionally followed by {@link options}.
   * @returns A promise that resolves to the return value of the function.
   */
  async run<F extends Func>(func: F, ...argsWithOpts: [...Params<F>, PartialOptions?]): Promise<Return<F>>;

  async run<F extends Func, R>(
    funcOrId: F | string,
    ...argsWithOpts: [...Params<F>, PartialOptions?]
  ): Promise<ReturnType<F> | R> {
    let handle!: InvocationHandle<R>;
    if (typeof funcOrId === "string") {
      handle = await this.invokeRemote<R>(funcOrId, ...argsWithOpts);
    } else {
      handle = await this.invokeLocal<F, ReturnType<F>>(funcOrId, ...argsWithOpts);
    }
    return await handle.result();
  }

  async invokeRemote<R>(funcId: string, ...argsWithOpts: [...any, PartialOptions?]): Promise<InvocationHandle<R>> {
    if (this.#invocationHandles.has(funcId)) {
      return this.#invocationHandles.get(funcId) as InvocationHandle<R>;
    }

    const { opts: givenOpts } = utils.split(argsWithOpts);

    const { opts: registeredOpts } = this.#resonate.registeredFunction(
      this.root.invocationData.name,
      this.root.invocationData.opts.version,
    );

    const opts = { ...registeredOpts, ...givenOpts };

    // Merge the tags
    opts.tags = { ...registeredOpts.tags, ...givenOpts?.tags };

    // Default lock is false for children execution
    opts.shouldLock = opts.shouldLock ?? false;

    // Children execution do not need params since we don't go trough the recovery path with children
    const param = {};

    const idempotencyKey = opts.idempotencyKeyFn(funcId);
    const storedPromise: DurablePromiseRecord = await this.#resonate.promisesStore.create(
      funcId,
      idempotencyKey,
      false,
      undefined,
      opts.encoder.encode(param),
      Date.now() + opts.timeout,
      opts.tags,
    );

    const runFunc = async (): Promise<R> => {
      while (!this.#stopAllPolling) {
        const durablePromiseRecord: DurablePromiseRecord = await this.#resonate.promisesStore.get(storedPromise.id);
        if (durablePromiseRecord.state !== "PENDING") {
          return handleCompletedPromise(durablePromiseRecord, opts.encoder);
        }
        // TODO: Consider using exponential backoff instead.
        sleep(opts.pollFrequency);
      }

      throw new Error(`Polling of remote invocation with ${funcId} was stopped`);
    };
    const resultPromise: Promise<R> = runFunc();
    const invocationHandle = new InvocationHandle(funcId, resultPromise);
    this.#invocationHandles.set(funcId, invocationHandle);

    return invocationHandle;
  }

  async invokeLocal<F extends Func, R>(
    func: F,
    ...argsWithOpts: [...Params<F>, PartialOptions?]
  ): Promise<InvocationHandle<R>> {
    const { args, opts: givenOpts } = utils.split(argsWithOpts);
    const { opts: registeredOpts } = this.#resonate.registeredFunction(
      this.root.invocationData.name,
      this.root.invocationData.opts.version,
    );

    const opts = { ...registeredOpts, ...givenOpts };

    // Merge the tags
    opts.tags = { ...registeredOpts.tags, ...givenOpts.tags };

    // Default lock is false for children execution
    opts.shouldLock = opts.shouldLock ?? false;

    this.childrenCount++;
    // If it is an anonymous function give at anon name nested within the current invocation name
    const name = func.name ? func.name : `${this.invocationData.name}__anon${this.childrenCount}`;
    const id = `${this.invocationData.id}.${this.childrenCount}.${name}`;

    if (this.#invocationHandles.has(id)) {
      return this.#invocationHandles.get(id) as InvocationHandle<R>;
    }
    const ctx = Context.createChildrenContext(this, { name, id, eid: opts.eidFn(id), opts });

    if (!opts.durable) {
      const runFunc = async () => {
        const timeout = Date.now() + opts.timeout;
        return (await runWithRetry(
          async () => await func(ctx, ...args),
          async () => await ctx.onRetry(),
          opts.retryPolicy,
          timeout,
        )) as R;
      };
      const resultPromise = runFunc();
      const handle = new InvocationHandle<R>(id, resultPromise);
      this.#invocationHandles.set(id, handle);
      return handle;
    }

    // Children execution do not need params since we don't go trough the recovery path with children
    const param = {};

    const idempotencyKey = opts.idempotencyKeyFn(id);
    const storedPromise: DurablePromiseRecord = await this.#resonate.promisesStore.create(
      id,
      idempotencyKey,
      false,
      undefined,
      opts.encoder.encode(param),
      Date.now() + opts.timeout,
      opts.tags,
    );

    const resultPromise: Promise<R> = _runFunc<R>(
      func,
      ctx,
      args,
      idempotencyKey,
      storedPromise,
      this.#resonate.store.locks,
      this.#resonate.store.promises,
    );

    const invocationHandle = new InvocationHandle(id, resultPromise);
    this.#invocationHandles.set(id, invocationHandle);

    return invocationHandle;
  }

  /**
   * Durable version of sleep.
   * Sleep for the specified time (ms).
   *
   * @param ms Amount of time to sleep in milliseconds.
   * @returns A Promise that resolves after the specified time has elapsed.
   */
  async sleep(ms: number): Promise<void> {
    const id = `${this.invocationData.id}.${this.childrenCount++}`;
    const handle = await this.invokeRemote(
      id,
      options({ timeout: ms, pollFrequency: ms, tags: { "resonate:timeout": "true" }, durable: true }),
    );

    await handle.result();
  }

  /**
   * Creates a Promise that is resolved with an array of results when all of the provided Promises
   * resolve, or rejected when any Promise is rejected.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  all<T extends readonly unknown[] | []>(
    values: T,
    opts: Partial<Options> = {},
  ): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.all will not be called in the case where the
    // durable promise already completed
    for (const v of values) {
      if (v instanceof Promise) v.catch(() => {});
    }

    // prettier-ignore
    return this.run(() => Promise.all(values), options({
      retryPolicy: retryPolicies.never(),
      ...opts,
    }));
  }

  /**
   * Creates a Promise that is fulfilled by the first given promise to be fulfilled, or rejected
   * with an AggregateError.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  any<T extends readonly unknown[] | []>(values: T, opts: Partial<Options> = {}): Promise<Awaited<Awaited<T[number]>>> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.any will not be called in the case where the
    // durable promise already completed
    for (const v of values) {
      if (v instanceof Promise) v.catch(() => {});
    }

    // prettier-ignore
    return this.run(() => Promise.any(values), options({
      retryPolicy: retryPolicies.never(),
      ...opts,
    }));
  }

  /**
   * Creates a Promise that is resolved or rejected when any of the provided Promises are resolved
   * or rejected.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new ResonatePromise.
   */
  race<T extends readonly unknown[] | []>(values: T, opts: Partial<Options> = {}): Promise<Awaited<T[number]>> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.race will not be called in the case where the
    // durable promise already completed
    for (const v of values) {
      if (v instanceof Promise) v.catch(() => {});
    }

    // prettier-ignore
    return this.run(() => Promise.race(values), options({
      retryPolicy: retryPolicies.never(),
      ...opts,
    }));
  }

  /**
   * Creates a Promise that is resolved with an array of results when all of the provided Promises
   * resolve or reject.
   *
   * @param values An array of Promises.
   * @param opts Optional {@link options}.
   * @returns A new Promise.
   */
  allSettled<T extends readonly unknown[] | []>(
    values: T,
    opts: Partial<Options> = {},
  ): Promise<{ -readonly [P in keyof T]: PromiseSettledResult<Awaited<T[P]>> }> {
    // catch all promises to prevent unhandled promise rejections,
    // since Promise.allSettled will not be called in the case where the
    // durable promise already completed
    for (const v of values) {
      if (v instanceof Promise) v.catch(() => {});
    }

    // prettier-ignore
    return this.run(() => Promise.allSettled(values), options({
      retryPolicy: retryPolicies.never(),
      ...opts,
    }));
  }

  /**
   * Invoke a Resonate function in detached mode. Functions must first be registered with Resonate.
   * a detached invocation will not be implecitly awaited at the end of the current context, instead
   * it will be "supervised" as a top level invocation.
   *
   * @template R The return type of the function.
   * @param id A unique id for the function invocation.
   * @param name The function name.
   * @param argsWithOverrides The function arguments and options overrides.
   * @returns A Res.
   */
  async detached<R>(
    name: string,
    id: string,
    ...argsWithOverrides: [...any, InvocationOverrides?]
  ): Promise<InvocationHandle<R>> {
    return await this.#resonate.invokeLocal(name, id, ...argsWithOverrides);
  }
}

export interface ResonatePromises {
  /**
   * Create a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param timeout Time (in milliseconds) after which the promise is considered expired.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  create<T>(
    id: string,
    timeout: number,
    opts?: Partial<durablePromises.CreateOptions>,
  ): Promise<durablePromises.DurablePromise<T>>;

  /**
   * Resolve a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param value The resolved value.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  resolve<T>(
    id: string,
    value: T,
    opts?: Partial<durablePromises.CompleteOptions>,
  ): Promise<durablePromises.DurablePromise<T>>;

  /**
   * Reject a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param error The reject value.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  reject<T>(
    id: string,
    error: any,
    opts?: Partial<durablePromises.CompleteOptions>,
  ): Promise<durablePromises.DurablePromise<T>>;

  /**
   * Cancel a durable promise.
   *
   * @template T The type of the promise.
   * @param id Unique identifier for the promise.
   * @param error The cancel value.
   * @param opts Additional options.
   * @returns A durable promise.
   */
  cancel<T>(
    id: string,
    error: any,
    opts?: Partial<durablePromises.CompleteOptions>,
  ): Promise<durablePromises.DurablePromise<T>>;

  /**
   * Get a durable promise.
   *
   * @template T The type of the promise.
   * @param id Id of the promise.
   * @returns A durable promise.
   */
  get<T>(id: string): Promise<durablePromises.DurablePromise<T>>;

  /**
   * Search durable promises.
   *
   * @param id Ids to match, can include wildcards.
   * @param state State to match.
   * @param tags Tags to match.
   * @param limit Maximum number of durablePromises to return.
   * @returns A generator that yields durable durablePromises.
   */
  search(
    id: string,
    state?: string,
    tags?: Record<string, string>,
    limit?: number,
  ): AsyncGenerator<durablePromises.DurablePromise<any>[]>;
}

export interface ResonateSchedules {
  /**
   * Create a new schedule.
   *
   * @param id Unique identifier for the schedule.
   * @param cron CRON expression defining the schedule's execution time.
   * @param promiseId Unique identifier for the associated promise.
   * @param promiseTimeout Timeout for the associated promise in milliseconds.
   * @param opts Additional options.
   * @returns A schedule.
   */
  create(
    id: string,
    cron: string,
    promiseId: string,
    promiseTimeout: number,
    opts?: Partial<schedules.Options>,
  ): Promise<schedules.Schedule>;

  /**
   * Get a schedule.
   *
   * @param id Id of the schedule.
   * @returns A schedule.
   */
  get(id: string): Promise<schedules.Schedule>;

  /**
   * Search for schedules.
   *
   * @param id Ids to match, can include wildcards.
   * @param tags Tags to match.
   * @param limit Maximum number of schedules to return.
   * @returns A generator that yields schedules.
   */
  search(
    id: string,
    tags: Record<string, string> | undefined,
    limit?: number,
  ): AsyncGenerator<schedules.Schedule[], void>;
}

export class InvocationHandle<R> {
  constructor(
    readonly invocationId: string,
    readonly resultPromise: Promise<R>,
  ) {}

  /**
   * get the current state of the resultPromise.
   *
   */
  async state(): Promise<"pending" | "resolved" | "rejected"> {
    return await utils.promiseState(this.resultPromise);
  }

  async result(): Promise<R> {
    return this.resultPromise;
  }
}

const acquireLock = async (id: string, eid: string, locksStore: ILockStore): Promise<boolean> => {
  try {
    return await locksStore.tryAcquire(id, eid);
  } catch (e: unknown) {
    // if lock is already acquired, return false so we can poll
    if (e instanceof ResonateError && e.code === ErrorCodes.STORE_FORBIDDEN) {
      return false;
    }

    throw e;
  }
};

const _runFunc = async <R>(
  func: Func,
  ctx: Context,
  args: Params<Func>,
  idempotencyKey: string,
  storedPromise: DurablePromiseRecord,
  locksStore: ILockStore,
  promisesStore: IPromiseStore,
): Promise<R> => {
  const { id, eid, opts } = ctx.invocationData;

  // If the promise that comes back from the server is already completed, resolve or reject right away.
  if (storedPromise.state !== "PENDING") {
    return handleCompletedPromise(storedPromise, opts.encoder);
  }

  // storedPromise.state === "PENDING"
  try {
    // Acquire the lock if necessary
    if (opts.shouldLock) {
      while (!(await acquireLock(id, eid, locksStore))) {
        await sleep(opts.pollFrequency);
      }
    }

    let error: any;
    let value!: R;

    // we need to hold on to a boolean to determine if the function was successful,
    // we cannot rely on the value or error as these values could be undefined
    let success = true;
    try {
      value = await runWithRetry(
        async () => await func(ctx, ...args),
        async () => await ctx.onRetry(),
        opts.retryPolicy,
        storedPromise.timeout,
      );
    } catch (e) {
      // We need to capture the error to be able to reject the durable promise,
      // after that we will then propagate this error by rejecting the result promise
      error = e;
      success = false;
    } finally {
      // Resonate will implicitly await all the invocationHandles as the last
      // thing it does with the context before it goes out of scope
      await ctx.finalize();
    }

    if (ctx.root.aborted) {
      throw new ResonateError("Unrecoverable Error: Aborting", ErrorCodes.ABORT, ctx.root.abortCause);
    }

    let completedPromiseRecord!: DurablePromiseRecord;
    if (success) {
      completedPromiseRecord = await promisesStore.resolve(
        id,
        idempotencyKey,
        false,
        storedPromise.value.headers,
        opts.encoder.encode(value),
      );
    } else {
      completedPromiseRecord = await promisesStore.reject(
        id,
        idempotencyKey,
        false,
        storedPromise.value.headers,
        opts.encoder.encode(error),
      );
    }

    // Because of eventual consistency and recovery paths it is possible that we get a
    // rejected promise even if we did call `resolve` on it or the other way around.
    // What should never happen is that we get a "PENDING" promise
    return handleCompletedPromise(completedPromiseRecord, opts.encoder);
  } catch (err) {
    if (err instanceof ResonateError && (err.code === ErrorCodes.CANCELED || err.code === ErrorCodes.TIMEDOUT)) {
      // Cancel and timeout errors, just forward them
      throw err;
    } else if (err instanceof ResonateError && err.code !== ErrorCodes.ABORT) {
      // Any other instance of ResonateError we must abort the current execution.
      ctx.abort(err);
      throw new ResonateError("Unrecoverable Error: Aborting", ErrorCodes.ABORT, err);
    } else {
      throw err;
    }
  } finally {
    // release lock if necessary
    if (opts.shouldLock) {
      await locksStore.release(id, eid);
    }
  }
};
