import { WallClock } from "../clock.js";
import { Codec } from "../codec.js";
import { type Encryptor, NoopEncryptor } from "../encryptor.js";
import exceptions, { ResonateTimeoutException } from "../exceptions.js";
import { AsyncHeartbeat, type Heartbeat, NoopHeartbeat } from "../heartbeat.js";
import { validateRootId } from "../ids.js";
import { ConsoleLogger, type Logger, type LogLevel } from "../logger.js";
import { HttpNetwork, PollMessageSource, PushMessageSource } from "../network/http.js";
import { LocalNetwork } from "../network/local.js";
import type { Network } from "../network/network.js";
import type { TokenProvider } from "../network/token.js";
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
import { Core, type Status } from "./core.js";

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

/** What one push came to: the task's status, or why there was none. */
type Outcome = { ok: true; status: Status | undefined } | { ok: false; error: unknown };

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
  private intervalId: ReturnType<typeof setInterval> | undefined;
  /** The push listener, while `handle()` or `listen()` has one open. */
  private listener: { close(): void } | undefined;
  /** Work is pushed over HTTP rather than received from the network. */
  private push = false;
  /** Whether the network is receiving: see `startReceiving`. */
  private receiving = false;
  /** While `handle()` waits for its one execute from the network. */
  private takeOne: ((msg: Message) => void) | undefined;
  /** `handle()` has taken its one: further executes are not this instance's. */
  private handled = false;

  public readonly promises: Promises;
  public readonly schedules: Schedules;

  constructor({
    url = undefined,
    group = "default",
    pid = undefined,
    ttl = 1 * util.MIN,
    token = undefined,
    tokenProvider = undefined,
    timeout = undefined,
    verbose = false,
    logLevel = undefined,
    logger = undefined,
    encryptor = undefined,
    network = undefined,
    push = getEnv("RESONATE_PUSH") === "1",
  }: {
    url?: string;
    group?: string;
    pid?: string;
    ttl?: number;
    token?: string;
    tokenProvider?: TokenProvider;
    timeout?: number;
    verbose?: boolean;
    logLevel?: LogLevel;
    logger?: Logger;
    encryptor?: Encryptor;
    network?: Network;
    /**
     * Handed work rather than asking for it: no polling, no connection held
     * open — work arrives through {@link Resonate.fetch}, {@link Resonate.handle}
     * or {@link Resonate.listen}. For a sandbox
     * guest behind rn8, or a serverless function. Defaults to
     * `RESONATE_PUSH=1`, which rn8 sets.
     */
    push?: boolean;
  } = {}) {
    this.clock = new WallClock();
    this.ttl = ttl;
    this.codec = new Codec(encryptor ?? new NoopEncryptor());

    const resolvedLogLevel: LogLevel = logLevel ?? (verbose ? "debug" : "warn");
    this.logger = logger ?? new ConsoleLogger(resolvedLogLevel);

    this.subscribeEvery = util.MIN;

    const resolvedUrl = url ?? (getEnv("RESONATE_URL") || undefined);
    this.pid = pid ?? randomUUID().replace(/-/g, "");

    let heartbeat: boolean;
    if (network) {
      this.network = network;
      heartbeat = true;
    } else if (push) {
      this.network = new HttpNetwork({
        url: resolvedUrl,
        token,
        tokenProvider,
        timeout,
        headers: {},
        adapter: new PushMessageSource({ group, pid: this.pid }),
        logger: this.logger,
      });
      heartbeat = true;
    } else if (resolvedUrl) {
      const adapter = new PollMessageSource({
        url: `${resolvedUrl}/poll/${encodeURIComponent(group)}/${encodeURIComponent(this.pid)}`,
        token,
        tokenProvider,
        logger: this.logger,
      });
      this.network = new HttpNetwork({
        url: resolvedUrl,
        token,
        tokenProvider,
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
    this.optsBuilder = new OptionsBuilder({ match: this.network.match.bind(this.network) });

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

    this.push = push;
    // Nothing is received yet: work arrives once `listen()` or `handle()` is
    // called, so constructing an instance opens no connection, starts no timer
    // and runs nothing on its own.
  }

  /**
   * Start receiving over the network: execute and unblock messages, and the
   * subscription poll that backs results. Idempotent.
   *
   * Called by `listen()` and `handle()` — and by `run()`/`rpc()` handing back
   * a handle, whose result arrives over the same connection: a client that
   * only calls `run()` gets its answer without listening first.
   */
  private startReceiving(): void {
    if (this.receiving) return;
    this.receiving = true;

    this.network.recv(this.onMessage.bind(this));
    this.network.init().catch((err) => {
      this.logger.error(
        { component: "async-resonate", error: err instanceof Error ? err.message : String(err) },
        "Failed to start network",
      );
    });

    // Pushed work is one task at a time and needs no subscriptions — nor a
    // timer that would keep a handled process from exiting.
    if (this.push) return;

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

    // Validated at the call site that named the workflow, rather than
    // surfacing later as an opaque 400 from the server: the id becomes the
    // origin of its whole lineage, so ':' is rejected outright ('.' is only
    // read below the origin).
    validateRootId(id);
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
              // A genuine top-level root is its own lineage origin, so
              // origin == branch == parent == id here. Every descendant id
              // extends it as `{id}:{lineage}`.
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

    validateRootId(id);
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
          // A genuine top-level root is its own lineage origin, so
          // origin == branch == parent == id here. Every descendant id
          // extends it as `{id}:{lineage}`.
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

    // Each firing creates a root promise named from this id, so the schedule
    // name is bound by the same rules as a run/rpc id. The server stamps the
    // *whole* templated id onto the fired promise's resonate:origin tag, so
    // the template must join with a plain '-': a ':' would hide the timestamp
    // below the origin, collapsing every firing onto one lineage. A '.' is
    // fine now — it is only read below the origin — but '-' keeps the
    // timestamp clearly distinct from any dots in the schedule name.
    validateRootId(name);
    await this.schedules.create(name, cron, "{{.id}}-{{.timestamp}}", opts.timeout, {
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
    // get is a lookup, not a create: it takes any id, including a child's
    // (e.g. "wf:1.2"), so it deliberately does NOT validate.
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

  /**
   * The push endpoint, as a web-standard fetch handler: one pushed `execute`
   * in, its answer out — once the step is over, `{"status":"completed"}` or
   * `{"status":"suspended"}`.
   *
   * The primitive the rest is built on, and what a fetch runtime takes as is:
   *
   * ```ts
   * Deno.serve(resonate.fetch);
   * Bun.serve({ fetch: resonate.fetch });
   * export default { fetch: resonate.fetch };   // Cloudflare Workers
   * ```
   *
   * Bound, so it can be passed around without its instance.
   */
  public readonly fetch = async (request: Request): Promise<Response> => {
    return (await this.respond(request)).response;
  };

  /**
   * Take one task, run it until it completes or suspends, then stop.
   *
   * - With a message — the `execute` a platform delivered — that one.
   * - In push mode, the one push answered over HTTP on `PORT` (default 8080,
   *   on `HOST`, default loopback), through {@link Resonate.fetch}.
   * - Otherwise, the first `execute` the network delivers.
   *
   * The whole life of a sandbox guest — rn8 pushes one task and the process
   * should be gone once it is done:
   *
   * ```ts
   * const resonate = new Resonate();
   * resonate.register("scrape", scrape);
   * await resonate.handle();
   * ```
   *
   * The instance is stopped afterwards: heartbeat, timers, listener and
   * network are released and nothing keeps the process alive.
   */
  public async handle(msg?: Message): Promise<Status | undefined> {
    try {
      if (msg !== undefined) {
        return await this.core.onMessage(msg);
      }
      if (this.push) {
        return await new Promise<Status | undefined>((resolve, reject) => {
          this.serveHttp(true, (outcome) => (outcome.ok ? resolve(outcome.status) : reject(outcome.error))).catch(
            reject,
          );
        });
      }
      const first = await new Promise<Message>((resolve) => {
        this.takeOne = (m) => {
          this.handled = true;
          resolve(m);
        };
        this.startReceiving();
      });
      return await this.core.onMessage(first);
    } finally {
      this.takeOne = undefined;
      await this.stop();
    }
  }

  /**
   * Take tasks until {@link Resonate.stop}: from the network, or — in push
   * mode — as HTTP pushes on `PORT` (default 8080, on `HOST`, default
   * loopback), each answered through {@link Resonate.fetch}.
   *
   * ```ts
   * const resonate = new Resonate();
   * resonate.register("scrapeAll", scrapeAll);
   * await resonate.listen();
   * ```
   *
   * Resolves once receiving has started.
   */
  public async listen(): Promise<void> {
    if (this.push) {
      await this.serveHttp(false, () => {});
      return;
    }
    this.startReceiving();
  }

  /** A push, answered: the Response, and the outcome behind it. */
  private async respond(request: Request): Promise<{ response: Response; outcome: Outcome }> {
    const json = (status: number, body: unknown) =>
      new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

    if (request.method !== "POST") {
      const error = new Error("method not allowed: push with POST");
      return { response: json(405, { error: error.message }), outcome: { ok: false, error } };
    }
    let message: Message;
    try {
      message = (await request.json()) as Message;
    } catch (error) {
      return { response: json(400, { error: "the body is not JSON" }), outcome: { ok: false, error } };
    }
    try {
      const status = await this.core.onMessage(message);
      return {
        response: json(200, { status: status?.kind === "done" ? "completed" : "suspended" }),
        outcome: { ok: true, status },
      };
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      return { response: json(500, { error: text }), outcome: { ok: false, error } };
    }
  }

  /**
   * Node's HTTP server in front of {@link Resonate.respond}: one push, then
   * close (`once`), or until stopped.
   */
  private async serveHttp(once: boolean, done: (outcome: Outcome) => void): Promise<void> {
    // Loaded here, not at the top: the async engine also runs where there is
    // no node:http, and only this path needs it.
    const http = await import("node:http");
    const port = Number(getEnv("PORT") ?? 8080);
    const host = getEnv("HOST") ?? "127.0.0.1";

    const server = http.createServer(async (req, res) => {
      // One push, and only one: anything after it is turned away.
      if (once) server.close();
      const chunks: Uint8Array[] = [];
      for await (const chunk of req) chunks.push(chunk as Uint8Array);
      const headers = new Headers();
      for (const [k, v] of Object.entries(req.headers)) {
        if (typeof v === "string") headers.set(k, v);
      }
      const request = new Request(`http://${req.headers.host ?? `${host}:${port}`}${req.url ?? "/"}`, {
        method: req.method,
        headers,
        body: req.method === "GET" || req.method === "HEAD" ? undefined : Buffer.concat(chunks),
      });

      const { response, outcome } = await this.respond(request);
      res.writeHead(response.status, Object.fromEntries(response.headers));
      res.end(await response.text());
      done(outcome);
    });
    this.listener = server;

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, host, () => resolve());
    });
  }

  public async stop(): Promise<void> {
    this.listener?.close();
    this.listener = undefined;
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
          // Listener registration can be retried for transient server pressure,
          // but semantic client errors cannot become valid by waiting.
          if (res.head.status !== 429 && res.head.status !== 500) {
            throw exceptions.SERVER_ERROR(res.data, false, {
              code: res.head.status,
              message: res.data,
            });
          }
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
    // A handle is a result on its way, and it arrives over the network.
    this.startReceiving();
    const registerListenerReq: PromiseRegisterListenerReq = {
      kind: "promise.register_listener",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: { awaited: promise.id, address: this.network.unicast },
    };

    return {
      id: promise.id,
      done: () =>
        promise.state === "pending"
          ? this.promiseRegisterListener(registerListenerReq).then((res) => res.state !== "pending")
          : Promise.resolve(true),
      result: () =>
        promise.state === "pending"
          ? this.promiseRegisterListener(registerListenerReq).then((res) => this.subscribe(promise.id, res))
          : this.subscribe(promise.id, promise),
    };
  }

  private onMessage(msg: Message): void {
    if (msg.kind === "execute" && this.takeOne !== undefined) {
      // `handle()` takes the first and only the first; anything after it
      // goes back to the server when this instance stops.
      const take = this.takeOne;
      this.takeOne = undefined;
      take(msg);
      return;
    }
    if (msg.kind === "execute" && this.handled) return;
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
