import type { Clock } from "../clock.js";
import exceptions, { type ResonateError } from "../exceptions.js";
import type { PromiseCreateReq, PromiseRecord } from "../network/types.js";
import type { Options, OptionsBuilder } from "../options.js";
import { delay, randomUUID } from "../platform.js";
import type { Registry } from "../registry.js";
import { Never, type RetryPolicy } from "../retries.js";
import type { TraceCollector } from "../trace.js";
import type { Effects } from "../types.js";
import * as util from "../util.js";

// ---------------------------------------------------------------------------
// Function typing (async-local, decoupled from the generator `Func`)
//
// The runtime always passes the full `Context` to every registered function;
// users type the first parameter as `Info` (leaf) or `Context` (workflow).
// `ctx: any` here lets both shapes register and call without variance pain.
// ---------------------------------------------------------------------------

export type AnyFunc = (ctx: any, ...args: any[]) => any;
export type Params<F> = F extends (ctx: any, ...args: infer P) => any ? P : never;
export type ParamsWithOptions<F> = [...Params<F>, Options?];
export type Return<F> = F extends (...args: any[]) => infer R ? Awaited<R> : never;

/**
 * Read-only execution metadata available to every function. A "leaf" function
 * types its first parameter as `Info` — it gets identity + dependencies but no
 * durable operations, so it never suspends and always completes in one pass.
 */
export interface Info {
  readonly id: string;
  readonly parentId: string;
  readonly originId: string;
  /** The id-generation prefix (resonate:prefix). Set once at the top and
   * propagated down unchanged — through every child AND across detached
   * re-roots — so recursive detached ids stay bounded. */
  readonly prefixId: string;
  readonly branchId: string;
  readonly timeoutAt: number;
  readonly attempt: number;
  readonly version: number;
  readonly func: string;

  getDependency<T = any>(key: string): T | undefined;
}

/**
 * Full durable API available to a "workflow" function (first parameter typed
 * `Context`). All operations are eager — calling `run`/`rpc`/`sleep` begins the
 * work immediately and returns an already-running {@link DurablePromise}.
 * Awaiting one now is call-and-wait; holding several and awaiting them later is
 * fan-out.
 *
 * A pending durable promise makes the returned promise **hang** (never settle)
 * for this pass — the engine ends the pass out of band. So `await` here behaves
 * like a normal JS promise: it only ever throws a real rejection.
 */
export interface Context extends Info {
  run<F extends AnyFunc>(func: F, ...args: ParamsWithOptions<F>): DurablePromise<Return<F>>;
  run<T>(func: string, ...args: any[]): DurablePromise<T>;

  rpc<F extends AnyFunc>(func: F, ...args: ParamsWithOptions<F>): DurablePromise<Return<F>>;
  rpc<T>(func: string, ...args: any[]): DurablePromise<T>;

  /**
   * Spawns a workflow as a fresh root promise — independent execution lifecycle
   * and replay scope (lineage break, new originId). Unlike `run`/`rpc`, the
   * spawned workflow is not a child of the current invocation: it survives parent
   * completion and is dispatched independently by the server.
   *
   * The returned promise resolves with a {@link DetachedHandle} once the spawn is
   * durably created — it does NOT wait for, and the parent does NOT suspend on,
   * the detached workflow's result. This is the primitive behind the bounded
   * replay forever-loop (recursive-tail) pattern.
   *
   * @example
   * ```ts
   * async function playGame(ctx: Context, n: number) {
   *   // ...play one game...
   *   await ctx.detached(playGame, n + 1); // last statement, then return
   * }
   * ```
   */
  detached<F extends AnyFunc>(func: F, ...args: ParamsWithOptions<F>): DurablePromise<DetachedHandle>;
  detached(func: string, ...args: any[]): DurablePromise<DetachedHandle>;

  /**
   * Creates a latent durable promise (DPC) with no associated function, intended
   * to be resolved out of band (human-in-the-loop / external signal) via
   * `resonate.promises.resolve(id)`. Awaiting it suspends the parent until the
   * promise is settled externally. The id is `${ctx.id}.${seq}`.
   */
  promise<T>(opts?: { timeout?: number; data?: any; tags?: { [key: string]: string } }): DurablePromise<T>;

  sleep(ms: number): DurablePromise<void>;
  sleep(opts: { for?: number; until?: Date }): DurablePromise<void>;
  sleep(msOrOpts: number | { for?: number; until?: Date }): DurablePromise<void>;

  options(opts?: Partial<Omit<Options, "id">>): Options;

  /**
   * Aborts the entire pass if condition is true (`panic`) / false (`assert`):
   * no promise is settled and the task is released for redelivery — mirroring
   * the generator engine's DIE. The abort is recorded on the context before
   * the throw, so a surrounding user try/catch cannot suppress it.
   */
  panic(condition: boolean, msg?: string): void;
  assert(condition: boolean, msg?: string): void;

  date: { now(): DurablePromise<number> };
  math: { random(): DurablePromise<number> };
}

/**
 * The internal outcome of a durable operation. Unlike the user-facing promise
 * (which may hang on suspend), an outcome ALWAYS settles, so the pass's drain
 * can read it without hanging.
 */
export type Outcome =
  | { kind: "value"; value: any }
  | { kind: "error"; error: any }
  | { kind: "suspended"; remote: string[] };

/** Lightweight reference to a detached (independently-rooted) workflow. */
export interface DetachedHandle {
  readonly id: string;
}

/**
 * The branded handle returned by every durable operation on {@link Context}
 * (`run`, `rpc`, `sleep`, `detached`, `promise`). Behaves exactly like the
 * user-facing promise it wraps (then/catch/finally/await, including the
 * hang-on-suspend semantics) and additionally exposes the durable promise `id`.
 *
 * The brand marks the awaitables that are safe inside a workflow: replay
 * determinism requires that `await` only ever targets durable promises —
 * awaiting a timer, I/O, or a plain async-helper chain lets durable ids be
 * assigned in completion order, which cross-wires them on replay.
 */
export class DurablePromise<T> implements Promise<T> {
  readonly [Symbol.toStringTag] = "DurablePromise";

  constructor(
    /** The durable promise id (`""` when the op failed before creating one). */
    readonly id: string,
    private readonly promise: Promise<T>,
  ) {}

  // biome-ignore lint/suspicious/noThenProperty: being awaitable is this class's purpose
  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2> {
    return this.promise.then(onfulfilled, onrejected);
  }

  catch<TResult = never>(onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | null): Promise<T | TResult> {
    return this.promise.catch(onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<T> {
    return this.promise.finally(onfinally);
  }
}

interface AsyncContextConfig {
  id: string;
  oId?: string;
  prId?: string;
  bId?: string;
  pId?: string;
  func: string;
  clock: Clock;
  registry: Registry;
  dependencies: Map<string, any>;
  optsBuilder: OptionsBuilder;
  timeout: number;
  version: number;
  attempt?: number;
}

export class AsyncContext implements Context {
  readonly id: string;
  readonly originId: string;
  readonly prefixId: string;
  readonly branchId: string;
  readonly parentId: string;
  readonly func: string;
  readonly timeoutAt: number;
  readonly attempt: number;
  readonly version: number;

  readonly clock: Clock;
  private registry: Registry;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private effects: Effects;
  /** Lifecycle-event sink for this pass; shared with the root + every child. */
  private trace: TraceCollector;

  private seq = 0;
  /** Set once the pass is finalized (drain stable); durable ops then panic. */
  private closed = false;

  // --- per-pass bookkeeping (owned by the pass; never escapes the pass) ---

  /** Every started op's always-settling outcome; the pass drains these. */
  readonly spawnedHandles: Promise<Outcome>[] = [];
  /** Resolved the first time any op observes a pending remote → ends the pass. */
  readonly suspendSignal: Promise<void>;
  private _fireSuspend!: () => void;
  /** Resolved by ctx.panic → ends the pass; the recorded error aborts it. */
  readonly panicSignal: Promise<void>;
  private _firePanic!: () => void;
  private _panicError?: ResonateError;
  /** Tail of the creation sequencer chain (serializes promiseCreate calls). */
  private createTail: Promise<void> = Promise.resolve();

  constructor(cfg: AsyncContextConfig, effects: Effects, trace: TraceCollector) {
    this.id = cfg.id;
    this.originId = cfg.oId ?? cfg.id;
    this.prefixId = cfg.prId ?? cfg.id;
    this.branchId = cfg.bId ?? cfg.id;
    this.parentId = cfg.pId ?? cfg.id;
    this.func = cfg.func;
    this.clock = cfg.clock;
    this.registry = cfg.registry;
    this.dependencies = cfg.dependencies;
    this.optsBuilder = cfg.optsBuilder;
    this.timeoutAt = cfg.timeout;
    this.version = cfg.version;
    this.attempt = cfg.attempt ?? 1;
    this.effects = effects;
    this.trace = trace;

    this.suspendSignal = new Promise<void>((resolve) => {
      this._fireSuspend = resolve;
    });
    this.panicSignal = new Promise<void>((resolve) => {
      this._firePanic = resolve;
    });
  }

  /** The panic that aborted this pass, if any. Authoritative: set by ctx.panic
   * before the throw, so it survives a user try/catch around the call. */
  get panicError(): ResonateError | undefined {
    return this._panicError;
  }

  /** End the pass: a durable op observed a pending remote. Idempotent. */
  fireSuspend(): void {
    this._fireSuspend();
  }

  /**
   * Finalize the pass. Every op in `spawnedHandles` has been drained; a durable
   * op started after this point comes from a continuation that outlived the pass
   * (parked on a non-durable await — a timer, I/O, etc.). It would escape the
   * drain and race task.suspend/fulfill as a zombie, so it panics instead.
   */
  close(): void {
    this.closed = true;
  }

  private guardOpen(op: string): void {
    if (this.closed) {
      throw exceptions.PANIC(
        "async-context",
        `ctx.${op} called after the pass ended (id=${this.id}). A durable operation ` +
          `must not follow a non-durable await (timer, I/O, ...) in a workflow — ` +
          `the pass can end while such a continuation is still parked.`,
      );
    }
  }

  private child(cfg: { id: string; func: string; timeout: number; version: number; attempt?: number }): AsyncContext {
    return new AsyncContext(
      {
        id: cfg.id,
        oId: this.originId,
        prId: this.prefixId,
        bId: this.branchId,
        pId: this.id,
        func: cfg.func,
        clock: this.clock,
        registry: this.registry,
        dependencies: this.dependencies,
        optsBuilder: this.optsBuilder,
        timeout: cfg.timeout,
        version: cfg.version,
        attempt: cfg.attempt,
      },
      this.effects,
      this.trace,
    );
  }

  // --- creation sequencer -------------------------------------------------

  // Take the current tail as the predecessor and install a fresh tail. Called
  // synchronously in the op prologue so the chain reflects source order.
  private claimCreateSlot(): { ready: Promise<void>; release: () => void } {
    const ready = this.createTail;
    let release!: () => void;
    this.createTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    return { ready, release };
  }

  // Run one op's sequenced create: wait our turn in the chain, create the
  // promise, then release the slot so the next op can proceed.
  private async createSlotted(
    createReq: PromiseCreateReq,
    name: string,
    slot: { ready: Promise<void>; release: () => void },
  ): Promise<PromiseRecord> {
    await slot.ready;
    try {
      return await this.effects.promiseCreate(createReq, name);
    } finally {
      slot.release();
    }
  }

  // --- durable operations -------------------------------------------------

  run(funcOrName: AnyFunc | string, ...args: any[]): DurablePromise<any> {
    this.guardOpen("run");
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "string" && !registered) {
      // No durable promise was created (and no seq consumed) — id is empty.
      return this.track(
        "",
        Promise.resolve<Outcome>({
          kind: "error",
          error: exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName, opts.version),
        }),
      );
    }

    const idChanged = opts.id !== undefined;
    const id = idChanged ? (opts.id as string) : this.seqid();
    this.seq++;

    const func = (registered ? registered.func : funcOrName) as AnyFunc;
    const version = registered ? registered.version : 1;
    const createReq = this.localCreateReq({ id, data: { func: func.name, version }, opts, breaksLineage: idChanged });
    const slot = this.claimCreateSlot();

    // Async functions are indistinguishable from workflows at runtime (no
    // generator marker), so retries are opt-in: default Never, pass an explicit
    // policy via opts to enable them.
    const retryPolicy = opts.retryPolicy ?? new Never();
    const nonRetryableErrors = opts.nonRetryableErrors ?? [];

    const outcome = (async (): Promise<Outcome> => {
      const rec = await this.createSlotted(createReq, func.name, slot);

      // run p→q: the parent created a local child (emitted after create, before
      // the spawn/dedup split, mirroring the generator coroutine).
      this.trace.emit({ kind: "run", id: this.id, callee: id });

      if (rec.state !== "pending") {
        // Replay: the child is already settled → dedup, no spawn/return.
        const state = rec.state === "resolved" ? "resolved" : "rejected";
        this.trace.emit({ kind: "dedup", id, state, value: rec.value?.data });
        return recordOutcome(rec);
      }

      // Pending: spawn the child, then execute it in-process via the recursive
      // runner, retrying failed attempts per the policy (each attempt gets a fresh
      // child context so durable child ids replay identically via dedup).
      this.trace.emit({ kind: "spawn", id });

      const childOutcome = await runWithRetry(
        (attempt) => this.child({ id, func: func.name, timeout: rec.timeoutAt, version, attempt }),
        func,
        argu,
        retryPolicy,
        nonRetryableErrors,
        this.clock,
        rec.timeoutAt,
      );

      if (childOutcome.kind === "suspended") {
        // The child has remote deps of its own and could not complete this pass.
        this.trace.emit({ kind: "suspend", id });
        this.fireSuspend();
        return childOutcome;
      }

      await this.settle(id, childOutcome, func.name);
      const state = childOutcome.kind === "value" ? "resolved" : "rejected";
      const value = childOutcome.kind === "value" ? childOutcome.value : childOutcome.error;
      this.trace.emit({ kind: "return", id, state, value });
      return childOutcome;
    })();

    return this.track(id, outcome);
  }

  rpc(funcOrName: AnyFunc | string, ...args: any[]): DurablePromise<any> {
    this.guardOpen("rpc");
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      // No durable promise was created (and no seq consumed) — id is empty.
      return this.track(
        "",
        Promise.resolve<Outcome>({
          kind: "error",
          error: exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version),
        }),
      );
    }

    const idChanged = opts.id !== undefined;
    const id = idChanged ? (opts.id as string) : this.seqid();
    this.seq++;

    const func = registered ? registered.name : (funcOrName as string);
    const version = registered ? registered.version : opts.version || 1;
    const data = { func, args: argu, retry: opts.retryPolicy?.encode(), version };
    const createReq = this.remoteCreateReq({ id, data, opts, breaksLineage: idChanged });
    const slot = this.claimCreateSlot();

    return this.track(id, this.remoteOutcome(id, createReq, slot, func));
  }

  sleep(ms: number): DurablePromise<void>;
  sleep(opts: { for?: number; until?: Date }): DurablePromise<void>;
  sleep(msOrOpts: number | { for?: number; until?: Date }): DurablePromise<void> {
    this.guardOpen("sleep");
    let time: number;
    if (typeof msOrOpts === "number") {
      time = this.clock.now() + msOrOpts;
    } else if (msOrOpts.for != null) {
      time = this.clock.now() + msOrOpts.for;
    } else if (msOrOpts.until != null) {
      time = msOrOpts.until.getTime();
    } else {
      time = 0;
    }

    const id = this.seqid();
    this.seq++;
    const createReq = this.sleepCreateReq({ id, time });
    const slot = this.claimCreateSlot();
    return this.track(id, this.remoteOutcome(id, createReq, slot, "sleep"));
  }

  detached(funcOrName: AnyFunc | string, ...args: any[]): DurablePromise<DetachedHandle> {
    this.guardOpen("detached");
    const [argu, opts] = util.splitArgsAndOpts(args, this.options());
    const registered = this.registry.get(funcOrName, opts.version);

    if (typeof funcOrName === "function" && !registered) {
      const errOutcome = Promise.resolve<Outcome>({
        kind: "error",
        error: exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(funcOrName.name, opts.version),
      });
      // No durable promise was created (and no seq consumed) — id is empty.
      return this.track("", errOutcome);
    }

    const idChanged = opts.id !== undefined;
    // Detached ids are minted off the fixed id-generation prefix (NOT the grown
    // id or the diverging origin), so recursive detached stays bounded at one
    // segment past the prefix. Mirrors the generator engine (src/context.ts).
    const id = idChanged ? (opts.id as string) : util.detachedId(this.prefixId, this.seqid());
    this.seq++;

    const func = registered ? registered.name : (funcOrName as string);
    const version = registered ? registered.version : opts.version || 1;
    const data = { func, args: argu, retry: opts.retryPolicy?.encode(), version };
    const createReq = this.detachedCreateReq({ id, data, opts });
    const slot = this.claimCreateSlot();

    // Fire-and-forget: the parent waits only for the durable CREATE to land
    // (so the spawn survives), never for the detached workflow's result, and
    // never suspends on it — the outcome is always `value`, never `suspended`.
    const outcome = (async (): Promise<Outcome> => {
      const rec = await this.createSlotted(createReq, func, slot);
      // Like the generator engine, detached takes the remote path (rpc + block):
      // it creates a global promise it does not run itself. It just never adds the
      // child to the remote-todo set, so the parent does not suspend on it.
      this.trace.emit({ kind: "rpc", id: this.id, callee: id });
      if (rec.state !== "pending") {
        const state = rec.state === "resolved" ? "resolved" : "rejected";
        this.trace.emit({ kind: "dedup", id, state, value: rec.value?.data });
      } else {
        this.trace.emit({ kind: "block", id });
      }
      return { kind: "value", value: { id } satisfies DetachedHandle };
    })();
    return this.track(id, outcome);
  }

  promise<T>({
    timeout,
    data,
    tags,
  }: {
    timeout?: number;
    data?: any;
    tags?: { [key: string]: string };
  } = {}): DurablePromise<T> {
    this.guardOpen("promise");
    const id = this.seqid();
    this.seq++;
    const createReq = this.latentCreateReq({ id, timeout, data, tags });
    const slot = this.claimCreateSlot();
    return this.track(id, this.remoteOutcome(id, createReq, slot, "promise"));
  }

  // Shared body for remote-backed ops (rpc, sleep): create the promise; if it
  // is already settled resolve/reject, otherwise record the remote-todo and
  // suspend (the user-facing promise will hang).
  private remoteOutcome(
    id: string,
    createReq: PromiseCreateReq,
    slot: { ready: Promise<void>; release: () => void },
    name: string,
  ): Promise<Outcome> {
    return (async (): Promise<Outcome> => {
      const rec = await this.createSlotted(createReq, name, slot);

      // rpc p→q: the parent created a remote child (rpc / sleep / DPC promise all
      // take this path — they create a global promise and block on its settle).
      this.trace.emit({ kind: "rpc", id: this.id, callee: id });

      if (rec.state !== "pending") {
        const state = rec.state === "resolved" ? "resolved" : "rejected";
        this.trace.emit({ kind: "dedup", id, state, value: rec.value?.data });
        return recordOutcome(rec);
      }

      this.trace.emit({ kind: "block", id });
      this.fireSuspend();
      return { kind: "suspended", remote: [id] };
    })();
  }

  // Register an outcome for draining and derive the branded user-facing handle.
  private track(id: string, outcome: Promise<Outcome>): DurablePromise<any> {
    this.spawnedHandles.push(outcome);
    return new DurablePromise(id, this.facing(outcome));
  }

  // Derive the user-facing promise from an outcome: value → resolve, error →
  // reject, suspended → hang forever. Does NOT register the outcome for draining
  // (callers that need draining push to `spawnedHandles` themselves or via track).
  private facing(outcome: Promise<Outcome>): Promise<any> {
    const facing = outcome.then((o) => {
      if (o.kind === "value") return o.value;
      if (o.kind === "error") return Promise.reject(o.error);
      return new Promise<never>(() => {}); // suspended → hang
    });
    // Suppress unhandled-rejection noise if the user ignores this handle.
    facing.catch(() => {});
    return facing;
  }

  private settle(id: string, outcome: { kind: "value"; value: any } | { kind: "error"; error: any }, func: string) {
    return this.effects.promiseSettle(
      {
        kind: "promise.settle",
        head: { corrId: randomUUID(), version: util.VERSION },
        data: {
          id,
          state: outcome.kind === "value" ? "resolved" : "rejected",
          value: { headers: {}, data: outcome.kind === "value" ? outcome.value : outcome.error },
        },
      },
      func,
    );
  }

  // --- metadata / utilities ----------------------------------------------

  getDependency<T = any>(name: string): T | undefined {
    return this.dependencies.get(name);
  }

  options(
    opts: Partial<Pick<Options, "tags" | "target" | "timeout" | "version" | "retryPolicy" | "nonRetryableErrors">> = {},
  ): Options {
    return this.optsBuilder.build(opts);
  }

  panic(condition: boolean, msg?: string): void {
    if (condition) {
      const error = exceptions.PANIC(util.getCallerInfo(), msg);
      // Record + signal BEFORE throwing: the abort must survive a user
      // try/catch around this call (the generator's DIE is thrown by the
      // driver, out of user reach — this is the async equivalent).
      this._panicError = error;
      this._firePanic();
      throw error;
    }
  }

  assert(condition: boolean, msg?: string): void {
    this.panic(!condition, msg);
  }

  readonly date = {
    now: (): DurablePromise<number> => this.run((this.getDependency<DateConstructor>("resonate:date") ?? Date).now),
  };

  readonly math = {
    random: (): DurablePromise<number> => this.run((this.getDependency<Math>("resonate:math") ?? Math).random),
  };

  // --- promise-create request builders (kept identical to InnerContext) ---

  // Shared promise.create envelope; each builder supplies only the clamped
  // timeout, the param data, and the scope-specific tag map.
  private createReq(args: {
    id: string;
    timeoutAt: number;
    data: any;
    tags: Record<string, string>;
  }): PromiseCreateReq {
    return {
      kind: "promise.create",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: {
        id: args.id,
        timeoutAt: args.timeoutAt,
        param: { headers: {}, data: args.data },
        tags: args.tags,
      },
    };
  }

  private localCreateReq({
    id,
    data,
    opts,
    breaksLineage,
  }: {
    id: string;
    data: any;
    opts: Options;
    breaksLineage: boolean;
  }): PromiseCreateReq {
    return this.createReq({
      id,
      timeoutAt: Math.min(this.clock.now() + opts.timeout, this.timeoutAt),
      data,
      tags: {
        "resonate:scope": "local",
        "resonate:branch": this.branchId,
        "resonate:parent": this.id,
        "resonate:origin": breaksLineage ? id : this.originId,
        // Prefix is set at the top and propagates down unchanged forever,
        // independent of origin (which detached/explicit-id may break).
        "resonate:prefix": this.prefixId,
        ...opts.tags,
      },
    });
  }

  private remoteCreateReq({
    id,
    data,
    opts,
    breaksLineage,
  }: {
    id: string;
    data: any;
    opts: Options;
    breaksLineage: boolean;
  }): PromiseCreateReq {
    return this.createReq({
      id,
      timeoutAt: Math.min(this.clock.now() + opts.timeout, this.timeoutAt),
      data,
      tags: {
        "resonate:scope": "global",
        "resonate:target": opts.target,
        "resonate:branch": id,
        "resonate:parent": this.id,
        "resonate:origin": breaksLineage ? id : this.originId,
        // Prefix is set at the top and propagates down unchanged forever,
        // independent of origin (which detached/explicit-id may break).
        "resonate:prefix": this.prefixId,
        ...opts.tags,
      },
    });
  }

  // Detached: a globally-scoped promise with its own origin (lineage break) and
  // an independent lifetime — its timeout is NOT clamped to the parent's.
  private detachedCreateReq({ id, data, opts }: { id: string; data: any; opts: Options }): PromiseCreateReq {
    return this.createReq({
      id,
      timeoutAt: this.clock.now() + opts.timeout,
      data,
      tags: {
        "resonate:scope": "global",
        "resonate:target": opts.target,
        "resonate:branch": id,
        "resonate:parent": this.id,
        "resonate:origin": id,
        // Prefix carries forward unchanged across the detached re-root (origin
        // breaks, prefix does not) — this is what keeps recursive ids bounded.
        "resonate:prefix": this.prefixId,
        ...opts.tags,
      },
    });
  }

  // Latent (DPC): a globally-scoped promise with no function, resolved out of band.
  private latentCreateReq({
    id,
    timeout,
    data,
    tags,
  }: {
    id: string;
    timeout?: number;
    data?: any;
    tags?: { [key: string]: string };
  }): PromiseCreateReq {
    return this.createReq({
      id,
      timeoutAt: Math.min(this.clock.now() + (timeout ?? 24 * util.HOUR), this.timeoutAt),
      data,
      tags: {
        "resonate:scope": "global",
        "resonate:branch": id,
        "resonate:parent": this.id,
        "resonate:origin": this.originId,
        // Prefix is set at the top and propagates down unchanged forever.
        "resonate:prefix": this.prefixId,
        ...tags,
      },
    });
  }

  private sleepCreateReq({ id, time }: { id: string; time: number }): PromiseCreateReq {
    return this.createReq({
      id,
      timeoutAt: Math.min(time, this.timeoutAt),
      data: "",
      tags: {
        "resonate:scope": "global",
        "resonate:branch": id,
        "resonate:parent": this.id,
        "resonate:origin": this.originId,
        // Prefix is set at the top and propagates down unchanged forever.
        "resonate:prefix": this.prefixId,
        "resonate:timer": "true",
      },
    });
  }

  private seqid(): string {
    return `${this.id}.${this.seq}`;
  }
}

// Map a settled durable promise record to an outcome.
function recordOutcome(rec: PromiseRecord): Outcome {
  return rec.state === "resolved"
    ? { kind: "value", value: rec.value?.data }
    : { kind: "error", error: rec.value?.data };
}

/**
 * Microtask generations the drain waits for the op set to stabilize. Bounds the
 * non-durable `await` hops a continuation may take between a settled durable op
 * and its next durable call while still landing inside the pass; deeper chains
 * are off-contract and panic via the closed context.
 */
const DRAIN_YIELDS = 16;

/**
 * Run a user function to completion or suspension under `ctx`.
 *
 * The function may park forever on a hung `await`, so we race it against the
 * suspend signal rather than waiting for it to finish. Whatever wins, we then
 * drain every started op (their outcomes always settle) to collect the complete
 * set of remote-todos — this is the authoritative suspend decision.
 */
export async function run(ctx: AsyncContext, func: AnyFunc, args: any[]): Promise<Outcome> {
  const race = await Promise.race([
    (async () => {
      try {
        return { tag: "returned" as const, v: await func(ctx, ...args) };
      } catch (e) {
        return { tag: "threw" as const, e };
      }
    })(),
    ctx.suspendSignal.then(() => ({ tag: "stuck" as const })),
    // A panic ends the pass even if the user catches the throw and then parks
    // on a non-durable await (the fn branch would never settle).
    ctx.panicSignal.then(() => ({ tag: "panicked" as const })),
  ]);

  // Structured-concurrency drain. allSettled awaits every started op (so no
  // unhandled rejections) and never hangs (outcomes always settle). Ops can
  // start MORE ops from continuations behind an earlier op's await (a chained
  // ctx.run), so a single snapshot is not enough: drain in rounds until the op
  // set stops growing, then yield a bounded number of microtask generations
  // before declaring the set stable — continuations may still be walking toward
  // their next durable call through combinator/microtask hops (Promise.all
  // resolution, `await null`). Microtask-only on purpose: the deterministic
  // simulator drains the whole microtask queue per tick (so these yields are
  // free in sim-time) but never fires engine timers/setImmediate. Only then is
  // the pass finalized (ctx.close): a later op — a continuation parked on a
  // timer or I/O — would race task.suspend/fulfill as a zombie, so it panics
  // instead.
  const settled: PromiseSettledResult<Outcome>[] = [];
  drain: while (true) {
    while (settled.length < ctx.spawnedHandles.length) {
      settled.push(...(await Promise.allSettled(ctx.spawnedHandles.slice(settled.length))));
    }
    for (let k = 0; k < DRAIN_YIELDS; k++) {
      await Promise.resolve();
      if (settled.length < ctx.spawnedHandles.length) continue drain;
    }
    break;
  }
  ctx.close();

  // A panic aborts the whole pass — the task is released, nothing settles —
  // exactly like the generator's DIE. Checked on the context, not the race,
  // so a user try/catch around ctx.panic cannot suppress it; checked after
  // the drain so no op is left mid-flight. It propagates: a child's panic
  // rejects its outcome, and the parent's drain rethrows it below.
  if (ctx.panicError) throw ctx.panicError;

  // An op whose promiseCreate/settle failed at the infra level fails the whole
  // pass (the task is released and retried), rather than settling a child
  // rejected. Deferred until the drain is complete so no op is left mid-flight.
  const remote: string[] = [];
  for (const s of settled) {
    if (s.status === "rejected") throw s.reason;
    if (s.value.kind === "suspended") remote.push(...s.value.remote);
  }

  const dedup = [...new Set(remote)];
  if (dedup.length > 0) return { kind: "suspended", remote: dedup };
  if (race.tag === "threw") return { kind: "error", error: race.e };
  return { kind: "value", value: race.tag === "returned" ? race.v : undefined };
}

/**
 * Run a user function with in-process retries (mirrors `util.executeWithRetry`
 * for the generator engine). Each attempt runs under a FRESH context — so a
 * function that creates durable children re-creates them with identical ids and
 * replays via dedup, exactly like a cross-process replay.
 *
 * Only an `error` outcome is retried; `value` and `suspended` short-circuit. A
 * retry is skipped (and the error returned) when the error is non-retryable, the
 * policy is exhausted, or the next delay would exceed the function's timeout.
 */
export async function runWithRetry(
  makeCtx: (attempt: number) => AsyncContext,
  func: AnyFunc,
  args: any[],
  policy: RetryPolicy,
  nonRetryableErrors: Array<new (...args: any[]) => Error>,
  clock: Clock,
  timeoutAt: number,
): Promise<Outcome> {
  let attempt = 1;
  while (true) {
    const outcome = await run(makeCtx(attempt), func, args);
    if (outcome.kind !== "error") return outcome;

    if (nonRetryableErrors.some((Ctor) => outcome.error instanceof Ctor)) return outcome;

    const retryIn = policy.next(attempt);
    if (retryIn === null || clock.now() + retryIn >= timeoutAt) return outcome;

    attempt++;
    await delay(retryIn);
  }
}
