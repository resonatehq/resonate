import type { Clock } from "../clock.js";
import type { Codec } from "../codec.js";
import exceptions, { ResonateError } from "../exceptions.js";
import type { Heartbeat } from "../heartbeat.js";
import type { Logger } from "../logger.js";
import { isRedirect, isSuccess, type Message, type PromiseRecord, type TaskRecord } from "../network/types.js";
import type { OptionsBuilder } from "../options.js";
import { randomUUID } from "../platform.js";
import type { Registry } from "../registry.js";
import { Constant, Exponential, Linear, Never, type RetryPolicy, type RetryPolicyConstructor } from "../retries.js";
import { type Trace, TraceCollector } from "../trace.js";
import type { Effects, Send } from "../types.js";
import * as util from "../util.js";
import { type AnyFunc, AsyncContext, runWithRetry } from "./context.js";

// Structurally identical to the generator engine's Status, including the
// lifecycle trace (the spawn/run/rpc/block/dedup/return/suspend subset — the
// async engine cannot observe await/resume; see `isWellFormedLifecycle`).
export type Done = { kind: "done"; id: string; state: "resolved" | "rejected"; value: any; trace: Trace };
export type Suspended = { kind: "suspended"; awaited: string[]; trace: Trace };
export type Status = Done | Suspended;

/**
 * Task-lifecycle orchestrator for the async/await engine. Mirrors `src/core.ts`
 * (acquire / suspend / fulfill / release + replay-with-preload recursion) but
 * folds the per-task driver in as `executeUntilBlockedInner`, following Rust's
 * `Core::execute_until_blocked_inner` (no separate `Computation` class).
 */
export class Core {
  private pid: string;
  private ttl: number;
  private clock: Clock;
  private send: Send;
  private codec: Codec;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private logger: Logger;
  private retries: Map<string, RetryPolicyConstructor>;

  constructor({
    pid,
    ttl,
    clock,
    send,
    codec,
    registry,
    heartbeat,
    dependencies,
    optsBuilder,
    logger,
  }: {
    pid: string;
    ttl: number;
    clock: Clock;
    send: Send;
    codec: Codec;
    registry: Registry;
    heartbeat: Heartbeat;
    dependencies: Map<string, any>;
    optsBuilder: OptionsBuilder;
    logger: Logger;
  }) {
    this.pid = pid;
    this.ttl = ttl;
    this.clock = clock;
    this.send = send;
    this.codec = codec;
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.dependencies = dependencies;
    this.optsBuilder = optsBuilder;
    this.logger = logger;

    this.retries = new Map<string, RetryPolicyConstructor>([
      [Constant.type, Constant],
      [Exponential.type, Exponential],
      [Linear.type, Linear],
      [Never.type, Never],
    ]);
  }

  // Decode the retry policy embedded in the root promise's param data, mirroring
  // the generator engine's Computation. Retries are opt-in for the async engine,
  // so the default (absent or unknown policy) is Never.
  private resolveRetry(retry?: { type: string; data: any }): RetryPolicy {
    const ctor = retry ? this.retries.get(retry.type) : undefined;
    if (retry && !ctor) {
      this.logger.warn(
        { component: "async-core", retryType: retry.type },
        `Retry policy '${retry.type}' not found. Will ignore.`,
      );
    }
    return ctor ? new ctor(retry?.data) : new Never();
  }

  public async executeUntilBlocked(
    task: TaskRecord,
    rootPromise: PromiseRecord,
    preload?: PromiseRecord[],
  ): Promise<Status> {
    util.assert(task.state === "acquired", `expected task state to be 'acquired', got '${task.state}'`);
    const effects = util.buildEffects(this.send, this.codec, { id: task.id, version: task.version }, preload);

    try {
      const status = await this.executeUntilBlockedInner(rootPromise, effects);
      if (status.kind === "suspended") {
        const suspendResult = await this.suspendTask(task, rootPromise, status.awaited);
        if (suspendResult.continue) {
          return this.executeUntilBlocked(task, rootPromise, suspendResult.preload);
        }
      }
      if (status.kind === "done") {
        await this.fulfillTask(task, rootPromise, status);
      }
      return status;
    } catch (err) {
      if (err instanceof ResonateError) {
        err.log(this.logger);
      } else {
        this.logger.warn(
          { component: "async-core", error: err instanceof Error ? err.message : String(err) },
          "executeUntilBlocked failed unexpectedly",
        );
      }
      await this.releaseTask(task, rootPromise).catch((releaseErr) =>
        this.logger.warn(
          {
            component: "async-core",
            error: releaseErr instanceof Error ? releaseErr.message : String(releaseErr),
          },
          "failed to release task after execution error",
        ),
      );
      throw err;
    }
  }

  // Per-task pass. Runs the user async fn under a fresh AsyncContext and
  // finalizes via the race + drain in `run`. Owns all per-pass state so the
  // parked frame (on suspend) is GC-collectible once this returns (GC rules).
  private async executeUntilBlockedInner(rootPromise: PromiseRecord, effects: Effects): Promise<Status> {
    if (!isValidData(rootPromise.param?.data)) {
      throw exceptions.PANIC("async-core", "invalid promise data");
    }

    const { func, args, retry, version = 1 } = rootPromise.param.data;
    const registered = this.registry.get(func, version);
    if (!registered) {
      throw exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(func, version);
    }

    if (version !== 0) util.assert(version === registered.version, "versions must match");
    util.assert(func === registered.name, "names must match");

    this.heartbeat.start();

    // One trace per pass (mirrors Computation.processGenerator); threaded into
    // the root context and propagated to every child via AsyncContext.child.
    const trace = new TraceCollector();

    // Root dedup: if the boundary promise is already settled, short-circuit with
    // a single dedup event (no spawn — the root never ran this pass).
    if (rootPromise.state !== "pending") {
      const state = rootPromise.state === "resolved" ? "resolved" : "rejected";
      trace.emit({ kind: "dedup", id: rootPromise.id, state, value: rootPromise.value?.data });
      return { kind: "done", id: rootPromise.id, state, value: rootPromise.value?.data, trace: trace.getTrace() };
    }

    // Root spawn — emitted once before the (possibly retried) run driver, matching
    // the generator coroutine's own spawn at exec start. Retries re-run the body
    // but do not re-emit spawn.
    trace.emit({ kind: "spawn", id: rootPromise.id });

    // Run the root with in-process retries. Each attempt runs under a fresh
    // context so durable children replay identically via dedup. nonRetryable
    // errors are empty at the root (error constructors can't be encoded into a
    // promise) — only the decoded retry policy applies.
    const outcome = await runWithRetry(
      (attempt) =>
        new AsyncContext(
          {
            id: rootPromise.id,
            oId: rootPromise.tags["resonate:origin"] ?? rootPromise.id,
            // The id-generation prefix, propagated unchanged across re-roots (a
            // detached child resets origin to its own id but carries prefix
            // forward), so recursive detached ids stay bounded. Falls back to
            // the id like oId.
            prId: rootPromise.tags["resonate:prefix"] ?? rootPromise.id,
            func: registered.func.name,
            clock: this.clock,
            registry: this.registry,
            dependencies: this.dependencies,
            optsBuilder: this.optsBuilder,
            timeout: rootPromise.timeoutAt,
            version: registered.version,
            attempt,
          },
          effects,
          trace,
        ),
      registered.func as AnyFunc,
      args,
      this.resolveRetry(retry),
      [],
      this.clock,
      rootPromise.timeoutAt,
    );

    if (outcome.kind === "suspended") {
      trace.emit({ kind: "suspend", id: rootPromise.id });
      return { kind: "suspended", awaited: outcome.remote, trace: trace.getTrace() };
    }
    const state = outcome.kind === "value" ? "resolved" : "rejected";
    const value = outcome.kind === "value" ? outcome.value : outcome.error;
    trace.emit({ kind: "return", id: rootPromise.id, state, value });
    return { kind: "done", id: rootPromise.id, state, value, trace: trace.getTrace() };
  }

  private async releaseTask(task: TaskRecord, rootPromise: PromiseRecord): Promise<void> {
    await this.send({
      kind: "task.release",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: { id: task.id, version: task.version },
    });
  }

  private async suspendTask(
    task: TaskRecord,
    rootPromise: PromiseRecord,
    awaited: string[],
  ): Promise<{ continue: true; preload: PromiseRecord[] } | { continue: false }> {
    const res = await this.send({
      kind: "task.suspend",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: {
        id: task.id,
        version: task.version,
        actions: awaited.map((a) => ({
          kind: "promise.register_callback",
          head: { corrId: randomUUID(), version: util.VERSION },
          data: { awaiter: rootPromise.id, awaited: a },
        })),
      },
    });
    if (isSuccess(res)) {
      return { continue: false };
    }
    if (isRedirect(res)) {
      return { continue: true, preload: res.data.preload };
    }
    throw exceptions.SERVER_ERROR(res.data, true, {
      code: res.head.status,
      message: res.data,
    });
  }

  private async fulfillTask(task: TaskRecord, rootPromise: PromiseRecord, doneValue: Done): Promise<void> {
    const encoded = this.codec.encode(doneValue.value);

    await this.send({
      kind: "task.fulfill",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: {
        id: task.id,
        version: task.version,
        action: {
          kind: "promise.settle",
          head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
          data: {
            id: doneValue.id,
            state: doneValue.state,
            value: encoded,
          },
        },
      },
    });
  }

  public async onMessage(msg: Message): Promise<Status | undefined> {
    // `msg` arrives off the network: an unexpected kind is an external
    // condition (unhandled message type, corruption), not an internal
    // invariant — drop it with a warning instead of asserting (which exits).
    if (msg.kind !== "execute") {
      this.logger.warn(
        { component: "async-core", kind: (msg as { kind?: unknown }).kind },
        "dropping message with unexpected kind",
      );
      return undefined;
    }

    const task = msg.data.task;
    const res = await this.send({
      kind: "task.acquire",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": task.id },
      data: { id: task.id, version: task.version, pid: this.pid, ttl: this.ttl },
    });

    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }

    const rootPromise = this.codec.decodePromise(res.data.promise);
    return this.executeUntilBlocked(res.data.task, rootPromise, res.data.preload);
  }
}

// Validates the root promise's decoded `param.data` shape (copied from
// src/computation.ts to keep the generator engine untouched).
function isValidData(
  data: unknown,
): data is { func: string; args: any[]; retry?: { type: string; data: any }; version?: number } {
  if (data === null || typeof data !== "object") return false;
  const d = data as any;
  if (typeof d.func !== "string") return false;
  if (!Array.isArray(d.args)) return false;
  if (d.version !== undefined && typeof d.version !== "number") return false;
  if (d.retry !== undefined) {
    if (d.retry === null || typeof d.retry !== "object" || typeof d.retry.type !== "string" || !("data" in d.retry)) {
      return false;
    }
  }
  return true;
}
