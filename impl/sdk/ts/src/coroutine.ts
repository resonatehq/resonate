import { randomUUID } from "node:crypto";
import type { Context, InnerContext } from "./context.js";
import { Decorator, type PromiseCompleted, type Value } from "./decorator.js";
import type { Logger } from "./logger.js";
import type { PromiseRecord } from "./network/types.js";
import { Never } from "./retries.js";
import type { TraceCollector } from "./trace.js";
import type { Effects, Result, Yieldable } from "./types.js";
import * as util from "./util.js";

export type Suspended = {
  type: "suspended";
  todo: { remote: RemoteTodo[] };
};

export type Done = {
  type: "done";
  result: Result<any, any>;
};

export interface RemoteTodo {
  id: string;
}

// a simple map to suppress duplicate warnings, necessary due to
// micro retries
const logged: Map<string, boolean> = new Map();

type LocalWorkEntry = {
  funcName: string;
  promise: Promise<Suspended | Done>;
};

export class Coroutine<T> {
  private ctx: InnerContext;
  private logger: Logger;
  private decorator: Decorator<T>;
  private effects: Effects;
  private trace: TraceCollector;

  constructor(ctx: InnerContext, logger: Logger, decorator: Decorator<T>, effects: Effects, trace: TraceCollector) {
    this.ctx = ctx;
    this.logger = logger;
    this.decorator = decorator;
    this.effects = effects;
    this.trace = trace;

    if (!(this.ctx.retryPolicy instanceof Never) && !logged.has(this.ctx.id)) {
      this.logger.warn(
        { component: "coroutine", func: this.ctx.func, id: this.ctx.id },
        `Generator function '${this.ctx.func}' does not support retries. Will ignore.`,
      );
      logged.set(this.ctx.id, true);
    }
  }

  private emitTrace(event: import("./trace.js").Event): void {
    this.trace.emit(event);
    this.logger.debug({ component: "trace", ...event }, `trace: ${event.kind}`);
  }

  public static async exec(
    logger: Logger,
    ctx: InnerContext,
    func: (ctx: Context, ...args: any[]) => Generator<Yieldable, any, any>,
    args: any[],
    effects: Effects,
    trace: TraceCollector,
  ): Promise<Suspended | Done> {
    const coroutine = new Coroutine(ctx, logger, new Decorator(func(ctx, ...args)), effects, trace);
    return await coroutine.exec();
  }

  private async exec(): Promise<Suspended | Done> {
    const remote: RemoteTodo[] = [];
    const localWork: Map<string, LocalWorkEntry> = new Map();

    // Emit spawn for this function's execution
    this.emitTrace({ kind: "spawn", id: this.ctx.id });

    let input: Value<any> = {
      type: "internal.nothing",
    };

    while (true) {
      const action = this.decorator.next(input);

      // Handle internal.async.l (lfi/lfc) — local child creation (run)
      if (action.type === "internal.async.l") {
        const res = await this.effects.promiseCreate(action.createReq, action.func.name);

        // Emit "run p q" — parent creates local child
        this.emitTrace({ kind: "run", id: this.ctx.id, callee: action.id });

        const ctx = this.ctx.child({
          id: res.id,
          func: action.func.name,
          timeout: res.timeoutAt,
          version: action.version,
          retryPolicy: action.retryPolicy,
          nonRetryableErrors: action.nonRetryableErrors,
        });

        if (res.state === "pending") {
          // Start local work eagerly — both regular functions and generators
          let localPromise: Promise<Suspended | Done>;

          if (!util.isGeneratorFunction(action.func)) {
            // For regular functions, emit spawn/return around execution
            localPromise = (async () => {
              this.emitTrace({ kind: "spawn", id: action.id });
              const result = await util.executeWithRetry(ctx, action.func, action.args, this.logger);
              const state = result.kind === "value" ? "resolved" : "rejected";
              const value = result.kind === "value" ? result.value : result.error;
              this.emitTrace({ kind: "return", id: action.id, state, value });
              return { type: "done" as const, result };
            })();
          } else {
            localPromise = Coroutine.exec(
              this.logger,
              ctx,
              action.func as (ctx: Context, ...args: any[]) => Generator<Yieldable, any, any>,
              action.args,
              this.effects,
              this.trace,
            );
          }

          localWork.set(action.id, { funcName: action.func.name, promise: localPromise });
          input = {
            type: "internal.promise",
            state: "pending",
            mode: "attached",
            id: action.id,
          };
        } else {
          // durable promise is already completed (replay) — dedup
          const state = res.state === "resolved" ? "resolved" : "rejected";
          const value = res.value?.data;
          this.emitTrace({ kind: "dedup", id: action.id, state, value });
          input = this.completedPromise(action.id, res);
        }
        continue;
      }

      // Handle internal.async.r — remote child creation (rpc)
      if (action.type === "internal.async.r") {
        const res = await this.effects.promiseCreate(action.createReq, "unknown");

        // Emit "rpc p q" — parent creates remote child
        this.emitTrace({ kind: "rpc", id: this.ctx.id, callee: action.id });

        if (res.state === "pending") {
          // Emit "block q" — rpc child is unsettled
          this.emitTrace({ kind: "block", id: action.id });

          if (action.mode === "attached") remote.push({ id: action.id });

          input = {
            type: "internal.promise",
            state: "pending",
            mode: action.mode,
            id: action.id,
          };
        } else {
          // Already settled — dedup
          const state = res.state === "resolved" ? "resolved" : "rejected";
          const value = res.value?.data;
          this.emitTrace({ kind: "dedup", id: action.id, state, value });
          input = this.completedPromise(action.id, res);
        }
        continue;
      }

      // Handle die
      if (action.type === "internal.die") {
        if (action.condition) {
          throw action.error;
        }
        input = { type: "internal.nothing" };
        continue;
      }

      // Handle await on a completed promise (fast-path / replay)
      if (action.type === "internal.await" && action.promise.state === "completed") {
        // Emit await + resume (child already settled)
        this.emitTrace({ kind: "await", id: this.ctx.id, callee: action.id });
        this.emitTrace({ kind: "resume", id: this.ctx.id, callee: action.id });

        util.assert(
          action.promise.value.type === "internal.literal",
          "promise value must be an 'internal.literal' type",
        );
        util.assertDefined(action.promise.value);
        input = action.promise.value;
        continue;
      }

      // Handle await on a pending promise
      if (action.type === "internal.await" && action.promise.state === "pending") {
        // Emit await
        this.emitTrace({ kind: "await", id: this.ctx.id, callee: action.id });

        if (action.promise.mode === "detached") {
          // User is explicitly awaiting a detached promise — add its remote todo now
          remote.push({ id: action.id });
        }

        const localEntry = localWork.get(action.id);

        if (localEntry) {
          // This is a local in-flight task — await it inline
          localWork.delete(action.id);
          const localResult = await localEntry.promise;

          if (localResult.type === "done") {
            const settleRes = await this.settle(action.id, localResult.result, localEntry.funcName);

            util.assert(settleRes.state !== "pending", "promise must be completed");

            // Emit resume — child completed, parent continues
            this.emitTrace({ kind: "resume", id: this.ctx.id, callee: action.id });

            // Feed the resolved/rejected value directly as a Literal so the generator continues
            input = {
              type: "internal.literal",
              value: localResult.result,
            };
            continue;
          }

          // localResult.type === "suspended": child generator has remote deps
          // Flush remaining local work and collect all remote todos
          const res = await this.flushLocalWork(localWork);

          // Emit suspend — parent cannot continue
          this.emitTrace({ kind: "suspend", id: this.ctx.id });

          const allRemote = [...remote, ...localResult.todo.remote, ...res.remote];
          return { type: "suspended", todo: { remote: allRemote } };
        }

        // Not in localWork — it's a remote pending promise.
        // Flush any in-flight local work so their remote dependencies are surfaced
        const flushResult = await this.flushLocalWork(localWork);

        // Emit suspend — parent cannot continue (blocked on remote)
        this.emitTrace({ kind: "suspend", id: this.ctx.id });

        const allRemote = [...remote, ...flushResult.remote];
        return { type: "suspended", todo: { remote: allRemote } };
      }

      // Handle return
      if (action.type === "internal.return") {
        util.assert(action.value.type === "internal.literal", "promise value must be an 'internal.literal' type");
        util.assertDefined(action.value);

        // Structured concurrency: flush all in-flight local work before completing
        if (localWork.size > 0) {
          const flushResult = await this.flushLocalWork(localWork);

          const allRemote = [...remote, ...flushResult.remote];
          if (allRemote.length > 0) {
            // Emit suspend — parent cannot return due to unsettled remote children
            this.emitTrace({ kind: "suspend", id: this.ctx.id });
            return { type: "suspended", todo: { remote: allRemote } };
          }
          // Emit return — function completes
          const result = action.value.value;
          const state = result.kind === "value" ? "resolved" : "rejected";
          const value = result.kind === "value" ? result.value : result.error;
          this.emitTrace({ kind: "return", id: this.ctx.id, state, value });
          return { type: "done", result: action.value.value };
        }

        if (remote.length > 0) {
          // Emit suspend — parent cannot return due to unsettled remote children
          this.emitTrace({ kind: "suspend", id: this.ctx.id });
          return { type: "suspended", todo: { remote } };
        }
        // Emit return — function completes
        const result = action.value.value;
        const state = result.kind === "value" ? "resolved" : "rejected";
        const value = result.kind === "value" ? result.value : result.error;
        this.emitTrace({ kind: "return", id: this.ctx.id, state, value });
        return { type: "done", result: action.value.value };
      }
    }
  }

  /**
   * Awaits all in-flight local work entries, settles completed durable promises,
   * and collects remote todos from any suspended child generators.
   */
  private async flushLocalWork(localWork: Map<string, LocalWorkEntry>): Promise<{ remote: RemoteTodo[] }> {
    const entries = [...localWork.entries()];
    localWork.clear();

    // Await all in-flight promises concurrently before checking results
    const results = await Promise.all(
      entries.map(async ([id, { funcName, promise }]) => ({ id, funcName, result: await promise })),
    );

    const allRemote: RemoteTodo[] = [];

    for (const { id, funcName, result } of results) {
      if (result.type === "suspended") {
        // Child generator has remote deps, collect them; its durable promise remains pending
        allRemote.push(...result.todo.remote);
      } else {
        // Child completed (success or user error), settle its durable promise
        await this.settle(id, result.result, funcName);
      }
    }

    return { remote: allRemote };
  }

  private settle(id: string, result: Result<any, any>, funcName: string) {
    return this.effects.promiseSettle(
      {
        kind: "promise.settle",
        head: { corrId: randomUUID(), version: util.VERSION },
        data: {
          id,
          state: result.kind === "value" ? "resolved" : "rejected",
          value: {
            headers: {},
            data: result.kind === "value" ? result.value : result.error,
          },
        },
      },
      funcName,
    );
  }

  private completedPromise(id: string, record: PromiseRecord): PromiseCompleted<any> {
    return {
      type: "internal.promise",
      state: "completed",
      id,
      value: {
        type: "internal.literal",
        value:
          record.state === "resolved"
            ? { kind: "value", value: record.value?.data }
            : { kind: "error", error: record.value?.data },
      },
    };
  }
}
