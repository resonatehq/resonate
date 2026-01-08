import type { Context, InnerContext } from "./context";
import { Decorator, type Value } from "./decorator";
import type { Handler } from "./handler";
import type { DurablePromiseRecord, TaskRecord } from "./network/network";
import { Never } from "./retries";
import type { Span } from "./tracer";
import type { Result, Yieldable } from "./types";
import * as util from "./util";

export type Suspended = {
  type: "suspended";
  todo: { local: LocalTodo[]; remote: RemoteTodo[] };
  spans: Span[];
};

export type Completed = {
  type: "completed";
  promise: DurablePromiseRecord;
};

export interface LocalTodo {
  id: string;
  ctx: InnerContext;
  span: Span;
  func: (ctx: Context, ...args: any[]) => any;
  args: any[];
}

export interface RemoteTodo {
  id: string;
}

type More = {
  type: "more";
  todo: { local: LocalTodo[]; remote: RemoteTodo[] };
  spans: Span[];
};

type Done = {
  type: "done";
  result: Result<any, any>;
};

// a simple map to suppress duplicate warnings, necessary due to
// micro retries
const logged: Map<string, boolean> = new Map();

export class Coroutine<T> {
  private ctx: InnerContext;
  private task: TaskRecord;
  private verbose: boolean;
  private decorator: Decorator<T>;
  private handler: Handler;
  private spans: Map<string, Span>;
  private readonly depth: number;
  private readonly queueMicrotaskEveryN: number = 1;

  constructor(
    ctx: InnerContext,
    task: TaskRecord,
    verbose: boolean,
    decorator: Decorator<T>,
    handler: Handler,
    spans: Map<string, Span>,
    depth = 1,
  ) {
    this.ctx = ctx;
    this.task = task;
    this.verbose = verbose;
    this.decorator = decorator;
    this.handler = handler;
    this.spans = spans;
    this.depth = depth;

    if (!(this.ctx.retryPolicy instanceof Never) && !logged.has(this.ctx.id)) {
      console.warn(`Options. Generator function '${this.ctx.func}' does not support retries. Will ignore.`);
      logged.set(this.ctx.id, true);
    }

    if (typeof process !== "undefined" && process.env.QUEUE_MICROTASK_EVERY_N) {
      this.queueMicrotaskEveryN = Number.parseInt(process.env.QUEUE_MICROTASK_EVERY_N, 10);
    }
  }

  public static exec(
    id: string,
    verbose: boolean,
    ctx: InnerContext,
    func: (ctx: Context, ...args: any[]) => Generator<Yieldable, any, any>,
    args: any[],
    task: TaskRecord,
    handler: Handler,
    spans: Map<string, Span>,
    callback: (res: Result<Suspended | Completed, any>) => void,
  ): void {
    handler.createPromise(
      {
        // The createReq for this specific creation is not relevant,
        // this promise is guaranteed to have been created already.
        kind: "createPromise",
        id,
        timeout: 0,
      },
      (res) => {
        if (res.kind === "error") return callback({ kind: "error", error: undefined });

        if (res.value.state !== "pending") {
          return callback({ kind: "value", value: { type: "completed", promise: res.value } });
        }

        const coroutine = new Coroutine(ctx, task, verbose, new Decorator(func(ctx, ...args)), handler, spans);
        coroutine.exec((res) => {
          if (res.kind === "error") return callback(res);
          const status = res.value;
          switch (status.type) {
            case "more":
              callback({ kind: "value", value: { type: "suspended", todo: status.todo, spans: status.spans } });
              break;

            case "done":
              handler.completePromise(
                {
                  kind: "completePromise",
                  id: id,
                  state: status.result.kind === "value" ? "resolved" : "rejected",
                  value: {
                    data: status.result.kind === "value" ? status.result.value : status.result.error,
                  },
                },
                (res) => {
                  if (res.kind === "error") {
                    res.error.log(verbose);
                    return callback({ kind: "error", error: undefined });
                  }

                  callback({ kind: "value", value: { type: "completed", promise: res.value } });
                },
                func.name,
              );
              break;
          }
        });
      },
      func.name,
    );
  }

  private exec(callback: (res: Result<More | Done, any>) => void) {
    const local: LocalTodo[] = [];
    const remote: RemoteTodo[] = [];
    const spans: Span[] = [];

    let input: Value<any> = {
      type: "internal.nothing",
    };

    // next needs to be called when we want to go to the top of the loop but are inside a callback
    const next = () => {
      while (true) {
        const action = this.decorator.next(input);

        // Handle internal.async.l (lfi/lfc)
        if (action.type === "internal.async.l") {
          let span: Span;
          if (!this.spans.has(action.createReq.id)) {
            span = this.ctx.span.startSpan(action.createReq.id, this.ctx.clock.now());
            span.setAttribute("type", "run");
            span.setAttribute("func", action.func.name);
            span.setAttribute("version", action.version);
            span.setAttribute("task.id", this.task.id);
            span.setAttribute("task.counter", this.task.counter);
            this.spans.set(action.createReq.id, span);
          } else {
            span = this.spans.get(action.createReq.id)!;
          }

          this.handler.createPromise(
            action.createReq,
            (res) => {
              if (res.kind === "error") {
                res.error.log(this.verbose);
                span.setStatus(false, String(res.error));
                span.end(this.ctx.clock.now());
                return callback({ kind: "error", error: undefined });
              }
              util.assertDefined(res);

              // if the promise is created, the span is considered successful
              span.setStatus(true);

              const ctx = this.ctx.child({
                id: res.value.id,
                func: action.func.name,
                timeout: res.value.timeout,
                version: action.version,
                retryPolicy: action.retryPolicy,
                span: span,
              });

              if (res.value.state === "pending") {
                if (!util.isGeneratorFunction(action.func)) {
                  local.push({
                    id: action.id,
                    ctx,
                    span,
                    func: action.func,
                    args: action.args,
                  });
                  input = {
                    type: "internal.promise",
                    state: "pending",
                    mode: "attached",
                    id: action.id,
                  };
                  next(); // Go back to the top of the loop
                  return;
                }

                spans.push(span);
                const coroutine = new Coroutine(
                  ctx,
                  this.task,
                  this.verbose,
                  new Decorator(action.func(ctx, ...action.args)),
                  this.handler,
                  this.spans,
                  this.depth + 1,
                );

                const cb = (res: Result<More | Done, any>) => {
                  if (res.kind === "error") {
                    span.end(this.ctx.clock.now());
                    return callback(res);
                  }
                  const status = res.value;

                  if (status.type === "more") {
                    local.push(...status.todo.local);
                    remote.push(...status.todo.remote);
                    input = {
                      type: "internal.promise",
                      state: "pending",
                      mode: "attached",
                      id: action.id,
                    };
                    next();
                  } else {
                    this.handler.completePromise(
                      {
                        kind: "completePromise",
                        id: action.id,
                        state: status.result.kind === "value" ? "resolved" : "rejected",
                        value: {
                          data: status.result.kind === "value" ? status.result.value : status.result.error,
                        },
                      },
                      (res) => {
                        span.end(this.ctx.clock.now());
                        spans.splice(spans.indexOf(span));

                        if (res.kind === "error") {
                          res.error.log(this.verbose);
                          return callback({ kind: "error", error: undefined });
                        }
                        util.assert(res.value.state !== "pending", "promise must be completed");

                        input = {
                          type: "internal.promise",
                          state: "completed",
                          id: action.id,
                          value: {
                            type: "internal.literal",
                            value:
                              res.value.state === "resolved"
                                ? { kind: "value", value: res.value.value?.data }
                                : { kind: "error", error: res.value.value?.data },
                          },
                        };
                        next();
                      },
                      action.func.name,
                    );
                  }
                };

                // Every nth level we kick off the next coroutine in a
                // microtask, escaping the current call stack. This is
                // necessary to avoid exceeding the maximum call stack
                // when the user program has adequate recursion.
                //
                // The microtask queue is exhausted before the
                // javascript engine moves on to macrotasks and a
                // coroutine may spawn recursive coroutines, opening up
                // the possibility of blocking the event loop
                // indefinitely. However, this is analagous to writing
                // a program with unbounded recursion, something that
                // is always possible.
                //
                // Experimenting with the queueMicrotaskEveryN value
                // shows that a value of 1 (our default) is optimal.
                if (this.depth % this.queueMicrotaskEveryN === 0) {
                  queueMicrotask(() => coroutine.exec(cb));
                } else {
                  coroutine.exec(cb);
                }
              } else {
                span.end(ctx.clock.now());
                // durable promise is completed
                input = {
                  type: "internal.promise",
                  state: "completed",
                  id: action.id,
                  value: {
                    type: "internal.literal",
                    value:
                      res.value.state === "resolved"
                        ? { kind: "value", value: res.value.value?.data }
                        : { kind: "error", error: res.value.value?.data },
                  },
                };
                next();
              }
            },
            action.func.name,
            span.encode(),
          );
          return; // Exit the while loop to wait for async callback
        }

        // Handle internal.async.r
        if (action.type === "internal.async.r") {
          let span: Span;
          if (!this.spans.has(action.createReq.id)) {
            span = this.ctx.span.startSpan(action.createReq.id, this.ctx.clock.now());
            span.setAttribute("type", "rpc");
            span.setAttribute("mode", action.mode);
            span.setAttribute("func", action.func);
            span.setAttribute("version", action.version);
            span.setAttribute("task.id", this.task.id);
            span.setAttribute("task.counter", this.task.counter);
            this.spans.set(action.createReq.id, span);
          } else {
            span = this.spans.get(action.createReq.id)!;
          }

          this.handler.createPromise(
            action.createReq,
            (res) => {
              if (res.kind === "error") {
                res.error.log(this.verbose);
                span.setStatus(false, String(res.error));
                span.end(this.ctx.clock.now());
                return callback({ kind: "error", error: undefined });
              }

              // if the promise is created, the span is considered successful
              span.setStatus(true);
              span.end(this.ctx.clock.now());

              util.assertDefined(res);

              if (res.value.state === "pending") {
                if (action.mode === "attached") remote.push({ id: action.id });

                input = {
                  type: "internal.promise",
                  state: "pending",
                  mode: action.mode,
                  id: action.id,
                };
              } else {
                input = {
                  type: "internal.promise",
                  state: "completed",
                  id: action.id,
                  value: {
                    type: "internal.literal",
                    value:
                      res.value.state === "resolved"
                        ? { kind: "value", value: res.value.value?.data }
                        : { kind: "error", error: res.value.value?.data },
                  },
                };
              }
              next();
            },
            "unknown",
            span.encode(),
          );
          return; // Exit the while loop to wait for async callback
        }

        // Handle die
        if (action.type === "internal.die" && !action.condition) {
          input = {
            type: "internal.nothing",
          };
          continue;
        }

        if (action.type === "internal.die" && action.condition) {
          action.error.log(this.verbose);
          return callback({ kind: "error", error: undefined });
        }

        // Handle await
        if (action.type === "internal.await" && action.promise.state === "completed") {
          util.assert(
            action.promise.value.type === "internal.literal",
            "promise value must be an 'internal.literal' type",
          );
          util.assertDefined(action.promise.value);
          input = action.promise.value;
          continue;
        }

        // invoke the callback when awaiting a pending "Future" the list of todos will include
        // the global callbacks to create.
        if (action.type === "internal.await" && action.promise.state === "pending") {
          if (action.promise.mode === "detached") {
            // We didn't add the associated todo of this promise, since the user is explicitly awaiting it we need to add the todo now.
            // All detached are remotes.
            remote.push({ id: action.id });
          }
          callback({ kind: "value", value: { type: "more", todo: { local, remote }, spans } });
          return;
        }

        // Handle return
        if (action.type === "internal.return") {
          util.assert(action.value.type === "internal.literal", "promise value must be an 'internal.literal' type");
          util.assertDefined(action.value);
          util.assert(spans.length === 0, "all spans should've been closed");

          callback({ kind: "value", value: { type: "done", result: action.value.value } });
          return;
        }
      }
    };

    next();
  }
}
