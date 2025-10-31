import type { Context, InnerContext } from "./context";
import { Decorator, type Value } from "./decorator";
import type { Handler } from "./handler";
import type { DurablePromiseRecord } from "./network/network";
import { type Callback, ko, ok, type Result, type Yieldable } from "./types";
import * as util from "./util";

export type Suspended = {
  type: "suspended";
  todo: { local: LocalTodo[]; remote: RemoteTodo[] };
};

export type Completed = {
  type: "completed";
  promise: DurablePromiseRecord;
};

export interface LocalTodo {
  id: string;
  ctx: InnerContext;
  func: (ctx: Context, ...args: any[]) => any;
  args: any[];
}

export interface RemoteTodo {
  id: string;
}

type More = {
  type: "more";
  todo: { local: LocalTodo[]; remote: RemoteTodo[] };
};

type Done = {
  type: "done";
  result: Result<any>;
};

export class Coroutine<T> {
  private ctx: InnerContext;
  private verbose: boolean;
  private decorator: Decorator<T>;
  private handler: Handler;
  private readonly depth: number;
  private readonly queueMicrotaskEveryN: number = 1;

  constructor(ctx: InnerContext, verbose: boolean, decorator: Decorator<T>, handler: Handler, depth = 1) {
    this.ctx = ctx;
    this.verbose = verbose;
    this.decorator = decorator;
    this.handler = handler;
    this.depth = depth;

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
    handler: Handler,
    callback: Callback<Suspended | Completed>,
  ): void {
    handler.createPromise(
      {
        // The createReq for this specific creation is not relevant,
        // this promise is guaranteed to have been created already.
        kind: "createPromise",
        id,
        timeout: 0,
        iKey: id,
        strict: false,
      },
      (err, res) => {
        if (err) return callback(true);
        util.assertDefined(res);

        if (res.state !== "pending") {
          return callback(false, { type: "completed", promise: res });
        }

        const coroutine = new Coroutine(ctx, verbose, new Decorator(func(ctx, ...args)), handler);
        coroutine.exec((err, status) => {
          if (err) return callback(err);
          util.assertDefined(status);
          switch (status.type) {
            case "more":
              callback(false, { type: "suspended", todo: status.todo });
              break;

            case "done":
              handler.completePromise(
                {
                  kind: "completePromise",
                  id: id,
                  state: status.result.success ? "resolved" : "rejected",
                  value: {
                    data: status.result.success ? status.result.value : status.result.error,
                  },
                  iKey: id,
                  strict: false,
                },
                (err, promise) => {
                  if (err) {
                    err.log(verbose);
                    return callback(true);
                  }
                  util.assertDefined(promise);

                  callback(false, { type: "completed", promise });
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

  private exec(callback: Callback<More | Done>) {
    const local: LocalTodo[] = [];
    const remote: RemoteTodo[] = [];

    let input: Value<any> = {
      type: "internal.nothing",
    };

    // next needs to be called when we want to go to the top of the loop but are inside a callback
    const next = () => {
      while (true) {
        const action = this.decorator.next(input);

        // Handle internal.async.l (lfi/lfc)
        if (action.type === "internal.async.l") {
          this.handler.createPromise(
            action.createReq,
            (err, res) => {
              if (err) {
                err.log(this.verbose);
                return callback(true);
              }
              util.assertDefined(res);

              const ctx = this.ctx.child({
                id: res.id,
                timeout: res.timeout,
                version: action.version,
                retryPolicy: action.retryPolicy,
              });

              if (res.state === "pending") {
                if (!util.isGeneratorFunction(action.func)) {
                  local.push({
                    id: action.id,
                    ctx: ctx,
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

                const coroutine = new Coroutine(
                  ctx,
                  this.verbose,
                  new Decorator(action.func(ctx, ...action.args)),
                  this.handler,
                  this.depth + 1,
                );

                const cb: Callback<More | Done> = (err, status) => {
                  if (err) return callback(err);
                  util.assertDefined(status);

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
                        state: status.result.success ? "resolved" : "rejected",
                        value: {
                          data: status.result.success ? status.result.value : status.result.error,
                        },
                        iKey: action.id,
                        strict: false,
                      },
                      (err, res) => {
                        if (err) {
                          err.log(this.verbose);
                          return callback(true);
                        }
                        util.assertDefined(res);
                        util.assert(res.state !== "pending", "promise must be completed");

                        input = {
                          type: "internal.promise",
                          state: "completed",
                          id: action.id,
                          value: {
                            type: "internal.literal",
                            value: res.state === "resolved" ? ok(res.value?.data) : ko(res.value?.data),
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
                // durable promise is completed
                input = {
                  type: "internal.promise",
                  state: "completed",
                  id: action.id,
                  value: {
                    type: "internal.literal",
                    value: res.state === "resolved" ? ok(res.value?.data) : ko(res.value?.data),
                  },
                };
                next();
              }
            },
            action.func.name,
          );
          return; // Exit the while loop to wait for async callback
        }

        // Handle internal.async.r
        if (action.type === "internal.async.r") {
          this.handler.createPromise(action.createReq, (err, res) => {
            if (err) {
              err.log(this.verbose);
              return callback(true);
            }
            util.assertDefined(res);

            if (res.state === "pending") {
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
                  value: res.state === "resolved" ? ok(res.value?.data) : ko(res.value?.data),
                },
              };
            }
            next();
          });
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
          return callback(true);
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
          callback(false, { type: "more", todo: { local, remote } });
          return;
        }

        // Handle return
        if (action.type === "internal.return") {
          util.assert(action.value.type === "internal.literal", "promise value must be an 'internal.literal' type");
          util.assertDefined(action.value);

          callback(false, { type: "done", result: action.value.value });
          return;
        }
      }
    };

    next();
  }
}
