import { Context, type Yieldable } from "./context";
import { Decorator } from "./decorator";
import type { Handler } from "./handler";
import type { DurablePromiseRecord } from "./network/network";
import { type Literal, type Result, type Value, ko, ok } from "./types";
import * as util from "./util";

export interface LocalTodo {
  id: string;
  func: (ctx: Context, ...args: any[]) => any;
  ctx: Context;
  args: any[];
  // Need function to execute and id to resolve the promise
}

export interface RemoteTodo {
  id: string;
  // Probably only need the promiseId to create the callback
}

export type Suspended = {
  type: "suspended";
  localTodos: LocalTodo[];
  remoteTodos: RemoteTodo[];
};

export type Completed<T> = {
  type: "completed";
  value: Result<T>;
};

function extractResult<T>(durablePromise: DurablePromiseRecord): Literal<T> {
  util.assert(durablePromise.state !== "pending", "Can not get result from a pending promise");
  const value: Result<T> = durablePromise.state === "resolved" ? ok(durablePromise.value) : ko(durablePromise.value);

  return {
    type: "internal.literal",
    value,
  };
}

export class Coroutine<T> {
  private decorator: Decorator<T>;
  private handler: Handler;
  private ctx: Context;

  constructor(ctx: Context, decorator: Decorator<T>, handler: Handler) {
    this.decorator = decorator;
    this.handler = handler;
    this.ctx = ctx;
  }

  public static exec<T>(
    id: string,
    func: (ctx: Context, ...args: any[]) => Generator<Yieldable, T, any>, // TODO: support any function as well
    args: any[],
    handler: Handler,
    callback: (result: Suspended | Completed<T>) => void,
  ): void {
    handler.createPromise({ id, timeout: Number.MAX_SAFE_INTEGER, tags: {}, func: func.name, args }, (durable) => {
      if (durable.state === "pending") {
        const ctx = new Context();
        const c = new Coroutine(ctx, new Decorator<T>(id, func(ctx, ...args)), handler);
        c.exec((r) => {
          if (r.type === "completed") {
            handler.completePromise(id, r.value, () => {
              callback(r);
            });
          } else {
            callback(r);
          }
        });
      } else {
        const f = durable.state === "resolved" ? ok : ko;
        callback({ type: "completed", value: f(durable.value) });
      }
    });
  }

  private exec(callback: (result: Suspended | Completed<T>) => void): void {
    const localTodos: LocalTodo[] = [];
    const remoteTodos: RemoteTodo[] = [];
    let input: Value<any> = {
      type: "internal.nothing",
    };

    // next needs to be called when we want to go to the top of the loop but are inside a callback
    const next = () => {
      while (true) {
        const action = this.decorator.next(input);

        // Handle internal.async.l (lfi/lfc)
        if (action.type === "internal.async.l") {
          this.handler.createPromise({ id: action.id, timeout: Number.MAX_SAFE_INTEGER, tags: {} }, (durable) => {
            if (durable.state === "pending") {
              if (!util.isGeneratorFunction(action.func)) {
                localTodos.push({
                  ctx: this.ctx,
                  id: action.id,
                  func: action.func,
                  args: action.args,
                });
                input = {
                  type: "internal.promise",
                  state: "pending",
                  id: action.id,
                };
                next(); // Go back to the top of the loop
                return;
              }

              const c = new Coroutine(
                this.ctx,
                new Decorator(action.id, action.func(this.ctx, ...action.args)),
                this.handler,
              );
              c.exec((r) => {
                if (r.type === "suspended") {
                  localTodos.push(...r.localTodos);
                  remoteTodos.push(...r.remoteTodos);
                  input = {
                    type: "internal.promise",
                    state: "pending",
                    id: action.id,
                  };
                  next();
                } else {
                  this.handler.completePromise(action.id, r.value, (durable) => {
                    input = {
                      type: "internal.promise",
                      state: "completed",
                      id: action.id,
                      value: extractResult(durable),
                    };
                    next();
                  });
                }
              });
            } else {
              // durable promise is completed
              input = {
                type: "internal.promise",
                state: "completed",
                id: action.id,
                value: extractResult(durable),
              };
              next();
            }
          });
          return; // Exit the while loop to wait for async callback
        }

        // Handle internal.async.r
        if (action.type === "internal.async.r") {
          this.handler.createPromise(
            {
              id: action.id,
              timeout: action.opts.timeout + Date.now(), // TODO(avillega): this is not deterministic, chage it
              tags: { "resonate:invoke": `poll://any@${action.opts.target}` }, // TODO(avillega): remove the poll assumption, might need server work
              func: action.func,
              args: action.args ?? [],
            },
            (durable) => {
              if (durable.state === "pending") {
                remoteTodos.push({ id: action.id });
                input = {
                  type: "internal.promise",
                  state: "pending",
                  id: action.id,
                };
              } else {
                input = {
                  type: "internal.promise",
                  state: "completed",
                  id: action.id,
                  value: extractResult(durable),
                };
              }
              next();
            },
          );
          return; // Exit the while loop to wait for async callback
        }

        // Handle await
        if (action.type === "internal.await" && action.promise.state === "completed") {
          input = action.promise.value;
          continue;
        }

        // invoke the callback when awaiting a pending "Future" the list of todos will include
        // the global callbacks to create.
        if (action.type === "internal.await" && action.promise.state === "pending") {
          callback({ type: "suspended", localTodos, remoteTodos });
          return;
        }

        // Handle return
        if (action.type === "internal.return") {
          callback({
            type: "completed",
            value: action.value.value,
          });
          return;
        }
      }
    };

    next();
  }
}
