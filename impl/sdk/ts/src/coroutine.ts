import type { Yieldable } from "./context";
import { Decorator } from "./decorator";
import type { Handler } from "./handler";
import type { InternalAsync, Value } from "./types";
import * as util from "./util";

export type Suspended = {
  type: "suspended";
  todos: InternalAsync<any>[];
};

export type Completed<T> = {
  type: "completed";
  value: T;
};

export class Coroutine<T> {
  private decorator: Decorator<T>;
  private handler: Handler;

  constructor(decorator: Decorator<T>, handler: Handler) {
    this.decorator = decorator;
    this.handler = handler;
  }

  public static exec<T>(
    id: string,
    func: (...args: any[]) => Generator<Yieldable, T, any>, // TODO: support any function as well
    args: any[],
    handler: Handler,
    callback: (result: Suspended | Completed<T>) => void,
  ): void {
    handler.createPromise<T>({ id, timeout: Number.MAX_SAFE_INTEGER, tags: {}, fn: func.name, args }, (durable) => {
      if (durable.state === "pending") {
        const c = new Coroutine(new Decorator<T>(id, func(...args)), handler);
        c.exec((r) => {
          if (r.type === "completed") {
            handler.resolvePromise(id, r.value, () => {
              callback(r);
            });
          } else {
            callback(r);
          }
        });
      } else {
        callback({ type: "completed", value: durable.value! });
      }
    });
  }

  public exec(callback: (result: Suspended | Completed<T>) => void): void {
    const todos: InternalAsync<any>[] = [];
    let input: Value<any> = {
      type: "internal.nothing",
      id: `${this.decorator.id}.nothing`,
    };

    // next needs to be called when we want to go to the top of the loop but are inside a callback
    const next = () => {
      while (true) {
        const action = this.decorator.next(input);

        // Handle internal.async with lfi kind
        if (action.type === "internal.async" && action.kind === "lfi") {
          this.handler.createPromise({ id: action.id, timeout: Number.MAX_SAFE_INTEGER, tags: {} }, (durable) => {
            if (durable.state === "pending") {
              if (!util.isGeneratorFunction(action.func)) {
                todos.push(action);
                input = {
                  type: "internal.promise",
                  state: "pending",
                  id: action.id,
                };
                next(); // Go back to the top of the loop
                return;
              }

              const c = new Coroutine(
                new Decorator(
                  action.id,
                  action.func(
                    ...(action.args?.map((arg) => (arg.type === "internal.literal" ? arg.value : undefined)) ?? []),
                  ),
                ),
                this.handler,
              );
              c.exec((r) => {
                if (r.type === "suspended") {
                  todos.push(...r.todos);
                  input = {
                    type: "internal.promise",
                    state: "pending",
                    id: action.id,
                  };
                  next();
                } else {
                  this.handler.resolvePromise(action.id, r.value, (durable) => {
                    input = {
                      type: "internal.promise",
                      state: "completed",
                      id: action.id,
                      value: {
                        type: "internal.literal",
                        id: `${action.id}.completed`,
                        value: durable.value,
                      },
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
                value: {
                  type: "internal.literal",
                  id: `${action.id}.completed`,
                  value: durable.value,
                },
              };
              next();
            }
          });
          return; // Exit the while loop to wait for async callback
        }

        // Handle internal.async with rfi kind
        if (action.type === "internal.async" && action.kind === "rfi") {
          this.handler.createPromise(
            {
              id: action.id,
              timeout: Number.MAX_SAFE_INTEGER,
              tags: { "resonate:invoke": "default" },
              fn: action.func.name,
              args: action.args?.map((arg) => (arg.type === "internal.literal" ? arg.value : undefined)) ?? [],
            },
            (durable) => {
              if (durable.state === "pending") {
                todos.push(action);
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
                  value: {
                    type: "internal.literal",
                    id: `${action.id}.completed`,
                    value: durable.value,
                  },
                };
              }
              next();
            },
          );
          return; // Exit the while loop to wait for async callback
        }

        // Handle await
        if (action.type === "internal.await" && action.promise.state === "completed") {
          util.assert(
            action.promise.value && action.promise.value.type === "internal.literal",
            "Promise value must be an 'internal.literal' type",
          );
          input = {
            type: "internal.literal",
            id: `${action.promise.id}.literal`,
            value: action.promise.value.value,
          };
          continue;
        }

        // invoke the callback when a awaiting a pending "Future" the list of todos will include
        // the global callbacks to create.
        if (action.type === "internal.await" && action.promise.state === "pending") {
          callback({ type: "suspended", todos });
          return;
        }

        // Handle return
        if (action.type === "internal.return") {
          util.assert(
            action.value && action.value.type === "internal.literal",
            "Promise value must be an 'internal.literal' type",
          );
          callback({
            type: "completed",
            value: action?.value?.type === "internal.literal" ? action.value.value : undefined, // Even with the assertion on top it is neccesary to have the ternary condition to make the typesystem happy
          });
          return;
        }
      }
    };

    next();
  }
}
