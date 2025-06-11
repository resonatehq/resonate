import type { Yieldable } from "./context";
import { Coroutine } from "./coroutine";
import type { Handler } from "./handler";
import type { InternalAsync, Value } from "./types";

export type Suspended = {
  type: "suspended";
  todos: InternalAsync<any>[];
};

export type Completed<T> = {
  type: "completed";
  value: T;
};

export class Computation<T> {
  private coroutine: Coroutine<T>;
  private handler: Handler;

  constructor(coroutine: Coroutine<T>, handler: Handler) {
    this.coroutine = coroutine;
    this.handler = handler;
  }

  public static exec<T>(
    uuid: string,
    func: (...args: any[]) => Generator<Yieldable, T, any>, // TODO: support any function as well
    args: any[],
    handler: Handler,
  ): Suspended | Completed<T> {
    const durable = handler.createPromise<T>(uuid);
    if (durable.state === "pending") {
      const c = new Computation(new Coroutine<T>(uuid, func(...args)), handler);
      const r = c.exec();

      if (r.type === "completed") {
        handler.resolvePromise(uuid, r.value);
      }
      return r;
    }

    return { type: "completed", value: durable.value! };
  }

  public exec(): Suspended | Completed<T> {
    const todos: InternalAsync<any>[] = [];
    let input: Value<any> = {
      type: "internal.nothing",
      uuid: `${this.coroutine.uuid}.nothing`,
    };

    // Exahust the generator until it can not make more progress.
    while (true) {
      const action = this.coroutine.next(input);

      // Handle int-async with lfi/lfc kind
      if (action.type === "internal.async" && action.kind === "lfi") {
        const durable = this.handler.createPromise(action.uuid);
        if (durable.state === "pending") {
          const c = new Computation(
            new Coroutine(
              action.uuid,
              action.func(
                ...(action.args?.map((arg) => (arg.type === "internal.literal" ? arg.value : undefined)) ?? []),
              ),
            ),
            this.handler,
          );
          const r = c.exec();
          if (r.type === "suspended") {
            todos.push(...r.todos);
            input = {
              type: "internal.promise",
              state: "pending",
              uuid: action.uuid,
            };
          } else {
            this.handler.resolvePromise(action.uuid, r.value);
            input = {
              type: "internal.promise",
              state: "completed",
              uuid: action.uuid,
              value: {
                type: "internal.literal",
                uuid: `${action.uuid}.completed`,
                value: r.value, // TODO: Return the value as encoded
              },
            };
          }
        } else {
          // durable promise is completed
          input = {
            type: "internal.promise",
            state: "completed",
            uuid: action.uuid,
            value: {
              type: "internal.literal",
              uuid: `${action.uuid}.completed`,
              value: durable.value,
            },
          };
        }
        // Continue at the top of the loop
        continue;
      }

      // Handle async with rfi kind
      if (action.type === "internal.async" && action.kind === "rfi") {
        const durable = this.handler.createPromise(action.uuid);
        if (durable.state === "pending") {
          todos.push(action);
          input = {
            type: "internal.promise",
            state: "pending",
            uuid: action.uuid,
          };
        } else {
          input = {
            type: "internal.promise",
            state: "completed",
            uuid: action.uuid,
            value: {
              type: "internal.literal",
              uuid: `${action.uuid}.completed`,
              value: durable.value,
            },
          };
        }
        continue;
      }

      // Handle await
      if (action.type === "internal.await" && action.promise.state === "completed") {
        input = {
          type: "internal.literal",
          uuid: `${action.promise.uuid}.literal`,
          value:
            action.promise.value && action.promise.value.type === "internal.literal"
              ? action.promise.value.value
              : undefined,
        };
        continue;
      }

      // Return when a awaiting a pending "Future" the list of todos will include
      // the global callbacks to create.
      if (action.type === "internal.await" && action.promise.state === "pending") {
        return { type: "suspended", todos };
      }

      // Handle return
      if (action.type === "internal.return") {
        return {
          type: "completed",
          value: action?.value?.type === "internal.literal" ? action.value.value : undefined,
        };
      }
    }
  }
}
