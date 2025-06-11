import { Future, Invoke, type Yieldable } from "./context";
import type { InternalAsync, InternalAwait, InternalExpr, Literal, Value } from "./types";

// TODO(avillega): to support any function (not only generators) we could make this type take
// any function and make it look as a coroutine for the Computation type
export class Coroutine<TRet> {
  public uuid: string;
  private sequ: number;
  private invokes: string[];

  private generator: Generator<Invoke<any> | Future<any>, any, TRet>;
  constructor(uuid: string, generator: Generator<Yieldable, any, TRet>) {
    this.uuid = uuid;
    this.sequ = 0;
    this.generator = generator;
    this.invokes = [];
  }

  public next(value: Value<any>): InternalExpr<any> {
    const result = this.generator.next(this.toExternal(value));
    if (result.done) {
      if (this.invokes.length > 0) {
        const val = this.invokes.pop() ?? "";
        return {
          type: "internal.await",
          uuid: val,
          promise: {
            type: "internal.promise",
            state: "pending",
            uuid: val,
          },
        };
      }
      return {
        type: "internal.return",
        uuid: this.uuidsequ(),
        value: this.toLiteral(result.value),
      };
    }
    return this.toInternal(result.value);
  }

  // From internal type to external type
  private toExternal<T>(value: Value<T>): Future<T> | T | undefined {
    switch (value.type) {
      case "internal.nothing":
        return undefined;
      case "internal.promise":
        if (value.state === "pending") {
          // We have invoked somethings and is pending, if the user don't await it
          // explicitly we might need to await it as part of the structured concurrency
          this.invokes.push(value.uuid);
          return new Future<T>(value.uuid, "pending", undefined);
        }
        // promise === "complete"
        return new Future<T>(value.uuid, "completed", value.value.value);
      case "internal.literal":
        return value.value;
    }
  }

  // From external type to internal type
  private toInternal<T>(event: Invoke<T> | Future<T>): InternalAsync<T> | InternalAwait<T> {
    if (event instanceof Invoke) {
      const uuid = this.uuidsequ();
      return {
        type: "internal.async",
        uuid,
        kind: event.type,
        mode: "eager", // default, adjust if needed
        func: event.func,
        args: (event.args || []).map((arg: any, i: number) => ({
          type: "internal.literal",
          uuid: `${uuid}.arg${i}`,
          value: arg,
        })),
      };
    }
    if (event instanceof Future) {
      // Map Future to InternalPromise union
      if (event.isCompleted()) {
        return {
          type: "internal.await",
          uuid: event.uuid,
          promise: {
            type: "internal.promise",
            state: "completed",
            uuid: event.uuid,
            value: {
              type: "internal.literal",
              uuid: `${event.uuid}.completed`,
              value: event.value!,
            },
          },
        };
      }
      // Only pop from invokes if the future was not completed before
      // The user await the future we remove it from the invokes
      this.invokes = this.invokes.filter((uuid) => uuid !== event.uuid);
      return {
        type: "internal.await",
        uuid: event.uuid,
        promise: {
          type: "internal.promise",
          state: "pending",
          uuid: event.uuid,
        },
      };
    }
    throw new Error("Unexpected input to extToInt");
  }

  private uuidsequ(): string {
    return `${this.uuid}.${this.sequ++}`;
  }

  private toLiteral<T>(value: T): Literal<T> {
    // If value is undefined, use null as a fallback to avoid type error
    return {
      type: "internal.literal",
      uuid: this.uuidsequ(),
      value: value === undefined ? (null as any as T) : value,
    };
  }
}
