import { Call, Future, Invoke, type Yieldable } from "./context";
import type { InternalAsync, InternalAwait, InternalExpr, Literal, Value } from "./types";

// TODO(avillega): to support any function (not only generators) we could make this Class take
// any function and make it look as a Coroutine for the Computation type
export class Coroutine<TRet> {
  public uuid: string;
  private sequ: number;
  private invokes: { kind: "call" | "invoke"; uuid: string }[];
  private generator: Generator<Yieldable, any, TRet>;

  constructor(uuid: string, generator: Generator<Yieldable, any, TRet>) {
    this.uuid = uuid;
    this.sequ = 0;
    this.generator = generator;
    this.invokes = [];
  }

  public next(value: Value<any>): InternalExpr<any> {
    // Handle rfc/lfc by either returning an await if the promise is not completed
    // or replacing the promise with a literal value if the promise was completed
    if (value.type === "internal.promise" && this.invokes.length > 0) {
      const prevInvoke = this.invokes.at(-1)!;
      if (prevInvoke.kind === "call") {
        this.invokes.pop();
        return {
          type: "internal.await",
          uuid: prevInvoke.uuid,
          promise: value,
        };
      }
    }

    const result = this.generator.next(this.toExternal(value));
    if (result.done) {
      if (this.invokes.length > 0) {
        const val = this.invokes.pop()!;
        return {
          type: "internal.await",
          uuid: val.uuid,
          promise: {
            type: "internal.promise",
            state: "pending",
            uuid: val.uuid,
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
          return new Future<T>(value.uuid, "pending", undefined);
        }
        // promise === "complete"
        // We know for sure this promise relates to the last invoke inserted
        this.invokes.pop();
        return new Future<T>(value.uuid, "completed", value.value.value);
      case "internal.literal":
        return value.value;
    }
  }

  // From external type to internal type
  private toInternal<T>(event: Invoke<T> | Future<T> | Call<T>): InternalAsync<T> | InternalAwait<T> {
    if (event instanceof Invoke || event instanceof Call) {
      const uuid = this.uuidsequ();
      this.invokes.push({ kind: event instanceof Invoke ? "invoke" : "call", uuid });
      return {
        type: "internal.async",
        uuid,
        kind: this.mapKind(event.type),
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
      // If the Future was completed (the promise was completed) we already poped the related invoke
      // when the user awaits the future we remove it from the invokes
      this.invokes = this.invokes.filter(({ uuid }) => uuid !== event.uuid);
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

  private mapKind(k: "lfi" | "rfi" | "lfc" | "rfc"): "lfi" | "rfi" {
    if (k === "lfi" || k === "rfi") return k;
    if (k === "lfc") return "lfi";
    if (k === "rfc") return "rfi";
    throw new Error(`Unknown value ${k}`);
  }
}
