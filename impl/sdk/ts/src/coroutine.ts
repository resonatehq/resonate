import { Call, Future, Invoke, type Yieldable } from "./context";
import type { InternalAsync, InternalAwait, InternalExpr, Literal, Value } from "./types";
import * as util from "./util";

export class Coroutine<TRet> {
  public uuid: string;
  private sequ: number;
  private invokes: { kind: "call" | "invoke"; uuid: string }[];
  private generator: Generator<Yieldable, any, TRet>;
  private nextState: "internal.nothing" | "internal.promise" | "internal.literal" | "over" = "internal.nothing";

  constructor(uuid: string, generator: Generator<Yieldable, any, TRet>) {
    this.uuid = uuid;
    this.sequ = 0;
    this.generator = generator;
    this.invokes = [];
  }

  public next(value: Value<any>): InternalExpr<any> {
    // If nextState was set to over, is becasue we shouldn't have been called
    util.assert(
      this.nextState !== "over" && value.type === this.nextState,
      `Coroutine called wit type "${value.type}" expected "${this.nextState}"`,
    );

    // Handle rfc/lfc by returning an await if the previous invocation was a call
    if (value.type === "internal.promise" && this.invokes.length > 0) {
      const prevInvoke = this.invokes.at(-1)!;
      if (prevInvoke.kind === "call") {
        this.invokes.pop();
        this.nextState = value.state === "completed" ? "internal.literal" : "over";
        return {
          type: "internal.await",
          uuid: prevInvoke.uuid,
          promise: value,
        };
      }
    }

    const result = this.generator.next(this.toExternal(value));
    if (result.done) {
      this.nextState = "over";
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
      this.nextState = "internal.promise";
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
        this.nextState = "internal.literal";
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
      this.nextState = "over";
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
