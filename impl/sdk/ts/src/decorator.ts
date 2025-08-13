import { Future, LFC, LFI, RFC, RFI, type Yieldable } from "./context";
import {
  type InternalAsyncL,
  type InternalAsyncR,
  type InternalAwait,
  type InternalExpr,
  type Literal,
  type Result,
  type Value,
  ko,
  ok,
} from "./types";
import * as util from "./util";

export class Decorator<TRet> {
  public id: string;
  private sequ: number;
  private invokes: { kind: "call" | "invoke"; id: string }[];
  private generator: Generator<Yieldable, TRet, any>;
  private nextState: "internal.nothing" | "internal.promise" | "internal.literal" | "over" = "internal.nothing";

  constructor(id: string, generator: Generator<Yieldable, TRet, any>) {
    this.id = id;
    this.sequ = 0;
    this.generator = generator;
    this.invokes = [];
  }

  public next(value: Value<any>): InternalExpr<any> {
    // If nextState was set to over, is becasue we shouldn't have been called
    util.assert(
      value.type === this.nextState,
      `Generator called wit type "${value.type}" expected "${this.nextState}"`,
    );

    // Handle rfc/lfc by returning an await if the previous invocation was a call
    // if we await an rpc/rfc promise that is not completed the corotine is done and the decorator shouldn't be called again
    if (value.type === "internal.promise" && this.invokes.length > 0) {
      const prevInvoke = this.invokes.at(-1)!;
      if (prevInvoke.kind === "call") {
        this.invokes.pop();
        this.nextState = value.state === "completed" ? "internal.literal" : "over";
        return {
          type: "internal.await",
          id: prevInvoke.id,
          promise: value,
        };
      }
    }

    const result = this.safeGeneratorNext(this.toExternal(value));
    if (result.done) {
      this.nextState = "over";
      if (this.invokes.length > 0) {
        // Handles structured concurrency
        const val = this.invokes.pop()!;
        return {
          type: "internal.await",
          id: val.id,
          promise: {
            type: "internal.promise",
            state: "pending",
            id: val.id,
          },
        };
      }
      return {
        type: "internal.return",
        value: this.toLiteral(result.value),
      };
    }
    return this.toInternal(result.value);
  }

  // From internal type to external type
  // Having to return a Result<> is an artifact of not being able to check
  // the instance of "Result" at runtime
  private toExternal<T>(value: Value<T>): Result<Future<T> | T | undefined> {
    switch (value.type) {
      case "internal.nothing":
        return ok(undefined);
      case "internal.promise":
        if (value.state === "pending") {
          return ok(new Future<T>(value.id, "pending", undefined));
        }
        // promise === "complete"
        // We know for sure this promise relates to the last invoke inserted
        this.invokes.pop();
        return ok(new Future<T>(value.id, "completed", value.value.value));
      case "internal.literal":
        return value.value;
    }
  }

  // From external type to internal type
  private toInternal<T>(
    event: LFI<T> | RFI<T> | Future<T> | LFC<T> | RFC<T>,
  ): InternalAsyncL | InternalAsyncR | InternalAwait<T> {
    if (event instanceof LFI || event instanceof LFC) {
      const id = this.idsequ();
      this.invokes.push({ kind: event instanceof LFI ? "invoke" : "call", id });
      this.nextState = "internal.promise";
      return {
        type: "internal.async.l",
        id,
        func: event.func,
        args: event.args ?? [],
        mode: "eager", // default, adjust if needed
      };
    }
    if (event instanceof RFI || event instanceof RFC) {
      const id = this.idsequ();
      this.invokes.push({ kind: event instanceof RFI ? "invoke" : "call", id });
      this.nextState = "internal.promise";
      return {
        type: "internal.async.r",
        id,
        func: event.func,
        args: event.args ?? [],
        opts: event.opts,
        mode: "eager", // default, adjust if needed
      };
    }
    if (event instanceof Future) {
      // Map Future to InternalPromise union
      if (event.state === "completed") {
        this.nextState = "internal.literal";
        return {
          type: "internal.await",
          id: event.id,
          promise: {
            type: "internal.promise",
            state: "completed",
            id: event.id,
            value: {
              type: "internal.literal",
              // biome-ignore lint/complexity/useLiteralKeys: We need to access this private member, it is only private to the user
              value: event["value"]!,
            },
          },
        };
      }
      // If the Future was completed (the promise was completed) we already poped the related invoke,
      // when the user awaits the future we remove it from the invokes
      this.invokes = this.invokes.filter(({ id }) => id !== event.id);
      this.nextState = "over";
      return {
        type: "internal.await",
        id: event.id,
        promise: {
          type: "internal.promise",
          state: "pending",
          id: event.id,
        },
      };
    }
    throw new Error("Unexpected input to extToInt");
  }

  private idsequ(): string {
    return `${this.id}.${this.sequ++}`;
  }

  private toLiteral<T>(result: Result<T>): Literal<T> {
    return {
      type: "internal.literal",
      value: result,
    };
  }

  private safeGeneratorNext<T>(value: Result<Future<T> | T | undefined>): IteratorResult<Yieldable, Result<TRet>> {
    try {
      let itResult: IteratorResult<Yieldable, TRet>;
      if (!value.success) {
        itResult = this.generator.throw(value.error);
      } else {
        itResult = this.generator.next(value.data);
      }

      if (!itResult.done) {
        return itResult;
      }
      return {
        done: true,
        value: ok(itResult.value),
      };
    } catch (e) {
      return {
        done: true,
        value: ko(e),
      };
    }
  }
}
