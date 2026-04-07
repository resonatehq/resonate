import { DIE, Future, LFC, LFI, RFC, RFI } from "./context.js";
import type { ResonateError } from "./exceptions.js";
import type { PromiseCreateReq } from "./network/types.js";
import type { RetryPolicy } from "./retries.js";
import type { Func, Result, Yieldable } from "./types.js";
import * as util from "./util.js";

// Expr

export type InternalExpr<T> = InternalAsyncL | InternalAsyncR | InternalAwait<T> | InternalReturn<T> | InternalDie;

export type InternalDie = {
  type: "internal.die";
  condition: boolean;
  error: ResonateError;
};

export type InternalAsyncR = {
  type: "internal.async.r";
  id: string;
  mode: "attached" | "detached";
  func: string;
  version: number;
  createReq: PromiseCreateReq;
};

export type InternalAsyncL = {
  type: "internal.async.l";
  id: string;
  func: Func;
  args: any[];
  version: number;
  retryPolicy: RetryPolicy;
  createReq: PromiseCreateReq;
};
export type InternalAwait<T> = {
  type: "internal.await";
  id: string;
  promise: PromisePending | PromiseCompleted<T>;
};

export type InternalReturn<T> = {
  type: "internal.return";
  value: Literal<T>;
};

// Value

export type Value<T> = Nothing | Literal<T> | PromisePending | PromiseCompleted<T>;

export type Nothing = {
  type: "internal.nothing";
};

export type Literal<T> = {
  type: "internal.literal";
  value: Result<T, any>;
};

export type PromisePending = {
  type: "internal.promise";
  state: "pending";
  mode: "attached" | "detached";
  id: string;
};

export type PromiseCompleted<T> = {
  type: "internal.promise";
  state: "completed";
  id: string;
  value: Literal<T>;
};

export class Decorator<TRet> {
  private generator: Generator<Yieldable, TRet, any>;

  // Tracks the id of a pending "call" (LFC/RFC) that needs auto-await.
  // Because `yield*` on LFC/RFC iterators produces two yields, the decorator
  // intercepts the promise response and auto-emits an `internal.await` before
  // feeding the resolved value back to the generator.
  // null = not mid-call, non-null = waiting for the promise for this call id.
  private pendingCall: string | null = null;

  private nextState: "internal.nothing" | "internal.promise" | "internal.literal" | "over" = "internal.nothing";

  constructor(generator: Generator<Yieldable, TRet, any>) {
    this.generator = generator;
  }

  public next(value: Value<any>): InternalExpr<any> {
    util.assert(
      value.type === this.nextState,
      `Generator called with type "${value.type}" expected "${this.nextState}"`,
    );

    // Auto-await for calls (LFC/RFC): intercept the promise response and
    // emit an `internal.await` so the coroutine resolves the value before
    // feeding it back to the generator.
    if (value.type === "internal.promise" && this.pendingCall !== null) {
      const callId = this.pendingCall;
      this.pendingCall = null;
      this.nextState = "internal.literal";
      return { type: "internal.await", id: callId, promise: value };
    }

    const result = this.safeGeneratorNext(this.toExternal(value));
    if (result.done) {
      this.nextState = "over";
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
  private toExternal<T>(value: Value<T>): Result<Future<T> | T | undefined, any> {
    switch (value.type) {
      case "internal.nothing":
        return { kind: "value", value: undefined };
      case "internal.promise":
        if (value.state === "pending") {
          return { kind: "value", value: new Future<T>(value.id, "pending", undefined, value.mode) };
        }
        return { kind: "value", value: new Future<T>(value.id, "completed", value.value.value) };
      case "internal.literal":
        return value.value;
    }
  }

  // From external type to internal type
  private toInternal<T>(
    event: LFI<T> | RFI<T> | LFC<T> | RFC<T> | Future<T> | DIE,
  ): InternalAsyncL | InternalAsyncR | InternalAwait<T> | InternalDie {
    if (event instanceof LFI || event instanceof LFC) {
      if (event instanceof LFC) this.pendingCall = event.id;
      this.nextState = "internal.promise";
      return {
        type: "internal.async.l",
        id: event.id,
        func: event.func,
        args: event.args ?? [],
        version: event.version,
        retryPolicy: event.retryPolicy,
        createReq: event.createReq,
      };
    }
    if (event instanceof RFI || event instanceof RFC) {
      if (event instanceof RFC) this.pendingCall = event.id;

      this.nextState = "internal.promise";
      return {
        type: "internal.async.r",
        id: event.id,
        mode: event.mode,
        func: event.func,
        version: event.version,
        createReq: event.createReq,
      };
    }
    if (event instanceof DIE) {
      this.nextState = "internal.nothing";
      return {
        type: "internal.die",
        condition: event.condition,
        error: event.error,
      };
    }
    if (event instanceof Future) {
      this.nextState = "internal.literal";

      if (event.state === "completed") {
        util.assertDefined(event.value);
        return {
          type: "internal.await",
          id: event.id,
          promise: {
            type: "internal.promise",
            state: "completed",
            id: event.id,
            value: { type: "internal.literal", value: event.value },
          },
        };
      }

      return {
        type: "internal.await",
        id: event.id,
        promise: {
          type: "internal.promise",
          state: "pending",
          // biome-ignore lint/complexity/useLiteralKeys: We need to access this private member, it is only private to the user
          mode: event["mode"],
          id: event.id,
        },
      };
    }
    throw new Error("Unexpected input to extToInt");
  }

  private toLiteral<T>(result: Result<T, any>): Literal<T> {
    return {
      type: "internal.literal",
      value: result,
    };
  }

  private safeGeneratorNext<T>(
    value: Result<Future<T> | T | undefined, any>,
  ): IteratorResult<Yieldable, Result<TRet, any>> {
    try {
      let itResult: IteratorResult<Yieldable, TRet>;
      if (value.kind === "error") {
        itResult = this.generator.throw(value.error);
      } else {
        itResult = this.generator.next(value.value);
      }

      if (!itResult.done) {
        return itResult;
      }
      return {
        done: true,
        value: { kind: "value", value: itResult.value },
      };
    } catch (e) {
      return {
        done: true,
        value: { kind: "error", error: e },
      };
    }
  }
}
