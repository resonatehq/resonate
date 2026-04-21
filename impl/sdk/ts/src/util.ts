import { randomUUID } from "node:crypto";
import type { Codec } from "./codec.js";
import type { InnerContext } from "./context.js";
import exceptions from "./exceptions.js";
import type { Logger } from "./logger.js";
import {
  isSuccess,
  type PromiseCreateReq,
  type PromiseCreateRes,
  type PromiseRecord,
  type PromiseSettleReq,
  type PromiseSettleRes,
} from "./network/types.js";
import { type Options, RESONATE_OPTIONS } from "./options.js";
import type { Effects, Func, Result, Send } from "./types.js";

// version

export const VERSION = "2026-04-01";

// time

export const MS = 1;
export const SEC = 1000;
export const MIN = 60 * SEC;
export const HOUR = 60 * MIN;

// assert

export function assert(cond: boolean, msg?: string): asserts cond {
  if (cond) return; // Early return if assertion passes

  console.assert(cond, "Assertion Failed: %s", msg);
  console.trace();

  if (typeof process !== "undefined" && process.versions.node) {
    process.exit(1);
  }
}

export function assertDefined<T>(val: T | undefined | null): asserts val is T {
  assert(val !== null && val !== undefined, "value must not be null");
}

export function isGeneratorFunction(func: Function): boolean {
  const GeneratorFunction = Object.getPrototypeOf(function* () {}).constructor;
  const AsyncGeneratorFunction = Object.getPrototypeOf(async function* () {}).constructor;
  return func instanceof GeneratorFunction || func instanceof AsyncGeneratorFunction;
}

// guards

export function isOptions(obj: unknown): obj is Options {
  return typeof obj === "object" && obj !== null && RESONATE_OPTIONS in obj;
}

// helpers

export function splitArgsAndOpts(args: any[], defaults: Options): [any[], Options] {
  const opts = isOptions(args.at(-1)) ? args.pop() : {};
  return [args, { ...defaults, ...opts }];
}

export function isUrl(str: string): boolean {
  try {
    new URL(str);
    return true;
  } catch {
    return false;
  }
}

export function base64Encode(str: string): string {
  const bytes = new TextEncoder().encode(str);
  return btoa(String.fromCharCode(...bytes));
}

export function base64Decode(str: string): string {
  const bytes = Uint8Array.from(atob(str), (c) => c.charCodeAt(0));
  const jsonStr = new TextDecoder().decode(bytes);
  return jsonStr;
}

export function semverLessThan(a: string, b: string): boolean {
  const [aMajor, aMinor, aPatch] = a.split(".").map((x) => Number.parseInt(x, 10));
  const [bMajor, bMinor, bPatch] = b.split(".").map((x) => Number.parseInt(x, 10));

  if (aMajor !== bMajor) return aMajor < bMajor;
  if (aMinor !== bMinor) return aMinor < bMinor;
  return aPatch < bPatch;
}

export function getCallerInfo(): string {
  const err = new Error();
  if (!err.stack) return "";

  const stack = err.stack.split("\n");

  // stack[0] is "Error"
  // stack[1] is this function (getCallerInfo)
  // stack[2] is the caller of this function
  // stack[3] is the info we want
  const callerLine = stack?.[3];

  return callerLine.trim();
}

// retry
export async function executeWithRetry(
  ctx: InnerContext,
  func: Func,
  args: any[],
  logger: Logger,
): Promise<Result<any, any>> {
  while (true) {
    try {
      const data = await func(ctx, ...args);
      return { kind: "value", value: data };
    } catch (error) {
      const isNonRetryable = ctx.nonRetryableErrors.some((Ctor) => error instanceof Ctor);
      if (isNonRetryable) {
        return { kind: "error", error };
      }

      const retryIn = ctx.retryPolicy.next(ctx.info.attempt);
      if (retryIn === null || ctx.clock.now() + retryIn >= ctx.info.timeout) {
        return { kind: "error", error };
      }
      logger.warn(
        { component: "runtime", func: ctx.func, attempt: ctx.info.attempt, retryIn },
        `Function '${ctx.func}' failed with '${String(error)}' (retrying in ${retryIn / 1000} secs)`,
      );
      logger.debug(
        { component: "runtime", func: ctx.func, error: error instanceof Error ? error.stack : String(error) },
        `Retry error details for '${ctx.func}'`,
      );
      ctx.info.attempt++;
      await new Promise((resolve) => setTimeout(resolve, retryIn));
    }
  }
}

// effects
export function buildEffects(
  send: Send,
  codec: Codec,
  fence: { id: string; version: number },
  preload: PromiseRecord[] = [],
): Effects {
  // Decode preloaded promises, skipping any that fail to decode
  const cache = new Map<string, PromiseRecord>();
  const mergePreload = (pls: PromiseRecord[]) => {
    for (const p of pls) {
      try {
        const decoded = codec.decodePromise(p);
        cache.set(p.id, decoded);
      } catch {
        // skip promises that fail to decode
      }
    }
  };
  mergePreload(preload);

  async function sendFenced<R extends PromiseCreateReq | PromiseSettleReq>(
    inner: R,
  ): Promise<R extends PromiseCreateReq ? PromiseCreateRes : PromiseSettleRes> {
    const outer = await send({
      kind: "task.fence",
      head: { corrId: randomUUID(), version: VERSION },
      data: { id: fence.id, version: fence.version, action: inner },
    });
    if (!isSuccess(outer)) {
      throw exceptions.SERVER_ERROR(outer.data, true, {
        code: outer.head.status,
        message: outer.data,
      });
    }
    mergePreload(outer.data.preload);
    return outer.data.action as any;
  }

  return {
    promiseCreate: async (req, func = "unknown") => {
      const cached = cache.get(req.data.id);
      if (cached) {
        return cached;
      }

      try {
        req.data.param = codec.encode(req.data.param.data);
      } catch (e) {
        throw exceptions.ENCODING_ARGS_UNENCODEABLE(func, e);
      }

      const res = await sendFenced(req);
      if (!isSuccess(res)) {
        throw exceptions.SERVER_ERROR(res.data, true, {
          code: res.head.status,
          message: res.data,
        });
      }

      const decoded = codec.decodePromise(res.data.promise);
      cache.set(decoded.id, decoded);
      return decoded;
    },

    promiseSettle: async (req, func = "unknown") => {
      const cached = cache.get(req.data.id);
      if (cached && cached.state !== "pending") {
        return cached;
      }

      try {
        req.data.value = codec.encode(req.data.value.data);
      } catch (e) {
        throw exceptions.ENCODING_RETV_UNENCODEABLE(func, e);
      }

      const res = await sendFenced(req);
      if (!isSuccess(res)) {
        throw exceptions.SERVER_ERROR(res.data, true, {
          code: res.head.status,
          message: res.data,
        });
      }

      const decoded = codec.decodePromise(res.data.promise);
      cache.set(decoded.id, decoded);
      return decoded;
    },
  };
}
