export type RetryPolicy = Exponential | Linear | Never;

export type Exponential = {
  kind: "exponential";
  initialDelayMs: number;
  backoffFactor: number;
  maxAttempts: number;
  maxDelayMs: number;
};

export type Linear = {
  kind: "linear";
  delayMs: number;
  maxAttempts: number;
};

export type Never = {
  kind: "never";
};

export function isRetryPolicy(value: unknown): value is RetryPolicy {
  // Check if the value is an object
  if (typeof value !== "object" || value === null) {
    return false;
  }

  // Check if the object has a 'kind' property and if its value is a valid kind string
  const kindValue = (value as RetryPolicy).kind;
  if (kindValue !== "exponential" && kindValue !== "linear" && kindValue !== "never") {
    return false;
  }

  // Check if the object matches the corresponding type based on the 'kind' value
  switch (kindValue) {
    case "exponential":
      return (
        "initialDelayMs" in value &&
        "backoffFactor" in value &&
        "maxAttempts" in value &&
        "maxDelayMs" in value &&
        typeof (value as Exponential).initialDelayMs === "number" &&
        typeof (value as Exponential).backoffFactor === "number" &&
        typeof (value as Exponential).maxAttempts === "number" &&
        typeof (value as Exponential).maxDelayMs === "number"
      );
    case "linear":
      return (
        "delayMs" in value &&
        "maxAttempts" in value &&
        typeof (value as Linear).delayMs === "number" &&
        typeof (value as Linear).maxAttempts === "number"
      );
    case "never":
      return true; // No additional properties to check for 'never' type
    default:
      return false; // unreachable
  }
}
export function exponential(
  initialDelayMs: number = 100,
  backoffFactor: number = 2,
  maxAttempts: number = Infinity,
  maxDelayMs: number = 60000,
): Exponential {
  return {
    kind: "exponential",
    initialDelayMs,
    backoffFactor,
    maxAttempts,
    maxDelayMs,
  };
}

export function linear(delayMs: number = 1000, maxAttempts: number = Infinity): Linear {
  return {
    kind: "linear",
    delayMs,
    maxAttempts,
  };
}

export function never(): Never {
  return { kind: "never" };
}

/**
 * Returns an iterable iterator that yields delay values for each retry attempt,
 * based on the specified retry policy.
 * The iterator stops yielding delay values when either the timeout is reached or the maximum
 * number of attempts is exceeded.
 *
 * @param ctx - The context object containing the retry policy, attempt number, and timeout.
 * @returns An iterable iterator that yields delay values for each retry attempt.
 *
 */
export function retryIterator<T extends { retryPolicy: RetryPolicy; attempt: number; timeout: number }>(
  ctx: T,
): IterableIterator<number> {
  const { initialDelay, backoffFactor, maxAttempts, maxDelay } = retryDefaults(ctx.retryPolicy);

  const __next = (itCtx: { attempt: number; timeout: number }): { done: boolean; delay?: number } => {
    // attempt 0: 0ms delay
    // attampt n: {initial * factor^(attempt-1)}ms delay (or max delay)
    const delay = Math.min(
      Math.min(itCtx.attempt, 1) * initialDelay * Math.pow(backoffFactor, itCtx.attempt - 1),
      maxDelay,
    );

    if (Date.now() + delay >= itCtx.timeout || itCtx.attempt >= maxAttempts) {
      return { done: true };
    }

    return {
      done: false,
      delay: delay,
    };
  };

  return {
    next() {
      const { done, delay } = __next(ctx);
      return { done, value: delay || 0 };
    },
    [Symbol.iterator]() {
      return this;
    },
  };
}

export async function runWithRetry<T>(
  func: () => Promise<T>,
  onRetry: () => Promise<void>,
  retryPolicy: RetryPolicy,
  timeout: number,
) {
  let error;

  const ctx = { attempt: 0, retryPolicy, timeout };
  // invoke the function according to the retry policy
  for (const delay of retryIterator(ctx)) {
    await new Promise((resolve) => setTimeout(resolve, delay));

    if (ctx.attempt > 0) {
      await onRetry();
    }

    try {
      return await func();
    } catch (e) {
      error = e;
      // bump the attempt count
      ctx.attempt++;
    }
  }

  // if all attempts fail throw the last error
  throw error;
}

function retryDefaults(retryPolicy: RetryPolicy): {
  initialDelay: number;
  backoffFactor: number;
  maxAttempts: number;
  maxDelay: number;
} {
  switch (retryPolicy.kind) {
    case "exponential":
      return {
        initialDelay: retryPolicy.initialDelayMs,
        backoffFactor: retryPolicy.backoffFactor,
        maxAttempts: retryPolicy.maxAttempts,
        maxDelay: retryPolicy.maxDelayMs,
      };
    case "linear":
      return {
        initialDelay: retryPolicy.delayMs,
        backoffFactor: 1,
        maxAttempts: retryPolicy.maxAttempts,
        maxDelay: retryPolicy.delayMs,
      };
    case "never":
      return {
        initialDelay: 0,
        backoffFactor: 0,
        maxAttempts: 1,
        maxDelay: 0,
      };
  }
}
