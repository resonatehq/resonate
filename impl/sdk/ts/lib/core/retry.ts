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
