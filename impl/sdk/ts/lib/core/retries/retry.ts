import { IRetry, IterableRetry } from "../retry";

export class Retry extends IterableRetry implements IRetry {
  constructor(
    private initialDelay: number,
    private backoffFactor: number,
    private maxAttempts: number,
    private maxDelay: number,
  ) {
    super();
  }

  static exponential(
    initialDelay: number = 100,
    backoffFactor: number = 2,
    maxAttempts: number = Infinity,
    maxDelay: number = 60000, // 1 minute
  ): Retry {
    return new Retry(initialDelay, backoffFactor, maxAttempts, maxDelay);
  }

  static linear(
    delay: number = 1000, // 1 second
    maxAttempts: number = Infinity,
  ): Retry {
    return new Retry(delay, 1, maxAttempts, delay);
  }

  static never(): Retry {
    return new Retry(0, 0, 1, 0);
  }

  next<T extends { attempt: number; timeout: number }>(ctx: T): { done: boolean; delay?: number } {
    // attempt 0: 0ms delay
    // attampt n: {initial * factor^(attempt-1)}ms delay (or max delay)
    const delay = Math.min(
      Math.min(ctx.attempt, 1) * this.initialDelay * Math.pow(this.backoffFactor, ctx.attempt - 1),
      this.maxDelay,
    );

    if (Date.now() + delay >= ctx.timeout || ctx.attempt >= this.maxAttempts) {
      return { done: true };
    }

    return {
      done: false,
      delay: delay,
    };
  }
}
