import { Context } from "../../resonate";
import { IRetry, IterableRetry } from "../retry";

export class ExponentialRetry extends IterableRetry implements IRetry {
  constructor(
    private maxAttempts: number,
    private maxDelay: number,
    private initialDelay: number,
    private backoffFactor: number,
  ) {
    super();
  }

  static atMostOnce(): ExponentialRetry {
    return new ExponentialRetry(1, 0, 0, 0);
  }

  static atLeastOnce(
    maxAttempts: number = Infinity,
    maxDelay: number = 60000, // 1 minute
    initialDelay: number = 100,
    backoffFactor: number = 2,
  ): ExponentialRetry {
    return new ExponentialRetry(maxAttempts, maxDelay, initialDelay, backoffFactor);
  }

  next(ctx: Context): { done: boolean; delay?: number } {
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
