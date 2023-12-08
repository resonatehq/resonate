import { Context } from "../../resonate";
import { IRetry } from "../retry";

export class Retry implements IRetry {
  constructor(
    private maxAttempts: number,
    private maxDelay: number,
    private initialDelay: number,
    private backoffFactor: number,
  ) {}

  static atMostOnce(): Retry {
    return new Retry(1, 0, 0, 0);
  }

  static atLeastOnce(
    maxAttempts: number = Infinity,
    maxDelay: number = 60000, // 1 minute
    initialDelay: number = 100,
    backoffFactor: number = 2,
  ): Retry {
    return new Retry(maxAttempts, maxDelay, initialDelay, backoffFactor);
  }

  next(context: Context): { done: boolean; delay?: number } {
    if (Date.now() >= context.timeout || context.attempt >= this.maxAttempts) {
      return { done: true };
    }

    const delay = Math.min(context.attempt, 1) * this.initialDelay * Math.pow(this.backoffFactor, context.attempt - 1);

    return {
      done: false,
      delay: Math.min(delay, this.maxDelay),
    };
  }
}
