export interface RetryPolicy {
  next(attempt: number): number | null;
}

export class Constant implements RetryPolicy {
  public readonly delay: number;
  public readonly maxRetries: number;

  constructor({ delay = 1000, maxRetries = Number.MAX_SAFE_INTEGER }: { delay?: number; maxRetries?: number } = {}) {
    this.delay = delay;
    this.maxRetries = maxRetries;
  }

  next(attempt: number): number | null {
    if (attempt < 0) {
      throw new Error("attempt must be greater than or equal to 0");
    }

    if (attempt > this.maxRetries) {
      return null;
    }

    if (attempt === 0) {
      return 0;
    }

    return this.delay;
  }
}

export class Exponential implements RetryPolicy {
  public readonly delay: number;
  public readonly maxRetries: number;
  public readonly factor: number;
  public readonly maxDelay: number;

  constructor({
    delay = 1000,
    factor = 2,
    maxRetries = Number.MAX_SAFE_INTEGER,
    maxDelay = 30000,
  }: { delay?: number; factor?: number; maxRetries?: number; maxDelay?: number } = {}) {
    this.delay = delay;
    this.maxRetries = maxRetries;
    this.factor = factor;
    this.maxDelay = maxDelay;
  }

  next(attempt: number): number | null {
    if (attempt < 0) {
      throw new Error("attempt must be greater than or equal to 0");
    }

    if (attempt > this.maxRetries) {
      return null;
    }

    if (attempt === 0) {
      return 0;
    }

    return Math.min(this.delay * this.factor ** attempt, this.maxDelay);
  }
}

export class Linear implements RetryPolicy {
  public readonly delay: number;
  public readonly maxRetries: number;

  constructor({ delay = 1000, maxRetries = Number.MAX_SAFE_INTEGER }: { delay?: number; maxRetries?: number } = {}) {
    this.delay = delay;
    this.maxRetries = maxRetries;
  }

  next(attempt: number): number | null {
    if (attempt < 0) {
      throw new Error("attempt must be greater than or equal to 0");
    }

    if (attempt > this.maxRetries) {
      return null;
    }

    return this.delay * attempt;
  }
}

export class Never implements RetryPolicy {
  next(attempt: number): number | null {
    if (attempt < 0) {
      throw new Error("attempt must be greater than or equal to 0");
    }

    return attempt === 0 ? 0 : null;
  }
}
