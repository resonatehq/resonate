export interface RetryPolicyConstructor {
  readonly type: string;
  new (arg: any): RetryPolicy;
}

export interface RetryPolicy {
  next(attempt: number): number | null;
  encode(): { type: string; data: any };
}

export class Constant implements RetryPolicy {
  static readonly type = "constant";
  private readonly delay: number;
  private readonly maxRetries: number;

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

  encode() {
    return {
      type: "constant",
      data: { delay: this.delay, maxRetries: this.maxRetries },
    };
  }
}

export class Exponential implements RetryPolicy {
  static readonly type = "exponential";
  private readonly delay: number;
  private readonly maxRetries: number;
  private readonly factor: number;
  private readonly maxDelay: number;

  constructor({
    delay = 1000,
    factor = 2,
    maxRetries = Number.MAX_SAFE_INTEGER,
    maxDelay = 30000,
  }: { delay?: number; factor?: number; maxRetries?: number; maxDelay?: number } = {}) {
    this.delay = delay;
    this.factor = factor;
    this.maxRetries = maxRetries;
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

  encode() {
    return {
      type: "exponential",
      data: { delay: this.delay, factor: this.factor, maxRetries: this.maxRetries, maxDelay: this.maxDelay },
    };
  }
}

export class Linear implements RetryPolicy {
  static readonly type = "linear";
  private readonly delay: number;
  private readonly maxRetries: number;

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

  encode() {
    return {
      type: "linear",
      data: { delay: this.delay, maxRetries: this.maxRetries },
    };
  }
}

export class Never implements RetryPolicy {
  static readonly type = "never";

  next(attempt: number): number | null {
    if (attempt < 0) {
      throw new Error("attempt must be greater than or equal to 0");
    }

    return attempt === 0 ? 0 : null;
  }

  encode() {
    return {
      type: "never",
      data: {},
    };
  }
}
