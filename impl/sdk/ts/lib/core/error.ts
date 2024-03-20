export enum ErrorCodes {
  UNKNOWN = 0,
  SERVER = 1,
  PAYLOAD = 2,
  FORBIDDEN = 3,
  NOT_FOUND = 4,
  ALREADY_EXISTS = 5,
  INVALID_STATE = 6,
  ENCODER = 7,
  CANCELED = 8,
  TIMEDOUT = 9,
  KILLED = 10,
}

export class ResonateError extends Error {
  constructor(public readonly message: string) {
    super(message);
  }

  public static fromError(e: unknown): ResonateError {
    return e instanceof ResonateError ? e : new ResonateError("Unexpected error: " + e);
  }
}

export class ResonateStorageError extends ResonateError {
  constructor(
    public readonly code: ErrorCodes,
    message: string,
    public readonly cause?: any,
    public readonly retryable: boolean = false,
  ) {
    super(message);
  }
}

export class ResonateCanceled extends ResonateError {
  constructor() {
    super("Promise Canceled");
  }
}

export class ResonateTimedout extends ResonateError {
  constructor() {
    super("Promise Timedout");
  }
}

export class ResonateKilled extends ResonateError {
  constructor() {
    super("Promise Killed");
  }
}
