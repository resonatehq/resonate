export enum ErrorCodes {
  // unknown
  UNKNOWN = 0,

  // canceled
  CANCELED = 10,

  // timedout
  TIMEDOUT = 20,

  // killed
  KILLED = 30,

  // store
  STORE = 40,
  STORE_PAYLOAD = 41,
  STORE_FORBIDDEN = 42,
  STORE_NOT_FOUND = 43,
  STORE_ALREADY_EXISTS = 44,
  STORE_INVALID_STATE = 45,
  STORE_ENCODER = 46,
}

export class ResonateError extends Error {
  constructor(
    message: string,
    public readonly code: ErrorCodes,
    public readonly cause?: any,
    public readonly retriable: boolean = false,
  ) {
    super(message);
  }

  public static fromError(e: unknown): ResonateError {
    return e instanceof ResonateError ? e : new ResonateError("Unknown error", ErrorCodes.UNKNOWN, e, true);
  }
}
