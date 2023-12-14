export enum ErrorCodes {
  UNKNOWN = 0,
  SERVER = 1,
  PAYLOAD = 2,
  FORBIDDEN = 3,
  NOT_FOUND = 4,
  ALREADY_EXISTS = 5,
  INVALID_STATE = 6,
  ENCODER = 7,
}

export class ResonateError extends Error {
  constructor(
    public readonly code: ErrorCodes,
    public readonly message: string,
    public readonly cause?: any,
    public readonly retryable: boolean = false,
  ) {
    super(message);
  }

  public static fromError(e: unknown): ResonateError {
    return e instanceof ResonateError ? e : new ResonateError(ErrorCodes.UNKNOWN, "Unexpected error", e);
  }
}
