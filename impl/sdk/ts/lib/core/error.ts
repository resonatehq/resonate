export enum ErrorCodes {
  UNKNOWN = 0,
  SERVER = 1,
  PAYLOAD = 2,
  FORBIDDEN = 3,
  NOT_FOUND = 4,
  ALREADY_EXISTS = 5,
  ENCODER = 6,
}

export class ResonateError extends Error {
  constructor(
    public readonly message: string,
    public readonly code: ErrorCodes,
    public readonly cause?: any,
    public readonly retryable: boolean = false,
  ) {
    super(message);
  }

  public static fromError(e: unknown): ResonateError {
    return e instanceof ResonateError ? e : new ResonateError("Unexpected error", ErrorCodes.UNKNOWN, e);
  }
}
