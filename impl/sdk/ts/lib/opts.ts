import { IRetry } from "./retry";

export type Opts = {
  /**
   * Overrides the default identifer.
   */
  id: string;

  /**
   * Overrides the default idempotency key.
   */
  idempotencyKey: string;

  /**
   * Overrides the default timeout.
   */
  timeout: number;

  /**
   * Overrides the default store.
   */
  store: string;

  /**
   * Overrides the default bucket.
   */
  bucket: string;

  /**
   * Overrides the default retry policy.
   */
  retry: IRetry;
};

export function isPartialOpts(obj: unknown): obj is Opts {
  const _isPartialOpts = (partialObj: Partial<Opts>): partialObj is Partial<Opts> => {
    return (
      (partialObj.id !== undefined && typeof partialObj.id === "string") ||
      (partialObj.idempotencyKey !== undefined && typeof partialObj.idempotencyKey === "string") ||
      (partialObj.timeout !== undefined && typeof partialObj.timeout === "number") ||
      (partialObj.store !== undefined && typeof partialObj.store === "string") ||
      (partialObj.bucket !== undefined && typeof partialObj.bucket === "string") ||
      (partialObj.retry !== undefined && typeof partialObj.retry === "object")
    );
  };

  return typeof obj === "object" && obj !== null && _isPartialOpts(obj);
}
