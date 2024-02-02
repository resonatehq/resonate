import { IEncoder } from "./encoder";
import { IRetry } from "./retry";

export type Opts = {
  /**
   * Overrides the default identifer.
   */
  id?: string;

  /**
   * Overrides the default idempotency key.
   */
  idempotencyKey?: string;

  /**
   * Overrides the default timeout.
   */
  timeout: number;

  /**
   * Overrides the default retry policy.
   */
  retry: IRetry;

  /**
   * Overrides the default encoder.
   */
  encoder: IEncoder<unknown, string | undefined>;

  /**
   * Overrides the default execution id.
   */
  eid: string;
};
