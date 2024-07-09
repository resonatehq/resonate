import { IEncoder } from "./encoder";
import { ILogger } from "./logger";
import { RetryPolicy } from "./retry";
import { IStore } from "./store";

/**
 * Resonate configuration options.
 */
export type ResonateOptions = {
  /**
   * Store authentication options.
   */
  auth: AuthOptions;

  /**
   * An encoder instance used for encoding and decoding values
   * returned (or thrown) by registered functions. If not provided,
   * a default JSON encoder will be used.
   */
  encoder: IEncoder<unknown, string | undefined>;

  /**
   * The frequency in ms to heartbeat locks.
   */
  heartbeat: number;

  /**
   * A process id that can be used to uniquely identify this Resonate
   * instance. If not provided a default value will be generated.
   */
  pid: string;

  /**
   * The frequency in ms to poll the promise store for remote
   * promises.
   */
  pollFrequency: number;

  /**
   * A logger instance, if not provided a default logger will be
   * used.
   */
  logger: ILogger;

  /**
   * A retry policy, defaults to exponential backoff.
   */
  retryPolicy: RetryPolicy;

  /**
   * Tags to add to all durable promises.
   */
  tags: Record<string, string>;

  /**
   * A store instance, if provided will take predence over the
   * default store.
   */
  store: IStore;

  /**
   * The default promise timeout in ms, used for every function
   * executed by calling run. Defaults to 1000.
   */
  timeout: number;

  /**
   * The remote promise store url. If not provided, an in-memory
   * promise store will be used.
   */
  url: string;
};

/**
 * Resonate function invocation options.
 */
export type Options = {
  __resonate: true;

  /**
   * Persist the result to durable storage.
   */
  durable: boolean;

  /**
   * A function that calculates the id for this execution
   * defaults to a random funciton.
   */
  eidFn: (id: string) => string;

  /**
   * Overrides the default encoder.
   */
  encoder: IEncoder<unknown, string | undefined>;

  /**
   * Overrides the default funciton to calculate the idempotency key.
   * defaults to a variation fnv-1a the hash funciton.
   */
  idempotencyKeyFn: (id: string) => string;

  /**
   * Acquire a lock for the execution.
   */
  shouldLock: boolean | undefined;

  /**
   * Overrides the default polling frequency.
   */
  pollFrequency: number;

  /**
   * Overrides the default retry policy.
   */
  retryPolicy: RetryPolicy;

  /**
   * Additional tags to add to the durable promise.
   */
  tags: Record<string, string>;

  /**
   * Overrides the default timeout.
   */
  timeout: number;

  /**
   * The function version to execute. Only applicable on calls to
   * resonate.run.
   */
  version: number;
};

export type PartialOptions = Partial<Options> & { __resonate: true };

export function isOptions(o: unknown): o is PartialOptions {
  return typeof o === "object" && o !== null && (o as PartialOptions).__resonate === true;
}

export type StoreOptions = {
  /**
   * The store authentication options.
   */
  auth: AuthOptions;

  /**
   * The store encoder, defaults to a base64 encoder.
   */
  encoder: IEncoder<string, string>;

  /**
   * The frequency in ms to heartbeat locks.
   */
  heartbeat: number;

  /**
   * A logger instance, if not provided a default logger will be
   * used.
   */
  logger: ILogger;

  /**
   * A process id that can be used to uniquely identify this Resonate
   * instance. If not provided a default value will be generated.
   */
  pid: string;

  /**
   * Number of retries to attempt before throwing an error. If not
   * provided, a default value will be used.
   */
  retries: number;
};

export type AuthOptions = {
  /**
   * Basic auth credentials.
   */
  basic: {
    password: string;
    username: string;
  };
};
