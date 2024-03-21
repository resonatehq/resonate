import { IEncoder } from "./encoder";
import { ILogger } from "./logger";
import { IRetry } from "./retry";
import { IStore } from "./store";

/**
 * Resonate configuration options.
 */
export interface ResonateOptions {
  /**
   * An encoder instance used for encoding and decoding values
   * returned (or thrown) by registered functions. If not provided,
   * a default JSON encoder will be used.
   */
  encoder: IEncoder<unknown, string | undefined>;

  /**
   * A process id that can be used to uniquely identify this Resonate
   * instance. If not provided a default value will be generated.
   */
  pid: string;

  /**
   * A logger instance, if not provided a default logger will be
   * used.
   */
  logger: ILogger;

  /**
   * A retry instance, defaults to exponential backoff.
   */
  retry: IRetry;

  /**
   * Tags to add to all durable promises.
   */
  tags: Record<string, string>;

  /**
   * A store instance, if provided this will take precedence over a
   * remote store.
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
}

/**
 * Resonate function invocation options.
 */
export interface Options {
  __resonate: true;

  /**
   * Overrides the default encoder.
   */
  encoder: IEncoder<unknown, string | undefined>;

  /**
   * Overrides the default retry policy.
   */
  retry: IRetry;

  /**
   * Overrides the default store.
   */
  store: IStore;

  /**
   * Additional tags to add to the durable promise.
   */
  tags: Record<string, string>;

  /**
   * Overrides the default timeout.
   */
  timeout: number;
}

export type PartialOptions = Partial<Options> & { __resonate: true };

export function isPartialOptions(o: unknown): o is PartialOptions {
  return typeof o === "object" && o !== null && (o as PartialOptions).__resonate === true;
}
