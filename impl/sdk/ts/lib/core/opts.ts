import { IEncoder } from "./encoder";
import { IRetry } from "./retry";
import { ILogger } from "./logger";
import { IStore } from "./store";
import { IBucket } from "./bucket";

/**
 * Resonate configuration options.
 */
export interface ResonateOptions {
  /**
   * A bucket instance, if not provided a default bucket will be
   * used.
   */
  bucket: IBucket;

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
   * Your resonate namespace, defaults to an empty string.
   */
  namespace: string;

  /**
   * The frequency in ms to poll the promise server for pending
   * promises that may need to be recovered, defaults to 1000.
   */
  recoveryDelay: number;

  /**
   * A retry instance, defaults to exponential backoff.
   */
  retry: IRetry;

  /**
   * A seperator token used for constructing promise ids, defaults to
   * "/".
   */
  separator: string;

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

export interface ContextOptions {
  /**
   * Overrides the default bucket.
   */
  bucket: IBucket;

  /**
   * Overrides the generated default execution id.
   */
  eid: string;

  /**
   * Overrides the default encoder.
   */
  encoder: IEncoder<unknown, string | undefined>;

  /**
   * Overrides the default promise identifer.
   */
  id?: string;

  /**
   * Overrides the default promise idempotency key.
   */
  idempotencyKey?: string;

  /**
   * Overrides the default retry policy.
   */
  retry: IRetry;

  /**
   * Overrides the default timeout.
   */
  timeout: number;
}

// Use a class to wrap ContextOptions so we can use the prototype
// (see the type guard below) to distinguish between options and a
// function parameters on calls to run.
export class Options {
  constructor(private opts: Partial<ContextOptions> = {}) {}

  all(): Partial<ContextOptions> {
    return this.opts;
  }

  merge(opts: Partial<ContextOptions>): Options {
    return new Options({
      ...this.opts,
      ...opts,
    });
  }
}

export function isContextOpts(o: unknown): o is Options {
  return o instanceof Options;
}
