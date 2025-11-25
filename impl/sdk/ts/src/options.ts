import type { RetryPolicy } from "./retries";
import * as util from "./util";

export const RESONATE_OPTIONS: unique symbol = Symbol("ResonateOptions");

export class OptionsBuilder {
  private match: (target: string) => string;
  constructor({ match }: { match: (target: string) => string }) {
    this.match = (target: string) => (util.isUrl(target) ? target : match(target));
  }

  build({
    id = undefined,
    retryPolicy = undefined,
    tags = {},
    target = "default",
    timeout = 24 * util.HOUR,
    version = 0,
  }: {
    id?: string;
    retryPolicy?: RetryPolicy;
    tags?: Record<string, string>;
    target?: string;
    timeout?: number;
    version?: number;
  } = {}): Options {
    return new Options({ id, retryPolicy, tags, target: this.match(target), timeout, version });
  }
}

export class Options {
  public readonly id: string | undefined;
  public readonly tags: Record<string, string>;
  public readonly target: string;
  public readonly timeout: number;
  public readonly version: number;
  public readonly retryPolicy: RetryPolicy | undefined;

  [RESONATE_OPTIONS] = true;

  constructor({
    id = undefined,
    retryPolicy = undefined,
    tags = {},
    target = "default",
    timeout = 24 * util.HOUR,
    version = 0,
  }: {
    id?: string;
    retryPolicy?: RetryPolicy;
    tags?: Record<string, string>;
    target?: string;
    timeout?: number;
    version?: number;
  }) {
    this.id = id;
    this.tags = tags;
    this.target = target;
    this.timeout = timeout;
    this.version = version;
    this.retryPolicy = retryPolicy;
  }
}
