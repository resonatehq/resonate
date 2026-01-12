import type { RetryPolicy } from "./retries";
import * as util from "./util";

export const RESONATE_OPTIONS: unique symbol = Symbol("ResonateOptions");

export class OptionsBuilder {
  private match: (target: string) => string;
  private idPrefix: string;
  constructor({ match, idPrefix }: { match: (target: string) => string; idPrefix: string }) {
    this.match = (target: string) => (util.isUrl(target) ? target : match(target));
    this.idPrefix = idPrefix;
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
    id = id ? `${this.idPrefix}${id}` : id;
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
