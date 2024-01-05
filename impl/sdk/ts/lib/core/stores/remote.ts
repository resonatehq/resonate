import {
  DurablePromise,
  PendingPromise,
  ResolvedPromise,
  RejectedPromise,
  CanceledPromise,
  TimedoutPromise,
  isDurablePromise,
  isCompletedPromise,
} from "../promise";
import { IPromiseStore, isSearchPromiseResult } from "../store";
import { IEncoder } from "../encoder";
import { Base64Encoder } from "../encoders/base64";
import { ErrorCodes, ResonateError } from "../error";
import { ILogger } from "../logger";

export class RemotePromiseStore implements IPromiseStore {
  constructor(
    private url: string,
    private logger: ILogger,
    private encoder: IEncoder<string, string> = new Base64Encoder(),
  ) {}

  private async call<T>(
    url: string,
    guard: (b: unknown) => b is T,
    options: RequestInit,
    retries: number = 3,
  ): Promise<T> {
    let error: unknown;

    for (let i = 0; i < retries; i++) {
      try {
        const r = await fetch(url, options);
        const body: unknown = await r.json();

        this.logger.debug("store", {
          req: {
            method: options.method,
            url: url,
            headers: options.headers,
            body: options.body,
          },
          res: {
            status: r.status,
            body: body,
          },
        });

        if (!r.ok) {
          switch (r.status) {
            case 400:
              throw new ResonateError(ErrorCodes.PAYLOAD, "Invalid request", body);
            case 403:
              throw new ResonateError(ErrorCodes.FORBIDDEN, "Forbidden request", body);
            case 404:
              throw new ResonateError(ErrorCodes.NOT_FOUND, "Not found", body);
            case 409:
              throw new ResonateError(ErrorCodes.ALREADY_EXISTS, "Already exists", body);
            default:
              throw new ResonateError(ErrorCodes.SERVER, "Server error", body, true);
          }
        }

        if (!guard(body)) {
          throw new ResonateError(ErrorCodes.PAYLOAD, "Invalid response", body);
        }

        return body;
      } catch (e: unknown) {
        if (e instanceof ResonateError && !e.retryable) {
          throw e;
        } else {
          error = e;
        }
      }
    }

    throw ResonateError.fromError(error);
  }

  async create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<PendingPromise | CanceledPromise | ResolvedPromise | RejectedPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.call(`${this.url}/promises`, isDurablePromise, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
        id: id,
        param: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
        timeout: timeout,
        tags: tags,
      }),
    });

    return this.decode(promise);
  }

  async cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.call(`${this.url}/promises/${id}`, isCompletedPromise, {
      method: "PATCH",
      headers: reqHeaders,
      body: JSON.stringify({
        state: "REJECTED_CANCELED",
        value: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
      }),
    });

    return this.decode(promise);
  }

  async resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.call(`${this.url}/promises/${id}`, isCompletedPromise, {
      method: "PATCH",
      headers: reqHeaders,
      body: JSON.stringify({
        state: "RESOLVED",
        value: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
      }),
    });

    return this.decode(promise);
  }

  async reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise> {
    const reqHeaders: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
      Strict: JSON.stringify(strict),
    };

    if (ikey !== undefined) {
      reqHeaders["Idempotency-Key"] = ikey;
    }

    const promise = await this.call(`${this.url}/promises/${id}`, isCompletedPromise, {
      method: "PATCH",
      headers: reqHeaders,
      body: JSON.stringify({
        state: "REJECTED",
        value: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
      }),
    });

    return this.decode(promise);
  }

  async get(id: string): Promise<DurablePromise> {
    const promise = await this.call(`${this.url}/promises/${id}`, isDurablePromise, {
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      method: "GET",
    });

    return this.decode(promise);
  }

  async *search(
    id: string,
    state: string | undefined,
    tags: Record<string, string> | undefined,
    limit: number | undefined,
  ): AsyncGenerator<DurablePromise[], void> {
    let cursor: string | null | undefined = undefined;

    while (cursor !== null) {
      const params = new URLSearchParams({ id });

      if (state !== undefined) {
        params.append("state", state);
      }

      for (const [k, v] of Object.entries(tags ?? {})) {
        params.append(`tags[${k}]`, v);
      }

      if (limit !== undefined) {
        params.append("limit", limit.toString());
      }

      if (cursor !== undefined) {
        params.append("cursor", cursor);
      }

      const url = new URL(`${this.url}/promises`);
      url.search = params.toString();

      const res = await this.call(url.toString(), isSearchPromiseResult, {
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        method: "GET",
      });

      cursor = res.cursor;
      yield res.promises.map((p) => this.decode(p));
    }
  }

  private encode(value: string): string {
    try {
      return this.encoder.encode(value);
    } catch (e: unknown) {
      throw new ResonateError(ErrorCodes.ENCODER, "Encode error", e);
    }
  }

  private decode<P extends DurablePromise>(promise: P): P {
    try {
      if (promise.param?.data) {
        promise.param.data = this.encoder.decode(promise.param.data);
      }

      if (promise.value?.data) {
        promise.value.data = this.encoder.decode(promise.value.data);
      }

      return promise;
    } catch (e: unknown) {
      throw new ResonateError(ErrorCodes.ENCODER, "Decode error", e);
    }
  }
}
