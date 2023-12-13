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
import { IPromiseStore } from "../store";
import { IEncoder } from "../encoder";
import { Base64Encoder } from "../encoders/base64";
import { ErrorCodes, ResonateError } from "../error";

export class RemotePromiseStore implements IPromiseStore {
  constructor(
    private url: string,
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

        if (!r.ok) {
          switch (r.status) {
            case 400:
              throw new ResonateError("Invalid request", ErrorCodes.PAYLOAD, body);
            case 403:
              throw new ResonateError("Forbidden request", ErrorCodes.FORBIDDEN, body);
            case 404:
              throw new ResonateError("Not found", ErrorCodes.NOT_FOUND, body);
            case 409:
              throw new ResonateError("Already exists", ErrorCodes.ALREADY_EXISTS, body);
            default:
              throw new ResonateError("Server error", ErrorCodes.SERVER, body, true);
          }
        }

        if (!guard(body)) {
          throw new ResonateError("Invalid response", ErrorCodes.PAYLOAD, body);
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

    const promise = await this.call(`${this.url}/promises/${id}/create`, isDurablePromise, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
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

    const promise = await this.call(`${this.url}/promises/${id}/cancel`, isCompletedPromise, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
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

    const promise = await this.call(`${this.url}/promises/${id}/resolve`, isCompletedPromise, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
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

    const promise = await this.call(`${this.url}/promises/${id}/reject`, isCompletedPromise, {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
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

  private encode(value: string): string {
    try {
      return this.encoder.encode(value);
    } catch (e: unknown) {
      throw new ResonateError("Encode error", ErrorCodes.ENCODER, e);
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
      throw new ResonateError("Decode error", ErrorCodes.ENCODER, e);
    }
  }
}
