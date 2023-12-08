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

export class DurablePromiseStore implements IPromiseStore {
  constructor(
    private url: string,
    private encoder: IEncoder<string, string> = new Base64Encoder(),
  ) {}

  private async call<T>(url: string, decode: (b: unknown) => T, options: RequestInit, retries: number = 3): Promise<T> {
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

        return decode(body);
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

  create(
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

    return this.call(`${this.url}/promises/${id}/create`, this.promiseDecoder(isDurablePromise), {
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
  }

  cancel(
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

    return this.call(`${this.url}/promises/${id}/cancel`, this.promiseDecoder(isCompletedPromise), {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
        value: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
      }),
    });
  }

  resolve(
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

    return this.call(`${this.url}/promises/${id}/resolve`, this.promiseDecoder(isCompletedPromise), {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
        value: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
      }),
    });
  }

  reject(
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

    return this.call(`${this.url}/promises/${id}/reject`, this.promiseDecoder(isCompletedPromise), {
      method: "POST",
      headers: reqHeaders,
      body: JSON.stringify({
        value: {
          headers: headers,
          data: data ? this.encode(data) : undefined,
        },
      }),
    });
  }

  get(id: string): Promise<DurablePromise> {
    return this.call(`${this.url}/promises/${id}`, this.promiseDecoder(isDurablePromise), {
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      method: "GET",
    });
  }

  private encode(value: string): string {
    try {
      return this.encoder.encode(value);
    } catch (e: unknown) {
      throw new ResonateError("Encode error", ErrorCodes.ENCODER, e);
    }
  }

  private decode(value: string): string {
    try {
      return this.encoder.decode(value);
    } catch (e: unknown) {
      throw new ResonateError("Decode error", ErrorCodes.ENCODER, e);
    }
  }

  private promiseDecoder<T extends DurablePromise>(guard: (p: unknown) => p is T): (p: unknown) => T {
    return (p: unknown) => {
      if (!guard(p)) {
        throw new ResonateError("Invalid response", ErrorCodes.PAYLOAD, p);
      }

      if (p.param?.data) {
        p.param.data = this.decode(p.param.data);
      }

      if (p.value?.data) {
        p.value.data = this.decode(p.value.data);
      }

      return p;
    };
  }
}
