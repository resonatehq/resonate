import { IEncoder } from "../encoder";
import { JSONEncoder } from "./json";

export class DataEncoder
  implements IEncoder<any, { headers: Record<string, string> | undefined; data: string | undefined }>
{
  key = "data";

  private encoders: IEncoder<any, string>[];

  constructor(
    private defaultEncoder: IEncoder<any, string> = new JSONEncoder(),
    ...encoders: IEncoder<any, string>[]
  ) {
    this.encoders = encoders;
  }

  add(encoder: IEncoder<any, string>): void {
    this.encoders.unshift(encoder);
  }

  encode<T>(data: T): { headers: Record<string, string> | undefined; data: string | undefined } {
    // note about undefined:
    // undefined is not json serializable, so immediately return undefined
    if (data === undefined) {
      return {
        headers: undefined,
        data: undefined,
      };
    }

    for (const encoder of this.encoders) {
      if (encoder.match?.(data)) {
        return {
          headers: { "Content-Encoding": encoder.key },
          data: encoder.encode(data),
        };
      }
    }

    return {
      headers: undefined,
      data: this.defaultEncoder.encode(data),
    };
  }

  decode<T>(value: { headers: Record<string, string> | undefined; data: string | undefined }): T {
    // note about undefined:
    // undefined causes JSON.parse to throw, so immediately return undefined as T
    if (value?.data === undefined) {
      return undefined as T;
    }

    const key = value.headers?.["Content-Encoding"];
    if (key !== undefined) {
      for (const encoder of this.encoders) {
        if (key === encoder.key) {
          return encoder.decode(value.data);
        }
      }
    }

    return this.defaultEncoder.decode(value.data);
  }
}
