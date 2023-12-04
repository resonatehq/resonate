import { IEncoder } from "../encoder";

export class ErrorEncoder implements IEncoder<Error, string> {
  key = "error";

  match(data: unknown): data is Error {
    return data instanceof Error;
  }

  encode(data: Error): string {
    return JSON.stringify({
      message: data.message,
      stack: data.stack,
      name: data.name,
    });
  }

  decode(data: string): Error {
    const error = JSON.parse(data);
    return Object.assign(new Error(error.message), error);
  }
}
