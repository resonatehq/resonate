import { IEncoder } from "../encoder";

export class Base64Encoder implements IEncoder<string, string> {
  match(data: unknown): data is string {
    return typeof data === "string";
  }

  encode(data: string): string {
    return btoa(data);
  }

  decode(data: string): string {
    return atob(data);
  }
}
