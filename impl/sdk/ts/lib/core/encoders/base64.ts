import { IEncoder } from "../encoder";

export class Base64Encoder implements IEncoder<string, string> {
  encode(data: string): string {
    return btoa(data);
  }

  decode(data: string): string {
    return atob(data);
  }
}
