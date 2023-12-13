import { IEncoder } from "../encoder";

export class JSONEncoder implements IEncoder<unknown, string> {
  key = "json";

  encode(data: unknown): string {
    return JSON.stringify(data);
  }

  decode(data: string): unknown {
    return JSON.parse(data);
  }
}
