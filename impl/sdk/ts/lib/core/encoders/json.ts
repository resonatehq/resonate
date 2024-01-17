import { IEncoder } from "../encoder";

export class JSONEncoder implements IEncoder<unknown, string | undefined> {
  encode(data: unknown): string | undefined {
    // note about undefined:
    // undefined is not json serializable, so immediately return undefined
    if (data === undefined) {
      return undefined;
    }

    return JSON.stringify(data, (_, value) => {
      if (value instanceof Error) {
        return {
          __type: "error",
          message: value.message,
          stack: value.stack,
          name: value.name,
        };
      }

      return value;
    });
  }

  decode(data: string | undefined): unknown {
    // note about undefined:
    // undefined causes JSON.parse to throw, so immediately return undefined as T
    if (data === undefined) {
      return undefined;
    }

    return JSON.parse(data, (_, value) => {
      if (value?.__type === "error") {
        return Object.assign(new Error(value.message), value);
      }

      return value;
    });
  }
}
