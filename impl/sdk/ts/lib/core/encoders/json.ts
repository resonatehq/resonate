import { IEncoder } from "../encoder";
import { ResonateError } from "../errors";

export class JSONEncoder implements IEncoder<unknown, string | undefined> {
  encode(data: unknown): string | undefined {
    // note about undefined:
    // undefined is not json serializable, so immediately return undefined
    if (data === undefined) {
      return undefined;
    }

    return JSON.stringify(data, (_, value) => {
      if (value === Infinity) {
        return "Infinity";
      }

      if (value === -Infinity) {
        return "-Infinity";
      }

      if (value instanceof AggregateError) {
        return {
          __type: "aggregate_error",
          message: value.message,
          stack: value.stack,
          name: value.name,
          errors: value.errors,
        };
      }

      if (value instanceof ResonateError) {
        return {
          __type: "resonate_error",
          message: value.message,
          stack: value.stack,
          name: value.name,
          code: value.code,
          cause: value.cause,
          retriable: value.retriable,
        };
      }

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
    // undefined causes JSON.parse to throw, so immediately return undefined
    if (data === undefined) {
      return undefined;
    }

    return JSON.parse(data, (_, value) => {
      if (value === "Infinity") {
        return Infinity;
      }

      if (value === "-Infinity") {
        return Infinity;
      }

      if (value?.__type === "aggregate_error") {
        return Object.assign(new AggregateError(value.errors, value.message), value);
      }

      if (value?.__type === "resonate_error") {
        return Object.assign(new ResonateError(value.message, value.code, value.cause, value.retriable), value);
      }

      if (value?.__type === "error") {
        return Object.assign(new Error(value.message), value);
      }

      return value;
    });
  }
}
