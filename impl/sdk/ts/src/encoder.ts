import type { Value } from "./types";
import * as util from "./util";

export interface Encoder {
  encode(value: any): Value<string>;
  decode(value: Value<string> | undefined): any;
}

export class JsonEncoder implements Encoder {
  inf = "__INF__";
  negInf = "__NEG_INF__";

  encode(value: any): Value<string> {
    // note about undefined:
    // undefined is not json serializable, so immediately return undefined
    if (value === undefined) {
      return {
        data: undefined,
      };
    }

    const data = JSON.stringify(value, (_, value) => {
      if (value === Number.POSITIVE_INFINITY) {
        return this.inf;
      }

      if (value === Number.NEGATIVE_INFINITY) {
        return this.negInf;
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

    return {
      headers: {},
      data: util.base64Encode(data),
    };
  }

  decode(value: Value<string> | undefined): any {
    if (!value?.data) {
      return undefined;
    }

    return JSON.parse(util.base64Decode(value.data), (_, value) => {
      if (value === this.inf) {
        return Number.POSITIVE_INFINITY;
      }

      if (value === this.negInf) {
        return Number.NEGATIVE_INFINITY;
      }

      if (value?.__type === "aggregate_error") {
        return Object.assign(new AggregateError(value.errors, value.message), value);
      }

      if (value?.__type === "error") {
        const error = new Error(value.message || "Unknown error");
        if (value.name) error.name = value.name;
        if (value.stack) error.stack = value.stack;
        return error;
      }

      return value;
    });
  }
}
