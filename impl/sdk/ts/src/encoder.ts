import type { Value } from "./types";
import * as util from "./util";

export interface Encoder {
  encode(value: any): Value<string>;
  decode(value: Value<string> | undefined): any | undefined;
}

export class JsonEncoder implements Encoder {
  private inf = "__INF__";
  private negInf = "__NEG_INF__";

  encode(value: any): Value<string> {
    // note about undefined:
    // undefined is not json serializable, so immediately return undefined
    if (value === undefined) {
      return { data: undefined };
    }

    const json = JSON.stringify(value, (_, v) => {
      if (v === Number.POSITIVE_INFINITY) return this.inf;
      if (v === Number.NEGATIVE_INFINITY) return this.negInf;

      if (v instanceof AggregateError) {
        return {
          __type: "aggregate_error",
          message: v.message,
          stack: v.stack,
          name: v.name,
          errors: v.errors,
        };
      }

      if (v instanceof Error) {
        return {
          __type: "error",
          message: v.message,
          stack: v.stack,
          name: v.name,
        };
      }

      return v;
    });

    return {
      headers: {},
      data: util.base64Encode(json),
    };
  }

  decode(value: Value<string> | undefined): any | undefined {
    if (!value?.data) {
      return undefined;
    }

    return JSON.parse(util.base64Decode(value.data), (_, v) => {
      if (v === this.inf) return Number.POSITIVE_INFINITY;
      if (v === this.negInf) return Number.NEGATIVE_INFINITY;

      if (v?.__type === "aggregate_error") {
        return Object.assign(new AggregateError(v.errors, v.message), v);
      }

      if (v?.__type === "error") {
        const err = new Error(v.message || "Unknown error");
        if (v.name) err.name = v.name;
        if (v.stack) err.stack = v.stack;
        return err;
      }

      return v;
    });
  }
}
