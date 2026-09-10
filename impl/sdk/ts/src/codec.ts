import { type Encryptor, NoopEncryptor } from "./encryptor.js";
import exceptions from "./exceptions.js";
import type { PromiseRecord, Value } from "./network/types.js";
import * as util from "./util.js";

class JsonEncoder {
  private inf = "__INF__";
  private negInf = "__NEG_INF__";

  encode(value: any): Value {
    if (value === undefined) {
      return { data: "", headers: {} };
    }

    let json: string;
    try {
      json = JSON.stringify(value, (_, v) => {
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
    } catch (e) {
      throw exceptions.ENCODING_ARGS_UNENCODEABLE("unknown", e);
    }
  }

  decode(value: Value | undefined): any | undefined {
    if (!value?.data) {
      return undefined;
    }

    try {
      const decoded = JSON.parse(util.base64Decode(value.data), (_, v) => {
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
      return decoded;
    } catch (e) {
      throw exceptions.ENCODING_ARGS_UNDECODEABLE("unknown", e);
    }
  }
}

export class Codec {
  private encoder: JsonEncoder;
  private encryptor: Encryptor;

  constructor(encryptor: Encryptor = new NoopEncryptor()) {
    this.encoder = new JsonEncoder();
    this.encryptor = encryptor;
  }

  encode(value: any): Value {
    const encoded = this.encoder.encode(value);

    try {
      const encrypted = this.encryptor.encrypt(encoded);
      return encrypted;
    } catch (e) {
      throw exceptions.ENCODING_ARGS_UNENCODEABLE("unknown", e);
    }
  }

  decode(value: Value | undefined): any | undefined {
    if (!value?.data) {
      return undefined;
    }

    try {
      const decrypted = this.encryptor.decrypt(value);
      return this.encoder.decode(decrypted);
    } catch (e) {
      throw exceptions.ENCODING_ARGS_UNDECODEABLE("unknown", e);
    }
  }

  decodePromise(promise: PromiseRecord): PromiseRecord {
    const param = this.decode(promise.param);
    const value = this.decode(promise.value);

    return {
      ...promise,
      param: { headers: promise.param?.headers, data: param },
      value: { headers: promise.value?.headers, data: value },
    };
  }
}
