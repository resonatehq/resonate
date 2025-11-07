import { WallClock } from "../src/clock";
import { type Context, InnerContext } from "../src/context";
import { Coroutine, type Suspended } from "../src/coroutine";
import { JsonEncoder } from "../src/encoder";
import { NoopEncryptor } from "../src/encryptor";
import type { ResonateError } from "../src/exceptions";
import { Handler } from "../src/handler";
import type { DurablePromiseRecord, Message, Network, Request, ResponseFor } from "../src/network/network";
import { Registry } from "../src/registry";
import { Never } from "../src/retries";
import { ok, type Result } from "../src/types";

class DummyNetwork implements Network {
  private promises = new Map<string, DurablePromiseRecord>();

  send<T extends Request>(request: T, callback: (err?: ResonateError, res?: ResponseFor<T>) => void): void {
    switch (request.kind) {
      case "createPromise": {
        const p: DurablePromiseRecord = {
          id: request.id,
          state: "pending",
          timeout: request.timeout,
          param: request.param,
          value: undefined,
          tags: request.tags || {},
          iKeyForCreate: request.iKey,
        };
        this.promises.set(p.id, p);
        callback(undefined, {
          kind: "createPromise",
          promise: p,
        } as ResponseFor<T>);
        return;
      }

      case "completePromise": {
        const p = this.promises.get(request.id)!;
        p.state = "resolved";
        p.value = request.value!;
        this.promises.set(p.id, p);
        callback(undefined, {
          kind: "completePromise",
          promise: p,
        } as ResponseFor<T>);
        break;
      }
      default:
        throw new Error("All other kind will not be implemented");
    }
  }

  recv(_msg: Message): void {
    throw new Error("Method not implemented.");
  }

  stop() {}
  subscribe(_t: "invoke" | "resume" | "notify", _c: (msg: Message) => void) {}
}

describe("Coroutine", () => {
  // Helper functions to write test easily
  const exec = (uuid: string, func: (ctx: Context, ...args: any[]) => any, args: any[], handler: Handler) => {
    return new Promise<any>((resolve) => {
      Coroutine.exec(
        uuid,
        false,
        new InnerContext({
          id: uuid,
          func: func.name,
          anycast: "poll://any@default",
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
        }),
        func,
        args,
        handler,
        (err, res) => {
          expect(err).toBe(false);
          resolve(res);
        },
      );
    });
  };

  const completePromise = (handler: Handler, id: string, result: Result<any>) => {
    return new Promise<any>((resolve) => {
      handler.completePromise(
        {
          kind: "completePromise",
          id: id,
          state: result.success ? "resolved" : "rejected",
          value: {
            data: result.success ? result.value : result.error,
          },
          iKey: id,
          strict: false,
        },
        (err, res) => {
          expect(err).toBeUndefined();
          resolve(res);
        },
      );
    });
  };

  test("basic coroutine completes with completed", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.beginRun(bar);
      const v = yield* p;
      return v;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());
    const r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 42 } } });
  });

  test("basic coroutine with function suspends after first await", async () => {
    function bar() {
      return 42;
    }
    function baz() {
      return 31416;
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.beginRun(bar);
      const p2 = yield* ctx.beginRun(baz);
      const v = yield* p;
      const v2 = yield* p2;
      return v + v2;
    }
    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());

    // First execution - should suspend
    let r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "suspended" });
    const suspended = r as Suspended;
    expect(suspended.todo.local).toHaveLength(2);

    await completePromise(h, "foo.1.0", ok(42));
    await completePromise(h, "foo.1.1", ok(31416));

    // Second execution - should complete
    r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 31458 } } });
  });

  test("coroutine with a suspension point suspends if can not make more progress", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const p1 = yield* ctx.beginRun(bar);
      const p2 = yield* ctx.beginRpc("bar");
      const v1 = yield* p1;
      yield* p1;
      yield* p2;
      return v1;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());

    let r = await exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(h, "foo.1.1", ok(42));
    r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 42 } } });
  });

  test("Structured concurrency", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      yield* ctx.beginRpc("bar");
      yield* ctx.beginRpc("bar");
      return 99;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());
    let r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(2);

    await completePromise(h, "foo.1.1", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(h, "foo.1.0", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 99 } } });
  });

  test("Detached concurrency", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      yield* ctx.beginRpc("bar");
      yield* ctx.detached("bar");
      return 99;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());
    let r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(h, "foo.1.0", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 99 } } });
  });

  test("Return the detached todo if explicitly awaited", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      yield* ctx.beginRpc("bar");
      const df = yield* ctx.detached("bar");
      const v = yield* df;
      return v;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());
    let r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(2);

    await completePromise(h, "foo.1.0", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(h, "foo.1.1", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 42 } } });
  });

  test("lfc/rfc", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const v1: number = yield* ctx.run(bar);
      const v2: number = yield* ctx.rpc<number>("bar");
      return v1 + v2;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());

    let r = await exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    expect(suspended.todo.remote).toHaveLength(1);

    await completePromise(h, "foo.1.1", ok(42));

    r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 84 } } });
  });

  test("DIE with condition true aborts execution", async () => {
    function* foo(ctx: Context) {
      yield* ctx.panic(true, "Abort execution");
      return 42;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());

    // DIE with condition=true causes callback to be called with err=true
    const result = await new Promise<any>((resolve) => {
      Coroutine.exec(
        "foo.1",
        false,
        new InnerContext({
          id: "foo.1",
          func: foo.name,
          anycast: "poll://any@default",
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
        }),
        foo,
        [],
        h,
        (err, res) => {
          resolve({ err, res });
        },
      );
    });

    expect(result.err).toBe(true);
    expect(result.res).toBeUndefined();
  });

  test("DIE with condition false continues execution", async () => {
    function* foo(ctx: Context) {
      yield* ctx.panic(false, "Should not abort");
      return 42;
    }

    const h = new Handler(new DummyNetwork(), new JsonEncoder(), new NoopEncryptor());
    const r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", promise: { id: "foo.1", value: { data: 42 } } });
  });
});
