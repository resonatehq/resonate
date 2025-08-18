import type { Context } from "../src/context";
import { Coroutine, type Suspended } from "../src/coroutine";
import { Handler } from "../src/handler";
import type { DurablePromiseRecord, Network, RecvMsg, RequestMsg, ResponseMsg } from "../src/network/network";
import { type CompResult, type Result, ok } from "../src/types";

class DummyNetwork implements Network {
  private promises = new Map<string, DurablePromiseRecord>();
  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void {
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
        callback(false, {
          kind: "createPromise",
          promise: p,
        });
        return;
      }

      case "completePromise": {
        const p = this.promises.get(request.id)!;
        p.state = "resolved";
        p.value = request.value!;
        this.promises.set(p.id, p);
        callback(false, {
          kind: "completePromise",
          promise: p,
        });
        break;
      }
      default:
        throw new Error("All other kind will not be implemented");
    }
  }
  recv(_msg: RecvMsg): void {
    throw new Error("Method not implemented.");
  }
  stop() {}
  onMessage?: ((msg: RecvMsg, cb: (res: CompResult) => void) => void) | undefined;
}

describe("Coroutine", () => {
  // Helper functions to write test easily
  const exec = (uuid: string, func: (ctx: Context, ...args: any[]) => any, args: any[], handler: Handler) => {
    return new Promise<any>((resolve) => {
      Coroutine.exec(uuid, func, args, handler, resolve);
    });
  };

  const completePromise = (handler: Handler, id: string, value: Result<any>) => {
    return new Promise<any>((resolve) => {
      handler.completePromise(id, value, resolve);
    });
  };

  test('basic coroutine completes with { type: "completed", value: 42 }', async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.lfi(bar);
      const v = yield* p;
      return v;
    }

    const h = new Handler(new DummyNetwork());
    const r = await exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: ok(42) });
  });

  test("basic coroutine with function suspends after first await", async () => {
    function bar() {
      return 42;
    }
    function baz() {
      return 31416;
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.lfi(bar);
      const p2 = yield* ctx.lfi(baz);
      const v = yield* p;
      const v2 = yield* p2;
      return v + v2;
    }
    const h = new Handler(new DummyNetwork());

    // First execution - should suspend
    let r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "suspended" });
    const suspended = r as Suspended;
    expect(suspended.localTodos).toHaveLength(2);

    await completePromise(h, "foo.1.0", ok(42));
    await completePromise(h, "foo.1.1", ok(31416));

    // Second execution - should complete
    r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", value: ok(42 + 31416) });
  });

  test("coroutine with a suspension point suspends if can not make more progress", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const p1 = yield* ctx.lfi(bar);
      const p2 = yield* ctx.rfi("bar");
      const v1 = yield* p1;
      const vx = yield* p1;
      const v2 = yield* p2;
      return v1;
    }

    const h = new Handler(new DummyNetwork());

    let r = await exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.remoteTodos).toHaveLength(1);

    await completePromise(h, "foo.1.1", ok(42));
    r = await exec("foo.1", foo, [], h);
    expect(r).toEqual({ type: "completed", value: ok(42) });
  });

  test("Structured concurrency", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      yield* ctx.rfi("bar");
      yield* ctx.rfi("bar");
      return 99;
    }

    const h = new Handler(new DummyNetwork());
    let r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.remoteTodos).toHaveLength(2);

    await completePromise(h, "foo.1.1", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.remoteTodos).toHaveLength(1);

    await completePromise(h, "foo.1.0", ok(42));
    r = await exec("foo.1", foo, [], h);

    expect(r).toEqual({ type: "completed", value: ok(99) });
  });

  test("lfc/rfc", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const v1: number = yield* ctx.lfc(bar);
      const v2: number = yield* ctx.rfc<number>("bar");
      return v1 + v2;
    }

    const h = new Handler(new DummyNetwork());

    let r = await exec("foo.1", foo, [], h);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    expect(suspended.remoteTodos).toHaveLength(1);

    await completePromise(h, "foo.1.1", ok(42));

    r = await exec("foo.1", foo, [], h);
    expect(r).toMatchObject({ type: "completed", value: ok(84) });
  });
});
