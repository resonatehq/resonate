import { randomUUID } from "node:crypto";
import { WallClock } from "../src/clock.js";
import { Codec } from "../src/codec.js";
import { type Context, InnerContext } from "../src/context.js";
import { Coroutine, type Suspended } from "../src/coroutine.js";
import { ConsoleLogger } from "../src/logger.js";
import type { PromiseRecord, Request, Response } from "../src/network/types.js";
import { OptionsBuilder } from "../src/options.js";
import { Registry } from "../src/registry.js";
import { Never } from "../src/retries.js";
import { TraceCollector } from "../src/trace.js";
import type { Effects, Result, Send } from "../src/types.js";
import * as util from "../src/util.js";

const testLogger = new ConsoleLogger("error");

class DummyNetwork {
  private promises = new Map<string, PromiseRecord>();

  send: Send = <K extends Request["kind"]>(
    req: Extract<Request, { kind: K }>,
  ): Promise<Extract<Response, { kind: K }>> => {
    switch (req.kind) {
      case "promise.create": {
        const createReq = req as Extract<Request, { kind: "promise.create" }>;
        const existing = this.promises.get(createReq.data.id);
        if (existing) {
          return Promise.resolve({
            kind: req.kind,
            head: { corrId: req.head.corrId, status: 200, version: req.head.version },
            data: { promise: existing },
          } as Extract<Response, { kind: K }>);
        }
        const p: PromiseRecord = {
          id: createReq.data.id,
          state: "pending",
          timeoutAt: createReq.data.timeoutAt,
          param: createReq.data.param,
          value: { headers: {}, data: "" },
          tags: createReq.data.tags,
          createdAt: Date.now(),
        };
        this.promises.set(p.id, p);
        return Promise.resolve({
          kind: req.kind,
          head: { corrId: req.head.corrId, status: 200, version: req.head.version },
          data: { promise: p },
        } as Extract<Response, { kind: K }>);
      }

      case "promise.settle": {
        const settleReq = req as Extract<Request, { kind: "promise.settle" }>;
        const p = this.promises.get(settleReq.data.id)!;
        p.state = "resolved";
        p.value = settleReq.data.value;
        this.promises.set(p.id, p);
        return Promise.resolve({
          kind: req.kind,
          head: { corrId: req.head.corrId, status: 200, version: req.head.version },
          data: { promise: p },
        } as Extract<Response, { kind: K }>);
      }
      default:
        throw new Error("All other kind will not be implemented");
    }
  };
}

function buildEffects(network: DummyNetwork): Effects {
  const codec = new Codec();
  return util.buildEffects(network.send, codec);
}

describe("Coroutine", () => {
  const exec = async (uuid: string, func: (ctx: Context, ...args: any[]) => any, args: any[], effects: Effects) => {
    const res = await Coroutine.exec(
      testLogger,
      new InnerContext({
        id: uuid,
        oId: uuid,
        func: func.name,
        clock: new WallClock(),
        registry: new Registry(),
        dependencies: new Map(),
        timeout: 0,
        version: 1,
        retryPolicy: new Never(),
        optsBuilder: new OptionsBuilder({ match: (t) => t, idPrefix: "" }),
      }),
      func,
      args,
      effects,
      new TraceCollector(),
    );

    return res;
  };

  const completePromise = async (effects: Effects, id: string, result: Result<any, any>) => {
    return await effects.promiseSettle(
      {
        kind: "promise.settle",
        head: { corrId: randomUUID(), version: util.VERSION },
        data: {
          id: id,
          state: result.kind === "value" ? "resolved" : "rejected",
          value: {
            headers: {},
            data: result.kind === "value" ? result.value : result.error,
          },
        },
      },
      "test",
    );
  };

  test("basic coroutine completes with done", async () => {
    function* bar() {
      return 42;
    }

    function* foo(ctx: Context) {
      const p = yield* ctx.beginRun(bar);
      const v = yield* p;
      return v;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 42 } });
  });

  // Local non-generator functions are now started eagerly and awaited inline.
  // The generator completes in a single execution — no suspension for locals.
  test("basic coroutine with functions completes in single execution", async () => {
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
    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // Completes in one execution — no suspension for locals
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 31458 } });
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

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    let r = await exec("foo.1", foo, [], effects);
    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(effects, "foo.1.1", { kind: "value", value: 42 });
    r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 42 } });
  });

  test("Structured concurrency", async () => {
    function* foo(ctx: Context) {
      yield* ctx.beginRpc("bar");
      yield* ctx.beginRpc("bar");
      return 99;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    let r = await exec("foo.1", foo, [], effects);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(2);

    await completePromise(effects, "foo.1.1", { kind: "value", value: 42 });
    r = await exec("foo.1", foo, [], effects);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(effects, "foo.1.0", { kind: "value", value: 42 });
    r = await exec("foo.1", foo, [], effects);

    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 99 } });
  });

  // Fire-and-forget local functions are flushed at return (structured concurrency).
  // They are settled inline — no external settlement needed, single execution.
  test("Structured concurrency with local non-generator fire-and-forget", async () => {
    function bar() {
      return 10;
    }
    function baz() {
      return 20;
    }

    function* foo(ctx: Context) {
      // Spawn two local (non-generator) tasks without awaiting the returned futures
      yield* ctx.beginRun(bar);
      yield* ctx.beginRun(baz);
      return 99;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // Locals are flushed at return → completes in single execution
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 99 } });
  });

  // Local fire-and-forget is flushed and settled inline at return time.
  // Only the remote causes suspension — two executions instead of three.
  test("Structured concurrency with mixed local and remote fire-and-forget", async () => {
    function bar() {
      return 10;
    }

    function* foo(ctx: Context) {
      // Spawn one local (non-generator) task and one remote task without awaiting
      yield* ctx.beginRun(bar);
      yield* ctx.beginRpc("someRemote");
      return 77;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // First execution: local is flushed and settled inline at return,
    // but remote is still pending → suspended with 1 remote todo
    let r = await exec("foo.1", foo, [], effects);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    expect(suspended.todo.remote).toHaveLength(1);

    // Settle the remote promise
    await completePromise(effects, "foo.1.1", { kind: "value", value: 42 });

    // Second execution: everything resolved → done
    r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 77 } });
  });

  test("Structured concurrency not triggered when inner generator completes inline", async () => {
    // When beginRun is used with a generator function and that generator
    // resolves without any pending RPCs, the coroutine settles it inline
    // so there are no pending todos at the time the parent returns.
    function* child() {
      return 55;
    }

    function* foo(ctx: Context) {
      // Spawn a generator that completes entirely inline (no RPCs inside)
      yield* ctx.beginRun(child);
      return 88;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // The child generator completes inline, so no pending todos exist when
    // foo returns → should complete in a single execution without suspending.
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 88 } });
  });

  test("Detached concurrency", async () => {
    function* foo(ctx: Context) {
      yield* ctx.beginRpc("bar");
      yield* ctx.detached("bar");
      return 99;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    let r = await exec("foo.1", foo, [], effects);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(effects, "foo.1.0", { kind: "value", value: 42 });
    r = await exec("foo.1", foo, [], effects);

    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 99 } });
  });

  test("Return the detached todo if explicitly awaited", async () => {
    function* foo(ctx: Context) {
      yield* ctx.beginRpc("bar");
      const df = yield* ctx.detached("bar");
      const v = yield* df;
      return v;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    let r = await exec("foo.1", foo, [], effects);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(2);

    await completePromise(effects, "foo.1.0", { kind: "value", value: 42 });
    r = await exec("foo.1", foo, [], effects);

    expect(r.type).toBe("suspended");
    r = r as Suspended;
    expect(r.todo.remote).toHaveLength(1);

    await completePromise(effects, "foo.1.1", { kind: "value", value: 42 });
    r = await exec("foo.1", foo, [], effects);

    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 42 } });
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

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    let r = await exec("foo.1", foo, [], effects);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    expect(suspended.todo.remote).toHaveLength(1);

    await completePromise(effects, "foo.1.1", { kind: "value", value: 42 });

    r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 84 } });
  });

  test("DIE with condition true aborts execution", async () => {
    const originalError = console.error;
    console.error = () => {};

    function* foo(ctx: Context) {
      yield* ctx.panic(true, "Abort execution");
      return 42;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    await expect(
      Coroutine.exec(
        testLogger,
        new InnerContext({
          id: "foo.1",
          func: foo.name,
          clock: new WallClock(),
          registry: new Registry(),
          dependencies: new Map(),
          timeout: 0,
          version: 1,
          retryPolicy: new Never(),
          optsBuilder: new OptionsBuilder({ match: (t) => t, idPrefix: "" }),
        }),
        foo,
        [],
        effects,
        new TraceCollector(),
      ),
    ).rejects.toThrow();

    console.error = originalError;
  });

  test("DIE with condition false continues execution", async () => {
    function* foo(ctx: Context) {
      yield* ctx.panic(false, "Should not abort");
      return 42;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 42 } });
  });

  // --- New tests ---

  test("local function error surfaces at await time", async () => {
    function failing(_ctx: Context) {
      throw new Error("boom");
    }

    function* foo(ctx: Context) {
      const f = yield* ctx.beginRun(failing, ctx.options({ retryPolicy: new Never() }));
      try {
        yield* f;
        return "should not reach";
      } catch (e) {
        return `caught: ${(e as Error).message}`;
      }
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: "caught: boom" } });
  });

  test("multiple local functions run concurrently", async () => {
    const log: string[] = [];

    async function slow(_ctx: Context) {
      log.push("slow-start");
      await new Promise((r) => setTimeout(r, 50));
      log.push("slow-end");
      return 1;
    }

    async function fast(_ctx: Context) {
      log.push("fast-start");
      log.push("fast-end");
      return 2;
    }

    function* foo(ctx: Context) {
      const f1 = yield* ctx.beginRun(slow, ctx.options({ retryPolicy: new Never() }));
      const f2 = yield* ctx.beginRun(fast, ctx.options({ retryPolicy: new Never() }));
      const v1 = yield* f1;
      const v2 = yield* f2;
      return v1 + v2;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 3 } });

    // Both started before either completes (true concurrency)
    expect(log[0]).toBe("slow-start");
    expect(log[1]).toBe("fast-start");
  });

  test("child generator suspends on remote, parent suspends too", async () => {
    function* child(ctx: Context) {
      const v: number = yield* ctx.rpc<number>("remoteFunc");
      return v * 2;
    }

    function* parent(ctx: Context) {
      const f = yield* ctx.beginRun(child);
      const v = yield* f;
      return v + 1;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // First execution: child suspends on remote → parent suspends too
    let r = await exec("foo.1", parent, [], effects);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    expect(suspended.todo.remote).toHaveLength(1);

    // Settle the remote promise that the child was waiting on
    await completePromise(effects, "foo.1.0.0", { kind: "value", value: 21 });

    // Second execution: child completes → parent completes
    r = await exec("foo.1", parent, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 43 } });
  });

  test("flush at return settles durable promises for fire-and-forget locals", async () => {
    function bar() {
      return 10;
    }

    function* foo(ctx: Context) {
      yield* ctx.beginRun(bar);
      yield* ctx.beginRpc("someRemote");
      return 77;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // First execution: local flushed + settled inline, remote still pending
    let r = await exec("foo.1", foo, [], effects);
    expect(r.type).toBe("suspended");

    // Settle remote
    await completePromise(effects, "foo.1.1", { kind: "value", value: 42 });

    // Re-execution: local promise already settled (fast-forward), completes
    r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 77 } });
  });

  test("local work is flushed when suspending on remote await (no deadlock)", async () => {
    // Reproduces the deadlock scenario from the fibonacci example:
    // beginRpc creates a remote pending promise (v1), beginRun starts local work (v2).
    // When the generator awaits v1 first, the coroutine must flush v2's local work
    // before suspending. Otherwise v2's durable promise stays pending forever with
    // no task record to drive it → deadlock on resume.
    function* child(ctx: Context) {
      // child itself has a remote dependency
      const v: number = yield* ctx.rpc<number>("remoteChild");
      return v * 10;
    }

    function* parent(ctx: Context) {
      const v1 = yield* ctx.beginRpc<number>("remoteParent");
      const v2 = yield* ctx.beginRun(child);
      // Await the remote first — this is where the deadlock used to occur
      const r1 = yield* v1;
      const r2 = yield* v2;
      return r1 + r2;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // First execution: parent suspends on remoteParent.
    // The child's local work must be flushed, surfacing its remote dep.
    let r = await exec("p.1", parent, [], effects);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    // Must include BOTH remoteParent AND the child's remoteChild
    expect(suspended.todo.remote.length).toBeGreaterThanOrEqual(2);

    // Settle both remote promises
    await completePromise(effects, "p.1.0", { kind: "value", value: 5 });
    await completePromise(effects, "p.1.1.0", { kind: "value", value: 3 });

    // Second execution: everything settled → done
    r = await exec("p.1", parent, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 35 } });
  });

  test("local non-generator work is flushed and settled when suspending on remote await", async () => {
    function bar() {
      return 42;
    }

    function* parent(ctx: Context) {
      const v1 = yield* ctx.beginRpc<number>("remoteParent");
      const v2 = yield* ctx.beginRun(bar);
      // Await remote first
      const r1 = yield* v1;
      const r2 = yield* v2;
      return r1 + r2;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);

    // First execution: suspends on remote, but local bar() must be flushed and settled
    let r = await exec("p.1", parent, [], effects);
    expect(r.type).toBe("suspended");
    const suspended = r as Suspended;
    expect(suspended.todo.remote).toHaveLength(1); // only remoteParent

    // Settle the remote
    await completePromise(effects, "p.1.0", { kind: "value", value: 8 });

    // Second execution: bar's durable promise was settled on first run → replay fast-path
    r = await exec("p.1", parent, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 50 } });
  });

  test("Future state and value update after inline resolution (double-yield)", async () => {
    function bar(_ctx: Context) {
      return 42;
    }

    function* foo(ctx: Context) {
      const f = yield* ctx.beginRun(bar, ctx.options({ retryPolicy: new Never() }));
      const v = yield* f;
      // Double-yield: f is now completed, should fast-path through
      yield* f;
      return v;
    }

    const network = new DummyNetwork();
    const effects = buildEffects(network);
    const r = await exec("foo.1", foo, [], effects);
    expect(r).toMatchObject({ type: "done", result: { kind: "value", value: 42 } });
  });
});
