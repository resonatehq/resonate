import { setTimeout as sleep } from "node:timers/promises";
import type { Context, Info } from "../../src/async/index.js";
import { Resonate } from "../../src/async/index.js";
import { Codec } from "../../src/codec.js";
import { LocalNetwork } from "../../src/network/local.js";
import type { Network } from "../../src/network/network.js";
import { Constant } from "../../src/retries.js";

function newResonate(): Resonate {
  return new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER });
}

// Wraps a network and records every durable promise.create issued — bare
// (resonate.rpc), embedded in task.create (resonate.run), or embedded in
// task.fence (the effects path) — used to assert creation ordering and tags.
class RecordingNetwork implements Network {
  readonly creates: string[] = [];
  readonly tags = new Map<string, Record<string, string>>();
  constructor(private inner: Network) {}
  get unicast() {
    return this.inner.unicast;
  }
  get anycast() {
    return this.inner.anycast;
  }
  match(target: string) {
    return this.inner.match(target);
  }
  init() {
    return this.inner.init();
  }
  stop() {
    return this.inner.stop();
  }
  send: Network["send"] = ((req: any) => {
    const data =
      req.kind === "promise.create"
        ? req.data
        : req.data?.action?.kind === "promise.create"
          ? req.data.action.data
          : undefined;
    if (data) {
      this.creates.push(data.id);
      this.tags.set(data.id, data.tags);
    }
    return this.inner.send(req);
  }) as Network["send"];
  recv: Network["recv"] = (cb) => this.inner.recv(cb);
}

describe("Resonate — async/await engine", () => {
  let resonate: Resonate;

  afterEach(async () => {
    await resonate?.stop();
    jest.restoreAllMocks();
  });

  test("leaf (Info) completes in one pass", async () => {
    resonate = newResonate();
    const double = async (_info: Info, n: number): Promise<number> => n * 2;
    resonate.register("double", double);

    const handle = await resonate.run("double-1", double, 21);
    expect(await handle.result()).toBe(42);
  });

  test("workflow runs a local leaf and returns its value", async () => {
    resonate = newResonate();
    resonate.register("inc", async (_info: Info, n: number): Promise<number> => n + 1);
    const wf = async (ctx: Context, n: number): Promise<number> => ctx.run<number>("inc", n);
    resonate.register("wf", wf);

    expect(await (await resonate.run("wf-1", wf, 9)).result()).toBe(10);
  });

  test("sequential local runs", async () => {
    resonate = newResonate();
    resonate.register("inc", async (_info: Info, n: number): Promise<number> => n + 1);
    const seq = async (ctx: Context, n: number): Promise<number> => {
      const a = await ctx.run<number>("inc", n);
      const b = await ctx.run<number>("inc", a);
      return b;
    };
    resonate.register("seq", seq);

    expect(await (await resonate.run("seq-1", seq, 0)).result()).toBe(2);
  });

  test("eager fan-out / fan-in", async () => {
    resonate = newResonate();
    resonate.register("sq", async (_info: Info, n: number): Promise<number> => n * n);
    const fanout = async (ctx: Context, items: number[]): Promise<number[]> => {
      // start all eagerly, then await — concurrent fan-out
      const handles = items.map((n) => ctx.run<number>("sq", n));
      return Promise.all(handles);
    };
    resonate.register("fanout", fanout);

    expect(await (await resonate.run("fanout-1", fanout, [1, 2, 3, 4])).result()).toEqual([1, 4, 9, 16]);
  });

  test("rpc suspends then resumes (cross-worker, same process)", async () => {
    resonate = newResonate();
    resonate.register("leaf", async (_info: Info, n: number): Promise<number> => n + 100);
    const rpcwf = async (ctx: Context, n: number): Promise<number> => ctx.rpc<number>("leaf", n);
    resonate.register("rpcwf", rpcwf);

    expect(await (await resonate.run("rpcwf-1", rpcwf, 5)).result()).toBe(105);
  });

  test("recursive fib via rpc", async () => {
    resonate = newResonate();
    const fib = async (ctx: Context, n: number): Promise<number> => {
      if (n <= 1) return n;
      const a = ctx.rpc<number>("fib", n - 1);
      const b = ctx.rpc<number>("fib", n - 2);
      return (await a) + (await b);
    };
    resonate.register("fib", fib);

    expect(await (await resonate.run("fib-6", fib, 6)).result()).toBe(8);
  });

  test("try/catch catches a real rejection from a durable call", async () => {
    resonate = newResonate();
    class Boom extends Error {}
    resonate.register("boom", async (_info: Info): Promise<number> => {
      throw new Boom("kaboom");
    });
    const catchwf = async (ctx: Context): Promise<string> => {
      try {
        await ctx.run<number>("boom");
        return "no-throw";
      } catch (e) {
        return `caught:${(e as Error).message}`;
      }
    };
    resonate.register("catchwf", catchwf);

    expect(await (await resonate.run("catchwf-1", catchwf)).result()).toBe("caught:kaboom");
  });

  test("error path: an uncaught workflow throw rejects the handle", async () => {
    resonate = newResonate();
    const err = async (_ctx: Context): Promise<number> => {
      throw new Error("workflow failed");
    };
    resonate.register("err", err);

    await expect((await resonate.run("err-1", err)).result()).rejects.toThrow("workflow failed");
  });

  test("sleep resolves and the workflow proceeds", async () => {
    resonate = newResonate();
    const sleepwf = async (ctx: Context): Promise<string> => {
      await ctx.sleep(1);
      return "awake";
    };
    resonate.register("sleepwf", sleepwf);

    expect(await (await resonate.run("sleepwf-1", sleepwf)).result()).toBe("awake");
  });

  test("creation sequencer: concurrent fan-out creates promises in source order", async () => {
    const recording = new RecordingNetwork(new LocalNetwork({ pid: "default", group: "default" }));
    resonate = new Resonate({ network: recording, ttl: Number.MAX_SAFE_INTEGER });
    resonate.register("noop", async (_info: Info, n: number): Promise<number> => n);
    const fanout = async (ctx: Context): Promise<number[]> => {
      const handles = [0, 1, 2, 3].map((n) => ctx.run<number>("noop", n));
      return Promise.all(handles);
    };
    resonate.register("createorder", fanout);

    await (await resonate.run("createorder", fanout)).result();

    const children = recording.creates.filter((id) => id.startsWith("createorder."));
    expect(children).toEqual(["createorder.0", "createorder.1", "createorder.2", "createorder.3"]);
  });

  test("hang model: a suspending pass never enters the surrounding try/catch", async () => {
    resonate = newResonate();
    let catchCount = 0; // non-durable; would increment if Suspended leaked into catch
    resonate.register("leaf", async (_info: Info, n: number): Promise<number> => n + 1);
    const wf = async (ctx: Context): Promise<string> => {
      try {
        const v = await ctx.rpc<number>("leaf", 41); // pending on pass 1 → hangs (no throw)
        return `ok:${v}`;
      } catch {
        catchCount++;
        return "caught";
      }
    };
    resonate.register("trycatchwf", wf);

    expect(await (await resonate.run("tc-1", wf)).result()).toBe("ok:42");
    expect(catchCount).toBe(0);
  });

  test("opt-in retry: a flaky leaf succeeds after retries and sees the attempt", async () => {
    resonate = newResonate();
    let calls = 0;
    resonate.register("flaky", async (info: Info): Promise<string> => {
      calls++;
      if (calls < 3) throw new Error("transient");
      return `ok-attempt-${info.attempt}`;
    });
    const wf = async (ctx: Context): Promise<string> =>
      ctx.run<string>("flaky", ctx.options({ retryPolicy: new Constant({ delay: 1, maxRetries: 5 }) }));
    resonate.register("retrywf", wf);

    expect(await (await resonate.run("retry-1", wf)).result()).toBe("ok-attempt-3");
    expect(calls).toBe(3);
  });

  test("nonRetryableErrors: a flaky leaf fails immediately for a non-retryable error", async () => {
    resonate = newResonate();
    class Fatal extends Error {}
    let calls = 0;
    resonate.register("fatal", async (_info: Info): Promise<void> => {
      calls++;
      throw new Fatal("nope");
    });
    const wf = async (ctx: Context): Promise<string> => {
      try {
        await ctx.run<void>(
          "fatal",
          ctx.options({ retryPolicy: new Constant({ delay: 1, maxRetries: 5 }), nonRetryableErrors: [Fatal] }),
        );
        return "no-throw";
      } catch (e) {
        return `caught:${(e as Error).message}`;
      }
    };
    resonate.register("fatalwf", wf);

    expect(await (await resonate.run("fatal-1", wf)).result()).toBe("caught:nope");
    expect(calls).toBe(1);
  });

  test("default policy does not retry a throwing leaf", async () => {
    resonate = newResonate();
    let calls = 0;
    resonate.register("once", async (_info: Info): Promise<void> => {
      calls++;
      throw new Error("boom");
    });
    const wf = async (ctx: Context): Promise<string> => {
      try {
        await ctx.run<void>("once");
        return "no-throw";
      } catch {
        return "caught";
      }
    };
    resonate.register("oncewf", wf);

    expect(await (await resonate.run("once-1", wf)).result()).toBe("caught");
    expect(calls).toBe(1);
  });

  test("sleep({ for }) and sleep({ until }) resolve", async () => {
    resonate = newResonate();
    const forwf = async (ctx: Context): Promise<string> => {
      await ctx.sleep({ for: 1 });
      return "awake-for";
    };
    const untilwf = async (ctx: Context): Promise<string> => {
      await ctx.sleep({ until: new Date(Date.now() + 1) });
      return "awake-until";
    };
    resonate.register("forwf", forwf);
    resonate.register("untilwf", untilwf);

    expect(await (await resonate.run("for-1", forwf)).result()).toBe("awake-for");
    expect(await (await resonate.run("until-1", untilwf)).result()).toBe("awake-until");
  });

  test("human-in-the-loop: ctx.promise (DPC) resolved externally", async () => {
    resonate = newResonate();
    const codec = new Codec();
    const wf = async (ctx: Context): Promise<string> => {
      expect(ctx.id).toBe("dpc-1");
      return ctx.promise<string>();
    };
    resonate.register("dpcwf", wf);

    const handle = await resonate.run("dpc-1", wf);
    await sleep(100); // ensure the latent promise dpc-1.0 has been created
    await resonate.promises.resolve("dpc-1.0", { data: codec.encode("signal").data });

    expect(await handle.result()).toBe("signal");
  });

  test("detached spawns an independent workflow that runs to completion", async () => {
    resonate = newResonate();
    resonate.register("detachedChild", async (_info: Info, n: number): Promise<number> => n * 10);
    const parent = async (ctx: Context): Promise<string> => {
      const h = await ctx.detached("detachedChild", 5);
      return h.id; // parent does NOT await the child's result
    };
    resonate.register("parent", parent);

    const childId = await (await resonate.run("parent-1", parent)).result();
    // The parent completed without waiting; the detached child runs on its own
    // root task and its result can be awaited out of band.
    expect(await (await resonate.get<number>(childId)).result()).toBe(50);
  });

  test("resonate:prefix is set at the root and propagates to every child create", async () => {
    const recording = new RecordingNetwork(new LocalNetwork({ pid: "default", group: "default" }));
    resonate = new Resonate({ network: recording, ttl: Number.MAX_SAFE_INTEGER });
    resonate.register("pchild", async (_info: Info, n: number): Promise<number> => n);
    resonate.register("pdet", async (_info: Info): Promise<void> => {});
    const wf = async (ctx: Context): Promise<number> => {
      const v = await ctx.run<number>("pchild", 7);
      await ctx.sleep(1);
      await ctx.detached("pdet");
      return v;
    };
    resonate.register("prefixwf", wf);

    await (await resonate.run("prefix-1", wf)).result();

    expect(recording.tags.get("prefix-1")?.["resonate:prefix"]).toBe("prefix-1"); // root
    expect(recording.tags.get("prefix-1.0")?.["resonate:prefix"]).toBe("prefix-1"); // local child
    expect(recording.tags.get("prefix-1.1")?.["resonate:prefix"]).toBe("prefix-1"); // sleep timer
    const detachedId = recording.creates.find((id) => id.startsWith("prefix-1.d"));
    expect(detachedId).toMatch(/^prefix-1\.d[0-9a-f]{14}$/);
    expect(recording.tags.get(detachedId as string)?.["resonate:prefix"]).toBe("prefix-1");
  });

  test("nested detached ids stay bounded via the fixed prefix", async () => {
    resonate = newResonate();
    const seen: { id: string; prefixId: string; originId: string }[] = [];
    const deepest = Promise.withResolvers<void>();
    const nest = async (ctx: Context, depth: number): Promise<void> => {
      seen.push({ id: ctx.id, prefixId: ctx.prefixId, originId: ctx.originId });
      if (depth >= 3) {
        deepest.resolve();
        return;
      }
      await ctx.detached("nest", depth + 1);
    };
    resonate.register("nest", nest);

    await (await resonate.run("nest-root", nest, 1)).result();
    await deepest.promise;

    expect(seen).toHaveLength(3);
    expect(seen[0].id).toBe("nest-root");
    // Every level keeps the top-level root as its id-generation prefix, so each
    // detached id is exactly one `.d` segment past it — bounded at any depth.
    for (const s of seen) expect(s.prefixId).toBe("nest-root");
    expect(seen[1].id).toMatch(/^nest-root\.d[0-9a-f]{14}$/);
    expect(seen[2].id).toMatch(/^nest-root\.d[0-9a-f]{14}$/);
    // The detached re-root breaks lineage: origin resets to its own id.
    expect(seen[1].originId).toBe(seen[1].id);
  });

  test("ctx.panic aborts the pass: execution stops and nothing settles", async () => {
    jest.spyOn(console, "error").mockImplementation(() => {});
    jest.spyOn(console, "warn").mockImplementation(() => {});
    resonate = newResonate();

    let completed = false;
    const wf = async (ctx: Context): Promise<string> => {
      ctx.panic(true, "This should abort");
      completed = true;
      return "should not reach here";
    };
    resonate.register("panicwf", wf);

    await resonate.run("panic-1", wf);
    await sleep(50);

    expect(completed).toBe(false);
    // The task is released without settling — the root stays pending, exactly
    // like the generator engine's DIE (never a rejected root).
    expect((await resonate.promises.get("panic-1")).state).toBe("pending");
  });

  test("a user try/catch cannot suppress a panic", async () => {
    jest.spyOn(console, "error").mockImplementation(() => {});
    jest.spyOn(console, "warn").mockImplementation(() => {});
    resonate = newResonate();

    const wf = async (ctx: Context): Promise<string> => {
      try {
        ctx.assert(false, "assertion failed");
      } catch {
        // swallowed — but the abort is recorded on the context and must win
      }
      return "survived";
    };
    resonate.register("sneakywf", wf);

    await resonate.run("panic-2", wf);
    await sleep(50);

    expect((await resonate.promises.get("panic-2")).state).toBe("pending");
  });

  test("a panic in a child workflow aborts the whole pass", async () => {
    jest.spyOn(console, "error").mockImplementation(() => {});
    jest.spyOn(console, "warn").mockImplementation(() => {});
    resonate = newResonate();

    resonate.register("panicchild", async (ctx: Context): Promise<void> => {
      ctx.panic(true, "child abort");
    });
    const parent = async (ctx: Context): Promise<string> => {
      await ctx.run<void>("panicchild");
      return "done";
    };
    resonate.register("panicparent", parent);

    await resonate.run("panic-3", parent);
    await sleep(50);

    // Neither the root nor the (created) child promise settles.
    expect((await resonate.promises.get("panic-3")).state).toBe("pending");
    expect((await resonate.promises.get("panic-3.0")).state).toBe("pending");
  });

  test("ctx.panic(false) and ctx.assert(true) do not abort", async () => {
    resonate = newResonate();
    const wf = async (ctx: Context): Promise<string> => {
      ctx.panic(false, "should not abort");
      ctx.assert(true, "should pass");
      return "success";
    };
    resonate.register("okwf", wf);

    expect(await (await resonate.run("panic-4", wf)).result()).toBe("success");
  });

  test("get() returns a handle to an existing promise", async () => {
    resonate = newResonate();
    resonate.register("idfn", async (_info: Info, n: number): Promise<number> => n + 7);
    await (await resonate.run("get-1", "idfn", 3)).result();

    expect(await (await resonate.get<number>("get-1")).result()).toBe(10);
  });

  test("schedule() creates and deletes a cron schedule", async () => {
    resonate = newResonate();
    resonate.register("reportfn", async (_info: Info): Promise<void> => {});

    const sched = await resonate.schedule("daily", "* * * * *", "reportfn");
    expect((await resonate.schedules.get("daily")).id).toBe("daily");

    await sched.delete();
    await expect(resonate.schedules.get("daily")).rejects.toThrow();
  });

  // Proves the parked frames are GC-collectible after suspend (the hang model's
  // one cost). Each suspended pass holds a ~400KB buffer live across the await;
  // if parked frames were retained, N of them would balloon the heap. Requires
  // `node --expose-gc`; skipped otherwise. (A single-object WeakRef probe is too
  // flaky for CI — closure/microtask artifacts pin one object — so we assert on
  // aggregate heap instead.)
  const maybeGc = (globalThis as any).gc as undefined | (() => void);
  (maybeGc ? test : test.skip)("parked frames do not accumulate across suspensions", async () => {
    resonate = newResonate();
    const wf = async (ctx: Context): Promise<number> => {
      const buf = new Array(50000).fill(7); // ~400KB, live across the await
      await ctx.sleep(60 * 60 * 1000); // suspends; won't fire during the test
      return buf.length;
    };
    resonate.register("leakwf", wf);

    maybeGc!();
    await new Promise((r) => setTimeout(r, 20));
    const before = process.memoryUsage().heapUsed;

    const N = 2000;
    for (let i = 0; i < N; i++) {
      await resonate.run(`leak-${i}`, wf); // each suspends
      if (i % 500 === 0) await new Promise((r) => setTimeout(r, 0));
    }
    await new Promise((r) => setTimeout(r, 100));
    for (let i = 0; i < 6; i++) {
      maybeGc!();
      await new Promise((r) => setTimeout(r, 20));
    }

    // Delta isolates the engine's retention from Jest/coverage baseline. Full
    // retention of N parked frames (~400KB each) would add ~800MB; a healthy
    // engine adds only bounded per-outstanding-promise bookkeeping. 150MB is a
    // generous ceiling that still proves frames (and their live locals) collect.
    const deltaMb = (process.memoryUsage().heapUsed - before) / 1024 / 1024;
    expect(deltaMb).toBeLessThan(150);
  });
});
