/**
 * Compliance tests for the promise id format the server enforces. Mirrors
 * resonate-sdk-py/tests/test_id_format.py.
 *
 * The server (resonatehq/resonate, "new promise id" / PR #1127) treats a
 * promise id as `<origin>:<lineage>`: the origin is everything before the
 * first `:` and the lineage segments below it are `.`-separated:
 *
 *   root -> root:1 -> root:1.1 -> root:1.1.1
 *
 * `serverValidate` is a direct port of the server's
 * `validate_promise_create_data`, and `serverOrigin` of its `origin()` helper.
 * Every promise the SDK creates is replayed through them here, so a drift in
 * id minting fails locally instead of as a 400 from a real server.
 */

import type { Context, Info } from "../src/async/context.js";
import { Resonate } from "../src/async/resonate.js";
import { joinId, originOf, validateRootId } from "../src/ids.js";
import { LocalNetwork, Server } from "../src/network/local.js";
import type { Network } from "../src/network/network.js";
import * as util from "../src/util.js";

// =============================================================================
// The server's rules, ported
// =============================================================================

/** The origin, per the server's `origin()`: text before the first `:`. */
function serverOrigin(id: string): string {
  const sep = id.indexOf(":");
  return sep === -1 ? id : id.slice(0, sep);
}

/** Port of the server's `validate_promise_create_data`. Throws on violation. */
function serverValidate(id: string, tags: Record<string, string>): void {
  if (id.includes("\0")) throw new Error(`null_bytes: id=${id}`);

  const origin = tags["resonate:origin"];
  if (origin !== undefined) {
    if (origin.includes(":")) throw new Error(`colon_in_origin: origin=${origin}`);
    if (id !== origin && !id.startsWith(`${origin}:`)) {
      throw new Error(`origin_prefix: id=${id} is not prefixed by origin=${origin}`);
    }
  }
  for (const key of ["resonate:branch", "resonate:parent"]) {
    const ancestor = tags[key];
    if (ancestor !== undefined) {
      // A bare root joins its first lineage segment with ':'; an ancestor that
      // already carries lineage joins deeper segments with '.'.
      const sep = ancestor.includes(":") ? "." : ":";
      if (id !== ancestor && !id.startsWith(`${ancestor}${sep}`)) {
        throw new Error(`${key}_prefix: id=${id} is not prefixed by ${key}=${ancestor}`);
      }
    }
  }
  const prefix = tags["resonate:prefix"];
  if (prefix?.includes(".")) {
    throw new Error(`dot_in_prefix: prefix=${prefix}`);
  }
}

// =============================================================================
// Workflow under test
// =============================================================================

/** Records every promise.create request (direct or nested in task.create). */
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

/**
 * Run a nested workflow — runs below runs, a durable sleep, and detached
 * children spawned from a nested context (one of which detaches again) — and
 * return every (id, tags) pair the SDK created.
 */
async function runWorkflow(rootId: string): Promise<{ creates: string[]; tags: Map<string, Record<string, string>> }> {
  const recording = new RecordingNetwork(new LocalNetwork({ pid: "default", group: "default" }));
  const resonate = new Resonate({ network: recording, ttl: Number.MAX_SAFE_INTEGER });
  try {
    const leaf = async (_info: Info, n: number): Promise<number> => n;
    const tail = async (_info: Info, n: number): Promise<number> => n;
    const grandchild = async (ctx: Context, n: number): Promise<number> => {
      await ctx.run("leaf", n);
      return n;
    };
    // A detached child that itself detaches — the recursion-bounding case.
    const detachesAgain = async (ctx: Context, n: number): Promise<number> => {
      await ctx.detached("tail", n);
      return n;
    };
    const mid = async (ctx: Context, n: number): Promise<number> => {
      await ctx.run("grandchild", n);
      // A global-scope timer promise: minted from the same seq as everything
      // else. A 1ms sleep settles fast enough for the pass to continue.
      await ctx.sleep(1);
      // Detached from a *nested* context: its id is minted off the origin,
      // not off this context, so its declared ancestors must be the origin
      // too.
      await ctx.detached("detachesAgain", n);
      return n;
    };
    const top = async (ctx: Context, n: number): Promise<number> => {
      await ctx.run("mid", n);
      await ctx.run("mid", n + 1);
      return n;
    };
    resonate.register("leaf", leaf);
    resonate.register("tail", tail);
    resonate.register("grandchild", grandchild);
    resonate.register("detachesAgain", detachesAgain);
    resonate.register("mid", mid);
    resonate.register("top", top);

    await (await resonate.run(rootId, top, 1)).result();

    // Let the fire-and-forget detached children be dispatched and run: 2x mid
    // each detach a child that detaches again -> 4 detached promises.
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const detached = recording.creates.filter((id) => id.startsWith(`${rootId}:d`));
      if (detached.length >= 4) break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    return { creates: [...new Set(recording.creates)], tags: recording.tags };
  } finally {
    await resonate.stop();
  }
}

// =============================================================================
// Tests
// =============================================================================

describe("promise id format compliance (async engine)", () => {
  let created: { creates: string[]; tags: Map<string, Record<string, string>> };

  beforeAll(async () => {
    created = await runWorkflow("wf");
  });

  test.each(["wf", "my.app.workflow"])("every created promise passes server validation (root %s)", async (root) => {
    // A dotted root is a caller's prerogative: '.' is only read below the
    // origin, so every id minted under it still validates.
    const result = await runWorkflow(root);
    expect(result.creates.length).toBeGreaterThan(1);
    for (const id of result.creates) {
      serverValidate(id, result.tags.get(id) ?? {});
      expect(serverOrigin(id)).toBe(root);
    }
  });

  test("whole workflow shares one origin", () => {
    // The origin is the server's partition key and the unit both
    // promise.register_callback and task.suspend match on, so every promise a
    // workflow creates — detached children included — must share it.
    for (const id of created.creates) {
      expect(serverOrigin(id)).toBe("wf");
      expect(created.tags.get(id)?.["resonate:origin"]).toBe("wf");
    }
  });

  test("child ids are colon-then-dot separated", () => {
    // First level below the root joins with ':', deeper levels with '.'.
    expect(created.creates).toContain("wf");
    expect(created.creates).toContain("wf:0");
    expect(created.creates).toContain("wf:0.0");
    expect(created.creates).toContain("wf:0.0.0");
    // No id keeps the old all-'.' shape.
    expect(created.creates.filter((id) => id.startsWith("wf."))).toEqual([]);
  });

  test("detached ids stay bounded below the origin", () => {
    // Detached ids are `{origin}:d{14 hex}` — one segment past the origin no
    // matter how deep the spawning context is, or how many times a detached
    // child detaches again.
    const detached = created.creates.filter((id) => id.startsWith("wf:d"));
    expect(detached.length).toBe(4); // 2x mid, each detaching a child that detaches
    for (const id of detached) {
      expect(id).toMatch(/^wf:d[0-9a-f]{14}$/);
      // The id hangs off the origin, not off the spawning context, so the
      // origin is also the ancestor it declares; branch stays the child's own.
      expect(created.tags.get(id)?.["resonate:parent"]).toBe("wf");
      expect(created.tags.get(id)?.["resonate:branch"]).toBe(id);
    }
  });

  test("the resonate:prefix tag is not emitted", () => {
    for (const id of created.creates) {
      expect(created.tags.get(id)?.["resonate:prefix"]).toBeUndefined();
    }
  });
});

describe("ids module", () => {
  test("joinId matches the server's separator rule", () => {
    expect(joinId("root", "1")).toBe("root:1");
    expect(joinId("root:1", "2")).toBe("root:1.2");
    expect(joinId("root:1.2", "3")).toBe("root:1.2.3");
    expect(joinId("root", "dbeef")).toBe("root:dbeef");
  });

  test("originOf matches the server's origin()", () => {
    for (const id of ["root", "root:1", "root:1.2", "root:dbeef"]) {
      expect(originOf(id)).toBe(serverOrigin(id));
    }
  });

  test.each(["a:b", "a.b:c", "", "a\0b"])("validateRootId rejects %j", (id) => {
    // ':' is the one reserved separator in a root id: it becomes the origin of
    // its whole lineage. See the test below for what it actually breaks.
    expect(() => validateRootId(id)).toThrow(/Invalid id/);
  });

  test.each(["a", "a-b", "a_b", "a.b", "wf-1786636678653183000"])("validateRootId accepts %j", (id) => {
    expect(validateRootId(id)).toBe(id);
  });

  test("a dot in a root id is accepted", () => {
    // '.' only separates lineage segments *below* the origin, which is read
    // after the origin has been split off at the first ':'. A dotted root is
    // therefore unambiguous, and the server takes it.
    expect(validateRootId("my.app.workflow")).toBe("my.app.workflow");
    const id = joinId("my.app.workflow", "1");
    expect(id).toBe("my.app.workflow:1");
    expect(originOf(id)).toBe("my.app.workflow");
    serverValidate(id, { "resonate:origin": "my.app.workflow" });
  });

  test("a colon in a root id is rejected by the server", () => {
    // ':' cannot create the root either: a root is its own origin, and the
    // origin is everything before an id's first ':', so an origin holding one
    // is unrepresentable — no id could ever split back to it.
    expect(() => serverValidate("a:b", { "resonate:origin": "a:b" })).toThrow(/colon_in_origin/);
  });

  test("detachedId agrees with the id format", () => {
    const id = util.detachedId("wf", "wf:3");
    expect(id).toMatch(/^wf:d[0-9a-f]{14}$/);
    expect(originOf(id)).toBe("wf");
  });
});

// =============================================================================
// Scheduler invariant: only targeted promises are timed out
// =============================================================================
//
// Mirrors the server's promise.pendingHasTimeout invariant: a pending promise
// carrying resonate:target always has a timeout scheduled, and one without a
// target must NOT. Divergence here is invisible in ordinary tests — a
// simulation that schedules timeouts for *every* promise simply lets more
// things succeed than the real server does — so it is asserted directly
// against the state machine.

describe("local server scheduler invariant", () => {
  function createPromise(server: Server, now: number, id: string, timeoutAt: number, tags: Record<string, string>) {
    const { response } = server.apply(now, {
      kind: "promise.create",
      head: { corrId: id, version: util.VERSION },
      data: { id, timeoutAt, param: { headers: {}, data: "" }, tags },
    } as any);
    expect((response as any).head.status).toBe(200);
  }

  test("only promises with a target are scheduled for timeout", () => {
    const server = new Server({ timeoutMode: "none" });
    const now = 1_000_000;
    const deadline = now + 60_000;

    createPromise(server, now, "no-target", deadline, { "resonate:scope": "global" });
    createPromise(server, now, "with-target", deadline, {
      "resonate:scope": "global",
      "resonate:target": "poll://any@default",
    });

    const scheduled = server.pTimeouts.map((pt) => pt.id);
    expect(scheduled).toContain("with-target");
    expect(scheduled).not.toContain("no-target");
  });

  test("tick expires only the targeted promise", () => {
    const server = new Server({ timeoutMode: "none" });
    const now = 1_000_000;
    const deadline = now + 60_000;

    createPromise(server, now, "bare", deadline, { "resonate:scope": "global" });
    createPromise(server, now, "timer", deadline, {
      "resonate:scope": "global",
      "resonate:target": "poll://any@default",
      "resonate:timer": "true",
    });

    server.apply(deadline + 1, {
      kind: "debug.tick",
      head: { corrId: "tick", version: util.VERSION },
      data: { time: deadline + 1 },
    } as any);

    // The timer fires — and resonate:timer settles it RESOLVED, which is what
    // wakes a sleeping workflow. The bare promise is left alone.
    expect(server.promises.get("timer")?.state).toBe("resolved");
    expect(server.promises.get("bare")?.state).toBe("pending");
  });
});
