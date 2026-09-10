/**
 * Timer tasks (durable sleep). Mirrors resonate-sdk-py/tests/test_core.py's
 * timer-task group, for both engines.
 *
 * A `resonate:timer` promise is a durable sleep: the wake IS its deadline, and
 * `resonate:timer` makes timing out settle it *resolved*. It carries a
 * `resonate:target` only because the server refuses to schedule a deadline for
 * a promise without one — which also spawns a task, dispatched right away
 * rather than at the wake. That task names no function, so Core must neither
 * run it nor hand it back (a release is re-dispatched immediately, which would
 * spin): it drops it and lets the deadline do the waking.
 */

import { Core as AsyncCore } from "../src/async/core.js";
import { WallClock } from "../src/clock.js";
import { Codec } from "../src/codec.js";
import { Core } from "../src/core.js";
import { NoopHeartbeat } from "../src/heartbeat.js";
import { ConsoleLogger } from "../src/logger.js";
import { LocalNetwork } from "../src/network/local.js";
import type { PromiseRecord, Request, TaskRecord } from "../src/network/types.js";
import { OptionsBuilder } from "../src/options.js";
import { randomUUID } from "../src/platform.js";
import { Registry } from "../src/registry.js";
import type { Send } from "../src/types.js";
import * as util from "../src/util.js";

const FAR_FUTURE = 2 ** 50;

function buildHarness() {
  const network = new LocalNetwork();
  const codec = new Codec();
  const logger = new ConsoleLogger("error");
  const sent: Request[] = [];
  const send: Send = ((req: any) => {
    sent.push(req);
    return network.send(req);
  }) as Send;
  const common = {
    pid: "test-pid",
    ttl: 60_000,
    clock: new WallClock(),
    send,
    codec,
    registry: new Registry(),
    heartbeat: new NoopHeartbeat(),
    dependencies: new Map<string, any>(),
    optsBuilder: new OptionsBuilder({ match: (t: string) => t }),
    logger,
  };
  return { network, codec, sent, send, common };
}

/** Create a timer promise the way ctx.sleep does, then acquire its task. */
async function acquireTimerTask(
  send: Send,
  id: string,
  timeoutAt: number,
): Promise<{ task: TaskRecord; promise: PromiseRecord }> {
  const createRes: any = await send({
    kind: "promise.create",
    head: { corrId: randomUUID(), version: util.VERSION },
    data: {
      id,
      timeoutAt,
      param: { headers: {}, data: "" },
      tags: {
        "resonate:scope": "global",
        "resonate:branch": id,
        "resonate:target": "local://any@default",
        "resonate:timer": "true",
      },
    },
  } as any);
  expect(createRes.head.status).toBe(200);

  const acquireRes: any = await send({
    kind: "task.acquire",
    head: { corrId: randomUUID(), version: util.VERSION },
    data: { id, version: 0, pid: "test-pid", ttl: 60_000 },
  } as any);
  expect(acquireRes.head.status).toBe(200);
  return { task: acquireRes.data.task, promise: acquireRes.data.promise };
}

describe.each([
  ["generator engine", (common: any) => new Core(common)],
  ["async engine", (common: any) => new AsyncCore(common)],
])("timer tasks — %s", (_name, makeCore) => {
  test("a not-yet-due timer task is dropped", async () => {
    const { sent, send, common } = buildHarness();
    const core = makeCore(common);
    const { task, promise } = await acquireTimerTask(send, "t-pending", FAR_FUTURE);
    expect(promise.state).toBe("pending");

    sent.length = 0;
    const status = await core.executeUntilBlocked(task, promise);

    expect(status.kind).toBe("suspended");
    // Dropped means dropped: no fulfill (that would end the sleep early), no
    // suspend, no release — nothing reached the server at all.
    expect(sent).toEqual([]);
    // Still pending: the sleep was not ended early.
    const getRes: any = await send({
      kind: "promise.get",
      head: { corrId: randomUUID(), version: util.VERSION },
      data: { id: "t-pending" },
    } as any);
    expect(getRes.data.promise.state).toBe("pending");
  });

  test("a due timer fulfills the task without decoding", async () => {
    // A delivery already in flight when the deadline settled the promise. A
    // timer's empty param holds no TaskData, so this has to short-circuit
    // before the decode rather than fail on it.
    const { sent, send, common } = buildHarness();
    const core = makeCore(common);
    const { task, promise } = await acquireTimerTask(send, "t-due", FAR_FUTURE);

    // The record as the worker would observe it after the deadline settled it.
    const settled: PromiseRecord = { ...promise, state: "resolved", value: { headers: {}, data: undefined } };

    sent.length = 0;
    const status = await core.executeUntilBlocked(task, settled);

    expect(status.kind).toBe("done");
    expect((status as any).state).toBe("resolved");
    const fulfill = sent.find((r) => r.kind === "task.fulfill");
    expect(fulfill).toBeDefined();
    expect((fulfill as any).data.action.data.state).toBe("resolved");
  });
});
