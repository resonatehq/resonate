import type { Clock } from "./clock.js";
import type { Codec } from "./codec.js";
import { Computation, type Done, type Status } from "./computation.js";
import exceptions, { ResonateError } from "./exceptions.js";
import type { Heartbeat } from "./heartbeat.js";
import { drive, type Fiber } from "./kernel.js";
import type { Logger } from "./logger.js";
import {
  isRedirect,
  isSuccess,
  type Message,
  type PromiseRecord,
  type Request,
  type Response,
  type TaskRecord,
} from "./network/types.js";
import type { OptionsBuilder } from "./options.js";
import { randomUUID } from "./platform.js";
import type { Registry } from "./registry.js";
import { Constant, Exponential, Linear, Never, type RetryPolicyConstructor } from "./retries.js";
import { TraceCollector } from "./trace.js";
import type { Effects, Send } from "./types.js";
import * as util from "./util.js";

export type PromiseHandler = {
  addEventListener: (event: "created" | "completed", callback: (p: PromiseRecord) => void) => void;
  subscribe: () => Promise<void>;
};

export class Core {
  private pid: string;
  private ttl: number;
  private clock: Clock;
  private send: Send;
  private codec: Codec;
  private retries: Map<string, RetryPolicyConstructor>;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private logger: Logger;

  constructor({
    pid,
    ttl,
    clock,
    send,
    codec,
    registry,
    heartbeat,
    dependencies,
    optsBuilder,
    logger,
  }: {
    pid: string;
    ttl: number;
    clock: Clock;
    send: Send;
    codec: Codec;
    registry: Registry;
    heartbeat: Heartbeat;
    dependencies: Map<string, any>;
    optsBuilder: OptionsBuilder;
    logger: Logger;
  }) {
    this.pid = pid;
    this.ttl = ttl;
    this.clock = clock;
    this.send = send;
    this.codec = codec;
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.dependencies = dependencies;
    this.optsBuilder = optsBuilder;
    this.logger = logger;

    // default retry policies
    this.retries = new Map<string, RetryPolicyConstructor>([
      [Constant.type, Constant],
      [Exponential.type, Exponential],
      [Linear.type, Linear],
      [Never.type, Never],
    ]);
  }

  /** Runs `execute` with the production driver. */
  public executeUntilBlocked(task: TaskRecord, rootPromise: PromiseRecord, preload?: PromiseRecord[]): Promise<Status> {
    return drive(this.execute(task, rootPromise, preload), { send: this.send });
  }

  /** Runs `handle` with the production driver. */
  public onMessage(msg: Message): Promise<Status | undefined> {
    return drive(this.handle(msg), { send: this.send });
  }

  /**
   * Executes an acquired task until it is done or blocked on remote promises.
   * A fiber: every request it makes and every leaf it runs is yielded to the
   * driver, so it contains no await.
   */
  public *execute(task: TaskRecord, rootPromise: PromiseRecord, preload?: PromiseRecord[]): Fiber<Status> {
    util.assert(task.state === "acquired", `expected task state to be 'acquired', got '${task.state}'`);

    // A resonate:timer promise is a durable sleep. It names no function to
    // run; it only carries a task at all because the target that gets the
    // server to *schedule* its deadline also spawns one.
    if (rootPromise.tags["resonate:timer"] === "true") {
      const collector = new TraceCollector();
      if (rootPromise.state === "pending") {
        // Not yet due: drop the task and let the deadline settle the promise,
        // which is what wakes the sleepers. Dropping means exactly that: no
        // fulfill (that would end the sleep early), no suspend (a task cannot
        // await its own promise), and no release (the server re-dispatches a
        // released task immediately, which would spin). The lease simply
        // lapses; a re-delivery before the wake is dropped again, and one
        // after it takes the settled branch below.
        this.logger.debug(
          { component: "core", taskId: task.id, timeoutAt: rootPromise.timeoutAt },
          "dropping not-yet-due timer task",
        );
        return { kind: "suspended", awaited: [], trace: collector.getTrace() };
      }
      // Settled: a durable sleep whose wake has passed. It carries no
      // function/args, so it cannot go through the computation; fulfill the
      // task with the promise's settlement. (The server normally fulfills a
      // timer's task itself when the deadline settles the promise, so this
      // only catches a delivery already in flight at that moment.)
      const status: Done = {
        kind: "done",
        id: rootPromise.id,
        state: rootPromise.state === "resolved" ? "resolved" : "rejected",
        value: rootPromise.value?.data,
        trace: collector.getTrace(),
      };
      yield* this.fulfillTask(task, rootPromise, status);
      return status;
    }

    const computation = this.createComputation(
      rootPromise.id,
      util.buildEffects(this.send, this.codec, { id: task.id, version: task.version }, preload),
    );

    try {
      const status = yield* computation.run(rootPromise);
      if (status.kind === "suspended") {
        const suspendResult = yield* this.suspendTask(task, rootPromise, status.awaited);
        if (suspendResult.continue) {
          return yield* this.execute(task, rootPromise, suspendResult.preload);
        }
      }
      if (status.kind === "done") {
        yield* this.fulfillTask(task, rootPromise, status);
      }
      return status;
    } catch (err) {
      // TODO: Some kinds of error must releaseTask, others must haltTask, figure out when to halt and when to release
      if (err instanceof ResonateError) {
        err.log(this.logger);
      } else {
        this.logger.warn(
          { component: "core", error: err instanceof Error ? err.message : String(err) },
          "executeUntilBlocked failed unexpectedly",
        );
      }
      try {
        yield* this.releaseTask(task, rootPromise);
      } catch {}
      throw err;
    }
  }

  // Extracted to allow tests to spy on computation creation.
  private createComputation(id: string, effects: Effects): Computation {
    return new Computation(
      id,
      this.clock,
      effects,
      this.retries,
      this.registry,
      this.heartbeat,
      this.dependencies,
      this.optsBuilder,
      this.logger,
    );
  }

  private *request<R extends Request>(req: R): Fiber<Extract<Response, { kind: R["kind"] }>> {
    return yield { op: "send", req };
  }

  private *releaseTask(task: TaskRecord, rootPromise: PromiseRecord): Fiber<void> {
    yield* this.request({
      kind: "task.release",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: { id: task.id, version: task.version },
    });
  }

  private *suspendTask(
    task: TaskRecord,
    rootPromise: PromiseRecord,
    awaited: string[],
  ): Fiber<{ continue: true; preload: PromiseRecord[] } | { continue: false }> {
    const res = yield* this.request({
      kind: "task.suspend",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: {
        id: task.id,
        version: task.version,
        actions: awaited.map((a) => ({
          kind: "promise.register_callback",
          head: { corrId: randomUUID(), version: util.VERSION },
          data: { awaiter: rootPromise.id, awaited: a },
        })),
      },
    });
    if (isSuccess(res)) {
      return { continue: false };
    }
    if (isRedirect(res)) {
      return { continue: true, preload: res.data.preload };
    }
    throw exceptions.SERVER_ERROR(res.data, true, {
      code: res.head.status,
      message: res.data,
    });
  }

  private *fulfillTask(task: TaskRecord, rootPromise: PromiseRecord, doneValue: Done): Fiber<void> {
    const encoded = this.codec.encode(doneValue.value);

    yield* this.request({
      kind: "task.fulfill",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: {
        id: task.id,
        version: task.version,
        action: {
          kind: "promise.settle",
          head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
          data: {
            id: doneValue.id,
            state: doneValue.state,
            value: encoded,
          },
        },
      },
    });
  }

  /** Handles one message off the network: acquires the task and executes it. */
  public *handle(msg: Message): Fiber<Status | undefined> {
    // `msg` arrives off the network: an unexpected kind is an external
    // condition (unhandled message type, corruption), not an internal
    // invariant — drop it with a warning instead of asserting (which exits).
    if (msg.kind !== "execute") {
      this.logger.warn(
        { component: "core", kind: (msg as { kind?: unknown }).kind },
        "dropping message with unexpected kind",
      );
      return undefined;
    }

    const task = msg.data.task;
    const res = yield* this.request({
      kind: "task.acquire",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": task.id },
      data: { id: task.id, version: task.version, pid: this.pid, ttl: this.ttl },
    });

    if (!isSuccess(res)) {
      throw exceptions.SERVER_ERROR(res.data, true, {
        code: res.head.status,
        message: res.data,
      });
    }

    const rootPromise = this.codec.decodePromise(res.data.promise);
    const acquiredTask = res.data.task;

    this.heartbeat.start(acquiredTask);
    try {
      return yield* this.execute(acquiredTask, rootPromise, res.data.preload);
    } finally {
      this.heartbeat.stop(acquiredTask);
    }
  }
}
