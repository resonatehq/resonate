import { randomUUID } from "node:crypto";
import type { Clock } from "./clock.js";
import type { Codec } from "./codec.js";
import { Computation, type Done, type Status } from "./computation.js";
import exceptions, { ResonateError } from "./exceptions.js";
import type { Heartbeat } from "./heartbeat.js";
import type { Logger } from "./logger.js";
import { isRedirect, isSuccess, type Message, type PromiseRecord, type TaskRecord } from "./network/types.js";
import type { OptionsBuilder } from "./options.js";
import type { Registry } from "./registry.js";
import { Constant, Exponential, Linear, Never, type RetryPolicyConstructor } from "./retries.js";
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

  public async executeUntilBlocked(
    task: TaskRecord,
    rootPromise: PromiseRecord,
    preload?: PromiseRecord[],
  ): Promise<Status> {
    util.assert(task.state === "acquired", `expected task state to be 'acquired', got '${task.state}'`);
    const computation = this.createComputation(
      rootPromise.id,
      util.buildEffects(this.send, this.codec, { id: task.id, version: task.version }, preload),
    );

    try {
      const status = await computation.executeUntilBlocked(rootPromise);
      if (status.kind === "suspended") {
        const suspendResult = await this.suspendTask(task, rootPromise, status.awaited);
        if (suspendResult.continue) {
          return this.executeUntilBlocked(task, rootPromise, suspendResult.preload);
        }
      }
      if (status.kind === "done") {
        await this.fulfillTask(task, rootPromise, status);
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
      await this.releaseTask(task, rootPromise).catch(() => {});
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

  private async releaseTask(task: TaskRecord, rootPromise: PromiseRecord): Promise<void> {
    await this.send({
      kind: "task.release",
      head: { corrId: randomUUID(), version: util.VERSION, "resonate:origin": rootPromise.tags["resonate:origin"] },
      data: { id: task.id, version: task.version },
    });
  }

  private async suspendTask(
    task: TaskRecord,
    rootPromise: PromiseRecord,
    awaited: string[],
  ): Promise<{ continue: true; preload: PromiseRecord[] } | { continue: false }> {
    const res = await this.send({
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

  private async fulfillTask(task: TaskRecord, rootPromise: PromiseRecord, doneValue: Done): Promise<void> {
    const encoded = this.codec.encode(doneValue.value);

    await this.send({
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

  public async onMessage(msg: Message): Promise<Status> {
    util.assert(msg.kind === "execute");

    const task = msg.data.task;
    const res = await this.send({
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
    return this.executeUntilBlocked(res.data.task, rootPromise, res.data.preload);
  }
}
