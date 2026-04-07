import type { Clock } from "./clock.js";
import { InnerContext } from "./context.js";
import { Coroutine } from "./coroutine.js";
import exceptions from "./exceptions.js";
import type { Heartbeat } from "./heartbeat.js";
import type { Logger } from "./logger.js";
import type { PromiseRecord } from "./network/types.js";
import type { OptionsBuilder } from "./options.js";
import type { Registry } from "./registry.js";
import { Exponential, Never, type RetryPolicyConstructor } from "./retries.js";
import { type Trace, TraceCollector } from "./trace.js";

import type { Effects, Func } from "./types.js";
import * as util from "./util.js";

export type Status = Done | Suspended;

export type Done = {
  kind: "done";
  id: string;
  state: "resolved" | "rejected";
  value: any;
  trace: Trace;
};

export type Suspended = {
  kind: "suspended";
  awaited: string[];
  trace: Trace;
};

interface Data {
  func: string;
  args: any[];
  retry?: { type: string; data: any };
  version?: number;
}

export class Computation {
  private id: string;
  private clock: Clock;
  private effects: Effects;
  private retries: Map<string, RetryPolicyConstructor>;
  private registry: Registry;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private logger: Logger;
  private heartbeat: Heartbeat;
  private processing = false;

  constructor(
    id: string,
    clock: Clock,
    effects: Effects,
    retries: Map<string, RetryPolicyConstructor>,
    registry: Registry,
    heartbeat: Heartbeat,
    dependencies: Map<string, any>,
    optsBuilder: OptionsBuilder,
    logger: Logger,
  ) {
    this.id = id;
    this.clock = clock;
    this.effects = effects;
    this.retries = retries;
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.dependencies = dependencies;
    this.optsBuilder = optsBuilder;
    this.logger = logger;
  }

  public async executeUntilBlocked(rootPromise: PromiseRecord): Promise<Status> {
    if (this.processing) throw exceptions.PANIC("computation", "already processing");

    this.processing = true;
    try {
      return await this.processAcquired(rootPromise);
    } finally {
      this.processing = false;
    }
  }

  private async processAcquired(rootPromise: PromiseRecord): Promise<Status> {
    if (!isValidData(rootPromise.param?.data)) {
      throw exceptions.PANIC("computation", "invalid promise data");
    }

    const { func, args, retry, version = 1 } = rootPromise.param.data;
    const registered = this.registry.get(func, version);

    // function must be registered
    if (!registered) {
      throw exceptions.REGISTRY_FUNCTION_NOT_REGISTERED(func, version);
    }

    if (version !== 0) util.assert(version === registered.version, "versions must match");
    util.assert(func === registered.name, "names must match");

    // start heartbeat
    this.heartbeat.start();

    const retryCtor = retry ? this.retries.get(retry.type) : undefined;
    const retryPolicy = retryCtor
      ? new retryCtor(retry?.data)
      : util.isGeneratorFunction(registered.func)
        ? new Never()
        : new Exponential();

    if (retry && !retryCtor) {
      this.logger.warn(
        { component: "computation", retryType: retry.type },
        `Retry policy '${retry.type}' not found. Will ignore.`,
      );
    }

    const ctxConfig = {
      id: this.id,
      oId: rootPromise.tags["resonate:origin"] ?? this.id,
      func: registered.func.name,
      clock: this.clock,
      registry: this.registry,
      dependencies: this.dependencies,
      optsBuilder: this.optsBuilder,
      timeout: rootPromise.timeoutAt,
      version: registered.version,
      retryPolicy: retryPolicy,
    };

    if (util.isGeneratorFunction(registered.func)) {
      return this.processGenerator(registered.func, ctxConfig, args, rootPromise);
    }

    return this.processFunction(this.id, registered.func, new InnerContext(ctxConfig), args);
  }

  private async processGenerator(
    func: Func,
    ctxConfig: ConstructorParameters<typeof InnerContext>[0],
    args: any[],
    rootPromise: PromiseRecord,
  ): Promise<Status> {
    const collector = new TraceCollector();

    // If boundary promise is done, short-circuit (dedup the root)
    if (rootPromise.state !== "pending") {
      const state = rootPromise.state === "resolved" ? "resolved" : "rejected";
      collector.emit({ kind: "dedup", id: rootPromise.id, state, value: rootPromise.value });
      this.logger.debug({ component: "trace", kind: "dedup", id: rootPromise.id }, "trace: dedup");
      return {
        kind: "done",
        id: rootPromise.id,
        state,
        value: rootPromise.value,
        trace: collector.getTrace(),
      };
    }

    const ctx = new InnerContext(ctxConfig);
    const status = await Coroutine.exec(this.logger, ctx, func, args, this.effects, collector);
    const trace = collector.getTrace();

    if (status.type === "done") {
      return {
        kind: "done",
        id: this.id,
        state: status.result.kind === "value" ? "resolved" : "rejected",
        value: status.result.kind === "value" ? status.result.value : status.result.error,
        trace,
      };
    }

    // Only remote todos remain — locals are handled inline by the coroutine
    return { kind: "suspended", awaited: status.todo.remote.map((t) => t.id), trace };
  }

  private async processFunction(id: string, func: Func, ctx: InnerContext, args: any[]): Promise<Status> {
    const collector = new TraceCollector();
    collector.emit({ kind: "spawn", id });
    this.logger.debug({ component: "trace", kind: "spawn", id }, "trace: spawn");

    const result = await util.executeWithRetry(ctx, func, args, this.logger);
    const state = result.kind === "value" ? "resolved" : "rejected";
    const value = result.kind === "value" ? result.value : result.error;

    collector.emit({ kind: "return", id, state, value });
    this.logger.debug({ component: "trace", kind: "return", id }, "trace: return");

    return {
      kind: "done",
      id,
      state,
      value,
      trace: collector.getTrace(),
    };
  }
}

// Helper functions

function isValidData(data: unknown): data is Data {
  if (data === null || typeof data !== "object") return false;

  const d = data as any;

  // func must be a string
  if (typeof d.func !== "string") return false;

  // args must be an array
  if (!Array.isArray(d.args)) return false;

  // retry (if present) must be an object with string `type` and any `data`
  if (d.retry !== undefined) {
    if (d.retry === null || typeof d.retry !== "object" || typeof d.retry.type !== "string" || !("data" in d.retry)) {
      return false;
    }
  }

  // version (if present) must be a number
  if (d.version !== undefined && typeof d.version !== "number") {
    return false;
  }

  return true;
}
