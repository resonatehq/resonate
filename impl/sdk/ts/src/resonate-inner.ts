import type { Clock } from "./clock";
import { Computation, type Status } from "./computation";
import type { Handler } from "./handler";
import type { Heartbeat } from "./heartbeat";
import type { DurablePromiseRecord, Message, MessageSource, Network, TaskRecord } from "./network/network";
import type { OptionsBuilder } from "./options";
import type { Registry } from "./registry";
import { Constant, Exponential, Linear, Never, type RetryPolicyConstructor } from "./retries";
import type { Span, Tracer } from "./tracer";
import type { Callback } from "./types";
import * as util from "./util";

export type PromiseHandler = {
  addEventListener: (event: "created" | "completed", callback: (p: DurablePromiseRecord) => void) => void;
  subscribe: () => Promise<void>;
};

export type Task = ClaimedTask | UnclaimedTask;

export type ClaimedTask = {
  kind: "claimed";
  task: TaskRecord;
  rootPromise: DurablePromiseRecord<any>;
  leafPromise?: DurablePromiseRecord<any>;
};

export type UnclaimedTask = {
  kind: "unclaimed";
  task: TaskRecord;
};

export class ResonateInner {
  private unicast: string;
  private anycast: string;
  private pid: string;
  private ttl: number;
  private clock: Clock;
  private network: Network;
  private handler: Handler;
  private tracer: Tracer;
  private retries: Map<string, RetryPolicyConstructor>;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private dependencies: Map<string, any>;
  private optsBuilder: OptionsBuilder;
  private verbose: boolean;
  private computations: Map<string, Computation> = new Map();

  constructor({
    unicast,
    anycast,
    pid,
    ttl,
    clock,
    network,
    handler,
    tracer,
    registry,
    heartbeat,
    dependencies,
    optsBuilder,
    verbose,
    messageSource = undefined,
  }: {
    unicast: string;
    anycast: string;
    pid: string;
    ttl: number;
    clock: Clock;
    network: Network;
    handler: Handler;
    tracer: Tracer;
    registry: Registry;
    heartbeat: Heartbeat;
    dependencies: Map<string, any>;
    optsBuilder: OptionsBuilder;
    verbose: boolean;
    messageSource?: MessageSource;
  }) {
    this.unicast = unicast;
    this.anycast = anycast;
    this.pid = pid;
    this.ttl = ttl;
    this.clock = clock;
    this.network = network;
    this.handler = handler;
    this.tracer = tracer;
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.dependencies = dependencies;
    this.optsBuilder = optsBuilder;
    this.verbose = verbose;

    // default retry policies
    this.retries = new Map<string, RetryPolicyConstructor>([
      [Constant.type, Constant],
      [Exponential.type, Exponential],
      [Linear.type, Linear],
      [Never.type, Never],
    ]);

    // subscribe to invoke and resume
    messageSource?.subscribe("invoke", this.onMessage.bind(this));
    messageSource?.subscribe("resume", this.onMessage.bind(this));
  }

  public process(span: Span, task: Task, done: Callback<Status>) {
    let computation = this.computations.get(task.task.rootPromiseId);
    if (!computation) {
      computation = new Computation(
        task.task.rootPromiseId,
        this.unicast,
        this.anycast,
        this.pid,
        this.ttl,
        this.clock,
        this.network,
        this.handler,
        this.retries,
        this.registry,
        this.heartbeat,
        this.dependencies,
        this.optsBuilder,
        this.verbose,
        this.tracer,
        span,
      );
      this.computations.set(task.task.rootPromiseId, computation);
    }

    computation.process(task, done);
  }

  private onMessage(msg: Message): void {
    util.assert(msg.type === "invoke" || msg.type === "resume");

    if (msg.type === "invoke" || msg.type === "resume") {
      this.process(this.tracer.decode(msg.headers), { kind: "unclaimed", task: msg.task }, () => {});
    }
  }
}
