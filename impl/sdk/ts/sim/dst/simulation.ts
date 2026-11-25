// A deterministic simulation runtime for Resonate workflows.
//
// Several simulated workers, each running the real SDK engine (Core), talk to
// one in-memory Resonate server (network/local.ts) through a simulated
// network. Nothing runs concurrently: the simulation advances one step at a
// time, and every step applies exactly one Action chosen by a Driver. All
// nondeterminism goes through that choice or through the tape:
//
//   scheduling and message order  -> which envelope / timer the step delivers
//   anycast routing               -> which worker receives an anycast message
//   time                          -> the clock moves only on an "advance" step
//   faults                        -> drop, delay, crash and restart are actions
//   randomness in user code       -> Math.random values are recorded on the tape
//
// Ids (corrIds, uuids) are minted from counters, so they are a function of the
// step sequence too. A run is therefore a pure function of (scenario, tape).

import { Codec } from "../../src/codec.js";
import { Core } from "../../src/core.js";
import { NoopHeartbeat } from "../../src/heartbeat.js";
import type { Logger } from "../../src/logger.js";
import { type Change, Server } from "../../src/network/local.js";
import { isMessage, type Message, type PromiseRecord, type Request, type Response } from "../../src/network/types.js";
import { OptionsBuilder } from "../../src/options.js";
import { Registry } from "../../src/registry.js";
import type { Func, Send } from "../../src/types.js";
import { VERSION } from "../../src/util.js";
import { install, quiesce } from "./sandbox.js";
import { type Action, type DecisionEntry, describeAction, emptyTape, type Tape } from "./tape.js";

// ---------------------------------------------------------------------------
// Scenario: what to simulate
// ---------------------------------------------------------------------------

export type Start = {
  /** Root promise id. */
  id: string;
  /** Registered function name. */
  func: string;
  args?: any[];
  /** Relative timeout in ms (default: one day). */
  timeout?: number;
};

export type Scenario<W = unknown> = {
  /** Worker names, or a count (named worker-1 .. worker-n). All join group "default". */
  workers: number | string[];
  /** Functions every worker registers, by name. */
  functions: Record<string, Func>;
  /** Fresh external state for one run, handed to functions as ctx.getDependency("world"). */
  world?: () => W;
  /** Workflows invoked before the first step. */
  start: Start[];
  /** Checked after every step; throw to fail the run. */
  invariant?: (world: W, sim: SimulationView) => void;
  /** Checked once every started workflow has settled; throw to fail the run. */
  check?: (world: W, results: Record<string, Settled>) => void;
  /** Task lease in ms (default 30s). */
  ttl?: number;
  /** A worker gives up on an unanswered request after this many ms (default 10s), like the HTTP transport. */
  requestTimeout?: number;
};

export type Settled = { state: PromiseRecord["state"]; value: unknown };

// ---------------------------------------------------------------------------
// Driver: who decides
// ---------------------------------------------------------------------------

export type EnvelopeView = {
  id: number;
  from: string;
  /** Worker name or "server"; for an anycast message, the group. */
  to: string;
  anycast: boolean;
  /** Request kind ("task.acquire", ...), response kind, or message kind ("execute"). */
  kind: string;
  /** "request" | "response" | "message" */
  type: "request" | "response" | "message";
  /** Task or promise id this envelope is about, when it has one. */
  subject?: string;
  readyAt: number;
};

export type TimerView = { id: number; owner: string; due: number };

export type SimulationView = {
  step: number;
  now: number;
  /** Actions that make progress, in a canonical order. */
  enabled: Action[];
  envelopes: EnvelopeView[];
  timers: TimerView[];
  workers: { name: string; alive: boolean }[];
  /** The server's view of every promise. */
  promises: () => PromiseRecord[];
};

export interface Driver {
  /** Seed recorded in the tape header when the run options do not give one. */
  readonly seed?: number;
  /** Pick the next action, or undefined to stop. Faults (drop, delay, crash) may be returned even though they are not in `enabled`. */
  choose(view: SimulationView): Action | undefined;
  /** A value in [0, 1) for Math.random in user code. */
  random(step: number): number;
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

export type Failure = {
  kind: "invariant" | "check" | "panic" | "divergence" | "liveness";
  message: string;
  step: number;
};

export type Result = {
  status: "completed" | "failed" | "stopped";
  steps: number;
  now: number;
  failure?: Failure;
  results: Record<string, Settled>;
  tape: Tape;
  /** One line per step, for humans. */
  log: string[];
};

export type RunOptions = {
  maxSteps?: number;
  /** Fail with "liveness" when maxSteps is reached before every workflow settles. */
  requireCompletion?: boolean;
  /** Recorded in the tape header. */
  seed?: number;
};

export class DivergenceError extends Error {
  name = "DivergenceError";
}

class Panic extends Error {
  name = "Panic";
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

type Envelope = {
  id: number;
  from: string;
  to: { kind: "server" } | { kind: "unicast"; worker: string } | { kind: "anycast"; group: string };
  type: "request" | "response" | "message";
  kind: string;
  subject?: string;
  body: string;
  corr?: number;
  /** For responses: the incarnation of the worker that sent the request. */
  incarnation?: number;
  readyAt: number;
};

type Timer = { id: number; owner: string; due: number; ms: number; repeat: boolean; fn: () => void };

type Pending = { resolve: (res: Response) => void; req: Request; timer: number };

class SimWorker {
  alive = true;
  incarnation = 0;
  core!: Core;
  pending = new Map<number, Pending>();
  corr = 0;

  constructor(
    readonly name: string,
    readonly group: string,
  ) {}

  get unicast(): string {
    return `sim://uni@${this.group}/${this.name}`;
  }
}

const silent: Logger = { debug() {}, info() {}, warn() {}, error() {} };

const ADDRESS = /^sim:\/\/(uni|any)@([^/]+)(?:\/(.+))?$/;

function subjectOf(kind: string, body: any): string | undefined {
  const data = body?.data;
  if (!data) return undefined;
  if (kind === "execute") return data.task?.id;
  if (kind === "unblock") return data.promise?.id;
  return data.id ?? data.action?.data?.id ?? data.promise?.id ?? data.task?.id;
}

export class Simulation<W = unknown> {
  private readonly scenario: Scenario<W>;
  private readonly server = new Server();
  private readonly registry = new Registry();
  private readonly codec = new Codec();
  private readonly world: W;
  private readonly workers: SimWorker[];
  private envelopes: Envelope[] = [];
  private timers: Timer[] = [];
  private now = 0;
  private step = 0;
  private nextEnvelope = 1;
  private nextTimer = 1;
  private nextUuid = 0;
  private current: string | undefined;
  private active = false;
  private panic: string | undefined;
  private driver!: Driver;
  private tape!: Tape;
  private log: string[] = [];

  constructor(scenario: Scenario<W>) {
    this.scenario = scenario;
    this.world = scenario.world ? scenario.world() : (undefined as W);
    const names =
      typeof scenario.workers === "number"
        ? Array.from({ length: scenario.workers }, (_, i) => `worker-${i + 1}`)
        : scenario.workers;
    this.workers = names.map((n) => new SimWorker(n, "default"));
    for (const [name, func] of Object.entries(scenario.functions)) {
      this.registry.add(func, name);
    }
  }

  async run(driver: Driver, { maxSteps = 10_000, requireCompletion = false, seed }: RunOptions = {}): Promise<Result> {
    this.driver = driver;
    this.tape = emptyTape(seed ?? driver.seed);
    const restore = install({
      active: () => this.active,
      now: () => this.now,
      random: () => this.random(),
      uuid: () => this.uuid(),
      setTimer: (fn, ms, repeat) => this.setTimer(fn, ms, repeat),
      clearTimer: (h) => this.clearTimer(h),
      exit: (code) => {
        this.panic ??= `process.exit(${code ?? ""}) called by ${this.current ?? "the simulation"}`;
        throw new Panic(this.panic);
      },
    });

    try {
      for (const w of this.workers) this.boot(w);
      for (const s of this.scenario.start) this.invoke(s);

      let failure: Failure | undefined;
      let status: Result["status"] = "stopped";
      while (true) {
        if (this.allSettled()) {
          failure = this.checkFinal();
          status = failure ? "failed" : "completed";
          break;
        }
        if (this.step >= maxSteps) {
          if (requireCompletion) {
            failure = { kind: "liveness", message: `not settled after ${maxSteps} steps`, step: this.step };
            status = "failed";
          }
          break;
        }
        const view = this.view();
        let action: Action | undefined;
        try {
          action = driver.choose(view);
        } catch (e) {
          if (e instanceof DivergenceError) {
            failure = { kind: "divergence", message: e.message, step: this.step + 1 };
            status = "failed";
            break;
          }
          throw e;
        }
        if (!action) break;
        const invalid = this.validate(action);
        if (invalid) {
          failure = { kind: "divergence", message: `${describeAction(action)}: ${invalid}`, step: this.step + 1 };
          status = "failed";
          break;
        }
        this.step++;
        this.tape.entries.push({ step: this.step, ...action } as DecisionEntry);
        this.log.push(`[${this.step}] t=${this.now} ${this.explain(action)}`);
        try {
          await this.apply(action);
        } catch (e) {
          if (e instanceof DivergenceError) {
            failure = { kind: "divergence", message: e.message, step: this.step };
            status = "failed";
            break;
          }
          throw e;
        }
        if (this.panic) {
          failure = { kind: "panic", message: this.panic, step: this.step };
          status = "failed";
          break;
        }
        if (this.scenario.invariant) {
          try {
            this.scenario.invariant(this.world, this.view());
          } catch (e) {
            failure = { kind: "invariant", message: e instanceof Error ? e.message : String(e), step: this.step };
            status = "failed";
            break;
          }
        }
      }
      if (failure) this.log.push(`[${failure.step}] FAILED (${failure.kind}): ${failure.message}`);
      return {
        status,
        steps: this.step,
        now: this.now,
        failure,
        results: this.results(),
        tape: this.tape,
        log: this.log,
      };
    } finally {
      restore();
    }
  }

  // -- hooks -----------------------------------------------------------------

  private random(): number {
    const value = this.driver.random(this.step);
    if (!(value >= 0 && value < 1)) throw new DivergenceError(`random value ${value} is not in [0, 1)`);
    this.tape.entries.push({ step: this.step, t: "random", value });
    return value;
  }

  private uuid(): string {
    const n = (this.nextUuid++).toString(16).padStart(12, "0");
    return `00000000-0000-4000-8000-${n}`;
  }

  private setTimer(fn: () => void, ms: number, repeat: boolean): object {
    const id = this.nextTimer++;
    const delay = Math.max(0, Math.floor(ms));
    this.timers.push({ id, owner: this.current ?? "sim", due: this.now + delay, ms: delay, repeat, fn });
    // A plain object handle: callers only pass it back to clearTimeout.
    return { dstTimer: id, unref() {}, ref() {}, hasRef: () => false };
  }

  private clearTimer(handle: unknown): boolean {
    const id = (handle as any)?.dstTimer;
    if (typeof id !== "number") return false;
    this.timers = this.timers.filter((t) => t.id !== id);
    return true;
  }

  // -- setup -----------------------------------------------------------------

  private boot(w: SimWorker): void {
    w.incarnation++;
    w.pending = new Map();
    const incarnation = w.incarnation;
    const send = ((req: Request) => this.send(w, incarnation, req)) as Send;
    const deps = new Map<string, any>([["world", this.world]]);
    w.core = new Core({
      pid: w.name,
      ttl: this.scenario.ttl ?? 30_000,
      clock: { now: () => this.now },
      send,
      codec: this.codec,
      registry: this.registry,
      heartbeat: new NoopHeartbeat(),
      dependencies: deps,
      optsBuilder: new OptionsBuilder({ match: (target: string) => `sim://any@${target}` }),
      logger: silent,
    });
  }

  private invoke(s: Start): void {
    const res = this.server.apply(this.now, {
      kind: "promise.create",
      head: { corrId: `start-${s.id}`, version: VERSION },
      data: {
        id: s.id,
        timeoutAt: this.now + (s.timeout ?? 24 * 60 * 60 * 1000),
        param: this.codec.encode({ func: s.func, args: s.args ?? [], version: 1 }),
        tags: {
          "resonate:target": "sim://any@default",
          "resonate:origin": s.id,
          "resonate:branch": s.id,
          "resonate:parent": s.id,
          "resonate:scope": "global",
        },
      },
    });
    if (res.response.head.status >= 300) {
      throw new Error(`could not start ${s.id}: ${JSON.stringify(res.response.data)}`);
    }
    this.route(res.changes);
  }

  // -- network ---------------------------------------------------------------

  private send(w: SimWorker, incarnation: number, req: Request): Promise<Response> {
    if (!w.alive || w.incarnation !== incarnation) {
      // A dead incarnation's leftover code: never answered.
      return new Promise(() => {});
    }
    const corr = ++w.corr;
    this.envelopes.push({
      id: this.nextEnvelope++,
      from: w.name,
      to: { kind: "server" },
      type: "request",
      kind: req.kind,
      subject: subjectOf(req.kind, req),
      body: JSON.stringify(req),
      corr,
      incarnation,
      readyAt: this.now,
    });
    return new Promise<Response>((resolve) => {
      const prev = this.current;
      this.current = w.name;
      const timer = this.setTimer(
        () => {
          const p = w.pending.get(corr);
          if (!p) return;
          w.pending.delete(corr);
          p.resolve({
            kind: req.kind,
            head: { corrId: req.head.corrId, status: 500, version: req.head.version },
            data: "request timed out",
          } as Response);
        },
        this.scenario.requestTimeout ?? 10_000,
        false,
      ) as any;
      this.current = prev;
      w.pending.set(corr, { resolve, req, timer: timer.dstTimer });
    });
  }

  private route(changes: Change[]): void {
    for (const c of changes) {
      if (c.kind !== "message.send") continue;
      const m = ADDRESS.exec(c.address);
      if (!m) throw new Error(`unroutable address ${c.address}`);
      const [, mode, group, name] = m;
      this.envelopes.push({
        id: this.nextEnvelope++,
        from: "server",
        to: mode === "uni" ? { kind: "unicast", worker: name } : { kind: "anycast", group },
        type: "message",
        kind: c.message.kind,
        subject: subjectOf(c.message.kind, c.message),
        body: JSON.stringify(c.message),
        readyAt: this.now,
      });
    }
  }

  // -- the step --------------------------------------------------------------

  private worker(name: string | undefined): SimWorker | undefined {
    return this.workers.find((w) => w.name === name);
  }

  private candidates(e: Envelope): SimWorker[] {
    if (e.to.kind === "anycast") {
      const group = e.to.group;
      return this.workers.filter((w) => w.alive && w.group === group);
    }
    if (e.to.kind === "unicast") {
      const w = this.worker(e.to.worker);
      return w?.alive ? [w] : [];
    }
    return [];
  }

  private nextDeadline(): number | undefined {
    let best: number | undefined;
    const consider = (t: number) => {
      if (t > this.now && (best === undefined || t < best)) best = t;
    };
    for (const t of this.timers) if (this.ownerAlive(t.owner)) consider(t.due);
    for (const e of this.envelopes) consider(e.readyAt);
    for (const pt of this.server.pTimeouts) {
      if (this.server.promises.get(pt.id)?.state === "pending") consider(pt.timeout);
    }
    for (const tt of this.server.tTimeouts) consider(tt.timeout);
    for (const st of this.server.sTimeouts) consider(st.timeout);
    return best;
  }

  private ownerAlive(owner: string): boolean {
    const w = this.worker(owner);
    return w ? w.alive : true;
  }

  private enabled(): Action[] {
    const out: Action[] = [];
    for (const e of this.envelopes) {
      if (e.readyAt > this.now) continue;
      if (e.to.kind === "server") {
        out.push({ t: "deliver", env: e.id });
      } else if (e.to.kind === "anycast") {
        for (const w of this.candidates(e)) out.push({ t: "deliver", env: e.id, to: w.name });
      } else {
        out.push({ t: "deliver", env: e.id });
      }
    }
    const due = this.timers
      .filter((t) => t.due <= this.now && this.ownerAlive(t.owner))
      .sort((a, b) => a.due - b.due || a.id - b.id);
    for (const t of due) out.push({ t: "fire", timer: t.id });
    const next = this.nextDeadline();
    if (next !== undefined) out.push({ t: "advance", to: next });
    for (const w of this.workers) if (!w.alive) out.push({ t: "restart", worker: w.name });
    return out;
  }

  private validate(a: Action): string | undefined {
    switch (a.t) {
      case "deliver": {
        const e = this.envelopes.find((x) => x.id === a.env);
        if (!e) return "no such envelope in flight";
        if (e.readyAt > this.now) return "envelope is delayed";
        if (e.to.kind === "anycast") {
          if (!a.to) return "anycast delivery must name a worker";
          if (!this.candidates(e).some((w) => w.name === a.to)) return `${a.to} cannot receive it`;
        } else if (a.to) {
          return "only anycast deliveries name a worker";
        }
        return undefined;
      }
      case "drop":
      case "delay": {
        if (!this.envelopes.some((x) => x.id === a.env)) return "no such envelope in flight";
        if (a.t === "delay" && !(Number.isSafeInteger(a.ms) && a.ms > 0)) return "delay must be a positive integer";
        return undefined;
      }
      case "fire": {
        const t = this.timers.find((x) => x.id === a.timer);
        if (!t) return "no such timer";
        if (t.due > this.now) return "timer is not due";
        if (!this.ownerAlive(t.owner)) return "timer owner is crashed";
        return undefined;
      }
      case "advance": {
        const next = this.nextDeadline();
        if (next === undefined) return "nothing to wait for";
        if (a.to !== next) return `next deadline is ${next}`;
        return undefined;
      }
      case "crash": {
        const w = this.worker(a.worker);
        if (!w) return "no such worker";
        if (!w.alive) return "already crashed";
        return undefined;
      }
      case "restart": {
        const w = this.worker(a.worker);
        if (!w) return "no such worker";
        if (w.alive) return "not crashed";
        return undefined;
      }
    }
  }

  private async apply(a: Action): Promise<void> {
    switch (a.t) {
      case "deliver": {
        const e = this.envelopes.find((x) => x.id === a.env)!;
        this.envelopes = this.envelopes.filter((x) => x !== e);
        if (e.to.kind === "server") {
          this.deliverToServer(e);
          return;
        }
        const w = e.to.kind === "anycast" ? this.worker(a.to)! : this.worker(e.to.worker)!;
        await this.inside(w.name, () => this.deliverToWorker(w, e));
        return;
      }
      case "drop": {
        this.envelopes = this.envelopes.filter((x) => x.id !== a.env);
        return;
      }
      case "delay": {
        const e = this.envelopes.find((x) => x.id === a.env)!;
        e.readyAt = this.now + a.ms;
        return;
      }
      case "fire": {
        const t = this.timers.find((x) => x.id === a.timer)!;
        if (t.repeat) {
          t.due = this.now + Math.max(1, t.ms);
        } else {
          this.timers = this.timers.filter((x) => x !== t);
        }
        await this.inside(t.owner === "sim" ? undefined : t.owner, t.fn);
        return;
      }
      case "advance": {
        this.now = a.to;
        const res = this.server.apply(this.now, {
          kind: "debug.tick",
          head: { corrId: `tick-${this.step}`, version: VERSION },
          data: { time: this.now },
        });
        this.route(res.changes);
        return;
      }
      case "crash": {
        const w = this.worker(a.worker)!;
        w.alive = false;
        w.pending = new Map();
        this.timers = this.timers.filter((t) => t.owner !== w.name);
        // In-flight envelopes addressed to it die with it; anycast ones may still reach another worker.
        this.envelopes = this.envelopes.filter(
          (e) =>
            !(
              (e.to.kind === "unicast" && e.to.worker === w.name) ||
              (e.type === "response" && e.to.kind === "unicast" && e.to.worker === w.name)
            ),
        );
        return;
      }
      case "restart": {
        const w = this.worker(a.worker)!;
        w.alive = true;
        await this.inside(w.name, () => this.boot(w));
        return;
      }
    }
  }

  /** Run `fn` as one step on behalf of `owner`, then drain every microtask it caused. */
  private async inside(owner: string | undefined, fn: () => void): Promise<void> {
    this.current = owner;
    this.active = true;
    try {
      try {
        fn();
      } catch (e) {
        if (!(e instanceof Panic)) throw e;
      }
      await quiesce();
      await quiesce();
    } finally {
      this.active = false;
      this.current = undefined;
    }
  }

  private deliverToServer(e: Envelope): void {
    const req = JSON.parse(e.body) as Request;
    const result = this.server.apply(this.now, req);
    const response = {
      kind: result.response.kind,
      head: { corrId: req.head.corrId, status: result.response.head.status, version: req.head.version },
      data: result.response.data,
    };
    this.envelopes.push({
      id: this.nextEnvelope++,
      from: "server",
      to: { kind: "unicast", worker: e.from },
      type: "response",
      kind: response.kind,
      subject: e.subject,
      body: JSON.stringify(response),
      corr: e.corr,
      incarnation: e.incarnation,
      readyAt: this.now,
    });
    this.route(result.changes);
  }

  private deliverToWorker(w: SimWorker, e: Envelope): void {
    if (e.type === "response") {
      if (e.incarnation !== w.incarnation) return;
      const p = w.pending.get(e.corr!);
      if (!p) return; // already timed out
      w.pending.delete(e.corr!);
      this.timers = this.timers.filter((t) => t.id !== p.timer);
      p.resolve(JSON.parse(e.body));
      return;
    }
    const msg = JSON.parse(e.body) as Message;
    if (!isMessage(msg) || msg.kind !== "execute") return;
    w.core.onMessage(msg).catch(() => {});
  }

  // -- observation -----------------------------------------------------------

  private explain(a: Action): string {
    if (a.t === "deliver" || a.t === "drop" || a.t === "delay") {
      const e = this.envelopes.find((x) => x.id === a.env);
      if (e) {
        const to =
          a.t === "deliver" && a.to
            ? a.to
            : e.to.kind === "server"
              ? "server"
              : e.to.kind === "unicast"
                ? e.to.worker
                : `any@${e.to.group}`;
        return `${describeAction(a)}: ${e.type} ${e.kind}${e.subject ? `(${e.subject})` : ""} ${e.from} -> ${to}`;
      }
    }
    if (a.t === "fire") {
      const t = this.timers.find((x) => x.id === a.timer);
      if (t) return `${describeAction(a)} on ${t.owner} (due ${t.due})`;
    }
    return describeAction(a);
  }

  view(): SimulationView {
    return {
      step: this.step,
      now: this.now,
      enabled: this.enabled(),
      envelopes: this.envelopes.map((e) => ({
        id: e.id,
        from: e.from,
        to: e.to.kind === "server" ? "server" : e.to.kind === "unicast" ? e.to.worker : e.to.group,
        anycast: e.to.kind === "anycast",
        kind: e.kind,
        type: e.type,
        subject: e.subject,
        readyAt: e.readyAt,
      })),
      timers: this.timers.map((t) => ({ id: t.id, owner: t.owner, due: t.due })),
      workers: this.workers.map((w) => ({ name: w.name, alive: w.alive })),
      promises: () => [...this.server.promises.values()].map((p) => ({ ...p }) as any),
    };
  }

  private allSettled(): boolean {
    return this.scenario.start.every((s) => {
      const p = this.server.promises.get(s.id);
      return p !== undefined && p.state !== "pending";
    });
  }

  private results(): Record<string, Settled> {
    const out: Record<string, Settled> = {};
    for (const s of this.scenario.start) {
      const p = this.server.promises.get(s.id);
      if (!p) continue;
      let value: unknown;
      try {
        value = this.codec.decode(p.value as any);
      } catch {
        value = p.value;
      }
      out[s.id] = { state: p.state, value };
    }
    return out;
  }

  private checkFinal(): Failure | undefined {
    if (!this.scenario.check) return undefined;
    try {
      this.scenario.check(this.world, this.results());
      return undefined;
    } catch (e) {
      return { kind: "check", message: e instanceof Error ? e.message : String(e), step: this.step };
    }
  }
}

/** Run a scenario once with a driver. Every run builds a fresh world, server and workers. */
export function simulate<W>(scenario: Scenario<W>, driver: Driver, options?: RunOptions): Promise<Result> {
  return new Simulation(scenario).run(driver, options);
}
