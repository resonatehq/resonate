// Drivers decide what happens next. Three ship here:
//
//   seeded(seed, faults)  explores: every choice comes from a seeded PRNG
//   scripted(steps)       follows an explicit scenario, step by step
//   replay(tape)          reproduces a recorded run exactly
//
// All of them produce the same kind of tape, and any tape can be replayed.

import { Prng } from "./prng.js";
import { DivergenceError, type Driver, type EnvelopeView, type SimulationView } from "./simulation.js";
import type { Action } from "./tape.js";
import { isRandomEntry, type Tape } from "./tape.js";

// ---------------------------------------------------------------------------
// Seeded
// ---------------------------------------------------------------------------

export type Faults = {
  /** Probability that a step drops an in-flight envelope instead of delivering. */
  drop?: number;
  /** Probability that a step delays an in-flight envelope. */
  delay?: number;
  /** Maximum delay in ms. */
  maxDelay?: number;
  /** Probability that a step crashes a live worker. */
  crash?: number;
  /** Probability that a step restarts a crashed worker (when one is crashed). */
  restart?: number;
  /** Keep at least this many workers alive (default 1). */
  minAlive?: number;
  /** Probability of advancing the clock while other work is still enabled (default 0.02). */
  advanceEarly?: number;
};

export function seeded(seed: number, faults: Faults = {}): Driver {
  const prng = new Prng(seed);
  const { drop = 0, delay = 0, maxDelay = 5_000, crash = 0, restart = 0.2, minAlive = 1, advanceEarly = 0.02 } = faults;
  return {
    seed,
    random: () => prng.next(),
    choose(view: SimulationView): Action | undefined {
      const live = view.envelopes.filter((e) => e.readyAt <= view.now);
      const alive = view.workers.filter((w) => w.alive);
      const dead = view.workers.filter((w) => !w.alive);

      if (crash > 0 && alive.length > minAlive && prng.next() < crash) {
        return { t: "crash", worker: prng.pick(alive).name };
      }
      if (dead.length > 0 && prng.next() < restart) {
        return { t: "restart", worker: prng.pick(dead).name };
      }
      if (live.length > 0 && drop > 0 && prng.next() < drop) {
        return { t: "drop", env: prng.pick(live).id };
      }
      if (live.length > 0 && delay > 0 && prng.next() < delay) {
        return { t: "delay", env: prng.pick(live).id, ms: 1 + prng.int(maxDelay) };
      }

      const enabled = view.enabled.filter((a) => a.t !== "restart");
      const progress = enabled.filter((a) => a.t !== "advance");
      const advance = enabled.find((a) => a.t === "advance");
      if (progress.length === 0 || (advance && prng.next() < advanceEarly)) {
        return advance ?? (dead.length > 0 ? { t: "restart", worker: prng.pick(dead).name } : undefined);
      }
      return prng.pick(progress);
    },
  };
}

// ---------------------------------------------------------------------------
// Scripted
// ---------------------------------------------------------------------------

/** Selects an in-flight envelope by what it is, not by its number. */
export type Match = {
  kind?: string;
  type?: EnvelopeView["type"];
  from?: string;
  to?: string;
  /** Task or promise id; a string matches exactly, a RegExp matches a pattern. */
  subject?: string | RegExp;
};

export type ScriptStep =
  | ({ deliver: Match; to?: string } & { count?: number })
  | { drop: Match }
  | { delay: Match; ms: number }
  | { crash: string }
  | { restart: string }
  /** Fire the oldest due timer, optionally the one owned by this worker. */
  | { fire: string | true }
  /** Advance the clock to the next deadline. */
  | { advance: true }
  /** Run until nothing but a clock advance is enabled, choosing the oldest enabled action each time. */
  | { settle: true }
  /** Values for Math.random in user code, consumed in order. */
  | { random: number[] };

export type ScriptOptions = {
  /** What to do once the script runs out: stop, or keep going in FIFO order (default). */
  after?: "stop" | "fifo";
};

function matches(e: EnvelopeView, m: Match): boolean {
  if (m.kind !== undefined && e.kind !== m.kind) return false;
  if (m.type !== undefined && e.type !== m.type) return false;
  if (m.from !== undefined && e.from !== m.from) return false;
  if (m.to !== undefined && e.to !== m.to && !e.anycast) return false;
  if (m.subject !== undefined) {
    if (e.subject === undefined) return false;
    if (typeof m.subject === "string" ? e.subject !== m.subject : !m.subject.test(e.subject)) return false;
  }
  return true;
}

function describe(m: Match): string {
  return JSON.stringify({ ...m, subject: m.subject instanceof RegExp ? String(m.subject) : m.subject });
}

/** The deterministic default: oldest enabled delivery or timer first, then the clock. */
export function fifoChoice(view: SimulationView): Action | undefined {
  const progress = view.enabled.filter((a) => a.t !== "advance" && a.t !== "restart");
  if (progress.length > 0) return progress[0];
  return view.enabled.find((a) => a.t === "advance");
}

export function fifo(): Driver {
  return {
    choose: fifoChoice,
    random: () => {
      throw new DivergenceError("fifo driver has no random values; script them with { random: [...] }");
    },
  };
}

/**
 * Drive a run with an explicit scenario. Each step names what must happen next;
 * a step that cannot happen (no matching envelope, nothing to fire) fails the
 * run with a divergence, so a script is also an assertion about the protocol.
 */
export function scripted(steps: ScriptStep[], { after = "fifo" }: ScriptOptions = {}): Driver {
  const queue = [...steps];
  const randoms: number[] = [];
  let remaining = 0;

  const take = (): ScriptStep | undefined => {
    while (queue.length > 0 && "random" in queue[0]) {
      randoms.push(...(queue.shift() as { random: number[] }).random);
    }
    return queue[0];
  };

  return {
    random() {
      take();
      const v = randoms.shift();
      if (v === undefined) throw new DivergenceError("user code asked for a random value the script does not provide");
      return v;
    },
    choose(view) {
      const next = take();
      if (!next) return after === "stop" ? undefined : fifoChoice(view);
      const step = next as Exclude<ScriptStep, { random: number[] }>;

      if ("settle" in step) {
        const next = fifoChoice(view);
        if (!next || next.t === "advance") {
          queue.shift();
          return this.choose(view);
        }
        return next;
      }
      if ("advance" in step) {
        queue.shift();
        const a = view.enabled.find((x) => x.t === "advance");
        if (!a) throw new DivergenceError("script: advance, but nothing to wait for");
        return a;
      }
      if ("crash" in step) {
        queue.shift();
        return { t: "crash", worker: step.crash };
      }
      if ("restart" in step) {
        queue.shift();
        return { t: "restart", worker: step.restart };
      }
      if ("fire" in step) {
        queue.shift();
        const t = view.timers
          .filter((x) => x.due <= view.now && (step.fire === true || x.owner === step.fire))
          .sort((a, b) => a.due - b.due || a.id - b.id)[0];
        if (!t) throw new DivergenceError(`script: no due timer${step.fire === true ? "" : ` on ${step.fire}`}`);
        return { t: "fire", timer: t.id };
      }

      const match = "deliver" in step ? step.deliver : "drop" in step ? step.drop : step.delay;
      const env = view.envelopes.find((e) => e.readyAt <= view.now && matches(e, match));
      if (!env) {
        const inflight = view.envelopes.map((e) => `${e.type} ${e.kind}(${e.subject ?? ""}) ${e.from}->${e.to}`);
        throw new DivergenceError(
          `script: no in-flight envelope matches ${describe(match)}; in flight: [${inflight.join(", ")}]`,
        );
      }
      if ("drop" in step) {
        queue.shift();
        return { t: "drop", env: env.id };
      }
      if ("delay" in step) {
        queue.shift();
        return { t: "delay", env: env.id, ms: step.ms };
      }
      const count = step.count ?? 1;
      if (++remaining >= count) {
        remaining = 0;
        queue.shift();
      }
      if (env.anycast) {
        const to = step.to ?? view.workers.find((w) => w.alive)?.name;
        if (!to) throw new DivergenceError("script: no live worker for an anycast delivery");
        return { t: "deliver", env: env.id, to };
      }
      return { t: "deliver", env: env.id };
    },
  };
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

/**
 * Reproduce a recorded run. Every decision and every random value is taken from
 * the tape; anything the run asks for that the tape does not hold, in the same
 * order and at the same step, is a divergence.
 */
export function replay(tape: Tape): Driver {
  const entries = tape.entries;
  let i = 0;
  return {
    seed: tape.seed,
    choose(view) {
      if (i >= entries.length) return undefined;
      const e = entries[i];
      if (isRandomEntry(e)) {
        throw new DivergenceError(`replay: tape expects a random value at step ${e.step}, but the run moved on`);
      }
      if (e.step !== view.step + 1) {
        throw new DivergenceError(`replay: tape entry is for step ${e.step}, run is at step ${view.step + 1}`);
      }
      i++;
      const { step: _step, ...action } = e;
      return action as Action;
    },
    random(step) {
      const e = entries[i];
      if (!e || !isRandomEntry(e) || e.step !== step) {
        throw new DivergenceError(`replay: run asked for a random value at step ${step} that the tape does not have`);
      }
      i++;
      return e.value;
    },
  };
}
