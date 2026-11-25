// The tape: every nondeterministic decision a simulation makes, in order.
//
// A run is a pure function of (setup, tape). The driver that produced the tape
// (a seed, a script, a person) does not matter: replaying the tape through the
// same setup reproduces the run step for step, including a failure.

/** One scheduling decision. Each is applied as exactly one simulation step. */
export type Action =
  /** Deliver an in-flight envelope. `to` names the worker for an anycast message. */
  | { t: "deliver"; env: number; to?: string }
  /** Lose an in-flight envelope (fault). */
  | { t: "drop"; env: number }
  /** Hold an in-flight envelope back for `ms` of simulated time (fault). */
  | { t: "delay"; env: number; ms: number }
  /** Run a due timer (setTimeout / setInterval) on its owner. */
  | { t: "fire"; timer: number }
  /** Move the simulated clock to the next deadline and let the server process its timeouts. */
  | { t: "advance"; to: number }
  /** Kill a worker: its timers, in-memory state and undelivered inbound envelopes are lost (fault). */
  | { t: "crash"; worker: string }
  /** Start a fresh incarnation of a crashed worker. */
  | { t: "restart"; worker: string };

export type ActionKind = Action["t"];

/** A value handed to user code by Math.random / ctx.math.random during a step. */
export type RandomEntry = { step: number; t: "random"; value: number };

export type DecisionEntry = { step: number } & Action;

export type TapeEntry = DecisionEntry | RandomEntry;

export type Tape = {
  format: "resonate-dst-tape/1";
  /** Seed of the driver that recorded this tape, when it was seeded. Informational only. */
  seed?: number;
  entries: TapeEntry[];
};

export function emptyTape(seed?: number): Tape {
  return seed === undefined
    ? { format: "resonate-dst-tape/1", entries: [] }
    : { format: "resonate-dst-tape/1", seed, entries: [] };
}

export function isRandomEntry(e: TapeEntry): e is RandomEntry {
  return e.t === "random";
}

/** Canonical serialization: two runs are identical iff their serialized tapes are byte-identical. */
export function serializeTape(tape: Tape): string {
  return JSON.stringify(tape);
}

export function parseTape(text: string): Tape {
  const tape = JSON.parse(text) as Tape;
  if (tape?.format !== "resonate-dst-tape/1" || !Array.isArray(tape.entries)) {
    throw new Error("not a resonate-dst tape");
  }
  return tape;
}

export function describeAction(a: Action): string {
  switch (a.t) {
    case "deliver":
      return a.to ? `deliver #${a.env} -> ${a.to}` : `deliver #${a.env}`;
    case "drop":
      return `drop #${a.env}`;
    case "delay":
      return `delay #${a.env} by ${a.ms}ms`;
    case "fire":
      return `fire timer ${a.timer}`;
    case "advance":
      return `advance clock to ${a.to}`;
    case "crash":
      return `crash ${a.worker}`;
    case "restart":
      return `restart ${a.worker}`;
  }
}
