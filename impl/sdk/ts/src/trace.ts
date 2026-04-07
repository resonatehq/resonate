/**
 * Trace/Event model matching the spec's 9 event kinds.
 *
 * See: execute_until_blocked_inner.md, Event Model section.
 */

export type Event =
  | { kind: "run"; id: string; callee: string }
  | { kind: "rpc"; id: string; callee: string }
  | { kind: "spawn"; id: string }
  | { kind: "block"; id: string }
  | { kind: "await"; id: string; callee: string }
  | { kind: "resume"; id: string; callee: string }
  | { kind: "suspend"; id: string }
  | { kind: "return"; id: string; state: string; value: any }
  | { kind: "dedup"; id: string; state: string; value: any };

export type Trace = Event[];

/**
 * Accumulates events during a single execute_until_blocked invocation.
 * Threaded through Coroutine and Computation.
 */
export class TraceCollector {
  private events: Event[] = [];

  emit(event: Event): void {
    this.events.push(event);
  }

  getTrace(): Trace {
    return [...this.events];
  }
}

// ─── Well-Formedness Predicates ─────────────────────────────────────────

/** No two events share the same (kind: "spawn", id). */
export function uniqueSpawn(t: Trace): boolean {
  const ids = new Set<string>();
  for (const e of t) {
    if (e.kind === "spawn") {
      if (ids.has(e.id)) return false;
      ids.add(e.id);
    }
  }
  return true;
}

/** A promise has at most one of: spawn, block, dedup. */
export function exclusiveLifecycle(t: Trace): boolean {
  const spawned = new Set<string>();
  const blocked = new Set<string>();
  const deduped = new Set<string>();

  for (const e of t) {
    if (e.kind === "spawn") spawned.add(e.id);
    if (e.kind === "block") blocked.add(e.id);
    if (e.kind === "dedup") deduped.add(e.id);
  }

  for (const id of spawned) {
    if (blocked.has(id) || deduped.has(id)) return false;
  }
  for (const id of blocked) {
    if (spawned.has(id) || deduped.has(id)) return false;
  }
  for (const id of deduped) {
    if (spawned.has(id) || blocked.has(id)) return false;
  }
  return true;
}

/** For any promise p with (spawn, p), spawn p is the first event with id = p. */
export function spawnIsFirst(t: Trace): boolean {
  const firstSeen = new Map<string, string>();
  for (const e of t) {
    // Only track the id field (not callee) — the spec says "first event with id = p"
    if (!firstSeen.has(e.id)) {
      firstSeen.set(e.id, e.kind);
    }
  }

  // For every spawned id, spawn must be the first event with that id
  for (const e of t) {
    if (e.kind === "spawn") {
      if (firstSeen.get(e.id) !== "spawn") return false;
    }
  }
  return true;
}

/** For any promise p, return p or suspend p is the last event with id = p. */
export function terminalIsLast(t: Trace): boolean {
  const lastSeen = new Map<string, string>();
  for (const e of t) {
    lastSeen.set(e.id, e.kind);
  }

  // For every id that has a terminal event, check it's the last
  for (let i = 0; i < t.length; i++) {
    const e = t[i];
    if (e.kind === "return" || e.kind === "suspend") {
      // No subsequent event should have the same id
      for (let j = i + 1; j < t.length; j++) {
        if (t[j].id === e.id) return false;
      }
    }
  }
  return true;
}

/** If (block, p) then no other event has id = p. */
export function blockIsSole(t: Trace): boolean {
  const blockedIds = new Set<string>();
  for (const e of t) {
    if (e.kind === "block") blockedIds.add(e.id);
  }
  for (const e of t) {
    if (blockedIds.has(e.id) && e.kind !== "block") return false;
  }
  return true;
}

/** If (dedup, p) then no other event has id = p. */
export function dedupIsSole(t: Trace): boolean {
  const dedupedIds = new Set<string>();
  for (const e of t) {
    if (e.kind === "dedup") dedupedIds.add(e.id);
  }
  for (const e of t) {
    if (dedupedIds.has(e.id) && e.kind !== "dedup") return false;
  }
  return true;
}

/** Every spawned promise has exactly one terminal: return or suspend. */
export function uniqueTerminal(t: Trace): boolean {
  const spawned = new Set<string>();
  const terminals = new Map<string, number>();

  for (const e of t) {
    if (e.kind === "spawn") spawned.add(e.id);
    if (e.kind === "return" || e.kind === "suspend") {
      terminals.set(e.id, (terminals.get(e.id) ?? 0) + 1);
    }
  }

  for (const id of spawned) {
    if ((terminals.get(id) ?? 0) !== 1) return false;
  }
  return true;
}

/**
 * Every (await, p, q) is followed by either (resume, p, q) or (suspend, p)
 * with no intervening events for p.
 */
export function awaitThenResumeOrSuspend(t: Trace): boolean {
  for (let i = 0; i < t.length; i++) {
    const e = t[i];
    if (e.kind === "await") {
      // Find the next event with id = e.id
      let found = false;
      for (let j = i + 1; j < t.length; j++) {
        if (t[j].id === e.id) {
          const next = t[j];
          if ((next.kind === "resume" && "callee" in next && next.callee === e.callee) || next.kind === "suspend") {
            found = true;
          }
          // Whether found or not, this is the next event for p — must be resume or suspend
          if (!found) return false;
          break;
        }
      }
      // If no subsequent event for p exists, await is dangling — that's a problem
      if (!found) return false;
    }
  }
  return true;
}

/** Every (run, p, q) is followed by either (spawn, q) ... (return, q) or (dedup, q). */
export function runHasCallee(t: Trace): boolean {
  for (const e of t) {
    if (e.kind === "run") {
      const calleeId = e.callee;
      let hasSpawnOrDedup = false;
      for (const e2 of t) {
        if (e2.id === calleeId && (e2.kind === "spawn" || e2.kind === "dedup")) {
          hasSpawnOrDedup = true;
          break;
        }
      }
      if (!hasSpawnOrDedup) return false;
    }
  }
  return true;
}

/** Every (rpc, p, q) is followed by either (block, q) or (dedup, q). */
export function rpcHasCallee(t: Trace): boolean {
  for (const e of t) {
    if (e.kind === "rpc") {
      const calleeId = e.callee;
      let hasBlockOrDedup = false;
      for (const e2 of t) {
        if (e2.id === calleeId && (e2.kind === "block" || e2.kind === "dedup")) {
          hasBlockOrDedup = true;
          break;
        }
      }
      if (!hasBlockOrDedup) return false;
    }
  }
  return true;
}

/** The trace starts with exactly one spawn for the root promise. */
export function rootSpawn(t: Trace): boolean {
  if (t.length === 0) return false;
  return t[0].kind === "spawn";
}

/** Check all well-formedness predicates at once. */
export function isWellFormed(t: Trace): boolean {
  return (
    uniqueSpawn(t) &&
    exclusiveLifecycle(t) &&
    spawnIsFirst(t) &&
    terminalIsLast(t) &&
    blockIsSole(t) &&
    dedupIsSole(t) &&
    uniqueTerminal(t) &&
    awaitThenResumeOrSuspend(t) &&
    runHasCallee(t) &&
    rpcHasCallee(t) &&
    rootSpawn(t)
  );
}
