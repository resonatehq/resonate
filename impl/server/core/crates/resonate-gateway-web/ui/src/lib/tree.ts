import type { ExecutionNode, ExecutionTask, ExecutionView } from './types';

/**
 * The execution tree, as rows.
 *
 * Two rules carry the whole view, and both are easy to lose:
 *
 * 1. **Rows are indented by enclosing task, not by promise depth.**
 *    `18 + taskLevel * 18 + depth * 8`. A task owns every row beneath it until
 *    the tree returns to its own depth; a `run` child executes inside its
 *    parent's task and never opens one. Indent by depth alone and membership
 *    becomes unreadable the moment a nested task block closes.
 *
 * 2. **The bar is real time**, from `createdAt` to `settledAt` — or to *now*
 *    for anything still running, which is why the span has to be recomputed as
 *    the clock moves rather than fixed when the data arrived.
 */

export interface TaskRow {
  kind: 'task';
  key: string;
  indent: number;
  task: ExecutionTask | undefined;
}

export interface PromiseRow {
  kind: 'promise';
  key: string;
  indent: number;
  node: ExecutionNode;
  /** Where the promise's own task, if it has one, sits. */
  task: ExecutionTask | undefined;
}

export type Row = TaskRow | PromiseRow;

/**
 * Flat nodes to display rows.
 *
 * Children are ordered by `createdAt` — the order they started, which is the
 * order an operator read them in the log. A node whose parent is missing (a
 * truncated tree, or a parent tag pointing outside the execution) is attached
 * to the root rather than dropped: showing it detached is better than not
 * showing it.
 */
export function rows(view: ExecutionView): Row[] {
  const byId = new Map<string, ExecutionNode>();
  for (const n of view.nodes) byId.set(n.id, n);
  const tasks = new Map<string, ExecutionTask>();
  for (const t of view.tasks) tasks.set(t.id, t);

  const children = new Map<string, ExecutionNode[]>();
  const roots: ExecutionNode[] = [];
  for (const n of view.nodes) {
    const parent = n.parentId && byId.has(n.parentId) ? n.parentId : null;
    if (parent === null || n.id === view.root.id) {
      roots.push(n);
      continue;
    }
    const list = children.get(parent);
    if (list) list.push(n);
    else children.set(parent, [n]);
  }
  const byCreated = (a: ExecutionNode, b: ExecutionNode) =>
    a.createdAt - b.createdAt || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  roots.sort(byCreated);
  for (const list of children.values()) list.sort(byCreated);

  const out: Row[] = [];
  const seen = new Set<string>();

  const walk = (node: ExecutionNode, depth: number, taskLevel: number) => {
    // A malformed parent chain must not spin forever.
    if (seen.has(node.id)) return;
    seen.add(node.id);

    let level = taskLevel;
    if (node.taskId) {
      out.push({
        kind: 'task',
        key: `task:${node.id}`,
        indent: 18 + level * 18,
        task: tasks.get(node.taskId)
      });
      level += 1;
    }
    out.push({
      kind: 'promise',
      key: node.id,
      indent: 18 + level * 18 + depth * 8,
      node,
      task: node.taskId ? tasks.get(node.taskId) : undefined
    });
    for (const child of children.get(node.id) ?? []) walk(child, depth + 1, level);
  };

  for (const r of roots) walk(r, 0, 0);
  return out;
}

export interface Span {
  from: number;
  to: number;
  /** Five labels across the track, as offsets from `from`. */
  ticks: number[];
}

/**
 * The window the bars are drawn in: the earliest start to the latest end, with
 * *now* standing in for anything still running.
 */
export function span(view: ExecutionView, now: number): Span {
  let from = view.root.createdAt;
  let to = view.root.settledAt ?? now;
  for (const n of view.nodes) {
    if (n.createdAt < from) from = n.createdAt;
    const end = n.settledAt ?? now;
    if (end > to) to = end;
  }
  // A zero-width span would divide by zero and draw nothing; one second of
  // width is the smallest honest picture of an execution that just started.
  if (to <= from) to = from + 1000;
  const total = to - from;
  return { from, to, ticks: [0, 0.25, 0.5, 0.75, 1].map((f) => Math.round(total * f)) };
}

/** A node's bar, in percent of the span. */
export function bar(node: ExecutionNode, s: Span, now: number) {
  const total = s.to - s.from;
  const start = node.createdAt - s.from;
  const end = (node.settledAt ?? now) - s.from;
  const left = (start / total) * 100;
  // A settled-on-arrival promise still needs a mark, or the row reads as empty.
  const width = Math.max(((end - start) / total) * 100, 0.7);
  return { left: Math.max(0, Math.min(100, left)), width: Math.min(width, 100 - left) };
}

/**
 * What to call a promise that carries no function name.
 *
 * Its lineage, verbatim — `:2.1` — because an identifier is never abbreviated
 * and never rewritten. The root falls back to its whole id.
 */
export function shortName(node: ExecutionNode, rootId: string): string {
  if (node.id === rootId) return node.id;
  const rest = node.id.slice(rootId.length);
  return rest || node.id;
}
