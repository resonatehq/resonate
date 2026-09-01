/** The wire types of the `ui.*` namespace, mirroring `resonate-core/src/ui.rs`. */

/** `PromiseState` — the promise vocabulary. Never merged with `TaskState`. */
export type PromiseState =
  | 'pending'
  | 'resolved'
  | 'rejected'
  | 'rejected_canceled'
  | 'rejected_timedout';

/** `TaskState` — a different vocabulary, and deliberately not the one above. */
export type TaskState = 'pending' | 'acquired' | 'suspended' | 'halted' | 'fulfilled';

export interface PromiseValue {
  headers?: Record<string, string>;
  data?: string;
}

export interface PromiseRecord {
  id: string;
  state: PromiseState;
  param: PromiseValue;
  value: PromiseValue;
  tags: Record<string, string>;
  timeoutAt: number;
  createdAt: number;
  settledAt?: number | null;
}

export interface ExecutionItem {
  id: string;
  state: PromiseState;
  func: string | null;
  tags: Record<string, string>;
  createdAt: number;
  settledAt: number | null;
  timeoutAt: number;
}

export interface ExecutionNode {
  id: string;
  parentId: string | null;
  state: PromiseState;
  func: string | null;
  param: PromiseValue;
  value: PromiseValue;
  tags: Record<string, string>;
  createdAt: number;
  settledAt: number | null;
  timeoutAt: number;
  /** The task this promise *is*, or null when it runs inside its parent's. */
  taskId: string | null;
}

export interface ExecutionTask {
  id: string;
  state: TaskState;
  version: number;
  resumes: number;
  ttl?: number;
  pid?: string;
  createdAt: number;
  expiresAt?: number;
}

export interface ExecutionView {
  root: PromiseRecord;
  nodes: ExecutionNode[];
  tasks: ExecutionTask[];
  truncated: boolean;
}

export interface ScheduleItem {
  id: string;
  cron: string;
  promiseId: string;
  tags: Record<string, string>;
  createdAt: number;
  nextRunAt: number;
  lastRunAt: number | null;
}

export interface ListResponse<T> {
  items: T[];
  cursor?: string;
  total?: number;
}

/** The word beside the dot. Five states, five words, no invented sixth. */
export const STATE_LABEL: Record<PromiseState, string> = {
  pending: 'Pending',
  resolved: 'Resolved',
  rejected: 'Rejected',
  rejected_canceled: 'Canceled',
  rejected_timedout: 'Timed out'
};

export const TASK_STATE_LABEL: Record<TaskState, string> = {
  pending: 'Pending',
  acquired: 'Acquired',
  suspended: 'Suspended',
  halted: 'Halted',
  fulfilled: 'Fulfilled'
};

/** The status filter's options, in the order the design lists them. */
export const STATE_FILTERS: { label: string; value: PromiseState | 'all' }[] = [
  { label: 'All', value: 'all' },
  { label: 'Pending', value: 'pending' },
  { label: 'Resolved', value: 'resolved' },
  { label: 'Rejected', value: 'rejected' },
  { label: 'Timed out', value: 'rejected_timedout' },
  { label: 'Canceled', value: 'rejected_canceled' }
];

export const isSettled = (s: PromiseState) => s !== 'pending';
export const isRejected = (s: PromiseState) => s === 'rejected';
