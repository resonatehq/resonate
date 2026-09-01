import { base } from '$app/paths';
import { base64 } from './duration';
import { settings } from './settings.svelte';
import type {
  ExecutionItem,
  ExecutionView,
  ListResponse,
  PromiseRecord,
  PromiseState,
  ScheduleItem
} from './types';

/**
 * The protocol client.
 *
 * One envelope in, one envelope out — the same shape a worker speaks, over the
 * console's own route. A non-2xx is a completed exchange carrying a reason, so
 * it is a rejected promise here with the server's own words, not a generic
 * failure.
 */

const VERSION = '2026-04-01';

export class RpcError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string
  ) {
    super(message);
    this.name = 'RpcError';
  }

  /** The one a client is expected to recover from by restarting page one. */
  get isCursorMismatch() {
    return this.code === 'cursor_sort_mismatch';
  }
  get isNotFound() {
    return this.status === 404;
  }
}

let corr = 0;

function endpoint(): string {
  // Empty `serverUrl` means the server that served this page: same origin, so
  // no CORS and no configuration to get wrong.
  const root = settings.serverUrl.trim().replace(/\/+$/, '');
  return root ? `${root}${base}/rpc` : `${base}/rpc`;
}

export async function rpc<T>(kind: string, data: unknown, signal?: AbortSignal): Promise<T> {
  const head: Record<string, unknown> = { corrId: `ui-${++corr}`, version: VERSION };
  if (settings.token) head.auth = settings.token;

  let res: Response;
  try {
    res = await fetch(endpoint(), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ kind, head, data }),
      signal
    });
  } catch (e) {
    if (signal?.aborted) throw e;
    throw new RpcError(0, 'unreachable', `Cannot reach the server at ${endpoint()}`);
  }

  let body: unknown;
  try {
    body = await res.json();
  } catch {
    throw new RpcError(res.status, 'malformed', `The server answered ${res.status} with no envelope`);
  }

  const envelope = body as { head?: { status?: number }; data?: unknown };
  const status = envelope.head?.status ?? res.status;
  if (status >= 200 && status < 300) return envelope.data as T;

  // `ui.*` refusals carry { error, message }; everything else carries a string.
  const d = envelope.data;
  if (d && typeof d === 'object' && 'error' in d) {
    const { error, message } = d as { error: string; message?: string };
    throw new RpcError(status, error, message ?? error);
  }
  throw new RpcError(status, 'error', typeof d === 'string' ? d : `Request failed (${status})`);
}

// --- the four requests the console makes ------------------------------------

export interface ExecutionsQuery {
  state?: PromiseState[];
  func?: string;
  idPrefix?: string;
  createdFrom?: number;
  sort?: string;
  limit?: number;
  cursor?: string;
  countTotal?: boolean;
}

export function searchExecutions(q: ExecutionsQuery, signal?: AbortSignal) {
  return rpc<ListResponse<ExecutionItem>>('ui.executions.search', q, signal);
}

export function getExecution(id: string, signal?: AbortSignal) {
  return rpc<ExecutionView>('ui.execution.get', { id }, signal);
}

export function searchSchedules(
  q: { sort?: string; limit?: number; cursor?: string; countTotal?: boolean },
  signal?: AbortSignal
) {
  return rpc<ListResponse<ScheduleItem>>('ui.schedules.search', q, signal);
}

/**
 * Cancel an execution.
 *
 * An ordinary `promise.settle` — there is no `ui.*` request that mutates, so
 * the console's writes are the same requests a worker sends.
 */
export function cancelExecution(id: string) {
  return rpc<unknown>('promise.settle', { id, state: 'rejected_canceled' });
}

/** Is this id already taken? `null` when it is free. */
export async function getPromise(id: string): Promise<PromiseRecord | null> {
  try {
    const data = await rpc<{ promise: PromiseRecord }>('promise.get', { id });
    return data.promise;
  } catch (e) {
    if (e instanceof RpcError && e.isNotFound) return null;
    throw e;
  }
}

export interface InvokeRequest {
  id: string;
  func: string;
  /** Already parsed; whatever JSON array the operator typed. */
  args: unknown[];
  version: number;
  /** Milliseconds from now. */
  timeoutMs: number;
  /** The dispatch address, e.g. `poll://any@default`. */
  target: string;
  /** Milliseconds to hold the message before delivering it. 0 for none. */
  delayMs: number;
}

/**
 * Start a durable execution.
 *
 * A `promise.create` shaped exactly as `resonate invoke` shapes it, because it
 * is the same act: the param is a base64 JSON `{func, args, version}` — the
 * convention every SDK reads — and `resonate:target` is what makes the promise
 * a task the server will dispatch. Without that tag nothing would ever pick it
 * up, which is why the field is required rather than optional.
 *
 * `promise.create` is idempotent: an id that already exists comes back with the
 * *existing* promise and a 200. The dialog checks the id first so that reads as
 * "already taken" rather than as a silent no-op.
 */
export function invoke(req: InvokeRequest) {
  const now = Date.now();
  const param = base64(JSON.stringify({ func: req.func, args: req.args, version: req.version }));
  const tags: Record<string, string> = { 'resonate:target': req.target };
  if (req.delayMs > 0) tags['resonate:delay'] = String(now + req.delayMs);
  return rpc<{ promise: PromiseRecord }>('promise.create', {
    id: req.id,
    // The deadline covers the delay as well as the run, as the CLI does it.
    timeoutAt: now + req.timeoutMs + req.delayMs,
    param: { headers: {}, data: param },
    tags
  });
}
