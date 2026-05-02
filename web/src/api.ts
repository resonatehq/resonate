import type { PromiseState, ResonatePromise, SearchResult } from './types'

interface Envelope<T> {
  kind: string
  head: { corrId: string; status: number; version: string }
  data: T
}

async function rpc<T>(kind: string, data: Record<string, unknown>): Promise<T> {
  const body = {
    kind,
    head: {
      corrId: crypto.randomUUID(),
      version: '2026-04-01',
    },
    data,
  }

  const response = await fetch('/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    throw new Error(`RPC error: ${response.status} ${response.statusText}`)
  }

  const envelope = (await response.json()) as Envelope<T>
  return envelope.data
}

export async function searchPromises(
  state?: PromiseState,
  limit = 50,
  cursor?: string,
): Promise<SearchResult> {
  const data: Record<string, unknown> = { limit }
  if (state !== undefined) data.state = state
  if (cursor !== undefined) data.cursor = cursor
  return rpc<SearchResult>('promise.search', data)
}

export async function settlePromise(
  id: string,
  state: 'resolved' | 'rejected',
  value?: unknown,
): Promise<ResonatePromise> {
  const data: Record<string, unknown> = {
    id,
    state,
    value: { data: value },
  }
  const result = await rpc<{ promise: ResonatePromise }>('promise.settle', data)
  return result.promise
}
