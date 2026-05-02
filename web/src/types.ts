export type PromiseState =
  | 'pending'
  | 'resolved'
  | 'rejected'
  | 'rejected_canceled'
  | 'rejected_timedout'

export interface PromiseValue {
  headers?: Record<string, string>
  data?: unknown
}

export interface ResonatePromise {
  id: string
  state: PromiseState
  param: PromiseValue
  value: PromiseValue
  tags: Record<string, string>
  timeoutAt: number
  createdAt: number
  settledAt?: number
}

export interface SearchResult {
  cursor?: string
  promises: ResonatePromise[]
}
