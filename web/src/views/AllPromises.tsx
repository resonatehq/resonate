import { useEffect, useState } from 'react'
import { searchPromises, settlePromise } from '../api'
import type { PromiseState, ResonatePromise } from '../types'

function formatDate(ms: number): string {
  return new Date(ms).toLocaleString()
}

function formatState(state: PromiseState): string {
  switch (state) {
    case 'pending':            return 'Pending'
    case 'resolved':           return 'Resolved'
    case 'rejected':           return 'Rejected'
    case 'rejected_timedout':  return 'Timed out'
    case 'rejected_canceled':  return 'Canceled'
    default:                   return state
  }
}

type FilterValue = PromiseState | undefined

interface Tab {
  label: string
  value: FilterValue
}

const TABS: Tab[] = [
  { label: 'All',      value: undefined },
  { label: 'Pending',  value: 'pending' },
  { label: 'Resolved', value: 'resolved' },
  { label: 'Failed',   value: 'rejected' },
]

export default function AllPromises() {
  const [promises, setPromises] = useState<ResonatePromise[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<Set<string>>(new Set())
  const [filter, setFilter] = useState<FilterValue>(undefined)

  async function load() {
    try {
      const result = await searchPromises(filter)
      setPromises(result.promises)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    setLoading(true)
    load()
    const timer = setInterval(load, 5000)
    return () => clearInterval(timer)
  }, [filter])

  async function handleRetry(id: string) {
    setBusy((prev) => new Set(prev).add(id))
    try {
      await settlePromise(id, 'rejected', { reason: 'retry' })
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy((prev) => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  async function handleCancel(id: string) {
    setBusy((prev) => new Set(prev).add(id))
    try {
      await settlePromise(id, 'rejected', { reason: 'cancel' })
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy((prev) => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  return (
    <>
      <div className="page-header">
        <div className="page-title">Promises</div>
        <div className="page-subtitle">All promises across all states</div>
      </div>

      <div className="filter-tabs">
        {TABS.map((tab) => (
          <button
            key={tab.label}
            className={`filter-tab${filter === tab.value ? ' active' : ''}`}
            onClick={() => setFilter(tab.value)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="table-card">
        {error && <div className="error-bar">Error: {error}</div>}
        {loading ? (
          <div className="status-bar">Loading…</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>State</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {promises.length === 0 ? (
                <tr>
                  <td colSpan={4}>
                    <div className="empty-state">
                      <div className="empty-state-title">No promises found</div>
                      <div className="empty-state-body">Try a different filter or check back later.</div>
                    </div>
                  </td>
                </tr>
              ) : (
                promises.map((p) => (
                  <tr key={p.id}>
                    <td className="cell-mono">{p.id}</td>
                    <td>
                      <span className={`badge badge-${p.state}`}>{formatState(p.state)}</span>
                    </td>
                    <td className="cell-secondary">{formatDate(p.createdAt)}</td>
                    <td>
                      {(p.state === 'rejected' || p.state === 'rejected_timedout') && (
                        <button
                          className="btn btn-default"
                          disabled={busy.has(p.id)}
                          onClick={() => handleRetry(p.id)}
                        >
                          Retry
                        </button>
                      )}
                      {p.state === 'pending' && (
                        <button
                          className="btn btn-danger"
                          disabled={busy.has(p.id)}
                          onClick={() => handleCancel(p.id)}
                        >
                          Cancel
                        </button>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
      </div>
    </>
  )
}
