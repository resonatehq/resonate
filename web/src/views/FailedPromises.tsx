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

export default function FailedPromises() {
  const [promises, setPromises] = useState<ResonatePromise[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<Set<string>>(new Set())

  async function load() {
    try {
      const result = await searchPromises('rejected')
      setPromises(result.promises)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
    const timer = setInterval(load, 5000)
    return () => clearInterval(timer)
  }, [])

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
        <div className="page-title">Failed</div>
        <div className="page-subtitle">Promises in a failed or timed-out state</div>
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
                      <div className="empty-state-title">No failed promises</div>
                      <div className="empty-state-body">Everything is running clean.</div>
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
                      <button
                        className="btn btn-default"
                        disabled={busy.has(p.id)}
                        onClick={() => handleRetry(p.id)}
                      >
                        Retry
                      </button>
                      <button
                        className="btn btn-danger"
                        disabled={busy.has(p.id)}
                        onClick={() => handleCancel(p.id)}
                      >
                        Cancel
                      </button>
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
