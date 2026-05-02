import { useEffect, useState } from 'react'
import { searchPromises } from '../api'
import type { ResonatePromise } from '../types'

function formatDate(ms: number): string {
  return new Date(ms).toLocaleString()
}

function formatTimeout(ms: number): string {
  if (ms === 0) return 'no timeout'
  return new Date(ms).toLocaleString()
}

interface StatCardProps {
  label: string
  value: string | number
  variant?: 'accent' | 'success' | 'danger' | 'info'
}

function StatCard({ label, value, variant }: StatCardProps) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className={['stat-value', variant].filter(Boolean).join(' ')}>{value}</div>
    </div>
  )
}

export default function Dashboard() {
  const [promises, setPromises] = useState<ResonatePromise[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  async function load() {
    try {
      const result = await searchPromises('pending')
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

  return (
    <>
      <div className="page-header">
        <div className="page-title">Dashboard</div>
        <div className="page-subtitle">Active promises being worked on</div>
      </div>

      <div className="stat-row">
        <StatCard label="Pending" value={loading ? '—' : promises.length} variant="info" />
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
                <th>Created</th>
                <th>Timeout</th>
                <th>Active Task</th>
              </tr>
            </thead>
            <tbody>
              {promises.length === 0 ? (
                <tr>
                  <td colSpan={4}>
                    <div className="empty-state">
                      <div className="empty-state-title">No pending promises</div>
                      <div className="empty-state-body">All promises have settled — check back soon.</div>
                    </div>
                  </td>
                </tr>
              ) : (
                promises.map((p) => (
                  <tr key={p.id}>
                    <td className="cell-mono">{p.id}</td>
                    <td className="cell-secondary">{formatDate(p.createdAt)}</td>
                    <td className="cell-secondary">{formatTimeout(p.timeoutAt)}</td>
                    <td className="cell-mono">{p.tags?.['resonate:invoke'] ?? '—'}</td>
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
