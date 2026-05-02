export default function TaskDropsPlaceholder() {
  return (
    <div className="placeholder-card">
      <div className="placeholder-badge">Coming Soon</div>
      <div className="placeholder-title">Repeated Task Drops</div>
      <p className="placeholder-body">
        Track promises whose tasks have been dropped multiple times. This view requires server-side
        support that hasn't been implemented yet — once the server exposes drop-count data, it will
        appear here.
      </p>
    </div>
  )
}
