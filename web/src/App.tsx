import { NavLink, Route, Routes } from 'react-router-dom'
import Dashboard from './views/Dashboard'
import AllPromises from './views/AllPromises'
import FailedPromises from './views/FailedPromises'
import TaskDropsPlaceholder from './views/TaskDropsPlaceholder'
import './App.css'

export default function App() {
  return (
    <div className="app">
      <nav className="nav">
        <div className="nav-inner">
          <span className="nav-brand">Resonate</span>
          <NavLink to="/" end className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            Dashboard
          </NavLink>
          <NavLink to="/all" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            All Promises
          </NavLink>
          <NavLink to="/failed" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            Failed
          </NavLink>
          <NavLink to="/task-drops" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
            Task Drops
          </NavLink>
        </div>
      </nav>
      <main>
        <div className="page">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/all" element={<AllPromises />} />
            <Route path="/failed" element={<FailedPromises />} />
            <Route path="/task-drops" element={<TaskDropsPlaceholder />} />
          </Routes>
        </div>
      </main>
    </div>
  )
}
