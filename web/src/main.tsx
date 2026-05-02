import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import './App.css'

const rootEl = document.getElementById('root')
if (!rootEl) throw new Error('No #root element found')

createRoot(rootEl).render(
  <StrictMode>
    <BrowserRouter basename="/web">
      <App />
    </BrowserRouter>
  </StrictMode>,
)
