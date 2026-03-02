import { Routes, Route, Navigate } from 'react-router-dom'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import Scada from './pages/Scada'
import Alarms from './pages/Alarms'

function RequireAuth({ children }: { children: JSX.Element }) {
  const token = sessionStorage.getItem('gfr_token')
  if (!token) return <Navigate to="/login" replace />
  return children
}

export default function App() {
  const token = sessionStorage.getItem('gfr_token')

  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route
        path="/"
        element={token ? <Navigate to="/dashboard" /> : <Navigate to="/login" />}
      />
      <Route path="/dashboard" element={<RequireAuth><Dashboard /></RequireAuth>} />
      <Route path="/scada/:plant" element={<RequireAuth><Scada /></RequireAuth>} />
      <Route path="/alarms" element={<RequireAuth><Alarms /></RequireAuth>} />
    </Routes>
  )
}
