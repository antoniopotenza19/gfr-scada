import { Link, useNavigate } from 'react-router-dom'
import { usePlants } from '../hooks/usePlants'
import { useEffect } from 'react'

interface TopBarProps {
  plant: string
  setPlant: (plant: string) => void
  date: string
  setDate: (date: string) => void
}

export default function TopBar({ plant, setPlant, date, setDate }: TopBarProps) {
  const navigate = useNavigate()
  const { data: plants, isLoading } = usePlants()

  // if current plant disappears from list, clear it
  useEffect(() => {
    if (plants && plant && !plants.includes(plant)) {
      setPlant('')
    }
  }, [plants, plant, setPlant])

  const handleLogout = () => {
    sessionStorage.removeItem('gfr_token')
    navigate('/login')
  }

  return (
    <header className="bg-white border-b border-slate-200">
      <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="h-8 w-8 bg-slate-200 rounded" />
          <div>
            <div className="text-lg font-semibold">GFR SCADA</div>
            <div className="text-xs text-slate-500">Plant: {plant || '—'}</div>
          </div>
        </div>

        <nav className="flex items-center gap-4">
          <Link to="/dashboard" className="text-sm text-slate-700 hover:underline">
            Dashboard
          </Link>
          {plant ? (
            <Link to={`/scada/${plant}`} className="text-sm text-slate-700 hover:underline">
              SCADA
            </Link>
          ) : (
            <span className="text-sm text-slate-400 cursor-not-allowed">SCADA</span>
          )}
          <Link to="/alarms" className="text-sm text-slate-700 hover:underline">
            Alarms
          </Link>
        </nav>

        <div className="flex items-center gap-3">
          <select
            value={plant}
            onChange={e => setPlant(e.target.value)}
            className="border rounded px-2 py-1"
            disabled={isLoading}
          >
            <option value="">Select</option>
            {plants?.map(p => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
          <input
            type="date"
            value={date}
            onChange={e => setDate(e.target.value)}
            className="border rounded px-2 py-1 text-sm"
          />
          <button
            onClick={handleLogout}
            className="text-sm text-red-600 hover:underline"
          >
            Logout
          </button>
        </div>
      </div>
    </header>
  )
}
