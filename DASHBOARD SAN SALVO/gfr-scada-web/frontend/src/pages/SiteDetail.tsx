import { Navigate, useNavigate, useParams } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import { SITE_ROOMS } from '../constants/siteRooms'
import { SITES, isSiteId } from '../constants/sites'
import { canViewSite, getAuthUserFromSessionToken } from '../utils/auth'

export default function SiteDetail() {
  const { siteId } = useParams()
  const navigate = useNavigate()
  const user = getAuthUserFromSessionToken()

  if (!siteId || !isSiteId(siteId)) {
    return <Navigate to="/sites" replace />
  }

  if (!canViewSite(user, siteId)) {
    return <Navigate to="/403" replace />
  }

  const site = SITES.find((item) => item.id === siteId)
  if (!site) {
    return <Navigate to="/sites" replace />
  }

  const rooms = SITE_ROOMS[site.legacyKey] || []

  const openDashboard = (room?: string) => {
    const params = new URLSearchParams({ site: siteId })
    if (room) params.set('room', room)
    navigate(`/dashboard?${params.toString()}`)
  }

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
          <div>
            <div className="text-xl font-semibold text-slate-900">{site.name}</div>
            <div className="text-sm text-slate-500">Seleziona una sala</div>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => navigate('/sites')}
              className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 hover:bg-slate-50"
            >
              Impianti
            </button>
            <button
              type="button"
              onClick={() => {
                sessionStorage.removeItem('gfr_token')
                navigate('/login', { replace: true })
              }}
              className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 hover:bg-slate-50"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto w-full max-w-6xl space-y-4 px-6 py-6">
        <Card>
          <CardHeader>
            <CardTitle>Sale disponibili</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <button
              type="button"
              onClick={() => openDashboard()}
              className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 hover:bg-slate-50"
            >
              Apri dashboard sito
            </button>
            <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
              {rooms.map((room) => (
                <button
                  key={room}
                  type="button"
                  onClick={() => openDashboard(room)}
                  className="rounded-md border border-slate-200 bg-white px-3 py-2 text-left text-sm text-slate-800 hover:border-slate-300 hover:bg-slate-50"
                >
                  {room}
                </button>
              ))}
            </div>
          </CardContent>
        </Card>
      </main>
    </div>
  )
}
