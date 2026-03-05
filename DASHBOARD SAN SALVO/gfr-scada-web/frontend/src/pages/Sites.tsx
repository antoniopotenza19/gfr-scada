import { Link, Navigate, useNavigate } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import { SITE_ROOMS } from '../constants/siteRooms'
import { SITES, type SiteId } from '../constants/sites'
import { canViewSite, getAuthUserFromSessionToken } from '../utils/auth'

function SiteCard({ siteId }: { siteId: SiteId }) {
  const site = SITES.find((item) => item.id === siteId)
  if (!site) return null

  const roomCount = (SITE_ROOMS[site.legacyKey] || []).length

  return (
    <Link to={`/sites/${site.id}`} className="block">
      <Card className="overflow-hidden transition hover:-translate-y-0.5 hover:shadow-md">
        <div className="flex h-36 items-center justify-center border-b border-slate-200 bg-slate-100">
          <div className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-semibold uppercase tracking-wide text-slate-600">
            Map Placeholder
          </div>
        </div>
        <CardHeader>
          <CardTitle className="text-xl">{site.name}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1 text-sm text-slate-600">
          <div className="flex items-center justify-between">
            <span>Stato</span>
            <span className="font-medium text-slate-800">--</span>
          </div>
          <div className="flex items-center justify-between">
            <span>Ult. update</span>
            <span className="font-medium text-slate-800">--</span>
          </div>
          <div className="flex items-center justify-between">
            <span>N sale</span>
            <span className="font-medium text-slate-800">{roomCount}</span>
          </div>
          <div className="pt-2">
            <span className="inline-flex rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700">
              Apri
            </span>
          </div>
        </CardContent>
      </Card>
    </Link>
  )
}

export default function Sites() {
  const navigate = useNavigate()
  const user = getAuthUserFromSessionToken()
  const visibleSites = SITES.filter((site) => canViewSite(user, site.id))

  const logout = () => {
    sessionStorage.removeItem('gfr_token')
    navigate('/login', { replace: true })
  }

  if (user.role === 'unknown') {
    return <Navigate to="/login" replace />
  }

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
          <div>
            <div className="text-xl font-semibold text-slate-900">Impianti</div>
            <div className="text-sm text-slate-500">Seleziona il sito da monitorare</div>
          </div>
          <button
            type="button"
            onClick={logout}
            className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 hover:bg-slate-50"
          >
            Logout
          </button>
        </div>
      </header>

      <main className="mx-auto w-full max-w-6xl px-6 py-6">
        {visibleSites.length === 0 ? (
          <Card>
            <CardContent className="text-sm text-slate-600">Nessun sito disponibile per il tuo ruolo.</CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
            {visibleSites.map((site) => (
              <SiteCard key={site.id} siteId={site.id} />
            ))}
          </div>
        )}
      </main>
    </div>
  )
}
