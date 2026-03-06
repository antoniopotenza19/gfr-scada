import { useState } from 'react'
import { Link, Navigate, useNavigate } from 'react-router-dom'
import AppLayout from '../components/layout/AppLayout'
import PlantGeoMap from '../components/PlantGeoMap'
import { Card, CardContent } from '../components/ui/Card'
import { SAN_SALVO_MAP_CENTER } from '../constants/plantMap'
import { legacyKeyToSiteId, SITES, type SiteId } from '../constants/sites'
import { canViewSite, getAuthUserFromSessionToken } from '../utils/auth'

function SiteCard({ siteId }: { siteId: SiteId }) {
  const site = SITES.find((item) => item.id === siteId)
  if (!site) return null

  return (
    <Link to={`/dashboard?site=${site.id}`} className="block">
      <Card className="overflow-hidden transition hover:-translate-y-0.5 hover:shadow-md">
        <div className="h-64 overflow-hidden border-b border-slate-200 bg-slate-100">
          <div className="h-full pointer-events-none [&_.leaflet-control-container]:hidden">
            <PlantGeoMap
              rooms={[]}
              selectedRoom=""
              markerStates={{}}
              bookmarks={{}}
              center={SAN_SALVO_MAP_CENTER}
              onSelectRoom={() => {}}
              showRecenter={false}
            />
          </div>
        </div>
        <CardContent className="py-4">
          <div className="text-lg font-semibold text-slate-900">{site.name}</div>
        </CardContent>
      </Card>
    </Link>
  )
}

export default function Sites() {
  const navigate = useNavigate()
  const user = getAuthUserFromSessionToken()
  const [selectedSite, setSelectedSite] = useState('')
  const visibleSites = SITES.filter((site) => canViewSite(user, site.id))
  const selectorOptions = visibleSites.map((site) => site.legacyKey)

  if (user.role === 'unknown') {
    return <Navigate to="/login" replace />
  }

  if (visibleSites.length === 1) {
    return <Navigate to={`/dashboard?site=${visibleSites[0].id}`} replace />
  }

  return (
    <AppLayout
      title="Impianti"
      subtitle="Seleziona il sito da monitorare"
      plant={selectedSite}
      onPlantChange={(nextSite) => {
        setSelectedSite(nextSite)
        const siteId = legacyKeyToSiteId(nextSite)
        if (siteId) navigate(`/dashboard?site=${siteId}`)
      }}
      selectorOptions={selectorOptions}
      selectorPlaceholder="Seleziona impianto"
      scadaPlant=""
    >
      <div className="mx-auto w-full max-w-6xl">
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
      </div>
    </AppLayout>
  )
}
