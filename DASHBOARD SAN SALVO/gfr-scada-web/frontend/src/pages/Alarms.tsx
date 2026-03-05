import { useEffect, useMemo, useState } from 'react'
import AppLayout from '../components/layout/AppLayout'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import SectionTitle from '../components/ui/SectionTitle'
import { useAlarms } from '../hooks/useAlarms'
import type { AlarmEvent } from '../types/api'
import { SITE_ROOMS } from '../constants/siteRooms'
import { canViewSite, getAuthUserFromSessionToken } from '../utils/auth'
import { legacyKeyToSiteId } from '../constants/sites'

function AlarmRow({ alarm }: { alarm: AlarmEvent }) {
  const severityClass = alarm.severity === 'critical' ? 'bg-red-600' : 'bg-amber-500'
  return (
    <div className="flex items-start justify-between rounded-md border border-slate-200 bg-white p-3">
      <div>
        <div className="text-xs text-slate-500">{new Date(alarm.ts).toLocaleString()}</div>
        <div className="mt-1 font-medium text-slate-900">{alarm.msg || alarm.message}</div>
      </div>
      <div className="text-right">
        <div className={`inline-flex rounded px-2 py-1 text-xs font-medium text-white ${severityClass}`}>
          {alarm.severity}
        </div>
        <button type="button" className="mt-2 block text-sm text-slate-600 hover:text-slate-900">
          Ack
        </button>
      </div>
    </div>
  )
}

export default function Alarms() {
  const [plant, setPlant] = useState('')
  const [date, setDate] = useState(() => new Date().toISOString().split('T')[0])
  const authUser = getAuthUserFromSessionToken()
  const allowedPlants = useMemo(() => {
    return Object.entries(SITE_ROOMS)
      .filter(([siteKey]) => {
        const siteId = legacyKeyToSiteId(siteKey)
        return siteId ? canViewSite(authUser, siteId) : false
      })
      .flatMap(([, rooms]) => rooms)
  }, [authUser])

  useEffect(() => {
    if (!plant) return
    if (allowedPlants.includes(plant)) return
    setPlant('')
  }, [plant, allowedPlants])

  const fromDate = date ? new Date(`${date}T00:00:00`) : null
  const fromStr = fromDate ? fromDate.toISOString() : undefined
  const toStr = fromDate ? new Date(fromDate.getTime() + 24 * 60 * 60 * 1000).toISOString() : undefined
  const { data: alarms, isLoading } = useAlarms(plant, fromStr, toStr)

  return (
    <AppLayout
      title="Alarms"
      subtitle="Events and active conditions by plant"
      plant={plant}
      onPlantChange={setPlant}
      selectorOptions={allowedPlants}
    >
      <div className="space-y-6">
        <SectionTitle
          title="Alarm Feed"
          subtitle="Filter by day and plant to review events"
          action={
            <div className="flex items-center gap-2">
              <label className="text-sm text-slate-600" htmlFor="alarms-date">
                Date
              </label>
              <input
                id="alarms-date"
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                className="h-9 rounded-md border border-slate-300 bg-white px-3 text-sm text-slate-700 outline-none focus:border-slate-400"
              />
            </div>
          }
        />

        <Card>
          <CardHeader>
            <CardTitle>Event List</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {isLoading ? <div className="text-sm text-slate-500">Loading alarms...</div> : null}
            {!plant ? <div className="text-sm text-slate-500">Select a plant to view alarms.</div> : null}
            {alarms && alarms.length === 0 ? (
              <div className="text-sm text-slate-500">No alarms for selected date.</div>
            ) : null}
            {alarms?.map((alarm, index) => (
              <AlarmRow key={alarm.id ?? `${alarm.code}-${alarm.ts}-${index}`} alarm={alarm} />
            ))}
          </CardContent>
        </Card>
      </div>
    </AppLayout>
  )
}
