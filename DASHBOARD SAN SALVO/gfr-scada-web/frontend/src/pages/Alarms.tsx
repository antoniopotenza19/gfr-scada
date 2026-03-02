import { useState } from 'react'
import TopBar from '../components/TopBar'
import { useAlarms } from '../hooks/useAlarms'
import { AlarmEvent } from '../types/api'

export default function Alarms() {
  const [plant, setPlant] = useState('')
  const [date, setDate] = useState(() => new Date().toISOString().split('T')[0])

  // search from start of selected day to next day
  const fromDate = date ? new Date(`${date}T00:00:00`) : null
  const fromStr = fromDate ? fromDate.toISOString() : undefined
  const toStr = fromDate ? new Date(fromDate.getTime() + 24 * 60 * 60 * 1000).toISOString() : undefined
  const { data: alarms, isLoading } = useAlarms(plant, fromStr, toStr)

  const renderAlarm = (a: AlarmEvent, index: number) => (
    <div key={a.id ?? `${a.code}-${a.ts}-${index}`} className="bg-white border border-slate-200 p-3 rounded flex justify-between items-start">
      <div>
        <div className="text-sm text-slate-500">{new Date(a.ts).toLocaleString()}</div>
        <div className="font-medium">{a.msg || a.message}</div>
      </div>
      <div className="text-sm text-right">
        <div className={"px-2 py-1 rounded text-white " + (a.severity === 'critical' ? 'bg-red-600' : 'bg-amber-500')}>
          {a.severity}
        </div>
        <button className="mt-2 text-sm text-slate-600">Ack</button>
      </div>
    </div>
  )

  return (
    <div className="min-h-screen bg-slate-50">
      <TopBar plant={plant} setPlant={setPlant} date={date} setDate={setDate} />
      <main className="max-w-4xl mx-auto p-6">
        <h1 className="text-2xl font-semibold mb-4">Alarms</h1>
        {isLoading && <div>Loading...</div>}
        {!plant && <div>Please select a plant.</div>}
        {alarms && alarms.length === 0 && <div>No alarms for selected date.</div>}
        <div className="space-y-3">
          {alarms?.map(renderAlarm)}
        </div>
      </main>
    </div>
  )
}
