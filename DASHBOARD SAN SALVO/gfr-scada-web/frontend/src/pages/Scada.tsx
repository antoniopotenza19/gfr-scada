import { useParams } from 'react-router-dom'
import TopBar from '../components/TopBar'
import { usePlantSummary } from '../hooks/usePlantSummary'
import { useTimeseries } from '../hooks/useTimeseries'
import { useState } from 'react'
import TrendChart from '../components/TrendChart'

export default function Scada() {
  const { plant } = useParams()
  const plantName = plant || ''
  const [date] = useState(() => new Date().toISOString().split('T')[0])

  const summaryQuery = usePlantSummary(plantName, !!plantName)
  const timeseriesPressure = useTimeseries({ plant: plantName, signal: 'PT-060', bucket: '1m' })
  const timeseriesDew = useTimeseries({ plant: plantName, signal: 'AT-061', bucket: '1m' })

  return (
    <div className="min-h-screen bg-slate-50">
      <TopBar plant={plantName} setPlant={() => {}} date={date} setDate={() => {}} />
      <main className="max-w-7xl mx-auto p-6">
        <h1 className="text-2xl font-semibold mb-4">{plantName}</h1>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          <div className="bg-white border border-slate-200 rounded p-4">
            <h3 className="font-medium">Pressure</h3>
            <div className="text-3xl font-bold mt-2">
              {summaryQuery.data?.signals['PT-060']?.value ?? '--'} {summaryQuery.data?.signals['PT-060']?.unit}
            </div>
          </div>
          <div className="bg-white border border-slate-200 rounded p-4">
            <h3 className="font-medium">Dew Point</h3>
            <div className="text-3xl font-bold mt-2">
              {summaryQuery.data?.signals['AT-061']?.value ?? '--'} {summaryQuery.data?.signals['AT-061']?.unit}
            </div>
          </div>
          <div className="bg-white border border-slate-200 rounded p-4">
            <h3 className="font-medium">Last Update</h3>
            <div className="mt-2">
              {summaryQuery.data ? new Date(summaryQuery.data.last_update).toLocaleTimeString() : '--'}
            </div>
          </div>
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white border border-slate-200 rounded p-4">
            <h3 className="font-medium mb-2">Pressure trend</h3>
            <TrendChart data={timeseriesPressure.data || []} />
          </div>
          <div className="bg-white border border-slate-200 rounded p-4">
            <h3 className="font-medium mb-2">Dew Point trend</h3>
            <TrendChart data={timeseriesDew.data || []} />
          </div>
        </div>
      </main>
    </div>
  )
}
