import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import TopBar from '../components/TopBar'
import PlantCard from '../components/PlantCard'
import TrendChart from '../components/TrendChart'
import { usePlants } from '../hooks/usePlants'
import { usePlantSummary } from '../hooks/usePlantSummary'
import { useTimeseries } from '../hooks/useTimeseries'

export default function Dashboard() {
  const [plant, setPlant] = useState('')
  const [date, setDate] = useState(() => new Date().toISOString().split('T')[0])
  const dayStart = date ? new Date(`${date}T00:00:00`) : null
  const from = dayStart ? dayStart.toISOString() : undefined
  const to = dayStart ? new Date(dayStart.getTime() + 24 * 60 * 60 * 1000).toISOString() : undefined

  const { data: plants, isLoading: loadingPlants } = usePlants()
  const plantSummary = usePlantSummary(plant, !!plant)
  const timeseriesQuery = useTimeseries({
    plant,
    signal: 'Potenza Attiva TOT',
    from,
    to,
    bucket: '1h'
  })

  const navigate = useNavigate()
  const handlePlantChange = (p: string) => {
    setPlant(p)
  }
  const handleOpenScada = (p: string) => {
    setPlant(p)
    navigate(`/scada/${p}`)
  }
  const handleDateChange = (newDate: string) => setDate(newDate)

  return (
    <div className="min-h-screen bg-slate-50">
      <TopBar plant={plant} setPlant={handlePlantChange} date={date} setDate={handleDateChange} />
      <main className="max-w-7xl mx-auto p-6">
        {loadingPlants && <div>Loading plants...</div>}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          {plants?.map((p) => (
            <PlantCard
              key={p}
              plant={p}
              summary={p === plant ? plantSummary.data : undefined}
              onClick={() => handleOpenScada(p)}
            />
          ))}
        </div>

        {plant && (
          <>
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 bg-white border border-slate-200 rounded p-4">
                <h2 className="text-lg font-medium mb-2">Time series</h2>
                <TrendChart data={timeseriesQuery.data || []} />
              </div>

              <div className="bg-white border border-slate-200 rounded p-4">
                <h2 className="text-lg font-medium mb-2">Summary</h2>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-slate-600">
                      <th>Signal</th>
                      <th>Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(plantSummary.data?.signals || {}).map(([sig, info]) => (
                      <tr key={sig}>
                        <td>{sig}</td>
                        <td>{info.value}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </main>
    </div>
  )
}
