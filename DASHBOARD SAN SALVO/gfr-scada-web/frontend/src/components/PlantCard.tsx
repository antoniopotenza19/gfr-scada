import React from 'react'
import { PlantSummary } from '../types/api'
import KpiCard from './KpiCard'

interface Props {
  plant: string
  summary?: PlantSummary
  onClick?: () => void
}

export default function PlantCard({ plant, summary, onClick }: Props) {
  const pressure = summary?.signals['PT-060']?.value ?? '--'
  const dew = summary?.signals['AT-061']?.value ?? '--'
  const updated = summary ? new Date(summary.last_update).toLocaleTimeString() : '--'

  return (
    <div
      className="bg-white border border-slate-200 rounded p-4 cursor-pointer hover:shadow"
      onClick={onClick}
    >
      <h3 className="font-semibold mb-2">{plant}</h3>
      <div className="grid grid-cols-2 gap-2">
        <div className="text-sm text-slate-500">Pressure</div>
        <div className="text-right font-semibold">
          {pressure} {pressure !== '--' ? 'barg' : ''}
        </div>
        <div className="text-sm text-slate-500">Dew Point</div>
        <div className="text-right font-semibold">
          {dew} {dew !== '--' ? '°C' : ''}
        </div>
      </div>
      <div className="mt-4 text-xs text-slate-400">Updated: {updated}</div>
    </div>
  )
}
