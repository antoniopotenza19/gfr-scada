import type { PlantSummary } from '../types/api'
import { Card, CardContent, CardHeader, CardTitle } from './ui/Card'

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
    <Card
      className="cursor-pointer transition-colors hover:border-slate-300"
      onClick={onClick}
    >
      <CardHeader>
        <CardTitle>{plant}</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-y-2 text-sm">
          <div className="text-slate-500">Pressure</div>
          <div className="text-right font-semibold text-slate-900">
            {pressure} {pressure !== '--' ? 'barg' : ''}
          </div>
          <div className="text-slate-500">Dew Point</div>
          <div className="text-right font-semibold text-slate-900">
            {dew} {dew !== '--' ? 'degC' : ''}
          </div>
        </div>
        <div className="mt-4 text-xs text-slate-500">Updated: {updated}</div>
      </CardContent>
    </Card>
  )
}
