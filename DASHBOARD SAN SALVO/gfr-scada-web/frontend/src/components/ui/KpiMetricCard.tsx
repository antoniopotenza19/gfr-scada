import { Card, CardContent } from './Card'

interface KpiMetricCardProps {
  label: string
  value: string
  unit?: string
  delta?: number
}

export default function KpiMetricCard({ label, value, unit, delta }: KpiMetricCardProps) {
  const deltaClass = typeof delta === 'number'
    ? delta >= 0
      ? 'text-emerald-600'
      : 'text-rose-600'
    : ''

  return (
    <Card className="border-slate-200">
      <CardContent className="p-4">
        <div className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</div>
        <div className="mt-3 flex items-end gap-2">
          <div className="text-3xl font-semibold leading-none text-slate-900">{value}</div>
          {unit ? <div className="text-sm font-medium text-slate-500">{unit}</div> : null}
        </div>
        {typeof delta === 'number' ? (
          <div className={`mt-2 text-xs font-medium ${deltaClass}`}>
            {delta >= 0 ? '+' : ''}
            {delta.toFixed(1)}%
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
