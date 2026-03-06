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
        <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">{label}</div>
        <div className="mt-3 flex items-end gap-2">
          <div className="text-[32px] font-bold leading-[1.2] text-slate-900">{value}</div>
          {unit ? <div className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">{unit}</div> : null}
        </div>
        {typeof delta === 'number' ? (
          <div className={`mt-2 text-[13px] font-medium leading-[1.5] tracking-[0.2px] ${deltaClass}`}>
            {delta >= 0 ? '+' : ''}
            {delta.toFixed(1)}%
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
