import BarMetricChart from './BarMetricChart'
import { Card, CardContent, CardHeader, CardTitle } from './ui/Card'
import Skeleton from './ui/Skeleton'
import type { TimeseriesPoint } from '../types/api'

interface DashboardMonthlyTrendCardProps {
  title: string
  unit: string
  accentClassName: string
  chartColor: string
  data: TimeseriesPoint[]
  loading?: boolean
  emptyMessage: string
}

function formatCompact(value: number) {
  return new Intl.NumberFormat('it-IT', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(value)
}

function formatFull(value: number) {
  return new Intl.NumberFormat('it-IT', {
    maximumFractionDigits: 1,
  }).format(value)
}

function latestPoint(points: TimeseriesPoint[]) {
  return points.length > 0 ? points[points.length - 1] : null
}

export default function DashboardMonthlyTrendCard({
  title,
  unit,
  accentClassName,
  chartColor,
  data,
  loading = false,
  emptyMessage,
}: DashboardMonthlyTrendCardProps) {
  const latest = latestPoint(data)
  const latestValue = latest ? Number(latest.value) || 0 : 0
  const totalValue = data.reduce((sum, point) => sum + (Number(point.value) || 0), 0)

  return (
    <Card className="overflow-hidden rounded-[22px] border-slate-200/80 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
      <CardHeader className={['border-b border-slate-100 bg-gradient-to-br', accentClassName].join(' ')}>
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Trend mensile</div>
            <CardTitle className="mt-1">{title}</CardTitle>
            <div className="mt-1 text-sm text-slate-500">Aggregazione impianto da novembre fino a oggi.</div>
          </div>
          <div className="flex flex-wrap gap-2 xl:justify-end">
            <div className="min-w-[9rem] rounded-2xl border border-white/70 bg-white/80 px-3 py-2 shadow-sm backdrop-blur">
              <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Ultimo mese</div>
              <div className="mt-1 whitespace-nowrap text-base font-semibold text-slate-900">
                {latest ? `${formatFull(latestValue)} ${unit}` : '--'}
              </div>
            </div>
            <div className="min-w-[9rem] rounded-2xl border border-white/70 bg-white/80 px-3 py-2 shadow-sm backdrop-blur">
              <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Totale periodo</div>
              <div className="mt-1 whitespace-nowrap text-base font-semibold text-slate-900">
                {data.length > 0 ? `${formatCompact(totalValue)} ${unit}` : '--'}
              </div>
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="bg-[radial-gradient(circle_at_top,#f8fafc,transparent_55%)] px-5 py-5">
        {loading ? (
          <Skeleton className="h-80 w-full rounded-2xl" />
        ) : data.length > 0 ? (
          <BarMetricChart
            data={data}
            barColor={chartColor}
            xMode="month"
            height={340}
            tooltipLabel={title}
            valueFormatter={(value) => `${formatFull(value)} ${unit}`}
          />
        ) : (
          <div className="flex h-[21.25rem] items-center justify-center rounded-2xl border border-dashed border-slate-200 bg-white/70 text-sm text-slate-500">
            {emptyMessage}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
