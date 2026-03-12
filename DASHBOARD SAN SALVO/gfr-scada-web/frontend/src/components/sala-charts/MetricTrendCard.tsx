import type { ReactNode } from 'react'

import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { SalaMetricKey, ThresholdLine } from '../../constants/salaCharts'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card'
import Skeleton from '../ui/Skeleton'

interface MetricPoint {
  timestamp: string
  value: number | null
}

interface MetricTrendCardProps {
  metricKey: SalaMetricKey
  title: string
  icon?: ReactNode
  unit: string
  color: string
  accentClassName: string
  description: string
  data: MetricPoint[]
  thresholds: ThresholdLine[]
  loading?: boolean
  currentValue?: number | null
}

function formatCompact(value: number) {
  return new Intl.NumberFormat('it-IT', { notation: 'compact', maximumFractionDigits: 1 }).format(value)
}

function metricFractionDigits(metricKey: SalaMetricKey) {
  if (metricKey === 'cons_specifico') return 3
  return 2
}

function formatValue(value: number | null, unit: string, metricKey: SalaMetricKey) {
  if (value == null || !Number.isFinite(value)) return '--'
  return `${new Intl.NumberFormat('it-IT', { maximumFractionDigits: metricFractionDigits(metricKey) }).format(value)} ${unit}`.trim()
}

function formatAxisValue(value: number, metricKey: SalaMetricKey) {
  if (metricKey === 'cons_specifico') {
    return new Intl.NumberFormat('it-IT', { maximumFractionDigits: 3 }).format(value)
  }
  if (Math.abs(value) >= 1000) return formatCompact(value)
  return new Intl.NumberFormat('it-IT', { maximumFractionDigits: 2 }).format(value)
}

function pad2(value: number) {
  return String(value).padStart(2, '0')
}

function formatChartAxisTimestamp(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return `${pad2(date.getDate())}/${pad2(date.getMonth() + 1)} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`
}

function formatChartTooltipTimestamp(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return `${pad2(date.getDate())}/${pad2(date.getMonth() + 1)}/${date.getFullYear()} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`
}

function computeStats(data: MetricPoint[]) {
  const values = data.map((item) => item.value).filter((item): item is number => typeof item === 'number' && Number.isFinite(item))
  const current = values.length ? values[values.length - 1] : null
  const average = values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null
  return { current, average }
}

function thresholdToneClasses(color: string) {
  if (color === '#dc2626') {
    return {
      badge: 'border-rose-200 bg-rose-50 text-rose-700',
    }
  }
  if (color === '#0ea5e9') {
    return {
      badge: 'border-sky-200 bg-sky-50 text-sky-700',
    }
  }
  if (color === '#0f766e') {
    return {
      badge: 'border-emerald-200 bg-emerald-50 text-emerald-700',
    }
  }
  return {
    badge: 'border-slate-200 bg-slate-50 text-slate-700',
  }
}

function thresholdDeltaTone(metricKey: SalaMetricKey, current: number | null, threshold: ThresholdLine) {
  if (current == null || !Number.isFinite(current)) return 'text-slate-500'
  const delta = current - threshold.value
  if (metricKey === 'cons_specifico') {
    if (threshold.label === 'CS realizzabile') {
      return delta <= 0 ? 'text-emerald-700' : 'text-rose-700'
    }
    return delta <= 0 ? 'text-sky-700' : 'text-amber-700'
  }
  return delta >= 0 ? 'text-emerald-700' : 'text-rose-700'
}

function formatDelta(current: number | null, target: number, metricKey: SalaMetricKey) {
  if (current == null || !Number.isFinite(current)) return '--'
  const delta = current - target
  const sign = delta > 0 ? '+' : ''
  return `${sign}${new Intl.NumberFormat('it-IT', { maximumFractionDigits: metricFractionDigits(metricKey) }).format(delta)}`
}

export default function MetricTrendCard({
  metricKey,
  title,
  icon,
  unit,
  color,
  accentClassName,
  description,
  data,
  thresholds,
  loading = false,
  currentValue = null,
}: MetricTrendCardProps) {
  const chartData = data
    .filter((item) => item.value != null)
    .map((item) => ({ timestamp: item.timestamp, value: Number(item.value) }))
  const stats = computeStats(data)
  const displayedCurrent = currentValue ?? stats.current
  const gradientId = `gradient-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`
  const isConsumoSpecifico = metricKey === 'cons_specifico'
  const highlightedThresholds = metricKey === 'cons_specifico'
    ? thresholds.filter((threshold) => threshold.label === 'CS realizzabile' || threshold.label === 'CS ottimale')
    : []
  const chartThresholds = metricKey === 'cons_specifico'
    ? highlightedThresholds.filter((threshold) => threshold.label === 'CS realizzabile')
    : thresholds
  const currentCard = (
    <div className="h-full min-w-[9.4rem] rounded-2xl border border-white/70 bg-white/78 px-3 py-1.5 shadow-sm backdrop-blur">
      <div className="inline-flex rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">
        Attuale
      </div>
      <div className="mt-1 whitespace-nowrap text-sm font-semibold leading-tight text-slate-900">{formatValue(displayedCurrent, unit, metricKey)}</div>
      <div className="mt-1 border-t border-white/70 pt-1 text-[11px] leading-tight text-slate-500">
        Media {formatValue(stats.average, unit, metricKey)}
      </div>
    </div>
  )

  return (
    <Card className="overflow-hidden border-slate-200/80 shadow-[0_18px_45px_-28px_rgba(15,23,42,0.35)]">
      <CardHeader className={['min-h-[118px] border-b border-slate-100 bg-gradient-to-br', accentClassName].join(' ')}>
        <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <CardTitle className="inline-flex items-center gap-2 text-slate-900">
              {icon ? (
                <span className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-slate-200/80 bg-white/80 text-slate-500 shadow-sm">
                  {icon}
                </span>
              ) : null}
              <span>{title}</span>
            </CardTitle>
          </div>
          <div className={isConsumoSpecifico ? 'flex flex-wrap items-stretch gap-2 xl:max-w-[34rem] xl:flex-nowrap xl:-translate-x-2' : 'flex flex-wrap items-stretch gap-2 xl:max-w-[30rem] xl:flex-nowrap'}>
            {currentCard}
            {highlightedThresholds.map((threshold) => {
              const tone = metricKey === 'cons_specifico' && threshold.label === 'CS realizzabile' && displayedCurrent != null
                ? (displayedCurrent <= threshold.value
                    ? { badge: 'border-emerald-200 bg-emerald-50 text-emerald-700' }
                    : { badge: 'border-rose-200 bg-rose-50 text-rose-700' })
                : thresholdToneClasses(threshold.color)
              return (
                <div
                  key={threshold.label}
                  className="h-full min-w-[9.4rem] rounded-2xl border border-white/70 bg-white/78 px-3 py-1.5 shadow-sm backdrop-blur"
                >
                  <div className={['inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em]', tone.badge].join(' ')}>
                    {threshold.label}
                  </div>
                  <div className="mt-1 whitespace-nowrap text-sm font-semibold leading-tight text-slate-900">{formatValue(threshold.value, unit, metricKey)}</div>
                  {displayedCurrent != null ? (
                    <div className="mt-1 border-t border-white/70 pt-1 text-[11px] leading-tight">
                      <div className="flex items-center justify-between gap-2">
                        <span className="text-slate-500">vs attuale</span>
                        <span className={['font-semibold', thresholdDeltaTone(metricKey, displayedCurrent, threshold)].join(' ')}>
                          {formatDelta(displayedCurrent, threshold.value, metricKey)} {unit}
                        </span>
                      </div>
                    </div>
                  ) : null}
                </div>
              )
            })}
          </div>
        </div>
      </CardHeader>
      <CardContent className="p-4">
        {loading ? (
          <Skeleton className="h-[20rem] w-full" />
        ) : chartData.length === 0 ? (
          <div className="flex h-[20rem] items-center justify-center rounded-2xl border border-dashed border-slate-200 bg-slate-50 text-sm text-slate-500">
            Nessun dato disponibile per il range selezionato.
          </div>
        ) : (
          <div className="h-[20rem]">
            <ResponsiveContainer>
              <AreaChart data={chartData} margin={{ top: 18, right: 12, left: 6, bottom: 0 }}>
                <defs>
                  <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={color} stopOpacity={0.22} />
                    <stop offset="95%" stopColor={color} stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" vertical={false} />
                <XAxis
                  dataKey="timestamp"
                  tickFormatter={(value: string) => formatChartAxisTimestamp(value)}
                  minTickGap={42}
                  tick={{ fontSize: 11, fill: '#64748b' }}
                  tickMargin={10}
                />
                <YAxis
                  width={72}
                  tickFormatter={(value: number) => formatAxisValue(value, metricKey)}
                  tick={{ fontSize: 11, fill: metricKey === 'cons_specifico' ? '#475569' : '#64748b' }}
                />
                <Tooltip
                  contentStyle={{
                    borderRadius: 16,
                    border: '1px solid #e2e8f0',
                    boxShadow: '0 20px 35px -20px rgba(15,23,42,0.35)',
                  }}
                  labelFormatter={(value: string) => formatChartTooltipTimestamp(value)}
                  formatter={(value: number) => [formatValue(Number(value), unit, metricKey), title]}
                />
                {chartThresholds.map((threshold) => (
                  <ReferenceLine
                    key={`${title}-${threshold.label}`}
                    y={threshold.value}
                    stroke={threshold.color}
                    strokeWidth={metricKey === 'cons_specifico' ? 1.8 : 1.4}
                    strokeDasharray={metricKey === 'cons_specifico' ? '6 4' : '5 5'}
                    strokeOpacity={metricKey === 'cons_specifico' ? 0.9 : 0.8}
                    ifOverflow="extendDomain"
                    label={metricKey === 'cons_specifico'
                      ? undefined
                      : { value: threshold.label, position: 'insideTopRight', fill: threshold.color, fontSize: 11 }}
                  />
                ))}
                <Area
                  type="monotone"
                  dataKey="value"
                  stroke={color}
                  strokeWidth={2.4}
                  fill={`url(#${gradientId})`}
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 0, fill: color }}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
