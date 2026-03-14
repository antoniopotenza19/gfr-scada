import { useRef, useState, type ReactNode } from 'react'

import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { SalaMetricKey, ThresholdLine } from '../../constants/salaCharts'
import { exportElementAsPng } from '../../utils/exportImage'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card'
import Skeleton from '../ui/Skeleton'

interface MetricPoint {
  timestamp: string
  value: number | null
  secondaryValue?: number | null
}

interface MetricTrendCardProps {
  metricKey: SalaMetricKey
  title: string
  primarySeriesLabel?: string
  secondaryCurrentValue?: number | null
  icon?: ReactNode
  unit: string
  color: string
  accentClassName: string
  description: string
  data: MetricPoint[]
  thresholds: ThresholdLine[]
  rangeStartLabel?: string
  rangeEndLabel?: string
  yAxisDomain?: readonly [number | string, number | string]
  loading?: boolean
  currentValue?: number | null
  secondarySeriesLabel?: string
  secondaryColor?: string
  onExpand?: () => void
  onExportImage?: () => void
  onExportData?: () => void
  exportFileName?: string
  expandable?: boolean
  expanded?: boolean
}

function ExpandIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M8 4H4v4M16 4h4v4M8 20H4v-4M20 16v4h-4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function DownloadIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M12 4v10m0 0 4-4m-4 4-4-4M5 18h14" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function ImageIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M4 6.5A2.5 2.5 0 0 1 6.5 4h11A2.5 2.5 0 0 1 20 6.5v11a2.5 2.5 0 0 1-2.5 2.5h-11A2.5 2.5 0 0 1 4 17.5v-11Z" fill="none" stroke="currentColor" strokeWidth="1.8" />
      <path d="m7 16 3.2-3.2a1 1 0 0 1 1.4 0L14 15l1.6-1.6a1 1 0 0 1 1.4 0L19 15.5M9 9.5h.01" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function CalendarRangeIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M7 3.5v2M17 3.5v2M4 8h16M6 5.5h12A2 2 0 0 1 20 7.5v10A2 2 0 0 1 18 19.5H6A2 2 0 0 1 4 17.5v-10a2 2 0 0 1 2-2Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  )
}

function RangeArrowIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M5 12h12m0 0-4-4m4 4-4 4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function formatCsvNumber(value: number | null) {
  if (value == null || !Number.isFinite(value)) return ''
  return String(value)
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

function computeYAxisDomain(metricKey: SalaMetricKey, data: MetricPoint[], thresholds: ThresholdLine[]) {
  const values = [
    ...data.map((item) => item.value),
    ...data.map((item) => item.secondaryValue ?? null),
    ...thresholds.map((threshold) => threshold.value),
  ].filter((item): item is number => typeof item === 'number' && Number.isFinite(item))

  if (!values.length) return ['auto', 'auto'] as const

  const min = Math.min(...values)
  const max = Math.max(...values)

  const paddingRatio = metricKey === 'pressione' || metricKey === 'pressione2' ? 0.14 : 0.08

  if (min === max) {
    const fallbackPadding = Math.abs(max) > 0 ? Math.abs(max) * 0.1 : 1
    const lower = metricKey === 'dewpoint' ? min - fallbackPadding : Math.max(0, min - fallbackPadding)
    return [lower, max + fallbackPadding] as const
  }

  const span = max - min
  const padding = span * paddingRatio
  const lower = metricKey === 'dewpoint' ? min - padding : Math.max(0, min - padding)
  const upper = max + padding
  return [lower, upper] as const
}

function computeSingleSeriesYAxisDomain(metricKey: SalaMetricKey, data: MetricPoint[], key: 'value' | 'secondaryValue', thresholds: ThresholdLine[]) {
  const values = [
    ...data.map((item) => item[key]),
    ...thresholds.map((threshold) => threshold.value),
  ].filter((item): item is number => typeof item === 'number' && Number.isFinite(item))

  if (!values.length) return ['auto', 'auto'] as const

  const min = Math.min(...values)
  const max = Math.max(...values)

  const paddingRatio = metricKey === 'pressione' || metricKey === 'pressione2' ? 0.14 : 0.08

  if (min === max) {
    const fallbackPadding = Math.abs(max) > 0 ? Math.abs(max) * 0.1 : 1
    const lower = metricKey === 'dewpoint' ? min - fallbackPadding : Math.max(0, min - fallbackPadding)
    return [lower, max + fallbackPadding] as const
  }

  const span = max - min
  const padding = span * paddingRatio
  const lower = metricKey === 'dewpoint' ? min - padding : Math.max(0, min - padding)
  const upper = max + padding
  return [lower, upper] as const
}

function computeSeriesStats(data: MetricPoint[], key: 'value' | 'secondaryValue') {
  const values = data.map((item) => item[key]).filter((item): item is number => typeof item === 'number' && Number.isFinite(item))
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

function seriesToneClasses(color: string) {
  if (color === '#0f766e') {
    return {
      card: 'border-emerald-200 bg-emerald-50/80',
      badge: 'border-emerald-200 bg-white text-emerald-700',
      value: 'text-emerald-950',
      detail: 'text-emerald-700/80',
    }
  }
  if (color === '#f97316') {
    return {
      card: 'border-orange-200 bg-orange-50/80',
      badge: 'border-orange-200 bg-white text-orange-700',
      value: 'text-orange-950',
      detail: 'text-orange-700/80',
    }
  }
  if (color === '#0284c7') {
    return {
      card: 'border-sky-200 bg-sky-50/80',
      badge: 'border-sky-200 bg-white text-sky-700',
      value: 'text-sky-950',
      detail: 'text-sky-700/80',
    }
  }
  return {
    card: 'border-slate-200 bg-slate-50/80',
    badge: 'border-slate-200 bg-white text-slate-700',
    value: 'text-slate-900',
    detail: 'text-slate-500',
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

function renderMetricTitle(title: string) {
  if (title === 'Consumo Specifico') {
    return (
      <span className="leading-tight">
        <span className="block">Consumo</span>
        <span className="block">Specifico</span>
      </span>
    )
  }
  return <span>{title}</span>
}

export default function MetricTrendCard({
  metricKey,
  title,
  primarySeriesLabel,
  secondaryCurrentValue = null,
  icon,
  unit,
  color,
  accentClassName,
  description,
  data,
  thresholds,
  rangeStartLabel,
  rangeEndLabel,
  yAxisDomain,
  loading = false,
  currentValue = null,
  secondarySeriesLabel,
  secondaryColor = '#f97316',
  onExpand,
  onExportImage,
  onExportData,
  exportFileName,
  expandable = true,
  expanded = false,
}: MetricTrendCardProps) {
  const cardRef = useRef<HTMLDivElement | null>(null)
  const [showRangeBadgeForExport, setShowRangeBadgeForExport] = useState(false)
  const chartData = data
    .filter((item) => item.value != null || item.secondaryValue != null)
    .map((item) => ({
      timestamp: item.timestamp,
      value: item.value != null ? Number(item.value) : null,
      secondaryValue: item.secondaryValue != null ? Number(item.secondaryValue) : null,
    }))
  const stats = computeSeriesStats(data, 'value')
  const secondaryStats = computeSeriesStats(data, 'secondaryValue')
  const displayedCurrent = currentValue ?? stats.current
  const displayedSecondaryCurrent = secondaryCurrentValue ?? secondaryStats.current
  const gradientId = `gradient-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`
  const isConsumoSpecifico = metricKey === 'cons_specifico'
  const isPressureChart = metricKey === 'pressione' || metricKey === 'pressione2'
  const highlightedThresholds = metricKey === 'cons_specifico'
    ? thresholds.filter((threshold) => threshold.label === 'CS realizzabile' || threshold.label === 'CS ottimale')
    : []
  const chartThresholds = metricKey === 'cons_specifico'
    ? highlightedThresholds.filter((threshold) => threshold.label === 'CS realizzabile')
    : thresholds
  const resolvedYAxisDomain = yAxisDomain ?? computeYAxisDomain(metricKey, data, chartThresholds)
  const primaryChartData = chartData.filter((item) => item.value != null)
  const secondaryChartData = chartData.filter((item) => item.secondaryValue != null).map((item) => ({
    timestamp: item.timestamp,
    value: item.secondaryValue,
  }))
  const primaryYAxisDomain = yAxisDomain ?? computeSingleSeriesYAxisDomain(metricKey, data, 'value', chartThresholds)
  const secondaryYAxisDomain = yAxisDomain ?? computeSingleSeriesYAxisDomain(metricKey, data, 'secondaryValue', chartThresholds)
  const showPressureSubcharts = isPressureChart && !!secondarySeriesLabel && secondaryChartData.length > 0
  const chartHeightClass = expanded ? 'h-[32rem] xl:h-[38rem]' : 'h-[20rem]'
  const primaryTone = seriesToneClasses(color)
  const secondaryTone = seriesToneClasses(secondaryColor)
  const handleExportImage = async () => {
    if (onExportImage) {
      onExportImage()
      return
    }
    if (!cardRef.current) return
    setShowRangeBadgeForExport(true)
    await new Promise<void>((resolve) => {
      window.requestAnimationFrame(() => window.requestAnimationFrame(() => resolve()))
    })
    const fallbackName = title.toLowerCase().replace(/[^a-z0-9]+/g, '-')
    try {
      await exportElementAsPng(cardRef.current, `${exportFileName || fallbackName}.png`)
    } finally {
      setShowRangeBadgeForExport(false)
    }
  }
  const handleExportData = () => {
    if (onExportData) {
      onExportData()
      return
    }
    const headers = ['timestamp', metricKey, ...(secondarySeriesLabel ? [`${metricKey}_2`] : [])]
    const csv = [
      headers.join(';'),
      ...data.map((point) => [
        point.timestamp,
        formatCsvNumber(point.value),
        ...(secondarySeriesLabel ? [formatCsvNumber(point.secondaryValue ?? null)] : []),
      ].join(';')),
    ].join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    const fallbackName = title.toLowerCase().replace(/[^a-z0-9]+/g, '-')
    link.download = `${exportFileName || fallbackName}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
  }
  const currentCard = (
    <div className={['h-full min-w-[9.4rem] rounded-2xl border px-3 py-1.5 shadow-sm backdrop-blur', showPressureSubcharts ? primaryTone.card : 'border-white/70 bg-white/78'].join(' ')}>
      <div className={['inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em]', showPressureSubcharts ? primaryTone.badge : 'border-slate-200 bg-slate-50 text-slate-500'].join(' ')}>
        {showPressureSubcharts ? (primarySeriesLabel || title) : 'Attuale'}
      </div>
      <div className={['mt-1 whitespace-nowrap text-sm font-semibold leading-tight', showPressureSubcharts ? primaryTone.value : 'text-slate-900'].join(' ')}>{formatValue(displayedCurrent, unit, metricKey)}</div>
      <div className={['mt-1 border-t pt-1 text-[11px] leading-tight', showPressureSubcharts ? `border-emerald-200/70 ${primaryTone.detail}` : 'border-white/70 text-slate-500'].join(' ')}>
        Media {formatValue(stats.average, unit, metricKey)}
      </div>
    </div>
  )
  const secondaryCurrentCard = secondarySeriesLabel ? (
    <div className={['h-full min-w-[9.4rem] rounded-2xl border px-3 py-1.5 shadow-sm backdrop-blur', showPressureSubcharts ? secondaryTone.card : 'border-white/70 bg-white/78'].join(' ')}>
      <div className={['inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em]', showPressureSubcharts ? secondaryTone.badge : 'border-slate-200 bg-slate-50 text-slate-500'].join(' ')}>
        {secondarySeriesLabel}
      </div>
      <div className={['mt-1 whitespace-nowrap text-sm font-semibold leading-tight', showPressureSubcharts ? secondaryTone.value : 'text-slate-900'].join(' ')}>{formatValue(displayedSecondaryCurrent, unit, metricKey)}</div>
      <div className={['mt-1 border-t pt-1 text-[11px] leading-tight', showPressureSubcharts ? `border-orange-200/70 ${secondaryTone.detail}` : 'border-white/70 text-slate-500'].join(' ')}>
        Media {formatValue(secondaryStats.average, unit, metricKey)}
      </div>
    </div>
  ) : null
  const showRangeBadge = Boolean(
    rangeStartLabel &&
    rangeEndLabel &&
    rangeStartLabel !== '--' &&
    rangeEndLabel !== '--' &&
    (expanded || showRangeBadgeForExport)
  )
  const renderChart = ({
    seriesData,
    seriesName,
    seriesColor,
    axisDomain,
    seriesGradientId,
  }: {
    seriesData: Array<{ timestamp: string; value: number | null }>
    seriesName: string
    seriesColor: string
    axisDomain: readonly [number | string, number | string]
    seriesGradientId: string
  }) => (
    <ResponsiveContainer>
      <AreaChart data={seriesData} margin={{ top: 18, right: 12, left: 6, bottom: 0 }}>
        <defs>
          <linearGradient id={seriesGradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor={seriesColor} stopOpacity={0.22} />
            <stop offset="95%" stopColor={seriesColor} stopOpacity={0.02} />
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
          domain={axisDomain}
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
          formatter={(value: number, name: string) => [formatValue(Number(value), unit, metricKey), name]}
        />
        {chartThresholds.map((threshold) => (
          <ReferenceLine
            key={`${seriesName}-${threshold.label}`}
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
          name={seriesName}
          stroke={seriesColor}
          strokeWidth={2.4}
          fill={`url(#${seriesGradientId})`}
          fillOpacity={1}
          dot={false}
          activeDot={{ r: 4, strokeWidth: 0, fill: seriesColor }}
          connectNulls
        />
      </AreaChart>
    </ResponsiveContainer>
  )

  return (
    <div ref={cardRef}>
      <Card className="overflow-hidden border-slate-200/80 shadow-[0_18px_45px_-28px_rgba(15,23,42,0.35)]">
      <CardHeader className={['min-h-[118px] border-b border-slate-100 bg-gradient-to-br', accentClassName].join(' ')}>
        <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
          <div className="flex flex-wrap items-center gap-3">
            <CardTitle className="inline-flex items-center gap-2 text-slate-900">
              {icon ? (
                <span className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-slate-200/80 bg-white/80 text-slate-500 shadow-sm">
                  {icon}
                </span>
              ) : null}
              {renderMetricTitle(title)}
            </CardTitle>
            {showRangeBadge ? (
              <div className="inline-flex max-w-full items-center gap-3.5 rounded-full border border-slate-200 bg-white/85 px-4.5 py-3 text-[14px] font-semibold tracking-[-0.01em] text-slate-700 shadow-[0_10px_24px_-22px_rgba(15,23,42,0.18)]">
                <span className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 bg-slate-50 text-slate-500">
                  <CalendarRangeIcon />
                </span>
                <span className="truncate">{rangeStartLabel}</span>
                <span className="inline-flex items-center justify-center text-slate-400">
                  <RangeArrowIcon />
                </span>
                <span className="truncate">{rangeEndLabel}</span>
              </div>
            ) : null}
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={handleExportData}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 bg-white/90 text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
                title={`Esporta ${title} come dati`}
              >
                <DownloadIcon />
              </button>
              <button
                type="button"
                onClick={() => void handleExportImage()}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 bg-white/90 text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
                title={`Esporta ${title} come immagine`}
              >
                <ImageIcon />
              </button>
              {expandable && onExpand ? (
                <button
                  type="button"
                  onClick={onExpand}
                  className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 bg-white/90 text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
                  title={`Apri ${title} a tutta pagina`}
                >
                  <ExpandIcon />
                </button>
              ) : null}
            </div>
          </div>
          <div className={isConsumoSpecifico ? 'flex flex-wrap items-stretch gap-2 xl:max-w-[34rem] xl:flex-nowrap xl:-translate-x-2' : 'flex flex-wrap items-stretch gap-2 xl:max-w-[30rem] xl:flex-nowrap'}>
            {currentCard}
            {secondaryCurrentCard}
            {highlightedThresholds.map((threshold) => {
              const tone = metricKey === 'cons_specifico' && threshold.label === 'CS realizzabile' && displayedCurrent != null
                ? (displayedCurrent <= threshold.value
                    ? { badge: 'border-emerald-200 bg-emerald-50 text-emerald-700' }
                    : { badge: 'border-rose-200 bg-rose-50 text-rose-700' })
                : thresholdToneClasses(threshold.color)
              return (
                <div
                  key={threshold.label}
                  className={[
                    'h-full rounded-2xl border border-white/70 bg-white/78 px-3 py-1.5 shadow-sm backdrop-blur',
                    isConsumoSpecifico
                      ? threshold.label === 'CS ottimale'
                        ? 'min-w-[11.8rem]'
                        : 'min-w-[10.9rem]'
                      : threshold.label === 'CS ottimale'
                        ? 'min-w-[11.4rem]'
                        : 'min-w-[10.6rem]',
                  ].join(' ')}
                >
                  <div className={['inline-flex rounded-full border px-2 py-0.5 font-semibold uppercase tracking-[0.14em]', isConsumoSpecifico ? 'text-[9px]' : 'text-[10px]', tone.badge].join(' ')}>
                    {threshold.label}
                  </div>
                  <div className={['mt-1 whitespace-nowrap font-semibold leading-tight text-slate-900', isConsumoSpecifico ? 'text-[13px]' : 'text-sm'].join(' ')}>{formatValue(threshold.value, unit, metricKey)}</div>
                  {displayedCurrent != null ? (
                    <div className={['mt-1 border-t border-white/70 pt-1 leading-tight', isConsumoSpecifico ? 'text-[10px]' : 'text-[11px]'].join(' ')}>
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
          <Skeleton className={`${chartHeightClass} w-full`} />
        ) : chartData.length === 0 ? (
          <div className={`${chartHeightClass} flex items-center justify-center rounded-2xl border border-dashed border-slate-200 bg-slate-50 text-sm text-slate-500`}>
            Nessun dato disponibile per il range selezionato.
          </div>
        ) : showPressureSubcharts ? (
          <div className={chartHeightClass}>
            <div className="grid h-full gap-3 xl:grid-cols-2">
              <div className="flex h-full min-h-[14rem] flex-col rounded-2xl border border-slate-200/80 bg-white px-3 py-2 shadow-[0_12px_28px_-24px_rgba(15,23,42,0.28)] xl:min-h-0">
                <div className="min-h-0 flex-1">
                  {renderChart({
                    seriesData: primaryChartData,
                    seriesName: primarySeriesLabel || title,
                    seriesColor: color,
                axisDomain: primaryYAxisDomain,
                    seriesGradientId: `${gradientId}-primary`,
                  })}
                </div>
              </div>
              <div className="flex h-full min-h-[14rem] flex-col rounded-2xl border border-slate-200/80 bg-white px-3 py-2 shadow-[0_12px_28px_-24px_rgba(15,23,42,0.28)] xl:min-h-0">
                <div className="min-h-0 flex-1">
                  {renderChart({
                    seriesData: secondaryChartData,
                    seriesName: secondarySeriesLabel,
                    seriesColor: secondaryColor,
                    axisDomain: secondaryYAxisDomain,
                    seriesGradientId: `${gradientId}-secondary`,
                  })}
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div className={chartHeightClass}>
            {renderChart({
              seriesData: chartData,
              seriesName: primarySeriesLabel || title,
              seriesColor: color,
                axisDomain: resolvedYAxisDomain,
              seriesGradientId: gradientId,
            })}
          </div>
        )}
      </CardContent>
      </Card>
    </div>
  )
}
