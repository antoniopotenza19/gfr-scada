import { startTransition, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { Navigate, useLocation, useNavigate, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'

import { fetchSaleCharts, fetchSaleCompressorActivity } from '../api/sale'
import { fetchPlantSummary } from '../api/plants'
import AppLayout from '../components/layout/AppLayout'
import CompressorActivityPanel from '../components/sala-charts/CompressorActivityPanel'
import CustomDateRangePicker from '../components/sala-charts/CustomDateRangePicker'
import MetricTrendCard from '../components/sala-charts/MetricTrendCard'
import RangePresetSelector from '../components/sala-charts/RangePresetSelector'
import { Card, CardContent } from '../components/ui/Card'
import Skeleton from '../components/ui/Skeleton'
import { INITIAL_SALA_CHART_RANGE, RANGE_PRESETS, SALA_METRICS, getSalaMetricThresholds, type ChartRangeKey, type SalaMetricKey } from '../constants/salaCharts'
import { SITE_ROOMS } from '../constants/siteRooms'
import { legacyKeyToSiteId } from '../constants/sites'
import { LIVE_SUMMARY_REFRESH_MS } from '../config/live'
import { useAlarms } from '../hooks/useAlarms'
import { usePlants } from '../hooks/usePlants'
import type { CompressorActivityItem, PlantSummary, SaleChartPoint } from '../types/api'
import { canViewDevFeatures, canViewSite, getAuthUserFromSessionToken } from '../utils/auth'
import { buildRoomApiMapping } from '../utils/liveSummary'
import { setLastSelectedSala } from '../utils/saleNavigation'
import { setSelectedSiteId } from '../utils/siteSelection'
import { exportElementAsPng } from '../utils/exportImage'

type CustomRange = {
  from: string
  to: string
}
type QuickRangeKey = ChartRangeKey | '3h' | '3y'
type SignalMap = PlantSummary['signals']

const FLOW_SIGNAL_EXACT = ['Flusso TOT', 'Flusso', 'Flow']
const FLOW_SIGNAL_INCLUDES = ['flusso tot', 'flusso 7 barg', 'flusso', 'flow', 'portat', 'nm3']
const POWER_SIGNAL_EXACT = ['Potenza Attiva TOT', 'Potenza Attiva', 'Power']
const POWER_SIGNAL_INCLUDES = ['potenza attiva tot', 'potenza attiva', 'power', 'kw']
const PRESSURE_SIGNAL_EXACT = ['PT-060', 'Pressione', 'Pressure']
const PRESSURE_SIGNAL_INCLUDES = ['pressione', 'pressure', 'barg', 'bar']
const DEW_SIGNAL_EXACT = ['AT-061', 'Dew Point', 'DewPoint']
const DEW_SIGNAL_INCLUDES = ['dew', 'rugiad']
const TEMP_SIGNAL_EXACT = ['Temperatura', 'Temperature']
const TEMP_SIGNAL_INCLUDES = ['temperatur', 'temp']

function norm(value: string) {
  return value.trim().toUpperCase().replace(/\s+/g, ' ')
}

function canon(value: string) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '')
}

function isExactSs2Sala(selectedSale: string | null | undefined, selectedSaleLabel: string | null | undefined) {
  return norm(selectedSale || '') === 'SS2' || norm(selectedSaleLabel || '') === 'SS2'
}

function toLocalDateTimeInput(value: string | null | undefined) {
  if (!value) return ''
  const date = new Date(value)
  const offsetMs = date.getTimezoneOffset() * 60_000
  return new Date(date.getTime() - offsetMs).toISOString().slice(0, 16)
}

function localInputToIso(value: string) {
  return new Date(value).toISOString()
}

function dateToLocalDateTimeInput(value: Date) {
  const offsetMs = value.getTimezoneOffset() * 60_000
  return new Date(value.getTime() - offsetMs).toISOString().slice(0, 16)
}

function formatTimestamp(value: string | null | undefined) {
  if (!value) return '--'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return '--'
  return date.toLocaleString('it-IT', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function formatTimeOnly(value: string | null | undefined) {
  if (!value) return '--'
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/)
  if (match) {
    const [, , , , hh, min, ss] = match
    return `${hh}:${min}:${ss}`
  }
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return '--'
  return date.toLocaleTimeString('it-IT', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function formatSalaDisplayLabel(value: string | null | undefined) {
  const normalized = (value || '').trim().toUpperCase()
  if (normalized === 'SS1 COMPOSIZIONE' || normalized === 'SS1_COMP') return 'SS1 COMP.'
  if (normalized === 'SS2 COMPOSIZIONE' || normalized === 'SS2_COMP') return 'SS2 COMP.'
  return value || 'Sala'
}

function formatSalaButtonLabel(value: string | null | undefined) {
  const normalized = (value || '').trim().toUpperCase()
  if (normalized === 'SS1_COMP') return 'SS1 COMPOSIZIONE'
  if (normalized === 'SS2_COMP') return 'SS2 COMPOSIZIONE'
  return value || 'Sala'
}

function formatDateTimeShort(value: string | null | undefined) {
  if (!value) return '--'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return '--'
  return date.toLocaleString('it-IT', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatDateTimeLong(value: string | null | undefined) {
  if (!value) return '--'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return '--'
  const months = ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic']
  const day = String(date.getDate()).padStart(2, '0')
  const month = months[date.getMonth()] || ''
  const year = date.getFullYear()
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${day} ${month} ${year} ${hours}:${minutes}`
}

function slugifyExportPart(value: string | null | undefined) {
  return (value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
}

function formatExportDatePart(value: string | null | undefined) {
  if (!value) return ''
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return ''
  const year = String(date.getFullYear())
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}_${hours}:${minutes}`
}

function computeMetricSummary(points: SaleChartPoint[], key: keyof SaleChartPoint) {
  const values = points
    .map((point) => point[key])
    .filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  const latest = values.length ? values[values.length - 1] : null
  const average = values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null
  return { latest, average }
}

function pickSignalNameByPatterns(signals: SignalMap | undefined, exact: string[], includes: string[]) {
  if (!signals) return null
  for (const name of exact) {
    if (signals[name]) return name
  }

  const lowerIncludes = includes.map((entry) => entry.toLowerCase())
  const candidates = Object.entries(signals)
    .filter(([name, info]) => {
      if (!Number.isFinite(Number(info?.value))) return false
      const lower = name.toLowerCase()
      return lowerIncludes.some((key) => lower.includes(key))
    })
    .sort((a, b) => Math.abs(Number(b[1].value)) - Math.abs(Number(a[1].value)))

  return candidates[0]?.[0] ?? null
}

function readSummarySignalByPatterns(summary: PlantSummary | undefined, exact: string[], includes: string[]) {
  const signalName = pickSignalNameByPatterns(summary?.signals, exact, includes)
  if (!signalName) return null
  const value = summary?.signals?.[signalName]?.value
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function computeRealtimeMetricValues(summary: PlantSummary | undefined) {
  const pressione = readSummarySignalByPatterns(summary, PRESSURE_SIGNAL_EXACT, PRESSURE_SIGNAL_INCLUDES)
  const potenza_kw = readSummarySignalByPatterns(summary, POWER_SIGNAL_EXACT, POWER_SIGNAL_INCLUDES)
  const flusso_nm3h = readSummarySignalByPatterns(summary, FLOW_SIGNAL_EXACT, FLOW_SIGNAL_INCLUDES)
  const dewpoint = readSummarySignalByPatterns(summary, DEW_SIGNAL_EXACT, DEW_SIGNAL_INCLUDES)
  const temperatura = readSummarySignalByPatterns(summary, TEMP_SIGNAL_EXACT, TEMP_SIGNAL_INCLUDES)
  const cons_specifico = potenza_kw != null && flusso_nm3h != null && flusso_nm3h > 0
    ? potenza_kw / flusso_nm3h
    : null

  return {
    pressione,
    potenza_kw,
    flusso_nm3h,
    dewpoint,
    temperatura,
    cons_specifico,
  }
}

function extractSignalVariants(
  signals: PlantSummary['signals'] | undefined,
  kind: 'pressure' | 'temperature'
): Array<number | null> {
  if (!signals) return []

  const byIndex = new Map<number, { value: number; tsMs: number | null }>()

  for (const [name, info] of Object.entries(signals)) {
    const unit = String(info?.unit || '').trim()
    if (/^\d+$/.test(unit)) continue
    const isPressure = /pressione|pressure/i.test(name)
    const isTemperature = /temperatur|temperature/i.test(name) && !/dew|rugiad/i.test(name)
    if (kind === 'pressure' && !isPressure) continue
    if (kind === 'temperature' && !isTemperature) continue

    const value = Number(info?.value)
    if (!Number.isFinite(value)) continue

    const indexMatch = name.match(/(?:\((\d+)\)|\b(\d+))\s*$/)
    const index = indexMatch ? Number(indexMatch[1] || indexMatch[2]) : 1
    const tsMs = info?.ts ? new Date(info.ts).getTime() : null
    const prev = byIndex.get(index)

    if (!prev || (tsMs != null && (prev.tsMs == null || tsMs >= prev.tsMs))) {
      byIndex.set(index, { value, tsMs: Number.isFinite(tsMs as number) ? tsMs : null })
    }
  }

  return Array.from(byIndex.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([, entry]) => entry.value)
}

function formatNumeric(value: number | null, fractionDigits: number = 2) {
  if (value == null || !Number.isFinite(value)) return '--'
  return new Intl.NumberFormat('it-IT', { maximumFractionDigits: fractionDigits }).format(value)
}

function ScadaIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M4 6h16v10H4zM9 20h6M12 16v4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  )
}

function ExportIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M12 4v10m0 0 4-4m-4 4-4-4M5 18h14" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
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

function ImageIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M4 6.5A2.5 2.5 0 0 1 6.5 4h11A2.5 2.5 0 0 1 20 6.5v11a2.5 2.5 0 0 1-2.5 2.5h-11A2.5 2.5 0 0 1 4 17.5v-11Z" fill="none" stroke="currentColor" strokeWidth="1.8" />
      <path d="m7 16 3.2-3.2a1 1 0 0 1 1.4 0L14 15l1.6-1.6a1 1 0 0 1 1.4 0L19 15.5M9 9.5h.01" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function FieldIcon({ kind }: { kind: 'update' | 'range' | 'flow' | 'power' | 'pressure' | 'temperature' | 'dewpoint' | 'cs' | 'target' }) {
  if (kind === 'update') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M12 7v5l3 2m6-2a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'range') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M7 3.5v2M17 3.5v2M4 8h16M6 5.5h12A2 2 0 0 1 20 7.5v10A2 2 0 0 1 18 19.5H6A2 2 0 0 1 4 17.5v-10a2 2 0 0 1 2-2Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    )
  }
  if (kind === 'flow') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M7 15c1.2-3.2 4.1-5.4 8-6 0 3.9-2.8 6.8-6.8 6.8-.4 0-.8 0-1.2-.1Zm0 0c-.8.6-1.5 1.5-2 2.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'power') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="m13 2-6 11h4l-1 9 7-12h-4l0-8Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'pressure') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M5 14a7 7 0 1 1 14 0M12 14l3-3M12 21a2 2 0 1 0 0-4 2 2 0 0 0 0 4Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'temperature') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M10 6a2 2 0 1 1 4 0v7.2a4 4 0 1 1-4 0Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M12 10v6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    )
  }
  if (kind === 'dewpoint') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M12 4c2.8 3.6 5 6.3 5 9a5 5 0 1 1-10 0c0-2.7 2.2-5.4 5-9Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'target') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M12 5v2m0 10v2m7-7h-2M7 12H5m12.07-4.93-1.41 1.41M8.34 15.66l-1.41 1.41m0-10 1.41 1.41m7.32 7.18 1.41 1.41M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M7 6h10M7 12h10M7 18h6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  )
}

function fieldKindForMetric(metricKey: keyof SaleChartPoint | 'last_update' | 'range') {
  if (metricKey === 'pressione') return 'pressure'
  if (metricKey === 'pressione2') return 'pressure'
  if (metricKey === 'potenza_kw') return 'power'
  if (metricKey === 'flusso_nm3h') return 'flow'
  if (metricKey === 'temperatura') return 'temperature'
  if (metricKey === 'temperatura2') return 'temperature'
  if (metricKey === 'dewpoint') return 'dewpoint'
  if (metricKey === 'cons_specifico') return 'cs'
  if (metricKey === 'last_update') return 'update'
  return 'range'
}

export default function SalaChartsPage() {
  const { saleId } = useParams()
  const location = useLocation()
  const navigate = useNavigate()
  const authUser = getAuthUserFromSessionToken()
  const requestedSaleId = (saleId || '').trim()
  const { data: apiPlants = [], isLoading: plantsLoading } = usePlants()

  const normalizedPlants = useMemo(() => {
    const map = new Map<string, string>()
    for (const item of apiPlants) map.set(norm(item), item)
    return map
  }, [apiPlants])
  const canonicalPlants = useMemo(() => {
    const map = new Map<string, string>()
    for (const item of apiPlants) map.set(canon(item), item)
    return map
  }, [apiPlants])

  const allowedSites = useMemo(
    () =>
      Object.keys(SITE_ROOMS).filter((siteKey) => {
        const siteIdValue = legacyKeyToSiteId(siteKey)
        return siteIdValue ? canViewSite(authUser, siteIdValue) : false
      }),
    [authUser]
  )

  const saleMappingsBySite = useMemo(() => {
    const index = new Map<string, Array<{ label: string; code: string }>>()
    for (const siteKey of allowedSites) {
      const labels = SITE_ROOMS[siteKey] || []
      const mapping = buildRoomApiMapping(labels, normalizedPlants, canonicalPlants)
      index.set(
        siteKey,
        labels.flatMap((label) => {
          const code = (mapping.get(label) || [])[0]
          return code ? [{ label, code }] : []
        })
      )
    }
    return index
  }, [allowedSites, normalizedPlants, canonicalPlants])

  const saleOptions = useMemo(() => {
    const labels = allowedSites.flatMap((siteKey) => SITE_ROOMS[siteKey] || [])
    const mapping = buildRoomApiMapping(labels, normalizedPlants, canonicalPlants)
    return Array.from(
      new Set(
        labels.flatMap((label) => mapping.get(label) || [])
      )
    )
  }, [allowedSites, normalizedPlants, canonicalPlants])

  const saleSiteByCode = useMemo(() => {
    const index = new Map<string, string>()
    for (const siteKey of allowedSites) {
      const labels = SITE_ROOMS[siteKey] || []
      const mapping = buildRoomApiMapping(labels, normalizedPlants, canonicalPlants)
      labels.forEach((label) => {
        ;(mapping.get(label) || []).forEach((saleCode) => {
          if (!index.has(saleCode)) index.set(saleCode, siteKey)
        })
      })
    }
    return index
  }, [allowedSites, normalizedPlants, canonicalPlants])

  const saleLabelByCode = useMemo(() => {
    const index = new Map<string, string>()
    for (const siteKey of allowedSites) {
      const labels = SITE_ROOMS[siteKey] || []
      const mapping = buildRoomApiMapping(labels, normalizedPlants, canonicalPlants)
      labels.forEach((label) => {
        ;(mapping.get(label) || []).forEach((saleCode) => {
          if (!index.has(saleCode)) index.set(saleCode, label)
        })
      })
    }
    return index
  }, [allowedSites, normalizedPlants, canonicalPlants])

  const saleCodeByRouteKey = useMemo(() => {
    const index = new Map<string, string>()
    saleOptions.forEach((saleCode) => {
      index.set(norm(saleCode), saleCode)
      index.set(canon(saleCode), saleCode)
    })
    saleLabelByCode.forEach((label, saleCode) => {
      index.set(norm(label), saleCode)
      index.set(canon(label), saleCode)
    })
    return index
  }, [saleOptions, saleLabelByCode])

  const fallbackSale = saleOptions[0] || ''
  const resolvedRequestedSale = requestedSaleId
    ? saleCodeByRouteKey.get(norm(requestedSaleId)) || saleCodeByRouteKey.get(canon(requestedSaleId)) || ''
    : ''
  const resolvedRequestedSite = resolvedRequestedSale ? saleSiteByCode.get(resolvedRequestedSale) || '' : ''
  const [selectedSite, setSelectedSite] = useState(() => resolvedRequestedSite || allowedSites[0] || '')
  const [selectedSale, setSelectedSale] = useState(() =>
    resolvedRequestedSale && saleOptions.includes(resolvedRequestedSale) ? resolvedRequestedSale : fallbackSale
  )
  const [activePreset, setActivePreset] = useState<ChartRangeKey | null>(INITIAL_SALA_CHART_RANGE)
  const [activeQuickRange, setActiveQuickRange] = useState<QuickRangeKey | null>(INITIAL_SALA_CHART_RANGE)
  const [appliedCustomRange, setAppliedCustomRange] = useState<CustomRange | null>(null)
  const [customFromInput, setCustomFromInput] = useState('')
  const [customToInput, setCustomToInput] = useState('')
  const [rangeError, setRangeError] = useState<string | null>(null)
  const [expandedMetricKey, setExpandedMetricKey] = useState<SalaMetricKey | null>(null)
  const chartsExportRef = useRef<HTMLDivElement | null>(null)

  useLayoutEffect(() => {
    const navigationState = (location.state || {}) as {
      resetRange?: boolean
      scrollToTop?: boolean
      chartRange?: { from?: string; to?: string }
    } | undefined
    if (!navigationState?.resetRange && !navigationState?.scrollToTop && !navigationState?.chartRange) return

    if (navigationState?.resetRange) {
      setRangeError(null)
      setActivePreset(INITIAL_SALA_CHART_RANGE)
      setActiveQuickRange(INITIAL_SALA_CHART_RANGE)
      setAppliedCustomRange(null)
    }

    if (navigationState?.chartRange?.from && navigationState?.chartRange?.to) {
      setRangeError(null)
      setActivePreset(null)
      setActiveQuickRange(null)
      setAppliedCustomRange({
        from: navigationState.chartRange.from,
        to: navigationState.chartRange.to,
      })
      setCustomFromInput(toLocalDateTimeInput(navigationState.chartRange.from))
      setCustomToInput(toLocalDateTimeInput(navigationState.chartRange.to))
    }

    if (typeof window === 'undefined') return

    const scrollToTop = () => {
      document.getElementById('app-page-top-anchor')?.scrollIntoView({ block: 'start', inline: 'nearest' })
      window.scrollTo({ top: 0, behavior: 'auto' })
      document.documentElement.scrollTop = 0
      document.body.scrollTop = 0
      document.querySelector<HTMLElement>('.app-shell-main')?.scrollTo({ top: 0, behavior: 'auto' })
      document.querySelector<HTMLElement>('.app-shell-content')?.scrollTo({ top: 0, behavior: 'auto' })
    }

    scrollToTop()
    const frameA = window.requestAnimationFrame(scrollToTop)
    const frameB = window.requestAnimationFrame(() => window.requestAnimationFrame(scrollToTop))
    const timeoutId = window.setTimeout(scrollToTop, 60)

    return () => {
      window.cancelAnimationFrame(frameA)
      window.cancelAnimationFrame(frameB)
      window.clearTimeout(timeoutId)
    }
  }, [location.key, location.state])

  useEffect(() => {
    if (plantsLoading) return
    if (!saleOptions.length) return
    const resolvedSale = resolvedRequestedSale && saleOptions.includes(resolvedRequestedSale) ? resolvedRequestedSale : fallbackSale
    setSelectedSale(resolvedSale)
    if (resolvedSale && requestedSaleId !== resolvedSale) {
      navigate(`/sale/${encodeURIComponent(resolvedSale)}/grafici`, { replace: true })
    }
  }, [plantsLoading, saleOptions, requestedSaleId, resolvedRequestedSale, fallbackSale, navigate])

  useEffect(() => {
    if (!selectedSale) return
    const siteKey = saleSiteByCode.get(selectedSale)
    const siteIdValue = legacyKeyToSiteId(siteKey)
    if (siteKey && siteKey !== selectedSite) {
      setSelectedSite(siteKey)
    }
    if (siteIdValue) {
      setSelectedSiteId(siteIdValue)
    }
  }, [selectedSale, saleSiteByCode, selectedSite])

  useEffect(() => {
    if (!allowedSites.length) return
    if (selectedSite && allowedSites.includes(selectedSite)) return
    setSelectedSite(allowedSites[0])
  }, [allowedSites, selectedSite])

  useEffect(() => {
    const saleLabel = saleLabelByCode.get(selectedSale)
    if (saleLabel) {
      setLastSelectedSala(saleLabel)
    }
  }, [selectedSale, saleLabelByCode])

  const siteSaleButtons = saleMappingsBySite.get(selectedSite) || []

  const queryOptions = activePreset
    ? { range: activePreset, maxPoints: 360 }
    : appliedCustomRange
      ? { from: appliedCustomRange.from, to: appliedCustomRange.to, maxPoints: 360 }
      : { range: INITIAL_SALA_CHART_RANGE, maxPoints: 360 }

  const autoRefreshEnabled = Boolean(selectedSale)

  const chartsQuery = useQuery({
    queryKey: ['sale-charts', selectedSale, queryOptions.range || null, queryOptions.from || null, queryOptions.to || null],
    queryFn: () => fetchSaleCharts(selectedSale, queryOptions),
    enabled: Boolean(selectedSale),
    staleTime: autoRefreshEnabled ? LIVE_SUMMARY_REFRESH_MS : 60_000,
    cacheTime: 300_000,
    refetchInterval: autoRefreshEnabled ? LIVE_SUMMARY_REFRESH_MS : false,
    refetchIntervalInBackground: autoRefreshEnabled,
    refetchOnWindowFocus: autoRefreshEnabled,
    retry: 1,
  })

  const compressorQuery = useQuery({
    queryKey: ['sale-compressor-activity', selectedSale, queryOptions.range || null, queryOptions.from || null, queryOptions.to || null],
    queryFn: () => fetchSaleCompressorActivity(selectedSale, queryOptions),
    enabled: Boolean(selectedSale),
    staleTime: autoRefreshEnabled ? LIVE_SUMMARY_REFRESH_MS : 60_000,
    cacheTime: 300_000,
    refetchInterval: autoRefreshEnabled ? LIVE_SUMMARY_REFRESH_MS : false,
    refetchIntervalInBackground: autoRefreshEnabled,
    refetchOnWindowFocus: autoRefreshEnabled,
    retry: 1,
  })

  const realtimeSummaryQuery = useQuery({
    queryKey: ['sale-realtime-summary', selectedSale],
    queryFn: () => fetchPlantSummary(selectedSale),
    enabled: Boolean(selectedSale),
    staleTime: autoRefreshEnabled ? LIVE_SUMMARY_REFRESH_MS : 60_000,
    cacheTime: 120_000,
    refetchInterval: autoRefreshEnabled ? LIVE_SUMMARY_REFRESH_MS : false,
    refetchIntervalInBackground: autoRefreshEnabled,
    refetchOnWindowFocus: autoRefreshEnabled,
    retry: 1,
  })

  useEffect(() => {
    if (!chartsQuery.data) return
    if (activePreset) {
      setCustomFromInput(toLocalDateTimeInput(chartsQuery.data.from_ts))
      setCustomToInput(toLocalDateTimeInput(chartsQuery.data.to_ts))
    }
  }, [chartsQuery.data, activePreset])

  if (!plantsLoading && allowedSites.length === 0) {
    return <Navigate to="/403" replace />
  }

  const chartPayload = chartsQuery.data
  const compressorPayload = compressorQuery.data
  const currentSite = selectedSale ? saleSiteByCode.get(selectedSale) || selectedSite : selectedSite
  const showMultiSiteMeta = authUser.allowedSiteIds.length > 1
  const latestPointTs = chartPayload && chartPayload.points.length > 0
    ? chartPayload.points[chartPayload.points.length - 1].timestamp
    : null
  const showDevMeta = canViewDevFeatures(authUser)
  const quickRangePresets: Array<{ key: QuickRangeKey; label: string }> = [
    { key: '5m', label: '5 m' },
    { key: '15m', label: '15 m' },
    { key: '30m', label: '30 m' },
    { key: '1h', label: '1 h' },
    { key: '3h', label: '3 h' },
    { key: '1d', label: '1 giorno' },
    { key: '1w', label: '1 settimana' },
    { key: '1mo', label: '1 mese' },
    { key: '3mo', label: '3 mesi' },
    { key: '1y', label: '1 anno' },
    { key: '3y', label: '3 anni' },
  ]
  const selectedSaleLabel = saleLabelByCode.get(selectedSale) || chartPayload?.sale_name || selectedSale
  const saleTitle = formatSalaDisplayLabel(selectedSaleLabel)
  const latestRealtimeUpdate = realtimeSummaryQuery.data?.last_update || chartPayload?.last_update || latestPointTs || null
  const lastUpdateLabel = formatTimeOnly(latestRealtimeUpdate)
  const roomAlarmsQuery = useAlarms(currentSite || selectedSite, undefined, undefined, selectedSaleLabel || undefined)
  const roomAlarmCount = useMemo(
    () => (roomAlarmsQuery.data || []).filter((alarm) => alarm.active !== false).length,
    [roomAlarmsQuery.data]
  )
  const isSs2Sala = isExactSs2Sala(selectedSale, selectedSaleLabel)
  const summaryPressure = computeMetricSummary(chartPayload?.points || [], 'pressione')
  const realtimeMetricValues = computeRealtimeMetricValues(realtimeSummaryQuery.data)
  const summaryPower = computeMetricSummary(chartPayload?.points || [], 'potenza_kw')
  const summaryFlow = computeMetricSummary(chartPayload?.points || [], 'flusso_nm3h')
  const summaryCs = computeMetricSummary(chartPayload?.points || [], 'cons_specifico')
  const summaryTemperature = computeMetricSummary(chartPayload?.points || [], 'temperatura')
  const csReferenceLine = getSalaMetricThresholds(selectedSale, 'cons_specifico').find((line) => line.label === 'CS realizzabile') || null
  const pressureVariants = extractSignalVariants(realtimeSummaryQuery.data?.signals, 'pressure')
  const temperatureVariants = extractSignalVariants(realtimeSummaryQuery.data?.signals, 'temperature')
  const pressurePrimary = pressureVariants[0] ?? summaryPressure.latest ?? realtimeMetricValues.pressione ?? null
  const pressureSecondary = pressureVariants[1] ?? null
  const temperaturePrimary = temperatureVariants[0] ?? summaryTemperature.latest ?? realtimeMetricValues.temperatura ?? null
  const temperatureSecondary = temperatureVariants[1] ?? null
  const showDualPressureCards = isSs2Sala && (pressurePrimary != null || pressureSecondary != null)
  const hasPressureSecondSeries = isSs2Sala && (chartPayload?.points || []).some((point) => typeof point.pressione2 === 'number' && Number.isFinite(point.pressione2))
  const hasTemperatureSecondSeries = isSs2Sala && (chartPayload?.points || []).some((point) => typeof point.temperatura2 === 'number' && Number.isFinite(point.temperatura2))
  const baseMetricCards = SALA_METRICS.map((metric) => ({
    ...metric,
    label:
      isSs2Sala && metric.key === 'pressione'
        ? 'Pressione 1'
        : isSs2Sala && metric.key === 'temperatura'
          ? 'Temperatura 1'
          : metric.label,
    data: (chartPayload?.points || []).map((point) => ({
      timestamp: point.timestamp,
      value: point[metric.key],
    })),
    thresholds: getSalaMetricThresholds(selectedSale, metric.key),
    currentValue:
      metric.key === 'pressione'
        ? pressurePrimary
        : metric.key === 'temperatura'
          ? temperaturePrimary
          : realtimeMetricValues[metric.key],
  }))
  const extraMetricCards = [
    ...(hasPressureSecondSeries
      ? [{
          key: 'pressione2' as const,
          label: 'Pressione 2',
          unit: 'barg',
          color: '#f97316',
          accent: 'from-[#fff4eb] to-[#fffaf5]',
          description: 'Seconda linea pressione della sala nel range selezionato.',
          data: (chartPayload?.points || []).map((point) => ({
            timestamp: point.timestamp,
            value: point.pressione2 ?? null,
          })),
          thresholds: [] as ReturnType<typeof getSalaMetricThresholds>,
          currentValue: pressureSecondary,
        }]
      : []),
    ...(hasTemperatureSecondSeries
      ? [{
          key: 'temperatura2' as const,
          label: 'Temperatura 2',
          unit: 'degC',
          color: '#f43f5e',
          accent: 'from-[#fff1f2] to-[#fff7f8]',
          description: 'Seconda misura temperatura della sala nel range selezionato.',
          data: (chartPayload?.points || []).map((point) => ({
            timestamp: point.timestamp,
            value: point.temperatura2 ?? null,
          })),
          thresholds: [] as ReturnType<typeof getSalaMetricThresholds>,
          currentValue: temperatureSecondary,
        }]
      : []),
  ]
  const metricCards = [...baseMetricCards, ...extraMetricCards]
  const metricsWithoutData = metricCards.filter((metric) =>
    !metric.data.some((point) => typeof point.value === 'number' && Number.isFinite(point.value))
  )
  const overviewKpis: Array<{
    label: string
    value: string
    detail: string
    iconKind: ReturnType<typeof fieldKindForMetric>
  }> = [
    {
      label: 'Flusso medio',
      value: `${formatNumeric(summaryFlow.average)} Nm3/h`,
      detail: realtimeMetricValues.flusso_nm3h != null ? `Attuale ${formatNumeric(realtimeMetricValues.flusso_nm3h)} Nm3/h` : summaryFlow.latest != null ? `Attuale ${formatNumeric(summaryFlow.latest)} Nm3/h` : 'Media periodo',
      iconKind: fieldKindForMetric('flusso_nm3h'),
    },
    {
      label: 'Potenza media',
      value: `${formatNumeric(summaryPower.average)} kW`,
      detail: realtimeMetricValues.potenza_kw != null ? `Attuale ${formatNumeric(realtimeMetricValues.potenza_kw)} kW` : summaryPower.latest != null ? `Attuale ${formatNumeric(summaryPower.latest)} kW` : 'Media periodo',
      iconKind: fieldKindForMetric('potenza_kw'),
    },
    ...(showDualPressureCards
        ? [
          {
            label: 'Pressione 1 media',
            value: `${formatNumeric(summaryPressure.average)} barg`,
            detail: pressurePrimary != null ? `Attuale ${formatNumeric(pressurePrimary)} barg` : 'Media periodo',
            iconKind: fieldKindForMetric('pressione') as ReturnType<typeof fieldKindForMetric>,
          },
          {
            label: 'Pressione 2 media',
            value: `${formatNumeric(pressureSecondary)} barg`,
            detail: pressureSecondary != null ? `Attuale ${formatNumeric(pressureSecondary)} barg` : 'Segnale non disponibile',
            iconKind: fieldKindForMetric('pressione') as ReturnType<typeof fieldKindForMetric>,
          },
        ]
      : [
          {
            label: 'Pressione media',
            value: `${formatNumeric(summaryPressure.average)} barg`,
            detail: realtimeMetricValues.pressione != null ? `Attuale ${formatNumeric(realtimeMetricValues.pressione)} barg` : summaryPressure.latest != null ? `Attuale ${formatNumeric(summaryPressure.latest)} barg` : 'Media periodo',
            iconKind: fieldKindForMetric('pressione') as ReturnType<typeof fieldKindForMetric>,
          },
        ]),
    {
      label: 'Consumo medio',
      value: `${formatNumeric(summaryCs.average, 3)} kWh/Nm3`,
      detail: csReferenceLine ? `Contratto ${formatNumeric(csReferenceLine.value, 3)} kWh/Nm3` : 'Media periodo',
      iconKind: fieldKindForMetric('cons_specifico'),
    },
  ]
  const featuredMetricLayout: Array<{ key: SalaMetricKey; className: string }> = [
    { key: 'pressione', className: 'xl:col-span-6' },
    { key: 'potenza_kw', className: 'xl:col-span-6' },
    { key: 'flusso_nm3h', className: 'xl:col-span-6' },
    { key: 'temperatura', className: 'xl:col-span-6' },
    ...(hasPressureSecondSeries ? [{ key: 'pressione2' as const, className: 'xl:col-span-6' }] : []),
    ...(hasTemperatureSecondSeries ? [{ key: 'temperatura2' as const, className: 'xl:col-span-6' }] : []),
  ]
  const featuredMetricKeys = new Set(featuredMetricLayout.map((item) => item.key))
  const metricCardByKey = new Map(metricCards.map((metric) => [metric.key, metric] as const))
  const pairedMetricCards = ['dewpoint', 'cons_specifico']
    .map((key) => metricCardByKey.get(key as SalaMetricKey))
    .filter((metric): metric is NonNullable<typeof metric> => Boolean(metric))
  const pairedMetricKeys = new Set(pairedMetricCards.map((metric) => metric.key))
  const supplementalMetricCards = metricCards.filter((metric) => !featuredMetricKeys.has(metric.key) && !pairedMetricKeys.has(metric.key))
  const expandedMetric = expandedMetricKey ? metricCardByKey.get(expandedMetricKey) || null : null
  const visibleRangeLabel = chartPayload
    ? `${formatDateTimeLong(chartPayload.from_ts)} — ${formatDateTimeLong(chartPayload.to_ts)}`
    : customFromInput && customToInput
      ? `${formatDateTimeLong(localInputToIso(customFromInput))} — ${formatDateTimeLong(localInputToIso(customToInput))}`
      : '--'

  const visibleRangeStart = chartPayload
    ? formatDateTimeLong(chartPayload.from_ts)
    : customFromInput
      ? formatDateTimeLong(localInputToIso(customFromInput))
      : '--'
  const visibleRangeEnd = chartPayload
    ? formatDateTimeLong(chartPayload.to_ts)
    : customToInput
      ? formatDateTimeLong(localInputToIso(customToInput))
      : '--'
  const exportBaseName = [currentSite || chartPayload?.plant || selectedSite, selectedSaleLabel]
    .map(slugifyExportPart)
    .filter(Boolean)
    .join('_')
  const exportRangeStart = chartPayload?.from_ts || (customFromInput ? localInputToIso(customFromInput) : null)
  const exportRangeEnd = chartPayload?.to_ts || (customToInput ? localInputToIso(customToInput) : null)
  const exportRangeName = [formatExportDatePart(exportRangeStart), formatExportDatePart(exportRangeEnd)]
    .filter(Boolean)
    .join('_')

  const buildExportFileName = (suffix: string) => {
    const suffixPart = slugifyExportPart(suffix)
    return [exportBaseName, suffixPart, exportRangeName].filter(Boolean).join('_')
  }

  const handleSaleChange = (nextSale: string) => {
    if (!nextSale) return
    startTransition(() => {
      setSelectedSale(nextSale)
      navigate(`/sale/${encodeURIComponent(nextSale)}/grafici`)
    })
  }

  const handleSiteChange = (nextSite: string) => {
    if (!nextSite) return
    const nextChoices = saleMappingsBySite.get(nextSite) || []
    const nextSale = nextChoices[0]?.code || ''
    setSelectedSite(nextSite)
    if (!nextSale) return
    startTransition(() => {
      setSelectedSale(nextSale)
      navigate(`/sale/${encodeURIComponent(nextSale)}/grafici`)
    })
  }

  const handlePresetSelect = (preset: string) => {
    setRangeError(null)
    if (preset === '3h' || preset === '3y') {
      const endDate = new Date()
      const startDate = new Date(endDate)
      if (preset === '3h') {
        startDate.setHours(startDate.getHours() - 3)
      } else {
        startDate.setFullYear(startDate.getFullYear() - 3)
      }
      const fromInput = dateToLocalDateTimeInput(startDate)
      const toInput = dateToLocalDateTimeInput(endDate)
      startTransition(() => {
        setCustomFromInput(fromInput)
        setCustomToInput(toInput)
        setActiveQuickRange(preset)
        setActivePreset(null)
        setAppliedCustomRange({ from: startDate.toISOString(), to: endDate.toISOString() })
      })
      return
    }
    startTransition(() => {
      setActiveQuickRange(preset as QuickRangeKey)
      setActivePreset(preset as ChartRangeKey)
      setAppliedCustomRange(null)
    })
  }

  const handleApplyCustomRangeValues = (fromInput: string, toInput: string) => {
    if (!fromInput || !toInput) {
      setRangeError('Imposta sia data/ora iniziale sia finale.')
      return
    }
    const fromIso = localInputToIso(fromInput)
    const toIso = localInputToIso(toInput)
    if (new Date(fromIso).getTime() >= new Date(toIso).getTime()) {
      setRangeError('La data finale deve essere successiva a quella iniziale.')
      return
    }
    setRangeError(null)
    startTransition(() => {
      setCustomFromInput(fromInput)
      setCustomToInput(toInput)
      setActiveQuickRange(null)
      setActivePreset(null)
      setAppliedCustomRange({ from: fromIso, to: toIso })
    })
  }

  const compressorItems: CompressorActivityItem[] = compressorPayload?.items || []

  const handleExportCharts = async () => {
    if (!chartsExportRef.current) return
    await exportElementAsPng(chartsExportRef.current, `${buildExportFileName('grafici')}.png`)
  }

  const handleExportChartsData = () => {
    if (!chartPayload?.points.length || typeof window === 'undefined') return

    const headers = ['timestamp', ...metricCards.map((metric) => metric.key)]
    const rows = chartPayload.points.map((point) => [
      point.timestamp,
      ...metricCards.map((metric) => {
        const value = point[metric.key]
        return value == null || !Number.isFinite(value) ? '' : String(value)
      }),
    ])

    const csv = [headers.join(';'), ...rows.map((row) => row.join(';'))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${buildExportFileName('grafici')}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
  }

  const exportMetricData = (metric: typeof metricCards[number]) => {
    if (typeof window === 'undefined') return
    const headers = ['timestamp', metric.key]
    const csv = [
      headers.join(';'),
      ...metric.data.map((point) => [
        point.timestamp,
        point.value == null || !Number.isFinite(point.value) ? '' : String(point.value),
      ].join(';')),
    ].join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${buildExportFileName(metric.label)}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
  }

  const handleExportCompressorsData = () => {
    if (!compressorItems.length || typeof window === 'undefined') return

    const headers = [
      'id_compressore',
      'code',
      'name',
      'current_state',
      'dominant_state',
      'minutes_on',
      'minutes_standby',
      'minutes_off',
      'utilization_pct',
      'standby_pct',
      'off_pct',
      'avg_power_kw',
      'energy_kwh',
    ]
    const csv = [
      headers.join(';'),
      ...compressorItems.map((item) => [
        item.id_compressore,
        item.code,
        item.name,
        item.current_state,
        item.dominant_state,
        item.minutes_on ?? '',
        item.minutes_standby ?? '',
        item.minutes_off ?? '',
        item.utilization_pct ?? '',
        item.standby_pct ?? '',
        item.off_pct ?? '',
        item.avg_power_kw ?? '',
        item.energy_kwh ?? '',
      ].join(';')),
    ].join('\n')

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${buildExportFileName('compressori')}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
  }

  return (
    <AppLayout
      title="Storico e Trend"
      subtitle={currentSite || chartPayload?.plant || ''}
      plant={selectedSite}
      onPlantChange={handleSiteChange}
      selectorOptions={allowedSites}
      selectorPlaceholder="Seleziona impianto"
      scadaPlant={selectedSaleLabel}
      chartsPlant={selectedSaleLabel}
      alarmCount={roomAlarmCount}
      alarmContextRoom={selectedSaleLabel || undefined}
      alarmContextPlant={currentSite || selectedSite}
    >
      <div className="space-y-5">
        <Card className="overflow-hidden border-slate-200/80 bg-[linear-gradient(180deg,_#ffffff,_#f8fafc)] shadow-[0_20px_50px_-34px_rgba(15,23,42,0.38)]">
          <CardContent className="space-y-6 p-5 sm:p-6">
            <div className="space-y-3">
              <div className="flex flex-wrap gap-2">
                {siteSaleButtons.map((item) => {
                  const active = item.code === selectedSale
                  return (
                    <button
                      key={item.code}
                      type="button"
                      onClick={() => handleSaleChange(item.code)}
                    className={[
                        'rounded-full border px-4 py-2 text-sm font-semibold transition',
                        active
                          ? 'border-sky-700 bg-sky-700 text-white shadow-sm'
                          : 'border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:bg-slate-50',
                      ].join(' ')}
                    >
                      {formatSalaButtonLabel(item.label)}
                    </button>
                  )
                })}
              </div>
            </div>

            <div className="grid gap-5 xl:grid-cols-[minmax(280px,340px)_1fr] xl:items-center">
              <div className="space-y-4">
                <div className="flex flex-wrap items-end gap-3">
                  <div className="space-y-1">
                    <h2 className="text-[2rem] font-bold leading-none tracking-[-0.03em] text-slate-950">{saleTitle}</h2>
                    {showMultiSiteMeta ? (
                      <div className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-600">{currentSite || chartPayload?.plant || '--'}</div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    onClick={() => navigate(`/scada/${encodeURIComponent(selectedSaleLabel)}`)}
                    className="relative top-2 inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white/90 px-5 py-2.5 text-[15px] font-semibold text-slate-700 shadow-sm transition hover:-translate-y-0.5 hover:border-slate-300 hover:bg-slate-50 hover:shadow-md"
                  >
                    <ScadaIcon />
                    SCADA
                  </button>
                </div>

                <div
                  className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500"
                  title={autoRefreshEnabled
                    ? `Aggiornamento automatico attivo. Last update ${lastUpdateLabel}.`
                    : `Modalita storica. Ultimo aggiornamento disponibile ${formatTimestamp(latestRealtimeUpdate)}.`}
                >
                  {`Last update - ${lastUpdateLabel}`}
                </div>
              </div>

              <div className={`grid gap-3 sm:grid-cols-2 ${showDualPressureCards ? 'xl:grid-cols-5' : 'xl:grid-cols-4'}`}>
                {overviewKpis.map((item) => (
                  <div
                    key={item.label}
                    className="rounded-2xl border border-slate-200/80 bg-white/90 px-4 py-3.5 shadow-[0_14px_24px_-22px_rgba(15,23,42,0.45)]"
                  >
                    <div className="inline-flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                      <FieldIcon kind={item.iconKind} />
                      <span>{item.label}</span>
                    </div>
                    <div className="mt-1.5 text-[1.3rem] font-bold tracking-[-0.02em] text-slate-950">{item.value}</div>
                    <div className="mt-1 text-xs font-medium text-slate-500">{item.detail}</div>
                  </div>
                ))}
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="overflow-visible border-slate-200/80 bg-white shadow-[0_20px_55px_-38px_rgba(15,23,42,0.28)]">
          <CardContent className="space-y-6 p-5 sm:p-6">
            <div className="rounded-2xl border border-slate-200/80 bg-slate-50/70 p-4">
              <div className="space-y-4">
                <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
                  <div className="text-[13px] font-bold uppercase tracking-[0.14em] text-slate-500 xl:min-w-[4.5rem] xl:text-center">Time Range</div>

                  <div className="flex flex-wrap items-center gap-2.5 xl:flex-nowrap xl:justify-end xl:gap-2.5">
                    <RangePresetSelector presets={quickRangePresets} activeKey={activeQuickRange} onSelect={handlePresetSelect} />

                    <div className="hidden h-10 w-px bg-slate-200 xl:block" aria-hidden="true" />

                    <div className="shrink-0 rounded-2xl border border-sky-100/90 bg-white/90 px-1.5 py-1.5 shadow-[0_12px_28px_-24px_rgba(14,165,233,0.35)]">
                      <CustomDateRangePicker
                        fromValue={customFromInput}
                        toValue={customToInput}
                        onApplyRange={handleApplyCustomRangeValues}
                        error={rangeError}
                        triggerLabel="Date libere"
                        showRangeLabel={false}
                      />
                    </div>
                  </div>
                </div>

                {showDevMeta ? (
                  <div className="flex flex-wrap gap-2 text-xs text-slate-500">
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1">Tabella: {chartPayload?.source_table || '--'}</span>
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1">Granularita: {chartPayload?.granularity || '--'}</span>
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1">Punti: {chartPayload?.points.length ?? 0}</span>
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1">Compressori: {compressorItems.length}</span>
                  </div>
                ) : null}
              </div>
            </div>

            <div ref={chartsExportRef} className="space-y-4">
              <div className="grid gap-3 xl:grid-cols-[1fr_auto_1fr] xl:items-center">
                <div className="flex flex-wrap items-center gap-3">
                  <div className="text-[1.15rem] font-extrabold uppercase tracking-[0.16em] text-slate-800">Grafici</div>
                </div>
                <div className="flex justify-center">
                  <div className="inline-flex items-center gap-3 rounded-full border border-slate-200 bg-[linear-gradient(180deg,_#ffffff,_#f8fafc)] px-5 py-2.5 text-[14px] font-semibold tracking-[-0.01em] text-slate-800 shadow-[0_10px_24px_-22px_rgba(15,23,42,0.18)]">
                    <span className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-slate-200 bg-slate-50 text-slate-500">
                      <CalendarRangeIcon />
                    </span>
                    <span className="whitespace-nowrap">{visibleRangeStart}</span>
                    <span className="inline-flex items-center justify-center text-slate-400">
                      <RangeArrowIcon />
                    </span>
                    <span className="whitespace-nowrap">{visibleRangeEnd}</span>
                  </div>
                </div>
                <div className="flex flex-wrap items-center justify-end gap-2 xl:justify-self-end">
                  <button
                    type="button"
                    onClick={handleExportChartsData}
                    disabled={!chartPayload?.points.length}
                    className={[
                      'inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-semibold transition',
                      chartPayload?.points.length
                        ? 'border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:bg-slate-50'
                        : 'cursor-not-allowed border-slate-200 bg-slate-100 text-slate-400',
                    ].join(' ')}
                  >
                    <ExportIcon />
                    Dati
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleExportCharts()}
                    disabled={!chartPayload?.points.length}
                    className={[
                      'inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-semibold transition',
                      chartPayload?.points.length
                        ? 'border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:bg-slate-50'
                        : 'cursor-not-allowed border-slate-200 bg-slate-100 text-slate-400',
                    ].join(' ')}
                  >
                    <ImageIcon />
                    Immagine
                  </button>
                </div>
              </div>

              {(chartsQuery.isLoading || compressorQuery.isLoading) && !chartPayload ? (
                <div className="grid gap-4 xl:grid-cols-12">
                  <Skeleton className="h-[27rem] w-full rounded-3xl xl:col-span-8" />
                  <Skeleton className="h-[27rem] w-full rounded-3xl xl:col-span-4" />
                  <Skeleton className="h-[27rem] w-full rounded-3xl xl:col-span-6" />
                  <Skeleton className="h-[27rem] w-full rounded-3xl xl:col-span-6" />
                </div>
              ) : null}

              {chartsQuery.isError ? (
                <Card className="border-rose-200 bg-rose-50 text-rose-700">
                  <CardContent className="p-4 text-sm">
                    Errore nel caricamento dei grafici sala. Controlla backend ed endpoint `/api/sale/{selectedSale}/timeseries`.
                  </CardContent>
                </Card>
              ) : null}

              {chartPayload && !chartPayload.range_has_data ? (
                <Card className="border-amber-200 bg-amber-50 text-amber-900">
                  <CardContent className="p-4 text-sm">
                    Nessun dato nell'intervallo selezionato. Copertura disponibile su `{chartPayload.source_table}`:
                    dal {formatDateTimeShort(chartPayload.available_from_ts)} al {formatDateTimeShort(chartPayload.available_to_ts)}.
                  </CardContent>
                </Card>
              ) : null}

              {chartPayload && chartPayload.range_has_data && metricsWithoutData.length > 0 ? (
                <Card className="border-sky-200 bg-sky-50 text-sky-900">
                  <CardContent className="p-4 text-sm">
                    Alcune metriche non hanno valori nell'aggregate selezionato: {metricsWithoutData.map((metric) => metric.label).join(', ')}.
                  </CardContent>
                </Card>
              ) : null}

              <div className="grid gap-4 xl:grid-cols-12">
                {featuredMetricLayout.map((slot) => {
                  const metric = metricCardByKey.get(slot.key)
                  if (!metric) return null
                  return (
                    <div key={metric.key} className={slot.className}>
                      <MetricTrendCard
                        metricKey={metric.key}
                        title={metric.label}
                        icon={<FieldIcon kind={fieldKindForMetric(metric.key)} />}
                        unit={metric.unit}
                        color={metric.color}
                        accentClassName={metric.accent}
                        description={metric.description}
                        data={metric.data}
                        thresholds={metric.thresholds}
                        rangeStartLabel={visibleRangeStart}
                        rangeEndLabel={visibleRangeEnd}
                        loading={chartsQuery.isFetching && !chartPayload}
                        currentValue={metric.currentValue ?? null}
                        exportFileName={buildExportFileName(metric.label)}
                        onExportData={() => exportMetricData(metric)}
                        onExpand={() => setExpandedMetricKey(metric.key)}
                      />
                    </div>
                  )
                })}
              </div>

              {pairedMetricCards.length ? (
                <div className="grid gap-4 xl:grid-cols-2">
                  {pairedMetricCards.map((metric) => (
                    <MetricTrendCard
                      key={metric.key}
                      metricKey={metric.key}
                      title={metric.label}
                      icon={<FieldIcon kind={fieldKindForMetric(metric.key)} />}
                      unit={metric.unit}
                      color={metric.color}
                      accentClassName={metric.accent}
                      description={metric.description}
                      data={metric.data}
                      thresholds={metric.thresholds}
                      rangeStartLabel={visibleRangeStart}
                      rangeEndLabel={visibleRangeEnd}
                      loading={chartsQuery.isFetching && !chartPayload}
                      currentValue={metric.currentValue ?? null}
                      exportFileName={buildExportFileName(metric.label)}
                      onExportData={() => exportMetricData(metric)}
                      onExpand={() => setExpandedMetricKey(metric.key)}
                    />
                  ))}
                </div>
              ) : null}

              {supplementalMetricCards.length ? (
                <div className="grid gap-4 xl:grid-cols-2">
                  {supplementalMetricCards.map((metric) => (
                    <MetricTrendCard
                      key={metric.key}
                      metricKey={metric.key}
                      title={metric.label}
                      icon={<FieldIcon kind={fieldKindForMetric(metric.key)} />}
                      unit={metric.unit}
                      color={metric.color}
                      accentClassName={metric.accent}
                      description={metric.description}
                      data={metric.data}
                      thresholds={metric.thresholds}
                      rangeStartLabel={visibleRangeStart}
                      rangeEndLabel={visibleRangeEnd}
                      loading={chartsQuery.isFetching && !chartPayload}
                      currentValue={metric.currentValue ?? null}
                      exportFileName={buildExportFileName(metric.label)}
                      onExportData={() => exportMetricData(metric)}
                      onExpand={() => setExpandedMetricKey(metric.key)}
                    />
                  ))}
                </div>
              ) : null}

              <CompressorActivityPanel
                items={compressorItems}
                loading={compressorQuery.isLoading && !compressorPayload}
                exportFileName={buildExportFileName('compressori')}
                onExportData={handleExportCompressorsData}
              />
            </div>
          </CardContent>
        </Card>
      </div>

      {expandedMetric ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/55 p-4 backdrop-blur-sm">
          <div className="relative max-h-[96vh] w-full max-w-[min(98vw,1440px)] overflow-auto rounded-[2rem] border border-slate-200 bg-white p-4 pt-8 shadow-[0_35px_80px_-28px_rgba(15,23,42,0.45)] sm:p-5 sm:pt-9">
            <button
              type="button"
              onClick={() => setExpandedMetricKey(null)}
              className="absolute right-3 top-5 z-10 inline-flex h-8 w-8 items-center justify-center rounded-full border border-slate-200 bg-white text-sm text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50"
              aria-label="Chiudi grafico espanso"
            >
              x
            </button>
            <MetricTrendCard
              metricKey={expandedMetric.key}
              title={expandedMetric.label}
              icon={<FieldIcon kind={fieldKindForMetric(expandedMetric.key)} />}
              unit={expandedMetric.unit}
              color={expandedMetric.color}
              accentClassName={expandedMetric.accent}
              description={expandedMetric.description}
              data={expandedMetric.data}
              thresholds={expandedMetric.thresholds}
              rangeStartLabel={visibleRangeStart}
              rangeEndLabel={visibleRangeEnd}
              loading={chartsQuery.isFetching && !chartPayload}
              currentValue={expandedMetric.currentValue ?? null}
              exportFileName={buildExportFileName(expandedMetric.label)}
              onExportData={() => exportMetricData(expandedMetric)}
              expandable={false}
              expanded
            />
          </div>
        </div>
      ) : null}
    </AppLayout>
  )
}

