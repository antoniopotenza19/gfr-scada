import { startTransition, useEffect, useLayoutEffect, useMemo, useState } from 'react'
import { Navigate, useLocation, useNavigate, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'

import { fetchSaleCharts, fetchSaleCompressorActivity } from '../api/sale'
import { fetchPlantSummary } from '../api/plants'
import AppLayout from '../components/layout/AppLayout'
import CompressorActivityPanel from '../components/sala-charts/CompressorActivityPanel'
import CustomDateRangePicker from '../components/sala-charts/CustomDateRangePicker'
import MetricTrendCard from '../components/sala-charts/MetricTrendCard'
import RangePresetSelector from '../components/sala-charts/RangePresetSelector'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import Skeleton from '../components/ui/Skeleton'
import { INITIAL_SALA_CHART_RANGE, RANGE_PRESETS, SALA_METRICS, getSalaMetricThresholds, type ChartRangeKey } from '../constants/salaCharts'
import { SITE_ROOMS } from '../constants/siteRooms'
import { legacyKeyToSiteId } from '../constants/sites'
import { LIVE_SUMMARY_REFRESH_MS } from '../config/live'
import { usePlants } from '../hooks/usePlants'
import type { CompressorActivityItem, PlantSummary, SaleChartPoint, SaleChartsResponse } from '../types/api'
import { canViewDevFeatures, canViewSite, getAuthUserFromSessionToken } from '../utils/auth'
import { buildRoomApiMapping } from '../utils/liveSummary'
import { setLastSelectedSala } from '../utils/saleNavigation'
import { setSelectedSiteId } from '../utils/siteSelection'

type CustomRange = {
  from: string
  to: string
}

function norm(value: string) {
  return value.trim().toUpperCase().replace(/\s+/g, ' ')
}

function canon(value: string) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '')
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

function formatRangeLabel(payload: SaleChartsResponse | undefined, preset: ChartRangeKey | null) {
  if (preset) {
    return RANGE_PRESETS.find((entry) => entry.key === preset)?.label || preset
  }
  if (!payload) return 'Custom'
  return `${formatTimestamp(payload.from_ts)} - ${formatTimestamp(payload.to_ts)}`
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

function computeMetricSummary(points: SaleChartPoint[], key: keyof SaleChartPoint) {
  const values = points
    .map((point) => point[key])
    .filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  const latest = values.length ? values[values.length - 1] : null
  const average = values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null
  return { latest, average }
}

function readSummarySignal(summary: PlantSummary | undefined, signalName: string) {
  const value = summary?.signals?.[signalName]?.value
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function computeRealtimeMetricValues(summary: PlantSummary | undefined) {
  const pressione = readSummarySignal(summary, 'Pressione')
  const potenza_kw = readSummarySignal(summary, 'Potenza Attiva TOT')
  const flusso_nm3h = readSummarySignal(summary, 'Flusso') ?? readSummarySignal(summary, 'Flusso TOT')
  const dewpoint = readSummarySignal(summary, 'Dew Point')
  const temperatura = readSummarySignal(summary, 'Temperatura')
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
  if (metricKey === 'potenza_kw') return 'power'
  if (metricKey === 'flusso_nm3h') return 'flow'
  if (metricKey === 'temperatura') return 'temperature'
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
  const [appliedCustomRange, setAppliedCustomRange] = useState<CustomRange | null>(null)
  const [customFromInput, setCustomFromInput] = useState('')
  const [customToInput, setCustomToInput] = useState('')
  const [rangeError, setRangeError] = useState<string | null>(null)

  useLayoutEffect(() => {
    const navigationState = (location.state || {}) as { resetRange?: boolean; scrollToTop?: boolean } | undefined
    if (!navigationState?.resetRange && !navigationState?.scrollToTop) return

    if (navigationState?.resetRange) {
      setRangeError(null)
      setActivePreset(INITIAL_SALA_CHART_RANGE)
      setAppliedCustomRange(null)
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

  const queryOptions = activePreset
    ? { range: activePreset, maxPoints: 360 }
    : appliedCustomRange
      ? { from: appliedCustomRange.from, to: appliedCustomRange.to, maxPoints: 360 }
      : { range: INITIAL_SALA_CHART_RANGE, maxPoints: 360 }

  const realtimeActive = activePreset ? RANGE_PRESETS.find((entry) => entry.key === activePreset)?.realtime ?? false : false

  const chartsQuery = useQuery({
    queryKey: ['sale-charts', selectedSale, queryOptions.range || null, queryOptions.from || null, queryOptions.to || null],
    queryFn: () => fetchSaleCharts(selectedSale, queryOptions),
    enabled: Boolean(selectedSale),
    staleTime: realtimeActive ? LIVE_SUMMARY_REFRESH_MS : 60_000,
    cacheTime: 300_000,
    refetchInterval: realtimeActive ? LIVE_SUMMARY_REFRESH_MS : false,
    refetchIntervalInBackground: realtimeActive,
    refetchOnWindowFocus: realtimeActive,
    retry: 1,
  })

  const compressorQuery = useQuery({
    queryKey: ['sale-compressor-activity', selectedSale, queryOptions.range || null, queryOptions.from || null, queryOptions.to || null],
    queryFn: () => fetchSaleCompressorActivity(selectedSale, queryOptions),
    enabled: Boolean(selectedSale),
    staleTime: realtimeActive ? LIVE_SUMMARY_REFRESH_MS : 60_000,
    cacheTime: 300_000,
    refetchInterval: realtimeActive ? LIVE_SUMMARY_REFRESH_MS : false,
    refetchIntervalInBackground: realtimeActive,
    refetchOnWindowFocus: realtimeActive,
    retry: 1,
  })

  const realtimeSummaryQuery = useQuery({
    queryKey: ['sale-realtime-summary', selectedSale],
    queryFn: () => fetchPlantSummary(selectedSale),
    enabled: Boolean(selectedSale),
    staleTime: LIVE_SUMMARY_REFRESH_MS,
    cacheTime: 120_000,
    refetchInterval: LIVE_SUMMARY_REFRESH_MS,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: true,
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

  const siteSaleButtons = saleMappingsBySite.get(selectedSite) || []
  const selectedSaleLabel = saleLabelByCode.get(selectedSale) || chartPayload?.sale_name || selectedSale
  const saleTitle = selectedSaleLabel || 'Sala'
  const metricCards = SALA_METRICS.map((metric) => ({
    ...metric,
    data: (chartPayload?.points || []).map((point) => ({
      timestamp: point.timestamp,
      value: point[metric.key],
    })),
    thresholds: getSalaMetricThresholds(selectedSale, metric.key),
  }))
  const realtimeMetricValues = computeRealtimeMetricValues(realtimeSummaryQuery.data)

  const summaryPressure = computeMetricSummary(chartPayload?.points || [], 'pressione')
  const summaryPower = computeMetricSummary(chartPayload?.points || [], 'potenza_kw')
  const summaryFlow = computeMetricSummary(chartPayload?.points || [], 'flusso_nm3h')
  const summaryCs = computeMetricSummary(chartPayload?.points || [], 'cons_specifico')
  const csReferenceLine = getSalaMetricThresholds(selectedSale, 'cons_specifico').find((line) => line.label === 'CS realizzabile') || null
  const metricsWithoutData = SALA_METRICS.filter((metric) =>
    !(chartPayload?.points || []).some((point) => typeof point[metric.key] === 'number' && Number.isFinite(point[metric.key]))
  )

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

  const handlePresetSelect = (preset: ChartRangeKey) => {
    setRangeError(null)
    startTransition(() => {
      setActivePreset(preset)
      setAppliedCustomRange(null)
    })
  }

  const handleApplyCustomRange = () => {
    if (!customFromInput || !customToInput) {
      setRangeError('Imposta sia data/ora iniziale sia finale.')
      return
    }
    const fromIso = localInputToIso(customFromInput)
    const toIso = localInputToIso(customToInput)
    if (new Date(fromIso).getTime() >= new Date(toIso).getTime()) {
      setRangeError('La data finale deve essere successiva a quella iniziale.')
      return
    }
    setRangeError(null)
    startTransition(() => {
      setActivePreset(null)
      setAppliedCustomRange({ from: fromIso, to: toIso })
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
      setActivePreset(null)
      setAppliedCustomRange({ from: fromIso, to: toIso })
    })
  }

  const compressorItems: CompressorActivityItem[] = compressorPayload?.items || []

  return (
    <AppLayout
      title={`Storico e trend - ${saleTitle}`}
      subtitle={currentSite || chartPayload?.plant || ''}
      plant={selectedSite}
      onPlantChange={handleSiteChange}
      selectorOptions={allowedSites}
      selectorPlaceholder="Seleziona impianto"
      scadaPlant={selectedSaleLabel}
      chartsPlant={selectedSaleLabel}
    >
      <div className="space-y-8">
        <Card className="border-slate-200/80 shadow-[0_12px_28px_-20px_rgba(15,23,42,0.35)]">
          <CardContent className="flex flex-wrap gap-2 p-4">
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
                      ? 'border-teal-600 bg-teal-600 text-white shadow-[0_10px_24px_-16px_rgba(13,148,136,0.85)] ring-2 ring-teal-100'
                      : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50',
                  ].join(' ')}
                >
                  {item.label}
                </button>
              )
            })}
          </CardContent>
        </Card>

        <Card className="overflow-hidden border-slate-200/80 bg-[radial-gradient(circle_at_top_left,_rgba(15,118,110,0.06),_transparent_40%),linear-gradient(180deg,_#ffffff,_#f8fafc)] shadow-[0_22px_50px_-34px_rgba(15,23,42,0.42)]">
          <CardContent className="flex flex-col gap-7 p-6 xl:grid xl:grid-cols-[minmax(270px,330px)_1fr] xl:items-end">
            <div className="space-y-5 xl:max-w-[330px]">
              <div className="space-y-4">
                <div className="flex flex-wrap items-start gap-3">
                  <div className="space-y-1">
                    <h2 className="text-[2rem] font-bold leading-none tracking-[-0.03em] text-slate-950">{saleTitle}</h2>
                    {showMultiSiteMeta ? (
                      <div className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-600">{currentSite || chartPayload?.plant || '--'}</div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    onClick={() => navigate(`/scada/${encodeURIComponent(selectedSaleLabel)}`)}
                    className="mt-0.5 inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white/90 px-4 py-2 text-sm font-semibold text-slate-700 shadow-sm transition hover:-translate-y-0.5 hover:border-slate-300 hover:bg-slate-50 hover:shadow-md"
                  >
                    <ScadaIcon />
                    Mostra SCADA
                  </button>
                </div>
                <div className="space-y-3 rounded-2xl border border-slate-200/80 border-l-[3px] border-l-slate-300 bg-[linear-gradient(180deg,_rgba(255,255,255,0.96),_rgba(248,250,252,0.94))] px-4 py-3.5 text-[13px] text-slate-700 shadow-[0_12px_24px_-22px_rgba(15,23,42,0.45)] xl:max-w-[19rem]">
                  {showMultiSiteMeta ? (
                    <div className="flex flex-col gap-1">
                      <span className="inline-flex items-center gap-2 font-semibold text-slate-900">
                        <span className="flex h-7 w-7 items-center justify-center rounded-full bg-slate-100 text-slate-600">
                          <FieldIcon kind={fieldKindForMetric('last_update')} />
                        </span>
                        <span>Ultimo aggiornamento</span>
                      </span>
                      <span className="pl-9 text-[13px] font-medium text-slate-700">{formatTimestamp(chartPayload?.last_update || latestPointTs || null)}</span>
                    </div>
                  ) : null}
                  <div className="flex flex-col gap-1">
                    <span className="inline-flex items-center gap-2 font-semibold text-slate-900">
                      <span className="flex h-7 w-7 items-center justify-center rounded-full bg-slate-100 text-slate-600">
                        <FieldIcon kind={fieldKindForMetric('range')} />
                      </span>
                      <span>Periodo selezionato</span>
                    </span>
                    <span className="pl-9 text-[13px] font-medium text-slate-700">{formatRangeLabel(chartPayload, activePreset)}</span>
                  </div>
                </div>
              </div>
            </div>
            <div className="min-w-0 grid gap-3 sm:grid-cols-2 xl:-translate-x-2 xl:grid-cols-[minmax(0,1.18fr)_minmax(0,1.18fr)_minmax(0,0.9fr)_minmax(0,0.98fr)_minmax(15.5rem,1.22fr)]">
              {[
                { label: 'Flusso medio', value: `${formatNumeric(summaryFlow.average)} Nm3/h`, emphasis: true },
                { label: 'Potenza media', value: `${formatNumeric(summaryPower.average)} kW`, emphasis: true },
                { label: 'Pressione media', value: `${formatNumeric(summaryPressure.average)} barg`, emphasis: false },
                { label: 'Consumo specifico medio', value: `${formatNumeric(summaryCs.average, 3)} kWh/Nm3`, emphasis: false },
                ...(csReferenceLine
                  ? [{ label: 'Consumo specifico contratto', value: `${formatNumeric(csReferenceLine.value, 3)} kWh/Nm3`, emphasis: false }]
                  : []),
              ].map((item, index) => (
                <div
                  key={item.label}
                  className={[
                    'rounded-2xl border border-slate-200/70 bg-white/92 px-4 py-3.5 shadow-[0_14px_26px_-24px_rgba(15,23,42,0.55)] backdrop-blur transition duration-200 hover:-translate-y-0.5 hover:shadow-[0_18px_32px_-24px_rgba(15,23,42,0.35)]',
                    index === 4 ? 'xl:min-w-[15.5rem]' : '',
                  ].join(' ')}
                >
                  <div className="inline-flex items-center gap-2 whitespace-nowrap text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">
                    <FieldIcon
                      kind={
                        item.label.startsWith('Flusso')
                          ? fieldKindForMetric('flusso_nm3h')
                          : item.label.startsWith('Potenza')
                            ? fieldKindForMetric('potenza_kw')
                            : item.label.startsWith('Pressione')
                              ? fieldKindForMetric('pressione')
                              : item.label.startsWith('Consumo')
                                ? fieldKindForMetric('cons_specifico')
                                : fieldKindForMetric('range')
                      }
                    />
                    <span>{item.label}</span>
                  </div>
                  <div className={item.emphasis ? 'mt-1.5 text-xl font-bold tracking-[-0.02em] text-slate-950 xl:text-[1.45rem]' : 'mt-1.5 text-lg font-bold tracking-[-0.02em] text-slate-950 xl:text-[1.18rem]'}>{item.value}</div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="border-slate-200/80 shadow-[0_18px_45px_-30px_rgba(15,23,42,0.35)]">
          <CardHeader className="border-b border-slate-100/90 pb-5">
            <CardTitle className="text-slate-900">Selezione periodo</CardTitle>
          </CardHeader>
          <CardContent className="space-y-7 pt-5">
            <RangePresetSelector presets={RANGE_PRESETS} activeKey={activePreset} onSelect={handlePresetSelect} />

            <CustomDateRangePicker
              fromValue={customFromInput}
              toValue={customToInput}
              onApplyRange={handleApplyCustomRangeValues}
              error={rangeError}
            />

            {showDevMeta ? (
              <div className="flex flex-wrap gap-2 text-xs text-slate-500">
                <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1">Tabella: {chartPayload?.source_table || '--'}</span>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1">Granularita: {chartPayload?.granularity || '--'}</span>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1">Punti: {chartPayload?.points.length ?? 0}</span>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1">Compressori: {compressorItems.length}</span>
              </div>
            ) : null}
          </CardContent>
        </Card>

        {(chartsQuery.isLoading || compressorQuery.isLoading) && !chartPayload ? (
          <div className="grid gap-4 xl:grid-cols-2">
            {Array.from({ length: 4 }).map((_, index) => (
              <Skeleton key={index} className="h-[27rem] w-full rounded-3xl" />
            ))}
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

        <div className="grid gap-4 xl:grid-cols-2">
          {metricCards.map((metric) => (
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
              loading={chartsQuery.isFetching && !chartPayload}
              currentValue={realtimeMetricValues[metric.key] ?? null}
            />
          ))}
        </div>

        <CompressorActivityPanel items={compressorItems} loading={compressorQuery.isLoading && !compressorPayload} />
      </div>
    </AppLayout>
  )
}

