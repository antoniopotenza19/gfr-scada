import { startTransition, useEffect, useMemo, useState } from 'react'
import { Navigate, useNavigate, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'

import { fetchSaleCharts, fetchSaleCompressorActivity } from '../api/sale'
import AppLayout from '../components/layout/AppLayout'
import CompressorActivityPanel from '../components/sala-charts/CompressorActivityPanel'
import CustomDateRangePicker from '../components/sala-charts/CustomDateRangePicker'
import MetricTrendCard from '../components/sala-charts/MetricTrendCard'
import RangePresetSelector from '../components/sala-charts/RangePresetSelector'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import Skeleton from '../components/ui/Skeleton'
import { DEFAULT_REALTIME_RANGE, INITIAL_SALA_CHART_RANGE, RANGE_PRESETS, SALA_METRICS, getSalaMetricThresholds, type ChartRangeKey } from '../constants/salaCharts'
import { SITE_ROOMS } from '../constants/siteRooms'
import { legacyKeyToSiteId } from '../constants/sites'
import { LIVE_SUMMARY_REFRESH_MS } from '../config/live'
import { usePlants } from '../hooks/usePlants'
import type { CompressorActivityItem, SaleChartPoint, SaleChartsResponse } from '../types/api'
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
  return date.toLocaleString('it-IT')
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

function formatNumeric(value: number | null, fractionDigits: number = 2) {
  if (value == null || !Number.isFinite(value)) return '--'
  return new Intl.NumberFormat('it-IT', { maximumFractionDigits: fractionDigits }).format(value)
}

export default function SalaChartsPage() {
  const { saleId } = useParams()
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
  const latestPointTs = chartPayload && chartPayload.points.length > 0
    ? chartPayload.points[chartPayload.points.length - 1].timestamp
    : null
  const showDevMeta = canViewDevFeatures(authUser)

  const siteSaleButtons = saleMappingsBySite.get(selectedSite) || []
  const selectedSaleLabel = saleLabelByCode.get(selectedSale) || chartPayload?.sale_name || selectedSale
  const saleTitle = selectedSaleLabel || 'Sala'
  const subtitleParts = [
    currentSite || chartPayload?.plant || null,
    `Intervallo: ${formatRangeLabel(chartPayload, activePreset)}`,
    `Ultimo update: ${formatTimestamp(chartPayload?.last_update || latestPointTs || null)}`,
  ].filter(Boolean)

  const metricCards = SALA_METRICS.map((metric) => ({
    ...metric,
    data: (chartPayload?.points || []).map((point) => ({
      timestamp: point.timestamp,
      value: point[metric.key],
    })),
    thresholds: getSalaMetricThresholds(selectedSale, metric.key),
  }))

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

  const handleResetRealtime = () => {
    setRangeError(null)
    startTransition(() => {
      setActivePreset(DEFAULT_REALTIME_RANGE)
      setAppliedCustomRange(null)
    })
  }

  const handleRefresh = () => {
    chartsQuery.refetch()
    compressorQuery.refetch()
  }

  const compressorItems: CompressorActivityItem[] = compressorPayload?.items || []

  return (
    <AppLayout
      title={`Storico e trend - ${saleTitle}`}
      subtitle={subtitleParts.join(' - ')}
      plant={selectedSite}
      onPlantChange={handleSiteChange}
      selectorOptions={allowedSites}
      selectorPlaceholder="Seleziona impianto"
      scadaPlant={selectedSaleLabel}
      chartsPlant={selectedSaleLabel}
    >
      <div className="space-y-5">
        <Card className="border-slate-200/80 shadow-[0_10px_25px_-18px_rgba(15,23,42,0.4)]">
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
                      ? 'border-teal-500 bg-teal-50 text-teal-700'
                      : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50',
                  ].join(' ')}
                >
                  {item.label}
                </button>
              )
            })}
          </CardContent>
        </Card>

        <Card className="overflow-hidden border-slate-200/80 bg-[radial-gradient(circle_at_top_left,_rgba(15,118,110,0.08),_transparent_38%),linear-gradient(180deg,_#ffffff,_#f8fafc)] shadow-[0_20px_55px_-32px_rgba(15,23,42,0.45)]">
          <CardContent className="flex flex-col gap-5 p-5 xl:grid xl:grid-cols-[minmax(240px,300px)_1fr] xl:items-end">
            <div className="space-y-3 xl:max-w-[300px]">
              <div className="inline-flex items-center rounded-full border border-teal-200 bg-teal-50 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-teal-700">
                Sala charts
              </div>
              <div>
                <div className="flex flex-wrap items-center gap-3">
                  <h2 className="text-2xl font-semibold text-slate-950">{saleTitle}</h2>
                  <button
                    type="button"
                    onClick={() => navigate(`/scada/${encodeURIComponent(selectedSaleLabel)}`)}
                    className="rounded-xl border border-teal-200 bg-teal-50 px-4 py-2 text-sm font-semibold text-teal-700 transition hover:border-teal-300 hover:bg-teal-100"
                  >
                    Torna alla sala
                  </button>
                </div>
                <p className="hidden">
                  Vista analitica dedicata ai trend storici e realtime della sala compressori, ottimizzata per lettura rapida,
                  confronto metriche e attività macchine.
                </p>
              </div>
            </div>
            <div className="min-w-0 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
              {[
                { label: 'Pressione media', value: `${formatNumeric(summaryPressure.average)} barg` },
                { label: 'Potenza media', value: `${formatNumeric(summaryPower.average)} kW` },
                { label: 'Flusso medio', value: `${formatNumeric(summaryFlow.average)} Nm3/h` },
                { label: 'CS medio', value: `${formatNumeric(summaryCs.average, 3)} kWh/Nm3` },
                ...(csReferenceLine
                  ? [{ label: 'CS realizzabile', value: `${formatNumeric(csReferenceLine.value, 3)} kWh/Nm3` }]
                  : []),
              ].map((item) => (
                <div key={item.label} className="rounded-2xl border border-white/70 bg-white/85 px-3 py-3 shadow-sm backdrop-blur">
                  <div className="text-[11px] uppercase tracking-[0.16em] text-slate-400">{item.label}</div>
                  <div className="mt-1 text-base font-semibold text-slate-900 xl:text-lg">{item.value}</div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="border-slate-200/80 shadow-[0_18px_45px_-28px_rgba(15,23,42,0.35)]">
          <CardHeader>
            <CardTitle className="text-slate-900">Filtri analisi</CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            <div className="space-y-3">
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Range rapido</div>
              <RangePresetSelector presets={RANGE_PRESETS} activeKey={activePreset} onSelect={handlePresetSelect} />
            </div>

            <CustomDateRangePicker
              fromValue={customFromInput}
              toValue={customToInput}
              onApplyRange={handleApplyCustomRangeValues}
              onResetRealtime={handleResetRealtime}
              onRefresh={handleRefresh}
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
              unit={metric.unit}
              color={metric.color}
              accentClassName={metric.accent}
              description={metric.description}
              data={metric.data}
              thresholds={metric.thresholds}
              loading={chartsQuery.isFetching && !chartPayload}
            />
          ))}
        </div>

        <CompressorActivityPanel items={compressorItems} loading={compressorQuery.isLoading && !compressorPayload} />
      </div>
    </AppLayout>
  )
}
