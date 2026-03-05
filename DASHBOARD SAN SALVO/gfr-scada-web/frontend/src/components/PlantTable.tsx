import { Fragment, Suspense, lazy, useMemo, useState } from 'react'
import type { PlantRow, PlantStatus } from '../types/plantTable'
import CompressorsTable from './compressors/CompressorsTable'
import PowerMetrics from './compressors/PowerMetrics'
import { Card, CardContent, CardHeader, CardTitle } from './ui/Card'

type FilterKey = 'all' | 'active' | 'anomaly' | 'stale'

interface PlantTableProps {
  rows: PlantRow[]
  selectedSala: string
  onSelectSala: (sala: string) => void
  siteId: 'san-salvo' | 'marghera'
}

interface DecoratedPlantRow extends PlantRow {
  flowValue: number | null
  updateMs: number | null
  ageMs: number | null
  isStale: boolean
  isWarningStale: boolean
  isAnomaly: boolean
}

interface SynopticMachineSelection {
  machineId: string
  slot: string
  label: string
}

const STALE_MS = 60_000
const STALE_WARNING_MS = 120_000
const SynopticPanel = lazy(() => import('./synoptic/SynopticPanel'))

function parseUpdateMs(value: string | null): number | null {
  if (!value) return null
  const trimmed = value.trim()
  if (!trimmed) return null

  if (/^\d{2}:\d{2}:\d{2}$/.test(trimmed)) {
    const now = new Date()
    const [hh, mm, ss] = trimmed.split(':').map((part) => Number(part))
    const point = new Date(now.getFullYear(), now.getMonth(), now.getDate(), hh, mm, ss, 0)
    return Number.isFinite(point.getTime()) ? point.getTime() : null
  }

  const ms = new Date(trimmed).getTime()
  return Number.isFinite(ms) ? ms : null
}

function formatLastUpdateTime(value: string | null) {
  if (!value) return '—'
  const trimmed = value.trim()
  if (!trimmed) return '—'
  const hhmmss = trimmed.match(/(\d{2}:\d{2}:\d{2})/)
  if (hhmmss) return hhmmss[1]
  const parsed = new Date(trimmed)
  if (!Number.isFinite(parsed.getTime())) return '—'
  return parsed.toLocaleTimeString('it-IT', { hour12: false, timeZone: 'UTC' })
}

function formatOneDecimal(value: number | null) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—'
  return value.toFixed(1)
}

function formatThreeDecimals(value: number | null) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—'
  return value.toFixed(3)
}

function formatPercent(value: number | null) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—'
  return `${value.toFixed(1)}%`
}

function parseSignalTsMs(value: string | null | undefined) {
  if (!value) return null
  const ms = new Date(value).getTime()
  return Number.isFinite(ms) ? ms : null
}

function extractSignalVariants(
  signals: PlantRow['detailSignals'],
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

    const indexMatch = name.match(/\((\d+)\)\s*$/)
    const index = indexMatch ? Number(indexMatch[1]) : 1
    const tsMs = parseSignalTsMs(info?.ts)
    const prev = byIndex.get(index)

    if (!prev || (tsMs != null && (prev.tsMs == null || tsMs >= prev.tsMs))) {
      byIndex.set(index, { value, tsMs })
    }
  }

  return Array.from(byIndex.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([, entry]) => entry.value)
}

function statusPill(status: PlantStatus) {
  const base = 'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium leading-5'
  if (status === 'active') {
    return (
      <span className={`${base} border-[#9ddfb9] bg-[#e9fbf3] text-[#118a52]`}>
        <span className="inline-block h-2 w-2 rounded-full bg-[#58d68d]" />
        active
      </span>
    )
  }
  if (status === 'idle') {
    return (
      <span className={`${base} border-[#ebcf80] bg-[#fff8df] text-[#996300]`}>
        <span className="inline-block h-2 w-2 rounded-full bg-[#e2b73b]" />
        standby
      </span>
    )
  }
  if (status === 'dism') {
    return (
      <span className={`${base} border-slate-300 bg-slate-100 text-slate-700`}>
        <span className="inline-block h-2 w-2 rounded-full bg-slate-500" />
        dismissed
      </span>
    )
  }
  return (
    <span className={`${base} border-slate-200 bg-slate-50 text-slate-600`}>
      <span className="inline-block h-2 w-2 rounded-full bg-slate-400" />
      n/d
    </span>
  )
}

function progressColor(value: number) {
  if (value > 80) return 'bg-rose-500'
  if (value >= 50) return 'bg-amber-500'
  return 'bg-emerald-500'
}

export default function PlantTable({ rows, selectedSala, onSelectSala, siteId }: PlantTableProps) {
  const [filter, setFilter] = useState<FilterKey>('all')
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [synopticOpen, setSynopticOpen] = useState<Record<string, boolean>>({})
  const [synopticSelectionBySala, setSynopticSelectionBySala] = useState<Record<string, SynopticMachineSelection>>({})
  const nowMs = Date.now()

  const decoratedRows = useMemo<DecoratedPlantRow[]>(() => {
    return rows.map((row) => {
      const flowValue =
        typeof row.realtimeNm3 === 'number' && Number.isFinite(row.realtimeNm3)
          ? row.realtimeNm3
          : typeof row.flussoMedio === 'number' && Number.isFinite(row.flussoMedio)
            ? row.flussoMedio
            : null
      const updateMs = parseUpdateMs(row.lastUpdate)
      const ageMs = updateMs == null ? null : Math.max(0, nowMs - updateMs)
      const isStale = ageMs != null && ageMs > STALE_MS
      const isWarningStale = ageMs != null && ageMs > STALE_WARNING_MS
      const isAnomaly =
        row.status === 'active' &&
        ((typeof row.potenzaMedia === 'number' && row.potenzaMedia === 0) ||
          (typeof flowValue === 'number' && flowValue === 0))

      return {
        ...row,
        flowValue,
        updateMs,
        ageMs,
        isStale,
        isWarningStale,
        isAnomaly,
      }
    })
  }, [rows, nowMs])

  const visibleRows = useMemo(() => {
    const searchNorm = search.trim().toLowerCase()
    const filtered = decoratedRows.filter((row) => {
      if (searchNorm && !row.sala.toLowerCase().includes(searchNorm)) return false
      if (filter === 'active') return row.status === 'active'
      if (filter === 'anomaly') return row.isAnomaly
      if (filter === 'stale') return row.isStale
      return true
    })

    return filtered.sort((a, b) => {
      if (a.isAnomaly !== b.isAnomaly) return a.isAnomaly ? -1 : 1
      if (a.isStale !== b.isStale) return a.isStale ? -1 : 1
      const pctA = typeof a.percentEnergiaConsumata === 'number' ? a.percentEnergiaConsumata : -1
      const pctB = typeof b.percentEnergiaConsumata === 'number' ? b.percentEnergiaConsumata : -1
      if (pctA !== pctB) return pctB - pctA
      return a.sala.localeCompare(b.sala, 'it')
    })
  }, [decoratedRows, filter, search])

  const counts = useMemo(() => {
    const active = decoratedRows.filter((row) => row.status === 'active').length
    const anomaly = decoratedRows.filter((row) => row.isAnomaly).length
    const stale = decoratedRows.filter((row) => row.isStale).length
    return { all: decoratedRows.length, active, anomaly, stale }
  }, [decoratedRows])

  const filterButtons: Array<{ key: FilterKey; label: string; count: number }> = [
    { key: 'all', label: 'Tutte', count: counts.all },
    { key: 'active', label: 'Active', count: counts.active },
    { key: 'anomaly', label: 'Anomalie', count: counts.anomaly },
    { key: 'stale', label: 'Stale', count: counts.stale },
  ]

  return (
    <Card>
      <CardHeader className="space-y-3">
        <CardTitle className="text-slate-900">Tabella Operativa Sale</CardTitle>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-wrap gap-2">
            {filterButtons.map((item) => (
              <button
                key={item.key}
                type="button"
                onClick={() => setFilter(item.key)}
                className={[
                  'rounded-full border px-3 py-1 text-xs font-medium',
                  filter === item.key
                    ? 'border-teal-500 bg-teal-50 text-teal-700'
                    : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300',
                ].join(' ')}
              >
                {item.label} ({item.count})
              </button>
            ))}
          </div>
          <input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Cerca sala..."
            className="w-full rounded-md border border-slate-200 px-3 py-2 text-sm text-slate-700 outline-none ring-teal-300 placeholder:text-slate-400 focus:ring lg:max-w-xs"
          />
        </div>
      </CardHeader>

      <CardContent>
        <div className="mb-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          <span className="font-semibold">!</span> Active con kW o flusso a zero: possibile perdita dati, sensore fermo o mapping errato.
        </div>

        <div className="overflow-x-auto rounded-md border border-slate-200">
          <table className="w-full min-w-[1220px] text-[13px] [&_th]:px-3 [&_td]:px-3 [&_th]:py-3 [&_td]:py-3 [&_th]:whitespace-nowrap [&_td]:whitespace-nowrap [&_th:first-child]:pl-4 [&_td:first-child]:pl-4 [&_th:last-child]:pr-4 [&_td:last-child]:pr-4">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50 text-left text-[10px] font-medium uppercase tracking-[0.06em] text-slate-500">
                <th>Sala</th>
                <th>Stato</th>
                <th>Last update</th>
                <th className="text-right">Flusso (Nm3/h)</th>
                <th className="text-right">kW</th>
                <th className="text-right">Pressione (bar)</th>
                <th className="text-right">Temperatura (degC)</th>
                <th>% energia consumata</th>
                <th className="text-center">Allarmi</th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((row) => {
                const isSelected = row.sala === selectedSala
                const isExpanded = Boolean(expanded[row.sala])
                const progress =
                  typeof row.percentEnergiaConsumata === 'number'
                    ? Math.max(0, Math.min(100, row.percentEnergiaConsumata))
                    : 0
                const progressClass = progressColor(progress)
                const hasDualPressureTemp = /^SS2\b/i.test(row.sala)
                const pressureVariants = extractSignalVariants(row.detailSignals, 'pressure')
                const temperatureVariants = extractSignalVariants(row.detailSignals, 'temperature')
                const pressure1 = pressureVariants[0] ?? row.pressioneMedia
                const pressure2 = pressureVariants[1] ?? null
                const temperature1 = temperatureVariants[0] ?? row.temperaturaMedia
                const temperature2 = temperatureVariants[1] ?? null
                const updateLabel = row.updateMs == null ? '—' : formatLastUpdateTime(row.lastUpdate)

                return (
                  <Fragment key={row.sala}>
                    <tr
                      role="button"
                      tabIndex={0}
                      aria-pressed={isSelected}
                      onClick={() => onSelectSala(row.sala)}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault()
                          onSelectSala(row.sala)
                        }
                      }}
                      className={[
                        'border-b border-slate-200 transition-colors hover:bg-slate-50/80 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-300',
                        isSelected ? 'bg-teal-50' : '',
                        row.isStale ? 'opacity-80' : '',
                        row.isAnomaly ? 'border-l-4 border-l-rose-500 bg-rose-50/40' : '',
                      ].join(' ')}
                    >
                      <td className="font-medium text-slate-900">
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation()
                              setExpanded((prev) => ({ ...prev, [row.sala]: !prev[row.sala] }))
                            }}
                            className="inline-flex h-5 w-5 items-center justify-center rounded border border-slate-300 text-[11px] text-slate-600 hover:bg-slate-100"
                            title={isExpanded ? 'Chiudi dettagli' : 'Apri dettagli'}
                          >
                            {isExpanded ? '-' : '+'}
                          </button>
                          <span>{row.sala}</span>
                        </div>
                      </td>
                      <td>{statusPill(row.status)}</td>
                      <td className="text-slate-700">
                        <span>{updateLabel}</span>
                        {row.isStale ? (
                          <span
                            className={[
                              'ml-2 rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide',
                              row.isWarningStale
                                ? 'border-rose-300 bg-rose-100 text-rose-700'
                                : 'border-amber-300 bg-amber-100 text-amber-700',
                            ].join(' ')}
                          >
                            STALE
                          </span>
                        ) : null}
                      </td>
                      <td className="text-right tabular-nums text-[14px] font-semibold text-slate-900">
                        {formatOneDecimal(row.flowValue)}
                      </td>
                      <td className="text-right tabular-nums text-[14px] font-semibold text-slate-900">
                        {formatOneDecimal(row.potenzaMedia)}
                      </td>
                      <td className="text-right tabular-nums text-slate-500">{formatOneDecimal(row.pressioneMedia)}</td>
                      <td className="text-right tabular-nums text-slate-500">{formatOneDecimal(row.temperaturaMedia)}</td>
                      <td>
                        {typeof row.percentEnergiaConsumata === 'number' ? (
                          <div className="flex items-center gap-2">
                            <div className="h-2.5 w-24 rounded-full bg-slate-100">
                              <div className={`h-2.5 rounded-full ${progressClass}`} style={{ width: `${progress}%` }} />
                            </div>
                            <span className="text-xs font-medium text-slate-700">
                              {formatPercent(row.percentEnergiaConsumata)}
                            </span>
                          </div>
                        ) : (
                          <span className="text-slate-400">—</span>
                        )}
                      </td>
                      <td className="text-center">
                        {row.isAnomaly ? (
                          <div
                            className="inline-flex items-center gap-1 rounded-full border border-rose-200 bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-700"
                            title="Active con kW o flusso a zero: possibile perdita dati, sensore fermo o mapping errato."
                          >
                            <span>!</span>
                            <span>1</span>
                          </div>
                        ) : row.isStale ? (
                          <span
                            className="inline-flex rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-700"
                            title="Telemetria stale: ultimo aggiornamento troppo vecchio."
                          >
                            T
                          </span>
                        ) : (
                          <span className="text-slate-400">—</span>
                        )}
                      </td>
                    </tr>

                    {isExpanded ? (
                      <tr className="border-b border-slate-200 bg-slate-50/60">
                        <td colSpan={9} className="pb-3 pt-2">
                          <div className="rounded-lg border border-slate-200 bg-slate-100 p-3">
                            <div className="mb-3 text-base font-semibold text-slate-900">
                              <div className="flex flex-wrap items-center justify-between gap-2">
                                <span>{`Sala ${row.sala} - Valori attuali`}</span>
                                <div className="flex items-center gap-2">
                                  <span className="text-xs font-medium text-slate-600">{`Last update ${updateLabel}`}</span>
                                  <button
                                    type="button"
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      setSynopticOpen((prev) => ({ ...prev, [row.sala]: !prev[row.sala] }))
                                    }}
                                    className="rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                                  >
                                    {synopticOpen[row.sala] ? 'Chiudi SCADA' : 'Apri SCADA'}
                                  </button>
                                </div>
                              </div>
                            </div>

                            <PowerMetrics
                              items={[
                                { label: 'Flusso', unit: 'Nm3/h', value: formatOneDecimal(row.flowValue) },
                                { label: 'Potenza Attiva', unit: 'kW', value: formatOneDecimal(row.potenzaMedia) },
                                { label: 'CS Attuale', unit: 'kWh/Nm3', value: formatThreeDecimals(row.csPeriodo), highlight: true },
                                { label: 'CS Contratto', unit: 'kWh/Nm3', value: formatThreeDecimals(row.csContratto) },
                                ...(hasDualPressureTemp
                                  ? [
                                      { label: 'Pressione 1', unit: 'bar', value: formatOneDecimal(pressure1) },
                                      { label: 'Pressione 2', unit: 'bar', value: formatOneDecimal(pressure2) },
                                    ]
                                  : [{ label: 'Pressione', unit: 'bar', value: formatOneDecimal(row.pressioneMedia) }]),
                                { label: 'Dew Point', unit: 'degC', value: formatOneDecimal(row.dewPointMedia) },
                                ...(hasDualPressureTemp
                                  ? [
                                      { label: 'Temperatura 1', unit: 'degC', value: formatOneDecimal(temperature1) },
                                      { label: 'Temperatura 2', unit: 'degC', value: formatOneDecimal(temperature2) },
                                    ]
                                  : [{ label: 'Temperatura', unit: 'degC', value: formatOneDecimal(row.temperaturaMedia) }]),
                                { label: 'Last update', unit: '', value: updateLabel },
                              ]}
                            />

                            {synopticOpen[row.sala] ? (
                              <Suspense
                                fallback={
                                    <div className="mt-3 rounded-lg border border-slate-200 bg-white px-3 py-4 text-sm text-slate-500">
                                    Caricamento SCADA...
                                  </div>
                                }
                              >
                                <SynopticPanel
                                  siteId={siteId}
                                  roomId={row.sala}
                                  roomLabel={row.sala}
                                  lastUpdate={updateLabel}
                                  signals={row.detailSignals}
                                  flowNm3h={row.flowValue}
                                  pressureBar={row.pressioneMedia}
                                  temperatureC={row.temperaturaMedia}
                                  dewPointC={row.dewPointMedia}
                                  powerTotalKw={row.potenzaMedia}
                                  csAttuale={row.csPeriodo}
                                  onMachineSelect={(machine) =>
                                    setSynopticSelectionBySala((prev) => ({ ...prev, [row.sala]: machine }))
                                  }
                                  onClose={() => setSynopticOpen((prev) => ({ ...prev, [row.sala]: false }))}
                                />
                              </Suspense>
                            ) : null}

                            <CompressorsTable
                              sala={row.sala}
                              signals={row.detailSignals}
                              selectedMachine={synopticSelectionBySala[row.sala] || null}
                            />
                          </div>
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                )
              })}

              {visibleRows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="py-8 text-center text-sm text-slate-500">
                    Nessuna sala trovata con i filtri selezionati.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
}
