import { Fragment, useEffect, useMemo, useState } from 'react'
import type { PlantRow, PlantStatus } from '../types/plantTable'
import CompressorsTable from './compressors/CompressorsTable'
import PowerMetrics from './compressors/PowerMetrics'
import ScadaSala, { type ScadaMachine, type ScadaMachineStatus } from './synoptic/ScadaSala'
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

const STALE_MS = 60_000
const STALE_WARNING_MS = 120_000

type ParsedMachine = {
  rawName: string
  u1: number | null
  u2: number | null
  u3: number | null
  i1: number | null
  i2: number | null
  i3: number | null
  cosphi: number | null
  activePowerKw: number | null
}

const LAMINATO_MACHINE_MAP: Array<{ id: string; name: string; aliases: string[]; imageUrl: string }> = [
  { id: 'C1', name: 'SIAD 1850', aliases: ['BOOSTER', 'TEMPO 2 1850', 'TEMPO2', 'SIAD 1850'], imageUrl: '/images/scada/siadbooster.png' },
  { id: 'C2', name: 'CREPELLE N.2 P27-200', aliases: ['CREPELLE N2', 'CREPELLE N 2', 'CREPELLE 2', 'CREPELLEN2'], imageUrl: '/images/scada/crepelle.png' },
  { id: 'C3', name: 'CREPELLE N.3 40P20', aliases: ['CREPELLE N3', 'CREPELLE N 3', 'CREPELLE 3', 'CREPELLEN3'], imageUrl: '/images/scada/crepelle.png' },
]

const BRAVO_MACHINE_MAP: Array<{ id: string; name: string; aliases: string[]; imageUrl: string }> = [
  { id: 'M1', name: 'MATTEI 1', aliases: ['MATTEI N1', 'MATTEI 1', 'M1'], imageUrl: '/images/scada/siadbooster.png' },
  { id: 'M2', name: 'MATTEI 2', aliases: ['MATTEI N2', 'MATTEI 2', 'M2'], imageUrl: '/images/scada/siadbooster.png' },
  { id: 'V1', name: 'GA90 VSD', aliases: ['GA90 VSD', 'GA90', 'V1'], imageUrl: '/images/scada/crepelle.png' },
]

function canonicalToken(value: string) {
  return value.toUpperCase().replace(/N[\u00B0\u00BA]/g, 'N').replace(/[^A-Z0-9]/g, '')
}

function normalizeMachineName(rawName: string) {
  return rawName
    .replace(/\s*\((?:V|A|KW)\)\s*$/i, '')
    .replace(/^3PH\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim()
}

function parseMachines(signals?: PlantRow['detailSignals']) {
  if (!signals) return new Map<string, ParsedMachine>()

  const map = new Map<string, ParsedMachine>()

  const getOrCreate = (name: string) => {
    const raw = normalizeMachineName(name)
    const key = canonicalToken(raw)
    const existing = map.get(key)
    if (existing) return existing
    const created: ParsedMachine = {
      rawName: raw,
      u1: null,
      u2: null,
      u3: null,
      i1: null,
      i2: null,
      i3: null,
      cosphi: null,
      activePowerKw: null,
    }
    map.set(key, created)
    return created
  }

  for (const [signalName, info] of Object.entries(signals)) {
    const signal = signalName.trim()
    const value = Number(info?.value)
    if (!Number.isFinite(value)) continue

    const voltage = signal.match(/^U([123])\s+(.+)$/i)
    if (voltage) {
      const machine = getOrCreate(voltage[2])
      if (voltage[1] === '1') machine.u1 = value
      if (voltage[1] === '2') machine.u2 = value
      if (voltage[1] === '3') machine.u3 = value
      continue
    }

    const current = signal.match(/^[IL]\s*([123])\s+(.+)$/i)
    if (current) {
      const machine = getOrCreate(current[2])
      if (current[1] === '1') machine.i1 = value
      if (current[1] === '2') machine.i2 = value
      if (current[1] === '3') machine.i3 = value
      continue
    }

    const power = signal.match(/^Potenza Attiva\s+(.+)$/i)
    if (power) {
      const machineName = normalizeMachineName(power[1])
      if (!/\bTOT\b|\bTOTAL\b/i.test(machineName)) {
        const machine = getOrCreate(machineName)
        machine.activePowerKw = value
      }
      continue
    }

    const cosphi = signal.match(/^cosphi\s+(.+)$/i)
    if (cosphi) {
      const machine = getOrCreate(cosphi[1])
      machine.cosphi = value
    }
  }

  return map
}

function machineStatus(machine: ParsedMachine | null): ScadaMachineStatus {
  if (!machine) return 'OFFLINE'
  const currents = [machine.i1, machine.i2, machine.i3].filter(
    (value): value is number => value != null && Number.isFinite(value)
  )
  const avgCurrent = currents.length ? currents.reduce((sum, value) => sum + value, 0) / currents.length : 0
  const hasVoltage = [machine.u1, machine.u2, machine.u3].some((value) => value != null && Number.isFinite(value))
  const power = machine.activePowerKw ?? 0
  if ((power > 0.5 || avgCurrent > 1) && ((power === 0 && currents.length > 0) || (avgCurrent === 0 && currents.length > 0))) {
    return 'ALARM'
  }
  if (power > 0.5 || avgCurrent > 1) return 'ACTIVE'
  if (hasVoltage) return 'STANDBY'
  return 'OFFLINE'
}

function resolveMappedMachine(
  rawName: string,
  mapping: Array<{ id: string; name: string; aliases: string[]; imageUrl: string }>
) {
  const key = canonicalToken(rawName)
  return (
    mapping.find((entry) =>
      entry.aliases.some((alias) => {
        const aliasKey = canonicalToken(alias)
        return key === aliasKey || key.includes(aliasKey) || aliasKey.includes(key)
      })
    ) || null
  )
}

function buildMappedMachines(
  mapping: Array<{ id: string; name: string; aliases: string[]; imageUrl: string }>,
  parsedMap: Map<string, ParsedMachine>
) {
  const slots = new Map<string, ParsedMachine>()
  for (const machine of parsedMap.values()) {
    const mapped = resolveMappedMachine(machine.rawName, mapping)
    if (!mapped) continue
    slots.set(mapped.id, machine)
  }

  return mapping.map((entry) => {
    const parsed = slots.get(entry.id) || null
    return {
      id: entry.id,
      name: entry.name,
      kw: Number(parsed?.activePowerKw ?? 0),
      status: machineStatus(parsed),
      imageUrl: entry.imageUrl,
      u1: parsed?.u1 ?? null,
      u2: parsed?.u2 ?? null,
      u3: parsed?.u3 ?? null,
      i1: parsed?.i1 ?? null,
      i2: parsed?.i2 ?? null,
      i3: parsed?.i3 ?? null,
      cosphi: parsed?.cosphi ?? null,
    } satisfies ScadaMachine
  })
}

function buildGenericMachines(parsedMap: Map<string, ParsedMachine>) {
  const parsed = Array.from(parsedMap.values())
    .sort((a, b) => Number(b.activePowerKw || 0) - Number(a.activePowerKw || 0))
    .slice(0, 3)

  const slots = ['M1', 'M2', 'V1']
  return slots.map((slot, index) => {
    const machine = parsed[index] || null
    return {
      id: slot,
      name: machine ? machine.rawName.toUpperCase() : slot,
      kw: Number(machine?.activePowerKw ?? 0),
      status: machineStatus(machine),
      imageUrl: '/images/scada/crepelle.png',
      u1: machine?.u1 ?? null,
      u2: machine?.u2 ?? null,
      u3: machine?.u3 ?? null,
      i1: machine?.i1 ?? null,
      i2: machine?.i2 ?? null,
      i3: machine?.i3 ?? null,
      cosphi: machine?.cosphi ?? null,
    } satisfies ScadaMachine
  })
}

function buildInlineScadaMachines(roomLabel: string, signals?: PlantRow['detailSignals']) {
  const parsedMap = parseMachines(signals)
  const upper = roomLabel.toUpperCase()
  if (upper.includes('LAMINATO') || upper.includes('LAMINATI')) return buildMappedMachines(LAMINATO_MACHINE_MAP, parsedMap)
  if (upper.includes('BRAVO')) return buildMappedMachines(BRAVO_MACHINE_MAP, parsedMap)
  return buildGenericMachines(parsedMap)
}

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

function statusPill(status: PlantStatus, hasWarning = false) {
  const base = 'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium leading-5'
  if (hasWarning) {
    return (
      <span className={`${base} border-rose-300 bg-rose-100 text-rose-700`}>
        <span className="inline-block h-2 w-2 rounded-full bg-rose-500" />
        ! warning
      </span>
    )
  }
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
  void value
  return 'bg-emerald-500'
}

export default function PlantTable({ rows, selectedSala, onSelectSala, siteId }: PlantTableProps) {
  void siteId
  const [filter, setFilter] = useState<FilterKey>('all')
  const [search, setSearch] = useState('')
  const [expandedSala, setExpandedSala] = useState<string | null>(null)
  const [synopticSala, setSynopticSala] = useState<string | null>(null)
  const nowMs = Date.now()

  useEffect(() => {
    if (!selectedSala) return
    setExpandedSala(selectedSala)
    setSynopticSala((prev) => (prev === selectedSala ? prev : null))
  }, [selectedSala])

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
                const isExpanded = expandedSala === row.sala
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
                const scadaMachines = buildInlineScadaMachines(row.sala, row.detailSignals)
                const ss1Warning = row.sala.trim().toUpperCase() === 'SS1' && (row.flowValue ?? 0) > 0.1 && (row.potenzaMedia ?? 0) <= 0

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
                              setExpandedSala((prev) => (prev === row.sala ? null : row.sala))
                              setSynopticSala((prev) => (prev === row.sala ? prev : null))
                            }}
                            className="inline-flex h-5 w-5 items-center justify-center rounded border border-slate-300 text-[11px] text-slate-600 hover:bg-slate-100"
                            title={isExpanded ? 'Chiudi dettagli' : 'Apri dettagli'}
                          >
                            {isExpanded ? '-' : '+'}
                          </button>
                          <span>{row.sala}</span>
                        </div>
                      </td>
                      <td>{statusPill(row.status, ss1Warning)}</td>
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
                                      setExpandedSala(row.sala)
                                      setSynopticSala((prev) => (prev === row.sala ? null : row.sala))
                                    }}
                                    className="rounded-md border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                                  >
                                    {synopticSala === row.sala ? 'Chiudi SCADA' : 'Apri SCADA'}
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

                            {synopticSala === row.sala ? (
                              <div className="mt-3 overflow-hidden rounded-lg border border-slate-200">
                                <ScadaSala
                                  title={`SCADA ${row.sala}`}
                                  lastUpdate={updateLabel}
                                  dryerImageUrl="/images/scada/essiccatore.png"
                                  machines={scadaMachines}
                                  instruments={{
                                    totalKw: Number.isFinite(row.potenzaMedia as number) ? Number(row.potenzaMedia) : 0,
                                    cs: Number.isFinite(row.csPeriodo as number) ? Number(row.csPeriodo) : 0,
                                    dewPoint: Number.isFinite(row.dewPointMedia as number) ? Number(row.dewPointMedia) : 0,
                                    pressure: Number.isFinite(row.pressioneMedia as number) ? Number(row.pressioneMedia) : 0,
                                    flow: Number.isFinite(row.flowValue as number) ? Number(row.flowValue) : 0,
                                    temp: Number.isFinite(row.temperaturaMedia as number) ? Number(row.temperaturaMedia) : 0,
                                  }}
                                />
                              </div>
                            ) : null}

                            <CompressorsTable
                              sala={row.sala}
                              signals={row.detailSignals}
                              selectedMachine={null}
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
