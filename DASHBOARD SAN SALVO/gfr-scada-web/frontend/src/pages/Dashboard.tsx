import { useEffect, useMemo, useRef, useState } from 'react'
import { Navigate, useSearchParams } from 'react-router-dom'
import { useQueries } from '@tanstack/react-query'
import AppLayout from '../components/layout/AppLayout'
import BarMetricChart from '../components/BarMetricChart'
import KpiRow from '../components/KpiRow'
import PlantGeoMap, { type MapMarkerState } from '../components/PlantGeoMap'
import PlantTable from '../components/PlantTable'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import ErrorBoundary from '../components/ui/ErrorBoundary'
import KpiMetricCard from '../components/ui/KpiMetricCard'
import Skeleton from '../components/ui/Skeleton'
import { PLANT_BOOKMARKS, SAN_SALVO_MAP_CENTER } from '../constants/plantMap'
import { SITE_LIST, SITE_ROOMS } from '../constants/siteRooms'
import { legacyKeyToSiteId, siteIdToLegacyKey, type SiteId } from '../constants/sites'
import { fetchPlantSummary, fetchTimeseries } from '../api/plants'
import { usePlants } from '../hooks/usePlants'
import type { PlantSummary, TimeseriesPoint } from '../types/api'
import type { PlantRow, PlantStatus } from '../types/plantTable'
import { canViewDevFeatures, canViewSite, getAuthUserFromSessionToken } from '../utils/auth'

type SignalInfo = { value: number; unit: string; ts: string }
type SignalMap = Record<string, SignalInfo>
type MetricType = 'flow' | 'power' | 'pressure' | 'dew' | 'temp'

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

const LIVE_WINDOW_MS = 180_000
const POWER_HISTORY_MINUTES = 2
const POWER_ZERO_FALLBACK_MAX_AGE_MS = LIVE_WINDOW_MS
const POWER_STALE_MAX_AGE_MS = LIVE_WINDOW_MS
const SUMMARY_CACHE_KEY = 'gfr_dashboard_summary_cache_v1'
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) || 'http://127.0.0.1:8000'

const ROOM_ALIASES: Record<string, string[]> = {
  LAMINATO: ['LAMINATI', 'LaminatiAlta', 'LaminatiBassa'],
  LAMINATI: ['LAMINATO', 'LaminatiAlta', 'LaminatiBassa'],
  'PRIMO ALTA': ['PRIMOAlta'],
  'PRIMO BASSA': ['PRIMOBassa'],
  'SS1 COMPOSIZIONE': ['COMPOSIZIONE'],
  'SS2 COMPOSIZIONE': ['SS2 Bassa Pressione'],
}

const FORCE_UNMAPPED_LABELS = new Set<string>()

const CS_CONTRACT_BY_ROOM: Record<string, number> = {
  BRAVO: 0.103,
  CENTAC: 0.101,
  LAMINATO: 0.184,
  LAMINATI: 0.184,
  'PRIMO ALTA': 0.5,
  'PRIMO BASSA': 0.3,
  SS1: 0.102,
  SS2: 0.107,
  'SS1 COMPOSIZIONE': 0.102,
  'SS2 COMPOSIZIONE': 0.102,
}

function norm(value: string) {
  return value.trim().toUpperCase().replace(/\s+/g, ' ')
}

function canon(value: string) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '')
}

function pickSignalNameByPatterns(signals: SignalMap, exact: string[], includes: string[]) {
  for (const name of exact) {
    if (signals[name]) return name
  }

  const lowerIncludes = includes.map((v) => v.toLowerCase())
  const candidates = Object.entries(signals)
    .filter(([name, info]) => {
      if (!Number.isFinite(Number(info.value))) return false
      const low = name.toLowerCase()
      return lowerIncludes.some((key) => low.includes(key))
    })
    .sort((a, b) => Math.abs(Number(b[1].value)) - Math.abs(Number(a[1].value)))

  return candidates[0]?.[0]
}

function getSignalPatterns(type: MetricType) {
  if (type === 'flow') return { exact: FLOW_SIGNAL_EXACT, includes: FLOW_SIGNAL_INCLUDES }
  if (type === 'power') return { exact: POWER_SIGNAL_EXACT, includes: POWER_SIGNAL_INCLUDES }
  if (type === 'pressure') return { exact: PRESSURE_SIGNAL_EXACT, includes: PRESSURE_SIGNAL_INCLUDES }
  if (type === 'dew') return { exact: DEW_SIGNAL_EXACT, includes: DEW_SIGNAL_INCLUDES }
  return { exact: TEMP_SIGNAL_EXACT, includes: TEMP_SIGNAL_INCLUDES }
}

function metricSignalName(signals: SignalMap, type: MetricType) {
  const { exact, includes } = getSignalPatterns(type)
  return pickSignalNameByPatterns(signals, exact, includes)
}

function metricSnapshotFromSummary(summary: PlantSummary | null, type: MetricType) {
  if (!summary) return { value: 0, unit: '', ts: null as string | null, signal: null as string | null }
  const signals = summary.signals || {}
  const signal = metricSignalName(signals, type)
  if (!signal) return { value: 0, unit: '', ts: null as string | null, signal: null as string | null }
  const info = signals[signal]
  return { value: Number(info?.value) || 0, unit: info?.unit || '', ts: info?.ts || null, signal }
}

function parseTsMs(ts: string | null | undefined) {
  if (!ts) return null
  const ms = new Date(ts).getTime()
  return Number.isFinite(ms) ? ms : null
}

function isStaleSignal(ts: string | null | undefined, nowMs: number, maxAgeMs: number) {
  const tsMs = parseTsMs(ts)
  if (tsMs == null) return true
  return nowMs - tsMs > maxAgeMs
}

function isRecentIso(ts: string | null | undefined, nowMs: number, maxAgeMs: number) {
  return !isStaleSignal(ts, nowMs, maxAgeMs)
}

function pickLatestRecentPoint(points: TimeseriesPoint[], nowMs: number, maxAgeMs: number) {
  for (let i = points.length - 1; i >= 0; i -= 1) {
    const point = points[i]
    if (!isRecentIso(point.ts, nowMs, maxAgeMs)) continue
    const value = Number(point.value)
    if (Number.isFinite(value)) return { ts: point.ts, value }
  }
  return null
}

function metricValueFromSummary(
  summary: PlantSummary | null,
  type: MetricType,
  nowMs: number,
  maxAgeMs: number
): number | null {
  const snapshot = metricSnapshotFromSummary(summary, type)
  if (!snapshot.signal || isStaleSignal(snapshot.ts, nowMs, maxAgeMs)) return null
  return Number(snapshot.value)
}

function metricValueAnyFromSummary(summary: PlantSummary | null, type: MetricType): number | null {
  const snapshot = metricSnapshotFromSummary(summary, type)
  if (!snapshot.signal) return null
  const value = Number(snapshot.value)
  return Number.isFinite(value) ? value : null
}

function resolveApiRoomsForLabel(
  label: string,
  normalizedPlants: Map<string, string>,
  canonicalPlants: Map<string, string>
) {
  const names = [label, ...(ROOM_ALIASES[label] || [])]
  const found: string[] = []
  for (const name of names) {
    const direct = normalizedPlants.get(norm(name))
    if (direct) {
      found.push(direct)
      continue
    }
    const canonical = canonicalPlants.get(canon(name))
    if (canonical) found.push(canonical)
  }
  return Array.from(new Set(found))
}

function buildRoomApiMapping(
  labels: string[],
  normalizedPlants: Map<string, string>,
  canonicalPlants: Map<string, string>
) {
  const mapping = new Map<string, string[]>()
  const assignedPlants = new Set<string>()
  const unresolved: string[] = []

  for (const label of labels) {
    if (FORCE_UNMAPPED_LABELS.has(label)) {
      mapping.set(label, [])
      continue
    }
    const direct = normalizedPlants.get(norm(label)) || canonicalPlants.get(canon(label))
    if (direct) {
      mapping.set(label, [direct])
      assignedPlants.add(direct)
    } else {
      unresolved.push(label)
      mapping.set(label, [])
    }
  }

  for (const label of unresolved) {
    const aliasMatches = resolveApiRoomsForLabel(label, normalizedPlants, canonicalPlants).filter(
      (plant) => !assignedPlants.has(plant)
    )
    if (aliasMatches.length > 0) {
      mapping.set(label, aliasMatches)
      aliasMatches.forEach((plant) => assignedPlants.add(plant))
    }
  }

  return mapping
}

function percent(part: number, total: number) {
  if (!Number.isFinite(part) || !Number.isFinite(total) || total <= 0) return 0
  return (part / total) * 100
}

function statusClass(value: number, dismissed: boolean, hasWarning: boolean = false) {
  if (dismissed) return 'border-slate-300 bg-slate-100 text-slate-700 dot-slate'
  if (hasWarning) return 'border-rose-300 bg-rose-100 text-rose-700 dot-warning'
  if (value > 0) return 'border-[#9ddfb9] bg-[#e9fbf3] text-[#118a52] dot-active'
  if (value < 0) return 'border-amber-300 bg-amber-50 text-amber-700 dot-amber'
  return 'border-[#ebcf80] bg-[#fff8df] text-[#996300] dot-standby'
}

function statusText(value: number, dismissed: boolean, hasWarning: boolean = false) {
  if (dismissed) return 'dism'
  if (hasWarning) return '! warning'
  if (value > 0) return 'active'
  if (value < 0) return 'reverse'
  return 'standby'
}

function formatLastUpdate(value: string | null) {
  if (!value) return '-'
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/)
  if (match) {
    const [, , , , hh, min, ss] = match
    return `${hh}:${min}:${ss}`
  }
  const d = new Date(value)
  if (!Number.isFinite(d.getTime())) return '-'
  return d.toLocaleTimeString('it-IT', { hour12: false, timeZone: 'UTC' })
}

function aggregateMonthlyPoints(seriesList: TimeseriesPoint[][]): TimeseriesPoint[] {
  const byTs = new Map<string, number>()
  for (const series of seriesList) {
    for (const point of series || []) {
      const value = Number(point.value)
      if (!Number.isFinite(value)) continue
      byTs.set(point.ts, (byTs.get(point.ts) || 0) + value)
    }
  }

  return Array.from(byTs.entries())
    .sort((a, b) => new Date(a[0]).getTime() - new Date(b[0]).getTime())
    .map(([ts, value]) => ({ ts, value }))
}

function buildMonthlyRange(fromIso: string, toIso: string): string[] {
  const from = new Date(fromIso)
  const to = new Date(toIso)
  const current = new Date(Date.UTC(from.getUTCFullYear(), from.getUTCMonth(), 1, 0, 0, 0))
  const end = new Date(Date.UTC(to.getUTCFullYear(), to.getUTCMonth(), 1, 0, 0, 0))
  const months: string[] = []

  while (current.getTime() <= end.getTime()) {
    months.push(current.toISOString())
    current.setUTCMonth(current.getUTCMonth() + 1)
  }
  return months
}

function fillMissingMonths(points: TimeseriesPoint[], fromIso: string, toIso: string): TimeseriesPoint[] {
  const months = buildMonthlyRange(fromIso, toIso)
  const valuesByMonth = new Map<string, number>()

  for (const point of points) {
    const d = new Date(point.ts)
    const key = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1, 0, 0, 0)).toISOString()
    valuesByMonth.set(key, Number(point.value) || 0)
  }

  return months.map((monthIso) => ({
    ts: monthIso,
    value: valuesByMonth.get(monthIso) ?? 0,
  }))
}

function loadSummaryCache(): Record<string, PlantSummary> {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.sessionStorage.getItem(SUMMARY_CACHE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return {}
    return parsed as Record<string, PlantSummary>
  } catch {
    return {}
  }
}

function formatFetchTime(ts: number | undefined) {
  if (!ts) return '-'
  return new Date(ts).toLocaleTimeString('it-IT')
}

function maxFetchTime(values: Array<number | undefined>) {
  return values.reduce<number>((max, value) => (typeof value === 'number' && value > max ? value : max), 0)
}

export default function Dashboard() {
  const [searchParams] = useSearchParams()
  const authUser = getAuthUserFromSessionToken()
  const requestedSiteId = searchParams.get('site')
  const requestedRoom = searchParams.get('room')
  const allowedSiteOptions = SITE_LIST.filter((siteKey) => {
    const siteId = legacyKeyToSiteId(siteKey)
    return siteId ? canViewSite(authUser, siteId) : false
  })
  const fallbackSite = allowedSiteOptions[0] || SITE_LIST[0]
  const initialSiteFromQuery = siteIdToLegacyKey(requestedSiteId)
  const initialSite = initialSiteFromQuery && allowedSiteOptions.includes(initialSiteFromQuery) ? initialSiteFromQuery : fallbackSite

  const [summaryCache, setSummaryCache] = useState<Record<string, PlantSummary>>(() => loadSummaryCache())
  const { data: apiPlants } = usePlants()
  const normalizedPlants = useMemo(() => {
    const map = new Map<string, string>()
    for (const plant of apiPlants || []) map.set(norm(plant), plant)
    return map
  }, [apiPlants])
  const canonicalPlants = useMemo(() => {
    const map = new Map<string, string>()
    for (const plant of apiPlants || []) map.set(canon(plant), plant)
    return map
  }, [apiPlants])

  const [site, setSite] = useState(initialSite)
  const [room, setRoom] = useState(requestedRoom || '')
  const tableSectionRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (allowedSiteOptions.length === 0) return
    if (allowedSiteOptions.includes(site)) return
    setSite(allowedSiteOptions[0])
  }, [allowedSiteOptions, site])

  const rooms = SITE_ROOMS[site] || []
  const roomApiMapping = useMemo(
    () => buildRoomApiMapping(rooms, normalizedPlants, canonicalPlants),
    [rooms, normalizedPlants, canonicalPlants]
  )

  useEffect(() => {
    if (!rooms.length) {
      setRoom('')
      return
    }
    const firstAvailable = rooms.find((label) => (roomApiMapping.get(label) || []).length > 0) || rooms[0]
    setRoom((prev) => (prev && rooms.includes(prev) ? prev : firstAvailable))
  }, [rooms, roomApiMapping])

  const selectedApiRoom = (roomApiMapping.get(room) || [])[0] || ''
  const currentNowMs = Date.now()
  const debugEnabled = Boolean(import.meta.env.DEV) && canViewDevFeatures(authUser)
  const queryDebugSeenRef = useRef<Record<string, number>>({})

  const monthlyFrom = useMemo(() => {
    const start = new Date()
    start.setFullYear(start.getFullYear() - 1)
    start.setHours(0, 0, 0, 0)
    return start.toISOString()
  }, [])
  const monthlyTo = useMemo(() => new Date().toISOString(), [])
  const siteApiRooms = useMemo(() => {
    const all = rooms.flatMap((label) => roomApiMapping.get(label) || [])
    return Array.from(new Set(all))
  }, [rooms, roomApiMapping])

  const siteSummaryQueries = useQueries({
    queries: siteApiRooms.map((apiRoom) => ({
      queryKey: ['site-room-summary', site, apiRoom],
      queryFn: async () => fetchPlantSummary(apiRoom),
      staleTime: 1_000,
      cacheTime: 120_000,
      keepPreviousData: true,
      refetchInterval: 10_000,
      refetchIntervalInBackground: true,
      refetchOnWindowFocus: true,
      refetchOnReconnect: true,
      retry: 1,
    })),
  })

  useEffect(() => {
    setSummaryCache((prev) => {
      const next = { ...prev }
      let changed = false
      siteApiRooms.forEach((apiRoom, idx) => {
        const data = siteSummaryQueries[idx]?.data as PlantSummary | undefined
        if (!data) return
        const prevUpdate = prev[apiRoom]?.last_update || ''
        if (prevUpdate !== data.last_update) {
          next[apiRoom] = data
          changed = true
        }
      })
      if (!changed) return prev
      if (typeof window !== 'undefined') {
        try {
          window.sessionStorage.setItem(SUMMARY_CACHE_KEY, JSON.stringify(next))
        } catch {
          // Ignore storage failures and keep in-memory cache.
        }
      }
      return next
    })
  }, [siteApiRooms, siteSummaryQueries])
  const summaryByApiRoom = useMemo(() => {
    const map = new Map<string, PlantSummary | null>()
    siteApiRooms.forEach((apiRoom, idx) => {
      const live = (siteSummaryQueries[idx]?.data as PlantSummary | undefined) || null
      const cached = summaryCache[apiRoom] || null
      const freshCached = cached && isRecentIso(cached.last_update, currentNowMs, LIVE_WINDOW_MS) ? cached : null
      map.set(apiRoom, live || freshCached || null)
    })
    return map
  }, [siteApiRooms, siteSummaryQueries, summaryCache, currentNowMs])
  const summaryLoadingByApiRoom = useMemo(() => {
    const map = new Map<string, boolean>()
    siteApiRooms.forEach((apiRoom, idx) => {
      const q = siteSummaryQueries[idx]
      const hasData = Boolean(summaryByApiRoom.get(apiRoom))
      map.set(apiRoom, Boolean(q?.isLoading && !hasData))
    })
    return map
  }, [siteApiRooms, siteSummaryQueries, summaryByApiRoom])
  const selectedApiRoomIndex = selectedApiRoom ? siteApiRooms.indexOf(selectedApiRoom) : -1
  const selectedSiteSummaryQuery = selectedApiRoomIndex >= 0 ? siteSummaryQueries[selectedApiRoomIndex] : undefined
  const selectedSummary = selectedApiRoom ? summaryByApiRoom.get(selectedApiRoom) || null : null
  const signals = selectedSummary?.signals || {}

  const pressureSignalName = pickSignalNameByPatterns(signals, PRESSURE_SIGNAL_EXACT, PRESSURE_SIGNAL_INCLUDES)
  const dewPointSignalName = pickSignalNameByPatterns(signals, DEW_SIGNAL_EXACT, DEW_SIGNAL_INCLUDES)
  const flowSignalName = pickSignalNameByPatterns(signals, FLOW_SIGNAL_EXACT, FLOW_SIGNAL_INCLUDES)
  const powerSignalName = pickSignalNameByPatterns(signals, POWER_SIGNAL_EXACT, POWER_SIGNAL_INCLUDES)

  const pressure = pressureSignalName ? signals[pressureSignalName] : undefined
  const dewPoint = dewPointSignalName ? signals[dewPointSignalName] : undefined
  const flow = flowSignalName ? signals[flowSignalName] : undefined
  const power = powerSignalName ? signals[powerSignalName] : undefined

  const powerSignalByApiRoom = useMemo(() => {
    const map = new Map<string, string | null>()
    siteApiRooms.forEach((apiRoom) => {
      const summary = summaryByApiRoom.get(apiRoom)
      const signal = summary ? metricSnapshotFromSummary(summary, 'power').signal : null
      map.set(apiRoom, signal)
    })
    return map
  }, [siteApiRooms, summaryByApiRoom])

  const recentPowerQueries = useQueries({
    queries: siteApiRooms.map((apiRoom) => {
      const signalName = powerSignalByApiRoom.get(apiRoom) || null
      return {
        queryKey: ['site-recent-power', site, apiRoom, signalName, POWER_HISTORY_MINUTES],
        queryFn: () =>
          signalName ? fetchTimeseries(apiRoom, signalName, undefined, undefined, undefined, POWER_HISTORY_MINUTES, 120) : Promise.resolve([]),
        enabled: Boolean(signalName),
        staleTime: 5_000,
        cacheTime: 120_000,
        refetchInterval: 15_000,
        refetchIntervalInBackground: true,
        refetchOnWindowFocus: true,
        refetchOnReconnect: true,
        retry: 1,
      }
    }),
  })

  const fallbackPowerByApiRoom = useMemo(() => {
    const map = new Map<string, { ts: string; value: number }>()
    siteApiRooms.forEach((apiRoom, idx) => {
      const points = (recentPowerQueries[idx]?.data as TimeseriesPoint[] | undefined) || []
      const latestRecent = pickLatestRecentPoint(points, currentNowMs, LIVE_WINDOW_MS)
      if (latestRecent) map.set(apiRoom, latestRecent)
    })
    return map
  }, [siteApiRooms, recentPowerQueries, currentNowMs])

  const monthlyVolumeQueries = useQueries({
    queries: siteApiRooms.map((apiRoom) => {
      const summary = summaryByApiRoom.get(apiRoom)
      const signalsMap = summary?.signals || {}
      const signalName =
        pickSignalNameByPatterns(signalsMap, ['Flusso TOT', 'Flusso', 'Flow'], ['flusso tot', 'flusso 7 barg', 'flusso', 'flow', 'portat', 'nm3']) ||
        null
      return {
        queryKey: ['site-monthly-volume', site, apiRoom, signalName, monthlyFrom, monthlyTo],
        queryFn: () =>
          signalName
            ? fetchTimeseries(apiRoom, signalName, monthlyFrom, monthlyTo, '1 month', undefined, 240, 'sum')
            : Promise.resolve([]),
        enabled: Boolean(signalName),
        staleTime: 30_000,
        cacheTime: 120_000,
        refetchInterval: 30_000,
        refetchIntervalInBackground: true,
        refetchOnWindowFocus: true,
        refetchOnReconnect: true,
        retry: 1,
      }
    }),
  })

  const monthlyEnergyQueries = useQueries({
    queries: siteApiRooms.map((apiRoom) => {
      const summary = summaryByApiRoom.get(apiRoom)
      const signalsMap = summary?.signals || {}
      const signalName =
        pickSignalNameByPatterns(signalsMap, ['Potenza Attiva TOT', 'Potenza Attiva', 'Power'], ['potenza attiva tot', 'potenza attiva', 'power', 'kw']) ||
        null
      return {
        queryKey: ['site-monthly-energy', site, apiRoom, signalName, monthlyFrom, monthlyTo],
        queryFn: () =>
          signalName
            ? fetchTimeseries(apiRoom, signalName, monthlyFrom, monthlyTo, '1 month', undefined, 240, 'sum')
            : Promise.resolve([]),
        enabled: Boolean(signalName),
        staleTime: 30_000,
        cacheTime: 120_000,
        refetchInterval: 30_000,
        refetchIntervalInBackground: true,
        refetchOnWindowFocus: true,
        refetchOnReconnect: true,
        retry: 1,
      }
    }),
  })

  useEffect(() => {
    if (!debugEnabled) return
    console.debug('[Dashboard][selection]', {
      site,
      roomLabel: room,
      selectedApiRoom,
      at: new Date().toISOString(),
    })
  }, [debugEnabled, site, room, selectedApiRoom])

  useEffect(() => {
    if (!debugEnabled) return
    const updatedAt = selectedSiteSummaryQuery?.dataUpdatedAt || 0
    const logKey = `selected-summary:${selectedApiRoom}`
    if (!updatedAt || queryDebugSeenRef.current[logKey] === updatedAt) return
    queryDebugSeenRef.current[logKey] = updatedAt
    const payload = selectedSiteSummaryQuery?.data as PlantSummary | undefined
    console.debug('[Dashboard][query]', {
      kind: 'selected-summary',
      queryKey: ['plantSummary', selectedApiRoom, null],
      lastFetchTime: new Date(updatedAt).toISOString(),
      responseSize: Object.keys(payload?.signals || {}).length,
    })
  }, [debugEnabled, selectedApiRoom, selectedSiteSummaryQuery])

  useEffect(() => {
    if (!debugEnabled) return
    siteSummaryQueries.forEach((query, idx) => {
      const updatedAt = query.dataUpdatedAt || 0
      const apiRoom = siteApiRooms[idx] || 'unknown'
      const logKey = `site-summary:${apiRoom}`
      if (!updatedAt || queryDebugSeenRef.current[logKey] === updatedAt) return
      queryDebugSeenRef.current[logKey] = updatedAt
      const payload = query.data as PlantSummary | undefined
      console.debug('[Dashboard][query]', {
        kind: 'site-summary',
        queryKey: ['site-room-summary', site, apiRoom],
        lastFetchTime: new Date(updatedAt).toISOString(),
        responseSize: Object.keys(payload?.signals || {}).length,
      })
    })
  }, [debugEnabled, site, siteSummaryQueries, siteApiRooms])

  useEffect(() => {
    if (!debugEnabled) return
    recentPowerQueries.forEach((query, idx) => {
      const updatedAt = query.dataUpdatedAt || 0
      const apiRoom = siteApiRooms[idx] || 'unknown'
      const signalName = powerSignalByApiRoom.get(apiRoom) || null
      const logKey = `recent-power:${apiRoom}:${signalName ?? 'none'}`
      if (!updatedAt || queryDebugSeenRef.current[logKey] === updatedAt) return
      queryDebugSeenRef.current[logKey] = updatedAt
      const points = (query.data as TimeseriesPoint[] | undefined) || []
      console.debug('[Dashboard][query]', {
        kind: 'timeseries-recent-power',
        queryKey: ['site-recent-power', site, apiRoom, signalName, POWER_HISTORY_MINUTES],
        lastFetchTime: new Date(updatedAt).toISOString(),
        responseSize: points.length,
      })
    })
  }, [debugEnabled, site, siteApiRooms, recentPowerQueries, powerSignalByApiRoom])

  const roomRows = rooms.map((label) => {
    const apiRooms = roomApiMapping.get(label) || []
    const summariesByRoom = apiRooms
      .map((name) => ({ apiRoom: name, summary: summaryByApiRoom.get(name) }))
      .filter((entry): entry is { apiRoom: string; summary: PlantSummary } => Boolean(entry.summary))
    const summaries = summariesByRoom.map((entry) => entry.summary)
    const detailSignals = summariesByRoom.reduce<Record<string, { value: number; unit: string; ts: string }>>(
      (acc, entry) => {
        const signals = entry.summary.signals || {}
        for (const [signalName, info] of Object.entries(signals)) {
          const prev = acc[signalName]
          const prevTs = parseTsMs(prev?.ts)
          const nextTs = parseTsMs(info.ts)
          if (!prev || (nextTs != null && (prevTs == null || nextTs >= prevTs))) {
            acc[signalName] = info
          }
        }
        return acc
      },
      {}
    )
    const powerParts = summariesByRoom.map(({ apiRoom, summary }) => {
      const current = metricSnapshotFromSummary(summary, 'power')
      const fallback = fallbackPowerByApiRoom.get(apiRoom)
      let value = current.value
      let ts = current.ts
      let fallbackUsed = false

      if (fallback) {
        const fallbackFresh = isRecentIso(fallback.ts, currentNowMs, POWER_ZERO_FALLBACK_MAX_AGE_MS)
        if (fallbackFresh) {
          if (!current.signal || isStaleSignal(current.ts, currentNowMs, POWER_STALE_MAX_AGE_MS)) {
            value = fallback.value
            ts = fallback.ts
            fallbackUsed = true
          }
        }
      }

      return { value, ts, fallbackUsed }
    })
    const livePowerParts = powerParts.filter((entry) => isRecentIso(entry.ts, currentNowMs, LIVE_WINDOW_MS))
    const powerPartsForDisplay = livePowerParts.length > 0 ? livePowerParts : powerParts
    const kwh = powerPartsForDisplay.reduce((acc, entry) => acc + (Number.isFinite(entry.value) ? entry.value : 0), 0)

    const flowValuesFresh = summaries
      .map((s) => metricValueFromSummary(s, 'flow', currentNowMs, LIVE_WINDOW_MS))
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
    const flowValues = flowValuesFresh.length > 0
      ? flowValuesFresh
      : summaries
        .map((s) => metricValueAnyFromSummary(s, 'flow'))
        .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))

    const pressureValuesFresh = summaries
      .map((s) => metricValueFromSummary(s, 'pressure', currentNowMs, LIVE_WINDOW_MS))
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
    const pressureValues = pressureValuesFresh.length > 0
      ? pressureValuesFresh
      : summaries
        .map((s) => metricValueAnyFromSummary(s, 'pressure'))
        .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))

    const dewValuesFresh = summaries
      .map((s) => metricValueFromSummary(s, 'dew', currentNowMs, LIVE_WINDOW_MS))
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
    const dewValues = dewValuesFresh.length > 0
      ? dewValuesFresh
      : summaries
        .map((s) => metricValueAnyFromSummary(s, 'dew'))
        .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))

    const tempValuesFresh = summaries
      .map((s) => metricValueFromSummary(s, 'temp', currentNowMs, LIVE_WINDOW_MS))
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
    const tempValues = tempValuesFresh.length > 0
      ? tempValuesFresh
      : summaries
        .map((s) => metricValueAnyFromSummary(s, 'temp'))
        .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))

    const nm3 = flowValues.reduce((acc, value) => acc + value, 0)
    const pressureVal = pressureValues.length ? pressureValues.reduce((a, b) => a + b, 0) / pressureValues.length : 0
    const dewVal = dewValues.length ? dewValues.reduce((a, b) => a + b, 0) / dewValues.length : 0
    const tempVal = tempValues.length ? tempValues.reduce((a, b) => a + b, 0) / tempValues.length : 0

    const lastSummaryTs = summaries
      .map((s) => s.last_update)
      .filter((ts): ts is string => Boolean(ts))
      .sort()
      .slice(-1)[0] || null
    const lastPowerTs = livePowerParts
      .map((entry) => entry.ts)
      .filter((ts): ts is string => Boolean(ts))
      .sort()
      .slice(-1)[0] || null
    const ts = [lastSummaryTs, lastPowerTs]
      .filter((value): value is string => Boolean(value))
      .sort()
      .slice(-1)[0] || null

    const dismissed = label.toUpperCase().includes('(DISMESSA)')
    const hasRealtimeMismatchIssue = label.toUpperCase() === 'SS1' && nm3 > 0.1 && kwh <= 0

    return {
      label,
      dismissed,
      hasRealtimeMismatchIssue,
      available: apiRooms.length > 0,
      loading: apiRooms.some((name) => summaryLoadingByApiRoom.get(name)),
      nm3,
      kwh,
      csPeriodo: nm3 > 0 ? kwh / nm3 : 0,
      csContratto: CS_CONTRACT_BY_ROOM[label] ?? null,
      flowAvg: nm3,
      powerAvg: kwh,
      pressureAvg: pressureVal,
      dewAvg: dewVal,
      tempAvg: tempVal,
      ts,
      fallbackPowerUsed: powerParts.some((entry) => entry.fallbackUsed),
      detailSignals,
    }
  })

  const totalPowerRooms = roomRows.reduce((acc, row) => acc + Math.max(0, row.kwh), 0)
  const plantRows: PlantRow[] = roomRows.map((row) => {
    const status: PlantStatus = row.dismissed
      ? 'dism'
      : !row.available
        ? 'idle'
        : row.kwh > 0
          ? 'active'
          : 'idle'
    return {
      sala: row.label,
      status,
      lastUpdate: row.ts,
      realtimeNm3: row.available ? row.nm3 : null,
      realtimeKwh: row.available ? row.kwh : null,
      flussoMedio: row.available ? row.flowAvg : null,
      potenzaMedia: row.available ? row.powerAvg : null,
      pressioneMedia: row.available ? row.pressureAvg : null,
      dewPointMedia: row.available ? row.dewAvg : null,
      temperaturaMedia: row.available ? row.tempAvg : null,
      csPeriodo: row.available ? row.csPeriodo : null,
      csContratto: row.csContratto,
      percentEnergiaConsumata: row.available ? percent(Math.max(0, row.kwh), totalPowerRooms) : null,
      detailSignals: row.available ? row.detailSignals : undefined,
    }
  })

  const nowForKpi = Date.now()
  const kpiRows = plantRows.filter((row) => row.status !== 'dism' && row.status !== 'nd')
  const kpiTotalFlow = kpiRows.reduce((sum, row) => sum + (row.realtimeNm3 || 0), 0)
  const kpiTotalPower = kpiRows.reduce((sum, row) => sum + (row.potenzaMedia || 0), 0)
  const kpiTotalKwh = kpiRows.reduce((sum, row) => sum + (row.realtimeKwh || 0), 0)
  const kpiAlertRooms = kpiRows.filter((row) => {
    const tsMs = parseTsMs(row.lastUpdate)
    const isStale = tsMs != null ? nowForKpi - tsMs > 60_000 : false
    const isAnomaly = row.status === 'active' && ((row.realtimeKwh ?? NaN) === 0 || (row.potenzaMedia ?? NaN) === 0)
    return isStale || isAnomaly
  }).length

  const selectedRow = roomRows.find((rowItem) => rowItem.label === room) || null
  const selectedRowIsOff = Boolean(
    selectedRow && selectedRow.available && !selectedRow.dismissed && !selectedRow.hasRealtimeMismatchIssue && selectedRow.kwh <= 0
  )
  const selectedRowHideValues = Boolean(selectedRow && (selectedRow.dismissed || selectedRowIsOff))
  const topFlowValue = selectedRow && selectedRow.available && !selectedRowHideValues ? selectedRow.nm3.toFixed(1) : '--'
  const topPowerValue = selectedRow && selectedRow.available && !selectedRowHideValues ? selectedRow.kwh.toFixed(1) : '--'
  const topPressureValue = selectedRow && selectedRow.available && !selectedRowHideValues ? selectedRow.pressureAvg.toFixed(1) : '--'
  const topDewValue = selectedRow && selectedRow.available && !selectedRowHideValues ? selectedRow.dewAvg.toFixed(1) : '--'
  const ss1RealtimeIssue = Boolean(selectedRow?.hasRealtimeMismatchIssue)
  const monthlyVolumeData = fillMissingMonths(
    aggregateMonthlyPoints(monthlyVolumeQueries.map((q) => (q.data as TimeseriesPoint[] | undefined) || [])),
    monthlyFrom,
    monthlyTo
  )
  const monthlyEnergyData = fillMissingMonths(
    aggregateMonthlyPoints(monthlyEnergyQueries.map((q) => (q.data as TimeseriesPoint[] | undefined) || [])),
    monthlyFrom,
    monthlyTo
  )
  const monthlyChartsLoading =
    monthlyVolumeQueries.some((q) => q.isLoading) || monthlyEnergyQueries.some((q) => q.isLoading)

  const lastUpdateText = formatLastUpdate(selectedRow?.ts || null)
  const subtitle = room
    ? `Sala ${room}${lastUpdateText !== '-' ? ` - Last update ${lastUpdateText}` : ''}`
    : 'Seleziona una sala'
  const selectedSummaryFetchTs = selectedSiteSummaryQuery?.dataUpdatedAt
  const siteSummaryFetchTs = maxFetchTime(siteSummaryQueries.map((q) => q.dataUpdatedAt))
  const recentPowerFetchTs = maxFetchTime(recentPowerQueries.map((q) => q.dataUpdatedAt))
  const monthlyVolumeFetchTs = maxFetchTime(monthlyVolumeQueries.map((q) => q.dataUpdatedAt))
  const monthlyEnergyFetchTs = maxFetchTime(monthlyEnergyQueries.map((q) => q.dataUpdatedAt))

  const isSummaryLoading = Boolean(selectedApiRoom && selectedSiteSummaryQuery?.isLoading && !selectedSummary)
  const mapMarkerStates = useMemo<Record<string, MapMarkerState>>(() => {
    const byLabel = new Map(roomRows.map((row) => [row.label, row]))
    const states: Record<string, MapMarkerState> = {}
    for (const label of rooms) {
      const row = byLabel.get(label)
      if (!row) {
        states[label] = 'warning'
        continue
      }
      if (row.dismissed) {
        states[label] = 'dismissed'
        continue
      }
      const tsMs = parseTsMs(row.ts)
      const isStale = tsMs != null ? currentNowMs - tsMs > 60_000 : true
      const isRoomActive = row.kwh > 0
      const hasAlarmCondition = row.available && isRoomActive && (row.kwh <= 0 || row.nm3 <= 0)
      if (hasAlarmCondition) {
        states[label] = 'alarm'
      } else if (row.hasRealtimeMismatchIssue || isStale || row.loading || !row.available) {
        states[label] = 'warning'
      } else if (!isRoomActive) {
        states[label] = 'standby'
      } else {
        states[label] = 'active'
      }
    }
    return states
  }, [roomRows, rooms, currentNowMs])
  const selectedSiteId = (legacyKeyToSiteId(site) || 'san-salvo') as SiteId

  const selectRoomAndScrollToTable = (nextRoom: string) => {
    setRoom(nextRoom)
    window.requestAnimationFrame(() => {
      tableSectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
  }

  if (allowedSiteOptions.length === 0) {
    return <Navigate to="/403" replace />
  }

  return (
    <AppLayout
      title={site}
      subtitle={subtitle}
      plant={site}
      onPlantChange={setSite}
      selectorOptions={allowedSiteOptions}
      selectorPlaceholder="Select site"
      scadaPlant={selectedApiRoom}
    >
      <div className="space-y-4">
        <div className="grid grid-cols-1 items-stretch gap-4 lg:grid-cols-3">
          <Card className="h-full">
            <CardHeader>
              <CardTitle className="text-slate-900">Sale Stabilimento</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {rooms.map((label, index) => {
                  const available = (roomApiMapping.get(label) || []).length > 0
                  const active = room === label
                  const row = roomRows.find((r) => r.label === label)
                  const roomStatus = row
                    ? statusText(row.kwh, row.dismissed, row.hasRealtimeMismatchIssue)
                    : available
                      ? 'standby'
                      : 'n/d'
                  const roomStatusClass = row
                    ? statusClass(row.kwh, row.dismissed, row.hasRealtimeMismatchIssue)
                    : available
                      ? 'border-[#ebcf80] bg-[#fff8df] text-[#996300] dot-standby'
                      : 'border-slate-200 bg-slate-50 text-slate-500 dot-slate'
                  const roomDotClass = roomStatusClass.includes('dot-active')
                    ? 'bg-[#58d68d]'
                    : roomStatusClass.includes('dot-warning')
                      ? 'bg-rose-500'
                    : roomStatusClass.includes('dot-standby')
                      ? 'bg-[#e2b73b]'
                      : roomStatusClass.includes('dot-amber')
                        ? 'bg-amber-500'
                        : 'bg-slate-400'
                  const roomStatusPillClass = roomStatusClass
                    .replace(' dot-active', '')
                    .replace(' dot-warning', '')
                    .replace(' dot-standby', '')
                    .replace(' dot-amber', '')
                    .replace(' dot-slate', '')
                  return (
                    <button
                      key={label}
                      type="button"
                      onClick={() => selectRoomAndScrollToTable(label)}
                      className={[
                        'flex w-full items-center justify-between rounded-md border px-3 py-2 text-left text-sm',
                        active ? 'border-teal-500 bg-teal-50 text-teal-700' : 'border-slate-200 bg-white text-slate-700 hover:border-slate-300',
                      ].join(' ')}
                    >
                      <span>{`${index + 1}. ${label}`}</span>
                      <span
                        className={[
                          'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium leading-5',
                          roomStatusPillClass,
                        ].join(' ')}
                      >
                        <span className={['inline-block h-2 w-2 rounded-full', roomDotClass].join(' ')} />
                        {roomStatus}
                      </span>
                    </button>
                  )
                })}
              </div>
            </CardContent>
          </Card>

          <Card className="h-full lg:col-span-2 lg:flex lg:flex-col">
            <CardHeader>
              <CardTitle className="text-slate-900">Mappa {site}</CardTitle>
            </CardHeader>
            <CardContent className="lg:flex-1">
              <div className="min-h-[20rem] overflow-hidden rounded-md border border-slate-200 lg:h-full lg:min-h-0">
                <ErrorBoundary
                  fallback={
                    <div className="flex h-full min-h-[20rem] items-center justify-center bg-slate-50 text-sm text-slate-600">
                      Mappa non disponibile al momento.
                    </div>
                  }
                >
                  <PlantGeoMap
                    rooms={rooms}
                    selectedRoom={room}
                    markerStates={mapMarkerStates}
                    bookmarks={PLANT_BOOKMARKS}
                    center={SAN_SALVO_MAP_CENTER}
                    onSelectRoom={selectRoomAndScrollToTable}
                  />
                </ErrorBoundary>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
          {isSummaryLoading
            ? Array.from({ length: 4 }).map((_, i) => (
                <Card key={`kpi-${i}`}>
                  <CardContent className="space-y-2 p-4">
                    <Skeleton className="h-3 w-24" />
                    <Skeleton className="h-8 w-24" />
                  </CardContent>
                </Card>
              ))
            : (
              <>
                <KpiMetricCard label="Flusso" value={topFlowValue} unit={flow?.unit || 'Nm3/h'} />
                <KpiMetricCard
                  label="Potenza Attiva"
                  value={`${topPowerValue}${topPowerValue !== '--' && ss1RealtimeIssue ? ' !' : ''}`}
                  unit={power?.unit || 'kW'}
                />
                <KpiMetricCard label="Pressione" value={topPressureValue} unit={pressure?.unit || 'barg'} />
                <KpiMetricCard label="Dew Point" value={topDewValue} unit={dewPoint?.unit || 'C'} />
              </>
            )}
        </div>

        {debugEnabled ? (
          <Card className="border-amber-200 bg-amber-50/50">
            <CardHeader>
              <CardTitle className="text-sm text-amber-900">Debug data freeze (dev only)</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 gap-2 text-xs text-slate-700 md:grid-cols-2 xl:grid-cols-3">
                <div><span className="font-semibold">Site:</span> {site}</div>
                <div><span className="font-semibold">Room label:</span> {room || '-'}</div>
                <div><span className="font-semibold">API plant:</span> {selectedApiRoom || '-'}</div>
                <div><span className="font-semibold">Summary last_update:</span> {selectedRow?.ts || '-'}</div>
                <div><span className="font-semibold">Summary last fetch:</span> {formatFetchTime(selectedSummaryFetchTs)}</div>
                <div><span className="font-semibold">Site summary fetch:</span> {formatFetchTime(siteSummaryFetchTs)}</div>
                <div><span className="font-semibold">Recent power fetch:</span> {formatFetchTime(recentPowerFetchTs)}</div>
                <div><span className="font-semibold">Monthly volume fetch:</span> {formatFetchTime(monthlyVolumeFetchTs)}</div>
                <div><span className="font-semibold">Monthly energy fetch:</span> {formatFetchTime(monthlyEnergyFetchTs)}</div>
                <div className="md:col-span-2 xl:col-span-3">
                  <span className="font-semibold">API base URL:</span> <code>{API_BASE_URL}</code>
                </div>
              </div>
            </CardContent>
          </Card>
        ) : null}

        <KpiRow
          totalFlowNm3={kpiTotalFlow}
          totalPowerKw={kpiTotalPower}
          totalKwhToday={kpiTotalKwh}
          alertRooms={kpiAlertRooms}
        />

        <div ref={tableSectionRef}>
          <PlantTable
            rows={plantRows}
            selectedSala={room}
            onSelectSala={setRoom}
            siteId={selectedSiteId}
          />
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="text-slate-900">Volume prodotto per Mese Nm3 (Impianto Totale)</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="mb-2 text-sm text-slate-500">Somma mensile delle sale disponibili nell'ultimo anno.</p>
              {monthlyChartsLoading ? (
                <Skeleton className="h-72 w-full" />
              ) : monthlyVolumeData.length > 0 ? (
                <BarMetricChart data={monthlyVolumeData} barColor="#0f766e" xMode="month" />
              ) : (
                <div className="text-sm text-slate-500">Dati mensili non disponibili per questo impianto.</div>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle className="text-slate-900">Energia consumata per Mese kWh (Impianto Totale)</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="mb-2 text-sm text-slate-500">Somma mensile delle sale disponibili nell'ultimo anno.</p>
              {monthlyChartsLoading ? (
                <Skeleton className="h-72 w-full" />
              ) : monthlyEnergyData.length > 0 ? (
                <BarMetricChart data={monthlyEnergyData} barColor="#0284c7" xMode="month" />
              ) : (
                <div className="text-sm text-slate-500">Dati mensili non disponibili per questo impianto.</div>
              )}
            </CardContent>
          </Card>
        </div>

      </div>
    </AppLayout>
  )
}
