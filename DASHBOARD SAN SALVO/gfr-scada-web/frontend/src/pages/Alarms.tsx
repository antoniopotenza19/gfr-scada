import { useDeferredValue, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

import AppLayout from '../components/layout/AppLayout'
import { Card, CardContent } from '../components/ui/Card'
import Skeleton from '../components/ui/Skeleton'
import { SITE_ROOMS } from '../constants/siteRooms'
import { legacyKeyToSiteId, siteNameFromId } from '../constants/sites'
import type { AlarmEvent } from '../types/api'
import { canViewSite, getAuthUserFromSessionToken } from '../utils/auth'

type AlarmSeverity = 'critical' | 'high' | 'medium' | 'info'
type AlarmStatus = 'active' | 'ack' | 'returned'
type AlarmArea = 'Compressori' | 'Essiccatori' | 'Pressione' | 'Dew Point' | 'Comunicazione / Sistema'
type QuickFilterKey = 'all' | 'critical' | 'compressors' | 'dryers' | 'pressure' | 'dew'

interface AlarmRecord {
  id: string
  severity: AlarmSeverity
  status: AlarmStatus
  timestampStart: string
  timestampEnd: string | null
  tag: string
  title: string
  description: string
  machine: string
  area: AlarmArea
  value: string
  threshold: string
  room: string
  acknowledged: boolean
  source: 'api' | 'mock'
  possibleCause: string
  notes: string
  chartTarget?: string
  chartSignals?: string[]
}

const QUICK_FILTERS: Array<{ key: QuickFilterKey; label: string }> = [
  { key: 'all', label: 'Tutti' },
  { key: 'critical', label: 'Critici' },
  { key: 'compressors', label: 'Compressori' },
  { key: 'dryers', label: 'Essiccatori' },
  { key: 'pressure', label: 'Pressione' },
  { key: 'dew', label: 'Dew Point' },
]

const SEVERITY_OPTIONS: Array<{ key: AlarmSeverity; label: string }> = [
  { key: 'critical', label: 'Critico' },
  { key: 'high', label: 'Alto' },
  { key: 'medium', label: 'Medio' },
  { key: 'info', label: 'Info' },
]

const AREA_OPTIONS: AlarmArea[] = [
  'Compressori',
  'Essiccatori',
  'Pressione',
  'Dew Point',
  'Comunicazione / Sistema',
]

const STATUS_OPTIONS: Array<{ key: AlarmStatus; label: string }> = [
  { key: 'active', label: 'Attivo' },
  { key: 'ack', label: 'Riconosciuto' },
  { key: 'returned', label: 'Rientrato' },
]

const ALARM_HISTORY_LIMIT = 1000

const SEVERITY_META: Record<AlarmSeverity, { label: string; badge: string; dot: string; rail: string }> = {
  critical: { label: 'Critico', badge: 'border-rose-200 bg-rose-50 text-rose-700', dot: 'bg-rose-500', rail: 'bg-rose-500' },
  high: { label: 'Alto', badge: 'border-orange-200 bg-orange-50 text-orange-700', dot: 'bg-orange-500', rail: 'bg-orange-500' },
  medium: { label: 'Medio', badge: 'border-amber-200 bg-amber-50 text-amber-700', dot: 'bg-amber-500', rail: 'bg-amber-500' },
  info: { label: 'Info', badge: 'border-sky-200 bg-sky-50 text-sky-700', dot: 'bg-sky-500', rail: 'bg-sky-500' },
}

const STATUS_META: Record<AlarmStatus, { label: string; badge: string }> = {
  active: { label: 'Attivo', badge: 'border-rose-200 bg-rose-50 text-rose-700' },
  ack: { label: 'Riconosciuto', badge: 'border-sky-200 bg-sky-50 text-sky-700' },
  returned: { label: 'Rientrato', badge: 'border-emerald-200 bg-emerald-50 text-emerald-700' },
}

function joinClassNames(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(' ')
}

function formatDateTime(value: string | null | undefined) {
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

function formatCompactDateTime(value: string | null | undefined) {
  if (!value) return '--'
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return '--'
  return date.toLocaleString('it-IT', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatLastUpdateLabel(value: number | null | undefined) {
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

function formatRelativeTime(value: string | null | undefined) {
  if (!value) return '--'
  const target = new Date(value).getTime()
  if (!Number.isFinite(target)) return '--'
  const diffMinutes = Math.round((Date.now() - target) / 60_000)
  if (diffMinutes < 1) return 'adesso'
  if (diffMinutes < 60) return `${diffMinutes} min fa`
  const diffHours = Math.round(diffMinutes / 60)
  if (diffHours < 24) return `${diffHours} h fa`
  const diffDays = Math.round(diffHours / 24)
  return `${diffDays} g fa`
}

function inferSeverity(raw: string): AlarmSeverity {
  const value = raw.trim().toLowerCase()
  if (value.includes('crit')) return 'critical'
  if (value.includes('high') || value.includes('alto') || value.includes('warn')) return 'high'
  if (value.includes('med')) return 'medium'
  return 'info'
}

function inferArea(text: string): AlarmArea {
  const value = text.toLowerCase()
  if (value.includes('dew')) return 'Dew Point'
  if (value.includes('pressure') || value.includes('pression')) return 'Pressione'
  if (value.includes('dryer') || value.includes('essic')) return 'Essiccatori'
  if (value.includes('comm') || value.includes('offline') || value.includes('timeout') || value.includes('modbus') || value.includes('system')) {
    return 'Comunicazione / Sistema'
  }
  return 'Compressori'
}

function inferStatus(text: string): AlarmStatus {
  const value = text.toLowerCase()
  if (value.includes('ack')) return 'ack'
  if (value.includes('clear') || value.includes('return') || value.includes('rientr')) return 'returned'
  return 'active'
}

function isoMinutesAgo(minutes: number, referenceNowMs: number) {
  return new Date(referenceNowMs - minutes * 60_000).toISOString()
}

function areaDefaults(area: AlarmArea, room: string) {
  if (area === 'Pressione') {
    return {
      machine: `Linea pressione ${room}`,
      value: '5,8 barg',
      threshold: '< 6,2 barg',
      cause: 'Caduta di pressione legata a richiesta improvvisa o regolazione compressori non allineata.',
      notes: 'Verificare carico compressori e setpoint rete nel periodo evento.',
      signals: ['pressione'],
    }
  }
  if (area === 'Dew Point') {
    return {
      machine: `Essiccatore ${room}`,
      value: '6,1 °C',
      threshold: '> 3,0 °C',
      cause: 'Prestazione essiccatore non stabile oppure rigenerazione non completata.',
      notes: 'Controllare stato dryers, scaricatori e temperatura ingresso.',
      signals: ['dewpoint', 'temperatura'],
    }
  }
  if (area === 'Essiccatori') {
    return {
      machine: `Essiccatore ${room}`,
      value: 'Fault',
      threshold: 'Running',
      cause: 'Arresto o fault segnalato dal dryer di linea.',
      notes: 'Verificare fault locale, alimentazione e stato contattore.',
      signals: ['dewpoint', 'temperatura'],
    }
  }
  if (area === 'Comunicazione / Sistema') {
    return {
      machine: `Gateway ${room}`,
      value: 'Offline',
      threshold: 'Online',
      cause: 'Perdita comunicazione temporanea tra acquisizione e sorgente campo.',
      notes: 'Controllare switch, gateway e timeout polling.',
      signals: ['potenza_kw'],
    }
  }
  return {
    machine: `Compressore ${room}`,
    value: 'Trip',
    threshold: 'Running',
    cause: 'Anomalia operativa o fermata imprevista compressore.',
    notes: 'Verificare storico macchina, fault locale e carico sala.',
    signals: ['potenza_kw', 'flusso_nm3h'],
  }
}

function enrichAlarmEvent(alarm: AlarmEvent, room: string, index: number): AlarmRecord {
  const effectiveRoom = alarm.room || room
  const description = alarm.msg || alarm.message || alarm.code
  const area = inferArea(`${alarm.code} ${description}`)
  const defaults = areaDefaults(area, effectiveRoom)
  const severity = inferSeverity(alarm.severity)
  const status =
    alarm.active === false
      ? 'returned'
      : alarm.ack_time
        ? 'ack'
        : inferStatus(description)
  const title = alarm.code ? `${alarm.code} - ${description}` : description

  return {
    id: alarm.id || `${room}-${alarm.code}-${alarm.ts}-${index}`,
    severity,
    status,
    timestampStart: alarm.ts,
    timestampEnd: status === 'returned' ? alarm.ack_time || new Date(new Date(alarm.ts).getTime() + 18 * 60_000).toISOString() : null,
    tag: alarm.code || `ALM-${index + 1}`,
    title,
    description,
    machine: defaults.machine,
    area,
    value: defaults.value,
    threshold: defaults.threshold,
    room: effectiveRoom,
    acknowledged: Boolean(alarm.ack_time) || status === 'ack',
    source: 'api',
    possibleCause: defaults.cause,
    notes: defaults.notes,
    chartTarget: effectiveRoom,
    chartSignals: defaults.signals,
  }
}

function buildMockAlarms(room: string, referenceNowMs: number): AlarmRecord[] {
  return [
    {
      id: `${room}-pressure-drop`,
      severity: 'critical',
      status: 'active',
      timestampStart: isoMinutesAgo(12, referenceNowMs),
      timestampEnd: null,
      tag: 'PT-060',
      title: 'Caduta pressione rete principale',
      description: 'La pressione linea principale e scesa sotto il limite minimo configurato.',
      machine: `Linea pressione ${room}`,
      area: 'Pressione',
      value: '5,7 barg',
      threshold: '< 6,2 barg',
      room,
      acknowledged: false,
      source: 'mock',
      possibleCause: 'Richiesta istantanea elevata o sequenza compressori non ottimale.',
      notes: 'Aprire i grafici nel periodo evento per verificare risposta rete e potenza.',
      chartTarget: room,
      chartSignals: ['pressione', 'flusso_nm3h', 'potenza_kw'],
    },
    {
      id: `${room}-dryer-dew`,
      severity: 'high',
      status: 'active',
      timestampStart: isoMinutesAgo(37, referenceNowMs),
      timestampEnd: null,
      tag: 'AT-061',
      title: 'Dew point fuori specifica',
      description: 'Il dew point ha superato la soglia ammessa in uscita essiccatore.',
      machine: `Essiccatore ${room}`,
      area: 'Dew Point',
      value: '5,9 °C',
      threshold: '> 3,0 °C',
      room,
      acknowledged: false,
      source: 'mock',
      possibleCause: 'Prestazione essiccatore degradata o carico umidita elevato.',
      notes: 'Controllare rigenerazione, by-pass e temperatura ingresso aria.',
      chartTarget: room,
      chartSignals: ['dewpoint', 'temperatura'],
    },
    {
      id: `${room}-compressor-trip`,
      severity: 'high',
      status: 'ack',
      timestampStart: isoMinutesAgo(84, referenceNowMs),
      timestampEnd: null,
      tag: 'CMP-204',
      title: 'Compressore fermato per fault',
      description: 'Il compressore di servizio ha segnalato fault e si e portato in arresto.',
      machine: `Compressore A ${room}`,
      area: 'Compressori',
      value: 'Trip',
      threshold: 'Running',
      room,
      acknowledged: true,
      source: 'mock',
      possibleCause: 'Fault macchina locale o protezione elettrica intervenuta.',
      notes: 'Verificare fault list PLC/controllore compressore.',
      chartTarget: room,
      chartSignals: ['potenza_kw', 'flusso_nm3h'],
    },
    {
      id: `${room}-dryer-state`,
      severity: 'medium',
      status: 'ack',
      timestampStart: isoMinutesAgo(205, referenceNowMs),
      timestampEnd: null,
      tag: 'DRY-118',
      title: 'Essiccatore in fault intermittente',
      description: 'Fault essiccatore riconosciuto, ancora da verificare in campo.',
      machine: `Essiccatore ${room}`,
      area: 'Essiccatori',
      value: 'Fault',
      threshold: 'Running',
      room,
      acknowledged: true,
      source: 'mock',
      possibleCause: 'Intervento termico o anomalia di rigenerazione temporanea.',
      notes: 'Monitorare eventuale ricorrenza nel turno corrente.',
      chartTarget: room,
      chartSignals: ['temperatura', 'dewpoint'],
    },
    {
      id: `${room}-comm-timeout`,
      severity: 'info',
      status: 'ack',
      timestampStart: isoMinutesAgo(318, referenceNowMs),
      timestampEnd: null,
      tag: 'SYS-COMM',
      title: 'Timeout comunicazione gateway riconosciuto',
      description: 'Timeout di comunicazione registrato sul gateway di acquisizione, da monitorare.',
      machine: `Gateway ${room}`,
      area: 'Comunicazione / Sistema',
      value: 'Offline',
      threshold: 'Online',
      room,
      acknowledged: true,
      source: 'mock',
      possibleCause: 'Perdita rete momentanea o ritardo polling sorgente.',
      notes: 'Controllare switch industriale e quality rete se evento ricorrente.',
      chartTarget: room,
      chartSignals: ['potenza_kw'],
    },
  ]
}

function toggleValue<T extends string>(items: T[], nextValue: T) {
  return items.includes(nextValue) ? items.filter((value) => value !== nextValue) : [...items, nextValue]
}

function countBy<T extends string>(items: AlarmRecord[], readValue: (alarm: AlarmRecord) => T) {
  return items.reduce<Record<string, number>>((acc, alarm) => {
    const key = readValue(alarm)
    acc[key] = (acc[key] || 0) + 1
    return acc
  }, {})
}

function buildAlarmChartRange(alarm: AlarmRecord, paddingMinutes: number = 60) {
  const start = new Date(alarm.timestampStart).getTime()
  const end = alarm.timestampEnd ? new Date(alarm.timestampEnd).getTime() : start
  return {
    from: new Date(start - paddingMinutes * 60_000).toISOString(),
    to: new Date(end + paddingMinutes * 60_000).toISOString(),
  }
}

function SeverityBadge({ severity }: { severity: AlarmSeverity }) {
  const meta = SEVERITY_META[severity]
  return (
    <span className={joinClassNames('inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs font-semibold', meta.badge)}>
      <span className={joinClassNames('h-2.5 w-2.5 rounded-full', meta.dot)} />
      <span>{meta.label}</span>
    </span>
  )
}

function StatusBadge({ status }: { status: AlarmStatus }) {
  const meta = STATUS_META[status]
  return <span className={joinClassNames('inline-flex rounded-full border px-2.5 py-1 text-xs font-semibold', meta.badge)}>{meta.label}</span>
}

function SummaryCard({
  label,
  value,
  detail,
  tone,
}: {
  label: string
  value: string
  detail: string
  tone?: 'critical' | 'high' | 'medium' | 'info' | 'neutral' | 'success'
}) {
  const toneClass =
    tone === 'critical'
      ? 'border-rose-200/90 bg-rose-50/80'
      : tone === 'high'
        ? 'border-orange-200/90 bg-orange-50/80'
        : tone === 'medium'
          ? 'border-amber-200/90 bg-amber-50/80'
          : tone === 'info'
            ? 'border-sky-200/90 bg-sky-50/80'
            : tone === 'warning'
              ? 'border-orange-200/80 bg-orange-50/60'
        : tone === 'success'
          ? 'border-emerald-200/80 bg-emerald-50/60'
          : 'border-slate-200/80 bg-white/92'

  const valueClass =
    tone === 'critical'
      ? 'text-rose-700'
      : tone === 'high'
        ? 'text-orange-700'
        : tone === 'medium'
          ? 'text-amber-700'
          : tone === 'info'
            ? 'text-sky-700'
            : 'text-slate-950'

  const detailClass =
    tone === 'critical'
      ? 'text-rose-700/80'
      : tone === 'high'
        ? 'text-orange-700/80'
        : tone === 'medium'
          ? 'text-amber-700/85'
          : tone === 'info'
            ? 'text-sky-700/80'
            : 'text-slate-500'

  const baseClassName = joinClassNames(
    'rounded-2xl border px-5 py-4 text-left shadow-[0_12px_22px_-26px_rgba(15,23,42,0.16)]',
    toneClass
  )

  return (
    <div className={baseClassName}>
      <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</div>
      <div className={joinClassNames('mt-2.5 text-[1.95rem] font-bold tracking-[-0.04em]', valueClass)}>{value}</div>
      <div className={joinClassNames('mt-1.5 text-[12px] leading-5', detailClass)}>{detail}</div>
    </div>
  )
}

function QuickFilterChip({
  label,
  count,
  active,
  onClick,
}: {
  label: string
  count: number
  active: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={joinClassNames(
        'inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-semibold transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-300',
        active
          ? 'border-sky-300 bg-sky-50 text-sky-900 ring-1 ring-sky-200 shadow-[0_14px_22px_-22px_rgba(14,165,233,0.28)]'
          : 'border-slate-200/90 bg-white/90 text-slate-600 hover:border-slate-300 hover:bg-slate-50'
      )}
    >
      <span>{label}</span>
      <span className={joinClassNames('rounded-full px-2 py-0.5 text-[11px] font-semibold', active ? 'bg-white text-sky-800 shadow-sm' : 'bg-slate-100 text-slate-500')}>
        {count}
      </span>
    </button>
  )
}

function FilterGroup<T extends string>({
  title,
  options,
  selected,
  counts,
  onToggle,
}: {
  title: string
  options: Array<{ key: T; label: string }>
  selected: T[]
  counts: Record<string, number>
  onToggle: (value: T) => void
}) {
  return (
    <div className="rounded-2xl bg-white/55 p-3.5">
      <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{title}</div>
      <div className="mt-3 space-y-2">
        {options.map((option) => {
          const active = selected.includes(option.key)
          return (
            <button
              key={option.key}
              type="button"
              onClick={() => onToggle(option.key)}
              className={joinClassNames(
                'flex w-full items-center justify-between rounded-xl border px-3 py-2.5 text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-300',
                active
                  ? 'border-sky-300 bg-sky-50 text-sky-950 ring-1 ring-sky-200 shadow-[0_12px_18px_-20px_rgba(14,165,233,0.28)]'
                  : 'border-transparent bg-white/80 text-slate-700 hover:border-slate-200 hover:bg-white'
              )}
            >
              <span className="inline-flex items-center gap-3">
                <span className={joinClassNames('flex h-4 w-4 items-center justify-center rounded-[5px] border text-[11px] font-bold transition', active ? 'border-sky-600 bg-sky-600 text-white shadow-sm' : 'border-slate-300 bg-white text-transparent')}>
                  ✓
                </span>
                <span className={joinClassNames('text-sm font-medium', active ? 'text-sky-950' : '')}>{option.label}</span>
              </span>
              <span className={joinClassNames('rounded-full px-2 py-0.5 text-xs font-semibold', active ? 'bg-white text-sky-800 shadow-sm' : 'bg-slate-100 text-slate-500')}>
                {counts[option.key] || 0}
              </span>
            </button>
          )
        })}
      </div>
    </div>
  )
}

function DetailField({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-slate-200/80 bg-white px-3.5 py-3 shadow-[0_10px_20px_-26px_rgba(15,23,42,0.16)]">
      <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</div>
      <div className="mt-1.5 text-[15px] font-bold text-slate-900">{value}</div>
    </div>
  )
}

function DetailSection({ title, children, className }: { title: string; children: ReactNode; className?: string }) {
  return (
    <section className={joinClassNames('rounded-2xl border border-slate-200/80 bg-white/92 p-4 shadow-[0_18px_30px_-30px_rgba(15,23,42,0.16)]', className)}>
      <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{title}</div>
      <div className="mt-3">{children}</div>
    </section>
  )
}

export default function Alarms() {
  const location = useLocation()
  const navigate = useNavigate()
  const authUser = useMemo(() => getAuthUserFromSessionToken(), [])
  const initialAlarmContext = useMemo(() => {
    const state = location.state as { alarmContext?: { plant?: string | null; room?: string | null } } | null
    return state?.alarmContext || null
  }, [location.state])
  const initialAlarmContextAppliedRef = useRef(false)
  const [demoReferenceNowMs] = useState(() => Date.now())
  const allowedSites = useMemo(
    () =>
      Object.entries(SITE_ROOMS)
        .filter(([siteKey]) => {
          const siteId = legacyKeyToSiteId(siteKey)
          return siteId ? canViewSite(authUser, siteId) : false
        })
        .map(([legacyKey]) => {
          const siteId = legacyKeyToSiteId(legacyKey)
          return {
            legacyKey,
            siteId,
            label: siteId ? siteNameFromId(siteId) : legacyKey,
          }
        }),
    [authUser]
  )
  const [selectedSiteLabel, setSelectedSiteLabel] = useState('')
  const [quickFilter, setQuickFilter] = useState<QuickFilterKey>('all')
  const [severityFilters, setSeverityFilters] = useState<AlarmSeverity[]>([])
  const [statusFilters, setStatusFilters] = useState<AlarmStatus[]>([])
  const [roomFilters, setRoomFilters] = useState<string[]>([])
  const [areaFilters, setAreaFilters] = useState<AlarmArea[]>([])
  const [searchText, setSearchText] = useState('')
  const [statusOverridesByAlarm, setStatusOverridesByAlarm] = useState<Record<string, AlarmStatus>>({})
  const [manualNotesByAlarm, setManualNotesByAlarm] = useState<Record<string, string>>({})
  const deferredSearchText = useDeferredValue(searchText.trim().toLowerCase())
  const [selectedAlarmId, setSelectedAlarmId] = useState<string | null>(null)
  const [scadaModalRoom, setScadaModalRoom] = useState<string | null>(null)

  useEffect(() => {
    if (selectedSiteLabel && allowedSites.some((site) => site.label === selectedSiteLabel)) return
    if (allowedSites.length) {
      setSelectedSiteLabel(allowedSites[0].label)
    }
  }, [allowedSites, selectedSiteLabel])

  useEffect(() => {
    if (initialAlarmContextAppliedRef.current) return
    if (!initialAlarmContext || !allowedSites.length) return

    const normalizedPlant = (initialAlarmContext.plant || '').trim().toUpperCase()
    const normalizedRoom = (initialAlarmContext.room || '').trim().toUpperCase()

    let matchedSite =
      allowedSites.find((site) => site.label.trim().toUpperCase() === normalizedPlant || site.legacyKey.trim().toUpperCase() === normalizedPlant) || null

    if (!matchedSite && normalizedRoom) {
      matchedSite =
        allowedSites.find((site) => (SITE_ROOMS[site.legacyKey] || []).some((room) => room.trim().toUpperCase() === normalizedRoom)) || null
    }

    if (matchedSite) {
      setSelectedSiteLabel(matchedSite.label)
      if (normalizedRoom) {
        const matchedRoom = (SITE_ROOMS[matchedSite.legacyKey] || []).find((room) => room.trim().toUpperCase() === normalizedRoom)
        if (matchedRoom) {
          setRoomFilters([matchedRoom])
        }
      }
    }

    initialAlarmContextAppliedRef.current = true
  }, [allowedSites, initialAlarmContext])

  const selectedSite = useMemo(
    () => allowedSites.find((site) => site.label === selectedSiteLabel) || allowedSites[0] || null,
    [allowedSites, selectedSiteLabel]
  )
  const allowedRooms = useMemo(
    () => (selectedSite ? [...(SITE_ROOMS[selectedSite.legacyKey] || [])] : []),
    [selectedSite]
  )

  useEffect(() => {
    setRoomFilters((current) => current.filter((room) => allowedRooms.includes(room)))
  }, [allowedRooms])
  const usingMockData = allowedRooms.length > 0

  const allAlarms = useMemo(
    () =>
      allowedRooms
        .flatMap((room) => buildMockAlarms(room, demoReferenceNowMs))
        .map((alarm) => {
          const overriddenStatus = statusOverridesByAlarm[alarm.id]
          return overriddenStatus
            ? {
                ...alarm,
                status: overriddenStatus,
                acknowledged: overriddenStatus === 'ack' || alarm.acknowledged,
              }
            : alarm
        })
        .sort((a, b) => new Date(b.timestampStart).getTime() - new Date(a.timestampStart).getTime()),
    [allowedRooms, demoReferenceNowMs, statusOverridesByAlarm]
  )

  const openAlarms = useMemo(() => allAlarms.filter((alarm) => alarm.status !== 'returned'), [allAlarms])
  const severityCounts = useMemo(() => countBy(openAlarms, (alarm) => alarm.severity), [openAlarms])
  const statusCounts = useMemo(() => countBy(allAlarms, (alarm) => alarm.status), [allAlarms])
  const roomCounts = useMemo(() => countBy(allAlarms, (alarm) => alarm.room), [allAlarms])
  const areaCounts = useMemo(() => countBy(allAlarms, (alarm) => alarm.area), [allAlarms])
  const roomOptions = useMemo(
    () => [...allowedRooms].sort((a, b) => a.localeCompare(b, 'it-IT')).map((room) => ({ key: room, label: room })),
    [allowedRooms]
  )

  const quickFilterCounts = useMemo<Record<QuickFilterKey, number>>(
    () => ({
      all: allAlarms.length,
      critical: allAlarms.filter((alarm) => alarm.severity === 'critical').length,
      compressors: allAlarms.filter((alarm) => alarm.area === 'Compressori').length,
      dryers: allAlarms.filter((alarm) => alarm.area === 'Essiccatori').length,
      pressure: allAlarms.filter((alarm) => alarm.area === 'Pressione').length,
      dew: allAlarms.filter((alarm) => alarm.area === 'Dew Point').length,
    }),
    [allAlarms]
  )
  const latestAlarm = allAlarms[0] || null

  const filteredAlarms = useMemo(() => {
    return allAlarms
      .filter((alarm) => {
        if (quickFilter === 'critical') return alarm.severity === 'critical'
        if (quickFilter === 'compressors') return alarm.area === 'Compressori'
        if (quickFilter === 'dryers') return alarm.area === 'Essiccatori'
        if (quickFilter === 'pressure') return alarm.area === 'Pressione'
        if (quickFilter === 'dew') return alarm.area === 'Dew Point'
        return true
      })
      .filter((alarm) => (severityFilters.length ? severityFilters.includes(alarm.severity) : true))
      .filter((alarm) => (statusFilters.length ? statusFilters.includes(alarm.status) : true))
      .filter((alarm) => (roomFilters.length ? roomFilters.includes(alarm.room) : true))
      .filter((alarm) => (areaFilters.length ? areaFilters.includes(alarm.area) : true))
      .filter((alarm) => {
        if (!deferredSearchText) return true
        const searchable = [alarm.tag, alarm.title, alarm.description, alarm.machine, alarm.area, alarm.room].join(' ').toLowerCase()
        return searchable.includes(deferredSearchText)
      })
      .sort((a, b) => new Date(b.timestampStart).getTime() - new Date(a.timestampStart).getTime())
  }, [allAlarms, quickFilter, severityFilters, statusFilters, roomFilters, areaFilters, deferredSearchText])

  useEffect(() => {
    if (!filteredAlarms.length) {
      setSelectedAlarmId(null)
      return
    }
    if (!selectedAlarmId || !filteredAlarms.some((alarm) => alarm.id === selectedAlarmId)) {
      setSelectedAlarmId(filteredAlarms[0].id)
    }
  }, [filteredAlarms, selectedAlarmId])

  const selectedAlarm = filteredAlarms.find((alarm) => alarm.id === selectedAlarmId) || null
  const selectedAlarmManualNotes = selectedAlarm ? (manualNotesByAlarm[selectedAlarm.id] ?? '') : ''
  const selectedAlarmChartRange = selectedAlarm ? buildAlarmChartRange(selectedAlarm, 60) : null
  const canAcknowledgeSelectedAlarm = selectedAlarm ? selectedAlarm.status !== 'ack' && selectedAlarm.status !== 'returned' : false
  const canResolveSelectedAlarm = selectedAlarm ? selectedAlarm.status !== 'returned' : false
  const activeCount = openAlarms.length
  const criticalCount = openAlarms.filter((alarm) => alarm.severity === 'critical').length
  const highCount = openAlarms.filter((alarm) => alarm.severity === 'high').length
  const mediumCount = openAlarms.filter((alarm) => alarm.severity === 'medium').length
  const infoCount = openAlarms.filter((alarm) => alarm.severity === 'info').length
  const lastUpdateLabel = formatDateTime(latestAlarm?.timestampStart || null)
  const isFetchingAny = false

  const openAlarmCharts = (alarm: AlarmRecord, centeredRange: boolean) => {
    if (!alarm.room) return
    navigate(`/sale/${encodeURIComponent(alarm.room)}/grafici`, {
      state: centeredRange
        ? {
            scrollToTop: true,
            chartRange: buildAlarmChartRange(alarm, 60),
            alarmContext: { alarmId: alarm.id, tag: alarm.tag, machine: alarm.machine },
          }
        : {
            scrollToTop: true,
            resetRange: true,
            alarmContext: { alarmId: alarm.id, tag: alarm.tag, machine: alarm.machine },
          },
    })
  }

  const acknowledgeAlarm = (alarm: AlarmRecord) => {
    setStatusOverridesByAlarm((current) => ({
      ...current,
      [alarm.id]: 'ack',
    }))
  }

  const resolveAlarm = (alarm: AlarmRecord) => {
    setStatusOverridesByAlarm((current) => ({
      ...current,
      [alarm.id]: 'returned',
    }))
  }

  useEffect(() => {
    if (!scadaModalRoom) return

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setScadaModalRoom(null)
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      window.removeEventListener('keydown', handleKeyDown)
      document.body.style.overflow = previousOverflow
    }
  }, [scadaModalRoom])

  const pageLoading = !selectedSite && allowedSites.length > 0

  return (
    <AppLayout
      title="Allarmi impianto"
      subtitle={selectedSite ? `Monitoraggio eventi in tempo reale - ${selectedSite.label}` : 'Monitoraggio eventi in tempo reale'}
      plant={selectedSiteLabel}
      onPlantChange={setSelectedSiteLabel}
      selectorOptions={allowedSites.map((site) => site.label)}
      selectorPlaceholder="Seleziona impianto"
      scadaPlant={selectedAlarm?.room || allowedRooms[0] || ''}
      chartsPlant={selectedAlarm?.room || allowedRooms[0] || ''}
    >
      <div className="space-y-5">
        <Card className="overflow-hidden border-slate-200/70 bg-[linear-gradient(180deg,_#ffffff,_#f8fafc)] shadow-[0_16px_36px_-34px_rgba(15,23,42,0.18)]">
          <CardContent className="space-y-6 p-5 sm:p-6">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
              <div className="space-y-1.5">
                <div>
                  <div className="flex flex-wrap items-center gap-3">
                    <h2 className="text-[2.05rem] font-bold leading-none tracking-[-0.035em] text-slate-950">Allarmi impianto</h2>
                    <span className="inline-flex items-center rounded-full border border-rose-200 bg-rose-50/90 px-3 py-1 text-base font-bold text-rose-700 shadow-[0_10px_18px_-18px_rgba(225,29,72,0.35)]">
                      {activeCount}
                    </span>
                  </div>
                  <p className="hidden">
                    Vista operativa per gravita, stato e area, con accesso diretto ai grafici nel periodo dell’evento.
                  </p>
                  <div className="mt-2 flex items-center gap-2 text-sm text-slate-500">
                    <span className={joinClassNames('h-1.5 w-1.5 rounded-full', isFetchingAny ? 'animate-pulse bg-sky-500' : 'bg-slate-400')} />
                    {`Last update ${lastUpdateLabel}`}
                  </div>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                {usingMockData ? (
                  <span className="inline-flex items-center gap-2 rounded-full border border-amber-200/80 bg-amber-50/80 px-3 py-1.5 text-xs font-semibold text-amber-700">
                    <span className="h-2 w-2 rounded-full bg-amber-500" />
                    Anteprima demo
                  </span>
                ) : null}
                {isFetchingAny ? (
                  <span className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-600">
                    <span className="h-2 w-2 animate-pulse rounded-full bg-sky-500" />
                    Aggiornamento
                  </span>
                ) : null}
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
              {pageLoading ? (
                Array.from({ length: 5 }).map((_, index) => <Skeleton key={index} className="h-[6.8rem] w-full rounded-2xl" />)
              ) : (
                <>
                  <SummaryCard
                    label="Critici"
                    value={String(criticalCount)}
                    detail={criticalCount > 0 ? 'Priorita immediata' : 'Nessun critico'}
                    tone={criticalCount > 0 ? 'critical' : 'neutral'}
                  />
                  <SummaryCard
                    label="Alto"
                    value={String(highCount)}
                    detail={highCount > 0 ? 'Priorita elevata' : 'Nessun alto'}
                    tone={highCount > 0 ? 'high' : 'success'}
                  />
                  <SummaryCard
                    label="Medio"
                    value={String(mediumCount)}
                    detail={mediumCount > 0 ? 'Anomalie da monitorare' : 'Nessun medio'}
                    tone={mediumCount > 0 ? 'medium' : 'success'}
                  />
                  <SummaryCard
                    label="Info"
                    value={String(infoCount)}
                    detail={infoCount > 0 ? 'Eventi informativi' : 'Nessun info'}
                    tone={infoCount > 0 ? 'info' : 'neutral'}
                  />
                  <SummaryCard
                    label="Ultimo evento"
                    value={latestAlarm ? formatCompactDateTime(latestAlarm.timestampStart) : '--'}
                    detail={latestAlarm ? latestAlarm.title : 'Nessun evento disponibile'}
                  />
                </>
              )}
            </div>

            <div className="flex flex-wrap gap-2">
              {QUICK_FILTERS.map((chip) => (
                <QuickFilterChip
                  key={chip.key}
                  label={chip.label}
                  count={quickFilterCounts[chip.key]}
                  active={quickFilter === chip.key}
                  onClick={() => setQuickFilter(chip.key)}
                />
              ))}
            </div>
          </CardContent>
        </Card>

        <div className="grid gap-5 xl:grid-cols-[236px_minmax(0,1fr)] xl:items-stretch">
          <div className="h-full xl:sticky xl:top-5">
            <Card className="h-full border-slate-200/60 bg-slate-50/70 shadow-[0_14px_28px_-34px_rgba(15,23,42,0.12)]">
              <CardContent className="flex h-full flex-col gap-4 p-4">
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Ricerca</div>
                  <div className="mt-3 rounded-2xl border border-slate-200/80 bg-white/85 px-3 py-2.5">
                    <input
                      value={searchText}
                      onChange={(event) => setSearchText(event.target.value)}
                      placeholder="Tag, descrizione, sala..."
                      className="w-full border-none bg-transparent text-sm text-slate-800 outline-none placeholder:text-slate-400"
                    />
                  </div>
                </div>

                <FilterGroup title="Severita" options={SEVERITY_OPTIONS} selected={severityFilters} counts={severityCounts} onToggle={(value) => setSeverityFilters((current) => toggleValue(current, value))} />
                <FilterGroup title="Stato" options={STATUS_OPTIONS} selected={statusFilters} counts={statusCounts} onToggle={(value) => setStatusFilters((current) => toggleValue(current, value))} />
                <FilterGroup title="Sale" options={roomOptions} selected={roomFilters} counts={roomCounts} onToggle={(value) => setRoomFilters((current) => toggleValue(current, value))} />
                <FilterGroup title="Area" options={AREA_OPTIONS.map((value) => ({ key: value, label: value }))} selected={areaFilters} counts={areaCounts} onToggle={(value) => setAreaFilters((current) => toggleValue(current, value))} />
              </CardContent>
            </Card>
          </div>

          <Card className="h-full overflow-hidden border-slate-200/70 bg-white shadow-[0_18px_42px_-32px_rgba(15,23,42,0.18)]">
            <CardContent className="h-full p-0">
              <div className="grid h-full xl:grid-cols-[minmax(0,1.92fr)_minmax(300px,0.68fr)]">
                <div className="flex min-w-0 flex-col border-b border-slate-200/80 xl:border-b-0 xl:border-r">
                  <div className="flex items-center justify-between border-b border-slate-200/80 px-5 py-4">
                    <div>
                      <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">Storico allarmi</div>
                      <div className="mt-1 text-sm font-semibold text-slate-900">{filteredAlarms.length} elementi</div>
                    </div>
                  </div>

                  <div className="min-h-0 flex-1 overflow-auto">
                    <table className="min-w-full table-fixed">
                      <thead className="sticky top-0 z-10 bg-white/95 backdrop-blur">
                        <tr className="border-b border-slate-200/90 text-left text-[11px] font-bold uppercase tracking-[0.16em] text-slate-700">
                          <th className="w-[118px] pl-5 pr-4 py-3.5">Severita</th>
                          <th className="w-[92px] px-4 py-3.5">Stato</th>
                          <th className="w-[128px] px-4 py-3.5">Ora/Data</th>
                          <th className="w-[88px] px-4 py-3.5">Tag</th>
                          <th className="px-4 py-3">Descrizione</th>
                          <th className="hidden w-[148px] px-4 py-3.5 lg:table-cell">Sala / Area</th>
                          <th className="hidden w-[96px] px-4 py-3.5 xl:table-cell">Valore</th>
                          <th className="hidden w-[96px] px-4 py-3.5 xl:table-cell">Soglia</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredAlarms.length === 0 ? (
                          <tr>
                            <td colSpan={8} className="px-6 py-12">
                              <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50/70 px-6 py-10 text-center">
                                <div className="text-base font-semibold text-slate-900">
                                  {allAlarms.length === 0 ? 'Nessun allarme disponibile' : 'Nessun allarme corrisponde ai filtri'}
                                </div>
                                <div className="mt-2 text-sm text-slate-500">
                                  {allAlarms.length === 0
                                    ? 'Quando arrivano nuovi eventi dalle sale abilitate compariranno qui automaticamente.'
                                    : 'Prova a rimuovere un filtro o a cercare un altro tag/area.'}
                                </div>
                              </div>
                            </td>
                          </tr>
                        ) : null}

                        {filteredAlarms.map((alarm) => {
                          const selected = alarm.id === selectedAlarmId
                          return (
                            <tr
                              key={alarm.id}
                              onClick={() => setSelectedAlarmId(alarm.id)}
                              className={joinClassNames(
                                'group cursor-pointer border-b border-slate-100/90 transition even:bg-slate-50/35 hover:bg-slate-100/70',
                                selected && 'bg-sky-50 shadow-[inset_0_0_0_1px_rgba(56,189,248,0.38)]'
                              )}
                            >
                              <td className="pl-5 pr-4 py-3 align-top">
                                <div className="relative pl-3">
                                  <span
                                    className={joinClassNames(
                                      'absolute inset-y-0 left-0 w-1.5 rounded-r-full opacity-0 transition group-hover:opacity-60',
                                      SEVERITY_META[alarm.severity].rail,
                                      selected && 'opacity-100'
                                    )}
                                  />
                                  <SeverityBadge severity={alarm.severity} />
                                </div>
                              </td>
                              <td className="px-4 py-3 align-top"><StatusBadge status={alarm.status} /></td>
                              <td className="px-4 py-3 align-top text-sm font-medium text-slate-800">{formatDateTime(alarm.timestampStart)}</td>
                              <td className="px-4 py-3 align-top text-sm font-semibold text-slate-800">{alarm.tag}</td>
                              <td className="px-4 py-3 align-top">
                                <div className="text-[14px] font-semibold text-slate-950">{alarm.title}</div>
                                <div className="mt-1.5 line-clamp-2 text-[13px] leading-5 text-slate-500">{alarm.description}</div>
                              </td>
                              <td className="hidden px-4 py-3 align-top lg:table-cell">
                                <div className="text-sm font-semibold text-slate-900">{alarm.room}</div>
                                <div className="mt-1 text-xs font-medium uppercase tracking-[0.12em] text-slate-500">{alarm.area}</div>
                              </td>
                              <td className="hidden px-4 py-3 align-top xl:table-cell text-sm font-semibold text-slate-700">{alarm.value}</td>
                              <td className="hidden px-4 py-3 align-top xl:table-cell text-sm text-slate-500">{alarm.threshold}</td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className={joinClassNames('flex min-h-0 min-w-0 flex-col self-stretch bg-[linear-gradient(180deg,_#f8fafc,_#eef3f8)] transition-colors', selectedAlarm && 'bg-[linear-gradient(180deg,_#eef8ff,_#e8f2fb)]')}>
                  <div className={joinClassNames('border-b border-slate-200/80 px-5 py-4 transition-colors', selectedAlarm && 'border-sky-300 bg-sky-100/70 shadow-[inset_0_-1px_0_rgba(125,211,252,0.45)]')}>
                    <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Dettaglio allarme</div>
                    <div className="mt-1 text-sm font-semibold text-transparent select-none">45 elementi</div>
                  </div>

                  <div className="hidden border-b border-slate-200/80 xl:block">
                    <div className="px-5 py-3.5">
                      <div className="text-[11px] font-bold uppercase tracking-[0.16em] text-transparent select-none">Allineamento tabella</div>
                    </div>
                  </div>

                  <div className="min-h-0 flex-1 overflow-auto p-5">
                    {selectedAlarm ? (
                      <div className="space-y-4">
                        <div className="rounded-[24px] border border-sky-200/80 bg-white px-4 py-4 shadow-[0_24px_40px_-28px_rgba(14,165,233,0.18)] ring-1 ring-sky-100/80">
                          <div className="flex flex-wrap items-center gap-2">
                            <SeverityBadge severity={selectedAlarm.severity} />
                            <StatusBadge status={selectedAlarm.status} />
                            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 shadow-sm">
                              {selectedAlarm.room}
                            </span>
                          </div>
                          <div className="mt-3">
                            <h3 className="text-[1.45rem] font-bold tracking-[-0.025em] text-slate-950">{selectedAlarm.title}</h3>
                            <p className="mt-2 text-[15px] leading-6 text-slate-700">{selectedAlarm.description}</p>
                          </div>
                        </div>

                        <DetailSection title="Azioni" className="bg-white">
                          <div className="flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() => setScadaModalRoom(selectedAlarm.room)}
                              className="rounded-xl border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-slate-50"
                            >
                              SCADA
                            </button>
                            <button
                              type="button"
                              onClick={() => openAlarmCharts(selectedAlarm, true)}
                              className="rounded-xl border border-sky-200 bg-sky-50 px-4 py-2.5 text-sm font-semibold text-sky-700 transition hover:border-sky-300 hover:bg-sky-100/70"
                            >
                              Apri nel periodo allarme
                            </button>
                            <button
                              type="button"
                              onClick={() => selectedAlarm && acknowledgeAlarm(selectedAlarm)}
                              disabled={!canAcknowledgeSelectedAlarm}
                              className={joinClassNames(
                                'rounded-xl px-4 py-2.5 text-sm font-semibold transition',
                                canAcknowledgeSelectedAlarm
                                  ? 'border border-indigo-200 bg-indigo-50 text-indigo-700 hover:border-indigo-300 hover:bg-indigo-100/70'
                                  : 'cursor-not-allowed border border-slate-200 bg-slate-100 text-slate-400'
                              )}
                            >
                              {selectedAlarm?.status === 'ack' ? 'Riconosciuto' : 'Segna riconosciuto'}
                            </button>
                            <button
                              type="button"
                              onClick={() => selectedAlarm && resolveAlarm(selectedAlarm)}
                              disabled={!canResolveSelectedAlarm}
                              className={joinClassNames(
                                'rounded-xl px-4 py-2.5 text-sm font-semibold transition',
                                canResolveSelectedAlarm
                                  ? 'border border-emerald-200 bg-emerald-50 text-emerald-700 hover:border-emerald-300 hover:bg-emerald-100/70'
                                  : 'cursor-not-allowed border border-slate-200 bg-slate-100 text-slate-400'
                              )}
                            >
                              {selectedAlarm?.status === 'returned' ? 'Rientrato' : 'Risolto'}
                            </button>
                          </div>
                          <div className="mt-3 rounded-2xl bg-slate-50 px-3 py-2.5 text-xs font-medium leading-5 text-slate-600">
                            {selectedAlarmChartRange
                              ? `Finestra evento: ${formatDateTime(selectedAlarmChartRange.from)} - ${formatDateTime(selectedAlarmChartRange.to)}`
                              : '--'}
                          </div>
                        </DetailSection>

                        <DetailSection title="Timeline">
                          <div className="grid gap-3 sm:grid-cols-2">
                            <DetailField label="Inizio" value={formatDateTime(selectedAlarm.timestampStart)} />
                            <DetailField label="Fine" value={formatDateTime(selectedAlarm.timestampEnd)} />
                            <DetailField label="Ultimo evento" value={formatRelativeTime(selectedAlarm.timestampStart)} />
                            <DetailField label="Stato corrente" value={STATUS_META[selectedAlarm.status].label} />
                          </div>
                        </DetailSection>

                        <DetailSection title="Sistema">
                          <div className="grid gap-3 sm:grid-cols-2">
                            <DetailField label="Sala" value={selectedAlarm.room} />
                            <DetailField label="Area" value={selectedAlarm.area} />
                            <DetailField label="Tag" value={selectedAlarm.tag} />
                            <DetailField label="Macchina" value={selectedAlarm.machine} />
                          </div>
                        </DetailSection>

                        <DetailSection title="Parametri / Codici">
                          <div className="grid gap-3 sm:grid-cols-2">
                            <DetailField label="Valore rilevato" value={selectedAlarm.value} />
                            <DetailField label="Soglia" value={selectedAlarm.threshold} />
                            <DetailField label="Origine dati" value={selectedAlarm.source === 'mock' ? 'Mock preview' : 'API realtime'} />
                            <DetailField label="Segnali utili" value={selectedAlarm.chartSignals?.join(', ') || '--'} />
                          </div>
                        </DetailSection>

                        <DetailSection title="Diagnostica">
                          <div className="space-y-3.5">
                            <div>
                              <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Possibile causa</div>
                              <p className="mt-1.5 text-sm leading-6 text-slate-700">{selectedAlarm.possibleCause}</p>
                            </div>
                            <div>
                              <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Note operative</div>
                              <p className="mt-1.5 text-sm leading-6 text-slate-700">{selectedAlarm.notes}</p>
                            </div>
                            <div>
                              <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Note aggiuntive</div>
                              <div className="mt-2 rounded-2xl border border-slate-200 bg-slate-50/80 px-3 py-3">
                                <textarea
                                  value={selectedAlarmManualNotes}
                                  onChange={(event) =>
                                    setManualNotesByAlarm((current) => ({
                                      ...current,
                                      [selectedAlarm.id]: event.target.value,
                                    }))
                                  }
                                  placeholder="Scrivi qui osservazioni operative, interventi eseguiti o controlli da fare..."
                                  rows={4}
                                  className="w-full resize-none border-none bg-transparent text-sm leading-6 text-slate-700 outline-none placeholder:text-slate-400"
                                />
                              </div>
                            </div>
                          </div>
                        </DetailSection>
                      </div>
                    ) : (
                      <div className="rounded-2xl border border-dashed border-slate-200 bg-white/80 px-6 py-12 text-center">
                        <div className="text-base font-semibold text-slate-900">Nessun allarme selezionato</div>
                        <div className="mt-2 text-sm text-slate-500">
                          Seleziona una riga dalla tabella per vedere dettaglio, gravita e azioni rapide.
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
      {scadaModalRoom ? (
        <div className="fixed inset-0 z-[80] flex items-center justify-center bg-slate-950/60 p-2 backdrop-blur-sm">
          <div className="relative flex h-[96vh] w-[calc(100vw-1rem)] max-w-[1800px] flex-col overflow-hidden rounded-[28px] border border-slate-200/80 bg-white shadow-[0_32px_80px_-36px_rgba(15,23,42,0.42)]">
            <div className="flex items-center justify-between border-b border-slate-200/80 px-5 py-4">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">SCADA</div>
                <div className="mt-1 text-base font-semibold text-slate-950">{scadaModalRoom}</div>
              </div>
              <button
                type="button"
                onClick={() => setScadaModalRoom(null)}
                className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-slate-200 bg-white text-slate-500 transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-800"
                aria-label="Chiudi popup SCADA"
              >
                <svg viewBox="0 0 24 24" className="h-4 w-4" aria-hidden="true">
                  <path d="M6 6 18 18M18 6 6 18" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" />
                </svg>
              </button>
            </div>
            <div className="min-h-0 flex-1 bg-slate-50">
              <iframe
                title={`SCADA ${scadaModalRoom}`}
                src={`/scada/${encodeURIComponent(scadaModalRoom)}?embedded=1`}
                className="h-full w-full border-0"
              />
            </div>
          </div>
        </div>
      ) : null}
    </AppLayout>
  )
}
