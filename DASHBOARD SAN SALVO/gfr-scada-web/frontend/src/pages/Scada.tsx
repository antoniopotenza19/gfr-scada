import { useEffect, useMemo, useState } from 'react'
import { Navigate, useNavigate, useParams } from 'react-router-dom'
import { useQueries } from '@tanstack/react-query'
import { sendMachineCommand } from '../api/commands'
import { fetchPlantSummary } from '../api/plants'
import AppLayout from '../components/layout/AppLayout'
import type { ScadaMachine, ScadaMachineStatus } from '../components/synoptic/ScadaSala'
import ScadaSala from '../components/synoptic/ScadaSala'
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card'
import { usePlantSummary } from '../hooks/usePlantSummary'
import { usePlants } from '../hooks/usePlants'
import { SITE_ROOMS } from '../constants/siteRooms'
import { legacyKeyToSiteId } from '../constants/sites'
import { canRemoteControl, canViewDevFeatures, canViewSite, getAuthUserFromSessionToken } from '../utils/auth'
import type { PlantSummary } from '../types/api'
import './scada-page.css'

type SignalInfo = { value: number; unit: string; ts: string }
type SignalMap = Record<string, SignalInfo>

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

const ROOM_ALIASES: Record<string, string[]> = {
  LAMINATO: ['LAMINATI', 'LaminatiAlta', 'LaminatiBassa'],
  LAMINATI: ['LAMINATO', 'LaminatiAlta', 'LaminatiBassa'],
  'PRIMO ALTA': ['PRIMOAlta'],
  'PRIMO BASSA': ['PRIMOBassa'],
  'SS1 COMPOSIZIONE': ['COMPOSIZIONE'],
  'SS2 COMPOSIZIONE': ['SS2 Bassa Pressione'],
}

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

function parseMachines(signals: SignalMap) {
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

  for (const [signalName, info] of Object.entries(signals || {})) {
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
      const machineName = normalizeMachineName(cosphi[1])
      const machine = getOrCreate(machineName)
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

function resolveMachine(rawName: string) {
  const key = canonicalToken(rawName)
  return (
    LAMINATO_MACHINE_MAP.find((entry) =>
      entry.aliases.some((alias) => {
        const aliasKey = canonicalToken(alias)
        return key === aliasKey || key.includes(aliasKey) || aliasKey.includes(key)
      })
    ) || null
  )
}

function buildLaminatoMachines(signals: SignalMap): ScadaMachine[] {
  const slots = new Map<string, ParsedMachine>()
  for (const machine of parseMachines(signals).values()) {
    const mapped = resolveMachine(machine.rawName)
    if (!mapped) continue
    slots.set(mapped.id, machine)
  }

  return LAMINATO_MACHINE_MAP.map((entry) => {
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
    }
  })
}

function buildBravoMachines(signals: SignalMap): ScadaMachine[] {
  const slots = new Map<string, ParsedMachine>()
  for (const machine of parseMachines(signals).values()) {
    const mapped = BRAVO_MACHINE_MAP.find((entry) =>
      entry.aliases.some((alias) => {
        const aliasKey = canonicalToken(alias)
        const key = canonicalToken(machine.rawName)
        return key === aliasKey || key.includes(aliasKey) || aliasKey.includes(key)
      })
    )
    if (!mapped) continue
    slots.set(mapped.id, machine)
  }

  return BRAVO_MACHINE_MAP.map((entry) => {
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
    }
  })
}

function buildGenericMachines(signals: SignalMap): ScadaMachine[] {
  const parsed = Array.from(parseMachines(signals).values())
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
    }
  })
}

function buildRoomMachines(roomLabel: string, signals: SignalMap): ScadaMachine[] {
  const upper = roomLabel.toUpperCase()
  if (upper.includes('LAMINATO') || upper.includes('LAMINATI')) return buildLaminatoMachines(signals)
  if (upper.includes('BRAVO')) return buildBravoMachines(signals)
  return buildGenericMachines(signals)
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

function norm(value: string) {
  return value.trim().toUpperCase().replace(/\s+/g, ' ')
}

function canon(value: string) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, '')
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
  for (const label of labels) {
    const direct = normalizedPlants.get(norm(label)) || canonicalPlants.get(canon(label))
    if (direct) {
      mapping.set(label, [direct])
      continue
    }
    const aliases = resolveApiRoomsForLabel(label, normalizedPlants, canonicalPlants)
    mapping.set(label, aliases)
  }
  return mapping
}

function parseTsMs(ts: string | null | undefined) {
  if (!ts) return null
  const ms = new Date(ts).getTime()
  return Number.isFinite(ms) ? ms : null
}

type RoomIndicatorStatus = 'active' | 'standby' | 'warning' | 'off'

export default function Scada() {
  const { plant } = useParams()
  const requested = (plant || '').trim()
  const navigate = useNavigate()
  const authUser = getAuthUserFromSessionToken()
  const debugEnabled = Boolean(import.meta.env.DEV) && canViewDevFeatures(authUser)
  const canControl = canRemoteControl(authUser)
  const { data: apiPlants } = usePlants()

  const normalizedPlants = useMemo(() => {
    const map = new Map<string, string>()
    for (const item of apiPlants || []) map.set(norm(item), item)
    return map
  }, [apiPlants])
  const canonicalPlants = useMemo(() => {
    const map = new Map<string, string>()
    for (const item of apiPlants || []) map.set(canon(item), item)
    return map
  }, [apiPlants])

  const siteEntry = Object.entries(SITE_ROOMS).find(([, rooms]) =>
    rooms.some((room) => room.trim().toUpperCase() === requested.toUpperCase())
  )
  const plantSiteId = legacyKeyToSiteId(siteEntry?.[0])
  if (requested && plantSiteId && !canViewSite(authUser, plantSiteId)) {
    return <Navigate to="/403" replace />
  }

  const allowedSites = Object.keys(SITE_ROOMS).filter((siteKey) => {
    const siteId = legacyKeyToSiteId(siteKey)
    return siteId ? canViewSite(authUser, siteId) : false
  })
  if (allowedSites.length === 0) return <Navigate to="/403" replace />

  const initialSite = siteEntry?.[0] && allowedSites.includes(siteEntry[0]) ? siteEntry[0] : allowedSites[0]
  const [site, setSite] = useState(initialSite)
  const rooms = SITE_ROOMS[site] || []
  const [room, setRoom] = useState(() => {
    const found = rooms.find((item) => item.trim().toUpperCase() === requested.toUpperCase())
    return found || rooms[0] || ''
  })

  useEffect(() => {
    if (!siteEntry?.[0] || !allowedSites.includes(siteEntry[0])) return
    setSite(siteEntry[0])
  }, [siteEntry, allowedSites])

  useEffect(() => {
    if (!rooms.length) {
      setRoom('')
      return
    }
    const found = rooms.find((item) => item.trim().toUpperCase() === requested.toUpperCase())
    setRoom((prev) => (found || (prev && rooms.includes(prev) ? prev : rooms[0])))
  }, [requested, rooms])

  const roomApiMapping = useMemo(
    () => buildRoomApiMapping(rooms, normalizedPlants, canonicalPlants),
    [rooms, normalizedPlants, canonicalPlants]
  )
  const siteApiRooms = useMemo(() => {
    const all = rooms.flatMap((label) => roomApiMapping.get(label) || [])
    return Array.from(new Set(all))
  }, [rooms, roomApiMapping])
  const siteSummaryQueries = useQueries({
    queries: siteApiRooms.map((apiRoom) => ({
      queryKey: ['scada-room-summary', site, apiRoom],
      queryFn: async () => fetchPlantSummary(apiRoom),
      staleTime: 10_000,
      cacheTime: 120_000,
      keepPreviousData: true,
      refetchInterval: 10_000,
      refetchIntervalInBackground: true,
      refetchOnWindowFocus: true,
      refetchOnReconnect: true,
      retry: 1,
    })),
  })
  const summariesByApiRoom = useMemo(() => {
    const map = new Map<string, PlantSummary>()
    siteApiRooms.forEach((apiRoom, index) => {
      const summary = siteSummaryQueries[index]?.data as PlantSummary | undefined
      if (summary) map.set(apiRoom, summary)
    })
    return map
  }, [siteApiRooms, siteSummaryQueries])

  const selectedApiRoom = (roomApiMapping.get(room) || [])[0] || ''
  const summaryQuery = usePlantSummary(selectedApiRoom, !!selectedApiRoom)
  const signals = (summaryQuery.data?.signals || {}) as SignalMap

  const flowSignalName = pickSignalNameByPatterns(signals, FLOW_SIGNAL_EXACT, FLOW_SIGNAL_INCLUDES) || ''
  const powerSignalName = pickSignalNameByPatterns(signals, POWER_SIGNAL_EXACT, POWER_SIGNAL_INCLUDES) || ''
  const pressureSignalName = pickSignalNameByPatterns(signals, PRESSURE_SIGNAL_EXACT, PRESSURE_SIGNAL_INCLUDES) || ''
  const dewSignalName = pickSignalNameByPatterns(signals, DEW_SIGNAL_EXACT, DEW_SIGNAL_INCLUDES) || ''
  const tempSignalName = pickSignalNameByPatterns(signals, TEMP_SIGNAL_EXACT, TEMP_SIGNAL_INCLUDES) || ''

  const flowValue = flowSignalName ? Number(signals[flowSignalName]?.value) : null
  const powerValue = powerSignalName ? Number(signals[powerSignalName]?.value) : null
  const pressureValue = pressureSignalName ? Number(signals[pressureSignalName]?.value) : null
  const dewValue = dewSignalName ? Number(signals[dewSignalName]?.value) : null
  const tempValue = tempSignalName ? Number(signals[tempSignalName]?.value) : null
  const csValue = powerValue != null && flowValue != null && flowValue > 0 ? powerValue / flowValue : null
  const roomMachines = useMemo(() => buildRoomMachines(room, signals), [room, signals])

  const lastUpdate = summaryQuery.data
    ? new Date(summaryQuery.data.last_update).toLocaleTimeString()
    : '--'

  const siteId = (legacyKeyToSiteId(site) || 'san-salvo') as 'san-salvo' | 'marghera'
  const commandRoomId = selectedApiRoom || room

  const handleMachineCommand = async (machineId: string, command: 'START' | 'STOP') => {
    if (!canControl || !commandRoomId) return
    try {
      await sendMachineCommand(siteId, commandRoomId, machineId, command)
      if (debugEnabled) {
        console.debug('[SCADA][command]', { siteId, roomId: commandRoomId, machineId, command })
      }
    } catch (error) {
      console.error('[SCADA][command:error]', { siteId, roomId: commandRoomId, machineId, command, error })
    }
  }

  useEffect(() => {
    if (!debugEnabled) return
    console.debug('[SCADA][selection]', {
      site,
      room,
      apiRoom: selectedApiRoom,
      map: Object.fromEntries(roomApiMapping),
      at: new Date().toISOString(),
    })
  }, [debugEnabled, site, room, selectedApiRoom, roomApiMapping])

  useEffect(() => {
    if (!debugEnabled) return
    const payload = summaryQuery.data as PlantSummary | undefined
    console.debug('[SCADA][summary]', {
      queryKey: ['plantSummary', selectedApiRoom, null],
      fetchedAt: summaryQuery.dataUpdatedAt ? new Date(summaryQuery.dataUpdatedAt).toISOString() : null,
      responseSize: Object.keys(payload?.signals || {}).length,
      lastUpdate: payload?.last_update || null,
    })
  }, [debugEnabled, selectedApiRoom, summaryQuery.data, summaryQuery.dataUpdatedAt])

  const roomStatusClass = (label: string) => {
    const mapped = (roomApiMapping.get(label) || [])[0] || ''
    if (!mapped) return 'is-unmapped'
    if (label === room) return 'is-selected'
    return 'is-ready'
  }

  const roomIndicatorStatus = (label: string): RoomIndicatorStatus => {
    const mapped = (roomApiMapping.get(label) || [])[0] || ''
    if (!mapped) return 'off'
    const summary = summariesByApiRoom.get(mapped)
    if (!summary) return 'off'

    const roomSignals = (summary.signals || {}) as SignalMap
    const flowName = pickSignalNameByPatterns(roomSignals, FLOW_SIGNAL_EXACT, FLOW_SIGNAL_INCLUDES)
    const powerName = pickSignalNameByPatterns(roomSignals, POWER_SIGNAL_EXACT, POWER_SIGNAL_INCLUDES)
    const flowValue = flowName ? Number(roomSignals[flowName]?.value) : null
    const powerValue = powerName ? Number(roomSignals[powerName]?.value) : null
    const hasRealtimeMismatchIssue =
      label.trim().toUpperCase() === 'SS1' &&
      Number.isFinite(flowValue as number) &&
      Number.isFinite(powerValue as number) &&
      (flowValue as number) > 0.1 &&
      (powerValue as number) <= 0

    const tsMs = parseTsMs(summary.last_update)
    const isStale = tsMs == null || Date.now() - tsMs > 60_000
    if (isStale) return 'warning'
    if (hasRealtimeMismatchIssue) return 'warning'

    if (!Number.isFinite(powerValue as number)) return 'off'
    if ((powerValue as number) > 0) {
      if (!Number.isFinite(flowValue as number) || (flowValue as number) <= 0) return 'warning'
      return 'active'
    }

    return 'standby'
  }

  const roomStatusText = (label: string) => {
    const status = roomIndicatorStatus(label)
    if (status === 'active') return 'ACTIVE'
    if (status === 'standby') return 'STANDBY'
    if (status === 'warning') return 'WARNING'
    return 'OFF'
  }

  const roomBadgeClass = (label: string) => {
    const status = roomIndicatorStatus(label)
    if (status === 'active') return 'is-active'
    if (status === 'standby') return 'is-standby'
    if (status === 'warning') return 'is-warning'
    return 'is-off'
  }
  const selectedRoomIndicatorStatus = room ? roomIndicatorStatus(room) : 'off'
  const pageTitle = `${site}${room ? ` - SALA ${room}` : ''}`

  return (
    <AppLayout
      title={pageTitle}
      subtitle={`Last update: ${lastUpdate || '--'}`}
      plant={site}
      onPlantChange={(nextSite) => {
        if (!nextSite) return
        const allowed = allowedSites.includes(nextSite) ? nextSite : site
        setSite(allowed)
        const nextRoom = (SITE_ROOMS[allowed] || [])[0] || ''
        setRoom(nextRoom)
        if (nextRoom) navigate(`/scada/${nextRoom}`)
      }}
      selectorOptions={allowedSites}
      selectorPlaceholder="Select site"
      scadaPlant={selectedApiRoom || room}
    >
      <div className="scada-page">
        <div className="scada-page-grid">
          <Card className="scada-rooms-card h-full">
            <CardHeader>
              <CardTitle className="text-slate-900">SALE</CardTitle>
            </CardHeader>
            <CardContent className="scada-rooms-content">
              <div className="scada-room-list">
                {rooms.map((label, index) => {
                  const rowClass = roomStatusClass(label)
                  const badgeClass = roomBadgeClass(label)
                  const statusText = roomStatusText(label)
                  return (
                    <button
                      key={label}
                      type="button"
                      onClick={() => {
                        setRoom(label)
                        navigate(`/scada/${label}`)
                      }}
                      className={`scada-room-row ${rowClass}`}
                    >
                      <span>{`${index + 1}. ${label}`}</span>
                      <span className={`scada-room-badge ${badgeClass}`}>
                        <span className={`scada-room-badge-dot ${badgeClass}`} aria-hidden="true" />
                        {statusText}
                      </span>
                    </button>
                  )
                })}
              </div>
            </CardContent>
          </Card>

          <div className="scada-shell-panel min-w-0">
            {selectedApiRoom ? (
              <div className="scada-shell-frame">
                <ScadaSala
                  title={`SCADA Sala ${room}`}
                  lastUpdate={lastUpdate}
                  dryerImageUrl="/images/scada/essiccatore.png"
                  machines={roomMachines}
                  roomStatus={selectedRoomIndicatorStatus}
                  instruments={{
                    totalKw: Number.isFinite(powerValue as number) ? Number(powerValue) : 0,
                    cs: Number.isFinite(csValue as number) ? Number(csValue) : 0,
                    dewPoint: Number.isFinite(dewValue as number) ? Number(dewValue) : 0,
                    pressure: Number.isFinite(pressureValue as number) ? Number(pressureValue) : 0,
                    flow: Number.isFinite(flowValue as number) ? Number(flowValue) : 0,
                    temp: Number.isFinite(tempValue as number) ? Number(tempValue) : 0,
                  }}
                  onStart={canControl ? (machineId) => handleMachineCommand(machineId, 'START') : undefined}
                  onStop={canControl ? (machineId) => handleMachineCommand(machineId, 'STOP') : undefined}
                  onClose={() => navigate('/dashboard')}
                />
              </div>
            ) : (
              <Card>
                <CardContent className="py-8 text-sm text-slate-500">
                  Nessun mapping API disponibile per la sala selezionata.
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </AppLayout>
  )
}
