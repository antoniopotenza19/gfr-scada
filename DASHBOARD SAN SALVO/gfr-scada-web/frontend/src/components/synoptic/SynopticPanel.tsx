import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { sendMachineCommand, type MachineCommand } from '../../api/commands'
import type { PlantSignalInfo } from '../../types/plantTable'
import { canRemoteControl, getAuthUserFromSessionToken } from '../../utils/auth'
import { Card, CardContent } from '../ui/Card'
import MachineNode, { type SynopticMachineStatus } from './MachineNode'
import PipeNetwork from './PipeNetwork'

type SynopticSlot = 'M1' | 'M2' | 'V1' | 'C1' | 'C2' | 'C3'

interface SynopticPanelProps {
  siteId: 'san-salvo' | 'marghera'
  roomId: string
  roomLabel: string
  lastUpdate: string
  signals?: Record<string, PlantSignalInfo>
  flowNm3h: number | null
  pressureBar: number | null
  temperatureC: number | null
  dewPointC: number | null
  powerTotalKw: number | null
  csAttuale: number | null
  onClose: () => void
  onMachineSelect?: (machine: { machineId: string; slot: SynopticSlot; label: string }) => void
}

interface ParsedMachine {
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

interface SynopticMachineView {
  slot: SynopticSlot
  machineId: string
  label: string
  status: SynopticMachineStatus
  powerKw: number | null
  cosphi: number | null
  u1: number | null
  u2: number | null
  u3: number | null
  i1: number | null
  i2: number | null
  i3: number | null
}

interface ToastItem {
  id: number
  type: 'success' | 'error'
  message: string
}

const BRAVO_MACHINE_MAP: Array<{ id: SynopticSlot; label: string; aliases: string[] }> = [
  { id: 'M1', label: 'MATTEI 1', aliases: ['MATTEI N1', 'MATTEI 1'] },
  { id: 'M2', label: 'MATTEI 2', aliases: ['MATTEI N2', 'MATTEI 2'] },
  { id: 'V1', label: 'GA90 VSD', aliases: ['GA90 VSD'] },
]

const LAMINATO_MACHINE_MAP: Array<{ id: SynopticSlot; label: string; aliases: string[] }> = [
  { id: 'C1', label: 'BOOSTER C1', aliases: ['BOOSTER', 'TEMPO 2 1850', 'TEMPO2'] },
  { id: 'C2', label: 'CREPELLE N.2 P27-200', aliases: ['CREPELLE N2', 'CREPELLE N 2', 'CREPELLE 2', 'CREPELLEN2'] },
  { id: 'C3', label: 'CREPELLE N.3 40P20', aliases: ['CREPELLE N3', 'CREPELLE N 3', 'CREPELLE 3', 'CREPELLEN3'] },
]
const CREPELLE_IMAGE_SRC = '/images/scada/crepelle.png'
const BOOSTER_IMAGE_SRC = '/images/scada/siadbooster.png'
const DRYER_IMAGE_SRC = '/images/scada/essiccatore.png'
const BOILER_IMAGE_SRC = '/images/scada/boiler.png'

function canonicalToken(value: string) {
  return value
    .toUpperCase()
    .replace(/N[\u00B0\u00BA]/g, 'N')
    .replace(/[^A-Z0-9]/g, '')
}

function normalizeMachineName(rawName: string) {
  return rawName
    .replace(/\s*\((?:V|A|KW)\)\s*$/i, '')
    .replace(/^3PH\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim()
}

function parseMachines(signals?: Record<string, PlantSignalInfo>) {
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

    const voltage = signal.match(/^U([123])\s+(.+)$/i)
    if (voltage) {
      const machine = getOrCreate(voltage[2])
      const value = Number(info.value)
      if (voltage[1] === '1') machine.u1 = value
      if (voltage[1] === '2') machine.u2 = value
      if (voltage[1] === '3') machine.u3 = value
      continue
    }

    const current = signal.match(/^[IL]\s*([123])\s+(.+)$/i)
    if (current) {
      const machine = getOrCreate(current[2])
      const value = Number(info.value)
      if (current[1] === '1') machine.i1 = value
      if (current[1] === '2') machine.i2 = value
      if (current[1] === '3') machine.i3 = value
      continue
    }

    const cosphi = signal.match(/^cosphi\s+(.+)$/i)
    if (cosphi) {
      const machine = getOrCreate(cosphi[1])
      machine.cosphi = Number(info.value)
      continue
    }

    const power = signal.match(/^Potenza Attiva\s+(.+)$/i)
    if (power) {
      const machineName = normalizeMachineName(power[1])
      if (/\bTOT\b|\bTOTAL\b/i.test(machineName)) continue
      const machine = getOrCreate(machineName)
      machine.activePowerKw = Number(info.value)
    }
  }

  return map
}

function statusFromMachine(machine: ParsedMachine): SynopticMachineStatus {
  const currents = [machine.i1, machine.i2, machine.i3].filter(
    (value): value is number => value != null && Number.isFinite(value)
  )
  const avgCurrent = currents.length > 0 ? currents.reduce((sum, value) => sum + value, 0) / currents.length : null
  const hasVoltage = [machine.u1, machine.u2, machine.u3].some((value) => value != null && Number.isFinite(value))
  const power = machine.activePowerKw
  const isOn = (power ?? 0) > 0.5 || (avgCurrent ?? 0) > 1
  if (isOn && (((avgCurrent ?? 0) === 0 && currents.length > 0) || (power != null && power === 0))) return 'alarm'
  if (isOn) return 'active'
  if (hasVoltage) return 'standby'
  return 'offline'
}

function resolveSlot(rawName: string, candidates: Array<{ id: SynopticSlot; label: string; aliases: string[] }>) {
  const machineKey = canonicalToken(rawName)
  return (
    candidates.find((entry) =>
      entry.aliases.some((alias) => {
        const aliasKey = canonicalToken(alias)
        return machineKey === aliasKey || machineKey.includes(aliasKey) || aliasKey.includes(machineKey)
      })
    ) || null
  )
}

function machineViewFromParsed(slot: SynopticSlot, label: string, machineId: string, machine: ParsedMachine | null): SynopticMachineView {
  if (!machine) {
    return {
      slot,
      machineId,
      label,
      status: 'offline',
      powerKw: null,
      cosphi: null,
      u1: null,
      u2: null,
      u3: null,
      i1: null,
      i2: null,
      i3: null,
    }
  }

  return {
    slot,
    machineId,
    label,
    status: statusFromMachine(machine),
    powerKw: machine.activePowerKw,
    cosphi: machine.cosphi,
    u1: machine.u1,
    u2: machine.u2,
    u3: machine.u3,
    i1: machine.i1,
    i2: machine.i2,
    i3: machine.i3,
  }
}

function buildMachineViews(roomLabel: string, parsedMap: Map<string, ParsedMachine>): SynopticMachineView[] {
  const parsed = Array.from(parsedMap.values())
  const upper = roomLabel.toUpperCase()
  const isBravo = upper.includes('BRAVO')
  const isLaminato = upper.includes('LAMINATO') || upper.includes('LAMINATI')

  if (isBravo) {
    const slots = BRAVO_MACHINE_MAP.map((entry) => machineViewFromParsed(entry.id, entry.label, entry.id, null))
    for (const machine of parsed) {
      const slot = resolveSlot(machine.rawName, BRAVO_MACHINE_MAP)
      if (!slot) continue
      const idx = slots.findIndex((item) => item.slot === slot.id)
      if (idx < 0) continue
      slots[idx] = machineViewFromParsed(slot.id, slot.label, slot.id, machine)
    }
    return slots
  }

  if (isLaminato) {
    const slots = LAMINATO_MACHINE_MAP.map((entry) => machineViewFromParsed(entry.id, entry.label, entry.id, null))
    for (const machine of parsed) {
      const slot = resolveSlot(machine.rawName, LAMINATO_MACHINE_MAP)
      if (!slot) continue
      const idx = slots.findIndex((item) => item.slot === slot.id)
      if (idx < 0) continue
      slots[idx] = machineViewFromParsed(slot.id, slot.label, slot.id, machine)
    }
    return slots
  }

  const sorted = parsed
    .slice()
    .sort((a, b) => (Number(b.activePowerKw || 0) || 0) - (Number(a.activePowerKw || 0) || 0))
    .slice(0, 3)

  const slotOrder: SynopticSlot[] = ['M1', 'M2', 'V1']
  return slotOrder.map((slot, index) => {
    const machine = sorted[index] || null
    const machineId = machine ? canonicalToken(machine.rawName).slice(0, 16) || slot : slot
    const label = machine ? machine.rawName.toUpperCase() : slot
    return machineViewFromParsed(slot, label, machineId, machine)
  })
}

function findDryerFromParsed(parsedMap: Map<string, ParsedMachine>) {
  const parsed = Array.from(parsedMap.values())
  const dryer = parsed.find((item) => /ZR4|DRY|ESSIC|ESS\b/i.test(item.rawName)) || null
  if (!dryer) {
    return { label: 'ESSICCATORE 1', status: 'offline' as SynopticMachineStatus }
  }
  return {
    label: `ESSICCATORE 1 (${dryer.rawName.toUpperCase()})`,
    status: statusFromMachine(dryer),
  }
}

function formatNumber(value: number | null, digits = 1) {
  if (value == null || !Number.isFinite(value)) return '--'
  return value.toFixed(digits)
}

function statusBadgeClass(status: SynopticMachineStatus) {
  if (status === 'active') return 'border-green-200 bg-green-50 text-green-700'
  if (status === 'standby') return 'border-[#ebcf80] bg-[#fff8df] text-[#996300]'
  if (status === 'alarm') return 'border-rose-200 bg-rose-50 text-rose-700'
  return 'border-slate-300 bg-slate-100 text-slate-700'
}

function statusLabel(status: SynopticMachineStatus) {
  if (status === 'active') return 'Acceso'
  if (status === 'standby') return 'Standby'
  if (status === 'alarm') return 'Allarme'
  return 'Offline'
}

function statusLabelCompact(status: SynopticMachineStatus) {
  if (status === 'active') return 'ACTIVE'
  if (status === 'standby') return 'STANDBY'
  if (status === 'alarm') return 'ALARM'
  return 'OFFLINE'
}

function statusTone(status: SynopticMachineStatus) {
  if (status === 'active') return { bg: '#ecfdf5', border: '#86efac', text: '#15803d' }
  if (status === 'standby') return { bg: '#fff8df', border: '#ebcf80', text: '#996300' }
  if (status === 'alarm') return { bg: '#fee2e2', border: '#fecaca', text: '#b91c1c' }
  return { bg: '#f1f5f9', border: '#cbd5e1', text: '#475569' }
}

export default function SynopticPanel({
  siteId,
  roomId,
  roomLabel,
  lastUpdate,
  signals,
  flowNm3h,
  pressureBar,
  temperatureC,
  dewPointC,
  powerTotalKw,
  csAttuale,
  onClose,
  onMachineSelect,
}: SynopticPanelProps) {
  const [isMobile, setIsMobile] = useState(() => window.matchMedia('(max-width: 767px)').matches)
  const [selectedSlot, setSelectedSlot] = useState<SynopticSlot>('M1')
  const [toasts, setToasts] = useState<ToastItem[]>([])
  const [loadingByKey, setLoadingByKey] = useState<Record<string, boolean>>({})
  const toastIdRef = useRef(1)

  const authUser = getAuthUserFromSessionToken()
  const canControl = canRemoteControl(authUser)
  const isLaminato = roomLabel.toUpperCase().includes('LAMINATO') || roomLabel.toUpperCase().includes('LAMINATI')

  const parsedMap = useMemo(() => parseMachines(signals), [signals])
  const machines = useMemo(() => buildMachineViews(roomLabel, parsedMap), [roomLabel, parsedMap])
  const dryerInfo = useMemo(() => findDryerFromParsed(parsedMap), [parsedMap])

  const selectedMachine = machines.find((machine) => machine.slot === selectedSlot) || machines[0]
  const machineStates = useMemo(
    () =>
      machines.reduce<Record<string, SynopticMachineStatus>>((acc, machine) => {
        acc[machine.slot] = machine.status
        return acc
      }, {}),
    [machines]
  )
  const machineBySlot = useMemo(
    () =>
      machines.reduce<Record<string, SynopticMachineView>>((acc, machine) => {
        acc[machine.slot] = machine
        return acc
      }, {}),
    [machines]
  )

  useEffect(() => {
    if (!machines.length) return
    if (machines.some((machine) => machine.slot === selectedSlot)) return
    setSelectedSlot(machines[0].slot)
  }, [machines, selectedSlot])

  useEffect(() => {
    if (!selectedMachine) return
    onMachineSelect?.({
      machineId: selectedMachine.machineId,
      slot: selectedMachine.slot,
      label: selectedMachine.label,
    })
  }, [selectedMachine, onMachineSelect])

  useEffect(() => {
    const query = window.matchMedia('(max-width: 767px)')
    const handler = () => setIsMobile(query.matches)
    query.addEventListener('change', handler)
    return () => query.removeEventListener('change', handler)
  }, [])

  const addToast = (type: 'success' | 'error', message: string) => {
    const id = toastIdRef.current++
    setToasts((prev) => [...prev, { id, type, message }])
    window.setTimeout(() => {
      setToasts((prev) => prev.filter((item) => item.id !== id))
    }, 2800)
  }

  const runCommand = async (machineId: string, command: MachineCommand) => {
    if (!canControl) {
      addToast('error', 'Comando non autorizzato per il tuo ruolo.')
      return
    }
    const key = `${machineId}:${command}`
    try {
      setLoadingByKey((prev) => ({ ...prev, [key]: true }))
      const response = await sendMachineCommand(siteId, roomId, machineId, command)
      if (response.mode === 'direct') {
        addToast('success', `${command} inviato a ${machineId}`)
      } else {
        addToast('success', `${command} accodato a ${machineId}`)
      }
    } catch (error: any) {
      const detail = String(error?.response?.data?.detail || error?.message || 'errore comando')
      addToast('error', `Comando fallito: ${detail}`)
    } finally {
      setLoadingByKey((prev) => ({ ...prev, [key]: false }))
    }
  }

  const renderDefs = () => (
    <defs>
      <filter id="scada-shadow" x="-20%" y="-20%" width="140%" height="140%">
        <feDropShadow dx="0" dy="1.5" stdDeviation="1.6" floodOpacity="0.13" />
      </filter>
      <linearGradient id="tank-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stopColor="#e2e8f0" />
        <stop offset="50%" stopColor="#f1f5f9" />
        <stop offset="100%" stopColor="#cbd5e1" />
      </linearGradient>
    </defs>
  )

  const renderTank = (cx: number, y: number, label: string) => (
    <g key={label}>
      <ellipse cx={cx} cy={y} rx={28} ry={7} fill="url(#tank-gradient)" stroke="#94a3b8" />
      <rect x={cx - 28} y={y} width={56} height={66} fill="url(#tank-gradient)" stroke="#94a3b8" />
      <ellipse cx={cx} cy={y + 66} rx={28} ry={7} fill="url(#tank-gradient)" stroke="#94a3b8" />
      <text x={cx} y={y + 86} textAnchor="middle" fontSize={10.5} fill="#475569" fontWeight={700}>
        {label}
      </text>
    </g>
  )

  const renderDefaultLayout = () => (
    <>
      {renderDefs()}
      <PipeNetwork machineStates={machineStates} />
      <foreignObject x={434} y={12} width={132} height={164}>
        <img src={DRYER_IMAGE_SRC} alt="Essiccatore" style={{ width: '100%', height: '100%', objectFit: 'contain' }} />
      </foreignObject>

      <MachineNode
        id={machines[0]?.machineId || 'M1'}
        label={machines[0]?.label || 'M1'}
        x={105}
        y={300}
        status={machines[0]?.status || 'offline'}
        powerKw={machines[0]?.powerKw ?? null}
        selected={selectedSlot === 'M1'}
        onClick={() => setSelectedSlot('M1')}
      />
      <MachineNode
        id={machines[1]?.machineId || 'M2'}
        label={machines[1]?.label || 'M2'}
        x={405}
        y={300}
        status={machines[1]?.status || 'offline'}
        powerKw={machines[1]?.powerKw ?? null}
        selected={selectedSlot === 'M2'}
        onClick={() => setSelectedSlot('M2')}
      />
      <MachineNode
        id={machines[2]?.machineId || 'V1'}
        label={machines[2]?.label || 'V1'}
        x={705}
        y={300}
        status={machines[2]?.status || 'offline'}
        powerKw={machines[2]?.powerKw ?? null}
        selected={selectedSlot === 'V1'}
        onClick={() => setSelectedSlot('V1')}
      />

      {renderTank(200, 512, 'SERB M1')}
      {renderTank(500, 512, 'SERB M2')}
      {renderTank(800, 512, 'SERB V1')}
    </>
  )

  const renderLaminatoLayout = () => (
    <>
      {renderDefs()}
      <PipeNetwork machineStates={machineStates} layout="laminato" />
      <foreignObject x={544} y={12} width={132} height={164}>
        <img src={DRYER_IMAGE_SRC} alt="Essiccatore" style={{ width: '100%', height: '100%', objectFit: 'contain' }} />
      </foreignObject>

      {(['C1', 'C2', 'C3'] as SynopticSlot[]).map((slot, index) => {
        const centerX = index === 0 ? 170 : index === 1 ? 610 : 1030
        const machine = machineBySlot[slot]
        const machineId = machine?.machineId || slot
        const startKey = `${machineId}:START`
        const stopKey = `${machineId}:STOP`
        const startLoading = Boolean(loadingByKey[startKey])
        const stopLoading = Boolean(loadingByKey[stopKey])
        const boilerLabel = slot === 'C1' ? 'BOILER C1' : slot === 'C2' ? 'BOILER C2' : 'BOILER C3'
        const tone = statusTone(machine?.status || 'offline')
        const imageSrc = slot === 'C1' ? BOOSTER_IMAGE_SRC : slot === 'C2' || slot === 'C3' ? CREPELLE_IMAGE_SRC : null
        const imageX = centerX - 170
        const imageY = 314
        const imageW = 240
        const imageH = 188
        const dataX = centerX + 78
        const dataY = 334
        const dataW = 205
        const dataH = 174

        return (
          <g
            key={slot}
            onClick={() => setSelectedSlot(slot)}
            style={{ cursor: 'pointer' }}
          >
            {imageSrc ? (
              <foreignObject x={imageX} y={imageY} width={imageW} height={imageH}>
                <img
                  src={imageSrc}
                  alt={machine?.label || slot}
                  style={{ width: '100%', height: '100%', objectFit: 'contain' }}
                />
              </foreignObject>
            ) : (
              <>
                <rect x={imageX + 6} y={imageY + 2} width={imageW - 12} height={imageH - 8} rx={10} fill="#ffffff" stroke="#cbd5e1" filter="url(#scada-shadow)" />
                <text x={imageX + imageW / 2} y={imageY + 86} textAnchor="middle" fontSize={12.5} fill="#64748b" fontWeight={700}>FOTO {slot}</text>
              </>
            )}

            <rect
              x={dataX}
              y={dataY}
              width={dataW}
              height={dataH}
              rx={10}
              fill="#ffffff"
              stroke={selectedSlot === slot ? '#0f172a' : '#cbd5e1'}
              strokeWidth={selectedSlot === slot ? 2 : 1}
              filter="url(#scada-shadow)"
            />
            <text x={dataX + 12} y={dataY + 22} fontSize={12} fill="#0f172a" fontWeight={700}>{machine?.label || slot}</text>
            <text x={dataX + 12} y={dataY + 62} fill="#0f172a" fontWeight={700}>
              <tspan fontSize={34}>{formatNumber(machine?.powerKw ?? null, 1)}</tspan>
              <tspan dx={8} fontSize={16} fill="#334155">kW</tspan>
            </text>
            <rect x={dataX + 12} y={dataY + 80} width={112} height={22} rx={11} fill={tone.bg} stroke={tone.border} />
            <text x={dataX + 22} y={dataY + 95} fontSize={11} fill={tone.text} fontWeight={700}>{statusLabelCompact(machine?.status || 'offline')}</text>
            <foreignObject x={dataX + 12} y={dataY + 122} width={dataW - 24} height={38}>
              <div
                xmlns="http://www.w3.org/1999/xhtml"
                style={{ display: 'flex', gap: '8px', alignItems: 'center', justifyContent: 'flex-start' }}
              >
                <button
                  type="button"
                  disabled={!canControl || startLoading || !machine}
                  onClick={(event) => {
                    event.stopPropagation()
                    runCommand(machineId, 'START')
                  }}
                  style={{
                    border: '1px solid #86efac',
                    background: '#ecfdf5',
                    color: '#15803d',
                    borderRadius: '8px',
                    padding: '5px 10px',
                    fontSize: '11px',
                    fontWeight: 700,
                    cursor: !canControl || startLoading || !machine ? 'not-allowed' : 'pointer',
                    opacity: !canControl || startLoading || !machine ? 0.55 : 1,
                  }}
                >
                  {startLoading ? 'START...' : 'ACCENDI'}
                </button>
                <button
                  type="button"
                  disabled={!canControl || stopLoading || !machine}
                  onClick={(event) => {
                    event.stopPropagation()
                    runCommand(machineId, 'STOP')
                  }}
                  style={{
                    border: '1px solid #fda4af',
                    background: '#fff1f2',
                    color: '#be123c',
                    borderRadius: '8px',
                    padding: '5px 10px',
                    fontSize: '11px',
                    fontWeight: 700,
                    cursor: !canControl || stopLoading || !machine ? 'not-allowed' : 'pointer',
                    opacity: !canControl || stopLoading || !machine ? 0.55 : 1,
                  }}
                >
                  {stopLoading ? 'STOP...' : 'SPEGNI'}
                </button>
              </div>
            </foreignObject>

            <foreignObject x={centerX - 44} y={232} width={88} height={72}>
              <img
                src={BOILER_IMAGE_SRC}
                alt={boilerLabel}
                style={{ width: '100%', height: '100%', objectFit: 'contain' }}
              />
            </foreignObject>
          </g>
        )
      })}
    </>
  )

  const panelContent = (
    <Card className="mt-3">
      <CardContent className="pt-3">
        {isLaminato ? (
          <div className="relative h-[78vh] min-h-[620px] overflow-hidden rounded-lg border border-slate-200 bg-slate-50">
            <button
              type="button"
              onClick={onClose}
              className="absolute right-3 top-3 z-10 inline-flex h-7 w-7 items-center justify-center rounded-md border border-slate-300 bg-white text-sm font-semibold text-slate-700 shadow-sm hover:bg-slate-50"
              aria-label="Chiudi SCADA"
              title="Chiudi"
            >
              x
            </button>
            <svg
              viewBox="0 0 1320 560"
              preserveAspectRatio="none"
              className={isMobile ? 'h-full min-w-[1160px] w-full select-none' : 'h-full w-full select-none'}
            >
              {renderLaminatoLayout()}
            </svg>
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,1fr)_340px]">
            <div className="relative h-[70vh] min-h-[560px] overflow-hidden rounded-lg border border-slate-200 bg-slate-50">
              <button
                type="button"
                onClick={onClose}
                className="absolute right-3 top-3 z-10 inline-flex h-7 w-7 items-center justify-center rounded-md border border-slate-300 bg-white text-sm font-semibold text-slate-700 shadow-sm hover:bg-slate-50"
                aria-label="Chiudi SCADA"
                title="Chiudi"
              >
                x
              </button>
              <svg
                viewBox="0 0 1320 560"
                preserveAspectRatio="none"
                className={isMobile ? 'h-full min-w-[1080px] w-full select-none' : 'h-full w-full select-none'}
              >
                {renderDefaultLayout()}
              </svg>
            </div>

            <div className="space-y-3">
              <div className="rounded-lg border border-slate-200 bg-white p-3 text-xs text-slate-700">
                <div className="mb-2 font-semibold text-slate-900">Strumenti di linea</div>
                <div className="space-y-2">
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-2.5">
                    <div className="text-[11px] font-semibold text-slate-600">Totale Sala</div>
                    <div className="mt-1 text-[22px] font-semibold text-slate-900">{`${formatNumber(powerTotalKw, 1)} kW`}</div>
                    <div className="text-xs text-slate-600">{`Consumo specifico ${formatNumber(csAttuale, 3)} kWh/Nm3`}</div>
                  </div>
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-2.5">
                    <div className="text-[11px] font-semibold text-slate-600">Dew Point</div>
                    <div className="mt-1 text-[31px] font-semibold text-slate-900">{`${formatNumber(dewPointC, 1)} degC`}</div>
                  </div>
                  <div className="rounded-lg border border-slate-200 bg-slate-50 p-2.5">
                    <div className="text-[11px] font-semibold text-slate-600">Flowmeter</div>
                    <div className="mt-1 space-y-1">
                      <div>{`Pressione ${formatNumber(pressureBar, 1)} bar`}</div>
                      <div>{`Flusso ${formatNumber(flowNm3h, 1)} Nm3/h`}</div>
                      <div>{`Temperatura ${formatNumber(temperatureC, 1)} degC`}</div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="rounded-lg border border-slate-200 bg-white p-3">
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-sm font-semibold text-slate-900">{selectedMachine?.label || '--'}</div>
                  <span
                    className={[
                      'inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium',
                      statusBadgeClass(selectedMachine?.status || 'offline'),
                    ].join(' ')}
                  >
                    {statusLabel(selectedMachine?.status || 'offline')}
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs text-slate-700">
                  <div>{`kW: ${formatNumber(selectedMachine?.powerKw ?? null, 1)}`}</div>
                  <div>{`cosphi: ${formatNumber(selectedMachine?.cosphi ?? null, 2)}`}</div>
                  <div>{`U: ${formatNumber(selectedMachine?.u1 ?? null, 0)} / ${formatNumber(selectedMachine?.u2 ?? null, 0)} / ${formatNumber(selectedMachine?.u3 ?? null, 0)}`}</div>
                  <div>{`I: ${formatNumber(selectedMachine?.i1 ?? null, 1)} / ${formatNumber(selectedMachine?.i2 ?? null, 1)} / ${formatNumber(selectedMachine?.i3 ?? null, 1)}`}</div>
                </div>
                <div className="mt-3 flex items-center gap-2">
                  <button
                    type="button"
                    disabled={!selectedMachine || loadingByKey[`${selectedMachine.machineId}:START`] || !canControl}
                    onClick={() => selectedMachine && runCommand(selectedMachine.machineId, 'START')}
                    className="rounded-md border border-emerald-300 bg-emerald-50 px-3 py-1.5 text-xs font-semibold text-emerald-700 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {loadingByKey[`${selectedMachine?.machineId}:START`] ? 'START...' : 'ACCENDI'}
                  </button>
                  <button
                    type="button"
                    disabled={!selectedMachine || loadingByKey[`${selectedMachine.machineId}:STOP`] || !canControl}
                    onClick={() => selectedMachine && runCommand(selectedMachine.machineId, 'STOP')}
                    className="rounded-md border border-rose-300 bg-rose-50 px-3 py-1.5 text-xs font-semibold text-rose-700 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {loadingByKey[`${selectedMachine?.machineId}:STOP`] ? 'STOP...' : 'SPEGNI'}
                  </button>
                </div>
                {!canControl ? (
                  <div className="mt-2 text-[11px] text-amber-700">Comandi bloccati: ruolo non autorizzato.</div>
                ) : null}
              </div>

              <div className="rounded-lg border border-slate-200 bg-white p-3 text-xs text-slate-700">
                <div className="mb-2 font-semibold text-slate-900">Valori sala</div>
                <div className="grid grid-cols-2 gap-1">
                  <div>{`Pressione: ${formatNumber(pressureBar, 1)} bar`}</div>
                  <div>{`Temperatura: ${formatNumber(temperatureC, 1)} degC`}</div>
                  <div>{`Dew Point: ${formatNumber(dewPointC, 1)} degC`}</div>
                  <div>{`CS: ${formatNumber(csAttuale, 3)} kWh/Nm3`}</div>
                </div>
              </div>
            </div>
          </div>
        )}
      </CardContent>

      <div className="pointer-events-none fixed bottom-4 right-4 z-[1200] flex flex-col gap-2">
        {toasts.map((toast) => (
          <div
            key={toast.id}
            className={[
              'pointer-events-auto rounded-md border px-3 py-2 text-xs shadow-md',
              toast.type === 'success' ? 'border-emerald-200 bg-emerald-50 text-emerald-800' : 'border-rose-200 bg-rose-50 text-rose-800',
            ].join(' ')}
          >
            {toast.message}
          </div>
        ))}
      </div>
    </Card>
  )

  if (isMobile) {
    return createPortal(
      <div className="fixed inset-0 z-[1100] bg-slate-950/45 p-2">
        <div className="h-full overflow-auto rounded-lg border border-slate-300 bg-white p-0">{panelContent}</div>
      </div>,
      document.body
    )
  }

  return panelContent
}
