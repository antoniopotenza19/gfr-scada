import type { PlantSignalInfo } from '../../types/plantTable'
import CompressorsRow, {
  type CompressorColumn,
  type CompressorDisplayRow,
  renderAlarmCell,
  renderStatusCell,
} from './CompressorsRow'
import type { CompressorStatus } from './StatusBadge'

interface CompressorsTableProps {
  sala: string
  signals?: Record<string, PlantSignalInfo>
  selectedMachine?: {
    machineId: string
    slot: string
    label: string
  } | null
}

interface MachineRuntimeRow {
  key: string
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

interface MachineMetaEntry {
  aliases: string[]
  name: string
  brand?: string
  model?: string
  type?: string
  nominalFlowNm3h?: string
  nominalPowerKw?: string
  usage?: string
}

const MISSING = '\u2014'

const ROOM_MACHINE_METADATA: Record<string, MachineMetaEntry[]> = {
  BRAVO: [
    {
      aliases: ['MATTEI N1', 'MATTEI 1'],
      name: 'M1',
      brand: 'MATTEI',
      model: 'MAXIMA 75',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '956',
      nominalPowerKw: '75',
      usage: 'Base (1)',
    },
    {
      aliases: ['MATTEI N2', 'MATTEI 2'],
      name: 'M2',
      brand: 'MATTEI',
      model: 'MAXIMA 75',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '956',
      nominalPowerKw: '75',
      usage: 'Base (2)',
    },
    {
      aliases: ['GA90 VSD'],
      name: 'V1',
      brand: 'ATLAS',
      model: 'GA90 VSD',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '1051',
      nominalPowerKw: '90',
      usage: 'Supporto',
    },
  ],
  CENTAC: [
    {
      aliases: ['P1'],
      name: 'C4',
      brand: 'ATLAS',
      model: 'ZR-315VSD',
      type: '---',
      nominalFlowNm3h: '2400',
      nominalPowerKw: '200',
      usage: 'Supporto',
    },
    {
      aliases: ['DB10'],
      name: 'DB10 - C4',
      brand: 'IR',
      model: 'C90 V4465',
      type: 'Centrifugo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '4960',
      nominalPowerKw: '600',
      usage: 'Base',
    },
    {
      aliases: ['DB11'],
      name: 'DB11 - C3',
      brand: 'IR',
      model: 'CV1A - C70 MX2',
      type: 'Centrifugo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '3850',
      nominalPowerKw: '480',
      usage: 'Supporto (1)',
    },
    {
      aliases: ['K6'],
      name: 'K6 - C1',
      brand: 'IR',
      model: '1ACII-SX75 MX2',
      type: 'Centrifugo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '4060',
      nominalPowerKw: '615',
      usage: 'Supporto (2)',
    },
    {
      aliases: ['K7'],
      name: 'K7 - C5',
      brand: 'IR',
      model: 'C700 MX3',
      type: 'Centrifugo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '4800',
      nominalPowerKw: '430',
      usage: 'Base',
    },
    {
      aliases: ['K9'],
      name: 'K9 - C2',
      brand: 'IR',
      model: 'C90 MX3',
      type: 'Centrifugo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '4960',
      nominalPowerKw: '600',
      usage: 'Base',
    },
  ],
  LAMINATO: [
    {
      aliases: ['BOOSTER'],
      name: 'C1',
      brand: 'SIAD',
      model: 'BOOSTER 2TV1',
      type: 'A Pistoni Raff. Acqua',
      nominalFlowNm3h: '2000',
      nominalPowerKw: '335',
      usage: 'Base',
    },
    {
      aliases: ['CREPELLE N2', 'CREPELLE 2'],
      name: 'C2',
      brand: 'ATLAS',
      model: 'CREPELLE N2 P27-200',
      type: 'Alternativo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '990',
      nominalPowerKw: '200',
      usage: 'Supporto (1)',
    },
    {
      aliases: ['CREPELLE N3', 'CREPELLE 3'],
      name: 'C3',
      brand: 'ATLAS',
      model: 'CREPELLE N3 40P20',
      type: 'Alternativo Non Lub. Raff. Acqua',
      nominalFlowNm3h: '750',
      nominalPowerKw: '132',
      usage: 'Supporto (2)',
    },
  ],
  'PRIMO ALTA': [
    {
      aliases: ['NEA ALTA PRESS', 'NEA'],
      name: 'N1',
      brand: 'NEA',
      model: '2TV1',
      type: 'A Pistoni',
      nominalFlowNm3h: '350',
      nominalPowerKw: '0',
      usage: '-',
    },
    {
      aliases: ['SIAD WS3 ALTA PRESS', 'WS3'],
      name: 'N2',
      brand: 'SIAD',
      model: 'WS3',
      type: 'A Pistoni',
      nominalFlowNm3h: '600',
      nominalPowerKw: '0',
      usage: '-',
    },
    {
      aliases: ['CREPELLE AP'],
      name: 'C1',
      brand: 'ATLAS',
      model: 'CREPELLE AP',
      type: 'A Pistoni Non Lub. Raff. Acqua',
      nominalFlowNm3h: '755',
      nominalPowerKw: '132',
      usage: 'Base',
    },
  ],
  'PRIMO BASSA': [
    {
      aliases: ['ZR55'],
      name: 'C1',
      brand: 'ATLAS',
      model: 'ZR 55',
      type: 'A Vite Non Lub. Raff. Acqua',
      nominalFlowNm3h: '516',
      nominalPowerKw: '55',
      usage: 'Base',
    },
    {
      aliases: ['GA45 N1', 'GA45 1'],
      name: 'N1',
      brand: 'ATLAS',
      model: 'GA45',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '482',
      nominalPowerKw: '45',
      usage: 'Supporto',
    },
    {
      aliases: ['GA45 N2', 'GA45 2'],
      name: 'N2',
      brand: 'ATLAS',
      model: 'GA45',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '482',
      nominalPowerKw: '45',
      usage: 'Supporto',
    },
  ],
  SS1: [
    {
      aliases: ['C2'],
      name: 'V1',
      brand: 'ATLAS',
      model: 'GA75 VSD',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '900',
      nominalPowerKw: '75',
      usage: 'Base',
    },
    {
      aliases: ['C3'],
      name: 'V2',
      brand: 'ATLAS',
      model: 'GA75 VSD',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '900',
      nominalPowerKw: '75',
      usage: 'Supporto (1)',
    },
  ],
  SS2: [
    {
      aliases: ['KAESER N1', 'KAESER 1'],
      name: 'K1',
      brand: 'KAESER',
      model: 'DSD 241',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '894',
      nominalPowerKw: '132',
      usage: 'Base (1)',
    },
    {
      aliases: ['KAESER N2', 'KAESER 2'],
      name: 'K2',
      brand: 'KAESER',
      model: 'DSD 241',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '894',
      nominalPowerKw: '132',
      usage: 'Base (2)',
    },
    {
      aliases: ['GA75 VSD'],
      name: 'V1',
      brand: 'ATLAS',
      model: 'GA75 VSD',
      type: 'A Vite Lub. Raff. Aria',
      nominalFlowNm3h: '900',
      nominalPowerKw: '75',
      usage: 'Supporto (1)',
    },
  ],
  'SS1 COMPOSIZIONE': [
    {
      aliases: ['ESS1'],
      name: '',
      brand: 'Atlas',
      model: 'SS1 Composizione - Multimetro ABB - addr 4',
      type: '',
      nominalFlowNm3h: '0',
      nominalPowerKw: '0',
      usage: '',
    },
    {
      aliases: ['ESS2'],
      name: '',
      brand: '',
      model: 'SS1 Composizione - Multimetro ABB - addr 5',
      type: '',
      nominalFlowNm3h: '0',
      nominalPowerKw: '0',
      usage: '',
    },
  ],
  'SS2 COMPOSIZIONE': [
    {
      aliases: ['C1', 'ESS', 'ESS1', 'ESS2'],
      name: 'C1 L90i',
      brand: '',
      model: 'C1 L90i',
      type: '',
      nominalFlowNm3h: '0',
      nominalPowerKw: '0',
      usage: '',
    },
  ],
}

function normalizeRoomMachineKey(roomLabel: string) {
  const key = roomLabel.toUpperCase()
  if (key === 'LAMINATI') return 'LAMINATO'
  return key
}

function canonicalMachineToken(value: string) {
  return value
    .toUpperCase()
    .replace(/N[\u00B0\u00BA]/g, 'N')
    .replace(/[^A-Z0-9]/g, '')
}

function isRowSelectedByMachine(
  row: CompressorDisplayRow,
  selectedMachine?: { machineId: string; slot: string; label: string } | null
) {
  if (!selectedMachine) return false

  const selectedTokens = [selectedMachine.machineId, selectedMachine.slot, selectedMachine.label]
    .map((value) => canonicalMachineToken(value))
    .filter((value) => value.length > 1)

  if (selectedTokens.length === 0) return false

  const rowTokens = [row.name, row.model, row.type]
    .map((value) => canonicalMachineToken(value || ''))
    .filter((value) => value.length > 1)

  return selectedTokens.some((selected) =>
    rowTokens.some((rowToken) => rowToken === selected || rowToken.includes(selected) || selected.includes(rowToken))
  )
}

function normalizeMachineName(rawName: string) {
  return rawName
    .replace(/\s*\((?:V|A|KW)\)\s*$/i, '')
    .replace(/^3PH\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim()
}

function formatNumber(value: number | null, digits: number = 1) {
  if (value == null || !Number.isFinite(value)) return MISSING
  return value.toLocaleString('it-IT', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function formatTriplet(values: Array<number | null>, digits: number = 0) {
  if (!values.some((value) => value != null && Number.isFinite(value))) return MISSING
  return values.map((value) => formatNumber(value, digits)).join(' / ')
}

function resolveMachineMeta(roomLabel: string, machineName: string): MachineMetaEntry | null {
  const metadata = ROOM_MACHINE_METADATA[normalizeRoomMachineKey(roomLabel)] || []
  const machineKey = canonicalMachineToken(machineName)
  return (
    metadata.find((entry) =>
      entry.aliases.some((alias) => {
        const aliasKey = canonicalMachineToken(alias)
        return machineKey === aliasKey || machineKey.includes(aliasKey) || aliasKey.includes(machineKey)
      })
    ) || null
  )
}

function parseRuntimeMachines(signals?: Record<string, PlantSignalInfo>) {
  if (!signals) return new Map<string, MachineRuntimeRow>()
  const byMachine = new Map<string, MachineRuntimeRow>()

  const getOrCreate = (name: string) => {
    const normalizedName = normalizeMachineName(name)
    const key = canonicalMachineToken(normalizedName)
    const existing = byMachine.get(key)
    if (existing) return existing
    const created: MachineRuntimeRow = {
      key,
      rawName: normalizedName,
      u1: null,
      u2: null,
      u3: null,
      i1: null,
      i2: null,
      i3: null,
      cosphi: null,
      activePowerKw: null,
    }
    byMachine.set(key, created)
    return created
  }

  for (const [signalName, info] of Object.entries(signals)) {
    const signal = signalName.trim()

    const phaseVoltage = signal.match(/^U([123])\s+(.+)$/i)
    if (phaseVoltage) {
      const phase = phaseVoltage[1]
      const machine = getOrCreate(phaseVoltage[2])
      if (phase === '1') machine.u1 = Number(info.value)
      if (phase === '2') machine.u2 = Number(info.value)
      if (phase === '3') machine.u3 = Number(info.value)
      continue
    }

    const phaseCurrent = signal.match(/^[IL]\s*([123])\s+(.+)$/i)
    if (phaseCurrent) {
      const phase = phaseCurrent[1]
      const machine = getOrCreate(phaseCurrent[2])
      if (phase === '1') machine.i1 = Number(info.value)
      if (phase === '2') machine.i2 = Number(info.value)
      if (phase === '3') machine.i3 = Number(info.value)
      continue
    }

    const cosphi = signal.match(/^cosphi\s+(.+)$/i)
    if (cosphi) {
      const machine = getOrCreate(cosphi[1])
      machine.cosphi = Number(info.value)
      continue
    }

    const activePower = signal.match(/^Potenza Attiva\s+(.+)$/i)
    if (activePower) {
      const machineName = normalizeMachineName(activePower[1])
      if (/\bTOT\b|\bTOTAL\b/i.test(machineName)) continue
      const machine = getOrCreate(machineName)
      machine.activePowerKw = Number(info.value)
    }
  }

  return byMachine
}

function toDisplayRows(roomLabel: string, runtimeMap: Map<string, MachineRuntimeRow>) {
  const metadata = ROOM_MACHINE_METADATA[normalizeRoomMachineKey(roomLabel)] || []
  const usedMeta = new Set<MachineMetaEntry>()
  const rows: CompressorDisplayRow[] = []

  runtimeMap.forEach((runtime) => {
    const meta = resolveMachineMeta(roomLabel, runtime.rawName)
    if (meta) usedMeta.add(meta)

    let nominalFlowNm3h = meta?.nominalFlowNm3h ?? MISSING
    let nominalPowerKw = meta?.nominalPowerKw ?? MISSING
    const model = meta?.model ?? MISSING

    if ((nominalFlowNm3h === MISSING || nominalPowerKw === MISSING) && /GA\s*37/i.test(`${runtime.rawName} ${model}`)) {
      if (nominalFlowNm3h === MISSING) nominalFlowNm3h = '400'
      if (nominalPowerKw === MISSING) nominalPowerKw = '37'
    }

    const currents = [runtime.i1, runtime.i2, runtime.i3].filter(
      (value): value is number => value != null && Number.isFinite(value)
    )
    const currentAvg = currents.length > 0 ? currents.reduce((sum, value) => sum + value, 0) / currents.length : null
    const hasVoltage = [runtime.u1, runtime.u2, runtime.u3].some((value) => value != null)
    const power = runtime.activePowerKw

    const isOn = (power ?? 0) > 0.5 || (currentAvg ?? 0) > 1
    const hasAlarm = isOn && ((currents.length > 0 && currentAvg === 0) || (power != null && power === 0))
    const isStandby = !isOn && hasVoltage
    const status: CompressorStatus = hasAlarm ? 'alarm' : isOn ? 'on' : isStandby ? 'standby' : 'off'

    rows.push({
      key: runtime.key,
      name: meta?.name || runtime.rawName,
      brand: meta?.brand || MISSING,
      model,
      type: meta?.type || MISSING,
      nominalFlowNm3h,
      nominalPowerKw,
      usage: meta?.usage || MISSING,
      voltageText: formatTriplet([runtime.u1, runtime.u2, runtime.u3], 0),
      currentText: formatTriplet([runtime.i1, runtime.i2, runtime.i3], 1),
      cosphiText: formatNumber(runtime.cosphi, 2),
      activePowerText: formatNumber(runtime.activePowerKw, 1),
      status,
      hasAlarm,
    })
  })

  metadata.forEach((meta, index) => {
    if (usedMeta.has(meta)) return
    let nominalFlowNm3h = meta.nominalFlowNm3h ?? MISSING
    let nominalPowerKw = meta.nominalPowerKw ?? MISSING

    if ((nominalFlowNm3h === MISSING || nominalPowerKw === MISSING) && /GA\s*37/i.test(`${meta.name} ${meta.model || ''}`)) {
      if (nominalFlowNm3h === MISSING) nominalFlowNm3h = '400'
      if (nominalPowerKw === MISSING) nominalPowerKw = '37'
    }

    rows.push({
      key: `meta-${index}-${meta.name || meta.aliases[0]}`,
      name: meta.name || MISSING,
      brand: meta.brand || MISSING,
      model: meta.model || MISSING,
      type: meta.type || MISSING,
      nominalFlowNm3h,
      nominalPowerKw,
      usage: meta.usage || MISSING,
      voltageText: MISSING,
      currentText: MISSING,
      cosphiText: MISSING,
      activePowerText: MISSING,
      status: 'off',
      hasAlarm: false,
    })
  })

  const metadataOrder = new Map<string, number>()
  metadata.forEach((meta, index) => {
    meta.aliases.forEach((alias) => metadataOrder.set(canonicalMachineToken(alias), index))
  })

  rows.sort((a, b) => {
    const aMetaIndex = metadataOrder.get(canonicalMachineToken(a.name)) ?? 999
    const bMetaIndex = metadataOrder.get(canonicalMachineToken(b.name)) ?? 999
    if (aMetaIndex !== bMetaIndex) return aMetaIndex - bMetaIndex
    return a.name.localeCompare(b.name, 'it', { numeric: true, sensitivity: 'base' })
  })

  return rows
}

function alignHeaderClass(align: CompressorColumn['align']) {
  if (align === 'right') return 'text-right'
  if (align === 'center') return 'text-center'
  return 'text-left'
}

export default function CompressorsTable({ sala, signals, selectedMachine = null }: CompressorsTableProps) {
  const runtimeMap = parseRuntimeMachines(signals)
  const rows = toDisplayRows(sala, runtimeMap)

  // Column arrays drive both header colSpan and body rendering.
  const staticColumns: CompressorColumn[] = [
    { key: 'name', label: 'Nome', group: 'static', align: 'left', render: (row) => row.name },
    { key: 'brand', label: 'Marca', group: 'static', align: 'left', render: (row) => row.brand },
    { key: 'model', label: 'Modello', group: 'static', align: 'left', render: (row) => row.model },
    { key: 'type', label: 'Tipologia', group: 'static', align: 'left', render: (row) => row.type },
    {
      key: 'nominalFlowNm3h',
      label: 'Portata Nominale (Nm3/h)',
      group: 'static',
      align: 'right',
      isNumeric: true,
      render: (row) => row.nominalFlowNm3h,
    },
    {
      key: 'nominalPowerKw',
      label: 'Potenza Nominale (kW)',
      group: 'static',
      align: 'right',
      isNumeric: true,
      render: (row) => row.nominalPowerKw,
    },
  ]

  const realtimeColumns: CompressorColumn[] = [
    {
      key: 'usage',
      label: 'Utilizzo',
      group: 'realtime',
      align: 'left',
      dividerLeft: true,
      render: (row) => row.usage,
    },
    {
      key: 'voltageText',
      label: 'Tensione (V)',
      subLabel: 'U1 / U2 / U3',
      group: 'realtime',
      align: 'right',
      isNumeric: true,
      render: (row) => row.voltageText,
    },
    {
      key: 'currentText',
      label: 'Corrente (A)',
      subLabel: 'I1 / I2 / I3',
      group: 'realtime',
      align: 'right',
      isNumeric: true,
      render: (row) => row.currentText,
    },
    {
      key: 'cosphiText',
      label: 'cosphi',
      group: 'realtime',
      align: 'right',
      isNumeric: true,
      render: (row) => row.cosphiText,
    },
    {
      key: 'activePowerText',
      label: 'Potenza Attiva (kW)',
      group: 'realtime',
      align: 'right',
      isNumeric: true,
      render: (row) => row.activePowerText,
    },
    {
      key: 'status',
      label: 'Stato',
      group: 'realtime',
      align: 'center',
      render: (row) => renderStatusCell(row.status),
    },
  ]

  const alarmsColumns: CompressorColumn[] = [
    {
      key: 'alarm',
      label: 'Icona',
      group: 'alarms',
      align: 'center',
      dividerLeft: true,
      render: (row) => renderAlarmCell(row.hasAlarm),
    },
  ]

  const columns = [...staticColumns, ...realtimeColumns, ...alarmsColumns]

  if (rows.length === 0) {
    return (
      <div className="mt-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs text-slate-500">
        Nessun dato compressori disponibile per questa sala.
      </div>
    )
  }

  return (
    <div className="mt-3 overflow-x-auto rounded-md border border-slate-200 bg-white">
      <table className="w-full min-w-[1200px] border-collapse">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-100 text-[11px] font-semibold uppercase tracking-wide text-slate-700">
            <th colSpan={staticColumns.length} className="px-2 py-2 text-left">
              COMPRESSORE
            </th>
            <th colSpan={realtimeColumns.length} className="border-l-2 border-slate-300 px-2 py-2 text-left">
              <div className="flex items-center gap-2">
                <span>REAL TIME</span>
                <span className="inline-flex items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-semibold text-emerald-700">
                  <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-500" />
                  LIVE
                </span>
                <span className="text-[10px] font-medium normal-case text-slate-500">agg. ogni 5s</span>
              </div>
            </th>
            <th colSpan={alarmsColumns.length} className="border-l-2 border-slate-300 px-2 py-2 text-center">
              ALLARMI
            </th>
          </tr>

          <tr className="border-b border-slate-200 text-[11px] font-semibold text-slate-600">
            {columns.map((column) => (
              <th
                key={column.key}
                className={[
                  'px-2 py-2',
                  alignHeaderClass(column.align),
                  column.group === 'realtime' ? 'bg-slate-100' : 'bg-slate-50',
                  column.dividerLeft ? 'border-l-2 border-slate-300' : '',
                ].join(' ')}
              >
                <div>{column.label}</div>
                {column.subLabel ? <div className="text-[10px] font-medium text-slate-500">{column.subLabel}</div> : null}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {rows.map((row) => (
            <CompressorsRow
              key={row.key}
              row={row}
              columns={columns}
              isSelected={isRowSelectedByMachine(row, selectedMachine)}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}
