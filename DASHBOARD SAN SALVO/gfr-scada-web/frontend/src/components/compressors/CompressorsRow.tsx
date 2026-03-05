import type { ReactNode } from 'react'
import StatusBadge, { type CompressorStatus } from './StatusBadge'

export interface CompressorDisplayRow {
  key: string
  name: string
  brand: string
  model: string
  type: string
  nominalFlowNm3h: string
  nominalPowerKw: string
  usage: string
  voltageText: string
  currentText: string
  cosphiText: string
  activePowerText: string
  status: CompressorStatus
  hasAlarm: boolean
}

export type CompressorColumnGroup = 'static' | 'realtime' | 'alarms'

export interface CompressorColumn {
  key: string
  label: string
  subLabel?: string
  group: CompressorColumnGroup
  align: 'left' | 'right' | 'center'
  isNumeric?: boolean
  dividerLeft?: boolean
  render: (row: CompressorDisplayRow) => ReactNode
}

interface CompressorsRowProps {
  row: CompressorDisplayRow
  columns: CompressorColumn[]
  isSelected?: boolean
}

function rowBorderClass(status: CompressorStatus) {
  if (status === 'alarm') return 'border-l-rose-500'
  if (status === 'on') return 'border-l-emerald-500'
  if (status === 'standby') return 'border-l-amber-500'
  return 'border-l-slate-400'
}

function alignClass(align: CompressorColumn['align']) {
  if (align === 'right') return 'text-right'
  if (align === 'center') return 'text-center'
  return 'text-left'
}

function groupCellClass(group: CompressorColumnGroup, isSelected: boolean) {
  if (isSelected) {
    if (group === 'realtime') return 'bg-teal-50 group-hover:bg-teal-100/90'
    return 'bg-teal-50/70 group-hover:bg-teal-100/80'
  }
  if (group === 'realtime') return 'bg-slate-50 group-hover:bg-slate-100/90'
  return 'bg-white group-hover:bg-slate-50/80'
}

export default function CompressorsRow({ row, columns, isSelected = false }: CompressorsRowProps) {
  return (
    <tr
      className={[
        'group border-b border-slate-200 border-l-4 transition-colors',
        rowBorderClass(row.status),
        isSelected ? 'relative z-[1] shadow-[inset_0_0_0_2px_rgba(13,148,136,0.45)]' : '',
      ].join(' ')}
    >
      {columns.map((column) => (
        <td
          key={`${row.key}-${column.key}`}
          className={[
            'px-2 py-3',
            alignClass(column.align),
            groupCellClass(column.group, isSelected),
            column.dividerLeft ? 'border-l-2 border-slate-300' : '',
            column.isNumeric ? 'tabular-nums' : '',
            column.group === 'realtime' && column.isNumeric ? 'text-[13.5px] font-semibold text-slate-900' : 'text-slate-700',
          ].join(' ')}
        >
          {column.render(row)}
        </td>
      ))}
    </tr>
  )
}

export function renderStatusCell(status: CompressorStatus) {
  return <StatusBadge status={status} />
}

export function renderAlarmCell(hasAlarm: boolean) {
  return hasAlarm ? (
    <span title="Active con corrente o potenza attiva a zero.">{'\u26A0'}</span>
  ) : (
    <span className="text-slate-300">{'\u2014'}</span>
  )
}
