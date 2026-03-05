import { useEffect, useState } from 'react'

export type SynopticMachineStatus = 'active' | 'standby' | 'alarm' | 'offline'

interface MachineNodeProps {
  id: string
  label: string
  x: number
  y: number
  status: SynopticMachineStatus
  powerKw: number | null
  imageHref?: string
  selected: boolean
  onClick: () => void
}

const STATUS_STYLE: Record<SynopticMachineStatus, { stroke: string; fill: string; dot: string; text: string }> = {
  active: {
    stroke: '#22c55e',
    fill: '#ecfdf5',
    dot: '#22c55e',
    text: 'ACTIVE',
  },
  standby: {
    stroke: '#d97706',
    fill: '#fffbeb',
    dot: '#f59e0b',
    text: 'STANDBY',
  },
  alarm: {
    stroke: '#dc2626',
    fill: '#fef2f2',
    dot: '#ef4444',
    text: 'ALARM',
  },
  offline: {
    stroke: '#64748b',
    fill: '#f8fafc',
    dot: '#94a3b8',
    text: 'OFFLINE',
  },
}

function formatKw(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '--'
  return value.toFixed(1)
}

export default function MachineNode({ id, label, x, y, status, powerKw, imageHref, selected, onClick }: MachineNodeProps) {
  const style = STATUS_STYLE[status]
  const width = 190
  const height = 98
  const [imageVisible, setImageVisible] = useState(Boolean(imageHref))

  useEffect(() => {
    setImageVisible(Boolean(imageHref))
  }, [imageHref])

  return (
    <g
      transform={`translate(${x} ${y})`}
      onClick={onClick}
      style={{ cursor: 'pointer' }}
      role="button"
      tabIndex={0}
      aria-label={`${label} ${style.text}`}
    >
      <title>{`${label} | ${style.text} | kW ${formatKw(powerKw)} | ID ${id}`}</title>

      <rect
        x={0}
        y={0}
        rx={10}
        ry={10}
        width={width}
        height={height}
        fill={style.fill}
        stroke={selected ? '#0f172a' : style.stroke}
        strokeWidth={selected ? 2.8 : 1.8}
      />

      <text x={12} y={24} fill="#0f172a" fontSize={13} fontWeight={700}>
        {label}
      </text>

      {imageVisible ? (
        <foreignObject x={10} y={30} width={170} height={42}>
          <img
            src={imageHref}
            alt={label}
            style={{ width: '100%', height: '100%', objectFit: 'contain' }}
            onError={() => setImageVisible(false)}
          />
        </foreignObject>
      ) : (
        <>
          <text x={12} y={52} fill="#0f172a" fontSize={21} fontWeight={700}>
            {formatKw(powerKw)}
          </text>
          <text x={68} y={52} fill="#475569" fontSize={11} fontWeight={600}>
            kW
          </text>
        </>
      )}

      <text x={12} y={72} fill="#0f172a" fontSize={13} fontWeight={700}>
        {`${formatKw(powerKw)} kW`}
      </text>

      <circle cx={16} cy={76} r={4.5} fill={style.dot} />
      <text x={26} y={80} fill="#334155" fontSize={10.5} fontWeight={700}>
        {style.text}
      </text>
    </g>
  )
}
