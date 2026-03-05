import { useEffect, useMemo, useState } from 'react'
import './scada.css'

export type ScadaMachineStatus = 'ACTIVE' | 'STANDBY' | 'ALARM' | 'OFFLINE'

export type ScadaMachine = {
  id: string
  name: string
  kw: number
  status: ScadaMachineStatus
  imageUrl?: string
  u1?: number | null
  u2?: number | null
  u3?: number | null
  i1?: number | null
  i2?: number | null
  i3?: number | null
  cosphi?: number | null
}

export type ScadaInstruments = {
  totalKw: number
  cs: number
  dewPoint: number
  pressure: number
  flow: number
  temp: number
  totalizer?: number
}

interface ScadaSalaProps {
  title: string
  lastUpdate: string
  dryerImageUrl?: string
  machines: ScadaMachine[]
  instruments: ScadaInstruments
  onStart?: (machineId: string) => Promise<void> | void
  onStop?: (machineId: string) => Promise<void> | void
  onClose?: () => void
}

function statusBadgeClass(status: ScadaMachineStatus) {
  if (status === 'ACTIVE') return 'badge badge-active'
  if (status === 'STANDBY') return 'badge badge-standby'
  if (status === 'ALARM') return 'badge badge-alarm'
  return 'badge badge-offline'
}

function isFlowActive(machines: ScadaMachine[]) {
  return machines.some((m) => m.status === 'ACTIVE' && m.kw > 0.1)
}

function fmt(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return '--'
  return Number(value).toFixed(digits)
}

export default function ScadaSala({
  title,
  lastUpdate,
  dryerImageUrl,
  machines,
  instruments,
  onStart,
  onStop,
  onClose,
}: ScadaSalaProps) {
  const [selectedId, setSelectedId] = useState<string | null>(machines[0]?.id ?? null)
  const flowActive = useMemo(() => isFlowActive(machines), [machines])

  useEffect(() => {
    if (!machines.length) {
      setSelectedId(null)
      return
    }
    if (selectedId && machines.some((m) => m.id === selectedId)) return
    setSelectedId(machines[0].id)
  }, [machines, selectedId])

  const cardLefts = [12, 390, 768]
  const branchXs = [cardLefts[0] + 110, cardLefts[1] + 110, cardLefts[2] + 110]

  const topY = 110
  const loopBottomY = 200
  const manifoldY = 250
  const tankY = 300
  const machineConnectY = 560
  const dryerY = 30
  const dryerW = 160
  const dryerH = 170
  const topPipeStartX = 200
  const topPipeEndX = 1100
  const topPipeCenterX = (topPipeStartX + topPipeEndX) / 2
  const centeredDryerX = topPipeCenterX - dryerW / 2
  const loopRightX = centeredDryerX + dryerW - 5
  const loopDropX = topPipeCenterX

  return (
    <div className="scada-wrap">
      <div className="scada-header">
        <div>
          <div className="scada-title">{title}</div>
          <div className="scada-subtitle">Last update: {lastUpdate || '--'}</div>
        </div>
        <button className="btn btn-ghost" onClick={onClose}>Chiudi</button>
      </div>

      <div className="scada-body">
        <div className="scada-canvas">
          <svg className="scada-svg" viewBox="0 0 1200 760" role="img" aria-label="SCADA aria compressa">
            <Pipe x1={topPipeStartX} y1={topY} x2={loopRightX} y2={topY} active={flowActive} thickness={10} />
            <Pipe x1={loopRightX} y1={topY} x2={topPipeEndX} y2={topY} active={flowActive} thickness={10} />
            <Pipe x1={topPipeStartX} y1={topY} x2={topPipeStartX} y2={loopBottomY} active={flowActive} thickness={10} />
            <Pipe x1={topPipeStartX} y1={loopBottomY} x2={loopRightX} y2={loopBottomY} active={flowActive} thickness={10} />
            <Pipe x1={loopRightX} y1={topY} x2={loopRightX} y2={loopBottomY} active={flowActive} thickness={10} />
            <Pipe x1={loopDropX} y1={loopBottomY} x2={loopDropX} y2={manifoldY} active={flowActive} thickness={10} />
            <Pipe x1={branchXs[0]} y1={manifoldY} x2={branchXs[2]} y2={manifoldY} active={flowActive} thickness={10} />

            <g transform={`translate(${centeredDryerX},${dryerY})`}>
              {dryerImageUrl ? (
                <image href={dryerImageUrl} x={0} y={0} width={dryerW} height={dryerH} preserveAspectRatio="xMidYMid meet" />
              ) : (
                <rect x={0} y={0} width={dryerW} height={dryerH} rx={14} className="placeholder" />
              )}
            </g>

            {machines.slice(0, 3).map((m, idx) => {
              const x = branchXs[idx] ?? 150 + idx * 320
              const active = m.status === 'ACTIVE' && m.kw > 0.1
              const rightRiserX = idx === 0 ? x : x + 74
              const leftRiserX = x - 74
              const tankInY = tankY + 24
              const tankOutY = tankY + 116
              return (
                <g key={m.id}>
                  <Pipe x1={x} y1={manifoldY} x2={rightRiserX} y2={manifoldY} active={active} thickness={10} />
                  <Pipe x1={rightRiserX} y1={manifoldY} x2={rightRiserX} y2={tankInY} active={active} thickness={10} />
                  <Pipe x1={rightRiserX} y1={tankInY} x2={x + 30} y2={tankInY} active={active} thickness={10} />

                  <Pipe x1={x - 30} y1={tankOutY} x2={leftRiserX} y2={tankOutY} active={active} thickness={10} />
                  <Pipe x1={leftRiserX} y1={tankOutY} x2={leftRiserX} y2={machineConnectY} active={active} thickness={10} />
                  <Pipe x1={leftRiserX} y1={machineConnectY} x2={x} y2={machineConnectY} active={active} thickness={10} />
                  <Tank x={x - 38} y={tankY} />
                </g>
              )
            })}
          </svg>

          <div className="line-tools">
            <div className="line-tools-title">Strumenti di linea</div>
            <div className="line-tools-card">
              <div className="line-tools-head">
                <span>Totale sala</span>
                <strong>{instruments.totalKw.toFixed(1)} kW</strong>
              </div>
              <div className="line-tools-row">Consumo specifico {instruments.cs.toFixed(3)} kWh/Nm3</div>
            </div>
            <div className="line-tools-card">
              <div className="line-tools-head"><span>Dew Point</span></div>
              <div className="line-tools-big">{instruments.dewPoint.toFixed(1)} degC</div>
            </div>
            <div className="line-tools-card">
              <div className="line-tools-head"><span>Flowmeter</span></div>
              <div className="line-tools-kv">
                <div><span>Pressione</span><span>{instruments.pressure.toFixed(1)} bar</span></div>
                <div><span>Flusso</span><span>{instruments.flow.toFixed(1)} Nm3/h</span></div>
                <div><span>Temperatura</span><span>{instruments.temp.toFixed(1)} degC</span></div>
                {typeof instruments.totalizer === 'number' ? (
                  <div><span>Totalizzatore</span><span>{instruments.totalizer.toFixed(1)} m3</span></div>
                ) : null}
              </div>
            </div>
          </div>

          <div className="machines-layer">
            {machines.slice(0, 3).map((m, idx) => (
              <div key={m.id} className={`machine-slot machine-slot-${idx + 1}`} style={{ left: `${cardLefts[idx]}px` }}>
                <div className="machine-skid">
                  <div className="machine-img machine-img-below machine-img-large">
                    {m.imageUrl ? <img src={m.imageUrl} alt={m.name} /> : <div className="img-placeholder">FOTO {m.id}</div>}
                  </div>

                  <button
                    className={`machine-table ${selectedId === m.id ? 'machine-table-selected' : ''}`}
                    onClick={() => setSelectedId(m.id)}
                  >
                    <div className="machine-table-head">{m.name}</div>
                    <div className="machine-table-row"><span>U 1-2</span><strong>{fmt(m.u1, 1)} V</strong></div>
                    <div className="machine-table-row"><span>U 2-3</span><strong>{fmt(m.u2, 1)} V</strong></div>
                    <div className="machine-table-row"><span>U 3-1</span><strong>{fmt(m.u3, 1)} V</strong></div>
                    <div className="machine-table-row"><span>I 1</span><strong>{fmt(m.i1, 1)} A</strong></div>
                    <div className="machine-table-row"><span>I 2</span><strong>{fmt(m.i2, 1)} A</strong></div>
                    <div className="machine-table-row"><span>I 3</span><strong>{fmt(m.i3, 1)} A</strong></div>
                    <div className="machine-table-row"><span>cosphi</span><strong>{fmt(m.cosphi, 2)}</strong></div>
                    <div className="machine-table-row"><span>P</span><strong>{fmt(m.kw, 1)} kW</strong></div>

                    <div className="machine-table-foot">
                      <span className={statusBadgeClass(m.status)}>{m.status.toLowerCase()}</span>
                      <button
                        className="btn btn-ok"
                        onClick={(event) => {
                          event.stopPropagation()
                          onStart?.(m.id)
                        }}
                        disabled={!onStart}
                      >
                        ACCENDI
                      </button>
                      <button
                        className="btn btn-bad"
                        onClick={(event) => {
                          event.stopPropagation()
                          onStop?.(m.id)
                        }}
                        disabled={!onStop}
                      >
                        SPEGNI
                      </button>
                    </div>
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

function Pipe({
  x1,
  y1,
  x2,
  y2,
  active,
  thickness,
}: {
  x1: number
  y1: number
  x2: number
  y2: number
  active: boolean
  thickness: number
}) {
  const baseClass = active ? 'pipe-core pipe-core-active' : 'pipe-core pipe-core-inactive'
  const flowClass = active ? 'pipe-flow' : 'pipe-flow-hidden'
  const isHorizontal = Math.abs(y1 - y2) < 0.5
  const isVertical = Math.abs(x1 - x2) < 0.5
  const length = isHorizontal ? Math.abs(x2 - x1) : isVertical ? Math.abs(y2 - y1) : 0
  const margin = 42
  const spacing = 270

  const valves =
    length > 160
      ? Array.from({ length: Math.max(0, Math.floor((length - margin * 2) / spacing) + 1) }, (_, i) => margin + i * spacing)
      : []

  return (
    <g>
      <line x1={x1} y1={y1} x2={x2} y2={y2} className="pipe-shell" strokeWidth={thickness + 8} strokeLinecap="round" />
      <line x1={x1} y1={y1} x2={x2} y2={y2} className={baseClass} strokeWidth={thickness} strokeLinecap="round" />
      <line x1={x1} y1={y1} x2={x2} y2={y2} className={flowClass} strokeWidth={Math.max(4, thickness - 5)} strokeLinecap="round" />
      {valves.map((offset) => {
        const px = isHorizontal ? (x1 < x2 ? x1 + offset : x1 - offset) : x1
        const py = isVertical ? (y1 < y2 ? y1 + offset : y1 - offset) : y1
        return <ValveMarker key={`${x1}-${y1}-${x2}-${y2}-${offset}`} x={px} y={py} vertical={isVertical} />
      })}
    </g>
  )
}

function ValveMarker({ x, y, vertical }: { x: number; y: number; vertical: boolean }) {
  if (vertical) {
    return (
      <g transform={`translate(${x}, ${y})`} className="pipe-valve">
        <rect x={-12} y={-5} width={24} height={10} rx={2.5} className="pipe-valve-body" />
        <rect x={-8} y={-7} width={16} height={14} rx={2.5} className="pipe-valve-band" />
        <circle cx={-15} cy={0} r={2.1} className="pipe-valve-handle" />
        <circle cx={15} cy={0} r={2.1} className="pipe-valve-handle" />
      </g>
    )
  }

  return (
    <g transform={`translate(${x}, ${y})`} className="pipe-valve">
      <rect x={-5} y={-12} width={10} height={24} rx={2.5} className="pipe-valve-body" />
      <rect x={-7} y={-8} width={14} height={16} rx={2.5} className="pipe-valve-band" />
      <circle cx={0} cy={-15} r={2.1} className="pipe-valve-handle" />
      <circle cx={0} cy={15} r={2.1} className="pipe-valve-handle" />
    </g>
  )
}

function Tank({ x, y }: { x: number; y: number }) {
  return (
    <g transform={`translate(${x}, ${y})`}>
      <rect x={0} y={0} width={76} height={150} rx={34} className="tank-body" />
      <rect x={18} y={26} width={40} height={44} rx={6} className="tank-window" />
      <circle cx={38} cy={112} r={6} className="tank-valve" />
      <line x1={18} y1={150} x2={18} y2={168} className="tank-leg" />
      <line x1={58} y1={150} x2={58} y2={168} className="tank-leg" />
    </g>
  )
}
