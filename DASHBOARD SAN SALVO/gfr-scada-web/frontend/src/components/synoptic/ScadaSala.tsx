import { useEffect, useId, useMemo, useState } from 'react'
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

  const topY = 96
  const loopBottomY = 198
  const manifoldY = 250
  const tankY = 300
  const machineConnectY = 560
  const dryerY = 26
  const dryerW = 160
  const dryerH = 170
  const topPipeStartX = 140
  const topPipeEndX = 1110
  const upperLeftRiserX = 150
  const innerLoopLeftX = 198
  const innerLoopRightX = 900
  const topPipeCenterX = (topPipeStartX + topPipeEndX) / 2
  const centeredDryerX = topPipeCenterX - dryerW / 2
  const loopDropX = 600
  const machineActive = machines.slice(0, 3).map((m) => m.status === 'ACTIVE' && m.kw > 0.1)
  const activeBranchXs = branchXs.filter((_, idx) => machineActive[idx])
  const hasAnyActive = activeBranchXs.length > 0
  const manifoldActiveStart = hasAnyActive ? Math.min(upperLeftRiserX, loopDropX, ...activeBranchXs) : branchXs[0]
  const manifoldActiveEnd = hasAnyActive ? Math.max(loopDropX, ...activeBranchXs) : branchXs[2]

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
            <PipePath points={[[topPipeStartX, topY], [topPipeEndX, topY]]} active={false} thickness={10} />
            <PipePath points={[[upperLeftRiserX, topY], [upperLeftRiserX, manifoldY]]} active={false} thickness={10} />
            <PipePath points={[[upperLeftRiserX, loopBottomY], [innerLoopRightX, loopBottomY]]} active={false} thickness={10} />
            <PipePath points={[[innerLoopLeftX, topY], [innerLoopLeftX, loopBottomY]]} active={false} thickness={10} />
            <PipePath points={[[innerLoopRightX, topY], [innerLoopRightX, loopBottomY]]} active={false} thickness={10} />
            <PipePath points={[[loopDropX, loopBottomY], [loopDropX, manifoldY]]} active={false} thickness={10} />
            <PipePath points={[[branchXs[0], manifoldY], [branchXs[2], manifoldY]]} active={false} thickness={10} />

            {hasAnyActive ? (
              <>
                <PipePath points={[[topPipeStartX, topY], [topPipeEndX, topY]]} active thickness={10} />
                <PipePath points={[[upperLeftRiserX, topY], [upperLeftRiserX, manifoldY]]} active thickness={10} />
                <PipePath points={[[upperLeftRiserX, loopBottomY], [innerLoopRightX, loopBottomY]]} active thickness={10} />
                <PipePath points={[[innerLoopLeftX, topY], [innerLoopLeftX, loopBottomY]]} active thickness={10} />
                <PipePath points={[[innerLoopRightX, topY], [innerLoopRightX, loopBottomY]]} active thickness={10} />
                <PipePath points={[[loopDropX, loopBottomY], [loopDropX, manifoldY]]} active thickness={10} />
                <PipePath points={[[manifoldActiveStart, manifoldY], [manifoldActiveEnd, manifoldY]]} active thickness={10} />
              </>
            ) : null}

            <g transform={`translate(${centeredDryerX},${dryerY})`}>
              {dryerImageUrl ? (
                <image href={dryerImageUrl} x={0} y={0} width={dryerW} height={dryerH} preserveAspectRatio="xMidYMid meet" />
              ) : (
                <rect x={0} y={0} width={dryerW} height={dryerH} rx={14} className="placeholder" />
              )}
            </g>

            {machines.slice(0, 3).map((m, idx) => {
              const x = branchXs[idx] ?? 150 + idx * 320
              const active = machineActive[idx]
              const rightRiserX = x
              const leftRiserX = x - 74
              const tankInY = tankY + 24
              const tankOutY = tankY + 116
              return (
                <g key={m.id}>
                  <PipePath
                    points={[
                      [rightRiserX, manifoldY],
                      [rightRiserX, tankInY],
                      [x + 30, tankInY],
                    ]}
                    active={false}
                    thickness={10}
                  />
                  <PipePath
                    points={[
                      [x - 30, tankOutY],
                      [leftRiserX, tankOutY],
                      [leftRiserX, machineConnectY],
                      [x, machineConnectY],
                    ]}
                    active={false}
                    thickness={10}
                  />
                  {active ? (
                    <>
                      <PipePath
                        points={[
                          [rightRiserX, manifoldY],
                          [rightRiserX, tankInY],
                          [x + 30, tankInY],
                        ]}
                        active
                        thickness={10}
                      />
                      <PipePath
                        points={[
                          [x - 30, tankOutY],
                          [leftRiserX, tankOutY],
                          [leftRiserX, machineConnectY],
                          [x, machineConnectY],
                        ]}
                        active
                        thickness={10}
                      />
                    </>
                  ) : null}
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

function PipePath({
  points,
  active,
  thickness,
}: {
  points: Array<[number, number]>
  active: boolean
  thickness: number
}) {
  const pathId = `pipe-${useId().replace(/:/g, '')}`
  const d = buildCubicOrthogonalPath(points, 10)
  if (!d) return null
  const baseClass = active ? 'pipe-core pipe-core-active' : 'pipe-core pipe-core-inactive'
  const pathLength = getPolylineLength(points)
  const arrowCount = active ? Math.max(2, Math.floor(pathLength / 90)) : 0
  return (
    <g>
      <path id={pathId} d={d} fill="none" />
      <path d={d} className="pipe-shell" strokeWidth={thickness + 8} strokeLinecap="round" strokeLinejoin="round" fill="none" />
      <path d={d} className={baseClass} strokeWidth={thickness} strokeLinecap="round" strokeLinejoin="round" fill="none" />
      {arrowCount > 0 ? (
        <g className="pipe-flow-arrows">
          {Array.from({ length: arrowCount }).map((_, index) => (
            <polygon key={index} points="0,0 -9,-5 -9,5">
              <animateMotion
                dur="1.6s"
                repeatCount="indefinite"
                rotate="auto"
                begin={`${(-1.6 / arrowCount) * index}s`}
              >
                <mpath href={`#${pathId}`} />
              </animateMotion>
            </polygon>
          ))}
        </g>
      ) : null}
    </g>
  )
}

function buildCubicOrthogonalPath(points: Array<[number, number]>, radius: number) {
  if (!points.length) return ''
  if (points.length === 1) return `M${points[0][0]} ${points[0][1]}`
  if (points.length === 2) return `M${points[0][0]} ${points[0][1]} L${points[1][0]} ${points[1][1]}`

  // Bezier handle factor for quarter-circle approximation.
  const K = 0.5522847498
  const fmt = (value: number) => Number(value.toFixed(2))
  let d = `M${fmt(points[0][0])} ${fmt(points[0][1])}`

  for (let i = 1; i < points.length - 1; i += 1) {
    const [x0, y0] = points[i - 1]
    const [x1, y1] = points[i]
    const [x2, y2] = points[i + 1]

    const inDx = x1 - x0
    const inDy = y1 - y0
    const outDx = x2 - x1
    const outDy = y2 - y1

    const inLen = Math.hypot(inDx, inDy)
    const outLen = Math.hypot(outDx, outDy)
    if (inLen === 0 || outLen === 0) continue

    const cornerRadius = Math.min(radius, inLen / 2, outLen / 2)
    const inUx = inDx / inLen
    const inUy = inDy / inLen
    const outUx = outDx / outLen
    const outUy = outDy / outLen

    const sx = x1 - inUx * cornerRadius
    const sy = y1 - inUy * cornerRadius
    const ex = x1 + outUx * cornerRadius
    const ey = y1 + outUy * cornerRadius

    const handle = cornerRadius * K
    const c1x = sx + inUx * handle
    const c1y = sy + inUy * handle
    const c2x = ex - outUx * handle
    const c2y = ey - outUy * handle

    d += ` L${fmt(sx)} ${fmt(sy)} C${fmt(c1x)} ${fmt(c1y)} ${fmt(c2x)} ${fmt(c2y)} ${fmt(ex)} ${fmt(ey)}`
  }

  const last = points[points.length - 1]
  d += ` L${fmt(last[0])} ${fmt(last[1])}`
  return d
}

function getPolylineLength(points: Array<[number, number]>) {
  let total = 0
  for (let i = 1; i < points.length; i += 1) {
    const [x0, y0] = points[i - 1]
    const [x1, y1] = points[i]
    total += Math.hypot(x1 - x0, y1 - y0)
  }
  return total
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
