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

function tableStatusClass(status: ScadaMachineStatus) {
  if (status === 'ACTIVE') return 'machine-table-active'
  if (status === 'STANDBY') return 'machine-table-standby'
  if (status === 'ALARM') return 'machine-table-alarm'
  return 'machine-table-offline'
}

function statusDotClass(status: ScadaMachineStatus) {
  if (status === 'ACTIVE') return 'machine-state-dot-active'
  if (status === 'STANDBY') return 'machine-state-dot-standby'
  if (status === 'ALARM') return 'machine-state-dot-alarm'
  return 'machine-state-dot-offline'
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

  const cardLefts = [12, 428, 830]
  const branchXs = [cardLefts[0] + 110, cardLefts[1] + 110, cardLefts[2] + 110]

  const yShift = -24
  const topY = 96 + yShift
  const loopBottomY = 198 + yShift
  const manifoldY = 250 + yShift
  const tankY = 300 + yShift
  const machineConnectY = 668 + yShift
  const dryerY = 14 + yShift
  const dryerW = 184
  const dryerH = 194
  const topPipePathEndX = 1182
  const upperLeftRiserX = 150
  const innerLoopLeftX = 198
  const innerLoopRightX = 900
  const loopBottomEndX = innerLoopRightX - 12
  const centeredDryerX = branchXs[1] - dryerW / 2
  const loopDropX = 600
  const upperLeftRiserBottomY = manifoldY - 12
  const topWithUpperLeftRiserPoints: Array<[number, number]> = [
    [upperLeftRiserX, upperLeftRiserBottomY],
    [upperLeftRiserX, topY],
    [topPipePathEndX, topY],
  ]
  const dryerLoopPoints: Array<[number, number]> = [
    [innerLoopRightX, topY],
    [innerLoopRightX, loopBottomY],
    [loopBottomEndX, loopBottomY],
    [innerLoopLeftX, loopBottomY],
    [innerLoopLeftX, topY],
  ]
  const rightBoilerLinkY = tankY - 58
  const centerToLeftConnectorY = rightBoilerLinkY
  const leftBoilerCenterX = branchXs[0] - 24
  const leftBoilerToHorizontalPoints: Array<[number, number]> = [
    [leftBoilerCenterX, tankY + 4],
    [leftBoilerCenterX, centerToLeftConnectorY],
    [upperLeftRiserX, centerToLeftConnectorY],
  ]
  const rightToCenterBoilerPoints: Array<[number, number]> = [
    [branchXs[2], tankY + 4],
    [branchXs[2], rightBoilerLinkY],
    [branchXs[1], rightBoilerLinkY],
  ]
  const centerBoilerUpPoints: Array<[number, number]> = [
    [branchXs[1], tankY + 4],
    [branchXs[1], centerToLeftConnectorY],
  ]
  const centerToLeftConnectorPoints: Array<[number, number]> = [
    [branchXs[1], centerToLeftConnectorY],
    [upperLeftRiserX, centerToLeftConnectorY],
  ]
  const valveJunctions = findPipeJunctions([
    topWithUpperLeftRiserPoints,
    dryerLoopPoints,
    leftBoilerToHorizontalPoints,
    rightToCenterBoilerPoints,
    centerBoilerUpPoints,
    centerToLeftConnectorPoints,
  ])
  const valveSize = 152
  const valveOffsetY = -8
  const machineActive = machines
    .slice(0, 3)
    .map((m) => m.status === 'ACTIVE' && m.kw > 0.1)
  const hasAlarm = machines.some((m) => m.status === 'ALARM')
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
            <PipePath points={topWithUpperLeftRiserPoints} active={false} thickness={10} />
            <PipePath points={dryerLoopPoints} active={false} thickness={10} />
            <PipePath points={leftBoilerToHorizontalPoints} active={false} thickness={10} />
            <PipePath points={rightToCenterBoilerPoints} active={false} thickness={10} />
            <PipePath points={centerBoilerUpPoints} active={false} thickness={10} />
            <PipePath points={centerToLeftConnectorPoints} active={false} thickness={10} />

            {hasAnyActive ? (
              <>
                <PipePath points={topWithUpperLeftRiserPoints} active thickness={10} />
                {machineActive[1] || machineActive[2] ? (
                  <PipePath points={centerToLeftConnectorPoints} active thickness={10} />
                ) : null}
                {machineActive[0] ? (
                  <PipePath points={leftBoilerToHorizontalPoints} active thickness={10} />
                ) : null}
                {machineActive[2] ? (
                  <PipePath points={rightToCenterBoilerPoints} active thickness={10} />
                ) : null}
                {machineActive[1] ? (
                  <PipePath points={centerBoilerUpPoints} active thickness={10} />
                ) : null}
              </>
            ) : null}
            {valveJunctions.map(([x, y], index) => (
              <image
                key={`valve-${index}`}
                href="/images/scada/valvola.png"
                x={x - valveSize / 2}
                y={y - valveSize / 2 + valveOffsetY}
                width={valveSize}
                height={valveSize}
                preserveAspectRatio="xMidYMid meet"
                className="pipe-valve-marker"
              />
            ))}

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
              const leftTankShiftX = idx === 0 ? -24 : 0
              const tankCenterX = x + leftTankShiftX
              const leftRiserX = tankCenterX - 118
              const tankOutY = tankY + 116
              const defaultOutletPoints: Array<[number, number]> = [
                [leftRiserX, machineConnectY],
                [leftRiserX, tankOutY],
                [tankCenterX - 30, tankOutY],
              ]
              return (
                <g key={m.id}>
                  <PipePath
                    points={defaultOutletPoints}
                    active={false}
                    thickness={10}
                  />
                  {active ? (
                    <>
                      <PipePath
                        points={defaultOutletPoints}
                        active
                        thickness={10}
                      />
                    </>
                  ) : null}
                  <Tank x={tankCenterX - 38} y={tankY} />
                </g>
              )
            })}
          </svg>

          <div className="line-tools">
            <div className="line-tools-box">
              <div className="line-tools-top">
                <span className="line-tools-room-title">SITUAZIONE SALA</span>
                <div className={`line-tools-alert ${hasAlarm ? 'line-tools-alert-alarm' : 'line-tools-alert-ok'}`}>
                  <span className="line-tools-alert-dot" aria-hidden="true" />
                  <span>{hasAlarm ? 'ALLARME' : 'OK'}</span>
                </div>
              </div>

              <div className="line-tools-head">
                <span>POTENZA</span>
                <strong className="line-tools-kw">
                  <span>{instruments.totalKw.toFixed(1)} kW</span>
                </strong>
              </div>
              <div className="line-tools-row line-tools-row-value-only">{instruments.cs.toFixed(3)} kWh/Nm3</div>
              <div className="line-tools-head">
                <span>Dew Point</span>
                <strong className="line-tools-inline-value">{instruments.dewPoint.toFixed(1)} degC</strong>
              </div>
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
                  <div className={`machine-img machine-img-below machine-img-large machine-img-slot-${idx + 1}`}>
                    {m.imageUrl ? <img src={m.imageUrl} alt={m.name} /> : <div className="img-placeholder">FOTO {m.id}</div>}
                  </div>

                  <button
                    className={`machine-table ${tableStatusClass(m.status)} ${selectedId === m.id ? 'machine-table-selected' : ''}`}
                    onClick={() => setSelectedId(m.id)}
                  >
                    <div className="machine-table-head">
                      <span className={`machine-state-dot ${statusDotClass(m.status)}`} aria-hidden="true">{'\u23FB'}</span>
                      <span className="machine-table-head-label">{m.name}</span>
                    </div>
                    <div className="machine-table-row"><span>U 1-2</span><strong>{fmt(m.u1, 1)} V</strong></div>
                    <div className="machine-table-row"><span>U 2-3</span><strong>{fmt(m.u2, 1)} V</strong></div>
                    <div className="machine-table-row"><span>U 3-1</span><strong>{fmt(m.u3, 1)} V</strong></div>
                    <div className="machine-table-row"><span>I 1</span><strong>{fmt(m.i1, 1)} A</strong></div>
                    <div className="machine-table-row"><span>I 2</span><strong>{fmt(m.i2, 1)} A</strong></div>
                    <div className="machine-table-row"><span>I 3</span><strong>{fmt(m.i3, 1)} A</strong></div>
                    <div className="machine-table-row"><span>cosphi</span><strong>{fmt(m.cosphi, 2)}</strong></div>
                    <div className="machine-table-row"><span>P</span><strong>{fmt(m.kw, 1)} kW</strong></div>

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
  const flowSpeedPxPerSecond = 70
  const flowDuration = Math.max(2.4, pathLength / flowSpeedPxPerSecond)
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
                dur={`${flowDuration}s`}
                repeatCount="indefinite"
                rotate="auto"
                begin={`${(-flowDuration / arrowCount) * index}s`}
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

    const inUx = inDx / inLen
    const inUy = inDy / inLen
    const outUx = outDx / outLen
    const outUy = outDy / outLen
    const cornerRadius = Math.min(radius, inLen / 2, outLen / 2)

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

function findPipeJunctions(paths: Array<Array<[number, number]>>) {
  const eps = 0.01
  const approxEq = (a: number, b: number) => Math.abs(a - b) <= eps
  const inRange = (value: number, start: number, end: number) => {
    const min = Math.min(start, end) - eps
    const max = Math.max(start, end) + eps
    return value >= min && value <= max
  }
  const addUnique = (acc: Array<[number, number]>, x: number, y: number) => {
    const exists = acc.some(([px, py]) => Math.abs(px - x) <= 0.5 && Math.abs(py - y) <= 0.5)
    if (!exists) acc.push([Number(x.toFixed(2)), Number(y.toFixed(2))])
  }

  const segmentsByPath = paths.map((points) => {
    const segments: Array<{ x1: number; y1: number; x2: number; y2: number; vertical: boolean }> = []
    for (let i = 1; i < points.length; i += 1) {
      const [x1, y1] = points[i - 1]
      const [x2, y2] = points[i]
      if (approxEq(x1, x2) && approxEq(y1, y2)) continue
      segments.push({ x1, y1, x2, y2, vertical: approxEq(x1, x2) })
    }
    return segments
  })

  const junctions: Array<[number, number]> = []
  for (let i = 0; i < segmentsByPath.length; i += 1) {
    for (let j = i + 1; j < segmentsByPath.length; j += 1) {
      for (const a of segmentsByPath[i]) {
        for (const b of segmentsByPath[j]) {
          if (a.vertical && !b.vertical) {
            const x = a.x1
            const y = b.y1
            if (inRange(x, b.x1, b.x2) && inRange(y, a.y1, a.y2)) addUnique(junctions, x, y)
            continue
          }
          if (!a.vertical && b.vertical) {
            const x = b.x1
            const y = a.y1
            if (inRange(x, a.x1, a.x2) && inRange(y, b.y1, b.y2)) addUnique(junctions, x, y)
            continue
          }

          if (a.vertical && b.vertical && approxEq(a.x1, b.x1)) {
            if (inRange(a.y1, b.y1, b.y2)) addUnique(junctions, a.x1, a.y1)
            if (inRange(a.y2, b.y1, b.y2)) addUnique(junctions, a.x1, a.y2)
            if (inRange(b.y1, a.y1, a.y2)) addUnique(junctions, a.x1, b.y1)
            if (inRange(b.y2, a.y1, a.y2)) addUnique(junctions, a.x1, b.y2)
            continue
          }

          if (!a.vertical && !b.vertical && approxEq(a.y1, b.y1)) {
            if (inRange(a.x1, b.x1, b.x2)) addUnique(junctions, a.x1, a.y1)
            if (inRange(a.x2, b.x1, b.x2)) addUnique(junctions, a.x2, a.y1)
            if (inRange(b.x1, a.x1, a.x2)) addUnique(junctions, b.x1, a.y1)
            if (inRange(b.x2, a.x1, a.x2)) addUnique(junctions, b.x2, a.y1)
          }
        }
      }
    }
  }

  return junctions
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
